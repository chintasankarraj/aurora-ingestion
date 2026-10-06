"""Screenshots / OCR ingestion connector using local Tesseract OCR engine.

Architecture:
    Local Screenshot / Image File
        ↓
    Image Validation (exists, readable, supported extension, non-zero, size limit, pixel limit)
        ↓
    Chunked SHA-256 Hashing (screenshot:sha256:<content_hash>)
        ↓
    Image Preprocessing (grayscale, alpha compositing, contrast enhancement, small image upscaling)
        ↓
    Local OCR Engine (pytesseract / Tesseract executable with configurable language & timeout)
        ↓
    Extracted Text Cleaning & Normalization (whitespace cleanup, newline collapse)
        ↓
    Aurora SourceItem & Attachment (copy original image to Attachments/Ingested)
        ↓
    Ingested/Screenshots/<date>_screenshots_<sanitized-title>.md (with ![[image]] embed)

Key Design Decisions:
- Local-first: Uses local Tesseract OCR engine with zero reliance on paid external APIs or cloud models.
- No external API keys or cloud tokens required.
- Memory safe: Chunked 64 KB SHA-256 computation avoids loading entire image files into memory for hashing.
- Decompression bomb safety: Checks Pillow pixel limits (Image.MAX_IMAGE_PIXELS) before decoding images.
- Unmodified attachments: Original image file is copied verbatim to Attachments/Ingested; preprocessing is applied
  only to an in-memory copy passed to OCR.
- Deterministic identity: Content-based SHA-256 hash ensures moved or renamed screenshots are deduplicated.
- Privacy protected: OCR text is never logged to application logs (could contain passwords, tokens, or PII).
  EXIF GPS geolocation tags are stripped.
- Testable via dependency injection: An optional `ocr_engine` callable can be injected for fast, deterministic,
  mock-driven testing without requiring a host Tesseract binary.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from PIL import Image, ImageEnhance

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

# Supported image file extensions for OCR ingestion
SUPPORTED_IMAGE_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".bmp",
    ".tiff",
    ".tif",
    ".gif",
}

DEFAULT_OCR_LANG = "eng"
DEFAULT_OCR_TIMEOUT = 30.0  # seconds
DEFAULT_MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB limit
DEFAULT_MAX_PIXELS = 100_000_000  # 100 Megapixels (decompression bomb protection)
CHUNK_SIZE_BYTES = 64 * 1024  # 64 KB chunk size for memory-safe hashing

# Characters forbidden in Obsidian filenames and note titles
FORBIDDEN_CHARS_PATTERN = re.compile(r'[\\/:*?"<>|#^[\]]')

# Non-printable control characters except standard whitespace (\t, \n, \r)
CONTROL_CHARS_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def compute_image_sha256(file_path: Union[str, Path], chunk_size: int = CHUNK_SIZE_BYTES) -> str:
    """Compute deterministic SHA-256 digest of an image file using chunked streaming.

    Avoids loading large image files into memory.
    """
    path = Path(file_path)
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
    except PermissionError as e:
        raise SourceError(f"Permission denied reading image file '{path}': {e}") from e
    except Exception as e:
        raise SourceError(f"Failed to read image file '{path}': {e}") from e
    return hasher.hexdigest()


def validate_image_file(
    file_path: Union[str, Path],
    max_file_size: int = DEFAULT_MAX_FILE_SIZE,
    max_pixels: int = DEFAULT_MAX_PIXELS,
) -> Tuple[Path, Dict[str, Any]]:
    """Validate that the given path exists, is a regular file, has a supported extension,
    is readable, does not exceed size limits, and is a valid non-corrupt image.

    Returns the resolved Path and probed metadata dictionary (width, height, format, file_size_bytes).
    Raises SourceError if any validation check fails.
    """
    path = Path(file_path).resolve()

    if not path.exists():
        raise SourceError(f"Image file does not exist: {path}")

    if path.is_dir():
        raise SourceError(
            f"Target path is a directory, not an image file: {path}. Use --directory instead."
        )

    if not path.is_file():
        raise SourceError(f"Target path is not a regular file: {path}")

    ext = path.suffix.lower()
    if ext not in SUPPORTED_IMAGE_EXTENSIONS:
        supported_list = ", ".join(sorted(SUPPORTED_IMAGE_EXTENSIONS))
        raise SourceError(
            f"Unsupported image format '{path.suffix}'. Supported formats: [{supported_list}]"
        )

    try:
        stat_info = path.stat()
    except Exception as e:
        raise SourceError(f"Cannot access image file metadata '{path}': {e}") from e

    if stat_info.st_size == 0:
        raise SourceError(f"Image file is empty (0 bytes): {path}")

    if stat_info.st_size > max_file_size:
        raise SourceError(
            f"Image file exceeds maximum size limit ({max_file_size:,} bytes): {path}"
        )

    # Verify basic read permission
    try:
        with path.open("rb") as f:
            f.read(1)
    except PermissionError as e:
        raise SourceError(f"Permission denied reading image file '{path}': {e}") from e
    except Exception as e:
        raise SourceError(f"Cannot read image file '{path}': {e}") from e

    # Pillow integrity & decompression bomb verification
    old_max_pixels = Image.MAX_IMAGE_PIXELS
    try:
        Image.MAX_IMAGE_PIXELS = max_pixels
        with Image.open(path) as img:
            w, h = img.size
            if (w * h) > max_pixels:
                raise SourceError(
                    f"Image dimensions ({w}x{h} = {w * h:,} pixels) exceed maximum allowed pixel limit "
                    f"({max_pixels:,} pixels): {path.name}"
                )
            fmt = img.format or ext.lstrip(".").upper()
            img.verify()
    except Image.DecompressionBombError as e:
        raise SourceError(
            f"Image exceeds decompression bomb pixel limit ({max_pixels:,} pixels): {path.name} ({e})"
        ) from e
    except SourceError:
        raise
    except Exception as e:
        raise SourceError(f"Corrupt or invalid image file '{path.name}': {e}") from e
    finally:
        Image.MAX_IMAGE_PIXELS = old_max_pixels

    # Re-open after verify() to read valid metadata (verify() invalidates image data)
    try:
        with Image.open(path) as img:
            width, height = img.size
            fmt = img.format or ext.lstrip(".").upper()
    except Exception as e:
        raise SourceError(f"Failed to read image attributes for '{path.name}': {e}") from e

    metadata: Dict[str, Any] = {
        "width": width,
        "height": height,
        "format": fmt,
        "file_size_bytes": stat_info.st_size,
    }

    return path, metadata


def preprocess_image(img: Image.Image) -> Image.Image:
    """Preprocess an image for OCR enhancement.

    Steps:
    1. Transparency handling: composite onto solid white background if RGBA/LA or palette with transparency.
    2. Convert to grayscale ('L').
    3. Small image upscaling: if width or height is under 600px, upscale with Lanczos filter to improve OCR accuracy.
    4. Contrast enhancement: increase contrast by 1.5x.

    Returns the preprocessed in-memory PIL Image. The original file is not modified.
    """
    # 1. Transparency / Alpha channel handling
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        rgba_img = img.convert("RGBA")
        white_bg = Image.new("RGBA", rgba_img.size, (255, 255, 255, 255))
        composite = Image.alpha_composite(white_bg, rgba_img)
        gray = composite.convert("L")
    elif img.mode != "L":
        gray = img.convert("L")
    else:
        gray = img.copy()

    # 2. Upscaling for small images (< 600px on either dimension)
    width, height = gray.size
    if width < 600 or height < 600:
        scale = max(600.0 / max(1, width), 600.0 / max(1, height))
        scale = min(scale, 4.0)  # Bound upscaling factor
        new_w = max(1, int(round(width * scale)))
        new_h = max(1, int(round(height * scale)))
        resample = getattr(getattr(Image, "Resampling", Image), "LANCZOS", Image.LANCZOS)
        gray = gray.resize((new_w, new_h), resample=resample)

    # 3. Contrast enhancement
    enhancer = ImageEnhance.Contrast(gray)
    enhanced = enhancer.enhance(1.5)

    return enhanced


def clean_ocr_text(raw_text: str) -> str:
    """Clean and normalize text extracted via OCR.

    - Replaces carriage returns with standard newlines.
    - Strips non-printable control characters.
    - Strips trailing whitespace on each line.
    - Collapses 3 or more consecutive newlines into double newlines.
    - Strips overall leading/trailing whitespace.
    """
    if not raw_text:
        return ""

    text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    text = CONTROL_CHARS_PATTERN.sub("", text)

    lines = [line.rstrip() for line in text.split("\n")]
    text = "\n".join(lines)

    # Collapse 3 or more newlines to 2
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def sanitize_screenshot_title(raw_stem: str) -> str:
    """Derive a clean Obsidian note title from a screenshot filename stem.

    - Replaces underscores and hyphens with spaces.
    - Strips forbidden Obsidian characters.
    - Collapses multiple whitespace.
    - Bounds length to 80 characters.
    """
    if not raw_stem:
        return "Screenshot"

    clean = raw_stem.replace("_", " ").replace("-", " ")
    clean = FORBIDDEN_CHARS_PATTERN.sub("", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    clean = clean[:80].rstrip()
    return clean or "Screenshot"


def format_screenshot_body(clean_text: str, image_filename: str) -> str:
    """Format the Markdown note body with OCR text and original image embed."""
    lines: List[str] = ["## OCR Text\n"]
    if clean_text:
        lines.append(f"{clean_text}\n")
    else:
        lines.append("*No text detected.*\n")

    lines.append(f"## Image\n\n![[{image_filename}]]")
    return "\n".join(lines).strip()


# ---------------------------------------------------------------------------
# Screenshot Ingestion Source Connector
# ---------------------------------------------------------------------------

class ScreenshotSource(BaseSource):
    """Local-first Screenshots / OCR ingestion connector using Tesseract."""

    source_type = "screenshots"

    def __init__(
        self,
        ocr_lang: Optional[str] = None,
        tesseract_cmd: Optional[str] = None,
        ocr_timeout: Optional[float] = None,
        max_file_size: Optional[int] = None,
        max_pixels: Optional[int] = None,
        recursive: bool = False,
        ocr_engine: Optional[Callable[..., str]] = None,
    ):
        self.ocr_lang = ocr_lang or os.environ.get("SCREENSHOT_OCR_LANG") or DEFAULT_OCR_LANG
        self.tesseract_cmd = tesseract_cmd or os.environ.get("SCREENSHOT_TESSERACT_CMD")
        
        timeout_env = os.environ.get("SCREENSHOT_OCR_TIMEOUT")
        raw_timeout = ocr_timeout if ocr_timeout is not None else (timeout_env or DEFAULT_OCR_TIMEOUT)
        try:
            timeout_val = float(raw_timeout)
        except (ValueError, TypeError) as e:
            raise SourceError(f"Invalid OCR timeout value '{raw_timeout}': must be a positive number.") from e

        if timeout_val <= 0:
            raise SourceError(
                f"Invalid OCR timeout '{timeout_val}'. Timeout must be greater than 0 seconds."
            )
        self.ocr_timeout = timeout_val

        size_env = os.environ.get("SCREENSHOT_MAX_FILE_SIZE")
        self.max_file_size = int(max_file_size if max_file_size is not None else (size_env or DEFAULT_MAX_FILE_SIZE))

        pixels_env = os.environ.get("SCREENSHOT_MAX_PIXELS")
        self.max_pixels = int(max_pixels if max_pixels is not None else (pixels_env or DEFAULT_MAX_PIXELS))

        self.recursive = bool(recursive)

        # Optional dependency-injected OCR engine for fast, mockable testing without system Tesseract
        self._ocr_engine = ocr_engine

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Screenshots / OCR"

    def _perform_ocr(self, img: Image.Image) -> str:
        """Execute OCR on a preprocessed PIL Image.

        Uses the injected OCR engine if provided, otherwise calls pytesseract.image_to_string.
        Privacy rule: The returned text is never logged.
        """
        if self._ocr_engine is not None:
            try:
                return self._ocr_engine(img, lang=self.ocr_lang, timeout=self.ocr_timeout)
            except Exception as e:
                if isinstance(e, SourceError):
                    raise
                err_str = str(e).lower()
                if "timeout" in err_str or "timed out" in err_str or isinstance(e, subprocess.TimeoutExpired):
                    raise SourceError(f"OCR processing timed out after {self.ocr_timeout}s for image.") from e
                raise SourceError(f"OCR engine execution failed: {e}") from e

        try:
            import pytesseract
        except ImportError as e:
            raise SourceError(
                "pytesseract is not installed. Install it with: pip install pytesseract"
            ) from e

        if self.tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd

        timeout_sec = self.ocr_timeout

        try:
            return pytesseract.image_to_string(
                img,
                lang=self.ocr_lang,
                timeout=timeout_sec,
            )
        except pytesseract.TesseractNotFoundError as e:
            raise SourceError(
                "Tesseract OCR executable not found. Please install Tesseract (e.g. 'brew install tesseract' "
                "or 'apt-get install tesseract-ocr' or download from UB-Mannheim on Windows) "
                "or specify --tesseract-cmd or SCREENSHOT_TESSERACT_CMD."
            ) from e
        except pytesseract.TesseractError as e:
            raise SourceError(f"Tesseract OCR failed: {e}") from e
        except (subprocess.TimeoutExpired, RuntimeError) as e:
            if "timeout" in str(e).lower() or isinstance(e, subprocess.TimeoutExpired):
                raise SourceError(f"OCR processing timed out after {self.ocr_timeout}s for image.") from e
            raise SourceError(f"OCR execution failed: {e}") from e
        except Exception as e:
            raise SourceError(f"Unexpected error during OCR execution: {e}") from e

    def _process_single_image(self, image_path: Path) -> SourceItem:
        """Validate, hash, preprocess, OCR, and build SourceItem for a single image file."""
        # 1. Validate image file and extract dimension/format metadata
        valid_path, probed_meta = validate_image_file(
            file_path=image_path,
            max_file_size=self.max_file_size,
            max_pixels=self.max_pixels,
        )

        # 2. Content-based deterministic hash and stable source ID
        content_hash = compute_image_sha256(valid_path)
        source_id = f"screenshot:sha256:{content_hash}"

        # 3. Load and preprocess image for OCR
        try:
            with Image.open(valid_path) as raw_img:
                preprocessed = preprocess_image(raw_img)
        except Exception as e:
            raise SourceError(f"Failed to preprocess image '{valid_path.name}': {e}") from e

        # 4. Perform OCR
        raw_ocr_text = self._perform_ocr(preprocessed)

        # 5. Clean extracted text
        clean_text = clean_ocr_text(raw_ocr_text)
        word_count = len(clean_text.split()) if clean_text else 0
        ocr_status = "success" if clean_text else "no_text"

        logger.info(
            "Completed OCR for image '%s' (status: %s, words: %d)",
            valid_path.name,
            ocr_status,
            word_count,
        )

        # 6. Sanitize note title
        title = sanitize_screenshot_title(valid_path.stem)

        # 7. Construct note body
        body = format_screenshot_body(clean_text, valid_path.name)

        # 8. Extract modification timestamp
        try:
            mtime = valid_path.stat().st_mtime
            date_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
        except Exception:
            date_iso = datetime.now(timezone.utc).isoformat()

        # 9. Create Attachment pointing to original unmodified image
        fmt = probed_meta.get("format", "PNG")
        attachment = Attachment(
            filename=valid_path.name,
            source_path=valid_path,
            mime_type=f"image/{fmt.lower()}",
        )

        extra_meta: Dict[str, Any] = {
            "source_file": valid_path.name,
            "dimensions": {
                "width": probed_meta.get("width", 0),
                "height": probed_meta.get("height", 0),
            },
            "format": fmt,
            "file_size_bytes": probed_meta.get("file_size_bytes", valid_path.stat().st_size),
            "ocr_engine": "tesseract",
            "ocr_lang": self.ocr_lang,
            "ocr_status": ocr_status,
            "ocr_word_count": word_count,
            "content_hash": content_hash,
        }

        item = SourceItem(
            source_id=source_id,
            title=title,
            source_type="screenshots",
            content=body,
            date=date_iso,
            source_url=None,
            author=None,
            tags=["ingested", "screenshots", "ocr"],
            attachments=[attachment],
            summary=clean_text[:200].strip() if clean_text else None,
            word_count=word_count,
            extra_metadata=extra_meta,
            content_hash=content_hash,
        )

        return item

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch and OCR items from one or more local screenshot/image files.

        Supported parameters:
        - directory / dir: Path to directory containing images
        - file / file_path: Single image file path
        - files: Comma-separated or list of image file paths
        - path: File or directory path
        - recursive: Whether to scan subdirectories recursively
        - ocr_lang: Override target OCR language
        - tesseract_cmd: Override tesseract binary path
        - ocr_timeout: Override OCR timeout in seconds
        - max_file_size: Override maximum file size in bytes
        - max_pixels: Override maximum image pixels
        """
        # Apply runtime overrides
        if kwargs.get("ocr_lang"):
            self.ocr_lang = str(kwargs["ocr_lang"])
        if kwargs.get("tesseract_cmd"):
            self.tesseract_cmd = str(kwargs["tesseract_cmd"])
        if kwargs.get("ocr_timeout") is not None:
            raw_t = kwargs["ocr_timeout"]
            try:
                t_val = float(raw_t)
            except (ValueError, TypeError) as e:
                raise SourceError(f"Invalid OCR timeout value '{raw_t}': must be a positive number.") from e
            if t_val <= 0:
                raise SourceError(f"Invalid OCR timeout '{t_val}'. Timeout must be greater than 0 seconds.")
            self.ocr_timeout = t_val
        if kwargs.get("max_file_size") is not None:
            self.max_file_size = int(kwargs["max_file_size"])
        if kwargs.get("max_pixels") is not None:
            self.max_pixels = int(kwargs["max_pixels"])
        if "recursive" in kwargs:
            self.recursive = bool(kwargs["recursive"])

        # 1. Resolve candidate paths
        candidate_paths: List[Path] = []
        is_directory_mode = False

        dir_arg = kwargs.get("directory") or kwargs.get("dir")
        file_arg = kwargs.get("file") or kwargs.get("file_path")
        files_arg = kwargs.get("files")
        path_arg = kwargs.get("path")

        if dir_arg:
            dir_path = Path(dir_arg).resolve()
            if not dir_path.exists():
                raise SourceError(f"Screenshots directory does not exist: {dir_path}")
            if not dir_path.is_dir():
                raise SourceError(f"Screenshots directory path is not a directory: {dir_path}")
            is_directory_mode = True
            iterator = dir_path.rglob("*") if self.recursive else dir_path.iterdir()
            candidate_paths = [
                p for p in sorted(iterator)
                if p.is_file() and p.suffix.lower() in SUPPORTED_IMAGE_EXTENSIONS
            ]
            if not candidate_paths:
                raise SourceError(f"No supported image files found in directory: {dir_path}")

        elif files_arg:
            if isinstance(files_arg, str):
                raw_list = [p.strip() for p in files_arg.split(",") if p.strip()]
            else:
                raw_list = [str(p).strip() for p in files_arg if str(p).strip()]
            if not raw_list:
                raise SourceError("No image files specified in --files.")
            candidate_paths = [Path(p).resolve() for p in raw_list]

        elif file_arg:
            candidate_paths = [Path(file_arg).resolve()]

        elif path_arg:
            target = Path(path_arg).resolve()
            if not target.exists():
                raise SourceError(f"Screenshot path does not exist: {target}")
            if target.is_dir():
                is_directory_mode = True
                iterator = target.rglob("*") if self.recursive else target.iterdir()
                candidate_paths = [
                    p for p in sorted(iterator)
                    if p.is_file() and p.suffix.lower() in SUPPORTED_IMAGE_EXTENSIONS
                ]
                if not candidate_paths:
                    raise SourceError(f"No supported image files found in directory: {target}")
            else:
                candidate_paths = [target]
        else:
            raise SourceError(
                "Screenshots source requires an image file or directory. "
                "Specify --file <image.png> or --directory <./screenshots>."
            )

        items: List[SourceItem] = []

        # 2. Process each image file
        for image_path in candidate_paths:
            try:
                item = self._process_single_image(image_path)
                items.append(item)
            except SourceError as se:
                if is_directory_mode:
                    logger.warning("Skipping failed image file in directory (%s): %s", image_path.name, se)
                    continue
                raise
            except Exception as e:
                if is_directory_mode:
                    logger.warning("Unexpected error processing image %s: %s", image_path.name, e)
                    continue
                raise SourceError(f"Failed to process image '{image_path.name}': {e}") from e

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a Screenshots SourceItem into frontmatter metadata and Markdown body targeted at Ingested/Screenshots/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Screenshots"
        return note


# Register Screenshot connector in SourceRegistry
SourceRegistry.register("screenshots", ScreenshotSource)
SourceRegistry.register("screenshot", ScreenshotSource)
