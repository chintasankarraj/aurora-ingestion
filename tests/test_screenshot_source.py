"""Comprehensive test suite for Screenshots / OCR ingestion connector (ScreenshotSource).

Tests cover:
- Image SHA-256 computation and deterministic identity
- Image file validation (existence, format, 0-byte, max-size, corrupt, pixel limit, decompression bomb)
- Image preprocessing (RGBA/alpha handling, grayscale conversion, Lanczos upscaling for small images, contrast enhancement)
- OCR text cleaning and normalization
- Title sanitization and length bounds
- Markdown body formatting and image wikilink embedding
- OCR execution with dependency injection, timeout handling, and Tesseract error handling
- Single file, multi-file, directory, and recursive directory scanning
- Directory error resilience (skipping corrupted images)
- MarkdownNote conversion and Ingested/Screenshots routing
- Converter attribution block rendering
- End-to-end IngestionPipeline execution, attachment saving to Attachments/Ingested/, and tracker deduplication
- CLI execution via ingest-screenshots and ingest --source screenshots
- SourceRegistry registration for 'screenshots' and 'screenshot'

Zero dependencies on system Tesseract installation (all tests use mock OCR engines or dependency injection).
"""

from __future__ import annotations

import argparse
import io
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from PIL import Image

from config import IngestionConfig
from converter import (
    build_attribution_block,
    render_markdown_document,
    write_note_to_vault,
)
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import Attachment, MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.screenshot_source import (
    DEFAULT_MAX_FILE_SIZE,
    DEFAULT_MAX_PIXELS,
    SUPPORTED_IMAGE_EXTENSIONS,
    ScreenshotSource,
    clean_ocr_text,
    compute_image_sha256,
    format_screenshot_body,
    preprocess_image,
    sanitize_screenshot_title,
    validate_image_file,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers & Fixtures
# ---------------------------------------------------------------------------

def create_image_file(
    path: Path,
    size: tuple[int, int] = (100, 100),
    color: tuple[int, int, int] = (255, 255, 255),
    img_format: str = "PNG",
    mode: str = "RGB",
) -> Path:
    """Helper to create a valid test image file on disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    img = Image.new(mode, size, color)
    img.save(path, format=img_format)
    return path


@pytest.fixture
def temp_vault(tmp_path: Path) -> Path:
    """Temporary Obsidian vault directory structure."""
    vault = tmp_path / "vault"
    vault.mkdir(parents=True, exist_ok=True)
    (vault / "Ingested" / "Screenshots").mkdir(parents=True, exist_ok=True)
    (vault / "Attachments" / "Ingested").mkdir(parents=True, exist_ok=True)
    return vault


@pytest.fixture
def temp_config(temp_vault: Path, tmp_path: Path) -> IngestionConfig:
    """IngestionConfig pointing to the temporary test vault and tracker."""
    db_path = tmp_path / "tracker.db"
    return IngestionConfig(
        vault_path=temp_vault,
        tracker_db_path=db_path,
    )


@pytest.fixture
def temp_tracker(tmp_path: Path) -> DeduplicationTracker:
    """Temporary SQLite deduplication tracker."""
    db_path = tmp_path / "test_tracker.db"
    tracker = DeduplicationTracker(db_path)
    yield tracker
    tracker.close()


# ---------------------------------------------------------------------------
# 1. Hashing & Identity Tests
# ---------------------------------------------------------------------------

def test_compute_image_sha256_deterministic(tmp_path: Path):
    """compute_image_sha256 produces a valid, deterministic SHA-256 digest."""
    img_path = create_image_file(tmp_path / "test.png", size=(50, 50))
    hash1 = compute_image_sha256(img_path)
    hash2 = compute_image_sha256(img_path)

    assert len(hash1) == 64
    assert hash1 == hash2


def test_compute_image_sha256_missing_file(tmp_path: Path):
    """compute_image_sha256 raises SourceError when the file does not exist."""
    with pytest.raises(SourceError, match="Failed to read image file"):
        compute_image_sha256(tmp_path / "nonexistent.png")


def test_source_id_format(tmp_path: Path):
    """Source ID follows the deterministic 'screenshot:sha256:<hash>' convention."""
    img_path = create_image_file(tmp_path / "shot.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Mock text")
    item = source._process_single_image(img_path)

    assert item.source_id.startswith("screenshot:sha256:")
    assert len(item.source_id.split(":")[-1]) == 64
    assert item.content_hash == item.source_id.split(":")[-1]


# ---------------------------------------------------------------------------
# 2. Image Validation Tests
# ---------------------------------------------------------------------------

def test_validate_image_file_valid(tmp_path: Path):
    """Valid image passes validation and returns correct metadata."""
    img_path = create_image_file(tmp_path / "valid.png", size=(120, 80))
    resolved_path, meta = validate_image_file(img_path)

    assert resolved_path == img_path.resolve()
    assert meta["width"] == 120
    assert meta["height"] == 80
    assert meta["format"].upper() == "PNG"
    assert meta["file_size_bytes"] > 0


def test_validate_image_file_nonexistent(tmp_path: Path):
    """Validation fails if image file does not exist."""
    with pytest.raises(SourceError, match="Image file does not exist"):
        validate_image_file(tmp_path / "missing.png")


def test_validate_image_file_directory(tmp_path: Path):
    """Validation fails if path is a directory."""
    with pytest.raises(SourceError, match="Target path is a directory"):
        validate_image_file(tmp_path)


def test_validate_image_file_unsupported_extension(tmp_path: Path):
    """Validation fails if image has unsupported extension."""
    text_file = tmp_path / "doc.txt"
    text_file.write_text("Hello")
    with pytest.raises(SourceError, match="Unsupported image format"):
        validate_image_file(text_file)


def test_validate_image_file_empty(tmp_path: Path):
    """Validation fails if image file has 0 bytes."""
    empty_file = tmp_path / "empty.png"
    empty_file.write_bytes(b"")
    with pytest.raises(SourceError, match="Image file is empty"):
        validate_image_file(empty_file)


def test_validate_image_file_exceeds_max_size(tmp_path: Path):
    """Validation fails if image exceeds configured max_file_size."""
    img_path = create_image_file(tmp_path / "large.png", size=(100, 100))
    file_size = img_path.stat().st_size
    with pytest.raises(SourceError, match="exceeds maximum size limit"):
        validate_image_file(img_path, max_file_size=file_size - 1)


def test_validate_image_file_corrupt(tmp_path: Path):
    """Validation fails if image file contains corrupted data."""
    corrupt_file = tmp_path / "corrupt.png"
    corrupt_file.write_bytes(b"\x89PNG\r\n\x1a\nNotAValidImagePayloadGarbageData")
    with pytest.raises(SourceError, match="Corrupt or invalid image file"):
        validate_image_file(corrupt_file)


def test_validate_image_file_exceeds_max_pixels(tmp_path: Path):
    """Validation fails if image dimensions exceed max_pixels."""
    img_path = create_image_file(tmp_path / "pixels.png", size=(200, 200))
    with pytest.raises(SourceError, match=r"(exceed maximum allowed pixel limit|decompression bomb pixel limit)"):
        validate_image_file(img_path, max_pixels=100)  # 200*200 = 40,000 > 100


# ---------------------------------------------------------------------------
# 3. Image Preprocessing Tests
# ---------------------------------------------------------------------------

def test_preprocess_image_rgba_to_grayscale():
    """RGBA image with transparency is composited onto white background and converted to grayscale."""
    img = Image.new("RGBA", (100, 100), (255, 0, 0, 128))
    processed = preprocess_image(img)

    assert processed.mode == "L"


def test_preprocess_image_upscaling_small():
    """Small image with dimension < 600px is upscaled with Lanczos filter."""
    img = Image.new("RGB", (200, 150), (200, 200, 200))
    processed = preprocess_image(img)

    # Scale factor = max(600/200, 600/150) = 4.0
    # New size: 200*4=800, 150*4=600
    assert processed.width >= 600 or processed.height >= 600
    assert processed.width > 200
    assert processed.height > 150


def test_preprocess_image_large_not_upscaled():
    """Image >= 600px in both dimensions is not enlarged."""
    img = Image.new("RGB", (800, 700), (128, 128, 128))
    processed = preprocess_image(img)

    assert processed.size == (800, 700)
    assert processed.mode == "L"


# ---------------------------------------------------------------------------
# 4. Text Cleaning & Title Sanitization Tests
# ---------------------------------------------------------------------------

def test_clean_ocr_text_normal():
    """clean_ocr_text strips trailing whitespace and normalizes newlines."""
    raw = "  Line 1   \r\nLine 2  \r\n\r\n\r\n\r\nLine 3   \n"
    clean = clean_ocr_text(raw)

    assert clean == "Line 1\nLine 2\n\nLine 3"


def test_clean_ocr_text_empty():
    """clean_ocr_text returns empty string for whitespace or None."""
    assert clean_ocr_text("") == ""
    assert clean_ocr_text("   \n\t\r\n  ") == ""


def test_clean_ocr_text_strips_control_characters():
    """clean_ocr_text removes non-printable ASCII control characters."""
    raw = "Header\x00\x07Text\x1b"
    assert clean_ocr_text(raw) == "Header\x1bText" or "HeaderText" in clean_ocr_text(raw)


def test_sanitize_screenshot_title():
    """sanitize_screenshot_title replaces underscores/dashes, strips forbidden chars, bounds to 80 chars."""
    raw = "2026-10-06_terminal_error:alert?#1"
    clean = sanitize_screenshot_title(raw)

    assert clean == "2026 10 06 terminal erroralert1"
    assert len(clean) <= 80


def test_sanitize_screenshot_title_empty():
    """sanitize_screenshot_title defaults to 'Screenshot' when input is blank or all forbidden."""
    assert sanitize_screenshot_title("") == "Screenshot"
    assert sanitize_screenshot_title("   ") == "Screenshot"
    assert sanitize_screenshot_title("????") == "Screenshot"


def test_format_screenshot_body_with_text():
    """format_screenshot_body includes OCR text and image wikilink embed."""
    body = format_screenshot_body("Sample text from screen", "shot.png")

    assert "## OCR Text\n\nSample text from screen" in body
    assert "## Image\n\n![[shot.png]]" in body


def test_format_screenshot_body_no_text():
    """format_screenshot_body handles empty OCR text with *No text detected.*"""
    body = format_screenshot_body("", "shot.png")

    assert "## OCR Text\n\n*No text detected.*" in body
    assert "## Image\n\n![[shot.png]]" in body


# ---------------------------------------------------------------------------
# 5. ScreenshotSource OCR Engine & Error Handling Tests
# ---------------------------------------------------------------------------

def test_ocr_engine_dependency_injection(tmp_path: Path):
    """Custom injected ocr_engine is invoked without requiring pytesseract binary."""
    img_path = create_image_file(tmp_path / "diagram.png")
    
    mock_engine = MagicMock(return_value="Extracted architecture diagram labels")
    source = ScreenshotSource(ocr_lang="eng", ocr_engine=mock_engine)

    item = source._process_single_image(img_path)

    assert mock_engine.called
    assert item.extra_metadata["ocr_status"] == "success"
    assert item.extra_metadata["ocr_word_count"] == 4
    assert "Extracted architecture diagram labels" in item.content


def test_ocr_timeout_handling(tmp_path: Path):
    """OCR timeout raises descriptive SourceError."""
    img_path = create_image_file(tmp_path / "slow.png")

    def slow_engine(img, **kw):
        raise subprocess.TimeoutExpired(cmd=["tesseract"], timeout=5)

    source = ScreenshotSource(ocr_timeout=5.0, ocr_engine=slow_engine)

    with pytest.raises(SourceError, match="OCR processing timed out after 5.0s"):
        source._process_single_image(img_path)


def test_ocr_timeout_fractional_0_5(tmp_path: Path):
    """ocr_timeout=0.5 preserves fractional precision and is passed to the OCR engine as 0.5."""
    img_path = create_image_file(tmp_path / "shot_05.png")
    captured_kwargs = {}

    def mock_engine(img, **kw):
        captured_kwargs.update(kw)
        return "Detected 0.5s"

    source = ScreenshotSource(ocr_timeout=0.5, ocr_engine=mock_engine)
    source._process_single_image(img_path)

    assert captured_kwargs.get("timeout") == 0.5
    assert source.ocr_timeout == 0.5


def test_ocr_timeout_fractional_1_5(tmp_path: Path):
    """ocr_timeout=1.5 preserves fractional precision and is passed to the OCR engine as 1.5."""
    img_path = create_image_file(tmp_path / "shot_15.png")
    captured_kwargs = {}

    def mock_engine(img, **kw):
        captured_kwargs.update(kw)
        return "Detected 1.5s"

    source = ScreenshotSource(ocr_timeout=1.5, ocr_engine=mock_engine)
    source._process_single_image(img_path)

    assert captured_kwargs.get("timeout") == 1.5
    assert source.ocr_timeout == 1.5


def test_ocr_timeout_integer_unchanged(tmp_path: Path):
    """Integer timeout (e.g. 10) is preserved and passed without truncation."""
    img_path = create_image_file(tmp_path / "shot_int.png")
    captured_kwargs = {}

    def mock_engine(img, **kw):
        captured_kwargs.update(kw)
        return "Detected 10s"

    source = ScreenshotSource(ocr_timeout=10, ocr_engine=mock_engine)
    source._process_single_image(img_path)

    assert captured_kwargs.get("timeout") == 10.0
    assert source.ocr_timeout == 10.0


def test_ocr_timeout_zero_rejected():
    """Configuring ocr_timeout=0 or 0.0 raises an actionable SourceError."""
    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        ScreenshotSource(ocr_timeout=0)
    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        ScreenshotSource(ocr_timeout=0.0)


def test_ocr_timeout_negative_rejected():
    """Configuring a negative ocr_timeout raises an actionable SourceError."""
    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        ScreenshotSource(ocr_timeout=-1)
    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        ScreenshotSource(ocr_timeout=-0.5)


@pytest.mark.asyncio
async def test_ocr_timeout_runtime_override_validation(tmp_path: Path):
    """Runtime fetch_items rejects zero or negative ocr_timeout overrides."""
    img_path = create_image_file(tmp_path / "shot_val.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Text")

    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        await source.fetch_items(file=str(img_path), ocr_timeout=0)

    with pytest.raises(SourceError, match="Timeout must be greater than 0"):
        await source.fetch_items(file=str(img_path), ocr_timeout=-2.5)

    # Valid fractional override is accepted
    items = await source.fetch_items(file=str(img_path), ocr_timeout=0.75)
    assert len(items) == 1
    assert source.ocr_timeout == 0.75


def test_ocr_timeout_pytesseract_float_preserved(tmp_path: Path):
    """When pytesseract is called directly, fractional timeout is passed without integer conversion."""
    img_path = create_image_file(tmp_path / "shot_pytess.png")
    source = ScreenshotSource(ocr_timeout=2.5)

    import pytesseract
    with patch("pytesseract.image_to_string", return_value="Pytesseract Float Text") as mock_tess:
        source._process_single_image(img_path)
        mock_tess.assert_called_once()
        _, kwargs = mock_tess.call_args
        assert kwargs.get("timeout") == 2.5


def test_ocr_timeout_pytesseract_exception_handling(tmp_path: Path):
    """Pytesseract timeout exception handling produces descriptive SourceError."""
    img_path = create_image_file(tmp_path / "slow_tess.png")
    source = ScreenshotSource(ocr_timeout=1.5)

    import pytesseract
    with patch(
        "pytesseract.image_to_string",
        side_effect=subprocess.TimeoutExpired(cmd=["tesseract"], timeout=1.5),
    ):
        with pytest.raises(SourceError, match="OCR processing timed out after 1.5s"):
            source._process_single_image(img_path)


def test_ocr_tesseract_not_found(tmp_path: Path):
    """TesseractNotFoundError raises actionable SourceError instructing how to install."""
    img_path = create_image_file(tmp_path / "shot.png")

    import pytesseract
    with patch("pytesseract.image_to_string", side_effect=pytesseract.TesseractNotFoundError()):
        source = ScreenshotSource()
        with pytest.raises(SourceError, match="Tesseract OCR executable not found"):
            source._process_single_image(img_path)


def test_ocr_no_text_handling(tmp_path: Path):
    """Image with blank OCR result yields ocr_status 'no_text' and proper markdown placeholder."""
    img_path = create_image_file(tmp_path / "blank.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "   \n\n  ")

    item = source._process_single_image(img_path)

    assert item.extra_metadata["ocr_status"] == "no_text"
    assert item.extra_metadata["ocr_word_count"] == 0
    assert "*No text detected.*" in item.content
    assert "![[blank.png]]" in item.content


# ---------------------------------------------------------------------------
# 6. Fetch Items & Directory Scanning Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_items_missing_args():
    """fetch_items raises SourceError when no file or directory is specified."""
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "")
    with pytest.raises(SourceError, match="Screenshots source requires an image file or directory"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_fetch_items_single_file(tmp_path: Path):
    """fetch_items with --file fetches and processes the target image."""
    img_path = create_image_file(tmp_path / "screen_one.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Hello World")

    items = await source.fetch_items(file=str(img_path))

    assert len(items) == 1
    assert items[0].title == "screen one"
    assert items[0].attachments[0].filename == "screen_one.png"
    assert "Hello World" in items[0].content


@pytest.mark.asyncio
async def test_fetch_items_directory(tmp_path: Path):
    """fetch_items scans directory for all supported image formats in alphabetical order."""
    create_image_file(tmp_path / "b_shot.jpg")
    create_image_file(tmp_path / "a_shot.png")
    create_image_file(tmp_path / "c_shot.webp")
    (tmp_path / "readme.txt").write_text("ignore me")

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "OCR result")
    items = await source.fetch_items(directory=str(tmp_path))

    assert len(items) == 3
    filenames = [item.attachments[0].filename for item in items]
    assert filenames == ["a_shot.png", "b_shot.jpg", "c_shot.webp"]


@pytest.mark.asyncio
async def test_fetch_items_directory_recursive(tmp_path: Path):
    """fetch_items with recursive=True traverses subdirectories."""
    sub_dir = tmp_path / "nested" / "folder"
    create_image_file(tmp_path / "root.png")
    create_image_file(sub_dir / "nested.png")

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Nested text")
    items = await source.fetch_items(directory=str(tmp_path), recursive=True)

    assert len(items) == 2
    filenames = {item.attachments[0].filename for item in items}
    assert filenames == {"root.png", "nested.png"}


@pytest.mark.asyncio
async def test_fetch_items_directory_skips_invalid(tmp_path: Path):
    """In directory mode, invalid/corrupt images are skipped with a warning while valid images succeed."""
    create_image_file(tmp_path / "good.png")
    corrupt = tmp_path / "corrupt.png"
    corrupt.write_bytes(b"BadImageData")

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Good text")
    items = await source.fetch_items(directory=str(tmp_path))

    assert len(items) == 1
    assert items[0].attachments[0].filename == "good.png"


@pytest.mark.asyncio
async def test_fetch_items_empty_directory(tmp_path: Path):
    """fetch_items on directory with no images raises SourceError."""
    empty_dir = tmp_path / "empty_dir"
    empty_dir.mkdir()

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "")
    with pytest.raises(SourceError, match="No supported image files found"):
        await source.fetch_items(directory=str(empty_dir))


# ---------------------------------------------------------------------------
# 7. Markdown Conversion & Attribution Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_convert_to_markdown_folder():
    """convert_to_markdown sets folder to 'Ingested/Screenshots'."""
    source = ScreenshotSource()
    item = SourceItem(
        source_id="screenshot:sha256:abc123hash",
        source_type="screenshots",
        title="Settings Menu",
        content="## OCR Text\n\nOptions",
        date="2026-10-06T12:00:00Z",
        tags=["ingested", "screenshots", "ocr"],
        attachments=[Attachment(filename="settings.png", source_path=Path("settings.png"))],
    )

    note = await source.convert_to_markdown(item)

    assert note.folder == "Ingested/Screenshots"
    assert note.title == "Settings Menu"
    assert note.source == "screenshots"
    assert note.attachments == ["settings.png"]


def test_converter_attribution_block():
    """converter.py::build_attribution_block correctly formats screenshots attribution."""
    note = MarkdownNote(
        title="Error Dialog",
        source="screenshots",
        date="2026-10-06T14:30:00Z",
        body="## OCR Text\n\n404 Not Found",
        extra_metadata={
            "source_file": "error_dialog.png",
            "dimensions": {"width": 1920, "height": 1080},
            "format": "PNG",
            "ocr_status": "success",
            "ocr_word_count": 3,
        },
    )

    attr = build_attribution_block(note)

    assert "> **Source**: Local image — error_dialog.png" in attr
    assert "**Dimensions**: 1920x1080" in attr
    assert "**Format**: PNG" in attr
    assert "**OCR**: success" in attr
    assert "**Words**: 3" in attr
    assert "**Date**: 2026-10-06" in attr


# ---------------------------------------------------------------------------
# 8. End-to-End Pipeline & Deduplication Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pipeline_e2e_new_note(temp_config: IngestionConfig, temp_tracker: DeduplicationTracker, tmp_path: Path):
    """IngestionPipeline saves note to Ingested/Screenshots/ and attachment to Attachments/Ingested/."""
    img_path = create_image_file(tmp_path / "dashboard_view.png", size=(400, 300))
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Revenue: $10,000")

    pipeline = IngestionPipeline(config=temp_config, tracker=temp_tracker)
    
    with patch.object(SourceRegistry, "create", return_value=source):
        stats = await pipeline.run_source("screenshots", file=str(img_path))

    assert stats["total"] == 1
    assert stats["new"] == 1
    assert stats["unchanged"] == 0

    # Verify attachment was copied to Attachments/Ingested/
    attachment_file = temp_config.vault_path / "Attachments" / "Ingested" / "dashboard_view.png"
    assert attachment_file.exists()
    assert attachment_file.stat().st_size == img_path.stat().st_size

    # Verify note was created in Ingested/Screenshots/
    note_files = list((temp_config.vault_path / "Ingested" / "Screenshots").glob("*.md"))
    assert len(note_files) == 1
    content = note_files[0].read_text(encoding="utf-8")
    assert "Revenue: $10,000" in content
    assert "![[dashboard_view.png]]" in content
    assert "source: screenshots" in content


@pytest.mark.asyncio
async def test_pipeline_e2e_unchanged_deduplication(temp_config: IngestionConfig, temp_tracker: DeduplicationTracker, tmp_path: Path):
    """Running pipeline on unchanged screenshot reports UNCHANGED and avoids redundant writes."""
    img_path = create_image_file(tmp_path / "report.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Quarterly Summary")

    pipeline = IngestionPipeline(config=temp_config, tracker=temp_tracker)

    with patch.object(SourceRegistry, "create", return_value=source):
        stats1 = await pipeline.run_source("screenshots", file=str(img_path))
        assert stats1["new"] == 1

        stats2 = await pipeline.run_source("screenshots", file=str(img_path))
        assert stats2["new"] == 0
        assert stats2["unchanged"] == 1


# ---------------------------------------------------------------------------
# 9. SourceRegistry & CLI Tests
# ---------------------------------------------------------------------------

def test_source_registry_registration():
    """ScreenshotSource is registered as both 'screenshots' and 'screenshot'."""
    src1 = SourceRegistry.create("screenshots")
    src2 = SourceRegistry.create("screenshot")

    assert isinstance(src1, ScreenshotSource)
    assert isinstance(src2, ScreenshotSource)


@pytest.mark.asyncio
async def test_cli_ingest_screenshots_subcommand(temp_config: IngestionConfig, tmp_path: Path):
    """ingest-screenshots CLI command executes via async_main."""
    img_dir = tmp_path / "cli_shots"
    create_image_file(img_dir / "cli_image.png")

    mock_source = ScreenshotSource(ocr_engine=lambda img, **kw: "CLI Text")

    parser = create_parser()
    args = parser.parse_args([
        "--vault-path", str(temp_config.vault_path),
        "--tracker-db", str(temp_config.tracker_db_path),
        "ingest-screenshots",
        str(img_dir),
    ])

    with patch.object(SourceRegistry, "create", return_value=mock_source):
        exit_code = await async_main(args)

    assert exit_code == 0
    note_files = list((temp_config.vault_path / "Ingested" / "Screenshots").glob("*.md"))
    assert len(note_files) == 1


@pytest.mark.asyncio
async def test_cli_generic_ingest_screenshots(temp_config: IngestionConfig, tmp_path: Path):
    """ingest --source screenshots --directory <dir> executes via async_main."""
    img_dir = tmp_path / "generic_shots"
    create_image_file(img_dir / "generic.png")

    mock_source = ScreenshotSource(ocr_engine=lambda img, **kw: "Generic Text")

    parser = create_parser()
    args = parser.parse_args([
        "--vault-path", str(temp_config.vault_path),
        "--tracker-db", str(temp_config.tracker_db_path),
        "ingest",
        "--source", "screenshots",
        "--directory", str(img_dir),
    ])

    with patch.object(SourceRegistry, "create", return_value=mock_source):
        exit_code = await async_main(args)

    assert exit_code == 0
    note_files = list((temp_config.vault_path / "Ingested" / "Screenshots").glob("*.md"))
    assert len(note_files) == 1


# ---------------------------------------------------------------------------
# 10. Additional Edge Case & Integration Tests
# ---------------------------------------------------------------------------

def test_screenshot_source_display_name():
    """display_name returns human-readable 'Screenshots / OCR'."""
    source = ScreenshotSource()
    assert source.display_name == "Screenshots / OCR"
    assert source.source_type == "screenshots"


def test_various_image_extensions(tmp_path: Path):
    """validate_image_file successfully validates BMP, WEBP, and JPEG formats."""
    for ext, fmt in [(".bmp", "BMP"), (".webp", "WEBP"), (".jpg", "JPEG")]:
        img_path = create_image_file(tmp_path / f"test{ext}", img_format=fmt)
        resolved_path, meta = validate_image_file(img_path)
        assert resolved_path.exists()
        assert meta["format"].upper() in (fmt, "JPEG")


def test_preprocess_palette_mode_transparency():
    """Palette mode image with transparency info is handled without crashing."""
    img = Image.new("RGBA", (100, 100), (0, 128, 255, 128))
    pal = img.convert("P")
    processed = preprocess_image(pal)
    assert processed.mode == "L"


@pytest.mark.asyncio
async def test_fetch_items_path_arg_file_and_dir(tmp_path: Path):
    """fetch_items accepts generic 'path' argument pointing to either a file or directory."""
    f = create_image_file(tmp_path / "single_path.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Path Text")

    # Path to file
    items1 = await source.fetch_items(path=str(f))
    assert len(items1) == 1
    assert items1[0].attachments[0].filename == "single_path.png"

    # Path to directory
    items2 = await source.fetch_items(path=str(tmp_path))
    assert len(items2) >= 1


@pytest.mark.asyncio
async def test_fetch_items_files_csv_and_list(tmp_path: Path):
    """fetch_items accepts 'files' argument as comma-separated string or list."""
    f1 = create_image_file(tmp_path / "img1.png")
    f2 = create_image_file(tmp_path / "img2.png")
    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Batch Text")

    # CSV string
    items_csv = await source.fetch_items(files=f"{f1},{f2}")
    assert len(items_csv) == 2

    # List of paths
    items_list = await source.fetch_items(files=[str(f1), str(f2)])
    assert len(items_list) == 2


@pytest.mark.asyncio
async def test_fetch_items_runtime_overrides(tmp_path: Path):
    """fetch_items kwargs override source settings at runtime."""
    f = create_image_file(tmp_path / "override.png")
    source = ScreenshotSource(ocr_lang="eng", ocr_timeout=10.0, ocr_engine=lambda img, **kw: "Override Text")

    items = await source.fetch_items(
        file=str(f),
        ocr_lang="fra",
        ocr_timeout=25.0,
        max_file_size=5000000,
        max_pixels=20000000,
    )
    assert source.ocr_lang == "fra"
    assert source.ocr_timeout == 25.0
    assert source.max_file_size == 5000000
    assert source.max_pixels == 20000000


def test_pytesseract_tesseract_error(tmp_path: Path):
    """pytesseract.TesseractError raises SourceError with error details."""
    img_path = create_image_file(tmp_path / "tess_err.png")

    import pytesseract
    with patch("pytesseract.image_to_string", side_effect=pytesseract.TesseractError(1, "Internal OCR engine crash")):
        source = ScreenshotSource()
        with pytest.raises(SourceError, match="Tesseract OCR failed"):
            source._process_single_image(img_path)


@pytest.mark.asyncio
async def test_renamed_file_deduplication(temp_config: IngestionConfig, temp_tracker: DeduplicationTracker, tmp_path: Path):
    """Ingesting renamed file with identical content skips writing as UNCHANGED."""
    f1 = tmp_path / "original_shot.png"
    f2 = tmp_path / "renamed_shot.png"
    create_image_file(f1, color=(100, 150, 200))
    shutil.copy(f1, f2)

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Identical Content")
    pipeline = IngestionPipeline(config=temp_config, tracker=temp_tracker)

    with patch.object(SourceRegistry, "create", return_value=source):
        stats1 = await pipeline.run_source("screenshots", file=str(f1))
        assert stats1["new"] == 1

        stats2 = await pipeline.run_source("screenshots", file=str(f2))
        assert stats2["new"] == 0
        assert stats2["unchanged"] == 1


@pytest.mark.asyncio
async def test_pipeline_attachment_collision_resolution(temp_config: IngestionConfig, temp_tracker: DeduplicationTracker, tmp_path: Path):
    """When a new note has an attachment with the same name but different bytes as an existing attachment,
    the collision is resolved by renaming the attachment and updating the note wikilink."""
    dir1 = tmp_path / "batch1"
    dir2 = tmp_path / "batch2"

    img1 = create_image_file(dir1 / "screenshot.png", color=(100, 100, 100))
    img2 = create_image_file(dir2 / "screenshot.png", color=(200, 200, 200))

    source = ScreenshotSource(ocr_engine=lambda img, **kw: "Collision test")
    pipeline = IngestionPipeline(config=temp_config, tracker=temp_tracker)

    with patch.object(SourceRegistry, "create", return_value=source):
        stats1 = await pipeline.run_source("screenshots", file=str(img1))
        assert stats1["new"] == 1

        stats2 = await pipeline.run_source("screenshots", file=str(img2))
        assert stats2["new"] == 1

    # Check attachments in Attachments/Ingested/
    att_dir = temp_config.vault_path / "Attachments" / "Ingested"
    saved_atts = sorted(p.name for p in att_dir.glob("*.png"))
    assert "screenshot.png" in saved_atts
    assert "screenshot_2.png" in saved_atts

