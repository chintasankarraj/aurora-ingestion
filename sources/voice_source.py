"""Voice / Audio ingestion connector using local Whisper speech-to-text models.

Architecture:
    Local Audio File
        ↓
    Audio Validation (exists, readable, supported extension, non-zero)
        ↓
    Chunked SHA-256 Hashing (voice:sha256:<content_hash>)
        ↓
    Local Speech-to-Text Model (faster-whisper / WhisperModel)
        ↓
    Transcript Normalization & Readable Timestamp Formatting ([HH:MM:SS - HH:MM:SS])
        ↓
    Aurora SourceItem
        ↓
    Ingested/Voice/<date>_voice_<sanitized-title>.md

Key Design Decisions:
- Local-first: Uses faster-whisper or local Whisper models with zero reliance on paid external APIs.
- No hardcoded API keys or external secrets required.
- Memory safe: Chunked 64 KB SHA-256 computation to avoid loading large audio files into memory.
- Safe path handling: Argument arrays only, no shell=True or string interpolation.
- Deterministic identity: Content-based SHA-256 hash ensures renamed/moved audio files are deduplicated.
- Testable via dependency injection: Transcriber instance can be injected for fast, mock-driven tests
  without downloading model weights.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

# Common audio extensions supported for ingestion
SUPPORTED_AUDIO_EXTENSIONS = {
    ".wav",
    ".mp3",
    ".m4a",
    ".flac",
    ".ogg",
    ".opus",
    ".aac",
    ".webm",
}

DEFAULT_MODEL = "small"
DEFAULT_DEVICE = "cpu"
DEFAULT_COMPUTE_TYPE = "default"
DEFAULT_BEAM_SIZE = 5
DEFAULT_MAX_DURATION = 7200.0  # 2 hours in seconds
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB limit
CHUNK_SIZE_BYTES = 64 * 1024  # 64 KB chunk size for memory-safe hashing

# Characters forbidden in Obsidian filenames and note titles
FORBIDDEN_CHARS_PATTERN = re.compile(r'[\\/:*?"<>|#^[\]]')


# ---------------------------------------------------------------------------
# Data Models for Transcription
# ---------------------------------------------------------------------------

@dataclass
class TranscriptSegment:
    """A single transcribed speech segment with start/end timestamps and text."""
    start: float
    end: float
    text: str


@dataclass
class TranscriptionResult:
    """Complete output of audio speech-to-text transcription."""
    text: str
    segments: List[TranscriptSegment] = field(default_factory=list)
    language: str = "en"
    language_probability: float = 1.0
    duration: float = 0.0


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def compute_audio_sha256(file_path: Union[str, Path], chunk_size: int = CHUNK_SIZE_BYTES) -> str:
    """Compute deterministic SHA-256 digest of an audio file using chunked streaming.

    Avoids loading large audio files into memory.
    """
    path = Path(file_path)
    hasher = hashlib.sha256()
    try:
        with path.open("rb") as f:
            while chunk := f.read(chunk_size):
                hasher.update(chunk)
    except PermissionError as e:
        raise SourceError(f"Permission denied reading audio file '{path}': {e}") from e
    except Exception as e:
        raise SourceError(f"Failed to read audio file '{path}': {e}") from e
    return hasher.hexdigest()


def format_timestamp(seconds: Union[int, float], include_millis: bool = False) -> str:
    """Format duration in seconds to standard [HH:MM:SS] or [HH:MM:SS.mmm]."""
    total_sec = max(0.0, float(seconds))
    hours = int(total_sec // 3600)
    minutes = int((total_sec % 3600) // 60)
    secs = int(total_sec % 60)
    if include_millis:
        millis = int(round((total_sec - int(total_sec)) * 1000))
        return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_voice_duration(seconds: Union[int, float]) -> str:
    """Format duration in seconds for frontmatter and attribution display (HH:MM:SS)."""
    return format_timestamp(seconds, include_millis=False)


def validate_audio_file(
    file_path: Union[str, Path],
    max_file_size: int = MAX_FILE_SIZE_BYTES,
) -> Path:
    """Validate that the given path exists, is a regular file, has a supported extension, and is readable.

    Raises SourceError if any validation check fails.
    """
    path = Path(file_path).resolve()

    if not path.exists():
        raise SourceError(f"Audio file does not exist: {path}")

    if path.is_dir():
        raise SourceError(
            f"Target path is a directory, not an audio file: {path}. Use --directory instead."
        )

    if not path.is_file():
        raise SourceError(f"Target path is not a regular file: {path}")

    ext = path.suffix.lower()
    if ext not in SUPPORTED_AUDIO_EXTENSIONS:
        supported_list = ", ".join(sorted(SUPPORTED_AUDIO_EXTENSIONS))
        raise SourceError(
            f"Unsupported audio format '{path.suffix}'. Supported formats: [{supported_list}]"
        )

    try:
        stat_info = path.stat()
    except Exception as e:
        raise SourceError(f"Cannot access audio file metadata '{path}': {e}") from e

    if stat_info.st_size == 0:
        raise SourceError(f"Audio file is empty (0 bytes): {path}")

    if stat_info.st_size > max_file_size:
        raise SourceError(
            f"Audio file exceeds maximum size limit ({max_file_size:,} bytes): {path}"
        )

    # Verify basic read permission
    try:
        with path.open("rb") as f:
            f.read(1)
    except PermissionError as e:
        raise SourceError(f"Permission denied reading audio file '{path}': {e}") from e
    except Exception as e:
        raise SourceError(f"Cannot read audio file '{path}': {e}") from e

    return path


def probe_audio_metadata(path: Path) -> Dict[str, Any]:
    """Safely probe audio file metadata (duration, sample rate, channels, codec, title tag).

    Uses PyAV (`av`) if available, falling back gracefully to basic file properties.
    """
    metadata: Dict[str, Any] = {
        "filename": path.name,
        "extension": path.suffix.lower(),
        "file_size_bytes": path.stat().st_size,
    }

    try:
        import av
        with av.open(str(path)) as container:
            if container.duration is not None and av.time_base:
                metadata["duration_seconds"] = round(float(container.duration) / av.time_base, 2)

            for stream in container.streams.audio:
                if stream.sample_rate:
                    metadata["sample_rate"] = stream.sample_rate
                if stream.channels:
                    metadata["channels"] = stream.channels
                if stream.bit_rate:
                    metadata["bitrate"] = stream.bit_rate
                if stream.codec_context and stream.codec_context.name:
                    metadata["codec"] = stream.codec_context.name
                break

            if container.metadata:
                title = container.metadata.get("title") or container.metadata.get("TITLE")
                if title:
                    metadata["title_tag"] = str(title).strip()
    except Exception:
        # PyAV not installed, or unparseable container header; graceful degradation
        pass

    return metadata


def sanitize_voice_title(raw_title: str) -> str:
    """Sanitize note title derived from embedded metadata or filename stem."""
    if not raw_title:
        return "Audio Recording"
    clean = FORBIDDEN_CHARS_PATTERN.sub("", raw_title).strip()
    clean = re.sub(r"\s+", " ", clean).strip()
    clean = clean[:80].rstrip()
    return clean or "Audio Recording"


def render_voice_transcript(segments: List[TranscriptSegment]) -> str:
    """Format transcribed speech segments into clean Markdown with readable timestamp ranges."""
    if not segments:
        return "## Transcript\n\n*No speech detected in audio file.*"

    lines: List[str] = ["## Transcript\n"]

    for seg in segments:
        start_str = format_timestamp(seg.start)
        end_str = format_timestamp(seg.end)
        text = seg.text.strip()
        if not text:
            continue
        lines.append(f"[{start_str} - {end_str}]\n\n{text}\n")

    if len(lines) == 1:
        return "## Transcript\n\n*No speech detected in audio file.*"

    return "\n".join(lines).strip()


def is_cuda_available() -> bool:
    """Check if CUDA device is available via ctranslate2.

    Returns True if ctranslate2 reports at least 1 CUDA device, False otherwise.
    Safe against ImportError and runtime exceptions.
    """
    try:
        import ctranslate2
        return bool(ctranslate2.get_cuda_device_count() > 0)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Voice Ingestion Source Connector
# ---------------------------------------------------------------------------

class VoiceSource(BaseSource):
    """Local-first Voice / Audio ingestion connector using Whisper-compatible models."""

    source_type = "voice"

    def __init__(
        self,
        model: Optional[str] = None,
        language: Optional[str] = None,
        device: Optional[str] = None,
        compute_type: Optional[str] = None,
        beam_size: int = DEFAULT_BEAM_SIZE,
        vad: bool = False,
        max_duration: Optional[float] = DEFAULT_MAX_DURATION,
        transcriber: Optional[Any] = None,
    ):
        self.model_name = model or os.environ.get("VOICE_MODEL") or DEFAULT_MODEL
        self.language = language or os.environ.get("VOICE_LANGUAGE")
        self.raw_device = (device or os.environ.get("VOICE_DEVICE") or DEFAULT_DEVICE).lower()
        self.device = self.raw_device
        self.compute_type = compute_type or os.environ.get("VOICE_COMPUTE_TYPE") or DEFAULT_COMPUTE_TYPE
        self.beam_size = max(1, int(beam_size))
        self.vad = bool(vad)
        self.max_duration = float(max_duration) if max_duration is not None else None

        # Resolve initial device if 'auto'
        if self.raw_device == "auto":
            self.device = "cuda" if is_cuda_available() else "cpu"

        # Optional dependency-injected transcriber (for fast unit testing without downloading models)
        self._transcriber = transcriber
        self._whisper_model = None

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Voice / Audio"

    def _resolve_device(self) -> str:
        """Resolve requested device ('auto', 'cuda', 'cpu') to a concrete device ('cuda' or 'cpu').

        - 'auto': chooses 'cuda' if CUDA is available, otherwise safely falls back to 'cpu'.
        - 'cuda': verifies CUDA availability; raises SourceError if unavailable.
        - 'cpu': resolves to 'cpu'.
        """
        requested = (getattr(self, "raw_device", None) or self.device or DEFAULT_DEVICE).lower()

        if requested == "auto":
            if is_cuda_available():
                self.device = "cuda"
            else:
                self.device = "cpu"
            return self.device

        if requested == "cuda":
            try:
                import ctranslate2
                cuda_count = ctranslate2.get_cuda_device_count()
                if cuda_count == 0:
                    raise SourceError(
                        "CUDA device was requested for Voice transcription, but CUDA is not available on this system. "
                        "Use '--device cpu' or set VOICE_DEVICE=cpu."
                    )
            except ImportError as e:
                raise SourceError(
                    "CUDA device was requested for Voice transcription, but ctranslate2 CUDA support is not available. "
                    "Use '--device cpu' or set VOICE_DEVICE=cpu."
                ) from e
            except SourceError:
                raise
            except Exception as e:
                raise SourceError(
                    f"CUDA device check failed: {e}. Use '--device cpu' or set VOICE_DEVICE=cpu."
                ) from e
            self.device = "cuda"
            return "cuda"

        self.device = "cpu"
        return "cpu"

    def _get_transcriber(self) -> Any:
        """Resolve and initialize local transcription engine."""
        # Resolve concrete execution device (auto -> cuda/cpu, cuda -> validated cuda, cpu -> cpu)
        self._resolve_device()

        if self._transcriber is not None:
            return self._transcriber

        if self._whisper_model is not None:
            return self._whisper_model

        try:
            from faster_whisper import WhisperModel
        except ImportError as e:
            raise SourceError(
                "faster-whisper is not installed. Install it with: pip install faster-whisper"
            ) from e

        try:
            logger.info(
                "Initializing local faster-whisper model '%s' on %s (%s)...",
                self.model_name,
                self.device,
                self.compute_type,
            )
            self._whisper_model = WhisperModel(
                self.model_name,
                device=self.device,
                compute_type=self.compute_type,
            )
            return self._whisper_model
        except Exception as e:
            err_str = str(e).lower()
            if "out of memory" in err_str or "cuda error: out of memory" in err_str:
                raise SourceError(
                    f"Out of GPU memory loading model '{self.model_name}'. Try a smaller model or run on CPU."
                ) from e
            if "cuda" in err_str and "driver" in err_str:
                raise SourceError(
                    f"CUDA initialization failed for model '{self.model_name}': {e}. Use '--device cpu'."
                ) from e
            raise SourceError(f"Failed to load speech-to-text model '{self.model_name}': {e}") from e

    def _transcribe_audio(self, audio_path: Path) -> TranscriptionResult:
        """Transcribe an audio file using the configured transcriber."""
        transcriber = self._get_transcriber()

        kwargs: Dict[str, Any] = {
            "beam_size": self.beam_size,
            "vad_filter": self.vad,
        }
        if self.language:
            kwargs["language"] = self.language

        try:
            result = transcriber.transcribe(str(audio_path), **kwargs)
        except MemoryError as e:
            raise SourceError(
                f"Out of memory during voice transcription for '{audio_path.name}'. "
                "Try a smaller model (e.g. tiny or base) or run on CPU with --compute-type int8."
            ) from e
        except Exception as e:
            err_str = str(e).lower()
            if "cuda" in err_str and "out of memory" in err_str:
                raise SourceError(
                    f"CUDA out of memory during transcription of '{audio_path.name}'. "
                    "Try a smaller model or switch to CPU."
                ) from e
            if "decoder" in err_str or "unsupported" in err_str or "codec" in err_str:
                raise SourceError(
                    f"Decoder error reading audio '{audio_path.name}': {e}. Verify file format."
                ) from e
            raise SourceError(f"Transcription failed for '{audio_path.name}': {e}") from e

        # Handle tuple of (segments, info) or custom result object
        raw_segments: Iterable[Any] = []
        raw_info: Any = None

        if isinstance(result, tuple) and len(result) >= 2:
            raw_segments, raw_info = result[0], result[1]
        elif hasattr(result, "segments") and hasattr(result, "info"):
            raw_segments = getattr(result, "segments")
            raw_info = getattr(result, "info")
        else:
            raw_segments = result

        # Parse segments
        segments: List[TranscriptSegment] = []
        full_text_parts: List[str] = []

        try:
            for s in raw_segments:
                start_val = float(getattr(s, "start", 0.0) if hasattr(s, "start") else s.get("start", 0.0))
                end_val = float(getattr(s, "end", 0.0) if hasattr(s, "end") else s.get("end", 0.0))
                text_val = str(getattr(s, "text", "") if hasattr(s, "text") else s.get("text", "")).strip()
                if text_val:
                    segments.append(TranscriptSegment(start=start_val, end=end_val, text=text_val))
                    full_text_parts.append(text_val)
        except Exception as e:
            raise SourceError(f"Error parsing transcript segments for '{audio_path.name}': {e}") from e

        # Parse info
        detected_lang = self.language or "en"
        lang_prob = 1.0
        duration = 0.0

        if raw_info is not None:
            if not self.language:
                if hasattr(raw_info, "language") and raw_info.language:
                    detected_lang = str(raw_info.language)
                elif isinstance(raw_info, dict) and raw_info.get("language"):
                    detected_lang = str(raw_info["language"])

            if hasattr(raw_info, "language_probability") and raw_info.language_probability is not None:
                lang_prob = float(raw_info.language_probability)
            elif isinstance(raw_info, dict) and raw_info.get("language_probability") is not None:
                lang_prob = float(raw_info["language_probability"])

            if hasattr(raw_info, "duration") and raw_info.duration is not None:
                duration = float(raw_info.duration)
            elif isinstance(raw_info, dict) and raw_info.get("duration") is not None:
                duration = float(raw_info["duration"])

        if duration == 0.0 and segments:
            duration = segments[-1].end

        # Check maximum duration if configured
        if self.max_duration is not None and duration > self.max_duration:
            raise SourceError(
                f"Audio file '{audio_path.name}' duration ({duration:.1f}s) exceeds maximum allowed duration "
                f"({self.max_duration:.1f}s). Increase --max-duration to process longer audio files."
            )

        full_text = " ".join(full_text_parts)
        return TranscriptionResult(
            text=full_text,
            segments=segments,
            language=detected_lang,
            language_probability=round(lang_prob, 4),
            duration=round(duration, 2),
        )

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch and transcribe items from one or more local audio files.

        Supported parameters:
        - file / file_path: Single audio file path
        - files: Comma-separated or list of audio file paths
        - directory / dir: Path to directory containing audio files
        - path: File or directory path
        - model: Override model name/size
        - language: Override target language code
        - device: Override device (cpu/cuda)
        - compute_type: Override compute type
        - beam_size: Override beam size
        - vad: Override VAD flag
        - max_duration: Override maximum allowed duration
        """
        # Apply runtime overrides
        if kwargs.get("model"):
            self.model_name = str(kwargs["model"])
        if kwargs.get("language"):
            self.language = str(kwargs["language"])
        if kwargs.get("device"):
            self.raw_device = str(kwargs["device"]).lower()
            self.device = self._resolve_device()
        if kwargs.get("compute_type"):
            self.compute_type = str(kwargs["compute_type"])
        if kwargs.get("beam_size"):
            self.beam_size = max(1, int(kwargs["beam_size"]))
        if "vad" in kwargs:
            self.vad = bool(kwargs["vad"])
        if "max_duration" in kwargs:
            val = kwargs["max_duration"]
            self.max_duration = float(val) if val is not None else None

        # 1. Resolve target candidate paths
        candidate_paths: List[Path] = []
        is_directory_mode = False

        dir_arg = kwargs.get("directory") or kwargs.get("dir")
        file_arg = kwargs.get("file") or kwargs.get("file_path")
        files_arg = kwargs.get("files")
        path_arg = kwargs.get("path")

        if dir_arg:
            dir_path = Path(dir_arg).resolve()
            if not dir_path.exists():
                raise SourceError(f"Audio directory does not exist: {dir_path}")
            if not dir_path.is_dir():
                raise SourceError(f"Audio directory path is not a directory: {dir_path}")
            is_directory_mode = True
            # Non-recursive scan of supported audio files
            candidate_paths = [
                p for p in sorted(dir_path.iterdir())
                if p.is_file() and p.suffix.lower() in SUPPORTED_AUDIO_EXTENSIONS
            ]
            if not candidate_paths:
                raise SourceError(f"No supported audio files found in directory: {dir_path}")

        elif files_arg:
            if isinstance(files_arg, str):
                raw_list = [p.strip() for p in files_arg.split(",") if p.strip()]
            else:
                raw_list = [str(p).strip() for p in files_arg if str(p).strip()]
            if not raw_list:
                raise SourceError("No audio files specified in --files.")
            candidate_paths = [Path(p).resolve() for p in raw_list]

        elif file_arg:
            candidate_paths = [Path(file_arg).resolve()]

        elif path_arg:
            target = Path(path_arg).resolve()
            if not target.exists():
                raise SourceError(f"Audio path does not exist: {target}")
            if target.is_dir():
                is_directory_mode = True
                candidate_paths = [
                    p for p in sorted(target.iterdir())
                    if p.is_file() and p.suffix.lower() in SUPPORTED_AUDIO_EXTENSIONS
                ]
                if not candidate_paths:
                    raise SourceError(f"No supported audio files found in directory: {target}")
            else:
                candidate_paths = [target]
        else:
            raise SourceError(
                "Voice source requires an audio file or directory. Specify --file <recording.mp3> or --directory <./recordings>."
            )

        items: List[SourceItem] = []

        # 2. Process each audio file
        for audio_path in candidate_paths:
            try:
                valid_path = validate_audio_file(audio_path)
            except SourceError as ve:
                if is_directory_mode:
                    logger.warning("Skipping invalid audio file in directory: %s", ve)
                    continue
                raise

            try:
                item = self._process_single_audio_file(valid_path)
                items.append(item)
            except SourceError:
                if is_directory_mode:
                    logger.warning("Skipping failed audio file %s in directory", valid_path.name, exc_info=True)
                    continue
                raise
            except Exception as e:
                if is_directory_mode:
                    logger.warning("Failed to process audio file %s: %s", valid_path.name, e)
                    continue
                raise SourceError(f"Failed to process audio file '{valid_path.name}': {e}") from e

        return items

    def _process_single_audio_file(self, audio_path: Path) -> SourceItem:
        """Extract content, transcribe speech, and build SourceItem for a single audio file."""
        # 1. Probe audio container metadata
        probed_meta = probe_audio_metadata(audio_path)

        # 2. Preflight duration check: reject before hashing and transcription if known duration exceeds limit
        known_duration = probed_meta.get("duration_seconds")
        if self.max_duration is not None and known_duration is not None and known_duration > self.max_duration:
            raise SourceError(
                f"Audio file '{audio_path.name}' duration ({known_duration:.1f}s) exceeds maximum allowed duration "
                f"({self.max_duration:.1f}s). Increase --max-duration to process longer audio files."
            )

        # 3. Deterministic content hash & stable source ID
        content_hash = compute_audio_sha256(audio_path)
        source_id = f"voice:sha256:{content_hash}"

        # 4. Transcribe audio (includes post-transcription duration check fallback if duration was not probed)
        result = self._transcribe_audio(audio_path)

        # 5. Determine note title: prefer embedded title tag, then filename stem
        raw_title = probed_meta.get("title_tag") or audio_path.stem
        title = sanitize_voice_title(raw_title)

        # 6. Format body markdown
        transcript_body = render_voice_transcript(result.segments)

        # 7. Extract modification / ingestion timestamp
        try:
            mtime = audio_path.stat().st_mtime
            date_iso = datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
        except Exception:
            date_iso = datetime.now(timezone.utc).isoformat()

        duration_sec = result.duration or probed_meta.get("duration_seconds") or 0.0

        extra_meta: Dict[str, Any] = {
            "source_file": audio_path.name,
            "duration_seconds": round(duration_sec, 2),
            "language": result.language,
            "model": self.model_name,
            "device": self.device,
            "compute_type": self.compute_type,
            "file_size_bytes": probed_meta.get("file_size_bytes", audio_path.stat().st_size),
        }
        if result.language_probability is not None:
            extra_meta["language_probability"] = result.language_probability
        if probed_meta.get("codec"):
            extra_meta["codec"] = probed_meta["codec"]
        if probed_meta.get("sample_rate"):
            extra_meta["sample_rate"] = probed_meta["sample_rate"]
        if probed_meta.get("channels"):
            extra_meta["channels"] = probed_meta["channels"]

        item = SourceItem(
            source_id=source_id,
            title=title,
            source_type="voice",
            content=transcript_body,
            date=date_iso,
            source_url=None,
            author=None,
            tags=["ingested", "voice", "audio"],
            language=result.language,
            summary=result.text[:200].strip() if result.text else None,
            extra_metadata=extra_meta,
            content_hash=content_hash,
        )

        return item

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a Voice SourceItem into frontmatter metadata and Markdown body targeted at Ingested/Voice/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Voice"
        return note


# Register Voice connector in SourceRegistry
SourceRegistry.register("voice", VoiceSource)
