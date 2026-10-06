"""Unit and integration tests for the Voice / Audio ingestion connector."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest
import yaml

from config import IngestionConfig
from converter import (
    build_attribution_block,
    extract_date_prefix,
    render_markdown_document,
)
from exceptions import SourceError
from main import IngestionPipeline, create_parser
from models import MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.voice_source import (
    DEFAULT_BEAM_SIZE,
    DEFAULT_COMPUTE_TYPE,
    DEFAULT_DEVICE,
    DEFAULT_MAX_DURATION,
    DEFAULT_MODEL,
    MAX_FILE_SIZE_BYTES,
    SUPPORTED_AUDIO_EXTENSIONS,
    TranscriptSegment,
    TranscriptionResult,
    VoiceSource,
    compute_audio_sha256,
    format_timestamp,
    format_voice_duration,
    is_cuda_available,
    probe_audio_metadata,
    render_voice_transcript,
    sanitize_voice_title,
    validate_audio_file,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Mocks and Helpers
# ---------------------------------------------------------------------------

class MockSegment:
    """Mock faster-whisper segment."""

    def __init__(self, start: float, end: float, text: str) -> None:
        self.start = start
        self.end = end
        self.text = text


class MockTranscriptionInfo:
    """Mock faster-whisper transcription info."""

    def __init__(
        self,
        language: str = "en",
        language_probability: float = 0.985,
        duration: float = 12.5,
    ) -> None:
        self.language = language
        self.language_probability = language_probability
        self.duration = duration


class MockTranscriber:
    """Injected mock transcriber for fast, isolated tests without model downloads."""

    def __init__(
        self,
        segments: Optional[List[MockSegment]] = None,
        info: Optional[MockTranscriptionInfo] = None,
        side_effect: Optional[Exception] = None,
    ) -> None:
        self.segments = segments if segments is not None else [
            MockSegment(0.0, 4.5, "Hello and welcome to the team meeting."),
            MockSegment(4.8, 12.5, "Today we will review the release roadmap for Q3."),
        ]
        self.info = info or MockTranscriptionInfo()
        self.side_effect = side_effect
        self.calls: List[Dict[str, Any]] = []

    def transcribe(
        self, audio_path: str, **kwargs: Any
    ) -> Tuple[List[MockSegment], MockTranscriptionInfo]:
        self.calls.append({"audio_path": audio_path, "kwargs": kwargs})
        if self.side_effect:
            raise self.side_effect
        return self.segments, self.info


@pytest.fixture
def temp_audio_file(tmp_path: Path) -> Path:
    """Create a temporary dummy audio file with some content."""
    audio = tmp_path / "meeting_notes.mp3"
    audio.write_bytes(b"DUMMY_MP3_AUDIO_STREAM_DATA_FOR_TESTING_123456789")
    return audio


# ---------------------------------------------------------------------------
# 1. Input Validation Tests
# ---------------------------------------------------------------------------

class TestVoiceValidation:
    """Tests for audio file and directory validation."""

    def test_validate_audio_file_success(self, temp_audio_file: Path) -> None:
        resolved = validate_audio_file(temp_audio_file)
        assert resolved.exists()
        assert resolved == temp_audio_file.resolve()

    def test_validate_audio_file_missing(self, tmp_path: Path) -> None:
        missing = tmp_path / "nonexistent.mp3"
        with pytest.raises(SourceError) as exc_info:
            validate_audio_file(missing)
        assert "Audio file does not exist" in str(exc_info.value)

    def test_validate_audio_file_is_directory(self, tmp_path: Path) -> None:
        test_dir = tmp_path / "audio_dir"
        test_dir.mkdir()
        with pytest.raises(SourceError) as exc_info:
            validate_audio_file(test_dir)
        assert "Target path is a directory" in str(exc_info.value)

    def test_validate_audio_file_unsupported_extension(self, tmp_path: Path) -> None:
        text_file = tmp_path / "notes.txt"
        text_file.write_text("Hello world", encoding="utf-8")
        with pytest.raises(SourceError) as exc_info:
            validate_audio_file(text_file)
        assert "Unsupported audio format" in str(exc_info.value)

    def test_validate_audio_file_empty_zero_bytes(self, tmp_path: Path) -> None:
        empty_file = tmp_path / "empty.wav"
        empty_file.write_bytes(b"")
        with pytest.raises(SourceError) as exc_info:
            validate_audio_file(empty_file)
        assert "Audio file is empty" in str(exc_info.value)

    def test_validate_audio_file_exceeds_max_size(self, tmp_path: Path) -> None:
        audio = tmp_path / "huge.mp3"
        audio.write_bytes(b"x" * 200)
        with pytest.raises(SourceError) as exc_info:
            validate_audio_file(audio, max_file_size=100)
        assert "Audio file exceeds maximum size limit" in str(exc_info.value)

    def test_validate_audio_file_permission_denied(self, temp_audio_file: Path) -> None:
        with patch.object(Path, "open", side_effect=PermissionError("Access denied")):
            with pytest.raises(SourceError) as exc_info:
                validate_audio_file(temp_audio_file)
            assert "Permission denied" in str(exc_info.value)

    def test_supported_audio_extensions_completeness(self) -> None:
        expected = {".wav", ".mp3", ".m4a", ".flac", ".ogg", ".opus", ".aac", ".webm"}
        assert SUPPORTED_AUDIO_EXTENSIONS == expected

    @pytest.mark.parametrize("ext", [".wav", ".mp3", ".m4a", ".flac", ".ogg", ".opus", ".aac", ".webm"])
    def test_all_supported_extensions_pass_validation(self, tmp_path: Path, ext: str) -> None:
        audio = tmp_path / f"sample{ext}"
        audio.write_bytes(b"VALID_AUDIO_BYTES")
        resolved = validate_audio_file(audio)
        assert resolved.exists()


# ---------------------------------------------------------------------------
# 2. Hashing and Deduplication Identity Tests
# ---------------------------------------------------------------------------

class TestVoiceHashing:
    """Tests for deterministic chunked SHA-256 calculation."""

    def test_compute_audio_sha256_stability(self, temp_audio_file: Path) -> None:
        hash1 = compute_audio_sha256(temp_audio_file)
        hash2 = compute_audio_sha256(temp_audio_file)
        assert hash1 == hash2
        assert len(hash1) == 64

    def test_compute_audio_sha256_chunked(self, tmp_path: Path) -> None:
        audio = tmp_path / "chunked.mp3"
        content = b"ABCDEFGHIJ" * 50  # 500 bytes
        audio.write_bytes(content)
        # Hash with 16 byte chunks
        chunked_hash = compute_audio_sha256(audio, chunk_size=16)
        import hashlib
        direct_hash = hashlib.sha256(content).hexdigest()
        assert chunked_hash == direct_hash

    def test_renamed_file_identical_hash(self, tmp_path: Path) -> None:
        content = b"IDENTICAL_AUDIO_DATA_FOR_DEDUPLICATION_TEST"
        f1 = tmp_path / "original.wav"
        f2 = tmp_path / "renamed_copy.wav"
        f1.write_bytes(content)
        f2.write_bytes(content)

        assert compute_audio_sha256(f1) == compute_audio_sha256(f2)

    def test_different_content_different_hash(self, tmp_path: Path) -> None:
        f1 = tmp_path / "rec1.m4a"
        f2 = tmp_path / "rec2.m4a"
        f1.write_bytes(b"AUDIO_TRACK_ONE")
        f2.write_bytes(b"AUDIO_TRACK_TWO")

        assert compute_audio_sha256(f1) != compute_audio_sha256(f2)

    def test_compute_audio_sha256_permission_error(self, temp_audio_file: Path) -> None:
        with patch.object(Path, "open", side_effect=PermissionError("Denied")):
            with pytest.raises(SourceError) as exc_info:
                compute_audio_sha256(temp_audio_file)
            assert "Permission denied" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. Timestamp and Duration Formatting Tests
# ---------------------------------------------------------------------------

class TestTimestampFormatting:
    """Tests for timestamp and duration string conversion."""

    def test_format_timestamp_seconds(self) -> None:
        assert format_timestamp(0) == "00:00:00"
        assert format_timestamp(5.2) == "00:00:05"
        assert format_timestamp(45) == "00:00:45"

    def test_format_timestamp_minutes(self) -> None:
        assert format_timestamp(65) == "00:01:05"
        assert format_timestamp(125.9) == "00:02:05"
        assert format_timestamp(600) == "00:10:00"

    def test_format_timestamp_hours(self) -> None:
        assert format_timestamp(3600) == "01:00:00"
        assert format_timestamp(4530) == "01:15:30"
        assert format_timestamp(36000) == "10:00:00"

    def test_format_timestamp_with_millis(self) -> None:
        assert format_timestamp(5.250, include_millis=True) == "00:00:05.250"
        assert format_timestamp(65.123, include_millis=True) == "00:01:05.123"

    def test_format_timestamp_negative_clamped(self) -> None:
        assert format_timestamp(-10) == "00:00:00"

    def test_format_voice_duration(self) -> None:
        assert format_voice_duration(125.0) == "00:02:05"
        assert format_voice_duration(3665.0) == "01:01:05"

    def test_render_voice_transcript_multi_segments(self) -> None:
        segments = [
            TranscriptSegment(start=0.0, end=5.2, text="First utterance."),
            TranscriptSegment(start=5.5, end=11.8, text="Second utterance."),
        ]
        body = render_voice_transcript(segments)
        assert "## Transcript" in body
        assert "[00:00:00 - 00:00:05]" in body
        assert "First utterance." in body
        assert "[00:00:05 - 00:00:11]" in body
        assert "Second utterance." in body

    def test_render_voice_transcript_empty(self) -> None:
        assert "*No speech detected in audio file.*" in render_voice_transcript([])

    def test_render_voice_transcript_whitespace_only(self) -> None:
        segments = [TranscriptSegment(start=0.0, end=2.0, text="   ")]
        assert "*No speech detected in audio file.*" in render_voice_transcript(segments)


# ---------------------------------------------------------------------------
# 4. Title Sanitization Tests
# ---------------------------------------------------------------------------

class TestTitleSanitization:
    """Tests for note title cleaning and sanitization."""

    def test_sanitize_normal_title(self) -> None:
        assert sanitize_voice_title("Team Standup Meeting") == "Team Standup Meeting"

    def test_sanitize_forbidden_characters(self) -> None:
        raw = 'Project: Review/Design? "Q3" <Important> [V1] #Roadmap | Test'
        cleaned = sanitize_voice_title(raw)
        for char in r'\/:*?"<>|#^[]':
            assert char not in cleaned
        assert cleaned == "Project ReviewDesign Q3 Important V1 Roadmap Test"

    def test_sanitize_excessive_whitespace(self) -> None:
        assert sanitize_voice_title("Sprint   Planning   Session") == "Sprint Planning Session"

    def test_sanitize_length_cap(self) -> None:
        long_title = "A" * 120
        cleaned = sanitize_voice_title(long_title)
        assert len(cleaned) == 80

    def test_sanitize_empty_fallback(self) -> None:
        assert sanitize_voice_title("") == "Audio Recording"
        assert sanitize_voice_title("   ") == "Audio Recording"
        assert sanitize_voice_title("###") == "Audio Recording"


# ---------------------------------------------------------------------------
# 5. Metadata Probing Tests
# ---------------------------------------------------------------------------

class TestMetadataProbing:
    """Tests for audio file metadata extraction and graceful degradation."""

    def test_probe_audio_metadata_basic(self, temp_audio_file: Path) -> None:
        meta = probe_audio_metadata(temp_audio_file)
        assert meta["filename"] == "meeting_notes.mp3"
        assert meta["extension"] == ".mp3"
        assert meta["file_size_bytes"] == temp_audio_file.stat().st_size

    def test_probe_audio_metadata_with_pyav_mock(self, temp_audio_file: Path) -> None:
        mock_stream = MagicMock()
        mock_stream.sample_rate = 44100
        mock_stream.channels = 2
        mock_stream.bit_rate = 128000
        mock_stream.codec_context.name = "mp3"

        mock_container = MagicMock()
        mock_container.duration = 120 * 1000000  # 120s with 1e6 time_base
        mock_container.streams.audio = [mock_stream]
        mock_container.metadata = {"title": "Design Discussion"}

        mock_av = MagicMock()
        mock_av.open.return_value.__enter__.return_value = mock_container
        mock_av.time_base = 1000000

        with patch.dict("sys.modules", {"av": mock_av}):
            meta = probe_audio_metadata(temp_audio_file)
            assert meta.get("title_tag") == "Design Discussion"
            assert meta.get("sample_rate") == 44100
            assert meta.get("channels") == 2
            assert meta.get("codec") == "mp3"
            assert meta.get("duration_seconds") == 120.0


# ---------------------------------------------------------------------------
# 6. Transcription Engine and Error Mapping Tests
# ---------------------------------------------------------------------------

class TestVoiceTranscription:
    """Tests for transcription parsing and error handling."""

    def test_transcribe_audio_successful(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)
        result = source._transcribe_audio(temp_audio_file)

        assert len(result.segments) == 2
        assert result.segments[0].text == "Hello and welcome to the team meeting."
        assert result.language == "en"
        assert result.duration == 12.5
        assert "Hello and welcome" in result.text
        assert "Today we will review" in result.text

    def test_transcribe_audio_empty_transcript(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber(segments=[])
        source = VoiceSource(transcriber=mock_transcriber)
        result = source._transcribe_audio(temp_audio_file)

        assert len(result.segments) == 0
        assert result.text == ""

    def test_transcribe_audio_language_override(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(language="es", transcriber=mock_transcriber)
        result = source._transcribe_audio(temp_audio_file)

        # Transcriber kwargs should include language="es"
        assert mock_transcriber.calls[0]["kwargs"]["language"] == "es"
        assert result.language == "es"

    def test_transcribe_audio_max_duration_exceeded(self, temp_audio_file: Path) -> None:
        mock_info = MockTranscriptionInfo(duration=3600.0)  # 1 hour
        mock_transcriber = MockTranscriber(info=mock_info)
        source = VoiceSource(max_duration=1800.0, transcriber=mock_transcriber)  # 30 min limit

        with pytest.raises(SourceError) as exc_info:
            source._transcribe_audio(temp_audio_file)
        assert "exceeds maximum allowed duration" in str(exc_info.value)

    def test_transcribe_audio_memory_error(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber(side_effect=MemoryError("Out of memory"))
        source = VoiceSource(transcriber=mock_transcriber)

        with pytest.raises(SourceError) as exc_info:
            source._transcribe_audio(temp_audio_file)
        assert "Out of memory during voice transcription" in str(exc_info.value)

    def test_transcribe_audio_cuda_oom(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber(side_effect=RuntimeError("CUDA error: out of memory"))
        source = VoiceSource(transcriber=mock_transcriber)

        with pytest.raises(SourceError) as exc_info:
            source._transcribe_audio(temp_audio_file)
        assert "CUDA out of memory" in str(exc_info.value)

    def test_transcribe_audio_decoder_error(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber(side_effect=RuntimeError("Decoder error: unsupported stream"))
        source = VoiceSource(transcriber=mock_transcriber)

        with pytest.raises(SourceError) as exc_info:
            source._transcribe_audio(temp_audio_file)
        assert "Decoder error reading audio" in str(exc_info.value)

    def test_transcribe_audio_generic_failure(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber(side_effect=ValueError("Corrupt frame header"))
        source = VoiceSource(transcriber=mock_transcriber)

        with pytest.raises(SourceError) as exc_info:
            source._transcribe_audio(temp_audio_file)
        assert "Transcription failed" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 7. Device and Model Resolution Tests
# ---------------------------------------------------------------------------

class TestModelAndDeviceResolution:
    """Tests for lazy model loading and device checks."""

    def test_get_transcriber_returns_injected(self) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)
        assert source._get_transcriber() is mock_transcriber

    def test_cuda_device_fails_when_unavailable(self) -> None:
        source = VoiceSource(device="cuda")
        mock_ct2 = MagicMock()
        mock_ct2.get_cuda_device_count.return_value = 0

        with patch.dict("sys.modules", {"ctranslate2": mock_ct2}):
            with pytest.raises(SourceError) as exc_info:
                source._get_transcriber()
            assert "CUDA device was requested for Voice transcription, but CUDA is not available" in str(exc_info.value)

    def test_device_auto_cuda_available(self) -> None:
        with patch("sources.voice_source.is_cuda_available", return_value=True):
            source = VoiceSource(device="auto")
            assert source._resolve_device() == "cuda"
            assert source.device == "cuda"

    def test_device_auto_cuda_unavailable_falls_back_to_cpu(self) -> None:
        with patch("sources.voice_source.is_cuda_available", return_value=False):
            source = VoiceSource(device="auto")
            assert source._resolve_device() == "cpu"
            assert source.device == "cpu"

    def test_device_explicit_cpu(self) -> None:
        source = VoiceSource(device="cpu")
        assert source._resolve_device() == "cpu"
        assert source.device == "cpu"

    def test_device_explicit_cuda_unavailable(self) -> None:
        mock_ct2 = MagicMock()
        mock_ct2.get_cuda_device_count.return_value = 0
        with patch.dict("sys.modules", {"ctranslate2": mock_ct2}):
            source = VoiceSource(device="cuda")
            with pytest.raises(SourceError) as exc_info:
                source._resolve_device()
            assert "CUDA device was requested for Voice transcription, but CUDA is not available" in str(exc_info.value)

    def test_missing_faster_whisper_module(self) -> None:
        source = VoiceSource(device="cpu")
        with patch.dict("sys.modules", {"faster_whisper": None}):
            with pytest.raises(SourceError) as exc_info:
                source._get_transcriber()
            assert "faster-whisper is not installed" in str(exc_info.value)

    def test_model_init_oom_maps_to_source_error(self) -> None:
        source = VoiceSource(device="cpu")
        mock_fw = MagicMock()
        mock_fw.WhisperModel.side_effect = RuntimeError("CUDA error: out of memory")

        with patch.dict("sys.modules", {"faster_whisper": mock_fw}):
            with pytest.raises(SourceError) as exc_info:
                source._get_transcriber()
            assert "Out of GPU memory loading model" in str(exc_info.value)

    def test_model_init_cuda_driver_maps_to_source_error(self) -> None:
        source = VoiceSource(device="cpu")
        mock_fw = MagicMock()
        mock_fw.WhisperModel.side_effect = RuntimeError("CUDA driver version is insufficient")

        with patch.dict("sys.modules", {"faster_whisper": mock_fw}):
            with pytest.raises(SourceError) as exc_info:
                source._get_transcriber()
            assert "CUDA initialization failed" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 8. Duration Preflight and Limit Enforcement Tests
# ---------------------------------------------------------------------------

class TestDurationPreflightAndLimits:
    """Tests verifying max-duration enforcement before and after transcription."""

    def test_duration_preflight_exceeds_limit_transcriber_not_called(
        self, temp_audio_file: Path
    ) -> None:
        """When container metadata duration exceeds max-duration, reject BEFORE transcribing."""
        mock_transcriber = MockTranscriber()
        source = VoiceSource(max_duration=1800.0, transcriber=mock_transcriber)
        mock_meta = {
            "filename": temp_audio_file.name,
            "duration_seconds": 7200.0,
            "file_size_bytes": 1000,
        }

        with patch("sources.voice_source.probe_audio_metadata", return_value=mock_meta):
            with pytest.raises(SourceError) as exc_info:
                source._process_single_audio_file(temp_audio_file)
            assert "duration (7200.0s) exceeds maximum allowed duration (1800.0s)" in str(exc_info.value)
            # Transcriber MUST NOT have been called
            assert len(mock_transcriber.calls) == 0

    def test_duration_preflight_exactly_at_limit_proceeds(
        self, temp_audio_file: Path
    ) -> None:
        """When container metadata duration equals max-duration, transcription proceeds."""
        mock_transcriber = MockTranscriber(info=MockTranscriptionInfo(duration=1800.0))
        source = VoiceSource(max_duration=1800.0, transcriber=mock_transcriber)
        mock_meta = {
            "filename": temp_audio_file.name,
            "duration_seconds": 1800.0,
            "file_size_bytes": 1000,
        }

        with patch("sources.voice_source.probe_audio_metadata", return_value=mock_meta):
            item = source._process_single_audio_file(temp_audio_file)
            assert len(mock_transcriber.calls) == 1
            assert item.extra_metadata["duration_seconds"] == 1800.0

    def test_duration_metadata_unavailable_proceeds_and_post_check_enforces(
        self, temp_audio_file: Path
    ) -> None:
        """When container metadata lacks duration, proceed to transcribe and enforce limit afterwards."""
        mock_transcriber = MockTranscriber(info=MockTranscriptionInfo(duration=3600.0))
        source = VoiceSource(max_duration=1800.0, transcriber=mock_transcriber)
        mock_meta = {
            "filename": temp_audio_file.name,
            "file_size_bytes": 1000,
        }  # No duration_seconds

        with patch("sources.voice_source.probe_audio_metadata", return_value=mock_meta):
            with pytest.raises(SourceError) as exc_info:
                source._process_single_audio_file(temp_audio_file)
            assert "duration (3600.0s) exceeds maximum allowed duration (1800.0s)" in str(exc_info.value)
            # Transcriber WAS called because metadata duration was unavailable
            assert len(mock_transcriber.calls) == 1

    def test_duration_metadata_unavailable_within_limit_succeeds(
        self, temp_audio_file: Path
    ) -> None:
        """When container metadata lacks duration and transcribed duration is within limit, succeed."""
        mock_transcriber = MockTranscriber(info=MockTranscriptionInfo(duration=120.0))
        source = VoiceSource(max_duration=1800.0, transcriber=mock_transcriber)
        mock_meta = {
            "filename": temp_audio_file.name,
            "file_size_bytes": 1000,
        }

        with patch("sources.voice_source.probe_audio_metadata", return_value=mock_meta):
            item = source._process_single_audio_file(temp_audio_file)
            assert len(mock_transcriber.calls) == 1
            assert item.extra_metadata["duration_seconds"] == 120.0


# ---------------------------------------------------------------------------
# 8. Fetch Items Tests (File, Multiple Files, Directory)
# ---------------------------------------------------------------------------

class TestVoiceFetchItems:
    """Tests for fetch_items across CLI parameters."""

    @pytest.mark.asyncio
    async def test_fetch_items_single_file(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(file=str(temp_audio_file))
        assert len(items) == 1
        item = items[0]

        assert item.source_type == "voice"
        assert item.source_id.startswith("voice:sha256:")
        assert item.title == "meeting_notes"
        assert "Hello and welcome" in item.content
        assert item.tags == ["ingested", "voice", "audio"]
        assert item.extra_metadata["source_file"] == "meeting_notes.mp3"
        assert item.extra_metadata["model"] == DEFAULT_MODEL
        assert item.extra_metadata["device"] == "cpu"

    @pytest.mark.asyncio
    async def test_fetch_items_single_file_via_path(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(path=str(temp_audio_file))
        assert len(items) == 1
        assert items[0].title == "meeting_notes"

    @pytest.mark.asyncio
    async def test_fetch_items_multiple_files_csv(self, tmp_path: Path) -> None:
        f1 = tmp_path / "rec1.wav"
        f2 = tmp_path / "rec2.wav"
        f1.write_bytes(b"REC1_BYTES")
        f2.write_bytes(b"REC2_BYTES")

        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(files=f"{f1},{f2}")
        assert len(items) == 2
        assert items[0].title == "rec1"
        assert items[1].title == "rec2"

    @pytest.mark.asyncio
    async def test_fetch_items_multiple_files_list(self, tmp_path: Path) -> None:
        f1 = tmp_path / "rec1.m4a"
        f2 = tmp_path / "rec2.m4a"
        f1.write_bytes(b"REC1_BYTES")
        f2.write_bytes(b"REC2_BYTES")

        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(files=[str(f1), str(f2)])
        assert len(items) == 2

    @pytest.mark.asyncio
    async def test_fetch_items_directory(self, tmp_path: Path) -> None:
        d = tmp_path / "recordings"
        d.mkdir()
        (d / "talk1.mp3").write_bytes(b"TALK1_BYTES")
        (d / "talk2.wav").write_bytes(b"TALK2_BYTES")
        (d / "ignored.txt").write_text("Ignore me", encoding="utf-8")

        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(directory=str(d))
        assert len(items) == 2
        titles = {it.title for it in items}
        assert titles == {"talk1", "talk2"}

    @pytest.mark.asyncio
    async def test_fetch_items_directory_fault_isolation(self, tmp_path: Path) -> None:
        d = tmp_path / "recordings"
        d.mkdir()
        (d / "good1.mp3").write_bytes(b"GOOD1_BYTES")
        (d / "corrupt.mp3").write_bytes(b"")  # 0 bytes -> invalid audio file
        (d / "good2.wav").write_bytes(b"GOOD2_BYTES")

        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        # In directory mode, corrupt/invalid files are logged and skipped
        items = await source.fetch_items(directory=str(d))
        assert len(items) == 2
        titles = {it.title for it in items}
        assert titles == {"good1", "good2"}

    @pytest.mark.asyncio
    async def test_fetch_items_directory_empty_raises(self, tmp_path: Path) -> None:
        d = tmp_path / "empty_dir"
        d.mkdir()

        source = VoiceSource(transcriber=MockTranscriber())
        with pytest.raises(SourceError) as exc_info:
            await source.fetch_items(directory=str(d))
        assert "No supported audio files found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_items_no_arguments_raises(self) -> None:
        source = VoiceSource(transcriber=MockTranscriber())
        with pytest.raises(SourceError) as exc_info:
            await source.fetch_items()
        assert "Voice source requires an audio file or directory" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_items_runtime_overrides(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)

        items = await source.fetch_items(
            file=str(temp_audio_file),
            model="large-v3",
            language="de",
            device="cpu",
            compute_type="int8",
            beam_size=3,
            vad=True,
            max_duration=3600.0,
        )
        assert len(items) == 1
        assert source.model_name == "large-v3"
        assert source.language == "de"
        assert source.compute_type == "int8"
        assert source.beam_size == 3
        assert source.vad is True
        assert source.max_duration == 3600.0


# ---------------------------------------------------------------------------
# 9. Markdown Conversion and Attribution Tests
# ---------------------------------------------------------------------------

class TestMarkdownConversion:
    """Tests for note generation and attribution rendering."""

    @pytest.mark.asyncio
    async def test_convert_to_markdown_folder(self, temp_audio_file: Path) -> None:
        mock_transcriber = MockTranscriber()
        source = VoiceSource(transcriber=mock_transcriber)
        items = await source.fetch_items(file=str(temp_audio_file))

        note = await source.convert_to_markdown(items[0])
        assert isinstance(note, MarkdownNote)
        assert note.folder == "Ingested/Voice"
        assert note.title == "meeting_notes"
        assert note.tags == ["ingested", "voice", "audio"]

    def test_attribution_block_rendering(self) -> None:
        note = MarkdownNote(
            title="Standup Meeting",
            source="voice",
            date="2026-10-06T10:00:00Z",
            body="## Transcript\n\nSome transcript",
            folder="Ingested/Voice",
            language="en",
            extra_metadata={
                "source_file": "standup.mp3",
                "duration_seconds": 125.0,
                "model": "small",
            },
        )

        attr = build_attribution_block(note)
        assert "> **Source**: Local audio — standup.mp3" in attr
        assert "**Language**: en" in attr
        assert "**Duration**: 00:02:05" in attr
        assert "**Model**: small" in attr
        assert "**Date**: 2026-10-06" in attr

    def test_transcript_with_special_markdown_characters(self, tmp_path: Path) -> None:
        """Verify that markdown symbols in speech (e.g. hashtags, quotes, backticks) don't break note structure."""
        special_text = "Let's discuss #general and *italic* and `code_sample` and [bracket link]."
        mock_transcriber = MockTranscriber(
            segments=[MockSegment(0.0, 5.0, special_text)]
        )
        source = VoiceSource(transcriber=mock_transcriber)

        audio = tmp_path / "markdown_speech.mp3"
        audio.write_bytes(b"SPECIAL_MARKDOWN_SPEECH_DATA")

        item = source._process_single_audio_file(audio)
        note = source.default_item_to_note(item)
        note.folder = "Ingested/Voice"

        doc = render_markdown_document(note)
        assert "---" in doc
        # Frontmatter should be valid YAML
        frontmatter_str = doc.split("---")[1]
        parsed = yaml.safe_load(frontmatter_str)
        assert parsed["source"] == "voice"
        assert parsed["title"] == item.title
        assert special_text in doc


# ---------------------------------------------------------------------------
# 10. Pipeline Integration and Deduplication Tests
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    """Tests for full IngestionPipeline execution with Voice connector."""

    @pytest.mark.asyncio
    async def test_pipeline_run_voice(self, tmp_path: Path, temp_audio_file: Path) -> None:
        vault = tmp_path / "vault"
        vault.mkdir()
        db_path = tmp_path / "tracker.db"

        config = IngestionConfig(vault_path=vault, tracker_db_path=db_path)
        tracker = DeduplicationTracker(str(db_path))
        pipeline = IngestionPipeline(config=config, tracker=tracker)

        mock_transcriber = MockTranscriber()
        voice_source = VoiceSource(transcriber=mock_transcriber)

        items = await voice_source.fetch_items(file=str(temp_audio_file))
        assert len(items) == 1

        action, rel_path = await pipeline.process_item(voice_source, items[0])
        assert action == IngestionAction.NEW
        assert rel_path is not None
        assert "Voice" in rel_path

        # Check file in vault
        vault_file = vault / rel_path
        assert vault_file.exists()
        note_content = vault_file.read_text(encoding="utf-8")
        assert "meeting_notes" in note_content
        assert "Hello and welcome to the team meeting." in note_content
        tracker.close()

    @pytest.mark.asyncio
    async def test_deduplication_via_content_hash(
        self, tmp_path: Path, temp_audio_file: Path
    ) -> None:
        vault = tmp_path / "vault"
        vault.mkdir()
        db_path = tmp_path / "tracker.db"

        config = IngestionConfig(vault_path=vault, tracker_db_path=db_path)
        tracker = DeduplicationTracker(str(db_path))
        pipeline = IngestionPipeline(config=config, tracker=tracker)

        mock_transcriber = MockTranscriber()
        voice_source = VoiceSource(transcriber=mock_transcriber)

        items1 = await voice_source.fetch_items(file=str(temp_audio_file))
        action1, rel_path1 = await pipeline.process_item(voice_source, items1[0])
        assert action1 == IngestionAction.NEW

        # Second run with same file
        items2 = await voice_source.fetch_items(file=str(temp_audio_file))
        action2, rel_path2 = await pipeline.process_item(voice_source, items2[0])
        assert action2 == IngestionAction.UNCHANGED
        assert rel_path2 == rel_path1
        tracker.close()

    @pytest.mark.asyncio
    async def test_renamed_file_deduplication(self, tmp_path: Path) -> None:
        vault = tmp_path / "vault"
        vault.mkdir()
        db_path = tmp_path / "tracker.db"

        config = IngestionConfig(vault_path=vault, tracker_db_path=db_path)
        tracker = DeduplicationTracker(str(db_path))
        pipeline = IngestionPipeline(config=config, tracker=tracker)

        mock_transcriber = MockTranscriber()
        voice_source = VoiceSource(transcriber=mock_transcriber)

        content = b"SAME_AUDIO_CONTENT_DIFFERENT_NAMES"
        f1 = tmp_path / "first_recording.mp3"
        f2 = tmp_path / "renamed_recording.mp3"
        f1.write_bytes(content)
        f2.write_bytes(content)

        items1 = await voice_source.fetch_items(file=str(f1))
        action1, rel_path1 = await pipeline.process_item(voice_source, items1[0])
        assert action1 == IngestionAction.NEW

        # Ingesting renamed file with identical content should skip as UNCHANGED
        items2 = await voice_source.fetch_items(file=str(f2))
        action2, rel_path2 = await pipeline.process_item(voice_source, items2[0])
        assert action2 == IngestionAction.UNCHANGED
        assert rel_path2 == rel_path1
        tracker.close()


# ---------------------------------------------------------------------------
# 11. CLI Parser and Registry Tests
# ---------------------------------------------------------------------------

class TestCLIParserAndRegistry:
    """Tests for CLI arguments and SourceRegistry registration."""

    def test_source_registry_has_voice(self) -> None:
        assert "voice" in SourceRegistry.list_sources()
        assert SourceRegistry.get("voice") is VoiceSource

    def test_ingest_voice_subcommand_parser(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-voice",
            "--file", "meeting.mp3",
            "--model", "medium",
            "--language", "fr",
            "--device", "cpu",
            "--compute-type", "int8",
            "--beam-size", "7",
            "--vad",
            "--max-duration", "3600",
        ])
        assert args.command == "ingest-voice"
        assert args.file == "meeting.mp3"
        assert args.model == "medium"
        assert args.language == "fr"
        assert args.device == "cpu"
        assert args.compute_type == "int8"
        assert args.beam_size == 7
        assert args.vad is True
        assert args.max_duration == 3600.0

    def test_ingest_voice_directory_parser(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-voice",
            "--directory", "./recordings",
        ])
        assert args.command == "ingest-voice"
        assert args.directory == "./recordings"

    def test_ingest_voice_files_list_parser(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-voice",
            "--files", "rec1.wav,rec2.mp3",
        ])
        assert args.command == "ingest-voice"
        assert args.files == "rec1.wav,rec2.mp3"

    def test_ingest_command_voice_arguments(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest",
            "--source", "voice",
            "--file", "call.ogg",
            "--model", "small",
            "--device", "cuda",
            "--vad",
        ])
        assert args.command == "ingest"
        assert args.source == "voice"
        assert args.file == "call.ogg"
        assert args.model == "small"
        assert args.device == "cuda"
        assert args.vad is True

    @pytest.mark.parametrize("dev", ["auto", "cpu", "cuda"])
    def test_cli_device_choices_accepted_on_voice(self, dev: str) -> None:
        parser = create_parser()
        args_sub = parser.parse_args(["ingest-voice", "--file", "rec.mp3", "--device", dev])
        assert args_sub.device == dev
        args_gen = parser.parse_args(["ingest", "--source", "voice", "--file", "rec.mp3", "--device", dev])
        assert args_gen.device == dev

