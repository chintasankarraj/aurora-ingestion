"""Comprehensive unit and integration tests for YouTube video source connector."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from youtube_transcript_api import (
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApiException,
)

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.youtube_source import (
    YouTubeSource,
    extract_video_id,
    format_duration,
    format_timestamp,
    format_transcript_content,
)
from tracker import DeduplicationTracker, IngestionAction


class MockResponse:
    """Mock requests response."""

    def __init__(
        self,
        content: bytes = b"",
        text: str = "",
        status_code: int = 200,
        headers: dict | None = None,
    ) -> None:
        self.content = content or text.encode("utf-8")
        self.text = text
        self.status_code = status_code
        self.headers = headers or {"content-type": "image/jpeg"}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}")


class MockSnippet:
    """Mock transcript snippet."""

    def __init__(self, text: str, start: float, duration: float) -> None:
        self.text = text
        self.start = start
        self.duration = duration


class MockTranscript:
    """Mock transcript item."""

    def __init__(
        self,
        snippets: list[MockSnippet],
        language_code: str = "en",
        is_generated: bool = False,
    ) -> None:
        self.snippets = snippets
        self.language_code = language_code
        self.is_generated = is_generated

    def fetch(self) -> list[MockSnippet]:
        return self.snippets


class MockTranscriptList:
    """Mock transcript list."""

    def __init__(self, transcript: MockTranscript | None = None) -> None:
        self.transcript = transcript

    def find_manually_created_transcript(self, languages):
        if self.transcript and not self.transcript.is_generated:
            return self.transcript
        raise NoTranscriptFound("dQw4w9WgXcQ", languages)

    def find_generated_transcript(self, languages):
        if self.transcript and self.transcript.is_generated:
            return self.transcript
        raise NoTranscriptFound("dQw4w9WgXcQ", languages)

    def find_transcript(self, languages):
        if self.transcript:
            return self.transcript
        raise NoTranscriptFound("dQw4w9WgXcQ", languages)

    def __iter__(self):
        if self.transcript:
            yield self.transcript


class MockTranscriptApi:
    """Mock YouTubeTranscriptApi instance."""

    def __init__(self, transcript_list: MockTranscriptList | None = None) -> None:
        self.transcript_list = transcript_list

    def list(self, video_id: str) -> MockTranscriptList:
        if self.transcript_list is None:
            raise TranscriptsDisabled(video_id)
        return self.transcript_list


class MockYdlClient:
    """Mock yt-dlp client."""

    def __init__(self, info: dict | Exception) -> None:
        self.info = info

    def extract_info(self, url: str, download: bool = False) -> dict:
        if isinstance(self.info, Exception):
            raise self.info
        return dict(self.info)


SAMPLE_SNIPPETS = [
    MockSnippet("Welcome to this tutorial on modern AI.", 0.0, 3.5),
    MockSnippet("Today we will explore vector databases and embeddings.", 3.8, 4.2),
    MockSnippet("First, let us examine how embeddings represent semantic meaning.", 32.0, 5.0),
    MockSnippet("Next, approximate nearest neighbor algorithms enable fast search.", 37.5, 4.5),
    MockSnippet("In conclusion, vector databases are fundamental to modern LLMs.", 70.0, 4.0),
]

SAMPLE_METADATA = {
    "title": "Introduction to Vector Databases",
    "uploader": "Tech Academy",
    "upload_date": "20260920",
    "description": "A comprehensive deep dive into vector embeddings and search.",
    "duration": 213,  # 3:33
    "thumbnail": "https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg",
}


def test_youtube_source_registration():
    src_cls = SourceRegistry.get("youtube")
    assert src_cls is YouTubeSource
    sources_dict = SourceRegistry.list_sources()
    assert "youtube" in sources_dict


@pytest.mark.asyncio
async def test_cli_list_sources_shows_youtube(capsys):
    parser = create_parser()
    args = parser.parse_args(["list-sources"])
    ret = await async_main(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "youtube (YouTubeSource)" in captured.out


def test_extract_video_id_valid_formats():
    expected_id = "dQw4w9WgXcQ"

    # Standard watch?v=
    assert extract_video_id("https://www.youtube.com/watch?v=dQw4w9WgXcQ") == expected_id
    # Extra query parameters
    assert (
        extract_video_id(
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=42s&feature=youtu.be"
        )
        == expected_id
    )
    # youtu.be shortlink
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ") == expected_id
    assert extract_video_id("https://youtu.be/dQw4w9WgXcQ?t=15s") == expected_id
    # Shorts
    assert extract_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ") == expected_id
    assert (
        extract_video_id("https://www.youtube.com/shorts/dQw4w9WgXcQ?feature=share")
        == expected_id
    )
    # Mobile
    assert extract_video_id("https://m.youtube.com/watch?v=dQw4w9WgXcQ") == expected_id
    # Embed
    assert extract_video_id("https://www.youtube.com/embed/dQw4w9WgXcQ") == expected_id
    # Protocol omitted
    assert extract_video_id("youtube.com/watch?v=dQw4w9WgXcQ") == expected_id


def test_extract_video_id_invalid_rejections():
    # Playlists rejected explicitly
    with pytest.raises(SourceError, match="Playlists are not supported"):
        extract_video_id("https://www.youtube.com/playlist?list=PL123456789")

    # Missing video ID in watch URL
    with pytest.raises(SourceError, match="Could not extract video ID"):
        extract_video_id("https://www.youtube.com/watch")

    # Unsupported host
    with pytest.raises(SourceError, match="Unsupported host"):
        extract_video_id("https://vimeo.com/12345678901")

    # Invalid ID length
    with pytest.raises(SourceError, match="Invalid YouTube video ID"):
        extract_video_id("https://www.youtube.com/watch?v=short")

    # Invalid characters in ID
    with pytest.raises(SourceError, match="Invalid YouTube video ID"):
        extract_video_id("https://www.youtube.com/watch?v=dQw4w9Wg$cQ")

    # Empty inputs
    with pytest.raises(SourceError, match="cannot be empty"):
        extract_video_id("   ")

    with pytest.raises(SourceError, match="must be a non-empty string"):
        extract_video_id(None)


def test_duration_and_timestamp_formatting():
    assert format_duration(None) == ""
    assert format_duration(0) == "00:00"
    assert format_duration(65) == "01:05"
    assert format_duration(754) == "12:34"
    assert format_duration(3665) == "1:01:05"

    assert format_timestamp(0) == "00:00"
    assert format_timestamp(42.4) == "00:42"
    assert format_timestamp(65) == "01:05"
    assert format_timestamp(3665) == "01:01:05"


def test_format_transcript_content():
    # Empty snippets
    assert format_transcript_content([]) == ""

    # Grouped snippets with timestamps
    content = format_transcript_content(SAMPLE_SNIPPETS, chunk_interval_seconds=30.0)
    assert "**[00:00]**" in content
    assert "Welcome to this tutorial on modern AI." in content
    assert "Today we will explore vector databases and embeddings." in content

    assert "**[00:32]**" in content
    assert "First, let us examine how embeddings represent semantic meaning." in content

    assert "**[01:10]**" in content
    assert "In conclusion, vector databases are fundamental to modern LLMs." in content


@pytest.mark.asyncio
async def test_youtube_metadata_and_transcript_extraction_mocked():
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    thumb_bytes = b"\xff\xd8\xff\xe0\x00\x10JFIFfake_thumb_bytes"

    mock_ydl = MockYdlClient(SAMPLE_METADATA)
    mock_t = MockTranscript(SAMPLE_SNIPPETS, language_code="en", is_generated=False)
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    mock_session = MagicMock()
    mock_session.get.return_value = MockResponse(
        content=thumb_bytes, headers={"content-type": "image/jpeg"}
    )

    source = YouTubeSource(
        ydl_client=mock_ydl,
        transcript_api=mock_tapi,
        session=mock_session,
    )

    items = await source.fetch_items(url=video_url)
    assert len(items) == 1
    item = items[0]

    # Verify source attributes
    assert item.source_type == "youtube"
    assert item.source_id == "youtube:dQw4w9WgXcQ"
    assert item.source_url == "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    assert item.title == "Introduction to Vector Databases"
    assert item.author == "Tech Academy"
    assert item.date == "2026-09-20"
    assert item.language == "en"
    assert item.extra_metadata["video_id"] == "dQw4w9WgXcQ"
    assert item.extra_metadata["channel"] == "Tech Academy"
    assert item.extra_metadata["duration"] == "03:33"

    # Verify content formatting
    assert "**Channel:** Tech Academy" in item.content
    assert "**Video ID:** dQw4w9WgXcQ" in item.content
    assert "**Duration:** 03:33" in item.content
    assert "![[dQw4w9WgXcQ_thumbnail.jpg]]" in item.content
    assert "## Description" in item.content
    assert "A comprehensive deep dive into vector embeddings and search." in item.content
    assert "## Transcript" in item.content
    assert "**[00:00]**" in item.content

    # Verify thumbnail attachment
    assert len(item.attachments) == 1
    assert item.attachments[0].filename == "dQw4w9WgXcQ_thumbnail.jpg"
    assert item.attachments[0].content == thumb_bytes

    # Convert to MarkdownNote
    note = await source.convert_to_markdown(item)
    assert note.source == "youtube"
    assert "ingested" in note.tags
    assert "youtube" in note.tags
    assert "dQw4w9WgXcQ_thumbnail.jpg" in note.attachments
    assert note.folder == "Ingested/YouTube"


@pytest.mark.asyncio
async def test_thumbnail_download_failure_resilience():
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    mock_ydl = MockYdlClient(SAMPLE_METADATA)
    mock_t = MockTranscript(SAMPLE_SNIPPETS)
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    mock_session = MagicMock()
    # 404 response on thumbnail download
    mock_session.get.return_value = MockResponse(status_code=404)

    source = YouTubeSource(
        ydl_client=mock_ydl,
        transcript_api=mock_tapi,
        session=mock_session,
    )

    items = await source.fetch_items(url=video_url)
    assert len(items) == 1
    item = items[0]

    # Note creation succeeds, attachments list is empty
    assert len(item.attachments) == 0
    assert "## Transcript" in item.content
    assert "![[" not in item.content


@pytest.mark.asyncio
async def test_missing_transcript_raises_source_error():
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    mock_ydl = MockYdlClient(SAMPLE_METADATA)
    # Transcript API raises TranscriptsDisabled
    mock_tapi = MockTranscriptApi(transcript_list=None)

    source = YouTubeSource(
        ydl_client=mock_ydl,
        transcript_api=mock_tapi,
    )

    with pytest.raises(SourceError, match="No transcript available for YouTube video"):
        await source.fetch_items(url=video_url)


@pytest.mark.asyncio
async def test_private_or_deleted_video_raises_source_error():
    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    mock_ydl = MockYdlClient(Exception("Video unavailable: This video is private."))
    mock_t = MockTranscript(SAMPLE_SNIPPETS)
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    source = YouTubeSource(
        ydl_client=mock_ydl,
        transcript_api=mock_tapi,
    )

    with pytest.raises(SourceError, match="Failed to extract metadata"):
        await source.fetch_items(url=video_url)


@pytest.mark.asyncio
async def test_youtube_pipeline_end_to_end_and_attachments(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    thumb_bytes = b"\xff\xd8\xff\xe0\x00\x10JFIFfake_thumb_bytes"

    mock_ydl = MockYdlClient(SAMPLE_METADATA)
    mock_t = MockTranscript(SAMPLE_SNIPPETS, language_code="en")
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    mock_session = MagicMock()
    mock_session.get.return_value = MockResponse(
        content=thumb_bytes, headers={"content-type": "image/jpeg"}
    )

    source = YouTubeSource(
        ydl_client=mock_ydl,
        transcript_api=mock_tapi,
        session=mock_session,
    )

    items = await source.fetch_items(url=video_url)
    assert len(items) == 1

    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW
    assert rel_path is not None
    assert "Ingested/YouTube" in rel_path or "Ingested\\YouTube" in rel_path

    # Verify Markdown note on disk
    note_path = vault_dir / rel_path
    assert note_path.exists()
    content = note_path.read_text(encoding="utf-8")

    # Verify frontmatter
    assert "source: youtube" in content
    assert "video_id: dQw4w9WgXcQ" in content
    assert "channel: Tech Academy" in content
    assert "duration: 03:33" in content
    assert "- dQw4w9WgXcQ_thumbnail.jpg" in content

    # Verify body structure
    assert "# Introduction to Vector Databases" in content
    assert "> **Source**: [Youtube](https://www.youtube.com/watch?v=dQw4w9WgXcQ)" in content
    assert "**Channel:** Tech Academy" in content
    assert "**Video ID:** dQw4w9WgXcQ" in content
    assert "![[dQw4w9WgXcQ_thumbnail.jpg]]" in content
    assert "## Description" in content
    assert "## Transcript" in content
    assert "**[00:00]**" in content

    # Verify thumbnail attachment saved on disk
    thumb_path = vault_dir / "Attachments" / "Ingested" / "dQw4w9WgXcQ_thumbnail.jpg"
    assert thumb_path.exists()
    assert thumb_path.read_bytes() == thumb_bytes

    # Verify tracker record
    record = tracker.get_record("youtube", "youtube:dQw4w9WgXcQ")
    assert record is not None
    assert record.vault_path == rel_path

    tracker.close()


@pytest.mark.asyncio
async def test_youtube_pipeline_deduplication_and_update(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    video_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    # 1. First run -> Action NEW
    mock_ydl_1 = MockYdlClient(SAMPLE_METADATA)
    mock_t_1 = MockTranscript(SAMPLE_SNIPPETS)
    mock_tapi_1 = MockTranscriptApi(MockTranscriptList(mock_t_1))
    mock_session = MagicMock()
    mock_session.get.return_value = MockResponse(content=b"thumb_bytes")

    source_1 = YouTubeSource(
        ydl_client=mock_ydl_1,
        transcript_api=mock_tapi_1,
        session=mock_session,
    )
    items_1 = await source_1.fetch_items(url=video_url)
    action_1, rel_path_1 = await pipeline.process_item(source_1, items_1[0])
    assert action_1 == IngestionAction.NEW

    note_path = vault_dir / rel_path_1
    assert "Welcome to this tutorial on modern AI." in note_path.read_text(encoding="utf-8")

    # 2. Re-ingesting same content -> UNCHANGED (skipped)
    items_same = await source_1.fetch_items(url=video_url)
    action_2, rel_path_2 = await pipeline.process_item(source_1, items_same[0])
    assert action_2 == IngestionAction.UNCHANGED
    assert rel_path_2 == rel_path_1

    # 3. Modified content (updated description/transcript) -> CHANGED (overwrites note in place)
    updated_metadata = dict(SAMPLE_METADATA)
    updated_metadata["description"] = "Updated description with new release notes."
    updated_snippets = SAMPLE_SNIPPETS + [
        MockSnippet("Bonus section: advanced quantization techniques.", 90.0, 5.0)
    ]

    mock_ydl_2 = MockYdlClient(updated_metadata)
    mock_t_2 = MockTranscript(updated_snippets)
    mock_tapi_2 = MockTranscriptApi(MockTranscriptList(mock_t_2))

    source_2 = YouTubeSource(
        ydl_client=mock_ydl_2,
        transcript_api=mock_tapi_2,
        session=mock_session,
    )
    items_updated = await source_2.fetch_items(url=video_url)
    action_3, rel_path_3 = await pipeline.process_item(source_2, items_updated[0])

    assert action_3 == IngestionAction.CHANGED
    assert rel_path_3 == rel_path_1  # Overwrites the SAME file

    updated_content = note_path.read_text(encoding="utf-8")
    assert "Updated description with new release notes." in updated_content
    assert "Bonus section: advanced quantization techniques." in updated_content

    tracker.close()


@pytest.mark.asyncio
async def test_cli_ingest_youtube_command(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker.sqlite"

    target_url = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    thumb_bytes = b"fake_thumb"

    mock_ydl = MockYdlClient(SAMPLE_METADATA)
    mock_t = MockTranscript(SAMPLE_SNIPPETS)
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    mock_session = MagicMock()
    mock_session.get.return_value = MockResponse(content=thumb_bytes)

    with (
        patch("sources.youtube_source.yt_dlp.YoutubeDL") as mock_ydl_cls,
        patch("sources.youtube_source.YouTubeTranscriptApi") as mock_tapi_cls,
        patch("requests.Session.get") as mock_http_get,
    ):
        mock_ydl_instance = MagicMock()
        mock_ydl_instance.extract_info.return_value = SAMPLE_METADATA
        mock_ydl_instance.__enter__.return_value = mock_ydl_instance
        mock_ydl_instance.__exit__.return_value = None
        mock_ydl_cls.return_value = mock_ydl_instance

        mock_tapi_cls.return_value = mock_tapi
        mock_http_get.return_value = MockResponse(content=thumb_bytes)

        parser = create_parser()
        args = parser.parse_args([
            "--vault-path", str(vault_dir),
            "--tracker-db", str(tracker_path),
            "ingest-youtube",
            target_url,
        ])

        ret = await async_main(args)
        assert ret == 0

        captured = capsys.readouterr()
        assert "Ingestion completed for 'youtube'" in captured.out

        # Verify Markdown note in Ingested/YouTube/
        yt_notes = list((vault_dir / "Ingested" / "YouTube").glob("*.md"))
        assert len(yt_notes) == 1
        content = yt_notes[0].read_text(encoding="utf-8")
        assert "Introduction to Vector Databases" in content
        assert "Tech Academy" in content


@pytest.mark.asyncio
async def test_cli_generic_ingest_youtube_command(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault_gen"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker_gen.sqlite"

    target_url = "https://youtu.be/dQw4w9WgXcQ"
    thumb_bytes = b"fake_thumb"

    mock_t = MockTranscript(SAMPLE_SNIPPETS)
    mock_tapi = MockTranscriptApi(MockTranscriptList(mock_t))

    with (
        patch("sources.youtube_source.yt_dlp.YoutubeDL") as mock_ydl_cls,
        patch("sources.youtube_source.YouTubeTranscriptApi") as mock_tapi_cls,
        patch("requests.Session.get") as mock_http_get,
    ):
        mock_ydl_instance = MagicMock()
        mock_ydl_instance.extract_info.return_value = SAMPLE_METADATA
        mock_ydl_instance.__enter__.return_value = mock_ydl_instance
        mock_ydl_instance.__exit__.return_value = None
        mock_ydl_cls.return_value = mock_ydl_instance

        mock_tapi_cls.return_value = mock_tapi
        mock_http_get.return_value = MockResponse(content=thumb_bytes)

        parser = create_parser()
        args = parser.parse_args([
            "--vault-path", str(vault_dir),
            "--tracker-db", str(tracker_path),
            "ingest",
            "--source", "youtube",
            "--url", target_url,
        ])

        ret = await async_main(args)
        assert ret == 0

        captured = capsys.readouterr()
        assert "Ingestion completed for 'youtube'" in captured.out

        yt_notes = list((vault_dir / "Ingested" / "YouTube").glob("*.md"))
        assert len(yt_notes) == 1
