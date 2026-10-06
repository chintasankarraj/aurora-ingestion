"""Comprehensive unit and integration tests for the Readwise source connector."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest
import requests

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.readwise_source import (
    ReadwiseSource,
    derive_readwise_stable_id,
    extract_readwise_tags,
    format_highlight_blockquote,
    map_readwise_error,
    render_highlight_block,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers & Mocks
# ---------------------------------------------------------------------------

class MockResponse:
    """Mock requests Response object for testing."""

    def __init__(
        self,
        json_data: Any,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self) -> Any:
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data

    @property
    def text(self) -> str:
        return json.dumps(self._json_data)


class MockSession:
    """Mock requests Session object capturing calls and returning queued responses."""

    def __init__(
        self,
        responses: Optional[List[MockResponse]] = None,
        side_effect: Optional[Exception] = None,
    ) -> None:
        self.responses: List[MockResponse] = list(responses or [])
        self.side_effect = side_effect
        self.calls: List[Dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"url": url, "kwargs": kwargs})
        if self.side_effect:
            raise self.side_effect
        if not self.responses:
            return MockResponse({"results": [], "nextPageCursor": None})
        return self.responses.pop(0)


def sample_readwise_item(
    user_book_id: int = 12345,
    title: str = "The Pragmatic Programmer",
    author: str = "Andy Hunt, Dave Thomas",
    category: str = "books",
    source: str = "kindle",
    source_url: str = "https://pragprog.com/titles/tpp20/",
    highlights: Optional[List[Dict[str, Any]]] = None,
    tags: Optional[List[Any]] = None,
    summary: Optional[str] = None,
    document_note: Optional[str] = None,
    last_highlight_at: str = "2026-03-15T10:00:00Z",
) -> Dict[str, Any]:
    """Generate a realistic Readwise API item object."""
    if highlights is None:
        highlights = [
            {
                "id": 101,
                "text": "Care about your craft.",
                "note": "A fundamental rule for any developer.",
                "location": 42,
                "location_type": "page",
                "highlighted_at": "2026-03-15T09:30:00Z",
                "color": "yellow",
            },
            {
                "id": 102,
                "text": "Provide options, don't make lame excuses.\nAlways take responsibility.",
                "note": None,
                "location": 58,
                "location_type": "page",
                "highlighted_at": "2026-03-15T09:45:00Z",
                "color": "blue",
            },
        ]

    data: Dict[str, Any] = {
        "user_book_id": user_book_id,
        "title": title,
        "author": author,
        "category": category,
        "source": source,
        "source_url": source_url,
        "readwise_url": f"https://readwise.io/book_review/{user_book_id}",
        "last_highlight_at": last_highlight_at,
        "updated": last_highlight_at,
        "highlights": highlights,
    }
    if tags is not None:
        data["tags"] = tags
    if summary is not None:
        data["summary"] = summary
    if document_note is not None:
        data["document_note"] = document_note
    return data


# ---------------------------------------------------------------------------
# 1. Registration
# ---------------------------------------------------------------------------

def test_readwise_source_registration():
    """Test ReadwiseSource registers as 'readwise' in SourceRegistry."""
    sources = SourceRegistry.list_sources()
    assert "readwise" in sources
    assert sources["readwise"] == "ReadwiseSource"

    source = ReadwiseSource(token="fake_token")
    assert source.source_type == "readwise"
    assert source.display_name == "Readwise"


# ---------------------------------------------------------------------------
# 2. Authentication & Configuration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_token_raises_source_error(monkeypatch):
    """Test missing token raises clear actionable SourceError."""
    monkeypatch.delenv("READWISE_TOKEN", raising=False)
    source = ReadwiseSource(token=None)
    with pytest.raises(SourceError, match="Readwise API token is missing"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_token_passed_via_environment_variable(monkeypatch):
    """Test token is read from READWISE_TOKEN environment variable."""
    monkeypatch.setenv("READWISE_TOKEN", "test_env_token_secret_123")
    mock_session = MockSession([MockResponse({"results": []})])
    source = ReadwiseSource(session=mock_session)

    await source.fetch_items()

    assert len(mock_session.calls) == 1
    auth_header = mock_session.calls[0]["kwargs"]["headers"]["Authorization"]
    assert auth_header == "Token test_env_token_secret_123"


@pytest.mark.asyncio
async def test_token_passed_via_kwargs_or_constructor():
    """Test token passed directly via constructor or kwargs overrides environment."""
    mock_session = MockSession([MockResponse({"results": []})])
    source = ReadwiseSource(token="constructor_token", session=mock_session)

    await source.fetch_items()

    auth_header = mock_session.calls[0]["kwargs"]["headers"]["Authorization"]
    assert auth_header == "Token constructor_token"


def test_token_never_written_to_exceptions_or_logs():
    """Test that auth error messages never reveal the secret token."""
    secret_token = "secret_readwise_token_9999"
    resp = requests.Response()
    resp.status_code = 401
    exc = requests.exceptions.HTTPError("401 Client Error", response=resp)

    err = map_readwise_error(exc)
    err_str = str(err)
    assert secret_token not in err_str
    assert "Invalid or expired API token" in err_str


# ---------------------------------------------------------------------------
# 3. API Retrieval & Pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_api_retrieval():
    """Test successful API retrieval converts documents and highlights."""
    raw_item = sample_readwise_item(user_book_id=101, title="Refactoring")
    mock_session = MockSession([MockResponse({"results": [raw_item], "nextPageCursor": None})])

    source = ReadwiseSource(token="valid_token", session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    item = items[0]
    assert item.source_id == "readwise:101"
    assert item.title == "Refactoring"
    assert item.author == "Andy Hunt, Dave Thomas"
    assert "readwise" in item.tags
    assert "Care about your craft." in item.content


@pytest.mark.asyncio
async def test_api_pagination_with_page_cursor():
    """Test pagination across multiple pages using nextPageCursor without dropping items."""
    item1 = sample_readwise_item(user_book_id=1, title="Book 1")
    item2 = sample_readwise_item(user_book_id=2, title="Book 2")
    item3 = sample_readwise_item(user_book_id=3, title="Book 3")

    page1 = MockResponse({"results": [item1], "nextPageCursor": "cursor_page_2"})
    page2 = MockResponse({"results": [item2], "nextPageCursor": "cursor_page_3"})
    page3 = MockResponse({"results": [item3], "nextPageCursor": None})

    mock_session = MockSession([page1, page2, page3])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 3
    assert [i.title for i in items] == ["Book 1", "Book 2", "Book 3"]
    assert len(mock_session.calls) == 3
    assert mock_session.calls[1]["kwargs"]["params"]["pageCursor"] == "cursor_page_2"
    assert mock_session.calls[2]["kwargs"]["params"]["pageCursor"] == "cursor_page_3"


@pytest.mark.asyncio
async def test_api_pagination_with_next_url():
    """Test pagination using next full URL fallback."""
    item1 = sample_readwise_item(user_book_id=11, title="Article 1")
    item2 = sample_readwise_item(user_book_id=12, title="Article 2")

    page1 = MockResponse({"results": [item1], "next": "https://readwise.io/api/v2/export/?page=2"})
    page2 = MockResponse({"results": [item2], "next": None})

    mock_session = MockSession([page1, page2])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 2
    assert [i.title for i in items] == ["Article 1", "Article 2"]


@pytest.mark.asyncio
async def test_pagination_stops_on_repeated_page_cursor(caplog):
    """Test that pagination loop terminates safely when API returns a repeated nextPageCursor."""
    item1 = sample_readwise_item(user_book_id=1, title="Page 1 Book")
    item2 = sample_readwise_item(user_book_id=2, title="Page 2 Book")

    page1 = MockResponse({"results": [item1], "nextPageCursor": "stuck_cursor_123"})
    page2 = MockResponse({"results": [item2], "nextPageCursor": "stuck_cursor_123"})

    mock_session = MockSession([page1, page2])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 2
    assert len(mock_session.calls) == 2
    assert "duplicate nextPageCursor 'stuck_cursor_123'" in caplog.text


@pytest.mark.asyncio
async def test_pagination_stops_on_repeated_next_url(caplog):
    """Test that pagination loop terminates safely when API returns a repeated next URL."""
    item1 = sample_readwise_item(user_book_id=1, title="Page 1 Book")
    item2 = sample_readwise_item(user_book_id=2, title="Page 2 Book")

    repeat_url = "https://readwise.io/api/v2/export/?page=2"
    page1 = MockResponse({"results": [item1], "next": repeat_url})
    page2 = MockResponse({"results": [item2], "next": repeat_url})

    mock_session = MockSession([page1, page2])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 2
    assert len(mock_session.calls) == 2
    assert f"duplicate next URL '{repeat_url}'" in caplog.text


@pytest.mark.asyncio
async def test_pagination_stops_on_cyclic_next_url_pointing_to_first_page(caplog):
    """Test that pagination loop terminates when next URL cycles back to the initial base URL."""
    item1 = sample_readwise_item(user_book_id=1, title="Page 1 Book")
    item2 = sample_readwise_item(user_book_id=2, title="Page 2 Book")

    base_url = "https://readwise.io/api/v2/export/"
    page2_url = "https://readwise.io/api/v2/export/?page=2"
    page1 = MockResponse({"results": [item1], "next": page2_url})
    page2 = MockResponse({"results": [item2], "next": base_url})

    mock_session = MockSession([page1, page2])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 2
    assert len(mock_session.calls) == 2
    assert f"duplicate next URL '{base_url}'" in caplog.text


@pytest.mark.asyncio
async def test_api_empty_response():
    """Test empty results list returns empty list without error."""
    mock_session = MockSession([MockResponse({"results": [], "nextPageCursor": None})])
    source = ReadwiseSource(token="token", session=mock_session)
    items = await source.fetch_items()
    assert items == []


@pytest.mark.asyncio
async def test_api_empty_highlights_handled_gracefully():
    """Test document with empty highlights list produces valid note."""
    item = sample_readwise_item(user_book_id=55, title="Article No Highlights", highlights=[])
    mock_session = MockSession([MockResponse({"results": [item], "nextPageCursor": None})])

    source = ReadwiseSource(token="token", session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    assert "*No highlights recorded.*" in items[0].content


@pytest.mark.asyncio
async def test_api_error_401():
    """Test HTTP 401 raises SourceError."""
    resp = requests.Response()
    resp.status_code = 401
    mock_session = MockSession([MockResponse({"detail": "Invalid token"}, status_code=401)])

    source = ReadwiseSource(token="bad_token", session=mock_session)
    with pytest.raises(SourceError, match="Readwise authentication failed"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_429():
    """Test HTTP 429 raises SourceError indicating rate limit."""
    mock_session = MockSession([MockResponse({"detail": "Throttled"}, status_code=429)])

    source = ReadwiseSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="rate limit exceeded"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_500():
    """Test HTTP 500 raises SourceError."""
    mock_session = MockSession([MockResponse({"detail": "Internal error"}, status_code=500)])

    source = ReadwiseSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="Readwise API error"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_network_timeout():
    """Test network timeout maps to descriptive SourceError."""
    mock_session = MockSession(side_effect=requests.exceptions.Timeout("Read timed out"))
    source = ReadwiseSource(token="token", session=mock_session)

    with pytest.raises(SourceError, match="Readwise API request timed out"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_connection_error():
    """Test connection error maps to descriptive SourceError."""
    mock_session = MockSession(side_effect=requests.exceptions.ConnectionError("Failed to resolve"))
    source = ReadwiseSource(token="token", session=mock_session)

    with pytest.raises(SourceError, match="Network connection failed"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_malformed_item_isolation():
    """Test individual malformed item in batch is skipped without aborting healthy items."""
    good_item1 = sample_readwise_item(user_book_id=1, title="Good Book 1")
    bad_item = "not a dict"
    good_item2 = sample_readwise_item(user_book_id=2, title="Good Book 2")

    mock_session = MockSession([MockResponse({"results": [good_item1, bad_item, good_item2]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 2
    assert [i.title for i in items] == ["Good Book 1", "Good Book 2"]


@pytest.mark.asyncio
async def test_non_dict_api_payload_error():
    """Test non-dict JSON response raises SourceError."""
    mock_session = MockSession([MockResponse(["unexpected", "list"])])
    source = ReadwiseSource(token="token", session=mock_session)

    with pytest.raises(SourceError, match="non-object JSON response"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_duplicate_items_deduplicated():
    """Test duplicate API items sharing identical source ID are deduplicated within a run."""
    item1 = sample_readwise_item(user_book_id=100, title="Duplicate Book")
    item2 = sample_readwise_item(user_book_id=100, title="Duplicate Book")

    mock_session = MockSession([MockResponse({"results": [item1, item2]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 1


# ---------------------------------------------------------------------------
# 4. Markdown & Frontmatter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markdown_frontmatter_and_attribution():
    """Test YAML frontmatter and attribution block generation."""
    raw_item = sample_readwise_item(
        user_book_id=777,
        title="Domain-Driven Design",
        author="Eric Evans",
        category="books",
        source_url="https://domainlanguage.com/ddd/",
        tags=[{"name": "software-architecture"}, {"name": "#ddd"}],
    )
    mock_session = MockSession([MockResponse({"results": [raw_item]})])
    source = ReadwiseSource(token="token", session=mock_session)
    items = await source.fetch_items()

    note = await source.convert_to_markdown(items[0])
    fm = note.to_frontmatter_dict()

    assert fm["title"] == "Domain-Driven Design"
    assert fm["source"] == "readwise"
    assert "ingested" in fm["tags"]
    assert "readwise" in fm["tags"]
    assert "software-architecture" in fm["tags"]
    assert "ddd" in fm["tags"]  # # stripped
    assert "#ddd" not in fm["tags"]
    assert fm["author"] == "Eric Evans"
    assert fm["source_url"] == "https://domainlanguage.com/ddd/"
    assert fm["readwise_id"] == 777
    assert fm["category"] == "books"


def test_highlights_rendered_as_blockquotes():
    """Test highlights are rendered as Markdown blockquotes."""
    h = {
        "text": "First line of highlight.\nSecond line of highlight.",
        "location": 42,
        "location_type": "page",
        "note": "A note.",
    }
    rendered = render_highlight_block(h)
    assert "> First line of highlight.\n> Second line of highlight." in rendered
    assert "*Location: page 42*" in rendered
    assert "**My note:** A note." in rendered


def test_highlight_location_preserved():
    """Test location with page, offset, or numeric location."""
    h_page = {"text": "Quote 1", "location": 100, "location_type": "page"}
    h_loc = {"text": "Quote 2", "location": 1500, "location_type": "location"}
    h_plain = {"text": "Quote 3", "location": 200, "location_type": ""}

    assert "*Location: page 100*" in render_highlight_block(h_page)
    assert "*Location: 1500*" in render_highlight_block(h_loc)
    assert "*Location: 200*" in render_highlight_block(h_plain)


def test_personal_notes_preserved_inline():
    """Test personal note is formatted with **My note:** and never confuses highlight text."""
    h = {
        "text": "The original highlighted text.",
        "note": "My unique personal insight.",
    }
    rendered = render_highlight_block(h)
    assert "> The original highlighted text." in rendered
    assert "**My note:** My unique personal insight." in rendered


def test_no_empty_fake_notes_when_note_absent():
    """Test no note section is rendered when personal note is absent or empty."""
    h1 = {"text": "Highlight without note", "note": None}
    h2 = {"text": "Highlight with whitespace note", "note": "   "}

    rendered1 = render_highlight_block(h1)
    rendered2 = render_highlight_block(h2)

    assert "**My note:**" not in rendered1
    assert "**My note:**" not in rendered2


def test_summary_and_document_note_rendered():
    """Test document-level summary and document_note are rendered in body."""
    raw_item = sample_readwise_item(
        summary="A comprehensive book summary.",
        document_note="My high-level thoughts on this book.",
    )
    source = ReadwiseSource(token="token")
    item = source._convert_raw_to_source_item(raw_item)

    assert "## Summary\n\nA comprehensive book summary." in item.content
    assert "## Document Note\n\nMy high-level thoughts on this book." in item.content


# ---------------------------------------------------------------------------
# 5. Files & Vault Destination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_target_folder_is_ingested_web(tmp_path):
    """Test Readwise notes are written under Ingested/Web/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_rw.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    raw_item = sample_readwise_item(user_book_id=888, title="Clean Code")
    mock_session = MockSession([MockResponse({"results": [raw_item]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    action, rel_path = await pipeline.process_item(source, items[0])

    assert action == IngestionAction.NEW
    assert rel_path.startswith("Ingested/Web/")
    assert rel_path.endswith(".md")
    assert (vault_dir / rel_path).exists()


@pytest.mark.asyncio
async def test_long_title_truncation(tmp_path):
    """Test that note titles over 80 characters are safely truncated in filenames."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_rw_long.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    super_long_title = "A" * 120
    raw_item = sample_readwise_item(user_book_id=999, title=super_long_title)
    mock_session = MockSession([MockResponse({"results": [raw_item]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])

    filename = Path(rel_path).name
    # <YYYY-MM-DD>_readwise_<80_chars>.md
    title_part = filename.split("_readwise_")[1].replace(".md", "")
    assert len(title_part) <= 80


@pytest.mark.asyncio
async def test_filename_collision_handling(tmp_path):
    """Test multiple distinct Readwise documents with identical title get unique filenames (_2.md)."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_rw_col.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    item1 = sample_readwise_item(user_book_id=1, title="Notes on Architecture")
    item2 = sample_readwise_item(user_book_id=2, title="Notes on Architecture")

    mock_session = MockSession([MockResponse({"results": [item1, item2]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    _, rel1 = await pipeline.process_item(source, items[0])
    _, rel2 = await pipeline.process_item(source, items[1])

    assert rel1 != rel2
    assert "_2.md" in rel2 or "_2.md" in rel1


# ---------------------------------------------------------------------------
# 6. Deduplication & In-Place Updates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deduplication_lifecycle(tmp_path):
    """Test NEW -> UNCHANGED -> CHANGED lifecycle with in-place overwriting."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_rw_life.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    # 1. First run: NEW
    raw_v1 = sample_readwise_item(
        user_book_id=500,
        title="Modern Software Engineering",
        highlights=[{"id": 1, "text": "Draft highlight.", "note": None}],
    )
    mock_session1 = MockSession([MockResponse({"results": [raw_v1]})])
    source1 = ReadwiseSource(token="token", session=mock_session1)
    items1 = await source1.fetch_items()

    action1, rel_path1 = await pipeline.process_item(source1, items1[0])
    assert action1 == IngestionAction.NEW
    note_file = vault_dir / rel_path1
    assert "Draft highlight." in note_file.read_text(encoding="utf-8")

    # 2. Second run without change: UNCHANGED
    mock_session2 = MockSession([MockResponse({"results": [raw_v1]})])
    source2 = ReadwiseSource(token="token", session=mock_session2)
    items2 = await source2.fetch_items()

    action2, rel_path2 = await pipeline.process_item(source2, items2[0])
    assert action2 == IngestionAction.UNCHANGED

    # 3. Third run with updated highlights: CHANGED (in-place update)
    raw_v2 = sample_readwise_item(
        user_book_id=500,
        title="Modern Software Engineering",
        highlights=[
            {"id": 1, "text": "Draft highlight.", "note": None},
            {"id": 2, "text": "Newly added second highlight.", "note": "Crucial concept."},
        ],
    )
    mock_session3 = MockSession([MockResponse({"results": [raw_v2]})])
    source3 = ReadwiseSource(token="token", session=mock_session3)
    items3 = await source3.fetch_items()

    action3, rel_path3 = await pipeline.process_item(source3, items3[0])
    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1  # Same path overwritten

    updated_text = note_file.read_text(encoding="utf-8")
    assert "Newly added second highlight." in updated_text
    assert "**My note:** Crucial concept." in updated_text

    # Verify no duplicate file was created
    all_notes = list((vault_dir / "Ingested" / "Web").glob("*.md"))
    assert len(all_notes) == 1


def test_stable_source_id_format():
    """Test stable source ID format readwise:<user_book_id>."""
    data_with_book_id = {"user_book_id": 123456}
    assert derive_readwise_stable_id(data_with_book_id) == "readwise:123456"

    data_with_id = {"id": 987}
    assert derive_readwise_stable_id(data_with_id) == "readwise:987"

    # Fallback to unique_url hash
    data_with_url = {"unique_url": "https://example.com/unique-article"}
    id_url = derive_readwise_stable_id(data_with_url)
    assert id_url.startswith("readwise:")


def test_record_without_id_or_url_raises_source_error():
    """Test that a record lacking both an immutable ID and a stable URL raises SourceError.

    Verifies that mutable title is never used to derive source identity.
    """
    record = {"title": "Mutable Book Title", "readable_title": "Mutable Book Title"}
    with pytest.raises(SourceError, match="missing both an immutable ID and a stable URL identifier"):
        derive_readwise_stable_id(record)


@pytest.mark.asyncio
async def test_batch_isolation_skips_record_lacking_id_and_url():
    """Test that records without immutable ID or stable URL are skipped by batch isolation without failing healthy items."""
    good_item = sample_readwise_item(user_book_id=1, title="Valid Book")
    malformed_item = {
        "title": "Title Without Any ID Or URL",
        "highlights": [{"text": "Some highlight"}],
    }
    good_item2 = sample_readwise_item(user_book_id=2, title="Another Valid Book")

    mock_session = MockSession([MockResponse({"results": [good_item, malformed_item, good_item2]})])
    source = ReadwiseSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 2
    assert [i.title for i in items] == ["Valid Book", "Another Valid Book"]


def test_stable_source_id_supports_book_id_and_all_url_fields():
    """Test stable source ID resolution for book_id, source_url, readwise_url."""
    assert derive_readwise_stable_id({"book_id": 999}) == "readwise:999"
    assert derive_readwise_stable_id({"user_book_id": 111, "book_id": 999}) == "readwise:111"

    url_source = derive_readwise_stable_id({"source_url": "https://example.com/source"})
    assert url_source.startswith("readwise:")

    url_readwise = derive_readwise_stable_id({"readwise_url": "https://readwise.io/book_review/222"})
    assert url_readwise.startswith("readwise:")


# ---------------------------------------------------------------------------
# 7. Edge Cases
# ---------------------------------------------------------------------------

def test_missing_title_fallback():
    """Test missing or empty title falls back to Untitled Readwise Document."""
    source = ReadwiseSource(token="token")
    item1 = source._convert_raw_to_source_item({"user_book_id": 1, "title": ""})
    item2 = source._convert_raw_to_source_item({"user_book_id": 2})

    assert item1.title == "Untitled Readwise Document"
    assert item2.title == "Untitled Readwise Document"


def test_missing_optional_fields():
    """Test missing author, source_url, summary are handled without error."""
    source = ReadwiseSource(token="token")
    item = source._convert_raw_to_source_item({"user_book_id": 3, "title": "Minimal"})

    assert item.author is None
    assert item.source_url is None
    assert item.summary is None


def test_special_unicode_characters():
    """Test Unicode emojis and non-ASCII characters in titles and highlights."""
    raw_item = sample_readwise_item(
        user_book_id=900,
        title="Thinking, Fast and Slow 🧠 & 科学",
        highlights=[{"id": 1, "text": "Heuristics and Biases 🔍 思考", "note": "Note with emoji 💡"}],
    )
    source = ReadwiseSource(token="token")
    item = source._convert_raw_to_source_item(raw_item)

    assert "🧠" in item.title
    assert "科学" in item.title
    assert "🔍" in item.content
    assert "💡" in item.content


@pytest.mark.asyncio
async def test_filtering_by_book_id_and_updated_after():
    """Test passing book_id and updated_after forwards query params to API call."""
    mock_session = MockSession([MockResponse({"results": []})])
    source = ReadwiseSource(token="token", session=mock_session)

    await source.fetch_items(book_id="12345", updated_after="2026-01-01T00:00:00Z")

    assert len(mock_session.calls) == 1
    params = mock_session.calls[0]["kwargs"]["params"]
    assert params["ids"] == "12345"
    assert params["updatedAfter"] == "2026-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# 8. CLI Integration
# ---------------------------------------------------------------------------

def test_cli_parser_registration():
    """Test CLI parser handles ingest-readwise and generic ingest --source readwise."""
    parser = create_parser()

    # Shortcut: ingest-readwise
    args1 = parser.parse_args([
        "ingest-readwise",
        "--token", "rw_tok",
        "--book-id", "99",
        "--updated-after", "2026-01-01",
    ])
    assert args1.command == "ingest-readwise"
    assert args1.token == "rw_tok"
    assert args1.book_id == "99"
    assert args1.updated_after == "2026-01-01"

    # Generic: ingest --source readwise
    args2 = parser.parse_args([
        "ingest",
        "--source", "readwise",
        "--token", "rw_tok",
        "--book-id", "99",
    ])
    assert args2.command == "ingest"
    assert args2.source == "readwise"
    assert args2.token == "rw_tok"
    assert args2.book_id == "99"


@pytest.mark.asyncio
async def test_cli_execution_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of CLI ingest-readwise command with mocked API."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_rw.sqlite"

    raw_item = sample_readwise_item(user_book_id=8888, title="CLI Readwise Note")
    mock_resp = MockResponse({"results": [raw_item], "nextPageCursor": None})

    monkeypatch.setattr(
        "requests.Session.get",
        lambda self, url, **kwargs: mock_resp,
    )

    parser = create_parser()
    args = parser.parse_args([
        "ingest-readwise",
        "--token", "fake_cli_token",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)
    assert exit_code == 0

    notes = list((vault_dir / "Ingested" / "Web").glob("*.md"))
    assert len(notes) == 1
    content = notes[0].read_text(encoding="utf-8")
    assert "CLI Readwise Note" in content
    assert "source: readwise" in content


@pytest.mark.asyncio
async def test_generic_cli_execution_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of generic CLI ingest --source readwise command with mocked API."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_gen_rw.sqlite"

    raw_item = sample_readwise_item(user_book_id=7777, title="Generic CLI Readwise Note")
    mock_resp = MockResponse({"results": [raw_item], "nextPageCursor": None})

    monkeypatch.setattr(
        "requests.Session.get",
        lambda self, url, **kwargs: mock_resp,
    )

    parser = create_parser()
    args = parser.parse_args([
        "ingest",
        "--source", "readwise",
        "--token", "fake_cli_token",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)
    assert exit_code == 0

    notes = list((vault_dir / "Ingested" / "Web").glob("*.md"))
    assert len(notes) == 1
    content = notes[0].read_text(encoding="utf-8")
    assert "Generic CLI Readwise Note" in content
    assert "source: readwise" in content

