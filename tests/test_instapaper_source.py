"""Comprehensive unit and integration tests for the Instapaper source connector."""

from __future__ import annotations

import json
import re
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
from sources.instapaper_source import (
    InstapaperSource,
    build_instapaper_body,
    derive_instapaper_stable_id,
    extract_instapaper_tags,
    format_highlight_blockquote,
    is_html_content,
    map_instapaper_error,
    normalize_instapaper_content,
    parse_instapaper_timestamp,
    render_instapaper_highlight,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers & Mocks
# ---------------------------------------------------------------------------

class MockResponse:
    """Mock requests Response object for testing Instapaper API."""

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
            return MockResponse({"bookmarks": []})
        return self.responses.pop(0)


def sample_instapaper_bookmark(
    bookmark_id: int = 123456,
    title: str = "Attention Is All You Need",
    url: str = "https://arxiv.org/abs/1706.03762",
    description: str = "The dominant sequence transduction models are based on complex recurrent or convolutional neural networks.",
    time: int = 1773561600,
    starred: str = "1",
    progress: float = 0.5,
    folder: str = "unread",
    author: str = "Vaswani et al.",
    word_count: int = 4200,
    content: Optional[str] = None,
    html: Optional[str] = None,
    highlights: Optional[List[Dict[str, Any]]] = None,
    tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Generate a realistic Instapaper bookmark object."""
    data: Dict[str, Any] = {
        "bookmark_id": bookmark_id,
        "id": bookmark_id,
        "title": title,
        "url": url,
        "description": description,
        "time": time,
        "starred": starred,
        "progress": progress,
        "folder": folder,
        "author": author,
        "word_count": word_count,
        "hash": f"hash_{bookmark_id}",
    }
    if content is not None:
        data["content"] = content
    if html is not None:
        data["html"] = html
    if highlights is not None:
        data["highlights"] = highlights
    if tags is not None:
        data["tags"] = tags
    return data


# ---------------------------------------------------------------------------
# 1. Registration & Identification
# ---------------------------------------------------------------------------

def test_instapaper_source_registration():
    """Test InstapaperSource registers as 'instapaper' in SourceRegistry."""
    sources = SourceRegistry.list_sources()
    assert "instapaper" in sources
    assert sources["instapaper"] == "InstapaperSource"

    source = InstapaperSource(token="fake_token")
    assert source.source_type == "instapaper"
    assert source.display_name == "Instapaper"


# ---------------------------------------------------------------------------
# 2. Authentication & Configuration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_credentials_raises_source_error(monkeypatch):
    """Test missing token and username/password raises clear actionable SourceError."""
    monkeypatch.delenv("INSTAPAPER_TOKEN", raising=False)
    monkeypatch.delenv("INSTAPAPER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("INSTAPAPER_USERNAME", raising=False)
    monkeypatch.delenv("INSTAPAPER_PASSWORD", raising=False)

    source = InstapaperSource()
    with pytest.raises(SourceError, match="Instapaper credentials missing"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_token_passed_via_environment_variable(monkeypatch):
    """Test token is read from INSTAPAPER_TOKEN environment variable."""
    monkeypatch.setenv("INSTAPAPER_TOKEN", "env_secret_token_abc")
    mock_session = MockSession([MockResponse({"bookmarks": []})])
    source = InstapaperSource(session=mock_session)

    await source.fetch_items()

    assert len(mock_session.calls) == 1
    headers = mock_session.calls[0]["kwargs"]["headers"]
    assert headers["Authorization"] == "Bearer env_secret_token_abc"


@pytest.mark.asyncio
async def test_username_password_passed_via_environment_or_args(monkeypatch):
    """Test username and password passed via environment or constructor."""
    monkeypatch.delenv("INSTAPAPER_TOKEN", raising=False)
    monkeypatch.setenv("INSTAPAPER_USERNAME", "alice@example.com")
    monkeypatch.setenv("INSTAPAPER_PASSWORD", "super_secret_pw")

    mock_session = MockSession([MockResponse({"bookmarks": []})])
    source = InstapaperSource(session=mock_session)

    await source.fetch_items()

    assert len(mock_session.calls) == 1
    auth = mock_session.calls[0]["kwargs"].get("auth")
    assert auth == ("alice@example.com", "super_secret_pw")


def test_credentials_never_leaked_into_exceptions_or_logs():
    """Test that auth error messages and exceptions never reveal secret tokens or passwords."""
    secret_token = "secret_instapaper_token_123"
    secret_pw = "secret_password_xyz"

    resp = requests.Response()
    resp.status_code = 401
    exc = requests.exceptions.HTTPError(f"401 Error token={secret_token} pw={secret_pw}", response=resp)

    err = map_instapaper_error(exc, response=resp, secrets=[secret_token, secret_pw])
    err_str = str(err)
    assert secret_token not in err_str
    assert secret_pw not in err_str
    assert "Instapaper authentication failed" in err_str


# ---------------------------------------------------------------------------
# 3. API Retrieval, Formats & Pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_api_v2_retrieval():
    """Test successful API v2 JSON response with bookmarks object."""
    raw_bm = sample_instapaper_bookmark(bookmark_id=501, title="Deep Learning Book")
    mock_session = MockSession([
        MockResponse({"bookmarks": [raw_bm], "has_more": False})
    ])

    source = InstapaperSource(token="token", session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    item = items[0]
    assert item.source_id == "instapaper:501"
    assert item.title == "Deep Learning Book"
    assert item.author == "Vaswani et al."
    assert "instapaper" in item.tags
    assert item.word_count == 4200
    assert "sequence transduction models" in item.content


@pytest.mark.asyncio
async def test_successful_legacy_v1_array_retrieval():
    """Test successful legacy API v1 response with mixed array of user, bookmark, and highlight objects."""
    user_obj = {"type": "user", "username": "alice"}
    bm_obj = sample_instapaper_bookmark(bookmark_id=701, title="Legacy Article")
    bm_obj["type"] = "bookmark"
    hl_obj = {
        "type": "highlight",
        "highlight_id": 99,
        "bookmark_id": 701,
        "text": "A crucial quote from the article.",
        "note": "A note on the quote.",
    }

    mock_session = MockSession([
        MockResponse([user_obj, bm_obj, hl_obj])
    ])

    source = InstapaperSource(token="token", session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    item = items[0]
    assert item.source_id == "instapaper:701"
    assert item.title == "Legacy Article"
    assert "A crucial quote from the article." in item.content
    assert "**My note:** A note on the quote." in item.content


@pytest.mark.asyncio
async def test_api_pagination_with_cursor():
    """Test pagination across multiple pages using cursor without dropping items."""
    bm1 = sample_instapaper_bookmark(bookmark_id=1, title="Bookmark 1")
    bm2 = sample_instapaper_bookmark(bookmark_id=2, title="Bookmark 2")
    bm3 = sample_instapaper_bookmark(bookmark_id=3, title="Bookmark 3")

    page1 = MockResponse({"bookmarks": [bm1], "cursor": "cursor_page_2"})
    page2 = MockResponse({"bookmarks": [bm2], "cursor": "cursor_page_3"})
    page3 = MockResponse({"bookmarks": [bm3], "cursor": None})

    mock_session = MockSession([page1, page2, page3])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 3
    assert [i.title for i in items] == ["Bookmark 1", "Bookmark 2", "Bookmark 3"]
    assert len(mock_session.calls) == 3
    assert mock_session.calls[1]["kwargs"]["params"]["cursor"] == "cursor_page_2"
    assert mock_session.calls[2]["kwargs"]["params"]["cursor"] == "cursor_page_3"


@pytest.mark.asyncio
async def test_pagination_stops_on_repeated_cursor(caplog):
    """Test that pagination safely stops and warns if cursor is repeated."""
    bm1 = sample_instapaper_bookmark(bookmark_id=1, title="Bookmark 1")
    bm2 = sample_instapaper_bookmark(bookmark_id=2, title="Bookmark 2")

    page1 = MockResponse({"bookmarks": [bm1], "cursor": "loop_cursor_999"})
    page2 = MockResponse({"bookmarks": [bm2], "cursor": "loop_cursor_999"})

    mock_session = MockSession([page1, page2])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 2
    assert "duplicate cursor 'loop_cursor_999'" in caplog.text


@pytest.mark.asyncio
async def test_pagination_stops_on_duplicate_batch_fingerprint(caplog):
    """Test that pagination safely stops if the exact same batch of bookmarks is returned repeatedly."""
    bm1 = sample_instapaper_bookmark(bookmark_id=1, title="Stuck Bookmark")

    page1 = MockResponse({"bookmarks": [bm1], "cursor": "next_1"})
    page2 = MockResponse({"bookmarks": [bm1], "cursor": "next_2"})

    mock_session = MockSession([page1, page2])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()

    assert len(items) == 1
    assert "duplicate batch of bookmarks" in caplog.text


@pytest.mark.asyncio
async def test_api_empty_response():
    """Test empty bookmarks list returns empty list without error."""
    mock_session = MockSession([MockResponse({"bookmarks": []})])
    source = InstapaperSource(token="token", session=mock_session)
    items = await source.fetch_items()
    assert items == []


@pytest.mark.asyncio
async def test_api_error_401():
    """Test HTTP 401 raises SourceError indicating authentication failure."""
    resp = requests.Response()
    resp.status_code = 401
    mock_session = MockSession([MockResponse({"error": "Unauthorized"}, status_code=401)])

    source = InstapaperSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="Instapaper authentication failed"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_403():
    """Test HTTP 403 raises SourceError indicating forbidden access."""
    resp = requests.Response()
    resp.status_code = 403
    mock_session = MockSession([MockResponse({"error": "Subscription required"}, status_code=403)])

    source = InstapaperSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="Instapaper access forbidden"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_429():
    """Test HTTP 429 raises SourceError indicating rate limit."""
    mock_session = MockSession([MockResponse({"error": "Rate limit exceeded"}, status_code=429)])

    source = InstapaperSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="rate limit exceeded"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_500():
    """Test HTTP 500 raises SourceError."""
    mock_session = MockSession([MockResponse({"error": "Server error"}, status_code=500)])

    source = InstapaperSource(token="token", session=mock_session)
    with pytest.raises(SourceError, match="Instapaper API error"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_network_timeout():
    """Test network timeout maps to descriptive SourceError."""
    mock_session = MockSession(side_effect=requests.exceptions.Timeout("Connection timed out"))
    source = InstapaperSource(token="token", session=mock_session)

    with pytest.raises(SourceError, match="Instapaper API request timed out"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_connection_error():
    """Test connection error maps to descriptive SourceError."""
    mock_session = MockSession(side_effect=requests.exceptions.ConnectionError("Failed to connect"))
    source = InstapaperSource(token="token", session=mock_session)

    with pytest.raises(SourceError, match="Network connection failed"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_malformed_item_isolation():
    """Test individual malformed item in batch is skipped without aborting healthy items."""
    good_item1 = sample_instapaper_bookmark(bookmark_id=1, title="Good Bookmark 1")
    bad_item = "not a dict"
    good_item2 = sample_instapaper_bookmark(bookmark_id=2, title="Good Bookmark 2")

    mock_session = MockSession([
        MockResponse({"bookmarks": [good_item1, bad_item, good_item2]})
    ])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 2
    assert [i.title for i in items] == ["Good Bookmark 1", "Good Bookmark 2"]


@pytest.mark.asyncio
async def test_duplicate_items_deduplicated():
    """Test duplicate items with identical bookmark ID within run are deduplicated."""
    bm1 = sample_instapaper_bookmark(bookmark_id=100, title="Duplicate Bookmark")
    bm2 = sample_instapaper_bookmark(bookmark_id=100, title="Duplicate Bookmark")

    mock_session = MockSession([
        MockResponse({"bookmarks": [bm1, bm2]})
    ])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 1


# ---------------------------------------------------------------------------
# 4. Identity & Deduplication
# ---------------------------------------------------------------------------

def test_stable_source_id_format():
    """Test stable source ID format instapaper:<bookmark_id>."""
    assert derive_instapaper_stable_id({"bookmark_id": 12345}) == "instapaper:12345"
    assert derive_instapaper_stable_id({"id": 67890}) == "instapaper:67890"


def test_missing_bookmark_id_raises_source_error():
    """Test that a record lacking an immutable ID raises SourceError rather than using title or URL."""
    record = {"title": "Title Without ID", "url": "https://example.com/no-id"}
    with pytest.raises(SourceError, match="missing an immutable bookmark ID"):
        derive_instapaper_stable_id(record)


@pytest.mark.asyncio
async def test_batch_isolation_skips_record_without_id():
    """Test that records without immutable ID are safely skipped without failing healthy records in batch."""
    good_item = sample_instapaper_bookmark(bookmark_id=1, title="Valid Bookmark")
    bad_item = {"title": "No ID Bookmark", "url": "https://example.com/bad"}
    good_item2 = sample_instapaper_bookmark(bookmark_id=2, title="Another Valid Bookmark")

    mock_session = MockSession([
        MockResponse({"bookmarks": [good_item, bad_item, good_item2]})
    ])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 2
    assert [i.title for i in items] == ["Valid Bookmark", "Another Valid Bookmark"]


@pytest.mark.asyncio
async def test_deduplication_lifecycle(tmp_path):
    """Test NEW -> UNCHANGED -> CHANGED lifecycle with in-place overwriting."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_instapaper.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    # 1. First run: NEW
    raw_v1 = sample_instapaper_bookmark(
        bookmark_id=800,
        title="Modern Software Engineering",
        description="First excerpt text.",
    )
    mock_session1 = MockSession([MockResponse({"bookmarks": [raw_v1]})])
    source1 = InstapaperSource(token="token", session=mock_session1)
    items1 = await source1.fetch_items()

    action1, rel_path1 = await pipeline.process_item(source1, items1[0])
    assert action1 == IngestionAction.NEW
    note_file = vault_dir / rel_path1
    assert "First excerpt text." in note_file.read_text(encoding="utf-8")

    # 2. Second run without change: UNCHANGED
    mock_session2 = MockSession([MockResponse({"bookmarks": [raw_v1]})])
    source2 = InstapaperSource(token="token", session=mock_session2)
    items2 = await source2.fetch_items()

    action2, rel_path2 = await pipeline.process_item(source2, items2[0])
    assert action2 == IngestionAction.UNCHANGED

    # 3. Third run with updated content and title change: CHANGED (in-place update, preserves path)
    raw_v2 = sample_instapaper_bookmark(
        bookmark_id=800,
        title="Modern Software Engineering - Updated Edition",
        description="Updated and expanded excerpt.",
    )
    mock_session3 = MockSession([MockResponse({"bookmarks": [raw_v2]})])
    source3 = InstapaperSource(token="token", session=mock_session3)
    items3 = await source3.fetch_items()

    action3, rel_path3 = await pipeline.process_item(source3, items3[0])
    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1  # Same file path preserved

    updated_text = note_file.read_text(encoding="utf-8")
    assert "Updated and expanded excerpt." in updated_text

    # Verify no duplicate file created
    all_notes = list((vault_dir / "Ingested" / "Web").glob("*.md"))
    assert len(all_notes) == 1


# ---------------------------------------------------------------------------
# 5. Markdown, Frontmatter & Highlights Rendering
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_markdown_frontmatter_and_attribution():
    """Test YAML frontmatter and attribution block generation."""
    raw_bm = sample_instapaper_bookmark(
        bookmark_id=999,
        title="Thinking in Systems",
        author="Donella Meadows",
        url="https://example.com/systems",
        folder="archive",
        starred="1",
        progress=0.85,
        tags=["systems", "#complexity"],
    )
    mock_session = MockSession([MockResponse({"bookmarks": [raw_bm]})])
    source = InstapaperSource(token="token", session=mock_session)
    items = await source.fetch_items()

    note = await source.convert_to_markdown(items[0])
    fm = note.to_frontmatter_dict()

    assert fm["title"] == "Thinking in Systems"
    assert fm["source"] == "instapaper"
    assert "ingested" in fm["tags"]
    assert "instapaper" in fm["tags"]
    assert "systems" in fm["tags"]
    assert "complexity" in fm["tags"]
    assert "#complexity" not in fm["tags"]
    assert fm["author"] == "Donella Meadows"
    assert fm["source_url"] == "https://example.com/systems"
    assert fm["bookmark_id"] == 999
    assert fm["folder"] == "archive"
    assert fm["starred"] is True
    assert fm["progress"] == 0.85


def test_highlights_and_personal_notes_rendered_cleanly():
    """Test highlight passages rendered as Markdown blockquotes with personal notes."""
    highlights = [
        {
            "highlight_id": 1,
            "text": "Self-organization produces heterogeneity and unpredictable novelty.\nIt is the source of all resilience.",
            "note": "Crucial principle of complex adaptive systems.",
        },
        {
            "highlight_id": 2,
            "text": "A system's function or purpose is not necessarily what its players intend.",
            "note": None,
        },
    ]
    data = {
        "description": "An introduction to system dynamics.",
        "highlights": highlights,
    }

    body = build_instapaper_body(data)
    assert "## Highlights" in body
    assert "> Self-organization produces heterogeneity and unpredictable novelty." in body
    assert "> It is the source of all resilience." in body
    assert "**My note:** Crucial principle of complex adaptive systems." in body
    assert "> A system's function or purpose is not necessarily what its players intend." in body


def test_no_empty_highlights_section_when_highlights_absent():
    """Test that no empty ## Highlights section is emitted when a bookmark has no highlights."""
    data = {"description": "Article without any highlights."}
    body = build_instapaper_body(data)
    assert "## Highlights" not in body
    assert "## Excerpt" in body


def test_html_content_converted_via_markdownify():
    """Test HTML article content is converted to clean Markdown via markdownify."""
    data = {
        "html": "<h2>Section Header</h2><p>This is paragraph text with <strong>bold</strong> words.</p>"
    }
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "## Section Header" in body
    assert "**bold**" in body


def test_html_in_content_is_converted_to_markdown():
    """Test that HTML in the 'content' field is converted to clean Markdown under ## Content."""
    data = {
        "content": "<p>First paragraph with <a href=\"https://example.com\">link</a>.</p><p>Second paragraph with <b>bold</b> text.</p>"
    }
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "First paragraph with [link](https://example.com)." in body
    assert "Second paragraph with **bold** text." in body
    assert "<p>" not in body
    assert "<b>" not in body
    assert "<a href" not in body


def test_html_in_text_is_converted_when_appropriate():
    """Test that HTML in the 'text' field is converted to clean Markdown when content is absent."""
    data = {
        "text": "<h3>Header 3</h3><p>Paragraph with <em>italics</em> and <code>inline_code()</code>.</p>"
    }
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "### Header 3" in body
    assert "*italics*" in body
    assert "`inline_code()`" in body
    assert "<h3>" not in body
    assert "<em>" not in body
    assert "<code>" not in body


def test_existing_html_field_converts_correctly():
    """Test that 'html' field continues to convert correctly as a fallback when content/text are absent."""
    data = {
        "html": "<article><h1>Article Title</h1><p>Main story with a <br>line break.</p></article>"
    }
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "# Article Title" in body
    assert "Main story with a" in body
    assert "<article>" not in body
    assert "<br>" not in body


def test_plain_text_remains_readable():
    """Test that plain text is preserved appropriately without mangling."""
    plain = "This is a simple plain text note without any HTML tags.\n\nIt has two paragraphs."
    data = {"content": plain}
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert plain in body
    assert normalize_instapaper_content(plain) == plain


def test_markdown_content_remains_readable():
    """Test that valid Markdown formatting is preserved without destructive conversion."""
    md_text = (
        "# Top Heading\n\n"
        "This has **bold** text, *italic* words, and `inline code`.\n\n"
        "- List item 1\n"
        "- List item 2\n\n"
        "> Quoted block\n\n"
        "Comparison: if x < 10 and y > 20:\n"
        "    print('ok')"
    )
    data = {"content": md_text}
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert md_text in body
    assert normalize_instapaper_content(md_text) == md_text


def test_script_and_style_content_removed():
    """Test that <script> and <style> elements and their internal content are completely removed."""
    html_with_scripts = (
        "<style>.hidden { display: none; } body { color: red; }</style>"
        "<p>Real article paragraph.</p>"
        "<script type=\"text/javascript\">alert('xss'); window.location = 'http://bad.com';</script>"
        "<p>Second paragraph after script.</p>"
        "<script>bad_func();</script>"
    )
    data = {"content": html_with_scripts}
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "Real article paragraph." in body
    assert "Second paragraph after script." in body
    assert "<script" not in body.lower()
    assert "</script>" not in body.lower()
    assert "alert('xss')" not in body
    assert "bad_func" not in body
    assert "<style" not in body.lower()
    assert "</style>" not in body.lower()
    assert "display: none" not in body


def test_generated_markdown_contains_no_raw_article_html_tags():
    """Test that generated Markdown output contains no raw HTML article tags."""
    raw_html = (
        "<section class=\"article-body\">"
        "<header><h2>Header Title</h2></header>"
        "<div class=\"content\"><p>Paragraph with <strong>bold</strong>, <em>italic</em>, "
        "and <a href=\"https://aurora.local\">link</a>.</p>"
        "<blockquote>A quote block</blockquote>"
        "<ul><li>Item 1</li><li>Item 2</li></ul>"
        "<hr><p>Final note.</p></div>"
        "<footer><small>Footer info</small></footer>"
        "</section>"
    )
    data = {"content": raw_html}
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "## Header Title" in body
    assert "**bold**" in body
    assert "*italic*" in body
    assert "[link](https://aurora.local)" in body
    assert "A quote block" in body
    html_tag_check = re.compile(
        r"</?(?:section|header|h2|div|p|strong|em|a|blockquote|ul|li|hr|footer|small)\b",
        re.IGNORECASE,
    )
    assert not html_tag_check.search(body)


def test_excerpt_fallback_still_works():
    """Test that when full content/text/html are absent, description/excerpt falls back to ## Excerpt."""
    data_desc = {"description": "Plain description fallback."}
    body_desc = build_instapaper_body(data_desc)
    assert "## Excerpt" in body_desc
    assert "Plain description fallback." in body_desc
    assert "## Content" not in body_desc

    data_html_desc = {"description": "<p>HTML excerpt with <b>emphasis</b>.</p>"}
    body_html_desc = build_instapaper_body(data_html_desc)
    assert "## Excerpt" in body_html_desc
    assert "HTML excerpt with **emphasis**." in body_html_desc
    assert "<p>" not in body_html_desc

    data_excerpt = {"excerpt": "Excerpt field fallback."}
    body_excerpt = build_instapaper_body(data_excerpt)
    assert "## Excerpt" in body_excerpt
    assert "Excerpt field fallback." in body_excerpt


def test_missing_content_still_produces_existing_fallback():
    """Test that completely missing or empty content/excerpt produces the standard fallback."""
    data_empty = {}
    body = build_instapaper_body(data_empty)
    assert "## Content" in body
    assert "*No excerpt or content provided by Instapaper.*" in body

    data_spaces = {"content": "   ", "description": ""}
    body_spaces = build_instapaper_body(data_spaces)
    assert "## Content" in body_spaces
    assert "*No excerpt or content provided by Instapaper.*" in body_spaces

    data_only_scripts = {"content": "<script>alert(1);</script><style>.a{}</style>"}
    body_scripts = build_instapaper_body(data_only_scripts)
    assert "## Content" in body_scripts
    assert "*No excerpt or content provided by Instapaper.*" in body_scripts


def test_highlights_and_notes_remain_unaffected_by_normalization():
    """Test that highlights and personal notes are formatted cleanly and remain unaffected by content normalization."""
    data = {
        "content": "<p>Article text here.</p>",
        "highlights": [
            {
                "highlight_id": 101,
                "text": "The limits of my language mean the limits of my world.",
                "note": "Wittgenstein's Tractatus",
            },
            {
                "highlight_id": 102,
                "text": "Whereof one cannot speak, thereof one must be silent.",
                "note": None,
            },
        ],
    }
    body = build_instapaper_body(data)
    assert "## Content" in body
    assert "Article text here." in body
    assert "## Highlights" in body
    assert "> The limits of my language mean the limits of my world." in body
    assert "**My note:** Wittgenstein's Tractatus" in body
    assert "> Whereof one cannot speak, thereof one must be silent." in body


def test_is_html_content_detection_matrix():
    """Test deterministic is_html_content detection across diverse inputs."""
    assert is_html_content("<p>Paragraph</p>") is True
    assert is_html_content("<p>Unclosed paragraph") is True
    assert is_html_content("<div>Content</div>") is True
    assert is_html_content("<script>alert(1)</script>") is True
    assert is_html_content("<style>.a{color:red}</style>") is True
    assert is_html_content("Hello<br>world") is True
    assert is_html_content("<img src='test.png'>") is True
    assert is_html_content("<a href='https://example.com'>Link</a>") is True
    assert is_html_content("<b>Bold</b>") is True
    assert is_html_content("<!DOCTYPE html><html><body></body></html>") is True
    assert is_html_content("<!-- comment -->") is True

    assert is_html_content("Plain text") is False
    assert is_html_content("Comparison: a < b and c > d") is False
    assert is_html_content("if (x < p and q > y): print(1)") is False
    assert is_html_content("# Heading\n\n**bold** and *italic*") is False
    assert is_html_content("- Item 1\n- Item 2") is False
    assert is_html_content("Check <https://example.com> for info") is False
    assert is_html_content("") is False
    assert is_html_content("   ") is False
    assert is_html_content(None) is False


def test_instapaper_metadata_section():
    """Test Instapaper metadata section formatting."""
    data = {
        "folder": "reading-list",
        "starred": "1",
        "progress": 0.42,
        "time": 1773561600,
    }
    body = build_instapaper_body(data)
    assert "## Instapaper Metadata" in body
    assert "- Folder: Reading-List" in body
    assert "- Starred: yes" in body
    assert "- Reading Progress: 42%" in body
    assert "- Saved: 2026-03-15" in body


# ---------------------------------------------------------------------------
# 6. Files & Vault Destination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_target_folder_is_ingested_web(tmp_path):
    """Test Instapaper notes are written under Ingested/Web/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_ip.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    raw_bm = sample_instapaper_bookmark(bookmark_id=333, title="Pragmatic Programmer")
    mock_session = MockSession([MockResponse({"bookmarks": [raw_bm]})])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    action, rel_path = await pipeline.process_item(source, items[0])

    assert action == IngestionAction.NEW
    assert rel_path.startswith("Ingested/Web/")
    assert rel_path.endswith(".md")
    assert (vault_dir / rel_path).exists()


@pytest.mark.asyncio
async def test_long_title_truncation(tmp_path):
    """Test note titles over 80 characters are safely truncated in filenames."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_ip_long.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    super_long_title = "I" * 120
    raw_bm = sample_instapaper_bookmark(bookmark_id=444, title=super_long_title)
    mock_session = MockSession([MockResponse({"bookmarks": [raw_bm]})])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])

    filename = Path(rel_path).name
    # <YYYY-MM-DD>_instapaper_<80_chars>.md
    title_part = filename.split("_instapaper_")[1].replace(".md", "")
    assert len(title_part) <= 80


@pytest.mark.asyncio
async def test_filename_collision_handling(tmp_path):
    """Test distinct Instapaper bookmarks with identical titles receive unique filenames (_2.md)."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_ip_col.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    bm1 = sample_instapaper_bookmark(bookmark_id=1, title="Notes on Architecture")
    bm2 = sample_instapaper_bookmark(bookmark_id=2, title="Notes on Architecture")

    mock_session = MockSession([MockResponse({"bookmarks": [bm1, bm2]})])
    source = InstapaperSource(token="token", session=mock_session)

    items = await source.fetch_items()
    _, rel1 = await pipeline.process_item(source, items[0])
    _, rel2 = await pipeline.process_item(source, items[1])

    assert rel1 != rel2
    assert "_2.md" in rel2 or "_2.md" in rel1


# ---------------------------------------------------------------------------
# 7. Edge Cases & Helpers
# ---------------------------------------------------------------------------

def test_missing_title_fallback():
    """Test missing or empty title falls back to Untitled Instapaper Bookmark."""
    source = InstapaperSource(token="token")
    item1 = source._convert_raw_to_source_item({"bookmark_id": 1, "title": ""})
    item2 = source._convert_raw_to_source_item({"bookmark_id": 2})

    assert item1.title == "Untitled Instapaper Bookmark"
    assert item2.title == "Untitled Instapaper Bookmark"


def test_missing_optional_fields():
    """Test missing author, URLs, description are handled without error."""
    source = InstapaperSource(token="token")
    item = source._convert_raw_to_source_item({"bookmark_id": 3, "title": "Minimal"})

    assert item.author is None
    assert item.source_url is None
    assert item.summary is None


def test_special_unicode_and_emojis():
    """Test Unicode emojis and non-ASCII characters in titles and highlights."""
    raw_bm = sample_instapaper_bookmark(
        bookmark_id=777,
        title="Neural Networks 🧠 & 深層学習",
        description="Backpropagation overview 🔍 説明",
        highlights=[{"text": "Gradient descent step 📉 勾配"}],
    )
    source = InstapaperSource(token="token")
    item = source._convert_raw_to_source_item(raw_bm)

    assert "🧠" in item.title
    assert "深層学習" in item.title
    assert "🔍" in item.content
    assert "📉" in item.content


def test_timestamp_parsing():
    """Test Unix timestamp and ISO date string parsing."""
    assert parse_instapaper_timestamp(1773561600) is not None
    assert parse_instapaper_timestamp("1773561600") is not None
    assert parse_instapaper_timestamp("2026-03-15T10:00:00Z") is not None
    assert parse_instapaper_timestamp(None) is None
    assert parse_instapaper_timestamp("invalid") is None


# ---------------------------------------------------------------------------
# 8. CLI Integration
# ---------------------------------------------------------------------------

def test_cli_parser_registration():
    """Test CLI parser handles ingest-instapaper and generic ingest --source instapaper."""
    parser = create_parser()

    # Shortcut: ingest-instapaper
    args1 = parser.parse_args([
        "ingest-instapaper",
        "--token", "tok_val",
        "--username", "user_val",
        "--password", "pass_val",
        "--folder", "archive",
        "--limit", "30",
    ])
    assert args1.command == "ingest-instapaper"
    assert args1.token == "tok_val"
    assert args1.username == "user_val"
    assert args1.password == "pass_val"
    assert args1.folder == "archive"
    assert args1.limit == 30

    # Generic: ingest --source instapaper
    args2 = parser.parse_args([
        "ingest",
        "--source", "instapaper",
        "--token", "tok_val",
        "--folder", "archive",
    ])
    assert args2.command == "ingest"
    assert args2.source == "instapaper"
    assert args2.token == "tok_val"
    assert args2.folder == "archive"


@pytest.mark.asyncio
async def test_cli_execution_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of CLI ingest-instapaper command with mocked API."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_ip.sqlite"

    raw_bm = sample_instapaper_bookmark(bookmark_id=8888, title="CLI Instapaper Note")
    mock_resp = MockResponse({"bookmarks": [raw_bm]})

    monkeypatch.setattr(
        "requests.Session.get",
        lambda self, url, **kwargs: mock_resp,
    )

    parser = create_parser()
    args = parser.parse_args([
        "ingest-instapaper",
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
    assert "CLI Instapaper Note" in content
    assert "source: instapaper" in content


@pytest.mark.asyncio
async def test_generic_cli_execution_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of generic CLI ingest --source instapaper command with mocked API."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_gen_ip.sqlite"

    raw_bm = sample_instapaper_bookmark(bookmark_id=7777, title="Generic CLI Instapaper Note")
    mock_resp = MockResponse({"bookmarks": [raw_bm]})

    monkeypatch.setattr(
        "requests.Session.get",
        lambda self, url, **kwargs: mock_resp,
    )

    parser = create_parser()
    args = parser.parse_args([
        "ingest",
        "--source", "instapaper",
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
    assert "Generic CLI Instapaper Note" in content
    assert "source: instapaper" in content
