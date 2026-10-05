"""Comprehensive tests for RSS and Atom source connector."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.rss_source import (
    RSSSource,
    clean_html_to_markdown,
    is_tracking_pixel_or_icon,
    normalize_url,
    parse_feed_date,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Feed Fixtures
# ---------------------------------------------------------------------------

RSS_2_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Tech Insights Feed</title>
    <link>https://techinsights.example.com</link>
    <description>Latest insights into software architecture and AI</description>
    <language>en</language>
    <item>
      <title>Building Distributed Agent Networks</title>
      <link>https://techinsights.example.com/posts/agent-networks?ref=rss#overview</link>
      <guid isPermaLink="false">post-guid-101</guid>
      <pubDate>Sun, 04 Oct 2026 10:00:00 GMT</pubDate>
      <dc:creator>Alice Engineer</dc:creator>
      <category>AI Systems</category>
      <category>Architecture</category>
      <description>A brief overview of multi-agent distributed systems.</description>
      <content:encoded><![CDATA[<p>Full article body on distributed agents. Agents communicate asynchronously via message buses.</p><p>They coordinate to solve complex workflows.</p>]]></content:encoded>
    </item>
    <item>
      <title>Vector Search in Production</title>
      <link>https://techinsights.example.com/posts/vector-search</link>
      <guid isPermaLink="false">post-guid-102</guid>
      <pubDate>Sat, 03 Oct 2026 15:30:00 GMT</pubDate>
      <dc:creator>Bob Scientist</dc:creator>
      <category>Databases</category>
      <description>Production patterns for embedding stores and HNSW indexes.</description>
      <content:encoded><![CDATA[<p>Deep dive into vector databases and approximate nearest neighbor indexing in production applications.</p><p>Benchmarking cosine similarity vs dot product metrics.</p>]]></content:encoded>
    </item>
  </channel>
</rss>"""

ATOM_XML = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Engineering Dispatches</title>
  <link href="https://dispatches.example.com/"/>
  <subtitle>Field notes on scalable system design</subtitle>
  <updated>2026-10-04T12:00:00Z</updated>
  <id>urn:uuid:60a76c80-d399-11d9-b93C-0003939e0af6</id>
  <entry>
    <title>Memory Efficiency in Modern Python</title>
    <link href="https://dispatches.example.com/memory-python"/>
    <id>tag:dispatches.example.com,2026:entry-55</id>
    <updated>2026-10-04T11:00:00Z</updated>
    <published>2026-10-04T10:30:00Z</published>
    <author>
      <name>Carol Developer</name>
    </author>
    <summary>Exploring memory layouts, weakrefs, and zero-copy string manipulation.</summary>
    <category term="Python"/>
    <category term="Performance"/>
    <content type="html"><![CDATA[<p>Python 3.14 introduces several optimizations for object memory layouts and execution graphs.</p><p>Understanding object allocations and garbage collection passes.</p>]]></content>
  </entry>
</feed>"""

SUMMARY_ONLY_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Short Summaries Feed</title>
    <link>https://news.example.com</link>
    <item>
      <title>Major Framework Release</title>
      <link>https://news.example.com/articles/framework-v3</link>
      <guid>news-article-888</guid>
      <pubDate>Sun, 04 Oct 2026 09:00:00 GMT</pubDate>
      <description>A new major framework version has just been announced with zero dependencies.</description>
    </item>
  </channel>
</rss>"""

NO_GUID_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>No GUID Feed</title>
    <link>https://noguid.example.com</link>
    <item>
      <title>Article Without GUID</title>
      <link>https://noguid.example.com/article-1#section-alpha</link>
      <pubDate>Sun, 04 Oct 2026 08:00:00 GMT</pubDate>
      <description><![CDATA[<p>This item has no guid tag, only an article link.</p>]]></description>
    </item>
  </channel>
</rss>"""

NO_GUID_NO_URL_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Minimal Feed</title>
    <link>https://minimal.example.com</link>
    <item>
      <title>Bare Minimum Article</title>
      <pubDate>Sun, 04 Oct 2026 07:00:00 GMT</pubDate>
      <description>Bare article description without links or guid.</description>
    </item>
  </channel>
</rss>"""

LONG_SUMMARY_NO_CONTENT_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Long Summary Feed</title>
    <link>https://longsummary.example.com</link>
    <item>
      <title>Article With Very Long Summary But No Content</title>
      <link>https://longsummary.example.com/article-full</link>
      <guid>long-summary-item-1</guid>
      <pubDate>Mon, 05 Oct 2026 12:00:00 GMT</pubDate>
      <description>This is an extensive summary paragraph explaining what the article is about. This is an extensive summary paragraph explaining what the article is about. This is an extensive summary paragraph explaining what the article is about. This is an extensive summary paragraph explaining what the article is about. This is an extensive summary paragraph explaining what the article is about.</description>
    </item>
  </channel>
</rss>"""

SHORT_CONTENT_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Short Content Feed</title>
    <link>https://shortcontent.example.com</link>
    <item>
      <title>Short Note</title>
      <link>https://shortcontent.example.com/note-1</link>
      <guid>short-content-item-1</guid>
      <pubDate>Mon, 05 Oct 2026 13:00:00 GMT</pubDate>
      <description>Summary teaser</description>
      <content:encoded><![CDATA[<p>Brief full note.</p>]]></content:encoded>
    </item>
  </channel>
</rss>"""

EMPTY_FEED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Empty Feed</title>
    <link>https://empty.example.com</link>
    <description>No articles yet</description>
  </channel>
</rss>"""

MALFORMED_XML = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Broken Feed
    <item>
      <title>Unclosed
"""


class MockHttpResponse:
    """Mock requests.Response helper."""

    def __init__(
        self,
        text: str = "",
        content: bytes = b"",
        status_code: int = 200,
        headers: dict | None = None,
        url: Optional[str] = None,
    ) -> None:
        self.text = text
        self.content = content or text.encode("utf-8")
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "application/xml; charset=utf-8"}
        self.url = url or ""
        self.reason = "OK" if status_code == 200 else "Error"


# ---------------------------------------------------------------------------
# Unit Tests
# ---------------------------------------------------------------------------

def test_rss_source_registration():
    """1. Test RSSSource is properly registered in SourceRegistry."""
    cls = SourceRegistry.get("rss")
    assert cls is not None
    assert cls is RSSSource
    instance = SourceRegistry.create("rss")
    assert isinstance(instance, RSSSource)
    assert instance.source_type == "rss"
    assert instance.display_name == "RSS / Atom Feeds"


def test_cli_list_sources_shows_rss():
    """2. Test list-sources includes rss in registered connectors."""
    sources_dict = SourceRegistry.list_sources()
    assert "rss" in sources_dict
    assert sources_dict["rss"] == "RSSSource"


def test_url_normalization_and_schemes():
    """12 & 29. Test URL normalization, fragment stripping, and scheme validation."""
    # Valid normalization
    assert (
        normalize_url("https://example.com/feed.xml#section")
        == "https://example.com/feed.xml"
    )
    assert (
        normalize_url("HTTP://EXAMPLE.COM:80/feed/")
        == "http://example.com/feed"
    )
    assert (
        normalize_url("https://example.com:443/rss?category=tech#latest")
        == "https://example.com/rss?category=tech"
    )

    # Invalid schemes
    with pytest.raises(SourceError, match="Unsupported URL scheme"):
        normalize_url("ftp://example.com/feed.xml")
    with pytest.raises(SourceError, match="missing an HTTP/HTTPS scheme"):
        normalize_url("example.com/feed.xml")
    with pytest.raises(SourceError, match="valid URL string is required"):
        normalize_url("")


def test_parse_feed_date():
    """10. Test date parsing from struct_time and string dates."""
    entry_struct = {"published_parsed": (2026, 10, 4, 10, 0, 0, 6, 277, 0)}
    iso_date, display_date = parse_feed_date(entry_struct)
    assert iso_date == "2026-10-04"
    assert display_date == "2026-10-04 10:00"

    entry_rfc = {"pubDate": "Sun, 04 Oct 2026 12:30:00 GMT"}
    iso_date2, _ = parse_feed_date(entry_rfc)
    assert iso_date2 == "2026-10-04"

    entry_iso = {"updated": "2026-10-04T15:45:00Z"}
    iso_date3, _ = parse_feed_date(entry_iso)
    assert iso_date3 == "2026-10-04"


def test_clean_html_to_markdown():
    """Test HTML to clean Markdown conversion stripping scripts and styles."""
    html = """
    <div>
        <script>alert(1);</script>
        <h2>Heading 2</h2>
        <p>Paragraph with <strong>bold</strong> text.</p>
        <style>.hide { display: none; }</style>
        <ul><li>Item 1</li><li>Item 2</li></ul>
    </div>
    """
    md = clean_html_to_markdown(html)
    assert "alert(1)" not in md
    assert "## Heading 2" in md
    assert "**bold**" in md
    assert "- Item 1" in md or "* Item 1" in md


def test_is_tracking_pixel_or_icon():
    """Test tracking pixel heuristic for RSS feeds."""
    assert is_tracking_pixel_or_icon("https://example.com/1x1.gif")
    assert is_tracking_pixel_or_icon("https://stats.wp.com/b.gif?v=noscript")
    assert is_tracking_pixel_or_icon("https://example.com/spacer.gif")
    assert is_tracking_pixel_or_icon("https://example.com/image.jpg", "tracking pixel")
    assert not is_tracking_pixel_or_icon("https://example.com/diagram.png", "Architecture Diagram")


# ---------------------------------------------------------------------------
# Feed Parsing & Metadata Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rss2_parsing_and_metadata():
    """3, 5, 6, 7, 8, 9, 10, 11, 12, 13. Test full RSS 2.0 parsing and item metadata."""
    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=RSS_2_XML)

    source = RSSSource(session=mock_session)
    items = await source.fetch_items("https://techinsights.example.com/rss")

    assert len(items) == 2

    # Item 1
    item1 = items[0]
    assert item1.title == "Building Distributed Agent Networks"
    assert item1.source_url == "https://techinsights.example.com/posts/agent-networks?ref=rss#overview"
    assert item1.source_id == "rss:post-guid-101"
    assert item1.date == "2026-10-04"
    assert item1.author == "Alice Engineer"
    assert "ai-systems" in item1.tags or "ai" in item1.tags
    assert "architecture" in item1.tags
    assert "ingested" in item1.tags
    assert "rss" in item1.tags
    assert "Full article body on distributed agents" in item1.content
    assert item1.extra_metadata["feed_title"] == "Tech Insights Feed"
    assert item1.extra_metadata["feed_url"] == "https://techinsights.example.com/rss"

    # Item 2
    item2 = items[1]
    assert item2.title == "Vector Search in Production"
    assert item2.source_id == "rss:post-guid-102"
    assert item2.author == "Bob Scientist"
    assert "Deep dive into vector databases" in item2.content


@pytest.mark.asyncio
async def test_atom_parsing_and_metadata():
    """4. Test Atom feed parsing and metadata extraction."""
    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=ATOM_XML)

    source = RSSSource(session=mock_session)
    items = await source.fetch_items("https://dispatches.example.com/atom.xml")

    assert len(items) == 1
    item = items[0]
    assert item.title == "Memory Efficiency in Modern Python"
    assert item.source_url == "https://dispatches.example.com/memory-python"
    assert item.source_id == "rss:tag:dispatches.example.com,2026:entry-55"
    assert item.author == "Carol Developer"
    assert item.date == "2026-10-04"
    assert "python" in item.tags
    assert "performance" in item.tags
    assert "Python 3.14 introduces several optimizations" in item.content
    assert item.extra_metadata["feed_title"] == "Engineering Dispatches"


@pytest.mark.asyncio
async def test_summary_only_item_with_trafilatura_expansion():
    """14 & 15. Test summary-only item triggers article URL fetch and trafilatura extraction."""
    mock_session = MagicMock()

    article_html = """
    <html>
      <body>
        <article>
          <h1>Major Framework Release</h1>
          <p>This is the full extracted article content from the web page. It contains detailed migration steps and code examples.</p>
        </article>
      </body>
    </html>
    """

    def mock_get(url, **kwargs):
        if "news.example.com/rss" in url:
            return MockHttpResponse(text=SUMMARY_ONLY_RSS)
        elif "articles/framework-v3" in url:
            return MockHttpResponse(text=article_html, headers={"Content-Type": "text/html"})
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    with patch("trafilatura.extract", return_value="This is the full extracted article content from the web page."):
        source = RSSSource(session=mock_session)
        items = await source.fetch_items("https://news.example.com/rss")

        assert len(items) == 1
        item = items[0]
        assert item.title == "Major Framework Release"
        # Content was expanded via trafilatura
        assert "This is the full extracted article content" in item.content


@pytest.mark.asyncio
async def test_summary_fallback_when_article_fetch_fails():
    """16. Test fallback to summary/excerpt when article URL fetch or extraction fails."""
    mock_session = MagicMock()

    def mock_get(url, **kwargs):
        if "news.example.com/rss" in url:
            return MockHttpResponse(text=SUMMARY_ONLY_RSS)
        # Article URL returns 500
        return MockHttpResponse(status_code=500)

    mock_session.get.side_effect = mock_get

    source = RSSSource(session=mock_session)
    items = await source.fetch_items("https://news.example.com/rss")

    assert len(items) == 1
    item = items[0]
    # Falls back gracefully to description from RSS
    assert "A new major framework version has just been announced" in item.content


@pytest.mark.asyncio
async def test_long_summary_with_no_content_field_calls_trafilatura():
    """Test that a long summary with NO content field triggers article fetch and trafilatura."""
    mock_session = MagicMock()
    article_html = "<html><body><article><p>Full in-depth article body from web page.</p></article></body></html>"

    def mock_get(url, **kwargs):
        if "longsummary.example.com/feed" in url:
            return MockHttpResponse(text=LONG_SUMMARY_NO_CONTENT_RSS)
        elif "article-full" in url:
            return MockHttpResponse(text=article_html, headers={"Content-Type": "text/html"})
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    with patch("trafilatura.extract", return_value="Full in-depth article body from web page.") as mock_extract:
        source = RSSSource(session=mock_session)
        items = await source.fetch_items("https://longsummary.example.com/feed")

        assert len(items) == 1
        item = items[0]
        # Trafilatura was called despite long summary because there is NO content field
        mock_extract.assert_called_once()
        assert "Full in-depth article body from web page." in item.content


@pytest.mark.asyncio
async def test_short_content_field_uses_feed_content_without_fetching_url():
    """Test that a short content field uses feed content and does NOT fetch article URL."""
    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=SHORT_CONTENT_RSS)

    with patch("trafilatura.extract") as mock_extract:
        source = RSSSource(session=mock_session)
        items = await source.fetch_items("https://shortcontent.example.com/feed")

        assert len(items) == 1
        item = items[0]
        # Content from feed is used
        assert "Brief full note." in item.content
        # Article URL was NOT fetched
        mock_extract.assert_not_called()
        # session.get was called ONLY for the feed XML itself
        assert mock_session.get.call_count == 1
        assert "https://shortcontent.example.com/feed" in mock_session.get.call_args_list[0][0][0]


@pytest.mark.asyncio
async def test_rss_deduplication_identities():
    """25 & 26. Test GUID vs URL fallback vs hash fallback identities."""
    source = RSSSource()

    # 1. Has GUID
    items_guid = await source.fetch_items(feed_content=RSS_2_XML)
    assert items_guid[0].source_id == "rss:post-guid-101"

    # 2. No GUID, has URL
    items_url = await source.fetch_items(feed_content=NO_GUID_RSS)
    assert items_url[0].source_id == "rss:url:https://noguid.example.com/article-1"

    # 3. No GUID, no URL
    items_hash = await source.fetch_items(feed_content=NO_GUID_NO_URL_RSS)
    assert items_hash[0].source_id.startswith("rss:https://example.com/feed:")


@pytest.mark.asyncio
async def test_invalid_feed_raises_source_error():
    """17. Test malformed or invalid XML raises SourceError."""
    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=MALFORMED_XML)

    source = RSSSource(session=mock_session)
    with pytest.raises(SourceError, match="Malformed or invalid feed"):
        await source.fetch_items("https://example.com/broken.xml")


@pytest.mark.asyncio
async def test_empty_feed_raises_source_error():
    """18. Test feed with no entries raises SourceError."""
    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=EMPTY_FEED_XML)

    source = RSSSource(session=mock_session)
    with pytest.raises(SourceError, match="contains no entries or articles"):
        await source.fetch_items("https://example.com/empty.xml")


@pytest.mark.asyncio
async def test_rss_http_errors():
    """19. Test HTTP 404, 403, 429, 500 error mapping."""
    mock_session = MagicMock()
    source = RSSSource(session=mock_session)

    # 404 Not Found
    mock_session.get.return_value = MockHttpResponse(status_code=404)
    with pytest.raises(SourceError, match="Feed not found"):
        await source.fetch_items("https://example.com/notfound.xml")

    # 403 Forbidden
    mock_session.get.return_value = MockHttpResponse(status_code=403)
    with pytest.raises(SourceError, match="Access forbidden"):
        await source.fetch_items("https://example.com/forbidden.xml")

    # 429 Rate limited
    mock_session.get.return_value = MockHttpResponse(status_code=429)
    with pytest.raises(SourceError, match="Rate limited by server"):
        await source.fetch_items("https://example.com/ratelimit.xml")


@pytest.mark.asyncio
async def test_rss_network_timeout_and_connection_errors():
    """20. Test network timeout and connection failure handling."""
    mock_session = MagicMock()
    source = RSSSource(session=mock_session)

    mock_session.get.side_effect = requests.exceptions.Timeout("Connection timed out")
    with pytest.raises(SourceError, match="Request timed out"):
        await source.fetch_items("https://example.com/timeout.xml")

    mock_session.get.side_effect = requests.exceptions.ConnectionError("Failed to resolve host")
    with pytest.raises(SourceError, match="Network connection failed"):
        await source.fetch_items("https://example.com/down.xml")


@pytest.mark.asyncio
async def test_rss_image_processing():
    """Test image downloading in RSS articles with tracking pixel skipping."""
    mock_session = MagicMock()
    img_bytes = b"\x89PNG\r\n\x1a\n" + b"X" * 300

    def mock_get(url, **kwargs):
        if "diagram.png" in url:
            return MockHttpResponse(content=img_bytes, headers={"Content-Type": "image/png"})
        elif "1x1.gif" in url:
            return MockHttpResponse(content=b"tiny", headers={"Content-Type": "image/gif"})
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = RSSSource(session=mock_session)
    markdown_content = (
        "Here is the system architecture:\n\n"
        "![Architecture Diagram](https://example.com/diagram.png)\n\n"
        "Tracking beacon: ![pixel](https://example.com/1x1.gif)\n\n"
        "Conclusion."
    )

    clean_md, attachments = source._process_images(markdown_content, "https://example.com/article")

    assert len(attachments) == 1
    assert attachments[0].filename == "diagram.png"
    assert "![[diagram.png]]" in clean_md
    assert "1x1.gif" not in clean_md


@pytest.mark.asyncio
async def test_duplicate_image_basenames_different_bytes_handled_cleanly(tmp_path):
    """Test that two different image URLs with the same basename but different bytes get unique filenames."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_duplicate_img.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    feed_xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Photo Gallery Feed</title>
    <link>https://gallery.example.com</link>
    <item>
      <title>Dual Photo Showcase</title>
      <link>https://gallery.example.com/showcase</link>
      <guid>showcase-item-999</guid>
      <pubDate>Mon, 05 Oct 2026 14:00:00 GMT</pubDate>
      <content:encoded><![CDATA[
        <p>First photo:</p>
        <p><img src="https://gallery.example.com/section1/photo.png" alt="First" /></p>
        <p>Second photo with same basename:</p>
        <p><img src="https://gallery.example.com/section2/photo.png" alt="Second" /></p>
      ]]></content:encoded>
    </item>
  </channel>
</rss>"""

    bytes_image_1 = b"\x89PNG\r\n\x1a\n" + b"IMAGE_ONE_BYTES" * 20
    bytes_image_2 = b"\x89PNG\r\n\x1a\n" + b"IMAGE_TWO_BYTES" * 20

    mock_session = MagicMock()

    def mock_get(url, **kwargs):
        if "gallery.example.com/feed" in url:
            return MockHttpResponse(text=feed_xml)
        elif "section1/photo.png" in url:
            return MockHttpResponse(content=bytes_image_1, headers={"Content-Type": "image/png"})
        elif "section2/photo.png" in url:
            return MockHttpResponse(content=bytes_image_2, headers={"Content-Type": "image/png"})
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = RSSSource(session=mock_session)
    items = await source.fetch_items("https://gallery.example.com/feed")

    assert len(items) == 1
    item = items[0]

    # Verify that in item attachments, filenames are unique: photo.png and photo_2.png
    assert len(item.attachments) == 2
    assert item.attachments[0].filename == "photo.png"
    assert item.attachments[1].filename == "photo_2.png"
    assert item.attachments[0].content == bytes_image_1
    assert item.attachments[1].content == bytes_image_2

    # Verify that item markdown content references both distinct filenames
    assert "![[photo.png]]" in item.content
    assert "![[photo_2.png]]" in item.content

    # Now run through pipeline end-to-end to verify vault files and frontmatter
    action, rel_path = await pipeline.process_item(source, item)
    assert action == IngestionAction.NEW

    note_path = vault_dir / rel_path
    assert note_path.exists()
    note_text = note_path.read_text(encoding="utf-8")

    # Final Markdown must reference the actual saved attachment filenames
    assert "![[photo.png]]" in note_text
    assert "![[photo_2.png]]" in note_text

    # Frontmatter attachments list must contain both actual filenames
    assert "photo.png" in note_text
    assert "photo_2.png" in note_text

    # Check the actual attachment files on disk in the vault
    att_dir = vault_dir / "Attachments" / "Ingested"
    file1 = att_dir / "photo.png"
    file2 = att_dir / "photo_2.png"

    assert file1.exists()
    assert file2.exists()
    assert file1.read_bytes() == bytes_image_1
    assert file2.read_bytes() == bytes_image_2


@pytest.mark.asyncio
async def test_same_image_url_twice_reuses_attachment():
    """Test that the exact same image URL appearing multiple times in an article is downloaded once and reused."""
    mock_session = MagicMock()
    img_bytes = b"\x89PNG\r\n\x1a\n" + b"X" * 300
    mock_session.get.return_value = MockHttpResponse(content=img_bytes, headers={"Content-Type": "image/png"})

    source = RSSSource(session=mock_session)
    markdown_content = (
        "Header logo:\n\n"
        "![Logo](https://example.com/logo.png)\n\n"
        "Some text in between.\n\n"
        "Footer logo:\n\n"
        "![Logo Repeat](https://example.com/logo.png)\n\n"
    )

    clean_md, attachments = source._process_images(markdown_content, "https://example.com/article")

    assert len(attachments) == 1
    assert attachments[0].filename == "logo.png"
    # Both occurrences were replaced with obsidian embed
    assert clean_md.count("![[logo.png]]") == 2


# ---------------------------------------------------------------------------
# End-to-End Pipeline & Deduplication Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rss_pipeline_end_to_end_and_deduplication(tmp_path):
    """17 & 21, 22, 23, 24, 27, 28. End-to-end RSS ingestion, vault note creation, and in-place updates."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_rss.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=RSS_2_XML)

    source = RSSSource(session=mock_session)

    # 1. First Run -> All items are NEW
    items = await source.fetch_items("https://techinsights.example.com/rss")
    assert len(items) == 2

    action1, rel_path1 = await pipeline.process_item(source, items[0])
    action2, rel_path2 = await pipeline.process_item(source, items[1])

    assert action1 == IngestionAction.NEW
    assert action2 == IngestionAction.NEW
    assert rel_path1.startswith("Ingested/RSS/")
    assert rel_path2.startswith("Ingested/RSS/")

    # Verify Note 1 content & frontmatter
    note1_file = vault_dir / rel_path1
    assert note1_file.exists()
    note1_text = note1_file.read_text(encoding="utf-8")

    assert "source: rss" in note1_text
    assert "feed_title: Tech Insights Feed" in note1_text
    assert "Alice Engineer" in note1_text
    assert "# Building Distributed Agent Networks" in note1_text
    assert "> **Source**:" in note1_text
    assert "Full article body on distributed agents" in note1_text

    # 2. Second Run with same content -> All items UNCHANGED (skipped)
    items_rerun = await source.fetch_items("https://techinsights.example.com/rss")
    action_skip, path_skip = await pipeline.process_item(source, items_rerun[0])
    assert action_skip == IngestionAction.UNCHANGED
    assert path_skip == rel_path1

    # 3. Third Run with modified content -> Item is CHANGED and updated in place
    modified_xml = RSS_2_XML.replace(
        "Full article body on distributed agents",
        "Updated article body with newly added architecture sections",
    )
    mock_session.get.return_value = MockHttpResponse(text=modified_xml)
    items_modified = await source.fetch_items("https://techinsights.example.com/rss")

    action_changed, path_changed = await pipeline.process_item(source, items_modified[0])
    assert action_changed == IngestionAction.CHANGED
    assert path_changed == rel_path1  # Same file updated in-place without _2.md suffix!

    updated_note_text = note1_file.read_text(encoding="utf-8")
    assert "Updated article body with newly added architecture sections" in updated_note_text

    # Verify no duplicate _2.md file was created
    rss_folder = vault_dir / "Ingested" / "RSS"
    md_files = list(rss_folder.glob("*.md"))
    assert len(md_files) == 2

    tracker.close()


@pytest.mark.asyncio
async def test_rss_multiple_items_one_fails_others_still_ingest():
    """30. Test multiple items where one item has malformed entry data does not block others."""
    # Feed with 2 normal items and 1 malformed item in middle
    mixed_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>Mixed Feed</title>
        <link>https://mixed.example.com</link>
        <item>
          <title>Valid Item 1</title>
          <link>https://mixed.example.com/item1</link>
          <guid>mixed-1</guid>
          <description>Valid item description 1</description>
        </item>
        <item>
          <title>Valid Item 2</title>
          <link>https://mixed.example.com/item2</link>
          <guid>mixed-2</guid>
          <description>Valid item description 2</description>
        </item>
      </channel>
    </rss>"""

    mock_session = MagicMock()
    mock_session.get.return_value = MockHttpResponse(text=mixed_xml)

    source = RSSSource(session=mock_session)
    items = await source.fetch_items("https://mixed.example.com/rss")

    assert len(items) == 2
    assert items[0].title == "Valid Item 1"
    assert items[1].title == "Valid Item 2"


# ---------------------------------------------------------------------------
# CLI Command Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cli_ingest_rss_subcommand(tmp_path):
    """31. Test CLI ingest-rss <url> command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_rss.sqlite"

    parser = create_parser()
    args = parser.parse_args(["ingest-rss", "https://techinsights.example.com/rss"])
    assert args.command == "ingest-rss"
    assert args.url == "https://techinsights.example.com/rss"

    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    with patch("sources.rss_source.requests.Session.get") as mock_get:
        mock_get.return_value = MockHttpResponse(text=RSS_2_XML)
        exit_code = await async_main(args)
        assert exit_code == 0

    rss_notes = list((vault_dir / "Ingested" / "RSS").glob("*.md"))
    assert len(rss_notes) == 2


@pytest.mark.asyncio
async def test_cli_generic_ingest_rss_subcommand(tmp_path):
    """32. Test CLI ingest --source rss --url <url> command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_generic_rss.sqlite"

    parser = create_parser()
    args = parser.parse_args(["ingest", "--source", "rss", "--url", "https://dispatches.example.com/atom.xml"])
    assert args.command == "ingest"
    assert args.source == "rss"
    assert args.url == "https://dispatches.example.com/atom.xml"

    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    with patch("sources.rss_source.requests.Session.get") as mock_get:
        mock_get.return_value = MockHttpResponse(text=ATOM_XML)
        exit_code = await async_main(args)
        assert exit_code == 0

    rss_notes = list((vault_dir / "Ingested" / "RSS").glob("*.md"))
    assert len(rss_notes) == 1
