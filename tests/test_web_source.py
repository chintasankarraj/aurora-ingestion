"""Comprehensive tests for Web article source connector."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.web_source import WebSource, is_tracking_pixel_or_icon, normalize_url
from tracker import DeduplicationTracker, IngestionAction


class MockResponse:
    """Mock requests Response object for testing."""

    def __init__(
        self,
        text: str = "",
        content: bytes = b"",
        status_code: int = 200,
        url: str = "",
        headers: dict | None = None,
        reason: str = "OK",
    ) -> None:
        self.text = text
        self.content = content or text.encode("utf-8")
        self.status_code = status_code
        self.url = url
        self.headers = headers or {"content-type": "text/html; charset=utf-8"}
        self.reason = reason


SAMPLE_ARTICLE_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Understanding Vector Databases</title>
    <meta name="author" content="Jane Smith">
    <meta name="date" content="2026-09-18">
    <meta name="description" content="A comprehensive guide to vector databases and similarity search.">
</head>
<body>
    <header>
        <nav><a href="/">Home</a><a href="/about">About</a></nav>
    </header>
    <div class="sidebar ads">
        <p>Sponsored: Buy SuperCloud today!</p>
    </div>
    <main>
        <article>
            <h1>Understanding Vector Databases</h1>
            <p>A vector database is a type of database optimized for storing and querying high-dimensional vector embeddings. Unlike traditional relational databases that match exact values, vector databases use approximate nearest neighbor algorithms.</p>
            <p>Read more on the official <a href="https://example.com/docs">documentation</a> page.</p>

            <h2>How Similarity Search Works</h2>
            <p>Vectors represent data points in a high-dimensional space. Distance metrics include:</p>
            <ul>
                <li>Cosine Similarity: Measures angular distance</li>
                <li>Euclidean Distance: Measures straight-line distance</li>
                <li>Dot Product: Measures magnitude and direction</li>
            </ul>

            <h2>Code Example</h2>
            <pre><code>import chromadb
client = chromadb.Client()
collection = client.create_collection("test")</code></pre>

            <p>Here is the vector architecture diagram:</p>
            <img src="/images/vector-diagram.png" alt="Vector Diagram">
            <img src="https://cdn.example.com/assets/benchmark.jpg" alt="Benchmark Chart">
            <img src="https://tracker.example.com/pixel.gif" alt="Tracking Pixel">
            <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==" alt="Data URI">
        </article>
    </main>
    <footer>
        <p>&copy; 2026 Tech Blog. All rights reserved.</p>
    </footer>
</body>
</html>
"""


def test_web_source_registration():
    src_cls = SourceRegistry.get("web")
    assert src_cls is WebSource
    sources_dict = SourceRegistry.list_sources()
    assert "web" in sources_dict


@pytest.mark.asyncio
async def test_cli_list_sources_shows_web(capsys):
    parser = create_parser()
    args = parser.parse_args(["list-sources"])
    ret = await async_main(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "web (WebSource)" in captured.out


def test_url_normalization():
    # Strip URL fragments
    assert normalize_url("https://example.com/article#section1") == "https://example.com/article"

    # Strip trailing slash from path
    assert normalize_url("https://example.com/article/") == "https://example.com/article"

    # Preserve root slash
    assert normalize_url("https://example.com/") == "https://example.com/"

    # Lowercase scheme and host
    assert normalize_url("HTTPS://EXAMPLE.COM/Blog/Post") == "https://example.com/Blog/Post"

    # Remove default port
    assert normalize_url("https://example.com:443/article") == "https://example.com/article"
    assert normalize_url("http://example.com:80/article") == "http://example.com/article"

    # Keep query parameters
    assert normalize_url("https://example.com/search?q=vectors&lang=en") == "https://example.com/search?q=vectors&lang=en"

    # Invalid schemes raise SourceError
    with pytest.raises(SourceError, match="Unsupported URL scheme"):
        normalize_url("ftp://example.com/file.txt")

    with pytest.raises(SourceError, match="missing an HTTP/HTTPS scheme"):
        normalize_url("example.com/article")

    with pytest.raises(SourceError):
        normalize_url("")


def test_tracking_pixel_filtering():
    assert is_tracking_pixel_or_icon("https://tracker.example.com/pixel.gif")
    assert is_tracking_pixel_or_icon("https://example.com/1x1.png")
    assert is_tracking_pixel_or_icon("https://stats.wp.com/b.gif?v=wp")
    assert is_tracking_pixel_or_icon("https://example.com/img.jpg", alt_text="tracking pixel")
    assert not is_tracking_pixel_or_icon("https://example.com/images/architecture.png")


@pytest.mark.asyncio
async def test_html_content_extraction_and_metadata():
    page_url = "https://example.com/vector-databases"

    mock_session = MagicMock()
    mock_session.headers = {}

    def mock_get(url, **kwargs):
        if url == page_url:
            return MockResponse(text=SAMPLE_ARTICLE_HTML, url=page_url)
        elif "vector-diagram.png" in url:
            return MockResponse(
                content=b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRdiagram_bytes",
                headers={"content-type": "image/png"},
                url=url,
            )
        elif "benchmark.jpg" in url:
            return MockResponse(
                content=b"\xff\xd8\xff\xe0\x00\x10JFIFbenchmark_bytes",
                headers={"content-type": "image/jpeg"},
                url=url,
            )
        return MockResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = WebSource(session=mock_session)
    items = await source.fetch_items(url=page_url)

    assert len(items) == 1
    item = items[0]

    # Verify metadata
    assert item.source_type == "web"
    assert item.source_id == f"web:{page_url}"
    assert item.source_url == page_url
    assert item.title == "Understanding Vector Databases"
    assert item.author == "Jane Smith"
    assert item.date == "2026-09-18"
    assert "ingested" in item.tags
    assert "web" in item.tags
    assert item.summary == "A comprehensive guide to vector databases and similarity search."

    # Verify content preservation
    assert "## How Similarity Search Works" in item.content
    assert "Cosine Similarity" in item.content
    assert "Euclidean Distance" in item.content
    assert "import chromadb" in item.content
    assert "[documentation](https://example.com/docs)" in item.content

    # Navigation, sidebar ads, and footer should not be present
    assert "Buy SuperCloud today!" not in item.content
    assert "Home" not in item.content
    assert "&copy; 2026 Tech Blog" not in item.content

    # Images: 2 valid images downloaded, tracking pixel and data URI skipped
    assert len(item.attachments) == 2
    att_names = [a.filename for a in item.attachments]
    assert "vector-diagram.png" in att_names
    assert "benchmark.jpg" in att_names

    # Check Obsidian embed replacements
    assert "![[vector-diagram.png]]" in item.content
    assert "![[benchmark.jpg]]" in item.content
    assert "pixel.gif" not in item.content


@pytest.mark.asyncio
async def test_image_download_failure_does_not_fail_note():
    page_url = "https://example.com/article-with-broken-image"
    html = """
    <html>
    <head><title>Article With Broken Image</title></head>
    <body>
        <article>
            <h1>Article With Broken Image</h1>
            <p>Main content of the article.</p>
            <img src="https://example.com/missing.png" alt="Missing">
        </article>
    </body>
    </html>
    """

    mock_session = MagicMock()
    mock_session.headers = {}

    def mock_get(url, **kwargs):
        if url == page_url:
            return MockResponse(text=html, url=page_url)
        # Fail image download
        raise requests.exceptions.ConnectionError("Could not connect to image server")

    mock_session.get.side_effect = mock_get

    source = WebSource(session=mock_session)
    items = await source.fetch_items(url=page_url)

    # Note creation succeeds even if image download fails
    assert len(items) == 1
    assert items[0].title == "Article With Broken Image"
    assert "Main content of the article." in items[0].content


@pytest.mark.asyncio
async def test_web_image_download_positive_flow_end_to_end(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    page_url = "https://example.com/visual-guide"
    image_url = "https://example.com/images/neural-net.png"
    fake_image_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRpositive_flow_image_bytes"

    html = """
    <html>
    <head>
        <title>Visual Guide to ML</title>
        <meta name="date" content="2026-09-25">
        <meta name="author" content="Alex Doe">
    </head>
    <body>
        <article>
            <h1>Visual Guide to ML</h1>
            <p>Below is the architectural diagram:</p>
            <img src="/images/neural-net.png" alt="Neural Network Architecture">
        </article>
    </body>
    </html>
    """

    mock_session = MagicMock()
    mock_session.headers = {}

    def mock_get(url, **kwargs):
        if url == page_url:
            return MockResponse(text=html, url=page_url)
        elif url == image_url:
            return MockResponse(
                content=fake_image_bytes,
                headers={"content-type": "image/png"},
                url=image_url,
            )
        return MockResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = WebSource(session=mock_session)
    items = await source.fetch_items(url=page_url)

    assert len(items) == 1
    item = items[0]
    assert len(item.attachments) == 1
    assert item.attachments[0].filename == "neural-net.png"

    # Process through pipeline to save into vault
    action, rel_path = await pipeline.process_item(source, item)
    assert action == IngestionAction.NEW
    assert rel_path is not None

    # Verify attachment file saved on disk under Attachments/Ingested/
    saved_attachment_path = vault_dir / "Attachments" / "Ingested" / "neural-net.png"
    assert saved_attachment_path.exists()
    assert saved_attachment_path.read_bytes() == fake_image_bytes

    # Verify Markdown note saved on disk in Ingested/Web/
    note_path = vault_dir / rel_path
    assert note_path.exists()
    note_content = note_path.read_text(encoding="utf-8")

    # Frontmatter has attachments entry
    assert "attachments:" in note_content
    assert "- neural-net.png" in note_content

    # Body contains Obsidian embed reference
    assert "![[neural-net.png]]" in note_content

    # Original remote URL was replaced, not left raw
    assert "https://example.com/images/neural-net.png" not in note_content

    tracker.close()


@pytest.mark.asyncio
async def test_web_pipeline_deduplication_and_update(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    page_url = "https://example.com/tech-article"

    html_v1 = """
    <html>
    <head><title>Modern AI Architectures</title><meta name="date" content="2026-09-22"></head>
    <body>
        <article>
            <h1>Modern AI Architectures</h1>
            <p>Version 1 of the article text.</p>
        </article>
    </body>
    </html>
    """

    mock_session = MagicMock()
    mock_session.headers = {}
    mock_session.get.return_value = MockResponse(text=html_v1, url=page_url)

    source = WebSource(session=mock_session)

    # 1. First Ingestion -> Action NEW
    items1 = await source.fetch_items(url=page_url)
    action1, rel_path1 = await pipeline.process_item(source, items1[0])

    assert action1 == IngestionAction.NEW
    assert rel_path1 is not None

    note_path = vault_dir / rel_path1
    assert note_path.exists()
    assert "Ingested/Web" in rel_path1 or "Ingested\\Web" in str(note_path)

    note_content1 = note_path.read_text(encoding="utf-8")
    assert "source: web" in note_content1
    assert f"source_url: {page_url}" in note_content1
    assert "Version 1 of the article text." in note_content1
    assert f"> **Source**: [Web]({page_url})" in note_content1

    # 2. Re-ingesting unchanged URL -> UNCHANGED (skip)
    items_same = await source.fetch_items(url=page_url)
    action2, rel_path2 = await pipeline.process_item(source, items_same[0])

    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1

    # 3. Same URL with updated content -> CHANGED (overwrites note in-place)
    html_v2 = """
    <html>
    <head><title>Modern AI Architectures</title><meta name="date" content="2026-09-22"></head>
    <body>
        <article>
            <h1>Modern AI Architectures</h1>
            <p>Version 2 of the article text with updated benchmarks.</p>
        </article>
    </body>
    </html>
    """
    mock_session.get.return_value = MockResponse(text=html_v2, url=page_url)

    items_v2 = await source.fetch_items(url=page_url)
    action3, rel_path3 = await pipeline.process_item(source, items_v2[0])

    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1  # Overwrites the SAME file

    note_content2 = note_path.read_text(encoding="utf-8")
    assert "Version 2 of the article text with updated benchmarks." in note_content2
    assert "Version 1 of the article text." not in note_content2

    tracker.close()


@pytest.mark.asyncio
async def test_web_http_error_handling():
    mock_session = MagicMock()
    mock_session.headers = {}

    source = WebSource(session=mock_session)

    # 404 Not Found
    mock_session.get.return_value = MockResponse(status_code=404, url="https://example.com/missing")
    with pytest.raises(SourceError, match="HTTP 404"):
        await source.fetch_items(url="https://example.com/missing")

    # 403 Forbidden
    mock_session.get.return_value = MockResponse(status_code=403, url="https://example.com/forbidden")
    with pytest.raises(SourceError, match="HTTP 403"):
        await source.fetch_items(url="https://example.com/forbidden")

    # 429 Rate Limited
    mock_session.get.return_value = MockResponse(status_code=429, url="https://example.com/rate-limited")
    with pytest.raises(SourceError, match="HTTP 429"):
        await source.fetch_items(url="https://example.com/rate-limited")

    # Timeout
    mock_session.get.side_effect = requests.exceptions.Timeout("Connection timed out")
    with pytest.raises(SourceError, match="timed out"):
        await source.fetch_items(url="https://example.com/timeout")

    # Connection Error
    mock_session.get.side_effect = requests.exceptions.ConnectionError("DNS failure")
    with pytest.raises(SourceError, match="Network connection failed"):
        await source.fetch_items(url="https://example.com/conn-error")

    # Empty Page
    mock_session.get.side_effect = None
    mock_session.get.return_value = MockResponse(text="   ", url="https://example.com/empty")
    with pytest.raises(SourceError, match="empty content"):
        await source.fetch_items(url="https://example.com/empty")


@pytest.mark.asyncio
async def test_cli_ingest_web_command(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker.sqlite"

    target_url = "https://example.com/cli-test"
    html = """
    <html>
    <head><title>CLI Test Page</title></head>
    <body>
        <article>
            <h1>CLI Test Page</h1>
            <p>Testing CLI ingest-web shortcut.</p>
        </article>
    </body>
    </html>
    """

    with patch("requests.Session.get") as mock_get:
        mock_get.return_value = MockResponse(text=html, url=target_url)

        parser = create_parser()
        args = parser.parse_args([
            "--vault-path", str(vault_dir),
            "--tracker-db", str(tracker_path),
            "ingest-web",
            target_url,
        ])

        ret = await async_main(args)
        assert ret == 0

        captured = capsys.readouterr()
        assert "Ingestion completed for 'web'" in captured.out

        # Verify Markdown note in Ingested/Web/
        web_notes = list((vault_dir / "Ingested" / "Web").glob("*.md"))
        assert len(web_notes) == 1
        assert "Testing CLI ingest-web shortcut." in web_notes[0].read_text(encoding="utf-8")
