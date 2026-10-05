"""Comprehensive unit and integration tests for the Notion source connector."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from notion_client.errors import APIErrorCode, APIResponseError, RequestTimeoutError

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import Attachment, SourceItem
from sources.base import SourceRegistry
from sources.notion_source import (
    NotionSource,
    extract_page_date,
    extract_page_tags,
    extract_page_title,
    map_notion_error,
    render_rich_text,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Fixture Helpers
# ---------------------------------------------------------------------------

def make_page_mock(
    page_id: str = "802ec8c6-2dd5-4dd6-9e8c-8f4f2c00a40d",
    title: str = "Quarterly Strategy Review",
    created_time: str = "2026-09-22T10:00:00.000Z",
    last_edited_time: str = "2026-09-22T12:00:00.000Z",
    author_name: str = "Elena Rostova",
    url: str = "https://notion.so/802ec8c62dd54dd69e8c8f4f2c00a40d",
    tags: list[str] | None = None,
) -> dict:
    """Create a realistic Notion page API response dict."""
    multi_select = [{"name": t} for t in (tags or ["strategy", "planning"])]
    return {
        "object": "page",
        "id": page_id,
        "created_time": created_time,
        "last_edited_time": last_edited_time,
        "created_by": {"object": "user", "name": author_name},
        "url": url,
        "properties": {
            "Name": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {"content": title},
                        "plain_text": title,
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                        },
                    }
                ],
            },
            "Tags": {
                "id": "tags",
                "type": "multi_select",
                "multi_select": multi_select,
            },
        },
    }


class MockHttpResponse:
    """Mock requests.Response for image downloads."""

    def __init__(
        self,
        content: bytes = b"default_content",
        status_code: int = 200,
        headers: dict | None = None,
    ):
        self.content = content
        self.status_code = status_code
        self.headers = headers or {"Content-Type": "image/png"}


# ---------------------------------------------------------------------------
# 1. Connector Registration
# ---------------------------------------------------------------------------

def test_notion_source_registration():
    """1. Test that NotionSource registers as 'notion' in SourceRegistry."""
    sources = SourceRegistry.list_sources()
    assert "notion" in sources
    assert sources["notion"] == "NotionSource"

    source = NotionSource(token="secret_test_token_123")
    assert source.source_type == "notion"
    assert source.display_name == "Notion"


# ---------------------------------------------------------------------------
# 2. Missing Token
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_token_raises_source_error(monkeypatch):
    """2. Test that missing token raises descriptive SourceError without crashing."""
    monkeypatch.delenv("NOTION_TOKEN", raising=False)
    monkeypatch.delenv("NOTION_API_KEY", raising=False)

    source = NotionSource(token=None)
    with pytest.raises(SourceError, match="Notion integration token is missing"):
        _ = source.client

    with pytest.raises(SourceError, match="Notion integration token is missing"):
        await source.fetch_items()


# ---------------------------------------------------------------------------
# 3. Successful Page Discovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_page_discovery():
    """3. Test successful discovery of accessible Notion pages via search."""
    mock_client = MagicMock()
    page1 = make_page_mock(page_id="page-1", title="First Document")
    page2 = make_page_mock(page_id="page-2", title="Second Document")

    mock_client.search.return_value = {
        "results": [page1, page2],
        "has_more": False,
        "next_cursor": None,
    }
    mock_client.blocks.children.list.return_value = {
        "results": [
            {
                "id": "block-1",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "plain_text": "Sample content here."}]
                },
            }
        ],
        "has_more": False,
    }

    source = NotionSource(client=mock_client)
    items = await source.fetch_items()

    assert len(items) == 2
    assert items[0].title == "First Document"
    assert items[0].source_id == "notion:page-1"
    assert items[1].title == "Second Document"
    assert items[1].source_id == "notion:page-2"


# ---------------------------------------------------------------------------
# 4. Pagination of Page Discovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pagination_of_page_discovery():
    """4. Test that search pagination retrieves all pages across multiple batches."""
    mock_client = MagicMock()
    page1 = make_page_mock(page_id="page-batch-1", title="Page Batch 1")
    page2 = make_page_mock(page_id="page-batch-2", title="Page Batch 2")

    def mock_search(**kwargs):
        if kwargs.get("start_cursor") == "cursor_token_page_2":
            return {"results": [page2], "has_more": False, "next_cursor": None}
        return {"results": [page1], "has_more": True, "next_cursor": "cursor_token_page_2"}

    mock_client.search.side_effect = mock_search
    mock_client.blocks.children.list.return_value = {"results": [], "has_more": False}

    source = NotionSource(client=mock_client)
    items = await source.fetch_items()

    assert len(items) == 2
    assert items[0].title == "Page Batch 1"
    assert items[1].title == "Page Batch 2"
    assert mock_client.search.call_count == 2


# ---------------------------------------------------------------------------
# 5. Page Title Extraction
# ---------------------------------------------------------------------------

def test_page_title_extraction():
    """5. Test title extraction across Name, title property, and fallbacks."""
    # Standard Name property
    p1 = make_page_mock(title="Document Alpha")
    assert extract_page_title(p1) == "Document Alpha"

    # Property named 'title' instead of 'Name'
    p2 = {
        "properties": {
            "title": {
                "type": "title",
                "title": [{"plain_text": "Document Beta"}],
            }
        }
    }
    assert extract_page_title(p2) == "Document Beta"

    # Fallback to direct title field
    p3 = {"title": "Direct Title"}
    assert extract_page_title(p3) == "Direct Title"

    # Empty fallback
    p4 = {"properties": {}}
    assert extract_page_title(p4) == "Untitled Notion Page"


# ---------------------------------------------------------------------------
# 6. Page Metadata and Frontmatter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_page_metadata_and_frontmatter():
    """6. Test extraction and formatting of YAML frontmatter metadata."""
    source = NotionSource(token="fake_token")
    item = SourceItem(
        source_type="notion",
        source_id="notion:page-uuid-1234",
        title="Architecture Roadmap",
        content="Overview of upcoming platform changes.",
        source_url="https://notion.so/roadmap-uuid",
        date="2026-09-22",
        author="Elena Rostova",
        tags=["ingested", "notion", "roadmap", "architecture"],
        summary="Overview of platform changes.",
        extra_metadata={
            "page_id": "page-uuid-1234",
            "last_edited_time": "2026-09-22T14:30:00.000Z",
            "word_count": 5,
        },
    )

    note = await source.convert_to_markdown(item)

    assert note.source == "notion"
    assert note.title == "Architecture Roadmap"
    assert "ingested" in note.tags
    assert "notion" in note.tags
    assert "roadmap" in note.tags
    assert note.author == "Elena Rostova"
    assert note.extra_metadata["page_id"] == "page-uuid-1234"
    assert note.extra_metadata["last_edited_time"] == "2026-09-22T14:30:00.000Z"
    assert note.extra_metadata["word_count"] == 5

    # Check Markdown body structure
    assert "# Architecture Roadmap" in note.body
    assert "> **Source**: [Notion](https://notion.so/roadmap-uuid)" in note.body
    assert "**Author**: Elena Rostova" in note.body
    assert "**Last Edited**: 2026-09-22" in note.body


# ---------------------------------------------------------------------------
# 7. Paragraph Conversion
# ---------------------------------------------------------------------------

def test_paragraph_conversion():
    """7. Test paragraph block conversion."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "b1",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    {"type": "text", "plain_text": "This is a paragraph with simple text."}
                ]
            },
        }
    ]
    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "This is a paragraph with simple text." in md
    assert len(atts) == 0


# ---------------------------------------------------------------------------
# 8. Headings Conversion
# ---------------------------------------------------------------------------

def test_headings_conversion():
    """8. Test heading_1, heading_2, and heading_3 blocks."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "h1",
            "type": "heading_1",
            "heading_1": {"rich_text": [{"type": "text", "plain_text": "Main Heading"}]},
        },
        {
            "id": "h2",
            "type": "heading_2",
            "heading_2": {"rich_text": [{"type": "text", "plain_text": "Sub Heading"}]},
        },
        {
            "id": "h3",
            "type": "heading_3",
            "heading_3": {"rich_text": [{"type": "text", "plain_text": "Detail Heading"}]},
        },
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "# Main Heading" in md
    assert "## Sub Heading" in md
    assert "### Detail Heading" in md


# ---------------------------------------------------------------------------
# 9. Bullet Lists Conversion
# ---------------------------------------------------------------------------

def test_bullet_lists_conversion():
    """9. Test bulleted_list_item block conversion."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "b1",
            "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": [{"type": "text", "plain_text": "Item A"}]},
        },
        {
            "id": "b2",
            "type": "bulleted_list_item",
            "bulleted_list_item": {"rich_text": [{"type": "text", "plain_text": "Item B"}]},
        },
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "- Item A" in md
    assert "- Item B" in md


# ---------------------------------------------------------------------------
# 10. Numbered Lists Conversion
# ---------------------------------------------------------------------------

def test_numbered_lists_conversion():
    """10. Test numbered_list_item block conversion with sequential numbering."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "n1",
            "type": "numbered_list_item",
            "numbered_list_item": {"rich_text": [{"type": "text", "plain_text": "First step"}]},
        },
        {
            "id": "n2",
            "type": "numbered_list_item",
            "numbered_list_item": {"rich_text": [{"type": "text", "plain_text": "Second step"}]},
        },
        {
            "id": "p1",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "plain_text": "Interlude text"}]},
        },
        {
            "id": "n3",
            "type": "numbered_list_item",
            "numbered_list_item": {"rich_text": [{"type": "text", "plain_text": "Restarted step"}]},
        },
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "1. First step" in md
    assert "2. Second step" in md
    assert "Interlude text" in md
    # Number resets after paragraph
    assert "1. Restarted step" in md


# ---------------------------------------------------------------------------
# 11. Todo / Checklist Conversion
# ---------------------------------------------------------------------------

def test_todo_checklist_conversion():
    """11. Test to_do blocks with checked and unchecked states."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "t1",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "plain_text": "Pending assignment"}],
                "checked": False,
            },
        },
        {
            "id": "t2",
            "type": "to_do",
            "to_do": {
                "rich_text": [{"type": "text", "plain_text": "Completed task"}],
                "checked": True,
            },
        },
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "- [ ] Pending assignment" in md
    assert "- [x] Completed task" in md


# ---------------------------------------------------------------------------
# 12. Toggles Conversion
# ---------------------------------------------------------------------------

def test_toggles_conversion():
    """12. Test toggle blocks convert to pure Markdown (### Title) without raw HTML."""
    mock_client = MagicMock()
    mock_client.blocks.children.list.return_value = {
        "results": [
            {
                "id": "child-p1",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "plain_text": "Hidden toggle content here."}]
                },
            }
        ],
        "has_more": False,
    }

    source = NotionSource(client=mock_client)
    blocks = [
        {
            "id": "toggle-1",
            "type": "toggle",
            "has_children": True,
            "toggle": {
                "rich_text": [{"type": "text", "plain_text": "Advanced Options"}],
            },
        }
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})

    assert "### Advanced Options" in md
    assert "Hidden toggle content here." in md
    assert "<details>" not in md
    assert "<summary>" not in md


# ---------------------------------------------------------------------------
# 13. Quotes Conversion
# ---------------------------------------------------------------------------

def test_quotes_conversion():
    """13. Test quote block conversion with multi-line support."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "q1",
            "type": "quote",
            "quote": {
                "rich_text": [
                    {"type": "text", "plain_text": "Design is how it works.\nNot just how it looks."}
                ]
            },
        }
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "> Design is how it works." in md
    assert "> Not just how it looks." in md


# ---------------------------------------------------------------------------
# 14. Callouts Conversion
# ---------------------------------------------------------------------------

def test_callouts_conversion():
    """14. Test callout block conversion with emoji icon preservation and blockquote."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "c1",
            "type": "callout",
            "callout": {
                "icon": {"type": "emoji", "emoji": "💡"},
                "rich_text": [
                    {"type": "text", "plain_text": "Important discovery: performance doubled."}
                ],
            },
        }
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "> 💡 Important discovery: performance doubled." in md
    assert "<div" not in md


# ---------------------------------------------------------------------------
# 15. Code Blocks Conversion
# ---------------------------------------------------------------------------

def test_code_blocks_conversion():
    """15. Test code blocks with language tag preservation."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "cd1",
            "type": "code",
            "code": {
                "language": "python",
                "rich_text": [
                    {"type": "text", "plain_text": "def compute():\n    return 42"}
                ],
            },
        }
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})
    assert "```python" in md
    assert "def compute():\n    return 42" in md
    assert "```" in md


# ---------------------------------------------------------------------------
# 16. Tables Conversion
# ---------------------------------------------------------------------------

def test_tables_conversion():
    """16. Test table block conversion with row alignment and column padding."""
    mock_client = MagicMock()
    mock_client.blocks.children.list.return_value = {
        "results": [
            {
                "id": "tr1",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "plain_text": "Task Name"}],
                        [{"type": "text", "plain_text": "Status"}],
                        [{"type": "text", "plain_text": "Owner"}],
                    ]
                },
            },
            {
                "id": "tr2",
                "type": "table_row",
                "table_row": {
                    "cells": [
                        [{"type": "text", "plain_text": "Build Pipeline"}],
                        [{"type": "text", "plain_text": "Done"}],
                        # Missing 3rd cell -> safe padding
                    ]
                },
            },
        ],
        "has_more": False,
    }

    source = NotionSource(client=mock_client)
    blocks = [
        {
            "id": "tbl1",
            "type": "table",
            "has_children": True,
            "table": {"table_width": 3, "has_column_header": True},
        }
    ]
    md, _ = source._convert_blocks_to_markdown(blocks, set(), {})

    assert "| Task Name | Status | Owner |" in md
    assert "| --- | --- | --- |" in md
    assert "| Build Pipeline | Done |  |" in md


# ---------------------------------------------------------------------------
# 17. Rich Text Formatting
# ---------------------------------------------------------------------------

def test_rich_text_formatting():
    """17. Test rich text bold, italic, code, strikethrough, equation, and link parsing."""
    rich_text = [
        {
            "type": "text",
            "plain_text": "Bold text",
            "annotations": {"bold": True, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": " and ",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": "italic text",
            "annotations": {"bold": False, "italic": True, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": " and ",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": "code snippet",
            "annotations": {"bold": False, "italic": False, "code": True, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": " and ",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": "deleted text",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": True},
        },
        {
            "type": "text",
            "plain_text": " and ",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "text",
            "plain_text": "hyperlink",
            "href": "https://example.com",
            "annotations": {"bold": False, "italic": False, "code": False, "strikethrough": False},
        },
        {
            "type": "equation",
            "equation": {"expression": "a^2 + b^2 = c^2"},
        },
    ]

    result = render_rich_text(rich_text)
    assert "**Bold text**" in result
    assert "*italic text*" in result
    assert "`code snippet`" in result
    assert "~~deleted text~~" in result
    assert "[hyperlink](https://example.com)" in result
    assert "$a^2 + b^2 = c^2$" in result


# ---------------------------------------------------------------------------
# 18. Image / File Handling
# ---------------------------------------------------------------------------

def test_image_and_file_handling():
    """18. Test Notion image block download and Obsidian embed syntax."""
    mock_session = MagicMock()
    img_data = b"\x89PNG\r\n\x1a\n" + b"TEST_BYTES" * 30
    mock_session.get.return_value = MockHttpResponse(content=img_data, headers={"Content-Type": "image/png"})

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "img1",
            "type": "image",
            "image": {
                "type": "file",
                "file": {
                    "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/architecture.png?X-Amz-Security=token"
                },
                "caption": [{"type": "text", "plain_text": "System Architecture"}],
            },
        }
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 1
    assert atts[0].filename == "architecture.png"
    assert atts[0].content == img_data
    assert "![[architecture.png]]" in md


# ---------------------------------------------------------------------------
# 19. Attachment Filename Collision
# ---------------------------------------------------------------------------

def test_attachment_filename_collision():
    """19. Test multiple images with same basename get unique filenames and embeds."""
    mock_session = MagicMock()
    bytes1 = b"\x89PNG\r\n\x1a\n" + b"ONE" * 50
    bytes2 = b"\x89PNG\r\n\x1a\n" + b"TWO" * 50

    def mock_get(url, **kwargs):
        if "section1/photo.png" in url:
            return MockHttpResponse(content=bytes1)
        elif "section2/photo.png" in url:
            return MockHttpResponse(content=bytes2)
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "img1",
            "type": "image",
            "image": {
                "type": "external",
                "external": {"url": "https://example.com/section1/photo.png"},
            },
        },
        {
            "id": "img2",
            "type": "image",
            "image": {
                "type": "external",
                "external": {"url": "https://example.com/section2/photo.png"},
            },
        },
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 2
    assert atts[0].filename == "photo.png"
    assert atts[1].filename == "photo_2.png"
    assert atts[0].content == bytes1
    assert atts[1].content == bytes2
    assert "![[photo.png]]" in md
    assert "![[photo_2.png]]" in md


# ---------------------------------------------------------------------------
# 19b. File Block Downloads & Obsidian Embeds
# ---------------------------------------------------------------------------

def test_hosted_file_block_download():
    """Test hosted Notion file block download and Obsidian embed syntax."""
    mock_session = MagicMock()
    pdf_bytes = b"%PDF-1.4 " + b"MOCK_PDF_DATA" * 20
    mock_session.get.return_value = MockHttpResponse(
        content=pdf_bytes,
        headers={"Content-Type": "application/pdf"},
    )

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "file1",
            "type": "file",
            "file": {
                "type": "file",
                "file": {
                    "url": "https://s3.us-west-2.amazonaws.com/secure.notion-static.com/financial_q3.pdf?X-Amz-Security=token"
                },
                "caption": [{"type": "text", "plain_text": "Q3 Financial Statement"}],
            },
        }
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 1
    assert atts[0].filename == "financial_q3.pdf"
    assert atts[0].content == pdf_bytes
    assert atts[0].mime_type == "application/pdf"
    assert "![[financial_q3.pdf]]" in md


def test_external_file_block_download():
    """Test external Notion file block download and Obsidian embed syntax."""
    mock_session = MagicMock()
    csv_bytes = b"id,name,value\n1,alpha,100\n2,beta,200\n"
    mock_session.get.return_value = MockHttpResponse(
        content=csv_bytes,
        headers={"Content-Type": "text/csv"},
    )

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "file2",
            "type": "file",
            "file": {
                "type": "external",
                "external": {"url": "https://example.com/datasets/metrics.csv"},
            },
        }
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 1
    assert atts[0].filename == "metrics.csv"
    assert atts[0].content == csv_bytes
    assert "![[metrics.csv]]" in md


@pytest.mark.asyncio
async def test_file_attachment_in_markdown_note():
    """Test that downloaded file attachment appears in MarkdownNote.attachments."""
    source = NotionSource(token="dummy")
    att = Attachment(
        filename="report.pdf",
        content=b"%PDF-1.4 report bytes",
        mime_type="application/pdf",
    )
    item = SourceItem(
        source_type="notion",
        source_id="notion:page-with-file",
        title="Page with File",
        content="Here is the report:\n\n![[report.pdf]]",
        source_url="https://notion.so/page-with-file",
        attachments=[att],
    )

    note = await source.convert_to_markdown(item)
    assert "report.pdf" in note.attachments
    assert "![[report.pdf]]" in note.body


def test_file_attachment_filename_collision():
    """Test two different file URLs with same basename produce unique filenames."""
    mock_session = MagicMock()
    bytes1 = b"%PDF-1.4 DOC_ONE" * 30
    bytes2 = b"%PDF-1.4 DOC_TWO" * 30

    def mock_get(url, **kwargs):
        if "server1/report.pdf" in url:
            return MockHttpResponse(content=bytes1, headers={"Content-Type": "application/pdf"})
        elif "server2/report.pdf" in url:
            return MockHttpResponse(content=bytes2, headers={"Content-Type": "application/pdf"})
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "f1",
            "type": "file",
            "file": {
                "type": "external",
                "external": {"url": "https://example.com/server1/report.pdf"},
            },
        },
        {
            "id": "f2",
            "type": "file",
            "file": {
                "type": "external",
                "external": {"url": "https://example.com/server2/report.pdf"},
            },
        },
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 2
    assert atts[0].filename == "report.pdf"
    assert atts[1].filename == "report_2.pdf"
    assert atts[0].content == bytes1
    assert atts[1].content == bytes2
    assert "![[report.pdf]]" in md
    assert "![[report_2.pdf]]" in md


def test_file_without_extension_inferred_from_mime():
    """Test file URL without extension infers extension from Content-Type."""
    mock_session = MagicMock()
    pdf_bytes = b"%PDF-1.4 DATA"
    mock_session.get.return_value = MockHttpResponse(
        content=pdf_bytes,
        headers={"Content-Type": "application/pdf"},
    )

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "f_no_ext",
            "type": "file",
            "file": {
                "type": "external",
                "external": {"url": "https://example.com/api/v1/export_data"},
            },
        }
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 1
    assert atts[0].filename == "export_data.pdf"
    assert "![[export_data.pdf]]" in md


def test_failed_file_download_falls_back_to_markdown_link():
    """Test failed file download falls back to normal Markdown link and does not abort page."""
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.HTTPError("404 Not Found")

    source = NotionSource(token="dummy", session=mock_session)
    blocks = [
        {
            "id": "f_fail",
            "type": "file",
            "file": {
                "type": "external",
                "external": {"url": "https://example.com/files/confidential.docx"},
                "caption": [{"type": "text", "plain_text": "Confidential Specification"}],
            },
        },
        {
            "id": "p_after",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "plain_text": "Subsequent page content continues."}],
            },
        },
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})

    assert len(atts) == 0
    # Preserved as normal Markdown link
    assert "[Confidential Specification](https://example.com/files/confidential.docx)" in md
    # Page did not crash or abort
    assert "Subsequent page content continues." in md


# ---------------------------------------------------------------------------
# 20. Unsupported Block Handling
# ---------------------------------------------------------------------------

def test_unsupported_block_handling():
    """20. Test that unsupported block types fail gracefully without crashing."""
    source = NotionSource(token="dummy")
    blocks = [
        {
            "id": "u1",
            "type": "unsupported_futuristic_block",
            "unsupported_futuristic_block": {},
        },
        {
            "id": "p1",
            "type": "paragraph",
            "paragraph": {"rich_text": [{"type": "text", "plain_text": "Follow-up paragraph"}]},
        },
    ]

    md, atts = source._convert_blocks_to_markdown(blocks, set(), {})
    # Paragraph succeeds despite preceding unsupported block
    assert "Follow-up paragraph" in md


# ---------------------------------------------------------------------------
# 21. Block Pagination
# ---------------------------------------------------------------------------

def test_block_pagination():
    """21. Test block children pagination when has_more=True."""
    mock_client = MagicMock()
    b1 = {
        "id": "b-page-1",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Block Page 1"}]},
    }
    b2 = {
        "id": "b-page-2",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Block Page 2"}]},
    }

    def mock_children(**kwargs):
        if kwargs.get("start_cursor") == "cursor_block_2":
            return {"results": [b2], "has_more": False, "next_cursor": None}
        return {"results": [b1], "has_more": True, "next_cursor": "cursor_block_2"}

    mock_client.blocks.children.list.side_effect = mock_children

    source = NotionSource(client=mock_client)
    blocks = source._get_page_blocks("test-page-id")

    assert len(blocks) == 2
    assert blocks[0]["id"] == "b-page-1"
    assert blocks[1]["id"] == "b-page-2"
    assert mock_client.blocks.children.list.call_count == 2


# ---------------------------------------------------------------------------
# 22. Page URL and Source Attribution
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_page_url_and_source_attribution():
    """22. Test page URL inclusion in frontmatter and body attribution."""
    source = NotionSource(token="dummy")
    item = SourceItem(
        source_type="notion",
        source_id="notion:page-url-test",
        title="URL Attribution Test",
        content="Testing source link.",
        source_url="https://notion.so/workspace/test-page-url-123",
        date="2026-09-22",
        author="Alice",
        tags=["ingested", "notion"],
        extra_metadata={"page_id": "page-url-test"},
    )

    note = await source.convert_to_markdown(item)

    assert note.source_url == "https://notion.so/workspace/test-page-url-123"
    assert "> **Source**: [Notion](https://notion.so/workspace/test-page-url-123)" in note.body


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 23. New Page Ingestion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_new_page_ingestion(tmp_path):
    """23. Test new page ingestion creates Markdown note in Ingested/Notes/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_notion_new.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    mock_client = MagicMock()
    page = make_page_mock(page_id="page-new-1", title="Brand New Page")
    block = {
        "id": "b-new",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Content of brand new page"}]},
    }
    mock_client.search.return_value = {"results": [page], "has_more": False}
    mock_client.blocks.children.list.return_value = {"results": [block], "has_more": False}

    source = NotionSource(client=mock_client)
    items = await source.fetch_items()
    assert len(items) == 1

    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW
    assert rel_path.startswith("Ingested/Notes/")

    note_file = vault_dir / rel_path
    assert note_file.exists()
    assert "Content of brand new page" in note_file.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 24. Unchanged Page Deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unchanged_page_deduplication(tmp_path):
    """24. Test unchanged page deduplication skips writing and produces no duplicate note."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_notion_unchanged.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    mock_client = MagicMock()
    page = make_page_mock(page_id="page-unchanged-1", title="Unchanged Document")
    block = {
        "id": "b-unc",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Static content"}]},
    }
    mock_client.search.return_value = {"results": [page], "has_more": False}
    mock_client.blocks.children.list.return_value = {"results": [block], "has_more": False}

    source = NotionSource(client=mock_client)

    # First run: Ingested as NEW
    items = await source.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source, items[0])
    assert action1 == IngestionAction.NEW

    # Second run: Same content & metadata -> UNCHANGED
    action2, rel_path2 = await pipeline.process_item(source, items[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1

    # Confirm only one file exists
    note_files = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(note_files) == 1


# ---------------------------------------------------------------------------
# 25. Modified Page Update
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_modified_page_updates_existing_note(tmp_path):
    """25. Test modified page updates existing note in place without duplicate note."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_notion_modified.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    mock_client = MagicMock()
    page_v1 = make_page_mock(
        page_id="page-mod-1",
        title="Living Document",
        last_edited_time="2026-09-22T10:00:00.000Z",
    )
    block_v1 = {
        "id": "b-v1",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Draft content v1"}]},
    }
    mock_client.search.return_value = {"results": [page_v1], "has_more": False}
    mock_client.blocks.children.list.return_value = {"results": [block_v1], "has_more": False}

    source = NotionSource(client=mock_client)

    # First run
    items1 = await source.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source, items1[0])
    assert action1 == IngestionAction.NEW

    note_file = vault_dir / rel_path1
    assert "Draft content v1" in note_file.read_text(encoding="utf-8")

    # Second run with modified content and updated timestamp
    page_v2 = make_page_mock(
        page_id="page-mod-1",
        title="Living Document",
        last_edited_time="2026-09-22T15:00:00.000Z",
    )
    block_v2 = {
        "id": "b-v2",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "plain_text": "Final published content v2"}]},
    }
    mock_client.search.return_value = {"results": [page_v2], "has_more": False}
    mock_client.blocks.children.list.return_value = {"results": [block_v2], "has_more": False}

    items2 = await source.fetch_items()
    action2, rel_path2 = await pipeline.process_item(source, items2[0])
    assert action2 == IngestionAction.CHANGED
    assert rel_path2 == rel_path1  # Overwrites exact same relative path

    # Verify content was updated in-place
    updated_text = note_file.read_text(encoding="utf-8")
    assert "Final published content v2" in updated_text
    assert "Draft content v1" not in updated_text

    # Verify no second _2.md note was created
    note_files = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(note_files) == 1



# ---------------------------------------------------------------------------
# 26. Batch Isolation When One Page Fails
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_isolation_when_one_page_fails():
    """26. Test that failure on one page does not abort remaining pages in the batch."""
    mock_client = MagicMock()
    page1 = make_page_mock(page_id="page-fail-1", title="Faulty Page")
    page2 = make_page_mock(page_id="page-ok-2", title="Healthy Page")

    mock_client.search.return_value = {"results": [page1, page2], "has_more": False}

    import httpx
    headers = httpx.Headers()

    def mock_children(**kwargs):
        if kwargs.get("block_id") == "page-fail-1":
            raise APIResponseError(
                code="restricted_resource",
                status=403,
                message="Permission error",
                headers=headers,
                raw_body_text="{}",
            )
        return {
            "results": [
                {
                    "id": "b-ok",
                    "type": "paragraph",
                    "paragraph": {"rich_text": [{"type": "text", "plain_text": "Good content."}]},
                }
            ],
            "has_more": False,
        }

    mock_client.blocks.children.list.side_effect = mock_children

    source = NotionSource(client=mock_client)
    items = await source.fetch_items()

    # Page 1 failed, but Page 2 was processed successfully
    assert len(items) == 1
    assert items[0].title == "Healthy Page"
    assert items[0].source_id == "notion:page-ok-2"


# ---------------------------------------------------------------------------
# 27. API / Auth / Rate-Limit Error Mapping
# ---------------------------------------------------------------------------

def test_api_auth_and_rate_limit_error_mapping():
    """27. Test mapping of Notion API errors to SourceError exceptions."""
    import httpx
    headers = httpx.Headers()

    # 401 Unauthorized
    err_401 = APIResponseError(
        code=APIErrorCode.Unauthorized,
        status=401,
        message="Invalid token",
        headers=headers,
        raw_body_text="{}",
    )
    mapped_401 = map_notion_error(err_401)
    assert isinstance(mapped_401, SourceError)
    assert "authentication failed" in str(mapped_401).lower()

    # 403 Restricted Resource
    err_403 = APIResponseError(
        code=APIErrorCode.RestrictedResource,
        status=403,
        message="Forbidden",
        headers=headers,
        raw_body_text="{}",
    )
    mapped_403 = map_notion_error(err_403)
    assert "access denied" in str(mapped_403).lower()

    # 404 Object Not Found
    err_404 = APIResponseError(
        code=APIErrorCode.ObjectNotFound,
        status=404,
        message="Not found",
        headers=headers,
        raw_body_text="{}",
    )
    mapped_404 = map_notion_error(err_404)
    assert "not found" in str(mapped_404).lower()

    # 429 Rate Limited
    err_429 = APIResponseError(
        code=APIErrorCode.RateLimited,
        status=429,
        message="Slow down",
        headers=headers,
        raw_body_text="{}",
    )
    mapped_429 = map_notion_error(err_429)
    assert "rate limit exceeded" in str(mapped_429).lower()

    # Timeout
    err_to = RequestTimeoutError()
    mapped_to = map_notion_error(err_to)
    assert "timed out" in str(mapped_to).lower()


# ---------------------------------------------------------------------------
# 28. CLI Registration and Commands
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cli_ingest_notion_command(tmp_path):
    """28. Test CLI commands ingest-notion and generic ingest --source notion."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_notion.sqlite"

    parser = create_parser()

    # 1. Test shortcut parser: ingest-notion
    args_shortcut = parser.parse_args(["ingest-notion", "--page-id", "test-page-123"])
    assert args_shortcut.command == "ingest-notion"
    assert args_shortcut.page_id == "test-page-123"

    # 2. Test generic parser: ingest --source notion
    args_generic = parser.parse_args(["ingest", "--source", "notion"])
    assert args_generic.command == "ingest"
    assert args_generic.source == "notion"

    # 3. Test execution with mocked NotionSource
    args_shortcut.vault_path = str(vault_dir)
    args_shortcut.tracker_db = str(tracker_file)
    args_shortcut.verbose = False

    fake_page = make_page_mock(page_id="cli-page-1", title="CLI Ingested Page")
    mock_client = MagicMock()
    mock_client.pages.retrieve.return_value = fake_page
    mock_client.blocks.children.list.return_value = {
        "results": [
            {
                "id": "b-cli",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "plain_text": "CLI content"}]},
            }
        ],
        "has_more": False,
    }

    with patch("sources.notion_source.NotionSource.client", new_callable=lambda: property(lambda self: mock_client)):
        exit_code = await async_main(args_shortcut)
        assert exit_code == 0

    notion_notes = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(notion_notes) == 1
    assert "CLI content" in notion_notes[0].read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_cli_generic_ingest_notion(tmp_path):
    """29. Test generic CLI command ingest --source notion execution."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_generic_notion.sqlite"

    parser = create_parser()
    args = parser.parse_args(["ingest", "--source", "notion"])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False
    args.url = None
    args.pdf_dir = None
    args.mbox = None
    args.feed_url = None
    args.page_id = None

    fake_page = make_page_mock(page_id="cli-gen-1", title="Generic CLI Page")
    mock_client = MagicMock()
    mock_client.search.return_value = {"results": [fake_page], "has_more": False}
    mock_client.blocks.children.list.return_value = {
        "results": [
            {
                "id": "b-gen",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"type": "text", "plain_text": "Generic CLI content"}]},
            }
        ],
        "has_more": False,
    }

    with patch("sources.notion_source.NotionSource.client", new_callable=lambda: property(lambda self: mock_client)):
        exit_code = await async_main(args)
        assert exit_code == 0

    notion_notes = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(notion_notes) == 1
    assert "Generic CLI content" in notion_notes[0].read_text(encoding="utf-8")

