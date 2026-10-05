"""Notion source connector for Aurora External Data Ingestion Pipeline.

Connects to Notion via the official notion-client library, discovers pages
accessible to the configured integration token, converts Notion block trees
into clean Markdown notes, downloads image/file attachments, and saves notes
under Ingested/Notes/ with YAML frontmatter.
"""

from __future__ import annotations

import logging
import mimetypes
import os
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import requests
from notion_client import Client
from notion_client.errors import APIErrorCode, APIResponseError, RequestTimeoutError

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

FORBIDDEN_CHARS_PATTERN = re.compile(r'[?:*"<>|/\\]')


def map_notion_error(exc: Exception) -> SourceError:
    """Map Notion client exceptions to descriptive SourceError exceptions without exposing tokens."""
    if isinstance(exc, SourceError):
        return exc

    if isinstance(exc, APIResponseError):
        code = getattr(exc, "code", "")
        status = getattr(exc, "status", None)
        msg = getattr(exc, "message", None) or str(exc)

        if code == APIErrorCode.Unauthorized or code == "unauthorized" or status == 401:
            return SourceError("Notion authentication failed: Invalid or expired integration token.")
        if code == APIErrorCode.RestrictedResource or code == "restricted_resource" or status == 403:
            return SourceError("Notion access denied: Integration does not have permission to access this resource.")
        if code == APIErrorCode.ObjectNotFound or code == "object_not_found" or status == 404:
            return SourceError("Notion resource not found: The specified page or database does not exist.")
        if code == APIErrorCode.RateLimited or code == "rate_limited" or status == 429:
            return SourceError("Notion API rate limit exceeded: Please wait before retrying.")
        return SourceError(f"Notion API error ({code}): {msg}")

    if isinstance(exc, RequestTimeoutError) or isinstance(exc, requests.exceptions.Timeout):
        return SourceError("Notion request timed out.")

    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError(f"Network connection failed connecting to Notion: {exc}")

    return SourceError(f"Notion connector error: {exc}")


def render_rich_text(rich_text_list: Optional[List[Dict[str, Any]]]) -> str:
    """Render a Notion rich text array to Markdown with formatting."""
    if not rich_text_list or not isinstance(rich_text_list, list):
        return ""

    parts: List[str] = []
    for item in rich_text_list:
        if not isinstance(item, dict):
            continue

        item_type = item.get("type", "text")
        if item_type == "equation":
            expr = item.get("equation", {}).get("expression") or item.get("plain_text", "")
            parts.append(f"${expr}$")
            continue

        raw_text = item.get("plain_text") or item.get("text", {}).get("content", "")
        if not raw_text:
            continue

        annotations = item.get("annotations", {})
        href = item.get("href") or (item.get("text", {}).get("link") or {}).get("url")

        is_code = annotations.get("code", False)
        is_bold = annotations.get("bold", False)
        is_italic = annotations.get("italic", False)
        is_strike = annotations.get("strikethrough", False)

        formatted = raw_text

        # Format code first
        if is_code:
            formatted = f"`{formatted}`"
        else:
            # Preserve leading and trailing whitespace outside bold/italic/strikethrough markers
            l_ws = len(formatted) - len(formatted.lstrip())
            r_ws = len(formatted) - len(formatted.rstrip())
            core = formatted.strip()

            if core:
                if is_bold:
                    core = f"**{core}**"
                if is_italic:
                    core = f"*{core}*"
                if is_strike:
                    core = f"~~{core}~~"
                formatted = f"{' ' * l_ws}{core}{' ' * r_ws}"

        # Link wrapping
        if href:
            formatted = f"[{formatted}]({href})"

        parts.append(formatted)

    return "".join(parts)


def extract_page_title(page: Dict[str, Any]) -> str:
    """Extract page title from Notion page object properties."""
    props = page.get("properties", {})
    if isinstance(props, dict):
        for prop_data in props.values():
            if isinstance(prop_data, dict) and prop_data.get("type") == "title":
                title_list = prop_data.get("title", [])
                if isinstance(title_list, list):
                    text = "".join(
                        item.get("plain_text", "")
                        for item in title_list
                        if isinstance(item, dict)
                    ).strip()
                    if text:
                        return text

    # Check for direct title attribute or fallback
    direct_title = page.get("title")
    if direct_title and isinstance(direct_title, str) and direct_title.strip():
        return direct_title.strip()

    return "Untitled Notion Page"


def extract_page_date(page: Dict[str, Any]) -> Tuple[str, str]:
    """Extract date and display date from Notion page."""
    raw_date = page.get("created_time") or page.get("last_edited_time") or ""
    if raw_date and isinstance(raw_date, str):
        try:
            clean_iso = raw_date.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_iso)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d %H:%M")


def extract_page_tags(page: Dict[str, Any]) -> List[str]:
    """Extract tags from Notion page properties (multi-select, select)."""
    tags = ["ingested", "notion"]
    props = page.get("properties", {})
    if isinstance(props, dict):
        for prop_data in props.values():
            if not isinstance(prop_data, dict):
                continue
            prop_type = prop_data.get("type")
            if prop_type == "multi_select":
                for item in prop_data.get("multi_select", []):
                    name = item.get("name", "") if isinstance(item, dict) else str(item)
                    clean_tag = re.sub(r"[^a-zA-Z0-9_-]", "", name.lower().strip().replace(" ", "-"))
                    if clean_tag and clean_tag not in tags:
                        tags.append(clean_tag)
            elif prop_type == "select":
                select_item = prop_data.get("select")
                if isinstance(select_item, dict):
                    name = select_item.get("name", "")
                    clean_tag = re.sub(r"[^a-zA-Z0-9_-]", "", name.lower().strip().replace(" ", "-"))
                    if clean_tag and clean_tag not in tags:
                        tags.append(clean_tag)
    return tags


class NotionSource(BaseSource):
    """Source connector for ingesting Notion workspace pages into Aurora vault."""

    source_type = "notion"
    display_name = "Notion"

    def __init__(
        self,
        token: Optional[str] = None,
        client: Optional[Any] = None,
        timeout: int = 15,
        session: Optional[requests.Session] = None,
    ) -> None:
        super().__init__()
        self.timeout = timeout
        self.session = session or requests.Session()
        self._custom_client = client

        # Resolve token if client is not provided directly
        self._token = token or os.getenv("NOTION_TOKEN") or os.getenv("NOTION_API_KEY")
        self._client: Optional[Client] = client

    @property
    def client(self) -> Client:
        """Lazily initialize or return the Notion client."""
        if self._client is not None:
            return self._client

        if not self._token or not str(self._token).strip():
            raise SourceError(
                "Notion integration token is missing. Please set NOTION_TOKEN in your environment or .env file."
            )

        try:
            self._client = Client(auth=self._token.strip(), timeout_ms=self.timeout * 1000)
            return self._client
        except Exception as e:
            raise map_notion_error(e)

    def _get_page_blocks(self, block_id: str) -> List[Dict[str, Any]]:
        """Retrieve all child blocks for a given block or page with pagination."""
        blocks: List[Dict[str, Any]] = []
        start_cursor: Optional[str] = None

        while True:
            params: Dict[str, Any] = {"block_id": block_id, "page_size": 100}
            if start_cursor:
                params["start_cursor"] = start_cursor

            try:
                res = self.client.blocks.children.list(**params)
            except Exception as e:
                raise map_notion_error(e)

            results = res.get("results", [])
            blocks.extend(results)

            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
            if not start_cursor:
                break

        return blocks

    def _download_attachment(
        self,
        file_url: Optional[str],
        seen_filenames: Set[str],
        downloaded_files: Dict[str, str],
        counter: int = 1,
        custom_name: Optional[str] = None,
        is_image: bool = False,
    ) -> Tuple[Optional[str], Optional[Attachment]]:
        """Download an attachment (image or file) safely, handling expiring URLs and disambiguation."""
        if not file_url or not str(file_url).strip():
            return None, None

        cleaned_url = file_url.strip()

        # If identical URL was already downloaded in this page, reuse filename
        if cleaned_url in downloaded_files:
            return downloaded_files[cleaned_url], None

        # Sanitize logging: do not leak query secrets/tokens
        url_path = urllib.parse.urlsplit(cleaned_url).path

        try:
            resp = self.session.get(cleaned_url, timeout=self.timeout)
            if resp.status_code != 200 or not resp.content:
                logger.warning(
                    f"Notion attachment download returned status {resp.status_code} for {url_path}"
                )
                return None, None
            file_bytes = resp.content
            mime_type = (
                resp.headers.get("Content-Type", "")
                .split(";")[0]
                .strip()
                or ("image/jpeg" if is_image else "application/octet-stream")
            )
        except Exception as e:
            logger.warning(f"Failed to download Notion attachment ({url_path}): {e}")
            return None, None

        # Determine base filename from custom name, URL path, or fallback counter
        path_name = Path(url_path).name
        raw_name = (custom_name or path_name or "").strip()
        if not raw_name:
            default_prefix = "notion_image" if is_image else "notion_file"
            raw_name = f"{default_prefix}_{counter}"

        clean_name = FORBIDDEN_CHARS_PATTERN.sub("_", raw_name).strip("._ ")
        if not clean_name:
            clean_name = f"attachment_{counter}"

        # Handle extensions safely
        if is_image:
            if not re.search(r"\.(jpe?g|png|gif|webp|svg)$", clean_name, re.I):
                clean_name = f"{clean_name}.jpg"
        else:
            suffix = Path(clean_name).suffix
            if not suffix:
                guess = mimetypes.guess_extension(mime_type)
                suffix = guess if guess else ".bin"
                clean_name = f"{clean_name}{suffix}"

        # Disambiguate against filenames already used in this page
        stem = Path(clean_name).stem
        suffix = Path(clean_name).suffix
        candidate_name = clean_name
        disambig_counter = 2
        while candidate_name in seen_filenames:
            candidate_name = f"{stem}_{disambig_counter}{suffix}"
            disambig_counter += 1
        clean_name = candidate_name

        seen_filenames.add(clean_name)
        downloaded_files[cleaned_url] = clean_name
        attachment = Attachment(
            filename=clean_name,
            content=file_bytes,
            mime_type=mime_type,
        )
        return clean_name, attachment

    def _download_image(
        self,
        img_url: Optional[str],
        seen_filenames: Set[str],
        downloaded_images: Dict[str, str],
        image_counter: int,
    ) -> Tuple[Optional[str], Optional[Attachment]]:
        """Download an image attachment safely, delegating to _download_attachment."""
        return self._download_attachment(
            file_url=img_url,
            seen_filenames=seen_filenames,
            downloaded_files=downloaded_images,
            counter=image_counter,
            is_image=True,
        )

    def _convert_blocks_to_markdown(
        self,
        blocks: List[Dict[str, Any]],
        seen_filenames: Set[str],
        downloaded_images: Dict[str, str],
        depth: int = 0,
    ) -> Tuple[str, List[Attachment]]:
        """Convert a list of Notion blocks to Markdown and extract attachments."""
        lines: List[str] = []
        attachments: List[Attachment] = []
        image_counter = 1
        file_counter = 1
        numbered_counter = 1

        i = 0
        while i < len(blocks):
            block = blocks[i]
            block_type = block.get("type", "")

            # Reset numbered list counter when block is not numbered_list_item
            if block_type != "numbered_list_item":
                numbered_counter = 1

            try:
                if block_type == "paragraph":
                    text = render_rich_text(block.get("paragraph", {}).get("rich_text", []))
                    lines.append(text)
                    lines.append("")

                elif block_type == "heading_1":
                    text = render_rich_text(block.get("heading_1", {}).get("rich_text", []))
                    lines.append(f"# {text}")
                    lines.append("")

                elif block_type == "heading_2":
                    text = render_rich_text(block.get("heading_2", {}).get("rich_text", []))
                    lines.append(f"## {text}")
                    lines.append("")

                elif block_type == "heading_3":
                    text = render_rich_text(block.get("heading_3", {}).get("rich_text", []))
                    lines.append(f"### {text}")
                    lines.append("")

                elif block_type == "bulleted_list_item":
                    text = render_rich_text(block.get("bulleted_list_item", {}).get("rich_text", []))
                    indent = "  " * depth
                    lines.append(f"{indent}- {text}")
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth + 1
                        )
                        attachments.extend(child_atts)
                        if child_md.strip():
                            lines.append(child_md)

                elif block_type == "numbered_list_item":
                    text = render_rich_text(block.get("numbered_list_item", {}).get("rich_text", []))
                    indent = "  " * depth
                    lines.append(f"{indent}{numbered_counter}. {text}")
                    numbered_counter += 1
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth + 1
                        )
                        attachments.extend(child_atts)
                        if child_md.strip():
                            lines.append(child_md)

                elif block_type == "to_do":
                    todo_data = block.get("to_do", {})
                    checked = todo_data.get("checked", False)
                    mark = "x" if checked else " "
                    text = render_rich_text(todo_data.get("rich_text", []))
                    indent = "  " * depth
                    lines.append(f"{indent}- [{mark}] {text}")
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth + 1
                        )
                        attachments.extend(child_atts)
                        if child_md.strip():
                            lines.append(child_md)

                elif block_type == "toggle":
                    # Requirement 7: Pure markdown toggle (### Toggle Title)
                    title = render_rich_text(block.get("toggle", {}).get("rich_text", []))
                    lines.append(f"### {title}")
                    lines.append("")
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth
                        )
                        attachments.extend(child_atts)
                        if child_md.strip():
                            lines.append(child_md)
                            lines.append("")

                elif block_type == "quote":
                    text = render_rich_text(block.get("quote", {}).get("rich_text", []))
                    quote_lines = [f"> {l}" for l in text.splitlines()] or ["> "]
                    lines.extend(quote_lines)
                    lines.append("")
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth
                        )
                        attachments.extend(child_atts)
                        for c_line in child_md.splitlines():
                            lines.append(f"> {c_line}")
                        lines.append("")

                elif block_type == "callout":
                    # Requirement 9: Blockquote with emoji/icon
                    callout_data = block.get("callout", {})
                    icon_data = callout_data.get("icon", {})
                    emoji = icon_data.get("emoji", "") if isinstance(icon_data, dict) and icon_data.get("type") == "emoji" else ""
                    text = render_rich_text(callout_data.get("rich_text", []))
                    prefix = f"{emoji} " if emoji else ""

                    c_lines = text.splitlines() or [""]
                    lines.append(f"> {prefix}{c_lines[0]}")
                    for cl in c_lines[1:]:
                        lines.append(f"> {cl}")
                    lines.append("")
                    if block.get("has_children") and depth < 6:
                        child_blocks = self._get_page_blocks(block["id"])
                        child_md, child_atts = self._convert_blocks_to_markdown(
                            child_blocks, seen_filenames, downloaded_images, depth=depth
                        )
                        attachments.extend(child_atts)
                        for c_line in child_md.splitlines():
                            lines.append(f"> {c_line}")
                        lines.append("")

                elif block_type == "code":
                    # Requirement 10: Code blocks with language
                    code_data = block.get("code", {})
                    lang = (code_data.get("language") or "").lower()
                    if lang == "plain text":
                        lang = ""
                    code_text = "".join(t.get("plain_text", "") for t in code_data.get("rich_text", []))
                    lines.append(f"```{lang}")
                    lines.append(code_text)
                    lines.append("```")
                    lines.append("")

                elif block_type == "divider":
                    lines.append("---")
                    lines.append("")

                elif block_type == "table":
                    # Requirement 8: Clean Markdown table from table_row children
                    table_data = block.get("table", {})
                    table_width = table_data.get("table_width", 0)
                    has_header = table_data.get("has_column_header", True)

                    # Fetch table rows
                    row_blocks = self._get_page_blocks(block["id"]) if block.get("has_children") else []
                    rows: List[List[str]] = []
                    for rb in row_blocks:
                        if rb.get("type") == "table_row":
                            cells = rb.get("table_row", {}).get("cells", [])
                            row_cells = [
                                render_rich_text(cell).replace("\n", " ").replace("|", "\\|")
                                for cell in cells
                            ]
                            rows.append(row_cells)

                    if rows:
                        max_cols = max(len(r) for r in rows)
                        if table_width > max_cols:
                            max_cols = table_width
                        max_cols = max(max_cols, 1)

                        # Pad all rows
                        padded_rows = [r + [""] * (max_cols - len(r)) for r in rows]

                        if has_header:
                            header_cells = padded_rows[0]
                            data_rows = padded_rows[1:]
                        else:
                            header_cells = [f"Col {idx + 1}" for idx in range(max_cols)]
                            data_rows = padded_rows

                        header_line = "| " + " | ".join(header_cells) + " |"
                        sep_line = "| " + " | ".join(["---"] * max_cols) + " |"

                        lines.append(header_line)
                        lines.append(sep_line)
                        for d_row in data_rows:
                            lines.append("| " + " | ".join(d_row) + " |")
                        lines.append("")

                elif block_type == "image":
                    # Requirement 11: Download and Obsidian embed
                    img_data = block.get("image", {})
                    itype = img_data.get("type", "file")
                    url = img_data.get(itype, {}).get("url", "")
                    if not url:
                        if isinstance(img_data.get("file"), dict):
                            url = img_data["file"].get("url", "")
                        elif isinstance(img_data.get("external"), dict):
                            url = img_data["external"].get("url", "")
                        elif isinstance(img_data.get("url"), str):
                            url = img_data["url"]
                    caption = render_rich_text(img_data.get("caption", []))

                    saved_name, att = self._download_image(
                        url, seen_filenames, downloaded_images, image_counter
                    )
                    if att:
                        attachments.append(att)
                    if saved_name:
                        lines.append(f"![[{saved_name}]]")
                    elif url:
                        # Fallback link
                        lines.append(f"![{caption}]({url})")
                    image_counter += 1
                    lines.append("")

                elif block_type == "file":
                    # File block handling (hosted Notion files and external files)
                    file_data = block.get("file", {})
                    ftype = file_data.get("type", "file")
                    url = file_data.get(ftype, {}).get("url", "")
                    if not url:
                        if isinstance(file_data.get("file"), dict):
                            url = file_data["file"].get("url", "")
                        elif isinstance(file_data.get("external"), dict):
                            url = file_data["external"].get("url", "")
                        elif isinstance(file_data.get("url"), str):
                            url = file_data["url"]
                    caption = render_rich_text(file_data.get("caption", []))
                    custom_name = file_data.get("name")

                    saved_name, att = self._download_attachment(
                        file_url=url,
                        seen_filenames=seen_filenames,
                        downloaded_files=downloaded_images,
                        counter=file_counter,
                        custom_name=custom_name,
                        is_image=False,
                    )
                    if att:
                        attachments.append(att)
                    if saved_name:
                        lines.append(f"![[{saved_name}]]")
                    elif url:
                        # Fallback normal Markdown link when download fails
                        fallback_title = (
                            caption
                            or custom_name
                            or Path(urllib.parse.urlsplit(url).path).name
                            or "Download file"
                        )
                        lines.append(f"[{fallback_title}]({url})")
                    file_counter += 1
                    lines.append("")

                elif block_type in {"bookmark", "link_preview"}:
                    b_data = block.get(block_type, {})
                    url = b_data.get("url", "")
                    caption = render_rich_text(b_data.get("caption", [])) or url
                    if url:
                        lines.append(f"[{caption}]({url})")
                        lines.append("")

                elif block_type == "equation":
                    expr = block.get("equation", {}).get("expression", "")
                    lines.append(f"$$\n{expr}\n$$")
                    lines.append("")

                elif block_type == "child_page":
                    child_title = block.get("child_page", {}).get("title", "Untitled Page")
                    lines.append(f"[[{child_title}]]")
                    lines.append("")

                else:
                    # Gracefully handle unsupported block types
                    logger.debug(f"Unsupported Notion block type '{block_type}' skipped.")

            except Exception as e:
                logger.warning(f"Error converting block {block.get('id', 'unknown')} ({block_type}): {e}")

            i += 1

        md_content = "\n".join(lines).strip()
        return md_content, attachments

    def _discover_pages(self, page_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Discover pages via Notion search or fetch a specific page by ID."""
        if page_id:
            try:
                page = self.client.pages.retrieve(page_id=page_id)
                return [page]
            except Exception as e:
                raise map_notion_error(e)

        pages: List[Dict[str, Any]] = []
        start_cursor: Optional[str] = None

        while True:
            params: Dict[str, Any] = {
                "filter": {"value": "page", "property": "object"},
                "page_size": 100,
            }
            if start_cursor:
                params["start_cursor"] = start_cursor

            try:
                res = self.client.search(**params)
            except Exception as e:
                raise map_notion_error(e)

            results = res.get("results", [])
            pages.extend(results)

            if not res.get("has_more"):
                break
            start_cursor = res.get("next_cursor")
            if not start_cursor:
                break

        return pages

    async def fetch_items(
        self,
        page_id: Optional[str] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch accessible Notion pages and convert their content blocks into SourceItems."""
        raw_pages = self._discover_pages(page_id=page_id)
        if not raw_pages:
            if page_id:
                raise SourceError(f"No Notion page found for ID: {page_id}")
            logger.info("No accessible Notion pages found for this integration.")
            return []

        items: List[SourceItem] = []

        for page in raw_pages:
            try:
                pid = str(page.get("id", "")).strip()
                if not pid:
                    continue

                source_id = f"notion:{pid}"
                title = extract_page_title(page)
                pub_date, pub_date_display = extract_page_date(page)
                tags = extract_page_tags(page)

                page_url = page.get("url") or f"https://notion.so/{pid.replace('-', '')}"
                last_edited = page.get("last_edited_time", "")

                # Author
                created_by = page.get("created_by", {})
                author = created_by.get("name") if isinstance(created_by, dict) else None

                # Fetch and convert blocks
                seen_filenames: Set[str] = set()
                downloaded_images: Dict[str, str] = {}
                blocks = self._get_page_blocks(pid)
                body_md, attachments = self._convert_blocks_to_markdown(
                    blocks, seen_filenames, downloaded_images
                )

                if not body_md:
                    body_md = f"No content found for Notion page '{title}'."

                # Avoid duplicate leading H1 matching title
                h1_line = f"# {title}"
                if body_md.startswith(h1_line):
                    body_md = body_md[len(h1_line):].strip()

                # Extra metadata
                extra_metadata: Dict[str, Any] = {
                    "page_id": pid,
                    "last_edited_time": last_edited,
                    "word_count": len(body_md.split()),
                }
                if pub_date_display:
                    extra_metadata["published_date"] = pub_date_display

                # Summary (first non-empty paragraph or sentence)
                summary: Optional[str] = None
                for line in body_md.splitlines():
                    clean_l = line.strip()
                    if clean_l and not clean_l.startswith(("#", ">", "!", "|", "---")):
                        summary = clean_l[:200]
                        break

                item = SourceItem(
                    source_type="notion",
                    source_id=source_id,
                    title=title,
                    content=body_md,
                    source_url=page_url,
                    date=pub_date,
                    author=author,
                    tags=tags,
                    summary=summary,
                    attachments=attachments,
                    extra_metadata=extra_metadata,
                )
                items.append(item)

            except Exception as e:
                logger.warning(
                    f"Failed to process Notion page {page.get('id', 'unknown')}: {e}",
                    exc_info=True,
                )
                # Batch isolation: individual page failure does not abort other pages

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a fetched Notion SourceItem into a canonical MarkdownNote."""
        page_url = item.source_url or ""
        source_ref = f"[Notion]({page_url})" if page_url else "Notion"
        last_edited = item.extra_metadata.get("last_edited_time", "")

        body_lines: List[str] = [
            f"# {item.title}",
            "",
            f"> **Source**: {source_ref}",
        ]

        meta_parts: List[str] = []
        if item.author:
            meta_parts.append(f"**Author**: {item.author}")
        if last_edited:
            date_clean = last_edited.split("T")[0] if "T" in last_edited else last_edited
            meta_parts.append(f"**Last Edited**: {date_clean}")
        elif item.date:
            meta_parts.append(f"**Date**: {item.date}")

        if meta_parts:
            body_lines.append(f"> {' · '.join(meta_parts)}")

        body_lines.extend(["", item.content])
        full_body = "\n".join(body_lines).strip() + "\n"

        attachment_filenames = [a.filename for a in item.attachments]

        return MarkdownNote(
            title=item.title,
            source="notion",
            date=item.date or "",
            body=full_body,
            tags=list(item.tags) if item.tags else ["ingested", "notion"],
            source_url=item.source_url,
            author=item.author,
            summary=item.summary,
            attachments=attachment_filenames,
            extra_metadata=dict(item.extra_metadata),
        )


# Register connector with global SourceRegistry
SourceRegistry.register("notion", NotionSource)
