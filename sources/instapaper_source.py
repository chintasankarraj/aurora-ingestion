"""Instapaper source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Instapaper API to retrieve saved bookmarks, articles,
and highlighted passages with personal notes. Converts each Instapaper bookmark
into a single clean Markdown note under Ingested/Web/ with YAML frontmatter,
attribution block, article content/excerpt, and highlights blockquotes.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from markdownify import markdownify as md

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)


def _scrub_secrets(text: str, secrets: List[Optional[str]]) -> str:
    """Scrub sensitive credentials from error messages or logs."""
    scrubbed = str(text)
    for s in secrets:
        if s and len(str(s).strip()) >= 3:
            scrubbed = scrubbed.replace(str(s).strip(), "[REDACTED]")
    return scrubbed


def map_instapaper_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map Instapaper HTTP/API exceptions to descriptive SourceError exceptions without exposing credentials."""
    if isinstance(exc, SourceError):
        return exc

    resp = response or getattr(exc, "response", None)
    status_code = getattr(resp, "status_code", None) if resp is not None else None

    # Try extracting Instapaper error detail from JSON response
    err_detail = ""
    if resp is not None:
        try:
            body = resp.json()
            if isinstance(body, list) and len(body) > 0 and isinstance(body[0], dict):
                err_detail = body[0].get("message") or ""
            elif isinstance(body, dict):
                err_detail = body.get("error") or body.get("message") or ""
        except Exception:
            pass

    sanitized_detail = _scrub_secrets(err_detail, secrets or []) if err_detail else ""

    if status_code == 400:
        msg = (
            f"Instapaper API bad request (HTTP 400): {sanitized_detail}"
            if sanitized_detail
            else "Instapaper API bad request (HTTP 400)."
        )
        return SourceError(msg)
    if status_code == 401:
        return SourceError("Instapaper authentication failed: Invalid or expired credentials (HTTP 401).")
    if status_code == 403:
        msg = (
            f"Instapaper access forbidden: {sanitized_detail} (HTTP 403)."
            if sanitized_detail
            else "Instapaper access forbidden (HTTP 403)."
        )
        return SourceError(msg)
    if status_code == 404:
        return SourceError("Instapaper resource not found (HTTP 404).")
    if status_code == 429:
        return SourceError("Instapaper API rate limit exceeded: Please wait before retrying (HTTP 429).")
    if status_code is not None:
        msg = (
            f"Instapaper API error (HTTP {status_code}): {sanitized_detail}"
            if sanitized_detail
            else f"Instapaper API error (HTTP {status_code})."
        )
        return SourceError(msg)

    if isinstance(exc, requests.exceptions.Timeout):
        return SourceError("Instapaper API request timed out.")
    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError("Network connection failed while connecting to Instapaper API.")

    return SourceError(f"Instapaper connector error: {_scrub_secrets(str(exc), secrets or [])}")


def derive_instapaper_stable_id(data: Dict[str, Any]) -> str:
    """Derive an immutable, stable source ID for an Instapaper bookmark.

    Prioritizes:
    1. Instapaper bookmark_id / id
    2. If no immutable ID exists, raises SourceError so the malformed record
       fails safely and is skipped by batch isolation without using mutable title as identity.
    """
    for field in ("bookmark_id", "id"):
        val = data.get(field)
        if val is not None and str(val).strip():
            return f"instapaper:{str(val).strip()}"

    raw_title = data.get("title") or "unknown"
    raise SourceError(
        f"Instapaper record is missing an immutable bookmark ID (title: '{raw_title}'). "
        "Cannot establish a stable source identity without a bookmark ID."
    )


def extract_instapaper_tags(data: Dict[str, Any]) -> List[str]:
    """Extract and normalize tags from an Instapaper item."""
    tags: List[str] = ["instapaper"]

    def _add_tag(name: Any) -> None:
        if name is not None and str(name).strip():
            clean = str(name).strip().lstrip("#").strip()
            if clean and clean.lower() not in [t.lower() for t in tags]:
                tags.append(clean)

    raw_tags = data.get("tags")
    if isinstance(raw_tags, list):
        for item in raw_tags:
            if isinstance(item, dict):
                _add_tag(item.get("name") or item.get("tag"))
            else:
                _add_tag(item)
    elif isinstance(raw_tags, str):
        for part in raw_tags.split(","):
            _add_tag(part)

    folder = data.get("folder")
    if folder and str(folder).strip() and str(folder).strip().lower() not in ("unread", "default"):
        _add_tag(str(folder).strip())

    return tags


def parse_instapaper_timestamp(ts: Any) -> Optional[str]:
    """Convert Instapaper timestamp (Unix epoch seconds or ISO string) to an ISO 8601 string."""
    if ts is None:
        return None
    ts_str = str(ts).strip()
    if not ts_str:
        return None

    try:
        val = float(ts_str)
        if val > 0:
            return datetime.fromtimestamp(int(val), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        pass

    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        pass

    return None


def format_highlight_blockquote(text: str) -> str:
    """Render highlight text as clean Markdown blockquotes."""
    lines = text.strip().splitlines()
    quoted = [f"> {line}" if line.strip() else ">" for line in lines]
    return "\n".join(quoted)


def render_instapaper_highlight(h: Dict[str, Any]) -> str:
    """Render a single Instapaper highlight with optional personal note."""
    text = (h.get("text") or "").strip()
    if not text:
        return ""
    parts = [format_highlight_blockquote(text)]
    note = (h.get("note") or "").strip()
    if note:
        parts.append(f"**My note:** {note}")
    return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Content Normalization & HTML Detection Helpers
# ---------------------------------------------------------------------------

_HTML_VOID_TAGS_PATTERN = re.compile(
    r"<(?:br|hr|img|meta|link|input|source|track|wbr)\b[^>]*\/?>",
    re.IGNORECASE,
)

_HTML_CLOSING_TAG_PATTERN = re.compile(
    r"</(?:[a-zA-Z][a-zA-Z0-9]*)\s*>",
    re.IGNORECASE,
)

_HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<\s*(?:script|style)\b",
    re.IGNORECASE,
)

_HTML_OPENING_TAG_NAMES = (
    "p|div|span|h[1-6]|ul|ol|li|blockquote|article|section|header|footer|"
    "nav|main|table|tr|td|th|tbody|thead|tfoot|pre|code|html|body|head|"
    "title|figure|figcaption|details|summary|b|i|em|strong|a"
)

_HTML_OPENING_TAG_PATTERN = re.compile(
    rf"<({_HTML_OPENING_TAG_NAMES})\b(?:[^>]*>|>|\s*/>)",
    re.IGNORECASE,
)

_HTML_ATTR_TAG_PATTERN = re.compile(
    r"<[a-zA-Z][a-zA-Z0-9]*\s+[^>]*\b(?:id|class|href|src|style|rel|target|title|alt|type|data-[a-zA-Z0-9_-]+)\s*=",
    re.IGNORECASE,
)

_HTML_DECLARATION_PATTERN = re.compile(
    r"<!DOCTYPE\s+html|<!--.*?-->",
    re.IGNORECASE | re.DOTALL,
)


def is_html_content(text: str) -> bool:
    """Deterministically check if content contains HTML markup.

    Safely distinguishes HTML markup from plain text and Markdown formatting
    (such as comparison operators, mathematical expressions, or Markdown syntax).
    """
    if not text or not text.strip():
        return False

    raw = text.strip()

    if _HTML_DECLARATION_PATTERN.search(raw):
        return True
    if _HTML_CLOSING_TAG_PATTERN.search(raw):
        return True
    if _HTML_VOID_TAGS_PATTERN.search(raw):
        return True
    if _HTML_SCRIPT_STYLE_PATTERN.search(raw):
        return True
    if _HTML_ATTR_TAG_PATTERN.search(raw):
        return True
    if _HTML_OPENING_TAG_PATTERN.search(raw):
        return True

    return False


def normalize_instapaper_content(content: Any) -> str:
    """Normalize Instapaper content into clean, Obsidian-compatible Markdown.

    - If content is HTML, converts it to clean Markdown via markdownify, stripping
      <script> and <style> elements and their content, ensuring no raw HTML
      article tags remain in the output.
    - If content is already plain text or Markdown, preserves it without destructive
      modifications.
    - Returns an empty string if content is empty or whitespace-only.
    """
    if content is None:
        return ""

    raw_str = str(content).strip()
    if not raw_str:
        return ""

    if not is_html_content(raw_str):
        return raw_str

    # 1. Strip script and style blocks and their internal contents
    clean_html = re.sub(
        r"<\s*(?:script|style)\b[^>]*>.*?<\s*/\s*(?:script|style)\s*>",
        "",
        raw_str,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Strip any stray, unclosed, or self-closing script/style tags
    clean_html = re.sub(
        r"<\s*(?:script|style)\b[^>]*\/?>",
        "",
        clean_html,
        flags=re.IGNORECASE,
    )

    clean_html = clean_html.strip()
    if not clean_html:
        return ""

    # 2. Convert HTML to clean Markdown via markdownify
    converted = md(
        clean_html,
        heading_style="ATX",
        bullets="-",
    ).strip()

    # Normalize excessive newlines and trim whitespace
    converted = re.sub(r"\n{3,}", "\n\n", converted).strip()
    return converted


def build_instapaper_body(
    data: Dict[str, Any],
    highlights: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Build Obsidian-compatible Markdown body with content/excerpt, highlights, and metadata."""
    sections: List[str] = []

    # 1. Content or Excerpt
    full_content = ""
    for field in ("content", "text", "html"):
        raw_val = data.get(field)
        if raw_val is not None and str(raw_val).strip():
            normalized = normalize_instapaper_content(raw_val)
            if normalized:
                full_content = normalized
                break

    if full_content:
        sections.append(f"## Content\n\n{full_content}")
    else:
        excerpt_content = ""
        for field in ("description", "excerpt"):
            raw_val = data.get(field)
            if raw_val is not None and str(raw_val).strip():
                normalized = normalize_instapaper_content(raw_val)
                if normalized:
                    excerpt_content = normalized
                    break

        if excerpt_content:
            sections.append(f"## Excerpt\n\n{excerpt_content}")
        else:
            sections.append("## Content\n\n*No excerpt or content provided by Instapaper.*")

    # 2. Highlights (if any)
    hl_list = highlights or data.get("highlights") or []
    rendered_highlights: List[str] = []
    if isinstance(hl_list, list):
        for h in hl_list:
            if isinstance(h, dict):
                rendered = render_instapaper_highlight(h)
                if rendered:
                    rendered_highlights.append(rendered)

    if rendered_highlights:
        sections.append("## Highlights\n\n" + "\n\n".join(rendered_highlights))

    # 3. Instapaper Metadata Section
    meta_bullets: List[str] = []
    folder = data.get("folder")
    if folder and str(folder).strip():
        meta_bullets.append(f"- Folder: {str(folder).strip().title()}")

    starred = data.get("starred")
    if starred is not None:
        is_starred = str(starred).strip() in ("1", "true", "True")
        meta_bullets.append(f"- Starred: {'yes' if is_starred else 'no'}")

    progress = data.get("progress")
    if progress is not None:
        try:
            prog_val = float(str(progress).strip())
            pct = int(round(prog_val * 100))
            meta_bullets.append(f"- Reading Progress: {pct}%")
        except (ValueError, TypeError):
            pass

    time_added = parse_instapaper_timestamp(data.get("time") or data.get("created_at"))
    if time_added:
        # Format as YYYY-MM-DD
        date_str = time_added.split("T")[0]
        meta_bullets.append(f"- Saved: {date_str}")

    if meta_bullets:
        sections.append("## Instapaper Metadata\n\n" + "\n".join(meta_bullets))

    return "\n\n".join(sections)


class InstapaperSource(BaseSource):
    """Source connector for Instapaper bookmarks and saved articles."""

    def __init__(
        self,
        token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        consumer_key: Optional[str] = None,
        consumer_secret: Optional[str] = None,
        session: Optional[Any] = None,
        base_url: str = "https://www.instapaper.com/api/2",
    ) -> None:
        super().__init__()
        self._token = token
        self._username = username
        self._password = password
        self._consumer_key = consumer_key
        self._consumer_secret = consumer_secret
        self._session = session
        self._base_url = base_url.rstrip("/")

    @property
    def source_type(self) -> str:
        return "instapaper"

    @property
    def display_name(self) -> str:
        return "Instapaper"

    def _resolve_credentials(self, **kwargs: Any) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Resolve token or username/password credentials without exposing secrets."""
        token = (
            kwargs.get("token")
            or kwargs.get("access_token")
            or self._token
            or os.environ.get("INSTAPAPER_TOKEN")
            or os.environ.get("INSTAPAPER_ACCESS_TOKEN")
        )
        username = (
            kwargs.get("username")
            or self._username
            or os.environ.get("INSTAPAPER_USERNAME")
        )
        password = (
            kwargs.get("password")
            or self._password
            or os.environ.get("INSTAPAPER_PASSWORD")
        )

        if not token and not (username and password):
            raise SourceError(
                "Instapaper credentials missing. Configure INSTAPAPER_TOKEN (Bearer/Personal token) "
                "or INSTAPAPER_USERNAME and INSTAPAPER_PASSWORD in your environment or via CLI."
            )

        return (
            str(token).strip() if token else None,
            str(username).strip() if username else None,
            str(password).strip() if password else None,
        )

    def _get_session(self) -> Any:
        """Return the injected HTTP session or a new requests.Session."""
        if self._session is not None:
            return self._session
        return requests.Session()

    def _get_headers(self, token: Optional[str]) -> Dict[str, str]:
        """Construct standard Instapaper API headers."""
        headers = {
            "Accept": "application/json",
            "User-Agent": "aurora-ingestion/1.0",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch bookmarks and highlights from the Instapaper API with pagination and batch isolation."""
        token, username, password = self._resolve_credentials(**kwargs)
        headers = self._get_headers(token)
        session = self._get_session()
        secrets = [token, username, password, self._consumer_key, self._consumer_secret]

        all_raw_bookmarks: List[Dict[str, Any]] = []
        highlights_by_bookmark: Dict[str, List[Dict[str, Any]]] = {}

        seen_cursors: Set[str] = set()
        seen_batch_fingerprints: Set[str] = set()
        cursor: Optional[str] = kwargs.get("cursor")
        limit = int(kwargs.get("limit") or 50)
        max_items = kwargs.get("max_items")
        folder = kwargs.get("folder") or "unread"

        while True:
            params: Dict[str, Any] = {"limit": limit}
            if folder:
                params["folder"] = folder
            if cursor:
                cursor_str = str(cursor).strip()
                if cursor_str in seen_cursors:
                    logger.warning(
                        f"Instapaper API returned duplicate cursor '{cursor_str}'. "
                        "Stopping pagination to prevent infinite loop."
                    )
                    break
                seen_cursors.add(cursor_str)
                params["cursor"] = cursor_str

            if kwargs.get("since"):
                params["since"] = kwargs["since"]
            if kwargs.get("have"):
                params["have"] = kwargs["have"]

            url = f"{self._base_url}/bookmarks"
            try:
                if username and password and not token:
                    resp = session.get(url, headers=headers, params=params, auth=(username, password), timeout=30)
                else:
                    resp = session.get(url, headers=headers, params=params, timeout=30)
            except Exception as e:
                raise map_instapaper_error(e, secrets=secrets)

            if resp.status_code != 200:
                raise map_instapaper_error(
                    requests.exceptions.HTTPError(
                        f"Instapaper API returned status {resp.status_code}",
                        response=resp,
                    ),
                    response=resp,
                    secrets=secrets,
                )

            try:
                payload = resp.json()
            except Exception as e:
                raise SourceError(f"Failed to parse Instapaper API JSON response: {e}")

            page_bookmarks: List[Dict[str, Any]] = []
            next_cursor: Optional[str] = None
            has_more = False

            if isinstance(payload, dict):
                # Check for error in response
                if payload.get("type") == "error" or payload.get("error"):
                    err_msg = payload.get("message") or payload.get("error")
                    raise SourceError(f"Instapaper API error: {err_msg}")

                raw_list = payload.get("bookmarks") or payload.get("items") or payload.get("results")
                if isinstance(raw_list, list):
                    page_bookmarks = [b for b in raw_list if isinstance(b, dict)]
                elif raw_list is None:
                    page_bookmarks = []
                else:
                    raise SourceError("Instapaper API response 'bookmarks' field is not a list.")

                # Extract top-level highlights array if provided
                raw_highlights = payload.get("highlights")
                if isinstance(raw_highlights, list):
                    for h in raw_highlights:
                        if isinstance(h, dict):
                            b_id = str(h.get("bookmark_id") or "")
                            if b_id:
                                highlights_by_bookmark.setdefault(b_id, []).append(h)

                next_cursor = payload.get("cursor") or payload.get("next_cursor")
                has_more = bool(payload.get("has_more"))

            elif isinstance(payload, list):
                # Legacy API v1 format: list of mixed objects
                for obj in payload:
                    if not isinstance(obj, dict):
                        continue
                    obj_type = obj.get("type")
                    if obj_type == "error":
                        err_msg = obj.get("message") or "Unknown error"
                        raise SourceError(f"Instapaper API error: {err_msg}")
                    elif obj_type == "bookmark" or ("bookmark_id" in obj and obj_type != "highlight"):
                        page_bookmarks.append(obj)
                    elif obj_type == "highlight":
                        b_id = str(obj.get("bookmark_id") or "")
                        if b_id:
                            highlights_by_bookmark.setdefault(b_id, []).append(obj)
            else:
                raise SourceError("Instapaper API returned an unexpected non-object/non-list response.")

            if not page_bookmarks:
                break

            # Defensive loop protection: check batch fingerprint
            page_ids = tuple(sorted(str(b.get("bookmark_id") or b.get("id") or "") for b in page_bookmarks))
            fingerprint = hashlib.sha256(",".join(page_ids).encode("utf-8")).hexdigest()
            if fingerprint in seen_batch_fingerprints:
                logger.warning(
                    "Instapaper API returned duplicate batch of bookmarks. "
                    "Stopping pagination to prevent infinite loop."
                )
                break
            seen_batch_fingerprints.add(fingerprint)

            all_raw_bookmarks.extend(page_bookmarks)

            if max_items and len(all_raw_bookmarks) >= int(max_items):
                all_raw_bookmarks = all_raw_bookmarks[: int(max_items)]
                break

            if next_cursor and str(next_cursor).strip():
                cursor = str(next_cursor).strip()
            elif len(page_bookmarks) < limit:
                break
            else:
                break

        # Process and convert each raw bookmark into a SourceItem with batch isolation
        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        for raw_bm in all_raw_bookmarks:
            try:
                bm_id = str(raw_bm.get("bookmark_id") or raw_bm.get("id") or "")
                extra_hl = highlights_by_bookmark.get(bm_id)
                source_item = self._convert_raw_to_source_item(raw_bm, extra_highlights=extra_hl)
                if source_item.source_id in seen_source_ids:
                    logger.debug(f"Skipping duplicate Instapaper bookmark ID: {source_item.source_id}")
                    continue
                seen_source_ids.add(source_item.source_id)
                items.append(source_item)
            except Exception as e:
                logger.error(f"Failed to process Instapaper item: {e}", exc_info=True)
                continue

        return items

    def _convert_raw_to_source_item(
        self,
        data: Dict[str, Any],
        extra_highlights: Optional[List[Dict[str, Any]]] = None,
    ) -> SourceItem:
        """Convert a single Instapaper bookmark object into a SourceItem."""
        source_id = derive_instapaper_stable_id(data)

        # Title
        raw_title = data.get("title") or ""
        title = str(raw_title).strip() if raw_title else "Untitled Instapaper Bookmark"

        # URLs
        url = data.get("url") or data.get("source_url")
        source_url = str(url).strip() if url else None
        resolved_url = data.get("resolved_url")
        if resolved_url:
            resolved_url = str(resolved_url).strip()

        # Author
        author = str(data.get("author")).strip() if data.get("author") else None

        # Tags
        tags = extract_instapaper_tags(data)

        # Date handling: use source timestamp (Unix seconds or ISO string)
        date_val = (
            parse_instapaper_timestamp(data.get("time"))
            or parse_instapaper_timestamp(data.get("published_at"))
            or parse_instapaper_timestamp(data.get("created_at"))
            or parse_instapaper_timestamp(data.get("updated_at"))
            or ""
        )

        # Summary / Excerpt
        desc = data.get("description") or data.get("excerpt")
        summary_val = normalize_instapaper_content(desc) if desc else None
        if not summary_val:
            summary_val = None

        # Content & highlights
        combined_highlights = list(extra_highlights or [])
        if isinstance(data.get("highlights"), list):
            for h in data["highlights"]:
                if isinstance(h, dict) and h not in combined_highlights:
                    combined_highlights.append(h)

        content = build_instapaper_body(data, highlights=combined_highlights)

        # Metadata
        folder = data.get("folder") or "unread"
        starred = data.get("starred")
        is_starred = str(starred).strip() in ("1", "true", "True") if starred is not None else False

        progress = data.get("progress")
        prog_float = None
        if progress is not None:
            try:
                prog_float = float(str(progress).strip())
            except (ValueError, TypeError):
                pass

        extra_metadata: Dict[str, Any] = {
            "bookmark_id": data.get("bookmark_id") or data.get("id"),
            "resolved_url": resolved_url,
            "folder": str(folder).strip() if folder else None,
            "starred": is_starred,
            "progress": prog_float,
            "num_highlights": len(combined_highlights) if combined_highlights else 0,
            "hash": data.get("hash"),
        }
        word_count = data.get("word_count")
        if word_count is not None:
            try:
                extra_metadata["word_count"] = int(str(word_count).strip())
            except (ValueError, TypeError):
                pass

        extra_metadata = {k: v for k, v in extra_metadata.items() if v is not None}

        item = SourceItem(
            source_id=source_id,
            source_type=self.source_type,
            title=title,
            content=content,
            date=date_val,
            source_url=source_url,
            author=author,
            tags=tags,
            summary=summary_val,
            status=str(folder).lower() if folder else "unread",
            word_count=extra_metadata.get("word_count"),
            extra_metadata=extra_metadata,
            raw_content=data,
        )
        item.content_hash = item.compute_content_hash()
        return item

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a fetched SourceItem into a MarkdownNote targeted for Ingested/Web/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Web"
        return note


# Register connector with global SourceRegistry
SourceRegistry.register("instapaper", InstapaperSource)
