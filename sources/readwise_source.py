"""Readwise source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Readwise v2 API to retrieve books, articles, tweets,
and other saved documents along with their highlights and personal notes.
Converts each Readwise document into a single clean Markdown note under
Ingested/Web/ with YAML frontmatter, attribution block, and blockquoted highlights.
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

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)


def map_readwise_error(exc: Exception) -> SourceError:
    """Map Readwise HTTP/API exceptions to descriptive SourceError exceptions without exposing tokens."""
    if isinstance(exc, SourceError):
        return exc

    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        status_code = response.status_code if response is not None else None
        if status_code == 401:
            return SourceError(
                "Readwise authentication failed: Invalid or expired API token (HTTP 401)."
            )
        if status_code == 403:
            return SourceError(
                "Readwise access forbidden: Token does not have permission for this resource (HTTP 403)."
            )
        if status_code == 404:
            return SourceError("Readwise resource not found (HTTP 404).")
        if status_code == 429:
            return SourceError(
                "Readwise API rate limit exceeded: Please wait before retrying (HTTP 429)."
            )
        return SourceError(f"Readwise API error (HTTP {status_code}).")

    if isinstance(exc, requests.exceptions.Timeout):
        return SourceError("Readwise API request timed out.")

    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError("Network connection failed while connecting to Readwise API.")

    return SourceError(f"Readwise connector error: {exc}")


def derive_readwise_stable_id(data: Dict[str, Any]) -> str:
    """Derive an immutable, stable source ID for a Readwise document.

    Prioritizes:
    1. Readwise user_book_id / id / book_id
    2. Deterministic hash of unique_url / source_url / readwise_url
    3. If neither an immutable ID nor a stable URL exists, raises SourceError
       so the malformed record fails safely and is skipped by batch isolation
       without using the mutable title as identity.
    """
    for field in ("user_book_id", "id", "book_id"):
        explicit_id = data.get(field)
        if explicit_id is not None and str(explicit_id).strip():
            return f"readwise:{str(explicit_id).strip()}"

    for field in ("unique_url", "source_url", "readwise_url"):
        unique_ref = data.get(field)
        if unique_ref is not None and str(unique_ref).strip():
            h = hashlib.sha256(str(unique_ref).strip().encode("utf-8")).hexdigest()[:16]
            return f"readwise:{h}"

    raw_title = data.get("title") or data.get("readable_title") or "unknown"
    raise SourceError(
        f"Readwise record is missing both an immutable ID and a stable URL identifier (title: '{raw_title}'). "
        "Cannot establish a stable source identity without an immutable ID or stable URL."
    )


def extract_readwise_tags(data: Dict[str, Any]) -> List[str]:
    """Extract and normalize tags from a Readwise item, avoiding duplicates and invalid YAML symbols."""
    raw_tags = data.get("tags") or data.get("book_tags") or []
    tags: List[str] = []
    if isinstance(raw_tags, list):
        for item in raw_tags:
            name: Optional[str] = None
            if isinstance(item, dict):
                name = item.get("name")
            elif item is not None:
                name = str(item)

            if name and str(name).strip():
                clean = str(name).strip().lstrip("#").strip()
                if clean and clean.lower() not in [t.lower() for t in tags]:
                    tags.append(clean)
    return tags


def format_highlight_blockquote(text: str) -> str:
    """Render highlight text as clean Markdown blockquotes."""
    lines = text.strip().splitlines()
    quoted = [f"> {line}" if line.strip() else ">" for line in lines]
    return "\n".join(quoted)


def render_highlight_block(h: Dict[str, Any]) -> str:
    """Render a single Readwise highlight item as Markdown.

    Includes:
    - Quoted highlight passage
    - Location / page number reference when available
    - Inline personal note / memo when available
    """
    text = (h.get("text") or "").strip()
    if not text:
        return ""

    parts: List[str] = [format_highlight_blockquote(text)]

    # Location / page reference
    location = h.get("location")
    if location is not None and str(location).strip():
        loc_val = str(location).strip()
        loc_type = str(h.get("location_type") or "").strip().lower()
        if loc_type == "page" or "page" in loc_type:
            parts.append(f"*Location: page {loc_val}*")
        elif loc_type and loc_type not in ("location", "order", "offset"):
            parts.append(f"*Location: {loc_type} {loc_val}*")
        else:
            parts.append(f"*Location: {loc_val}*")

    # Personal note / memo
    note = h.get("note")
    if note and str(note).strip():
        parts.append(f"**My note:** {str(note).strip()}")

    return "\n\n".join(parts)


class ReadwiseSource(BaseSource):
    """Source connector for Readwise highlights and saved articles."""

    def __init__(
        self,
        token: Optional[str] = None,
        session: Optional[Any] = None,
        base_url: str = "https://readwise.io/api/v2",
    ) -> None:
        super().__init__()
        self._token = token
        self._session = session
        self._base_url = base_url.rstrip("/")

    @property
    def source_type(self) -> str:
        return "readwise"

    @property
    def display_name(self) -> str:
        return "Readwise"

    def _resolve_token(self, **kwargs: Any) -> str:
        """Resolve Readwise API token from arguments, constructor, or environment variable."""
        token = (
            kwargs.get("token")
            or self._token
            or os.environ.get("READWISE_TOKEN")
        )
        if not token:
            raise SourceError(
                "Readwise API token is missing. Configure READWISE_TOKEN in your environment "
                "or pass --token."
            )
        return str(token).strip()

    def _get_session(self) -> Any:
        """Return the injected HTTP session or a new requests.Session."""
        if self._session is not None:
            return self._session
        return requests.Session()

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Construct authorization headers without logging or exposing secrets."""
        return {
            "Authorization": f"Token {token}",
            "User-Agent": "aurora-ingestion/1.0",
        }

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch documents and their highlights from the Readwise v2 Export API with pagination."""
        token = self._resolve_token(**kwargs)
        headers = self._get_headers(token)
        session = self._get_session()

        updated_after = kwargs.get("updated_after") or kwargs.get("updatedAfter")
        book_id = kwargs.get("book_id") or kwargs.get("ids")

        base_params: Dict[str, Any] = {}
        if updated_after:
            base_params["updatedAfter"] = str(updated_after).strip()
        if book_id:
            base_params["ids"] = str(book_id).strip()

        all_raw_items: List[Dict[str, Any]] = []
        page_cursor: Optional[str] = None
        next_url: Optional[str] = None
        seen_page_cursors: Set[str] = set()
        seen_pagination_urls: Set[str] = set()

        while True:
            params = dict(base_params)
            if page_cursor:
                params["pageCursor"] = page_cursor

            url = next_url or f"{self._base_url}/export/"
            seen_pagination_urls.add(url)

            try:
                if next_url:
                    resp = session.get(next_url, headers=headers, timeout=30)
                else:
                    resp = session.get(url, headers=headers, params=params, timeout=30)
            except Exception as e:
                raise map_readwise_error(e)

            if resp.status_code != 200:
                raise map_readwise_error(
                    requests.exceptions.HTTPError(
                        f"Readwise API returned status {resp.status_code}",
                        response=resp,
                    )
                )

            try:
                payload = resp.json()
            except Exception as e:
                raise SourceError(f"Failed to parse Readwise API JSON response: {e}")

            if not isinstance(payload, dict):
                raise SourceError("Readwise API returned a non-object JSON response.")

            results = payload.get("results")
            if results is None:
                results = []
            elif not isinstance(results, list):
                raise SourceError("Readwise API response 'results' field is not a list.")

            for item in results:
                if isinstance(item, dict):
                    all_raw_items.append(item)
                else:
                    logger.warning("Skipping non-dict item in Readwise results list.")

            # Pagination handling with defensive loop protection
            next_cursor = payload.get("nextPageCursor")
            next_link = payload.get("next")

            if next_cursor is not None and str(next_cursor).strip():
                cursor_str = str(next_cursor).strip()
                if cursor_str in seen_page_cursors:
                    logger.warning(
                        f"Readwise API returned duplicate nextPageCursor '{cursor_str}'. "
                        "Stopping pagination to prevent infinite loop."
                    )
                    break
                seen_page_cursors.add(cursor_str)
                page_cursor = cursor_str
                next_url = None
            elif next_link is not None and str(next_link).strip():
                link_str = str(next_link).strip()
                if link_str in seen_pagination_urls:
                    logger.warning(
                        f"Readwise API returned duplicate next URL '{link_str}'. "
                        "Stopping pagination to prevent infinite loop."
                    )
                    break
                seen_pagination_urls.add(link_str)
                next_url = link_str
                page_cursor = None
            else:
                # No more pages to fetch
                break

        # Process and convert each raw document into a SourceItem with batch isolation
        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        for raw_item in all_raw_items:
            try:
                source_item = self._convert_raw_to_source_item(raw_item)
                if source_item.source_id in seen_source_ids:
                    logger.debug(
                        f"Skipping duplicate Readwise item ID: {source_item.source_id}"
                    )
                    continue
                seen_source_ids.add(source_item.source_id)
                items.append(source_item)
            except Exception as e:
                logger.error(
                    f"Failed to process Readwise item: {e}",
                    exc_info=True,
                )
                continue

        return items

    def _convert_raw_to_source_item(self, data: Dict[str, Any]) -> SourceItem:
        """Convert a single Readwise API result object into a SourceItem."""
        source_id = derive_readwise_stable_id(data)

        # Title extraction with fallback
        raw_title = data.get("title") or data.get("readable_title") or ""
        title = str(raw_title).strip() if raw_title else "Untitled Readwise Document"

        # Author and URLs
        author = str(data.get("author")).strip() if data.get("author") else None
        source_url = (
            data.get("source_url")
            or data.get("unique_url")
            or data.get("readwise_url")
            or None
        )
        if source_url:
            source_url = str(source_url).strip()

        # Tags: enforce 'readwise'
        item_tags = extract_readwise_tags(data)
        normalized_tags = ["readwise"]
        for t in item_tags:
            if t.lower() not in [nt.lower() for nt in normalized_tags]:
                normalized_tags.append(t)

        # Date handling
        date_val = (
            data.get("last_highlight_at")
            or data.get("updated")
            or data.get("created")
            or datetime.now(timezone.utc).isoformat()
        )

        # Render highlights
        raw_highlights = data.get("highlights") or []
        rendered_highlights: List[str] = []
        if isinstance(raw_highlights, list):
            for h in raw_highlights:
                if isinstance(h, dict):
                    block = render_highlight_block(h)
                    if block:
                        rendered_highlights.append(block)

        # Build note body
        body_parts: List[str] = []

        summary = data.get("summary")
        if summary and str(summary).strip():
            body_parts.append(f"## Summary\n\n{str(summary).strip()}")

        doc_note = data.get("document_note")
        if doc_note and str(doc_note).strip():
            body_parts.append(f"## Document Note\n\n{str(doc_note).strip()}")

        if rendered_highlights:
            highlights_section = "## Highlights\n\n" + "\n\n".join(rendered_highlights)
            body_parts.append(highlights_section)
        else:
            body_parts.append("## Highlights\n\n*No highlights recorded.*")

        content = "\n\n".join(body_parts)

        # Extra metadata for frontmatter
        extra_metadata: Dict[str, Any] = {
            "readwise_id": data.get("user_book_id") or data.get("id"),
            "category": data.get("category"),
            "readwise_url": data.get("readwise_url"),
            "original_source": data.get("source"),
            "num_highlights": len(rendered_highlights),
            "last_highlight_at": data.get("last_highlight_at"),
            "updated": data.get("updated"),
        }
        # Clean None values from extra_metadata
        extra_metadata = {k: v for k, v in extra_metadata.items() if v is not None}

        item = SourceItem(
            source_id=source_id,
            source_type=self.source_type,
            title=title,
            content=content,
            date=date_val,
            source_url=source_url,
            author=author,
            tags=normalized_tags,
            summary=str(summary).strip() if summary else None,
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
SourceRegistry.register("readwise", ReadwiseSource)
