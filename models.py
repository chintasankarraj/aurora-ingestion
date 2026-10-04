"""Data models for the Aurora Ingestion Pipeline."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


def format_iso_timestamp(dt: Union[datetime, date, str, None]) -> str:
    """Format a date or datetime into an ISO 8601 string with timezone awareness."""
    if dt is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            # Assume local/UTC
            return dt.replace(tzinfo=timezone.utc).isoformat()
        return dt.isoformat()
    if isinstance(dt, date):
        return dt.isoformat()
    return str(dt)


@dataclass
class Attachment:
    """Represents a media or document attachment associated with a source item."""
    filename: str
    content: Optional[bytes] = None
    source_path: Optional[Path] = None
    mime_type: Optional[str] = None

    @property
    def embed_reference(self) -> str:
        """Obsidian embed syntax: ![[filename]]"""
        return f"![[{self.filename}]]"


@dataclass
class SourceItem:
    """Raw item fetched from an external source before conversion."""
    source_id: str
    source_type: str
    title: str
    content: str
    date: Optional[Union[datetime, date, str]] = None
    source_url: Optional[str] = None
    author: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)
    status: str = "unread"
    summary: Optional[str] = None
    language: Optional[str] = None
    word_count: Optional[int] = None
    attachments: List[Attachment] = field(default_factory=list)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)
    raw_content: Optional[Any] = None
    content_hash: Optional[str] = None

    def compute_content_hash(self) -> str:
        """Compute a deterministic SHA-256 hash of this item's content and key attributes."""
        if self.content_hash:
            return self.content_hash
        hasher = hashlib.sha256()
        hasher.update((self.title or "").strip().encode("utf-8"))
        hasher.update(b"\n")
        hasher.update((self.content or "").strip().encode("utf-8"))
        hasher.update(b"\n")
        if self.source_url:
            hasher.update(self.source_url.strip().encode("utf-8"))
            hasher.update(b"\n")
        if self.author:
            hasher.update(self.author.strip().encode("utf-8"))
            hasher.update(b"\n")
        if self.extra_metadata:
            # Deterministic serialization of extra metadata
            meta_str = json.dumps(self.extra_metadata, sort_keys=True, default=str)
            hasher.update(meta_str.encode("utf-8"))
        return hasher.hexdigest()


@dataclass
class MarkdownNote:
    """A prepared note ready to be rendered and written into the Obsidian vault."""
    title: str
    source: str
    date: Union[datetime, date, str]
    body: str
    tags: List[str] = field(default_factory=lambda: ["ingested"])
    ingested_at: Optional[Union[datetime, str]] = None
    source_url: Optional[str] = None
    author: Optional[str] = None
    aliases: List[str] = field(default_factory=list)
    status: str = "unread"
    summary: Optional[str] = None
    language: Optional[str] = None
    word_count: Optional[int] = None
    attachments: List[str] = field(default_factory=list)
    extra_metadata: Dict[str, Any] = field(default_factory=dict)
    folder: Optional[str] = None

    def __post_init__(self) -> None:
        # Enforce 'ingested' as the first tag
        clean_tags: List[str] = []
        for tag in self.tags:
            t = str(tag).lstrip("#").strip()
            if t and t not in clean_tags:
                clean_tags.append(t)
        if "ingested" in clean_tags:
            clean_tags.remove("ingested")
        clean_tags.insert(0, "ingested")
        self.tags = clean_tags

        # Set default ingested_at if absent
        if not self.ingested_at:
            self.ingested_at = datetime.now(timezone.utc).isoformat()
        elif isinstance(self.ingested_at, datetime):
            self.ingested_at = format_iso_timestamp(self.ingested_at)

        # Set default word count from body if not explicitly set
        if self.word_count is None and self.body:
            self.word_count = len(self.body.split())

    def to_frontmatter_dict(self) -> Dict[str, Any]:
        """Convert note attributes to frontmatter dictionary according to the specification."""
        # Required fields
        fm: Dict[str, Any] = {
            "title": self.title,
            "date": format_iso_timestamp(self.date) if not isinstance(self.date, str) else self.date,
            "source": self.source,
            "tags": list(self.tags),
            "ingested_at": str(self.ingested_at),
        }

        # Recommended fields when available
        if self.source_url:
            fm["source_url"] = self.source_url
        if self.author:
            fm["author"] = self.author
        if self.aliases:
            fm["aliases"] = list(self.aliases)
        if self.status:
            fm["status"] = self.status
        if self.summary:
            fm["summary"] = self.summary
        if self.language:
            fm["language"] = self.language
        if self.word_count is not None:
            fm["word_count"] = self.word_count
        if self.attachments:
            fm["attachments"] = list(self.attachments)

        # Source-specific extra fields
        for k, v in self.extra_metadata.items():
            if v is not None and k not in fm:
                fm[k] = v

        return fm


@dataclass
class TrackingRecord:
    """A record stored in the deduplication SQLite tracker."""
    source_type: str
    source_id: str
    vault_path: str
    ingested_at: str
    content_hash: str
