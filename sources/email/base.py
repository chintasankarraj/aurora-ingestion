"""Base abstractions and models for Email provider integrations."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import email.utils
from typing import Any, List, Optional, Sequence, Tuple

from bs4 import BeautifulSoup, Comment
import markdownify


def parse_email_date(raw_date: Optional[str]) -> Tuple[str, str]:
    """Parse various email date formats into (date_iso, date_display).
    
    Returns:
        date_iso: YYYY-MM-DD string for frontmatter / note naming.
        date_display: YYYY-MM-DD HH:MM string for message headers.
    """
    if not raw_date or not str(raw_date).strip():
        now_dt = datetime.now(timezone.utc)
        return now_dt.strftime("%Y-%m-%d"), now_dt.strftime("%Y-%m-%d %H:%M")

    raw_str = str(raw_date).strip()

    # 1. Try RFC 2822 email format (e.g. "Fri, 4 Oct 2026 10:30:00 +0000")
    try:
        dt = email.utils.parsedate_to_datetime(raw_str)
        if dt:
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass

    # 2. Try ISO 8601 (e.g. "2026-10-04T10:30:00Z" or "2026-10-04T10:30:00.000Z")
    try:
        clean_iso = raw_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_iso)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass

    # 3. Try integer timestamp (milliseconds or seconds)
    if raw_str.isdigit():
        try:
            ts = int(raw_str)
            if ts > 1e11:  # milliseconds
                ts = ts / 1000.0
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass

    # Fallback
    now_dt = datetime.now(timezone.utc)
    return now_dt.strftime("%Y-%m-%d"), now_dt.strftime("%Y-%m-%d %H:%M")


def clean_html_to_markdown(raw_html: str) -> str:
    """Convert email HTML content into clean, readable Markdown."""
    if not raw_html or not raw_html.strip():
        return ""

    try:
        soup = BeautifulSoup(raw_html, "html.parser")

        # Decompose non-content tags
        for tag in soup(["script", "style", "head", "link", "meta"]):
            tag.decompose()

        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()

        md_text = markdownify.markdownify(
            str(soup),
            heading_style="ATX",
            strip=["style", "script"],
        )

        # Normalize multiple blank lines
        clean_md = re.sub(r"\n{3,}", "\n\n", md_text).strip()
        return clean_md
    except Exception:
        # Fallback to plain text extraction
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text("\n\n").strip()


def is_tracking_pixel_attachment(
    filename: str,
    content: bytes,
    mime_type: str = "",
    is_inline: bool = False,
) -> bool:
    """Detect whether an email attachment is an analytics tracking pixel or spacer."""
    name_lower = (filename or "").strip().lower()

    # Obvious pixel filenames
    pixel_names = {
        "pixel.gif", "spacer.gif", "1x1.gif", "1x1.png",
        "blank.gif", "beacon.gif", "clear.gif", "empty.gif",
    }
    if name_lower in pixel_names:
        return True

    if re.search(r"(?:pixel|spacer|tracker|beacon|analytics|1x1)[\w-]*\.(?:gif|png|jpg|jpeg)$", name_lower):
        return True

    # Tiny 1x1 gif / png payloads (typically under 100 bytes)
    mime_lower = (mime_type or "").lower()
    if len(content) < 100 and (
        "image/gif" in mime_lower
        or "image/png" in mime_lower
        or name_lower.endswith((".gif", ".png"))
    ):
        return True

    return False


@dataclass
class EmailAttachmentData:
    """Raw attachment data extracted from an email message."""
    filename: str
    content: bytes
    mime_type: str = "application/octet-stream"
    content_id: Optional[str] = None
    is_inline: bool = False


@dataclass
class EmailMessageData:
    """Individual email message within a thread."""
    message_id: str
    sender: str
    recipients: List[str]
    date: str
    subject: str
    body_text: str = ""
    body_html: Optional[str] = None
    attachments: List[EmailAttachmentData] = field(default_factory=list)


@dataclass
class EmailThreadData:
    """Full email conversation/thread containing one or more messages."""
    thread_id: str
    provider: str
    subject: str
    messages: List[EmailMessageData] = field(default_factory=list)
    participants: List[str] = field(default_factory=list)


class EmailProvider(ABC):
    """Abstract base class for email service providers (Gmail, Outlook)."""

    provider_name: str

    @abstractmethod
    async def fetch_threads(
        self, thread_ids: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> List[EmailThreadData]:
        """Fetch multiple email threads by IDs or query."""
        pass

    @abstractmethod
    async def fetch_thread(self, thread_id: str) -> EmailThreadData:
        """Fetch a single email thread by thread/conversation ID."""
        pass
