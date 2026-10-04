"""Email source connector for Aurora Ingestion Pipeline.

Supports Gmail and Microsoft Outlook email thread ingestion with HTML-to-Markdown
conversion, attachment preservation, tracking pixel filtering, and deduplication.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry
from sources.email.base import (
    EmailAttachmentData,
    EmailMessageData,
    EmailProvider,
    EmailThreadData,
    clean_html_to_markdown,
    is_tracking_pixel_attachment,
    parse_email_date,
)
from sources.email.gmail import GmailProvider
from sources.email.outlook import OutlookProvider

logger = logging.getLogger("aurora.ingestion.email")


class EmailSource(BaseSource):
    """Source connector for ingesting email threads into Obsidian Markdown notes."""

    source_type = "email"
    display_name = "Email"

    def __init__(
        self,
        providers: Optional[Dict[str, EmailProvider]] = None,
        active_provider: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.providers: Dict[str, EmailProvider] = providers or {
            "gmail": GmailProvider(),
            "outlook": OutlookProvider(),
        }
        self.active_provider = active_provider or "gmail"

    def register_provider(self, name: str, provider: EmailProvider) -> None:
        """Register or override an email provider implementation."""
        self.providers[name.lower()] = provider

    def _convert_message_body_to_markdown(
        self,
        msg: EmailMessageData,
        inline_att_map: Dict[str, str],
    ) -> str:
        """Convert a message's HTML or plain text body to clean Markdown, resolving inline images."""
        if msg.body_html and msg.body_html.strip():
            raw_html = msg.body_html
            # Replace inline CID references with Obsidian embed syntax or placeholders
            for cid, filename in inline_att_map.items():
                raw_html = re.sub(
                    rf'src=["\']cid:{re.escape(cid)}["\']',
                    f'src="{filename}"',
                    raw_html,
                    flags=re.IGNORECASE,
                )

            md = clean_html_to_markdown(raw_html)

            # Convert <img> tags or image links created from CIDs to Obsidian embeds
            for filename in inline_att_map.values():
                md = re.sub(
                    rf'!\[(.*?)\]\({re.escape(filename)}\)',
                    f'![[{filename}]]',
                    md,
                )

            if md.strip():
                return md.strip()

        # Fallback to plain text body
        return (msg.body_text or "").strip()

    def _format_thread_markdown(
        self,
        thread: EmailThreadData,
        valid_attachments: List[EmailAttachmentData],
    ) -> Tuple[str, List[Attachment]]:
        """Format thread metadata, chronological messages, and attachments into a Markdown body."""
        body_parts: List[str] = []

        # 1. Thread Information block
        participants_str = ", ".join(thread.participants) if thread.participants else "None"
        info_lines = [
            "## Thread Information",
            "",
            f"- **Subject:** {thread.subject}",
            f"- **Participants:** {participants_str}",
            f"- **Provider:** {thread.provider.capitalize()}",
            f"- **Messages:** {len(thread.messages)}",
        ]
        body_parts.append("\n".join(info_lines))

        # Inline CID map: cid -> filename
        inline_att_map: Dict[str, str] = {}
        for att in valid_attachments:
            if att.content_id:
                clean_cid = att.content_id.strip("<>")
                inline_att_map[clean_cid] = att.filename

        # 2. Chronological messages
        for idx, msg in enumerate(thread.messages, 1):
            msg_md_body = self._convert_message_body_to_markdown(msg, inline_att_map)
            recipients_str = ", ".join(msg.recipients) if msg.recipients else "Undisclosed"

            msg_lines = [
                f"## Message {idx}",
                "",
                f"**From:** {msg.sender}  ",
                f"**To:** {recipients_str}  ",
                f"**Date:** {msg.date}",
                "",
                msg_md_body or "*[No message body]*",
            ]
            body_parts.append("\n".join(msg_lines))

        # 3. Attachments section (non-inline or all downloadable files)
        non_inline_atts = [a for a in valid_attachments if not a.is_inline]
        if non_inline_atts:
            att_lines = ["## Attachments", ""]
            for a in non_inline_atts:
                att_lines.append(f"- ![[{a.filename}]]")
            body_parts.append("\n".join(att_lines))

        full_body = "\n\n".join(body_parts)

        # Convert EmailAttachmentData to pipeline Attachment models
        pipeline_attachments: List[Attachment] = [
            Attachment(
                filename=a.filename,
                content=a.content,
                mime_type=a.mime_type,
            )
            for a in valid_attachments
        ]

        return full_body, pipeline_attachments

    async def fetch_items(
        self,
        provider: Optional[str] = None,
        thread_id: Optional[str] = None,
        thread_ids: Optional[Sequence[str]] = None,
        url: Optional[str] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch email threads from specified provider (Gmail or Outlook)."""
        prov_key = (provider or self.active_provider or "gmail").strip().lower()
        if prov_key not in self.providers:
            raise SourceError(
                f"Unsupported email provider '{prov_key}'. Supported providers: "
                f"{', '.join(sorted(self.providers.keys()))}"
            )

        prov_client = self.providers[prov_key]

        # Determine target thread IDs
        clean_ids: List[str] = []
        if thread_id:
            clean_ids.append(thread_id.strip())
        elif url:
            # Support --url argument as thread identifier
            clean_ids.append(url.strip())
        elif thread_ids:
            clean_ids.extend([tid.strip() for tid in thread_ids if tid.strip()])

        # Fetch threads from provider
        threads: List[EmailThreadData] = []
        if clean_ids:
            for tid in clean_ids:
                thread_data = await prov_client.fetch_thread(tid)
                threads.append(thread_data)
        else:
            threads = await prov_client.fetch_threads(**kwargs)

        if not threads:
            logger.info(f"No email threads returned for provider '{prov_key}'")
            return []

        items: List[SourceItem] = []
        for thread in threads:
            # Filter tracking pixels and analytics images
            valid_attachments: List[EmailAttachmentData] = []
            for msg in thread.messages:
                for att in msg.attachments:
                    if is_tracking_pixel_attachment(
                        att.filename, att.content, att.mime_type, att.is_inline
                    ):
                        logger.debug(
                            f"Filtered tracking pixel attachment '{att.filename}' ({len(att.content)} bytes)"
                        )
                        continue
                    valid_attachments.append(att)

            body_content, pipeline_attachments = self._format_thread_markdown(
                thread, valid_attachments
            )

            # Determine latest date across thread
            thread_date_iso = datetime.now().strftime("%Y-%m-%d")
            for msg in reversed(thread.messages):
                d_iso, _ = parse_email_date(msg.date)
                if d_iso:
                    thread_date_iso = d_iso
                    break

            # Stable identity: email:<provider>:<thread_id>
            source_id = f"email:{thread.provider}:{thread.thread_id}"

            # Calculate deterministic SHA-256 content hash
            hasher = hashlib.sha256()
            hasher.update(source_id.encode("utf-8"))
            hasher.update(b"\n")
            hasher.update(thread.subject.encode("utf-8"))
            hasher.update(b"\n")
            hasher.update(body_content.encode("utf-8"))
            for a in pipeline_attachments:
                if a.content:
                    hasher.update(b"\n")
                    hasher.update(a.content)
            content_hash = hasher.hexdigest()

            # Metadata
            author = thread.messages[0].sender if thread.messages else "Unknown"
            to_val = (
                ", ".join(thread.messages[0].recipients)
                if thread.messages and thread.messages[0].recipients
                else "Undisclosed"
            )

            extra_metadata: Dict[str, Any] = {
                "thread_id": thread.thread_id,
                "provider": thread.provider,
                "participants": list(thread.participants),
                "message_count": len(thread.messages),
                "from": author,
                "to": to_val,
                "subject": thread.subject,
            }

            item = SourceItem(
                source_id=source_id,
                source_type="email",
                title=thread.subject or f"Email Thread {thread.thread_id}",
                content=body_content,
                date=thread_date_iso,
                source_url=None,
                author=author,
                tags=["ingested", "email"],
                summary=thread.messages[0].body_text[:200] if thread.messages else None,
                attachments=pipeline_attachments,
                extra_metadata=extra_metadata,
                content_hash=content_hash,
            )
            items.append(item)

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert an email SourceItem into an Obsidian MarkdownNote."""
        attachment_names = [a.filename for a in item.attachments]
        return MarkdownNote(
            title=item.title,
            source="email",
            date=item.date or datetime.now().strftime("%Y-%m-%d"),
            body=item.content,
            tags=list(item.tags) if item.tags else ["ingested", "email"],
            source_url=item.source_url,
            author=item.author,
            aliases=list(item.aliases),
            status=item.status,
            summary=item.summary,
            language=item.language,
            word_count=item.word_count,
            attachments=attachment_names,
            extra_metadata=dict(item.extra_metadata),
            folder="Ingested/Email",
        )


# Register EmailSource in the global SourceRegistry
SourceRegistry.register("email", EmailSource)
