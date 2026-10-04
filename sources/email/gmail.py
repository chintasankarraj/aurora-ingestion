"""Gmail API provider implementation for Aurora Ingestion Pipeline."""

from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from exceptions import SourceError
from sources.email.base import (
    EmailAttachmentData,
    EmailMessageData,
    EmailProvider,
    EmailThreadData,
    parse_email_date,
)

logger = logging.getLogger("aurora.ingestion.email.gmail")


def decode_gmail_data(raw_data: str) -> bytes:
    """Decode Gmail API base64url-encoded body/attachment data."""
    if not raw_data:
        return b""
    # Handle missing padding in base64url strings
    padded = raw_data + "=" * (-len(raw_data) % 4)
    return base64.urlsafe_b64decode(padded)


class GmailProvider(EmailProvider):
    """Email provider implementation for Google Gmail API."""

    provider_name = "gmail"

    def __init__(
        self,
        service: Optional[Any] = None,
        credentials_path: Optional[str | Path] = None,
        token_path: Optional[str | Path] = None,
    ) -> None:
        self._service = service
        self.credentials_path = credentials_path
        self.token_path = token_path

    def _get_service(self) -> Any:
        """Initialize or return authenticated Google Gmail API service."""
        if self._service is not None:
            return self._service

        token_p = (
            self.token_path
            or os.getenv("GMAIL_TOKEN_PATH")
            or Path(".credentials/gmail_token.json")
        )
        creds_p = (
            self.credentials_path
            or os.getenv("GMAIL_CREDENTIALS_PATH")
            or Path(".credentials/gmail_credentials.json")
        )

        token_path_obj = Path(token_p)
        creds_path_obj = Path(creds_p)

        creds = None
        if token_path_obj.exists():
            try:
                from google.oauth2.credentials import Credentials
                creds = Credentials.from_authorized_user_file(
                    str(token_path_obj),
                    scopes=["https://www.googleapis.com/auth/gmail.readonly"],
                )
            except Exception as e:
                raise SourceError(f"Failed to load Gmail OAuth token: {e}") from e
        elif creds_path_obj.exists():
            try:
                from google_auth_oauthlib.flow import InstalledAppFlow
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(creds_path_obj),
                    scopes=["https://www.googleapis.com/auth/gmail.readonly"],
                )
                creds = flow.run_local_server(port=0)
                token_path_obj.parent.mkdir(parents=True, exist_ok=True)
                with open(token_path_obj, "w", encoding="utf-8") as token_file:
                    token_file.write(creds.to_json())
            except Exception as e:
                raise SourceError(f"Gmail OAuth authorization failed: {e}") from e
        else:
            raise SourceError(
                "Gmail authentication error: OAuth credentials not configured. "
                "Please configure GMAIL_CREDENTIALS_PATH or GMAIL_TOKEN_PATH."
            )

        try:
            from googleapiclient.discovery import build
            self._service = build("gmail", "v1", credentials=creds)
            return self._service
        except Exception as e:
            raise SourceError(f"Failed to build Gmail service client: {e}") from e

    def _parse_mime_part(
        self, part: Dict[str, Any], message_id: str, service: Any
    ) -> Tuple[str, Optional[str], List[EmailAttachmentData]]:
        """Recursively extract plain text, HTML, and attachments from a MIME part."""
        text_body = ""
        html_body: Optional[str] = None
        attachments: List[EmailAttachmentData] = []

        mime_type = (part.get("mimeType") or "").lower()
        filename = (part.get("filename") or "").strip()
        body_obj = part.get("body", {})
        data_str = body_obj.get("data", "")
        att_id = body_obj.get("attachmentId")

        # Check for attachment
        if filename:
            att_bytes = b""
            if data_str:
                att_bytes = decode_gmail_data(data_str)
            elif att_id:
                try:
                    att_resp = (
                        service.users()
                        .messages()
                        .attachments()
                        .get(userId="me", messageId=message_id, id=att_id)
                        .execute()
                    )
                    att_bytes = decode_gmail_data(att_resp.get("data", ""))
                except Exception as e:
                    logger.warning(
                        f"Failed to download Gmail attachment '{filename}' (ID: {att_id}): {e}"
                    )

            if att_bytes:
                # Find Content-ID header if inline
                headers = part.get("headers", [])
                cid: Optional[str] = None
                is_inline = False
                for h in headers:
                    h_name = (h.get("name") or "").lower()
                    h_val = h.get("value") or ""
                    if h_name == "content-id":
                        cid = h_val.strip("<>")
                    elif h_name == "content-disposition" and "inline" in h_val.lower():
                        is_inline = True

                attachments.append(
                    EmailAttachmentData(
                        filename=filename,
                        content=att_bytes,
                        mime_type=mime_type or "application/octet-stream",
                        content_id=cid,
                        is_inline=is_inline,
                    )
                )

        # Body text / HTML
        elif mime_type == "text/plain" and data_str:
            try:
                text_body = decode_gmail_data(data_str).decode("utf-8", errors="replace")
            except Exception:
                text_body = ""

        elif mime_type == "text/html" and data_str:
            try:
                html_body = decode_gmail_data(data_str).decode("utf-8", errors="replace")
            except Exception:
                html_body = None

        # Recursively process multipart sub-parts
        sub_parts = part.get("parts", [])
        for sp in sub_parts:
            sp_text, sp_html, sp_atts = self._parse_mime_part(sp, message_id, service)
            if sp_text and not text_body:
                text_body = sp_text
            elif sp_text and text_body:
                text_body += "\n" + sp_text
            if sp_html and not html_body:
                html_body = sp_html
            attachments.extend(sp_atts)

        return text_body, html_body, attachments

    def _parse_message(self, msg_raw: Dict[str, Any], service: Any) -> EmailMessageData:
        """Parse raw Gmail message dictionary into an EmailMessageData object."""
        msg_id = msg_raw.get("id") or ""
        payload = msg_raw.get("payload", {})
        headers = payload.get("headers", [])

        # Extract standard email headers
        subject = "No Subject"
        sender = "Unknown Sender"
        recipients: List[str] = []
        date_str = ""

        for h in headers:
            name = (h.get("name") or "").lower()
            val = (h.get("value") or "").strip()
            if name == "subject":
                subject = val or "No Subject"
            elif name == "from":
                sender = val or "Unknown Sender"
            elif name in {"to", "cc"}:
                if val:
                    # Split comma-separated recipient addresses
                    for r in val.split(","):
                        r_clean = r.strip()
                        if r_clean and r_clean not in recipients:
                            recipients.append(r_clean)
            elif name == "date":
                date_str = val

        if not date_str:
            date_str = msg_raw.get("internalDate", "")

        date_iso, date_display = parse_email_date(date_str)

        text_body, html_body, attachments = self._parse_mime_part(
            payload, msg_id, service
        )

        return EmailMessageData(
            message_id=msg_id,
            sender=sender,
            recipients=recipients,
            date=date_display,
            subject=subject,
            body_text=text_body,
            body_html=html_body,
            attachments=attachments,
        )

    async def fetch_thread(self, thread_id: str) -> EmailThreadData:
        """Fetch a single Gmail thread by thread ID."""
        if not thread_id or not str(thread_id).strip():
            raise SourceError("Gmail thread ID must be provided.")

        clean_id = str(thread_id).strip()
        service = self._get_service()

        try:
            thread_raw = (
                service.users()
                .threads()
                .get(userId="me", id=clean_id, format="full")
                .execute()
            )
        except Exception as e:
            err_str = str(e).lower()
            if "notfound" in err_str or "404" in err_str:
                raise SourceError(f"Gmail thread '{clean_id}' not found or deleted: {e}") from e
            elif "401" in err_str or "403" in err_str or "invalid_grant" in err_str:
                raise SourceError(f"Gmail authorization error for thread '{clean_id}': {e}") from e
            elif "429" in err_str or "rate limit" in err_str:
                raise SourceError(f"Gmail rate limit exceeded while fetching thread '{clean_id}': {e}") from e
            raise SourceError(f"Failed to fetch Gmail thread '{clean_id}': {e}") from e

        raw_messages = thread_raw.get("messages", [])
        if not raw_messages:
            raise SourceError(f"Gmail thread '{clean_id}' contains no messages.")

        messages: List[EmailMessageData] = []
        participants: List[str] = []
        thread_subject = "No Subject"

        for raw_msg in raw_messages:
            msg_obj = self._parse_message(raw_msg, service)
            messages.append(msg_obj)

            if thread_subject == "No Subject" and msg_obj.subject != "No Subject":
                thread_subject = msg_obj.subject

            # Collect participants
            if msg_obj.sender and msg_obj.sender not in participants:
                participants.append(msg_obj.sender)
            for r in msg_obj.recipients:
                if r and r not in participants:
                    participants.append(r)

        return EmailThreadData(
            thread_id=clean_id,
            provider="gmail",
            subject=thread_subject,
            messages=messages,
            participants=participants,
        )

    async def fetch_threads(
        self, thread_ids: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> List[EmailThreadData]:
        """Fetch Gmail threads specified by thread IDs or list most recent."""
        results: List[EmailThreadData] = []

        if thread_ids:
            for tid in thread_ids:
                results.append(await self.fetch_thread(tid))
            return results

        # If no specific thread IDs passed, list recent threads
        service = self._get_service()
        max_results = kwargs.get("max_results", 5)
        try:
            list_resp = (
                service.users()
                .threads()
                .list(userId="me", maxResults=max_results)
                .execute()
            )
            threads_meta = list_resp.get("threads", [])
            for tm in threads_meta:
                t_id = tm.get("id")
                if t_id:
                    results.append(await self.fetch_thread(t_id))
            return results
        except SourceError:
            raise
        except Exception as e:
            raise SourceError(f"Failed to list Gmail threads: {e}") from e
