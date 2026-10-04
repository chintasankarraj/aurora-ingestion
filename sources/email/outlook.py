"""Microsoft Outlook / Graph API provider implementation for Aurora Ingestion Pipeline."""

from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import requests

from exceptions import SourceError
from sources.email.base import (
    EmailAttachmentData,
    EmailMessageData,
    EmailProvider,
    EmailThreadData,
    parse_email_date,
)

logger = logging.getLogger("aurora.ingestion.email.outlook")

GRAPH_API_BASE_URL = "https://graph.microsoft.com/v1.0"
DELEGATED_SCOPES = ["Mail.Read"]


class OutlookProvider(EmailProvider):
    """Email provider implementation for Microsoft Outlook via Microsoft Graph API."""

    provider_name = "outlook"

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        access_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        tenant_id: Optional[str] = None,
        token_cache_path: Optional[str | Path] = None,
        app: Optional[Any] = None,
    ) -> None:
        self._session = session or requests.Session()
        self._access_token = access_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.token_cache_path = token_cache_path
        self._app = app

    def _get_access_token(self) -> str:
        """Retrieve Microsoft Graph OAuth2 delegated access token from memory, env, or MSAL."""
        if self._access_token:
            return self._access_token

        token_from_env = os.getenv("OUTLOOK_ACCESS_TOKEN")
        if token_from_env:
            self._access_token = token_from_env
            return self._access_token

        c_id = self.client_id or os.getenv("OUTLOOK_CLIENT_ID")
        t_id = self.tenant_id or os.getenv("OUTLOOK_TENANT_ID") or "common"

        if c_id:
            try:
                import msal
            except ImportError as e:
                raise SourceError(
                    "msal library is required for Outlook delegated authentication. "
                    "Please install msal."
                ) from e

            try:
                cache = msal.SerializableTokenCache()
                cache_p = (
                    self.token_cache_path
                    or os.getenv("OUTLOOK_TOKEN_CACHE_PATH")
                    or Path(".credentials/outlook_token_cache.bin")
                )
                cache_file = Path(cache_p)
                if cache_file.exists():
                    try:
                        cache.deserialize(cache_file.read_text(encoding="utf-8"))
                    except Exception as e:
                        logger.warning(f"Could not load Outlook token cache: {e}")

                if self._app is not None:
                    app = self._app
                else:
                    authority = f"https://login.microsoftonline.com/{t_id}"
                    app = msal.PublicClientApplication(
                        c_id,
                        authority=authority,
                        token_cache=cache,
                    )

                # Attempt silent token acquisition from cache
                accounts = app.get_accounts() if hasattr(app, "get_accounts") else []
                result = None
                if accounts and hasattr(app, "acquire_token_silent"):
                    result = app.acquire_token_silent(DELEGATED_SCOPES, account=accounts[0])

                if not result or "access_token" not in result:
                    if not hasattr(app, "initiate_device_flow"):
                        raise SourceError("MSAL application does not support device flow.")
                    flow = app.initiate_device_flow(scopes=DELEGATED_SCOPES)
                    if not flow or not isinstance(flow, dict) or "user_code" not in flow:
                        error_msg = (
                            (flow.get("error_description") if isinstance(flow, dict) else None)
                            or (flow.get("error") if isinstance(flow, dict) else None)
                            or "Failed to create device code flow"
                        )
                        raise SourceError(f"MSAL device code flow initialization failed: {error_msg}")

                    user_message = flow.get("message") or (
                        f"To sign in, use a web browser to open {flow.get('verification_uri', 'https://microsoft.com/devicelogin')} "
                        f"and enter the code {flow.get('user_code')} to authenticate."
                    )
                    logger.info(user_message)
                    print(user_message)

                    if not hasattr(app, "acquire_token_by_device_flow"):
                        raise SourceError("MSAL application does not support acquire_token_by_device_flow.")
                    result = app.acquire_token_by_device_flow(flow)

                if result and isinstance(result, dict) and "access_token" in result:
                    self._access_token = result["access_token"]
                    if getattr(cache, "has_state_changed", False):
                        try:
                            cache_file.parent.mkdir(parents=True, exist_ok=True)
                            cache_file.write_text(cache.serialize(), encoding="utf-8")
                        except Exception as e:
                            logger.warning(f"Failed to persist Outlook token cache: {e}")
                    return self._access_token
                else:
                    error_desc = (
                        result.get("error_description") if isinstance(result, dict) else None
                    ) or "Unknown MSAL error"
                    raise SourceError(f"MSAL token acquisition failed: {error_desc}")

            except SourceError:
                raise
            except Exception as e:
                raise SourceError(f"Failed to acquire Outlook OAuth token via MSAL: {e}") from e

        raise SourceError(
            "Outlook authentication error: Microsoft Graph credentials not configured. "
            "Please configure OUTLOOK_ACCESS_TOKEN or OUTLOOK_CLIENT_ID."
        )

    def _request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Perform authenticated HTTP request to Microsoft Graph API."""
        token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        url = f"{GRAPH_API_BASE_URL}/{endpoint.lstrip('/')}"
        try:
            resp = self._session.get(url, headers=headers, params=params, timeout=30)
        except Exception as e:
            raise SourceError(f"Network error contacting Microsoft Graph API: {e}") from e

        if resp.status_code == 200:
            try:
                return resp.json()
            except Exception as e:
                raise SourceError(f"Failed to parse Microsoft Graph JSON response: {e}") from e
        elif resp.status_code == 404:
            raise SourceError(f"Microsoft Graph resource not found: {endpoint}")
        elif resp.status_code in {401, 403}:
            raise SourceError(f"Outlook authentication or permission denied (HTTP {resp.status_code})")
        elif resp.status_code == 429:
            raise SourceError("Outlook Microsoft Graph API rate limit exceeded.")
        else:
            raise SourceError(
                f"Microsoft Graph API request failed with HTTP {resp.status_code}"
            )

    def _fetch_attachments(self, message_id: str) -> List[EmailAttachmentData]:
        """Fetch attachments for a specific Outlook message."""
        attachments: List[EmailAttachmentData] = []
        try:
            data = self._request(f"me/messages/{message_id}/attachments")
            for item in data.get("value", []):
                content_bytes_b64 = item.get("contentBytes")
                if not content_bytes_b64:
                    continue

                filename = item.get("name") or f"attachment_{item.get('id', 'item')}"
                try:
                    content = base64.b64decode(content_bytes_b64)
                except Exception as e:
                    logger.warning(f"Failed to decode base64 for Outlook attachment '{filename}': {e}")
                    continue

                mime_type = item.get("contentType") or "application/octet-stream"
                is_inline = bool(item.get("isInline"))
                content_id = item.get("contentId")

                attachments.append(
                    EmailAttachmentData(
                        filename=filename,
                        content=content,
                        mime_type=mime_type,
                        content_id=content_id,
                        is_inline=is_inline,
                    )
                )
        except Exception as e:
            logger.warning(f"Failed to fetch attachments for Outlook message '{message_id}': {e}")

        return attachments

    def _parse_message(self, raw_msg: Dict[str, Any]) -> EmailMessageData:
        """Parse raw Microsoft Graph message JSON into EmailMessageData."""
        msg_id = raw_msg.get("id") or ""
        subject = raw_msg.get("subject") or "No Subject"

        # Sender
        from_dict = raw_msg.get("from", {}).get("emailAddress", {})
        sender_address = from_dict.get("address") or ""
        sender_name = from_dict.get("name") or ""
        if sender_name and sender_address and sender_name != sender_address:
            sender = f"{sender_name} <{sender_address}>"
        else:
            sender = sender_address or sender_name or "Unknown Sender"

        # Recipients
        recipients: List[str] = []
        for r_item in raw_msg.get("toRecipients", []):
            ea = r_item.get("emailAddress", {})
            r_addr = ea.get("address") or ""
            r_name = ea.get("name") or ""
            if r_name and r_addr and r_name != r_addr:
                recipients.append(f"{r_name} <{r_addr}>")
            elif r_addr or r_name:
                recipients.append(r_addr or r_name)

        raw_date = raw_msg.get("receivedDateTime") or raw_msg.get("sentDateTime")
        date_iso, date_display = parse_email_date(raw_date)

        # Body
        body_dict = raw_msg.get("body", {})
        content_type = (body_dict.get("contentType") or "").lower()
        content = body_dict.get("content") or ""

        if content_type == "html":
            body_html = content
            body_text = raw_msg.get("bodyPreview") or ""
        else:
            body_html = None
            body_text = content

        # Attachments
        attachments: List[EmailAttachmentData] = []
        if raw_msg.get("hasAttachments"):
            attachments = self._fetch_attachments(msg_id)

        return EmailMessageData(
            message_id=msg_id,
            sender=sender,
            recipients=recipients,
            date=date_display,
            subject=subject,
            body_text=body_text,
            body_html=body_html,
            attachments=attachments,
        )

    async def fetch_thread(self, thread_id: str) -> EmailThreadData:
        """Fetch a single conversation/thread by Outlook conversationId (or message ID)."""
        if not thread_id or not str(thread_id).strip():
            raise SourceError("Outlook conversation/thread ID must be provided.")

        clean_id = str(thread_id).strip()

        # Try fetching messages matching the conversationId
        params = {
            "$filter": f"conversationId eq '{clean_id}'",
            "$orderby": "receivedDateTime asc",
        }
        try:
            data = self._request("me/messages", params=params)
            raw_messages = data.get("value", [])
        except SourceError as e:
            # If conversationId filter fails or is empty, try direct message ID
            if "not found" in str(e).lower() or "failed" in str(e).lower():
                raw_messages = []
            else:
                raise

        # Fallback to single message retrieval if query returned no messages
        if not raw_messages:
            try:
                single_msg = self._request(f"me/messages/{clean_id}")
                raw_messages = [single_msg]
            except SourceError:
                raise SourceError(
                    f"Outlook conversation or message '{clean_id}' not found."
                )

        if not raw_messages:
            raise SourceError(f"Outlook conversation '{clean_id}' contains no messages.")

        messages: List[EmailMessageData] = []
        participants: List[str] = []
        thread_subject = "No Subject"

        for raw_msg in raw_messages:
            msg_obj = self._parse_message(raw_msg)
            messages.append(msg_obj)

            if thread_subject == "No Subject" and msg_obj.subject != "No Subject":
                thread_subject = msg_obj.subject

            # Collect unique participants
            if msg_obj.sender and msg_obj.sender not in participants:
                participants.append(msg_obj.sender)
            for r in msg_obj.recipients:
                if r and r not in participants:
                    participants.append(r)

        return EmailThreadData(
            thread_id=clean_id,
            provider="outlook",
            subject=thread_subject,
            messages=messages,
            participants=participants,
        )

    async def fetch_threads(
        self, thread_ids: Optional[Sequence[str]] = None, **kwargs: Any
    ) -> List[EmailThreadData]:
        """Fetch Outlook conversations specified by IDs or list most recent."""
        results: List[EmailThreadData] = []

        if thread_ids:
            for tid in thread_ids:
                results.append(await self.fetch_thread(tid))
            return results

        # List recent messages and group by conversationId
        max_results = kwargs.get("max_results", 10)
        params = {
            "$top": str(max_results),
            "$orderby": "receivedDateTime desc",
        }
        try:
            data = self._request("me/messages", params=params)
            raw_messages = data.get("value", [])

            # Extract distinct conversation IDs in order
            conv_ids: List[str] = []
            for m in raw_messages:
                cid = m.get("conversationId") or m.get("id")
                if cid and cid not in conv_ids:
                    conv_ids.append(cid)

            for cid in conv_ids[:5]:
                results.append(await self.fetch_thread(cid))

            return results
        except SourceError:
            raise
        except Exception as e:
            raise SourceError(f"Failed to list Outlook messages: {e}") from e
