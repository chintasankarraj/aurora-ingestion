"""Comprehensive tests for Email source connector (Gmail & Outlook)."""

import base64
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.email import (
    EmailAttachmentData,
    EmailMessageData,
    EmailProvider,
    EmailThreadData,
    GmailProvider,
    OutlookProvider,
    clean_html_to_markdown,
    is_tracking_pixel_attachment,
    parse_email_date,
)
from sources.email_source import EmailSource
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers and Mocks
# ---------------------------------------------------------------------------

def b64url(data: bytes) -> str:
    """Helper to encode bytes to base64url string."""
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def b64std(data: bytes) -> str:
    """Helper to encode bytes to standard base64 string."""
    return base64.b64encode(data).decode("utf-8")


class MockHttpResponse:
    """Mock requests response for Outlook/Graph API testing."""

    def __init__(
        self,
        json_data: dict | None = None,
        status_code: int = 200,
        text: str = "",
    ) -> None:
        self._json = json_data or {}
        self.status_code = status_code
        self.text = text or str(json_data)

    def json(self) -> dict:
        return self._json


# ---------------------------------------------------------------------------
# 1. Registration & CLI List Sources
# ---------------------------------------------------------------------------

def test_email_source_registration():
    src_cls = SourceRegistry.get("email")
    assert src_cls is EmailSource
    sources_dict = SourceRegistry.list_sources()
    assert "email" in sources_dict


@pytest.mark.asyncio
async def test_cli_list_sources_shows_email(capsys):
    parser = create_parser()
    args = parser.parse_args(["list-sources"])
    ret = await async_main(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "email (EmailSource)" in captured.out


# ---------------------------------------------------------------------------
# 2. Base Helpers (Date, HTML Cleaning, Tracking Pixel Filter)
# ---------------------------------------------------------------------------

def test_parse_email_date_formats():
    # RFC 2822
    d_iso, d_disp = parse_email_date("Fri, 4 Oct 2026 10:30:00 +0000")
    assert d_iso == "2026-10-04"
    assert "2026-10-04 10:30" in d_disp

    # ISO 8601
    d_iso, d_disp = parse_email_date("2026-10-04T14:45:00Z")
    assert d_iso == "2026-10-04"
    assert "2026-10-04 14:45" in d_disp

    # Timestamp in milliseconds
    d_iso, d_disp = parse_email_date("1791110400000")
    assert len(d_iso) == 10  # YYYY-MM-DD format

    # Empty / None fallback
    d_iso, d_disp = parse_email_date(None)
    assert len(d_iso) == 10


def test_clean_html_to_markdown():
    raw_html = """
    <html>
    <head><style>body { font-family: sans-serif; }</style></head>
    <body>
        <!-- Outlook comment -->
        <h1>Project Proposal</h1>
        <p>Hello team, here is the proposal for Q4.</p>
        <ul>
            <li>Milestone 1: Prototype</li>
            <li>Milestone 2: Review</li>
        </ul>
        <blockquote>Approved by management.</blockquote>
        <p>Check the <a href="https://example.com/spec">specification</a>.</p>
        <script>console.log("tracking");</script>
    </body>
    </html>
    """
    md = clean_html_to_markdown(raw_html)
    assert "# Project Proposal" in md
    assert "Hello team, here is the proposal for Q4." in md
    assert "Milestone 1: Prototype" in md
    assert "> Approved by management." in md
    assert "[specification](https://example.com/spec)" in md
    assert "font-family" not in md
    assert "console.log" not in md


def test_is_tracking_pixel_attachment():
    # Obvious pixel filenames
    assert is_tracking_pixel_attachment("pixel.gif", b"GIF89a...", mime_type="image/gif")
    assert is_tracking_pixel_attachment("spacer.gif", b"GIF89a...", mime_type="image/gif")
    assert is_tracking_pixel_attachment("1x1.png", b"\x89PNG...", mime_type="image/png")
    assert is_tracking_pixel_attachment("beacon.gif", b"GIF...", mime_type="image/gif")

    # Tiny byte payloads (<100 bytes)
    tiny_gif = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;"
    assert is_tracking_pixel_attachment("image001.gif", tiny_gif, mime_type="image/gif", is_inline=True)

    # Legitimate non-tracking attachments
    legit_pdf = b"%PDF-1.4 1234567890" * 50
    assert not is_tracking_pixel_attachment("proposal.pdf", legit_pdf, mime_type="application/pdf")

    legit_img = b"\x89PNG\r\n\x1a\n" + b"\x00" * 500
    assert not is_tracking_pixel_attachment("screenshot.png", legit_img, mime_type="image/png")


# ---------------------------------------------------------------------------
# 3. Gmail Provider Unit & MIME Parsing Tests
# ---------------------------------------------------------------------------

def test_gmail_provider_missing_credentials_raises_error(tmp_path):
    provider = GmailProvider(
        credentials_path=tmp_path / "nonexistent_creds.json",
        token_path=tmp_path / "nonexistent_token.json",
    )
    with pytest.raises(SourceError, match="OAuth credentials not configured"):
        provider._get_service()


@pytest.mark.asyncio
async def test_gmail_mime_parsing_and_thread_identity():
    # Build mock Gmail API service
    mock_service = MagicMock()

    pdf_bytes = b"%PDF-1.4 test pdf content for sprint notes"
    pixel_bytes = b"GIF89a123"  # tiny tracking pixel

    thread_json = {
        "id": "thread_gmail_123",
        "messages": [
            {
                "id": "msg_001",
                "internalDate": "1727780000000",
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Sprint Planning Discussion"},
                        {"name": "From", "value": "Alice <alice@company.com>"},
                        {"name": "To", "value": "Bob <bob@company.com>, Charlie <charlie@company.com>"},
                        {"name": "Date", "value": "Fri, 4 Oct 2026 10:00:00 +0000"},
                    ],
                    "mimeType": "multipart/alternative",
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "body": {"data": b64url(b"Hi team, here is the agenda for sprint planning.")},
                        },
                        {
                            "mimeType": "text/html",
                            "body": {"data": b64url(b"<p>Hi team, here is the <b>agenda</b> for sprint planning.</p>")},
                        },
                    ],
                },
            },
            {
                "id": "msg_002",
                "internalDate": "1727783600000",
                "payload": {
                    "headers": [
                        {"name": "Subject", "value": "Re: Sprint Planning Discussion"},
                        {"name": "From", "value": "Bob <bob@company.com>"},
                        {"name": "To", "value": "Alice <alice@company.com>"},
                        {"name": "Date", "value": "Fri, 4 Oct 2026 11:00:00 +0000"},
                    ],
                    "mimeType": "multipart/mixed",
                    "parts": [
                        {
                            "mimeType": "text/plain",
                            "body": {"data": b64url(b"Attached are the sprint notes.")},
                        },
                        {
                            "mimeType": "application/pdf",
                            "filename": "sprint_notes.pdf",
                            "body": {"attachmentId": "att_pdf_01"},
                        },
                        {
                            "mimeType": "image/gif",
                            "filename": "pixel.gif",
                            "body": {"attachmentId": "att_pixel_01"},
                        },
                    ],
                },
            },
        ],
    }

    mock_service.users().threads().get().execute.return_value = thread_json

    def mock_attachment_get(userId, messageId, id):
        mock_req = MagicMock()
        if id == "att_pdf_01":
            mock_req.execute.return_value = {"data": b64url(pdf_bytes)}
        else:
            mock_req.execute.return_value = {"data": b64url(pixel_bytes)}
        return mock_req

    mock_service.users().messages().attachments().get.side_effect = mock_attachment_get

    provider = GmailProvider(service=mock_service)
    thread = await provider.fetch_thread("thread_gmail_123")

    # Verify thread structure
    assert thread.thread_id == "thread_gmail_123"
    assert thread.provider == "gmail"
    assert thread.subject == "Sprint Planning Discussion"
    assert len(thread.messages) == 2

    # Verify participants
    assert "Alice <alice@company.com>" in thread.participants
    assert "Bob <bob@company.com>" in thread.participants
    assert "Charlie <charlie@company.com>" in thread.participants

    # Message 1
    m1 = thread.messages[0]
    assert m1.sender == "Alice <alice@company.com>"
    assert m1.body_html is not None
    assert "<b>agenda</b>" in m1.body_html

    # Message 2 & Attachments
    m2 = thread.messages[1]
    assert m2.sender == "Bob <bob@company.com>"
    assert len(m2.attachments) == 2
    att_names = [a.filename for a in m2.attachments]
    assert "sprint_notes.pdf" in att_names
    assert "pixel.gif" in att_names


@pytest.mark.asyncio
async def test_gmail_error_handling():
    mock_service = MagicMock()
    provider = GmailProvider(service=mock_service)

    # 404 Thread Not Found
    mock_service.users().threads().get().execute.side_effect = Exception("404 Not Found")
    with pytest.raises(SourceError, match="not found or deleted"):
        await provider.fetch_thread("missing_123")

    # 401 Authorization Error
    mock_service.users().threads().get().execute.side_effect = Exception("401 Unauthorized: invalid_grant")
    with pytest.raises(SourceError, match="authorization error"):
        await provider.fetch_thread("unauth_123")

    # 429 Rate Limit
    mock_service.users().threads().get().execute.side_effect = Exception("429 Rate limit exceeded")
    with pytest.raises(SourceError, match="rate limit"):
        await provider.fetch_thread("ratelimit_123")


# ---------------------------------------------------------------------------
# 4. Outlook Provider Unit & Conversation Tests
# ---------------------------------------------------------------------------

def test_outlook_provider_missing_credentials_raises_error():
    provider = OutlookProvider()
    with pytest.raises(SourceError, match="Microsoft Graph credentials not configured"):
        provider._get_access_token()


def test_outlook_explicit_access_token_param_and_env(monkeypatch):
    """1. Explicit OUTLOOK_ACCESS_TOKEN via constructor param or env var still works."""
    p1 = OutlookProvider(access_token="param_access_token_123")
    assert p1._get_access_token() == "param_access_token_123"

    monkeypatch.setenv("OUTLOOK_ACCESS_TOKEN", "env_access_token_456")
    p2 = OutlookProvider()
    assert p2._get_access_token() == "env_access_token_456"


def test_outlook_delegated_msal_flow_invoked_when_no_explicit_token(monkeypatch):
    """2 & 3. Delegated MSAL flow is invoked when no explicit token exists and returns token."""
    monkeypatch.delenv("OUTLOOK_ACCESS_TOKEN", raising=False)
    monkeypatch.setenv("OUTLOOK_CLIENT_ID", "test_client_id_001")
    monkeypatch.setenv("OUTLOOK_TENANT_ID", "organizations")

    mock_app = MagicMock()
    mock_app.get_accounts.return_value = []
    mock_app.initiate_device_flow.return_value = {
        "user_code": "MS-CODE-99",
        "verification_uri": "https://microsoft.com/devicelogin",
        "message": "To sign in, open https://microsoft.com/devicelogin and enter code MS-CODE-99",
    }
    mock_app.acquire_token_by_device_flow.return_value = {
        "access_token": "delegated_msal_token_success_777"
    }

    with patch("msal.PublicClientApplication", return_value=mock_app) as mock_pca_cls:
        provider = OutlookProvider()
        token = provider._get_access_token()

        assert token == "delegated_msal_token_success_777"
        mock_pca_cls.assert_called_once()
        _, kwargs = mock_pca_cls.call_args
        assert kwargs["authority"] == "https://login.microsoftonline.com/organizations"
        mock_app.initiate_device_flow.assert_called_once_with(scopes=["Mail.Read"])
        mock_app.acquire_token_by_device_flow.assert_called_once()


def test_outlook_msal_delegated_token_cached_and_silent_acquisition():
    """Delegated token acquisition uses silent cache acquisition when accounts exist."""
    mock_app = MagicMock()
    mock_app.get_accounts.return_value = [{"username": "alex@corp.com"}]
    mock_app.acquire_token_silent.return_value = {
        "access_token": "cached_silent_token_888"
    }

    provider = OutlookProvider(client_id="dummy_client_id", app=mock_app)
    token = provider._get_access_token()

    assert token == "cached_silent_token_888"
    mock_app.acquire_token_silent.assert_called_once_with(["Mail.Read"], account={"username": "alex@corp.com"})
    mock_app.initiate_device_flow.assert_not_called()


def test_outlook_msal_authentication_failure_becomes_source_error(monkeypatch):
    """4. MSAL authentication failure raises SourceError with helpful description."""
    monkeypatch.delenv("OUTLOOK_ACCESS_TOKEN", raising=False)

    # 4a. Failure during initiate_device_flow
    mock_app_fail_init = MagicMock()
    mock_app_fail_init.get_accounts.return_value = []
    mock_app_fail_init.initiate_device_flow.return_value = {
        "error": "invalid_client",
        "error_description": "The client ID is invalid or not registered in Azure AD.",
    }

    provider_fail_init = OutlookProvider(client_id="bad_client_id", app=mock_app_fail_init)
    with pytest.raises(SourceError, match="The client ID is invalid"):
        provider_fail_init._get_access_token()

    # 4b. Failure during acquire_token_by_device_flow
    mock_app_fail_token = MagicMock()
    mock_app_fail_token.get_accounts.return_value = []
    mock_app_fail_token.initiate_device_flow.return_value = {
        "user_code": "CODE-123",
        "message": "Sign in",
    }
    mock_app_fail_token.acquire_token_by_device_flow.return_value = {
        "error": "authorization_declined",
        "error_description": "User cancelled or timed out during device login.",
    }

    provider_fail_token = OutlookProvider(client_id="good_client_id", app=mock_app_fail_token)
    with pytest.raises(SourceError, match="User cancelled or timed out"):
        provider_fail_token._get_access_token()


def test_outlook_no_access_token_printed_or_logged(capsys, caplog):
    """5. Verify that sensitive delegated access token is never printed or logged."""
    secret_token = "SECRET_DELEGATED_ACCESS_TOKEN_VALUE_XYZ_DO_NOT_LEAK"

    mock_app = MagicMock()
    mock_app.get_accounts.return_value = []
    mock_app.initiate_device_flow.return_value = {
        "user_code": "SAFE-CODE-123",
        "message": "Please visit https://microsoft.com/devicelogin and enter code SAFE-CODE-123",
    }
    mock_app.acquire_token_by_device_flow.return_value = {
        "access_token": secret_token
    }

    provider = OutlookProvider(client_id="client_test", app=mock_app)
    token = provider._get_access_token()
    assert token == secret_token

    captured = capsys.readouterr()
    assert "SAFE-CODE-123" in captured.out
    assert secret_token not in captured.out
    assert secret_token not in captured.err
    assert secret_token not in caplog.text


@pytest.mark.asyncio
async def test_outlook_conversation_parsing_and_identity():
    mock_session = MagicMock()
    conv_id = "AAQkAD_conv_outlook_456"

    png_bytes = b"\x89PNG\r\n\x1a\nfake_image_bytes"

    messages_json = {
        "value": [
            {
                "id": "ms_msg_001",
                "conversationId": conv_id,
                "subject": "Q3 Architecture Review",
                "receivedDateTime": "2026-10-04T12:00:00Z",
                "from": {"emailAddress": {"name": "David", "address": "david@corp.com"}},
                "toRecipients": [{"emailAddress": {"name": "Elena", "address": "elena@corp.com"}}],
                "body": {
                    "contentType": "html",
                    "content": "<p>Hello Elena, please review the architecture diagrams.</p>",
                },
                "hasAttachments": False,
            },
            {
                "id": "ms_msg_002",
                "conversationId": conv_id,
                "subject": "Re: Q3 Architecture Review",
                "receivedDateTime": "2026-10-04T12:45:00Z",
                "from": {"emailAddress": {"name": "Elena", "address": "elena@corp.com"}},
                "toRecipients": [{"emailAddress": {"name": "David", "address": "david@corp.com"}}],
                "body": {
                    "contentType": "text",
                    "content": "Reviewed and approved. Diagram attached.",
                },
                "hasAttachments": True,
            },
        ]
    }

    attachments_json = {
        "value": [
            {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "id": "att_01",
                "name": "architecture_diagram.png",
                "contentType": "image/png",
                "contentBytes": b64std(png_bytes),
                "isInline": False,
            }
        ]
    }

    def mock_get(url, **kwargs):
        if "me/messages" in url and "attachments" not in url:
            return MockHttpResponse(messages_json, status_code=200)
        elif "attachments" in url:
            return MockHttpResponse(attachments_json, status_code=200)
        return MockHttpResponse(status_code=404)

    mock_session.get.side_effect = mock_get

    provider = OutlookProvider(session=mock_session, access_token="mock_token_123")
    thread = await provider.fetch_thread(conv_id)

    assert thread.thread_id == conv_id
    assert thread.provider == "outlook"
    assert thread.subject == "Q3 Architecture Review"
    assert len(thread.messages) == 2

    # Participants
    assert "David <david@corp.com>" in thread.participants
    assert "Elena <elena@corp.com>" in thread.participants

    # Message 2 attachments
    m2 = thread.messages[1]
    assert len(m2.attachments) == 1
    assert m2.attachments[0].filename == "architecture_diagram.png"
    assert m2.attachments[0].content == png_bytes


@pytest.mark.asyncio
async def test_outlook_error_handling():
    mock_session = MagicMock()
    provider = OutlookProvider(session=mock_session, access_token="mock_token_123")

    # 401 Unauthorized
    mock_session.get.return_value = MockHttpResponse(status_code=401)
    with pytest.raises(SourceError, match="authentication or permission denied"):
        await provider.fetch_thread("test_conv")

    # 429 Rate Limit
    mock_session.get.return_value = MockHttpResponse(status_code=429)
    with pytest.raises(SourceError, match="rate limit"):
        await provider.fetch_thread("test_conv")


# ---------------------------------------------------------------------------
# 5. End-to-End Pipeline & Deduplication Tests (Section 17 & 18)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gmail_pipeline_end_to_end_and_attachments(tmp_path):
    """Section 17: Mocked Gmail thread end-to-end through IngestionPipeline."""
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    pdf_bytes = b"%PDF-1.4 official sprint spec document"
    pixel_bytes = b"GIF89a"

    thread_id = "gmail_thread_789"
    thread_data = EmailThreadData(
        thread_id=thread_id,
        provider="gmail",
        subject="Q4 Roadmap Discussion",
        messages=[
            EmailMessageData(
                message_id="msg_1",
                sender="Alice <alice@test.com>",
                recipients=["Bob <bob@test.com>"],
                date="2026-10-04 09:30",
                subject="Q4 Roadmap Discussion",
                body_html="<p>Hi Bob, here is the draft roadmap.</p>",
            ),
            EmailMessageData(
                message_id="msg_2",
                sender="Bob <bob@test.com>",
                recipients=["Alice <alice@test.com>"],
                date="2026-10-04 10:15",
                subject="Re: Q4 Roadmap Discussion",
                body_text="Thanks Alice! I have attached the specifications.",
                attachments=[
                    EmailAttachmentData(
                        filename="roadmap_spec.pdf",
                        content=pdf_bytes,
                        mime_type="application/pdf",
                    ),
                    EmailAttachmentData(
                        filename="pixel.gif",
                        content=pixel_bytes,
                        mime_type="image/gif",
                        is_inline=True,
                    ),
                ],
            ),
        ],
        participants=["Alice <alice@test.com>", "Bob <bob@test.com>"],
    )

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = thread_data

    source = EmailSource(providers={"gmail": mock_provider}, active_provider="gmail")
    items = await source.fetch_items(thread_id=thread_id)
    assert len(items) == 1

    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW
    assert rel_path is not None
    assert "Ingested/Email" in rel_path or "Ingested\\Email" in rel_path

    # Verify Note on Disk
    note_path = vault_dir / rel_path
    assert note_path.exists()
    note_content = note_path.read_text(encoding="utf-8")

    # Frontmatter assertions
    assert "source: email" in note_content
    assert "thread_id: gmail_thread_789" in note_content
    assert "provider: gmail" in note_content
    assert "roadmap_spec.pdf" in note_content
    assert "pixel.gif" not in note_content  # Tracking pixel filtered

    # Note Body assertions
    assert "# Q4 Roadmap Discussion" in note_content
    assert "## Thread Information" in note_content
    assert "## Message 1" in note_content
    assert "Hi Bob, here is the draft roadmap." in note_content
    assert "## Message 2" in note_content
    assert "Thanks Alice! I have attached the specifications." in note_content
    assert "## Attachments" in note_content
    assert "![[roadmap_spec.pdf]]" in note_content

    # Verify Attachment on Disk
    att_path = vault_dir / "Attachments" / "Ingested" / "roadmap_spec.pdf"
    assert att_path.exists()
    assert att_path.read_bytes() == pdf_bytes

    # Tracker assertion
    record = tracker.get_record("email", f"email:gmail:{thread_id}")
    assert record is not None
    assert record.vault_path == rel_path

    tracker.close()


@pytest.mark.asyncio
async def test_outlook_pipeline_end_to_end_and_attachments(tmp_path):
    """End-to-end Outlook pipeline test."""
    vault_dir = tmp_path / "vault_outlook"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_outlook.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    conv_id = "outlook_conv_999"
    img_bytes = b"\x89PNG\r\n\x1a\n" + b"X" * 500

    thread_data = EmailThreadData(
        thread_id=conv_id,
        provider="outlook",
        subject="Sync on Database Migration",
        messages=[
            EmailMessageData(
                message_id="m1",
                sender="Sara <sara@ms.com>",
                recipients=["Tim <tim@ms.com>"],
                date="2026-10-04 14:00",
                subject="Sync on Database Migration",
                body_text="Hi Tim, migration architecture is attached.",
                attachments=[
                    EmailAttachmentData(
                        filename="schema_design.png",
                        content=img_bytes,
                        mime_type="image/png",
                    )
                ],
            )
        ],
        participants=["Sara <sara@ms.com>", "Tim <tim@ms.com>"],
    )

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = thread_data

    source = EmailSource(providers={"outlook": mock_provider}, active_provider="outlook")
    items = await source.fetch_items(thread_id=conv_id, provider="outlook")
    assert len(items) == 1

    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW

    note_path = vault_dir / rel_path
    assert note_path.exists()
    content = note_path.read_text(encoding="utf-8")
    assert "source: email" in content
    assert "provider: outlook" in content
    assert "schema_design.png" in content
    assert "![[schema_design.png]]" in content

    att_path = vault_dir / "Attachments" / "Ingested" / "schema_design.png"
    assert att_path.exists()
    assert att_path.read_bytes() == img_bytes

    tracker.close()


@pytest.mark.asyncio
async def test_email_pipeline_deduplication_and_update(tmp_path):
    """Section 18: First run -> NEW; same content -> UNCHANGED; new message -> CHANGED in-place."""
    vault_dir = tmp_path / "vault_dedup"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_dedup.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    thread_id = "thread_dedup_001"

    # 1. First run with 1 message
    t1 = EmailThreadData(
        thread_id=thread_id,
        provider="gmail",
        subject="Project Alpha Launch",
        messages=[
            EmailMessageData(
                message_id="msg_1",
                sender="Leader <leader@team.com>",
                recipients=["Dev <dev@team.com>"],
                date="2026-10-04 09:00",
                subject="Project Alpha Launch",
                body_text="Initial launch plan message.",
            )
        ],
        participants=["Leader <leader@team.com>", "Dev <dev@team.com>"],
    )

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = t1

    source = EmailSource(providers={"gmail": mock_provider}, active_provider="gmail")
    items1 = await source.fetch_items(thread_id=thread_id)

    action1, rel_path1 = await pipeline.process_item(source, items1[0])
    assert action1 == IngestionAction.NEW
    note_path = vault_dir / rel_path1
    assert "Initial launch plan message." in note_path.read_text(encoding="utf-8")

    # 2. Same thread, unchanged content -> UNCHANGED (skip)
    items_same = await source.fetch_items(thread_id=thread_id)
    action2, rel_path2 = await pipeline.process_item(source, items_same[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1

    # 3. Same thread with a NEW reply message -> CHANGED (overwrites note in place)
    t2 = EmailThreadData(
        thread_id=thread_id,
        provider="gmail",
        subject="Project Alpha Launch",
        messages=[
            t1.messages[0],
            EmailMessageData(
                message_id="msg_2",
                sender="Dev <dev@team.com>",
                recipients=["Leader <leader@team.com>"],
                date="2026-10-04 09:45",
                subject="Re: Project Alpha Launch",
                body_text="All systems green for launch.",
            ),
        ],
        participants=t1.participants,
    )
    mock_provider.fetch_thread.return_value = t2

    items_updated = await source.fetch_items(thread_id=thread_id)
    action3, rel_path3 = await pipeline.process_item(source, items_updated[0])

    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1  # Must be the exact SAME file path, no _2 created!

    updated_note_content = note_path.read_text(encoding="utf-8")
    assert "Initial launch plan message." in updated_note_content
    assert "All systems green for launch." in updated_note_content
    assert "## Message 2" in updated_note_content

    # Ensure no duplicate _2 note exists in folder
    email_notes = list((vault_dir / "Ingested" / "Email").glob("*.md"))
    assert len(email_notes) == 1

    tracker.close()


@pytest.mark.asyncio
async def test_email_attachment_collision_handling(tmp_path):
    """Test that colliding email attachment names are saved cleanly with _2 suffixes."""
    vault_dir = tmp_path / "vault_coll"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_coll.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    # Pre-create an attachment with same filename but different content
    att_dir = vault_dir / "Attachments" / "Ingested"
    att_dir.mkdir(parents=True)
    existing_file = att_dir / "report.pdf"
    existing_file.write_bytes(b"existing PDF bytes")

    incoming_bytes = b"NEW different email report PDF bytes"
    thread = EmailThreadData(
        thread_id="thread_coll_01",
        provider="gmail",
        subject="Weekly Report",
        messages=[
            EmailMessageData(
                message_id="m1",
                sender="Reporter <r@co.com>",
                recipients=["Boss <b@co.com>"],
                date="2026-10-04 10:00",
                subject="Weekly Report",
                body_text="See report attached.",
                attachments=[
                    EmailAttachmentData(
                        filename="report.pdf",
                        content=incoming_bytes,
                        mime_type="application/pdf",
                    )
                ],
            )
        ],
        participants=["Reporter <r@co.com>"],
    )

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = thread

    source = EmailSource(providers={"gmail": mock_provider}, active_provider="gmail")
    items = await source.fetch_items(thread_id="thread_coll_01")
    action, rel_path = await pipeline.process_item(source, items[0])

    assert action == IngestionAction.NEW

    # The original file is preserved
    assert existing_file.read_bytes() == b"existing PDF bytes"

    # Collision-safe file created
    coll_file = att_dir / "report_2.pdf"
    assert coll_file.exists()
    assert coll_file.read_bytes() == incoming_bytes

    # Markdown note references report_2.pdf
    note_content = (vault_dir / rel_path).read_text(encoding="utf-8")
    assert "![[report_2.pdf]]" in note_content
    assert "- report_2.pdf" in note_content

    tracker.close()


# ---------------------------------------------------------------------------
# 6. CLI Command Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cli_ingest_email_gmail(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault_gmail"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker_gmail.sqlite"

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = EmailThreadData(
        thread_id="thread_cli_01",
        provider="gmail",
        subject="CLI Gmail Test",
        messages=[
            EmailMessageData(
                message_id="m1",
                sender="User <user@test.com>",
                recipients=["Team <team@test.com>"],
                date="2026-10-04 15:00",
                subject="CLI Gmail Test",
                body_text="Testing ingest-email command.",
            )
        ],
        participants=["User <user@test.com>"],
    )

    with patch.object(EmailSource, "__init__", lambda self: None):
        source = EmailSource()
        source.source_type = "email"
        source.display_name = "Email"
        source.providers = {"gmail": mock_provider}
        source.active_provider = "gmail"

        with patch("sources.base.SourceRegistry.create", return_value=source):
            parser = create_parser()
            args = parser.parse_args([
                "--vault-path", str(vault_dir),
                "--tracker-db", str(tracker_path),
                "ingest-email",
                "--provider", "gmail",
                "--thread-id", "thread_cli_01",
            ])

            ret = await async_main(args)
            assert ret == 0

            captured = capsys.readouterr()
            assert "Ingestion completed for 'email'" in captured.out

            notes = list((vault_dir / "Ingested" / "Email").glob("*.md"))
            assert len(notes) == 1
            assert "Testing ingest-email command." in notes[0].read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_cli_ingest_email_outlook(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault_outlook"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker_outlook.sqlite"

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = EmailThreadData(
        thread_id="conv_cli_02",
        provider="outlook",
        subject="CLI Outlook Test",
        messages=[
            EmailMessageData(
                message_id="m1",
                sender="User <user@test.com>",
                recipients=["Team <team@test.com>"],
                date="2026-10-04 15:30",
                subject="CLI Outlook Test",
                body_text="Testing Outlook CLI ingestion.",
            )
        ],
        participants=["User <user@test.com>"],
    )

    with patch.object(EmailSource, "__init__", lambda self: None):
        source = EmailSource()
        source.source_type = "email"
        source.display_name = "Email"
        source.providers = {"outlook": mock_provider}
        source.active_provider = "outlook"

        with patch("sources.base.SourceRegistry.create", return_value=source):
            parser = create_parser()
            args = parser.parse_args([
                "--vault-path", str(vault_dir),
                "--tracker-db", str(tracker_path),
                "ingest-email",
                "--provider", "outlook",
                "--thread-id", "conv_cli_02",
            ])

            ret = await async_main(args)
            assert ret == 0

            captured = capsys.readouterr()
            assert "Ingestion completed for 'email'" in captured.out


@pytest.mark.asyncio
async def test_cli_generic_ingest_email(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault_gen"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker_gen.sqlite"

    mock_provider = MagicMock(spec=EmailProvider)
    mock_provider.fetch_thread.return_value = EmailThreadData(
        thread_id="thread_gen_03",
        provider="gmail",
        subject="Generic CLI Ingest",
        messages=[
            EmailMessageData(
                message_id="m1",
                sender="User <user@test.com>",
                recipients=["Team <team@test.com>"],
                date="2026-10-04 16:00",
                subject="Generic CLI Ingest",
                body_text="Testing generic ingest CLI.",
            )
        ],
        participants=["User <user@test.com>"],
    )

    with patch.object(EmailSource, "__init__", lambda self: None):
        source = EmailSource()
        source.source_type = "email"
        source.display_name = "Email"
        source.providers = {"gmail": mock_provider}
        source.active_provider = "gmail"

        with patch("sources.base.SourceRegistry.create", return_value=source):
            parser = create_parser()
            args = parser.parse_args([
                "--vault-path", str(vault_dir),
                "--tracker-db", str(tracker_path),
                "ingest",
                "--source", "email",
                "--provider", "gmail",
                "--thread-id", "thread_gen_03",
            ])

            ret = await async_main(args)
            assert ret == 0

            captured = capsys.readouterr()
            assert "Ingestion completed for 'email'" in captured.out


@pytest.mark.asyncio
async def test_email_unsupported_provider_fails():
    source = EmailSource()
    with pytest.raises(SourceError, match="Unsupported email provider"):
        await source.fetch_items(provider="yahoo", thread_id="thread_123")
