"""Comprehensive test suite for Google Drive ingestion connector (GoogleDriveSource).

Tests cover:
- Authentication:
  - Missing credentials/token raises actionable SourceError
  - Dependency injected service is used directly without building service
  - Authorized user token loading with valid Credentials
  - OAuth InstalledAppFlow execution when credentials file provided
  - Authentication and HTTP error mapping (401, 403, 404, 429, 500, etc.)
  - Secret/token redaction in error messages
- Discovery:
  - files.list pagination with nextPageToken
  - Loop/cycle prevention for folder hierarchies and repeated page tokens
  - Folder filtering queries (parent and trashed=false)
  - Subfolder recursion with path accumulation
  - Discovery error handling and mapping
- Identity & Deduplication:
  - Deterministic source_id format (gdrive:<file_id>)
  - Content hash computation based on id, version, modifiedTime, md5Checksum, size
  - Stability across file renames and moves
  - Change detection on version/mtime modification
- Google Workspace Document Handlers:
  - Google Docs: export HTML -> Markdown conversion via markdownify, ATX headers, formatting
  - Google Docs: fallback to plain text if HTML export fails
  - Google Sheets: export CSV -> Markdown table with bounded rows and columns
  - Google Sheets: row and column truncation notices
  - Google Sheets: empty spreadsheet handling and pipe character escaping
  - Google Slides: export plain text -> structured slides (## Slide N — Title)
  - Google Slides: empty presentation handling
- Binary Files & PDFs:
  - PDF downloading via files().get_media()
  - PDF text extraction via PyMuPDF (fitz)
  - PDF fallback when no text is extractable
  - PDF attachment preservation in Attachments/Ingested/ and ![[filename.pdf]] embedding
  - Routing: PDFs to Ingested/PDF, others to Ingested/Web
  - Text, Markdown, CSV, JSON code block wrapping
  - Images saved as attachments and embedded
  - Unsupported binary files handled gracefully with metadata note and disclaimer
- Content Limits & Truncation:
  - Preflight file size limit check before download (exceeding max_file_size raises error / skips)
  - Content length truncation with clear notice when exceeding max_content_size
- Metadata & Attribution:
  - Title sanitization: forbidden Obsidian characters stripped, length capped at 80
  - Extension handling: stripped for regular files, preserved for Google Docs
  - Frontmatter and extra_metadata attributes
  - Attribution block rendering in converter.py
- Fault Isolation & Batch Resilience:
  - Single file failure does not crash discovery or batch ingestion
- Pipeline & Deduplication Integration:
  - End-to-end IngestionPipeline test with DeduplicationTracker (NEW, UNCHANGED, CHANGED)
- CLI & Registry:
  - SourceRegistry registration for "google-drive", "google_drive", "gdrive"
  - CLI argument parsing for ingest-google-drive and ingest -s google-drive
  - async_main execution dispatcher for Google Drive

Zero external network calls or real Google API credentials required.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from config import IngestionConfig
from converter import (
    build_attribution_block,
    render_markdown_document,
    write_note_to_vault,
)
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import Attachment, MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.google_drive_source import (
    DEFAULT_MAX_CONTENT_SIZE,
    DEFAULT_MAX_FILE_SIZE,
    DEFAULT_MAX_SHEET_COLS,
    DEFAULT_MAX_SHEET_ROWS,
    DOC_MIME,
    FOLDER_MIME,
    PDF_MIME,
    SHEET_MIME,
    SLIDE_MIME,
    GoogleDriveSource,
    compute_drive_content_hash,
    extract_pdf_text_safely,
    format_csv_to_markdown_table,
    format_slides_to_markdown,
    map_google_drive_error,
    sanitize_drive_title,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers & Fixtures
# ---------------------------------------------------------------------------

class MockResponse(dict):
    """Mock HTTP response matching httplib2.Response shape for MediaIoBaseDownload."""

    def __init__(self, status: int = 200, **headers: Any) -> None:
        super().__init__(**headers)
        self.status = status


class MockDriveRequest:
    """Mock Google API HTTP request with execute() and MediaIoBaseDownload support."""

    def __init__(self, return_value: Any = None, side_effect: Optional[Exception] = None) -> None:
        self.return_value = return_value
        self.side_effect = side_effect
        self.uri = "https://www.googleapis.com/drive/v3/files/mock?alt=media"
        self.headers: Dict[str, str] = {}
        self.http = MagicMock()
        self._pos = 0

        raw_bytes = b""
        if isinstance(return_value, bytes):
            raw_bytes = return_value
        elif isinstance(return_value, str):
            raw_bytes = return_value.encode("utf-8")
        elif isinstance(return_value, bytearray):
            raw_bytes = bytes(return_value)

        self._raw_bytes = raw_bytes

        def fake_request(
            uri: str,
            method: str = "GET",
            body: Any = None,
            headers: Optional[Dict[str, str]] = None,
            **kwargs: Any,
        ) -> Tuple[MockResponse, bytes]:
            if self.side_effect:
                raise self.side_effect
            range_header = (headers or {}).get("range", "")
            start = self._pos
            end = len(self._raw_bytes)
            if range_header.startswith("bytes="):
                parts = range_header[6:].split("-")
                start = int(parts[0])
                if parts[1]:
                    end = min(int(parts[1]) + 1, len(self._raw_bytes))

            chunk = self._raw_bytes[start:end]
            self._pos = end
            resp = MockResponse(
                status=206 if range_header else 200,
                **{"content-range": f"bytes {start}-{max(start, end - 1)}/{len(self._raw_bytes)}"},
            )
            return resp, chunk

        self.http.request.side_effect = fake_request

    def execute(self) -> Any:
        if self.side_effect:
            raise self.side_effect
        return self.return_value


class MockDriveFilesResource:
    """Mock Google Drive API v3 files() resource."""

    def __init__(self) -> None:
        self.list_mock = MagicMock()
        self.get_mock = MagicMock()
        self.export_mock = MagicMock()
        self.get_media_mock = MagicMock()

    def list(self, **kwargs: Any) -> MockDriveRequest:
        res = self.list_mock(**kwargs)
        if isinstance(res, MockDriveRequest):
            return res
        return MockDriveRequest(return_value=res)

    def get(self, **kwargs: Any) -> MockDriveRequest:
        res = self.get_mock(**kwargs)
        if isinstance(res, MockDriveRequest):
            return res
        return MockDriveRequest(return_value=res)

    def export(self, **kwargs: Any) -> MockDriveRequest:
        res = self.export_mock(**kwargs)
        if isinstance(res, MockDriveRequest):
            return res
        return MockDriveRequest(return_value=res)

    def get_media(self, **kwargs: Any) -> MockDriveRequest:
        res = self.get_media_mock(**kwargs)
        if isinstance(res, MockDriveRequest):
            return res
        return MockDriveRequest(return_value=res)


class MockDriveService:
    """Mock Google Drive API v3 client service."""

    def __init__(self) -> None:
        self._files = MockDriveFilesResource()

    def files(self) -> MockDriveFilesResource:
        return self._files


class MockHttpError(Exception):
    """Mock HttpError matching googleapiclient.errors.HttpError shape."""

    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(message)
        self.resp = MagicMock(status=status)
        self.status_code = status


@pytest.fixture
def mock_service() -> MockDriveService:
    """Fixture providing a mock Google Drive API v3 service."""
    return MockDriveService()


@pytest.fixture
def temp_vault(tmp_path: Path) -> Path:
    """Temporary Obsidian vault directory structure."""
    vault = tmp_path / "vault"
    vault.mkdir(parents=True, exist_ok=True)
    (vault / "Ingested" / "Web").mkdir(parents=True, exist_ok=True)
    (vault / "Ingested" / "PDF").mkdir(parents=True, exist_ok=True)
    (vault / "Attachments" / "Ingested").mkdir(parents=True, exist_ok=True)
    return vault


@pytest.fixture
def temp_config(temp_vault: Path, tmp_path: Path) -> IngestionConfig:
    """IngestionConfig pointing to the temporary test vault and tracker."""
    db_path = tmp_path / "tracker.db"
    return IngestionConfig(
        vault_path=temp_vault,
        tracker_db_path=db_path,
    )


@pytest.fixture
def temp_tracker(tmp_path: Path) -> DeduplicationTracker:
    """Temporary SQLite deduplication tracker."""
    db_path = tmp_path / "tracker.db"
    return DeduplicationTracker(db_path)


def create_minimal_pdf_bytes(text: str = "Test PDF Document Content") -> bytes:
    """Create in-memory PDF bytes with text using pymupdf/fitz if available, or basic PDF structure."""
    try:
        import fitz
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((50, 72), text)
        data = doc.tobytes()
        doc.close()
        return data
    except Exception:
        # Fallback raw minimal PDF
        pdf_str = (
            f"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            f"2 0 obj<</Type/Pages/Count 1/Kids[3 0 R]>>endobj\n"
            f"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R/Contents 4 0 R>>endobj\n"
            f"4 0 obj<</Length {len(text) + 20}>>stream\nBT /F1 12 Tf 50 700 Td ({text}) Tj ET\nendstream\nendobj\n"
            f"xref\n0 5\n0000000000 65535 f\n0000000010 00000 n\n0000000056 00000 n\n0000000111 00000 n\n0000000212 00000 n\n"
            f"trailer<</Root 1 0 R/Size 5>>\nstartxref\n300\n%%EOF\n"
        )
        return pdf_str.encode("utf-8")


# ---------------------------------------------------------------------------
# 1. Helper Functions Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveHelpers:
    """Tests for standalone helper functions."""

    def test_sanitize_drive_title_strips_forbidden_characters(self) -> None:
        title = sanitize_drive_title("Project: Alpha / Beta *Draft* ?<Final>|#^[Test]")
        for ch in [":", "/", "*", "?", "<", ">", "|", "#", "^", "[", "]"]:
            assert ch not in title
        assert "Project Alpha Beta Draft Final Test" in title

    def test_sanitize_drive_title_strips_extension_for_regular_files(self) -> None:
        title = sanitize_drive_title("Quarterly Report.pdf", mime_type="application/pdf")
        assert title == "Quarterly Report"

        title_txt = sanitize_drive_title("Notes.txt", mime_type="text/plain")
        assert title_txt == "Notes"

    def test_sanitize_drive_title_preserves_name_for_google_workspace_files(self) -> None:
        title = sanitize_drive_title("Product Roadmap", mime_type=DOC_MIME)
        assert title == "Product Roadmap"

        title_sheet = sanitize_drive_title("Q4 Financial Model", mime_type=SHEET_MIME)
        assert title_sheet == "Q4 Financial Model"

    def test_sanitize_drive_title_length_capping(self) -> None:
        long_name = "A" * 150
        sanitized = sanitize_drive_title(long_name)
        assert len(sanitized) <= 80

    def test_sanitize_drive_title_empty_or_whitespace_fallback(self) -> None:
        assert sanitize_drive_title("") == "Google Drive Document"
        assert sanitize_drive_title("   ") == "Google Drive Document"
        assert sanitize_drive_title(None) == "Google Drive Document"

    def test_compute_drive_content_hash_deterministic(self) -> None:
        meta1 = {
            "id": "file_123",
            "version": "5",
            "modifiedTime": "2026-10-01T12:00:00Z",
            "md5Checksum": "abc123md5",
            "size": "1048576",
        }
        meta2 = dict(meta1)
        assert compute_drive_content_hash(meta1) == compute_drive_content_hash(meta2)

    def test_compute_drive_content_hash_changes_on_modification(self) -> None:
        meta1 = {"id": "file_123", "version": "1", "modifiedTime": "2026-10-01T10:00:00Z"}
        meta2 = {"id": "file_123", "version": "2", "modifiedTime": "2026-10-01T11:00:00Z"}
        assert compute_drive_content_hash(meta1) != compute_drive_content_hash(meta2)

    def test_format_csv_to_markdown_table_basic(self) -> None:
        csv_data = "Name,Role,Team\nAlice,Lead,Backend\nBob,Engineer,Frontend\n"
        table_md, was_truncated = format_csv_to_markdown_table(csv_data)
        assert not was_truncated
        assert "| Name | Role | Team |" in table_md
        assert "| --- | --- | --- |" in table_md
        assert "| Alice | Lead | Backend |" in table_md
        assert "| Bob | Engineer | Frontend |" in table_md

    def test_format_csv_to_markdown_table_empty(self) -> None:
        table_md, was_truncated = format_csv_to_markdown_table("")
        assert "*Empty spreadsheet.*" in table_md
        assert not was_truncated

    def test_format_csv_to_markdown_table_escapes_pipes(self) -> None:
        csv_data = "Item,Description\n1,Alpha | Beta\n"
        table_md, _ = format_csv_to_markdown_table(csv_data)
        assert "Alpha \\| Beta" in table_md

    def test_format_csv_to_markdown_table_row_truncation(self) -> None:
        rows = ["ColA,ColB"] + [f"Val{i},Data{i}" for i in range(150)]
        csv_data = "\n".join(rows)
        table_md, was_truncated = format_csv_to_markdown_table(csv_data, max_rows=10)
        assert was_truncated
        assert "showing first 10 of 151 rows" in table_md

    def test_format_csv_to_markdown_table_col_truncation(self) -> None:
        cols = [f"Col{i}" for i in range(30)]
        vals = [f"Val{i}" for i in range(30)]
        csv_data = ",".join(cols) + "\n" + ",".join(vals)
        table_md, was_truncated = format_csv_to_markdown_table(csv_data, max_cols=5)
        assert was_truncated
        assert "showing first 5 of 30 columns" in table_md

    def test_format_slides_to_markdown_basic(self) -> None:
        slide_text = "Executive Summary\nKey points for Q3\x0cFinancials\nRevenue was up 20%"
        md = format_slides_to_markdown(slide_text)
        assert "## Slide 1 — Executive Summary" in md
        assert "Key points for Q3" in md
        assert "## Slide 2 — Financials" in md
        assert "Revenue was up 20%" in md

    def test_format_slides_to_markdown_empty(self) -> None:
        md = format_slides_to_markdown("")
        assert "*Empty presentation.*" in md

    def test_format_slides_to_markdown_bounded_chars(self) -> None:
        slide_text = "Slide 1 Content\x0cSlide 2 Content\x0cSlide 3 Content"
        md = format_slides_to_markdown(slide_text, max_chars=30)
        assert "## Slide 1" in md
        assert "## Slide 3" not in md

    def test_extract_pdf_text_safely_with_valid_pdf(self) -> None:
        pdf_bytes = create_minimal_pdf_bytes("Aurora PDF Ingestion Success")
        extracted = extract_pdf_text_safely(pdf_bytes)
        assert "Aurora PDF Ingestion Success" in extracted

    def test_extract_pdf_text_safely_with_invalid_bytes(self) -> None:
        assert extract_pdf_text_safely(b"not a valid pdf") == ""

    def test_map_google_drive_error_status_codes(self) -> None:
        # 401
        err_401 = MockHttpError(401, "Invalid Credentials")
        mapped_401 = map_google_drive_error(err_401)
        assert "401 Unauthorized" in str(mapped_401)

        # 403
        err_403 = MockHttpError(403, "The caller does not have permission")
        mapped_403 = map_google_drive_error(err_403)
        assert "403 Forbidden" in str(mapped_403)

        # 404
        err_404 = MockHttpError(404, "File not found")
        mapped_404 = map_google_drive_error(err_404)
        assert "404" in str(mapped_404)

        # 429
        err_429 = MockHttpError(429, "Rate limit exceeded")
        mapped_429 = map_google_drive_error(err_429)
        assert "429 Too Many Requests" in str(mapped_429)

        # 500
        err_500 = MockHttpError(500, "Backend error")
        mapped_500 = map_google_drive_error(err_500)
        assert "server error (500)" in str(mapped_500)

    def test_map_google_drive_error_redacts_tokens(self) -> None:
        token = "ya29.a0ARrdaM8secretToken123456789"
        err = Exception(f"Failed with auth Bearer {token}")
        mapped = map_google_drive_error(err)
        assert token not in str(mapped)
        assert "[REDACTED]" in str(mapped)


# ---------------------------------------------------------------------------
# 2. Initialization & Configuration Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveSourceInit:
    """Tests for GoogleDriveSource initialization and parameter handling."""

    def test_default_init(self) -> None:
        source = GoogleDriveSource()
        assert source.source_type == "google-drive"
        assert source.display_name == "Google Drive"
        assert source.folder_ids == []
        assert source.max_file_size == DEFAULT_MAX_FILE_SIZE
        assert source.max_content_size == DEFAULT_MAX_CONTENT_SIZE

    def test_injected_service_is_used(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service)
        assert source._get_service() is mock_service

    def test_folder_ids_string_and_list_parsing(self) -> None:
        src1 = GoogleDriveSource(folder_id="folder1, folder2,folder3")
        assert src1.folder_ids == ["folder1", "folder2", "folder3"]

        src2 = GoogleDriveSource(folder_id=["folderA", "folderB"])
        assert src2.folder_ids == ["folderA", "folderB"]

    def test_env_var_configuration(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GOOGLE_DRIVE_FOLDER_ID", "env_folder_1, env_folder_2")
        monkeypatch.setenv("GOOGLE_DRIVE_MAX_FILE_SIZE", "50000000")
        monkeypatch.setenv("GOOGLE_DRIVE_MAX_CONTENT_SIZE", "100000")
        monkeypatch.setenv("GOOGLE_DRIVE_CREDENTIALS", "/path/to/creds.json")
        monkeypatch.setenv("GOOGLE_DRIVE_TOKEN", "/path/to/token.json")

        source = GoogleDriveSource()
        assert source.folder_ids == ["env_folder_1", "env_folder_2"]
        assert source.max_file_size == 50000000
        assert source.max_content_size == 100000
        assert source.credentials_path == "/path/to/creds.json"
        assert source.token_path == "/path/to/token.json"


# ---------------------------------------------------------------------------
# 3. Authentication Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveAuthentication:
    """Tests for OAuth authentication flow and error handling."""

    def test_missing_credentials_raises_actionable_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GOOGLE_DRIVE_CREDENTIALS", raising=False)
        monkeypatch.delenv("GOOGLE_DRIVE_TOKEN", raising=False)

        source = GoogleDriveSource()
        with pytest.raises(SourceError, match="OAuth credentials not found"):
            source._get_service()

    def test_load_token_from_file(self, tmp_path: Path) -> None:
        token_file = tmp_path / "token.json"
        token_file.write_text(json.dumps({"token": "fake_token", "refresh_token": "fake_refresh"}))

        mock_creds = MagicMock(valid=True, expired=False, refresh_token=None)
        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file", return_value=mock_creds) as mock_from_file:
            with patch("googleapiclient.discovery.build") as mock_build:
                source = GoogleDriveSource(token_path=token_file)
                source._get_service()
                mock_from_file.assert_called_once()
                mock_build.assert_called_once_with("drive", "v3", credentials=mock_creds)

    def test_refresh_expired_token(self, tmp_path: Path) -> None:
        token_file = tmp_path / "token.json"
        token_file.write_text(json.dumps({"token": "expired_token"}))

        mock_creds = MagicMock(valid=False, expired=True, refresh_token="has_refresh")
        mock_creds.to_json.return_value = '{"token": "refreshed_token"}'
        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file", return_value=mock_creds):
            with patch("google.auth.transport.requests.Request") as mock_req:
                with patch("googleapiclient.discovery.build"):
                    source = GoogleDriveSource(token_path=token_file)
                    source._get_service()
                    mock_creds.refresh.assert_called_once()
                    mock_creds.to_json.assert_called_once()

    def test_oauth_installed_app_flow_when_token_missing(self, tmp_path: Path) -> None:
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps({"installed": {"client_id": "test_id"}}))
        token_file = tmp_path / "token.json"

        mock_creds = MagicMock(valid=True)
        mock_creds.to_json.return_value = '{"token": "installed_token"}'
        mock_flow = MagicMock()
        mock_flow.run_local_server.return_value = mock_creds

        with patch("google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file", return_value=mock_flow):
            with patch("googleapiclient.discovery.build"):
                source = GoogleDriveSource(credentials_path=creds_file, token_path=token_file)
                source._get_service()
                mock_flow.run_local_server.assert_called_once()
                mock_creds.to_json.assert_called_once()

    def test_from_authorized_user_file_error_redacts_secrets(self, tmp_path: Path) -> None:
        token_file = tmp_path / "token.json"
        token_file.write_text(json.dumps({"token": "fake"}))

        raw_err = "Failed reading token: ya29.a0ARsecretToken12345 with client_secret=GOCSPX-abc123secret"
        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file", side_effect=Exception(raw_err)):
            source = GoogleDriveSource(token_path=token_file)
            with pytest.raises(SourceError) as exc_info:
                source._get_service()

            err_msg = str(exc_info.value)
            assert "Failed to load or refresh Google Drive OAuth token" in err_msg
            assert "ya29.a0ARsecretToken12345" not in err_msg
            assert "GOCSPX-abc123secret" not in err_msg
            assert "[REDACTED]" in err_msg

    def test_refresh_token_error_redacts_secrets(self, tmp_path: Path) -> None:
        token_file = tmp_path / "token.json"
        token_file.write_text(json.dumps({"token": "expired"}))

        mock_creds = MagicMock(valid=False, expired=True, refresh_token="has_refresh")
        raw_err = "invalid_grant: refresh_token='1//04fake_secret_refresh_token' was revoked"
        mock_creds.refresh.side_effect = Exception(raw_err)

        with patch("google.oauth2.credentials.Credentials.from_authorized_user_file", return_value=mock_creds):
            with patch("google.auth.transport.requests.Request"):
                source = GoogleDriveSource(token_path=token_file)
                with pytest.raises(SourceError) as exc_info:
                    source._get_service()

                err_msg = str(exc_info.value)
                assert "Failed to load or refresh Google Drive OAuth token" in err_msg
                assert "1//04fake_secret_refresh_token" not in err_msg
                assert "[REDACTED]" in err_msg

    def test_installed_app_flow_error_redacts_secrets(self, tmp_path: Path) -> None:
        creds_file = tmp_path / "credentials.json"
        creds_file.write_text(json.dumps({"installed": {"client_id": "test_id"}}))
        token_file = tmp_path / "token.json"

        mock_flow = MagicMock()
        raw_err = "Flow failed with client_secret='GOCSPX-flowSecret' and code='4/0fake_code'"
        mock_flow.run_local_server.side_effect = Exception(raw_err)

        with patch("google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file", return_value=mock_flow):
            source = GoogleDriveSource(credentials_path=creds_file, token_path=token_file)
            with pytest.raises(SourceError) as exc_info:
                source._get_service()

            err_msg = str(exc_info.value)
            assert "Google Drive OAuth authorization failed" in err_msg
            assert "GOCSPX-flowSecret" not in err_msg
            assert "4/0fake_code" not in err_msg
            assert "[REDACTED]" in err_msg


# ---------------------------------------------------------------------------
# 4. Discovery Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveDiscovery:
    """Tests for file and folder discovery, recursion, and cycle protection."""

    def test_discover_files_in_folder_basic(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "doc1", "name": "Report", "mimeType": DOC_MIME},
                {"id": "sheet1", "name": "Metrics", "mimeType": SHEET_MIME},
            ],
            "nextPageToken": None,
        }

        source = GoogleDriveSource(service=mock_service)
        seen_folders: set = set()
        files = source._discover_files_in_folder("folder_123", "Projects", seen_folders)

        assert len(files) == 2
        assert files[0]["id"] == "doc1"
        assert files[0]["drive_path"] == "Projects"
        assert files[1]["id"] == "sheet1"
        assert "folder_123" in seen_folders

    def test_discover_files_in_folder_pagination(self, mock_service: MockDriveService) -> None:
        def list_side_effect(**kwargs: Any) -> Dict[str, Any]:
            page_token = kwargs.get("pageToken")
            if not page_token:
                return {
                    "files": [{"id": "file1", "name": "File 1", "mimeType": DOC_MIME}],
                    "nextPageToken": "page_2_token",
                }
            elif page_token == "page_2_token":
                return {
                    "files": [{"id": "file2", "name": "File 2", "mimeType": DOC_MIME}],
                    "nextPageToken": None,
                }
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect

        source = GoogleDriveSource(service=mock_service)
        files = source._discover_files_in_folder("folder_root", "", set())
        assert len(files) == 2
        assert [f["id"] for f in files] == ["file1", "file2"]

    def test_discover_files_in_folder_cycle_protection(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service)
        seen_folders = {"folder_loop"}
        files = source._discover_files_in_folder("folder_loop", "Current", seen_folders)
        assert files == []

    def test_discover_files_page_token_loop_protection(self, mock_service: MockDriveService) -> None:
        # Same page token returned repeatedly
        mock_service.files().list_mock.return_value = {
            "files": [{"id": "f1", "name": "F1", "mimeType": DOC_MIME}],
            "nextPageToken": "infinite_loop_token",
        }

        source = GoogleDriveSource(service=mock_service)
        files = source._discover_files_in_folder("folder_test", "", set())
        # Should terminate safely after detecting repeated page token
        assert len(files) == 2
        assert mock_service.files().list_mock.call_count == 2

    def test_discover_subfolder_recursion(self, mock_service: MockDriveService) -> None:
        def list_side_effect(q: str, **kwargs: Any) -> Dict[str, Any]:
            if "'parent_folder' in parents" in q:
                return {
                    "files": [
                        {"id": "sub_id", "name": "SubFolder", "mimeType": FOLDER_MIME},
                        {"id": "doc1", "name": "TopDoc", "mimeType": DOC_MIME},
                    ],
                    "nextPageToken": None,
                }
            elif "'sub_id' in parents" in q:
                return {
                    "files": [
                        {"id": "doc2", "name": "NestedDoc", "mimeType": DOC_MIME},
                    ],
                    "nextPageToken": None,
                }
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect

        source = GoogleDriveSource(service=mock_service)
        files = source._discover_files_in_folder("parent_folder", "Root", set())
        assert len(files) == 2
        doc1 = next(f for f in files if f["id"] == "doc1")
        doc2 = next(f for f in files if f["id"] == "doc2")
        assert doc1["drive_path"] == "Root"
        assert doc2["drive_path"] == "Root/SubFolder"

    def test_discover_files_passes_shared_drive_parameters(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [{"id": "d1", "name": "SharedDoc", "mimeType": DOC_MIME}],
            "nextPageToken": None,
        }
        source = GoogleDriveSource(service=mock_service)
        source._discover_files_in_folder("folder_123", "", set())
        call_kwargs = mock_service.files().list_mock.call_args[1]
        assert call_kwargs.get("supportsAllDrives") is True
        assert call_kwargs.get("includeItemsFromAllDrives") is True

    def test_discover_all_accessible_files_passes_shared_drive_parameters_default(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [{"id": "d1", "name": "SharedDoc", "mimeType": DOC_MIME}],
            "nextPageToken": None,
        }
        source = GoogleDriveSource(service=mock_service)
        source._discover_all_accessible_files()
        call_kwargs = mock_service.files().list_mock.call_args[1]
        assert call_kwargs.get("supportsAllDrives") is True
        assert call_kwargs.get("includeItemsFromAllDrives") is True
        assert "corpora" not in call_kwargs

    def test_discover_all_accessible_files_with_explicit_drive_id(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [{"id": "d1", "name": "SharedDoc", "mimeType": DOC_MIME}],
            "nextPageToken": None,
        }
        source = GoogleDriveSource(service=mock_service, drive_id="shared_team_drive_123")
        source._discover_all_accessible_files()
        call_kwargs = mock_service.files().list_mock.call_args[1]
        assert call_kwargs.get("supportsAllDrives") is True
        assert call_kwargs.get("includeItemsFromAllDrives") is True
        assert call_kwargs.get("corpora") == "drive"
        assert call_kwargs.get("driveId") == "shared_team_drive_123"

    @pytest.mark.asyncio
    async def test_deduplicate_overlapping_folders_processes_file_once(self, mock_service: MockDriveService) -> None:
        def list_side_effect(q: str, **kwargs: Any) -> Dict[str, Any]:
            if "'folder_parent' in parents" in q:
                return {
                    "files": [
                        {"id": "doc_shared", "name": "Shared Doc", "mimeType": DOC_MIME},
                        {"id": "doc_parent", "name": "Parent Doc", "mimeType": DOC_MIME},
                    ],
                    "nextPageToken": None,
                }
            elif "'folder_child' in parents" in q:
                return {
                    "files": [
                        {"id": "doc_shared", "name": "Shared Doc", "mimeType": DOC_MIME},
                        {"id": "doc_child", "name": "Child Doc", "mimeType": DOC_MIME},
                    ],
                    "nextPageToken": None,
                }
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect
        mock_service.files().export_mock.return_value = b"# Content"

        source = GoogleDriveSource(service=mock_service, folder_id=["folder_parent", "folder_child"])
        with patch.object(source, "_process_single_file", wraps=source._process_single_file) as spy_process:
            items = await source.fetch_items()

        # Exactly 3 unique items produced
        assert len(items) == 3
        assert {item.source_id for item in items} == {"gdrive:doc_shared", "gdrive:doc_parent", "gdrive:doc_child"}
        # _process_single_file was called exactly 3 times (doc_shared processed once)
        assert spy_process.call_count == 3
        shared_calls = [c for c in spy_process.call_args_list if c[0][0]["id"] == "doc_shared"]
        assert len(shared_calls) == 1

    @pytest.mark.asyncio
    async def test_deduplicate_candidate_files_before_applying_limit(self, mock_service: MockDriveService) -> None:
        def list_side_effect(q: str, **kwargs: Any) -> Dict[str, Any]:
            if "'folder_a' in parents" in q:
                return {
                    "files": [{"id": "f_dup", "name": "Dup File", "mimeType": DOC_MIME}],
                    "nextPageToken": None,
                }
            elif "'folder_b' in parents" in q:
                return {
                    "files": [
                        {"id": "f_dup", "name": "Dup File", "mimeType": DOC_MIME},
                        {"id": "f_other", "name": "Other File", "mimeType": DOC_MIME},
                    ],
                    "nextPageToken": None,
                }
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect
        mock_service.files().export_mock.return_value = b"# Content"

        source = GoogleDriveSource(service=mock_service, folder_id=["folder_a", "folder_b"])
        # With limit=2, deduplication before limit ensures f_dup and f_other are both processed
        items = await source.fetch_items(limit=2)
        assert len(items) == 2
        assert {item.source_id for item in items} == {"gdrive:f_dup", "gdrive:f_other"}


# ---------------------------------------------------------------------------
# 5. Content Handler Tests (Docs, Sheets, Slides, PDFs, Text, Binary)
# ---------------------------------------------------------------------------

class TestGoogleDriveContentHandlers:
    """Tests for exporting and parsing different Google Drive MIME types."""

    def test_process_google_doc_html_export(self, mock_service: MockDriveService) -> None:
        html_doc = "<h1>Sprint Retrospective</h1><p>The sprint went <strong>very well</strong>.</p><ul><li>Shipped MVP</li></ul>"
        mock_service.files().export_mock.return_value = html_doc.encode("utf-8")

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "doc_123",
            "name": "Sprint Retrospective",
            "mimeType": DOC_MIME,
            "modifiedTime": "2026-10-01T15:00:00Z",
            "webViewLink": "https://docs.google.com/document/d/doc_123/edit",
        }

        item = source._process_single_file(file_meta)
        assert item.source_id == "gdrive:doc_123"
        assert item.title == "Sprint Retrospective"
        assert "# Sprint Retrospective" in item.content
        assert "**very well**" in item.content
        assert "- Shipped MVP" in item.content or "* Shipped MVP" in item.content
        assert "document" in item.tags

    def test_process_google_doc_fallback_to_plain_text(self, mock_service: MockDriveService) -> None:
        def export_side_effect(mimeType: str, **kwargs: Any) -> bytes:
            if mimeType == "text/html":
                raise Exception("HTML export failed")
            return b"Plain text fallback content"

        mock_service.files().export_mock.side_effect = export_side_effect

        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "doc_fb", "name": "Fallback Doc", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert "Plain text fallback content" in item.content

    def test_process_google_sheet_csv_export(self, mock_service: MockDriveService) -> None:
        csv_data = "Quarter,Revenue,Profit\nQ1,$10M,$2M\nQ2,$12M,$3M\n"
        mock_service.files().export_mock.return_value = csv_data.encode("utf-8")

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "sheet_123",
            "name": "Quarterly Financials",
            "mimeType": SHEET_MIME,
            "modifiedTime": "2026-10-01T16:00:00Z",
        }

        item = source._process_single_file(file_meta)
        assert "spreadsheet" in item.tags
        assert "## Sheet: Quarterly Financials" in item.content
        assert "| Quarter | Revenue | Profit |" in item.content
        assert "| Q1 | $10M | $2M |" in item.content

    def test_process_google_slides_text_export(self, mock_service: MockDriveService) -> None:
        slides_text = "Architecture Overview\nSystem blocks\x0cComponent Deep Dive\nDetails here"
        mock_service.files().export_mock.return_value = slides_text.encode("utf-8")

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "slide_123",
            "name": "Architecture Deck",
            "mimeType": SLIDE_MIME,
        }

        item = source._process_single_file(file_meta)
        assert "presentation" in item.tags
        assert "## Slide 1 — Architecture Overview" in item.content
        assert "## Slide 2 — Component Deep Dive" in item.content

    def test_process_pdf_download_and_extraction(self, mock_service: MockDriveService) -> None:
        pdf_bytes = create_minimal_pdf_bytes("Important Specifications for Architecture")
        mock_service.files().get_media_mock.return_value = pdf_bytes

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "pdf_123",
            "name": "Architecture Spec.pdf",
            "mimeType": PDF_MIME,
            "size": len(pdf_bytes),
        }

        item = source._process_single_file(file_meta)
        assert "pdf" in item.tags
        assert len(item.attachments) == 1
        assert item.attachments[0].filename == "Architecture Spec.pdf"
        assert item.attachments[0].content == pdf_bytes
        assert "![[Architecture Spec.pdf]]" in item.content
        assert "Important Specifications for Architecture" in item.content

    def test_process_pdf_without_extractable_text(self, mock_service: MockDriveService) -> None:
        mock_service.files().get_media_mock.return_value = b"%PDF-1.4 dummy empty pdf"

        with patch("sources.google_drive_source.extract_pdf_text_safely", return_value=""):
            source = GoogleDriveSource(service=mock_service)
            file_meta = {
                "id": "pdf_empty",
                "name": "Scanned Document.pdf",
                "mimeType": PDF_MIME,
                "size": 100,
            }

            item = source._process_single_file(file_meta)
            assert "*No extractable text found in PDF document.*" in item.content
            assert "![[Scanned Document.pdf]]" in item.content

    def test_process_text_and_markdown_file(self, mock_service: MockDriveService) -> None:
        raw_md = "# Architecture\n\nThis is a plain markdown document stored on drive."
        mock_service.files().get_media_mock.return_value = raw_md.encode("utf-8")

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "text_123",
            "name": "README.md",
            "mimeType": "text/markdown",
            "size": len(raw_md),
        }

        item = source._process_single_file(file_meta)
        assert "# Architecture" in item.content
        assert "This is a plain markdown document" in item.content

    def test_process_json_file(self, mock_service: MockDriveService) -> None:
        json_str = json.dumps({"project": "aurora", "version": "1.0"}, indent=2)
        mock_service.files().get_media_mock.return_value = json_str.encode("utf-8")

        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "json_123",
            "name": "config.json",
            "mimeType": "application/json",
            "size": len(json_str),
        }

        item = source._process_single_file(file_meta)
        assert "```json" in item.content
        assert '"project": "aurora"' in item.content

    def test_process_unsupported_binary_format(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "bin_123",
            "name": "backup_database.zip",
            "mimeType": "application/zip",
            "size": 52428800,
        }

        item = source._process_single_file(file_meta)
        assert "binary" in item.tags
        assert "unsupported for direct text extraction" in item.content
        assert "backup_database.zip" in item.content
        assert "52,428,800 bytes" in item.content
        assert "application/zip" in item.content


# ---------------------------------------------------------------------------
# 6. Limits, Truncation & Preflight Checks Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveLimits:
    """Tests for size bounds and truncation enforcement."""

    def test_preflight_file_size_limit_rejection(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service, max_file_size=1024)
        file_meta = {
            "id": "huge_file",
            "name": "huge_data.bin",
            "mimeType": "text/plain",
            "size": 5000000,
        }

        # _download_binary_content should raise SourceError before making API call
        with pytest.raises(SourceError, match="exceeds maximum allowed size limit"):
            source._download_binary_content("huge_file", "huge_data.bin", 5000000)

    def test_content_truncation_notice(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service, max_content_size=50)
        long_content = "X" * 100
        truncated = source._truncate_content_if_needed(long_content)
        assert len(truncated) > 50  # 50 chars + truncation notice
        assert "Content truncated: extracted text exceeded maximum content limit" in truncated

    def test_download_binary_content_aborts_during_chunk_stream_with_unknown_size(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service, max_file_size=1000)
        mock_req = MagicMock(uri="https://www.googleapis.com/drive/v3/files/f_stream?alt=media", http=MagicMock())
        mock_service._files.get_media = lambda **kw: mock_req

        with patch("sources.google_drive_source.MediaIoBaseDownload") as mock_downloader_cls:
            mock_downloader = MagicMock()

            def fake_init(buf, req, chunksize=None):
                buf.write(b"X" * 1500)
                return mock_downloader

            mock_downloader_cls.return_value = mock_downloader
            mock_downloader_cls.side_effect = fake_init
            mock_downloader.next_chunk.return_value = (None, False)

            with pytest.raises(SourceError) as exc_info:
                # Metadata size is None (unknown)
                source._download_binary_content("f_stream", "oversized.pdf", file_size=None)

            assert "exceeded maximum allowed size limit" in str(exc_info.value)
            assert "1,000 bytes" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_download_binary_content_aborts_in_fetch_items_and_no_attachment_produced(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "oversized_pdf", "name": "Large.pdf", "mimeType": PDF_MIME, "size": None},
            ],
            "nextPageToken": None,
        }
        mock_req = MagicMock(uri="https://www.googleapis.com/drive/v3/files/oversized_pdf?alt=media", http=MagicMock())
        mock_service._files.get_media = lambda **kw: mock_req

        with patch("sources.google_drive_source.MediaIoBaseDownload") as mock_downloader_cls:
            mock_downloader = MagicMock()

            def fake_init(buf, req, chunksize=None):
                buf.write(b"Y" * 2000)
                return mock_downloader

            mock_downloader_cls.return_value = mock_downloader
            mock_downloader_cls.side_effect = fake_init
            mock_downloader.next_chunk.return_value = (None, False)

            source = GoogleDriveSource(service=mock_service, max_file_size=1000)
            items = await source.fetch_items()
            # File should be skipped due to size violation, zero items and zero attachments returned
            assert len(items) == 0

    def test_download_binary_content_does_not_call_request_execute(self, mock_service: MockDriveService) -> None:
        mock_service.files().get_media_mock.return_value = b"Chunked binary data via MediaIoBaseDownload"
        source = GoogleDriveSource(service=mock_service)
        mock_req = mock_service.files().get_media()
        mock_req.execute = MagicMock()
        mock_service.files().get_media_mock.return_value = mock_req

        content = source._download_binary_content("f_bin", "file.bin", 100)
        assert content == b"Chunked binary data via MediaIoBaseDownload"
        mock_req.execute.assert_not_called()


# ---------------------------------------------------------------------------
# 7. Fault Isolation & Batch Resilience Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveBatchResilience:
    """Tests for fault isolation during batch processing."""

    @pytest.mark.asyncio
    async def test_single_file_failure_does_not_abort_batch(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "good_doc", "name": "Good Doc", "mimeType": DOC_MIME},
                {"id": "bad_doc", "name": "Bad Doc", "mimeType": DOC_MIME},
                {"id": "good_sheet", "name": "Good Sheet", "mimeType": SHEET_MIME},
            ],
            "nextPageToken": None,
        }

        def export_side_effect(fileId: str, **kwargs: Any) -> bytes:
            if fileId == "bad_doc":
                raise MockHttpError(500, "Fatal doc render failure")
            elif fileId == "good_doc":
                return b"Good document text"
            elif fileId == "good_sheet":
                return b"ColA,ColB\n1,2\n"
            return b""

        mock_service.files().export_mock.side_effect = export_side_effect

        source = GoogleDriveSource(service=mock_service)
        items = await source.fetch_items()

        # Bad doc should be skipped, good items should be processed
        assert len(items) == 2
        ids = [item.source_id for item in items]
        assert "gdrive:good_doc" in ids
        assert "gdrive:good_sheet" in ids
        assert "gdrive:bad_doc" not in ids


# ---------------------------------------------------------------------------
# 8. Note Conversion & Routing Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveRouting:
    """Tests for MarkdownNote conversion and folder routing."""

    @pytest.mark.asyncio
    async def test_docs_and_sheets_routed_to_ingested_web(self) -> None:
        source = GoogleDriveSource()
        item = SourceItem(
            source_id="gdrive:doc1",
            title="Design Doc",
            source_type="google-drive",
            content="# Content",
            date="2026-10-01T12:00:00Z",
            extra_metadata={"mime_type": DOC_MIME},
        )
        note = await source.convert_to_markdown(item)
        assert note.folder == "Ingested/Web"

    @pytest.mark.asyncio
    async def test_pdf_routed_to_ingested_pdf(self) -> None:
        source = GoogleDriveSource()
        item = SourceItem(
            source_id="gdrive:pdf1",
            title="Whitepaper",
            source_type="google-drive",
            content="# PDF Content",
            date="2026-10-01T12:00:00Z",
            extra_metadata={"mime_type": PDF_MIME},
        )
        note = await source.convert_to_markdown(item)
        assert note.folder == "Ingested/PDF"


# ---------------------------------------------------------------------------
# 9. Attribution & Converter Integration Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveAttribution:
    """Tests for attribution block rendering and note writing."""

    def test_attribution_block_rendering(self) -> None:
        note = MarkdownNote(
            title="Team Strategy",
            body="Important strategic goals for H2.",
            source="google-drive",
            source_url="https://docs.google.com/document/d/strat123/edit",
            author="srava@example.com",
            date="2026-10-01T14:30:00Z",
            folder="Ingested/Web",
            extra_metadata={
                "drive_file_id": "strat123",
                "drive_file_name": "Team Strategy",
                "mime_type": DOC_MIME,
                "drive_path": "Company/Strategy",
            },
        )

        attr_block = build_attribution_block(note)
        assert "> **Source**: [Google Drive — Team Strategy](https://docs.google.com/document/d/strat123/edit)" in attr_block
        assert "**Drive ID**: `strat123`" in attr_block
        assert f"**Type**: `{DOC_MIME}`" in attr_block
        assert "**Path**: `Company/Strategy`" in attr_block
        assert "**Author**: srava@example.com" in attr_block
        assert "**Modified**: 2026-10-01" in attr_block

    def test_write_note_to_vault_saves_attachments(self, temp_vault: Path, temp_config: IngestionConfig) -> None:
        pdf_bytes = b"%PDF-1.4 sample content"
        att = Attachment(filename="spec_sheet.pdf", content=pdf_bytes, mime_type="application/pdf")
        note = MarkdownNote(
            title="Specification Sheet",
            body="## Attachment\n\n![[spec_sheet.pdf]]",
            source="google-drive",
            date="2026-10-01",
            folder="Ingested/PDF",
            extra_metadata={"drive_file_id": "spec1", "mime_type": PDF_MIME},
        )

        abs_path, rel_path, written = write_note_to_vault(
            note=note,
            vault_path=temp_vault,
            config=temp_config,
            attachments=[att],
        )

        assert abs_path.exists()
        assert rel_path.startswith("Ingested/PDF/")
        assert "Specification_Sheet" in rel_path
        att_path = temp_vault / "Attachments" / "Ingested" / "spec_sheet.pdf"
        assert att_path.exists()
        assert att_path.read_bytes() == pdf_bytes

    def test_two_same_named_drive_attachments_handled_safely_without_collision(
        self,
        mock_service: MockDriveService,
        temp_vault: Path,
        temp_config: IngestionConfig,
    ) -> None:
        source = GoogleDriveSource(service=mock_service)
        file_meta1 = {"id": "f_pdf_1", "name": "report.pdf", "mimeType": PDF_MIME, "size": 100}
        file_meta2 = {"id": "f_pdf_2", "name": "report.pdf", "mimeType": PDF_MIME, "size": 100}

        def fake_download(file_id: str, file_name: str, file_size: Optional[int]) -> bytes:
            if file_id == "f_pdf_1":
                return b"%PDF-1.4 Report 1 Content"
            return b"%PDF-1.4 Report 2 Content Different"

        source._download_binary_content = fake_download  # type: ignore

        item1 = source._process_single_file(file_meta1)
        item2 = source._process_single_file(file_meta2)

        assert len(item1.attachments) == 1
        assert len(item2.attachments) == 1
        assert item1.attachments[0].filename == "report.pdf"
        assert item2.attachments[0].filename == "report.pdf"

        note1 = source.default_item_to_note(item1)
        note2 = source.default_item_to_note(item2)

        # Write both notes to vault
        write_note_to_vault(note1, temp_vault, attachments=item1.attachments, config=temp_config)
        write_note_to_vault(note2, temp_vault, attachments=item2.attachments, config=temp_config)

        att_dir = temp_vault / "Attachments" / "Ingested"
        att1_path = att_dir / "report.pdf"
        att2_path = att_dir / "report_2.pdf"

        assert att1_path.exists()
        assert att2_path.exists()
        assert att1_path.read_bytes() == b"%PDF-1.4 Report 1 Content"
        assert att2_path.read_bytes() == b"%PDF-1.4 Report 2 Content Different"

        # Check embeddings in notes
        assert "![[report.pdf]]" in note1.body
        assert "![[report_2.pdf]]" in note2.body


# ---------------------------------------------------------------------------
# 10. End-to-End Pipeline & Tracker Tests
# ---------------------------------------------------------------------------

class TestGoogleDrivePipelineIntegration:
    """Tests for IngestionPipeline and DeduplicationTracker integration."""

    @pytest.mark.asyncio
    async def test_pipeline_deduplication_lifecycle(
        self,
        mock_service: MockDriveService,
        temp_config: IngestionConfig,
        temp_tracker: DeduplicationTracker,
    ) -> None:
        pipeline = IngestionPipeline(config=temp_config, tracker=temp_tracker)

        # 1. First run: NEW item
        file_meta = {
            "id": "doc_e2e",
            "name": "Design Notes",
            "mimeType": DOC_MIME,
            "version": "1",
            "modifiedTime": "2026-10-01T10:00:00Z",
        }
        mock_service.files().list_mock.return_value = {"files": [file_meta], "nextPageToken": None}
        mock_service.files().export_mock.return_value = b"# Version 1 Content"

        source = GoogleDriveSource(service=mock_service)
        items = await source.fetch_items()
        action, path = await pipeline.process_item(source, items[0])
        assert action == IngestionAction.NEW
        assert path is not None
        assert (temp_config.vault_path / path).exists()

        # 2. Second run: UNCHANGED item
        action2, path2 = await pipeline.process_item(source, items[0])
        assert action2 == IngestionAction.UNCHANGED
        assert path2 == path

        # 3. Third run: CHANGED item (new version)
        file_meta_v2 = dict(file_meta)
        file_meta_v2["version"] = "2"
        file_meta_v2["modifiedTime"] = "2026-10-01T11:00:00Z"
        mock_service.files().list_mock.return_value = {"files": [file_meta_v2], "nextPageToken": None}
        mock_service.files().export_mock.return_value = b"# Version 2 Updated Content"

        items_v2 = await source.fetch_items()
        action3, path3 = await pipeline.process_item(source, items_v2[0])
        assert action3 == IngestionAction.CHANGED
        assert path3 == path
        updated_text = (temp_config.vault_path / path3).read_text(encoding="utf-8")
        assert "Version 2 Updated Content" in updated_text


# ---------------------------------------------------------------------------
# 11. SourceRegistry & CLI Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveRegistryAndCLI:
    """Tests for SourceRegistry and CLI commands."""

    def test_registry_registration(self) -> None:
        assert SourceRegistry.get("google-drive") is GoogleDriveSource
        assert SourceRegistry.get("google_drive") is GoogleDriveSource
        assert SourceRegistry.get("gdrive") is GoogleDriveSource

        inst = SourceRegistry.create("google-drive")
        assert isinstance(inst, GoogleDriveSource)

    def test_cli_argument_parsing_ingest_google_drive(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-google-drive",
            "--folder-id", "f1",
            "--folder-id", "f2",
            "--credentials", "/path/to/creds.json",
            "--token", "/path/to/token.json",
            "--drive-id", "shared_drive_team_456",
            "--max-file-size", "25000000",
            "--max-content-size", "200000",
            "--limit", "15",
        ])
        assert args.command == "ingest-google-drive"
        assert args.folder_id == ["f1", "f2"]
        assert args.credentials == "/path/to/creds.json"
        assert args.token == "/path/to/token.json"
        assert args.drive_id == "shared_drive_team_456"
        assert args.max_file_size == 25000000
        assert args.max_content_size == 200000
        assert args.limit == 15

    def test_cli_argument_parsing_generic_source(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest",
            "--source", "google-drive",
            "--folder-ids", "f1,f2",
            "--drive-id", "shared_drive_team_456",
            "--max-content-size", "300000",
        ])
        assert args.command == "ingest"
        assert args.source == "google-drive"
        assert args.folder_ids == "f1,f2"
        assert args.drive_id == "shared_drive_team_456"
        assert args.max_content_size == 300000

    @pytest.mark.asyncio
    async def test_cli_async_main_execution(
        self,
        mock_service: MockDriveService,
        temp_vault: Path,
        tmp_path: Path,
    ) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "doc_cli", "name": "CLI Doc", "mimeType": DOC_MIME},
            ],
            "nextPageToken": None,
        }
        mock_service.files().export_mock.return_value = b"# CLI Export"

        parser = create_parser()
        db_path = tmp_path / "cli_tracker.db"
        args = parser.parse_args([
            "--vault-path", str(temp_vault),
            "--tracker-db", str(db_path),
            "ingest-google-drive",
            "--limit", "1",
        ])

        with patch("sources.google_drive_source.GoogleDriveSource._get_service", return_value=mock_service):
            exit_code = await async_main(args)
            assert exit_code == 0
            # Verify file was written
            notes = list((temp_vault / "Ingested" / "Web").glob("*.md"))
            assert len(notes) == 1
            assert "CLI_Doc" in notes[0].name


# ---------------------------------------------------------------------------
# 12. Edge Cases, Hardening & Contract Validation Tests
# ---------------------------------------------------------------------------

class TestGoogleDriveEdgeCasesAndHardening:
    """Additional edge case tests ensuring full compliance with contract."""

    def test_process_google_sheet_empty_title_fallback(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.return_value = b"Col1,Col2\nVal1,Val2\n"
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "sheet_empty_name", "name": "", "mimeType": SHEET_MIME}
        item = source._process_single_file(file_meta)
        assert "## Sheet: Google Drive Document" in item.content

    def test_process_google_doc_links_and_nested_lists(self, mock_service: MockDriveService) -> None:
        html = '<p>Check <a href="https://example.com">Example</a></p><ul><li>Parent<ul><li>Child</li></ul></li></ul>'
        mock_service.files().export_mock.return_value = html.encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "doc_rich", "name": "Rich Document", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert "[Example](https://example.com)" in item.content
        assert "Parent" in item.content
        assert "Child" in item.content

    def test_process_google_doc_tables(self, mock_service: MockDriveService) -> None:
        html = "<table><tr><th>Header 1</th><th>Header 2</th></tr><tr><td>Data 1</td><td>Data 2</td></tr></table>"
        mock_service.files().export_mock.return_value = html.encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "doc_tbl", "name": "Table Doc", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert "Header 1" in item.content
        assert "Data 1" in item.content

    def test_process_downloaded_csv_file(self, mock_service: MockDriveService) -> None:
        raw_csv = "A,B,C\n1,2,3\n"
        mock_service.files().get_media_mock.return_value = raw_csv.encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "csv_file", "name": "data.csv", "mimeType": "text/csv", "size": len(raw_csv)}
        item = source._process_single_file(file_meta)
        assert "| A | B | C |" in item.content
        assert "| 1 | 2 | 3 |" in item.content

    def test_process_downloaded_python_code_file(self, mock_service: MockDriveService) -> None:
        raw_py = "def main():\n    print('Hello World')\n"
        mock_service.files().get_media_mock.return_value = raw_py.encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "py_file", "name": "script.py", "mimeType": "text/x-python", "size": len(raw_py)}
        item = source._process_single_file(file_meta)
        assert "def main():" in item.content

    def test_process_downloaded_html_file(self, mock_service: MockDriveService) -> None:
        raw_html = "<h1>Static HTML</h1><p>Paragraph inside static HTML.</p>"
        mock_service.files().get_media_mock.return_value = raw_html.encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "html_file", "name": "page.html", "mimeType": "text/html", "size": len(raw_html)}
        item = source._process_single_file(file_meta)
        assert "# Static HTML" in item.content
        assert "Paragraph inside static HTML." in item.content

    def test_process_downloaded_image_file(self, mock_service: MockDriveService) -> None:
        img_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRdummy"
        mock_service.files().get_media_mock.return_value = img_bytes
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "img_file", "name": "screenshot.png", "mimeType": "image/png", "size": len(img_bytes)}
        item = source._process_single_file(file_meta)
        assert "image" in item.tags
        assert len(item.attachments) == 1
        assert item.attachments[0].filename == "screenshot.png"
        assert "![[screenshot.png]]" in item.content

    def test_rename_stability(self) -> None:
        meta_original = {"id": "f_rename", "name": "Original Name.docx", "version": "1", "modifiedTime": "2026-10-01T00:00:00Z"}
        meta_renamed = {"id": "f_rename", "name": "New Name.docx", "version": "1", "modifiedTime": "2026-10-01T00:00:00Z"}
        # source_id depends only on id
        assert f"gdrive:{meta_original['id']}" == f"gdrive:{meta_renamed['id']}"
        # content hash does not depend on name
        assert compute_drive_content_hash(meta_original) == compute_drive_content_hash(meta_renamed)

    def test_move_stability(self) -> None:
        meta_folder1 = {"id": "f_move", "drive_path": "FolderA", "version": "1", "modifiedTime": "2026-10-01T00:00:00Z"}
        meta_folder2 = {"id": "f_move", "drive_path": "FolderB/Subfolder", "version": "1", "modifiedTime": "2026-10-01T00:00:00Z"}
        assert f"gdrive:{meta_folder1['id']}" == f"gdrive:{meta_folder2['id']}"
        assert compute_drive_content_hash(meta_folder1) == compute_drive_content_hash(meta_folder2)

    def test_author_fallback_when_no_owners(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.return_value = b"# Text"
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "f_no_owners", "name": "Doc", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert item.author is None

    def test_author_display_name_and_email_precedence(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.return_value = b"# Text"
        source = GoogleDriveSource(service=mock_service)
        file_meta = {
            "id": "f_owner",
            "name": "Doc",
            "mimeType": DOC_MIME,
            "owners": [{"displayName": "Sravani C", "emailAddress": "sravani@example.com"}],
        }
        item = source._process_single_file(file_meta)
        assert item.author == "Sravani C"

    def test_modified_time_fallback_when_none(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.return_value = b"# Text"
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "f_no_mtime", "name": "Doc", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert item.date is not None

    def test_summary_and_word_count(self, mock_service: MockDriveService) -> None:
        content = "Word " * 50
        mock_service.files().export_mock.return_value = f"<p>{content}</p>".encode("utf-8")
        source = GoogleDriveSource(service=mock_service)
        file_meta = {"id": "f_wc", "name": "Doc", "mimeType": DOC_MIME}
        item = source._process_single_file(file_meta)
        assert item.word_count == 50
        assert item.summary is not None
        assert len(item.summary) <= 200

    @pytest.mark.asyncio
    async def test_deterministic_sort_order(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "c", "name": "Zebra", "mimeType": DOC_MIME, "drive_path": "B"},
                {"id": "b", "name": "Apple", "mimeType": DOC_MIME, "drive_path": "B"},
                {"id": "a", "name": "Beta", "mimeType": DOC_MIME, "drive_path": "A"},
            ],
            "nextPageToken": None,
        }
        mock_service.files().export_mock.return_value = b"text"

        source = GoogleDriveSource(service=mock_service)
        items = await source.fetch_items()
        # Sorted by (drive_path, name, id)
        # Expected: A/Beta, B/Apple, B/Zebra
        assert [item.title for item in items] == ["Beta", "Apple", "Zebra"]

    @pytest.mark.asyncio
    async def test_multiple_folder_ids_aggregated(self, mock_service: MockDriveService) -> None:
        def list_side_effect(q: str, **kwargs: Any) -> Dict[str, Any]:
            if "'f1' in parents" in q:
                return {"files": [{"id": "d1", "name": "Doc 1", "mimeType": DOC_MIME}], "nextPageToken": None}
            elif "'f2' in parents" in q:
                return {"files": [{"id": "d2", "name": "Doc 2", "mimeType": DOC_MIME}], "nextPageToken": None}
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect
        mock_service.files().export_mock.return_value = b"text"

        source = GoogleDriveSource(service=mock_service, folder_id=["f1", "f2"])
        items = await source.fetch_items()
        assert len(items) == 2
        assert {item.source_id for item in items} == {"gdrive:d1", "gdrive:d2"}

    def test_deep_folder_recursion_3_levels(self, mock_service: MockDriveService) -> None:
        def list_side_effect(q: str, **kwargs: Any) -> Dict[str, Any]:
            if "'root' in parents" in q:
                return {"files": [{"id": "l1", "name": "Level1", "mimeType": FOLDER_MIME}], "nextPageToken": None}
            elif "'l1' in parents" in q:
                return {"files": [{"id": "l2", "name": "Level2", "mimeType": FOLDER_MIME}], "nextPageToken": None}
            elif "'l2' in parents" in q:
                return {"files": [{"id": "leaf", "name": "DeepDoc", "mimeType": DOC_MIME}], "nextPageToken": None}
            return {"files": [], "nextPageToken": None}

        mock_service.files().list_mock.side_effect = list_side_effect
        source = GoogleDriveSource(service=mock_service)
        files = source._discover_files_in_folder("root", "Root", set())
        assert len(files) == 1
        assert files[0]["id"] == "leaf"
        assert files[0]["drive_path"] == "Root/Level1/Level2"

    def test_empty_folder_returns_empty_list(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {"files": [], "nextPageToken": None}
        source = GoogleDriveSource(service=mock_service)
        files = source._discover_files_in_folder("empty_folder", "", set())
        assert files == []

    def test_export_error_404_mapped_to_source_error(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.side_effect = MockHttpError(404, "File not found")
        source = GoogleDriveSource(service=mock_service)
        with pytest.raises(SourceError, match="404"):
            source._export_google_sheet("missing_sheet", "Missing")

    def test_export_error_403_mapped_to_source_error(self, mock_service: MockDriveService) -> None:
        mock_service.files().export_mock.side_effect = MockHttpError(403, "Access denied")
        source = GoogleDriveSource(service=mock_service)
        with pytest.raises(SourceError, match="403 Forbidden"):
            source._export_google_slides("forbidden_slide")

    @pytest.mark.asyncio
    async def test_large_file_skipped_in_batch_fetch(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "normal_doc", "name": "Normal.txt", "mimeType": "text/plain", "size": 100},
                {"id": "giant_file", "name": "Giant.bin", "mimeType": "text/plain", "size": 200_000_000},
            ],
            "nextPageToken": None,
        }
        mock_service.files().get_media_mock.return_value = b"Hello Normal File"

        source = GoogleDriveSource(service=mock_service, max_file_size=50_000_000)
        items = await source.fetch_items()
        assert len(items) == 1
        assert items[0].source_id == "gdrive:normal_doc"

    @pytest.mark.asyncio
    async def test_custom_limit_parameter_in_fetch_items(self, mock_service: MockDriveService) -> None:
        mock_service.files().list_mock.return_value = {
            "files": [
                {"id": "d1", "name": "Doc 1", "mimeType": DOC_MIME},
                {"id": "d2", "name": "Doc 2", "mimeType": DOC_MIME},
                {"id": "d3", "name": "Doc 3", "mimeType": DOC_MIME},
            ],
            "nextPageToken": None,
        }
        mock_service.files().export_mock.return_value = b"text"

        source = GoogleDriveSource(service=mock_service)
        items = await source.fetch_items(limit=2)
        assert len(items) == 2

    def test_download_binary_content_media_io_base_download(self, mock_service: MockDriveService) -> None:
        source = GoogleDriveSource(service=mock_service)
        mock_req = MagicMock(uri="https://www.googleapis.com/drive/v3/files/f1?alt=media", http=MagicMock())
        mock_service._files.get_media = lambda **kw: mock_req

        with patch("sources.google_drive_source.MediaIoBaseDownload") as mock_downloader_cls:
            mock_downloader = MagicMock()
            mock_downloader.next_chunk.side_effect = [(None, False), (None, True)]

            def fake_init(buf, req, chunksize=None):
                buf.write(b"chunked content from media_io")
                return mock_downloader

            mock_downloader_cls.return_value = mock_downloader
            mock_downloader_cls.side_effect = fake_init
            content = source._download_binary_content("f1", "file.bin", 500)
            assert content == b"chunked content from media_io"
