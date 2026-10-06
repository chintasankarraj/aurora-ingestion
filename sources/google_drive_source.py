"""Google Drive ingestion connector using official Google Drive API v3.

Architecture:
    Google Drive API v3
        ↓
    OAuth 2.0 Read-Only Authentication (https://www.googleapis.com/auth/drive.readonly)
        ↓
    Recursive File & Folder Discovery (files.list with pagination & cycle protection)
        ↓
    Deterministic Ordering & Deduplication (gdrive:<file_id> with version/mtime/checksum hashing)
        ↓
    Content Extraction & Export:
        - Google Docs (application/vnd.google-apps.document) → HTML export → clean Markdown
        - Google Sheets (application/vnd.google-apps.spreadsheet) → CSV export → bounded Markdown tables
        - Google Slides (application/vnd.google-apps.presentation) → plain text export → structured slide sections
        - PDFs (application/pdf) → binary download → text extraction & attachment embed → Ingested/PDF
        - Text/Markdown/Code/JSON/CSV → decoded text → formatted Markdown
        - Unsupported binaries → safe metadata note with unsupported disclaimer
        ↓
    Vault Placement:
        - Docs, Sheets, Slides, Text files → Ingested/Web/<date>_web_<title>.md
        - PDFs → Ingested/PDF/<date>_pdf_<title>.md
        - Attachments → Attachments/Ingested/<collision_safe_name>

Key Design Decisions:
- Read-Only Security: Only requests 'drive.readonly' scope; never modifies, deletes, moves, or creates files.
- Privacy Protected: File contents, OAuth tokens, and refresh tokens are NEVER logged.
- Dependency Injection: Supports injecting a pre-built Drive service instance for fast, offline, deterministic unit tests.
- Deduplication: Content-based hash from Drive metadata (version, modifiedTime, md5Checksum, size) prevents redundant downloads.
- Memory & Size Safety: Preflight file size check (default 100 MB limit) prevents downloading oversized binaries into RAM.
- Content Truncation: Configurable content size limit (default 500k chars) truncates oversized tables/documents with clear notices.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Set, Tuple, Union
from googleapiclient.http import MediaIoBaseDownload

from markdownify import markdownify

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger("aurora.ingestion.google_drive")

DRIVE_READONLY_SCOPE = "https://www.googleapis.com/auth/drive.readonly"
DEFAULT_MAX_FILE_SIZE = 100 * 1024 * 1024  # 100 MB limit
DEFAULT_MAX_CONTENT_SIZE = 500_000  # 500k characters limit
DEFAULT_MAX_SHEET_ROWS = 100
DEFAULT_MAX_SHEET_COLS = 20
CHUNK_SIZE_BYTES = 64 * 1024

# Characters forbidden in Obsidian filenames and note titles
FORBIDDEN_CHARS_PATTERN = re.compile(r'[\\/:*?"<>|#\^\[\]]')

# Supported Google Workspace MIME types
DOC_MIME = "application/vnd.google-apps.document"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"
SLIDE_MIME = "application/vnd.google-apps.presentation"
FOLDER_MIME = "application/vnd.google-apps.folder"
PDF_MIME = "application/pdf"


# ---------------------------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------------------------

def sanitize_drive_title(raw_name: str, mime_type: str = "") -> str:
    """Sanitize note title derived from Google Drive file name.

    - Removes file extensions for binary/regular files.
    - Strips forbidden Obsidian characters.
    - Collapses multiple whitespace.
    - Caps length at 80 characters.
    """
    if not raw_name:
        return "Google Drive Document"

    name_str = raw_name.strip()
    # Strip extension for non-Google Workspace files
    if not mime_type.startswith("application/vnd.google-apps"):
        if "." in name_str and not name_str.startswith("."):
            name_str = name_str.rsplit(".", 1)[0]

    clean = FORBIDDEN_CHARS_PATTERN.sub(" ", name_str).strip()
    clean = re.sub(r"\s+", " ", clean).strip()
    clean = clean[:80].rstrip()
    return clean or "Google Drive Document"


def compute_drive_content_hash(file_meta: Dict[str, Any]) -> str:
    """Compute deterministic content hash from Google Drive file metadata.

    Combines file ID, version, modifiedTime, md5Checksum, and size.
    """
    fid = str(file_meta.get("id", ""))
    ver = str(file_meta.get("version", ""))
    mtime = str(file_meta.get("modifiedTime", ""))
    md5 = str(file_meta.get("md5Checksum", ""))
    size = str(file_meta.get("size", ""))

    raw_signature = f"{fid}:{ver}:{mtime}:{md5}:{size}"
    return hashlib.sha256(raw_signature.encode("utf-8")).hexdigest()


def format_csv_to_markdown_table(
    csv_content: str,
    max_rows: int = DEFAULT_MAX_SHEET_ROWS,
    max_cols: int = DEFAULT_MAX_SHEET_COLS,
) -> Tuple[str, bool]:
    """Convert CSV content into a clean Markdown table with bounded rows and columns.

    Returns a tuple of (markdown_table, was_truncated).
    """
    if not csv_content or not csv_content.strip():
        return "*Empty spreadsheet.*", False

    try:
        reader = csv.reader(io.StringIO(csv_content))
        rows: List[List[str]] = []
        total_rows = 0
        for r in reader:
            total_rows += 1
            if len(rows) < max_rows:
                rows.append(r)
    except Exception as e:
        return f"*Error parsing spreadsheet CSV: {e}*", False

    if not rows:
        return "*Empty spreadsheet.*", False

    is_truncated = False
    trunc_notes = []

    # Row truncation
    if total_rows > max_rows:
        is_truncated = True
        trunc_notes.append(f"showing first {max_rows} of {total_rows} rows")

    # Col truncation
    max_actual_cols = max((len(r) for r in rows), default=0)
    if max_actual_cols > max_cols:
        is_truncated = True
        trunc_notes.append(f"showing first {max_cols} of {max_actual_cols} columns")
        rows = [r[:max_cols] for r in rows]

    num_cols = max((len(r) for r in rows), default=0)
    if num_cols == 0:
        return "*Empty spreadsheet.*", False

    # Standardize column count and escape markdown table delimiters
    padded_rows: List[List[str]] = []
    for r in rows:
        padded = [cell.replace("|", "\\|").replace("\n", " ").strip() for cell in r]
        if len(padded) < num_cols:
            padded += [""] * (num_cols - len(padded))
        padded_rows.append(padded)

    # First row as header, remaining as data rows
    header = padded_rows[0]
    separator = ["---"] * num_cols

    md_lines: List[str] = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(separator) + " |",
    ]

    for data_row in padded_rows[1:]:
        md_lines.append("| " + " | ".join(data_row) + " |")

    table_md = "\n".join(md_lines)
    if is_truncated:
        table_md += f"\n\n*(Table truncated: {', '.join(trunc_notes)})*"

    return table_md, is_truncated


def format_slides_to_markdown(raw_text: str, max_chars: Optional[int] = None) -> str:
    """Convert exported presentation plain text into readable Markdown slides.

    Google Drive presentation plain text export separates slides with form-feed characters (\x0c or \f).
    """
    if not raw_text or not raw_text.strip():
        return "*Empty presentation.*"

    raw_slides = raw_text.split("\x0c") if "\x0c" in raw_text else raw_text.split("\f")
    # If no form feeds present, treat entire content as single slide
    if len(raw_slides) == 1:
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]
        if not lines:
            return "*Empty presentation.*"
        title = lines[0][:60]
        body = "\n\n".join(lines[1:])
        return f"## Slide 1 — {title}\n\n{body}" if body else f"## Slide 1 — {title}"

    slides_md: List[str] = []
    slide_index = 1
    current_length = 0
    for slide_text in raw_slides:
        cleaned = slide_text.strip()
        if not cleaned:
            continue

        slide_lines = [l.strip() for l in cleaned.split("\n") if l.strip()]
        if slide_lines:
            header_title = slide_lines[0][:60]
            body_text = "\n\n".join(slide_lines[1:])
            if body_text:
                slide_md = f"## Slide {slide_index} — {header_title}\n\n{body_text}"
            else:
                slide_md = f"## Slide {slide_index} — {header_title}"
        else:
            slide_md = f"## Slide {slide_index}\n\n*Empty slide.*"

        slides_md.append(slide_md)
        current_length += len(slide_md) + 2
        slide_index += 1
        if max_chars is not None and current_length >= max_chars:
            break

    if not slides_md:
        return "*Empty presentation.*"

    return "\n\n".join(slides_md).strip()


def extract_pdf_text_safely(pdf_bytes: bytes) -> str:
    """Safely extract plain text from PDF bytes without crashing or invoking external processes."""
    if not pdf_bytes:
        return ""
    try:
        import fitz  # PyMuPDF
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            pages_text = [page.get_text() for page in doc]
            return "\n\n".join(p.strip() for p in pages_text if p.strip()).strip()
    except Exception:
        pass

    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_text = [page.extract_text() or "" for page in pdf.pages]
            return "\n\n".join(p.strip() for p in pages_text if p.strip()).strip()
    except Exception:
        pass

    return ""


def sanitize_secret_error_message(err: Union[str, Exception]) -> str:
    """Sanitize error messages to prevent exposing OAuth tokens, secrets, or auth headers."""
    err_str = str(err)
    patterns: List[Tuple[str, str]] = [
        # Google OAuth access tokens
        (r"ya29\.[a-zA-Z0-9_\-\.]+", "[REDACTED]"),
        # Google refresh tokens
        (r"1//[a-zA-Z0-9_\-]+", "[REDACTED]"),
        # Google client secrets (GOCSPX prefix)
        (r"GOCSPX-[a-zA-Z0-9_\-]+", "[REDACTED]"),
        # Bearer tokens in headers
        (r"(?i)bearer\s+[a-zA-Z0-9_\-\.]+", "Bearer [REDACTED]"),
        # Authorization header
        (r"(?i)authorization:\s*[^\s,]+", "Authorization: [REDACTED]"),
        # Query parameters with secrets/tokens
        (r"(?i)(client_secret|refresh_token|access_token|token|code)=([^&\s]+)", r"\1=[REDACTED]"),
        # JSON / Dict key-value pairs
        (
            r"""(?i)(["']?(?:client_secret|refresh_token|access_token|token|secret)["']?\s*[:=]\s*["']?)([^"'\s,{}]+)(["']?)""",
            r"\1[REDACTED]\3",
        ),
    ]
    for pattern, repl in patterns:
        err_str = re.sub(pattern, repl, err_str)
    return err_str


def map_google_drive_error(err: Exception) -> SourceError:
    """Map Google API HttpError and transport exceptions into clean, actionable SourceErrors.

    Guarantees that sensitive tokens, client secrets, and auth headers are never leaked.
    """
    clean_err_str = sanitize_secret_error_message(err)

    status_code: Optional[int] = None
    if hasattr(err, "resp") and hasattr(err.resp, "status"):
        try:
            status_code = int(err.resp.status)
        except Exception:
            pass
    elif hasattr(err, "status_code"):
        try:
            status_code = int(err.status_code)
        except Exception:
            pass

    if status_code == 401:
        return SourceError(
            "Google Drive authentication failed (401 Unauthorized). "
            "Please verify your credentials or re-authorize with a new OAuth token."
        )
    if status_code == 403:
        return SourceError(
            "Google Drive access denied (403 Forbidden). "
            "The configured account does not have permission to access this file or folder, or quota was exceeded."
        )
    if status_code == 404:
        return SourceError(
            "Google Drive resource not found (404). Verify that the configured file or folder ID exists."
        )
    if status_code == 429:
        return SourceError(
            "Google Drive rate limit exceeded (429 Too Many Requests). Please retry after backoff delay."
        )
    if status_code and 500 <= status_code <= 599:
        return SourceError(
            f"Google Drive server error ({status_code}). Temporary failure on Google servers; please retry later."
        )

    return SourceError(f"Google Drive API error: {clean_err_str}")


# ---------------------------------------------------------------------------
# Google Drive Source Connector
# ---------------------------------------------------------------------------

class GoogleDriveSource(BaseSource):
    """Google Drive ingestion connector for Aurora Ingestion Pipeline."""

    source_type = "google-drive"

    def __init__(
        self,
        service: Optional[Any] = None,
        credentials_path: Optional[Union[str, Path]] = None,
        token_path: Optional[Union[str, Path]] = None,
        folder_id: Optional[Union[str, Sequence[str]]] = None,
        max_file_size: Optional[int] = None,
        max_content_size: Optional[int] = None,
        limit: Optional[int] = None,
        drive_id: Optional[str] = None,
    ) -> None:
        super().__init__()
        self._service = service
        self.credentials_path = credentials_path or os.getenv("GOOGLE_DRIVE_CREDENTIALS")
        self.token_path = token_path or os.getenv("GOOGLE_DRIVE_TOKEN")

        # Resolve folder IDs
        raw_folders = folder_id if folder_id is not None else os.getenv("GOOGLE_DRIVE_FOLDER_ID")
        if isinstance(raw_folders, str):
            self.folder_ids = [fid.strip() for fid in raw_folders.split(",") if fid.strip()]
        elif isinstance(raw_folders, (list, tuple)):
            self.folder_ids = [str(fid).strip() for fid in raw_folders if str(fid).strip()]
        else:
            self.folder_ids = []

        # Shared Drive ID
        self.drive_id = (
            drive_id
            or os.getenv("GOOGLE_DRIVE_SHARED_DRIVE_ID")
            or os.getenv("GOOGLE_DRIVE_DRIVE_ID")
        )

        # File size limits
        size_env = os.getenv("GOOGLE_DRIVE_MAX_FILE_SIZE")
        self.max_file_size = int(
            max_file_size if max_file_size is not None else (size_env or DEFAULT_MAX_FILE_SIZE)
        )

        # Content extraction size limits
        content_env = os.getenv("GOOGLE_DRIVE_MAX_CONTENT_SIZE")
        self.max_content_size = int(
            max_content_size if max_content_size is not None else (content_env or DEFAULT_MAX_CONTENT_SIZE)
        )

        self.limit = int(limit) if limit is not None else None

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Google Drive"

    def _get_service(self) -> Any:
        """Initialize or return authenticated Google Drive API v3 service."""
        if self._service is not None:
            return self._service

        token_p = Path(self.token_path or ".credentials/google_drive_token.json")
        creds_p = Path(self.credentials_path or ".credentials/google_drive_credentials.json")

        creds = None
        if token_p.exists():
            try:
                from google.oauth2.credentials import Credentials
                creds = Credentials.from_authorized_user_file(str(token_p), scopes=[DRIVE_READONLY_SCOPE])
                if creds and getattr(creds, "expired", False) is True and getattr(creds, "refresh_token", None):
                    from google.auth.transport.requests import Request
                    creds.refresh(Request())
                    token_p.parent.mkdir(parents=True, exist_ok=True)
                    json_data = creds.to_json()
                    token_p.write_text(str(json_data) if not isinstance(json_data, str) else json_data, encoding="utf-8")
            except Exception as e:
                sanitized_msg = sanitize_secret_error_message(e)
                raise SourceError(f"Failed to load or refresh Google Drive OAuth token: {sanitized_msg}") from e

        elif creds_p.exists():
            try:
                from google_auth_oauthlib.flow import InstalledAppFlow
                flow = InstalledAppFlow.from_client_secrets_file(str(creds_p), scopes=[DRIVE_READONLY_SCOPE])
                creds = flow.run_local_server(port=0)
                token_p.parent.mkdir(parents=True, exist_ok=True)
                json_data = creds.to_json()
                token_p.write_text(str(json_data) if not isinstance(json_data, str) else json_data, encoding="utf-8")
            except Exception as e:
                sanitized_msg = sanitize_secret_error_message(e)
                raise SourceError(f"Google Drive OAuth authorization failed: {sanitized_msg}") from e

        else:
            raise SourceError(
                "Google Drive authentication error: OAuth credentials not found. "
                "Please configure GOOGLE_DRIVE_CREDENTIALS or GOOGLE_DRIVE_TOKEN with valid client secrets or token file."
            )

        try:
            from googleapiclient.discovery import build
            self._service = build("drive", "v3", credentials=creds)
            return self._service
        except Exception as e:
            sanitized_msg = sanitize_secret_error_message(e)
            raise SourceError(f"Failed to build Google Drive API service client: {sanitized_msg}") from e

    def _discover_files_in_folder(
        self,
        folder_id: str,
        current_path: str,
        seen_folders: Set[str],
    ) -> List[Dict[str, Any]]:
        """Recursively discover files within a specific folder ID, tracking folder paths and cycle protection."""
        if folder_id in seen_folders:
            logger.warning("Cyclic folder reference detected for folder ID %s. Skipping.", folder_id)
            return []
        seen_folders.add(folder_id)

        service = self._get_service()
        discovered: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        seen_page_tokens: Set[str] = set()

        q = f"'{folder_id}' in parents and trashed = false"

        while True:
            try:
                list_kwargs: Dict[str, Any] = {
                    "q": q,
                    "pageSize": 100,
                    "pageToken": page_token,
                    "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, size, version, md5Checksum, webViewLink, parents, owners, description)",
                    "supportsAllDrives": True,
                    "includeItemsFromAllDrives": True,
                }
                request = service.files().list(**list_kwargs)
                response = request.execute()
            except Exception as e:
                raise map_google_drive_error(e)

            files = response.get("files", [])
            for f in files:
                m_type = f.get("mimeType", "")
                name = f.get("name", "Untitled")

                if m_type == FOLDER_MIME:
                    sub_path = f"{current_path}/{name}" if current_path else name
                    sub_files = self._discover_files_in_folder(
                        folder_id=f["id"],
                        current_path=sub_path,
                        seen_folders=seen_folders,
                    )
                    discovered.extend(sub_files)
                else:
                    item_meta = dict(f)
                    item_meta["drive_path"] = current_path
                    discovered.append(item_meta)

            page_token = response.get("nextPageToken")
            if not page_token or page_token in seen_page_tokens:
                break
            seen_page_tokens.add(page_token)

        return discovered

    def _discover_all_accessible_files(self) -> List[Dict[str, Any]]:
        """Discover all non-trashed, non-folder files accessible to the user across Drive."""
        service = self._get_service()
        discovered: List[Dict[str, Any]] = []
        page_token: Optional[str] = None
        seen_page_tokens: Set[str] = set()

        q = f"trashed = false and mimeType != '{FOLDER_MIME}'"

        while True:
            try:
                list_kwargs: Dict[str, Any] = {
                    "q": q,
                    "pageSize": 100,
                    "pageToken": page_token,
                    "fields": "nextPageToken, files(id, name, mimeType, modifiedTime, createdTime, size, version, md5Checksum, webViewLink, parents, owners, description)",
                    "supportsAllDrives": True,
                    "includeItemsFromAllDrives": True,
                }
                if self.drive_id:
                    list_kwargs["corpora"] = "drive"
                    list_kwargs["driveId"] = self.drive_id

                request = service.files().list(**list_kwargs)
                response = request.execute()
            except Exception as e:
                raise map_google_drive_error(e)

            files = response.get("files", [])
            for f in files:
                item_meta = dict(f)
                item_meta["drive_path"] = f.get("drive_path") or "My Drive"
                discovered.append(item_meta)

            page_token = response.get("nextPageToken")
            if not page_token or page_token in seen_page_tokens:
                break
            seen_page_tokens.add(page_token)

        return discovered

    def _export_google_doc(self, file_id: str) -> str:
        """Export a Google Doc to clean Markdown via HTML export."""
        service = self._get_service()
        early_bound = max(self.max_content_size * 4, 1_000_000)
        try:
            html_content = service.files().export(fileId=file_id, mimeType="text/html").execute()
            if isinstance(html_content, bytes):
                html_text = html_content.decode("utf-8", errors="replace")
            else:
                html_text = str(html_content)

            if len(html_text) > early_bound:
                html_text = html_text[:early_bound]

            md = markdownify(html_text, heading_style="ATX", strip=["script", "style"])
            return md.strip()
        except Exception:
            # Fallback to plain text export if HTML export fails
            try:
                plain_content = service.files().export(fileId=file_id, mimeType="text/plain").execute()
                if isinstance(plain_content, bytes):
                    plain_text = plain_content.decode("utf-8", errors="replace").strip()
                else:
                    plain_text = str(plain_content).strip()

                if len(plain_text) > early_bound:
                    plain_text = plain_text[:early_bound]
                return plain_text
            except Exception as e:
                raise map_google_drive_error(e)

    def _export_google_sheet(self, file_id: str, title: str) -> str:
        """Export a Google Sheet to CSV and format as a clean Markdown table."""
        service = self._get_service()
        try:
            csv_content = service.files().export(fileId=file_id, mimeType="text/csv").execute()
            if isinstance(csv_content, bytes):
                csv_text = csv_content.decode("utf-8", errors="replace")
            else:
                csv_text = str(csv_content)

            table_md, _ = format_csv_to_markdown_table(csv_text)
            sheet_name = title or "Sheet1"
            return f"## Sheet: {sheet_name}\n\n{table_md}"
        except Exception as e:
            raise map_google_drive_error(e)

    def _export_google_slides(self, file_id: str) -> str:
        """Export a Google Presentation to plain text and format into structured slides."""
        service = self._get_service()
        try:
            text_content = service.files().export(fileId=file_id, mimeType="text/plain").execute()
            if isinstance(text_content, bytes):
                raw_text = text_content.decode("utf-8", errors="replace")
            else:
                raw_text = str(text_content)

            return format_slides_to_markdown(raw_text, max_chars=self.max_content_size * 2)
        except Exception as e:
            raise map_google_drive_error(e)

    def _download_binary_content(
        self,
        file_id: str,
        file_name: str,
        file_size: Optional[int],
    ) -> bytes:
        """Download binary content in chunks using MediaIoBaseDownload with size-limit enforcement."""
        if file_size is not None and file_size > self.max_file_size:
            raise SourceError(
                f"File '{file_name}' ({file_size:,} bytes) exceeds maximum allowed size limit ({self.max_file_size:,} bytes)."
            )

        service = self._get_service()
        try:
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        except TypeError:
            request = service.files().get_media(fileId=file_id)

        buffer = io.BytesIO()
        try:
            downloader = MediaIoBaseDownload(buffer, request, chunksize=CHUNK_SIZE_BYTES)
            done = False
            while not done:
                _, done = downloader.next_chunk()
                if buffer.tell() > self.max_file_size:
                    raise SourceError(
                        f"File '{file_name}' exceeded maximum allowed size limit ({self.max_file_size:,} bytes) during download."
                    )
            return buffer.getvalue()
        except SourceError:
            raise
        except Exception as e:
            raise map_google_drive_error(e)
        finally:
            buffer.close()
    def _truncate_content_if_needed(self, text: str) -> str:
        """Enforce maximum content size limits with clear truncation notices."""
        if not text:
            return ""
        if len(text) > self.max_content_size:
            truncated = text[:self.max_content_size].rstrip()
            notice = f"\n\n*(Content truncated: extracted text exceeded maximum content limit of {self.max_content_size:,} characters.)*"
            return truncated + notice
        return text

    def _process_single_file(self, f: Dict[str, Any]) -> SourceItem:
        """Extract content, build metadata, manage attachments, and construct SourceItem for a single Drive file."""
        file_id = f["id"]
        raw_name = f.get("name", "Untitled")
        mime_type = f.get("mimeType", "")
        file_size = int(f["size"]) if f.get("size") is not None else None
        modified_time = f.get("modifiedTime") or datetime.now(timezone.utc).isoformat()
        web_link = f.get("webViewLink") or f"https://drive.google.com/file/d/{file_id}/view"
        drive_path = f.get("drive_path", "")
        version = f.get("version", "")
        md5_checksum = f.get("md5Checksum", "")

        # 1. Deterministic Content Hash & Source ID
        content_hash = compute_drive_content_hash(f)
        source_id = f"gdrive:{file_id}"

        # 2. Extract Title
        title = sanitize_drive_title(raw_name, mime_type=mime_type)

        # 3. Process Content & Attachments by MIME Type
        body = ""
        attachments: List[Attachment] = []
        tags = ["ingested", "google-drive"]

        if mime_type == DOC_MIME:
            extracted_text = self._export_google_doc(file_id)
            body = self._truncate_content_if_needed(extracted_text)
            tags.append("document")

        elif mime_type == SHEET_MIME:
            extracted_text = self._export_google_sheet(file_id, title)
            body = self._truncate_content_if_needed(extracted_text)
            tags.append("spreadsheet")

        elif mime_type == SLIDE_MIME:
            extracted_text = self._export_google_slides(file_id)
            body = self._truncate_content_if_needed(extracted_text)
            tags.append("presentation")

        elif mime_type == PDF_MIME:
            tags.append("pdf")
            pdf_bytes = self._download_binary_content(file_id, raw_name, file_size)
            safe_filename = FORBIDDEN_CHARS_PATTERN.sub("", raw_name).strip() or f"document_{file_id}.pdf"
            att = Attachment(filename=safe_filename, content=pdf_bytes, mime_type="application/pdf")
            attachments.append(att)

            pdf_text = extract_pdf_text_safely(pdf_bytes)
            if pdf_text:
                pdf_text = self._truncate_content_if_needed(pdf_text)
                body = f"## Extracted Text\n\n{pdf_text}\n\n## Attachment\n\n![[{safe_filename}]]"
            else:
                body = f"*No extractable text found in PDF document.*\n\n## Attachment\n\n![[{safe_filename}]]"

        elif (
            mime_type.startswith("text/")
            or mime_type in {"application/json", "application/javascript", "application/xml"}
            or raw_name.lower().endswith((".txt", ".md", ".csv", ".json", ".py", ".html", ".js"))
        ):
            text_bytes = self._download_binary_content(file_id, raw_name, file_size)
            decoded_text = text_bytes.decode("utf-8", errors="replace")

            if mime_type == "text/csv" or raw_name.lower().endswith(".csv"):
                table_md, _ = format_csv_to_markdown_table(decoded_text)
                body = self._truncate_content_if_needed(table_md)
            elif mime_type == "application/json" or raw_name.lower().endswith(".json"):
                body = f"```json\n{decoded_text.strip()}\n```"
                body = self._truncate_content_if_needed(body)
            elif mime_type == "text/html" or raw_name.lower().endswith(".html"):
                body = markdownify(decoded_text, heading_style="ATX", strip=["script", "style"]).strip()
                body = self._truncate_content_if_needed(body)
            else:
                body = self._truncate_content_if_needed(decoded_text.strip())

        elif mime_type.startswith("image/"):
            tags.append("image")
            img_bytes = self._download_binary_content(file_id, raw_name, file_size)
            safe_filename = FORBIDDEN_CHARS_PATTERN.sub("", raw_name).strip() or f"image_{file_id}"
            att = Attachment(filename=safe_filename, content=img_bytes, mime_type=mime_type)
            attachments.append(att)
            body = f"## Image\n\n![[{safe_filename}]]"

        else:
            # Unsupported binary file format
            tags.append("binary")
            size_disp = f"{file_size:,} bytes" if file_size is not None else "Unknown size"
            body = (
                f"*File type '{mime_type}' is unsupported for direct text extraction.*\n\n"
                f"### File Information\n"
                f"- **Filename**: {raw_name}\n"
                f"- **Size**: {size_disp}\n"
                f"- **MIME Type**: `{mime_type}`\n"
                f"- **Google Drive ID**: `{file_id}`"
            )

        # 4. Extract owners/author
        owners = f.get("owners", [])
        author = owners[0].get("displayName") or owners[0].get("emailAddress") if owners else None

        # 5. Build extra metadata
        extra_meta: Dict[str, Any] = {
            "drive_file_id": file_id,
            "drive_file_name": raw_name,
            "mime_type": mime_type,
            "modified_time": modified_time,
            "drive_path": drive_path or "My Drive",
            "source_url": web_link,
        }
        if version:
            extra_meta["drive_version"] = version
        if file_size is not None:
            extra_meta["file_size_bytes"] = file_size
        if md5_checksum:
            extra_meta["md5_checksum"] = md5_checksum
        if author:
            extra_meta["drive_owner"] = author

        word_count = len(body.split()) if body else 0

        item = SourceItem(
            source_id=source_id,
            title=title,
            source_type=self.source_type,
            content=body,
            date=modified_time,
            source_url=web_link,
            author=author,
            tags=tags,
            attachments=attachments,
            summary=body[:200].strip() if body else None,
            word_count=word_count,
            extra_metadata=extra_meta,
            content_hash=content_hash,
        )

        return item

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch items from Google Drive based on configured folder IDs or accessible files.

        Supported parameters:
        - folder_id: Single folder ID or list of folder IDs
        - folder_ids: Comma-separated string of folder IDs
        - credentials: Path to OAuth credentials client secrets file
        - token: Path to OAuth token file
        - max_file_size: Override max file size limit
        - max_content_size: Override max content size limit
        - limit: Maximum number of files to process
        """
        # Runtime overrides
        if kwargs.get("credentials"):
            self.credentials_path = str(kwargs["credentials"])
        if kwargs.get("token"):
            self.token_path = str(kwargs["token"])
        if kwargs.get("drive_id"):
            self.drive_id = str(kwargs["drive_id"])
        elif kwargs.get("shared_drive_id"):
            self.drive_id = str(kwargs["shared_drive_id"])
        if kwargs.get("max_file_size") is not None:
            self.max_file_size = int(kwargs["max_file_size"])
        if kwargs.get("max_content_size") is not None:
            self.max_content_size = int(kwargs["max_content_size"])
        if kwargs.get("limit") is not None:
            self.limit = int(kwargs["limit"])

        # Folder ID resolution
        if kwargs.get("folder_ids"):
            raw_fids = str(kwargs["folder_ids"])
            self.folder_ids = [fid.strip() for fid in raw_fids.split(",") if fid.strip()]
        elif kwargs.get("folder_id"):
            f_arg = kwargs["folder_id"]
            if isinstance(f_arg, str):
                self.folder_ids = [fid.strip() for fid in f_arg.split(",") if fid.strip()]
            elif isinstance(f_arg, (list, tuple)):
                self.folder_ids = [str(fid).strip() for fid in f_arg if str(fid).strip()]

        # 1. Discover target files
        candidate_files: List[Dict[str, Any]] = []

        if self.folder_ids:
            seen_folders: Set[str] = set()
            for fid in self.folder_ids:
                folder_files = self._discover_files_in_folder(
                    folder_id=fid,
                    current_path="",
                    seen_folders=seen_folders,
                )
                candidate_files.extend(folder_files)
        else:
            candidate_files = self._discover_all_accessible_files()

        # 2. Deterministic sorting: sort by folder path, then file name, then file ID
        candidate_files.sort(
            key=lambda f: (f.get("drive_path", ""), f.get("name", ""), f.get("id", ""))
        )

        # 3. Deduplicate by Drive file ID before applying limit, preserving first deterministic occurrence
        seen_file_ids: Dict[str, Dict[str, Any]] = {}
        unique_candidates: List[Dict[str, Any]] = []
        for f in candidate_files:
            fid = f.get("id")
            if not fid:
                continue
            if fid not in seen_file_ids:
                seen_file_ids[fid] = f
                unique_candidates.append(f)
            else:
                # If existing entry has empty/generic drive_path and this one has a specific path, preserve richer path
                existing = seen_file_ids[fid]
                if (not existing.get("drive_path") or existing.get("drive_path") == "My Drive") and f.get("drive_path") and f.get("drive_path") != "My Drive":
                    existing["drive_path"] = f["drive_path"]

        candidate_files = unique_candidates

        # 4. Apply limit if configured
        if self.limit and self.limit > 0:
            candidate_files = candidate_files[:self.limit]

        # 4. Process files with batch fault isolation
        items: List[SourceItem] = []
        for file_meta in candidate_files:
            try:
                item = self._process_single_file(file_meta)
                items.append(item)
            except SourceError as se:
                logger.warning(
                    "Skipping failed Google Drive file '%s' (%s): %s",
                    file_meta.get("name"),
                    file_meta.get("id"),
                    se,
                )
                continue
            except Exception as e:
                logger.warning(
                    "Unexpected error processing Google Drive file '%s' (%s): %s",
                    file_meta.get("name"),
                    file_meta.get("id"),
                    e,
                )
                continue

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a Google Drive SourceItem into a MarkdownNote, routing PDFs to Ingested/PDF and others to Ingested/Web."""
        note = self.default_item_to_note(item)
        mime = item.extra_metadata.get("mime_type", "")
        if mime == PDF_MIME or item.title.lower().endswith(".pdf"):
            note.folder = "Ingested/PDF"
        else:
            note.folder = "Ingested/Web"
        return note


# Register Google Drive connector in SourceRegistry
SourceRegistry.register("google-drive", GoogleDriveSource)
SourceRegistry.register("google_drive", GoogleDriveSource)
SourceRegistry.register("gdrive", GoogleDriveSource)
