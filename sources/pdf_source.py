"""PDF document source connector for Aurora External Data Ingestion Pipeline.

Extracts text, metadata, and tables from PDF documents using pymupdf and pdfplumber,
creates Obsidian-compatible Markdown notes in Ingested/PDF/, attaches original PDFs in
Attachments/Ingested/, and enforces SHA-256 deduplication.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Union

import pdfplumber
import pymupdf

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)


def parse_pdf_date(date_str: Optional[str]) -> Optional[str]:
    """Parse PDF date format (e.g. 'D:20260915123000' or '2026-09-15') into YYYY-MM-DD."""
    if not date_str:
        return None
    cleaned = str(date_str).strip()
    match = re.search(r"(\d{4})[-_]?(\d{2})[-_]?(\d{2})", cleaned)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None


def format_markdown_table(table: List[List[Any]]) -> str:
    """Format a 2D list of cells extracted by pdfplumber into a Markdown table."""
    if not table or not any(table):
        return ""

    cleaned_rows: List[List[str]] = []
    for row in table:
        cleaned_cells = [
            " ".join(str(cell or "").strip().split()).replace("|", "\\|")
            for cell in row
        ]
        cleaned_rows.append(cleaned_cells)

    if not cleaned_rows:
        return ""

    header = cleaned_rows[0]
    col_count = len(header)
    # Ensure all rows have equal columns
    for row in cleaned_rows:
        if len(row) < col_count:
            row.extend([""] * (col_count - len(row)))

    sep = ["---"] * col_count
    lines = [
        "| " + " | ".join(header) + " |",
        "| " + " | ".join(sep) + " |",
    ]
    for row in cleaned_rows[1:]:
        lines.append("| " + " | ".join(row[:col_count]) + " |")

    return "\n".join(lines)


class PDFSource(BaseSource):
    """Source connector for ingesting PDF files into Aurora vault."""

    @property
    def source_type(self) -> str:
        return "pdf"

    @property
    def display_name(self) -> str:
        return "PDF Documents"

    async def fetch_items(
        self,
        file: Optional[Union[str, Path]] = None,
        file_path: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch and extract items from one or more PDF files.

        Supports:
        - Single file path via `file` or `file_path`
        - Directory path (scans for all *.pdf files)
        """
        raw_path = file or file_path or kwargs.get("url")
        if not raw_path:
            raise SourceError(
                "PDF source requires a file path. Specify --file <path/to/document.pdf>."
            )

        target_path = Path(raw_path).resolve()

        if not target_path.exists():
            raise SourceError(f"PDF file does not exist: {target_path}")

        pdf_files: List[Path] = []
        if target_path.is_dir():
            pdf_files = sorted(target_path.glob("*.pdf"))
            if not pdf_files:
                raise SourceError(f"No PDF files found in directory: {target_path}")
        elif target_path.is_file():
            pdf_files = [target_path]
        else:
            raise SourceError(f"Target path is neither a file nor a directory: {target_path}")

        items: List[SourceItem] = []
        for pdf_file in pdf_files:
            item = self._extract_single_pdf(pdf_file)
            items.append(item)

        return items

    def _extract_single_pdf(self, pdf_path: Path) -> SourceItem:
        """Extract text, metadata, tables, and attachment payload from a single PDF file."""
        if not pdf_path.exists():
            raise SourceError(f"PDF file does not exist: {pdf_path}")
        if not pdf_path.is_file():
            raise SourceError(f"PDF path is not a file: {pdf_path}")

        try:
            file_bytes = pdf_path.read_bytes()
        except PermissionError as e:
            raise SourceError(f"Permission denied reading PDF '{pdf_path}': {e}") from e
        except Exception as e:
            raise SourceError(f"Failed to read PDF file '{pdf_path}': {e}") from e

        if not file_bytes:
            raise SourceError(f"PDF file is empty (0 bytes): {pdf_path}")

        # Compute deterministic SHA-256 of file bytes
        hasher = hashlib.sha256()
        hasher.update(file_bytes)
        file_hash = hasher.hexdigest()

        # Open and inspect with pymupdf
        try:
            doc = pymupdf.open(stream=file_bytes, filetype="pdf")
        except Exception as e:
            raise SourceError(f"Invalid or corrupt PDF file '{pdf_path.name}': {e}") from e

        try:
            if doc.is_encrypted:
                raise SourceError(f"PDF file is encrypted or password-protected: {pdf_path.name}")

            page_count = len(doc)
            if page_count == 0:
                raise SourceError(f"PDF document contains 0 pages: {pdf_path.name}")

            meta = doc.metadata or {}

            # Author extraction
            author = (meta.get("author") or "").strip() or None

            # Date extraction
            date_val = parse_pdf_date(meta.get("creationDate") or meta.get("modDate"))
            if not date_val:
                # Fallback to file modification time
                mtime = pdf_path.stat().st_mtime
                date_val = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d")

            # Page-by-page text extraction
            page_contents: List[str] = []
            extracted_tables_by_page: dict[int, List[str]] = {}

            # Attempt table extraction with pdfplumber
            try:
                with pdfplumber.open(pdf_path) as plumber_pdf:
                    for idx, plumber_page in enumerate(plumber_pdf.pages, start=1):
                        tables = plumber_page.extract_tables()
                        formatted_tables = [
                            format_markdown_table(tbl) for tbl in tables if tbl
                        ]
                        if any(formatted_tables):
                            extracted_tables_by_page[idx] = [
                                t for t in formatted_tables if t.strip()
                            ]
            except Exception as e:
                logger.warning(
                    f"Could not extract tables from '{pdf_path.name}' via pdfplumber: {e}. "
                    "Proceeding with standard text extraction."
                )

            # Build markdown text per page
            for page_num in range(1, page_count + 1):
                page = doc[page_num - 1]
                text = page.get_text("text").strip()

                page_block_parts = [f"## Page {page_num} [p.{page_num}]"]

                if text:
                    page_block_parts.append(text)

                if page_num in extracted_tables_by_page:
                    for tbl_md in extracted_tables_by_page[page_num]:
                        page_block_parts.append(f"### Table (Page {page_num})\n\n{tbl_md}")

                page_contents.append("\n\n".join(page_block_parts))

            # Full document content
            full_content = "\n\n".join(page_contents) if page_contents else "*[No extractable text found]*"

            # Title extraction
            raw_title = (meta.get("title") or "").strip()
            if not raw_title or raw_title.lower() in {"untitled", "none", ""}:
                # Try first page heading heuristic or fallback to stem
                first_lines = [
                    line.strip()
                    for line in doc[0].get_text("text").splitlines()
                    if line.strip()
                ]
                if first_lines and len(first_lines[0]) <= 80:
                    raw_title = first_lines[0]
                else:
                    raw_title = pdf_path.stem.replace("_", " ").title()

            file_size_kb = max(1, round(len(file_bytes) / 1024))

            # Attach original PDF file
            attachment = Attachment(
                filename=pdf_path.name,
                content=file_bytes,
                source_path=pdf_path,
                mime_type="application/pdf",
            )

            # Build SourceItem
            extra_metadata = {
                "original_filename": pdf_path.name,
                "page_count": page_count,
                "file_size_kb": file_size_kb,
                "file_hash": file_hash,
            }
            resolved_identity = f"pdf:{pdf_path.resolve().as_posix()}"

            return SourceItem(
                source_id=resolved_identity,
                source_type="pdf",
                title=raw_title,
                content=full_content,
                date=date_val,
                author=author,
                tags=["ingested", "pdf"],
                attachments=[attachment],
                extra_metadata=extra_metadata,
                content_hash=file_hash,
            )

        finally:
            doc.close()

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a PDF SourceItem into a structured MarkdownNote."""
        orig_name = item.extra_metadata.get("original_filename", f"{item.title}.pdf")

        body_parts = [
            item.content,
            f"## Original Document\n\nOriginal: ![[{orig_name}]]",
        ]
        full_body = "\n\n".join(body_parts)

        return MarkdownNote(
            title=item.title,
            source="pdf",
            date=item.date or "",
            body=full_body,
            tags=list(item.tags),
            author=item.author,
            aliases=list(item.aliases),
            status=item.status,
            attachments=[orig_name],
            extra_metadata=dict(item.extra_metadata),
            folder="Ingested/PDF",
        )


# Register connector into the global SourceRegistry
SourceRegistry.register("pdf", PDFSource)
