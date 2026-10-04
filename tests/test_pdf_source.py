"""Comprehensive tests for PDF document source connector."""

import hashlib
from pathlib import Path

import pymupdf
import pytest

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.pdf_source import PDFSource, format_markdown_table, parse_pdf_date
from tracker import DeduplicationTracker, IngestionAction


def create_sample_pdf(
    file_path: Path,
    title: str = "Q3 2026 Financial Report",
    author: str = "Finance Department",
    date_str: str = "D:20260915120000",
    pages: int = 2,
    body_text: str = "Revenue grew 12% driven by strong performance.",
) -> Path:
    """Helper to generate a valid PDF file with custom metadata and pages."""
    doc = pymupdf.open()
    doc.set_metadata({
        "title": title,
        "author": author,
        "creationDate": date_str,
    })
    for i in range(1, pages + 1):
        page = doc.new_page()
        page.insert_text((50, 72), f"Page {i} Header")
        page.insert_text((50, 100), f"{body_text} - Page {i} details.")
    doc.save(str(file_path))
    doc.close()
    return file_path


def test_pdf_registered_in_registry():
    src_cls = SourceRegistry.get("pdf")
    assert src_cls is PDFSource
    sources_dict = SourceRegistry.list_sources()
    assert "pdf" in sources_dict


def test_parse_pdf_date():
    assert parse_pdf_date("D:20260915120000") == "2026-09-15"
    assert parse_pdf_date("2026-09-20") == "2026-09-20"
    assert parse_pdf_date("20260920") == "2026-09-20"
    assert parse_pdf_date(None) is None


def test_format_markdown_table():
    raw_table = [
        ["Product", "Q2", "Q3"],
        ["Product A", "$1.2M", "$1.4M"],
        ["Product B", "$800K", "$870K"],
    ]
    md = format_markdown_table(raw_table)
    assert "| Product | Q2 | Q3 |" in md
    assert "| --- | --- | --- |" in md
    assert "| Product A | $1.2M | $1.4M |" in md


@pytest.mark.asyncio
async def test_pdf_extraction_valid(tmp_path):
    pdf_path = tmp_path / "test_report.pdf"
    create_sample_pdf(pdf_path, title="Annual Overview", author="Jane Smith", pages=3)

    source = PDFSource()
    items = await source.fetch_items(file=pdf_path)

    assert len(items) == 1
    item = items[0]

    assert item.source_type == "pdf"
    assert item.source_id == f"pdf:{pdf_path.resolve().as_posix()}"
    assert item.title == "Annual Overview"
    assert item.author == "Jane Smith"
    assert item.date == "2026-09-15"
    assert item.extra_metadata["page_count"] == 3
    assert item.extra_metadata["original_filename"] == "test_report.pdf"
    assert item.extra_metadata["file_size_kb"] > 0

    # Content should include page references
    assert "[p.1]" in item.content
    assert "[p.2]" in item.content
    assert "[p.3]" in item.content

    # Attachment check
    assert len(item.attachments) == 1
    assert item.attachments[0].filename == "test_report.pdf"
    assert item.attachments[0].content == pdf_path.read_bytes()

    # SHA-256 match
    expected_hash = hashlib.sha256(pdf_path.read_bytes()).hexdigest()
    assert item.compute_content_hash() == expected_hash


@pytest.mark.asyncio
async def test_pdf_title_and_author_fallbacks(tmp_path):
    # PDF with no metadata title or author
    pdf_path = tmp_path / "simple_document.pdf"
    doc = pymupdf.open()
    page = doc.new_page()
    page.insert_text((50, 72), "Document Without Title")
    doc.save(str(pdf_path))
    doc.close()

    source = PDFSource()
    items = await source.fetch_items(file_path=pdf_path)
    assert len(items) == 1
    item = items[0]

    # Falls back to first line or filename stem
    assert item.title in ["Document Without Title", "Simple Document"]
    assert item.author is None


@pytest.mark.asyncio
async def test_pdf_convert_to_markdown(tmp_path):
    pdf_path = tmp_path / "q3_report.pdf"
    create_sample_pdf(pdf_path, title="Q3 Financials", author="CFO Office", pages=1)

    source = PDFSource()
    items = await source.fetch_items(file=pdf_path)
    note = await source.convert_to_markdown(items[0])

    assert isinstance(note, MarkdownNote)
    assert note.title == "Q3 Financials"
    assert note.source == "pdf"
    assert note.author == "CFO Office"
    assert "ingested" in note.tags
    assert "pdf" in note.tags
    assert note.folder == "Ingested/PDF"
    assert "Original: ![[q3_report.pdf]]" in note.body
    assert note.attachments == ["q3_report.pdf"]


@pytest.mark.asyncio
async def test_pdf_pipeline_deduplication_and_update(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    pdf_file = tmp_path / "financial_summary.pdf"
    create_sample_pdf(pdf_file, title="Financial Summary", body_text="Q1 Revenue $10M")

    source = PDFSource()

    # 1. First Ingestion -> NEW
    items = await source.fetch_items(file=pdf_file)
    action1, rel_path1 = await pipeline.process_item(source, items[0])
    assert action1 == IngestionAction.NEW
    assert rel_path1 is not None

    note_path = vault_dir / rel_path1
    assert note_path.exists()
    note_content = note_path.read_text(encoding="utf-8")
    assert "source: pdf" in note_content
    assert "original_filename: financial_summary.pdf" in note_content
    assert "Q1 Revenue $10M" in note_content
    assert "> **Source**: [[financial_summary.pdf]]" in note_content

    # Attachment check
    att_path = vault_dir / "Attachments" / "Ingested" / "financial_summary.pdf"
    assert att_path.exists()

    # 2. Second Ingestion with unchanged PDF -> UNCHANGED (skip)
    items_same = await source.fetch_items(file=pdf_file)
    action2, rel_path2 = await pipeline.process_item(source, items_same[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1

    # 3. Third Ingestion with modified PDF -> CHANGED (overwrites in place)
    create_sample_pdf(pdf_file, title="Financial Summary", body_text="Q1 Revenue $15M REVISED")
    items_mod = await source.fetch_items(file=pdf_file)
    action3, rel_path3 = await pipeline.process_item(source, items_mod[0])
    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1  # Overwrites the SAME file

    updated_content = note_path.read_text(encoding="utf-8")
    assert "Q1 Revenue $15M REVISED" in updated_content
    assert "Q1 Revenue $10M" not in updated_content

    tracker.close()


@pytest.mark.asyncio
async def test_pdf_missing_file_raises(tmp_path):
    source = PDFSource()
    missing_file = tmp_path / "non_existent.pdf"
    with pytest.raises(SourceError, match="does not exist"):
        await source.fetch_items(file=missing_file)


@pytest.mark.asyncio
async def test_pdf_empty_file_raises(tmp_path):
    empty_pdf = tmp_path / "empty.pdf"
    empty_pdf.write_bytes(b"")
    source = PDFSource()
    with pytest.raises(SourceError, match="empty"):
        await source.fetch_items(file=empty_pdf)


@pytest.mark.asyncio
async def test_pdf_corrupt_file_raises(tmp_path):
    corrupt_pdf = tmp_path / "corrupt.pdf"
    corrupt_pdf.write_bytes(b"This is not a PDF file at all.")
    source = PDFSource()
    with pytest.raises(SourceError, match="Invalid or corrupt PDF"):
        await source.fetch_items(file=corrupt_pdf)


@pytest.mark.asyncio
async def test_pdf_directory_batch_ingestion(tmp_path):
    pdf_dir = tmp_path / "pdf_folder"
    pdf_dir.mkdir()
    create_sample_pdf(pdf_dir / "doc1.pdf", title="Doc 1")
    create_sample_pdf(pdf_dir / "doc2.pdf", title="Doc 2")

    source = PDFSource()
    items = await source.fetch_items(file=pdf_dir)
    assert len(items) == 2
    titles = {item.title for item in items}
    assert "Doc 1" in titles
    assert "Doc 2" in titles


@pytest.mark.asyncio
async def test_pdf_cli_ingestion(tmp_path, capsys):
    vault_dir = tmp_path / "cli_vault"
    vault_dir.mkdir()
    tracker_path = tmp_path / "cli_tracker.sqlite"

    pdf_file = tmp_path / "cli_test_doc.pdf"
    create_sample_pdf(pdf_file, title="CLI Test Document", body_text="Testing via CLI")

    parser = create_parser()
    args = parser.parse_args([
        "--vault-path", str(vault_dir),
        "--tracker-db", str(tracker_path),
        "ingest",
        "--source", "pdf",
        "--file", str(pdf_file),
    ])

    ret = await async_main(args)
    assert ret == 0

    captured = capsys.readouterr()
    assert "Ingestion completed for 'pdf'" in captured.out

    # Verify Markdown note in vault
    notes = list((vault_dir / "Ingested" / "PDF").glob("*.md"))
    assert len(notes) == 1
    assert "Testing via CLI" in notes[0].read_text(encoding="utf-8")

    # Verify attachment in vault
    assert (vault_dir / "Attachments" / "Ingested" / "cli_test_doc.pdf").exists()


@pytest.mark.asyncio
async def test_pdf_same_filename_different_paths_separate_sources(tmp_path):
    """Two different directories containing report.pdf are treated as separate PDF source items."""
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    dir_a = tmp_path / "dir_a"
    dir_b = tmp_path / "dir_b"
    dir_a.mkdir()
    dir_b.mkdir()

    pdf_a = dir_a / "report.pdf"
    pdf_b = dir_b / "report.pdf"

    create_sample_pdf(pdf_a, title="Report A", body_text="Content from Dir A")
    create_sample_pdf(pdf_b, title="Report B", body_text="Content from Dir B")

    source = PDFSource()

    # Extract items
    items_a = await source.fetch_items(file=pdf_a)
    items_b = await source.fetch_items(file=pdf_b)

    assert len(items_a) == 1
    assert len(items_b) == 1

    item_a = items_a[0]
    item_b = items_b[0]

    # Verify source_id are distinct and match resolved paths
    assert item_a.source_id != item_b.source_id
    assert item_a.source_id == f"pdf:{pdf_a.resolve().as_posix()}"
    assert item_b.source_id == f"pdf:{pdf_b.resolve().as_posix()}"

    # Ingest both
    action_a, path_a = await pipeline.process_item(source, item_a)
    action_b, path_b = await pipeline.process_item(source, item_b)

    assert action_a == IngestionAction.NEW
    assert action_b == IngestionAction.NEW

    # Note files must be different
    assert path_a != path_b

    # Attachments must NOT overwrite each other
    att_dir = vault_dir / "Attachments" / "Ingested"
    assert (att_dir / "report.pdf").exists()
    assert (att_dir / "report_2.pdf").exists()

    # Attachment contents match their respective sources
    assert (att_dir / "report.pdf").read_bytes() == pdf_a.read_bytes()
    assert (att_dir / "report_2.pdf").read_bytes() == pdf_b.read_bytes()

    # Note B must reference report_2.pdf in embed and frontmatter
    note_b_content = (vault_dir / path_b).read_text(encoding="utf-8")
    assert "- report_2.pdf" in note_b_content
    assert "![[report_2.pdf]]" in note_b_content
    assert "[[report_2.pdf]]" in note_b_content

    # Re-ingest unchanged -> both produce UNCHANGED
    re_action_a, _ = await pipeline.process_item(source, item_a)
    re_action_b, _ = await pipeline.process_item(source, item_b)
    assert re_action_a == IngestionAction.UNCHANGED
    assert re_action_b == IngestionAction.UNCHANGED

    # Modifying report_b -> produces CHANGED and updates existing note in-place
    create_sample_pdf(pdf_b, title="Report B", body_text="Content from Dir B REVISED")
    items_b_mod = await source.fetch_items(file=pdf_b)
    mod_action_b, mod_path_b = await pipeline.process_item(source, items_b_mod[0])

    assert mod_action_b == IngestionAction.CHANGED
    assert mod_path_b == path_b  # Overwrites the same note in-place

    # Attachment report_2.pdf updated in-place without creating report_3.pdf
    assert not (att_dir / "report_3.pdf").exists()
    assert (att_dir / "report_2.pdf").read_bytes() == pdf_b.read_bytes()

    # Note B content updated and still references report_2.pdf
    updated_note_b = (vault_dir / path_b).read_text(encoding="utf-8")
    assert "Content from Dir B REVISED" in updated_note_b
    assert "- report_2.pdf" in updated_note_b
    assert "![[report_2.pdf]]" in updated_note_b

    tracker.close()
