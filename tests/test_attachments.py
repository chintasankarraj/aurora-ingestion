"""Tests for attachment handling and Obsidian embed references."""

from pathlib import Path

import pytest

from converter import resolve_collision_safe_attachment_name, save_attachment, write_note_to_vault
from exceptions import ConversionError, ExcludedFolderError
from models import Attachment, MarkdownNote


def test_attachment_model_embed_reference():
    att = Attachment(filename="sprint_board.png")
    assert att.embed_reference == "![[sprint_board.png]]"


def test_save_attachment_bytes(tmp_path):
    att = Attachment(
        filename="diagram.png",
        content=b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDRtest",
        mime_type="image/png",
    )
    saved_name = save_attachment(att, vault_path=tmp_path)
    assert saved_name == "diagram.png"

    dest_file = tmp_path / "Attachments" / "Ingested" / "diagram.png"
    assert dest_file.exists()
    assert dest_file.read_bytes() == att.content


def test_save_attachment_from_source_file(tmp_path):
    source_file = tmp_path / "original.pdf"
    source_file.write_bytes(b"%PDF-1.4 sample content")

    att = Attachment(
        filename="report.pdf",
        source_path=source_file,
    )
    saved_name = save_attachment(att, vault_path=tmp_path)
    assert saved_name == "report.pdf"

    dest_file = tmp_path / "Attachments" / "Ingested" / "report.pdf"
    assert dest_file.exists()
    assert dest_file.read_bytes() == b"%PDF-1.4 sample content"


def test_save_attachment_no_content_raises(tmp_path):
    att = Attachment(filename="empty.png")
    with pytest.raises(ConversionError):
        save_attachment(att, vault_path=tmp_path)


def test_write_note_with_attachments(tmp_path):
    att = Attachment(
        filename="chart.png",
        content=b"chart bytes",
    )
    note = MarkdownNote(
        title="Weekly Report with Chart",
        source="email",
        date="2026-09-22",
        body="Here is the chart:\n\n![[chart.png]]",
    )

    abs_path, rel_path, saved_attachments = write_note_to_vault(
        note=note,
        vault_path=tmp_path,
        attachments=[att],
    )

    assert "chart.png" in saved_attachments
    assert (tmp_path / "Attachments" / "Ingested" / "chart.png").exists()

    # Verify frontmatter records attachments
    content = abs_path.read_text(encoding="utf-8")
    assert "attachments:" in content
    assert "- chart.png" in content
    assert "![[chart.png]]" in content


def test_save_attachment_collision_handling(tmp_path):
    # 1. First attachment
    att1 = Attachment(filename="photo.jpg", content=b"photo 1 content")
    saved1 = save_attachment(att1, vault_path=tmp_path)
    assert saved1 == "photo.jpg"

    # 2. Same filename, identical content -> reuses photo.jpg
    att1_dup = Attachment(filename="photo.jpg", content=b"photo 1 content")
    saved1_dup = save_attachment(att1_dup, vault_path=tmp_path)
    assert saved1_dup == "photo.jpg"

    # 3. Same filename, different content -> collision safe photo_2.jpg
    att2 = Attachment(filename="photo.jpg", content=b"photo 2 content (different)")
    saved2 = save_attachment(att2, vault_path=tmp_path)
    assert saved2 == "photo_2.jpg"
    assert (tmp_path / "Attachments" / "Ingested" / "photo_2.jpg").read_bytes() == b"photo 2 content (different)"

    # 4. Third different content -> photo_3.jpg
    att3 = Attachment(filename="photo.jpg", content=b"photo 3 content (third)")
    saved3 = save_attachment(att3, vault_path=tmp_path)
    assert saved3 == "photo_3.jpg"

    # 5. Same as photo_2 content -> reuses photo_2.jpg
    att2_dup = Attachment(filename="photo.jpg", content=b"photo 2 content (different)")
    saved2_dup = save_attachment(att2_dup, vault_path=tmp_path)
    assert saved2_dup == "photo_2.jpg"


def test_write_note_with_colliding_attachment_updates_references(tmp_path):
    # Pre-populate an existing attachment with different content
    att_dir = tmp_path / "Attachments" / "Ingested"
    att_dir.mkdir(parents=True)
    (att_dir / "invoice.pdf").write_bytes(b"existing invoice content")

    # Incoming note referencing invoice.pdf with different content
    att = Attachment(filename="invoice.pdf", content=b"new invoice content")
    note = MarkdownNote(
        title="Invoice October",
        source="email",
        date="2026-10-01",
        body="Please see invoice: ![[invoice.pdf]] or link [[invoice.pdf]].",
        attachments=["invoice.pdf"],
    )

    abs_path, rel_path, saved_attachments = write_note_to_vault(
        note=note,
        vault_path=tmp_path,
        attachments=[att],
    )

    # Should be saved as invoice_2.pdf
    assert "invoice_2.pdf" in saved_attachments
    assert (att_dir / "invoice_2.pdf").exists()
    # Original invoice.pdf preserved
    assert (att_dir / "invoice.pdf").read_bytes() == b"existing invoice content"

    # Note body and frontmatter must reference invoice_2.pdf
    content = abs_path.read_text(encoding="utf-8")
    assert "- invoice_2.pdf" in content
    assert "![[invoice_2.pdf]]" in content
    assert "[[invoice_2.pdf]]" in content
