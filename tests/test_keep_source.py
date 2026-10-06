"""Comprehensive unit and integration tests for the Google Keep source connector."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.keep_source import (
    KeepSource,
    derive_keep_stable_id,
    extract_keep_checklist,
    extract_keep_content,
    extract_keep_labels,
    parse_keep_timestamp,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers
# ---------------------------------------------------------------------------

def write_keep_json(
    folder: Path,
    filename: str = "Note.json",
    title: str = "Test Note",
    text_content: str = "This is a note body.",
    list_content: list | None = None,
    created_usec: int | None = 1774350000000000,
    edited_usec: int | None = 1774357200000000,
    labels: list | None = None,
    color: str = "DEFAULT",
    is_archived: bool = False,
    is_trashed: bool = False,
    attachments: list | None = None,
    note_id: str | None = None,
) -> Path:
    """Helper to generate synthetic Google Keep JSON files."""
    data: dict = {
        "title": title,
        "textContent": text_content,
        "color": color,
        "isArchived": is_archived,
        "isTrashed": is_trashed,
    }
    if note_id is not None:
        data["id"] = note_id
    if list_content is not None:
        data["listContent"] = list_content
    if created_usec is not None:
        data["createdTimestampUsec"] = created_usec
    if edited_usec is not None:
        data["userEditedTimestampUsec"] = edited_usec
    if labels is not None:
        data["labels"] = labels
    if attachments is not None:
        data["attachments"] = attachments

    file_path = folder / filename
    file_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return file_path


# ---------------------------------------------------------------------------
# 1. Connector Registration
# ---------------------------------------------------------------------------

def test_keep_source_registration():
    """1. Test that KeepSource registers as 'google-keep' in SourceRegistry."""
    sources = SourceRegistry.list_sources()
    assert "google-keep" in sources
    assert sources["google-keep"] == "KeepSource"

    source = KeepSource()
    assert source.source_type == "google-keep"
    assert source.display_name == "Google Keep"


# ---------------------------------------------------------------------------
# 2. Missing Configured Path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_configured_path_raises_source_error(monkeypatch):
    """2. Test that missing export path raises clear SourceError."""
    monkeypatch.delenv("GOOGLE_KEEP_EXPORT_PATH", raising=False)
    monkeypatch.delenv("KEEP_EXPORT_PATH", raising=False)

    source = KeepSource(export_path=None)
    with pytest.raises(SourceError, match="Google Keep export path is missing"):
        await source.fetch_items()


# ---------------------------------------------------------------------------
# 3. Invalid Path
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_invalid_path_raises_source_error(tmp_path):
    """3. Test that nonexistent path or non-json file raises SourceError."""
    nonexistent = tmp_path / "does_not_exist"
    source = KeepSource(export_path=nonexistent)
    with pytest.raises(SourceError, match="does not exist"):
        await source.fetch_items()

    txt_file = tmp_path / "notes.txt"
    txt_file.write_text("hello", encoding="utf-8")
    source_txt = KeepSource(export_path=txt_file)
    with pytest.raises(SourceError, match="must have a .json extension"):
        await source_txt.fetch_items()


# ---------------------------------------------------------------------------
# 4. Single JSON File Ingestion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_json_file_ingestion(tmp_path):
    """4. Test direct path to a single Keep JSON file."""
    note_path = write_keep_json(tmp_path, filename="Shopping.json", title="Shopping List")
    source = KeepSource(export_path=note_path)

    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].title == "Shopping List"
    assert items[0].source_type == "google-keep"


# ---------------------------------------------------------------------------
# 5. Directory Ingestion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_directory_ingestion(tmp_path):
    """5. Test discovery and ingestion of multiple notes in a directory."""
    write_keep_json(tmp_path, filename="NoteA.json", title="Note A")
    write_keep_json(tmp_path, filename="NoteB.json", title="Note B")
    (tmp_path / "ignore_me.txt").write_text("ignored", encoding="utf-8")

    source = KeepSource(export_path=tmp_path)
    items = await source.fetch_items()

    assert len(items) == 2
    titles = [item.title for item in items]
    assert "Note A" in titles
    assert "Note B" in titles


# ---------------------------------------------------------------------------
# 6. Deterministic File Discovery
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_deterministic_file_discovery(tmp_path):
    """6. Test deterministic discovery ordered alphabetically by filename."""
    write_keep_json(tmp_path, filename="Zeta.json", title="Zeta")
    write_keep_json(tmp_path, filename="Alpha.json", title="Alpha")
    write_keep_json(tmp_path, filename="Beta.json", title="Beta")

    source = KeepSource(export_path=tmp_path)
    items = await source.fetch_items()

    assert len(items) == 3
    assert items[0].title == "Alpha"
    assert items[1].title == "Beta"
    assert items[2].title == "Zeta"


# ---------------------------------------------------------------------------
# 7. Malformed JSON Isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_malformed_json_handling(tmp_path):
    """7. Test malformed JSON fails cleanly for single file and isolates in directory."""
    bad_file = tmp_path / "Corrupt.json"
    bad_file.write_text("{invalid json", encoding="utf-8")

    # Single-file mode: raises SourceError
    source_single = KeepSource(export_path=bad_file)
    with pytest.raises(SourceError, match="Malformed Google Keep JSON"):
        await source_single.fetch_items()

    # Directory mode: skips corrupt file and processes valid ones
    write_keep_json(tmp_path, filename="Valid.json", title="Valid Note")
    source_dir = KeepSource(export_path=tmp_path)
    items = await source_dir.fetch_items()

    assert len(items) == 1
    assert items[0].title == "Valid Note"


# ---------------------------------------------------------------------------
# 8. Title Extraction
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_title_extraction_and_fallback(tmp_path):
    """8. Test note title extraction and untitled fallback."""
    write_keep_json(tmp_path, filename="Named.json", title="Project Roadmap")
    write_keep_json(tmp_path, filename="Empty.json", title="")

    source = KeepSource(export_path=tmp_path)
    items = await source.fetch_items()

    assert items[0].title == "Untitled Keep Note"
    assert items[1].title == "Project Roadmap"


# ---------------------------------------------------------------------------
# 9. Plain Text Conversion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_plain_text_conversion(tmp_path):
    """9. Test plain text note converts to structured Markdown paragraphs."""
    note_path = write_keep_json(
        tmp_path,
        filename="Plain.json",
        title="Shopping",
        text_content="Buy milk\nBuy eggs",
    )
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()
    note = await source.convert_to_markdown(items[0])

    assert "# Shopping" in note.body
    assert "> **Source**: Google Keep" in note.body
    assert "Buy milk\n\nBuy eggs" in note.body


# ---------------------------------------------------------------------------
# 10. Created Timestamp Conversion
# ---------------------------------------------------------------------------

def test_created_timestamp_conversion():
    """10. Test parsing microseconds timestamp into UTC datetime."""
    # 1774350000000000 usec = 2026-03-24 11:00:00 UTC
    dt = parse_keep_timestamp(1774350000000000)
    assert dt is not None
    assert dt.strftime("%Y-%m-%d") == "2026-03-24"
    assert dt.hour == 11
    assert dt.minute == 0


# ---------------------------------------------------------------------------
# 11. Edited Timestamp Fallback
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_edited_timestamp_fallback(tmp_path):
    """11. Test falling back to userEditedTimestampUsec when created is missing."""
    note_path = write_keep_json(
        tmp_path,
        filename="Fallback.json",
        created_usec=None,
        edited_usec=1774357200000000,  # 2026-03-24 13:00:00 UTC
    )
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()

    assert items[0].date == "2026-03-24"
    assert "updated_at" in items[0].extra_metadata


# ---------------------------------------------------------------------------
# 12. Checklist Conversion
# ---------------------------------------------------------------------------

def test_checklist_conversion():
    """12. Test listContent converted to Markdown checkboxes preserving order."""
    data = {
        "listContent": [
            {"text": "Task A", "isChecked": False},
            {"text": "Task B", "isChecked": True},
        ]
    }
    lines, _ = extract_keep_checklist(data)
    assert len(lines) == 2
    assert lines[0] == "- [ ] Task A"
    assert lines[1] == "- [x] Task B"


# ---------------------------------------------------------------------------
# 13. Checked Checklist Item
# ---------------------------------------------------------------------------

def test_checked_checklist_item():
    """13. Test checked checklist item renders as - [x]."""
    data = {"listContent": [{"text": "Completed task", "isChecked": True}]}
    lines, _ = extract_keep_checklist(data)
    assert len(lines) == 1
    assert lines[0] == "- [x] Completed task"


# ---------------------------------------------------------------------------
# 14. Unchecked Checklist Item
# ---------------------------------------------------------------------------

def test_unchecked_checklist_item():
    """14. Test unchecked checklist item renders as - [ ]."""
    data = {"listContent": [{"text": "Pending task", "isChecked": False}]}
    lines, _ = extract_keep_checklist(data)
    assert len(lines) == 1
    assert lines[0] == "- [ ] Pending task"


# ---------------------------------------------------------------------------
# 15. No Checklist/Text Duplication
# ---------------------------------------------------------------------------

def test_no_checklist_text_duplication():
    """15. Test that textContent repeating checklist items is not duplicated."""
    data = {
        "listContent": [
            {"text": "Buy milk", "isChecked": False},
            {"text": "Buy eggs", "isChecked": True},
        ],
        "textContent": "Buy milk\nBuy eggs",
    }
    body = extract_keep_content(data)

    # Should only contain the checklist
    assert body.strip() == "- [ ] Buy milk\n- [x] Buy eggs"

    # With extra preamble text
    data_with_preamble = {
        "listContent": [
            {"text": "Buy milk", "isChecked": False},
        ],
        "textContent": "Grocery list:\nBuy milk",
    }
    body_preamble = extract_keep_content(data_with_preamble)
    assert "Grocery list:" in body_preamble
    assert "- [ ] Buy milk" in body_preamble
    assert body_preamble.count("Buy milk") == 1


# ---------------------------------------------------------------------------
# 16. Labels -> Tags
# ---------------------------------------------------------------------------

def test_labels_to_tags_mapping():
    """16. Test Keep labels mapped into frontmatter tags after ingested and google-keep."""
    data = {
        "labels": [
            {"name": "Personal"},
            {"name": "Work/Projects"},
            "ReadingList",
        ]
    }
    labels = extract_keep_labels(data)
    assert "personal" in labels
    assert "work-projects" in labels
    assert "readinglist" in labels


# ---------------------------------------------------------------------------
# 17. Leading '#' Label Sanitization
# ---------------------------------------------------------------------------

def test_leading_hash_label_sanitization():
    """17. Test that leading '#' symbols in Keep labels are cleanly stripped."""
    data = {
        "labels": [
            {"name": "#Finance"},
            {"name": "  #Budget  "},
            "#RawHashTag",
        ]
    }
    labels = extract_keep_labels(data)
    for tag in labels:
        assert not tag.startswith("#")
    assert "finance" in labels
    assert "budget" in labels
    assert "rawhashtag" in labels


# ---------------------------------------------------------------------------
# 18. Color Metadata
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_color_metadata(tmp_path):
    """18. Test Keep color is preserved in metadata."""
    note_path = write_keep_json(tmp_path, filename="ColorMeta.json", color="RED")
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()
    note = await source.convert_to_markdown(items[0])

    assert note.extra_metadata["color"] == "RED"


# ---------------------------------------------------------------------------
# 19. Color -> keep-* Tag
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_color_to_keep_tag(tmp_path):
    """19. Test Keep color generates a keep-<color> frontmatter tag."""
    note_path = write_keep_json(tmp_path, filename="BlueNote.json", color="BLUE")
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()
    note = await source.convert_to_markdown(items[0])

    assert "keep-blue" in note.tags
    assert "ingested" in note.tags
    assert "google-keep" in note.tags

    # Default color should not add keep-default tag
    default_path = write_keep_json(tmp_path, filename="DefNote.json", color="DEFAULT")
    items_def = await KeepSource(export_path=default_path).fetch_items()
    assert "color" not in items_def[0].extra_metadata
    assert "keep-default" not in items_def[0].tags


# ---------------------------------------------------------------------------
# 20. Archived State
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_archived_state(tmp_path):
    """20. Test archived note status and frontmatter flag."""
    note_path = write_keep_json(tmp_path, filename="Archived.json", is_archived=True)
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()
    note = await source.convert_to_markdown(items[0])

    assert note.status == "archived"
    assert note.extra_metadata["archived"] is True
    assert note.extra_metadata["trashed"] is False


# ---------------------------------------------------------------------------
# 21. Trashed State
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_trashed_state(tmp_path):
    """21. Test trashed note status and frontmatter flag."""
    note_path = write_keep_json(tmp_path, filename="Trash.json", is_trashed=True)
    source = KeepSource(export_path=note_path)
    items = await source.fetch_items()
    note = await source.convert_to_markdown(items[0])

    assert note.status == "trashed"
    assert note.extra_metadata["trashed"] is True


# ---------------------------------------------------------------------------
# 22. Stable Source Identity
# ---------------------------------------------------------------------------
# 22. Stable Source Identity & Hardened Deduplication
# ---------------------------------------------------------------------------

def test_stable_source_identity_explicit_id(tmp_path):
    """22a. Test explicit Keep ID preserves exact google-keep:<id> behavior."""
    p = tmp_path / "Memo.json"

    # Explicit id
    id1 = derive_keep_stable_id({"id": "keep-note-99"}, p)
    assert id1 == "google-keep:keep-note-99"

    # Explicit noteId
    id2 = derive_keep_stable_id({"noteId": "note-42"}, p)
    assert id2 == "google-keep:note-42"

    # Explicit serverId
    id3 = derive_keep_stable_id({"serverId": "srv-100"}, p)
    assert id3 == "google-keep:srv-100"


def test_two_notes_same_created_timestamp_distinguished_by_immutable_metadata(tmp_path):
    """22b. Test two notes with same createdTimestampUsec get different IDs when immutable metadata exists."""
    p = tmp_path / "Note.json"
    ts = 1774350000000000

    note_a = {"createdTimestampUsec": ts, "exportId": "export-alpha"}
    note_b = {"createdTimestampUsec": ts, "exportId": "export-beta"}

    id_a = derive_keep_stable_id(note_a, p)
    id_b = derive_keep_stable_id(note_b, p)

    assert id_a.startswith("google-keep:")
    assert id_b.startswith("google-keep:")
    assert id_a != id_b


def test_editing_content_does_not_change_fallback_source_id(tmp_path):
    """22c. Test editing title, text, labels, or color does not change fallback source ID."""
    p = tmp_path / "ProjectNotes.json"
    ts = 1774350000000000

    note_v1 = {
        "title": "Initial Project Title",
        "textContent": "Initial draft body text.",
        "createdTimestampUsec": ts,
        "color": "RED",
        "labels": [{"name": "draft"}],
    }
    note_v2 = {
        "title": "Updated Final Project Title",
        "textContent": "Completely rewritten body with checklist.",
        "createdTimestampUsec": ts,
        "color": "BLUE",
        "labels": [{"name": "published"}],
    }

    id_v1 = derive_keep_stable_id(note_v1, p)
    id_v2 = derive_keep_stable_id(note_v2, p)

    assert id_v1 == id_v2


def test_fallback_source_id_is_deterministic(tmp_path):
    """22d. Test fallback source ID calculation is deterministic across repeated calls."""
    p = tmp_path / "Memo.json"
    data = {
        "title": "Deterministic Test",
        "createdTimestampUsec": 1774350000000000,
        "exportId": "doc-uuid-999",
    }

    results = [derive_keep_stable_id(data, p) for _ in range(10)]
    assert len(set(results)) == 1
    assert results[0].startswith("google-keep:")


def test_filename_fallback_only_used_when_no_better_stable_identity_exists(tmp_path):
    """22e. Test filename fallback is only used when no better stable identity material exists."""
    p_orig = tmp_path / "OriginalName.json"
    p_renamed = tmp_path / "RenamedFile.json"
    ts = 1774350000000000

    # Case A: Better immutable identifier exists (exportId) -> Renaming file does NOT change source ID
    with_better_id = {"createdTimestampUsec": ts, "exportId": "stable-export-id-1"}
    id_orig_a = derive_keep_stable_id(with_better_id, p_orig)
    id_renamed_a = derive_keep_stable_id(with_better_id, p_renamed)
    assert id_orig_a == id_renamed_a

    # Case B: No better immutable identifier exists -> filename stem is used as last-resort discriminator
    without_better_id = {"createdTimestampUsec": ts}
    id_orig_b = derive_keep_stable_id(without_better_id, p_orig)
    id_renamed_b = derive_keep_stable_id(without_better_id, p_renamed)
    assert id_orig_b != id_renamed_b

    # Case C: Completely empty metadata -> filename stem is used as fallback discriminator
    id_orig_c = derive_keep_stable_id({}, p_orig)
    id_renamed_c = derive_keep_stable_id({}, p_renamed)
    assert id_orig_c != id_renamed_c


# ---------------------------------------------------------------------------
# 23. New Note Ingestion
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_new_note_ingestion(tmp_path):
    """23. Test new Keep note creates a Markdown note in Ingested/Notes/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_keep_new.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()
    write_keep_json(export_dir, filename="Meeting.json", title="Design Sync", text_content="Key decisions.")

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()
    assert len(items) == 1

    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW
    assert rel_path.startswith("Ingested/Notes/")

    note_file = vault_dir / rel_path
    assert note_file.exists()
    content = note_file.read_text(encoding="utf-8")
    assert "# Design Sync" in content
    assert "> **Source**: Google Keep" in content
    assert "Key decisions." in content


# ---------------------------------------------------------------------------
# 24. Unchanged Note Deduplication
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unchanged_note_deduplication(tmp_path):
    """24. Test unchanged Keep note is skipped by deduplication tracker."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_keep_unc.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()
    write_keep_json(export_dir, filename="Recipes.json", title="Pancake Recipe", text_content="Flour, eggs, milk.")

    source = KeepSource(export_path=export_dir)

    # First run
    items1 = await source.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source, items1[0])
    assert action1 == IngestionAction.NEW

    # Second run without changes
    items2 = await source.fetch_items()
    action2, rel_path2 = await pipeline.process_item(source, items2[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1


# ---------------------------------------------------------------------------
# 25. Modified Note In-Place Update
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_modified_note_in_place_update(tmp_path):
    """25. Test modified Keep note overwrites the same file in place without duplicate."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_keep_mod.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()
    note_path = write_keep_json(
        export_dir,
        filename="Todo.json",
        title="Todo List",
        text_content="Initial draft.",
        created_usec=1774350000000000,
    )

    source = KeepSource(export_path=export_dir)

    # First run
    items1 = await source.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source, items1[0])
    assert action1 == IngestionAction.NEW

    # Edit note content while preserving createdTimestampUsec
    write_keep_json(
        export_dir,
        filename="Todo.json",
        title="Todo List",
        text_content="Updated final tasks.",
        created_usec=1774350000000000,
        edited_usec=1774359000000000,
    )

    items2 = await source.fetch_items()
    action2, rel_path2 = await pipeline.process_item(source, items2[0])
    assert action2 == IngestionAction.CHANGED
    assert rel_path2 == rel_path1  # Overwrites exact same relative path

    note_file = vault_dir / rel_path1
    text = note_file.read_text(encoding="utf-8")
    assert "Updated final tasks." in text
    assert "Initial draft." not in text

    # Confirm only one file exists
    notes = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(notes) == 1


# ---------------------------------------------------------------------------
# 26. Filename Collision
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_filename_collision_resolution(tmp_path):
    """26. Test two different Keep notes with identical title get unique filenames."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_keep_col.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()

    write_keep_json(
        export_dir,
        filename="Note1.json",
        title="Project Ideas",
        text_content="Idea 1",
        created_usec=1774350000000000,
    )
    write_keep_json(
        export_dir,
        filename="Note2.json",
        title="Project Ideas",
        text_content="Idea 2",
        created_usec=1774351000000000,
    )

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()
    assert len(items) == 2

    _, rel1 = await pipeline.process_item(source, items[0])
    _, rel2 = await pipeline.process_item(source, items[1])

    assert rel1 != rel2
    assert "_2.md" in rel2 or "_2.md" in rel1


# ---------------------------------------------------------------------------
# 27. Attachment Copying
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_attachment_copying(tmp_path):
    """27. Test copying local Keep attachment into Attachments/Ingested/ and embedding."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_keep_att.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()

    # Create dummy attachment image file
    img_file = export_dir / "diagram.png"
    img_bytes = b"\x89PNG\r\n\x1a\n" + b"TEST_IMAGE_DATA"
    img_file.write_bytes(img_bytes)

    write_keep_json(
        export_dir,
        filename="Diagram.json",
        title="Architecture",
        attachments=[{"filePath": "diagram.png", "mimetype": "image/png"}],
    )

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()
    assert len(items) == 1
    assert len(items[0].attachments) == 1
    assert items[0].attachments[0].filename == "diagram.png"

    _, rel_path = await pipeline.process_item(source, items[0])
    note_content = (vault_dir / rel_path).read_text(encoding="utf-8")
    assert "![[diagram.png]]" in note_content

    saved_att = vault_dir / "Attachments" / "Ingested" / "diagram.png"
    assert saved_att.exists()
    assert saved_att.read_bytes() == img_bytes


# ---------------------------------------------------------------------------
# 28. Attachment Collision
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_attachment_collision(tmp_path):
    """28. Test multiple attachments with same basename get unique filenames."""
    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()

    sub1 = export_dir / "sub1"
    sub2 = export_dir / "sub2"
    sub1.mkdir()
    sub2.mkdir()

    img1 = sub1 / "photo.png"
    img2 = sub2 / "photo.png"
    img1.write_bytes(b"ONE" * 20)
    img2.write_bytes(b"TWO" * 20)

    write_keep_json(
        export_dir,
        filename="Gallery.json",
        title="Photo Gallery",
        attachments=[
            {"filePath": "sub1/photo.png"},
            {"filePath": "sub2/photo.png"},
        ],
    )

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()
    assert len(items) == 1

    atts = items[0].attachments
    assert len(atts) == 2
    assert atts[0].filename == "photo.png"
    assert atts[1].filename == "photo_2.png"
    assert "![[photo.png]]" in items[0].content
    assert "![[photo_2.png]]" in items[0].content


# ---------------------------------------------------------------------------
# 29. Missing Attachment Handling
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_missing_attachment_handling(tmp_path):
    """29. Test that nonexistent attachment file logs warning without failing note."""
    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()

    write_keep_json(
        export_dir,
        filename="MissingAtt.json",
        title="Missing Attachment Note",
        attachments=[{"filePath": "nonexistent_file.png"}],
    )

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()

    assert len(items) == 1
    assert items[0].title == "Missing Attachment Note"
    assert len(items[0].attachments) == 0


# ---------------------------------------------------------------------------
# 30. Batch Isolation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_batch_isolation(tmp_path):
    """30. Test that bad file in a batch does not abort other notes."""
    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()

    write_keep_json(export_dir, filename="Note1.json", title="Good Note 1")
    (export_dir / "Corrupt.json").write_text("{bad json", encoding="utf-8")
    write_keep_json(export_dir, filename="Note2.json", title="Good Note 2")

    source = KeepSource(export_path=export_dir)
    items = await source.fetch_items()

    assert len(items) == 2
    titles = [i.title for i in items]
    assert "Good Note 1" in titles
    assert "Good Note 2" in titles


# ---------------------------------------------------------------------------
# 31. CLI Registration
# ---------------------------------------------------------------------------

def test_cli_parser_registration():
    """31. Test CLI parser handles ingest-google-keep shortcut and arguments."""
    parser = create_parser()

    # Shortcut: ingest-google-keep
    args1 = parser.parse_args(["ingest-google-keep", "--path", "path/to/keep"])
    assert args1.command == "ingest-google-keep"
    assert args1.path == "path/to/keep"

    # Generic: ingest --source google-keep
    args2 = parser.parse_args(["ingest", "--source", "google-keep", "--path", "path/to/keep"])
    assert args2.command == "ingest"
    assert args2.source == "google-keep"
    assert args2.path == "path/to/keep"


# ---------------------------------------------------------------------------
# 32. Generic CLI Behavior
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_cli_execution_end_to_end(tmp_path):
    """32. Test end-to-end execution of CLI ingest-google-keep command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_keep.sqlite"

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()
    write_keep_json(export_dir, filename="CLI_Note.json", title="CLI Note", text_content="CLI Body.")

    parser = create_parser()
    args = parser.parse_args(["ingest-google-keep", "--path", str(export_dir)])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)
    assert exit_code == 0

    notes = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(notes) == 1
    assert "CLI Body." in notes[0].read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_generic_cli_execution_end_to_end(tmp_path):
    """33. Test end-to-end execution of generic CLI ingest --source google-keep command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_generic_keep.sqlite"

    export_dir = tmp_path / "TakeoutKeep"
    export_dir.mkdir()
    write_keep_json(export_dir, filename="Generic_Note.json", title="Generic CLI Note", text_content="Generic CLI Body.")

    parser = create_parser()
    args = parser.parse_args(["ingest", "--source", "google-keep", "--path", str(export_dir)])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)
    assert exit_code == 0

    notes = list((vault_dir / "Ingested" / "Notes").glob("*.md"))
    assert len(notes) == 1
    assert "Generic CLI Body." in notes[0].read_text(encoding="utf-8")

