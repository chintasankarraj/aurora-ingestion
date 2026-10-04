"""Tests for Markdown note conversion, frontmatter formatting, and vault writing."""

from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from converter import (
    build_attribution_block,
    extract_date_prefix,
    format_yaml_frontmatter,
    generate_note_filename,
    render_markdown_document,
    resolve_unique_filepath,
    sanitize_filename_title,
    validate_vault_destination,
    write_note_to_vault,
)
from exceptions import ExcludedFolderError, VaultPathError
from models import MarkdownNote


def test_sanitize_filename_title():
    # Basic space replacement
    assert sanitize_filename_title("Weekly Team Standup Notes") == "Weekly_Team_Standup_Notes"

    # Stripping forbidden characters: ? : * " < > | / \
    assert sanitize_filename_title("What Is A Vector DB? Part 1: Intro*") == "What_Is_A_Vector_DB_Part_1_Intro"
    assert sanitize_filename_title('<Dangerous> "Title" | With / Slash \\') == "Dangerous_Title_With_Slash"

    # Truncate to maximum 80 characters
    long_title = "A" * 100
    sanitized = sanitize_filename_title(long_title, max_length=80)
    assert len(sanitized) == 80
    assert sanitized == "A" * 80

    # Truncate title ending cleanly without trailing underscores
    title_with_underscore = ("Word_" * 25)  # 125 chars
    sanitized_words = sanitize_filename_title(title_with_underscore, max_length=80)
    assert len(sanitized_words) <= 80
    assert not sanitized_words.endswith("_")

    # Empty or all special characters fallback
    assert sanitize_filename_title("???:::***") == "Untitled"
    assert sanitize_filename_title("") == "Untitled"


def test_generate_note_filename():
    filename = generate_note_filename(
        date_val="2026-09-22",
        source_type="email",
        title="Weekly Team Standup Notes",
    )
    assert filename == "2026-09-22_email_Weekly_Team_Standup_Notes.md"

    # Test with datetime object
    dt = datetime(2026, 9, 20, 15, 30, 0)
    filename_yt = generate_note_filename(
        date_val=dt,
        source_type="youtube",
        title="How Transformers Work",
    )
    assert filename_yt == "2026-09-20_youtube_How_Transformers_Work.md"


def test_resolve_unique_filepath(tmp_path):
    # If file doesn't exist, use original name
    base_name = "2026-09-22_email_Test_Note.md"
    path1 = resolve_unique_filepath(tmp_path, base_name)
    assert path1 == tmp_path / base_name
    path1.write_text("Note 1")

    # Next call should generate _2
    path2 = resolve_unique_filepath(tmp_path, base_name)
    assert path2 == tmp_path / "2026-09-22_email_Test_Note_2.md"
    path2.write_text("Note 2")

    # Next call should generate _3
    path3 = resolve_unique_filepath(tmp_path, base_name)
    assert path3 == tmp_path / "2026-09-22_email_Test_Note_3.md"


def test_format_yaml_frontmatter():
    meta = {
        "title": "Test Title",
        "date": "2026-09-22",
        "source": "web",
        "tags": ["ingested", "web", "tech"],
        "ingested_at": "2026-09-22T16:45:00+00:00",
    }
    fm_block = format_yaml_frontmatter(meta)
    assert fm_block.startswith("---\n")
    assert fm_block.endswith("---\n")

    # Parse back with PyYAML to verify validity
    content_inside = fm_block.strip()[3:-3].strip()
    parsed = yaml.safe_load(content_inside)
    assert parsed["title"] == "Test Title"
    assert parsed["source"] == "web"
    assert parsed["tags"] == ["ingested", "web", "tech"]


def test_markdown_note_frontmatter_enforces_ingested_tag():
    note = MarkdownNote(
        title="My Note",
        source="email",
        date="2026-09-22",
        body="Body text",
        tags=["email", "work"],  # Does not include 'ingested'
    )
    fm = note.to_frontmatter_dict()
    assert fm["tags"][0] == "ingested"
    assert "email" in fm["tags"]
    assert "work" in fm["tags"]


def test_render_markdown_document():
    note = MarkdownNote(
        title="Understanding Vector Databases",
        source="web",
        date="2026-09-18",
        source_url="https://example.com/vector-db",
        author="Jane Smith",
        tags=["ingested", "web"],
        body="## What is a Vector DB?\n\nA vector database is optimized for vectors.",
    )
    rendered = render_markdown_document(note)

    # Starts with YAML frontmatter
    assert rendered.startswith("---\n")
    assert "title: Understanding Vector Databases" in rendered

    # Contains H1 matching title
    assert "\n# Understanding Vector Databases\n" in rendered

    # Contains attribution blockquote
    assert "> **Source**: [Web](https://example.com/vector-db)" in rendered
    assert "> **Author**: Jane Smith" in rendered

    # Contains body content
    assert "## What is a Vector DB?" in rendered


def test_write_note_to_vault_creates_folder_and_file(tmp_path):
    note = MarkdownNote(
        title="Weekly Team Standup Notes",
        source="email",
        date="2026-09-22",
        body="## Updates\n- Work in progress.",
        extra_metadata={"from": "alice@company.com", "to": "team@company.com"},
    )

    abs_path, rel_path, _ = write_note_to_vault(note, vault_path=tmp_path)

    assert abs_path.exists()
    # Should be placed into Ingested/Email/
    assert "Ingested/Email" in rel_path or "Ingested\\Email" in str(abs_path)
    assert abs_path.name == "2026-09-22_email_Weekly_Team_Standup_Notes.md"

    # Verify UTF-8 output with no BOM
    raw_bytes = abs_path.read_bytes()
    assert not raw_bytes.startswith(b"\xef\xbb\xbf")  # No UTF-8 BOM

    content = abs_path.read_text(encoding="utf-8")
    assert "# Weekly Team Standup Notes" in content
    assert "alice@company.com" in content


def test_write_note_to_vault_overwrites_in_place(tmp_path):
    note1 = MarkdownNote(
        title="My Document",
        source="pdf",
        date="2026-09-15",
        body="First version",
    )
    abs_path1, rel_path1, _ = write_note_to_vault(note1, vault_path=tmp_path)
    assert abs_path1.read_text(encoding="utf-8").find("First version") != -1

    # Overwrite in-place
    note2 = MarkdownNote(
        title="My Document",
        source="pdf",
        date="2026-09-15",
        body="Updated version",
    )
    abs_path2, rel_path2, _ = write_note_to_vault(
        note2, vault_path=tmp_path, existing_vault_path=rel_path1
    )

    assert abs_path1 == abs_path2
    assert rel_path1 == rel_path2
    assert "Updated version" in abs_path2.read_text(encoding="utf-8")
    assert "First version" not in abs_path2.read_text(encoding="utf-8")


def test_excluded_folders_prevention(tmp_path):
    # Attempt to write directly into .obsidian
    obsidian_note = MarkdownNote(
        title="Secret Note",
        source="notes",
        date="2026-09-22",
        body="Should fail",
    )
    with pytest.raises(ExcludedFolderError):
        write_note_to_vault(obsidian_note, vault_path=tmp_path, folder=".obsidian")

    with pytest.raises(ExcludedFolderError):
        write_note_to_vault(obsidian_note, vault_path=tmp_path, folder=".trash")

    with pytest.raises(ExcludedFolderError):
        write_note_to_vault(obsidian_note, vault_path=tmp_path, folder=".git")


def test_path_traversal_prevention(tmp_path):
    note = MarkdownNote(
        title="Escape Attempt",
        source="web",
        date="2026-09-22",
        body="Should fail",
    )
    with pytest.raises(VaultPathError):
        write_note_to_vault(note, vault_path=tmp_path, folder="../../escape")
