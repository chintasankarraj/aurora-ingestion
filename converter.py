"""Markdown note converter and file writer for the Aurora Ingestion Pipeline.

Handles:
- YAML frontmatter generation (valid YAML, required & optional fields)
- Markdown body structure (H1 matching frontmatter, attribution blockquote, embeds)
- UTF-8 output with no BOM
- Filename sanitization (<YYYY-MM-DD>_<source>_<sanitized-title>.md, max 80 char title)
- Unique filename collision handling (_2, _3)
- In-place overwrite for updated notes
- Attachment saving to Attachments/Ingested/ and Obsidian embed generation
- Vault folder placement and excluded folder enforcement (.obsidian, .trash, .git)
"""

from __future__ import annotations

import os
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from config import DEFAULT_ATTACHMENT_FOLDER, DEFAULT_SOURCE_FOLDERS, EXCLUDED_FOLDERS, IngestionConfig
from exceptions import ConversionError, ExcludedFolderError, VaultPathError
from models import Attachment, MarkdownNote, format_iso_timestamp

# Characters disallowed in filenames (strip these)
FORBIDDEN_CHARS_PATTERN = re.compile(r'[?:*"<>|/\\]')


def sanitize_filename_title(title: str, max_length: int = 80) -> str:
    """Sanitize the title portion of a note filename.
    
    Rules:
    - Replace spaces and consecutive whitespace with '_'
    - Strip forbidden characters: ? : * " < > | / \\
    - Truncate title portion to max_length (default: 80 characters max)
    - Clean leading and trailing underscores
    - Fallback to 'Untitled' if empty
    """
    if not title:
        return "Untitled"

    # Strip forbidden characters
    cleaned = FORBIDDEN_CHARS_PATTERN.sub("", title)

    # Replace whitespace sequences and multiple underscores with a single underscore
    cleaned = re.sub(r"[\s_]+", "_", cleaned).strip("_")

    if not cleaned:
        return "Untitled"

    # Truncate to maximum characters
    if len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rstrip("_")

    return cleaned or "Untitled"


def extract_date_prefix(date_val: Union[datetime, date, str, None]) -> str:
    """Extract YYYY-MM-DD date prefix from date, datetime, or ISO string."""
    if date_val is None:
        return datetime.now().strftime("%Y-%m-%d")
    if isinstance(date_val, (datetime, date)):
        return date_val.strftime("%Y-%m-%d")

    # If string, try to match YYYY-MM-DD
    match = re.match(r"^(\d{4}-\d{2}-\d{2})", str(date_val).strip())
    if match:
        return match.group(1)

    return datetime.now().strftime("%Y-%m-%d")


def generate_note_filename(
    date_val: Union[datetime, date, str, None],
    source_type: str,
    title: str,
    max_title_length: int = 80,
) -> str:
    """Generate canonical filename: <YYYY-MM-DD>_<source>_<sanitized-title>.md"""
    date_str = extract_date_prefix(date_val)
    source_clean = re.sub(r"[^a-zA-Z0-9_-]", "", source_type.strip().lower()) or "source"
    sanitized_title = sanitize_filename_title(title, max_length=max_title_length)
    return f"{date_str}_{source_clean}_{sanitized_title}.md"


def resolve_unique_filepath(target_dir: Path, base_filename: str) -> Path:
    """Ensure filename is unique within target_dir by appending _2, _3, etc. if needed."""
    candidate = target_dir / base_filename
    if not candidate.exists():
        return candidate

    stem = candidate.stem
    suffix = candidate.suffix

    counter = 2
    while True:
        candidate = target_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def validate_vault_destination(target_path: Path, vault_path: Path) -> None:
    """Ensure target path is inside the vault and does not touch any excluded folders."""
    resolved_target = target_path.resolve()
    resolved_vault = vault_path.resolve()

    # Prevent directory traversal outside vault
    try:
        rel_path = resolved_target.relative_to(resolved_vault)
    except ValueError as e:
        raise VaultPathError(
            f"Target path '{target_path}' is outside the configured vault '{vault_path}'"
        ) from e

    # Check for excluded folders (.obsidian, .trash, .git, or any dot-folder)
    for part in rel_path.parts:
        if part in EXCLUDED_FOLDERS or (part.startswith(".") and part not in {".", ".."}):
            raise ExcludedFolderError(
                f"Writing to excluded folder '{part}' is strictly prohibited: {rel_path}"
            )


def format_yaml_frontmatter(metadata: Dict[str, Any]) -> str:
    """Construct a clean, valid YAML frontmatter block enclosed in --- delimiters."""
    yaml_str = yaml.safe_dump(
        metadata,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    ).strip()
    return f"---\n{yaml_str}\n---\n"


def build_attribution_block(note: MarkdownNote) -> str:
    """Build Obsidian Markdown source attribution block as a blockquote."""
    lines: List[str] = []

    # Format source link or name
    source_display = note.source.capitalize()
    if note.source_url:
        source_ref = f"[{source_display}]({note.source_url})"
    else:
        source_ref = source_display

    # Source line
    if note.source == "email":
        from_val = note.extra_metadata.get("from", note.author or "Unknown")
        to_val = note.extra_metadata.get("to", "Unknown")
        lines.append(f"> **From**: {from_val} · **To**: {to_val}")
        subj = note.extra_metadata.get("subject", note.title)
        date_str = extract_date_prefix(note.date)
        lines.append(f"> **Date**: {date_str} · **Subject**: {subj}")
    elif note.source == "pdf":
        orig_file = note.extra_metadata.get("original_filename", f"{note.title}.pdf")
        display_file = note.attachments[0] if note.attachments else orig_file
        page_info = f" · {note.extra_metadata['page_count']} pages" if "page_count" in note.extra_metadata else ""
        lines.append(f"> **Source**: [[{display_file}]]{page_info}")
        if note.author:
            lines.append(f"> **Author**: {note.author}")
    elif note.source == "youtube":
        channel = note.extra_metadata.get("channel", note.author or "")
        duration = note.extra_metadata.get("duration", "")
        extra_parts: List[str] = []
        if duration:
            extra_parts.append(f"**Duration**: {duration}")
        if channel:
            extra_parts.append(f"**Channel**: {channel}")
        lines.append(f"> **Source**: {source_ref}")
        if extra_parts:
            lines.append(f"> {' · '.join(extra_parts)}")
    elif note.source == "web":
        site_name = note.extra_metadata.get("site_name", "")
        site_ref = f"[{site_name}]({note.source_url})" if site_name and note.source_url else source_ref
        lines.append(f"> **Source**: {site_ref}")
        meta_parts: List[str] = []
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Published**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "rss":
        feed_title = note.extra_metadata.get("feed_title", "")
        feed_url = note.extra_metadata.get("feed_url", "")
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if feed_title:
            feed_ref = f"[{feed_title}]({feed_url})" if feed_url else feed_title
            meta_parts.append(f"**Feed**: {feed_ref}")
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Published**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "notion":
        page_url = note.source_url or ""
        source_ref = f"[Notion]({page_url})" if page_url else "Notion"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        last_edited = note.extra_metadata.get("last_edited_time")
        if last_edited:
            date_str = extract_date_prefix(last_edited)
            meta_parts.append(f"**Last Edited**: {date_str}")
        elif note.date:
            date_str = extract_date_prefix(note.date)
            if date_str:
                meta_parts.append(f"**Date**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "google-keep":
        source_url = note.source_url or ""
        source_ref = f"[Google Keep]({source_url})" if source_url else "Google Keep"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        created_at = note.extra_metadata.get("created_at")
        updated_at = note.extra_metadata.get("updated_at")
        if created_at:
            meta_parts.append(f"**Created**: {extract_date_prefix(created_at)}")
        if updated_at:
            meta_parts.append(f"**Updated**: {extract_date_prefix(updated_at)}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "readwise":
        readwise_url = note.extra_metadata.get("readwise_url") or ""
        source_ref = f"[Readwise]({readwise_url})" if readwise_url else "Readwise"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        if note.source_url:
            meta_parts.append(f"**URL**: [{note.source_url}]({note.source_url})")
        category = note.extra_metadata.get("category")
        if category:
            meta_parts.append(f"**Category**: {category.title()}")
        original_source = note.extra_metadata.get("original_source")
        if original_source:
            meta_parts.append(f"**Original Source**: {original_source}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "instapaper":
        source_url = note.source_url or ""
        source_ref = f"[Instapaper]({source_url})" if source_url else "Instapaper"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        if source_url:
            meta_parts.append(f"**URL**: [{source_url}]({source_url})")
        folder = note.extra_metadata.get("folder")
        if folder:
            meta_parts.append(f"**Folder**: {folder.title()}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "github":
        github_type = note.extra_metadata.get("github_type", "issue")
        source_url = note.source_url or ""
        if github_type == "gist":
            source_ref = f"[GitHub Gist]({source_url})" if source_url else "GitHub Gist"
            lines.append(f"> **Source**: {source_ref}")
            meta_parts = []
            if note.author:
                meta_parts.append(f"**Owner**: {note.author}")
            gist_id = note.extra_metadata.get("gist_id")
            if gist_id:
                meta_parts.append(f"**Gist ID**: {gist_id}")
            if source_url:
                meta_parts.append(f"**URL**: [{source_url}]({source_url})")
            date_str = extract_date_prefix(note.date)
            if date_str:
                meta_parts.append(f"**Date**: {date_str}")
            if meta_parts:
                lines.append(f"> {' · '.join(meta_parts)}")
        else:
            # Issue
            source_ref = f"[GitHub]({source_url})" if source_url else "GitHub"
            lines.append(f"> **Source**: {source_ref}")
            meta_parts = []
            repo = note.extra_metadata.get("repository")
            if repo:
                meta_parts.append(f"**Repository**: {repo}")
            issue_num = note.extra_metadata.get("issue_number")
            if issue_num:
                meta_parts.append(f"**Issue**: #{issue_num}")
            if note.author:
                meta_parts.append(f"**Author**: {note.author}")
            state = note.extra_metadata.get("state")
            if state:
                meta_parts.append(f"**State**: {state.title()}")
            if source_url:
                meta_parts.append(f"**URL**: [{source_url}]({source_url})")
            date_str = extract_date_prefix(note.date)
            if date_str:
                meta_parts.append(f"**Date**: {date_str}")
            if meta_parts:
                lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "reddit":
        subreddit = note.extra_metadata.get("subreddit", "")
        sub_str = f"r/{subreddit}" if subreddit else "Reddit"
        source_ref = f"[Reddit — {sub_str}]({note.source_url})" if note.source_url else f"Reddit — {sub_str}"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            author_disp = f"u/{note.author}" if note.author != "[deleted]" else "[deleted]"
            meta_parts.append(f"**Author**: {author_disp}")
        score = note.extra_metadata.get("score")
        if score is not None:
            meta_parts.append(f"**Score**: {score}")
        comment_count = note.extra_metadata.get("comment_count")
        if comment_count is not None:
            meta_parts.append(f"**Comments**: {comment_count}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Created**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "slack":
        channel = note.extra_metadata.get("channel", "")
        chan_disp = f"#{channel}" if channel and not channel.startswith("#") else (channel or "Slack")
        source_url = note.source_url or ""
        source_ref = f"[Slack — {chan_disp}]({source_url})" if source_url else f"Slack — {chan_disp}"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            author_disp = f"@{note.author}" if not note.author.startswith("@") else note.author
            meta_parts.append(f"**Author**: {author_disp}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        reply_count = note.extra_metadata.get("reply_count")
        if reply_count is not None and reply_count > 0:
            meta_parts.append(f"**Replies**: {reply_count}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "discord":
        guild = note.extra_metadata.get("guild", "")
        channel = note.extra_metadata.get("channel", "")
        chan_disp = f"#{channel}" if channel and not channel.startswith("#") else (channel or "Discord")
        server_channel = f"{guild} / {chan_disp}" if guild else chan_disp
        source_url = note.source_url or ""
        source_ref = f"[Discord — {server_channel}]({source_url})" if source_url else f"Discord — {server_channel}"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            author_disp = f"@{note.author}" if not note.author.startswith("@") else note.author
            meta_parts.append(f"**Author**: {author_disp}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        reply_count = note.extra_metadata.get("reply_count")
        if reply_count is not None and reply_count > 0:
            meta_parts.append(f"**Replies**: {reply_count}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "telegram":
        chat_title = note.extra_metadata.get("chat_title", "")
        chat_id = note.extra_metadata.get("chat_id", "")
        chat_disp = chat_title or (f"Chat {chat_id}" if chat_id else "Telegram")
        source_url = note.source_url or ""
        source_ref = f"[Telegram — {chat_disp}]({source_url})" if source_url else f"Telegram — {chat_disp}"
        lines.append(f"> **Source**: {source_ref}")
        meta_parts = []
        if note.author:
            author_disp = f"@{note.author}" if not note.author.startswith("@") else note.author
            meta_parts.append(f"**Author**: {author_disp}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        reply_to_id = note.extra_metadata.get("reply_to_message_id")
        if reply_to_id:
            meta_parts.append(f"**Reply to**: #{reply_to_id}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source == "voice":
        source_file = note.extra_metadata.get("source_file") or note.source_url or "audio file"
        file_disp = Path(source_file).name if ("/" in str(source_file) or "\\" in str(source_file)) else str(source_file)
        lines.append(f"> **Source**: Local audio — {file_disp}")
        meta_parts = []
        lang = note.language or note.extra_metadata.get("language")
        if lang:
            meta_parts.append(f"**Language**: {lang}")
        dur = note.extra_metadata.get("duration_seconds")
        if dur is not None:
            try:
                tot = max(0.0, float(dur))
                h = int(tot // 3600)
                m = int((tot % 3600) // 60)
                s = int(tot % 60)
                meta_parts.append(f"**Duration**: {h:02d}:{m:02d}:{s:02d}")
            except Exception:
                meta_parts.append(f"**Duration**: {dur}s")
        model = note.extra_metadata.get("model")
        if model:
            meta_parts.append(f"**Model**: {model}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    elif note.source in {"screenshots", "screenshot"}:
        source_file = note.extra_metadata.get("source_file") or note.source_url or "image"
        file_disp = Path(source_file).name if ("/" in str(source_file) or "\\" in str(source_file)) else str(source_file)
        lines.append(f"> **Source**: Local image — {file_disp}")
        meta_parts = []
        dims = note.extra_metadata.get("dimensions")
        if dims and isinstance(dims, dict):
            w = dims.get("width")
            h = dims.get("height")
            if w is not None and h is not None:
                meta_parts.append(f"**Dimensions**: {w}x{h}")
        fmt = note.extra_metadata.get("format")
        if fmt:
            meta_parts.append(f"**Format**: {fmt}")
        ocr_status = note.extra_metadata.get("ocr_status")
        if ocr_status:
            meta_parts.append(f"**OCR**: {ocr_status}")
        ocr_words = note.extra_metadata.get("ocr_word_count")
        if ocr_words is not None:
            meta_parts.append(f"**Words**: {ocr_words}")
        date_str = extract_date_prefix(note.date)
        if date_str:
            meta_parts.append(f"**Date**: {date_str}")
        if meta_parts:
            lines.append(f"> {' · '.join(meta_parts)}")
    else:
        # Generic attribution block
        meta_parts = [f"**Source**: {source_ref}"]
        if note.author:
            meta_parts.append(f"**Author**: {note.author}")
        lines.append(f"> {' · '.join(meta_parts)}")

    return "\n".join(lines)


def render_markdown_document(note: MarkdownNote) -> str:
    """Render the full UTF-8 Markdown document with frontmatter, H1 title, attribution, and body."""
    frontmatter_dict = note.to_frontmatter_dict()
    frontmatter_block = format_yaml_frontmatter(frontmatter_dict)

    body_content = (note.body or "").strip()

    # Check if body already has the top H1 matching note title
    h1_header = f"# {note.title}"
    has_h1 = body_content.startswith(h1_header)

    doc_parts: List[str] = [frontmatter_block]

    if not has_h1:
        doc_parts.append(f"{h1_header}\n")
        attribution = build_attribution_block(note)
        if attribution:
            doc_parts.append(f"{attribution}\n")
        if body_content:
            doc_parts.append(body_content)
    else:
        # Body already structured with H1
        doc_parts.append(body_content)

    return "\n".join(part.strip() for part in doc_parts if part.strip()) + "\n"


def resolve_collision_safe_attachment_name(
    target_dir: Path,
    base_name: str,
    incoming_bytes: bytes,
    overwrite_target: Optional[str] = None,
) -> Path:
    """Resolve a collision-safe destination path for an attachment.

    - If overwrite_target is specified (e.g. updating an existing note),
      returns target_dir / overwrite_target.
    - If base_name does not exist, returns target_dir / base_name.
    - If base_name exists with identical content, returns target_dir / base_name (reuses file).
    - If base_name exists with different content, searches for base_name_2, base_name_3, etc.,
      reusing any existing file that matches incoming_bytes or picking the first unused name.
    """
    if overwrite_target:
        return target_dir / overwrite_target

    candidate = target_dir / base_name
    if not candidate.exists():
        return candidate

    try:
        if candidate.read_bytes() == incoming_bytes:
            return candidate
    except Exception:
        pass

    stem = candidate.stem
    suffix = candidate.suffix

    counter = 2
    while True:
        candidate = target_dir / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        try:
            if candidate.read_bytes() == incoming_bytes:
                return candidate
        except Exception:
            pass
        counter += 1


def save_attachment(
    attachment: Attachment,
    vault_path: Union[str, Path],
    attachment_subfolder: str = DEFAULT_ATTACHMENT_FOLDER,
    overwrite_target: Optional[str] = None,
) -> str:
    """Save an attachment into the vault's attachment directory in a collision-safe manner.
    
    Returns the filename of the saved or reused attachment.
    """
    vault = Path(vault_path).resolve()
    target_dir = vault / attachment_subfolder
    target_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize attachment filename
    safe_name = FORBIDDEN_CHARS_PATTERN.sub("", Path(attachment.filename).name).strip()
    if not safe_name:
        safe_name = f"attachment_{int(datetime.now().timestamp())}"

    # Read incoming bytes
    if attachment.content is not None:
        incoming_bytes = attachment.content
    elif attachment.source_path is not None and Path(attachment.source_path).exists():
        incoming_bytes = Path(attachment.source_path).read_bytes()
    else:
        raise ConversionError(f"Attachment '{attachment.filename}' has no content or source_path.")

    dest_path = resolve_collision_safe_attachment_name(
        target_dir=target_dir,
        base_name=safe_name,
        incoming_bytes=incoming_bytes,
        overwrite_target=overwrite_target,
    )
    validate_vault_destination(dest_path, vault)

    # Write content
    dest_path.write_bytes(incoming_bytes)

    return dest_path.name


def write_note_to_vault(
    note: MarkdownNote,
    vault_path: Union[str, Path],
    folder: Optional[str] = None,
    existing_vault_path: Optional[str] = None,
    attachments: Optional[List[Attachment]] = None,
    config: Optional[IngestionConfig] = None,
) -> Tuple[Path, str, List[str]]:
    """Writes a MarkdownNote to the configured Aurora vault.
    
    Args:
        note: The MarkdownNote instance to write.
        vault_path: Root path of the Obsidian vault.
        folder: Optional relative folder inside the vault (defaults to source-mapped folder).
        existing_vault_path: If updating an existing note, relative path in vault to overwrite in-place.
        attachments: Optional list of media/document attachments to save.
        config: Optional configuration instance.

    Returns:
        Tuple of:
            - absolute Path of written file
            - relative path string within vault (e.g. 'Ingested/Email/2026-09-22_email_Notes.md')
            - list of saved attachment filenames
    """
    vault = Path(vault_path).resolve()

    # If updating an existing note, inspect previous note's attachments
    existing_attachments: List[str] = []
    if existing_vault_path:
        existing_file = (vault / existing_vault_path).resolve()
        if existing_file.exists():
            try:
                content = existing_file.read_text(encoding="utf-8")
                if content.startswith("---"):
                    parts = content.split("---", 2)
                    if len(parts) >= 3:
                        fm = yaml.safe_load(parts[1]) or {}
                        existing_attachments = [str(a) for a in fm.get("attachments", [])]
            except Exception:
                existing_attachments = []

    # Save any attachments first and link them
    saved_attachments: List[str] = []
    if attachments:
        att_folder = config.attachment_folder if config else DEFAULT_ATTACHMENT_FOLDER
        for i, att in enumerate(attachments):
            orig_raw_name = Path(att.filename).name

            # Determine overwrite target if updating an existing note
            overwrite_target: Optional[str] = None
            if existing_attachments:
                stem = Path(orig_raw_name).stem
                suffix = Path(orig_raw_name).suffix
                for ea in existing_attachments:
                    if ea == orig_raw_name or re.match(
                        rf"^{re.escape(stem)}(_\d+)?{re.escape(suffix)}$", ea
                    ):
                        overwrite_target = ea
                        break

            saved_name = save_attachment(
                att, vault, att_folder, overwrite_target=overwrite_target
            )
            saved_attachments.append(saved_name)

            # Update note.attachments list: replace original reference with saved name
            if i < len(note.attachments) and note.attachments[i] == orig_raw_name:
                note.attachments[i] = saved_name
            elif orig_raw_name in note.attachments:
                idx = note.attachments.index(orig_raw_name)
                note.attachments[idx] = saved_name
            elif saved_name not in note.attachments:
                note.attachments.append(saved_name)

            # If saved_name differs from orig_raw_name, update references in note body
            if saved_name != orig_raw_name:
                if note.body:
                    note.body = note.body.replace(f"![[{orig_raw_name}]]", f"![[{saved_name}]]")
                    note.body = note.body.replace(f"[[{orig_raw_name}]]", f"[[{saved_name}]]")

    # Determine target file path
    if existing_vault_path:
        # Overwrite in-place
        target_file = (vault / existing_vault_path).resolve()
    else:
        # Determine target folder
        if folder:
            subfolder = folder
        elif note.folder:
            subfolder = note.folder
        elif config:
            subfolder = config.get_folder_for_source(note.source)
        else:
            subfolder = DEFAULT_SOURCE_FOLDERS.get(note.source.lower(), "Inbox")

        target_dir = vault / subfolder
        target_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename and resolve collision
        filename = generate_note_filename(note.date, note.source, note.title)
        target_file = resolve_unique_filepath(target_dir, filename)

    # Validate destination security (not in .obsidian, .trash, .git)
    validate_vault_destination(target_file, vault)

    # Render document content
    content = render_markdown_document(note)

    # Ensure parent exists
    target_file.parent.mkdir(parents=True, exist_ok=True)

    # Write note as UTF-8 with no BOM
    target_file.write_text(content, encoding="utf-8")

    # Relative path within vault using forward slashes for cross-platform Obsidian standard
    rel_path = target_file.relative_to(vault).as_posix()

    return target_file, rel_path, saved_attachments
