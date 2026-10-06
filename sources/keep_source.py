"""Google Keep source connector for Aurora External Data Ingestion Pipeline.

Ingests Google Keep notes from Google Takeout JSON exports (either an export directory
containing .json files or a direct path to a single .json note). Converts notes into
clean Obsidian Markdown notes under Ingested/Notes/ with YAML frontmatter, checklists,
labels/tags, color preservation, and local attachment embeds.
"""

from __future__ import annotations

import hashlib
import json
import logging
import mimetypes
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

FORBIDDEN_CHARS_PATTERN = re.compile(r'[?:*"<>|/\\]')


def parse_keep_timestamp(usec_val: Any) -> Optional[datetime]:
    """Parse Google Keep microsecond timestamp into a timezone-aware UTC datetime.

    Google Keep commonly exports timestamps like createdTimestampUsec as
    microseconds since the Unix epoch (e.g. 1598765432100000).
    """
    if usec_val is None:
        return None
    try:
        usec_int = int(usec_val)
        if usec_int <= 0:
            return None
        seconds = usec_int / 1_000_000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def extract_keep_labels(data: Dict[str, Any]) -> List[str]:
    """Extract and sanitize labels from Google Keep JSON export.

    Handles both list-of-dicts ([{"name": "tag"}]) and list-of-strings (["tag"]).
    Strips leading '#' symbols, trims whitespace, and formats as clean tag strings.
    """
    raw_labels = data.get("labels", [])
    labels: List[str] = []
    if not isinstance(raw_labels, list):
        return labels

    for item in raw_labels:
        val = ""
        if isinstance(item, dict):
            val = str(item.get("name") or "")
        elif isinstance(item, str):
            val = item

        if not val or not val.strip():
            continue

        clean = val.strip().lstrip("#").strip()
        # Normalize to safe tag identifier (lowercase, replace spaces and slashes with hyphens)
        clean_tag = clean.lower().replace(" ", "-").replace("/", "-")
        clean_tag = re.sub(r"[^a-zA-Z0-9_\-]", "", clean_tag)
        if clean_tag and clean_tag not in labels and clean_tag not in {"ingested", "google-keep"}:
            labels.append(clean_tag)

    return labels


def extract_keep_checklist(data: Dict[str, Any]) -> Tuple[List[str], Set[str]]:
    """Extract checklist items from Keep listContent array.

    Returns:
        (checklist_markdown_lines, set_of_normalized_item_texts)
    """
    raw_list = data.get("listContent", [])
    if not isinstance(raw_list, list) or not raw_list:
        return [], set()

    checklist_lines: List[str] = []
    item_texts: Set[str] = set()

    for item in raw_list:
        if not isinstance(item, dict):
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        checked = bool(item.get("isChecked", item.get("checked", False)))
        mark = "x" if checked else " "
        checklist_lines.append(f"- [{mark}] {text}")
        item_texts.add(text.lower())

    return checklist_lines, item_texts


def extract_keep_content(data: Dict[str, Any]) -> str:
    """Extract body content preserving checklists and text without duplication."""
    checklist_lines, checklist_item_texts = extract_keep_checklist(data)

    raw_text = str(data.get("textContent") or data.get("text") or "").strip()

    # If there is a checklist, remove any redundant plain-text duplicate of the list items
    if checklist_lines:
        distinct_text_lines: List[str] = []
        if raw_text:
            for line in raw_text.splitlines():
                clean_l = line.strip()
                if clean_l and clean_l.lower() not in checklist_item_texts:
                    distinct_text_lines.append(clean_l)

        parts: List[str] = []
        if distinct_text_lines:
            parts.append("\n\n".join(distinct_text_lines))
        parts.append("\n".join(checklist_lines))
        return "\n\n".join(parts)

    # Normal text-only note: preserve paragraphs meaningfully
    if raw_text:
        paragraphs = [p.strip() for p in raw_text.splitlines() if p.strip()]
        return "\n\n".join(paragraphs)

    return ""


IMMUTABLE_IDENTIFIER_FIELDS = (
    "exportId",
    "export_id",
    "uuid",
    "archiveId",
    "documentId",
    "clientId",
    "resourceId",
    "originId",
    "shareUrl",
    "url",
)


def derive_keep_stable_id(data: Dict[str, Any], json_path: Path) -> str:
    """Derive a stable source ID for a Google Keep note.

    Prioritizes:
    1. Explicit note ID ('id', 'noteId', 'serverId') if present in JSON:
       google-keep:<explicit_id>
    2. Strong immutable identifier in JSON (e.g. 'exportId', 'uuid')
       combined with createdTimestampUsec if present:
       google-keep:<sha256> (filename stem is NOT used)
    3. Last-resort fallback combining available immutable metadata
       (createdTimestampUsec) with the filename stem as a source discriminator:
       google-keep:<sha256>
    """
    explicit_id = data.get("id") or data.get("noteId") or data.get("serverId")
    if explicit_id:
        return f"google-keep:{explicit_id}"

    # Check for secondary immutable identifier / reference fields in the JSON
    secondary_id: Optional[str] = None
    for field in IMMUTABLE_IDENTIFIER_FIELDS:
        val = data.get(field)
        if val is not None and str(val).strip():
            secondary_id = str(val).strip()
            break

    created_usec = data.get("createdTimestampUsec")

    material_parts: List[str] = []
    if secondary_id:
        material_parts.append(f"id:{secondary_id}")
        if created_usec is not None:
            material_parts.append(f"created:{created_usec}")
    else:
        # No strong immutable identifier in JSON: use createdTimestampUsec if present,
        # with filename stem only as a last-resort source discriminator.
        if created_usec is not None:
            material_parts.append(f"created:{created_usec}")
        material_parts.append(f"file:{json_path.stem}")

    identity_material = "|".join(material_parts)
    digest = hashlib.sha256(identity_material.encode("utf-8")).hexdigest()
    return f"google-keep:{digest}"


class KeepSource(BaseSource):
    """Source connector for Google Keep notes exported via Google Takeout."""

    def __init__(self, export_path: Optional[Union[str, Path]] = None) -> None:
        super().__init__()
        self.export_path = Path(export_path) if export_path else None

    @property
    def source_type(self) -> str:
        return "google-keep"

    @property
    def display_name(self) -> str:
        return "Google Keep"

    def _resolve_export_path(self, **kwargs: Any) -> Path:
        """Resolve export path from argument, constructor, or environment variable."""
        raw_path = (
            kwargs.get("export_path")
            or kwargs.get("path")
            or kwargs.get("file")
            or self.export_path
            or os.environ.get("GOOGLE_KEEP_EXPORT_PATH")
            or os.environ.get("KEEP_EXPORT_PATH")
        )

        if not raw_path:
            raise SourceError(
                "Google Keep export path is missing. Configure GOOGLE_KEEP_EXPORT_PATH "
                "or pass --path <path_to_takeout_keep>."
            )

        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise SourceError(f"Google Keep export path does not exist: {path}")

        return path

    def _discover_json_files(self, target_path: Path) -> Tuple[List[Path], bool]:
        """Discover Keep JSON files to process.

        Returns:
            (list_of_json_paths, is_single_file_mode)
        """
        if target_path.is_file():
            if target_path.suffix.lower() != ".json":
                raise SourceError(
                    f"Google Keep export file must have a .json extension: {target_path}"
                )
            return [target_path], True

        if target_path.is_dir():
            # Discover .json files deterministically (case-insensitive filename order)
            json_files = sorted(
                [f for f in target_path.iterdir() if f.is_file() and f.suffix.lower() == ".json"],
                key=lambda p: p.name.lower(),
            )
            return json_files, False

        raise SourceError(
            f"Google Keep export path is neither a file nor a directory: {target_path}"
        )

    def _process_attachments(
        self,
        data: Dict[str, Any],
        json_path: Path,
        seen_filenames: Set[str],
    ) -> Tuple[List[Attachment], List[str]]:
        """Extract attachments referenced in Takeout JSON and residing in the export directory."""
        raw_attachments = data.get("attachments", [])
        if not isinstance(raw_attachments, list) or not raw_attachments:
            return [], []

        attachments: List[Attachment] = []
        embed_tags: List[str] = []

        for item in raw_attachments:
            if not isinstance(item, dict):
                continue

            file_ref = (
                item.get("filePath")
                or item.get("name")
                or item.get("path")
                or item.get("originalFileName")
                or ""
            ).strip()

            if not file_ref:
                continue

            # Candidate paths on disk
            candidates = [
                json_path.parent / file_ref,
                json_path.parent / Path(file_ref).name,
            ]
            if self.export_path and self.export_path.is_dir():
                candidates.append(self.export_path / file_ref)
                candidates.append(self.export_path / Path(file_ref).name)

            matched_file: Optional[Path] = None
            for cand in candidates:
                if cand.is_file():
                    matched_file = cand
                    break

            if not matched_file:
                logger.warning(
                    f"Google Keep attachment file '{file_ref}' referenced in '{json_path.name}' "
                    f"was not found on disk. Skipping attachment."
                )
                continue

            try:
                content = matched_file.read_bytes()
                mime_type = (
                    item.get("mimetype")
                    or item.get("type")
                    or mimetypes.guess_type(matched_file.name)[0]
                    or "application/octet-stream"
                )

                # Derive clean unique filename
                raw_name = FORBIDDEN_CHARS_PATTERN.sub("_", matched_file.name).strip("._ ")
                if not raw_name:
                    raw_name = f"keep_attachment_{len(attachments) + 1}.bin"

                suffix = Path(raw_name).suffix
                if not suffix:
                    guess_ext = mimetypes.guess_extension(mime_type) or ".bin"
                    raw_name = f"{raw_name}{guess_ext}"

                stem = Path(raw_name).stem
                suffix = Path(raw_name).suffix
                candidate_name = raw_name
                counter = 2
                while candidate_name in seen_filenames:
                    candidate_name = f"{stem}_{counter}{suffix}"
                    counter += 1

                seen_filenames.add(candidate_name)
                attachment = Attachment(
                    filename=candidate_name,
                    content=content,
                    mime_type=mime_type,
                )
                attachments.append(attachment)
                embed_tags.append(f"![[{candidate_name}]]")

            except Exception as e:
                logger.warning(
                    f"Failed to read Google Keep attachment '{matched_file}': {e}"
                )

        return attachments, embed_tags

    def _parse_single_note(
        self,
        json_path: Path,
        seen_attachment_names: Set[str],
    ) -> SourceItem:
        """Parse a single Google Keep JSON file into a SourceItem."""
        try:
            raw_text = json_path.read_text(encoding="utf-8")
            data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise SourceError(f"Malformed Google Keep JSON file '{json_path.name}': {e}") from e
        except Exception as e:
            raise SourceError(f"Failed to read Google Keep file '{json_path.name}': {e}") from e

        if not isinstance(data, dict):
            raise SourceError(
                f"Invalid Google Keep JSON structure in '{json_path.name}': expected dict root."
            )

        # 1. Title
        raw_title = str(data.get("title") or "").strip()
        title = raw_title if raw_title else "Untitled Keep Note"

        # 2. Body & Checklists
        content_body = extract_keep_content(data)

        # 3. Attachments
        attachments, embed_tags = self._process_attachments(
            data, json_path, seen_attachment_names
        )

        # 4. Timestamps
        created_dt = parse_keep_timestamp(data.get("createdTimestampUsec"))
        edited_dt = parse_keep_timestamp(data.get("userEditedTimestampUsec"))

        note_date: str
        if created_dt:
            note_date = created_dt.strftime("%Y-%m-%d")
        elif edited_dt:
            note_date = edited_dt.strftime("%Y-%m-%d")
        else:
            note_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # 5. Archived & Trashed state
        is_archived = bool(data.get("isArchived", False))
        is_trashed = bool(data.get("isTrashed", False))

        if is_trashed:
            status = "trashed"
        elif is_archived:
            status = "archived"
        else:
            status = "unread"

        # 6. Color
        raw_color = str(data.get("color") or "").strip().upper()
        color: Optional[str] = raw_color if raw_color and raw_color != "DEFAULT" else None

        # 7. Labels & Tags
        tags: List[str] = ["ingested", "google-keep"]
        labels = extract_keep_labels(data)
        for lbl in labels:
            if lbl not in tags:
                tags.append(lbl)

        if color:
            color_tag = f"keep-{color.lower()}"
            if color_tag not in tags:
                tags.append(color_tag)

        # 8. Extra metadata
        extra_metadata: Dict[str, Any] = {
            "archived": is_archived,
            "trashed": is_trashed,
        }
        if created_dt:
            extra_metadata["created_at"] = created_dt.isoformat()
        if edited_dt:
            extra_metadata["updated_at"] = edited_dt.isoformat()
        if color:
            extra_metadata["color"] = color
        if labels:
            extra_metadata["labels"] = labels

        # 9. Assemble full Markdown body with header & attribution
        body_lines: List[str] = [
            f"# {title}",
            "",
            "> **Source**: Google Keep",
        ]

        meta_parts: List[str] = []
        if created_dt:
            meta_parts.append(f"**Created**: {created_dt.strftime('%Y-%m-%d')}")
        if edited_dt:
            meta_parts.append(f"**Updated**: {edited_dt.strftime('%Y-%m-%d')}")
        if meta_parts:
            body_lines.append(f"> {' · '.join(meta_parts)}")

        body_lines.append("")
        if content_body:
            body_lines.append(content_body)
            body_lines.append("")

        if embed_tags:
            body_lines.extend(embed_tags)
            body_lines.append("")

        full_content = "\n".join(body_lines).strip() + "\n"

        # Summary (first 200 chars of non-structural content)
        summary: Optional[str] = None
        for line in full_content.splitlines():
            cl = line.strip()
            if cl and not cl.startswith(("#", ">", "!", "---")):
                summary = cl[:200]
                break

        # 10. Source identity
        source_id = derive_keep_stable_id(data, json_path)

        return SourceItem(
            source_type="google-keep",
            source_id=source_id,
            title=title,
            content=full_content,
            date=note_date,
            tags=tags,
            status=status,
            summary=summary,
            attachments=attachments,
            extra_metadata=extra_metadata,
        )

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Discover and parse Keep notes from configured path."""
        target_path = self._resolve_export_path(**kwargs)
        json_files, is_single_file = self._discover_json_files(target_path)

        items: List[SourceItem] = []
        seen_attachment_names: Set[str] = set()

        for json_path in json_files:
            try:
                item = self._parse_single_note(json_path, seen_attachment_names)
                items.append(item)
            except SourceError as e:
                if is_single_file:
                    raise
                logger.warning(f"Error parsing Keep note '{json_path.name}': {e}")
            except Exception as e:
                if is_single_file:
                    raise SourceError(f"Failed to process '{json_path.name}': {e}") from e
                logger.warning(
                    f"Unexpected error processing Keep note '{json_path.name}': {e}",
                    exc_info=True,
                )

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a Google Keep SourceItem into a canonical MarkdownNote."""
        attachment_filenames = [a.filename for a in item.attachments]

        return MarkdownNote(
            title=item.title,
            source="google-keep",
            date=item.date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            body=item.content,
            tags=list(item.tags) if item.tags else ["ingested", "google-keep"],
            source_url=item.source_url,
            author=item.author,
            status=item.status,
            summary=item.summary,
            attachments=attachment_filenames,
            extra_metadata=dict(item.extra_metadata),
            folder="Ingested/Notes",
        )


# Register KeepSource with the global registry
SourceRegistry.register("google-keep", KeepSource)
