"""Telegram source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Telegram Bot API (https://api.telegram.org/bot<TOKEN>/) using
official Bot token authentication (TELEGRAM_BOT_TOKEN or --token). Ingests messages,
edited messages, channel posts, and captions from explicitly configured Telegram chats,
channels, or groups into the Aurora vault under Ingested/Social/ as clean Obsidian-compatible
Markdown notes with YAML frontmatter, attribution blocks, and media metadata.

API and Authentication Contract:
- API Base URL: https://api.telegram.org
- Authentication: Endpoint URL routing via /bot<TOKEN>/<method>
- User/Account Automation: Strictly prohibited; uses official Bot API only.
- Environment Variables:
    TELEGRAM_BOT_TOKEN: Telegram Bot token from @BotFather
    TELEGRAM_CHAT_ID / TELEGRAM_CHAT_IDS: Explicitly configured chat IDs or usernames
- Supported Message Retrieval:
    Telegram Bot API does not provide arbitrary historical chat exploration (no getChatHistory).
    Ingestion operates over updates received via getUpdates with robust offset advancement,
    bounded batching, and duplicate protection.
- Bot Privacy & Permissions:
    In groups, bots with Group Privacy enabled only receive commands, direct mentions,
    or replies. For all group messages, Group Privacy must be disabled via @BotFather or the
    bot promoted to administrator. In channels, bots must be added as administrators to receive
    channel_post updates.
- Webhook Conflict:
    If getUpdates returns HTTP 409 indicating an active webhook, a descriptive SourceError is
    raised. Aurora will never automatically delete or alter external webhook configurations.
- Rate Limiting:
    HTTP 429 Too Many Requests / Flood Limits return parameters.retry_after or Retry-After header.
    Handled via bounded authoritative retry/backoff without busy-looping.
"""

from __future__ import annotations

import logging
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import requests
from markdownify import markdownify as md

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE_URL = "https://api.telegram.org"
DEFAULT_USER_AGENT = "aurora-ingestion:telegram:v1.0.0"
MAX_BATCH_SIZE = 100
DEFAULT_PAGE_SIZE = 50
DEFAULT_MAX_RETRIES = 3

# Update types supported for message ingestion
SUPPORTED_UPDATE_TYPES = [
    "message",
    "edited_message",
    "channel_post",
    "edited_channel_post",
]

# ---------------------------------------------------------------------------
# HTML Detection & Normalization Patterns
# ---------------------------------------------------------------------------

_HTML_DECLARATION_PATTERN = re.compile(
    r"<!DOCTYPE\s+html|<!--.*?-->",
    re.IGNORECASE | re.DOTALL,
)

_HTML_CLOSING_TAG_PATTERN = re.compile(
    r"</(?:[a-zA-Z][a-zA-Z0-9]*)\s*>",
    re.IGNORECASE,
)

_HTML_VOID_TAGS_PATTERN = re.compile(
    r"<(?:br|hr|img|meta|link|input|source|track|wbr)\b[^>]*\/?>",
    re.IGNORECASE,
)

_HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<\s*(?:script|style)\b",
    re.IGNORECASE,
)

_HTML_OPENING_TAG_NAMES = (
    "p|div|span|h[1-6]|ul|ol|li|blockquote|article|section|header|footer|"
    "nav|main|table|tr|td|th|tbody|thead|tfoot|pre|code|html|body|head|"
    "title|figure|figcaption|details|summary|b|i|em|strong|a|kbd|samp|sub|sup|del|ins|mark|dl|dt|dd|font|center"
)

_HTML_OPENING_TAG_PATTERN = re.compile(
    rf"<({_HTML_OPENING_TAG_NAMES})\b(?:[^>]*>|>|\s*/>)",
    re.IGNORECASE,
)

_HTML_ATTR_TAG_PATTERN = re.compile(
    r"<[a-zA-Z][a-zA-Z0-9]*\s+[^>]*\b(?:id|class|href|src|style|rel|target|title|alt|type|data-[a-zA-Z0-9_-]+)\s*=",
    re.IGNORECASE,
)


def _scrub_secrets(text: str, secrets: List[Optional[str]]) -> str:
    """Scrub sensitive bot tokens, credentials, or URLs from strings or logs."""
    scrubbed = str(text)
    for s in secrets:
        if s and len(str(s).strip()) >= 5:
            val = str(s).strip()
            scrubbed = scrubbed.replace(val, "[REDACTED]")
    # Redact Telegram bot token in URLs: https://api.telegram.org/bot<TOKEN>/...
    scrubbed = re.sub(r"(api\.telegram\.org/bot)[^/\s'\",]+", r"\1[REDACTED]", scrubbed)
    scrubbed = re.sub(r"(bot[0-9]+:[a-zA-Z0-9_-]+)", "[REDACTED]", scrubbed)
    return scrubbed


def is_safe_http_url(url: Optional[str]) -> bool:
    """Validate that a URL has an http or https scheme and is safe to render as a Markdown link."""
    if not url or not str(url).strip():
        return False
    try:
        parsed = urlparse(str(url).strip())
        return parsed.scheme.lower() in ("http", "https")
    except Exception:
        return False


def is_html_content(text: str) -> bool:
    """Deterministically check if content contains HTML markup.

    Preserves ordinary math/text comparisons like 'a < b' and 'x > y' as non-HTML.
    """
    if not text or not isinstance(text, str):
        return False

    raw = text.strip()
    if not raw or "<" not in raw:
        return False

    if _HTML_SCRIPT_STYLE_PATTERN.search(raw):
        return True
    if _HTML_DECLARATION_PATTERN.search(raw):
        return True
    if _HTML_CLOSING_TAG_PATTERN.search(raw):
        return True
    if _HTML_VOID_TAGS_PATTERN.search(raw):
        return True
    if _HTML_OPENING_TAG_PATTERN.search(raw):
        return True
    if _HTML_ATTR_TAG_PATTERN.search(raw):
        return True

    return False


def normalize_telegram_content(content: str) -> str:
    """Normalize HTML/text content into clean Markdown while preserving code blocks and comparisons."""
    if not content or not isinstance(content, str):
        return ""

    raw = content.strip()
    if not raw:
        return ""

    if not is_html_content(raw):
        return raw

    code_blocks: List[str] = []

    def _mask_code(m: re.Match[str]) -> str:
        code_blocks.append(m.group(0))
        return f"AURORATELEGRAMCODE{len(code_blocks)-1}TOKEN"

    masked = re.sub(r"(`{3,}[\s\S]*?`{3,}|`[^`\n]+`)", _mask_code, raw)

    clean_html = re.sub(
        r"<\s*(?:script|style)\b[^>]*>.*?<\s*/\s*(?:script|style)\s*>",
        "",
        masked,
        flags=re.DOTALL | re.IGNORECASE,
    )
    clean_html = re.sub(
        r"<\s*(?:script|style)\b[^>]*\/?>",
        "",
        clean_html,
        flags=re.IGNORECASE,
    )

    clean_html = clean_html.strip()
    if not clean_html:
        for i, block in enumerate(code_blocks):
            clean_html = clean_html.replace(f"AURORATELEGRAMCODE{i}TOKEN", block)
        return clean_html

    converted = md(
        clean_html,
        heading_style="ATX",
        bullets="-",
    ).strip()

    converted = re.sub(r"<[a-zA-Z/][^>]*>", "", converted)

    for i, block in enumerate(code_blocks):
        converted = converted.replace(f"AURORATELEGRAMCODE{i}TOKEN", block)

    converted = re.sub(r"\n{3,}", "\n\n", converted).strip()
    return converted


# ---------------------------------------------------------------------------
# UTF-16 Entity Processing & Rendering
# ---------------------------------------------------------------------------

def build_utf16_to_codepoint_map(text: str) -> List[int]:
    """Map each UTF-16 code unit offset to the corresponding Python character index.

    Telegram MessageEntity offsets and lengths are computed using UTF-16 code units.
    Characters outside the Basic Multilingual Plane (BMP, ord > 0xFFFF, such as emojis)
    occupy 2 code units in UTF-16 (surrogate pair) but only 1 Python string character.
    This mapping ensures perfect slicing without offset drift or Unicode corruption.
    """
    utf16_to_cp: List[int] = []
    for cp_idx, char in enumerate(text):
        units = 2 if ord(char) > 0xFFFF else 1
        for _ in range(units):
            utf16_to_cp.append(cp_idx)
    utf16_to_cp.append(len(text))
    return utf16_to_cp


class _EntitySpan:
    def __init__(
        self,
        start_cp: int,
        end_cp: int,
        entity_type: str,
        url: Optional[str] = None,
        language: Optional[str] = None,
    ) -> None:
        self.start_cp = start_cp
        self.end_cp = end_cp
        self.entity_type = entity_type
        self.url = url
        self.language = language
        self.children: List[_EntitySpan] = []


def render_telegram_entities(
    text: Optional[str],
    entities: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Render Telegram text and MessageEntity structures into clean Markdown.

    Handles UTF-16 code unit offsets accurately, nesting, code fences, spoilers,
    mentions, and URL security verification.
    """
    if not text:
        return ""

    raw_text = str(text)
    if not raw_text.strip() or not entities:
        if is_html_content(raw_text):
            return normalize_telegram_content(raw_text)
        return raw_text

    utf16_map = build_utf16_to_codepoint_map(raw_text)
    max_unit = len(utf16_map) - 1

    spans: List[_EntitySpan] = []
    for ent in entities:
        if not isinstance(ent, dict):
            continue
        etype = str(ent.get("type") or "").strip().lower()
        offset = ent.get("offset")
        length = ent.get("length")

        if offset is None or length is None:
            continue

        try:
            u_start = max(0, int(offset))
            u_len = max(0, int(length))
        except (ValueError, TypeError):
            continue

        if u_len <= 0 or u_start >= max_unit:
            continue

        u_end = min(max_unit, u_start + u_len)
        cp_start = utf16_map[u_start]
        cp_end = utf16_map[u_end]

        if cp_start >= cp_end or cp_start >= len(raw_text):
            continue

        spans.append(
            _EntitySpan(
                start_cp=cp_start,
                end_cp=cp_end,
                entity_type=etype,
                url=ent.get("url"),
                language=ent.get("language"),
            )
        )

    if not spans:
        if is_html_content(raw_text):
            return normalize_telegram_content(raw_text)
        return raw_text

    # Sort spans: outermost first (start_cp asc, end_cp desc)
    spans.sort(key=lambda s: (s.start_cp, -s.end_cp))

    # Build hierarchical tree of spans
    def _build_tree(span_list: List[_EntitySpan]) -> List[_EntitySpan]:
        root_nodes: List[_EntitySpan] = []
        for span in span_list:
            placed = False
            for parent in root_nodes:
                if parent.start_cp <= span.start_cp and span.end_cp <= parent.end_cp:
                    parent.children.append(span)
                    placed = True
                    break
            if not placed:
                root_nodes.append(span)

        for node in root_nodes:
            if node.children:
                node.children = _build_tree(node.children)
        return root_nodes

    roots = _build_tree(spans)

    # Render spans recursively
    def _render_range(start_idx: int, end_idx: int, active_spans: List[_EntitySpan]) -> str:
        pieces: List[str] = []
        cursor = start_idx

        for sp in active_spans:
            if sp.start_cp > cursor:
                pieces.append(raw_text[cursor : sp.start_cp])

            inner_content = _render_range(sp.start_cp, sp.end_cp, sp.children)
            formatted = _format_span(sp, inner_content)
            pieces.append(formatted)
            cursor = sp.end_cp

        if cursor < end_idx:
            pieces.append(raw_text[cursor:end_idx])

        return "".join(pieces)

    def _format_span(sp: _EntitySpan, inner: str) -> str:
        etype = sp.entity_type
        if etype == "bold":
            return f"**{inner}**"
        elif etype == "italic":
            return f"*{inner}*"
        elif etype == "underline":
            return f"<u>{inner}</u>"
        elif etype == "strikethrough":
            return f"~~{inner}~~"
        elif etype == "code":
            # Avoid breaking existing backticks
            clean_code = inner.replace("`", "'")
            return f"`{clean_code}`"
        elif etype == "pre":
            lang = (sp.language or "").strip()
            return f"```{lang}\n{inner}\n```" if lang else f"```\n{inner}\n```"
        elif etype == "spoiler":
            return f"||{inner}||"
        elif etype == "text_link":
            target_url = str(sp.url or "").strip()
            if is_safe_http_url(target_url):
                return f"[{inner}]({target_url})"
            return f"{inner} (`{target_url}`)"
        elif etype == "url":
            target_url = inner.strip()
            if is_safe_http_url(target_url):
                return f"[{target_url}]({target_url})"
            return f"`{target_url}`"
        elif etype in ("mention", "hashtag", "cashtag", "bot_command", "custom_emoji"):
            return inner
        return inner

    rendered = _render_range(0, len(raw_text), roots)

    if is_html_content(rendered):
        rendered = normalize_telegram_content(rendered)

    return rendered.strip()


# ---------------------------------------------------------------------------
# Metadata Extraction & Timestamp Formatting
# ---------------------------------------------------------------------------

def format_telegram_timestamp(ts: Any) -> str:
    """Normalize Unix timestamp or ISO string into standard ISO 8601 string."""
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    try:
        if isinstance(ts, (int, float)):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return dt.isoformat()
        s = str(ts).strip()
        if s.isdigit():
            dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
            return dt.isoformat()
        dt = datetime.fromisoformat(s)
        return dt.isoformat()
    except Exception:
        return str(ts).strip()


def format_telegram_display_time(ts: Any) -> str:
    """Format Unix timestamp or ISO string for human display: YYYY-MM-DD HH:MM."""
    if not ts:
        return "unknown"
    try:
        if isinstance(ts, (int, float)) or (isinstance(ts, str) and ts.strip().isdigit()):
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M")
        s = str(ts).strip()
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        s = str(ts).strip()
        if len(s) >= 16 and "T" in s:
            return s[:16].replace("T", " ")
        return s[:10]


def resolve_telegram_author(msg_data: Dict[str, Any]) -> Tuple[str, str, str]:
    """Resolve author information from a Telegram message dict: (display_name, handle, user_id).

    Handles user senders, channel senders, sender_chat, and author_signature.
    """
    if not isinstance(msg_data, dict):
        return ("Telegram User", "unknown", "")

    from_user = msg_data.get("from")
    if isinstance(from_user, dict):
        uid = str(from_user.get("id") or "").strip()
        fname = (from_user.get("first_name") or "").strip()
        lname = (from_user.get("last_name") or "").strip()
        uname = (from_user.get("username") or "").strip()

        full_name = f"{fname} {lname}".strip() or fname
        display_name = full_name or (f"@{uname}" if uname else uid or "Telegram User")
        handle = f"@{uname}" if uname else (display_name or uid or "unknown")
        return (display_name, handle, uid)

    sender_chat = msg_data.get("sender_chat")
    if isinstance(sender_chat, dict):
        cid = str(sender_chat.get("id") or "").strip()
        title = (sender_chat.get("title") or "").strip()
        uname = (sender_chat.get("username") or "").strip()
        display_name = title or (f"@{uname}" if uname else f"Chat {cid}")
        handle = f"@{uname}" if uname else display_name
        return (display_name, handle, cid)

    # Channel posts
    author_sig = (msg_data.get("author_signature") or "").strip()
    chat = msg_data.get("chat") or {}
    chat_title = (chat.get("title") or "").strip()
    chat_uname = (chat.get("username") or "").strip()

    if author_sig:
        display_name = f"{chat_title} ({author_sig})" if chat_title else author_sig
        handle = f"@{chat_uname}" if chat_uname else display_name
        return (display_name, handle, str(chat.get("id") or ""))

    if chat_title:
        display_name = chat_title
        handle = f"@{chat_uname}" if chat_uname else chat_title
        return (display_name, handle, str(chat.get("id") or ""))

    return ("Telegram User", "unknown", "")


def extract_telegram_media_metadata(msg_data: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
    """Extract safe attachment and media metadata from Telegram message without binary downloads.

    Returns:
    (list_of_markdown_bullet_lines, primary_media_type_name)
    """
    if not isinstance(msg_data, dict):
        return ([], None)

    lines: List[str] = []
    primary_type: Optional[str] = None

    # 1. Photo (list of PhotoSize objects; choose highest resolution)
    photos = msg_data.get("photo")
    if isinstance(photos, list) and photos:
        primary_type = "photo"
        largest = photos[-1]
        if isinstance(largest, dict):
            fid = largest.get("file_id") or "unknown"
            w = largest.get("width", 0)
            h = largest.get("height", 0)
            sz = largest.get("file_size", 0)
            dim_str = f"{w}x{h}" if w and h else "photo"
            sz_str = f", {sz:,} bytes" if sz else ""
            lines.append(f"- **Photo**: `{dim_str}` (`telegram_file_id`: `{fid}`{sz_str})")

    # 2. Document
    doc = msg_data.get("document")
    if isinstance(doc, dict):
        primary_type = primary_type or "document"
        fname = doc.get("file_name") or "Document"
        mime = doc.get("mime_type") or "application/octet-stream"
        sz = doc.get("file_size", 0)
        fid = doc.get("file_id") or "unknown"
        sz_str = f", {sz:,} bytes" if sz else ""
        lines.append(f"- **Document**: `{fname}` (`{mime}`, `telegram_file_id`: `{fid}`{sz_str})")

    # 3. Video
    vid = msg_data.get("video")
    if isinstance(vid, dict):
        primary_type = primary_type or "video"
        fname = vid.get("file_name") or "Video"
        mime = vid.get("mime_type") or "video/mp4"
        dur = vid.get("duration", 0)
        sz = vid.get("file_size", 0)
        fid = vid.get("file_id") or "unknown"
        dur_str = f", duration: {dur}s" if dur else ""
        sz_str = f", {sz:,} bytes" if sz else ""
        lines.append(f"- **Video**: `{fname}` (`{mime}`{dur_str}, `telegram_file_id`: `{fid}`{sz_str})")

    # 4. Audio
    aud = msg_data.get("audio")
    if isinstance(aud, dict):
        primary_type = primary_type or "audio"
        title = aud.get("title") or aud.get("file_name") or "Audio"
        perf = aud.get("performer") or ""
        mime = aud.get("mime_type") or "audio/mpeg"
        dur = aud.get("duration", 0)
        sz = aud.get("file_size", 0)
        fid = aud.get("file_id") or "unknown"
        desc = f"{perf} — {title}" if perf else title
        dur_str = f", duration: {dur}s" if dur else ""
        sz_str = f", {sz:,} bytes" if sz else ""
        lines.append(f"- **Audio**: `{desc}` (`{mime}`{dur_str}, `telegram_file_id`: `{fid}`{sz_str})")

    # 5. Voice
    voice = msg_data.get("voice")
    if isinstance(voice, dict):
        primary_type = primary_type or "voice"
        dur = voice.get("duration", 0)
        mime = voice.get("mime_type") or "audio/ogg"
        sz = voice.get("file_size", 0)
        fid = voice.get("file_id") or "unknown"
        sz_str = f", {sz:,} bytes" if sz else ""
        lines.append(f"- **Voice Note**: `{dur}s` (`{mime}`, `telegram_file_id`: `{fid}`{sz_str})")

    # 6. Animation (GIF)
    anim = msg_data.get("animation")
    if isinstance(anim, dict):
        primary_type = primary_type or "animation"
        fname = anim.get("file_name") or "Animation"
        mime = anim.get("mime_type") or "video/mp4"
        sz = anim.get("file_size", 0)
        fid = anim.get("file_id") or "unknown"
        sz_str = f", {sz:,} bytes" if sz else ""
        lines.append(f"- **Animation**: `{fname}` (`{mime}`, `telegram_file_id`: `{fid}`{sz_str})")

    # 7. Sticker
    stk = msg_data.get("sticker")
    if isinstance(stk, dict):
        primary_type = primary_type or "sticker"
        emoji = stk.get("emoji") or ""
        set_name = stk.get("set_name") or ""
        fid = stk.get("file_id") or "unknown"
        stk_label = f"Sticker {emoji}".strip() + (f" ({set_name})" if set_name else "")
        lines.append(f"- **Sticker**: `{stk_label}` (`telegram_file_id`: `{fid}`)")

    # 8. Poll
    poll = msg_data.get("poll")
    if isinstance(poll, dict):
        primary_type = primary_type or "poll"
        q = poll.get("question") or "Poll"
        opts = poll.get("options") or []
        lines.append(f"- **Poll**: `{q}` ({len(opts)} options)")

    # 9. Location / Venue
    venue = msg_data.get("venue")
    if isinstance(venue, dict):
        primary_type = primary_type or "venue"
        v_title = venue.get("title") or "Venue"
        v_addr = venue.get("address") or ""
        lines.append(f"- **Venue**: `{v_title}` ({v_addr})")
    elif isinstance(msg_data.get("location"), dict):
        primary_type = primary_type or "location"
        loc = msg_data["location"]
        lat = loc.get("latitude")
        lon = loc.get("longitude")
        lines.append(f"- **Location**: `lat: {lat}, lon: {lon}`")

    # 10. Contact (Sanitized: NO phone numbers or private fields exposed)
    contact = msg_data.get("contact")
    if isinstance(contact, dict):
        primary_type = primary_type or "contact"
        c_fn = contact.get("first_name") or ""
        c_ln = contact.get("last_name") or ""
        c_name = f"{c_fn} {c_ln}".strip() or "Contact"
        lines.append(f"- **Shared Contact**: `{c_name}`")

    return (lines, primary_type)


def build_telegram_message_body(
    msg_data: Dict[str, Any],
    chat_title: str,
    chat_id: str,
    chat_username: str = "",
    permalink: Optional[str] = None,
) -> str:
    """Construct full Obsidian-compatible Markdown document body for a Telegram message."""
    author_disp, author_handle, author_id = resolve_telegram_author(msg_data)
    author_heading = f"@{author_disp}" if not author_disp.startswith("@") else author_disp
    author_attr = f"@{author_handle}" if not author_handle.startswith("@") else author_handle

    ts = msg_data.get("date") or 0
    date_iso = format_telegram_timestamp(ts)
    date_str = date_iso[:10] if len(date_iso) >= 10 else date_iso
    display_time = format_telegram_display_time(ts)

    msg_id = str(msg_data.get("message_id") or "").strip()
    chat_display = chat_title or (f"@{chat_username}" if chat_username else f"Chat {chat_id}")

    # 1. Resolve content (text or caption)
    raw_text = msg_data.get("text")
    entities = msg_data.get("entities")
    if raw_text is None:
        raw_text = msg_data.get("caption") or ""
        entities = msg_data.get("caption_entities")

    media_lines, media_type = extract_telegram_media_metadata(msg_data)

    if not raw_text.strip():
        # Service / system messages or media without text
        if msg_data.get("pinned_message"):
            pin_sub = msg_data["pinned_message"].get("text") or msg_data["pinned_message"].get("caption") or "message"
            raw_text = f"*[System: Pinned a message ({pin_sub[:40]})]*"
        elif msg_data.get("new_chat_title"):
            raw_text = f"*[System: Chat title changed to '{msg_data['new_chat_title']}']*"
        elif media_type:
            raw_text = f"*[{media_type.capitalize()} attachment]*"
        else:
            raw_text = "*No message content.*"

    normalized_text = render_telegram_entities(raw_text, entities)

    # Derive note title
    first_line = ""
    for line in normalized_text.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if first_line:
        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
        title = clean_title[:80].rstrip() or f"Telegram message {msg_id}"
    elif media_type:
        title = f"Telegram {media_type} {msg_id}"
    else:
        title = f"Telegram message {msg_id}"

    title = normalize_telegram_content(title)

    # Source reference in attribution
    if permalink and is_safe_http_url(permalink):
        source_ref = f"[Telegram — {chat_display}]({permalink})"
    else:
        source_ref = f"Telegram — {chat_display}"

    reply_to = msg_data.get("reply_to_message")
    reply_to_id = reply_to.get("message_id") if isinstance(reply_to, dict) else None

    attr_parts = [
        f"> **Source**: {source_ref}",
        f"> **Author**: {author_attr} · **Date**: {date_str}" + (f" · **Reply to**: #{reply_to_id}" if reply_to_id else ""),
    ]

    # Forward attribution if applicable
    if msg_data.get("forward_date"):
        fwd_author = ""
        if isinstance(msg_data.get("forward_from"), dict):
            ff = msg_data["forward_from"]
            fwd_author = f"{ff.get('first_name', '')} {ff.get('last_name', '')}".strip() or ff.get("username", "")
        elif isinstance(msg_data.get("forward_from_chat"), dict):
            fc = msg_data["forward_from_chat"]
            fwd_author = fc.get("title") or fc.get("username") or ""
        elif msg_data.get("forward_sender_name"):
            fwd_author = str(msg_data["forward_sender_name"]).strip()

        fwd_time = format_telegram_display_time(msg_data["forward_date"])
        fwd_label = f"**Forwarded from**: {fwd_author}" if fwd_author else "**Forwarded message**"
        attr_parts.append(f"> {fwd_label} · **Original Date**: {fwd_time}")

    sections: List[str] = [
        f"# {title}",
        "\n".join(attr_parts),
    ]

    # Reply preview section
    if isinstance(reply_to, dict):
        r_disp, r_handle, _ = resolve_telegram_author(reply_to)
        r_author = r_handle if r_handle and r_handle != "unknown" else r_disp
        r_author_heading = r_author if r_author.startswith("@") else f"@{r_author}"
        r_raw = reply_to.get("text") or reply_to.get("caption") or ""
        r_ents = reply_to.get("entities") or reply_to.get("caption_entities")
        r_norm = render_telegram_entities(r_raw, r_ents) if r_raw else "*[Attachment]*"
        quoted_reply = "\n".join(f"> {line}" for line in r_norm.splitlines()) or "> *No content*"
        sections.append(f"## Replying to\n\n> **{r_author_heading}** (Message #{reply_to_id}):\n{quoted_reply}")

    # Main Message section
    sections.append(f"## Message\n\n### {author_heading} — {display_time}\n\n{normalized_text}")

    # Attachments section
    if media_lines:
        sections.append("### Attachments\n\n" + "\n".join(media_lines))

    return "\n\n".join(sections).strip() + "\n"


# ---------------------------------------------------------------------------
# Error Mapping & Rate Limit Extraction
# ---------------------------------------------------------------------------

def _extract_retry_after(
    response: Optional[requests.Response] = None,
    json_body: Optional[Union[Dict[str, Any], List[Any]]] = None,
) -> float:
    """Extract authoritative rate limit retry delay in seconds from Telegram JSON body or response headers.

    Precedence:
    1. json_body['parameters']['retry_after'] (official Telegram API contract)
    2. Description regex 'retry after (\\d+)'
    3. Retry-After HTTP response header
    Defaults to 1.0s if neither is available or parsable.
    """
    delay: Optional[float] = None
    if isinstance(json_body, dict):
        params = json_body.get("parameters")
        if isinstance(params, dict) and params.get("retry_after") is not None:
            try:
                delay = float(params["retry_after"])
            except (ValueError, TypeError):
                pass
        if delay is None:
            desc = str(json_body.get("description") or "")
            m = re.search(r"retry after\s+([0-9]+(?:\.[0-9]+)?)", desc, re.IGNORECASE)
            if m:
                try:
                    delay = float(m.group(1))
                except (ValueError, TypeError):
                    pass

    if delay is None and response is not None:
        hdr = response.headers.get("Retry-After")
        if hdr is not None:
            try:
                delay = float(hdr)
            except (ValueError, TypeError):
                pass
        if delay is None:
            try:
                b = response.json()
                if isinstance(b, dict):
                    params = b.get("parameters")
                    if isinstance(params, dict) and params.get("retry_after") is not None:
                        delay = float(params["retry_after"])
            except Exception:
                pass

    if delay is None:
        delay = 1.0

    return max(0.0, delay)


def map_telegram_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    json_body: Optional[Union[Dict[str, Any], List[Any]]] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map Telegram HTTP and API errors to descriptive SourceError without leaking credentials."""
    if response is None and json_body is None and isinstance(exc, SourceError):
        return exc

    sec_list = secrets or []
    status_code = getattr(response, "status_code", None)
    error_code: Optional[int] = None
    description: str = ""

    if json_body and isinstance(json_body, dict):
        error_code = json_body.get("error_code")
        description = str(json_body.get("description") or "").strip()
    elif response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                error_code = body.get("error_code")
                description = str(body.get("description") or "").strip()
        except Exception:
            pass

    # 429 Too Many Requests / Flood limits
    if status_code == 429 or error_code == 429 or "too many requests" in description.lower():
        retry_delay = _extract_retry_after(response=response, json_body=json_body)
        delay_str = f"{retry_delay:.2f}s" if retry_delay < 10 else f"{int(retry_delay)}s"
        return SourceError(f"Telegram API rate limit exceeded (HTTP 429). Retry after {delay_str}.")

    # 409 Conflict (Webhook active)
    if status_code == 409 or error_code == 409 or ("webhook" in description.lower() and "conflict" in description.lower()):
        return SourceError(
            "Telegram webhook conflict (HTTP 409): getUpdates cannot be used while a webhook is active for this bot. "
            "To use polling ingestion, delete or disable the active webhook via Telegram's deleteWebhook endpoint. "
            "Aurora will not automatically modify external bot webhook configurations."
        )

    # 401 Unauthorized
    if status_code == 401 or error_code == 401 or "unauthorized" in description.lower():
        return SourceError(
            "Telegram authentication failed (HTTP 401): Invalid or missing bot token. "
            "Ensure TELEGRAM_BOT_TOKEN is a valid Telegram bot token from @BotFather."
        )

    # 403 Forbidden
    if status_code == 403 or error_code == 403 or "forbidden" in description.lower():
        detail = f" ({description})" if description else ""
        return SourceError(
            f"Telegram permission denied (HTTP 403){detail}: Bot was blocked by the user or lacks permissions in the chat. "
            "Ensure the bot is added to the chat and has permission to read messages."
        )

    # 400 Bad Request
    if status_code == 400 or error_code == 400:
        if "chat not found" in description.lower():
            return SourceError("Telegram chat not found (HTTP 400): The specified chat ID or username could not be found.")
        detail = f": {description}" if description else "."
        return SourceError(f"Telegram API bad request (HTTP 400){detail}")

    # 404 Not Found
    if status_code == 404 or error_code == 404:
        detail = f": {description}" if description else "."
        return SourceError(f"Telegram API method or resource not found (HTTP 404){detail}")

    # 5xx Server Error
    if (status_code and status_code >= 500) or (error_code and error_code >= 500):
        return SourceError(
            f"Telegram server error (HTTP {status_code or error_code}): Telegram API is temporarily unavailable."
        )

    exc_str = str(exc)
    if (
        isinstance(exc, requests.exceptions.Timeout)
        or "timeout" in exc_str.lower()
        or "timed out" in exc_str.lower()
    ):
        return SourceError("Telegram request timed out.")
    if (
        isinstance(exc, requests.exceptions.ConnectionError)
        or "connection" in exc_str.lower()
        or "connect" in exc_str.lower()
    ):
        return SourceError("Network error connecting to Telegram API.")

    scrubbed = _scrub_secrets(exc_str, sec_list)
    return SourceError(f"Telegram API request failed: {scrubbed}")


# ---------------------------------------------------------------------------
# Telegram Connector Class
# ---------------------------------------------------------------------------

class TelegramSource(BaseSource):
    """Telegram ingestion connector using official Telegram Bot API."""

    source_type = "telegram"

    def __init__(
        self,
        token: Optional[str] = None,
        chat: Optional[Union[str, List[str]]] = None,
        chats: Optional[str] = None,
        limit: Optional[int] = None,
        max_messages: Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRIES,
        offset: Optional[int] = None,
        session: Optional[requests.Session] = None,
        api_base_url: str = TELEGRAM_API_BASE_URL,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ):
        self.token = self._resolve_token(token)
        self.configured_chats = self._parse_inputs(chat or chats, ["TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_IDS"])
        self.default_limit = limit or max_messages or DEFAULT_PAGE_SIZE
        self.max_retries = max(0, int(max_retries))
        self.offset = offset
        self.last_consumed_update_id: Optional[int] = None
        self._pending_updates: List[Dict[str, Any]] = []
        self.session = session or requests.Session()
        self.api_base_url = api_base_url.rstrip("/")
        self.sleep_fn = sleep_fn or time.sleep

        self._secrets: List[Optional[str]] = [self.token] if self.token else []

    @property
    def pending_updates(self) -> List[Dict[str, Any]]:
        """Return a copy of unconsumed updates currently held in memory."""
        return list(self._pending_updates)

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Telegram"

    @staticmethod
    def _resolve_token(token: Optional[str] = None) -> Optional[str]:
        """Resolve Telegram Bot token from parameter or TELEGRAM_BOT_TOKEN environment variable."""
        if token and str(token).strip():
            raw = str(token).strip()
        else:
            env_val = os.environ.get("TELEGRAM_BOT_TOKEN")
            raw = env_val.strip() if env_val else ""

        if not raw:
            return None

        # Clean any accidental 'bot' URL prefix
        if raw.lower().startswith("bot"):
            raw = raw[3:].strip()

        return raw

    @staticmethod
    def _parse_inputs(raw: Optional[Union[List[str], str]], env_keys: List[str]) -> List[str]:
        """Normalize comma-separated strings or string lists into cleaned chat identifiers."""
        if not raw:
            for k in env_keys:
                env_val = os.environ.get(k)
                if env_val:
                    raw = env_val
                    break
        if not raw:
            return []

        if isinstance(raw, str):
            items = [item.strip() for item in raw.split(",") if item.strip()]
        else:
            items = []
            for item in raw:
                if isinstance(item, str) and "," in item:
                    items.extend([sub.strip() for sub in item.split(",") if sub.strip()])
                elif item and str(item).strip():
                    items.append(str(item).strip())

        return [i for i in items if i]

    def _api_call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Perform an HTTP request against the Telegram Bot API with bounded 429 retry and error mapping."""
        if not self.token:
            raise SourceError(
                "Telegram bot token is required. Set TELEGRAM_BOT_TOKEN environment variable or pass --token."
            )

        clean_endpoint = endpoint.lstrip("/")
        url = f"{self.api_base_url}/bot{self.token}/{clean_endpoint}"
        headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }

        attempt = 0
        while True:
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, headers=headers, params=params, timeout=30)
                else:
                    resp = self.session.post(url, headers=headers, params=params, json=json_data, timeout=30)
            except Exception as e:
                raise map_telegram_error(e, secrets=self._secrets)

            try:
                data = resp.json()
            except Exception:
                data = None

            if resp.status_code == 429:
                if attempt < self.max_retries:
                    attempt += 1
                    delay = _extract_retry_after(response=resp, json_body=data)
                    logger.warning(
                        "Telegram API rate limit hit (HTTP 429). Retrying attempt %d/%d after %.2fs...",
                        attempt,
                        self.max_retries,
                        delay,
                    )
                    self.sleep_fn(delay)
                    continue
                else:
                    raise map_telegram_error(
                        SourceError(f"Telegram API rate limit exceeded (HTTP 429) after {self.max_retries} retries"),
                        response=resp,
                        json_body=data,
                        secrets=self._secrets,
                    )

            if data is None:
                raise map_telegram_error(
                    SourceError(f"Telegram API returned non-JSON response (HTTP {resp.status_code})"),
                    response=resp,
                    secrets=self._secrets,
                )

            if resp.status_code != 200 or not data.get("ok"):
                raise map_telegram_error(
                    SourceError(f"Telegram API error (HTTP {resp.status_code})"),
                    response=resp,
                    json_body=data,
                    secrets=self._secrets,
                )

            return data

    def _matches_chat(self, chat_data: Dict[str, Any], configured_chats: List[str]) -> bool:
        """Check if message chat matches any of the configured chat IDs, usernames, or titles."""
        if not configured_chats:
            return False

        c_id = str(chat_data.get("id") or "").strip()
        c_uname = str(chat_data.get("username") or "").strip().lstrip("@").lower()
        c_title = str(chat_data.get("title") or "").strip().lower()

        for target in configured_chats:
            clean_target = str(target).strip()
            clean_target_lower = clean_target.lower()
            clean_target_uname = clean_target_lower.lstrip("@")

            # Numeric ID match (handles positive, negative, and supergroup formats)
            if c_id and clean_target == c_id:
                return True

            # Username match
            if c_uname and clean_target_uname == c_uname:
                return True

            # Title match
            if c_title and clean_target_lower == c_title:
                return True

        return False

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch messages and channel posts from configured Telegram chats via getUpdates."""
        # 1. Resolve token override
        token_override = kwargs.get("token")
        if token_override:
            self.token = self._resolve_token(str(token_override))
            self._secrets = [self.token] if self.token else []

        if not self.token:
            raise SourceError(
                "Telegram bot token is required. Set TELEGRAM_BOT_TOKEN environment variable or pass --token."
            )

        # 2. Resolve configured chats
        chat_arg = kwargs.get("chat") or kwargs.get("chats")
        if chat_arg:
            target_chats = self._parse_inputs(chat_arg, ["TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_IDS"])
        else:
            target_chats = list(self.configured_chats)

        if not target_chats:
            raise SourceError(
                "No Telegram chat specified. Provide --chat/--chats or set TELEGRAM_CHAT_ID "
                "(Telegram chat IDs can be positive or negative, e.g. -1001234567890, or usernames like @my_channel)."
            )

        limit = kwargs.get("limit") or kwargs.get("max_messages") or self.default_limit
        max_retries = kwargs.get("max_retries", self.max_retries)
        self.max_retries = max(0, int(max_retries))

        current_offset = kwargs.get("offset")
        if current_offset is None:
            current_offset = self.offset

        # Filter out pending updates that precede an explicitly requested offset
        if current_offset is not None and self._pending_updates:
            self._pending_updates = [
                u
                for u in self._pending_updates
                if isinstance(u, dict) and u.get("update_id") is not None and int(u["update_id"]) >= current_offset
            ]

        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()
        seen_update_ids: Set[int] = set()
        last_consumed_update_id: Optional[int] = self.last_consumed_update_id

        # 3. Bounded getUpdates pagination loop
        while len(items) < limit:
            # Drain from in-memory pending updates first if available
            if self._pending_updates:
                updates = self._pending_updates
                self._pending_updates = []
            else:
                remaining_needed = limit - len(items)
                batch_limit = max(1, min(MAX_BATCH_SIZE, remaining_needed))
                params: Dict[str, Any] = {
                    "limit": batch_limit,
                    "timeout": 0,
                    "allowed_updates": SUPPORTED_UPDATE_TYPES,
                }
                if current_offset is not None:
                    params["offset"] = current_offset

                resp_data = self._api_call("GET", "getUpdates", params=params)
                updates = resp_data.get("result", [])

                if not isinstance(updates, list) or not updates:
                    break

            for idx, update in enumerate(updates):
                if not isinstance(update, dict):
                    continue

                u_id = update.get("update_id")
                u_num: Optional[int] = None
                if u_id is not None:
                    try:
                        u_num = int(u_id)
                    except (ValueError, TypeError):
                        u_num = None

                if u_num is not None:
                    if u_num in seen_update_ids:
                        # Duplicate update replayed in this run; mark handled
                        last_consumed_update_id = u_num
                        continue
                    seen_update_ids.add(u_num)

                # Supported update payload extraction
                msg_data = (
                    update.get("message")
                    or update.get("edited_message")
                    or update.get("channel_post")
                    or update.get("edited_channel_post")
                )

                if not isinstance(msg_data, dict):
                    # Unsupported update type (e.g. inline_query, callback_query, poll)
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

                chat = msg_data.get("chat")
                if not isinstance(chat, dict):
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

                # Filter by configured chats
                if not self._matches_chat(chat, target_chats):
                    # Deliberately ignored: non-configured chat
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

                chat_id = str(chat.get("id") or "").strip()
                chat_title = str(chat.get("title") or "").strip()
                chat_username = str(chat.get("username") or "").strip().lstrip("@")
                chat_type = str(chat.get("type") or "chat").strip()

                msg_id = str(msg_data.get("message_id") or "").strip()
                if not msg_id or not chat_id:
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

                source_id = f"telegram:message:{chat_id}:{msg_id}"
                if source_id in seen_source_ids:
                    # In-run duplicate message (e.g. replayed update or edit)
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

                try:
                    # Construct stable public URL if chat has a public username
                    permalink = f"https://t.me/{chat_username}/{msg_id}" if chat_username else None

                    body = build_telegram_message_body(
                        msg_data,
                        chat_title=chat_title,
                        chat_id=chat_id,
                        chat_username=chat_username,
                        permalink=permalink,
                    )

                    # Extract title and date
                    raw_text = msg_data.get("text") or msg_data.get("caption") or ""
                    entities = msg_data.get("entities") or msg_data.get("caption_entities")
                    norm_text = render_telegram_entities(raw_text, entities)

                    first_line = ""
                    for line in norm_text.splitlines():
                        if line.strip():
                            first_line = line.strip()
                            break
                    if first_line:
                        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
                        title = clean_title[:80].rstrip() or f"Telegram message {msg_id}"
                    else:
                        title = f"Telegram message {msg_id}"

                    title = normalize_telegram_content(title)

                    date_iso = format_telegram_timestamp(msg_data.get("date"))
                    _, author_handle, author_id = resolve_telegram_author(msg_data)

                    reply_to = msg_data.get("reply_to_message")
                    reply_id = reply_to.get("message_id") if isinstance(reply_to, dict) else None

                    is_edited = "edited_message" in update or "edited_channel_post" in update

                    extra_meta: Dict[str, Any] = {
                        "chat_id": chat_id,
                        "chat_title": chat_title or chat_username or f"Chat {chat_id}",
                        "chat_type": chat_type,
                        "telegram_message_id": int(msg_id) if msg_id.isdigit() else msg_id,
                        "author": author_handle,
                        "author_id": author_id,
                    }
                    if reply_id:
                        extra_meta["reply_to_message_id"] = reply_id
                    if is_edited:
                        extra_meta["edited"] = True
                        if msg_data.get("edit_date"):
                            extra_meta["edit_date"] = format_telegram_timestamp(msg_data["edit_date"])

                    item = SourceItem(
                        source_id=source_id,
                        title=title,
                        source_type="telegram",
                        content=body,
                        author=author_handle,
                        date=date_iso,
                        source_url=permalink,
                        tags=["ingested", "telegram", "social"],
                        summary=norm_text[:200].strip() or None,
                        extra_metadata=extra_meta,
                    )

                    seen_source_ids.add(source_id)
                    items.append(item)
                    if u_num is not None:
                        last_consumed_update_id = u_num

                    # Check if limit reached
                    if len(items) >= limit:
                        remaining_in_batch = updates[idx + 1:]
                        if remaining_in_batch:
                            self._pending_updates = remaining_in_batch
                        break

                except SourceError:
                    raise
                except Exception as me:
                    logger.warning(
                        "Skipping malformed Telegram message %s in chat %s: %s",
                        msg_id,
                        chat_id,
                        _scrub_secrets(str(me), self._secrets),
                    )
                    if u_num is not None:
                        last_consumed_update_id = u_num
                    continue

            if last_consumed_update_id is not None:
                current_offset = last_consumed_update_id + 1
                self.offset = current_offset
                self.last_consumed_update_id = last_consumed_update_id
            elif not updates:
                break

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a SourceItem into frontmatter metadata and Markdown body targeted at Ingested/Social/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Social"
        return note


# Register Telegram connector in SourceRegistry
SourceRegistry.register("telegram", TelegramSource)
