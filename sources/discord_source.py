"""Discord source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Discord REST API v10 using official Bot token authentication
(DISCORD_BOT_TOKEN or --token). Ingests conversation messages and threaded replies from
explicitly configured guild channels into the Aurora vault under Ingested/Social/ as clean
Obsidian-compatible Markdown notes with YAML frontmatter, attribution blocks, and
chronologically ordered threads.

API and Authentication Contract:
- API Base URL: https://discord.com/api/v10
- Authentication: HTTP Bot token via Authorization: Bot <token>
- User/Self-Bot Tokens: Strictly prohibited and rejected per Discord Developer Policy.
- Environment Variables:
    DISCORD_BOT_TOKEN: Discord Bot token
    DISCORD_GUILD_ID / DISCORD_GUILDS: Optional default guild/server ID(s)
    DISCORD_CHANNEL_ID / DISCORD_CHANNELS: Optional default channel ID(s) or names
- Required Bot Permissions & Privileged Intents:
    Guild permissions: View Channel (0x400), Read Message History (0x10000)
    Privileged Intents: MESSAGE CONTENT INTENT enabled in Discord Developer Portal
    (Applications > [Your App] > Bot > Privileged Gateway Intents > Message Content Intent).
- Rate Limiting Notice:
    Discord applies global (50 req/sec), per-route, and resource-specific rate limits.
    Inspects X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset,
    X-RateLimit-Reset-After, X-RateLimit-Scope, and Retry-After headers / JSON retry_after.
- Supported Channels:
    Text channels (type 0: GUILD_TEXT, 5: GUILD_ANNOUNCEMENT, 15: GUILD_FORUM, 16: GUILD_MEDIA).
    Voice channels (type 2: GUILD_VOICE, 13: GUILD_STAGE_VOICE) and category channels (type 4)
    are strictly excluded.
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

DISCORD_API_BASE_URL = "https://discord.com/api/v10"
DEFAULT_USER_AGENT = "aurora-ingestion:discord:v1.0.0"
MAX_PAGE_SIZE = 100
DEFAULT_PAGE_SIZE = 50
DEFAULT_REPLIES_LIMIT = 50

# Supported Discord channel types for message ingestion
# 0: GUILD_TEXT, 5: GUILD_ANNOUNCEMENT, 15: GUILD_FORUM, 16: GUILD_MEDIA
SUPPORTED_TEXT_CHANNEL_TYPES: Set[int] = {0, 5, 15, 16}
# Excluded voice and stage channel types
EXCLUDED_VOICE_CHANNEL_TYPES: Set[int] = {2, 13}

# Known system message type labels
SYSTEM_MESSAGE_TYPES: Dict[int, str] = {
    1: "Recipient added",
    2: "Recipient removed",
    3: "Call started",
    4: "Channel name changed",
    5: "Channel icon changed",
    6: "Pinned a message",
    7: "Server boost",
    8: "Server boost (Tier 1)",
    9: "Server boost (Tier 2)",
    10: "Server boost (Tier 3)",
    11: "Channel follow added",
    12: "Guild discovery disqualified",
    13: "Guild discovery requalified",
    14: "Guild discovery grace period initial warning",
    15: "Guild discovery grace period final warning",
    16: "Thread created",
    18: "Slash command executed",
    20: "Context menu executed",
    22: "Role subscription purchase",
}

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
    """Scrub sensitive credentials, tokens, or headers from strings or logs."""
    scrubbed = str(text)
    for s in secrets:
        if s and len(str(s).strip()) >= 3:
            val = str(s).strip()
            scrubbed = scrubbed.replace(val, "[REDACTED]")
    scrubbed = re.sub(r"(Bot\s+)[^\s'\",]+", r"\1[REDACTED]", scrubbed)
    scrubbed = re.sub(r"(Bearer\s+)[^\s'\",]+", r"\1[REDACTED]", scrubbed)
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

    Distinguishes HTML markup from plain text, mathematical comparisons ('<' and '>'),
    and Markdown syntax. Ignores HTML-like tags inside fenced or inline code blocks.
    """
    if not text or not str(text).strip():
        return False

    text_without_code = re.sub(r"(`{1,}[\s\S]*?`{1,})", "", str(text))
    raw = text_without_code.strip()
    if not raw:
        return False

    if _HTML_DECLARATION_PATTERN.search(raw):
        return True
    if _HTML_CLOSING_TAG_PATTERN.search(raw):
        return True
    if _HTML_VOID_TAGS_PATTERN.search(raw):
        return True
    if _HTML_SCRIPT_STYLE_PATTERN.search(raw):
        return True
    if _HTML_ATTR_TAG_PATTERN.search(raw):
        return True
    if _HTML_OPENING_TAG_PATTERN.search(raw):
        return True

    return False


def normalize_discord_content(content: Any) -> str:
    """Normalize content into clean Obsidian-compatible Markdown without raw HTML.

    - Preserves plain text, mathematical comparisons ('<' and '>'), headings, links,
      tables, lists, and code blocks.
    - If content contains HTML markup, converts it to clean Markdown via markdownify,
      stripping <script> and <style> elements and their internal contents.
    - Code fences and inline backticks are preserved without corruption.
    - Removes residual raw HTML tags while preserving mathematical comparisons.
    """
    if content is None:
        return ""

    raw = str(content).strip()
    if not raw:
        return ""

    if not is_html_content(raw):
        return raw

    clean_html = re.sub(
        r"<\s*(?:script|style)\b[^>]*>.*?<\s*/\s*(?:script|style)\s*>",
        "",
        raw,
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
        return ""

    code_blocks: List[str] = []

    def _mask_code(m: re.Match[str]) -> str:
        code_blocks.append(m.group(0))
        return f"AURORADISCORDCODE{len(code_blocks)-1}TOKEN"

    masked = re.sub(r"(`{3,}[\s\S]*?`{3,}|`[^`\n]+`)", _mask_code, clean_html)

    converted = md(
        masked,
        heading_style="ATX",
        bullets="-",
    ).strip()

    converted = re.sub(r"<[a-zA-Z/][^>]*>", "", converted)

    for i, block in enumerate(code_blocks):
        converted = converted.replace(f"AURORADISCORDCODE{i}TOKEN", block)

    converted = re.sub(r"\n{3,}", "\n\n", converted).strip()
    return converted


def normalize_discord_text(
    text: Any,
    user_cache: Optional[Dict[str, str]] = None,
    channel_cache: Optional[Dict[str, str]] = None,
    role_cache: Optional[Dict[str, str]] = None,
) -> str:
    """Normalize Discord message text into clean Obsidian-compatible Markdown.

    Handles:
    - User mentions: <@123456789> or <@!123456789> -> @username
    - Channel mentions: <#123456789> -> #channel-name
    - Role mentions: <@&123456789> -> @role-name or @role-123456789
    - Custom emojis: <:name:123456789> or <a:name:123456789> -> :name:
    - Safe links: HTTP/HTTPS links preserved or linkified
    - Unsafe schemes: javascript:, data:, file:, ftp: rendered as plain/code text
    - Preserves inline code, code fences, blockquotes, comparisons (< and >)
    - Normalizes raw HTML tags
    """
    if text is None:
        return ""

    raw = str(text)
    if not raw.strip():
        return ""

    users = user_cache or {}
    channels = channel_cache or {}
    roles = role_cache or {}

    # 1. Mask code blocks and inline code
    code_segments: List[str] = []

    def _mask_code_segment(m: re.Match[str]) -> str:
        code_segments.append(m.group(0))
        return f"AURORADISCORDTEXTCODE{len(code_segments)-1}TOKEN"

    masked = re.sub(r"(`{3,}[\s\S]*?`{3,}|`[^`\n]+`)", _mask_code_segment, raw)

    # 2. User mentions: <@123456789> or <@!123456789>
    def _replace_user_mention(m: re.Match[str]) -> str:
        uid = m.group(1).strip()
        resolved = users.get(uid)
        if resolved:
            clean_resolved = resolved.lstrip("@")
            return f"@{clean_resolved}"
        return f"@{uid}"

    masked = re.sub(r"<@!?([0-9]+)>", _replace_user_mention, masked)

    # 3. Channel mentions: <#123456789>
    def _replace_channel_mention(m: re.Match[str]) -> str:
        cid = m.group(1).strip()
        resolved = channels.get(cid)
        if resolved:
            clean_resolved = resolved.lstrip("#")
            return f"#{clean_resolved}"
        return f"#{cid}"

    masked = re.sub(r"<#([0-9]+)>", _replace_channel_mention, masked)

    # 4. Role mentions: <@&123456789>
    def _replace_role_mention(m: re.Match[str]) -> str:
        rid = m.group(1).strip()
        resolved = roles.get(rid)
        if resolved:
            clean_resolved = resolved.lstrip("@")
            return f"@{clean_resolved}"
        return f"@role-{rid}"

    masked = re.sub(r"<@&([0-9]+)>", _replace_role_mention, masked)

    # 5. Custom emojis: <:name:123456789> or <a:name:123456789>
    masked = re.sub(r"<a?:([a-zA-Z0-9_]+):[0-9]+>", r":\1:", masked)

    # 6. Validate Markdown links: [Label](URL)
    def _validate_markdown_link(m: re.Match[str]) -> str:
        label = m.group(1)
        target_url = m.group(2).strip()
        if is_safe_http_url(target_url):
            return f"[{label}]({target_url})"
        # Unsafe scheme: do not render as clickable link
        return f"{label} (`{target_url}`)"

    masked = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _validate_markdown_link, masked)

    # 7. Angle-bracketed URLs: <http://...> or <javascript:...>
    def _validate_angle_url(m: re.Match[str]) -> str:
        target_url = m.group(1).strip()
        if is_safe_http_url(target_url):
            return f"[{target_url}]({target_url})"
        return f"`{target_url}`"

    masked = re.sub(r"<([a-zA-Z][a-zA-Z0-9+.-]*:[^>]+)>", _validate_angle_url, masked)

    # 8. Unescape HTML entities
    masked = masked.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")

    # 9. HTML normalization if raw HTML tags introduced outside code
    if is_html_content(masked):
        masked = normalize_discord_content(masked)

    # 10. Restore code segments
    for i, seg in enumerate(code_segments):
        masked = masked.replace(f"AURORADISCORDTEXTCODE{i}TOKEN", seg)

    return masked.strip()


def format_discord_timestamp(iso_str: Any) -> str:
    """Normalize timestamp into standard ISO 8601 string."""
    if not iso_str:
        return datetime.now(timezone.utc).isoformat()
    try:
        s = str(iso_str).strip()
        dt = datetime.fromisoformat(s)
        return dt.isoformat()
    except Exception:
        return str(iso_str).strip()


def format_discord_display_time(iso_str: Any) -> str:
    """Format ISO 8601 timestamp string for human display: YYYY-MM-DD HH:MM."""
    if not iso_str:
        return "unknown"
    try:
        s = str(iso_str).strip()
        dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        s = str(iso_str).strip()
        if len(s) >= 16 and "T" in s:
            return s[:16].replace("T", " ")
        return s[:10]


def resolve_discord_author(author_data: Optional[Dict[str, Any]]) -> Tuple[str, str, str]:
    """Resolve author information: (display_name, username, user_id).

    Precedence for display: global_name -> username -> user_id -> [user unavailable]
    """
    if not author_data or not isinstance(author_data, dict):
        return ("[user unavailable]", "unknown", "")

    user_id = str(author_data.get("id") or "").strip()
    username = (author_data.get("username") or "").strip()
    global_name = (author_data.get("global_name") or "").strip()

    display_name = global_name or username or user_id or "[user unavailable]"
    author_username = username or global_name or user_id or "unknown"

    return (display_name, author_username, user_id)


def _extract_retry_after(
    response: Optional[requests.Response] = None,
    json_body: Optional[Union[Dict[str, Any], List[Any]]] = None,
) -> float:
    """Extract authoritative rate limit retry delay in seconds from JSON body or response headers.

    Precedence:
    1. json_body['retry_after'] (official Discord JSON payload)
    2. Retry-After response header
    Defaults to 1.0s if neither is available or parsable.
    """
    delay: Optional[float] = None
    if isinstance(json_body, dict):
        val = json_body.get("retry_after")
        if val is not None:
            try:
                delay = float(val)
            except (ValueError, TypeError):
                pass

    if delay is None and response is not None:
        val = response.headers.get("Retry-After")
        if val is not None:
            try:
                delay = float(val)
            except (ValueError, TypeError):
                pass
        if delay is None:
            try:
                b = response.json()
                if isinstance(b, dict) and b.get("retry_after") is not None:
                    delay = float(b["retry_after"])
            except Exception:
                pass

    if delay is None:
        delay = 1.0

    return max(0.0, delay)


def map_discord_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    json_body: Optional[Union[Dict[str, Any], List[Any]]] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map Discord HTTP and API errors to descriptive SourceError without leaking credentials."""
    if response is None and json_body is None and isinstance(exc, SourceError):
        return exc

    sec_list = secrets or []
    status_code = getattr(response, "status_code", None)
    error_code: Optional[int] = None
    error_message: str = ""

    if json_body and isinstance(json_body, dict):
        error_code = json_body.get("code")
        error_message = str(json_body.get("message") or "").strip()
    elif response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                error_code = body.get("code")
                error_message = str(body.get("message") or "").strip()
        except Exception:
            pass

    # Rate Limiting (HTTP 429)
    if status_code == 429:
        retry_after = None
        if response is not None:
            retry_after = response.headers.get("Retry-After")
        if not retry_after and isinstance(json_body, dict):
            retry_after = json_body.get("retry_after")
        elif not retry_after and response is not None:
            try:
                body = response.json()
                if isinstance(body, dict):
                    retry_after = body.get("retry_after")
            except Exception:
                pass

        scope = None
        if response is not None:
            scope = response.headers.get("X-RateLimit-Scope")
            if not scope and response.headers.get("X-RateLimit-Global") == "true":
                scope = "global"
        if not scope and isinstance(json_body, dict):
            if json_body.get("global"):
                scope = "global"

        msg = "Discord API rate limit exceeded (HTTP 429)."
        if retry_after is not None:
            try:
                sec = float(retry_after)
                msg += f" Retry after {sec:.2f}s" if sec < 10 else f" Retry after {int(sec)}s"
            except (ValueError, TypeError):
                msg += f" Retry after {retry_after}s"
        if scope:
            msg += f" (scope: {scope})."
        else:
            msg += "."
        return SourceError(msg)

    # 401 Unauthorized
    if status_code == 401:
        return SourceError(
            "Discord authentication failed (HTTP 401): Invalid or missing bot token. "
            "Ensure DISCORD_BOT_TOKEN is a valid Discord bot token."
        )

    # 403 Forbidden
    if status_code == 403:
        if error_code == 50001:
            return SourceError(
                "Discord permission denied (50001): Missing Access. "
                "The bot is not in the guild or lacks access to the channel."
            )
        if error_code == 50013:
            return SourceError(
                "Discord permission denied (50013): Missing Permissions. "
                "The bot lacks required permissions (View Channel, Read Message History)."
            )
        detail = f" ({error_message})" if error_message else ""
        return SourceError(
            f"Discord permission denied (HTTP 403){detail}: Bot lacks required permissions or privileged "
            "Message Content Intent. Ensure 'MESSAGE CONTENT INTENT' is enabled in the Discord Developer Portal "
            "(Applications > [Your App] > Bot > Privileged Gateway Intents) and the bot has 'View Channel' "
            "and 'Read Message History' permissions in the server."
        )

    # 404 Not Found
    if status_code == 404:
        if error_code == 10003:
            return SourceError("Discord channel not found (10003): Unknown Channel.")
        if error_code == 10004:
            return SourceError("Discord guild not found (10004): Unknown Guild.")
        if error_code == 10008:
            return SourceError("Discord message not found (10008): Unknown Message.")
        detail = f": {error_message}" if error_message else "."
        return SourceError(f"Discord resource not found (HTTP 404){detail}")

    # 5xx Server Error
    if status_code and status_code >= 500:
        return SourceError(f"Discord server error (HTTP {status_code}): Discord API is temporarily unavailable.")

    exc_str = str(exc)
    if (
        isinstance(exc, requests.exceptions.Timeout)
        or "timeout" in exc_str.lower()
        or "timed out" in exc_str.lower()
    ):
        return SourceError("Discord request timed out.")
    if (
        isinstance(exc, requests.exceptions.ConnectionError)
        or "connection" in exc_str.lower()
        or "connect" in exc_str.lower()
    ):
        return SourceError("Network error connecting to Discord API.")

    scrubbed = _scrub_secrets(exc_str, sec_list)
    return SourceError(f"Discord API request failed: {scrubbed}")


def sanitize_attachment_filename(filename: Any) -> str:
    """Sanitize attachment filenames to prevent Markdown link injection or formatting breakage.

    Sanitizes brackets '[', ']', parentheses '(', ')', backticks '`', newlines,
    and control characters while preserving readability.
    """
    if filename is None:
        return "Attachment"
    name = str(filename).strip()
    if not name:
        return "Attachment"

    name = name.replace("[", "_").replace("]", "_").replace("(", "_").replace(")", "_").replace("`", "'")
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name or "Attachment"


def format_discord_embed(emb: Any) -> Optional[str]:
    """Defensively format a single Discord embed object into Markdown.

    Safely handles non-dict embeds, non-dict providers, non-string fields,
    malformed URLs, and markdown injection. One malformed embed will never
    fail the entire message.
    """
    if not isinstance(emb, dict):
        return None

    try:
        # 1. Resolve and sanitize title
        title_raw = emb.get("title")
        if not title_raw:
            provider = emb.get("provider")
            if isinstance(provider, dict):
                title_raw = provider.get("name")

        if title_raw is not None and not isinstance(title_raw, (str, bytes)):
            title_raw = str(title_raw)

        clean_title = str(title_raw or "Embed").strip()
        clean_title = re.sub(r"[\r\n\t]+", " ", clean_title).strip()
        clean_title = clean_title.replace("[", "(").replace("]", ")").replace("`", "'")
        if not clean_title:
            clean_title = "Embed"

        # 2. Resolve and sanitize description
        desc_raw = emb.get("description")
        if desc_raw is not None and not isinstance(desc_raw, (str, bytes)):
            desc_raw = str(desc_raw)
        clean_desc = str(desc_raw or "").strip()
        if clean_desc and is_html_content(clean_desc):
            clean_desc = normalize_discord_content(clean_desc)
        clean_desc = re.sub(r"[\r\n]+", " ", clean_desc).strip()

        # 3. Resolve URL
        url_raw = emb.get("url")
        url_str = str(url_raw).strip() if url_raw is not None else ""

        if url_str and is_safe_http_url(url_str):
            link_part = f"[{clean_title}]({url_str})"
        else:
            link_part = f"`{clean_title}`"

        if clean_desc:
            return f"- **{link_part}**: {clean_desc}"
        return f"- **{link_part}**"
    except Exception as e:
        logger.debug("Skipping malformed embed: %s", e)
        return None


def message_has_content_payload(msg_data: Dict[str, Any]) -> bool:
    """Check if message contains any content-bearing payload or structure.

    Accounts for text content, embeds, attachments, interactive components,
    stickers, and polls.
    """
    if not isinstance(msg_data, dict):
        return False
    if (msg_data.get("content") or "").strip():
        return True
    if msg_data.get("embeds"):
        return True
    if msg_data.get("attachments"):
        return True
    if msg_data.get("components"):
        return True
    if msg_data.get("sticker_items") or msg_data.get("stickers"):
        return True
    if "poll" in msg_data and msg_data.get("poll") is not None:
        return True
    return False


def build_discord_message_body(
    msg_data: Dict[str, Any],
    guild_name: str,
    guild_id: str,
    channel_name: str,
    channel_id: str,
    replies: Optional[List[Dict[str, Any]]] = None,
    user_cache: Optional[Dict[str, str]] = None,
    channel_cache: Optional[Dict[str, str]] = None,
    role_cache: Optional[Dict[str, str]] = None,
) -> str:
    """Construct full Obsidian-compatible Markdown document body for a Discord message/thread."""
    users = user_cache or {}
    chans = channel_cache or {}
    roles = role_cache or {}

    author_data = msg_data.get("author") or {}
    display_author, author_username, author_id = resolve_discord_author(author_data)

    author_heading = f"@{display_author}" if not display_author.startswith("@") else display_author
    author_attr = f"@{author_username}" if not author_username.startswith("@") else author_username

    ts = msg_data.get("timestamp") or ""
    date_iso = format_discord_timestamp(ts)
    date_str = date_iso[:10] if len(date_iso) >= 10 else date_iso
    display_time = format_discord_display_time(ts)

    chan_display = f"#{channel_name.lstrip('#')}" if channel_name else "#unknown"
    guild_display = guild_name.strip() if guild_name else ""
    server_channel = f"{guild_display} / {chan_display}" if guild_display else chan_display

    msg_id = str(msg_data.get("id") or "").strip()
    target_guild_id = str(guild_id).strip() if guild_id else "@me"
    permalink = f"https://discord.com/channels/{target_guild_id}/{channel_id}/{msg_id}"

    msg_type = msg_data.get("type", 0)
    raw_text = msg_data.get("content") or ""

    # Synthesize description for system message events or content structures if text is empty
    if not raw_text.strip():
        if msg_type in SYSTEM_MESSAGE_TYPES:
            raw_text = f"*[System: {SYSTEM_MESSAGE_TYPES[msg_type]}]*"
        elif msg_data.get("poll"):
            poll = msg_data.get("poll")
            q = ""
            if isinstance(poll, dict):
                question_obj = poll.get("question")
                if isinstance(question_obj, dict):
                    q = str(question_obj.get("text") or "").strip()
            raw_text = f"*[Poll: {q}]*" if q else "*[Poll]*"
        elif msg_data.get("sticker_items") or msg_data.get("stickers"):
            stickers = msg_data.get("sticker_items") or msg_data.get("stickers") or []
            names = [str(s.get("name") or "").strip() for s in stickers if isinstance(s, dict) and s.get("name")]
            raw_text = f"*[Sticker: {', '.join(names)}]*" if names else "*[Sticker]*"
        elif msg_data.get("components"):
            raw_text = "*[Interactive Component]*"

    normalized_text = normalize_discord_text(
        raw_text, user_cache=users, channel_cache=chans, role_cache=roles
    )

    # Derive note title
    first_line = ""
    for line in normalized_text.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if first_line:
        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
        title = clean_title[:80].rstrip() or f"Discord message in {chan_display}"
    else:
        title = f"Discord message {msg_id}"

    # Ensure title contains no raw HTML
    title = normalize_discord_content(title)

    # 1. Top H1 & Source Attribution Block
    source_ref = f"[Discord — {server_channel}]({permalink})" if is_safe_http_url(permalink) else f"Discord — {server_channel}"
    reply_count = len(replies) if replies else 0

    attr_parts = [
        f"> **Source**: {source_ref}",
        f"> **Author**: {author_attr} · **Date**: {date_str}" + (f" · **Replies**: {reply_count}" if reply_count else ""),
    ]

    sections: List[str] = [
        f"# {title}",
        "\n".join(attr_parts),
    ]

    # 2. Main Message Section
    msg_body = normalized_text if normalized_text else "*No message content.*"
    sections.append(f"## Message\n\n### {author_heading} — {display_time}\n\n{msg_body}")

    # Attachments
    attachments = msg_data.get("attachments") or []
    if attachments and isinstance(attachments, list):
        att_lines: List[str] = []
        for att in attachments:
            if isinstance(att, dict):
                a_name = sanitize_attachment_filename(att.get("filename"))
                a_type = re.sub(r"[^\w\./+-]", "", str(att.get("content_type") or "file")).strip() or "file"
                a_size = att.get("size", 0)
                a_url = att.get("url")
                size_str = f"{a_size:,} bytes" if isinstance(a_size, int) else str(a_size)
                if a_url and is_safe_http_url(a_url):
                    att_lines.append(f"- [{a_name}]({a_url}) (`{a_type}`, {size_str})")
                else:
                    att_lines.append(f"- `{a_name}` (`{a_type}`, {size_str})")
        if att_lines:
            sections.append("### Attachments\n\n" + "\n".join(att_lines))

    # Embeds
    embeds = msg_data.get("embeds") or []
    if embeds and isinstance(embeds, list):
        emb_lines: List[str] = []
        for emb in embeds:
            formatted_emb = format_discord_embed(emb)
            if formatted_emb:
                emb_lines.append(formatted_emb)
        if emb_lines:
            sections.append("### Embeds\n\n" + "\n".join(emb_lines))

    # 3. Thread Section if replies exist
    if replies:
        thread_parts: List[str] = []
        for reply in replies:
            r_author_data = reply.get("author") or {}
            r_disp, _, _ = resolve_discord_author(r_author_data)
            r_heading = f"@{r_disp}" if not r_disp.startswith("@") else r_disp
            r_ts = reply.get("timestamp") or ""
            r_time = format_discord_display_time(r_ts)
            r_text = normalize_discord_text(
                reply.get("content") or "", user_cache=users, channel_cache=chans, role_cache=roles
            )
            r_body = r_text if r_text else "*No reply content.*"
            thread_parts.append(f"### {r_heading} — {r_time}\n\n{r_body}")

        if thread_parts:
            sections.append("## Thread\n\n" + "\n\n".join(thread_parts))

    return "\n\n".join(sections).strip() + "\n"


class DiscordSource(BaseSource):
    """Discord ingestion connector using official Discord REST API v10."""

    source_type = "discord"

    def __init__(
        self,
        token: Optional[str] = None,
        guild: Optional[Union[str, List[str]]] = None,
        guilds: Optional[str] = None,
        channel: Optional[Union[str, List[str]]] = None,
        channels: Optional[str] = None,
        limit: Optional[int] = None,
        max_messages: Optional[int] = None,
        max_replies: int = DEFAULT_REPLIES_LIMIT,
        include_threads: bool = True,
        no_threads: bool = False,
        page_size: Optional[int] = None,
        session: Optional[requests.Session] = None,
        api_base_url: str = DISCORD_API_BASE_URL,
        max_retries: int = 3,
        sleep_fn: Optional[Callable[[float], None]] = None,
    ):
        self.token = self._resolve_token(token)
        self.configured_guilds = self._parse_inputs(guild or guilds, ["DISCORD_GUILD_ID", "DISCORD_GUILDS"])
        self.configured_channels = self._parse_inputs(channel or channels, ["DISCORD_CHANNEL_ID", "DISCORD_CHANNELS"])
        self.default_limit = limit or max_messages or DEFAULT_PAGE_SIZE
        self.page_size = page_size or DEFAULT_PAGE_SIZE
        self.max_replies = max_replies
        self.include_threads = False if no_threads else include_threads
        self.session = session or requests.Session()
        self.api_base_url = api_base_url.rstrip("/")
        self.max_retries = max(0, int(max_retries))
        self.sleep_fn = sleep_fn or time.sleep

        self._secrets: List[Optional[str]] = [self.token] if self.token else []
        self._user_cache: Dict[str, str] = {}
        self._channel_cache: Dict[str, str] = {}
        self._role_cache: Dict[str, str] = {}
        self._guild_cache: Dict[str, str] = {}

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Discord"

    @staticmethod
    def _resolve_token(token: Optional[str] = None) -> Optional[str]:
        """Resolve Discord Bot token from parameter or DISCORD_BOT_TOKEN environment variable."""
        if token and str(token).strip():
            raw_token = str(token).strip()
        else:
            env_val = os.environ.get("DISCORD_BOT_TOKEN")
            raw_token = env_val.strip() if env_val else ""

        if not raw_token:
            return None

        if raw_token.lower().startswith("bearer "):
            raise SourceError(
                "Invalid Discord token: User/Bearer tokens are not supported. "
                "Aurora requires an official Discord Bot token."
            )

        if raw_token.startswith("Bot "):
            raw_token = raw_token[4:].strip()

        return raw_token

    @staticmethod
    def _parse_inputs(raw: Optional[Union[List[str], str]], env_keys: List[str]) -> List[str]:
        """Normalize comma-separated strings or string lists into cleaned items."""
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

    def _get_headers(self) -> Dict[str, str]:
        """Build HTTP request headers with Bot authorization."""
        headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bot {self.token}"
        return headers

    def _api_call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Perform an HTTP request against the Discord REST API with bounded 429 retry and error mapping."""
        if not self.token:
            raise SourceError(
                "Discord bot token is required. Set DISCORD_BOT_TOKEN environment variable or pass --token."
            )

        url = f"{self.api_base_url}/{endpoint.lstrip('/')}"
        headers = self._get_headers()

        attempt = 0
        while True:
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, headers=headers, params=params, timeout=30)
                else:
                    resp = self.session.post(url, headers=headers, params=params, json=json_data, timeout=30)
            except Exception as e:
                raise map_discord_error(e, secrets=self._secrets)

            try:
                data = resp.json()
            except Exception:
                data = None

            if resp.status_code == 429:
                if attempt < self.max_retries:
                    attempt += 1
                    delay = _extract_retry_after(response=resp, json_body=data)
                    logger.warning(
                        "Discord API rate limit hit (HTTP 429). Retrying attempt %d/%d after %.2fs...",
                        attempt,
                        self.max_retries,
                        delay,
                    )
                    self.sleep_fn(delay)
                    continue
                else:
                    raise map_discord_error(
                        SourceError(f"Discord API rate limit exceeded (HTTP 429) after {self.max_retries} retries"),
                        response=resp,
                        json_body=data,
                        secrets=self._secrets,
                    )

            if data is None:
                raise map_discord_error(
                    SourceError(f"Discord API returned non-JSON response (HTTP {resp.status_code})"),
                    response=resp,
                    secrets=self._secrets,
                )

            if resp.status_code != 200:
                raise map_discord_error(
                    SourceError(f"Discord API error (HTTP {resp.status_code})"),
                    response=resp,
                    json_body=data,
                    secrets=self._secrets,
                )

            return data

    def _get_guild_name(self, guild_id: str) -> str:
        """Fetch and cache guild name for a given guild ID."""
        if not guild_id or guild_id == "@me":
            return ""
        if guild_id in self._guild_cache:
            return self._guild_cache[guild_id]

        try:
            g_data = self._api_call("GET", f"/guilds/{guild_id}")
            if isinstance(g_data, dict):
                name = str(g_data.get("name") or "").strip()
                if name:
                    self._guild_cache[guild_id] = name
                    return name
        except Exception as e:
            logger.debug("Failed to fetch guild %s name: %s", guild_id, _scrub_secrets(str(e), self._secrets))

        self._guild_cache[guild_id] = guild_id
        return guild_id

    def _resolve_channels(
        self,
        guild_inputs: List[str],
        channel_inputs: List[str],
    ) -> List[Dict[str, Any]]:
        """Discover and resolve target Discord channels from guild/channel inputs.

        Filters out voice channels (type 2, 13) and non-message channel types.
        Populates channel cache.
        """
        resolved: List[Dict[str, Any]] = []
        seen_channel_ids: Set[str] = set()

        # Case 1: Guilds configured
        if guild_inputs:
            for gid in guild_inputs:
                try:
                    guild_channels = self._api_call("GET", f"/guilds/{gid}/channels")
                except Exception as ge:
                    logger.warning(
                        "Failed to fetch channels for Discord guild %s: %s",
                        gid,
                        _scrub_secrets(str(ge), self._secrets),
                    )
                    continue

                if not isinstance(guild_channels, list):
                    continue

                g_name = self._get_guild_name(gid)

                for ch in guild_channels:
                    c_id = str(ch.get("id") or "").strip()
                    c_name = str(ch.get("name") or "").strip()
                    c_type = ch.get("type", 0)

                    # Populate channel cache
                    if c_id and c_name:
                        self._channel_cache[c_id] = c_name

                    if c_type in EXCLUDED_VOICE_CHANNEL_TYPES:
                        continue
                    if c_type not in SUPPORTED_TEXT_CHANNEL_TYPES:
                        continue

                    # If channel filters are specified, match by ID or clean name
                    if channel_inputs:
                        clean_inputs = {c.lstrip("#").lower() for c in channel_inputs}
                        if c_id not in channel_inputs and c_name.lower() not in clean_inputs:
                            continue

                    if c_id and c_id not in seen_channel_ids:
                        seen_channel_ids.add(c_id)
                        resolved.append({
                            "id": c_id,
                            "name": c_name or c_id,
                            "type": c_type,
                            "guild_id": gid,
                            "guild_name": g_name,
                        })

        # Case 2: Channels configured without guild
        elif channel_inputs:
            for ch_input in channel_inputs:
                clean_target = ch_input.strip()
                is_numeric_id = clean_target.isdigit()

                if not is_numeric_id:
                    raise SourceError(
                        f"Channel name '{ch_input}' requires --guild / DISCORD_GUILD_ID to resolve. "
                        "Alternatively, pass the channel Snowflake ID."
                    )

                try:
                    ch_obj = self._api_call("GET", f"/channels/{clean_target}")
                except Exception as ce:
                    logger.warning(
                        "Skipping inaccessible Discord channel %s: %s",
                        clean_target,
                        _scrub_secrets(str(ce), self._secrets),
                    )
                    continue

                if not isinstance(ch_obj, dict):
                    continue

                c_id = str(ch_obj.get("id") or clean_target).strip()
                c_name = str(ch_obj.get("name") or c_id).strip()
                c_type = ch_obj.get("type", 0)
                gid = str(ch_obj.get("guild_id") or "").strip()
                g_name = self._get_guild_name(gid) if gid else ""

                if c_id and c_name:
                    self._channel_cache[c_id] = c_name

                if c_type in EXCLUDED_VOICE_CHANNEL_TYPES:
                    logger.warning("Channel %s is a voice channel; skipping.", c_id)
                    continue
                if c_type not in SUPPORTED_TEXT_CHANNEL_TYPES:
                    logger.warning("Channel %s has unsupported type %s; skipping.", c_id, c_type)
                    continue

                if c_id and c_id not in seen_channel_ids:
                    seen_channel_ids.add(c_id)
                    resolved.append({
                        "id": c_id,
                        "name": c_name,
                        "type": c_type,
                        "guild_id": gid,
                        "guild_name": g_name,
                    })

        return resolved

    def _fetch_channel_messages(
        self,
        channel_id: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Paginate backwards through Discord channel message history using before=<oldest_id>.

        Returns collected messages sorted chronologically (ascending).
        """
        collected: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()
        before_id: Optional[str] = None

        while len(collected) < limit:
            batch_size = min(limit - len(collected), self.page_size, MAX_PAGE_SIZE)
            params: Dict[str, Any] = {"limit": batch_size}
            if before_id:
                params["before"] = before_id

            resp_data = self._api_call("GET", f"/channels/{channel_id}/messages", params=params)
            if not isinstance(resp_data, list) or not resp_data:
                break

            new_messages: List[Dict[str, Any]] = []
            for msg in resp_data:
                m_id = str(msg.get("id") or "")
                if m_id and m_id not in seen_ids:
                    seen_ids.add(m_id)
                    new_messages.append(msg)

            if not new_messages:
                break

            collected.extend(new_messages)

            oldest_id = str(resp_data[-1].get("id") or "")
            if not oldest_id or oldest_id == before_id:
                break
            before_id = oldest_id

            if len(resp_data) < batch_size:
                break

        # Chronological sort: ascending by timestamp / snowflake ID
        def _sort_key(m: Dict[str, Any]) -> Tuple[str, int]:
            ts = str(m.get("timestamp") or "")
            try:
                msg_id_num = int(str(m.get("id") or 0))
            except (ValueError, TypeError):
                msg_id_num = 0
            return (ts, msg_id_num)

        collected.sort(key=_sort_key)
        return collected

    def _fetch_thread_replies(
        self,
        thread_id: str,
        root_message_id: str,
        max_replies: int,
    ) -> List[Dict[str, Any]]:
        """Fetch replies from a Discord thread channel, ordered chronologically."""
        replies: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()
        before_id: Optional[str] = None

        while len(replies) < max_replies:
            batch_size = min(max_replies - len(replies), self.page_size, MAX_PAGE_SIZE)
            params: Dict[str, Any] = {"limit": batch_size}
            if before_id:
                params["before"] = before_id

            try:
                resp_data = self._api_call("GET", f"/channels/{thread_id}/messages", params=params)
            except Exception as te:
                logger.warning(
                    "Failed to fetch thread %s replies: %s",
                    thread_id,
                    _scrub_secrets(str(te), self._secrets),
                )
                break

            if not isinstance(resp_data, list) or not resp_data:
                break

            new_replies: List[Dict[str, Any]] = []
            for reply in resp_data:
                r_id = str(reply.get("id") or "")
                # Exclude root message itself
                if r_id == root_message_id:
                    continue
                if r_id and r_id not in seen_ids:
                    seen_ids.add(r_id)
                    new_replies.append(reply)

            if not new_replies:
                break

            replies.extend(new_replies)

            oldest_id = str(resp_data[-1].get("id") or "")
            if not oldest_id or oldest_id == before_id:
                break
            before_id = oldest_id

            if len(resp_data) < batch_size:
                break

        def _sort_key(m: Dict[str, Any]) -> Tuple[str, int]:
            ts = str(m.get("timestamp") or "")
            try:
                msg_id_num = int(str(m.get("id") or 0))
            except (ValueError, TypeError):
                msg_id_num = 0
            return (ts, msg_id_num)

        replies.sort(key=_sort_key)
        return replies

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch conversation messages and threads from configured Discord guild channels."""
        # 1. Resolve token override
        token_override = kwargs.get("token")
        if token_override:
            self.token = self._resolve_token(str(token_override))
            self._secrets = [self.token] if self.token else []

        if not self.token:
            raise SourceError(
                "Discord bot token is required. Set DISCORD_BOT_TOKEN environment variable or pass --token."
            )

        # 2. Resolve guild and channel inputs
        guild_arg = kwargs.get("guild") or kwargs.get("guilds")
        if guild_arg:
            target_guilds = self._parse_inputs(guild_arg, ["DISCORD_GUILD_ID", "DISCORD_GUILDS"])
        else:
            target_guilds = list(self.configured_guilds)

        channel_arg = kwargs.get("channel") or kwargs.get("channels")
        if channel_arg:
            target_channels = self._parse_inputs(channel_arg, ["DISCORD_CHANNEL_ID", "DISCORD_CHANNELS"])
        else:
            target_channels = list(self.configured_channels)

        if not target_guilds and not target_channels:
            raise SourceError(
                "No Discord guild or channel specified. Provide --guild/--guilds or --channel/--channels "
                "(or set DISCORD_GUILD_ID / DISCORD_CHANNEL_ID)."
            )

        limit = kwargs.get("limit") or kwargs.get("max_messages") or self.default_limit
        max_replies = kwargs.get("max_replies", self.max_replies)
        include_threads = kwargs.get("include_threads", self.include_threads)
        if kwargs.get("no_threads", False):
            include_threads = False

        # 3. Discover and resolve target channels
        channels_to_fetch = self._resolve_channels(target_guilds, target_channels)
        if not channels_to_fetch:
            raise SourceError(
                f"None of the configured Discord channels could be found or accessed (guilds: {target_guilds}, channels: {target_channels})"
            )

        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        # 4. Fetch messages from each channel
        for ch_meta in channels_to_fetch:
            channel_id = ch_meta["id"]
            channel_name = ch_meta["name"]
            guild_id = ch_meta.get("guild_id", "")
            guild_name = ch_meta.get("guild_name", "")

            try:
                messages = self._fetch_channel_messages(channel_id, limit=limit)
            except Exception as fe:
                logger.warning(
                    "Failed to fetch messages for Discord channel %s: %s",
                    channel_id,
                    _scrub_secrets(str(fe), self._secrets),
                )
                continue

            for msg_data in messages:
                msg_id = str(msg_data.get("id") or "").strip()
                if not msg_id:
                    continue

                source_id = f"discord:message:{channel_id}:{msg_id}"
                if source_id in seen_source_ids:
                    continue

                try:
                    # Update cache with mentions in this message
                    for u in msg_data.get("mentions") or []:
                        if isinstance(u, dict):
                            u_id = str(u.get("id") or "")
                            u_name = u.get("global_name") or u.get("username") or u_id
                            if u_id and u_name:
                                self._user_cache[u_id] = u_name

                    raw_text = msg_data.get("content") or ""
                    attachments = msg_data.get("attachments") or []
                    embeds = msg_data.get("embeds") or []
                    msg_type = msg_data.get("type", 0)

                    # Validate Message Content Intent
                    # Standard user messages (type 0/19) that have no content payload (no text, attachments,
                    # embeds, interactive components, stickers, or polls) indicate missing privileged intent.
                    if not message_has_content_payload(msg_data) and msg_type in (0, 19):
                        raise SourceError(
                            "Discord message content is empty. This typically indicates the bot is missing "
                            "the privileged 'MESSAGE CONTENT INTENT'. Enable 'Message Content Intent' in the "
                            "Discord Developer Portal under Applications > [Your App] > Bot > Privileged Gateway Intents."
                        )

                    # Fetch thread replies if applicable
                    replies: List[Dict[str, Any]] = []
                    thread_id: Optional[str] = None
                    if include_threads:
                        if isinstance(msg_data.get("thread"), dict):
                            thread_id = str(msg_data["thread"].get("id") or "")
                        elif msg_data.get("has_thread"):
                            thread_id = msg_id

                        if thread_id:
                            try:
                                replies = self._fetch_thread_replies(
                                    thread_id, root_message_id=msg_id, max_replies=max_replies
                                )
                            except Exception as te:
                                logger.warning(
                                    "Failed to fetch thread replies for message %s: %s",
                                    msg_id,
                                    _scrub_secrets(str(te), self._secrets),
                                )
                                replies = []

                    body = build_discord_message_body(
                        msg_data=msg_data,
                        guild_name=guild_name,
                        guild_id=guild_id,
                        channel_name=channel_name,
                        channel_id=channel_id,
                        replies=replies if include_threads else None,
                        user_cache=self._user_cache,
                        channel_cache=self._channel_cache,
                        role_cache=self._role_cache,
                    )

                    author_data = msg_data.get("author") or {}
                    _, author_username, author_id = resolve_discord_author(author_data)

                    ts = msg_data.get("timestamp") or ""
                    date_iso = format_discord_timestamp(ts)
                    target_guild_id = str(guild_id).strip() if guild_id else "@me"
                    permalink = f"https://discord.com/channels/{target_guild_id}/{channel_id}/{msg_id}"

                    first_line = ""
                    clean_norm = normalize_discord_text(
                        raw_text, user_cache=self._user_cache, channel_cache=self._channel_cache, role_cache=self._role_cache
                    )
                    for line in clean_norm.splitlines():
                        if line.strip():
                            first_line = line.strip()
                            break
                    if first_line:
                        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
                        title = clean_title[:80].rstrip() or f"Discord message in #{channel_name}"
                    else:
                        title = f"Discord message {msg_id}"
                    title = normalize_discord_content(title)

                    extra_meta: Dict[str, Any] = {
                        "guild_id": guild_id,
                        "guild": guild_name,
                        "channel": channel_name,
                        "channel_id": channel_id,
                        "author": author_username,
                        "author_id": author_id,
                        "discord_message_id": msg_id,
                        "reply_count": len(replies) if replies else 0,
                    }
                    if thread_id:
                        extra_meta["thread_id"] = thread_id
                    if msg_data.get("edited_timestamp"):
                        extra_meta["edited_timestamp"] = msg_data.get("edited_timestamp")

                    item = SourceItem(
                        source_id=source_id,
                        title=title,
                        source_type="discord",
                        content=body,
                        author=author_username,
                        date=date_iso,
                        source_url=permalink,
                        tags=["ingested", "discord", "social"],
                        summary=clean_norm[:200].strip() or None,
                        extra_metadata=extra_meta,
                    )

                    seen_source_ids.add(source_id)
                    items.append(item)

                except SourceError:
                    raise
                except Exception as me:
                    logger.warning(
                        "Skipping malformed Discord message %s: %s",
                        msg_id,
                        _scrub_secrets(str(me), self._secrets),
                    )
                    continue

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a SourceItem into frontmatter metadata and Markdown body targeted at Ingested/Social/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Social"
        return note


# Register Discord connector in SourceRegistry
SourceRegistry.register("discord", DiscordSource)
