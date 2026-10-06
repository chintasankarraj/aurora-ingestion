"""Slack source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Slack Web API using Bearer token authentication
(SLACK_TOKEN or SLACK_BOT_TOKEN). Ingests conversation messages and threaded
replies from accessible public and private Slack channels into Aurora vault
under Ingested/Social/ as clean Obsidian-compatible Markdown notes with YAML
frontmatter, attribution blocks, and chronologically ordered threads.

API and Authentication Contract:
- API Base URL: https://slack.com/api
- Authentication: HTTP Bearer token via Authorization: Bearer <token>
- Environment Variables:
    SLACK_TOKEN / SLACK_BOT_TOKEN: Slack User or Bot OAuth token (e.g. xoxb-..., xoxp-...)
    SLACK_CHANNELS / SLACK_CHANNEL: Optional comma-separated list of channel names or IDs
- Endpoints:
    POST/GET https://slack.com/api/conversations.list (channel discovery)
    POST/GET https://slack.com/api/conversations.history (channel message history)
    POST/GET https://slack.com/api/conversations.replies (thread replies)
    POST/GET https://slack.com/api/users.list (workspace user resolution cache)
    POST/GET https://slack.com/api/users.info (individual user fallback lookup)
- Required OAuth Scopes:
    Public channels:  channels:history, channels:read
    Private channels: groups:history, groups:read
    User resolution:  users:read
- Rate Limiting Notice:
    Slack imposes Tier 3/Tier 2 or non-Marketplace limits on conversations.history
    and conversations.replies (as low as 1 request/minute and max 15 objects/request
    for newer non-Marketplace apps). HTTP 429 and ok: false, error: ratelimited are
    defensively handled with Retry-After inspection.
- Response Protocol:
    Slack returns HTTP 200 with {"ok": false, "error": "..."} on API errors.
    All responses validate resp.json().get("ok") is True.
"""

from __future__ import annotations

import html
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import requests
from markdownify import markdownify as md

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

SLACK_API_BASE_URL = "https://slack.com/api"
DEFAULT_USER_AGENT = "aurora-ingestion:slack:v1.0.0"
DEFAULT_CONVERSATIONS_LIST_LIMIT = 100
SLACK_API_MAX_BATCH_SIZE = 15
DEFAULT_HISTORY_LIMIT = 15
DEFAULT_REPLIES_LIMIT = 15

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
    # Scrub potential Bearer tokens matching Slack patterns
    scrubbed = re.sub(r"xox[baprs]-[0-9a-zA-Z-]+", "[REDACTED_SLACK_TOKEN]", scrubbed)
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

    Safely distinguishes HTML markup from plain text and Markdown formatting
    (such as comparison operators, mathematical expressions, or Markdown syntax).
    Ignores HTML-like tags inside fenced or inline code blocks.
    """
    if not text or not str(text).strip():
        return False

    # Mask out code blocks and inline code when evaluating whether text contains HTML
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


def normalize_slack_content(content: Any) -> str:
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

    # 1. Strip script and style blocks and their internal contents
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

    # 2. Mask code blocks so markdownify does not alter code fences or inline backticks
    code_blocks: List[str] = []

    def _mask_code(m: re.Match[str]) -> str:
        code_blocks.append(m.group(0))
        return f"AURORACODEBLOCK{len(code_blocks)-1}TOKEN"

    masked = re.sub(r"(`{3,}[\s\S]*?`{3,}|`[^`\n]+`)", _mask_code, clean_html)

    # 3. Convert HTML to clean Markdown via markdownify
    converted = md(
        masked,
        heading_style="ATX",
        bullets="-",
    ).strip()

    # 4. Strip any residual unparsed HTML tags outside of code blocks
    converted = re.sub(r"<[a-zA-Z/][^>]*>", "", converted)

    # 5. Restore code blocks
    for i, block in enumerate(code_blocks):
        converted = converted.replace(f"AURORACODEBLOCK{i}TOKEN", block)

    # Normalize excessive newlines
    converted = re.sub(r"\n{3,}", "\n\n", converted).strip()
    return converted


def normalize_slack_text(
    text: Any,
    user_cache: Optional[Dict[str, str]] = None,
    channel_cache: Optional[Dict[str, str]] = None,
) -> str:
    """Normalize Slack mrkdwn text into clean Obsidian-compatible Markdown.

    Handles:
    - User mentions: <@U123456> or <@U123456|alice> -> @alice
    - Special mentions: <!here> -> @here, <!channel> -> @channel, <!everyone> -> @everyone
    - Subteam mentions: <!subteam^S123|@group> -> @group
    - Channel mentions: <#C123456|general> -> #general, <#C123456> -> #general
    - Links: <https://example.com|Label> -> [Label](https://example.com) (HTTP/HTTPS only)
    - Raw links: <https://example.com> -> [https://example.com](https://example.com) (HTTP/HTTPS only)
    - Unsafe schemes: javascript:, data:, file: -> rendered as escaped/plain text
    - Preserves code blocks, inline code, lists, blockquotes, comparisons (< and >)
    - Cleans raw HTML
    """
    if text is None:
        return ""

    raw = str(text)
    if not raw.strip():
        return ""

    users = user_cache or {}
    channels = channel_cache or {}

    # 1. Mask code blocks and inline code to protect code contents from Slack entity replacement
    code_segments: List[str] = []

    def _mask_code_segment(m: re.Match[str]) -> str:
        code_segments.append(m.group(0))
        return f"AURORASLACKCODE{len(code_segments)-1}TOKEN"

    masked = re.sub(r"(`{3,}[\s\S]*?`{3,}|`[^`\n]+`)", _mask_code_segment, raw)

    # 2. Replace Slack user mentions: <@U12345678> or <@U12345678|alice>
    def _replace_user_mention(m: re.Match[str]) -> str:
        user_id = m.group(1).strip()
        label = (m.group(2) or "").strip()
        if label:
            clean_label = label.lstrip("@")
            return f"@{clean_label}"
        resolved = users.get(user_id)
        if resolved:
            clean_resolved = resolved.lstrip("@")
            return f"@{clean_resolved}"
        return f"@{user_id}"

    masked = re.sub(r"<@([A-Z0-9]+)(?:\|([^>]+))?>", _replace_user_mention, masked)

    # 3. Replace Slack special commands / group mentions
    masked = re.sub(r"<!here(?:\|[^>]+)?>", "@here", masked)
    masked = re.sub(r"<!channel(?:\|[^>]+)?>", "@channel", masked)
    masked = re.sub(r"<!everyone(?:\|[^>]+)?>", "@everyone", masked)
    masked = re.sub(r"<!subteam\^[A-Z0-9]+(?:\|(@?[^>]+))?>", lambda m: f"@{m.group(1).lstrip('@')}" if m.group(1) else "@subteam", masked)
    masked = re.sub(r"<!date\^([^|>]+)\^([^|>]+)(?:\|([^>]+))?>", lambda m: m.group(3) or "date", masked)

    # 4. Replace Slack channel mentions: <#C123456|channel-name> or <#C123456>
    def _replace_channel_mention(m: re.Match[str]) -> str:
        chan_id = m.group(1).strip()
        label = (m.group(2) or "").strip()
        if label:
            clean_name = label.lstrip("#")
            return f"#{clean_name}"
        resolved = channels.get(chan_id)
        if resolved:
            clean_name = resolved.lstrip("#")
            return f"#{clean_name}"
        return f"#{chan_id}"

    masked = re.sub(r"<#([A-Z0-9]+)(?:\|([^>]+))?>", _replace_channel_mention, masked)

    # 5. Replace Slack links: <URL|Label> or <URL>
    def _replace_slack_link(m: re.Match[str]) -> str:
        target_url = m.group(1).strip()
        label = (m.group(2) or "").strip()

        if is_safe_http_url(target_url):
            if label:
                return f"[{label}]({target_url})"
            return f"[{target_url}]({target_url})"
        else:
            # Unsafe URL: do not create clickable Markdown link
            if label:
                return f"{label} (`{target_url}`)"
            return f"`{target_url}`"

    masked = re.sub(r"<([a-zA-Z][a-zA-Z0-9+.-]*:[^|>]+)(?:\|([^>]+))?>", _replace_slack_link, masked)

    # 6. Unescape Slack-encoded HTML entities: &amp; -> &, &lt; -> <, &gt; -> >
    # Slack message text encodes literal <, >, & as &lt;, &gt;, &amp;.
    # Notice that standard blockquote syntax in Slack might be represented as '&gt; '
    masked = masked.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")

    # 7. Convert HTML to clean Markdown if HTML was introduced, stripping <script> and <style>
    if is_html_content(masked):
        masked = normalize_slack_content(masked)

    # 8. Restore code segments untouched
    for i, seg in enumerate(code_segments):
        masked = masked.replace(f"AURORASLACKCODE{i}TOKEN", seg)

    return masked.strip()


def format_slack_timestamp(ts: Any) -> str:
    """Convert Slack ts (epoch seconds with microsecond string) to ISO 8601 string."""
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    try:
        sec = float(str(ts).strip())
        return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return str(ts)


def format_slack_display_time(ts: Any) -> str:
    """Format Slack ts for human display in Markdown headings: YYYY-MM-DD HH:MM."""
    if not ts:
        return "unknown"
    try:
        sec = float(str(ts).strip())
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError, OSError):
        return str(ts)


def map_slack_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    json_body: Optional[Dict[str, Any]] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map Slack HTTP and API errors to descriptive SourceError without leaking credentials."""
    if response is None and json_body is None and isinstance(exc, SourceError):
        return exc

    sec_list = secrets or []
    status_code = getattr(response, "status_code", None)
    error_code = ""

    if json_body and isinstance(json_body, dict):
        error_code = str(json_body.get("error") or "").strip()
    elif response is not None:
        try:
            body = response.json()
            if isinstance(body, dict):
                error_code = str(body.get("error") or "").strip()
        except Exception:
            pass

    retry_after = None
    if response is not None:
        retry_after = response.headers.get("Retry-After")

    if status_code == 429 or error_code == "ratelimited":
        msg = "Slack API rate limit exceeded. Please wait before retrying (HTTP 429)."
        if retry_after:
            try:
                sec = int(float(retry_after))
                msg += f" Rate limit resets in {sec}s."
            except (ValueError, TypeError):
                pass
        return SourceError(msg)

    if status_code == 401 or error_code in ("invalid_auth", "not_authed", "account_inactive"):
        return SourceError(
            "Slack authentication failed: Invalid, missing, or inactive token (invalid_auth)."
        )

    if error_code == "token_revoked":
        return SourceError("Slack authentication failed: Token has been revoked (token_revoked).")

    if error_code == "missing_scope":
        needed = ""
        if json_body and isinstance(json_body, dict):
            needed = str(json_body.get("needed") or "").strip()
        detail = f" Missing required scope: {needed}." if needed else ""
        return SourceError(f"Slack permission denied: Insufficient OAuth scopes (missing_scope).{detail}".strip())

    if error_code == "channel_not_found":
        return SourceError("Slack channel not found: Channel does not exist or bot lacks access (channel_not_found).")

    if error_code == "not_in_channel":
        return SourceError(
            "Slack bot is not a member of the channel. Please invite the bot to the channel (/invite @bot) (not_in_channel)."
        )

    if error_code == "is_archived":
        return SourceError("Slack channel is archived (is_archived).")

    if error_code == "restricted_action":
        return SourceError("Slack action restricted by workspace policy (restricted_action).")

    if status_code is not None and status_code >= 500:
        return SourceError(f"Slack server error (HTTP {status_code}): Service temporarily unavailable.")

    if error_code:
        return SourceError(f"Slack API error: {_scrub_secrets(error_code, sec_list)}")

    if status_code is not None and status_code != 200:
        return SourceError(f"Slack HTTP error (HTTP {status_code}).")

    if isinstance(exc, requests.exceptions.Timeout):
        return SourceError("Slack API request timed out.")
    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError("Network connection failed while connecting to Slack API.")

    return SourceError(f"Slack connector error: {_scrub_secrets(str(exc), sec_list)}")


def build_slack_message_body(
    msg_data: Dict[str, Any],
    channel_name: str,
    channel_id: str,
    replies: Optional[List[Dict[str, Any]]] = None,
    user_cache: Optional[Dict[str, str]] = None,
    channel_cache: Optional[Dict[str, str]] = None,
) -> str:
    """Construct full Obsidian-compatible Markdown document body for a Slack message/thread."""
    users = user_cache or {}
    chans = channel_cache or {}

    raw_user_id = msg_data.get("user") or msg_data.get("bot_id") or "unknown"
    author_display = users.get(raw_user_id) or msg_data.get("username") or raw_user_id
    if author_display != "unknown" and not author_display.startswith("@"):
        author_display = f"@{author_display}"

    ts = msg_data.get("ts") or ""
    date_iso = format_slack_timestamp(ts)
    date_str = date_iso[:10] if len(date_iso) >= 10 else date_iso

    chan_display = f"#{channel_name.lstrip('#')}" if channel_name else "#unknown"
    clean_ts_num = str(ts).replace(".", "")
    permalink = msg_data.get("permalink") or f"https://slack.com/archives/{channel_id}/p{clean_ts_num}"

    raw_text = msg_data.get("text") or ""
    normalized_text = normalize_slack_text(raw_text, user_cache=users, channel_cache=chans)

    # Title derivation
    first_line = ""
    for line in normalized_text.splitlines():
        if line.strip():
            first_line = line.strip()
            break
    if first_line:
        # Strip leading markdown header tokens or blockquote symbols from title
        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
        title = clean_title[:80].rstrip() or f"Slack message in {chan_display}"
    else:
        title = f"Slack message from {author_display} in {chan_display}"

    # 1. Top H1 & Source Attribution Block
    source_ref = f"[Slack — {chan_display}]({permalink})" if is_safe_http_url(permalink) else f"Slack — {chan_display}"
    reply_count = msg_data.get("reply_count", 0)

    attr_parts = [
        f"> **Source**: {source_ref}",
        f"> **Author**: {author_display} · **Date**: {date_str}" + (f" · **Replies**: {reply_count}" if reply_count else ""),
    ]

    sections: List[str] = [
        f"# {title}",
        "\n".join(attr_parts),
    ]

    # 2. Main Message Section
    msg_body = normalized_text if normalized_text else "*No message content.*"
    sections.append(f"## Message\n\n{msg_body}")

    # Files / Attachments metadata if present
    files = msg_data.get("files") or []
    if files and isinstance(files, list):
        file_lines: List[str] = []
        for f in files:
            if isinstance(f, dict):
                f_name = f.get("name") or f.get("title") or "Attachment"
                f_mime = f.get("mimetype") or "unknown"
                f_link = f.get("permalink")
                if f_link and is_safe_http_url(f_link):
                    file_lines.append(f"- [{f_name}]({f_link}) (`{f_mime}`)")
                else:
                    file_lines.append(f"- `{f_name}` (`{f_mime}`)")
        if file_lines:
            sections.append("### Attachments\n\n" + "\n".join(file_lines))

    # 3. Thread Section if replies exist
    if replies:
        thread_parts: List[str] = []

        # Parent in thread
        parent_time = format_slack_display_time(ts)
        thread_parts.append(f"### {author_display} — {parent_time}\n\n{msg_body}")

        # Chronological replies
        for r in replies:
            r_ts = r.get("ts") or ""
            r_user_id = r.get("user") or r.get("bot_id") or "unknown"
            r_author = users.get(r_user_id) or r.get("username") or r_user_id
            if r_author != "unknown" and not r_author.startswith("@"):
                r_author = f"@{r_author}"
            r_time = format_slack_display_time(r_ts)
            r_raw_text = r.get("text") or ""
            r_norm = normalize_slack_text(r_raw_text, user_cache=users, channel_cache=chans)
            r_body = r_norm if r_norm else "*No message content.*"

            thread_parts.append(f"#### {r_author} — {r_time}\n\n{r_body}")

            # File metadata in reply if present
            r_files = r.get("files") or []
            if r_files and isinstance(r_files, list):
                r_file_lines: List[str] = []
                for rf in r_files:
                    if isinstance(rf, dict):
                        rf_name = rf.get("name") or rf.get("title") or "Attachment"
                        rf_mime = rf.get("mimetype") or "unknown"
                        rf_link = rf.get("permalink")
                        if rf_link and is_safe_http_url(rf_link):
                            r_file_lines.append(f"- [{rf_name}]({rf_link}) (`{rf_mime}`)")
                        else:
                            r_file_lines.append(f"- `{rf_name}` (`{rf_mime}`)")
                if r_file_lines:
                    thread_parts.append("**Attachments:**\n" + "\n".join(r_file_lines))

        sections.append("## Thread\n\n" + "\n\n".join(thread_parts))

    return "\n\n".join(sections) + "\n"


# ---------------------------------------------------------------------------
# SlackSource Connector
# ---------------------------------------------------------------------------

class SlackSource(BaseSource):
    """Slack conversation and thread ingestion connector for Aurora."""

    def __init__(
        self,
        token: Optional[str] = None,
        channels: Optional[Union[List[str], str]] = None,
        session: Optional[requests.Session] = None,
        default_limit: int = DEFAULT_HISTORY_LIMIT,
        max_replies: int = 50,
        include_threads: bool = True,
    ) -> None:
        """Initialize Slack connector with token, channel configuration, and HTTP session."""
        self.token = self._resolve_token(token)
        self.configured_channels = self._parse_channel_inputs(channels)
        self.session = session or requests.Session()
        self.default_limit = max(1, default_limit)
        self.max_replies = max(1, max_replies)
        self.include_threads = include_threads

        self._user_cache: Dict[str, str] = {}
        self._channel_cache: Dict[str, str] = {}
        self._secrets: List[Optional[str]] = [self.token]

    @property
    def source_type(self) -> str:
        """Source type identifier."""
        return "slack"

    @property
    def display_name(self) -> str:
        """Human-readable connector display name."""
        return "Slack"

    @staticmethod
    def _resolve_token(token: Optional[str] = None) -> Optional[str]:
        """Resolve Slack API token from parameter or environment variables."""
        if token and str(token).strip():
            return str(token).strip()
        env_token = os.environ.get("SLACK_TOKEN") or os.environ.get("SLACK_BOT_TOKEN")
        return env_token.strip() if env_token else None

    @staticmethod
    def _parse_channel_inputs(raw: Optional[Union[List[str], str]]) -> List[str]:
        """Normalize channel list or comma-separated string into cleaned list."""
        if not raw:
            env_val = os.environ.get("SLACK_CHANNELS") or os.environ.get("SLACK_CHANNEL")
            if not env_val:
                return []
            raw = env_val

        if isinstance(raw, str):
            items = [c.strip() for c in raw.split(",") if c.strip()]
        else:
            items = []
            for item in raw:
                if isinstance(item, str) and "," in item:
                    items.extend([c.strip() for c in item.split(",") if c.strip()])
                elif item and str(item).strip():
                    items.append(str(item).strip())

        # Normalize channel names: strip leading '#'
        return [c.lstrip("#") for c in items if c]

    def _get_headers(self) -> Dict[str, str]:
        """Build HTTP request headers including Authorization and User-Agent."""
        headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _api_call(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Perform an HTTP request against the Slack Web API with comprehensive error handling."""
        if not self.token:
            raise SourceError(
                "Slack authentication token missing. Please set SLACK_TOKEN or SLACK_BOT_TOKEN "
                "environment variable, or pass --token."
            )

        url = f"{SLACK_API_BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._get_headers()

        try:
            if method.upper() == "GET":
                resp = self.session.get(url, headers=headers, params=params, timeout=30)
            else:
                resp = self.session.post(url, headers=headers, params=params, json=json_data, timeout=30)
        except Exception as e:
            raise map_slack_error(e, secrets=self._secrets)

        try:
            data = resp.json()
        except Exception:
            raise map_slack_error(
                SourceError(f"Slack API returned non-JSON response (HTTP {resp.status_code})"),
                response=resp,
                secrets=self._secrets,
            )

        if resp.status_code != 200 or not data.get("ok"):
            raise map_slack_error(
                SourceError(f"Slack API error: {data.get('error', 'unknown')}"),
                response=resp,
                json_body=data,
                secrets=self._secrets,
            )

        return data

    def _load_user_cache(self) -> None:
        """Fetch workspace users via users.list to populate user ID -> display name cache."""
        cursor: Optional[str] = None
        seen_cursors: Set[str] = set()

        while True:
            params: Dict[str, Any] = {"limit": 200}
            if cursor:
                params["cursor"] = cursor

            try:
                data = self._api_call("GET", "users.list", params=params)
            except Exception as e:
                logger.warning("Failed to fetch users.list for Slack user resolution: %s", _scrub_secrets(str(e), self._secrets))
                break

            members = data.get("members") or []
            for u in members:
                if isinstance(u, dict):
                    uid = u.get("id")
                    if uid:
                        prof = u.get("profile") or {}
                        disp = (
                            prof.get("display_name")
                            or prof.get("real_name")
                            or u.get("name")
                            or "unknown"
                        )
                        self._user_cache[uid] = disp.lstrip("@")

            meta = data.get("response_metadata") or {}
            next_cursor = (meta.get("next_cursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

    def _resolve_user(self, user_id: Optional[str]) -> str:
        """Resolve a Slack user ID to display name using cache or fallback to users.info."""
        if not user_id:
            return "unknown"
        clean_id = str(user_id).strip()
        if clean_id in self._user_cache:
            return self._user_cache[clean_id]

        # On-demand lookup via users.info
        try:
            data = self._api_call("GET", "users.info", params={"user": clean_id})
            u = data.get("user") or {}
            prof = u.get("profile") or {}
            disp = (
                prof.get("display_name")
                or prof.get("real_name")
                or u.get("name")
                or clean_id
            )
            self._user_cache[clean_id] = disp.lstrip("@")
            return self._user_cache[clean_id]
        except Exception:
            self._user_cache[clean_id] = clean_id
            return clean_id

    def _discover_channels(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """Fetch accessible public and private conversations via conversations.list."""
        by_id: Dict[str, Dict[str, Any]] = {}
        by_name: Dict[str, Dict[str, Any]] = {}
        cursor: Optional[str] = None
        seen_cursors: Set[str] = set()

        while True:
            params: Dict[str, Any] = {
                "types": "public_channel,private_channel",
                "exclude_archived": "false",
                "limit": DEFAULT_CONVERSATIONS_LIST_LIMIT,
            }
            if cursor:
                params["cursor"] = cursor

            data = self._api_call("GET", "conversations.list", params=params)
            channels = data.get("channels") or []

            for ch in channels:
                if isinstance(ch, dict):
                    cid = ch.get("id")
                    cname = ch.get("name")
                    if cid:
                        by_id[cid] = ch
                        if cname:
                            by_name[cname.lstrip("#")] = ch
                            self._channel_cache[cid] = cname.lstrip("#")

            meta = data.get("response_metadata") or {}
            next_cursor = (meta.get("next_cursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return by_id, by_name

    def _resolve_target_channels(
        self, requested: List[str]
    ) -> List[Tuple[str, str]]:
        """Resolve requested channel names/IDs to (channel_id, channel_name) tuples."""
        by_id, by_name = self._discover_channels()
        targets: List[Tuple[str, str]] = []
        seen_ids: Set[str] = set()

        for req in requested:
            clean = req.lstrip("#").strip()
            if not clean:
                continue

            if clean in by_id:
                ch = by_id[clean]
                cid = clean
                cname = ch.get("name", clean)
                if cid not in seen_ids:
                    seen_ids.add(cid)
                    targets.append((cid, cname))
            elif clean in by_name:
                ch = by_name[clean]
                cid = ch.get("id", clean)
                cname = clean
                if cid not in seen_ids:
                    seen_ids.add(cid)
                    targets.append((cid, cname))
            elif re.match(r"^[C|G][A-Z0-9]{8,}$", clean):
                # Channel ID that wasn't in conversations.list (e.g. unlisted private channel)
                if clean not in seen_ids:
                    seen_ids.add(clean)
                    targets.append((clean, clean))
            else:
                logger.warning(
                    "Slack channel '%s' could not be found or is inaccessible.",
                    _scrub_secrets(clean, self._secrets),
                )

        return targets

    def _fetch_channel_messages(
        self, channel_id: str, limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch messages from a channel via conversations.history using cursor pagination."""
        messages: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        seen_cursors: Set[str] = set()
        max_to_fetch = limit if limit is not None else self.default_limit

        while len(messages) < max_to_fetch:
            remaining = max_to_fetch - len(messages)
            batch_limit = min(SLACK_API_MAX_BATCH_SIZE, remaining)
            params: Dict[str, Any] = {
                "channel": channel_id,
                "limit": batch_limit,
            }
            if cursor:
                params["cursor"] = cursor

            data = self._api_call("GET", "conversations.history", params=params)
            batch = data.get("messages") or []

            if not batch:
                break

            for raw_msg in batch:
                if not isinstance(raw_msg, dict):
                    continue

                subtype = raw_msg.get("subtype")
                # Filter noise system subtypes
                if subtype in (
                    "channel_join",
                    "channel_leave",
                    "channel_topic",
                    "channel_purpose",
                    "channel_name",
                    "channel_archive",
                    "channel_unarchive",
                    "group_join",
                    "group_leave",
                    "pinned_item",
                    "unpinned_item",
                ):
                    continue

                # Skip deleted messages
                if subtype == "message_deleted":
                    continue

                # Handle message_changed (edited message)
                if subtype == "message_changed":
                    inner = raw_msg.get("message")
                    if isinstance(inner, dict):
                        msg = dict(inner)
                        # Preserve channel ID if present
                        msg["channel"] = channel_id
                        messages.append(msg)
                    continue

                # Skip threaded replies that appear in channel history when thread_ts != ts
                # (They will be ingested as part of the parent thread)
                t_ts = raw_msg.get("thread_ts")
                m_ts = raw_msg.get("ts")
                if t_ts and m_ts and t_ts != m_ts:
                    continue

                msg = dict(raw_msg)
                msg["channel"] = channel_id
                messages.append(msg)

                if len(messages) >= max_to_fetch:
                    break

            meta = data.get("response_metadata") or {}
            next_cursor = (meta.get("next_cursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        return messages

    def _fetch_thread_replies(
        self, channel_id: str, thread_ts: str, max_replies: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch replies for a thread via conversations.replies using cursor pagination."""
        replies: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        seen_cursors: Set[str] = set()
        limit_target = max_replies if max_replies is not None else self.max_replies

        while len(replies) < limit_target:
            remaining = limit_target - len(replies)
            batch_limit = min(SLACK_API_MAX_BATCH_SIZE, remaining)
            params: Dict[str, Any] = {
                "channel": channel_id,
                "ts": thread_ts,
                "limit": batch_limit,
            }
            if cursor:
                params["cursor"] = cursor

            data = self._api_call("GET", "conversations.replies", params=params)
            batch = data.get("messages") or []

            if not batch:
                break

            for m in batch:
                if not isinstance(m, dict):
                    continue
                # Skip the parent message itself (its ts equals thread_ts)
                if m.get("ts") == thread_ts:
                    continue
                if m.get("subtype") == "message_deleted":
                    continue
                replies.append(m)
                if len(replies) >= limit_target:
                    break

            meta = data.get("response_metadata") or {}
            next_cursor = (meta.get("next_cursor") or "").strip()
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor

        # Chronological sort by ts ascending
        def _ts_key(m: Dict[str, Any]) -> float:
            try:
                return float(str(m.get("ts", 0)))
            except (ValueError, TypeError):
                return 0.0

        replies.sort(key=_ts_key)
        return replies

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch messages and threads from configured Slack channels."""
        # 1. Resolve credentials and options
        token_override = kwargs.get("token")
        if token_override:
            self.token = str(token_override).strip()
            self._secrets = [self.token]

        if not self.token:
            raise SourceError(
                "Slack authentication token missing. Please set SLACK_TOKEN or SLACK_BOT_TOKEN "
                "environment variable, or pass --token."
            )

        channels_arg = kwargs.get("channel") or kwargs.get("channels")
        if channels_arg:
            target_channels = self._parse_channel_inputs(channels_arg)
        else:
            target_channels = list(self.configured_channels)

        if not target_channels:
            raise SourceError(
                "No Slack channels specified. Please provide at least one channel via --channel, "
                "--channels, or the SLACK_CHANNELS environment variable."
            )

        limit = kwargs.get("limit") or kwargs.get("max_messages") or self.default_limit
        max_replies = kwargs.get("max_replies", self.max_replies)
        include_threads = kwargs.get("include_threads", self.include_threads)
        if kwargs.get("no_threads", False):
            include_threads = False

        # 2. Build user resolution cache
        self._load_user_cache()

        # 3. Discover and resolve target channels
        resolved_channels = self._resolve_target_channels(target_channels)
        if not resolved_channels:
            raise SourceError(
                f"None of the configured Slack channels could be found or accessed: {target_channels}"
            )

        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        for channel_id, channel_name in resolved_channels:
            try:
                messages = self._fetch_channel_messages(channel_id, limit=limit)
            except Exception as ce:
                logger.warning(
                    "Failed to fetch messages for Slack channel %s (%s): %s",
                    channel_name,
                    channel_id,
                    _scrub_secrets(str(ce), self._secrets),
                )
                continue

            for msg_data in messages:
                try:
                    ts = msg_data.get("ts")
                    if not ts:
                        continue

                    # Stable Identity: For a root message or thread, use channel_id and root ts
                    root_ts = msg_data.get("thread_ts") or ts
                    source_id = f"slack:message:{channel_id}:{root_ts}"
                    if source_id in seen_source_ids:
                        continue

                    # Fetch thread replies if message has replies and thread ingestion is enabled
                    replies: List[Dict[str, Any]] = []
                    reply_count = msg_data.get("reply_count", 0)
                    if include_threads and reply_count > 0:
                        try:
                            replies = self._fetch_thread_replies(
                                channel_id, root_ts, max_replies=max_replies
                            )
                        except Exception as te:
                            logger.warning(
                                "Failed to fetch thread replies for message %s in channel %s: %s",
                                root_ts,
                                channel_name,
                                _scrub_secrets(str(te), self._secrets),
                            )
                            # Fault isolation: preserve parent message without replies
                            replies = []

                    raw_user_id = msg_data.get("user") or msg_data.get("bot_id") or "unknown"
                    author_name = self._resolve_user(raw_user_id)
                    author_disp = f"@{author_name.lstrip('@')}" if author_name != "unknown" else "unknown"

                    date_iso = format_slack_timestamp(ts)
                    clean_ts_num = str(ts).replace(".", "")
                    permalink = msg_data.get("permalink") or f"https://slack.com/archives/{channel_id}/p{clean_ts_num}"

                    raw_text = msg_data.get("text") or ""
                    norm_text = normalize_slack_text(
                        raw_text, user_cache=self._user_cache, channel_cache=self._channel_cache
                    )

                    # Extract title
                    first_line = ""
                    for line in norm_text.splitlines():
                        if line.strip():
                            first_line = line.strip()
                            break
                    if first_line:
                        clean_title = re.sub(r"^[#>\-\*\s]+", "", first_line).strip()
                        title = clean_title[:80].rstrip() or f"Slack message in #{channel_name}"
                    else:
                        title = f"Slack message from {author_disp} in #{channel_name}"

                    body = build_slack_message_body(
                        msg_data=msg_data,
                        channel_name=channel_name,
                        channel_id=channel_id,
                        replies=replies if include_threads else None,
                        user_cache=self._user_cache,
                        channel_cache=self._channel_cache,
                    )

                    extra_meta: Dict[str, Any] = {
                        "channel": channel_name,
                        "channel_id": channel_id,
                        "author": author_name,
                        "author_id": raw_user_id,
                        "slack_ts": str(ts),
                        "thread_ts": str(root_ts),
                        "reply_count": len(replies) if replies else reply_count,
                    }
                    if msg_data.get("edited"):
                        extra_meta["edited"] = msg_data.get("edited")

                    item = SourceItem(
                        source_id=source_id,
                        title=title,
                        source_type="slack",
                        content=body,
                        author=author_name,
                        date=date_iso,
                        source_url=permalink,
                        tags=["ingested", "slack", "social"],
                        summary=norm_text[:200].strip() or None,
                        extra_metadata=extra_meta,
                    )

                    seen_source_ids.add(source_id)
                    items.append(item)

                except Exception as me:
                    logger.warning(
                        "Skipping malformed Slack message: %s",
                        _scrub_secrets(str(me), self._secrets),
                    )
                    continue

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a SourceItem into frontmatter metadata and Markdown body targeted at Ingested/Social/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Social"
        return note


# Register connector
SourceRegistry.register("slack", SlackSource)
