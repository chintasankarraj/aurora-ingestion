"""Reddit source connector for Aurora External Data Ingestion Pipeline.

Connects to the official Reddit REST API using OAuth2 Application-Only
authentication (client_credentials) or explicit Bearer tokens. Retrieves
posts and comments from configured subreddits and converts each post into
a clean Markdown note under Ingested/Social/ with YAML frontmatter,
attribution block, post body, metadata, and chronologically ordered comments.

API and Authentication Contract:
- Authentication: OAuth2 application-only flow (grant_type=client_credentials)
  using HTTP Basic Authentication (client_id:client_secret) against
  https://www.reddit.com/api/v1/access_token, or direct Bearer token.
- API Base URL: https://oauth.reddit.com
- Environment Variables:
    REDDIT_CLIENT_ID: OAuth2 application client ID
    REDDIT_CLIENT_SECRET: OAuth2 application client secret
    REDDIT_USER_AGENT: Custom descriptive User-Agent (required by Reddit)
    REDDIT_ACCESS_TOKEN / REDDIT_TOKEN: Optional direct Bearer token
    REDDIT_SUBREDDITS / REDDIT_SUBREDDIT: Optional comma-separated subreddit list
- Endpoints:
    POST https://www.reddit.com/api/v1/access_token (token endpoint)
    GET  https://oauth.reddit.com/r/{subreddit}/{listing}.json (posts listing)
    GET  https://oauth.reddit.com/r/{subreddit}/comments/{post_id}.json (comments tree)
- Rate-limiting: Header tracking (x-ratelimit-remaining, x-ratelimit-reset, x-ratelimit-used).
  HTTP 429 mapped to descriptive SourceError with reset time.
- Pagination: Cursor-based pagination using the 'after' fullname token with
  cycle/loop detection and maximum page boundaries.
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
import requests.auth
from markdownify import markdownify as md

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

# Fallback default User-Agent conforming to Reddit API rules
DEFAULT_USER_AGENT = "aurora-ingestion:v1.0.0 (by /u/aurora-bot)"

# Regex patterns for HTML detection
_HTML_DECLARATION_PATTERN = re.compile(r"<!(?:DOCTYPE|ENTITY|\[CDATA\[)", re.IGNORECASE)
_HTML_CLOSING_TAG_PATTERN = re.compile(
    r"</\s*(?:p|div|span|h[1-6]|ul|ol|li|table|thead|tbody|tr|th|td|a|em|strong|b|i|blockquote|pre|code|form|input|button|header|footer|nav|section|article|aside|figure|figcaption)\b",
    re.IGNORECASE,
)
_HTML_VOID_TAGS_PATTERN = re.compile(
    r"<\s*(?:img|br|hr|input|meta|link)\b[^>]*\/?>",
    re.IGNORECASE,
)
_HTML_SCRIPT_STYLE_PATTERN = re.compile(
    r"<\s*(?:script|style)\b",
    re.IGNORECASE,
)
_HTML_ATTR_TAG_PATTERN = re.compile(
    r"<\s*[a-zA-Z][a-zA-Z0-9-]*\b[^>]*\s+(?:href|src|class|id|style|title|alt|target|rel|width|height|data-[a-zA-Z0-9-]+)\s*=\s*['\"][^'\"]*['\"][^>]*>",
    re.IGNORECASE,
)
_HTML_OPENING_TAG_PATTERN = re.compile(
    r"<\s*(?:p|div|span|h[1-6]|ul|ol|li|table|thead|tbody|tr|th|td|blockquote|pre|form|header|footer|nav|section|article|aside|figure|figcaption)\b(?:\s+[^>]*)?>",
    re.IGNORECASE,
)


def _scrub_secrets(text: str, secrets: List[Optional[str]]) -> str:
    """Scrub sensitive credentials from error messages or logs."""
    scrubbed = str(text)
    for s in secrets:
        if s and len(str(s).strip()) >= 3:
            scrubbed = scrubbed.replace(str(s).strip(), "[REDACTED]")
    return scrubbed


def map_reddit_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map Reddit HTTP/API exceptions to descriptive SourceError without exposing credentials."""
    if response is None and isinstance(exc, SourceError):
        return exc

    status_code = getattr(response, "status_code", None)
    err_body = ""
    rate_remaining = None
    rate_reset = None

    if response is not None:
        try:
            err_json = response.json()
            err_body = err_json.get("message") or err_json.get("error") or ""
        except Exception:
            err_body = response.text[:200]
        rate_remaining = response.headers.get("x-ratelimit-remaining")
        rate_reset = response.headers.get("x-ratelimit-reset")

    sanitized_detail = _scrub_secrets(err_body, secrets or [])

    if status_code == 401:
        return SourceError(
            "Reddit authentication failed: Invalid credentials or expired access token (HTTP 401)."
        )

    if status_code == 403:
        return SourceError(
            f"Reddit access forbidden (HTTP 403): Subreddit may be private or credentials lack permissions. {sanitized_detail}".strip()
        )

    if status_code == 404:
        return SourceError("Reddit resource or subreddit not found (HTTP 404).")

    if status_code == 429:
        msg = "Reddit API rate limit exceeded. Please wait before retrying (HTTP 429)."
        if rate_reset:
            try:
                reset_sec = int(float(rate_reset))
                msg += f" Rate limit resets in {reset_sec}s."
            except (ValueError, TypeError):
                pass
        return SourceError(msg)

    if status_code is not None and status_code >= 500:
        return SourceError(
            f"Reddit server error (HTTP {status_code}): Service temporarily unavailable."
        )

    if status_code is not None:
        msg = (
            f"Reddit API error (HTTP {status_code}): {sanitized_detail}"
            if sanitized_detail
            else f"Reddit API error (HTTP {status_code})."
        )
        return SourceError(msg)

    if isinstance(exc, requests.exceptions.Timeout):
        return SourceError("Reddit API request timed out.")
    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError("Network connection failed while connecting to Reddit API.")

    return SourceError(f"Reddit connector error: {_scrub_secrets(str(exc), secrets or [])}")


def normalize_subreddit_name(raw: str) -> str:
    """Validate and normalize a subreddit string into a clean subreddit name.

    Accepts formats such as 'programming', 'r/programming', '/r/programming/',
    strips whitespace and slashes, and validates against allowed Reddit characters.
    """
    cleaned = str(raw).strip().strip("/")
    if cleaned.lower().startswith("r/"):
        cleaned = cleaned[2:].strip("/")

    if not cleaned:
        raise SourceError("Subreddit name cannot be empty.")

    if not re.match(r"^[a-zA-Z0-9_]{2,50}$", cleaned):
        raise SourceError(
            f"Invalid subreddit name: '{raw}'. Subreddit names must contain only letters, numbers, or underscores (2-50 chars)."
        )

    return cleaned


def derive_reddit_post_id(post_id: Any) -> str:
    """Derive an immutable, stable source ID for a Reddit post."""
    if not post_id or not str(post_id).strip():
        raise SourceError("Cannot establish Reddit post source ID: Missing post ID.")
    cleaned = str(post_id).strip()
    if cleaned.startswith("t3_"):
        cleaned = cleaned[3:]
    if not cleaned:
        raise SourceError("Cannot establish Reddit post source ID: Empty post ID.")
    return f"reddit:post:{cleaned}"


def format_reddit_timestamp(utc_val: Any) -> str:
    """Convert a UTC timestamp (seconds) into ISO 8601 string."""
    if not utc_val:
        return datetime.now(timezone.utc).isoformat()
    try:
        ts = float(utc_val)
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return str(utc_val)


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


def normalize_reddit_content(content: Any) -> str:
    """Normalize Reddit content into clean Obsidian-compatible Markdown without raw HTML.

    - Unescapes standard HTML entities commonly returned by Reddit API (e.g. &gt;, &lt;, &amp;).
    - Preserves plain text, mathematical comparisons ('<' and '>'), headings, links,
      tables, lists, and code blocks.
    - If content contains HTML markup, converts it to clean Markdown via markdownify,
      stripping <script> and <style> elements and their internal contents.
    - Code fences and inline backticks are preserved without corruption.
    - Ensures generated post descriptions and comments contain no raw HTML tags.
    """
    if content is None:
        return ""

    raw = str(content).strip()
    if not raw:
        return ""

    # Unescape common Reddit entities
    raw = html.unescape(raw)
    raw = raw.replace("\u200b", "").strip()

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


def extract_reddit_comments(
    children: List[Dict[str, Any]],
    depth: int = 0,
    parent_author: Optional[str] = None,
    collected: Optional[List[Dict[str, Any]]] = None,
    max_comments: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Traverse and extract comments from Reddit API comment tree up to max_comments."""
    if collected is None:
        collected = []

    for child in children:
        if max_comments is not None and len(collected) >= max_comments:
            break

        if not isinstance(child, dict):
            continue

        kind = child.get("kind")
        if kind != "t1":
            # Skip 'more' or other non-comment placeholder objects
            continue

        c_data = child.get("data")
        if not isinstance(c_data, dict):
            continue

        c_id = c_data.get("id")
        if not c_id:
            continue

        raw_author = c_data.get("author")
        author = str(raw_author).strip() if raw_author else "[deleted]"

        created_utc = c_data.get("created_utc") or 0.0
        try:
            created_float = float(created_utc)
        except (ValueError, TypeError):
            created_float = 0.0

        score = c_data.get("score")
        try:
            score_int = int(score) if score is not None else 0
        except (ValueError, TypeError):
            score_int = 0

        raw_body = c_data.get("body") or ""
        body = normalize_reddit_content(raw_body)

        comment_record: Dict[str, Any] = {
            "id": str(c_id),
            "author": author,
            "created_utc": created_float,
            "created_iso": format_reddit_timestamp(created_float),
            "score": score_int,
            "body": body,
            "permalink": c_data.get("permalink") or "",
            "parent_id": str(c_data.get("parent_id") or ""),
            "depth": depth,
            "parent_author": parent_author,
        }
        collected.append(comment_record)

        # Process nested replies
        replies = c_data.get("replies")
        if isinstance(replies, dict):
            reply_children = replies.get("data", {}).get("children", [])
            if isinstance(reply_children, list) and reply_children:
                extract_reddit_comments(
                    reply_children,
                    depth=depth + 1,
                    parent_author=author,
                    collected=collected,
                    max_comments=max_comments,
                )

    return collected


def format_reddit_comments(comments: List[Dict[str, Any]]) -> str:
    """Format extracted Reddit comments into clean, chronologically ordered Markdown.

    - Orders top-level comments chronologically by created_utc ascending, then id ascending.
    - Places replies under their parent thread formatted as readable blockquotes with
      reply attribution (e.g. > **↳ @user** — date (Score: X) *(reply to @parent)*:).
    - Avoids deep excessive nesting while preserving thread context.
    - Handles deleted authors and empty/deleted comment bodies gracefully.
    """
    if not comments:
        return ""

    def _sort_key(c: Dict[str, Any]) -> Tuple[float, str]:
        return (c.get("created_utc", 0.0), str(c.get("id", "")))

    # Identify top-level comments vs replies
    top_level: List[Dict[str, Any]] = []
    replies_by_parent: Dict[str, List[Dict[str, Any]]] = {}

    for c in comments:
        parent_id = c.get("parent_id", "")
        # A comment is top-level if depth == 0 or parent_id starts with t3_ (link/post)
        if c.get("depth", 0) == 0 or parent_id.startswith("t3_"):
            top_level.append(c)
        else:
            # Strip prefix if needed
            p_key = parent_id[3:] if parent_id.startswith("t1_") else parent_id
            replies_by_parent.setdefault(p_key, []).append(c)

    # If no comments were identified as top-level (e.g. flat list without parent_id), treat all as top-level
    if not top_level:
        top_level = list(comments)

    top_level.sort(key=_sort_key)

    formatted_blocks: List[str] = []

    def _render_comment_body(body_str: str) -> str:
        b = body_str.strip()
        if not b:
            return "*No content.*"
        if b in ("[deleted]", "[removed]"):
            return f"*{b}*"
        return b

    def _render_replies_recursive(parent_id: str, current_parent_author: str) -> List[str]:
        reply_list = replies_by_parent.get(parent_id, [])
        reply_list.sort(key=_sort_key)
        rendered_replies: List[str] = []

        for r in reply_list:
            r_author = r.get("author") or "[deleted]"
            r_author_display = f"@{r_author}" if r_author != "[deleted]" else "@deleted"
            r_date_str = r.get("created_iso", "")[:10] or "unknown"
            r_score = r.get("score", 0)
            score_text = f" (Score: {r_score})" if r_score != 0 else ""

            target_parent = r.get("parent_author") or current_parent_author
            target_display = f"@{target_parent}" if target_parent != "[deleted]" else "@deleted"
            reply_notice = f" *(reply to {target_display})*" if target_parent else ""

            r_body = _render_comment_body(r.get("body", ""))
            # Prefix each line of reply body with blockquote '> '
            quoted_body_lines = [f"> {line}" if line.strip() else ">" for line in r_body.splitlines()]
            quoted_body = "\n".join(quoted_body_lines)

            r_header = f"> **↳ {r_author_display}** — {r_date_str}{score_text}{reply_notice}:"
            rendered_replies.append(f"{r_header}\n>\n{quoted_body}")

            # Sub-replies to this comment
            sub = _render_replies_recursive(r.get("id", ""), r_author)
            rendered_replies.extend(sub)

        return rendered_replies

    for tl in top_level:
        tl_author = tl.get("author") or "[deleted]"
        tl_author_display = f"@{tl_author}" if tl_author != "[deleted]" else "@deleted"
        tl_date_str = tl.get("created_iso", "")[:10] or "unknown"
        tl_score = tl.get("score", 0)
        score_text = f" (Score: {tl_score})" if tl_score != 0 else ""

        tl_body = _render_comment_body(tl.get("body", ""))
        block_parts: List[str] = [
            f"### {tl_author_display} — {tl_date_str}{score_text}\n\n{tl_body}"
        ]

        # Attach replies
        replies = _render_replies_recursive(tl.get("id", ""), tl_author)
        if replies:
            block_parts.append("\n\n".join(replies))

        formatted_blocks.append("\n\n".join(block_parts))

    return "\n\n".join(formatted_blocks)


def is_safe_http_url(url: Optional[str]) -> bool:
    """Validate that a URL has an http or https scheme and is safe to render as a Markdown link."""
    if not url or not str(url).strip():
        return False
    try:
        parsed = urlparse(str(url).strip())
        return parsed.scheme.lower() in ("http", "https")
    except Exception:
        return False


def build_reddit_post_body(
    post_data: Dict[str, Any],
    comments: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Construct full Obsidian-compatible Markdown document body for a Reddit post."""
    raw_title = (post_data.get("title") or "Untitled").strip()
    title = normalize_reddit_content(raw_title) or "Untitled"
    subreddit = (post_data.get("subreddit") or "").strip()
    sub_display = f"r/{subreddit}" if subreddit else "Reddit"
    permalink = post_data.get("permalink") or ""
    clean_permalink = re.sub(r"<[^>]+>", "", permalink)
    source_url = f"https://www.reddit.com{clean_permalink}" if clean_permalink.startswith("/") else clean_permalink
    author = (post_data.get("author") or "[deleted]").strip()
    author_display = f"u/{author}" if author != "[deleted]" else "[deleted]"
    score = post_data.get("score", 0)
    num_comments = post_data.get("num_comments", 0)
    created_utc = post_data.get("created_utc")
    created_iso = format_reddit_timestamp(created_utc)
    date_str = created_iso[:10] if len(created_iso) >= 10 else created_iso

    is_self = post_data.get("is_self", True)
    ext_url = (post_data.get("url") or "").strip()
    raw_selftext = post_data.get("selftext") or ""
    selftext = normalize_reddit_content(raw_selftext)

    # 1. H1 Header & Attribution
    source_link = f"[{sub_display}]({source_url})" if source_url else sub_display
    sections: List[str] = [
        f"# {title}",
        f"> **Source**: [Reddit — {source_link}]\n"
        f"> **Author**: {author_display} · **Score**: {score} · **Comments**: {num_comments} · **Created**: {date_str}",
    ]

    # 2. Post Content
    post_parts: List[str] = []
    # If link post
    if not is_self and ext_url and ext_url != source_url:
        if is_safe_http_url(ext_url):
            post_parts.append(f"**Link**: [{ext_url}]({ext_url})")
        else:
            post_parts.append(f"**Link**: `{ext_url}`")

    if selftext:
        if selftext in ("[deleted]", "[removed]"):
            post_parts.append(f"*{selftext}*")
        else:
            post_parts.append(selftext)
    elif is_self:
        post_parts.append("*No content.*")

    if post_parts:
        sections.append("## Post\n\n" + "\n\n".join(post_parts))

    # 3. Metadata Section
    meta_bullets: List[str] = [
        f"- **Subreddit:** {sub_display}",
        f"- **Author:** {author_display}",
        f"- **Score:** {score}",
        f"- **Comments:** {num_comments}",
        f"- **Created:** {created_iso}",
    ]

    flair = post_data.get("link_flair_text")
    if flair:
        meta_bullets.append(f"- **Flair:** {flair.strip()}")

    edited = post_data.get("edited")
    if edited and edited is not True:
        meta_bullets.append(f"- **Edited:** {format_reddit_timestamp(edited)}")
    elif edited is True:
        meta_bullets.append("- **Edited:** yes")

    if not is_self and ext_url and ext_url != source_url:
        if is_safe_http_url(ext_url):
            meta_bullets.append(f"- **External URL:** [{ext_url}]({ext_url})")
        else:
            meta_bullets.append(f"- **External URL:** `{ext_url}`")

    sections.append("## Metadata\n\n" + "\n".join(meta_bullets))

    # 4. Comments Section
    if comments:
        formatted_comments = format_reddit_comments(comments)
        if formatted_comments.strip():
            sections.append(f"## Comments\n\n{formatted_comments}")

    return "\n\n".join(sections) + "\n"


class RedditSource(BaseSource):
    """Reddit source connector for retrieving posts and comments from subreddits."""

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        user_agent: Optional[str] = None,
        token: Optional[str] = None,
        subreddits: Optional[Union[List[str], str]] = None,
        session: Optional[requests.Session] = None,
        base_url: str = "https://oauth.reddit.com",
        auth_url: str = "https://www.reddit.com/api/v1/access_token",
        limit: Optional[int] = None,
        max_comments: int = 50,
        include_comments: bool = True,
        listing: str = "hot",
    ) -> None:
        super().__init__()
        self._client_id = client_id
        self._client_secret = client_secret
        self._user_agent = user_agent
        self._token = token
        self._subreddits = self._parse_subreddit_list(subreddits)
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._auth_url = auth_url.rstrip("/")
        self._limit = limit
        self._max_comments = max_comments
        self._include_comments = include_comments
        self._listing = listing or "hot"

    @property
    def source_type(self) -> str:
        return "reddit"

    @property
    def display_name(self) -> str:
        return "Reddit"

    @staticmethod
    def _parse_subreddit_list(subs: Optional[Union[List[str], str]]) -> List[str]:
        """Parse subreddits from a string, list, or comma-separated string."""
        if not subs:
            return []
        result: List[str] = []
        if isinstance(subs, str):
            for part in subs.split(","):
                part = part.strip()
                if part:
                    result.append(normalize_subreddit_name(part))
        elif isinstance(subs, (list, tuple, set)):
            for item in subs:
                if isinstance(item, str):
                    for part in item.split(","):
                        part = part.strip()
                        if part:
                            result.append(normalize_subreddit_name(part))

        # Deduplicate while preserving order case-insensitively
        deduped: List[str] = []
        seen: Set[str] = set()
        for s in result:
            key = s.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(s)
        return deduped

    def _get_session(self) -> Any:
        """Return injected HTTP session or a new requests.Session."""
        if self._session is not None:
            return self._session
        return requests.Session()

    def _resolve_credentials(
        self, **kwargs: Any
    ) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
        """Resolve Reddit authentication parameters (token or client_id/client_secret)."""
        token = (
            kwargs.get("token")
            or kwargs.get("access_token")
            or self._token
            or os.environ.get("REDDIT_ACCESS_TOKEN")
            or os.environ.get("REDDIT_TOKEN")
        )
        client_id = (
            kwargs.get("client_id")
            or self._client_id
            or os.environ.get("REDDIT_CLIENT_ID")
        )
        client_secret = (
            kwargs.get("client_secret")
            or self._client_secret
            or os.environ.get("REDDIT_CLIENT_SECRET")
        )
        user_agent = (
            kwargs.get("user_agent")
            or self._user_agent
            or os.environ.get("REDDIT_USER_AGENT")
            or DEFAULT_USER_AGENT
        )

        return (
            str(token).strip() if token else None,
            str(client_id).strip() if client_id else None,
            str(client_secret).strip() if client_secret else None,
            str(user_agent).strip(),
        )

    def _obtain_oauth_token(
        self,
        session: Any,
        client_id: str,
        client_secret: str,
        user_agent: str,
        secrets: List[Optional[str]],
    ) -> str:
        """Obtain OAuth2 application-only Bearer token via client_credentials flow."""
        auth = requests.auth.HTTPBasicAuth(client_id, client_secret)
        headers = {"User-Agent": user_agent}
        data = {"grant_type": "client_credentials"}

        try:
            resp = session.post(
                self._auth_url,
                auth=auth,
                data=data,
                headers=headers,
                timeout=15,
            )
        except Exception as e:
            raise map_reddit_error(e, secrets=secrets)

        if resp.status_code != 200:
            raise map_reddit_error(
                SourceError(f"OAuth token request failed with status {resp.status_code}"),
                response=resp,
                secrets=secrets,
            )

        try:
            body = resp.json()
        except Exception as e:
            raise SourceError(f"Malformed JSON in Reddit OAuth token response: {e}")

        if not isinstance(body, dict):
            raise SourceError("Unexpected response format from Reddit OAuth endpoint.")

        if "error" in body:
            err_msg = body.get("error", "Unknown error")
            raise SourceError(
                f"Reddit OAuth authentication error: {_scrub_secrets(err_msg, secrets)}"
            )

        token = body.get("access_token")
        if not token:
            raise SourceError("Reddit OAuth response did not contain an access_token.")

        return str(token).strip()

    def _fetch_subreddit_posts(
        self,
        session: Any,
        subreddit: str,
        headers: Dict[str, str],
        secrets: List[Optional[str]],
        listing: str = "hot",
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Paginate and fetch posts for a subreddit."""
        url = f"{self._base_url}/r/{subreddit}/{listing}.json"
        collected: List[Dict[str, Any]] = []
        seen_cursors: Set[str] = set()
        seen_post_ids: Set[str] = set()
        after: Optional[str] = None
        max_pages = 50

        for _ in range(max_pages):
            if limit is not None and len(collected) >= limit:
                break

            params: Dict[str, Any] = {"raw_json": 1}
            if limit is not None:
                params["limit"] = min(limit - len(collected), 100)
            else:
                params["limit"] = 100

            if after:
                params["after"] = after

            try:
                resp = session.get(url, headers=headers, params=params, timeout=15)
            except Exception as e:
                raise map_reddit_error(e, secrets=secrets)

            if resp.status_code != 200:
                raise map_reddit_error(
                    SourceError(f"Failed to fetch posts from r/{subreddit}"),
                    response=resp,
                    secrets=secrets,
                )

            try:
                data = resp.json()
            except Exception as e:
                raise SourceError(f"Malformed JSON in Reddit r/{subreddit} response: {e}")

            if not isinstance(data, dict):
                raise SourceError(f"Unexpected response format from Reddit r/{subreddit}.")

            listing_data = data.get("data", {})
            children = listing_data.get("children", [])
            after = listing_data.get("after")

            if not children:
                break

            # Cursor loop protection
            if after and after in seen_cursors:
                logger.warning(
                    "Repeated pagination cursor '%s' detected for r/%s. Stopping pagination.",
                    after,
                    subreddit,
                )
                break
            if after:
                seen_cursors.add(after)

            for child in children:
                if not isinstance(child, dict):
                    continue
                c_data = child.get("data", {})
                if not isinstance(c_data, dict):
                    continue

                # Filter advertisements / promoted items
                if c_data.get("promoted") is True:
                    continue

                post_id = c_data.get("id")
                if not post_id or post_id in seen_post_ids:
                    continue

                seen_post_ids.add(post_id)
                collected.append(c_data)

                if limit is not None and len(collected) >= limit:
                    break

            if not after:
                break

        return collected

    def _fetch_post_comments(
        self,
        session: Any,
        subreddit: str,
        post_id: str,
        headers: Dict[str, str],
        secrets: List[Optional[str]],
        max_comments: int = 50,
    ) -> List[Dict[str, Any]]:
        """Fetch comments for a Reddit post."""
        url = f"{self._base_url}/r/{subreddit}/comments/{post_id}.json"
        params: Dict[str, Any] = {"sort": "old", "raw_json": 1}

        try:
            resp = session.get(url, headers=headers, params=params, timeout=15)
        except Exception as e:
            raise map_reddit_error(e, secrets=secrets)

        if resp.status_code != 200:
            raise map_reddit_error(
                SourceError(f"Failed to fetch comments for post {post_id}"),
                response=resp,
                secrets=secrets,
            )

        try:
            data = resp.json()
        except Exception as e:
            raise SourceError(f"Malformed JSON in Reddit comments for post {post_id}: {e}")

        if not isinstance(data, list) or len(data) < 2:
            return []

        comments_listing = data[1]
        if not isinstance(comments_listing, dict):
            return []

        children = comments_listing.get("data", {}).get("children", [])
        if not isinstance(children, list):
            return []

        return extract_reddit_comments(children, max_comments=max_comments)

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch posts and comments from configured subreddits."""
        token, client_id, client_secret, user_agent = self._resolve_credentials(**kwargs)
        secrets: List[Optional[str]] = [token, client_id, client_secret]

        session = self._get_session()

        # If token not directly provided, authenticate via client_credentials
        if not token:
            if not client_id or not client_secret:
                raise SourceError(
                    "Reddit credentials missing. Configure REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET "
                    "(or REDDIT_ACCESS_TOKEN) in your environment or via CLI."
                )
            token = self._obtain_oauth_token(
                session=session,
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
                secrets=secrets,
            )
            secrets.append(token)

        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": user_agent,
            "Accept": "application/json",
        }

        # Resolve subreddits to fetch
        cli_subs = kwargs.get("subreddits") or kwargs.get("subreddit")
        subs_to_fetch = self._parse_subreddit_list(cli_subs) if cli_subs else list(self._subreddits)

        if not subs_to_fetch:
            env_subs = os.environ.get("REDDIT_SUBREDDITS") or os.environ.get("REDDIT_SUBREDDIT")
            if env_subs:
                subs_to_fetch = self._parse_subreddit_list(env_subs)

        if not subs_to_fetch:
            raise SourceError(
                "No subreddits specified. Configure subreddits via --subreddit, --subreddits, "
                "or REDDIT_SUBREDDITS environment variable."
            )

        # Options
        limit = kwargs.get("limit", self._limit)
        limit_val = int(limit) if limit is not None else None
        max_comments = int(kwargs.get("max_comments", self._max_comments))
        include_comments = bool(kwargs.get("include_comments", self._include_comments))
        listing = str(kwargs.get("listing", self._listing)).strip().lower()

        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        for sub in subs_to_fetch:
            try:
                posts = self._fetch_subreddit_posts(
                    session=session,
                    subreddit=sub,
                    headers=headers,
                    secrets=secrets,
                    listing=listing,
                    limit=limit_val,
                )
            except Exception as e:
                mapped_err = map_reddit_error(e, secrets=secrets)
                err_msg = str(mapped_err).lower()
                # Stop on authentication failures, access forbidden, or rate limit exhaustion
                if "authentication failed" in err_msg or "access forbidden" in err_msg or "rate limit" in err_msg:
                    raise mapped_err
                logger.error("Failed to fetch posts for subreddit 'r/%s': %s", sub, mapped_err)
                continue

            for post_data in posts:
                try:
                    post_id = post_data.get("id")
                    if not post_id:
                        continue

                    source_id = derive_reddit_post_id(post_id)
                    if source_id in seen_source_ids:
                        continue

                    # Fetch comments with fault isolation
                    comments: List[Dict[str, Any]] = []
                    if include_comments and max_comments > 0:
                        try:
                            comments = self._fetch_post_comments(
                                session=session,
                                subreddit=sub,
                                post_id=str(post_id),
                                headers=headers,
                                secrets=secrets,
                                max_comments=max_comments,
                            )
                        except Exception as ce:
                            logger.warning(
                                "Failed to fetch comments for Reddit post '%s' in r/%s: %s",
                                post_id,
                                sub,
                                _scrub_secrets(str(ce), secrets),
                            )
                            comments = []

                    raw_title = (post_data.get("title") or f"Reddit Post {post_id}").strip()
                    title = normalize_reddit_content(raw_title) or f"Reddit Post {post_id}"
                    author = post_data.get("author") or "[deleted]"
                    created_utc = post_data.get("created_utc")
                    created_iso = format_reddit_timestamp(created_utc)
                    permalink = post_data.get("permalink") or ""
                    source_url = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
                    score = post_data.get("score", 0)
                    num_comments = post_data.get("num_comments", 0)
                    flair = post_data.get("link_flair_text")
                    is_self = post_data.get("is_self", True)
                    ext_url = post_data.get("url") if not is_self else None

                    body = build_reddit_post_body(post_data, comments=comments)

                    extra_meta: Dict[str, Any] = {
                        "subreddit": sub,
                        "reddit_id": str(post_id),
                        "score": score,
                        "comment_count": num_comments,
                        "item_type": "post",
                    }
                    if flair:
                        extra_meta["flair"] = str(flair).strip()
                    if ext_url:
                        extra_meta["external_url"] = str(ext_url).strip()
                    if post_data.get("edited"):
                        extra_meta["edited"] = post_data.get("edited")

                    item = SourceItem(
                        source_id=source_id,
                        title=title,
                        source_type="reddit",
                        content=body,
                        author=author,
                        date=created_iso,
                        source_url=source_url,
                        tags=["ingested", "reddit", "social"],
                        summary=normalize_reddit_content(post_data.get("selftext") or "")[:200].strip() or None,
                        extra_metadata=extra_meta,
                    )

                    seen_source_ids.add(source_id)
                    items.append(item)

                except Exception as pe:
                    logger.warning("Skipping malformed Reddit post: %s", _scrub_secrets(str(pe), secrets))
                    continue

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a SourceItem into frontmatter metadata and Markdown body."""
        return self.default_item_to_note(item)


# Register connector
SourceRegistry.register("reddit", RedditSource)
