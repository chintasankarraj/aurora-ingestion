"""GitHub source connector for Aurora External Data Ingestion Pipeline.

Connects to the official GitHub REST API to retrieve repository issues
and user gists. Converts each issue and gist into a clean Markdown note
under Ingested/Code/ with YAML frontmatter, attribution block, description,
comments, and GitHub metadata.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from markdownify import markdownify as md

from exceptions import SourceError
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)


def _scrub_secrets(text: str, secrets: List[Optional[str]]) -> str:
    """Scrub sensitive credentials from error messages or logs."""
    scrubbed = str(text)
    for s in secrets:
        if s and len(str(s).strip()) >= 3:
            scrubbed = scrubbed.replace(str(s).strip(), "[REDACTED]")
    return scrubbed


def map_github_error(
    exc: Exception,
    response: Optional[requests.Response] = None,
    secrets: Optional[List[Optional[str]]] = None,
) -> SourceError:
    """Map GitHub HTTP/API exceptions to descriptive SourceError exceptions without exposing credentials."""
    if response is None and isinstance(exc, SourceError):
        return exc

    status_code = getattr(response, "status_code", None)
    err_body = ""
    rate_remaining = None
    rate_reset = None

    if response is not None:
        try:
            err_json = response.json()
            err_body = err_json.get("message", "")
        except Exception:
            err_body = response.text[:200]
        rate_remaining = response.headers.get("X-RateLimit-Remaining")
        rate_reset = response.headers.get("X-RateLimit-Reset")

    sanitized_detail = _scrub_secrets(err_body, secrets or [])

    if status_code == 401:
        return SourceError("GitHub authentication failed: Invalid or expired token (HTTP 401).")

    if status_code in (403, 429):
        is_rate_limit = (
            rate_remaining == "0"
            or "rate limit" in sanitized_detail.lower()
            or "secondary rate limit" in sanitized_detail.lower()
        )
        if is_rate_limit:
            msg = "GitHub API rate limit exceeded. Please wait before retrying (HTTP 403/429)."
            if rate_reset:
                try:
                    reset_time = datetime.fromtimestamp(int(rate_reset), tz=timezone.utc).isoformat()
                    msg += f" Rate limit resets at {reset_time}."
                except (ValueError, TypeError, OSError):
                    pass
            return SourceError(msg)
        return SourceError(
            f"GitHub access forbidden (HTTP 403): {sanitized_detail or 'Insufficient permissions.'}"
        )

    if status_code == 404:
        return SourceError("GitHub repository or resource not found (HTTP 404).")

    if status_code is not None:
        msg = (
            f"GitHub API error (HTTP {status_code}): {sanitized_detail}"
            if sanitized_detail
            else f"GitHub API error (HTTP {status_code})."
        )
        return SourceError(msg)

    if isinstance(exc, requests.exceptions.Timeout):
        return SourceError("GitHub API request timed out.")
    if isinstance(exc, requests.exceptions.ConnectionError):
        return SourceError("Network connection failed while connecting to GitHub API.")

    return SourceError(f"GitHub connector error: {_scrub_secrets(str(exc), secrets or [])}")


def normalize_repo_name(raw: str) -> str:
    """Validate and normalize a GitHub repository string into 'owner/repo' format."""
    cleaned = str(raw).strip().strip("/")
    if not cleaned:
        raise SourceError("Repository name cannot be empty.")
    parts = cleaned.split("/")
    if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
        raise SourceError(
            f"Invalid GitHub repository format: '{raw}'. Expected 'owner/repository'."
        )
    return f"{parts[0].strip()}/{parts[1].strip()}"


def derive_github_issue_id(repo: str, issue_number: Any) -> str:
    """Derive an immutable, stable source ID for a GitHub issue."""
    if not repo or not str(repo).strip():
        raise SourceError("Cannot establish issue source ID: Missing repository name.")
    if issue_number is None or not str(issue_number).strip():
        raise SourceError("Cannot establish issue source ID: Missing issue number.")
    return f"github:issue:{str(repo).strip()}#{str(issue_number).strip()}"


def derive_github_gist_id(gist_id: Any) -> str:
    """Derive an immutable, stable source ID for a GitHub gist."""
    if gist_id is None or not str(gist_id).strip():
        raise SourceError("Cannot establish gist source ID: Missing gist ID.")
    return f"github:gist:{str(gist_id).strip()}"


def normalize_github_tags(raw_labels: Optional[List[Any]]) -> List[str]:
    """Extract and normalize tags from GitHub labels, enforcing ingested, github, and code."""
    tags: List[str] = ["ingested", "github", "code"]
    if not raw_labels:
        return tags

    for item in raw_labels:
        label_name = ""
        if isinstance(item, dict):
            label_name = str(item.get("name") or "")
        elif isinstance(item, str):
            label_name = item
        clean = label_name.strip().lstrip("#").strip().lower()
        # Sanitize whitespace to hyphens
        clean = re.sub(r"\s+", "-", clean)
        # Strip forbidden punctuation
        clean = re.sub(r"[^\w-]", "", clean)
        if clean and clean not in tags:
            tags.append(clean)

    return tags


# ---------------------------------------------------------------------------
# Content Normalization & HTML Detection Helpers
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


def normalize_github_content(content: Any) -> str:
    """Normalize GitHub content into clean Obsidian-compatible Markdown without raw HTML.

    - Preserves plain text, mathematical comparisons ('<' and '>'), headings, links,
      tables, lists, and code blocks.
    - If content contains HTML markup, converts it to clean Markdown via markdownify,
      stripping <script> and <style> elements and their internal contents.
    - Code fences and inline backticks are preserved without corruption.
    - Ensures generated issue descriptions, comments, and gist descriptions contain no raw HTML tags.
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


def format_code_fence(code: str, language: Optional[str] = None) -> str:
    """Format code with a dynamic backtick fence to prevent accidental fence breakout."""
    lang_tag = (language or "").strip().lower()
    backtick_matches = re.findall(r"`+", code)
    max_backticks = max([len(m) for m in backtick_matches], default=0)
    fence_len = max(3, max_backticks + 1)
    fence = "`" * fence_len
    return f"{fence}{lang_tag}\n{code}\n{fence}"


def build_github_issue_body(
    issue_data: Dict[str, Any],
    comments: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Build clean Obsidian-compatible Markdown body for a GitHub issue without raw HTML."""
    sections: List[str] = []

    # 1. Description
    raw_body = issue_data.get("body")
    body = normalize_github_content(raw_body) if raw_body else ""
    if body:
        sections.append(f"## Description\n\n{body}")

    # 2. Comments (if any)
    if comments:
        # Sort locally as well to ensure deterministic chronological order (created_at asc, id asc)
        def _comment_sort_key(c: Dict[str, Any]) -> Tuple[str, int]:
            if not isinstance(c, dict):
                return ("", 0)
            created = str(c.get("created_at") or "")
            cid = c.get("id")
            try:
                cid_int = int(cid) if cid is not None else 0
            except (ValueError, TypeError):
                cid_int = 0
            return (created, cid_int)

        sorted_comments = sorted(
            [c for c in comments if isinstance(c, dict)],
            key=_comment_sort_key,
        )

        comment_blocks: List[str] = []
        for c in sorted_comments:
            try:
                if not isinstance(c, dict):
                    continue

                # Safely extract user login
                c_author = "unknown"
                user_obj = c.get("user")
                if isinstance(user_obj, dict):
                    login = user_obj.get("login")
                    if login and str(login).strip():
                        c_author = str(login).strip()
                elif isinstance(user_obj, str) and user_obj.strip():
                    c_author = user_obj.strip()

                c_date = str(c.get("created_at") or "").strip()
                raw_c_body = c.get("body")
                c_body = normalize_github_content(raw_c_body) if raw_c_body else ""
                if not c_body:
                    continue
                header = f"### {c_author}" + (f" — {c_date}" if c_date else "")
                comment_blocks.append(f"{header}\n\n{c_body}")
            except Exception as e:
                logger.warning(f"Skipping malformed comment: {e}")
                continue

        if comment_blocks:
            sections.append("## Comments\n\n" + "\n\n".join(comment_blocks))

    # 3. GitHub Metadata Section
    meta_bullets: List[str] = []
    state = issue_data.get("state")
    if state:
        meta_bullets.append(f"- State: {str(state).strip().lower()}")

    labels = issue_data.get("labels")
    if isinstance(labels, list) and labels:
        label_names = [
            lbl.get("name") if isinstance(lbl, dict) else str(lbl)
            for lbl in labels
            if lbl
        ]
        label_names = [str(n).strip() for n in label_names if str(n).strip()]
        if label_names:
            meta_bullets.append(f"- Labels: {', '.join(label_names)}")

    assignees = issue_data.get("assignees")
    if isinstance(assignees, list) and assignees:
        assignee_names = [
            a.get("login") if isinstance(a, dict) else str(a)
            for a in assignees
            if a
        ]
        assignee_names = [str(n).strip() for n in assignee_names if str(n).strip()]
        if assignee_names:
            meta_bullets.append(f"- Assignees: {', '.join(assignee_names)}")

    milestone = issue_data.get("milestone")
    if isinstance(milestone, dict) and milestone.get("title"):
        meta_bullets.append(f"- Milestone: {str(milestone['title']).strip()}")

    if meta_bullets:
        sections.append("## GitHub Metadata\n\n" + "\n".join(meta_bullets))

    return "\n\n".join(sections)


def build_github_gist_body(gist_data: Dict[str, Any]) -> str:
    """Build clean Obsidian-compatible Markdown body for a GitHub gist without raw HTML."""
    sections: List[str] = []

    # 1. Description (if any)
    raw_desc = gist_data.get("description") or ""
    desc = normalize_github_content(raw_desc)
    if desc:
        sections.append(f"## Description\n\n{desc}")

    # 2. Files
    files_dict = gist_data.get("files") or {}
    file_blocks: List[str] = []

    # Sort files deterministically by filename
    for filename in sorted(files_dict.keys()):
        file_info = files_dict[filename]
        if not isinstance(file_info, dict):
            continue
        content = file_info.get("content") or ""
        lang = file_info.get("language") or ""
        is_truncated = bool(file_info.get("truncated"))

        block_lines = [f"### {filename}"]
        if content:
            block_lines.append(format_code_fence(content, language=lang))
        else:
            block_lines.append("*No content provided for this file.*")

        if is_truncated:
            block_lines.append("*(File content truncated by GitHub API)*")

        file_blocks.append("\n\n".join(block_lines))

    if file_blocks:
        sections.append("## Files\n\n" + "\n\n".join(file_blocks))

    # 3. Gist Metadata
    meta_bullets: List[str] = []
    public = gist_data.get("public")
    if public is not None:
        meta_bullets.append(f"- Visibility: {'public' if public else 'secret'}")

    created_at = gist_data.get("created_at")
    if created_at:
        meta_bullets.append(f"- Created: {created_at}")

    updated_at = gist_data.get("updated_at")
    if updated_at:
        meta_bullets.append(f"- Updated: {updated_at}")

    if meta_bullets:
        sections.append("## Gist Metadata\n\n" + "\n".join(meta_bullets))

    return "\n\n".join(sections)


class GitHubSource(BaseSource):
    """Source connector for GitHub repository issues and user gists."""

    def __init__(
        self,
        token: Optional[str] = None,
        repositories: Optional[Union[List[str], str]] = None,
        session: Optional[Any] = None,
        base_url: str = "https://api.github.com",
        include_gists: bool = False,
        gists_only: bool = False,
        state: str = "all",
        include_comments: bool = True,
    ) -> None:
        super().__init__()
        self._token = token
        self._repositories = self._parse_repo_list(repositories)
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._include_gists = include_gists
        self._gists_only = gists_only
        self._state = state
        self._include_comments = include_comments

    @property
    def source_type(self) -> str:
        return "github"

    @property
    def display_name(self) -> str:
        return "GitHub"

    @staticmethod
    def _parse_repo_list(repos: Optional[Union[List[str], str]]) -> List[str]:
        """Parse repositories from a string, list, or comma-separated string."""
        if not repos:
            return []
        result: List[str] = []
        if isinstance(repos, str):
            for part in repos.split(","):
                part = part.strip()
                if part:
                    result.append(normalize_repo_name(part))
        elif isinstance(repos, (list, tuple, set)):
            for item in repos:
                if isinstance(item, str):
                    for part in item.split(","):
                        part = part.strip()
                        if part:
                            result.append(normalize_repo_name(part))
        # Deduplicate while preserving order
        deduped: List[str] = []
        for r in result:
            if r not in deduped:
                deduped.append(r)
        return deduped

    def _resolve_token(self, **kwargs: Any) -> str:
        """Resolve GitHub personal access token without leaking it."""
        token = (
            kwargs.get("token")
            or kwargs.get("access_token")
            or self._token
            or os.environ.get("GITHUB_TOKEN")
            or os.environ.get("GH_TOKEN")
        )
        if not token or not str(token).strip():
            raise SourceError(
                "GitHub token missing. Configure GITHUB_TOKEN in your environment or via CLI --token."
            )
        return str(token).strip()

    def _get_session(self) -> Any:
        """Return injected HTTP session or a new requests.Session."""
        if self._session is not None:
            return self._session
        return requests.Session()

    def _get_headers(self, token: str) -> Dict[str, str]:
        """Construct standard GitHub REST API headers."""
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "aurora-ingestion/1.0",
        }

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch issues from configured repositories and/or user gists."""
        token = self._resolve_token(**kwargs)
        headers = self._get_headers(token)
        session = self._get_session()
        secrets = [token]

        # Resolve repositories
        cli_repos = kwargs.get("repositories") or kwargs.get("repo") or kwargs.get("repos")
        repos_to_fetch = self._parse_repo_list(cli_repos) if cli_repos else list(self._repositories)

        # Options
        gists_only = kwargs.get("gists_only", self._gists_only)
        include_gists = kwargs.get("include_gists", self._include_gists) or gists_only
        state = kwargs.get("state", self._state) or "all"
        include_comments = kwargs.get("include_comments", self._include_comments)
        limit = kwargs.get("limit")
        limit_val = int(limit) if limit is not None else None

        if not repos_to_fetch and not include_gists:
            raise SourceError(
                "No GitHub repositories or gists specified. Provide at least one repository "
                "(owner/repository) or enable gists ingestion via --include-gists / --gists-only."
            )

        items: List[SourceItem] = []
        seen_source_ids: Set[str] = set()

        # 1. Fetch Repository Issues (unless gists_only is True)
        if not gists_only:
            for repo in repos_to_fetch:
                try:
                    repo_items = self._fetch_repo_issues(
                        session=session,
                        repo=repo,
                        headers=headers,
                        secrets=secrets,
                        state=state,
                        include_comments=include_comments,
                        limit=limit_val,
                    )
                    for item in repo_items:
                        if item.source_id not in seen_source_ids:
                            seen_source_ids.add(item.source_id)
                            items.append(item)
                except Exception as e:
                    mapped_err = map_github_error(e, secrets=secrets)
                    err_msg = str(mapped_err).lower()
                    if "authentication failed" in err_msg or "access forbidden" in err_msg or "rate limit" in err_msg:
                        raise mapped_err
                    logger.error(f"Failed to fetch issues for repository '{repo}': {mapped_err}")
                    continue

        # 2. Fetch User Gists (if enabled)
        if include_gists:
            try:
                gist_items = self._fetch_user_gists(
                    session=session,
                    headers=headers,
                    secrets=secrets,
                    limit=limit_val,
                )
                for item in gist_items:
                    if item.source_id not in seen_source_ids:
                        seen_source_ids.add(item.source_id)
                        items.append(item)
            except Exception as e:
                mapped_err = map_github_error(e, secrets=secrets)
                err_msg = str(mapped_err).lower()
                if "authentication failed" in err_msg or "access forbidden" in err_msg or "rate limit" in err_msg:
                    raise mapped_err
                logger.error(f"Failed to fetch user gists: {mapped_err}")

        return items

    def _fetch_repo_issues(
        self,
        session: Any,
        repo: str,
        headers: Dict[str, str],
        secrets: List[Optional[str]],
        state: str,
        include_comments: bool,
        limit: Optional[int],
    ) -> List[SourceItem]:
        """Fetch issues from a single repository with pagination and batch fault isolation."""
        url = f"{self._base_url}/repos/{repo}/issues"
        page = 1
        per_page = min(limit, 100) if limit else 100
        repo_items: List[SourceItem] = []
        seen_batch_fingerprints: Set[Tuple[Any, ...]] = set()

        while True:
            params = {"state": state, "per_page": per_page, "page": page}
            try:
                resp = session.get(url, headers=headers, params=params, timeout=30)
            except Exception as exc:
                raise map_github_error(exc, secrets=secrets)

            if resp.status_code != 200:
                raise map_github_error(
                    SourceError(f"Failed to fetch issues for {repo}"),
                    response=resp,
                    secrets=secrets,
                )

            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            # Loop detection
            fingerprint = tuple(
                item.get("id") for item in data if isinstance(item, dict) and "id" in item
            )
            if fingerprint in seen_batch_fingerprints:
                logger.warning(f"Duplicate batch detected for repository {repo}. Stopping pagination.")
                break
            seen_batch_fingerprints.add(fingerprint)

            for issue in data:
                if not isinstance(issue, dict):
                    continue
                # Explicitly filter out pull requests per specification
                if "pull_request" in issue:
                    continue

                try:
                    # Fetch comments if requested
                    comments: Optional[List[Dict[str, Any]]] = None
                    if include_comments and issue.get("comments", 0) > 0:
                        issue_num = issue.get("number")
                        if issue_num:
                            comments = self._fetch_issue_comments(
                                session=session,
                                repo=repo,
                                issue_number=issue_num,
                                headers=headers,
                                secrets=secrets,
                            )

                    item = self._convert_issue_to_source_item(repo, issue, comments=comments)
                    repo_items.append(item)
                    if limit and len(repo_items) >= limit:
                        return repo_items
                except Exception as e:
                    logger.error(
                        f"Skipping malformed issue in {repo}: {_scrub_secrets(str(e), secrets)}"
                    )
                    continue

            if len(data) < per_page:
                break
            page += 1
            if page > 100:  # Safety ceiling
                break

        return repo_items

    def _fetch_issue_comments(
        self,
        session: Any,
        repo: str,
        issue_number: Any,
        headers: Dict[str, str],
        secrets: List[Optional[str]],
    ) -> List[Dict[str, Any]]:
        """Paginate and fetch all comments for an issue in chronological order."""
        url = f"{self._base_url}/repos/{repo}/issues/{issue_number}/comments"
        page = 1
        per_page = 100
        all_comments: List[Dict[str, Any]] = []
        seen_batch_fingerprints: Set[Tuple[Any, ...]] = set()

        while True:
            params = {
                "per_page": per_page,
                "page": page,
                "sort": "created",
                "direction": "asc",
            }
            try:
                resp = session.get(url, headers=headers, params=params, timeout=30)
            except Exception as exc:
                logger.warning(
                    f"Failed to fetch comments for issue #{issue_number}: {_scrub_secrets(str(exc), secrets)}"
                )
                break

            if resp.status_code != 200:
                logger.warning(f"Non-200 status fetching comments for issue #{issue_number}: {resp.status_code}")
                break

            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            fingerprint = tuple(
                item.get("id") for item in data if isinstance(item, dict) and "id" in item
            )
            if fingerprint in seen_batch_fingerprints:
                break
            seen_batch_fingerprints.add(fingerprint)

            for c in data:
                if isinstance(c, dict):
                    all_comments.append(c)

            if len(data) < per_page:
                break
            page += 1
            if page > 50:  # Safety ceiling
                break

        # Locally sort chronologically by created_at ascending, then id ascending as tie-breaker
        def _comment_sort_key(c: Dict[str, Any]) -> Tuple[str, int]:
            created = str(c.get("created_at") or "")
            cid = c.get("id")
            try:
                cid_int = int(cid) if cid is not None else 0
            except (ValueError, TypeError):
                cid_int = 0
            return (created, cid_int)

        all_comments.sort(key=_comment_sort_key)
        return all_comments

    def _recover_truncated_gist_files(
        self,
        session: Any,
        gist: Dict[str, Any],
        headers: Dict[str, str],
        secrets: List[Optional[str]],
    ) -> None:
        """Attempt to recover complete file content for truncated gist files via their raw_url."""
        files = gist.get("files")
        if not isinstance(files, dict):
            return

        for filename, file_info in files.items():
            if not isinstance(file_info, dict):
                continue
            if not file_info.get("truncated"):
                continue

            raw_url = file_info.get("raw_url")
            if not raw_url or not str(raw_url).strip():
                logger.info(f"Gist file '{filename}' is truncated but has no raw_url to recover from.")
                continue

            clean_url = str(raw_url).strip()
            try:
                resp = session.get(clean_url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    file_info["content"] = resp.text
                    file_info["truncated"] = False
                    logger.info(f"Successfully recovered truncated content for gist file '{filename}'")
                else:
                    logger.warning(
                        f"Failed to recover truncated gist file '{filename}' via raw_url (HTTP {resp.status_code})"
                    )
            except Exception as exc:
                logger.warning(
                    f"Error recovering truncated gist file '{filename}': {_scrub_secrets(str(exc), secrets)}"
                )

    def _fetch_user_gists(
        self,
        session: Any,
        headers: Dict[str, str],
        secrets: List[Optional[str]],
        limit: Optional[int],
    ) -> List[SourceItem]:
        """Fetch accessible gists for authenticated user with pagination and batch fault isolation."""
        url = f"{self._base_url}/gists"
        page = 1
        per_page = min(limit, 100) if limit else 100
        gist_items: List[SourceItem] = []
        seen_batch_fingerprints: Set[Tuple[Any, ...]] = set()

        while True:
            params = {"per_page": per_page, "page": page}
            try:
                resp = session.get(url, headers=headers, params=params, timeout=30)
            except Exception as exc:
                raise map_github_error(exc, secrets=secrets)

            if resp.status_code != 200:
                raise map_github_error(
                    SourceError("Failed to fetch user gists"),
                    response=resp,
                    secrets=secrets,
                )

            data = resp.json()
            if not isinstance(data, list) or not data:
                break

            fingerprint = tuple(
                item.get("id") for item in data if isinstance(item, dict) and "id" in item
            )
            if fingerprint in seen_batch_fingerprints:
                logger.warning("Duplicate batch detected for gists. Stopping pagination.")
                break
            seen_batch_fingerprints.add(fingerprint)

            for gist in data:
                if not isinstance(gist, dict):
                    continue
                try:
                    self._recover_truncated_gist_files(
                        session=session,
                        gist=gist,
                        headers=headers,
                        secrets=secrets,
                    )
                    item = self._convert_gist_to_source_item(gist)
                    gist_items.append(item)
                    if limit and len(gist_items) >= limit:
                        return gist_items
                except Exception as e:
                    logger.error(f"Skipping malformed gist: {_scrub_secrets(str(e), secrets)}")
                    continue

            if len(data) < per_page:
                break
            page += 1
            if page > 100:  # Safety ceiling
                break

        return gist_items

    def _convert_issue_to_source_item(
        self,
        repo: str,
        issue: Dict[str, Any],
        comments: Optional[List[Dict[str, Any]]] = None,
    ) -> SourceItem:
        """Convert a raw GitHub issue object into a SourceItem."""
        issue_number = issue.get("number")
        source_id = derive_github_issue_id(repo, issue_number)

        raw_title = issue.get("title") or ""
        normalized_title = normalize_github_content(raw_title)
        title = str(normalized_title).strip() if normalized_title else f"Issue #{issue_number}"

        date_val = issue.get("created_at") or ""
        source_url = issue.get("html_url")

        author = None
        user_obj = issue.get("user")
        if isinstance(user_obj, dict):
            login = user_obj.get("login")
            if login and str(login).strip():
                author = str(login).strip()
        elif isinstance(user_obj, str) and user_obj.strip():
            author = user_obj.strip()

        tags = normalize_github_tags(issue.get("labels"))

        state = str(issue.get("state") or "open").lower()
        body_content = build_github_issue_body(issue, comments=comments)

        labels = issue.get("labels")
        label_names = [
            lbl.get("name") if isinstance(lbl, dict) else str(lbl)
            for lbl in labels
            if lbl
        ] if isinstance(labels, list) else []

        assignees = issue.get("assignees")
        assignee_names = [
            a.get("login") if isinstance(a, dict) else str(a)
            for a in assignees
            if a
        ] if isinstance(assignees, list) else []

        milestone_title = None
        milestone = issue.get("milestone")
        if isinstance(milestone, dict):
            milestone_title = milestone.get("title")

        extra_metadata: Dict[str, Any] = {
            "github_type": "issue",
            "repository": repo,
            "issue_number": issue_number,
            "github_id": issue.get("id"),
            "source_url": source_url,
            "author": author,
            "state": state,
            "updated_at": issue.get("updated_at"),
            "closed_at": issue.get("closed_at"),
            "labels": label_names,
            "assignees": assignee_names,
            "milestone": milestone_title,
            "comment_count": len(comments) if comments is not None else issue.get("comments", 0),
        }
        extra_metadata = {k: v for k, v in extra_metadata.items() if v is not None}

        raw_issue_body = issue.get("body")
        summary_text = normalize_github_content(raw_issue_body)[:200].strip() if raw_issue_body else None

        item = SourceItem(
            source_id=source_id,
            source_type=self.source_type,
            title=title,
            content=body_content,
            date=date_val,
            source_url=source_url,
            author=author,
            tags=tags,
            summary=summary_text,
            status=state,
            extra_metadata=extra_metadata,
            raw_content=issue,
        )
        item.content_hash = item.compute_content_hash()
        return item

    def _convert_gist_to_source_item(
        self,
        gist: Dict[str, Any],
        session: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        secrets: Optional[List[Optional[str]]] = None,
    ) -> SourceItem:
        """Convert a raw GitHub gist object into a SourceItem."""
        if session and headers:
            self._recover_truncated_gist_files(
                session=session,
                gist=gist,
                headers=headers,
                secrets=secrets or [],
            )

        gist_id = gist.get("id")
        source_id = derive_github_gist_id(gist_id)

        raw_desc = gist.get("description") or ""
        normalized_desc = normalize_github_content(raw_desc)
        title = str(normalized_desc).strip() if normalized_desc else f"Gist {gist_id}"

        date_val = gist.get("created_at") or ""
        source_url = gist.get("html_url")

        author = None
        owner_obj = gist.get("owner")
        if isinstance(owner_obj, dict):
            login = owner_obj.get("login")
            if login and str(login).strip():
                author = str(login).strip()
        elif isinstance(owner_obj, str) and owner_obj.strip():
            author = owner_obj.strip()

        tags = ["ingested", "github", "code"]
        body_content = build_github_gist_body(gist)

        files = gist.get("files") or {}

        extra_metadata: Dict[str, Any] = {
            "github_type": "gist",
            "gist_id": gist_id,
            "source_url": source_url,
            "author": author,
            "public": gist.get("public"),
            "updated_at": gist.get("updated_at"),
            "file_count": len(files) if isinstance(files, dict) else 0,
        }
        extra_metadata = {k: v for k, v in extra_metadata.items() if v is not None}

        item = SourceItem(
            source_id=source_id,
            source_type=self.source_type,
            title=title,
            content=body_content,
            date=date_val,
            source_url=source_url,
            author=author,
            tags=tags,
            summary=normalized_desc[:200].strip() if normalized_desc else None,
            status="unread",
            extra_metadata=extra_metadata,
            raw_content=gist,
        )
        item.content_hash = item.compute_content_hash()
        return item

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a fetched SourceItem into a MarkdownNote targeted for Ingested/Code/."""
        note = self.default_item_to_note(item)
        note.folder = "Ingested/Code"
        return note


# Register connector with global SourceRegistry
SourceRegistry.register("github", GitHubSource)
