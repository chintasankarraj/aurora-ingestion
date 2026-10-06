"""Unit and integration tests for the Reddit ingestion connector."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
import requests
import yaml
import pytest

from config import IngestionConfig
from converter import (
    build_attribution_block,
    extract_date_prefix,
    generate_note_filename,
    render_markdown_document,
)
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import MarkdownNote
from tracker import DeduplicationTracker, IngestionAction
from sources.base import SourceRegistry
from sources.reddit_source import (
    RedditSource,
    _scrub_secrets,
    build_reddit_post_body,
    derive_reddit_post_id,
    extract_reddit_comments,
    format_reddit_comments,
    format_reddit_timestamp,
    is_html_content,
    is_safe_http_url,
    map_reddit_error,
    normalize_reddit_content,
    normalize_subreddit_name,
)


class MockResponse:
    """Mock requests Response object for testing Reddit API."""

    def __init__(
        self,
        json_data: Any,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.text = json.dumps(json_data) if not isinstance(json_data, str) else json_data

    def json(self) -> Any:
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class MockSession:
    """Mock requests.Session object allowing chained responses."""

    def __init__(self, responses: Optional[List[Any]] = None) -> None:
        self.responses = list(responses or [])
        self.calls: List[Dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"method": "GET", "url": url, "kwargs": kwargs})
        if not self.responses:
            return MockResponse({})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def post(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"method": "POST", "url": url, "kwargs": kwargs})
        if not self.responses:
            return MockResponse({})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def sample_reddit_post(
    post_id: str = "p101",
    title: str = "Best local LLM for coding",
    subreddit: str = "programming",
    author: str = "dev_guru",
    selftext: str = "What is the best local LLM you have used for code generation?",
    is_self: bool = True,
    url: Optional[str] = None,
    score: int = 142,
    num_comments: int = 2,
    created_utc: float = 1728216000.0,
    flair: Optional[str] = "Discussion",
    edited: Any = False,
    promoted: bool = False,
) -> Dict[str, Any]:
    """Helper creating a sample Reddit post dictionary."""
    slug = re.sub(r"[^a-zA-Z0-9_-]", "_", title.lower()).strip("_") or "post"
    permalink = f"/r/{subreddit}/comments/{post_id}/{slug}/"
    full_url = (
        url
        if url is not None
        else (f"https://www.reddit.com{permalink}" if is_self else "https://external.example.com/article")
    )
    return {
        "id": post_id,
        "name": f"t3_{post_id}",
        "title": title,
        "author": author,
        "subreddit": subreddit,
        "selftext": selftext,
        "is_self": is_self,
        "url": full_url,
        "permalink": permalink,
        "score": score,
        "ups": score,
        "num_comments": num_comments,
        "created_utc": created_utc,
        "link_flair_text": flair,
        "edited": edited,
        "promoted": promoted,
    }


def sample_reddit_listing(
    children: List[Dict[str, Any]], after: Optional[str] = None
) -> Dict[str, Any]:
    """Helper wrapping post dictionaries into a Reddit Listing response."""
    return {
        "kind": "Listing",
        "data": {
            "after": after,
            "before": None,
            "children": [{"kind": "t3", "data": c} for c in children],
        },
    }


def sample_reddit_comment(
    comment_id: str = "c201",
    author: str = "alice",
    body: str = "I really like Qwen 2.5 Coder.",
    created_utc: float = 1728216100.0,
    score: int = 25,
    parent_id: str = "t3_p101",
    replies: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Helper creating a sample Reddit comment tree node."""
    c_data: Dict[str, Any] = {
        "id": comment_id,
        "name": f"t1_{comment_id}",
        "parent_id": parent_id,
        "author": author,
        "body": body,
        "created_utc": created_utc,
        "score": score,
        "permalink": f"/r/programming/comments/p101/title/{comment_id}/",
        "replies": "",
    }
    if replies:
        c_data["replies"] = {
            "kind": "Listing",
            "data": {
                "children": replies,
            },
        }
    return {"kind": "t1", "data": c_data}


# ---------------------------------------------------------------------------
# 1. Registration & Properties Tests
# ---------------------------------------------------------------------------


def test_reddit_source_registration():
    """Test that RedditSource is registered under 'reddit' in SourceRegistry."""
    source_cls = SourceRegistry.get("reddit")
    assert source_cls is RedditSource
    sources = SourceRegistry.list_sources()
    assert "reddit" in sources
    assert sources["reddit"] == "RedditSource"


def test_reddit_source_properties():
    """Test standard BaseSource properties for Reddit."""
    source = RedditSource(token="fake_token", subreddits=["programming"])
    assert source.source_type == "reddit"
    assert source.display_name == "Reddit"


# ---------------------------------------------------------------------------
# 2. Subreddit Normalization Tests
# ---------------------------------------------------------------------------


def test_normalize_subreddit_name_valid():
    """Test valid subreddit names are normalized properly."""
    assert normalize_subreddit_name("programming") == "programming"
    assert normalize_subreddit_name("r/programming") == "programming"
    assert normalize_subreddit_name("R/programming") == "programming"
    assert normalize_subreddit_name("/r/programming/") == "programming"
    assert normalize_subreddit_name("MachineLearning") == "MachineLearning"
    assert normalize_subreddit_name("r/LocalLLaMA") == "LocalLLaMA"


def test_normalize_subreddit_name_invalid():
    """Test invalid or empty subreddit names raise SourceError."""
    with pytest.raises(SourceError, match="empty"):
        normalize_subreddit_name("")
    with pytest.raises(SourceError, match="empty"):
        normalize_subreddit_name("   ")
    with pytest.raises(SourceError, match="Invalid subreddit name"):
        normalize_subreddit_name("invalid sub with spaces")
    with pytest.raises(SourceError, match="Invalid subreddit name"):
        normalize_subreddit_name("a")  # too short (< 2 chars)


def test_parse_subreddit_list():
    """Test parsing comma-separated strings and lists with deduplication."""
    parsed = RedditSource._parse_subreddit_list("programming, r/MachineLearning, programming")
    assert parsed == ["programming", "MachineLearning"]

    parsed_list = RedditSource._parse_subreddit_list(["r/artificial", "LocalLLaMA", "artificial"])
    assert parsed_list == ["artificial", "LocalLLaMA"]


@pytest.mark.asyncio
async def test_no_subreddits_specified_raises_error(monkeypatch):
    """Test that missing subreddits raises SourceError."""
    monkeypatch.delenv("REDDIT_SUBREDDITS", raising=False)
    monkeypatch.delenv("REDDIT_SUBREDDIT", raising=False)
    source = RedditSource(token="valid_token")
    with pytest.raises(SourceError, match="No subreddits specified"):
        await source.fetch_items()


# ---------------------------------------------------------------------------
# 3. Post ID Derivation Tests
# ---------------------------------------------------------------------------


def test_derive_reddit_post_id():
    """Test derive_reddit_post_id returns stable immutable IDs."""
    assert derive_reddit_post_id("123abc") == "reddit:post:123abc"
    assert derive_reddit_post_id("t3_123abc") == "reddit:post:123abc"


def test_derive_reddit_post_id_empty_raises():
    """Test missing post ID raises SourceError."""
    with pytest.raises(SourceError, match="Missing post ID"):
        derive_reddit_post_id("")
    with pytest.raises(SourceError, match="Missing post ID"):
        derive_reddit_post_id(None)


# ---------------------------------------------------------------------------
# 4. Authentication Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auth_with_direct_token():
    """Test that direct Bearer token bypasses OAuth endpoint."""
    post = sample_reddit_post(post_id="p1")
    resp_listing = MockResponse(sample_reddit_listing([post]))
    mock_session = MockSession([resp_listing])

    source = RedditSource(
        token="direct_bearer_token",
        subreddits=["programming"],
        include_comments=False,
        session=mock_session,
    )
    items = await source.fetch_items()
    assert len(items) == 1
    assert len(mock_session.calls) == 1
    assert mock_session.calls[0]["method"] == "GET"
    assert "Bearer direct_bearer_token" in mock_session.calls[0]["kwargs"]["headers"]["Authorization"]


@pytest.mark.asyncio
async def test_auth_with_client_id_and_secret():
    """Test OAuth2 application-only flow requests Bearer token via POST."""
    resp_token = MockResponse({"access_token": "oauth_token_123", "token_type": "bearer", "expires_in": 3600})
    post = sample_reddit_post(post_id="p2")
    resp_listing = MockResponse(sample_reddit_listing([post]))
    mock_session = MockSession([resp_token, resp_listing])

    source = RedditSource(
        client_id="my_client_id",
        client_secret="my_client_secret",
        user_agent="my-app:v1.0 (by /u/tester)",
        subreddits=["programming"],
        include_comments=False,
        session=mock_session,
    )
    items = await source.fetch_items()
    assert len(items) == 1
    assert len(mock_session.calls) == 2
    # Call 0: POST to token endpoint
    assert mock_session.calls[0]["method"] == "POST"
    assert mock_session.calls[0]["kwargs"]["data"] == {"grant_type": "client_credentials"}
    # Call 1: GET to listing
    assert mock_session.calls[1]["method"] == "GET"
    assert "Bearer oauth_token_123" in mock_session.calls[1]["kwargs"]["headers"]["Authorization"]


@pytest.mark.asyncio
async def test_auth_missing_credentials_raises_error(monkeypatch):
    """Test missing token and client credentials raises SourceError."""
    monkeypatch.delenv("REDDIT_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("REDDIT_TOKEN", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)

    source = RedditSource(subreddits=["programming"])
    with pytest.raises(SourceError, match="Reddit credentials missing"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_auth_401_invalid_credentials():
    """Test OAuth endpoint 401 raises descriptive authentication SourceError."""
    resp_fail = MockResponse({"error": "invalid_grant"}, status_code=401)
    mock_session = MockSession([resp_fail])

    source = RedditSource(
        client_id="wrong_id",
        client_secret="wrong_secret",
        subreddits=["programming"],
        session=mock_session,
    )
    with pytest.raises(SourceError, match="authentication failed"):
        await source.fetch_items()


def test_secrets_never_leaked_in_scrub():
    """Test that _scrub_secrets redacts secret tokens and passwords."""
    secret = "secret_reddit_key_xyz987"
    raw = f"Error connecting with secret_reddit_key_xyz987 to https://api.reddit.com"
    scrubbed = _scrub_secrets(raw, [secret])
    assert secret not in scrubbed
    assert "[REDACTED]" in scrubbed


# ---------------------------------------------------------------------------
# 5. Post Content & Metadata Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_self_post_retrieval_and_structure():
    """Test retrieval and Markdown conversion of a self-post."""
    post = sample_reddit_post(
        post_id="post_self_1",
        title="Comparing Local Coding Models",
        selftext="Here is my benchmark comparing various open-source LLMs.",
        score=200,
        num_comments=0,
        flair="Discussion",
    )
    mock_session = MockSession([MockResponse(sample_reddit_listing([post]))])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()
    assert len(items) == 1
    item = items[0]

    assert item.source_id == "reddit:post:post_self_1"
    assert item.title == "Comparing Local Coding Models"
    assert item.tags == ["ingested", "reddit", "social"]
    assert item.extra_metadata["subreddit"] == "programming"
    assert item.extra_metadata["score"] == 200
    assert item.extra_metadata["flair"] == "Discussion"

    assert "# Comparing Local Coding Models" in item.content
    assert "## Post" in item.content
    assert "Here is my benchmark" in item.content
    assert "## Metadata" in item.content
    assert "- **Subreddit:** r/programming" in item.content
    assert "- **Author:** u/dev_guru" in item.content
    assert "- **Score:** 200" in item.content
    assert "- **Flair:** Discussion" in item.content


@pytest.mark.asyncio
async def test_link_post_retrieval_shows_external_link():
    """Test link post properly links to external URL in Post and Metadata sections."""
    post = sample_reddit_post(
        post_id="post_link_1",
        title="Python 3.14 Release Notes",
        is_self=False,
        url="https://docs.python.org/release/3.14.html",
        selftext="",
    )
    mock_session = MockSession([MockResponse(sample_reddit_listing([post]))])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()
    assert len(items) == 1
    content = items[0].content

    assert "**Link**: [https://docs.python.org/release/3.14.html](https://docs.python.org/release/3.14.html)" in content
    assert "- **External URL:** [https://docs.python.org/release/3.14.html](https://docs.python.org/release/3.14.html)" in content
    assert items[0].extra_metadata["external_url"] == "https://docs.python.org/release/3.14.html"


def test_deleted_author_post_handling():
    """Test that deleted post author is rendered safely as [deleted]."""
    post = sample_reddit_post(post_id="p_del_auth", author="[deleted]")
    body = build_reddit_post_body(post)
    assert "**Author**: [deleted]" in body
    assert "- **Author:** [deleted]" in body


def test_deleted_selftext_post_handling():
    """Test that deleted/removed selftext is formatted as italicized notice."""
    post1 = sample_reddit_post(post_id="p_del_body1", selftext="[deleted]")
    body1 = build_reddit_post_body(post1)
    assert "*[deleted]*" in body1

    post2 = sample_reddit_post(post_id="p_del_body2", selftext="[removed]")
    body2 = build_reddit_post_body(post2)
    assert "*[removed]*" in body2


def test_empty_selftext_post_handling():
    """Test that self-post with empty text displays *No content.*"""
    post = sample_reddit_post(post_id="p_empty", selftext="")
    body = build_reddit_post_body(post)
    assert "*No content.*" in body


def test_edited_post_metadata():
    """Test that edited post includes edited timestamp."""
    post = sample_reddit_post(post_id="p_edit", edited=1728220000.0)
    body = build_reddit_post_body(post)
    assert "- **Edited:**" in body


@pytest.mark.asyncio
async def test_promoted_ad_posts_are_filtered():
    """Test that promoted/advertisement posts are strictly skipped."""
    ad_post = sample_reddit_post(post_id="ad1", title="Sponsored product", promoted=True)
    real_post = sample_reddit_post(post_id="p_real", title="Actual discussion", promoted=False)
    mock_session = MockSession([MockResponse(sample_reddit_listing([ad_post, real_post]))])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].source_id == "reddit:post:p_real"


def test_unicode_in_post_title_and_body():
    """Test emoji and Unicode characters in title and body are preserved."""
    post = sample_reddit_post(
        post_id="p_uni",
        title="🚀 Awesome Rust Project 🔥",
        selftext="Testing 漢字 and emojis 😊.",
    )
    body = build_reddit_post_body(post)
    assert "🚀 Awesome Rust Project 🔥" in body
    assert "漢字 and emojis 😊" in body


# ---------------------------------------------------------------------------
# 6. Comments Formatting & Ingestion Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_comments_chronological_ordering_and_replies():
    """Test that comments are ordered chronologically and nested replies formatted cleanly."""
    post = sample_reddit_post(post_id="p_thread", title="Thread Test")
    resp_listing = MockResponse(sample_reddit_listing([post]))

    # Comment tree:
    # c2: created at 100
    #   c3 (reply to c2): created at 200
    # c1: created at 50
    c1 = sample_reddit_comment(comment_id="c1", author="bob", body="Early comment", created_utc=50.0)
    c3 = sample_reddit_comment(comment_id="c3", author="dan", body="Reply to alice", created_utc=200.0, parent_id="t1_c2")
    c2 = sample_reddit_comment(comment_id="c2", author="alice", body="Later comment", created_utc=100.0, replies=[c3])

    resp_comments = MockResponse([
        sample_reddit_listing([post]),
        {"kind": "Listing", "data": {"children": [c2, c1]}},  # Out of order from API
    ])

    mock_session = MockSession([resp_listing, resp_comments])
    source = RedditSource(token="token", subreddits=["programming"], session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    content = items[0].content
    assert "## Comments" in content
    assert "### @bob — 1970-01-01" in content
    assert "Early comment" in content
    assert "### @alice — 1970-01-01" in content
    assert "Later comment" in content
    # Reply formatting
    assert "> **↳ @dan** — 1970-01-01" in content
    assert "*(reply to @alice)*:" in content
    assert "> Reply to alice" in content

    # Verify chronological appearance: c1 (created at 50) appears before c2 (created at 100)
    idx_c1 = content.index("Early comment")
    idx_c2 = content.index("Later comment")
    assert idx_c1 < idx_c2


def test_deleted_comment_author_and_body():
    """Test comment with deleted author and deleted body."""
    c = {
        "id": "c_del",
        "author": "[deleted]",
        "created_utc": 1728216100.0,
        "created_iso": "2026-10-06T12:00:00Z",
        "score": 0,
        "body": "[deleted]",
        "parent_id": "t3_p101",
        "depth": 0,
    }
    formatted = format_reddit_comments([c])
    assert "### @deleted" in formatted
    assert "*[deleted]*" in formatted


def test_malformed_comment_isolation():
    """Test that extract_reddit_comments ignores malformed nodes without raising."""
    children = [
        "not_a_dict",
        {"kind": "more", "data": {"children": ["c99"]}},  # 'more' placeholder
        {"kind": "t1", "data": "not_a_dict"},
        {"kind": "t1", "data": {"id": ""}},  # missing id
        {"kind": "t1", "data": {"id": "valid_c", "body": "Valid body", "author": "dev"}},
    ]
    extracted = extract_reddit_comments(children)
    assert len(extracted) == 1
    assert extracted[0]["id"] == "valid_c"


@pytest.mark.asyncio
async def test_comment_limit_respected():
    """Test that comment extraction respects max_comments boundary."""
    post = sample_reddit_post(post_id="p_many")
    resp_listing = MockResponse(sample_reddit_listing([post]))

    comments = [sample_reddit_comment(comment_id=f"c_{i}", created_utc=float(i)) for i in range(10)]
    resp_comments = MockResponse([
        sample_reddit_listing([post]),
        {"kind": "Listing", "data": {"children": comments}},
    ])

    mock_session = MockSession([resp_listing, resp_comments])
    source = RedditSource(token="token", subreddits=["programming"], max_comments=3, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    # Check that only 3 comments were included
    assert items[0].content.count("### @alice") == 3


@pytest.mark.asyncio
async def test_no_comments_flag_omits_comments():
    """Test that include_comments=False does not fetch or render comments."""
    post = sample_reddit_post(post_id="p_no_comm")
    resp_listing = MockResponse(sample_reddit_listing([post]))
    mock_session = MockSession([resp_listing])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    assert "## Comments" not in items[0].content
    assert len(mock_session.calls) == 1  # No secondary comments API request


@pytest.mark.asyncio
async def test_comments_fetch_failure_keeps_post():
    """Test that a network/API failure when fetching comments still generates the post without crashing."""
    post = sample_reddit_post(post_id="p_fail_comm")
    resp_listing = MockResponse(sample_reddit_listing([post]))
    resp_comments_fail = MockResponse({"message": "Not Found"}, status_code=404)
    mock_session = MockSession([resp_listing, resp_comments_fail])

    source = RedditSource(token="token", subreddits=["programming"], session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    assert items[0].source_id == "reddit:post:p_fail_comm"
    assert "## Comments" not in items[0].content  # Comments omitted safely


# ---------------------------------------------------------------------------
# 7. Pagination Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pagination_multiple_pages():
    """Test pagination across multiple pages using the 'after' cursor."""
    p1 = sample_reddit_post(post_id="page1_post")
    p2 = sample_reddit_post(post_id="page2_post")

    resp1 = MockResponse(sample_reddit_listing([p1], after="t3_page1_post"))
    resp2 = MockResponse(sample_reddit_listing([p2], after=None))
    mock_session = MockSession([resp1, resp2])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 2
    assert items[0].source_id == "reddit:post:page1_post"
    assert items[1].source_id == "reddit:post:page2_post"
    assert len(mock_session.calls) == 2
    assert mock_session.calls[1]["kwargs"]["params"]["after"] == "t3_page1_post"


@pytest.mark.asyncio
async def test_pagination_stops_at_configured_limit():
    """Test pagination halts as soon as configured limit is met."""
    p1 = sample_reddit_post(post_id="p1")
    p2 = sample_reddit_post(post_id="p2")
    p3 = sample_reddit_post(post_id="p3")

    resp1 = MockResponse(sample_reddit_listing([p1, p2, p3], after="t3_p3"))
    mock_session = MockSession([resp1])

    source = RedditSource(token="token", subreddits=["programming"], limit=2, include_comments=False, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 2
    assert len(mock_session.calls) == 1


@pytest.mark.asyncio
async def test_pagination_repeated_cursor_loop_protection():
    """Test that a repeated 'after' cursor terminates pagination to prevent infinite loops."""
    p1 = sample_reddit_post(post_id="p1")
    # Both responses return the exact same 'after' cursor
    resp1 = MockResponse(sample_reddit_listing([p1], after="cursor_repeat"))
    resp2 = MockResponse(sample_reddit_listing([], after="cursor_repeat"))
    mock_session = MockSession([resp1, resp2])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    assert len(mock_session.calls) == 2


# ---------------------------------------------------------------------------
# 8. Deduplication Lifecycle Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_deduplication_lifecycle_reddit_post(tmp_path: Path):
    """Test NEW creates file in Ingested/Social/, UNCHANGED skips, CHANGED overwrites in-place."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_reddit.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    raw_post = sample_reddit_post(post_id="dedup_1", title="Original Title", selftext="First content")
    mock_session1 = MockSession([MockResponse(sample_reddit_listing([raw_post]))])
    source1 = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session1)

    # 1. NEW
    items1 = await source1.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source1, items1[0])
    assert action1 == IngestionAction.NEW
    assert rel_path1.startswith("Ingested/Social/")
    note_path = vault_dir / rel_path1
    assert note_path.exists()
    assert "Original Title" in note_path.read_text(encoding="utf-8")

    # 2. UNCHANGED
    action2, rel_path2 = await pipeline.process_item(source1, items1[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path1

    # 3. CHANGED (selftext updated)
    raw_post_updated = sample_reddit_post(post_id="dedup_1", title="Original Title", selftext="Updated content text")
    mock_session2 = MockSession([MockResponse(sample_reddit_listing([raw_post_updated]))])
    source2 = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session2)
    items2 = await source2.fetch_items()

    action3, rel_path3 = await pipeline.process_item(source2, items2[0])
    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path1
    assert "Updated content text" in note_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_post_title_change_preserves_path_on_update(tmp_path: Path):
    """Test that updating post title overwrites the existing note in-place rather than creating a duplicate note."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_reddit_title.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    post1 = sample_reddit_post(post_id="title_change_1", title="First Title", selftext="Body")
    source1 = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=MockSession([MockResponse(sample_reddit_listing([post1]))]))
    items1 = await source1.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source1, items1[0])
    assert action1 == IngestionAction.NEW

    # Renamed post
    post2 = sample_reddit_post(post_id="title_change_1", title="Second Title (Updated)", selftext="Body")
    source2 = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=MockSession([MockResponse(sample_reddit_listing([post2]))]))
    items2 = await source2.fetch_items()
    action2, rel_path2 = await pipeline.process_item(source2, items2[0])

    assert action2 == IngestionAction.CHANGED
    assert rel_path2 == rel_path1
    # File count in Ingested/Social should still be exactly 1
    social_files = list((vault_dir / "Ingested" / "Social").glob("*.md"))
    assert len(social_files) == 1


# ---------------------------------------------------------------------------
# 9. HTML & Markdown Normalization Tests
# ---------------------------------------------------------------------------


def test_normalize_reddit_content_preserves_markdown():
    """Test that clean Markdown and mathematical comparisons are preserved without alteration."""
    raw = (
        "# Heading\n\n"
        "Here is a list:\n- item 1\n- item 2\n\n"
        "Comparison: 5 < 10 and 20 > 15.\n\n"
        "```python\ndef test():\n    return a < b\n```"
    )
    normalized = normalize_reddit_content(raw)
    assert "# Heading" in normalized
    assert "- item 1" in normalized
    assert "5 < 10 and 20 > 15" in normalized
    assert "return a < b" in normalized


def test_normalize_reddit_content_converts_html():
    """Test that HTML elements are converted to Markdown."""
    html_input = "<p>This is a <strong>bold</strong> statement with a <a href='https://example.com'>link</a>.</p>"
    normalized = normalize_reddit_content(html_input)
    assert "**bold**" in normalized
    assert "[link](https://example.com)" in normalized
    assert "<p>" not in normalized
    assert "<strong>" not in normalized


def test_normalize_reddit_content_strips_script_and_style():
    """Test that script and style blocks and their internal code are removed."""
    dirty = "Text before.<script>alert('xss');</script><style>body { color: red; }</style>Text after."
    normalized = normalize_reddit_content(dirty)
    assert "alert('xss')" not in normalized
    assert "color: red" not in normalized
    assert "Text before." in normalized
    assert "Text after." in normalized


def test_normalize_reddit_content_unescapes_entities():
    """Test that HTML entities like &gt; and &lt; are unescaped properly."""
    raw = "&gt; This is a quoted block\n\nAlso a &amp; b with x &lt; y."
    normalized = normalize_reddit_content(raw)
    assert "> This is a quoted block" in normalized
    assert "a & b" in normalized
    assert "x < y" in normalized


# ---------------------------------------------------------------------------
# 10. HTTP Failures & Error Handling Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_403_forbidden_error():
    """Test HTTP 403 maps to SourceError indicating private subreddit or lack of permissions."""
    resp = MockResponse({"message": "Forbidden", "error": 403}, status_code=403)
    source = RedditSource(token="token", subreddits=["private_sub"], session=MockSession([resp]))
    with pytest.raises(SourceError, match="Reddit access forbidden"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_http_404_not_found_error():
    """Test HTTP 404 maps to SourceError."""
    resp = MockResponse({"message": "Not Found"}, status_code=404)
    # A single 404 subreddit logs error in fetch_items (sub isolation) but direct method raises
    source = RedditSource(token="token", subreddits=["nonexistent_sub"], session=MockSession([resp]))
    items = await source.fetch_items()
    assert items == []  # Subreddit was isolated


@pytest.mark.asyncio
async def test_http_429_rate_limit_with_reset():
    """Test HTTP 429 maps to rate limit SourceError with reset seconds."""
    resp = MockResponse(
        {"message": "Too Many Requests"},
        status_code=429,
        headers={"x-ratelimit-reset": "45"},
    )
    source = RedditSource(token="token", subreddits=["programming"], session=MockSession([resp]))
    with pytest.raises(SourceError, match="rate limit exceeded.*45s"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_http_500_server_error():
    """Test HTTP 500 server error maps to SourceError."""
    resp = MockResponse({"message": "Internal error"}, status_code=500)
    source = RedditSource(token="token", subreddits=["programming"], session=MockSession([resp]))
    items = await source.fetch_items()
    assert items == []  # Isolated


@pytest.mark.asyncio
async def test_network_timeout():
    """Test request timeout raises SourceError."""
    mock_session = MockSession([requests.exceptions.Timeout("Connection timed out")])
    source = RedditSource(token="token", subreddits=["programming"], session=mock_session)
    items = await source.fetch_items()
    assert items == []  # Subreddit isolated


@pytest.mark.asyncio
async def test_network_connection_error():
    """Test request connection error raises SourceError."""
    mock_session = MockSession([requests.exceptions.ConnectionError("DNS failure")])
    source = RedditSource(token="token", subreddits=["programming"], session=mock_session)
    items = await source.fetch_items()
    assert items == []  # Subreddit isolated


@pytest.mark.asyncio
async def test_malformed_json_response():
    """Test malformed JSON response raises SourceError."""
    resp = MockResponse("invalid json string", status_code=200)
    resp._json_data = json.JSONDecodeError("Expecting value", "doc", 0)
    source = RedditSource(token="token", subreddits=["programming"], session=MockSession([resp]))
    items = await source.fetch_items()
    assert items == []


# ---------------------------------------------------------------------------
# 11. Fault Isolation Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subreddit_isolation():
    """Test that a failure in one subreddit does not abort remaining healthy subreddits."""
    resp_fail = MockResponse({"message": "Not Found"}, status_code=404)
    p_good = sample_reddit_post(post_id="good_sub_post", subreddit="MachineLearning")
    resp_good = MockResponse(sample_reddit_listing([p_good]))
    mock_session = MockSession([resp_fail, resp_good])

    source = RedditSource(
        token="token",
        subreddits=["nonexistent_sub", "MachineLearning"],
        include_comments=False,
        session=mock_session,
    )
    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].source_id == "reddit:post:good_sub_post"


@pytest.mark.asyncio
async def test_malformed_post_isolation():
    """Test that a single malformed post record in a batch does not discard other valid posts."""
    malformed_post = {"id": None}  # Invalid post missing id
    valid_post = sample_reddit_post(post_id="p_valid")
    resp = MockResponse(sample_reddit_listing([malformed_post, valid_post]))
    mock_session = MockSession([resp])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].source_id == "reddit:post:p_valid"


# ---------------------------------------------------------------------------
# 12. YAML & Frontmatter Safety Tests
# ---------------------------------------------------------------------------


def test_frontmatter_special_characters_safety():
    """Test quotes, colons, and multiline characters in titles do not break YAML parsing."""
    post = sample_reddit_post(
        post_id="special_chars",
        title='What is: "The Best Way" to handle local AI? [Discussion]',
    )
    body = build_reddit_post_body(post)
    note = MarkdownNote(
        title=post["title"],
        source="reddit",
        date="2026-10-06T12:00:00Z",
        body=body,
        tags=["ingested", "reddit", "social"],
        source_url=post["url"],
        author=post["author"],
        extra_metadata={"subreddit": post["subreddit"], "score": post["score"]},
    )
    rendered = render_markdown_document(note)
    assert rendered.startswith("---\n")

    # Extract frontmatter block and parse with safe_load
    parts = rendered.split("---\n")
    frontmatter_content = parts[1]
    parsed_yaml = yaml.safe_load(frontmatter_content)
    assert parsed_yaml["title"] == 'What is: "The Best Way" to handle local AI? [Discussion]'
    assert parsed_yaml["source"] == "reddit"
    assert parsed_yaml["tags"] == ["ingested", "reddit", "social"]


def test_converter_attribution_block_reddit():
    """Test build_attribution_block specifically formats Reddit notes."""
    note = MarkdownNote(
        title="Attribution Check",
        source="reddit",
        date="2026-10-06T12:00:00Z",
        body="Body text",
        source_url="https://reddit.com/r/programming/comments/123",
        author="dev_guru",
        extra_metadata={"subreddit": "programming", "score": 42, "comment_count": 5},
    )
    attr = build_attribution_block(note)
    assert "> **Source**: [Reddit — r/programming](https://reddit.com/r/programming/comments/123)" in attr
    assert "**Author**: u/dev_guru" in attr
    assert "**Score**: 42" in attr
    assert "**Comments**: 5" in attr


# ---------------------------------------------------------------------------
# 13. Filename & Directory Target Tests
# ---------------------------------------------------------------------------


def test_generate_note_filename_reddit():
    """Test filename generation adheres to <YYYY-MM-DD>_reddit_<sanitized-title>.md."""
    fn = generate_note_filename("2026-10-06", "reddit", "Best local LLM for coding?")
    assert fn == "2026-10-06_reddit_Best_local_LLM_for_coding.md"


# ---------------------------------------------------------------------------
# 14. CLI Command Tests
# ---------------------------------------------------------------------------


def test_cli_parser_registration_reddit():
    """Test that CLI subparser recognizes ingest-reddit and its options."""
    parser = create_parser()
    args = parser.parse_args([
        "ingest-reddit",
        "--subreddit", "programming",
        "--subreddit", "MachineLearning",
        "--listing", "top",
        "--limit", "10",
        "--max-comments", "20",
        "--no-comments",
    ])
    assert args.command == "ingest-reddit"
    assert args.subreddit == ["programming", "MachineLearning"]
    assert args.listing == "top"
    assert args.limit == 10
    assert args.max_comments == 20
    assert args.no_comments is True


@pytest.mark.asyncio
async def test_cli_ingest_reddit_end_to_end(tmp_path: Path, monkeypatch):
    """Test end-to-end execution of ingest-reddit command via async_main."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_reddit.sqlite"

    post = sample_reddit_post(post_id="cli_post_1", title="CLI Ingested Reddit Post")
    mock_resp = MockResponse(sample_reddit_listing([post]))
    monkeypatch.setattr("requests.Session.get", lambda self, url, **kwargs: mock_resp)

    parser = create_parser()
    args = parser.parse_args([
        "ingest-reddit",
        "--token", "mock_cli_token",
        "--subreddit", "programming",
        "--no-comments",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)

    assert exit_code == 0
    created_notes = list((vault_dir / "Ingested" / "Social").glob("*.md"))
    assert len(created_notes) == 1
    assert "CLI Ingested Reddit Post" in created_notes[0].read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 15. Title Normalization & External URL Validation Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_post_title_html_normalized_without_raw_html():
    """Regression test: verify HTML in post titles is cleaned and no raw HTML remains in H1 and SourceItem.title."""
    raw_title = "<b>Exciting</b> news about <script>alert(1)</script> Python &amp; AI (5 &lt; 10)!"
    post = sample_reddit_post(
        post_id="p_html_title",
        title=raw_title,
        selftext="Body content",
    )
    mock_session = MockSession([MockResponse(sample_reddit_listing([post]))])

    source = RedditSource(token="token", subreddits=["programming"], include_comments=False, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    item = items[0]

    # SourceItem.title must not contain raw HTML tags or script blocks
    assert "<b>" not in item.title
    assert "</b>" not in item.title
    assert "<script>" not in item.title
    assert "alert(1)" not in item.title
    assert "**Exciting**" in item.title
    assert "5 < 10" in item.title

    # The generated H1 in content must match normalized title
    assert f"# {item.title}" in item.content
    assert "<script>" not in item.content
    assert "<b>" not in item.content

    # Stable identity remains unchanged
    assert item.source_id == "reddit:post:p_html_title"


def test_post_title_comparisons_preserved():
    """Test that mathematical comparisons in titles like a < b and x > y are preserved."""
    title = "Why 5 < 10 and 20 > 15 in Python"
    post = sample_reddit_post(post_id="p_comp_title", title=title)
    body = build_reddit_post_body(post)
    assert f"# {title}" in body


def test_external_url_validation_https():
    """Regression test: normal HTTPS URL creates clickable Markdown links in post body and metadata."""
    url = "https://example.com/research/paper.pdf"
    post = sample_reddit_post(post_id="p_https", is_self=False, url=url)
    body = build_reddit_post_body(post)

    assert f"**Link**: [{url}]({url})" in body
    assert f"- **External URL:** [{url}]({url})" in body


def test_external_url_validation_http():
    """Regression test: normal HTTP URL creates clickable Markdown links in post body and metadata."""
    url = "http://insecure.example.com/blog/post"
    post = sample_reddit_post(post_id="p_http", is_self=False, url=url)
    body = build_reddit_post_body(post)

    assert f"**Link**: [{url}]({url})" in body
    assert f"- **External URL:** [{url}]({url})" in body


def test_external_url_validation_unsafe_schemes():
    """Regression test: unsafe schemes (javascript:, data:, file:) are NOT rendered as Markdown links."""
    unsafe_urls = [
        "javascript:alert(document.cookie)",
        "data:text/html,<script>alert(1)</script>",
        "file:///etc/passwd",
    ]
    for unsafe_url in unsafe_urls:
        post = sample_reddit_post(post_id="p_unsafe", is_self=False, url=unsafe_url)
        body = build_reddit_post_body(post)

        # Must NOT be a clickable Markdown link
        assert f"[{unsafe_url}]({unsafe_url})" not in body
        assert f"[{unsafe_url}]" not in body
        # Instead rendered as plain text/escaped code
        assert unsafe_url in body
        assert f"`{unsafe_url}`" in body


def test_is_safe_http_url_helper():
    """Unit test for is_safe_http_url helper across various URL schemes."""
    assert is_safe_http_url("https://example.com") is True
    assert is_safe_http_url("http://example.com/test?a=1&b=2") is True
    assert is_safe_http_url("HTTP://EXAMPLE.COM") is True
    assert is_safe_http_url("javascript:alert(1)") is False
    assert is_safe_http_url("data:text/plain;base64,SGVsbG8=") is False
    assert is_safe_http_url("file:///local/file.txt") is False
    assert is_safe_http_url("ftp://ftp.example.com") is False
    assert is_safe_http_url("") is False
    assert is_safe_http_url(None) is False


