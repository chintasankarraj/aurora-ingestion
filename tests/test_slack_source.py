"""Unit and integration tests for the Slack ingestion connector."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch
import pytest
import requests
import yaml

from config import IngestionConfig
from converter import (
    build_attribution_block,
    extract_date_prefix,
    generate_note_filename,
    render_markdown_document,
)
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.slack_source import (
    SlackSource,
    _scrub_secrets,
    build_slack_message_body,
    format_slack_display_time,
    format_slack_timestamp,
    is_html_content,
    is_safe_http_url,
    map_slack_error,
    normalize_slack_content,
    normalize_slack_text,
)
from tracker import DeduplicationTracker, IngestionAction


class MockResponse:
    """Mock requests Response object for testing Slack API."""

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
            return MockResponse({"ok": True})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def post(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"method": "POST", "url": url, "kwargs": kwargs})
        if not self.responses:
            return MockResponse({"ok": True})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def sample_channel(cid: str = "C12345678", name: str = "general", is_private: bool = False) -> Dict[str, Any]:
    return {
        "id": cid,
        "name": name,
        "is_channel": not is_private,
        "is_group": is_private,
        "is_private": is_private,
        "is_archived": False,
    }


def sample_user(uid: str = "U1001", name: str = "alice", real_name: str = "Alice Smith") -> Dict[str, Any]:
    return {
        "id": uid,
        "name": name,
        "deleted": False,
        "profile": {
            "display_name": name,
            "real_name": real_name,
        },
    }


def sample_message(
    ts: str = "1728212345.100000",
    user: str = "U1001",
    text: str = "Hello Slack team!",
    reply_count: int = 0,
    thread_ts: Optional[str] = None,
    subtype: Optional[str] = None,
    files: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    msg: Dict[str, Any] = {
        "type": "message",
        "ts": ts,
        "user": user,
        "text": text,
    }
    if thread_ts:
        msg["thread_ts"] = thread_ts
    if reply_count:
        msg["reply_count"] = reply_count
    if subtype:
        msg["subtype"] = subtype
    if files:
        msg["files"] = files
    return msg


# ===========================================================================
# 1. Authentication Tests
# ===========================================================================

def test_token_loading_param():
    src = SlackSource(token="xoxb-explicit-token")
    assert src.token == "xoxb-explicit-token"


def test_token_loading_env_slack_token(monkeypatch):
    monkeypatch.setenv("SLACK_TOKEN", "xoxb-env-token")
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    src = SlackSource()
    assert src.token == "xoxb-env-token"


def test_token_loading_env_slack_bot_token(monkeypatch):
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-bot-env-token")
    src = SlackSource()
    assert src.token == "xoxb-bot-env-token"


@pytest.mark.asyncio
async def test_missing_token_raises_error(monkeypatch):
    monkeypatch.delenv("SLACK_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    src = SlackSource(channels=["general"])
    with pytest.raises(SourceError, match="Slack authentication token missing"):
        await src.fetch_items()


def test_authorization_header():
    src = SlackSource(token="xoxb-secret-12345")
    headers = src._get_headers()
    assert headers["Authorization"] == "Bearer xoxb-secret-12345"
    assert headers["Accept"] == "application/json"


def test_secrets_scrubbing_in_errors_and_logs():
    token = "xoxb-verysecrettoken123"
    secrets = [token]
    scrubbed = _scrub_secrets(f"Error authenticating with {token} via Bearer {token}", secrets)
    assert token not in scrubbed
    assert "[REDACTED" in scrubbed

    err = map_slack_error(
        Exception(f"Call with {token} failed"),
        secrets=secrets,
    )
    assert token not in str(err)


# ===========================================================================
# 2. Conversation Discovery Tests
# ===========================================================================

def test_channel_lookup_by_name():
    ch_gen = sample_channel("C111", "general")
    ch_eng = sample_channel("C222", "engineering")
    session = MockSession([
        MockResponse({"ok": True, "channels": [ch_gen, ch_eng]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    resolved = src._resolve_target_channels(["general"])
    assert resolved == [("C111", "general")]


def test_channel_lookup_with_leading_hash():
    ch_gen = sample_channel("C111", "general")
    session = MockSession([
        MockResponse({"ok": True, "channels": [ch_gen]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    resolved = src._resolve_target_channels(["#general"])
    assert resolved == [("C111", "general")]


def test_channel_lookup_by_id():
    ch_gen = sample_channel("C111", "general")
    session = MockSession([
        MockResponse({"ok": True, "channels": [ch_gen]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    resolved = src._resolve_target_channels(["C111"])
    assert resolved == [("C111", "general")]


def test_channel_discovery_pagination():
    ch1 = sample_channel("C1", "ch1")
    ch2 = sample_channel("C2", "ch2")
    session = MockSession([
        MockResponse({
            "ok": True,
            "channels": [ch1],
            "response_metadata": {"next_cursor": "cur_page2"},
        }),
        MockResponse({
            "ok": True,
            "channels": [ch2],
            "response_metadata": {"next_cursor": ""},
        }),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    by_id, by_name = src._discover_channels()
    assert "C1" in by_id
    assert "C2" in by_id
    assert "ch1" in by_name
    assert "ch2" in by_name


def test_channel_unlisted_direct_id():
    session = MockSession([
        MockResponse({"ok": True, "channels": []}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    # Direct valid Slack channel ID format C12345678
    resolved = src._resolve_target_channels(["C98765432"])
    assert resolved == [("C98765432", "C98765432")]


def test_channel_missing_or_inaccessible(caplog):
    session = MockSession([
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    with caplog.at_level(logging.WARNING):
        resolved = src._resolve_target_channels(["nonexistent-channel"])
    assert resolved == []
    assert "could not be found or is inaccessible" in caplog.text


# ===========================================================================
# 3. Message Handling Tests
# ===========================================================================

def test_normal_message_parsing():
    msg = sample_message(ts="1728212345.100000", text="Standard team announcement")
    body = build_slack_message_body(
        msg_data=msg,
        channel_name="general",
        channel_id="C111",
        user_cache={"U1001": "alice"},
    )
    assert "# Standard team announcement" in body
    assert "> **Source**: [Slack — #general]" in body
    assert "> **Author**: @alice" in body
    assert "## Message\n\nStandard team announcement" in body


def test_edited_message_changed_subtype():
    orig_changed = {
        "type": "message",
        "subtype": "message_changed",
        "message": {
            "ts": "1728212345.100000",
            "user": "U1001",
            "text": "Edited and updated message content",
            "edited": {"user": "U1001", "ts": "1728212400.000000"},
        },
    }
    session = MockSession([
        MockResponse({"ok": True, "messages": [orig_changed]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    messages = src._fetch_channel_messages("C111")
    assert len(messages) == 1
    assert messages[0]["text"] == "Edited and updated message content"
    assert messages[0]["channel"] == "C111"


def test_deleted_message_skipped():
    del_msg = {
        "type": "message",
        "subtype": "message_deleted",
        "previous_message": {"text": "Will be ignored"},
    }
    norm_msg = sample_message(ts="1728212345.200000", text="Active note")
    session = MockSession([
        MockResponse({"ok": True, "messages": [del_msg, norm_msg]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    messages = src._fetch_channel_messages("C111")
    assert len(messages) == 1
    assert messages[0]["text"] == "Active note"


def test_bot_message_handling():
    bot_msg = {
        "type": "message",
        "subtype": "bot_message",
        "bot_id": "B999",
        "username": "deploy-bot",
        "text": "Deployment to production successful!",
        "ts": "1728212345.300000",
    }
    body = build_slack_message_body(
        msg_data=bot_msg,
        channel_name="releases",
        channel_id="C222",
    )
    assert "Deployment to production successful!" in body
    assert "@deploy-bot" in body


def test_system_join_leave_messages_filtered():
    join_msg = {"type": "message", "subtype": "channel_join", "user": "U1001", "ts": "1728212345.000001"}
    topic_msg = {"type": "message", "subtype": "channel_topic", "topic": "New topic", "ts": "1728212345.000002"}
    normal = sample_message(ts="1728212345.000003", text="Real content")
    session = MockSession([
        MockResponse({"ok": True, "messages": [join_msg, topic_msg, normal]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    messages = src._fetch_channel_messages("C111")
    assert len(messages) == 1
    assert messages[0]["text"] == "Real content"


def test_unicode_and_special_characters():
    text = "Deploying 🚀 to Tokyo 🗼: test_var = [1, 2, 3] & status = OK 🎉"
    msg = sample_message(text=text)
    body = build_slack_message_body(msg, "general", "C1")
    assert "Deploying 🚀 to Tokyo 🗼" in body
    assert "test_var = [1, 2, 3] & status = OK 🎉" in body


# ===========================================================================
# 4. Thread Handling Tests
# ===========================================================================

def test_threaded_message_with_replies():
    parent = sample_message(
        ts="1728212000.000000",
        user="U1",
        text="RFC: Proposed new architecture for data pipeline",
        reply_count=2,
        thread_ts="1728212000.000000",
    )
    rep1 = sample_message(ts="1728212100.000000", user="U2", text="Looks solid, but check memory limits.")
    rep2 = sample_message(ts="1728212200.000000", user="U3", text="+1, let's ship it.")

    body = build_slack_message_body(
        msg_data=parent,
        channel_name="eng",
        channel_id="C1",
        replies=[rep1, rep2],
        user_cache={"U1": "alice", "U2": "bob", "U3": "charlie"},
    )

    assert "## Message" in body
    assert "Proposed new architecture" in body
    assert "## Thread" in body
    assert "### @alice — " in body
    assert "#### @bob — " in body
    assert "Looks solid, but check memory limits." in body
    assert "#### @charlie — " in body
    assert "+1, let's ship it." in body


def test_thread_replies_chronological_ordering():
    # Out of order timestamps
    rep_later = sample_message(ts="1728212900.000000", text="Second reply")
    rep_earlier = sample_message(ts="1728212100.000000", text="First reply")

    session = MockSession([
        # conversations.replies returns parent first, then replies in any order
        MockResponse({
            "ok": True,
            "messages": [
                sample_message(ts="1728212000.000000", text="Parent"),
                rep_later,
                rep_earlier,
            ],
        }),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    replies = src._fetch_thread_replies("C1", "1728212000.000000")
    assert len(replies) == 2
    assert replies[0]["text"] == "First reply"
    assert replies[1]["text"] == "Second reply"


def test_thread_replies_pagination():
    parent = sample_message(ts="100.0", text="Parent")
    rep1 = sample_message(ts="101.0", text="Rep 1")
    rep2 = sample_message(ts="102.0", text="Rep 2")

    session = MockSession([
        MockResponse({
            "ok": True,
            "messages": [parent, rep1],
            "response_metadata": {"next_cursor": "cur_rep2"},
        }),
        MockResponse({
            "ok": True,
            "messages": [rep2],
            "response_metadata": {"next_cursor": ""},
        }),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    replies = src._fetch_thread_replies("C1", "100.0", max_replies=10)
    assert len(replies) == 2
    assert [r["text"] for r in replies] == ["Rep 1", "Rep 2"]


@pytest.mark.asyncio
async def test_no_threads_option_skips_replies():
    parent = sample_message(
        ts="1728212000.000000",
        text="Parent note",
        reply_count=5,
        thread_ts="1728212000.000000",
    )
    session = MockSession([
        MockResponse({"ok": True, "members": []}),  # users.list
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),  # conversations.list
        MockResponse({"ok": True, "messages": [parent]}),  # conversations.history
        # Notice: conversations.replies should NOT be called
    ])
    src = SlackSource(token="xoxb-tok", channels=["general"], session=session, include_threads=False)
    items = await src.fetch_items()
    assert len(items) == 1
    assert "## Thread" not in items[0].content


# ===========================================================================
# 5. User Resolution Tests
# ===========================================================================

def test_user_cache_users_list_pagination():
    u1 = sample_user("U1", "alice")
    u2 = sample_user("U2", "bob")
    session = MockSession([
        MockResponse({
            "ok": True,
            "members": [u1],
            "response_metadata": {"next_cursor": "cur_u2"},
        }),
        MockResponse({
            "ok": True,
            "members": [u2],
            "response_metadata": {"next_cursor": ""},
        }),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    src._load_user_cache()
    assert src._resolve_user("U1") == "alice"
    assert src._resolve_user("U2") == "bob"


def test_user_info_on_demand_fallback():
    session = MockSession([
        MockResponse({"ok": True, "user": sample_user("U999", "eve", "Eve Hacker")}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    res = src._resolve_user("U999")
    assert res == "eve"
    # Cached now
    assert src._user_cache["U999"] == "eve"


def test_user_lookup_failure_fallback():
    session = MockSession([
        MockResponse({"ok": False, "error": "user_not_found"}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    res = src._resolve_user("U_UNKNOWN")
    assert res == "U_UNKNOWN"


# ===========================================================================
# 6. Slack Markup Normalization Tests
# ===========================================================================

def test_normalize_user_and_channel_mentions():
    user_cache = {"U101": "alice"}
    channel_cache = {"C202": "dev"}
    text = "Hey <@U101>, please post the release notes in <#C202|general-dev> and check <!here>."
    res = normalize_slack_text(text, user_cache=user_cache, channel_cache=channel_cache)
    assert "@alice" in res
    assert "#general-dev" in res
    assert "@here" in res
    assert "<@" not in res


def test_normalize_channel_mention_without_label():
    channel_cache = {"C202": "dev-ops"}
    text = "Join <#C202>!"
    res = normalize_slack_text(text, channel_cache=channel_cache)
    assert "#dev-ops" in res


def test_normalize_http_and_https_links():
    text = "Read <https://example.com/guide|Developer Guide> and <http://aurora.local/docs>."
    res = normalize_slack_text(text)
    assert "[Developer Guide](https://example.com/guide)" in res
    assert "[http://aurora.local/docs](http://aurora.local/docs)" in res


def test_unsafe_url_schemes_not_rendered_as_clickable_links():
    text = (
        "Do not click: <javascript:alert(1)|Click Here> or <data:text/html;base64,PHNjcmlwdD4|DataURI> "
        "or <file:///etc/passwd|Secrets>."
    )
    res = normalize_slack_text(text)
    assert "[Click Here](javascript:" not in res
    assert "[DataURI](data:" not in res
    assert "[Secrets](file:" not in res
    assert "Click Here (`javascript:alert(1)`)" in res


def test_preserve_code_blocks_and_comparisons():
    text = (
        "Here is the comparison check:\n"
        "```python\n"
        "if a < b and c > d:\n"
        "    print('<@U123> is untouched inside code!')\n"
        "```\n"
        "Also note that threshold < 100 and count > 0."
    )
    res = normalize_slack_text(text)
    assert "if a < b and c > d:" in res
    assert "<@U123> is untouched inside code!" in res
    assert "threshold < 100" in res
    assert "count > 0" in res


def test_unescape_slack_html_entities():
    text = "Terms &amp; conditions: x &lt; y and a &gt; b."
    res = normalize_slack_text(text)
    assert "Terms & conditions:" in res
    assert "x < y" in res
    assert "a > b" in res


def test_strip_raw_html_markup():
    text = "Announcement: <script>evil()</script><p>Welcome to <b>Aurora</b>!</p>"
    res = normalize_slack_text(text)
    assert "<script>" not in res
    assert "evil()" not in res
    assert "<p>" not in res
    assert "**Aurora**" in res or "Aurora" in res


# ===========================================================================
# 7. Pagination & Cycle Protection Tests
# ===========================================================================

def test_repeated_cursor_cycle_protection(caplog):
    m1 = sample_message(ts="1.0", text="Msg 1")
    m2 = sample_message(ts="2.0", text="Msg 2")
    session = MockSession([
        MockResponse({"ok": True, "messages": [m1], "response_metadata": {"next_cursor": "loop_cursor"}}),
        # Returns the same cursor again
        MockResponse({"ok": True, "messages": [m2], "response_metadata": {"next_cursor": "loop_cursor"}}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    messages = src._fetch_channel_messages("C1", limit=10)
    assert len(messages) == 2


def test_limit_enforced_strictly():
    messages = [sample_message(ts=f"{i}.0", text=f"Msg {i}") for i in range(10)]
    session = MockSession([
        MockResponse({"ok": True, "messages": messages[:5], "response_metadata": {"next_cursor": "page2"}}),
        MockResponse({"ok": True, "messages": messages[5:], "response_metadata": {"next_cursor": ""}}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    fetched = src._fetch_channel_messages("C1", limit=3)
    assert len(fetched) == 3


def test_default_history_request_never_exceeds_15():
    session = MockSession([
        MockResponse({"ok": True, "messages": []}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    src._fetch_channel_messages("C1")
    assert len(session.calls) == 1
    req_limit = session.calls[0]["kwargs"]["params"]["limit"]
    assert req_limit <= 15
    assert req_limit == 15


def test_configured_limit_100_requests_at_most_15_per_page():
    # 100 messages total across 7 pages: 6 pages of 15 + 1 page of 10
    responses = []
    total_messages = 100
    generated = 0
    page = 1
    while generated < total_messages:
        batch_size = min(15, total_messages - generated)
        msgs = [sample_message(ts=f"{generated + i}.0", text=f"Msg {generated + i}") for i in range(batch_size)]
        generated += batch_size
        next_cursor = f"cursor_page_{page+1}" if generated < total_messages else ""
        responses.append(MockResponse({
            "ok": True,
            "messages": msgs,
            "response_metadata": {"next_cursor": next_cursor},
        }))
        page += 1

    session = MockSession(responses)
    src = SlackSource(token="xoxb-tok", session=session)
    fetched = src._fetch_channel_messages("C1", limit=100)

    assert len(fetched) == 100
    assert len(session.calls) == 7
    call_limits = [call["kwargs"]["params"]["limit"] for call in session.calls]
    assert call_limits == [15, 15, 15, 15, 15, 15, 10]
    assert all(lim <= 15 for lim in call_limits)


def test_default_thread_request_never_exceeds_15():
    session = MockSession([
        MockResponse({"ok": True, "messages": [sample_message(ts="100.0", text="Parent")]}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    src._fetch_thread_replies("C1", "100.0")
    assert len(session.calls) == 1
    req_limit = session.calls[0]["kwargs"]["params"]["limit"]
    assert req_limit <= 15


def test_configured_max_replies_50_requests_at_most_15_per_page():
    # 50 replies total across 4 pages: 15, 15, 15, 5
    responses = []
    total_replies = 50
    generated = 0
    page = 1
    while generated < total_replies:
        batch_size = min(15, total_replies - generated)
        replies = [sample_message(ts=f"{1000 + generated + i}.0", text=f"Reply {generated + i}") for i in range(batch_size)]
        generated += batch_size
        next_cursor = f"cursor_rep_{page+1}" if generated < total_replies else ""
        responses.append(MockResponse({
            "ok": True,
            "messages": replies,
            "response_metadata": {"next_cursor": next_cursor},
        }))
        page += 1

    session = MockSession(responses)
    src = SlackSource(token="xoxb-tok", session=session)
    fetched = src._fetch_thread_replies("C1", "100.0", max_replies=50)

    assert len(fetched) == 50
    assert len(session.calls) == 4
    call_limits = [call["kwargs"]["params"]["limit"] for call in session.calls]
    assert call_limits == [15, 15, 15, 5]
    assert all(lim <= 15 for lim in call_limits)


def test_multiple_pages_accumulate_until_overall_limit_reached():
    # Ensure limit of 35 accumulates across 3 pages (15, 15, 5)
    msgs_p1 = [sample_message(ts=f"1.{i}", text=f"P1-{i}") for i in range(15)]
    msgs_p2 = [sample_message(ts=f"2.{i}", text=f"P2-{i}") for i in range(15)]
    msgs_p3 = [sample_message(ts=f"3.{i}", text=f"P3-{i}") for i in range(5)]

    session = MockSession([
        MockResponse({"ok": True, "messages": msgs_p1, "response_metadata": {"next_cursor": "cur2"}}),
        MockResponse({"ok": True, "messages": msgs_p2, "response_metadata": {"next_cursor": "cur3"}}),
        MockResponse({"ok": True, "messages": msgs_p3, "response_metadata": {"next_cursor": ""}}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    fetched = src._fetch_channel_messages("C1", limit=35)

    assert len(fetched) == 35
    assert [call["kwargs"]["params"]["limit"] for call in session.calls] == [15, 15, 5]
    assert all(call["kwargs"]["params"]["limit"] <= 15 for call in session.calls)


# ===========================================================================
# 8. Rate Limit Handling Tests
# ===========================================================================

def test_http_429_rate_limiting_with_retry_after():
    session = MockSession([
        MockResponse(
            {"ok": False, "error": "ratelimited"},
            status_code=429,
            headers={"Retry-After": "45"},
        ),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    with pytest.raises(SourceError, match=r"Rate limit resets in 45s"):
        src._api_call("GET", "conversations.list")


def test_slack_ratelimited_error_json():
    session = MockSession([
        MockResponse(
            {"ok": False, "error": "ratelimited"},
            status_code=200,
            headers={"Retry-After": "60"},
        ),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    with pytest.raises(SourceError, match=r"Rate limit resets in 60s"):
        src._api_call("GET", "conversations.history", params={"channel": "C1"})


# ===========================================================================
# 9. Slack API Error Mapping Tests
# ===========================================================================

@pytest.mark.parametrize(
    "error_code,match_str",
    [
        ("invalid_auth", "Slack authentication failed"),
        ("not_authed", "Slack authentication failed"),
        ("token_revoked", "Token has been revoked"),
        ("missing_scope", "Insufficient OAuth scopes"),
        ("channel_not_found", "Slack channel not found"),
        ("not_in_channel", "Slack bot is not a member of the channel"),
        ("is_archived", "channel is archived"),
        ("restricted_action", "restricted by workspace policy"),
    ],
)
def test_slack_api_errors(error_code, match_str):
    session = MockSession([
        MockResponse({"ok": False, "error": error_code, "needed": "channels:history"}),
    ])
    src = SlackSource(token="xoxb-tok", session=session)
    with pytest.raises(SourceError, match=match_str):
        src._api_call("GET", "conversations.history", params={"channel": "C1"})


# ===========================================================================
# 10. Identity & Deduplication Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_stable_identity_and_deduplication(tmp_path):
    vault_path = tmp_path / "vault"
    vault_path.mkdir()
    tracker_path = tmp_path / "tracker.sqlite"
    tracker = DeduplicationTracker(tracker_path)
    config = IngestionConfig(vault_path=vault_path, tracker_db_path=tracker_path)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    parent = sample_message(ts="1728212000.123456", text="Original parent note")

    session = MockSession([
        # Run 1
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice")]}),
        MockResponse({"ok": True, "channels": [sample_channel("C100", "general")]}),
        MockResponse({"ok": True, "messages": [parent]}),

        # Run 2: Unchanged
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice")]}),
        MockResponse({"ok": True, "channels": [sample_channel("C100", "general")]}),
        MockResponse({"ok": True, "messages": [parent]}),

        # Run 3: Modified/edited
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice")]}),
        MockResponse({"ok": True, "channels": [sample_channel("C100", "general")]}),
        MockResponse({
            "ok": True,
            "messages": [
                sample_message(
                    ts="1728212000.123456",
                    text="Updated parent message with edits",
                )
            ],
        }),
    ])

    src = SlackSource(token="xoxb-tok", channels=["general"], session=session)

    # Run 1: NEW
    items1 = await src.fetch_items()
    action1, path1 = await pipeline.process_item(src, items1[0])
    assert action1 == IngestionAction.NEW
    assert items1[0].source_id == "slack:message:C100:1728212000.123456"
    assert (vault_path / path1).exists()
    assert "Original parent note" in (vault_path / path1).read_text(encoding="utf-8")

    # Run 2: UNCHANGED
    items2 = await src.fetch_items()
    action2, path2 = await pipeline.process_item(src, items2[0])
    assert action2 == IngestionAction.UNCHANGED
    assert path2 == path1

    # Run 3: CHANGED -> updates same note path
    items3 = await src.fetch_items()
    action3, path3 = await pipeline.process_item(src, items3[0])
    assert action3 == IngestionAction.CHANGED
    assert path3 == path1
    content_updated = (vault_path / path1).read_text(encoding="utf-8")
    assert "Updated parent message with edits" in content_updated

    tracker.close()


@pytest.mark.asyncio
async def test_thread_reply_updates_existing_note(tmp_path):
    vault_path = tmp_path / "vault"
    vault_path.mkdir()
    tracker_path = tmp_path / "tracker.sqlite"
    tracker = DeduplicationTracker(tracker_path)
    config = IngestionConfig(vault_path=vault_path, tracker_db_path=tracker_path)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    parent_initial = sample_message(
        ts="1728212000.000000",
        text="Thread root discussion",
        reply_count=0,
        thread_ts="1728212000.000000",
    )
    parent_with_replies = sample_message(
        ts="1728212000.000000",
        text="Thread root discussion",
        reply_count=1,
        thread_ts="1728212000.000000",
    )
    reply1 = sample_message(ts="1728212500.000000", user="U2", text="New reply joined!")

    session = MockSession([
        # Run 1: initial without replies
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice"), sample_user("U2", "bob")]}),
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),
        MockResponse({"ok": True, "messages": [parent_initial]}),

        # Run 2: new reply added
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice"), sample_user("U2", "bob")]}),
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),
        MockResponse({"ok": True, "messages": [parent_with_replies]}),
        MockResponse({"ok": True, "messages": [parent_with_replies, reply1]}),
    ])

    src = SlackSource(token="xoxb-tok", channels=["general"], session=session)

    # First ingest
    items1 = await src.fetch_items()
    action1, path1 = await pipeline.process_item(src, items1[0])
    assert action1 == IngestionAction.NEW

    # Second ingest after reply
    items2 = await src.fetch_items()
    action2, path2 = await pipeline.process_item(src, items2[0])
    assert action2 == IngestionAction.CHANGED
    assert path2 == path1
    assert "New reply joined!" in (vault_path / path1).read_text(encoding="utf-8")

    tracker.close()


# ===========================================================================
# 11. Frontmatter, Routing & Vault Placement Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_frontmatter_and_vault_placement(tmp_path):
    vault_path = tmp_path / "vault"
    vault_path.mkdir()
    config = IngestionConfig(vault_path=vault_path)

    item = SourceItem(
        source_id="slack:message:C1:1728212000.000000",
        title="Weekly sync roadmap",
        source_type="slack",
        content="# Weekly sync roadmap\n\n> **Source**: [Slack — #general](https://slack.com)\n\n## Message\n\nSync updates.",
        author="alice",
        date="2026-10-06T10:00:00+00:00",
        source_url="https://slack.com/archives/C1/p1728212000000000",
        tags=["ingested", "slack", "social"],
        extra_metadata={
            "channel": "general",
            "channel_id": "C1",
            "author": "alice",
            "reply_count": 3,
        },
    )

    src = SlackSource(token="xoxb-tok")
    note = await src.convert_to_markdown(item)

    assert note.folder == "Ingested/Social"
    assert note.tags == ["ingested", "slack", "social"]

    doc = render_markdown_document(note)
    assert doc.startswith("---\n")
    parts = doc.split("---", 2)
    fm = yaml.safe_load(parts[1])
    assert fm["source"] == "slack"
    assert "ingested" in fm["tags"]
    assert fm["title"] == "Weekly sync roadmap"

    # Attribution block formatting
    attr = build_attribution_block(note)
    assert "> **Source**: [Slack — #general]" in attr
    assert "**Author**: @alice" in attr
    assert "**Replies**: 3" in attr


# ===========================================================================
# 12. Fault Isolation Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_one_channel_failure_does_not_abort_other_channels(caplog):
    ch_good = sample_channel("C_GOOD", "good-channel")
    ch_bad = sample_channel("C_BAD", "bad-channel")

    session = MockSession([
        MockResponse({"ok": True, "members": [sample_user("U1001", "alice")]}),
        MockResponse({"ok": True, "channels": [ch_good, ch_bad]}),
        # Request for C_GOOD succeeds
        MockResponse({"ok": True, "messages": [sample_message(ts="10.0", text="Message in good channel")]}),
        # Request for C_BAD fails with not_in_channel
        MockResponse({"ok": False, "error": "not_in_channel"}),
    ])

    src = SlackSource(token="xoxb-tok", channels=["good-channel", "bad-channel"], session=session)
    with caplog.at_level(logging.WARNING):
        items = await src.fetch_items()

    assert len(items) == 1
    assert items[0].title == "Message in good channel"
    assert "Failed to fetch messages for Slack channel bad-channel" in caplog.text


@pytest.mark.asyncio
async def test_thread_reply_failure_preserves_parent_message(caplog):
    parent = sample_message(ts="10.0", text="Parent with failed replies", reply_count=4)

    session = MockSession([
        MockResponse({"ok": True, "members": []}),
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),
        MockResponse({"ok": True, "messages": [parent]}),
        # Thread replies fails
        MockResponse({"ok": False, "error": "internal_error"}),
    ])

    src = SlackSource(token="xoxb-tok", channels=["general"], session=session)
    with caplog.at_level(logging.WARNING):
        items = await src.fetch_items()

    assert len(items) == 1
    assert "Parent with failed replies" in items[0].content
    assert "Failed to fetch thread replies" in caplog.text


# ===========================================================================
# 13. CLI Tests
# ===========================================================================

def test_cli_parser_options():
    parser = create_parser()

    # ingest-slack command
    args = parser.parse_args([
        "ingest-slack",
        "--channel", "general",
        "--channels", "eng,random",
        "--token", "xoxb-test",
        "--limit", "15",
        "--max-messages", "15",
        "--max-replies", "25",
        "--no-threads",
    ])
    assert args.command == "ingest-slack"
    assert args.channel == ["general"]
    assert args.channels == "eng,random"
    assert args.token == "xoxb-test"
    assert args.limit == 15
    assert args.max_messages == 15
    assert args.max_replies == 25
    assert args.no_threads is True


def test_cli_generic_ingest_source_slack():
    parser = create_parser()
    args = parser.parse_args([
        "ingest",
        "--source", "slack",
        "--channel", "announcements",
        "--no-threads",
    ])
    assert args.command == "ingest"
    assert args.source == "slack"
    assert args.channel == ["announcements"]
    assert args.no_threads is True


def test_list_sources_shows_slack():
    sources = SourceRegistry.list_sources()
    assert "slack" in sources
    assert sources["slack"] == "SlackSource"


@pytest.mark.asyncio
async def test_cli_async_main_ingest_slack_success(tmp_path, monkeypatch):
    vault_path = tmp_path / "vault"
    vault_path.mkdir()
    tracker_path = tmp_path / "tracker.sqlite"

    parser = create_parser()
    args = parser.parse_args([
        "--vault-path", str(vault_path),
        "--tracker-db", str(tracker_path),
        "ingest-slack",
        "--channel", "general",
        "--token", "xoxb-mock",
    ])

    msg = sample_message(ts="1728212000.000000", text="CLI Slack ingestion message")
    session = MockSession([
        MockResponse({"ok": True, "members": []}),
        MockResponse({"ok": True, "channels": [sample_channel("C1", "general")]}),
        MockResponse({"ok": True, "messages": [msg]}),
    ])

    with patch("sources.slack_source.requests.Session", return_value=session):
        exit_code = await async_main(args)

    assert exit_code == 0
    saved_notes = list((vault_path / "Ingested" / "Social").glob("*.md"))
    assert len(saved_notes) == 1
    assert "CLI Slack ingestion message" in saved_notes[0].read_text(encoding="utf-8")
