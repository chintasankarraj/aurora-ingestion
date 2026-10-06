"""Unit and integration tests for the Discord ingestion connector."""

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
from sources.discord_source import (
    DiscordSource,
    _extract_retry_after,
    _scrub_secrets,
    build_discord_message_body,
    format_discord_display_time,
    format_discord_embed,
    format_discord_timestamp,
    is_html_content,
    is_safe_http_url,
    map_discord_error,
    message_has_content_payload,
    normalize_discord_content,
    normalize_discord_text,
    resolve_discord_author,
    sanitize_attachment_filename,
)
from tracker import DeduplicationTracker, IngestionAction


class MockResponse:
    """Mock requests Response object for testing Discord API."""

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


def sample_discord_channel(
    cid: str = "111222333444555666",
    name: str = "general",
    c_type: int = 0,
    guild_id: str = "999888777666555444",
) -> Dict[str, Any]:
    return {
        "id": cid,
        "name": name,
        "type": c_type,
        "guild_id": guild_id,
    }


def sample_discord_user(
    uid: str = "123456789012345678",
    username: str = "alice",
    global_name: Optional[str] = None,
    bot: bool = False,
) -> Dict[str, Any]:
    return {
        "id": uid,
        "username": username,
        "global_name": global_name if global_name is not None else username,
        "bot": bot,
        "discriminator": "0",
    }


def sample_discord_message(
    mid: str = "234567890123456789",
    cid: str = "111222333444555666",
    content: str = "Hello from Discord!",
    author: Optional[Dict[str, Any]] = None,
    timestamp: str = "2026-10-06T10:30:00.000000+00:00",
    edited_timestamp: Optional[str] = None,
    msg_type: int = 0,
    attachments: Optional[List[Dict[str, Any]]] = None,
    embeds: Optional[List[Dict[str, Any]]] = None,
    mentions: Optional[List[Dict[str, Any]]] = None,
    thread: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "id": mid,
        "channel_id": cid,
        "content": content,
        "author": author or sample_discord_user(),
        "timestamp": timestamp,
        "edited_timestamp": edited_timestamp,
        "type": msg_type,
        "attachments": attachments or [],
        "embeds": embeds or [],
        "mentions": mentions or [],
        "thread": thread,
    }


# ===========================================================================
# 1. Authentication Tests
# ===========================================================================


class TestDiscordAuthentication:
    def test_token_loaded_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DISCORD_BOT_TOKEN", "test_bot_token_123")
        source = DiscordSource()
        assert source.token == "test_bot_token_123"

    def test_token_override_via_cli(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DISCORD_BOT_TOKEN", "env_token")
        source = DiscordSource(token="cli_token_override")
        assert source.token == "cli_token_override"

    def test_missing_token_raises_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
        source = DiscordSource(token="")
        with pytest.raises(SourceError, match="Discord bot token is required"):
            source._api_call("GET", "/channels/123/messages")

    def test_authorization_header_format(self) -> None:
        source = DiscordSource(token="valid_bot_token_xyz")
        headers = source._get_headers()
        assert headers["Authorization"] == "Bot valid_bot_token_xyz"
        assert "aurora-ingestion" in headers["User-Agent"]

    def test_bearer_user_token_rejected(self) -> None:
        with pytest.raises(SourceError, match="User/Bearer tokens are not supported"):
            DiscordSource(token="Bearer user_token_secret_123")

    def test_bot_prefix_stripped_if_supplied(self) -> None:
        source = DiscordSource(token="Bot secret_bot_token")
        assert source.token == "secret_bot_token"
        assert source._get_headers()["Authorization"] == "Bot secret_bot_token"

    def test_secret_scrubbing_redacts_tokens(self) -> None:
        raw = "Error with token Bot secret_token_abc and Bearer sensitive_123"
        scrubbed = _scrub_secrets(raw, ["secret_token_abc"])
        assert "secret_token_abc" not in scrubbed
        assert "[REDACTED]" in scrubbed


# ===========================================================================
# 2. Channel Resolution Tests
# ===========================================================================


class TestDiscordChannelResolution:
    def test_guild_channels_discovery(self) -> None:
        guild_id = "999888777666555444"
        channels_resp = [
            sample_discord_channel(cid="101", name="announcements", c_type=5, guild_id=guild_id),
            sample_discord_channel(cid="102", name="general", c_type=0, guild_id=guild_id),
            sample_discord_channel(cid="103", name="voice-chat", c_type=2, guild_id=guild_id),  # voice
            sample_discord_channel(cid="104", name="stage", c_type=13, guild_id=guild_id),      # stage
        ]
        session = MockSession([
            MockResponse(channels_resp),
            MockResponse({"id": guild_id, "name": "Test Server"}),
        ])
        source = DiscordSource(token="bot_token", guild=guild_id, session=session)
        resolved = source._resolve_channels([guild_id], [])
        assert len(resolved) == 2
        assert {ch["id"] for ch in resolved} == {"101", "102"}
        assert {ch["name"] for ch in resolved} == {"announcements", "general"}

    def test_channel_name_filtering_with_hash(self) -> None:
        guild_id = "999888777666555444"
        channels_resp = [
            sample_discord_channel(cid="101", name="general", c_type=0, guild_id=guild_id),
            sample_discord_channel(cid="102", name="dev", c_type=0, guild_id=guild_id),
        ]
        session = MockSession([
            MockResponse(channels_resp),
            MockResponse({"id": guild_id, "name": "Test Server"}),
        ])
        source = DiscordSource(token="bot_token", guild=guild_id, channel="#dev", session=session)
        resolved = source._resolve_channels([guild_id], ["#dev"])
        assert len(resolved) == 1
        assert resolved[0]["id"] == "102"
        assert resolved[0]["name"] == "dev"

    def test_direct_channel_id_without_guild(self) -> None:
        ch_obj = sample_discord_channel(cid="201", name="direct-general", c_type=0, guild_id="888")
        session = MockSession([
            MockResponse(ch_obj),
            MockResponse({"id": "888", "name": "Direct Server"}),
        ])
        source = DiscordSource(token="bot_token", channel="201", session=session)
        resolved = source._resolve_channels([], ["201"])
        assert len(resolved) == 1
        assert resolved[0]["id"] == "201"
        assert resolved[0]["name"] == "direct-general"

    def test_channel_name_without_guild_raises_error(self) -> None:
        source = DiscordSource(token="bot_token", channel="general")
        with pytest.raises(SourceError, match="requires --guild / DISCORD_GUILD_ID to resolve"):
            source._resolve_channels([], ["general"])

    def test_voice_channel_direct_is_skipped(self) -> None:
        ch_obj = sample_discord_channel(cid="301", name="lounge", c_type=2, guild_id="888")
        session = MockSession([MockResponse(ch_obj)])
        source = DiscordSource(token="bot_token", channel="301", session=session)
        resolved = source._resolve_channels([], ["301"])
        assert len(resolved) == 0

    def test_inaccessible_channel_fault_isolation(self) -> None:
        session = MockSession([
            MockResponse({"message": "Missing Access", "code": 50001}, status_code=403),
            MockResponse(sample_discord_channel(cid="502", name="accessible", c_type=0, guild_id="888")),
            MockResponse({"id": "888", "name": "Server"}),
        ])
        source = DiscordSource(token="bot_token", channels="501,502", session=session)
        resolved = source._resolve_channels([], ["501", "502"])
        assert len(resolved) == 1
        assert resolved[0]["id"] == "502"


# ===========================================================================
# 3. Message Types & Content Handling
# ===========================================================================


class TestDiscordMessageTypes:
    @pytest.mark.asyncio
    async def test_normal_message_ingestion(self) -> None:
        msg = sample_discord_message(content="Discussing project roadmap for Q4.")
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        item = items[0]
        assert item.source_id == "discord:message:111:" + msg["id"]
        assert "Discussing project roadmap for Q4." in item.content
        assert item.author == "alice"
        assert item.tags == ["ingested", "discord", "social"]

    @pytest.mark.asyncio
    async def test_empty_message_raises_intent_error(self) -> None:
        msg = sample_discord_message(content="", attachments=[], embeds=[], msg_type=0)
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        with pytest.raises(SourceError, match="MESSAGE CONTENT INTENT"):
            await source.fetch_items()

    @pytest.mark.asyncio
    async def test_empty_content_with_attachment_succeeds(self) -> None:
        att = {
            "id": "att1",
            "filename": "screenshot.png",
            "content_type": "image/png",
            "size": 10240,
            "url": "https://cdn.discordapp.com/attachments/111/att1/screenshot.png",
        }
        msg = sample_discord_message(content="", attachments=[att])
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "screenshot.png" in items[0].content
        assert "image/png" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_with_embed_succeeds(self) -> None:
        embed = {
            "title": "Release Notes v2.0",
            "description": "Major release with new features.",
            "url": "https://example.com/v2",
        }
        msg = sample_discord_message(content="", embeds=[embed])
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "Release Notes v2.0" in items[0].content
        assert "Major release with new features." in items[0].content

    @pytest.mark.asyncio
    async def test_edited_message_metadata(self) -> None:
        msg = sample_discord_message(
            content="Updated proposal text.",
            edited_timestamp="2026-10-06T11:00:00.000000+00:00",
        )
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert items[0].extra_metadata["edited_timestamp"] == "2026-10-06T11:00:00.000000+00:00"

    @pytest.mark.asyncio
    async def test_system_message_synthesis(self) -> None:
        msg = sample_discord_message(content="", msg_type=6)  # Pinned message
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "Pinned a message" in items[0].content

    @pytest.mark.asyncio
    async def test_unicode_and_markdown_preservation(self) -> None:
        unicode_content = (
            "✨ Testing multilingual support: 日本語, Español, العربية. "
            "**Bold**, *italic*, and `inline_code()`."
        )
        msg = sample_discord_message(content=unicode_content)
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "日本語" in items[0].content
        assert "**Bold**" in items[0].content
        assert "`inline_code()`" in items[0].content


# ===========================================================================
# 4. Message Pagination Tests
# ===========================================================================


class TestDiscordPagination:
    def test_message_history_pagination_walking_backwards(self) -> None:
        # 3 pages: 2 messages per page
        msg1 = sample_discord_message(mid="300", timestamp="2026-10-06T12:00:00Z")
        msg2 = sample_discord_message(mid="200", timestamp="2026-10-06T11:00:00Z")
        msg3 = sample_discord_message(mid="100", timestamp="2026-10-06T10:00:00Z")
        msg4 = sample_discord_message(mid="050", timestamp="2026-10-06T09:00:00Z")

        session = MockSession([
            MockResponse([msg1, msg2]),
            MockResponse([msg3, msg4]),
            MockResponse([]),
        ])
        source = DiscordSource(token="bot_token", page_size=2, session=session)
        messages = source._fetch_channel_messages("chan1", limit=10)

        assert len(messages) == 4
        # Verify chronological order (oldest first)
        assert [m["id"] for m in messages] == ["050", "100", "200", "300"]
        # Verify pagination call params
        assert session.calls[0]["kwargs"]["params"]["limit"] == 2
        assert session.calls[1]["kwargs"]["params"]["before"] == "200"

    def test_pagination_stops_at_configured_limit(self) -> None:
        msg1 = sample_discord_message(mid="300")
        msg2 = sample_discord_message(mid="200")
        msg3 = sample_discord_message(mid="100")

        session = MockSession([
            MockResponse([msg1, msg2]),
        ])
        source = DiscordSource(token="bot_token", session=session)
        messages = source._fetch_channel_messages("chan1", limit=2)
        assert len(messages) == 2
        # No second page requested because limit was met
        assert len(session.calls) == 1

    def test_pagination_repeated_cursor_protection(self) -> None:
        msg1 = sample_discord_message(mid="300")
        session = MockSession([
            MockResponse([msg1]),
            MockResponse([msg1]),  # Server returns same oldest message
        ])
        source = DiscordSource(token="bot_token", session=session)
        messages = source._fetch_channel_messages("chan1", limit=10)
        assert len(messages) == 1


# ===========================================================================
# 5. Thread Tests
# ===========================================================================


class TestDiscordThreads:
    @pytest.mark.asyncio
    async def test_thread_replies_fetched_and_ordered(self) -> None:
        root_msg = sample_discord_message(
            mid="500",
            content="Starting thread on feature design.",
            thread={"id": "500", "name": "Feature Design Thread"},
        )
        reply1 = sample_discord_message(
            mid="501",
            content="Reply 1: Looks good.",
            author=sample_discord_user(uid="201", username="bob"),
            timestamp="2026-10-06T10:35:00Z",
        )
        reply2 = sample_discord_message(
            mid="502",
            content="Reply 2: Added suggestions.",
            author=sample_discord_user(uid="202", username="charlie"),
            timestamp="2026-10-06T10:40:00Z",
        )

        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([root_msg]),               # Channel messages
            MockResponse([reply2, reply1]),         # Thread messages (newest first from API)
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        item = items[0]
        assert "## Thread" in item.content
        assert "@bob — 2026-10-06 10:35" in item.content
        assert "Reply 1: Looks good." in item.content
        assert "@charlie — 2026-10-06 10:40" in item.content
        assert "Reply 2: Added suggestions." in item.content
        assert item.extra_metadata["reply_count"] == 2

    @pytest.mark.asyncio
    async def test_thread_root_message_excluded_from_replies(self) -> None:
        root_msg = sample_discord_message(
            mid="600",
            content="Root thread message.",
            thread={"id": "600"},
        )
        reply = sample_discord_message(mid="601", content="Reply message.")
        # If Discord API includes root message in thread history response
        thread_resp = [reply, root_msg]

        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([root_msg]),
            MockResponse(thread_resp),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert items[0].extra_metadata["reply_count"] == 1

    @pytest.mark.asyncio
    async def test_thread_failure_preserves_root_message(self) -> None:
        root_msg = sample_discord_message(
            mid="700",
            content="Root message with failing thread.",
            thread={"id": "700"},
        )
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([root_msg]),
            MockResponse({"message": "Missing Access", "code": 50001}, status_code=403),
        ])
        source = DiscordSource(token="bot_token", channel="111", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "Root message with failing thread." in items[0].content
        assert "## Thread" not in items[0].content

    @pytest.mark.asyncio
    async def test_no_threads_flag_skips_thread_fetch(self) -> None:
        root_msg = sample_discord_message(
            mid="800",
            content="Root message.",
            thread={"id": "800"},
        )
        session = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="general", guild_id="999")),
            MockResponse({"id": "999", "name": "Aurora Guild"}),
            MockResponse([root_msg]),
        ])
        source = DiscordSource(token="bot_token", channel="111", no_threads=True, session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert len(session.calls) == 3  # channel, guild, messages (no thread call)


# ===========================================================================
# 6. Author Resolution Tests
# ===========================================================================


class TestDiscordAuthorResolution:
    def test_global_name_preferred_for_display(self) -> None:
        author = sample_discord_user(username="asmith", global_name="Alice Smith")
        disp, user, uid = resolve_discord_author(author)
        assert disp == "Alice Smith"
        assert user == "asmith"

    def test_username_fallback_when_global_name_missing(self) -> None:
        author = sample_discord_user(username="bob_builder", global_name="")
        disp, user, uid = resolve_discord_author(author)
        assert disp == "bob_builder"
        assert user == "bob_builder"

    def test_user_id_fallback_when_name_missing(self) -> None:
        author = {"id": "999111", "username": "", "global_name": ""}
        disp, user, uid = resolve_discord_author(author)
        assert disp == "999111"
        assert user == "999111"

    def test_missing_author_returns_unavailable(self) -> None:
        disp, user, uid = resolve_discord_author(None)
        assert disp == "[user unavailable]"
        assert user == "unknown"


# ===========================================================================
# 7. Mentions & Formatting Tests
# ===========================================================================


class TestDiscordMentionsAndFormatting:
    def test_user_mentions_resolved(self) -> None:
        raw = "Hey <@101> and <@!102>, check this out!"
        user_cache = {"101": "alice", "102": "bob"}
        normalized = normalize_discord_text(raw, user_cache=user_cache)
        assert normalized == "Hey @alice and @bob, check this out!"

    def test_unresolved_user_mention_fallback(self) -> None:
        raw = "Pinging <@999>."
        normalized = normalize_discord_text(raw, user_cache={})
        assert normalized == "Pinging @999."

    def test_channel_mention_resolved(self) -> None:
        raw = "Head over to <#201>."
        channel_cache = {"201": "announcements"}
        normalized = normalize_discord_text(raw, channel_cache=channel_cache)
        assert normalized == "Head over to #announcements."

    def test_role_mention_resolved(self) -> None:
        raw = "Calling <@&301>!"
        role_cache = {"301": "Admins"}
        normalized = normalize_discord_text(raw, role_cache=role_cache)
        assert normalized == "Calling @Admins!"

    def test_custom_emojis_rendered_cleanly(self) -> None:
        raw = "Great work! <:party_blob:401> and <a:dancing_cat:402>."
        normalized = normalize_discord_text(raw)
        assert normalized == "Great work! :party_blob: and :dancing_cat:."

    def test_html_entities_unescaped(self) -> None:
        raw = "Condition: &lt;tag&gt; and &amp; symbol."
        normalized = normalize_discord_text(raw)
        assert "Condition: <tag> and & symbol." in normalized


# ===========================================================================
# 8. URL Safety Tests
# ===========================================================================


class TestDiscordUrlSafety:
    def test_https_and_http_urls_are_safe(self) -> None:
        assert is_safe_http_url("https://example.com") is True
        assert is_safe_http_url("http://example.com/api?q=1") is True

    def test_unsafe_schemes_are_rejected(self) -> None:
        assert is_safe_http_url("javascript:alert(1)") is False
        assert is_safe_http_url("data:text/html;base64,PHNjcmlwdD4=") is False
        assert is_safe_http_url("file:///etc/passwd") is False
        assert is_safe_http_url("ftp://ftp.example.com") is False
        assert is_safe_http_url("") is False
        assert is_safe_http_url(None) is False

    def test_markdown_links_with_unsafe_schemes_not_clickable(self) -> None:
        text = "Check [Exploit](javascript:alert('xss')) and [File](file:///secret.txt)"
        norm = normalize_discord_text(text)
        assert "javascript:" in norm
        assert "[Exploit](javascript:" not in norm
        assert "[File](file:" not in norm

    def test_angle_bracket_unsafe_urls_rendered_as_code(self) -> None:
        text = "Visit <javascript:steal()> or <https://safe.com>"
        norm = normalize_discord_text(text)
        assert "`javascript:steal()`" in norm
        assert "[https://safe.com](https://safe.com)" in norm


# ===========================================================================
# 9. HTML Normalization & Comparisons
# ===========================================================================


class TestDiscordHtmlNormalization:
    def test_is_html_content_detects_real_html(self) -> None:
        assert is_html_content("<p>Hello world</p>") is True
        assert is_html_content("<div><span>Test</span></div>") is True
        assert is_html_content("<script>alert(1)</script>") is True

    def test_is_html_content_ignores_comparisons(self) -> None:
        assert is_html_content("a < b and c > d") is False
        assert is_html_content("x < 100") is False

    def test_is_html_content_ignores_code_blocks(self) -> None:
        assert is_html_content("```html\n<div>Code block</div>\n```") is False
        assert is_html_content("Run `x < y` comparison") is False

    def test_normalize_strips_script_and_style(self) -> None:
        raw = "<p>Welcome</p><script>alert('pwn')</script><style>body{color:red}</style>"
        clean = normalize_discord_content(raw)
        assert "alert" not in clean
        assert "color:red" not in clean
        assert "Welcome" in clean

    def test_normalize_preserves_code_fences(self) -> None:
        code_fence = "```python\ndef test():\n    return a < b\n```"
        norm = normalize_discord_content(code_fence)
        assert norm == code_fence


# ===========================================================================
# 10. Rate Limiting Tests (HTTP 429)
# ===========================================================================


class TestDiscordRateLimits:
    def test_rate_limit_with_retry_after_header(self) -> None:
        resp = MockResponse(
            {"message": "You are being rate limited.", "retry_after": 2.5, "global": False},
            status_code=429,
            headers={"Retry-After": "2.5", "X-RateLimit-Scope": "user"},
        )
        err = map_discord_error(Exception("Rate limited"), response=resp)
        assert "Discord API rate limit exceeded (HTTP 429)" in str(err)
        assert "Retry after 2.50s" in str(err)
        assert "scope: user" in str(err)

    def test_global_rate_limit_scope(self) -> None:
        resp = MockResponse(
            {"message": "You are being rate limited.", "retry_after": 5.0, "global": True},
            status_code=429,
            headers={"Retry-After": "5", "X-RateLimit-Global": "true"},
        )
        err = map_discord_error(Exception("Rate limited"), response=resp)
        assert "Retry after 5.00s" in str(err)
        assert "scope: global" in str(err)

    def test_shared_resource_scope(self) -> None:
        resp = MockResponse(
            {"message": "You are being rate limited.", "retry_after": 1.2},
            status_code=429,
            headers={"X-RateLimit-Scope": "shared"},
        )
        err = map_discord_error(Exception("Rate limited"), response=resp)
        assert "scope: shared" in str(err)


# ===========================================================================
# 11. API Error Mapping Tests
# ===========================================================================


class TestDiscordApiErrors:
    def test_401_unauthorized(self) -> None:
        resp = MockResponse({"message": "401: Unauthorized", "code": 0}, status_code=401)
        err = map_discord_error(Exception("Auth error"), response=resp)
        assert "Discord authentication failed (HTTP 401)" in str(err)

    def test_403_missing_access(self) -> None:
        resp = MockResponse({"message": "Missing Access", "code": 50001}, status_code=403)
        err = map_discord_error(Exception("Forbidden"), response=resp)
        assert "50001" in str(err) and "Missing Access" in str(err)

    def test_403_missing_permissions(self) -> None:
        resp = MockResponse({"message": "Missing Permissions", "code": 50013}, status_code=403)
        err = map_discord_error(Exception("Forbidden"), response=resp)
        assert "50013" in str(err) and "Missing Permissions" in str(err)

    def test_403_general_includes_message_content_guidance(self) -> None:
        resp = MockResponse({"message": "Forbidden", "code": 0}, status_code=403)
        err = map_discord_error(Exception("Forbidden"), response=resp)
        assert "MESSAGE CONTENT INTENT" in str(err)

    def test_404_unknown_channel(self) -> None:
        resp = MockResponse({"message": "Unknown Channel", "code": 10003}, status_code=404)
        err = map_discord_error(Exception("Not found"), response=resp)
        assert "10003" in str(err) and "Unknown Channel" in str(err)

    def test_404_unknown_guild(self) -> None:
        resp = MockResponse({"message": "Unknown Guild", "code": 10004}, status_code=404)
        err = map_discord_error(Exception("Not found"), response=resp)
        assert "10004" in str(err) and "Unknown Guild" in str(err)

    def test_500_server_error(self) -> None:
        resp = MockResponse({}, status_code=500)
        err = map_discord_error(Exception("Server error"), response=resp)
        assert "Discord server error (HTTP 500)" in str(err)

    def test_timeout_and_network_error(self) -> None:
        err1 = map_discord_error(requests.exceptions.Timeout("Read timed out"))
        assert "Discord request timed out" in str(err1)
        err2 = map_discord_error(requests.exceptions.ConnectionError("Failed to establish a new connection"))
        assert "Network error connecting to Discord API" in str(err2)


# ===========================================================================
# 12. Deduplication & Note Generation Tests
# ===========================================================================


class TestDiscordDeduplicationAndStorage:
    @pytest.mark.asyncio
    async def test_deduplication_lifecycle(self, tmp_path: Path) -> None:
        vault_path = tmp_path / "vault"
        vault_path.mkdir()
        db_path = tmp_path / "tracker.db"
        tracker = DeduplicationTracker(db_path)
        config = IngestionConfig(vault_path=vault_path, tracker_db_path=db_path)
        pipeline = IngestionPipeline(config=config, tracker=tracker)

        msg1 = sample_discord_message(mid="9001", cid="111", content="Initial version")
        msg1_edited = sample_discord_message(mid="9001", cid="111", content="Edited version")

        # 1. First run: NEW
        session1 = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Server"}),
            MockResponse([msg1]),
        ])
        source1 = DiscordSource(token="bot_token", channel="111", session=session1)
        items1 = await source1.fetch_items()
        action1, path1 = await pipeline.process_item(source1, items1[0])
        assert action1 == IngestionAction.NEW
        assert items1[0].source_id == "discord:message:111:9001"
        assert (vault_path / path1).exists()
        assert "Initial version" in (vault_path / path1).read_text(encoding="utf-8")

        # 2. Second run identical: UNCHANGED
        session2 = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Server"}),
            MockResponse([msg1]),
        ])
        source2 = DiscordSource(token="bot_token", channel="111", session=session2)
        items2 = await source2.fetch_items()
        action2, path2 = await pipeline.process_item(source2, items2[0])
        assert action2 == IngestionAction.UNCHANGED
        assert path2 == path1

        # 3. Third run edited: CHANGED
        session3 = MockSession([
            MockResponse(sample_discord_channel(cid="111", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Server"}),
            MockResponse([msg1_edited]),
        ])
        source3 = DiscordSource(token="bot_token", channel="111", session=session3)
        items3 = await source3.fetch_items()
        action3, path3 = await pipeline.process_item(source3, items3[0])
        assert action3 == IngestionAction.CHANGED
        assert path3 == path1
        content_updated = (vault_path / path1).read_text(encoding="utf-8")
        assert "Edited version" in content_updated

        tracker.close()

    @pytest.mark.asyncio
    async def test_convert_to_markdown_folder_routing(self) -> None:
        source = DiscordSource(token="bot_token")
        item = SourceItem(
            source_id="discord:message:111:222",
            title="Meeting Notes",
            source_type="discord",
            content="# Meeting Notes\n\nContent here",
            author="alice",
            date="2026-10-06T10:00:00Z",
            source_url="https://discord.com/channels/999/111/222",
            tags=["ingested", "discord", "social"],
        )
        note = await source.convert_to_markdown(item)
        assert note.folder == "Ingested/Social"
        assert note.source == "discord"

    def test_attribution_block_generation(self) -> None:
        note = MarkdownNote(
            title="Architecture Proposal",
            source="discord",
            date="2026-10-06T10:00:00Z",
            body="Proposal details",
            author="alice",
            source_url="https://discord.com/channels/999/111/222",
            extra_metadata={
                "guild": "Tech Guild",
                "channel": "architecture",
                "reply_count": 3,
            },
        )
        attr = build_attribution_block(note)
        assert "[Discord — Tech Guild / #architecture](https://discord.com/channels/999/111/222)" in attr
        assert "**Author**: @alice" in attr
        assert "**Date**: 2026-10-06" in attr
        assert "**Replies**: 3" in attr


# ===========================================================================
# 13. CLI Tests
# ===========================================================================


class TestDiscordCli:
    def test_cli_parser_ingest_discord_arguments(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-discord",
            "--guild", "999888777",
            "--channel", "111222333",
            "--token", "custom_bot_token",
            "--limit", "75",
            "--max-replies", "25",
            "--no-threads",
        ])
        assert args.command == "ingest-discord"
        assert args.guild == ["999888777"]
        assert args.channel == ["111222333"]
        assert args.token == "custom_bot_token"
        assert args.limit == 75
        assert args.max_replies == 25
        assert args.no_threads is True

    def test_source_registry_contains_discord(self) -> None:
        sources = SourceRegistry.list_sources()
        assert "discord" in sources
        assert SourceRegistry.get("discord") is DiscordSource


# ===========================================================================
# 14. Bounded 429 Retry and Rate Limiting Tests
# ===========================================================================


class TestDiscordRateLimitRetry:
    def test_api_call_429_single_retry_success(self) -> None:
        sleep_delays: List[float] = []

        def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        session = MockSession([
            MockResponse({"message": "Rate limited", "retry_after": 0.05}, status_code=429),
            MockResponse({"id": "123", "name": "general"}, status_code=200),
        ])

        source = DiscordSource(token="test_bot_token", session=session, max_retries=3, sleep_fn=fake_sleep)
        res = source._api_call("GET", "/channels/123")

        assert res == {"id": "123", "name": "general"}
        assert sleep_delays == [0.05]
        assert len(session.calls) == 2

    def test_api_call_429_retry_exhaustion_raises_source_error(self) -> None:
        sleep_delays: List[float] = []

        def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        session = MockSession([
            MockResponse({"message": "Rate limited", "retry_after": 0.02}, status_code=429),
            MockResponse({"message": "Rate limited", "retry_after": 0.02}, status_code=429),
            MockResponse({"message": "Rate limited", "retry_after": 0.02}, status_code=429),
            MockResponse({"message": "Rate limited", "retry_after": 0.02}, status_code=429),
        ])

        source = DiscordSource(token="test_bot_token", session=session, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source._api_call("GET", "/channels/123")

        assert "rate limit exceeded" in str(exc_info.value).lower()
        assert len(sleep_delays) == 3
        assert len(session.calls) == 4

    def test_api_call_non_429_errors_not_retried(self) -> None:
        sleep_delays: List[float] = []

        def fake_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        # 401 Unauthorized
        session_401 = MockSession([MockResponse({"message": "401"}, status_code=401)])
        source_401 = DiscordSource(token="test_bot_token", session=session_401, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source_401._api_call("GET", "/channels/123")
        assert "Discord authentication failed" in str(exc_info.value)
        assert len(sleep_delays) == 0

        # 403 Forbidden
        session_403 = MockSession([MockResponse({"code": 50001, "message": "Missing Access"}, status_code=403)])
        source_403 = DiscordSource(token="test_bot_token", session=session_403, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source_403._api_call("GET", "/channels/123")
        assert "Missing Access" in str(exc_info.value)
        assert len(sleep_delays) == 0

        # 404 Not Found
        session_404 = MockSession([MockResponse({"code": 10003, "message": "Unknown Channel"}, status_code=404)])
        source_404 = DiscordSource(token="test_bot_token", session=session_404, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source_404._api_call("GET", "/channels/123")
        assert "Unknown Channel" in str(exc_info.value)
        assert len(sleep_delays) == 0

        # 500 Internal Server Error
        session_500 = MockSession([MockResponse({"message": "Server Error"}, status_code=500)])
        source_500 = DiscordSource(token="test_bot_token", session=session_500, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source_500._api_call("GET", "/channels/123")
        assert "Discord server error" in str(exc_info.value)
        assert len(sleep_delays) == 0

    def test_extract_retry_after_precedence(self) -> None:
        # 1. JSON retry_after takes priority
        resp = MockResponse({"retry_after": 2.5}, status_code=429, headers={"Retry-After": "10"})
        assert _extract_retry_after(response=resp, json_body={"retry_after": 2.5}) == 2.5

        # 2. Header Retry-After used if JSON missing
        resp_hdr = MockResponse({}, status_code=429, headers={"Retry-After": "4.5"})
        assert _extract_retry_after(response=resp_hdr, json_body={}) == 4.5

        # 3. Default fallback to 1.0s if neither available
        resp_none = MockResponse({}, status_code=429, headers={})
        assert _extract_retry_after(response=resp_none, json_body={}) == 1.0


# ===========================================================================
# 15. Message Content Intent Validation Tests
# ===========================================================================


class TestDiscordMessageContentIntent:
    @pytest.mark.asyncio
    async def test_empty_content_with_components_does_not_raise(self) -> None:
        # Interactive components (e.g. buttons/selects) should not trigger intent error
        msg = sample_discord_message(mid="101", cid="202", content="")
        msg["components"] = [{"type": 1, "components": [{"type": 2, "label": "Click"}]}]

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "*[Interactive Component]*" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_with_stickers_does_not_raise(self) -> None:
        # Message with sticker_items should not trigger intent error
        msg = sample_discord_message(mid="102", cid="202", content="")
        msg["sticker_items"] = [{"id": "99", "name": "Wave"}]

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "*[Sticker: Wave]*" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_with_poll_does_not_raise(self) -> None:
        # Message with poll structure should not trigger intent error
        msg = sample_discord_message(mid="103", cid="202", content="")
        msg["poll"] = {"question": {"text": "What is your preference?"}}

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "*[Poll: What is your preference?]*" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_with_attachments_does_not_raise(self) -> None:
        msg = sample_discord_message(mid="104", cid="202", content="")
        msg["attachments"] = [{"id": "1", "filename": "photo.jpg", "url": "https://cdn.discordapp.com/photo.jpg"}]

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "photo.jpg" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_with_embeds_does_not_raise(self) -> None:
        msg = sample_discord_message(mid="105", cid="202", content="")
        msg["embeds"] = [{"title": "Article", "url": "https://example.com"}]

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        items = await source.fetch_items()
        assert len(items) == 1
        assert "Article" in items[0].content

    @pytest.mark.asyncio
    async def test_empty_content_on_user_message_without_payload_raises(self) -> None:
        msg = sample_discord_message(mid="106", cid="202", content="")
        msg["attachments"] = []
        msg["embeds"] = []

        session = MockSession([
            MockResponse(sample_discord_channel(cid="202", name="dev", guild_id="999")),
            MockResponse({"id": "999", "name": "Guild"}),
            MockResponse([msg]),
        ])
        source = DiscordSource(token="bot_token", channel="202", session=session)
        with pytest.raises(SourceError) as exc_info:
            await source.fetch_items()
        assert "MESSAGE CONTENT INTENT" in str(exc_info.value)

    def test_message_has_content_payload_helper(self) -> None:
        assert message_has_content_payload({"content": "hello"}) is True
        assert message_has_content_payload({"attachments": [{}]}) is True
        assert message_has_content_payload({"embeds": [{}]}) is True
        assert message_has_content_payload({"components": [{}]}) is True
        assert message_has_content_payload({"sticker_items": [{}]}) is True
        assert message_has_content_payload({"stickers": [{}]}) is True
        assert message_has_content_payload({"poll": {}}) is True
        assert message_has_content_payload({"content": "", "attachments": []}) is False
        assert message_has_content_payload({}) is False
        assert message_has_content_payload(None) is False


# ===========================================================================
# 16. Attachment Filename Hardening Tests
# ===========================================================================


class TestDiscordAttachmentHardening:
    def test_sanitize_attachment_filename(self) -> None:
        # Brackets and parentheses sanitized to prevent link breakout
        malicious = "report](https://evil.com/fake.pdf"
        sanitized = sanitize_attachment_filename(malicious)
        assert "[" not in sanitized
        assert "]" not in sanitized
        assert "(" not in sanitized
        assert ")" not in sanitized
        assert sanitized == "report__https://evil.com/fake.pdf"

        # Backticks and newlines stripped/replaced
        backtick_nl = "file `code` \r\n test.png"
        clean = sanitize_attachment_filename(backtick_nl)
        assert "`" not in clean
        assert "\r" not in clean
        assert "\n" not in clean
        assert clean == "file 'code' test.png"

        # None and empty defaults
        assert sanitize_attachment_filename(None) == "Attachment"
        assert sanitize_attachment_filename("   ") == "Attachment"

    def test_attachment_rendering_in_message_body(self) -> None:
        msg = sample_discord_message(mid="501", cid="202", content="See attached")
        msg["attachments"] = [
            {
                "id": "1",
                "filename": "quarterly](http://evil.com/doc).pdf",
                "content_type": "application/pdf",
                "size": 1024,
                "url": "https://cdn.discordapp.com/attachments/202/1/doc.pdf",
            }
        ]
        body = build_discord_message_body(
            msg,
            guild_name="Server",
            guild_id="999",
            channel_name="general",
            channel_id="202",
        )
        assert "### Attachments" in body
        # Verified that no unescaped or breaking bracket sequence appears in the link text
        assert "](http://evil.com" not in body
        assert "[quarterly__http://evil.com/doc_.pdf](https://cdn.discordapp.com/attachments/202/1/doc.pdf)" in body


# ===========================================================================
# 17. Defensive Embed Rendering Tests
# ===========================================================================


class TestDiscordEmbedHardening:
    def test_format_discord_embed_non_dict_inputs(self) -> None:
        assert format_discord_embed(None) is None
        assert format_discord_embed("string") is None
        assert format_discord_embed(12345) is None
        assert format_discord_embed([{"title": "embed"}]) is None

    def test_format_discord_embed_non_dict_provider(self) -> None:
        # provider is a string instead of dict
        emb = {"provider": "Google", "description": "Search engine"}
        rendered = format_discord_embed(emb)
        assert rendered is not None
        assert "Search engine" in rendered

    def test_format_discord_embed_non_string_fields(self) -> None:
        # title and description are non-string types
        emb = {"title": 12345, "description": 67890}
        rendered = format_discord_embed(emb)
        assert rendered is not None
        assert "12345" in rendered
        assert "67890" in rendered

    def test_format_discord_embed_unsafe_url_scheme(self) -> None:
        # javascript: or data: URLs must not be clickable markdown links
        emb = {"title": "XSS Probe", "url": "javascript:alert(document.cookie)"}
        rendered = format_discord_embed(emb)
        assert rendered is not None
        assert "[XSS Probe](" not in rendered
        assert "`XSS Probe`" in rendered

    def test_format_discord_embed_html_in_description(self) -> None:
        emb = {
            "title": "Clean Title",
            "description": "<p>Formatted <b>bold</b> text</p><script>alert(1)</script>",
            "url": "https://example.com/clean",
        }
        rendered = format_discord_embed(emb)
        assert rendered is not None
        assert "<script>" not in rendered
        assert "<b>" not in rendered
        assert "**bold**" in rendered or "bold" in rendered

    def test_malformed_embed_alongside_valid_embed_in_body(self) -> None:
        msg = sample_discord_message(mid="601", cid="202", content="Check embeds")
        msg["embeds"] = [
            None,
            "not a dict",
            {"title": "Valid Article", "url": "https://example.com/article", "description": "Good content"},
            {"broken": True},
        ]
        body = build_discord_message_body(
            msg,
            guild_name="Server",
            guild_id="999",
            channel_name="general",
            channel_id="202",
        )
        assert "### Embeds" in body
        assert "[Valid Article](https://example.com/article)" in body
        assert "Good content" in body

