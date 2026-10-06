"""Unit and integration tests for the Telegram ingestion connector."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import pytest
import requests
import yaml

from config import IngestionConfig
from converter import (
    build_attribution_block,
    extract_date_prefix,
    render_markdown_document,
)
from exceptions import SourceError
from main import IngestionPipeline, create_parser
from models import MarkdownNote, SourceItem
from sources.base import SourceRegistry
from sources.telegram_source import (
    TelegramSource,
    _extract_retry_after,
    _scrub_secrets,
    build_telegram_message_body,
    build_utf16_to_codepoint_map,
    extract_telegram_media_metadata,
    format_telegram_display_time,
    format_telegram_timestamp,
    is_html_content,
    is_safe_http_url,
    map_telegram_error,
    normalize_telegram_content,
    render_telegram_entities,
    resolve_telegram_author,
)
from tracker import DeduplicationTracker, IngestionAction


class MockResponse:
    """Mock requests Response object for testing Telegram Bot API."""

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
            return MockResponse({"ok": True, "result": []})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    def post(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"method": "POST", "url": url, "kwargs": kwargs})
        if not self.responses:
            return MockResponse({"ok": True, "result": []})
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def sample_telegram_update(
    update_id: int = 1000,
    update_type: str = "message",
    msg_id: int = 42,
    chat_id: Union[int, str] = -1001234567890,
    chat_title: str = "Announcements",
    chat_username: str = "my_channel",
    chat_type: str = "channel",
    text: Optional[str] = "Hello world",
    entities: Optional[List[Dict[str, Any]]] = None,
    caption: Optional[str] = None,
    caption_entities: Optional[List[Dict[str, Any]]] = None,
    date: int = 1775563200,
    from_user: Optional[Dict[str, Any]] = None,
    reply_to_message: Optional[Dict[str, Any]] = None,
    forward_date: Optional[int] = None,
    forward_from: Optional[Dict[str, Any]] = None,
    forward_from_chat: Optional[Dict[str, Any]] = None,
    media: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Helper to build a mock Telegram update."""
    msg: Dict[str, Any] = {
        "message_id": msg_id,
        "date": date,
        "chat": {
            "id": chat_id,
            "title": chat_title,
            "username": chat_username,
            "type": chat_type,
        },
    }
    if text is not None:
        msg["text"] = text
    if entities is not None:
        msg["entities"] = entities
    if caption is not None:
        msg["caption"] = caption
    if caption_entities is not None:
        msg["caption_entities"] = caption_entities
    if from_user is not None:
        msg["from"] = from_user
    if reply_to_message is not None:
        msg["reply_to_message"] = reply_to_message
    if forward_date is not None:
        msg["forward_date"] = forward_date
    if forward_from is not None:
        msg["forward_from"] = forward_from
    if forward_from_chat is not None:
        msg["forward_from_chat"] = forward_from_chat
    if media:
        msg.update(media)

    return {
        "update_id": update_id,
        update_type: msg,
    }


# ===========================================================================
# 1. Authentication & Secret Safety Tests
# ===========================================================================


class TestTelegramAuthAndSecrets:
    def test_token_from_parameter(self) -> None:
        source = TelegramSource(token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11")
        assert source.token == "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"

    def test_token_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "654321:XYZ-bot-token")
        source = TelegramSource()
        assert source.token == "654321:XYZ-bot-token"

    def test_token_bot_prefix_stripped(self) -> None:
        source = TelegramSource(token="bot123456:ABC-DEF")
        assert source.token == "123456:ABC-DEF"

    def test_missing_token_raises_source_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
        source = TelegramSource(token=None, chat="-1001234567890")
        with pytest.raises(SourceError) as exc_info:
            source._api_call("GET", "getUpdates")
        assert "bot token is required" in str(exc_info.value).lower()

    def test_api_call_url_structure(self) -> None:
        session = MockSession([
            MockResponse({"ok": True, "result": []}),
        ])
        source = TelegramSource(
            token="123456:SECRET_TOKEN",
            chat="-1001234567890",
            session=session,
        )
        source._api_call("GET", "getUpdates", params={"limit": 10})
        assert len(session.calls) == 1
        call_url = session.calls[0]["url"]
        assert call_url == "https://api.telegram.org/bot123456:SECRET_TOKEN/getUpdates"
        assert session.calls[0]["kwargs"]["params"] == {"limit": 10}

    def test_secrets_scrubbed_from_strings_and_urls(self) -> None:
        token = "123456:SECRET_TOKEN_999"
        raw_msg = f"Failed to call https://api.telegram.org/bot{token}/getUpdates: bot{token} error"
        scrubbed = _scrub_secrets(raw_msg, [token])
        assert token not in scrubbed
        assert "https://api.telegram.org/bot[REDACTED]/getUpdates" in scrubbed
        assert "[REDACTED]" in scrubbed

    def test_token_not_leaked_in_exceptions(self) -> None:
        token = "999888:SUPER_SECRET_VALUE"
        source = TelegramSource(token=token, chat="-100123")
        exc = requests.exceptions.HTTPError(f"404 Client Error for https://api.telegram.org/bot{token}/test")
        mapped = map_telegram_error(exc, secrets=source._secrets)
        assert token not in str(mapped)
        assert "[REDACTED]" in str(mapped)


# ===========================================================================
# 2. Chat Filtering Tests
# ===========================================================================


class TestTelegramChatFiltering:
    def test_missing_configured_chats_raises_source_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
        monkeypatch.delenv("TELEGRAM_CHAT_IDS", raising=False)
        source = TelegramSource(token="fake_token")
        with pytest.raises(SourceError) as exc_info:
            import asyncio
            asyncio.run(source.fetch_items())
        assert "No Telegram chat specified" in str(exc_info.value)

    def test_numeric_negative_and_positive_chat_id_matching(self) -> None:
        source = TelegramSource(token="fake_token", chats="-1001234567890, 987654321")
        assert "-1001234567890" in source.configured_chats
        assert "987654321" in source.configured_chats

        # Match negative supergroup
        assert source._matches_chat({"id": -1001234567890}, source.configured_chats) is True
        # Match positive private chat
        assert source._matches_chat({"id": 987654321}, source.configured_chats) is True
        # Unconfigured chat ignored
        assert source._matches_chat({"id": -1009999999999}, source.configured_chats) is False

    def test_username_chat_matching(self) -> None:
        source = TelegramSource(token="fake_token", chat="@my_channel")
        assert source._matches_chat({"username": "my_channel"}, source.configured_chats) is True
        assert source._matches_chat({"username": "@my_channel"}, source.configured_chats) is True
        assert source._matches_chat({"username": "other_channel"}, source.configured_chats) is False

    def test_title_chat_matching(self) -> None:
        source = TelegramSource(token="fake_token", chat="Engineering Core")
        assert source._matches_chat({"title": "Engineering Core"}, source.configured_chats) is True
        assert source._matches_chat({"title": "engineering core"}, source.configured_chats) is True
        assert source._matches_chat({"title": "Marketing"}, source.configured_chats) is False


# ===========================================================================
# 3. Update Types & Ingestion Tests
# ===========================================================================


class TestTelegramUpdateTypes:
    @pytest.mark.asyncio
    async def test_normal_message_update(self) -> None:
        upd = sample_telegram_update(
            update_id=1,
            update_type="message",
            msg_id=101,
            chat_id=-100123,
            chat_title="Dev Channel",
            chat_username="dev_chan",
            text="First message in chat",
            from_user={"id": 1, "first_name": "Alice", "username": "alice"},
        )
        session = MockSession([
            MockResponse({"ok": True, "result": [upd]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items()

        assert len(items) == 1
        assert items[0].source_id == "telegram:message:-100123:101"
        assert items[0].title == "First message in chat"
        assert items[0].source_type == "telegram"
        assert "First message in chat" in items[0].content
        assert items[0].author == "@alice"
        assert items[0].source_url == "https://t.me/dev_chan/101"
        assert items[0].tags == ["ingested", "telegram", "social"]

    @pytest.mark.asyncio
    async def test_edited_message_update_preserves_same_source_id(self) -> None:
        upd = sample_telegram_update(
            update_id=2,
            update_type="edited_message",
            msg_id=101,
            chat_id=-100123,
            text="Edited version of message",
        )
        session = MockSession([
            MockResponse({"ok": True, "result": [upd]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items()

        assert len(items) == 1
        assert items[0].source_id == "telegram:message:-100123:101"
        assert items[0].title == "Edited version of message"
        assert items[0].extra_metadata["edited"] is True

    @pytest.mark.asyncio
    async def test_channel_post_and_edited_channel_post(self) -> None:
        post1 = sample_telegram_update(
            update_id=3,
            update_type="channel_post",
            msg_id=201,
            chat_id=-100555,
            chat_title="News Channel",
            chat_username="news_chan",
            text="Channel announcement",
        )
        post2 = sample_telegram_update(
            update_id=4,
            update_type="edited_channel_post",
            msg_id=202,
            chat_id=-100555,
            chat_title="News Channel",
            chat_username="news_chan",
            text="Updated announcement",
        )
        session = MockSession([
            MockResponse({"ok": True, "result": [post1, post2]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100555", session=session)
        items = await source.fetch_items()

        assert len(items) == 2
        assert items[0].source_id == "telegram:message:-100555:201"
        assert items[1].source_id == "telegram:message:-100555:202"
        assert items[1].extra_metadata["edited"] is True

    @pytest.mark.asyncio
    async def test_unsupported_updates_safely_ignored(self) -> None:
        # poll, callback_query, inline_query should be ignored without failing the run
        upds = [
            {"update_id": 10, "inline_query": {"id": "1", "query": "test"}},
            {"update_id": 11, "callback_query": {"id": "2", "data": "btn"}},
            sample_telegram_update(update_id=12, msg_id=301, chat_id="-100123", text="Valid message"),
            {"update_id": 13, "my_chat_member": {"chat": {"id": -100123}, "status": "administrator"}},
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": upds}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items()

        assert len(items) == 1
        assert items[0].source_id == "telegram:message:-100123:301"

    @pytest.mark.asyncio
    async def test_duplicate_updates_in_batch_deduplicated(self) -> None:
        upd = sample_telegram_update(update_id=20, msg_id=401, chat_id="-100123", text="Hello duplicate")
        session = MockSession([
            MockResponse({"ok": True, "result": [upd, upd]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items()

        assert len(items) == 1


# ===========================================================================
# 4. Polling & getUpdates Offset Handling
# ===========================================================================


class TestTelegramPollingAndOffsets:
    @pytest.mark.asyncio
    async def test_offset_advances_to_highest_update_id_plus_one(self) -> None:
        batch1 = [
            sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Msg 1"),
            sample_telegram_update(update_id=101, msg_id=2, chat_id="-100123", text="Msg 2"),
        ]
        batch2 = [
            sample_telegram_update(update_id=102, msg_id=3, chat_id="-100123", text="Msg 3"),
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": batch1}),
            MockResponse({"ok": True, "result": batch2}),
            MockResponse({"ok": True, "result": []}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session, limit=10)
        items = await source.fetch_items()

        assert len(items) == 3
        # First call has no offset or initial offset
        assert "offset" not in session.calls[0]["kwargs"]["params"]
        # Second call has offset 102 (101 + 1)
        assert session.calls[1]["kwargs"]["params"]["offset"] == 102
        # Third call has offset 103 (102 + 1)
        assert session.calls[2]["kwargs"]["params"]["offset"] == 103

    @pytest.mark.asyncio
    async def test_user_provided_initial_offset(self) -> None:
        session = MockSession([
            MockResponse({"ok": True, "result": [
                sample_telegram_update(update_id=505, msg_id=10, chat_id="-100123", text="Offset msg")
            ]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", offset=500, session=session)
        items = await source.fetch_items()

        assert len(items) == 1
        assert session.calls[0]["kwargs"]["params"]["offset"] == 500

    @pytest.mark.asyncio
    async def test_bounded_run_stops_at_limit(self) -> None:
        batch = [
            sample_telegram_update(update_id=i, msg_id=i, chat_id="-100123", text=f"Msg {i}")
            for i in range(1, 11)
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": batch}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session, limit=5)
        items = await source.fetch_items()

        assert len(items) == 5
        assert source.last_consumed_update_id == 5
        assert source.offset == 6
        assert len(source.pending_updates) == 5

    @pytest.mark.asyncio
    async def test_a_limit_smaller_than_returned_batch(self) -> None:
        """Test A: Limit is smaller than returned batch.
        Mock returns 100, 101, 102, 103. Call limit=1.
        Verify only update 100 is ingested, offset does not jump to 104,
        and next ingestion retrieves 101, 102, 103 without data loss.
        """
        updates = [
            sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Msg 100"),
            sample_telegram_update(update_id=101, msg_id=2, chat_id="-100123", text="Msg 101"),
            sample_telegram_update(update_id=102, msg_id=3, chat_id="-100123", text="Msg 102"),
            sample_telegram_update(update_id=103, msg_id=4, chat_id="-100123", text="Msg 103"),
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": updates}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items1 = await source.fetch_items(limit=1)

        # 1. Exactly 1 item ingested
        assert len(items1) == 1
        assert items1[0].source_id == "telegram:message:-100123:1"
        # 2. Last consumed is 100, next offset is 101 (NOT 104)
        assert source.last_consumed_update_id == 100
        assert source.offset == 101
        assert len(source.pending_updates) == 3

        # 3. Next ingestion call retrieves the remaining 3 updates
        items2 = await source.fetch_items(limit=3)
        assert len(items2) == 3
        assert [it.source_id for it in items2] == [
            "telegram:message:-100123:2",
            "telegram:message:-100123:3",
            "telegram:message:-100123:4",
        ]
        assert source.last_consumed_update_id == 103
        assert source.offset == 104
        assert len(source.pending_updates) == 0

    @pytest.mark.asyncio
    async def test_b_limit_reached_halfway_through_batch(self) -> None:
        """Test B: Limit reached halfway through batch.
        Mock returns 100, 101, 102, 103, 104.
        Aurora limit: 3.
        Verify:
        100 -> consumed
        101 -> consumed
        102 -> consumed
        103 -> remains pending
        104 -> remains pending
        No update after the consumed boundary may be lost.
        """
        updates = [
            sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Msg 100"),
            sample_telegram_update(update_id=101, msg_id=2, chat_id="-100123", text="Msg 101"),
            sample_telegram_update(update_id=102, msg_id=3, chat_id="-100123", text="Msg 102"),
            sample_telegram_update(update_id=103, msg_id=4, chat_id="-100123", text="Msg 103"),
            sample_telegram_update(update_id=104, msg_id=5, chat_id="-100123", text="Msg 104"),
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": updates}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items(limit=3)

        assert len(items) == 3
        assert [it.source_id for it in items] == [
            "telegram:message:-100123:1",
            "telegram:message:-100123:2",
            "telegram:message:-100123:3",
        ]
        assert source.last_consumed_update_id == 102
        assert source.offset == 103

        # 103 and 104 remain pending and are not lost
        pending = source.pending_updates
        assert len(pending) == 2
        assert [u["update_id"] for u in pending] == [103, 104]

    @pytest.mark.asyncio
    async def test_c_exact_batch_size(self) -> None:
        """Test C: Exact batch size.
        getUpdates(limit=5) returns exactly 100..104, and Aurora consumes all 5.
        Verify next pagination step uses offset=105 and no duplicate processing occurs.
        """
        batch1 = [
            sample_telegram_update(update_id=i, msg_id=i - 99, chat_id="-100123", text=f"Msg {i}")
            for i in range(100, 105)
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": batch1}),
            MockResponse({"ok": True, "result": []}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items(limit=10)

        assert len(items) == 5
        assert source.last_consumed_update_id == 104
        assert source.offset == 105
        assert len(source.pending_updates) == 0

        # Verify second API call requested offset=105
        assert len(session.calls) == 2
        assert session.calls[1]["kwargs"]["params"]["offset"] == 105

    @pytest.mark.asyncio
    async def test_d_multiple_pagination_pages(self) -> None:
        """Test D: Multiple pagination pages.
        Page 1: 100-149 (50 updates)
        Page 2: 150-199 (50 updates)
        Aurora limit: 75.
        Verify:
        100-174 are processed exactly once.
        No messages are lost.
        """
        page1 = [
            sample_telegram_update(update_id=i, msg_id=i, chat_id="-100123", text=f"Msg {i}")
            for i in range(100, 150)
        ]
        page2 = [
            sample_telegram_update(update_id=i, msg_id=i, chat_id="-100123", text=f"Msg {i}")
            for i in range(150, 200)
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": page1}),
            MockResponse({"ok": True, "result": page2}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items(limit=75)

        assert len(items) == 75
        expected_ids = [f"telegram:message:-100123:{i}" for i in range(100, 175)]
        assert [it.source_id for it in items] == expected_ids
        assert len(set(it.source_id for it in items)) == 75

        assert source.last_consumed_update_id == 174
        assert source.offset == 175
        assert len(source.pending_updates) == 25
        assert [u["update_id"] for u in source.pending_updates] == list(range(175, 200))

        # Check call parameters: page 1 requested limit=75, page 2 requested limit=25 with offset=150
        assert session.calls[0]["kwargs"]["params"]["limit"] == 75
        assert session.calls[1]["kwargs"]["params"]["offset"] == 150
        assert session.calls[1]["kwargs"]["params"]["limit"] == 25

    @pytest.mark.asyncio
    async def test_e_duplicate_update_replay(self) -> None:
        """Test E: Duplicate update replay.
        Telegram may return an update again if not yet acknowledged.
        Verify stable identity and in-run deduplication prevent duplicate SourceItems.
        """
        u100 = sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Message 1")
        u100_replay = sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Message 1")
        u101_same_msg = sample_telegram_update(update_id=101, msg_id=1, chat_id="-100123", text="Message 1")
        u102 = sample_telegram_update(update_id=102, msg_id=2, chat_id="-100123", text="Message 2")

        session = MockSession([
            MockResponse({"ok": True, "result": [u100, u100_replay, u101_same_msg, u102]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        items = await source.fetch_items(limit=10)

        # Only 2 distinct items produced
        assert len(items) == 2
        assert items[0].source_id == "telegram:message:-100123:1"
        assert items[1].source_id == "telegram:message:-100123:2"

        # Offset correctly progressed through all replayed updates
        assert source.last_consumed_update_id == 102
        assert source.offset == 103

    @pytest.mark.asyncio
    async def test_f_configured_chat_filtering(self) -> None:
        """Test F: Configured chat filtering.
        Mock:
        100 -> chat A
        101 -> configured chat B
        102 -> chat A
        103 -> configured chat B
        Verify only B is returned as SourceItems, filtering is deterministic,
        and offset progression remains correct.
        """
        updates = [
            sample_telegram_update(update_id=100, msg_id=10, chat_id="-100AAA", text="Chat A msg 1"),
            sample_telegram_update(update_id=101, msg_id=20, chat_id="-100BBB", text="Chat B msg 1"),
            sample_telegram_update(update_id=102, msg_id=11, chat_id="-100AAA", text="Chat A msg 2"),
            sample_telegram_update(update_id=103, msg_id=21, chat_id="-100BBB", text="Chat B msg 2"),
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": updates}),
        ])
        source = TelegramSource(token="fake_token", chat="-100BBB", session=session)
        items = await source.fetch_items(limit=10)

        assert len(items) == 2
        assert items[0].source_id == "telegram:message:-100BBB:20"
        assert items[1].source_id == "telegram:message:-100BBB:21"

        # Unconfigured chat A updates were deliberately ignored and consumed
        assert source.last_consumed_update_id == 103
        assert source.offset == 104
        assert len(source.pending_updates) == 0

    @pytest.mark.asyncio
    async def test_g_limit_plus_filtering_combined(self) -> None:
        """Test G: Limit + filtering combined.
        Mock:
        100 -> chat A
        101 -> chat B
        102 -> chat A
        103 -> chat B
        104 -> chat B
        Configured: chat B
        Limit: 2
        Verify:
        101 -> ingested
        103 -> ingested
        104 -> NOT lost
        The next run must still be capable of receiving 104.
        """
        updates = [
            sample_telegram_update(update_id=100, msg_id=10, chat_id="-100AAA", text="Chat A msg 1"),
            sample_telegram_update(update_id=101, msg_id=20, chat_id="-100BBB", text="Chat B msg 1"),
            sample_telegram_update(update_id=102, msg_id=11, chat_id="-100AAA", text="Chat A msg 2"),
            sample_telegram_update(update_id=103, msg_id=21, chat_id="-100BBB", text="Chat B msg 2"),
            sample_telegram_update(update_id=104, msg_id=22, chat_id="-100BBB", text="Chat B msg 3"),
        ]
        session = MockSession([
            MockResponse({"ok": True, "result": updates}),
        ])
        source = TelegramSource(token="fake_token", chat="-100BBB", session=session)

        # Run 1: limit=2
        items_run1 = await source.fetch_items(limit=2)
        assert len(items_run1) == 2
        assert items_run1[0].source_id == "telegram:message:-100BBB:20"
        assert items_run1[1].source_id == "telegram:message:-100BBB:21"

        # Boundary must be 103, next offset 104 (NOT 105)
        assert source.last_consumed_update_id == 103
        assert source.offset == 104

        # 104 is preserved in pending updates
        assert len(source.pending_updates) == 1
        assert source.pending_updates[0]["update_id"] == 104

        # Run 2: Next run retrieves 104
        items_run2 = await source.fetch_items(limit=1)
        assert len(items_run2) == 1
        assert items_run2[0].source_id == "telegram:message:-100BBB:22"
        assert source.last_consumed_update_id == 104
        assert source.offset == 105
        assert len(source.pending_updates) == 0

    @pytest.mark.asyncio
    async def test_dynamic_server_acknowledgement_across_runs(self) -> None:
        """Verify server-side offset acknowledgement when responses are provided per-offset."""
        session = MockSession([
            MockResponse({"ok": True, "result": [
                sample_telegram_update(update_id=100, msg_id=1, chat_id="-100123", text="Msg 1")
            ]}),
            MockResponse({"ok": True, "result": [
                sample_telegram_update(update_id=101, msg_id=2, chat_id="-100123", text="Msg 2"),
                sample_telegram_update(update_id=102, msg_id=3, chat_id="-100123", text="Msg 3"),
            ]}),
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)

        # Run 1
        items1 = await source.fetch_items(limit=1)
        assert len(items1) == 1
        assert items1[0].source_id == "telegram:message:-100123:1"
        assert source.last_consumed_update_id == 100
        assert source.offset == 101
        assert session.calls[0]["kwargs"]["params"]["limit"] == 1

        # Run 2
        items2 = await source.fetch_items(limit=2)
        assert len(items2) == 2
        assert items2[0].source_id == "telegram:message:-100123:2"
        assert items2[1].source_id == "telegram:message:-100123:3"
        assert source.last_consumed_update_id == 102
        assert source.offset == 103
        assert session.calls[1]["kwargs"]["params"]["offset"] == 101
        assert session.calls[1]["kwargs"]["params"]["limit"] == 2


# ===========================================================================
# 5. Webhook Conflict (HTTP 409) Tests
# ===========================================================================


class TestTelegramWebhookConflict:
    def test_webhook_conflict_raises_actionable_source_error(self) -> None:
        resp = MockResponse(
            {
                "ok": False,
                "error_code": 409,
                "description": "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first",
            },
            status_code=409,
        )
        err = map_telegram_error(Exception("Conflict"), response=resp)
        assert "Telegram webhook conflict (HTTP 409)" in str(err)
        assert "Aurora will not automatically modify external bot webhook configurations" in str(err)

    @pytest.mark.asyncio
    async def test_webhook_conflict_in_fetch_items_does_not_call_delete_webhook(self) -> None:
        session = MockSession([
            MockResponse(
                {
                    "ok": False,
                    "error_code": 409,
                    "description": "Conflict: can't use getUpdates method while webhook is active",
                },
                status_code=409,
            )
        ])
        source = TelegramSource(token="fake_token", chat="-100123", session=session)
        with pytest.raises(SourceError) as exc_info:
            await source.fetch_items()

        assert "webhook conflict" in str(exc_info.value).lower()
        # Verify no call to deleteWebhook was attempted
        assert len(session.calls) == 1
        assert "deleteWebhook" not in session.calls[0]["url"]


# ===========================================================================
# 6. UTF-16 Entity Normalization Tests
# ===========================================================================


class TestTelegramEntityNormalization:
    def test_utf16_codepoint_mapping_with_emoji(self) -> None:
        text = "Hello 🌍 World"
        m = build_utf16_to_codepoint_map(text)
        # 🌍 is ord > 0xFFFF, takes 2 UTF-16 code units
        # 'Hello ' takes 6 code units (indices 0..5)
        # '🌍' takes 2 units (indices 6, 7 -> both point to Python char index 6)
        assert m[6] == 6
        assert m[7] == 6
        # ' ' is index 8 -> points to char index 7
        assert m[8] == 7
        # 'World' is indices 9..14 -> points to char indices 8..13
        assert text[m[9] : m[14]] == "World"

    def test_entity_formatting_basic(self) -> None:
        text = "Hello bold and italic and code world"
        entities = [
            {"type": "bold", "offset": 6, "length": 4},
            {"type": "italic", "offset": 15, "length": 6},
            {"type": "code", "offset": 26, "length": 4},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "Hello **bold** and *italic* and `code` world" == rendered

    def test_entity_formatting_with_emojis_before_entity(self) -> None:
        # 🚀 is 2 code units, 🔥 is 2 code units
        # Total prefix: "🚀🔥 " = 2 + 2 + 1 = 5 UTF-16 code units
        text = "🚀🔥 Important announcement"
        entities = [
            {"type": "bold", "offset": 3, "length": 9},  # In UTF-16, "Important" starts at offset 3? No, 🚀(2)+🔥(2)+' '(1) = 5
        ]
        # Let's compute actual Telegram offsets:
        # 🚀 (0..1), 🔥 (2..3), ' ' (4), 'Important' (5..14)
        entities = [
            {"type": "bold", "offset": 5, "length": 9},
        ]
        rendered = render_telegram_entities(text, entities)
        assert rendered == "🚀🔥 **Important** announcement"

    def test_nested_entities(self) -> None:
        text = "This is bold italic text"
        # "bold italic" is indices 8..19
        # "italic" is indices 13..19
        entities = [
            {"type": "bold", "offset": 8, "length": 11},
            {"type": "italic", "offset": 13, "length": 6},
        ]
        rendered = render_telegram_entities(text, entities)
        assert rendered == "This is **bold *italic*** text"

    def test_pre_code_block_entity(self) -> None:
        text = "def hello():\n    return 42"
        entities = [
            {"type": "pre", "offset": 0, "length": len(text), "language": "python"},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "```python\ndef hello():\n    return 42\n```" in rendered

    def test_spoiler_and_strikethrough(self) -> None:
        text = "Spoiler: secret text and deleted text"
        entities = [
            {"type": "spoiler", "offset": 9, "length": 11},
            {"type": "strikethrough", "offset": 25, "length": 12},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "Spoiler: ||secret text|| and ~~deleted text~~" == rendered


# ===========================================================================
# 7. URL Safety Tests
# ===========================================================================


class TestTelegramUrlSafety:
    def test_safe_http_and_https_links(self) -> None:
        text = "Visit our site or docs"
        entities = [
            {"type": "text_link", "offset": 10, "length": 4, "url": "https://example.com/site"},
            {"type": "text_link", "offset": 18, "length": 4, "url": "http://example.com/docs"},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "[site](https://example.com/site)" in rendered
        assert "[docs](http://example.com/docs)" in rendered

    def test_unsafe_schemes_not_clickable(self) -> None:
        text = "Click bad1 or bad2 or bad3 or bad4"
        entities = [
            {"type": "text_link", "offset": 6, "length": 4, "url": "javascript:alert(1)"},
            {"type": "text_link", "offset": 14, "length": 4, "url": "data:text/html,<script>"},
            {"type": "text_link", "offset": 22, "length": 4, "url": "file:///etc/passwd"},
            {"type": "text_link", "offset": 30, "length": 4, "url": "ftp://example.com/file"},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "[bad1](" not in rendered
        assert "bad1 (`javascript:alert(1)`)" in rendered
        assert "[bad2](" not in rendered
        assert "bad2 (`data:text/html,<script>`)" in rendered
        assert "[bad3](" not in rendered
        assert "bad3 (`file:///etc/passwd`)" in rendered
        assert "[bad4](" not in rendered
        assert "bad4 (`ftp://example.com/file`)" in rendered

    def test_plain_url_entity_safety(self) -> None:
        text = "Check https://safe.com and javascript:void(0)"
        entities = [
            {"type": "url", "offset": 6, "length": 16},
            {"type": "url", "offset": 27, "length": 18},
        ]
        rendered = render_telegram_entities(text, entities)
        assert "[https://safe.com](https://safe.com)" in rendered
        assert "`javascript:void(0)`" in rendered
        assert "[javascript:void(0)](" not in rendered


# ===========================================================================
# 8. HTML Normalization & Markdown Sanitization
# ===========================================================================


class TestTelegramHtmlNormalization:
    def test_is_html_content_detects_real_html(self) -> None:
        assert is_html_content("<p>Hello</p>") is True
        assert is_html_content("<script>evil()</script>") is True
        assert is_html_content("<a href='http://x.com'>link</a>") is True

    def test_is_html_content_ignores_comparisons(self) -> None:
        assert is_html_content("if a < b and x > y:") is False
        assert is_html_content("Price is < 100 USD") is False

    def test_normalize_strips_script_and_style(self) -> None:
        raw = "<p>Clean</p><script>alert('xss')</script><style>body {color:red}</style>"
        clean = normalize_telegram_content(raw)
        assert "<script>" not in clean
        assert "<style>" not in clean
        assert "Clean" in clean

    def test_normalize_preserves_code_fences_and_comparisons(self) -> None:
        raw = "<p>Formula: a &lt; b</p>\n```python\nif x < y:\n    return x\n```"
        clean = normalize_telegram_content(raw)
        assert "```python\nif x < y:\n    return x\n```" in clean


# ===========================================================================
# 9. Media & Captions Tests
# ===========================================================================


class TestTelegramMediaAndCaptions:
    def test_photo_and_caption_formatting(self) -> None:
        msg = sample_telegram_update(
            msg_id=701,
            chat_id=-100123,
            text=None,
            caption="Sunset at the beach",
            media={
                "photo": [
                    {"file_id": "small_id", "width": 100, "height": 100, "file_size": 1000},
                    {"file_id": "large_id", "width": 1920, "height": 1080, "file_size": 250000},
                ]
            },
        )["message"]
        lines, ptype = extract_telegram_media_metadata(msg)
        assert ptype == "photo"
        assert len(lines) == 1
        assert "1920x1080" in lines[0]
        assert "large_id" in lines[0]
        assert "250,000 bytes" in lines[0]

        body = build_telegram_message_body(msg, chat_title="Dev", chat_id="-100123")
        assert "Sunset at the beach" in body
        assert "### Attachments" in body
        assert "Photo" in body

    def test_document_and_video_metadata(self) -> None:
        msg_doc = sample_telegram_update(
            msg_id=702,
            chat_id=-100123,
            text=None,
            media={
                "document": {
                    "file_id": "doc_file_id",
                    "file_name": "quarterly_report.pdf",
                    "mime_type": "application/pdf",
                    "file_size": 1048576,
                }
            },
        )["message"]
        lines_doc, ptype_doc = extract_telegram_media_metadata(msg_doc)
        assert ptype_doc == "document"
        assert "quarterly_report.pdf" in lines_doc[0]
        assert "application/pdf" in lines_doc[0]
        assert "1,048,576 bytes" in lines_doc[0]

        msg_vid = sample_telegram_update(
            msg_id=703,
            chat_id=-100123,
            text=None,
            media={
                "video": {
                    "file_id": "vid_file_id",
                    "file_name": "demo.mp4",
                    "mime_type": "video/mp4",
                    "duration": 45,
                    "file_size": 5000000,
                }
            },
        )["message"]
        lines_vid, ptype_vid = extract_telegram_media_metadata(msg_vid)
        assert ptype_vid == "video"
        assert "demo.mp4" in lines_vid[0]
        assert "duration: 45s" in lines_vid[0]

    def test_contact_sanitization_no_phone_numbers(self) -> None:
        msg = sample_telegram_update(
            msg_id=704,
            chat_id=-100123,
            text=None,
            media={
                "contact": {
                    "first_name": "John",
                    "last_name": "Doe",
                    "phone_number": "+1234567890",
                    "vcard": "BEGIN:VCARD...",
                }
            },
        )["message"]
        lines, ptype = extract_telegram_media_metadata(msg)
        assert ptype == "contact"
        assert "John Doe" in lines[0]
        # Sensitive phone number and vcard MUST NOT be included
        assert "+1234567890" not in lines[0]
        assert "BEGIN:VCARD" not in lines[0]


# ===========================================================================
# 10. Replies & Forwarding Tests
# ===========================================================================


class TestTelegramRepliesAndForwarding:
    def test_reply_to_message_metadata_and_preview(self) -> None:
        reply_target = {
            "message_id": 800,
            "text": "Can someone help with the deployment?",
            "from": {"id": 10, "first_name": "Bob", "username": "bob"},
        }
        msg = sample_telegram_update(
            msg_id=801,
            chat_id=-100123,
            text="I'm on it!",
            reply_to_message=reply_target,
        )["message"]
        body = build_telegram_message_body(msg, chat_title="Dev", chat_id="-100123")

        assert "## Replying to" in body
        assert "> **@bob** (Message #800):" in body
        assert "> Can someone help with the deployment?" in body
        assert "I'm on it!" in body

    def test_forwarded_message_attribution(self) -> None:
        msg = sample_telegram_update(
            msg_id=802,
            chat_id=-100123,
            text="Check this out",
            forward_date=1775560000,
            forward_from_chat={"id": -100999, "title": "Tech Radar", "username": "techradar"},
        )["message"]
        body = build_telegram_message_body(msg, chat_title="Dev", chat_id="-100123")

        assert "**Forwarded from**: Tech Radar" in body
        assert "**Original Date**:" in body


# ===========================================================================
# 11. Rate Limits & Bounded Retry Tests
# ===========================================================================


class TestTelegramRateLimits:
    def test_extract_retry_after_from_parameters_json(self) -> None:
        json_body = {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 8",
            "parameters": {"retry_after": 8},
        }
        assert _extract_retry_after(json_body=json_body) == 8.0

    def test_extract_retry_after_from_description_regex(self) -> None:
        json_body = {
            "ok": False,
            "error_code": 429,
            "description": "Too Many Requests: retry after 12",
        }
        assert _extract_retry_after(json_body=json_body) == 12.0

    def test_extract_retry_after_from_header_fallback(self) -> None:
        resp = MockResponse({}, status_code=429, headers={"Retry-After": "4"})
        assert _extract_retry_after(response=resp, json_body={}) == 4.0

    def test_api_call_429_single_retry_success(self) -> None:
        sleep_delays: List[float] = []

        def fake_sleep(d: float) -> None:
            sleep_delays.append(d)

        session = MockSession([
            MockResponse(
                {"ok": False, "error_code": 429, "parameters": {"retry_after": 0.05}},
                status_code=429,
            ),
            MockResponse({"ok": True, "result": []}, status_code=200),
        ])

        source = TelegramSource(token="test_token", chat="-100123", session=session, max_retries=3, sleep_fn=fake_sleep)
        res = source._api_call("GET", "getUpdates")

        assert res == {"ok": True, "result": []}
        assert sleep_delays == [0.05]
        assert len(session.calls) == 2

    def test_api_call_429_exhaustion_raises_source_error(self) -> None:
        sleep_delays: List[float] = []

        def fake_sleep(d: float) -> None:
            sleep_delays.append(d)

        session = MockSession([
            MockResponse({"ok": False, "error_code": 429, "parameters": {"retry_after": 0.01}}, status_code=429),
            MockResponse({"ok": False, "error_code": 429, "parameters": {"retry_after": 0.01}}, status_code=429),
            MockResponse({"ok": False, "error_code": 429, "parameters": {"retry_after": 0.01}}, status_code=429),
            MockResponse({"ok": False, "error_code": 429, "parameters": {"retry_after": 0.01}}, status_code=429),
        ])

        source = TelegramSource(token="test_token", chat="-100123", session=session, max_retries=3, sleep_fn=fake_sleep)
        with pytest.raises(SourceError) as exc_info:
            source._api_call("GET", "getUpdates")

        assert "rate limit exceeded" in str(exc_info.value).lower()
        assert len(sleep_delays) == 3


# ===========================================================================
# 12. API Error Mapping Tests
# ===========================================================================


class TestTelegramApiErrors:
    def test_401_unauthorized(self) -> None:
        resp = MockResponse({"ok": False, "error_code": 401, "description": "Unauthorized"}, status_code=401)
        err = map_telegram_error(Exception("Unauthorized"), response=resp)
        assert "Telegram authentication failed (HTTP 401)" in str(err)

    def test_403_forbidden(self) -> None:
        resp = MockResponse({"ok": False, "error_code": 403, "description": "Forbidden: bot was blocked by the user"}, status_code=403)
        err = map_telegram_error(Exception("Forbidden"), response=resp)
        assert "Telegram permission denied (HTTP 403)" in str(err)

    def test_400_chat_not_found(self) -> None:
        resp = MockResponse({"ok": False, "error_code": 400, "description": "Bad Request: chat not found"}, status_code=400)
        err = map_telegram_error(Exception("Chat not found"), response=resp)
        assert "Telegram chat not found (HTTP 400)" in str(err)

    def test_404_not_found(self) -> None:
        resp = MockResponse({"ok": False, "error_code": 404, "description": "Not Found"}, status_code=404)
        err = map_telegram_error(Exception("Not found"), response=resp)
        assert "Telegram API method or resource not found (HTTP 404)" in str(err)

    def test_500_server_error(self) -> None:
        resp = MockResponse({"ok": False, "error_code": 500, "description": "Internal Server Error"}, status_code=500)
        err = map_telegram_error(Exception("Server error"), response=resp)
        assert "Telegram server error (HTTP 500)" in str(err)

    def test_timeout_and_network_error(self) -> None:
        err1 = map_telegram_error(requests.exceptions.Timeout("Read timed out"))
        assert "Telegram request timed out" in str(err1)
        err2 = map_telegram_error(requests.exceptions.ConnectionError("Failed to establish a new connection"))
        assert "Network error connecting to Telegram API" in str(err2)


# ===========================================================================
# 13. Deduplication & Note Generation Tests
# ===========================================================================


class TestTelegramDeduplicationAndStorage:
    @pytest.mark.asyncio
    async def test_deduplication_lifecycle(self, tmp_path: Path) -> None:
        vault_path = tmp_path / "vault"
        vault_path.mkdir()
        db_path = tmp_path / "tracker.db"
        tracker = DeduplicationTracker(db_path)
        config = IngestionConfig(vault_path=vault_path, tracker_db_path=db_path)
        pipeline = IngestionPipeline(config=config, tracker=tracker)

        upd1 = sample_telegram_update(update_id=1, msg_id=9001, chat_id="-100123", text="Initial telegram post")
        upd1_edited = sample_telegram_update(update_id=2, update_type="edited_message", msg_id=9001, chat_id="-100123", text="Edited telegram post")

        # 1. First run: NEW
        session1 = MockSession([
            MockResponse({"ok": True, "result": [upd1]}),
        ])
        source1 = TelegramSource(token="fake_token", chat="-100123", session=session1)
        items1 = await source1.fetch_items()
        action1, path1 = await pipeline.process_item(source1, items1[0])
        assert action1 == IngestionAction.NEW
        assert items1[0].source_id == "telegram:message:-100123:9001"
        assert (vault_path / path1).exists()
        assert "Initial telegram post" in (vault_path / path1).read_text(encoding="utf-8")

        # 2. Second run identical: UNCHANGED
        session2 = MockSession([
            MockResponse({"ok": True, "result": [upd1]}),
        ])
        source2 = TelegramSource(token="fake_token", chat="-100123", session=session2)
        items2 = await source2.fetch_items()
        action2, path2 = await pipeline.process_item(source2, items2[0])
        assert action2 == IngestionAction.UNCHANGED
        assert path2 == path1

        # 3. Third run edited: CHANGED
        session3 = MockSession([
            MockResponse({"ok": True, "result": [upd1_edited]}),
        ])
        source3 = TelegramSource(token="fake_token", chat="-100123", session=session3)
        items3 = await source3.fetch_items()
        action3, path3 = await pipeline.process_item(source3, items3[0])
        assert action3 == IngestionAction.CHANGED
        assert path3 == path1
        content_updated = (vault_path / path1).read_text(encoding="utf-8")
        assert "Edited telegram post" in content_updated

        tracker.close()

    @pytest.mark.asyncio
    async def test_convert_to_markdown_folder_routing(self) -> None:
        source = TelegramSource(token="fake_token", chat="-100123")
        item = SourceItem(
            source_id="telegram:message:-100123:42",
            title="Important Announcement",
            source_type="telegram",
            content="# Important Announcement\n\nContent here",
            author="@my_channel",
            date="2026-10-06T10:00:00Z",
            source_url="https://t.me/my_channel/42",
            tags=["ingested", "telegram", "social"],
        )
        note = await source.convert_to_markdown(item)
        assert note.folder == "Ingested/Social"
        assert note.source == "telegram"

    def test_attribution_block_generation(self) -> None:
        note = MarkdownNote(
            title="Release Notes",
            source="telegram",
            date="2026-10-06T10:00:00Z",
            body="Content",
            author="alice",
            source_url="https://t.me/dev/10",
            extra_metadata={
                "chat_title": "Dev Channel",
                "chat_id": "-100123",
                "reply_to_message_id": 9,
            },
        )
        attr = build_attribution_block(note)
        assert "[Telegram — Dev Channel](https://t.me/dev/10)" in attr
        assert "**Author**: @alice" in attr
        assert "**Date**: 2026-10-06" in attr
        assert "**Reply to**: #9" in attr


# ===========================================================================
# 14. CLI Tests
# ===========================================================================


class TestTelegramCli:
    def test_cli_parser_ingest_telegram_arguments(self) -> None:
        parser = create_parser()
        args = parser.parse_args([
            "ingest-telegram",
            "--chat", "-1001234567890",
            "--token", "custom_bot_token",
            "--limit", "75",
            "--max-retries", "4",
            "--offset", "10050",
        ])
        assert args.command == "ingest-telegram"
        assert args.chat == ["-1001234567890"]
        assert args.token == "custom_bot_token"
        assert args.limit == 75
        assert args.max_retries == 4
        assert args.offset == 10050

    def test_source_registry_contains_telegram(self) -> None:
        sources = SourceRegistry.list_sources()
        assert "telegram" in sources
        assert SourceRegistry.get("telegram") is TelegramSource
