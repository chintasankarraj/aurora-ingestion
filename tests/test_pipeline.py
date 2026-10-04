"""Integration tests for the ingestion pipeline orchestration and BaseSource interface."""

from datetime import datetime
from pathlib import Path
from typing import Any, List

import pytest

from config import IngestionConfig
from main import IngestionPipeline
from models import MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry, async_retry
from tracker import DeduplicationTracker, IngestionAction


class MockSource(BaseSource):
    """Mock external source for testing pipeline flows."""

    def __init__(self, items: List[SourceItem]) -> None:
        self._items = items

    @property
    def source_type(self) -> str:
        return "mock_test"

    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        return self._items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        return self.default_item_to_note(item)


@pytest.mark.asyncio
async def test_pipeline_deduplication_and_update_lifecycle(tmp_path):
    vault_dir = tmp_path / "vault"
    vault_dir.mkdir()
    db_file = tmp_path / "tracker.sqlite"

    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=db_file)
    tracker = DeduplicationTracker(db_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    # 1. Create first item
    item1 = SourceItem(
        source_id="item_001",
        source_type="mock_test",
        title="First Article",
        content="Original content of article 1.",
        date="2026-09-22",
    )

    mock_source = MockSource([item1])
    SourceRegistry.register("mock_test", lambda: mock_source)

    # Run pipeline for the first time
    action1, path1 = await pipeline.process_item(mock_source, item1)
    assert action1 == IngestionAction.NEW
    assert path1 is not None

    note_file = vault_dir / path1
    assert note_file.exists()
    assert "Original content of article 1." in note_file.read_text(encoding="utf-8")

    # 2. Run again with UNCHANGED item
    action2, path2 = await pipeline.process_item(mock_source, item1)
    assert action2 == IngestionAction.UNCHANGED
    assert path2 == path1

    # 3. Update item content
    item1_updated = SourceItem(
        source_id="item_001",
        source_type="mock_test",
        title="First Article",
        content="Modified content of article 1.",
        date="2026-09-22",
    )

    action3, path3 = await pipeline.process_item(mock_source, item1_updated)
    assert action3 == IngestionAction.CHANGED
    assert path3 == path1  # Overwrites the SAME file

    # Verify overwritten content
    assert "Modified content of article 1." in note_file.read_text(encoding="utf-8")
    assert "Original content of article 1." not in note_file.read_text(encoding="utf-8")

    tracker.close()


@pytest.mark.asyncio
async def test_async_retry_decorator():
    call_count = 0

    @async_retry(max_retries=3, initial_delay=0.01, backoff_factor=1.5)
    async def flaky_api_call():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Transient network glitch")
        return "success"

    result = await flaky_api_call()
    assert result == "success"
    assert call_count == 3


@pytest.mark.asyncio
async def test_async_retry_fails_after_max():
    @async_retry(max_retries=2, initial_delay=0.01)
    async def always_failing():
        raise ValueError("Permanent failure")

    with pytest.raises(ValueError, match="Permanent failure"):
        await always_failing()
