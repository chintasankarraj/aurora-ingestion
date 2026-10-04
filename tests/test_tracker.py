"""Tests for SQLite deduplication tracker."""

from pathlib import Path

import pytest

from tracker import DeduplicationTracker, IngestionAction


def test_tracker_lifecycle(tmp_path):
    db_file = tmp_path / "test_tracker.sqlite"
    tracker = DeduplicationTracker(db_file)

    source_type = "email"
    source_id = "msg_123"
    initial_hash = "hash_v1_aaa"
    vault_path = "Ingested/Email/2026-09-22_email_Test.md"

    # 1. Non-existent item should return NEW
    action, record = tracker.check_item(source_type, source_id, initial_hash)
    assert action == IngestionAction.NEW
    assert record is None

    # 2. Record item
    recorded = tracker.record_ingestion(
        source_type=source_type,
        source_id=source_id,
        vault_path=vault_path,
        content_hash=initial_hash,
    )
    assert recorded.source_id == source_id
    assert recorded.content_hash == initial_hash
    assert recorded.vault_path == vault_path

    # 3. Check again with same hash -> UNCHANGED
    action2, record2 = tracker.check_item(source_type, source_id, initial_hash)
    assert action2 == IngestionAction.UNCHANGED
    assert record2 is not None
    assert record2.vault_path == vault_path

    # 4. Check with altered hash -> CHANGED
    updated_hash = "hash_v2_bbb"
    action3, record3 = tracker.check_item(source_type, source_id, updated_hash)
    assert action3 == IngestionAction.CHANGED
    assert record3 is not None

    # 5. Record update with new hash
    updated_record = tracker.record_ingestion(
        source_type=source_type,
        source_id=source_id,
        vault_path=vault_path,
        content_hash=updated_hash,
    )
    assert updated_record.content_hash == updated_hash

    # Check again with new hash -> UNCHANGED
    action4, _ = tracker.check_item(source_type, source_id, updated_hash)
    assert action4 == IngestionAction.UNCHANGED

    # 6. Listing records
    records = tracker.list_records(source_type="email")
    assert len(records) == 1
    assert records[0].source_id == source_id

    # 7. Delete record
    deleted = tracker.delete_record(source_type, source_id)
    assert deleted is True
    assert tracker.get_record(source_type, source_id) is None

    tracker.close()


def test_tracker_multiple_sources(tmp_path):
    db_file = tmp_path / "multi_tracker.sqlite"
    with DeduplicationTracker(db_file) as tracker:
        tracker.record_ingestion("youtube", "yt_1", "Ingested/YouTube/1.md", "hash1")
        tracker.record_ingestion("web", "url_1", "Ingested/Web/1.md", "hash2")
        tracker.record_ingestion("email", "em_1", "Ingested/Email/1.md", "hash3")

        all_records = tracker.list_records()
        assert len(all_records) == 3

        yt_records = tracker.list_records(source_type="youtube")
        assert len(yt_records) == 1
        assert yt_records[0].source_id == "yt_1"
