"""SQLite-based deduplication tracker for the Aurora Ingestion Pipeline."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import List, Optional, Tuple, Union

from exceptions import TrackingError
from models import TrackingRecord, format_iso_timestamp


class IngestionAction(str, Enum):
    """Action to take based on deduplication state."""
    NEW = "new"              # Brand new item -> create note
    UNCHANGED = "unchanged"  # Existing item with identical hash -> skip
    CHANGED = "changed"      # Existing item with altered hash -> overwrite in-place


class DeduplicationTracker:
    """Manages tracking records in SQLite to prevent duplicate or redundant ingestions."""

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path).resolve()
        # Ensure directory for tracker database exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(str(self.db_path))
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_db(self) -> None:
        try:
            conn = self._get_connection()
            with conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ingestion_tracker (
                        source_type TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        vault_path TEXT NOT NULL,
                        ingested_at TEXT NOT NULL,
                        content_hash TEXT NOT NULL,
                        PRIMARY KEY (source_type, source_id)
                    );
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tracker_source ON ingestion_tracker(source_type);"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_tracker_vault_path ON ingestion_tracker(vault_path);"
                )
        except sqlite3.Error as e:
            raise TrackingError(f"Failed to initialize SQLite tracker at {self.db_path}: {e}") from e

    def get_record(self, source_type: str, source_id: str) -> Optional[TrackingRecord]:
        """Fetch tracking record by source_type and source_id."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT source_type, source_id, vault_path, ingested_at, content_hash
                FROM ingestion_tracker
                WHERE source_type = ? AND source_id = ?
                """,
                (source_type, source_id),
            )
            row = cursor.fetchone()
            if row:
                return TrackingRecord(
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    vault_path=row["vault_path"],
                    ingested_at=row["ingested_at"],
                    content_hash=row["content_hash"],
                )
            return None
        except sqlite3.Error as e:
            raise TrackingError(f"Error querying tracking record ({source_type}:{source_id}): {e}") from e

    def get_record_by_vault_path(self, vault_path: str) -> Optional[TrackingRecord]:
        """Fetch tracking record by relative vault path."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT source_type, source_id, vault_path, ingested_at, content_hash
                FROM ingestion_tracker
                WHERE vault_path = ?
                """,
                (vault_path,),
            )
            row = cursor.fetchone()
            if row:
                return TrackingRecord(
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    vault_path=row["vault_path"],
                    ingested_at=row["ingested_at"],
                    content_hash=row["content_hash"],
                )
            return None
        except sqlite3.Error as e:
            raise TrackingError(f"Error querying tracking record by path ({vault_path}): {e}") from e

    def check_item(
        self, source_type: str, source_id: str, content_hash: str
    ) -> Tuple[IngestionAction, Optional[TrackingRecord]]:
        """Determine what action to take for an incoming source item.
        
        Returns:
            (IngestionAction.NEW, None) if item has never been ingested.
            (IngestionAction.UNCHANGED, existing_record) if item exists and hash matches.
            (IngestionAction.CHANGED, existing_record) if item exists but hash differs.
        """
        record = self.get_record(source_type, source_id)
        if record is None:
            return IngestionAction.NEW, None

        if record.content_hash == content_hash:
            return IngestionAction.UNCHANGED, record

        return IngestionAction.CHANGED, record

    def record_ingestion(
        self,
        source_type: str,
        source_id: str,
        vault_path: str,
        content_hash: str,
        ingested_at: Optional[Union[datetime, str]] = None,
    ) -> TrackingRecord:
        """Create or update a tracking record after successful ingestion."""
        if ingested_at is None:
            ts_str = datetime.now(timezone.utc).isoformat()
        elif isinstance(ingested_at, datetime):
            ts_str = format_iso_timestamp(ingested_at)
        else:
            ts_str = str(ingested_at)

        try:
            conn = self._get_connection()
            with conn:
                conn.execute(
                    """
                    INSERT INTO ingestion_tracker (source_type, source_id, vault_path, ingested_at, content_hash)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(source_type, source_id) DO UPDATE SET
                        vault_path = excluded.vault_path,
                        ingested_at = excluded.ingested_at,
                        content_hash = excluded.content_hash
                    """,
                    (source_type, source_id, vault_path, ts_str, content_hash),
                )
            return TrackingRecord(
                source_type=source_type,
                source_id=source_id,
                vault_path=vault_path,
                ingested_at=ts_str,
                content_hash=content_hash,
            )
        except sqlite3.Error as e:
            raise TrackingError(
                f"Failed to record ingestion for {source_type}:{source_id} -> {vault_path}: {e}"
            ) from e

    def delete_record(self, source_type: str, source_id: str) -> bool:
        """Delete a tracking record. Returns True if a record was removed."""
        try:
            conn = self._get_connection()
            with conn:
                cursor = conn.execute(
                    "DELETE FROM ingestion_tracker WHERE source_type = ? AND source_id = ?",
                    (source_type, source_id),
                )
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            raise TrackingError(f"Failed to delete record {source_type}:{source_id}: {e}") from e

    def list_records(self, source_type: Optional[str] = None) -> List[TrackingRecord]:
        """List tracking records, optionally filtered by source_type."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            if source_type:
                cursor.execute(
                    """
                    SELECT source_type, source_id, vault_path, ingested_at, content_hash
                    FROM ingestion_tracker
                    WHERE source_type = ?
                    ORDER BY ingested_at DESC
                    """,
                    (source_type,),
                )
            else:
                cursor.execute(
                    """
                    SELECT source_type, source_id, vault_path, ingested_at, content_hash
                    FROM ingestion_tracker
                    ORDER BY ingested_at DESC
                    """
                )
            rows = cursor.fetchall()
            return [
                TrackingRecord(
                    source_type=r["source_type"],
                    source_id=r["source_id"],
                    vault_path=r["vault_path"],
                    ingested_at=r["ingested_at"],
                    content_hash=r["content_hash"],
                )
                for r in rows
            ]
        except sqlite3.Error as e:
            raise TrackingError(f"Failed to list tracking records: {e}") from e

    def close(self) -> None:
        """Close SQLite connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> DeduplicationTracker:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
