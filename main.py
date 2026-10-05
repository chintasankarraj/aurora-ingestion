"""Aurora Ingestion Pipeline — CLI entrypoint and orchestrator service."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import DEFAULT_SOURCE_FOLDERS, IngestionConfig, load_config
from converter import write_note_to_vault
from exceptions import AuroraIngestionError, SourceError, VaultPathError
from models import SourceItem
import sources  # Register built-in source connectors
from sources.base import BaseSource, SourceRegistry
from tracker import DeduplicationTracker, IngestionAction

# Setup default logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("aurora_ingestion")


class IngestionPipeline:
    """Core orchestrator coordinating sources, deduplication, and vault note writing."""

    def __init__(
        self,
        config: IngestionConfig,
        tracker: DeduplicationTracker,
    ) -> None:
        self.config = config
        self.tracker = tracker

    async def process_item(
        self, source: BaseSource, item: SourceItem
    ) -> Tuple[IngestionAction, Optional[str]]:
        """Process a single SourceItem through deduplication, conversion, and vault writing.

        Returns:
            Tuple of (IngestionAction, relative_vault_path or None)
        """
        content_hash = item.compute_content_hash()
        action, existing_record = self.tracker.check_item(
            item.source_type, item.source_id, content_hash
        )

        if action == IngestionAction.UNCHANGED:
            logger.info(
                f"Skipping unchanged {item.source_type} item '{item.source_id}' "
                f"(already in {existing_record.vault_path if existing_record else 'vault'})"
            )
            return IngestionAction.UNCHANGED, existing_record.vault_path if existing_record else None

        # Convert to Markdown note
        note = await source.convert_to_markdown(item)

        if action == IngestionAction.NEW:
            logger.info(f"Ingesting new {item.source_type} item '{item.source_id}': {item.title}")
            _, rel_path, _ = write_note_to_vault(
                note=note,
                vault_path=self.config.vault_path,
                config=self.config,
                attachments=item.attachments,
            )
            self.tracker.record_ingestion(
                source_type=item.source_type,
                source_id=item.source_id,
                vault_path=rel_path,
                content_hash=content_hash,
            )
            logger.info(f"Successfully saved note to vault: {rel_path}")
            return IngestionAction.NEW, rel_path

        elif action == IngestionAction.CHANGED:
            existing_path = existing_record.vault_path if existing_record else None
            logger.info(
                f"Updating modified {item.source_type} item '{item.source_id}': {item.title} -> {existing_path}"
            )
            _, rel_path, _ = write_note_to_vault(
                note=note,
                vault_path=self.config.vault_path,
                existing_vault_path=existing_path,
                config=self.config,
                attachments=item.attachments,
            )
            self.tracker.record_ingestion(
                source_type=item.source_type,
                source_id=item.source_id,
                vault_path=rel_path,
                content_hash=content_hash,
            )
            logger.info(f"Successfully updated note in vault: {rel_path}")
            return IngestionAction.CHANGED, rel_path

        return action, None

    async def run_source(
        self, source_name: str, **kwargs: Any
    ) -> Dict[str, Any]:
        """Fetch items from a registered source connector and process each item."""
        source = SourceRegistry.create(source_name)
        logger.info(f"Starting ingestion for source: {source.display_name} ({source.source_type})")

        items: List[SourceItem] = await source.fetch_items(**kwargs)
        logger.info(f"Fetched {len(items)} items from {source.source_type}")

        stats = {"total": len(items), "new": 0, "changed": 0, "unchanged": 0, "failed": 0}

        for item in items:
            try:
                action, _ = await self.process_item(source, item)
                if action == IngestionAction.NEW:
                    stats["new"] += 1
                elif action == IngestionAction.CHANGED:
                    stats["changed"] += 1
                elif action == IngestionAction.UNCHANGED:
                    stats["unchanged"] += 1
            except Exception as e:
                stats["failed"] += 1
                logger.error(
                    f"Failed to process item '{item.source_id}' from {item.source_type}: {e}",
                    exc_info=True,
                )

        logger.info(
            f"Finished ingestion for {source.source_type}. Stats: "
            f"Total={stats['total']}, New={stats['new']}, "
            f"Changed={stats['changed']}, Unchanged={stats['unchanged']}, "
            f"Failed={stats['failed']}"
        )
        return stats


def ensure_vault_folders(vault_path: Path) -> None:
    """Ensure the standard Aurora Ingested folder topology exists in the vault."""
    for rel_folder in DEFAULT_SOURCE_FOLDERS.values():
        folder_path = vault_path / rel_folder
        folder_path.mkdir(parents=True, exist_ok=True)
    # Attachment folder
    (vault_path / "Attachments" / "Ingested").mkdir(parents=True, exist_ok=True)
    logger.info(f"Verified standard folder hierarchy in vault at: {vault_path}")


def create_parser() -> argparse.ArgumentParser:
    """Create command-line interface argument parser."""
    parser = argparse.ArgumentParser(
        prog="aurora-ingestion",
        description="Aurora External Data Ingestion Pipeline Service",
    )
    parser.add_argument(
        "--vault-path",
        help="Path to Aurora Obsidian vault (overrides AURORA_VAULT_PATH env var)",
    )
    parser.add_argument(
        "--tracker-db",
        help="Path to tracker SQLite database file",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug log output",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: ingest
    ingest_parser = subparsers.add_parser("ingest", help="Run ingestion for a specified source")
    ingest_parser.add_argument(
        "--source", "-s",
        required=True,
        help="Source type identifier (e.g. web, email, pdf, youtube, etc.)",
    )
    ingest_parser.add_argument(
        "--file", "-f",
        help="Target file or directory path for file-based ingestion (e.g. PDF)",
    )
    ingest_parser.add_argument(
        "--url", "-u",
        help="Target URL or identifier for on-demand ingestion",
    )
    ingest_parser.add_argument(
        "--provider", "-p",
        help="Email provider (gmail or outlook)",
    )
    ingest_parser.add_argument(
        "--thread-id", "-t",
        help="Specific thread ID (Gmail) or conversation ID (Outlook) to ingest",
    )
    ingest_parser.add_argument(
        "--page-id",
        help="Specific Notion page ID to ingest",
    )

    # Command: ingest-web
    ingest_web_parser = subparsers.add_parser("ingest-web", help="Shortcut to ingest a webpage by URL")
    ingest_web_parser.add_argument("url", help="URL of the webpage to ingest")

    # Command: ingest-youtube
    ingest_yt_parser = subparsers.add_parser("ingest-youtube", help="Shortcut to ingest a YouTube video by URL")
    ingest_yt_parser.add_argument("url", help="URL of the YouTube video to ingest")

    # Command: ingest-email
    ingest_email_parser = subparsers.add_parser(
        "ingest-email", help="Ingest email thread(s) from Gmail or Outlook"
    )
    ingest_email_parser.add_argument(
        "--provider", "-p",
        choices=["gmail", "outlook"],
        default="gmail",
        help="Email provider (default: gmail)",
    )
    ingest_email_parser.add_argument(
        "--thread-id", "-t",
        help="Specific thread ID (Gmail) or conversation ID (Outlook) to ingest",
    )

    # Command: ingest-rss
    ingest_rss_parser = subparsers.add_parser(
        "ingest-rss", help="Shortcut to ingest an RSS or Atom feed by URL"
    )
    ingest_rss_parser.add_argument("url", help="URL of the RSS or Atom feed to ingest")

    # Command: ingest-notion
    ingest_notion_parser = subparsers.add_parser(
        "ingest-notion", help="Ingest Notion pages accessible to integration token"
    )
    ingest_notion_parser.add_argument(
        "--page-id",
        help="Optional specific Notion page ID to ingest (defaults to all accessible pages)",
    )

    # Command: status
    status_parser = subparsers.add_parser("status", help="Show deduplication tracker status")
    status_parser.add_argument(
        "--source", "-s",
        help="Filter status by source type",
    )

    # Command: list-sources
    subparsers.add_parser("list-sources", help="List registered source connectors")

    # Command: init-vault
    subparsers.add_parser(
        "init-vault", help="Create default Ingested folder hierarchy in configured vault"
    )

    return parser


async def async_main(args: argparse.Namespace) -> int:
    """Main asynchronous CLI execution dispatcher."""
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        config = load_config(
            vault_path_override=args.vault_path,
            tracker_db_path_override=args.tracker_db,
        )
    except VaultPathError as e:
        logger.error(f"Configuration error: {e}")
        return 1

    tracker = DeduplicationTracker(config.tracker_db_path)

    try:
        if args.command == "init-vault":
            config.validate()
            ensure_vault_folders(config.vault_path)
            print(f"Initialized vault folder structure at: {config.vault_path}")
            return 0

        elif args.command == "list-sources":
            sources_dict = SourceRegistry.list_sources()
            print("Registered source connectors:")
            if not sources_dict:
                print("  (None registered yet - foundation ready)")
            for src_name, cls_name in sources_dict.items():
                print(f"  - {src_name} ({cls_name})")
            return 0

        elif args.command == "status":
            records = tracker.list_records(source_type=args.source)
            print(f"Tracker database: {config.tracker_db_path}")
            if config.vault_path:
                print(f"Configured vault: {config.vault_path}")
            else:
                print("Configured vault: (Not configured - set AURORA_VAULT_PATH or use --vault-path)")
            print(f"Total tracked items: {len(records)}")
            for r in records[:20]:
                print(f"  [{r.source_type}] {r.source_id} -> {r.vault_path} ({r.ingested_at})")
            if len(records) > 20:
                print(f"  ... and {len(records) - 20} more records.")
            return 0

        elif args.command in {"ingest", "ingest-web", "ingest-youtube", "ingest-email", "ingest-rss", "ingest-notion"}:
            config.validate()
            pipeline = IngestionPipeline(config=config, tracker=tracker)
            if args.command == "ingest-email":
                source_name = "email"
            elif args.command == "ingest-youtube":
                source_name = "youtube"
            elif args.command == "ingest-web":
                source_name = "web"
            elif args.command == "ingest-rss":
                source_name = "rss"
            elif args.command == "ingest-notion":
                source_name = "notion"
            else:
                source_name = getattr(args, "source", None) or "web"

            kwargs: Dict[str, Any] = {}
            if getattr(args, "url", None):
                kwargs["url"] = args.url
            if getattr(args, "file", None):
                kwargs["file"] = args.file
            if getattr(args, "provider", None):
                kwargs["provider"] = args.provider
            if getattr(args, "thread_id", None):
                kwargs["thread_id"] = args.thread_id
            if getattr(args, "page_id", None):
                kwargs["page_id"] = args.page_id

            try:
                stats = await pipeline.run_source(source_name, **kwargs)
                print(f"Ingestion completed for '{source_name}': {stats}")
                return 0 if stats["failed"] == 0 else 1
            except SourceError as e:
                logger.error(f"Source error: {e}")
                return 1

        else:
            create_parser().print_help()
            return 0

    finally:
        tracker.close()


def main() -> None:
    """CLI script entrypoint."""
    parser = create_parser()
    args = parser.parse_args()
    try:
        exit_code = asyncio.run(async_main(args))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
