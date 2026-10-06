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
    ingest_parser.add_argument(
        "--path",
        help="Path to source directory or file (e.g. for Google Keep Takeout)",
    )
    ingest_parser.add_argument(
        "--token",
        help="API token or integration secret (e.g. for Readwise)",
    )
    ingest_parser.add_argument(
        "--book-id",
        help="Specific book or article ID (e.g. for Readwise)",
    )
    ingest_parser.add_argument(
        "--updated-after",
        help="Fetch items updated after date (e.g. for Readwise)",
    )
    ingest_parser.add_argument(
        "--username",
        help="Username (e.g. for Instapaper)",
    )
    ingest_parser.add_argument(
        "--password",
        help="Password (e.g. for Instapaper)",
    )
    ingest_parser.add_argument(
        "--folder",
        help="Folder name or ID (e.g. for Instapaper)",
    )
    ingest_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum items to fetch (e.g. for Instapaper/GitHub)",
    )
    ingest_parser.add_argument(
        "--repo",
        action="append",
        help="GitHub repository to ingest (owner/repo, can be repeated)",
    )
    ingest_parser.add_argument(
        "--repos",
        help="Comma-separated list of GitHub repositories (e.g. owner/repo1,owner/repo2)",
    )
    ingest_parser.add_argument(
        "--state",
        choices=["all", "open", "closed"],
        help="Issue state to fetch for GitHub (default: all)",
    )
    ingest_parser.add_argument(
        "--include-gists",
        action="store_true",
        help="Include user gists in GitHub ingestion",
    )
    ingest_parser.add_argument(
        "--gists-only",
        action="store_true",
        help="Ingest only user gists in GitHub ingestion",
    )
    ingest_parser.add_argument(
        "--subreddit", "-sub",
        action="append",
        help="Subreddit to ingest for Reddit (can be repeated, e.g. -sub programming)",
    )
    ingest_parser.add_argument(
        "--subreddits",
        help="Comma-separated list of subreddits for Reddit (e.g. programming,MachineLearning)",
    )
    ingest_parser.add_argument(
        "--client-id",
        help="Client ID (e.g. for Reddit)",
    )
    ingest_parser.add_argument(
        "--client-secret",
        help="Client secret (e.g. for Reddit)",
    )
    ingest_parser.add_argument(
        "--user-agent",
        help="Custom User-Agent (e.g. for Reddit)",
    )
    ingest_parser.add_argument(
        "--listing",
        choices=["hot", "new", "top", "rising"],
        default="hot",
        help="Listing type to fetch for Reddit: hot, new, top, rising (default: hot)",
    )
    ingest_parser.add_argument(
        "--max-comments",
        type=int,
        help="Maximum comments to fetch per item (e.g. for Reddit)",
    )
    ingest_parser.add_argument(
        "--channel", "-c",
        action="append",
        help="Channel to ingest for Slack or Discord (can be repeated, e.g. -c general)",
    )
    ingest_parser.add_argument(
        "--channels",
        help="Comma-separated list of channels for Slack or Discord (e.g. general,engineering)",
    )
    ingest_parser.add_argument(
        "--guild", "-g",
        action="append",
        help="Guild/server ID or name to ingest for Discord (can be repeated, e.g. -g 123456789012345678)",
    )
    ingest_parser.add_argument(
        "--guilds",
        help="Comma-separated list of guild IDs or names for Discord",
    )
    ingest_parser.add_argument(
        "--chat",
        action="append",
        help="Chat ID, username, or title to ingest for Telegram (can be repeated)",
    )
    ingest_parser.add_argument(
        "--chats",
        help="Comma-separated list of chat IDs, usernames, or titles for Telegram",
    )
    ingest_parser.add_argument(
        "--offset",
        type=int,
        help="Identifier of first update to retrieve for Telegram",
    )
    ingest_parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum messages to fetch per channel for Slack or Discord",
    )
    ingest_parser.add_argument(
        "--max-replies",
        type=int,
        help="Maximum replies to fetch per thread for Slack or Discord",
    )
    ingest_parser.add_argument(
        "--no-threads",
        action="store_true",
        help="Do not fetch threaded replies for Slack or Discord",
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

    # Command: ingest-google-keep
    ingest_keep_parser = subparsers.add_parser(
        "ingest-google-keep",
        help="Ingest Google Keep notes from Google Takeout export",
    )
    ingest_keep_parser.add_argument(
        "--path", "-p",
        help="Path to Google Keep Takeout JSON file or directory (defaults to GOOGLE_KEEP_EXPORT_PATH)",
    )

    # Command: ingest-readwise
    ingest_rw_parser = subparsers.add_parser(
        "ingest-readwise",
        help="Ingest highlights and articles from Readwise API",
    )
    ingest_rw_parser.add_argument(
        "--token",
        help="Readwise API token (defaults to READWISE_TOKEN)",
    )
    ingest_rw_parser.add_argument(
        "--book-id",
        help="Optional specific Readwise book/article ID to ingest",
    )
    ingest_rw_parser.add_argument(
        "--updated-after",
        help="Optional ISO 8601 date to ingest items updated after (e.g. 2026-01-01)",
    )

    # Command: ingest-instapaper
    ingest_ip_parser = subparsers.add_parser(
        "ingest-instapaper",
        help="Ingest saved bookmarks and highlights from Instapaper API",
    )
    ingest_ip_parser.add_argument(
        "--token",
        help="Instapaper API token / Personal token (defaults to INSTAPAPER_TOKEN)",
    )
    ingest_ip_parser.add_argument(
        "--username",
        help="Instapaper username (defaults to INSTAPAPER_USERNAME)",
    )
    ingest_ip_parser.add_argument(
        "--password",
        help="Instapaper password (defaults to INSTAPAPER_PASSWORD)",
    )
    ingest_ip_parser.add_argument(
        "--folder",
        default="unread",
        help="Folder to fetch: unread, archive, starred, or folder ID (default: unread)",
    )
    ingest_ip_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of bookmarks to fetch per request (default: 50)",
    )

    # Command: ingest-github
    ingest_gh_parser = subparsers.add_parser(
        "ingest-github",
        help="Ingest GitHub repository issues and/or user gists",
    )
    ingest_gh_parser.add_argument(
        "--repo", "-r",
        action="append",
        help="Repository to ingest in owner/repo format (can be repeated)",
    )
    ingest_gh_parser.add_argument(
        "--repos",
        help="Comma-separated list of repositories (e.g. owner/repo1,owner/repo2)",
    )
    ingest_gh_parser.add_argument(
        "--token",
        help="GitHub personal access token (defaults to GITHUB_TOKEN)",
    )
    ingest_gh_parser.add_argument(
        "--state",
        choices=["all", "open", "closed"],
        default="all",
        help="Issue state to fetch: all, open, or closed (default: all)",
    )
    ingest_gh_parser.add_argument(
        "--include-gists",
        action="store_true",
        help="Include user gists in ingestion",
    )
    ingest_gh_parser.add_argument(
        "--gists-only",
        action="store_true",
        help="Ingest only user gists",
    )
    ingest_gh_parser.add_argument(
        "--no-comments",
        action="store_true",
        help="Do not fetch issue comments",
    )
    ingest_gh_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum issues/gists to fetch per repository/gists",
    )

    # Command: ingest-reddit
    ingest_reddit_parser = subparsers.add_parser(
        "ingest-reddit",
        help="Ingest Reddit posts and comments from configured subreddits",
    )
    ingest_reddit_parser.add_argument(
        "--subreddit", "-sub",
        action="append",
        help="Subreddit to ingest (can be repeated, e.g. -sub programming)",
    )
    ingest_reddit_parser.add_argument(
        "--subreddits",
        help="Comma-separated list of subreddits (e.g. programming,MachineLearning)",
    )
    ingest_reddit_parser.add_argument(
        "--client-id",
        help="Reddit application client ID (defaults to REDDIT_CLIENT_ID)",
    )
    ingest_reddit_parser.add_argument(
        "--client-secret",
        help="Reddit application client secret (defaults to REDDIT_CLIENT_SECRET)",
    )
    ingest_reddit_parser.add_argument(
        "--user-agent",
        help="Reddit application User-Agent (defaults to REDDIT_USER_AGENT)",
    )
    ingest_reddit_parser.add_argument(
        "--token",
        help="Reddit OAuth access token override (defaults to REDDIT_ACCESS_TOKEN)",
    )
    ingest_reddit_parser.add_argument(
        "--listing",
        choices=["hot", "new", "top", "rising"],
        default="hot",
        help="Subreddit listing type to fetch: hot, new, top, rising (default: hot)",
    )
    ingest_reddit_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum posts to fetch per subreddit",
    )
    ingest_reddit_parser.add_argument(
        "--max-comments",
        type=int,
        default=50,
        help="Maximum comments to fetch per post (default: 50)",
    )
    ingest_reddit_parser.add_argument(
        "--no-comments",
        action="store_true",
        help="Do not fetch post comments",
    )

    # Command: ingest-slack
    ingest_slack_parser = subparsers.add_parser(
        "ingest-slack",
        help="Ingest Slack conversation messages and threads from configured channels",
    )
    ingest_slack_parser.add_argument(
        "--channel", "-c",
        action="append",
        help="Channel to ingest (can be repeated, e.g. -c general)",
    )
    ingest_slack_parser.add_argument(
        "--channels",
        help="Comma-separated list of channels (e.g. general,engineering)",
    )
    ingest_slack_parser.add_argument(
        "--token",
        help="Slack API OAuth token override (defaults to SLACK_TOKEN or SLACK_BOT_TOKEN)",
    )
    ingest_slack_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum messages to fetch per channel",
    )
    ingest_slack_parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum messages to fetch per channel (alias for --limit)",
    )
    ingest_slack_parser.add_argument(
        "--max-replies",
        type=int,
        default=50,
        help="Maximum replies to fetch per thread (default: 50)",
    )
    ingest_slack_parser.add_argument(
        "--no-threads",
        action="store_true",
        help="Do not fetch threaded replies",
    )

    # Command: ingest-discord
    ingest_discord_parser = subparsers.add_parser(
        "ingest-discord",
        help="Ingest Discord conversation messages and threads from configured guild channels",
    )
    ingest_discord_parser.add_argument(
        "--guild", "-g",
        action="append",
        help="Guild/server ID or name to ingest (can be repeated, e.g. -g 123456789012345678)",
    )
    ingest_discord_parser.add_argument(
        "--guilds",
        help="Comma-separated list of guild IDs or names (e.g. 123456789012345678,234567890123456789)",
    )
    ingest_discord_parser.add_argument(
        "--channel", "-c",
        action="append",
        help="Channel ID or name to ingest (can be repeated, e.g. -c 234567890123456789 or -c general)",
    )
    ingest_discord_parser.add_argument(
        "--channels",
        help="Comma-separated list of channel IDs or names (e.g. 234567890123456789,general)",
    )
    ingest_discord_parser.add_argument(
        "--token",
        help="Discord Bot token override (defaults to DISCORD_BOT_TOKEN)",
    )
    ingest_discord_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum messages to fetch per channel",
    )
    ingest_discord_parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum messages to fetch per channel (alias for --limit)",
    )
    ingest_discord_parser.add_argument(
        "--max-replies",
        type=int,
        default=50,
        help="Maximum replies to fetch per thread (default: 50)",
    )
    ingest_discord_parser.add_argument(
        "--no-threads",
        action="store_true",
        help="Do not fetch threaded replies",
    )

    # Command: ingest-telegram
    ingest_telegram_parser = subparsers.add_parser(
        "ingest-telegram",
        help="Ingest Telegram conversation messages and channel posts from configured chats",
    )
    ingest_telegram_parser.add_argument(
        "--chat", "-c",
        action="append",
        help="Chat ID, username (e.g. @my_channel), or title to ingest (can be repeated)",
    )
    ingest_telegram_parser.add_argument(
        "--chats",
        help="Comma-separated list of chat IDs, usernames, or titles",
    )
    ingest_telegram_parser.add_argument(
        "--token",
        help="Telegram Bot token override (defaults to TELEGRAM_BOT_TOKEN)",
    )
    ingest_telegram_parser.add_argument(
        "--limit",
        type=int,
        help="Maximum messages to fetch (default: 50)",
    )
    ingest_telegram_parser.add_argument(
        "--max-messages",
        type=int,
        help="Maximum messages to fetch (alias for --limit)",
    )
    ingest_telegram_parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries on rate limits (default: 3)",
    )
    ingest_telegram_parser.add_argument(
        "--offset",
        type=int,
        help="Identifier of first update to retrieve",
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

        elif args.command in {"ingest", "ingest-web", "ingest-youtube", "ingest-email", "ingest-rss", "ingest-notion", "ingest-google-keep", "ingest-readwise", "ingest-instapaper", "ingest-github", "ingest-reddit", "ingest-slack"}:
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
            elif args.command == "ingest-google-keep":
                source_name = "google-keep"
            elif args.command == "ingest-readwise":
                source_name = "readwise"
            elif args.command == "ingest-instapaper":
                source_name = "instapaper"
            elif args.command == "ingest-github":
                source_name = "github"
            elif args.command == "ingest-reddit":
                source_name = "reddit"
            elif args.command == "ingest-slack":
                source_name = "slack"
            elif args.command == "ingest-discord":
                source_name = "discord"
            elif args.command == "ingest-telegram":
                source_name = "telegram"
            else:
                source_name = getattr(args, "source", None) or "web"

            kwargs: Dict[str, Any] = {}
            if getattr(args, "url", None):
                kwargs["url"] = args.url
            if getattr(args, "file", None):
                kwargs["file"] = args.file
            if getattr(args, "path", None):
                kwargs["path"] = args.path
            if getattr(args, "provider", None):
                kwargs["provider"] = args.provider
            if getattr(args, "thread_id", None):
                kwargs["thread_id"] = args.thread_id
            if getattr(args, "page_id", None):
                kwargs["page_id"] = args.page_id
            if getattr(args, "token", None):
                kwargs["token"] = args.token
            if getattr(args, "guild", None):
                kwargs["guild"] = args.guild
            if getattr(args, "guilds", None):
                kwargs["guilds"] = args.guilds
            if getattr(args, "book_id", None):
                kwargs["book_id"] = args.book_id
            if getattr(args, "updated_after", None):
                kwargs["updated_after"] = args.updated_after
            if getattr(args, "username", None):
                kwargs["username"] = args.username
            if getattr(args, "password", None):
                kwargs["password"] = args.password
            if getattr(args, "folder", None):
                kwargs["folder"] = args.folder
            if getattr(args, "limit", None):
                kwargs["limit"] = args.limit
            if getattr(args, "repo", None):
                kwargs["repo"] = args.repo
            if getattr(args, "repos", None):
                kwargs["repos"] = args.repos
            if getattr(args, "state", None):
                kwargs["state"] = args.state
            if getattr(args, "include_gists", False):
                kwargs["include_gists"] = True
            if getattr(args, "gists_only", False):
                kwargs["gists_only"] = True
            if getattr(args, "no_comments", False):
                kwargs["include_comments"] = False
            if getattr(args, "subreddit", None):
                kwargs["subreddit"] = args.subreddit
            if getattr(args, "subreddits", None):
                kwargs["subreddits"] = args.subreddits
            if getattr(args, "client_id", None):
                kwargs["client_id"] = args.client_id
            if getattr(args, "client_secret", None):
                kwargs["client_secret"] = args.client_secret
            if getattr(args, "user_agent", None):
                kwargs["user_agent"] = args.user_agent
            if getattr(args, "listing", None):
                kwargs["listing"] = args.listing
            if getattr(args, "max_comments", None) is not None:
                kwargs["max_comments"] = args.max_comments
            if getattr(args, "channel", None):
                kwargs["channel"] = args.channel
            if getattr(args, "channels", None):
                kwargs["channels"] = args.channels
            if getattr(args, "max_messages", None) is not None:
                kwargs["max_messages"] = args.max_messages
            if getattr(args, "max_replies", None) is not None:
                kwargs["max_replies"] = args.max_replies
            if getattr(args, "no_threads", False):
                kwargs["no_threads"] = True
                kwargs["include_threads"] = False
            if getattr(args, "chat", None):
                kwargs["chat"] = args.chat
            if getattr(args, "chats", None):
                kwargs["chats"] = args.chats
            if getattr(args, "offset", None) is not None:
                kwargs["offset"] = args.offset
            if getattr(args, "max_retries", None) is not None:
                kwargs["max_retries"] = args.max_retries

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
