"""Sources package for Aurora Ingestion Pipeline."""

from sources.base import BaseSource, SourceRegistry, async_retry
from sources.discord_source import DiscordSource
from sources.email_source import EmailSource
from sources.github_source import GitHubSource
from sources.google_drive_source import GoogleDriveSource
from sources.instapaper_source import InstapaperSource
from sources.keep_source import KeepSource
from sources.notion_source import NotionSource
from sources.pdf_source import PDFSource
from sources.readwise_source import ReadwiseSource
from sources.reddit_source import RedditSource
from sources.rss_source import RSSSource
from sources.screenshot_source import ScreenshotSource
from sources.slack_source import SlackSource
from sources.telegram_source import TelegramSource
from sources.voice_source import VoiceSource
from sources.web_source import WebSource
from sources.youtube_source import YouTubeSource

__all__ = [
    "BaseSource",
    "SourceRegistry",
    "async_retry",
    "PDFSource",
    "WebSource",
    "YouTubeSource",
    "EmailSource",
    "RSSSource",
    "NotionSource",
    "KeepSource",
    "ReadwiseSource",
    "InstapaperSource",
    "GitHubSource",
    "RedditSource",
    "SlackSource",
    "DiscordSource",
    "TelegramSource",
    "VoiceSource",
    "ScreenshotSource",
    "GoogleDriveSource",
]

