"""Sources package for Aurora Ingestion Pipeline."""

from sources.base import BaseSource, SourceRegistry, async_retry
from sources.email_source import EmailSource
from sources.instapaper_source import InstapaperSource
from sources.keep_source import KeepSource
from sources.notion_source import NotionSource
from sources.pdf_source import PDFSource
from sources.readwise_source import ReadwiseSource
from sources.rss_source import RSSSource
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
]

