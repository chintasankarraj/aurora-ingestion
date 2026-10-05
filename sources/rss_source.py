"""RSS and Atom feed source connector for Aurora External Data Ingestion Pipeline.

Parses RSS 2.0 and Atom feeds using feedparser, extracts individual articles,
optionally expands summary-only items using trafilatura, processes embedded images,
and outputs clean Markdown notes into Ingested/RSS/ with YAML frontmatter.
"""

from __future__ import annotations

import email.utils
import hashlib
import logging
import re
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from bs4 import BeautifulSoup
import feedparser
from markdownify import markdownify
import requests
import trafilatura

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = "AuroraIngestion/1.0 (+https://github.com/aurora-agent; RSSIngestionPipeline)"
MD_IMAGE_PATTERN = re.compile(r'!\[(.*?)\]\((.*?)\)')
FORBIDDEN_CHARS_PATTERN = re.compile(r'[?:*"<>|/\\]')


def normalize_url(raw_url: str) -> str:
    """Normalize an RSS/Atom feed or article URL for deterministic deduplication."""
    if not raw_url or not isinstance(raw_url, str):
        raise SourceError("A valid URL string is required.")

    stripped = raw_url.strip()
    defragged, _ = urllib.parse.urldefrag(stripped)

    try:
        parts = urllib.parse.urlsplit(defragged)
    except Exception as e:
        raise SourceError(f"Malformed URL '{raw_url}': {e}") from e

    scheme = (parts.scheme or "").lower()
    if not scheme:
        raise SourceError(f"URL is missing an HTTP/HTTPS scheme: {raw_url}")
    if scheme not in {"http", "https"}:
        raise SourceError(
            f"Unsupported URL scheme '{scheme}'. Only http:// and https:// are supported."
        )

    netloc = (parts.netloc or "").lower()
    if not netloc:
        raise SourceError(f"URL is missing host domain: {raw_url}")

    # Remove standard default ports
    if (scheme == "http" and netloc.endswith(":80")) or (
        scheme == "https" and netloc.endswith(":443")
    ):
        netloc = netloc.rsplit(":", 1)[0]

    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urllib.parse.urlunsplit((scheme, netloc, path, parts.query, ""))


def clean_html_to_markdown(raw_html: str) -> str:
    """Convert raw HTML content or summary into clean, readable Markdown."""
    if not raw_html or not str(raw_html).strip():
        return ""

    try:
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe"]):
            tag.decompose()

        md_text = markdownify(
            str(soup),
            heading_style="ATX",
            strip=["style", "script"],
        )
        clean_md = re.sub(r"\n{3,}", "\n\n", md_text).strip()
        return clean_md
    except Exception as e:
        logger.warning(f"Error converting HTML to Markdown, falling back to plain text: {e}")
        soup = BeautifulSoup(raw_html, "html.parser")
        return soup.get_text("\n\n").strip()


def parse_feed_date(entry: Any) -> Tuple[str, str]:
    """Extract and parse publication date from feed entry into (YYYY-MM-DD, display_string)."""
    # 1. Try struct_time parsed by feedparser
    for field_name in ("published_parsed", "updated_parsed", "created_parsed"):
        st = getattr(entry, field_name, None) or entry.get(field_name)
        if st and hasattr(st, "__getitem__"):
            try:
                dt = datetime(*st[:6], tzinfo=timezone.utc)
                return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass

    # 2. Try raw string dates (RFC 2822, ISO 8601)
    for field_name in ("published", "updated", "created", "pubDate"):
        raw_val = getattr(entry, field_name, None) or entry.get(field_name)
        if raw_val and isinstance(raw_val, str) and raw_val.strip():
            raw_str = raw_val.strip()
            # RFC 2822
            try:
                dt = email.utils.parsedate_to_datetime(raw_str)
                if dt:
                    return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass
            # ISO 8601
            try:
                clean_iso = raw_str.replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_iso)
                return dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M")
            except Exception:
                pass

    # 3. Fallback to current UTC date
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d %H:%M")


def is_tracking_pixel_or_icon(url: str, alt_text: str = "") -> bool:
    """Detect tracking pixels, beacons, or small spacers."""
    lower_url = (url or "").lower()
    lower_alt = (alt_text or "").lower()

    tracking_signatures = [
        "pixel", "tracking", "tracker", "beacon", "telemetry",
        "1x1", "spacer.gif", "blank.gif", "stats.wp.com", "analytics",
        "feedburner", "feedads",
    ]
    if any(sig in lower_url for sig in tracking_signatures):
        return True

    if any(sig in lower_alt for sig in ["tracking pixel", "spacer", "beacon"]):
        return True

    return False


class RSSSource(BaseSource):
    """Source connector for ingesting RSS 2.0 and Atom feeds into Aurora vault."""

    source_type = "rss"
    display_name = "RSS / Atom Feeds"

    def __init__(
        self,
        timeout: int = 15,
        session: Optional[requests.Session] = None,
        min_content_length: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        self.min_content_length = min_content_length

    def fetch_feed_xml(self, url: str) -> Tuple[str, str]:
        """Fetch RSS/Atom feed XML safely with error handling and timeouts."""
        norm_url = normalize_url(url)
        try:
            resp = self.session.get(norm_url, timeout=self.timeout)
        except requests.exceptions.SSLError as e:
            raise SourceError(f"SSL certificate verification failed for feed {url}: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise SourceError(f"Network connection failed fetching feed {url}: {e}") from e
        except requests.exceptions.Timeout as e:
            raise SourceError(f"Request timed out after {self.timeout}s fetching feed {url}: {e}") from e
        except requests.exceptions.RequestException as e:
            raise SourceError(f"Network error fetching feed {url}: {e}") from e

        if resp.status_code == 404:
            raise SourceError(f"Feed not found (HTTP 404): {url}")
        if resp.status_code == 403:
            raise SourceError(f"Access forbidden (HTTP 403): {url}")
        if resp.status_code == 429:
            raise SourceError(f"Rate limited by server (HTTP 429): {url}")
        if resp.status_code >= 400:
            raise SourceError(f"HTTP error {resp.status_code} fetching feed {url}: {resp.reason}")

        text = resp.text
        if not text or not text.strip():
            raise SourceError(f"Feed returned empty content: {url}")

        final_url = resp.url or norm_url
        return text, final_url

    def _fetch_article_content(self, article_url: str) -> Optional[str]:
        """Fetch article URL and extract main article content via trafilatura."""
        try:
            resp = self.session.get(article_url, timeout=self.timeout)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} fetching article page {article_url}")
                return None
            html = resp.text
            if not html or not html.strip():
                return None

            extracted_md = trafilatura.extract(
                html,
                include_formatting=True,
                include_images=True,
                include_links=True,
                include_tables=True,
                output_format="markdown",
                url=article_url,
            )
            if extracted_md and extracted_md.strip():
                return extracted_md.strip()
        except Exception as e:
            logger.warning(f"Failed to fetch/extract full article from {article_url}: {e}")
        return None

    def _process_images(
        self, markdown_text: str, base_url: str
    ) -> Tuple[str, List[Attachment]]:
        """Download embedded article images and replace references with Obsidian embeds."""
        attachments: List[Attachment] = []
        updated_markdown = markdown_text

        matches = MD_IMAGE_PATTERN.findall(markdown_text)
        image_counter = 1
        seen_filenames: set[str] = set()
        downloaded_images: Dict[str, str] = {}

        for alt_text, img_url in matches:
            cleaned_url = img_url.strip()
            if not cleaned_url or cleaned_url.startswith("data:"):
                continue

            if is_tracking_pixel_or_icon(cleaned_url, alt_text):
                # Remove tracking pixel embed
                updated_markdown = updated_markdown.replace(f"![{alt_text}]({img_url})", "", 1)
                continue

            full_img_url = urllib.parse.urljoin(base_url, cleaned_url)

            # If this exact image URL was already downloaded in this article, reuse its embed
            if full_img_url in downloaded_images:
                existing_name = downloaded_images[full_img_url]
                obsidian_embed = f"![[{existing_name}]]"
                updated_markdown = updated_markdown.replace(
                    f"![{alt_text}]({img_url})", obsidian_embed, 1
                )
                continue

            # Determine safe base filename
            url_path = urllib.parse.urlsplit(full_img_url).path
            raw_filename = Path(url_path).name or f"image_{image_counter}.jpg"
            clean_filename = FORBIDDEN_CHARS_PATTERN.sub("_", raw_filename).strip()
            if not re.search(r"\.(jpe?g|png|gif|webp|svg)$", clean_filename, re.I):
                clean_filename = f"{clean_filename}.jpg"

            # Allocate unique filename within this article
            stem = Path(clean_filename).stem
            suffix = Path(clean_filename).suffix
            candidate_name = clean_filename
            counter = 2
            while candidate_name in seen_filenames:
                candidate_name = f"{stem}_{counter}{suffix}"
                counter += 1
            clean_filename = candidate_name

            # Fetch image binary
            try:
                img_resp = self.session.get(full_img_url, timeout=self.timeout)
                if img_resp.status_code == 200 and img_resp.content:
                    img_bytes = img_resp.content
                    if len(img_bytes) < 100:
                        # Skip tiny images
                        updated_markdown = updated_markdown.replace(f"![{alt_text}]({img_url})", "", 1)
                        continue

                    mime_type = (
                        img_resp.headers.get("Content-Type", "")
                        .split(";")[0]
                        .strip()
                        or "image/jpeg"
                    )

                    seen_filenames.add(clean_filename)
                    downloaded_images[full_img_url] = clean_filename
                    attachments.append(
                        Attachment(
                            filename=clean_filename,
                            content=img_bytes,
                            mime_type=mime_type,
                        )
                    )

                    # Replace markdown image syntax with Obsidian embed
                    obsidian_embed = f"![[{clean_filename}]]"
                    updated_markdown = updated_markdown.replace(
                        f"![{alt_text}]({img_url})", obsidian_embed, 1
                    )
                    image_counter += 1
            except Exception as e:
                logger.warning(f"Failed to download image {full_img_url}: {e}")
                # Keep original markdown reference if download fails

        return updated_markdown, attachments

    def _build_source_id(
        self,
        entry: Any,
        feed_url: str,
        article_url: str,
        item_title: str,
        pub_date: str,
    ) -> str:
        """Derive deterministic, stable source identity for a feed item."""
        # 1. Preferred: GUID / ID
        guid = getattr(entry, "id", None) or entry.get("id") or getattr(entry, "guid", None) or entry.get("guid")
        if guid and str(guid).strip():
            return f"rss:{str(guid).strip()}"

        # 2. Fallback: Normalized article URL
        if article_url and str(article_url).strip():
            try:
                norm_art = normalize_url(article_url)
                return f"rss:url:{norm_art}"
            except Exception:
                pass

        # 3. Stable fallback derived from feed URL + title + publication date
        feed_norm = ""
        try:
            feed_norm = normalize_url(feed_url)
        except Exception:
            feed_norm = feed_url or "unknown_feed"

        digest = hashlib.sha256(
            f"{feed_norm}:{item_title}:{pub_date}".encode("utf-8")
        ).hexdigest()[:16]
        return f"rss:{feed_norm}:{digest}"

    async def fetch_items(
        self,
        url: Optional[str] = None,
        feed_content: Optional[str] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch and parse feed items from an RSS or Atom feed."""
        if feed_content:
            xml_text = feed_content
            feed_url = url or "https://example.com/feed"
        elif url:
            xml_text, feed_url = self.fetch_feed_xml(url)
        else:
            raise SourceError("Feed URL is required for RSS ingestion. Use --url <feed_url>")

        parsed = feedparser.parse(xml_text)

        # Validate feed parsing
        if parsed.bozo and not parsed.entries and not getattr(parsed, "feed", None):
            bozo_exc = getattr(parsed, "bozo_exception", "Unknown XML parse error")
            raise SourceError(f"Malformed or invalid feed at {feed_url}: {bozo_exc}")

        if not parsed.entries:
            raise SourceError(f"Feed at {feed_url} contains no entries or articles.")

        # Feed-level metadata
        feed_obj = getattr(parsed, "feed", {})
        feed_title = feed_obj.get("title", "").strip() or "RSS Feed"
        feed_link = feed_obj.get("link", "").strip() or feed_url
        feed_desc = (
            feed_obj.get("description", "").strip()
            or feed_obj.get("subtitle", "").strip()
        )
        feed_lang = feed_obj.get("language", "").strip()

        items: List[SourceItem] = []

        for entry in parsed.entries:
            try:
                item_title = entry.get("title", "").strip() or "Untitled Article"
                article_url = entry.get("link", "").strip()

                pub_date, pub_date_display = parse_feed_date(entry)

                # Author
                author = entry.get("author", "").strip()
                if not author and entry.get("authors"):
                    author = entry["authors"][0].get("name", "").strip()

                guid = (
                    getattr(entry, "id", None)
                    or entry.get("id")
                    or getattr(entry, "guid", None)
                    or entry.get("guid")
                    or ""
                ).strip()

                summary = entry.get("summary", "").strip()

                # Determine full content vs summary according to INGESTION_PIPELINE.md:
                # - If the RSS/Atom entry contains a non-empty content field (for example RSS content:encoded
                #   or Atom <content>), treat that as the feed-provided article content and use it.
                # - If there is no usable content field, treat summary / description as summary/excerpt content.
                content_entries = entry.get("content")
                usable_content = ""
                if content_entries:
                    if isinstance(content_entries, list):
                        for c in content_entries:
                            val = c.get("value", "") if isinstance(c, dict) else str(c)
                            if val and val.strip():
                                usable_content = val.strip()
                                break
                    elif isinstance(content_entries, str) and content_entries.strip():
                        usable_content = content_entries.strip()

                summary_val = summary or entry.get("description", "")

                article_md = ""
                # Case A: Full content exists in feed
                if usable_content:
                    article_md = clean_html_to_markdown(usable_content)
                else:
                    # Case B: Feed only provides summary/excerpt -> Attempt trafilatura expansion
                    expanded = None
                    if article_url and article_url.startswith(("http://", "https://")):
                        expanded = self._fetch_article_content(article_url)

                    if expanded and expanded.strip():
                        article_md = expanded
                    else:
                        # Case C: Fallback to feed summary/excerpt
                        article_md = clean_html_to_markdown(summary_val)

                if not article_md:
                    article_md = f"No article content available for '{item_title}'."

                # Avoid duplicate leading H1 matching title
                h1_line = f"# {item_title}"
                if article_md.startswith(h1_line):
                    article_md = article_md[len(h1_line):].strip()

                # Process images
                base_ref = article_url or feed_url
                article_md, attachments = self._process_images(article_md, base_ref)

                # Extract and sanitize categories/tags
                tags: List[str] = ["ingested", "rss"]
                entry_tags = entry.get("tags", [])
                if isinstance(entry_tags, list):
                    for t in entry_tags:
                        raw_tag = t.get("term") or t.get("label") if isinstance(t, dict) else str(t)
                        if raw_tag:
                            clean_tag = re.sub(r"[^a-zA-Z0-9_-]", "", raw_tag.lower().strip().replace(" ", "-"))
                            if clean_tag and clean_tag not in tags:
                                tags.append(clean_tag)

                # Deduplication identity
                source_id = self._build_source_id(
                    entry=entry,
                    feed_url=feed_url,
                    article_url=article_url,
                    item_title=item_title,
                    pub_date=pub_date,
                )

                # Extra metadata
                extra_metadata: Dict[str, Any] = {
                    "feed_title": feed_title,
                    "feed_url": feed_url,
                }
                if guid:
                    extra_metadata["guid"] = guid
                if feed_desc:
                    extra_metadata["feed_description"] = feed_desc
                if feed_lang:
                    extra_metadata["language"] = feed_lang
                if pub_date_display:
                    extra_metadata["published_date"] = pub_date_display

                # Clean summary text for metadata
                summary_clean = clean_html_to_markdown(summary) if summary else None

                source_item = SourceItem(
                    source_type="rss",
                    source_id=source_id,
                    title=item_title,
                    content=article_md,
                    source_url=article_url or feed_url,
                    date=pub_date,
                    author=author or None,
                    tags=tags,
                    summary=summary_clean or None,
                    attachments=attachments,
                    extra_metadata=extra_metadata,
                )
                items.append(source_item)
            except Exception as e:
                logger.warning(f"Error processing RSS entry '{entry.get('title', 'Unknown')}': {e}", exc_info=True)

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a fetched RSS SourceItem into a canonical MarkdownNote."""
        feed_title = item.extra_metadata.get("feed_title", "RSS Feed")
        feed_url = item.extra_metadata.get("feed_url", "")
        pub_date_display = item.extra_metadata.get("published_date", item.date)

        attribution_url = item.source_url or feed_url or ""
        source_ref = f"[{item.title}]({attribution_url})" if attribution_url else item.title

        body_lines: List[str] = [
            f"# {item.title}",
            "",
            f"> **Source**: {source_ref}",
        ]

        meta_parts: List[str] = []
        if feed_title:
            feed_ref = f"[{feed_title}]({feed_url})" if feed_url else feed_title
            meta_parts.append(f"**Feed**: {feed_ref}")
        if item.author:
            meta_parts.append(f"**Author**: {item.author}")
        if pub_date_display:
            meta_parts.append(f"**Published**: {pub_date_display}")

        if meta_parts:
            body_lines.append(f"> {' · '.join(meta_parts)}")

        body_lines.extend(["", item.content])
        full_body = "\n".join(body_lines).strip() + "\n"

        attachment_filenames = [a.filename for a in item.attachments]

        return MarkdownNote(
            title=item.title,
            source="rss",
            date=item.date or "",
            body=full_body,
            tags=list(item.tags) if item.tags else ["ingested", "rss"],
            source_url=item.source_url,
            author=item.author,
            summary=item.summary,
            language=item.extra_metadata.get("language"),
            attachments=attachment_filenames,
            extra_metadata=dict(item.extra_metadata),
        )


# Register connector with global SourceRegistry
SourceRegistry.register("rss", RSSSource)
