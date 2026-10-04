"""Web page source connector for Aurora External Data Ingestion Pipeline.

Fetches web articles, extracts primary content (stripping navigation, ads, headers,
and footers) using trafilatura and markdownify, downloads relevant images into
Attachments/Ingested/, and produces clean Obsidian Markdown notes in Ingested/Web/.
"""

from __future__ import annotations

import hashlib
import logging
import re
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

from bs4 import BeautifulSoup
from markdownify import markdownify
import requests
import trafilatura

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger(__name__)

# Disallowed characters in attachment filenames
FORBIDDEN_CHARS_PATTERN = re.compile(r'[?:*"<>|/\\]')

# Markdown image pattern: ![alt](url)
MD_IMAGE_PATTERN = re.compile(r'!\[(.*?)\]\((.*?)\)')

DEFAULT_USER_AGENT = "AuroraIngestion/1.0 (+https://github.com/aurora-agent; WebIngestionPipeline)"


def normalize_url(raw_url: str) -> str:
    """Normalize a webpage URL for deterministic deduplication."""
    if not raw_url or not isinstance(raw_url, str):
        raise SourceError("A valid URL string is required.")

    stripped = raw_url.strip()
    # Strip URL fragment (#section)
    defragged, _ = urllib.parse.urldefrag(stripped)

    try:
        parts = urllib.parse.urlsplit(defragged)
    except Exception as e:
        raise SourceError(f"Malformed URL '{raw_url}': {e}") from e

    scheme = (parts.scheme or "").lower()
    if not scheme:
        raise SourceError(f"URL is missing an HTTP/HTTPS scheme: {raw_url}")
    if scheme not in {"http", "https"}:
        raise SourceError(f"Unsupported URL scheme '{scheme}'. Only http:// and https:// are supported.")

    netloc = (parts.netloc or "").lower()
    if not netloc:
        raise SourceError(f"URL is missing host domain: {raw_url}")

    # Remove standard default ports
    if (scheme == "http" and netloc.endswith(":80")) or (scheme == "https" and netloc.endswith(":443")):
        netloc = netloc.rsplit(":", 1)[0]

    path = parts.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return urllib.parse.urlunsplit((scheme, netloc, path, parts.query, ""))


def is_tracking_pixel_or_icon(url: str, alt_text: str = "") -> bool:
    """Heuristic to identify tracking pixels, tiny spacers, or decorative icons."""
    lower_url = url.lower()
    lower_alt = alt_text.lower()

    tracking_signatures = [
        "pixel", "tracking", "tracker", "beacon", "telemetry",
        "1x1", "spacer.gif", "blank.gif", "stats.wp.com", "analytics",
    ]
    if any(sig in lower_url for sig in tracking_signatures):
        return True

    if any(sig in lower_alt for sig in ["tracking pixel", "spacer", "beacon"]):
        return True

    return False


class WebSource(BaseSource):
    """Source connector for ingesting web pages and articles into Aurora vault."""

    def __init__(self, timeout: int = 15, session: Optional[requests.Session] = None) -> None:
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    @property
    def source_type(self) -> str:
        return "web"

    @property
    def display_name(self) -> str:
        return "Web Articles"

    def fetch_page_html(self, url: str) -> Tuple[str, str]:
        """Fetch webpage HTML safely with timeouts and error handling."""
        try:
            resp = self.session.get(url, timeout=self.timeout)
        except requests.exceptions.SSLError as e:
            raise SourceError(f"SSL certificate verification failed for {url}: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise SourceError(f"Network connection failed for {url}: {e}") from e
        except requests.exceptions.Timeout as e:
            raise SourceError(f"Request timed out after {self.timeout}s fetching {url}: {e}") from e
        except requests.exceptions.RequestException as e:
            raise SourceError(f"Network error fetching {url}: {e}") from e

        if resp.status_code == 404:
            raise SourceError(f"Webpage not found (HTTP 404): {url}")
        if resp.status_code == 403:
            raise SourceError(f"Access forbidden (HTTP 403): {url}")
        if resp.status_code == 429:
            raise SourceError(f"Rate limited by server (HTTP 429): {url}")
        if resp.status_code >= 400:
            raise SourceError(f"HTTP error {resp.status_code} fetching {url}: {resp.reason}")

        text = resp.text
        if not text or not text.strip():
            raise SourceError(f"Webpage returned empty content: {url}")

        final_url = resp.url or url
        return text, final_url

    def _extract_main_content_and_metadata(self, html: str, page_url: str) -> dict[str, Any]:
        """Extract article metadata and clean Markdown content from HTML."""
        # Extract metadata via trafilatura
        meta = trafilatura.extract_metadata(html)

        # Fallback soup for title and basic metadata
        soup = BeautifulSoup(html, "html.parser")
        title: Optional[str] = None
        if meta and meta.title:
            title = meta.title.strip()
        if not title:
            h1 = soup.find("h1")
            if h1 and h1.get_text().strip():
                title = h1.get_text().strip()
            elif soup.title and soup.title.get_text().strip():
                title = soup.title.get_text().strip()
            else:
                path_stem = Path(urllib.parse.urlsplit(page_url).path).stem
                title = path_stem.replace("-", " ").replace("_", " ").title() if path_stem else "Web Article"

        # Extract main content via trafilatura
        trafilatura_md = trafilatura.extract(
            html,
            include_formatting=True,
            include_images=True,
            include_links=True,
            include_tables=True,
            output_format="markdown",
            url=page_url,
        )

        final_md: str
        if trafilatura_md and trafilatura_md.strip():
            final_md = trafilatura_md.strip()
        else:
            # Fallback extraction: BeautifulSoup cleanup + markdownify
            for el in soup(["nav", "footer", "header", "aside", "script", "style", "noscript", "iframe"]):
                el.decompose()

            # Target main article container if available
            container = (
                soup.find("article")
                or soup.find("main")
                or soup.find(attrs={"role": "main"})
                or soup.find("div", class_=re.compile(r"(content|article|post|entry)", re.I))
                or soup.body
            )

            if container:
                raw_converted = markdownify(str(container), heading_style="ATX").strip()
                final_md = raw_converted
            else:
                final_md = ""

        if not final_md:
            raise SourceError(f"Failed to extract readable content from {page_url}")

        # Normalize redundant top H1 matching title to avoid duplication
        h1_line = f"# {title}"
        if final_md.startswith(h1_line):
            final_md = final_md[len(h1_line):].strip()

        # Clean excessive blank lines
        final_md = re.sub(r"\n{3,}", "\n\n", final_md)

        # Author
        author = meta.author.strip() if (meta and meta.author) else None

        # Publication date
        date_val: Optional[str] = None
        if meta and meta.date:
            date_val = str(meta.date).strip()
        else:
            date_val = datetime.now().strftime("%Y-%m-%d")

        # Description / Summary
        summary = meta.description.strip() if (meta and meta.description) else None

        # Language
        language = meta.language.strip() if (meta and meta.language) else None

        # Site name
        site_name = meta.sitename.strip() if (meta and meta.sitename) else None

        # Tags
        tags = ["ingested", "web"]
        if meta and meta.categories:
            for c in meta.categories:
                clean_c = re.sub(r"[^a-zA-Z0-9_-]", "", c.lower().strip())
                if clean_c and clean_c not in tags:
                    tags.append(clean_c)

        return {
            "title": title,
            "content": final_md,
            "author": author,
            "date": date_val,
            "summary": summary,
            "language": language,
            "site_name": site_name,
            "tags": tags,
        }

    def _process_images(self, markdown_text: str, page_url: str) -> Tuple[str, List[Attachment]]:
        """Download embedded article images and replace references with Obsidian embeds."""
        attachments: List[Attachment] = []
        updated_markdown = markdown_text

        matches = MD_IMAGE_PATTERN.findall(markdown_text)
        image_counter = 1

        for alt_text, img_url in matches:
            cleaned_url = img_url.strip()
            if not cleaned_url:
                continue

            if cleaned_url.startswith("data:") or is_tracking_pixel_or_icon(cleaned_url, alt_text):
                # Strip tracking pixel or raw data URI from the markdown output
                old_embed = f"![{alt_text}]({img_url})"
                updated_markdown = updated_markdown.replace(old_embed, "")
                continue

            # Resolve relative URLs against the page URL
            resolved_img_url = urllib.parse.urljoin(page_url, cleaned_url)

            try:
                img_resp = self.session.get(resolved_img_url, timeout=10)
                if img_resp.status_code == 200 and img_resp.content:
                    img_bytes = img_resp.content
                    if not img_bytes:
                        continue

                    # Extract filename
                    url_path = urllib.parse.urlsplit(resolved_img_url).path
                    base_name = Path(url_path).name
                    if not base_name or "." not in base_name:
                        ext = ".png"
                        ct = img_resp.headers.get("content-type", "")
                        if "jpeg" in ct or "jpg" in ct:
                            ext = ".jpg"
                        elif "webp" in ct:
                            ext = ".webp"
                        elif "gif" in ct:
                            ext = ".gif"
                        base_name = f"web_image_{image_counter}{ext}"
                        image_counter += 1

                    safe_filename = FORBIDDEN_CHARS_PATTERN.sub("", base_name).strip("_")
                    if not safe_filename:
                        safe_filename = f"web_image_{image_counter}.png"
                        image_counter += 1

                    attachment = Attachment(
                        filename=safe_filename,
                        content=img_bytes,
                        mime_type=img_resp.headers.get("content-type"),
                    )
                    attachments.append(attachment)

                    # Replace in markdown with Obsidian embed
                    old_embed = f"![{alt_text}]({img_url})"
                    new_embed = f"![[{attachment.filename}]]"
                    updated_markdown = updated_markdown.replace(old_embed, new_embed)

            except Exception as e:
                logger.warning(
                    f"Could not download image from '{resolved_img_url}': {e}. Preserving original URL."
                )

        return updated_markdown, attachments

    async def fetch_items(
        self,
        url: Optional[str] = None,
        urls: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch and extract content from one or more URLs.

        Supports:
        - `url`: Single URL string
        - `urls`: List of URL strings
        """
        raw_url = url or kwargs.get("file")
        target_urls: List[str] = []

        if raw_url:
            target_urls.append(raw_url)
        elif urls:
            target_urls.extend(urls)

        if not target_urls:
            raise SourceError("Web source requires a URL. Specify --url <webpage_url>.")

        items: List[SourceItem] = []
        for u in target_urls:
            normalized = normalize_url(u)
            html, final_url = self.fetch_page_html(normalized)
            extracted = self._extract_main_content_and_metadata(html, final_url)

            # Download relevant images and replace embeds
            final_content, attachments = self._process_images(extracted["content"], final_url)

            # Deterministic SHA-256 hash of extracted content
            hasher = hashlib.sha256()
            hasher.update(extracted["title"].encode("utf-8"))
            hasher.update(b"\n")
            hasher.update(final_content.encode("utf-8"))
            content_hash = hasher.hexdigest()

            # Source identity: web:<normalized_url>
            source_id = f"web:{normalized}"

            extra_metadata: dict[str, Any] = {}
            if extracted.get("site_name"):
                extra_metadata["site_name"] = extracted["site_name"]

            item = SourceItem(
                source_id=source_id,
                source_type="web",
                title=extracted["title"],
                content=final_content,
                date=extracted["date"],
                source_url=normalized,
                author=extracted["author"],
                tags=extracted["tags"],
                summary=extracted["summary"],
                language=extracted["language"],
                attachments=attachments,
                extra_metadata=extra_metadata,
                content_hash=content_hash,
            )
            items.append(item)

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a web SourceItem into an Obsidian MarkdownNote."""
        attachment_names = [a.filename for a in item.attachments]

        return MarkdownNote(
            title=item.title,
            source="web",
            date=item.date or datetime.now().strftime("%Y-%m-%d"),
            body=item.content,
            tags=list(item.tags) if item.tags else ["ingested", "web"],
            source_url=item.source_url,
            author=item.author,
            aliases=list(item.aliases),
            status=item.status,
            summary=item.summary,
            language=item.language,
            word_count=item.word_count,
            attachments=attachment_names,
            extra_metadata=dict(item.extra_metadata),
            folder="Ingested/Web",
        )


# Register WebSource in the global SourceRegistry
SourceRegistry.register("web", WebSource)
