"""YouTube video source connector for Aurora Ingestion Pipeline.

Extracts YouTube video metadata via yt-dlp, transcript via youtube-transcript-api,
and downloads video thumbnail into the vault attachments folder.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

import requests
import yt_dlp
from youtube_transcript_api import (
    FetchedTranscript,
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
    YouTubeTranscriptApi,
    YouTubeTranscriptApiException,
)

from exceptions import SourceError
from models import Attachment, MarkdownNote, SourceItem
from sources.base import BaseSource, SourceRegistry

logger = logging.getLogger("aurora.ingestion.youtube")

# YouTube 11-character video ID regex
YOUTUBE_ID_REGEX = re.compile(r"^[a-zA-Z0-9_-]{11}$")


def extract_video_id(raw_url: str) -> str:
    """Extract and validate the 11-character YouTube video ID from various URL formats.

    Supported formats:
    - https://www.youtube.com/watch?v=VIDEO_ID
    - https://www.youtube.com/watch?v=VIDEO_ID&t=10s
    - https://m.youtube.com/watch?v=VIDEO_ID
    - https://youtu.be/VIDEO_ID
    - https://youtu.be/VIDEO_ID?t=10s
    - https://www.youtube.com/shorts/VIDEO_ID
    - https://www.youtube.com/shorts/VIDEO_ID?feature=share
    - https://www.youtube.com/embed/VIDEO_ID
    - https://www.youtube.com/v/VIDEO_ID
    """
    if not raw_url or not isinstance(raw_url, str):
        raise SourceError("YouTube URL must be a non-empty string.")

    url = raw_url.strip()
    if not url:
        raise SourceError("YouTube URL cannot be empty.")

    # Normalize protocol if omitted
    if not url.startswith(("http://", "https://")):
        if url.startswith(("youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com")):
            url = f"https://{url}"
        else:
            raise SourceError(
                f"Invalid or unsupported YouTube URL (missing HTTP/HTTPS scheme): '{raw_url}'"
            )

    try:
        parsed = urlparse(url)
    except Exception as e:
        raise SourceError(f"Malformed YouTube URL '{raw_url}': {e}")

    hostname = (parsed.hostname or "").lower()
    path = parsed.path.strip("/")
    query = parse_qs(parsed.query)

    # Reject playlist-only URLs explicitly
    if "playlist" in path.lower() and "v" not in query:
        raise SourceError(
            f"Playlists are not supported as a single ingestion item: '{raw_url}'. "
            "Please provide an individual video URL."
        )

    video_id: Optional[str] = None

    if hostname in {"youtube.com", "www.youtube.com", "m.youtube.com"}:
        if path == "watch":
            v_list = query.get("v")
            if v_list and len(v_list) > 0:
                video_id = v_list[0]
        elif path.startswith("shorts/"):
            parts = path.split("/")
            if len(parts) >= 2:
                video_id = parts[1]
        elif path.startswith("embed/"):
            parts = path.split("/")
            if len(parts) >= 2:
                video_id = parts[1]
        elif path.startswith("v/"):
            parts = path.split("/")
            if len(parts) >= 2:
                video_id = parts[1]
    elif hostname in {"youtu.be", "www.youtu.be"}:
        parts = path.split("/")
        if parts and parts[0]:
            video_id = parts[0]
    else:
        raise SourceError(f"Unsupported host '{hostname}' in YouTube URL: '{raw_url}'")

    if not video_id:
        raise SourceError(f"Could not extract video ID from YouTube URL: '{raw_url}'")

    video_id = video_id.strip()
    if not YOUTUBE_ID_REGEX.match(video_id):
        raise SourceError(
            f"Invalid YouTube video ID '{video_id}' extracted from '{raw_url}'. "
            "Video ID must be exactly 11 characters."
        )

    return video_id


def format_duration(seconds: Optional[float | int]) -> str:
    """Format duration in seconds into human-readable MM:SS or H:MM:SS."""
    if seconds is None:
        return ""
    try:
        total = int(round(float(seconds)))
    except (ValueError, TypeError):
        return ""

    if total < 0:
        return "00:00"

    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60

    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def format_timestamp(seconds: float | int) -> str:
    """Format timestamp offset in seconds into human-readable MM:SS or HH:MM:SS."""
    try:
        total = max(0, int(round(float(seconds))))
    except (ValueError, TypeError):
        total = 0

    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60

    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def format_transcript_content(
    snippets: Sequence[Any], chunk_interval_seconds: float = 30.0
) -> str:
    """Format transcript snippets into structured Markdown with human-readable timestamp markers.

    Consecutive snippets are grouped into paragraphs of approximately ~30 seconds or when a natural pause occurs.
    """
    if not snippets:
        return ""

    groups: List[Tuple[float, List[str]]] = []
    current_start: Optional[float] = None
    current_texts: List[str] = []
    last_end: float = 0.0

    for s in snippets:
        if isinstance(s, dict):
            raw_text = s.get("text", "")
            start = float(s.get("start", 0.0))
            duration = float(s.get("duration", 0.0))
        else:
            raw_text = getattr(s, "text", "")
            start = float(getattr(s, "start", 0.0))
            duration = float(getattr(s, "duration", 0.0))

        text = str(raw_text).strip()
        if not text:
            continue

        if current_start is None:
            current_start = start
            current_texts = [text]
            last_end = start + duration
        else:
            # If interval elapsed or significant silence gap (> 3s), start new group
            if (start - current_start >= chunk_interval_seconds) or (start - last_end >= 3.0):
                groups.append((current_start, current_texts))
                current_start = start
                current_texts = [text]
                last_end = start + duration
            else:
                current_texts.append(text)
                last_end = max(last_end, start + duration)

    if current_start is not None and current_texts:
        groups.append((current_start, current_texts))

    formatted_sections: List[str] = []
    for grp_start, grp_texts in groups:
        time_tag = format_timestamp(grp_start)
        paragraph = " ".join(grp_texts)
        formatted_sections.append(f"**[{time_tag}]**\n\n{paragraph}")

    return "\n\n".join(formatted_sections)


class YouTubeSource(BaseSource):
    """Source connector for ingesting YouTube videos into Obsidian Markdown notes."""

    source_type = "youtube"
    display_name = "YouTube"

    def __init__(
        self,
        ydl_client: Optional[Any] = None,
        transcript_api: Optional[Any] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        super().__init__()
        self.ydl_client = ydl_client
        self.transcript_api = transcript_api or YouTubeTranscriptApi()
        self.session = session or requests.Session()

    def _parse_info_dict(self, info: Dict[str, Any], video_id: str) -> Dict[str, Any]:
        """Normalize extracted yt-dlp info into a consistent dictionary."""
        title = (info.get("title") or f"YouTube Video {video_id}").strip()
        channel = (
            info.get("uploader")
            or info.get("channel")
            or info.get("uploader_id")
            or ""
        ).strip() or None

        # Upload date (usually YYYYMMDD string)
        upload_date = info.get("upload_date")
        if (
            upload_date
            and isinstance(upload_date, str)
            and len(upload_date) == 8
            and upload_date.isdigit()
        ):
            date_str = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}"
        else:
            date_str = datetime.now().strftime("%Y-%m-%d")

        description = (info.get("description") or "").strip()

        duration_val = info.get("duration")
        duration_str = format_duration(duration_val) if duration_val is not None else None

        # Thumbnail URL
        thumbnail_url = info.get("thumbnail")
        if not thumbnail_url:
            thumbnails = info.get("thumbnails")
            if thumbnails and isinstance(thumbnails, list):
                thumbnail_url = thumbnails[-1].get("url")
        if not thumbnail_url:
            thumbnail_url = f"https://i.ytimg.com/vi/{video_id}/maxresdefault.jpg"

        return {
            "title": title,
            "channel": channel,
            "date": date_str,
            "description": description,
            "duration": duration_str,
            "thumbnail_url": thumbnail_url,
        }

    def _extract_metadata(self, url: str, video_id: str) -> Dict[str, Any]:
        """Extract metadata (title, channel, date, description, duration, thumbnail_url) via yt-dlp."""
        if self.ydl_client is not None:
            try:
                info = self.ydl_client.extract_info(url, download=False)
                return self._parse_info_dict(info, video_id)
            except Exception as e:
                logger.warning(f"Custom ydl_client failed for video '{video_id}': {e}")
                raise SourceError(f"Failed to extract metadata for video '{video_id}': {e}") from e

        ydl_opts = {
            "skip_download": True,
            "quiet": True,
            "no_warnings": True,
            "extract_flat": False,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not info:
                    raise SourceError(f"No metadata returned by yt-dlp for '{video_id}'.")
                return self._parse_info_dict(info, video_id)
        except yt_dlp.utils.DownloadError as e:
            msg = str(e).lower()
            if "private" in msg:
                raise SourceError(f"YouTube video '{video_id}' is private: {e}") from e
            elif "deleted" in msg or "not exist" in msg or "unavailable" in msg:
                raise SourceError(f"YouTube video '{video_id}' is unavailable or deleted: {e}") from e
            raise SourceError(f"Failed to extract metadata for YouTube video '{video_id}': {e}") from e
        except Exception as e:
            raise SourceError(f"Failed to extract metadata for YouTube video '{video_id}': {e}") from e

    def _fetch_transcript(
        self, video_id: str, languages: Sequence[str] = ("en",)
    ) -> Tuple[List[Any], str]:
        """Retrieve transcript snippets and language code using youtube-transcript-api."""
        try:
            # 1. Use standard object API if available
            if hasattr(self.transcript_api, "list"):
                transcript_list = self.transcript_api.list(video_id)
                transcript_obj = None

                # Prefer manually created transcript
                try:
                    transcript_obj = transcript_list.find_manually_created_transcript(languages)
                except Exception:
                    transcript_obj = None

                # Fallback to generated transcript
                if transcript_obj is None:
                    try:
                        transcript_obj = transcript_list.find_generated_transcript(languages)
                    except Exception:
                        transcript_obj = None

                # Fallback to any transcript matching languages
                if transcript_obj is None:
                    try:
                        transcript_obj = transcript_list.find_transcript(languages)
                    except Exception:
                        transcript_obj = None

                # Fallback to first available transcript
                if transcript_obj is None:
                    try:
                        for t in transcript_list:
                            transcript_obj = t
                            break
                    except Exception:
                        transcript_obj = None

                if transcript_obj is None:
                    raise SourceError(f"No transcript available for YouTube video '{video_id}'.")

                snippets = list(transcript_obj.fetch())
                lang_code = getattr(transcript_obj, "language_code", "en")
                return snippets, lang_code

            # 2. Support get_transcript method (e.g. older library versions or simple mocks)
            elif hasattr(self.transcript_api, "get_transcript"):
                snippets = self.transcript_api.get_transcript(video_id, languages=languages)
                return list(snippets), "en"

            else:
                raise SourceError("Invalid transcript API provider configured for YouTube.")

        except (NoTranscriptFound, TranscriptsDisabled) as e:
            raise SourceError(f"No transcript available for YouTube video '{video_id}': {e}") from e
        except VideoUnavailable as e:
            raise SourceError(f"YouTube video '{video_id}' is unavailable or private: {e}") from e
        except YouTubeTranscriptApiException as e:
            raise SourceError(f"YouTube transcript API error for '{video_id}': {e}") from e
        except SourceError:
            raise
        except Exception as e:
            raise SourceError(f"Failed to retrieve transcript for YouTube video '{video_id}': {e}") from e

    def _download_thumbnail(self, thumbnail_url: str, video_id: str) -> Optional[Attachment]:
        """Download video thumbnail and return Attachment object. Logs warning on failure."""
        if not thumbnail_url:
            return None

        try:
            resp = self.session.get(thumbnail_url, timeout=15)
            if resp.status_code != 200:
                logger.warning(
                    f"Thumbnail download returned HTTP {resp.status_code} for video '{video_id}' at {thumbnail_url}"
                )
                return None

            content_type = resp.headers.get("content-type", "image/jpeg").lower()
            if "png" in content_type or thumbnail_url.endswith(".png"):
                ext = ".png"
                mime = "image/png"
            elif "webp" in content_type or thumbnail_url.endswith(".webp"):
                ext = ".webp"
                mime = "image/webp"
            else:
                ext = ".jpg"
                mime = "image/jpeg"

            filename = f"{video_id}_thumbnail{ext}"
            return Attachment(
                filename=filename,
                content=resp.content,
                mime_type=mime,
            )
        except Exception as e:
            logger.warning(
                f"Failed to download thumbnail for video '{video_id}' from {thumbnail_url}: {e}"
            )
            return None

    async def fetch_items(
        self,
        url: Optional[str] = None,
        urls: Optional[Sequence[str]] = None,
        **kwargs: Any,
    ) -> List[SourceItem]:
        """Fetch YouTube video item(s), extracting metadata, transcript, and thumbnail."""
        target_urls: List[str] = []
        if url:
            target_urls.append(url)
        elif urls:
            target_urls.extend(urls)

        if not target_urls:
            raise SourceError("YouTube source requires a URL. Specify --url <youtube_url>.")

        items: List[SourceItem] = []
        for raw_u in target_urls:
            video_id = extract_video_id(raw_u)
            canonical_url = f"https://www.youtube.com/watch?v={video_id}"

            # 1. Fetch metadata
            metadata = self._extract_metadata(canonical_url, video_id)

            # 2. Fetch transcript (fails ingestion if unavailable)
            snippets, lang_code = self._fetch_transcript(video_id)
            if not snippets:
                raise SourceError(f"No transcript content found for YouTube video '{video_id}'.")

            formatted_transcript = format_transcript_content(snippets)

            # 3. Download thumbnail (non-fatal if fails)
            attachments: List[Attachment] = []
            thumbnail_attachment = self._download_thumbnail(metadata["thumbnail_url"], video_id)
            if thumbnail_attachment:
                attachments.append(thumbnail_attachment)

            # 4. Build body parts
            body_parts: List[str] = []

            # Metadata lines
            meta_lines: List[str] = []
            if metadata["channel"]:
                meta_lines.append(f"**Channel:** {metadata['channel']}  ")
            meta_lines.append(f"**Video ID:** {video_id}  ")
            if metadata["duration"]:
                meta_lines.append(f"**Duration:** {metadata['duration']}")

            if meta_lines:
                body_parts.append("\n".join(meta_lines))

            # Thumbnail embed
            if thumbnail_attachment:
                body_parts.append(f"![[{thumbnail_attachment.filename}]]")

            # Description (if present and non-empty)
            if metadata["description"]:
                body_parts.append(f"## Description\n\n{metadata['description']}")

            # Transcript
            body_parts.append(f"## Transcript\n\n{formatted_transcript}")

            body_content = "\n\n".join(body_parts)

            # Compute SHA-256 content hash
            hasher = hashlib.sha256()
            hasher.update(metadata["title"].encode("utf-8"))
            hasher.update(b"\n")
            hasher.update(body_content.encode("utf-8"))
            if thumbnail_attachment and thumbnail_attachment.content:
                hasher.update(b"\n")
                hasher.update(thumbnail_attachment.content)
            content_hash = hasher.hexdigest()

            extra_metadata: Dict[str, Any] = {
                "video_id": video_id,
            }
            if metadata["channel"]:
                extra_metadata["channel"] = metadata["channel"]
            if metadata["duration"]:
                extra_metadata["duration"] = metadata["duration"]

            item = SourceItem(
                source_id=f"youtube:{video_id}",
                source_type="youtube",
                title=metadata["title"],
                content=body_content,
                date=metadata["date"],
                source_url=canonical_url,
                author=metadata["channel"],
                tags=["ingested", "youtube"],
                summary=metadata["description"][:200] if metadata["description"] else None,
                language=lang_code,
                attachments=attachments,
                extra_metadata=extra_metadata,
                content_hash=content_hash,
            )
            items.append(item)

        return items

    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a YouTube SourceItem into an Obsidian MarkdownNote."""
        attachment_names = [a.filename for a in item.attachments]
        return MarkdownNote(
            title=item.title,
            source="youtube",
            date=item.date or datetime.now().strftime("%Y-%m-%d"),
            body=item.content,
            tags=list(item.tags) if item.tags else ["ingested", "youtube"],
            source_url=item.source_url,
            author=item.author,
            aliases=list(item.aliases),
            status=item.status,
            summary=item.summary,
            language=item.language,
            word_count=item.word_count,
            attachments=attachment_names,
            extra_metadata=dict(item.extra_metadata),
            folder="Ingested/YouTube",
        )


# Register YouTubeSource in the global SourceRegistry
SourceRegistry.register("youtube", YouTubeSource)
