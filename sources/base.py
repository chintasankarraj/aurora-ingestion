"""Abstract base class and registry for external data sources."""

from __future__ import annotations

import asyncio
import functools
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from exceptions import SourceError
from models import MarkdownNote, SourceItem

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


def async_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator for asynchronous functions implementing exponential backoff retry.
    
    Useful for future API connectors facing network hiccups or rate limits.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = initial_delay
            last_exception: Optional[Exception] = None

            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} attempts: {e}"
                        )
                        raise
                    logger.warning(
                        f"Attempt {attempt}/{max_retries} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

            if last_exception:
                raise last_exception

        return wrapper  # type: ignore

    return decorator


class BaseSource(ABC):
    """Abstract base class that every Aurora external source connector must implement."""

    @property
    @abstractmethod
    def source_type(self) -> str:
        """Identifier for the source type (e.g. 'email', 'web', 'pdf', 'youtube')."""
        pass

    @property
    def display_name(self) -> str:
        """Human-readable display name for the source connector."""
        return self.source_type.capitalize()

    @abstractmethod
    async def fetch_items(self, **kwargs: Any) -> List[SourceItem]:
        """Fetch new or updated items from the external source."""
        pass

    @abstractmethod
    async def convert_to_markdown(self, item: SourceItem) -> MarkdownNote:
        """Convert a fetched SourceItem into frontmatter metadata and Markdown body."""
        pass

    def default_item_to_note(self, item: SourceItem) -> MarkdownNote:
        """Convenience helper to construct a MarkdownNote from a SourceItem."""
        attachment_names = [a.filename for a in item.attachments]
        return MarkdownNote(
            title=item.title,
            source=item.source_type,
            date=item.date or "",
            body=item.content,
            tags=list(item.tags) if item.tags else ["ingested", item.source_type],
            source_url=item.source_url,
            author=item.author,
            aliases=list(item.aliases),
            status=item.status,
            summary=item.summary,
            language=item.language,
            word_count=item.word_count,
            attachments=attachment_names,
            extra_metadata=dict(item.extra_metadata),
        )


class SourceRegistry:
    """Registry to manage and discover available source connectors."""

    _registry: Dict[str, Type[BaseSource]] = {}

    @classmethod
    def register(cls, source_type: str, source_cls: Type[BaseSource]) -> None:
        """Register a source class under a unique source_type key."""
        cls._registry[source_type.lower().strip()] = source_cls
        logger.debug(f"Registered source connector: {source_type}")

    @classmethod
    def get(cls, source_type: str) -> Optional[Type[BaseSource]]:
        """Retrieve registered source class by name."""
        return cls._registry.get(source_type.lower().strip())

    @classmethod
    def list_sources(cls) -> Dict[str, str]:
        """Return all registered source types and their class names."""
        return {k: v.__name__ for k, v in cls._registry.items()}

    @classmethod
    def create(cls, source_type: str, *args: Any, **kwargs: Any) -> BaseSource:
        """Instantiate a registered source connector."""
        src_cls = cls.get(source_type)
        if not src_cls:
            available = ", ".join(cls._registry.keys()) or "None"
            raise SourceError(
                f"Unknown source connector '{source_type}'. Registered sources: [{available}]"
            )
        return src_cls(*args, **kwargs)
