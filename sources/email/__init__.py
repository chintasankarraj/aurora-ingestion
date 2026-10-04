"""Email providers package for Aurora Ingestion Pipeline."""

from sources.email.base import (
    EmailAttachmentData,
    EmailMessageData,
    EmailProvider,
    EmailThreadData,
    clean_html_to_markdown,
    is_tracking_pixel_attachment,
    parse_email_date,
)
from sources.email.gmail import GmailProvider
from sources.email.outlook import OutlookProvider

__all__ = [
    "EmailAttachmentData",
    "EmailMessageData",
    "EmailProvider",
    "EmailThreadData",
    "GmailProvider",
    "OutlookProvider",
    "clean_html_to_markdown",
    "is_tracking_pixel_attachment",
    "parse_email_date",
]
