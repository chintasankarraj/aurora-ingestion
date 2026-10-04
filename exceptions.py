"""Custom exception hierarchy for the Aurora Ingestion Pipeline."""


class AuroraIngestionError(Exception):
    """Base exception for all aurora-ingestion errors."""
    pass


class VaultPathError(AuroraIngestionError):
    """Raised when the vault path is missing, invalid, or inaccessible."""
    pass


class ExcludedFolderError(AuroraIngestionError):
    """Raised when an operation attempts to write into an excluded folder (.obsidian, .trash, .git)."""
    pass


class SourceError(AuroraIngestionError):
    """Raised when an error occurs during source fetching or parsing."""
    pass


class TrackingError(AuroraIngestionError):
    """Raised when an error occurs in the deduplication tracker."""
    pass


class ConversionError(AuroraIngestionError):
    """Raised when an error occurs while converting or writing a Markdown note."""
    pass
