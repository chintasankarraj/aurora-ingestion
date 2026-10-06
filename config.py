"""Configuration handling for the Aurora Ingestion Pipeline.

Supports:
- Reading AURORA_VAULT_PATH from environment or .env
- Expanding ${AURORA_VAULT_PATH} syntax
- Vault folder topology definitions
- Excluded folder rules (.obsidian, .trash, .git)
- Extensible source credentials configuration
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Set

from dotenv import load_dotenv

from exceptions import ExcludedFolderError, VaultPathError

# Ensure environment variables from .env are loaded
load_dotenv()

# Folders inside the vault that must NEVER be written to
EXCLUDED_FOLDERS: Set[str] = {".obsidian", ".trash", ".git"}

# Folder mapping for supported source types
DEFAULT_SOURCE_FOLDERS: Dict[str, str] = {
    "email": "Ingested/Email",
    "web": "Ingested/Web",
    "pdf": "Ingested/PDF",
    "youtube": "Ingested/YouTube",
    "rss": "Ingested/RSS",
    "social": "Ingested/Social",
    "twitter": "Ingested/Social",
    "reddit": "Ingested/Social",
    "chat": "Ingested/Chat",
    "slack": "Ingested/Chat",
    "discord": "Ingested/Chat",
    "telegram": "Ingested/Chat",
    "voice": "Ingested/Voice",
    "voice-memo": "Ingested/Voice",
    "screenshot": "Ingested/Screenshots",
    "screenshots": "Ingested/Screenshots",
    "code": "Ingested/Code",
    "github": "Ingested/Code",
    "notes": "Ingested/Notes",
    "notion": "Ingested/Notes",
    "google-keep": "Ingested/Notes",
    "google-drive": "Ingested/Web",
    "google_drive": "Ingested/Web",
    "gdrive": "Ingested/Web",
    "readwise": "Ingested/Web",
    "pocket": "Ingested/Web",
    "manual": "Inbox",
}

DEFAULT_ATTACHMENT_FOLDER: str = "Attachments/Ingested"


def expand_env_vars(raw_value: str) -> str:
    """Expand ${VAR} or $VAR environment variables in a string.
    
    If the variable is not set, leaves it or returns empty if unmatched.
    """
    if not raw_value:
        return ""

    def replace_var(match: re.Match[str]) -> str:
        var_name = match.group(1) or match.group(2)
        val = os.getenv(var_name)
        if val is not None:
            return val
        # If the variable is not found in env, keep the original placeholder
        return match.group(0)

    # Match ${VAR_NAME} or $VAR_NAME
    pattern = re.compile(r"\$\{([A-Za-z0-9_]+)\}|\$([A-Za-z0-9_]+)")
    return pattern.sub(replace_var, raw_value)


@dataclass
class SourceCredentials:
    """Placeholder for source-specific credentials and configuration."""
    gmail_client_id: Optional[str] = None
    gmail_client_secret: Optional[str] = None
    notion_api_key: Optional[str] = None
    github_token: Optional[str] = None
    slack_bot_token: Optional[str] = None
    custom: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionConfig:
    """Master configuration for the Aurora Ingestion service."""
    vault_path: Optional[Path] = None
    tracker_db_path: Path = Path("aurora_ingestion_tracker.sqlite")
    log_level: str = "INFO"
    source_folders: Dict[str, str] = field(default_factory=lambda: dict(DEFAULT_SOURCE_FOLDERS))
    attachment_folder: str = DEFAULT_ATTACHMENT_FOLDER
    excluded_folders: Set[str] = field(default_factory=lambda: set(EXCLUDED_FOLDERS))
    credentials: SourceCredentials = field(default_factory=SourceCredentials)

    def get_folder_for_source(self, source_type: str) -> str:
        """Resolve the relative vault folder for a given source identifier."""
        normalized = (source_type or "").strip().lower()
        return self.source_folders.get(normalized, "Inbox")

    def is_path_excluded(self, rel_or_abs_path: str | Path) -> bool:
        """Check if any path component matches the excluded folders list.
        
        Prevents writes to .obsidian, .trash, .git, or any hidden dot-directory.
        """
        parts = Path(rel_or_abs_path).parts
        for part in parts:
            if part in self.excluded_folders or (part.startswith(".") and part not in {".", ".."}):
                return True
        return False

    def validate(self) -> None:
        """Validate that the configured vault path exists and is accessible."""
        if not self.vault_path:
            raise VaultPathError(
                "Aurora vault path is not set. Please set the AURORA_VAULT_PATH environment "
                "variable or specify --vault-path."
            )
        if not self.vault_path.exists():
            raise VaultPathError(
                f"Configured Aurora vault path does not exist: {self.vault_path}"
            )
        if not self.vault_path.is_dir():
            raise VaultPathError(
                f"Configured Aurora vault path is not a directory: {self.vault_path}"
            )


def resolve_vault_path(explicit_path: Optional[str | Path] = None) -> Path:
    """Resolves and expands the Aurora vault path from explicit parameter or env.
    
    Supports ${AURORA_VAULT_PATH}.
    Never returns a hardcoded path.
    """
    raw_path: Optional[str] = None

    if explicit_path:
        raw_path = str(explicit_path)
    else:
        # Check environment variable
        raw_path = os.getenv("AURORA_VAULT_PATH")

    if not raw_path:
        raise VaultPathError(
            "Aurora vault path is not set. Please set the AURORA_VAULT_PATH environment "
            "variable or specify --vault-path."
        )

    expanded = expand_env_vars(raw_path.strip())
    resolved = Path(expanded).resolve()
    return resolved


def load_config(
    vault_path_override: Optional[str | Path] = None,
    tracker_db_path_override: Optional[str | Path] = None,
    log_level: Optional[str] = None,
    require_vault: bool = False,
) -> IngestionConfig:
    """Load configuration from environment with optional runtime overrides."""
    vault_path: Optional[Path] = None
    if require_vault:
        vault_path = resolve_vault_path(vault_path_override)
    else:
        try:
            vault_path = resolve_vault_path(vault_path_override)
        except VaultPathError:
            vault_path = None

    raw_db_path = (
        str(tracker_db_path_override)
        if tracker_db_path_override
        else os.getenv("AURORA_TRACKER_DB_PATH", "aurora_ingestion_tracker.sqlite")
    )
    expanded_db_path = expand_env_vars(raw_db_path.strip())
    tracker_db_path = Path(expanded_db_path).resolve()

    resolved_log_level = log_level or os.getenv("LOG_LEVEL", "INFO").upper()

    credentials = SourceCredentials(
        gmail_client_id=os.getenv("GMAIL_CLIENT_ID"),
        gmail_client_secret=os.getenv("GMAIL_CLIENT_SECRET"),
        notion_api_key=os.getenv("NOTION_API_KEY"),
        github_token=os.getenv("GITHUB_TOKEN"),
        slack_bot_token=os.getenv("SLACK_BOT_TOKEN"),
    )

    return IngestionConfig(
        vault_path=vault_path,
        tracker_db_path=tracker_db_path,
        log_level=resolved_log_level,
        credentials=credentials,
    )
