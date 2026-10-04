"""Tests for configuration handling and vault path resolution."""

import os
from pathlib import Path

import pytest

from config import (
    DEFAULT_SOURCE_FOLDERS,
    EXCLUDED_FOLDERS,
    IngestionConfig,
    expand_env_vars,
    load_config,
    resolve_vault_path,
)
from exceptions import VaultPathError


def test_expand_env_vars(monkeypatch):
    monkeypatch.setenv("TEST_VAR", "my_custom_value")
    assert expand_env_vars("${TEST_VAR}/folder") == "my_custom_value/folder"
    assert expand_env_vars("$TEST_VAR/folder") == "my_custom_value/folder"
    assert expand_env_vars("plain_string") == "plain_string"


def test_resolve_vault_path_explicit(tmp_path):
    resolved = resolve_vault_path(tmp_path)
    assert resolved == tmp_path.resolve()


def test_resolve_vault_path_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AURORA_VAULT_PATH", str(tmp_path))
    resolved = resolve_vault_path()
    assert resolved == tmp_path.resolve()


def test_resolve_vault_path_missing(monkeypatch):
    monkeypatch.delenv("AURORA_VAULT_PATH", raising=False)
    with pytest.raises(VaultPathError, match="vault path is not set"):
        resolve_vault_path()


def test_validate_vault_path(tmp_path):
    config = IngestionConfig(vault_path=tmp_path)
    # Directory exists -> should not raise
    config.validate()

    # Non-existent directory
    missing_dir = tmp_path / "does_not_exist"
    invalid_config = IngestionConfig(vault_path=missing_dir)
    with pytest.raises(VaultPathError, match="does not exist"):
        invalid_config.validate()


def test_is_path_excluded(tmp_path):
    config = IngestionConfig(vault_path=tmp_path)
    assert config.is_path_excluded(".obsidian")
    assert config.is_path_excluded(".obsidian/plugins/test.js")
    assert config.is_path_excluded(".trash/deleted_note.md")
    assert config.is_path_excluded(".git/config")
    assert config.is_path_excluded(".hidden_folder/note.md")
    assert not config.is_path_excluded("Ingested/Email/note.md")
    assert not config.is_path_excluded("Attachments/Ingested/img.png")


def test_source_folder_mapping(tmp_path):
    config = IngestionConfig(vault_path=tmp_path)
    assert config.get_folder_for_source("email") == "Ingested/Email"
    assert config.get_folder_for_source("web") == "Ingested/Web"
    assert config.get_folder_for_source("pdf") == "Ingested/PDF"
    assert config.get_folder_for_source("youtube") == "Ingested/YouTube"
    assert config.get_folder_for_source("rss") == "Ingested/RSS"
    assert config.get_folder_for_source("twitter") == "Ingested/Social"
    assert config.get_folder_for_source("slack") == "Ingested/Chat"
    assert config.get_folder_for_source("voice-memo") == "Ingested/Voice"
    assert config.get_folder_for_source("screenshot") == "Ingested/Screenshots"
    assert config.get_folder_for_source("github") == "Ingested/Code"
    assert config.get_folder_for_source("notion") == "Ingested/Notes"
    assert config.get_folder_for_source("unknown_xyz") == "Inbox"
