"""Tests for the CLI parser and execution commands."""

import pytest

from main import async_main, create_parser
from sources.base import BaseSource, SourceRegistry


def test_cli_parser_defaults():
    parser = create_parser()

    # Test ingest arguments
    args = parser.parse_args(["ingest", "--source", "web", "--url", "https://example.com"])
    assert args.command == "ingest"
    assert args.source == "web"
    assert args.url == "https://example.com"

    # Test list-sources
    args_list = parser.parse_args(["list-sources"])
    assert args_list.command == "list-sources"

    # Test status
    args_status = parser.parse_args(["status", "--source", "email"])
    assert args_status.command == "status"
    assert args_status.source == "email"


@pytest.mark.asyncio
async def test_cli_list_sources(capsys):
    parser = create_parser()
    args = parser.parse_args(["list-sources"])
    ret = await async_main(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "Registered source connectors:" in captured.out


@pytest.mark.asyncio
async def test_cli_init_vault(tmp_path, capsys):
    parser = create_parser()
    vault_dir = tmp_path / "cli_vault"
    vault_dir.mkdir()

    args = parser.parse_args(["--vault-path", str(vault_dir), "init-vault"])
    ret = await async_main(args)
    assert ret == 0
    captured = capsys.readouterr()
    assert "Initialized vault folder structure" in captured.out
    assert (vault_dir / "Ingested" / "Email").exists()
    assert (vault_dir / "Attachments" / "Ingested").exists()
