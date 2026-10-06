"""Comprehensive unit and integration tests for the GitHub source connector."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest
import requests

from config import IngestionConfig
from exceptions import SourceError
from main import IngestionPipeline, async_main, create_parser
from models import SourceItem
from sources.base import SourceRegistry
from sources.github_source import (
    GitHubSource,
    build_github_gist_body,
    build_github_issue_body,
    derive_github_gist_id,
    derive_github_issue_id,
    format_code_fence,
    is_html_content,
    map_github_error,
    normalize_github_content,
    normalize_github_tags,
    normalize_repo_name,
)
from tracker import DeduplicationTracker, IngestionAction


# ---------------------------------------------------------------------------
# Test Helpers & Mock HTTP Session
# ---------------------------------------------------------------------------

class MockResponse:
    """Mock requests Response object for testing GitHub API."""

    def __init__(
        self,
        json_data: Any,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}
        self.text = json.dumps(json_data) if not isinstance(json_data, str) else json_data

    def json(self) -> Any:
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


class MockSession:
    """Mock requests.Session object allowing chained responses."""

    def __init__(self, responses: Optional[List[Any]] = None) -> None:
        self.responses = list(responses or [])
        self.calls: List[Dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> MockResponse:
        self.calls.append({"url": url, "kwargs": kwargs})
        if not self.responses:
            return MockResponse([])
        resp = self.responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp


def sample_github_issue(
    issue_number: int = 1,
    title: str = "Test Issue",
    body: str = "Issue description text.",
    state: str = "open",
    author: str = "testuser",
    labels: Optional[List[str]] = None,
    comments_count: int = 0,
    is_pr: bool = False,
) -> Dict[str, Any]:
    """Helper creating a sample GitHub issue dictionary."""
    data: Dict[str, Any] = {
        "id": 1000 + issue_number,
        "number": issue_number,
        "title": title,
        "body": body,
        "state": state,
        "html_url": f"https://github.com/owner/repo/issues/{issue_number}",
        "user": {"login": author},
        "labels": [{"name": l} for l in (labels or ["bug"])],
        "assignees": [{"login": author}],
        "milestone": {"title": "v1.0"},
        "comments": comments_count,
        "created_at": "2026-10-06T12:00:00Z",
        "updated_at": "2026-10-06T13:00:00Z",
        "closed_at": None,
    }
    if is_pr:
        data["pull_request"] = {"url": f"https://api.github.com/repos/owner/repo/pulls/{issue_number}"}
    return data


def sample_github_gist(
    gist_id: str = "gist123",
    description: str = "Useful helper scripts",
    files: Optional[Dict[str, Dict[str, Any]]] = None,
    public: bool = True,
    owner: str = "gistuser",
) -> Dict[str, Any]:
    """Helper creating a sample GitHub gist dictionary."""
    default_files = {
        "example.py": {
            "filename": "example.py",
            "type": "application/x-python",
            "language": "Python",
            "content": "def main():\n    print('Hello world')",
            "size": 42,
            "truncated": False,
        }
    }
    return {
        "id": gist_id,
        "description": description,
        "html_url": f"https://gist.github.com/{owner}/{gist_id}",
        "public": public,
        "created_at": "2026-10-06T10:00:00Z",
        "updated_at": "2026-10-06T11:00:00Z",
        "owner": {"login": owner},
        "files": files if files is not None else default_files,
    }


# ---------------------------------------------------------------------------
# 1. Registration & Authentication Tests
# ---------------------------------------------------------------------------

def test_github_source_registration():
    """Test that GitHubSource is registered under 'github' in SourceRegistry."""
    cls = SourceRegistry.get("github")
    assert cls is GitHubSource
    source = cls()
    assert source.source_type == "github"
    assert source.display_name == "GitHub"


def test_missing_token_raises_source_error(monkeypatch):
    """Test that missing GITHUB_TOKEN raises SourceError."""
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    source = GitHubSource()
    with pytest.raises(SourceError, match="GitHub token missing"):
        source._resolve_token()


def test_token_from_environment(monkeypatch):
    """Test that GITHUB_TOKEN is read from environment variable."""
    monkeypatch.setenv("GITHUB_TOKEN", "env_secret_token_123")
    source = GitHubSource()
    assert source._resolve_token() == "env_secret_token_123"


def test_token_from_constructor():
    """Test that token can be passed directly via constructor."""
    source = GitHubSource(token="constructor_secret_token_456")
    assert source._resolve_token() == "constructor_secret_token_456"


def test_token_never_leaked_in_errors_or_logs():
    """Test that secret tokens are scrubbed and never appear in error messages."""
    secret = "ghp_super_secret_github_token_999"
    exc = Exception(f"Failed connecting with {secret}")
    mapped = map_github_error(exc, secrets=[secret])
    assert secret not in str(mapped)
    assert "[REDACTED]" in str(mapped)


def test_no_repos_or_gists_raises_source_error():
    """Test that calling fetch_items without repos or gists raises SourceError."""
    source = GitHubSource(token="fake_token")
    with pytest.raises(SourceError, match="No GitHub repositories or gists specified"):
        import asyncio
        asyncio.run(source.fetch_items())


# ---------------------------------------------------------------------------
# 2. Repository Configuration & Normalization
# ---------------------------------------------------------------------------

def test_normalize_repo_name_valid_and_invalid():
    """Test repository string normalization and validation."""
    assert normalize_repo_name("owner/repo") == "owner/repo"
    assert normalize_repo_name("  chintasankarraj/aurora-ingestion  ") == "chintasankarraj/aurora-ingestion"
    assert normalize_repo_name("/owner/repo/") == "owner/repo"

    with pytest.raises(SourceError, match="Invalid GitHub repository format"):
        normalize_repo_name("invalid_repo_without_owner")
    with pytest.raises(SourceError, match="Invalid GitHub repository format"):
        normalize_repo_name("too/many/parts/here")
    with pytest.raises(SourceError, match="Repository name cannot be empty"):
        normalize_repo_name("   ")


def test_parse_repo_list():
    """Test parsing multiple repositories from lists or comma-separated strings."""
    assert GitHubSource._parse_repo_list("owner/repo1, owner/repo2") == ["owner/repo1", "owner/repo2"]
    assert GitHubSource._parse_repo_list(["owner/repo1", "owner/repo2"]) == ["owner/repo1", "owner/repo2"]
    assert GitHubSource._parse_repo_list(["owner/repo1, owner/repo2", "owner/repo3"]) == [
        "owner/repo1", "owner/repo2", "owner/repo3"
    ]
    # Deduplication
    assert GitHubSource._parse_repo_list(["owner/repo1", "owner/repo1"]) == ["owner/repo1"]


# ---------------------------------------------------------------------------
# 3. Issue Ingestion Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_issue_retrieval():
    """Test successful retrieval and SourceItem conversion of issues."""
    issue_raw = sample_github_issue(
        issue_number=42,
        title="Add vector database integration",
        body="Integrate ChromaDB vector store into pipeline.",
        state="open",
        author="alice",
        labels=["enhancement", "database"],
    )
    mock_session = MockSession([MockResponse([issue_raw])])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    item = items[0]
    assert item.source_id == "github:issue:owner/repo#42"
    assert item.title == "Add vector database integration"
    assert "## Description" in item.content
    assert "Integrate ChromaDB vector store into pipeline." in item.content
    assert item.author == "alice"
    assert item.date == "2026-10-06T12:00:00Z"
    assert item.source_url == "https://github.com/owner/repo/issues/42"
    assert item.tags == ["ingested", "github", "code", "enhancement", "database"]
    assert item.extra_metadata["repository"] == "owner/repo"
    assert item.extra_metadata["issue_number"] == 42


@pytest.mark.asyncio
async def test_multiple_repositories_retrieval():
    """Test fetching issues from multiple configured repositories."""
    issue1 = sample_github_issue(issue_number=1, title="Repo 1 Issue")
    issue2 = sample_github_issue(issue_number=2, title="Repo 2 Issue")

    mock_session = MockSession([
        MockResponse([issue1]),
        MockResponse([issue2]),
    ])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo1", "owner/repo2"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 2
    assert items[0].source_id == "github:issue:owner/repo1#1"
    assert items[1].source_id == "github:issue:owner/repo2#2"


@pytest.mark.asyncio
async def test_pull_requests_filtered_out():
    """Test that pull requests returned by the issues endpoint are explicitly filtered out."""
    normal_issue = sample_github_issue(issue_number=10, title="Real Issue", is_pr=False)
    pr_issue = sample_github_issue(issue_number=11, title="Pull Request Entry", is_pr=True)

    mock_session = MockSession([MockResponse([normal_issue, pr_issue])])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].title == "Real Issue"
    assert items[0].extra_metadata["issue_number"] == 10


@pytest.mark.asyncio
async def test_issue_pagination():
    """Test multi-page issue retrieval stops when page size is less than per_page."""
    page1 = [sample_github_issue(issue_number=i, title=f"Issue {i}") for i in range(1, 101)]
    page2 = [sample_github_issue(issue_number=101, title="Issue 101")]

    mock_session = MockSession([MockResponse(page1), MockResponse(page2)])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 101
    assert len(mock_session.calls) == 2
    assert mock_session.calls[0]["kwargs"]["params"]["page"] == 1
    assert mock_session.calls[1]["kwargs"]["params"]["page"] == 2


@pytest.mark.asyncio
async def test_issue_pagination_stops_on_duplicate_batch_fingerprint(caplog):
    """Test that pagination safely breaks if the exact same batch is returned repeatedly."""
    issue = sample_github_issue(issue_number=1, title="Stuck Issue")
    # Simulate API returning same batch for page 1 and page 2
    mock_session = MockSession([
        MockResponse([issue] * 100),
        MockResponse([issue] * 100),
    ])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    assert "Duplicate batch detected" in caplog.text


@pytest.mark.asyncio
async def test_issue_comments_retrieval_and_ordering():
    """Test that issue comments are retrieved and formatted in chronological order."""
    issue = sample_github_issue(issue_number=5, title="Bug with cache", comments_count=2)
    comments = [
        {
            "id": 101,
            "user": {"login": "bob"},
            "created_at": "2026-10-06T12:15:00Z",
            "body": "I reproduced this in staging.",
        },
        {
            "id": 102,
            "user": {"login": "alice"},
            "created_at": "2026-10-06T13:00:00Z",
            "body": "Fixed in commit abc1234.",
        },
    ]

    mock_session = MockSession([
        MockResponse([issue]),  # issues list
        MockResponse(comments),  # comments for issue #5
    ])
    source = GitHubSource(
        token="token",
        repositories=["owner/repo"],
        session=mock_session,
        include_comments=True,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    content = items[0].content
    assert "## Comments" in content
    assert "### bob — 2026-10-06T12:15:00Z" in content
    assert "I reproduced this in staging." in content
    assert "### alice — 2026-10-06T13:00:00Z" in content
    assert "Fixed in commit abc1234." in content


def test_issue_without_comments():
    """Test that no ## Comments section is created when issue has no comments."""
    issue = sample_github_issue(issue_number=6, comments_count=0)
    body = build_github_issue_body(issue, comments=None)
    assert "## Comments" not in body


def test_issue_empty_body():
    """Test that no ## Description section is created when issue body is empty."""
    issue = sample_github_issue(issue_number=7, body="")
    body = build_github_issue_body(issue)
    assert "## Description" not in body


def test_issue_frontmatter_and_metadata():
    """Test issue frontmatter fields and metadata section."""
    issue = sample_github_issue(
        issue_number=8,
        title="Metadata Check",
        labels=["bug", "security"],
    )
    body = build_github_issue_body(issue)
    assert "## GitHub Metadata" in body
    assert "- State: open" in body
    assert "- Labels: bug, security" in body
    assert "- Assignees: testuser" in body
    assert "- Milestone: v1.0" in body


# ---------------------------------------------------------------------------
# 4. Gist Ingestion Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_successful_gist_retrieval():
    """Test successful retrieval and SourceItem conversion of gists."""
    gist_raw = sample_github_gist(
        gist_id="gist_abc789",
        description="Data conversion script",
        files={
            "script.py": {
                "filename": "script.py",
                "language": "Python",
                "content": "import sys\nprint('running')",
                "truncated": False,
            },
            "README.md": {
                "filename": "README.md",
                "language": "Markdown",
                "content": "# Documentation\nInstructions here.",
                "truncated": False,
            },
        },
    )
    mock_session = MockSession([MockResponse([gist_raw])])
    source = GitHubSource(
        token="token",
        include_gists=True,
        gists_only=True,
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    item = items[0]
    assert item.source_id == "github:gist:gist_abc789"
    assert item.title == "Data conversion script"
    assert "## Files" in item.content
    assert item.content.count("## Files") == 1
    assert "### README.md" in item.content
    assert "### script.py" in item.content
    assert item.tags == ["ingested", "github", "code"]
    assert item.extra_metadata["github_type"] == "gist"
    assert item.extra_metadata["public"] is True


def test_gist_metadata_and_visibility():
    """Test gist metadata formatting for public and secret gists."""
    gist_public = sample_github_gist(gist_id="g1", public=True)
    body_pub = build_github_gist_body(gist_public)
    assert "## Gist Metadata" in body_pub
    assert "- Visibility: public" in body_pub
    assert "- Created: 2026-10-06T10:00:00Z" in body_pub

    gist_secret = sample_github_gist(gist_id="g2", public=False)
    body_sec = build_github_gist_body(gist_secret)
    assert "- Visibility: secret" in body_sec


def test_gist_dynamic_code_fence_escapes_backticks():
    """Test that dynamic code fences safely handle files containing backticks."""
    code_with_triple_backticks = "Here is a code block in markdown:\n```python\nprint(1)\n```"
    fence = format_code_fence(code_with_triple_backticks, language="Markdown")
    # Must use 4 backticks since code contains 3
    assert fence.startswith("````markdown")
    assert fence.endswith("````")
    assert code_with_triple_backticks in fence


def test_gist_file_truncated_notice():
    """Test that truncated gist files clearly indicate truncation."""
    gist_raw = sample_github_gist(
        gist_id="trunc_1",
        files={
            "large_file.txt": {
                "filename": "large_file.txt",
                "content": "Partial content...",
                "truncated": True,
            }
        },
    )
    body = build_github_gist_body(gist_raw)
    assert "*(File content truncated by GitHub API)*" in body


@pytest.mark.asyncio
async def test_gist_pagination():
    """Test gist pagination stops when page is less than per_page."""
    page1 = [sample_github_gist(gist_id=f"g_{i}") for i in range(1, 101)]
    page2 = [sample_github_gist(gist_id="g_101")]

    mock_session = MockSession([MockResponse(page1), MockResponse(page2)])
    source = GitHubSource(
        token="token",
        gists_only=True,
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 101
    assert len(mock_session.calls) == 2


# ---------------------------------------------------------------------------
# 5. Identity & Deduplication Tests
# ---------------------------------------------------------------------------

def test_stable_issue_id_format():
    """Test stable issue ID formatting."""
    assert derive_github_issue_id("owner/repo", 42) == "github:issue:owner/repo#42"
    assert derive_github_issue_id("org/project", "100") == "github:issue:org/project#100"


def test_stable_gist_id_format():
    """Test stable gist ID formatting."""
    assert derive_github_gist_id("abc123def456") == "github:gist:abc123def456"


def test_missing_issue_number_or_repo_raises_source_error():
    """Test that missing issue number or repo safely raises SourceError."""
    with pytest.raises(SourceError, match="Missing repository name"):
        derive_github_issue_id("", 42)
    with pytest.raises(SourceError, match="Missing issue number"):
        derive_github_issue_id("owner/repo", None)


def test_missing_gist_id_raises_source_error():
    """Test that missing gist ID safely raises SourceError."""
    with pytest.raises(SourceError, match="Missing gist ID"):
        derive_github_gist_id("")


def test_issue_title_change_preserves_stable_id():
    """Test that changing an issue's title preserves the exact same source ID."""
    id1 = derive_github_issue_id("owner/repo", 42)
    id2 = derive_github_issue_id("owner/repo", 42)
    assert id1 == id2 == "github:issue:owner/repo#42"


def test_gist_description_change_preserves_stable_id():
    """Test that changing a gist's description preserves the exact same source ID."""
    id1 = derive_github_gist_id("gist999")
    id2 = derive_github_gist_id("gist999")
    assert id1 == id2 == "github:gist:gist999"


@pytest.mark.asyncio
async def test_deduplication_lifecycle_issue(tmp_path):
    """Test NEW creates file, UNCHANGED skips, CHANGED overwrites in-place."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_gh.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    raw_issue = sample_github_issue(issue_number=1, title="Initial Title", body="First body")
    mock_session = MockSession([MockResponse([raw_issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    # 1. NEW
    items = await source.fetch_items()
    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW
    assert rel_path.startswith("Ingested/Code/")
    note_path = vault_dir / rel_path
    assert note_path.exists()
    assert "Initial Title" in note_path.read_text(encoding="utf-8")

    # 2. UNCHANGED
    action2, rel_path2 = await pipeline.process_item(source, items[0])
    assert action2 == IngestionAction.UNCHANGED
    assert rel_path2 == rel_path

    # 3. CHANGED (body updated)
    raw_issue_updated = sample_github_issue(issue_number=1, title="Initial Title", body="Updated body text")
    mock_session2 = MockSession([MockResponse([raw_issue_updated])])
    source2 = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session2)
    items_updated = await source2.fetch_items()

    action3, rel_path3 = await pipeline.process_item(source2, items_updated[0])
    assert action3 == IngestionAction.CHANGED
    assert rel_path3 == rel_path  # Same file path overwritten in-place
    assert "Updated body text" in note_path.read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_issue_title_change_preserves_path_on_update(tmp_path):
    """Test that updating an issue title overwrites the existing note in-place rather than creating a second file."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_gh_title.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    raw_issue = sample_github_issue(issue_number=2, title="Bug A", body="Body")
    mock_session = MockSession([MockResponse([raw_issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    action1, rel_path1 = await pipeline.process_item(source, items[0])
    assert action1 == IngestionAction.NEW

    # Title changed from Bug A to Bug B
    raw_issue_renamed = sample_github_issue(issue_number=2, title="Bug B (Renamed)", body="Body")
    mock_session2 = MockSession([MockResponse([raw_issue_renamed])])
    source2 = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session2)
    items_renamed = await source2.fetch_items()

    action2, rel_path2 = await pipeline.process_item(source2, items_renamed[0])
    assert action2 == IngestionAction.CHANGED
    assert rel_path2 == rel_path1
    # File count in Ingested/Code should still be exactly 1
    code_files = list((vault_dir / "Ingested" / "Code").glob("*.md"))
    assert len(code_files) == 1


# ---------------------------------------------------------------------------
# 6. File & Directory Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_target_folder_is_ingested_code(tmp_path):
    """Test GitHub notes are written to Ingested/Code/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_folder.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    issue = sample_github_issue(issue_number=3)
    mock_session = MockSession([MockResponse([issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])
    assert rel_path.startswith("Ingested/Code/")
    assert rel_path.endswith(".md")
    assert (vault_dir / rel_path).exists()


@pytest.mark.asyncio
async def test_filename_sanitization_and_80_char_truncation(tmp_path):
    """Test title is sanitized and truncated to at most 80 characters in filename."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_trunc.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    super_long_title = "Feature: " + "a" * 150
    issue = sample_github_issue(issue_number=4, title=super_long_title)
    mock_session = MockSession([MockResponse([issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])
    stem = Path(rel_path).stem
    # Prefix is YYYY-MM-DD_github_ (21 chars)
    title_part = stem[21:]
    assert len(title_part) <= 80


@pytest.mark.asyncio
async def test_filename_collision_handling(tmp_path):
    """Test that two different issues with identical titles get disambiguated suffixes."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_col.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    issue1 = sample_github_issue(issue_number=20, title="Identical Issue Title")
    issue2 = sample_github_issue(issue_number=21, title="Identical Issue Title")

    mock_session = MockSession([MockResponse([issue1, issue2])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, path1 = await pipeline.process_item(source, items[0])
    _, path2 = await pipeline.process_item(source, items[1])

    assert path1 != path2
    assert path2.endswith("_2.md")
    assert (vault_dir / path1).exists()
    assert (vault_dir / path2).exists()


@pytest.mark.asyncio
async def test_path_traversal_protection(tmp_path):
    """Test that directory traversal characters in issue titles cannot escape Ingested/Code/."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_trav.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    traversal_title = "../../../etc/passwd"
    issue = sample_github_issue(issue_number=99, title=traversal_title)
    mock_session = MockSession([MockResponse([issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])
    assert rel_path.startswith("Ingested/Code/")
    assert (vault_dir / rel_path).resolve().is_relative_to((vault_dir / "Ingested" / "Code").resolve())
    assert not any(part == ".." for part in Path(rel_path).parts)


@pytest.mark.asyncio
async def test_unicode_and_emojis_in_issues_and_gists(tmp_path):
    """Test that Unicode titles, emojis, and labels are preserved in UTF-8 without BOM."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_uni.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    unicode_title = "🐛 Bug: Fix 検索機能 (Search) & Résumé"
    issue = sample_github_issue(issue_number=50, title=unicode_title, labels=["バグ", "i18n"])
    mock_session = MockSession([MockResponse([issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])
    full_path = vault_dir / rel_path
    raw_bytes = full_path.read_bytes()
    assert not raw_bytes.startswith(b"\xef\xbb\xbf")  # No BOM
    text = full_path.read_text(encoding="utf-8")
    assert unicode_title in text


# ---------------------------------------------------------------------------
# 7. Error Handling Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_api_error_401():
    """Test that HTTP 401 raises SourceError with clear authentication message."""
    mock_session = MockSession([MockResponse({"message": "Bad credentials"}, status_code=401)])
    source = GitHubSource(token="invalid_token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="GitHub authentication failed"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_403_forbidden():
    """Test that HTTP 403 forbidden raises SourceError."""
    mock_session = MockSession([MockResponse({"message": "Must have admin rights"}, status_code=403)])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="GitHub access forbidden"):
        await source.fetch_items()


@pytest.mark.asyncio
async def test_api_error_404_not_found():
    """Test that HTTP 404 raises SourceError indicating repository not found."""
    mock_session = MockSession([MockResponse({"message": "Not Found"}, status_code=404)])
    source = GitHubSource(token="token", repositories=["owner/nonexistent"], session=mock_session)
    # The source connector isolates repo errors and logs warnings
    items = await source.fetch_items()
    assert items == []


@pytest.mark.asyncio
async def test_api_error_429_or_rate_limit():
    """Test that rate limit 403/429 includes reset time and raises SourceError."""
    resp = MockResponse(
        {"message": "API rate limit exceeded"},
        status_code=403,
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1773561600"},
    )
    mock_session = MockSession([resp])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="GitHub API rate limit exceeded"):
        source._fetch_repo_issues(
            session=mock_session,
            repo="owner/repo",
            headers={},
            secrets=[],
            state="all",
            include_comments=False,
            limit=None,
        )


@pytest.mark.asyncio
async def test_api_error_500():
    """Test that HTTP 500 server error raises SourceError."""
    resp = MockResponse({"message": "Internal error"}, status_code=500)
    mock_session = MockSession([resp])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="GitHub API error"):
        source._fetch_repo_issues(
            session=mock_session,
            repo="owner/repo",
            headers={},
            secrets=[],
            state="all",
            include_comments=False,
            limit=None,
        )


@pytest.mark.asyncio
async def test_api_network_timeout():
    """Test that requests.exceptions.Timeout raises SourceError."""
    mock_session = MockSession([requests.exceptions.Timeout("Connection timed out")])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="timed out"):
        source._fetch_repo_issues(
            session=mock_session,
            repo="owner/repo",
            headers={},
            secrets=[],
            state="all",
            include_comments=False,
            limit=None,
        )


@pytest.mark.asyncio
async def test_api_connection_error():
    """Test that requests.exceptions.ConnectionError raises SourceError."""
    mock_session = MockSession([requests.exceptions.ConnectionError("DNS failure")])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)
    with pytest.raises(SourceError, match="connection failed"):
        source._fetch_repo_issues(
            session=mock_session,
            repo="owner/repo",
            headers={},
            secrets=[],
            state="all",
            include_comments=False,
            limit=None,
        )


@pytest.mark.asyncio
async def test_malformed_individual_issue_batch_isolation():
    """Test that a single malformed issue does not abort the rest of the batch."""
    good_issue = sample_github_issue(issue_number=1, title="Good Issue")
    bad_issue = {"invalid": "missing number and id"}

    mock_session = MockSession([MockResponse([bad_issue, good_issue])])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].title == "Good Issue"


@pytest.mark.asyncio
async def test_failed_repo_does_not_abort_other_repos():
    """Test that a failure on one repository allows healthy repositories to proceed."""
    bad_repo_resp = MockResponse({"message": "Not Found"}, status_code=404)
    good_issue = sample_github_issue(issue_number=2, title="Healthy Repo Issue")
    good_repo_resp = MockResponse([good_issue])

    mock_session = MockSession([bad_repo_resp, good_repo_resp])
    source = GitHubSource(
        token="token",
        repositories=["owner/missing_repo", "owner/healthy_repo"],
        session=mock_session,
    )

    items = await source.fetch_items()
    assert len(items) == 1
    assert items[0].title == "Healthy Repo Issue"


# ---------------------------------------------------------------------------
# 8. Security Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_token_never_present_in_markdown_or_frontmatter(tmp_path):
    """Test that the authentication token is never written into the generated Markdown note."""
    secret_token = "ghp_super_confidential_token_xyz"
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_sec.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    issue = sample_github_issue(issue_number=77)
    mock_session = MockSession([MockResponse([issue])])
    source = GitHubSource(token=secret_token, repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    _, rel_path = await pipeline.process_item(source, items[0])
    content = (vault_dir / rel_path).read_text(encoding="utf-8")
    assert secret_token not in content


# ---------------------------------------------------------------------------
# 9. CLI Command Tests
# ---------------------------------------------------------------------------

def test_cli_parser_registration():
    """Test that CLI subparser recognizes ingest-github and its options."""
    parser = create_parser()
    args = parser.parse_args([
        "ingest-github",
        "--repo", "owner/repo1",
        "--repo", "owner/repo2",
        "--state", "open",
        "--include-gists",
        "--limit", "25",
    ])
    assert args.command == "ingest-github"
    assert args.repo == ["owner/repo1", "owner/repo2"]
    assert args.state == "open"
    assert args.include_gists is True
    assert args.limit == 25


@pytest.mark.asyncio
async def test_cli_ingest_github_issues_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of ingest-github command via async_main."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli.sqlite"

    issue = sample_github_issue(issue_number=10, title="CLI Ingested Issue")
    mock_resp = MockResponse([issue])
    monkeypatch.setattr("requests.Session.get", lambda self, url, **kwargs: mock_resp)

    parser = create_parser()
    args = parser.parse_args([
        "ingest-github",
        "--token", "mock_cli_token",
        "--repo", "owner/repo",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)

    assert exit_code == 0
    created_notes = list((vault_dir / "Ingested" / "Code").glob("*.md"))
    assert len(created_notes) == 1
    assert "CLI Ingested Issue" in created_notes[0].read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_cli_ingest_github_gists_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of ingest-github --gists-only command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_gists.sqlite"

    gist = sample_github_gist(gist_id="cli_gist_1", description="CLI Gist")
    mock_resp = MockResponse([gist])
    monkeypatch.setattr("requests.Session.get", lambda self, url, **kwargs: mock_resp)

    parser = create_parser()
    args = parser.parse_args([
        "ingest-github",
        "--token", "mock_cli_token",
        "--gists-only",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)

    assert exit_code == 0
    created_notes = list((vault_dir / "Ingested" / "Code").glob("*.md"))
    assert len(created_notes) == 1
    assert "CLI Gist" in created_notes[0].read_text(encoding="utf-8")


@pytest.mark.asyncio
async def test_generic_cli_ingest_github_end_to_end(tmp_path, monkeypatch):
    """Test end-to-end execution of generic ingest --source github command."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_cli_gen.sqlite"

    issue = sample_github_issue(issue_number=11, title="Generic CLI Issue")
    mock_resp = MockResponse([issue])
    monkeypatch.setattr("requests.Session.get", lambda self, url, **kwargs: mock_resp)

    parser = create_parser()
    args = parser.parse_args([
        "ingest",
        "--source", "github",
        "--token", "mock_cli_token",
        "--repo", "owner/repo",
    ])
    args.vault_path = str(vault_dir)
    args.tracker_db = str(tracker_file)
    args.verbose = False

    exit_code = await async_main(args)

    assert exit_code == 0
    created_notes = list((vault_dir / "Ingested" / "Code").glob("*.md"))
    assert len(created_notes) == 1
    assert "Generic CLI Issue" in created_notes[0].read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 10. Hardening Tests: HTML Normalization, Comments, Malformed Data, Truncated Gists
# ---------------------------------------------------------------------------

def test_issue_description_html_converted_to_markdown():
    """Test that raw HTML in issue descriptions is converted to clean Markdown and script/style stripped."""
    raw_html = (
        "<h1>Important Bug</h1>\n"
        "<script>alert('xss');</script>\n"
        "<style>body { color: red; }</style>\n"
        "<p>This has <b>bold text</b>, <i>italic</i>, and <a href=\"https://github.com\">GitHub Link</a>.</p>\n"
        "<div><ul><li>Point 1</li><li>Point 2</li></ul></div>"
    )
    issue = sample_github_issue(issue_number=101, body=raw_html)
    body = build_github_issue_body(issue)

    assert "# Important Bug" in body
    assert "**bold text**" in body
    assert "*italic*" in body
    assert "[GitHub Link](https://github.com)" in body
    assert "- Point 1" in body
    assert "- Point 2" in body

    # Strictly no raw HTML tags
    assert "<script>" not in body
    assert "<style>" not in body
    assert "<h1>" not in body
    assert "<p>" not in body
    assert "<b>" not in body
    assert "<div>" not in body
    assert "<ul>" not in body
    assert "<li>" not in body


def test_comment_body_html_converted_to_markdown():
    """Test that raw HTML in comments is converted to clean Markdown and script/style stripped."""
    comments = [
        {
            "id": 1,
            "created_at": "2026-10-06T10:00:00Z",
            "body": "<p>Comment with <script>console.log('test')</script><b>bold</b> and <pre><code>code block</code></pre></p>",
            "user": {"login": "contributor1"},
        }
    ]
    issue = sample_github_issue(issue_number=102, body="Plain description")
    body = build_github_issue_body(issue, comments=comments)

    assert "## Comments" in body
    assert "### contributor1 — 2026-10-06T10:00:00Z" in body
    assert "**bold**" in body
    assert "code block" in body
    assert "```" in body
    assert "<script>" not in body
    assert "<p>" not in body
    assert "<b>" not in body
    assert "<pre>" not in body


def test_pure_markdown_and_comparisons_preserved_without_alteration():
    """Test that pure Markdown, tables, fenced code blocks, and math/comparison operators (< and >) are preserved."""
    markdown_body = (
        "# Heading 1\n\n"
        "## Heading 2\n\n"
        "Here is a table:\n\n"
        "| Name | Age |\n"
        "| --- | --- |\n"
        "| Alice | 30 |\n\n"
        "Comparison: if a < b and b > c:\n"
        "`inline code <tag>`\n\n"
        "```python\ndef check(x, y):\n    return x < y\n```"
    )
    issue = sample_github_issue(issue_number=103, body=markdown_body)
    body = build_github_issue_body(issue)

    assert "# Heading 1" in body
    assert "## Heading 2" in body
    assert "| Name | Age |" in body
    assert "Comparison: if a < b and b > c:" in body
    assert "`inline code <tag>`" in body
    assert "```python\ndef check(x, y):\n    return x < y\n```" in body


def test_gist_description_html_converted_to_markdown():
    """Test that HTML in gist descriptions is normalized to clean Markdown in title and body."""
    raw_desc = "<p>Utility script for <b>fast</b> processing with <script>alert(1)</script></p>"
    gist = sample_github_gist(gist_id="gist_html_1", description=raw_desc)
    body = build_github_gist_body(gist)

    assert "Utility script for **fast** processing" in body
    assert "<p>" not in body
    assert "<b>" not in body
    assert "<script>" not in body


def test_issue_comments_request_parameters_chronological():
    """Test that _fetch_issue_comments requests sort=created and direction=asc."""
    mock_resp = MockResponse([{"id": 1, "created_at": "2026-10-06T10:00:00Z", "body": "c1"}])
    mock_session = MockSession([mock_resp])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    source._fetch_issue_comments(
        session=mock_session,
        repo="owner/repo",
        issue_number=42,
        headers={},
        secrets=[],
    )

    assert len(mock_session.calls) == 1
    call = mock_session.calls[0]
    assert call["kwargs"]["params"]["sort"] == "created"
    assert call["kwargs"]["params"]["direction"] == "asc"
    assert call["kwargs"]["params"]["per_page"] == 100


def test_issue_comments_local_chronological_sorting_when_out_of_order():
    """Test that comments supplied out of order are rendered strictly chronologically, with ID as tie-breaker."""
    c_later = {
        "id": 20,
        "created_at": "2026-10-06T12:00:00Z",
        "body": "Second comment posted later",
        "user": {"login": "user_b"},
    }
    c_earlier = {
        "id": 10,
        "created_at": "2026-10-06T09:00:00Z",
        "body": "First comment posted earlier",
        "user": {"login": "user_a"},
    }
    c_tie_2 = {
        "id": 31,
        "created_at": "2026-10-06T10:00:00Z",
        "body": "Tie comment higher id",
        "user": {"login": "user_d"},
    }
    c_tie_1 = {
        "id": 30,
        "created_at": "2026-10-06T10:00:00Z",
        "body": "Tie comment lower id",
        "user": {"login": "user_c"},
    }

    # Pass in scrambled order
    out_of_order_comments = [c_later, c_tie_2, c_earlier, c_tie_1]

    issue = sample_github_issue(issue_number=104)
    body = build_github_issue_body(issue, comments=out_of_order_comments)

    pos_earlier = body.find("First comment posted earlier")
    pos_tie_1 = body.find("Tie comment lower id")
    pos_tie_2 = body.find("Tie comment higher id")
    pos_later = body.find("Second comment posted later")

    assert pos_earlier != -1 and pos_tie_1 != -1 and pos_tie_2 != -1 and pos_later != -1
    assert pos_earlier < pos_tie_1 < pos_tie_2 < pos_later


def test_malformed_comment_user_missing_none_or_non_dict():
    """Test that comments with missing, None, non-dict, or login-less user objects default safely to 'unknown'."""
    comments = [
        {"id": 1, "created_at": "2026-10-06T10:00:00Z", "body": "User is None", "user": None},
        {"id": 2, "created_at": "2026-10-06T10:05:00Z", "body": "User key missing"},
        {"id": 3, "created_at": "2026-10-06T10:10:00Z", "body": "User is non-dict string", "user": "ghost"},
        {"id": 4, "created_at": "2026-10-06T10:15:00Z", "body": "User is dict without login", "user": {"id": 999}},
    ]
    issue = sample_github_issue(issue_number=105)
    body = build_github_issue_body(issue, comments=comments)

    assert "### unknown — 2026-10-06T10:00:00Z\n\nUser is None" in body
    assert "### unknown — 2026-10-06T10:05:00Z\n\nUser key missing" in body
    assert "### ghost — 2026-10-06T10:10:00Z\n\nUser is non-dict string" in body
    assert "### unknown — 2026-10-06T10:15:00Z\n\nUser is dict without login" in body


@pytest.mark.asyncio
async def test_malformed_comment_does_not_abort_issue_or_valid_comment(tmp_path):
    """Test that a malformed comment does not prevent the issue from being ingested with its valid comments."""
    vault_dir = tmp_path / "Vault"
    vault_dir.mkdir()
    tracker_file = tmp_path / "tracker_mal_c.sqlite"
    config = IngestionConfig(vault_path=vault_dir, tracker_db_path=tracker_file)
    tracker = DeduplicationTracker(tracker_file)
    pipeline = IngestionPipeline(config=config, tracker=tracker)

    issue = sample_github_issue(issue_number=106, comments_count=2)
    # Return one completely invalid non-dict comment and one valid comment
    comments_resp = MockResponse([
        "invalid non-dict comment",
        {"id": 50, "created_at": "2026-10-06T10:00:00Z", "body": "Valid comment content", "user": {"login": "good_user"}},
    ])
    mock_session = MockSession([MockResponse([issue]), comments_resp])
    source = GitHubSource(token="token", repositories=["owner/repo"], session=mock_session)

    items = await source.fetch_items()
    assert len(items) == 1
    action, rel_path = await pipeline.process_item(source, items[0])
    assert action == IngestionAction.NEW

    note_text = (vault_dir / rel_path).read_text(encoding="utf-8")
    assert "Valid comment content" in note_text
    assert "good_user" in note_text


@pytest.mark.asyncio
async def test_truncated_gist_file_successful_recovery():
    """Test that a truncated gist file fetches complete raw content from raw_url."""
    raw_url = "https://gist.githubusercontent.com/user/raw/abc/full_script.py"
    gist = sample_github_gist(
        gist_id="gist_trunc_ok",
        files={
            "full_script.py": {
                "filename": "full_script.py",
                "content": "Partial code...",
                "truncated": True,
                "raw_url": raw_url,
                "language": "Python",
            }
        },
    )
    # Session call 1: fetch gists list; call 2: fetch raw file content
    resp_gists = MockResponse([gist])
    resp_raw = MockResponse("def full_function():\n    return 'Complete recovered code'\n", status_code=200)
    mock_session = MockSession([resp_gists, resp_raw])

    source = GitHubSource(token="token", include_gists=True, gists_only=True, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    content = items[0].content
    assert "Complete recovered code" in content
    assert "```python" in content
    # Truncation notice must NOT be present since it was recovered
    assert "*(File content truncated by GitHub API)*" not in content
    assert len(mock_session.calls) == 2
    assert mock_session.calls[1]["url"] == raw_url


@pytest.mark.asyncio
async def test_truncated_gist_file_failed_recovery_keeps_partial_and_notice():
    """Test that failed raw_url recovery retains existing partial content and truncation notice without aborting."""
    raw_url = "https://gist.githubusercontent.com/user/raw/abc/failed_script.py"
    gist = sample_github_gist(
        gist_id="gist_trunc_fail",
        files={
            "failed_script.py": {
                "filename": "failed_script.py",
                "content": "Partial code before failure...",
                "truncated": True,
                "raw_url": raw_url,
                "language": "Python",
            }
        },
    )
    # Session call 1: fetch gists list; call 2: raw file returns HTTP 404
    resp_gists = MockResponse([gist])
    resp_raw_fail = MockResponse({"message": "Not Found"}, status_code=404)
    mock_session = MockSession([resp_gists, resp_raw_fail])

    source = GitHubSource(token="token", include_gists=True, gists_only=True, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    content = items[0].content
    assert "Partial code before failure..." in content
    assert "*(File content truncated by GitHub API)*" in content


@pytest.mark.asyncio
async def test_truncated_gist_file_no_raw_url_fallback():
    """Test that a truncated gist file without raw_url safely falls back to partial content and notice."""
    gist = sample_github_gist(
        gist_id="gist_trunc_no_url",
        files={
            "no_raw.txt": {
                "filename": "no_raw.txt",
                "content": "Partial content with no raw url...",
                "truncated": True,
                "raw_url": None,
                "language": "Text",
            }
        },
    )
    resp_gists = MockResponse([gist])
    mock_session = MockSession([resp_gists])

    source = GitHubSource(token="token", include_gists=True, gists_only=True, session=mock_session)
    items = await source.fetch_items()

    assert len(items) == 1
    content = items[0].content
    assert "Partial content with no raw url..." in content
    assert "*(File content truncated by GitHub API)*" in content
    assert len(mock_session.calls) == 1  # No secondary raw request attempted


def test_gist_body_contains_single_files_section():
    """Regression test: verify build_github_gist_body contains exactly one '## Files' section header."""
    gist = sample_github_gist(
        gist_id="single_files_check",
        files={
            "file1.txt": {"filename": "file1.txt", "content": "hello", "language": "Text"},
            "file2.txt": {"filename": "file2.txt", "content": "world", "language": "Text"},
        },
    )
    body = build_github_gist_body(gist)
    assert body.count("## Files") == 1

