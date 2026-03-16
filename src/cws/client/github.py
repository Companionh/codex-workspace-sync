from __future__ import annotations

from dataclasses import dataclass

import httpx


@dataclass
class RepoMetadata:
    repo_url: str
    repo_name: str
    default_branch: str | None = None
    description: str | None = None


def parse_repo_url(repo_url: str) -> tuple[str, str]:
    cleaned = repo_url.strip().removesuffix(".git").rstrip("/")
    owner, name = cleaned.split("/")[-2:]
    return owner, name


def fetch_repo_metadata(repo_url: str, github_token: str | None = None) -> RepoMetadata:
    owner, name = parse_repo_url(repo_url)
    headers = {"Accept": "application/vnd.github+json"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    response = httpx.get(
        f"https://api.github.com/repos/{owner}/{name}",
        headers=headers,
        timeout=20.0,
    )
    if response.status_code >= 400:
        return RepoMetadata(repo_url=repo_url, repo_name=name)
    payload = response.json()
    return RepoMetadata(
        repo_url=repo_url,
        repo_name=payload["name"],
        default_branch=payload.get("default_branch"),
        description=payload.get("description"),
    )

