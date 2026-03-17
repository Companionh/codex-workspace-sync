from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from cws.models import (
    ManagedDocument,
    ManagedFileClass,
    ManagedFileRecord,
    RawFileArtifact,
    RawSessionBundle,
)
from cws.utils import encode_b64, file_line_count, is_relative_to, relative_posix, sha256_bytes, sha256_file, utc_now

EXCLUDED_DIR_NAMES = {".git", ".venv", "node_modules", "dist", "build", "__pycache__"}


def classify_markdown(relative_path: str) -> ManagedFileClass:
    if relative_path.startswith("baseline/") or relative_path.startswith("ecosystem/"):
        return ManagedFileClass.PROTECTED
    if relative_path.startswith("generated/"):
        return ManagedFileClass.GENERATED
    return ManagedFileClass.NORMAL


def iter_managed_markdown_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    results: list[Path] = []
    for path in root.rglob("*.md"):
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        results.append(path)
    return sorted(results)


def build_managed_documents(
    managed_root: Path,
    path_to_id: dict[str, str],
) -> tuple[list[ManagedDocument], dict[str, str]]:
    documents: list[ManagedDocument] = []
    updated_ids = dict(path_to_id)
    for path in iter_managed_markdown_files(managed_root):
        relative_path = relative_posix(path, managed_root)
        file_id = updated_ids.get(relative_path)
        record = ManagedFileRecord(
            file_id=file_id or str(uuid4()),
            relative_path=relative_path,
            sha256=sha256_file(path),
            size_bytes=path.stat().st_size,
            line_count=file_line_count(path),
            classification=classify_markdown(relative_path),
        )
        updated_ids[relative_path] = record.file_id
        documents.append(
            ManagedDocument(
                record=record,
                content=path.read_text(encoding="utf-8"),
            )
        )
    return documents, updated_ids


def _matching_session_files(codex_root: Path, workspace_roots: list[Path]) -> tuple[list[Path], list[str]]:
    sessions_root = codex_root / "sessions"
    if not sessions_root.exists():
        return [], []

    session_entries: list[tuple[float, str, Path, str]] = []
    for path in sessions_root.rglob("*.jsonl"):
        try:
            first_line = path.read_text(encoding="utf-8").splitlines()[0]
            payload = json.loads(first_line)
        except Exception:
            continue
        session_id = payload.get("payload", {}).get("id")
        if not session_id:
            continue
        # Session sync is whole-machine by design. Workspace roots still matter for
        # managed Markdown ownership, but Codex history should follow the device.
        session_entries.append((path.stat().st_mtime, str(path).lower(), path, session_id))

    session_entries.sort(key=lambda item: (item[0], item[1]))
    matched_files = [path for _, _, path, _ in session_entries]
    matched_session_ids = [session_id for _, _, _, session_id in session_entries]
    return matched_files, matched_session_ids


def extract_turn_hashes(session_files: list[Path]) -> list[str]:
    turn_hashes: list[str] = []
    for path in session_files:
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("type") == "event_msg" and payload.get("payload", {}).get("type") == "user_message":
                message = payload["payload"].get("message", "")
                turn_hashes.append(sha256_bytes(message.encode("utf-8")))
    return turn_hashes


def build_raw_session_bundle(codex_root: Path, workspace_roots: list[Path]) -> RawSessionBundle:
    session_files, session_ids = _matching_session_files(codex_root, workspace_roots)
    files: list[RawFileArtifact] = []
    for path in session_files:
        files.append(
            RawFileArtifact(
                relative_path=relative_posix(path, codex_root),
                sha256=sha256_file(path),
                content_b64=encode_b64(path.read_bytes()),
            )
        )
    shared_files = [
        codex_root / "session_index.jsonl",
        codex_root / "config.toml",
        codex_root / "models_cache.json",
    ]
    shared_files.extend(codex_root.glob("state_*.sqlite*"))
    shared_files.extend(codex_root.glob("logs_*.sqlite*"))
    for path in shared_files:
        if not path.exists():
            continue
        files.append(
            RawFileArtifact(
                relative_path=relative_posix(path, codex_root),
                sha256=sha256_file(path),
                content_b64=encode_b64(path.read_bytes()),
            )
        )
    thread_id = session_ids[-1] if session_ids else None
    return RawSessionBundle(
        captured_at=utc_now(),
        thread_id=thread_id,
        session_ids=session_ids,
        files=files,
    )
