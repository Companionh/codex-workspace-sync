from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable
from uuid import uuid4

from cws.models import (
    ManagedDocument,
    ManagedFileClass,
    ManagedFileRecord,
    RawFileArtifact,
    RawSessionBundle,
    ThreadSummary,
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


def _load_thread_index(codex_root: Path) -> dict[str, dict[str, object]]:
    index_path = codex_root / "session_index.jsonl"
    if not index_path.exists():
        return {}
    by_id: dict[str, dict[str, object]] = {}
    for line in index_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        thread_id = payload.get("id")
        if not thread_id:
            continue
        updated_at = payload.get("updated_at")
        parsed_updated_at = None
        if isinstance(updated_at, str):
            try:
                parsed_updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            except ValueError:
                parsed_updated_at = None
        by_id[thread_id] = {
            "thread_name": payload.get("thread_name"),
            "updated_at": parsed_updated_at,
        }
    return by_id


def _session_meta(path: Path) -> dict[str, object] | None:
    try:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
        payload = json.loads(first_line)
    except Exception:
        return None
    meta = payload.get("payload", {})
    session_id = meta.get("id")
    if not session_id:
        return None
    return {
        "thread_id": session_id,
        "cwd": meta.get("cwd"),
    }


def _fallback_thread_name(session_files: list[Path]) -> str | None:
    for path in sorted(session_files):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("type") != "event_msg":
                continue
            message_payload = payload.get("payload", {})
            if message_payload.get("type") != "user_message":
                continue
            message = str(message_payload.get("message", "")).strip()
            if not message:
                continue
            message = re.sub(r"(?is)^<environment_context>.*?</environment_context>", "", message).strip()
            message = re.sub(r"\s+", " ", message).strip()
            if not message:
                continue
            return message[:80]
    return None


def _collect_thread_entries(codex_root: Path) -> list[dict[str, object]]:
    sessions_root = codex_root / "sessions"
    if not sessions_root.exists():
        return []

    index_by_id = _load_thread_index(codex_root)
    grouped: dict[str, dict[str, object]] = {}
    for path in sessions_root.rglob("*.jsonl"):
        meta = _session_meta(path)
        if meta is None:
            continue
        thread_id = str(meta["thread_id"])
        thread = grouped.setdefault(
            thread_id,
            {
                "thread_id": thread_id,
                "session_files": [],
                "workspace_roots": set(),
                "updated_at": None,
                "thread_name": None,
            },
        )
        thread["session_files"].append(path)
        cwd = meta.get("cwd")
        if cwd:
            thread["workspace_roots"].add(str(cwd))
        stat_updated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=utc_now().tzinfo)
        current_updated_at = thread["updated_at"]
        if current_updated_at is None or stat_updated_at > current_updated_at:
            thread["updated_at"] = stat_updated_at
        index_entry = index_by_id.get(thread_id, {})
        if index_entry.get("thread_name"):
            thread["thread_name"] = index_entry["thread_name"]
        if index_entry.get("updated_at") is not None:
            index_updated_at = index_entry["updated_at"]
            current_updated_at = thread["updated_at"]
            if current_updated_at is None or index_updated_at > current_updated_at:
                thread["updated_at"] = index_updated_at

    entries = []
    for thread in grouped.values():
        session_files = sorted(thread["session_files"])
        thread_name = thread["thread_name"] or _fallback_thread_name(session_files) or thread["thread_id"]
        entries.append(
            {
                "thread_id": thread["thread_id"],
                "thread_name": thread_name,
                "updated_at": thread["updated_at"] or utc_now(),
                "session_files": session_files,
                "workspace_roots": sorted(thread["workspace_roots"]),
            }
        )
    entries.sort(key=lambda item: (item["updated_at"], item["thread_name"], item["thread_id"]))
    return entries


def list_local_threads(codex_root: Path) -> list[ThreadSummary]:
    return [
        ThreadSummary(
            thread_id=entry["thread_id"],
            thread_name=entry["thread_name"],
            updated_at=entry["updated_at"],
            source="local",
        )
        for entry in _collect_thread_entries(codex_root)
    ]


def _matching_session_files(
    codex_root: Path,
    workspace_roots: list[Path],
    *,
    thread_id: str | None = None,
) -> tuple[list[Path], list[str], str | None, datetime | None]:
    entries = _collect_thread_entries(codex_root)
    if thread_id:
        entries = [entry for entry in entries if entry["thread_id"] == thread_id]
    elif workspace_roots:
        workspace_root_values = {str(path).lower() for path in workspace_roots}
        matched_entries = [
            entry
            for entry in entries
            if any(str(root).lower() in workspace_root_values for root in entry["workspace_roots"])
        ]
        if matched_entries:
            entries = matched_entries

    matched_files: list[Path] = []
    matched_session_ids: list[str] = []
    selected_name: str | None = None
    selected_updated_at: datetime | None = None
    for entry in entries:
        matched_files.extend(entry["session_files"])
        matched_session_ids.append(entry["thread_id"])
        if (thread_id and entry["thread_id"] == thread_id) or (thread_id is None and len(entries) == 1):
            selected_name = entry["thread_name"]
            selected_updated_at = entry["updated_at"]

    matched_files.sort(key=lambda path: (path.stat().st_mtime, str(path).lower()))
    return matched_files, matched_session_ids, selected_name, selected_updated_at


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


def build_raw_session_bundle(
    codex_root: Path,
    workspace_roots: list[Path],
    *,
    thread_id: str | None = None,
) -> RawSessionBundle:
    session_files, session_ids, thread_name, thread_updated_at = _matching_session_files(
        codex_root,
        workspace_roots,
        thread_id=thread_id,
    )
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
    bundle_thread_id = thread_id or (session_ids[-1] if session_ids else None)
    return RawSessionBundle(
        captured_at=utc_now(),
        thread_id=bundle_thread_id,
        thread_name=thread_name,
        thread_updated_at=thread_updated_at,
        session_ids=session_ids,
        files=files,
    )
