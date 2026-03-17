from __future__ import annotations

import json
from pathlib import Path

from cws.client.codex import build_managed_documents, build_raw_session_bundle, extract_turn_hashes, list_local_threads
from cws.utils import atomic_write_bytes


def test_build_managed_documents_classifies_paths(tmp_path: Path) -> None:
    (tmp_path / "baseline").mkdir()
    (tmp_path / "subprojects" / "bot-a").mkdir(parents=True)
    (tmp_path / "baseline" / "rules.md").write_text("baseline", encoding="utf-8")
    (tmp_path / "subprojects" / "bot-a" / "rules.md").write_text("subproject", encoding="utf-8")

    documents, ids = build_managed_documents(tmp_path, {})

    assert len(documents) == 2
    assert ids["baseline/rules.md"]
    assert ids["subprojects/bot-a/rules.md"]
    assert documents[0].record.classification.value in {"protected", "normal"}


def test_raw_bundle_collects_matching_sessions_and_turn_hashes(tmp_path: Path) -> None:
    codex_root = tmp_path / ".codex"
    session_dir = codex_root / "sessions" / "2026" / "03" / "16"
    session_dir.mkdir(parents=True)
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir()
    unrelated_root = tmp_path / "other-workspace"
    unrelated_root.mkdir()
    session_path = session_dir / "rollout-abc.jsonl"
    session_path_two = session_dir / "rollout-def.jsonl"
    lines = [
        json.dumps(
            {
                "type": "session_meta",
                "payload": {
                    "id": "thread-1",
                    "cwd": str(workspace_root),
                },
            }
        ),
        json.dumps(
            {
                "type": "event_msg",
                "payload": {
                    "type": "user_message",
                    "message": "hello world",
                },
            }
        ),
    ]
    session_path.write_text("\n".join(lines), encoding="utf-8")
    session_path_two.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "session_meta",
                        "payload": {
                            "id": "thread-2",
                            "cwd": str(unrelated_root),
                        },
                    }
                ),
                json.dumps(
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "user_message",
                            "message": "hello again",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    session_path_two.touch()
    (codex_root / "session_index.jsonl").write_text(
        json.dumps({"id": "thread-1", "thread_name": "demo"}),
        encoding="utf-8",
    )

    bundle = build_raw_session_bundle(codex_root, [workspace_root])
    hashes = extract_turn_hashes([session_path, session_path_two])

    assert bundle.thread_id == "thread-1"
    assert bundle.thread_name == "demo"
    assert bundle.session_ids == ["thread-1"]
    assert any(artifact.relative_path.endswith("rollout-abc.jsonl") for artifact in bundle.files)
    assert not any(artifact.relative_path.endswith("rollout-def.jsonl") for artifact in bundle.files)
    assert len(hashes) == 2


def test_list_local_threads_prefers_codex_thread_name(tmp_path: Path) -> None:
    codex_root = tmp_path / ".codex"
    session_dir = codex_root / "sessions" / "2026" / "03" / "16"
    session_dir.mkdir(parents=True)
    session_path = session_dir / "rollout-abc.jsonl"
    session_path.write_text(
        "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"id": "thread-1", "cwd": str(tmp_path)}}),
                json.dumps(
                    {
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "fallback title"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    (codex_root / "session_index.jsonl").write_text(
        json.dumps({"id": "thread-1", "thread_name": "Clone Companionh repos"}),
        encoding="utf-8",
    )

    threads = list_local_threads(codex_root)

    assert len(threads) == 1
    assert threads[0].thread_id == "thread-1"
    assert threads[0].thread_name == "Clone Companionh repos"


def test_atomic_write_bytes_retries_transient_permission_error(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "client-state.json"
    attempts = {"count": 0}
    real_replace = __import__("os").replace

    def flaky_replace(src: str, dst: str) -> None:
        if attempts["count"] == 0:
            attempts["count"] += 1
            raise PermissionError(5, "Access is denied")
        real_replace(src, dst)

    monkeypatch.setattr("cws.utils.os.replace", flaky_replace)

    atomic_write_bytes(target, b"hello")

    assert attempts["count"] == 1
    assert target.read_bytes() == b"hello"
