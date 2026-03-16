from __future__ import annotations

import json
from pathlib import Path

from cws.client.codex import build_managed_documents, build_raw_session_bundle, extract_turn_hashes


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
    session_path = session_dir / "rollout-abc.jsonl"
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
    (codex_root / "session_index.jsonl").write_text(
        json.dumps({"id": "thread-1", "thread_name": "demo"}),
        encoding="utf-8",
    )

    bundle = build_raw_session_bundle(codex_root, [workspace_root])
    hashes = extract_turn_hashes([session_path])

    assert bundle.thread_id == "thread-1"
    assert any(artifact.relative_path.endswith("rollout-abc.jsonl") for artifact in bundle.files)
    assert len(hashes) == 1
