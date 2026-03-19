from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from cws.config import ServerPaths
from cws.models import (
    AcquireLeaseRequest,
    CreateSuperprojectRequest,
    ManagedDocument,
    PushCheckpointRequest,
    RawCodexSharedBundle,
    RawFileArtifact,
    RawSessionBundle,
    RegisterDeviceRequest,
    SubprojectRecord,
    ThreadCheckpoint,
)
from cws.server.service import ServerService
from cws.utils import encode_b64, sha256_text, utc_now


def make_service(tmp_path: Path) -> ServerService:
    repo_root = Path(__file__).resolve().parents[1]
    paths = ServerPaths.default(app_root=repo_root, state_root=tmp_path / "state")
    service = ServerService(paths=paths)
    service.init_state("secondary-passphrase")
    return service


def test_register_device_and_authenticate(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    response = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
            metadata={"platform": "windows"},
        )
    )

    authenticated = service.authenticate_device(
        response.device.device_id,
        response.device_secret,
    )

    assert authenticated.device_name == "machine-a"
    assert authenticated.metadata["platform"] == "windows"


def test_global_lease_expires_after_two_minutes(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    response = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    lease_response = service.acquire_lease(
        AcquireLeaseRequest(device_id=response.device.device_id)
    )
    assert lease_response.granted is True
    assert lease_response.lease.heartbeat_timeout_seconds == 120

    stale_timestamp = (utc_now() - timedelta(seconds=121)).isoformat()
    with service.db.connect() as connection:
        connection.execute(
            "UPDATE leases SET last_heartbeat_at = ? WHERE resource_id = 'global'",
            (stale_timestamp,),
        )
        connection.commit()

    heartbeat = service.heartbeat(response.device.device_id)
    assert heartbeat.accepted is False
    assert heartbeat.lease.device_id is None


def test_create_superproject_scaffolds_expected_directories(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[
                SubprojectRecord(
                    repo_url="https://github.com/example/bot-a",
                    repo_name="bot-a",
                ),
                SubprojectRecord(
                    repo_url="https://github.com/example/bot-b",
                    repo_name="bot-b",
                ),
            ],
        )
    ).manifest

    root = tmp_path / "state" / "superprojects" / "telegram-suite"
    assert (root / "baseline" / "base_rules.md").exists()
    assert (root / "ecosystem" / "sibling_repos.md").exists()
    assert (root / "subprojects" / "registry.json").exists()
    assert len(manifest.managed_files) >= 4


def test_rename_superproject_updates_manifest_name(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    )

    response = service.rename_superproject("telegram-suite", "My Custom Suite")

    manifest = service.get_manifest("telegram-suite")
    assert response.manifest.name == "My Custom Suite"
    assert manifest.name == "My Custom Suite"


def test_push_checkpoint_rejects_missing_protected_file(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest

    broken_manifest = manifest.model_copy(update={"managed_files": []})
    documents = [
        ManagedDocument(
            record=record,
            content="replacement",
        )
        for record in manifest.managed_files
    ]
    snapshot_hash = sha256_text(json.dumps({"documents": len(documents)}))
    request = PushCheckpointRequest(
        checkpoint=ThreadCheckpoint(
            superproject_slug="telegram-suite",
            revision=0,
            created_at=utc_now(),
            source_device_id=device.device.device_id,
            canonical=True,
            base_revision=manifest.revision,
            turn_hashes=[],
            summary="test checkpoint",
            manifest=broken_manifest,
            managed_documents=documents,
            raw_bundle=None,
            snapshot_hash=snapshot_hash,
        )
    )

    with pytest.raises(ValueError):
        service.push_checkpoint(device.device.device_id, request)


def test_push_checkpoint_deduplicates_identical_canonical_payloads(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest
    documents = [ManagedDocument(record=record, content="replacement") for record in manifest.managed_files]

    checkpoint = ThreadCheckpoint(
        superproject_slug="telegram-suite",
        thread_id="thread-a",
        revision=0,
        created_at=utc_now(),
        source_device_id=device.device.device_id,
        canonical=True,
        base_revision=manifest.revision,
        turn_hashes=["turn-a"],
        summary="thread-a",
        manifest=manifest.model_copy(update={"managed_files": [doc.record for doc in documents]}),
        managed_documents=[],
        raw_bundle=RawSessionBundle(
            captured_at=utc_now(),
            thread_id="thread-a",
            session_ids=["thread-a"],
            files=[
                RawFileArtifact(
                    relative_path="sessions/2026/03/17/thread-a.jsonl",
                    sha256="session-a",
                    content_b64="dGhyZWFkLWE=",
                )
            ],
        ),
        snapshot_hash="snapshot-thread-a",
    )

    first = service.push_checkpoint(device.device.device_id, PushCheckpointRequest(checkpoint=checkpoint))
    second = service.push_checkpoint(device.device.device_id, PushCheckpointRequest(checkpoint=checkpoint))

    assert first.revision == 1
    assert second.revision == 1
    with service.db.connect() as connection:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM checkpoints WHERE superproject_slug = ? AND thread_id = ?",
            ("telegram-suite", "thread-a"),
        ).fetchone()
    assert row["count"] == 1


def test_push_checkpoint_prunes_old_history_and_raw_bundles(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    service.checkpoint_retention_per_thread = 2
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest

    for index in range(3):
        checkpoint = ThreadCheckpoint(
            checkpoint_id=f"checkpoint-{index}",
            superproject_slug="telegram-suite",
            thread_id="thread-a",
            revision=0,
            created_at=utc_now(),
            source_device_id=device.device.device_id,
            canonical=True,
            base_revision=manifest.revision + index,
            turn_hashes=[f"turn-{index}"],
            summary="thread-a",
            manifest=manifest,
            managed_documents=[],
            raw_bundle=RawSessionBundle(
                bundle_id=f"bundle-{index}",
                captured_at=utc_now(),
                thread_id="thread-a",
                session_ids=["thread-a"],
                files=[
                    RawFileArtifact(
                        relative_path=f"sessions/2026/03/17/thread-a-{index}.jsonl",
                        sha256=f"session-{index}",
                        content_b64=encode_b64(f"thread-a-{index}".encode("utf-8")),
                    )
                ],
            ),
            snapshot_hash=f"snapshot-thread-a-{index}",
        )
        service.push_checkpoint(device.device.device_id, PushCheckpointRequest(checkpoint=checkpoint))
        manifest = service.get_manifest("telegram-suite")

    with service.db.connect() as connection:
        rows = connection.execute(
            """
            SELECT revision
            FROM checkpoints
            WHERE superproject_slug = ? AND thread_id = ?
            ORDER BY revision
            """,
            ("telegram-suite", "thread-a"),
        ).fetchall()

    assert [row["revision"] for row in rows] == [2, 3]
    assert not (tmp_path / "state" / "superprojects" / "telegram-suite" / "threads" / "thread-a" / "checkpoints" / "1.json").exists()
    assert not (tmp_path / "state" / "superprojects" / "telegram-suite" / "raw_codex" / "bundle-0.json").exists()
    assert (tmp_path / "state" / "superprojects" / "telegram-suite" / "raw_codex" / "bundle-1.json").exists()
    assert (tmp_path / "state" / "superprojects" / "telegram-suite" / "raw_codex" / "bundle-2.json").exists()


def test_pull_state_returns_latest_checkpoint_per_thread(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest
    documents = [ManagedDocument(record=record, content="replacement") for record in manifest.managed_files]

    service.push_checkpoint(
        device.device.device_id,
        PushCheckpointRequest(
            checkpoint=ThreadCheckpoint(
                superproject_slug="telegram-suite",
                thread_id="thread-a",
                revision=0,
                created_at=utc_now(),
                source_device_id=device.device.device_id,
                canonical=True,
                base_revision=manifest.revision,
                turn_hashes=["turn-a"],
                summary="thread-a",
                manifest=manifest.model_copy(update={"managed_files": [doc.record for doc in documents]}),
                managed_documents=documents,
                raw_bundle=RawSessionBundle(
                    captured_at=utc_now(),
                    thread_id="thread-a",
                    session_ids=["thread-a"],
                    files=[
                        RawFileArtifact(
                            relative_path="sessions/2026/03/17/thread-a.jsonl",
                            sha256="session-a",
                            content_b64="dGhyZWFkLWE=",
                        )
                    ],
                ),
                snapshot_hash="snapshot-thread-a",
            )
        ),
    )
    service.push_checkpoint(
        device.device.device_id,
        PushCheckpointRequest(
            checkpoint=ThreadCheckpoint(
                superproject_slug="telegram-suite",
                thread_id=None,
                revision=0,
                created_at=utc_now(),
                source_device_id=device.device.device_id,
                canonical=True,
                base_revision=manifest.revision,
                turn_hashes=[],
                summary="default",
                manifest=manifest.model_copy(update={"managed_files": [doc.record for doc in documents]}),
                managed_documents=documents,
                shared_bundle=RawCodexSharedBundle(
                    captured_at=utc_now(),
                    files=[
                        RawFileArtifact(
                            relative_path="session_index.jsonl",
                            sha256="index",
                            content_b64="aW5kZXg=",
                        )
                    ],
                ),
                snapshot_hash="snapshot-default",
            )
        ),
    )

    state = service.pull_state("telegram-suite")
    returned_thread_ids = {checkpoint.thread_id for checkpoint in state.thread_checkpoints}

    assert state.latest_checkpoint is not None
    assert state.latest_checkpoint.thread_id is None
    assert state.shared_checkpoint is not None
    assert state.shared_checkpoint.thread_id is None
    assert returned_thread_ids == {None, "thread-a"}


def test_delete_superproject_removes_server_state(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    )

    result = service.delete_superproject(
        "telegram-suite",
        requesting_device_id=device.device.device_id,
    )

    assert result["deleted"] is True
    assert not (tmp_path / "state" / "superprojects" / "telegram-suite").exists()
    with pytest.raises(FileNotFoundError):
        service.get_manifest("telegram-suite")


def test_list_threads_uses_raw_bundle_thread_name(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest
    documents = [ManagedDocument(record=record, content="replacement") for record in manifest.managed_files]

    service.push_checkpoint(
        device.device.device_id,
        PushCheckpointRequest(
            checkpoint=ThreadCheckpoint(
                superproject_slug="telegram-suite",
                thread_id="thread-a",
                revision=0,
                created_at=utc_now(),
                source_device_id=device.device.device_id,
                canonical=True,
                base_revision=manifest.revision,
                turn_hashes=["turn-a"],
                summary="fallback summary",
                manifest=manifest.model_copy(update={"managed_files": [doc.record for doc in documents]}),
                managed_documents=documents,
                raw_bundle=RawSessionBundle(
                    captured_at=utc_now(),
                    thread_id="thread-a",
                    thread_name="Clone Companionh repos",
                    last_user_turn_preview="first line\nsecond line",
                    session_ids=["thread-a"],
                    files=[],
                ),
                snapshot_hash="snapshot-thread-a",
            )
        ),
    )

    threads = service.list_threads("telegram-suite")

    assert len(threads) == 1
    assert threads[0].thread_id == "thread-a"
    assert threads[0].thread_name == "Clone Companionh repos"
    assert threads[0].last_user_turn_preview == "first line\nsecond line"


def test_list_threads_backfills_name_and_preview_from_raw_bundle_files(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest
    documents = [ManagedDocument(record=record, content="replacement") for record in manifest.managed_files]

    session_index = json.dumps(
        {
            "id": "thread-a",
            "thread_name": "Clone Companionh repos",
            "updated_at": utc_now().isoformat(),
        }
    )
    session_file = "\n".join(
        [
            json.dumps(
                {
                    "type": "session_meta",
                    "payload": {"id": "thread-a", "cwd": "c:\\coding projects\\Telegram-bot-suite"},
                }
            ),
            json.dumps(
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message",
                        "message": "# Context from my IDE setup:\n\n## Open tabs:\n- README.md: codex-workplace-sync/README.md\n\n## My request for Codex:\nfirst line\nsecond line\nthird line",
                    },
                }
            ),
        ]
    )
    service.push_checkpoint(
        device.device.device_id,
        PushCheckpointRequest(
            checkpoint=ThreadCheckpoint(
                superproject_slug="telegram-suite",
                thread_id="thread-a",
                revision=0,
                created_at=utc_now(),
                source_device_id=device.device.device_id,
                canonical=True,
                base_revision=manifest.revision,
                turn_hashes=["turn-a"],
                summary="fallback summary",
                manifest=manifest.model_copy(update={"managed_files": [doc.record for doc in documents]}),
                managed_documents=documents,
                raw_bundle=RawSessionBundle(
                    captured_at=utc_now(),
                    thread_id="thread-a",
                    thread_name=None,
                    last_user_turn_preview=None,
                    session_ids=["thread-a"],
                    files=[
                        RawFileArtifact(
                            relative_path="session_index.jsonl",
                            sha256="index",
                            content_b64=encode_b64(session_index.encode("utf-8")),
                        ),
                        RawFileArtifact(
                            relative_path="sessions/2026/03/17/thread-a.jsonl",
                            sha256="session-a",
                            content_b64=encode_b64(session_file.encode("utf-8")),
                        ),
                    ],
                ),
                snapshot_hash="snapshot-thread-a",
            )
        ),
    )

    threads = service.list_threads("telegram-suite")

    assert len(threads) == 1
    assert threads[0].thread_name == "Clone Companionh repos"
    assert threads[0].last_user_turn_preview == "first line\nsecond line"


def test_rename_thread_preserves_manual_name_across_new_checkpoints(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest

    def push_thread_checkpoint(thread_name: str, snapshot_hash: str) -> None:
        service.push_checkpoint(
            device.device.device_id,
            PushCheckpointRequest(
                checkpoint=ThreadCheckpoint(
                    superproject_slug="telegram-suite",
                    thread_id="thread-a",
                    revision=0,
                    created_at=utc_now(),
                    source_device_id=device.device.device_id,
                    canonical=True,
                    base_revision=manifest.revision,
                    turn_hashes=["turn-a"],
                    summary="fallback summary",
                    manifest=manifest,
                    managed_documents=[],
                    raw_bundle=RawSessionBundle(
                        captured_at=utc_now(),
                        thread_id="thread-a",
                        thread_name=thread_name,
                        last_user_turn_preview="preview text",
                        session_ids=["thread-a"],
                        files=[],
                    ),
                    snapshot_hash=snapshot_hash,
                )
            ),
        )

    push_thread_checkpoint("Original auto name", "snapshot-thread-a-1")
    renamed = service.rename_thread("telegram-suite", "thread-a", "My Manual Name")
    push_thread_checkpoint("New auto name from bundle", "snapshot-thread-a-2")

    threads = service.list_threads("telegram-suite")

    assert renamed.thread.thread_name == "My Manual Name"
    assert renamed.thread.name_manually_set is True
    assert len(threads) == 1
    assert threads[0].thread_name == "My Manual Name"
    assert threads[0].name_manually_set is True
    assert threads[0].last_user_turn_preview == "preview text"
    assert threads[0].revision == 2
