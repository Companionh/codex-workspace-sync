from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from cws.client.state import ClientStateStore
from cws.client.sync import ClientService, SyncWorker
from cws.config import ClientPaths
from cws.models import (
    AlignmentAction,
    ClientConfig,
    ClientSuperprojectState,
    ManagedDocument,
    ManagedFileClass,
    ManagedFileRecord,
    OutboundQueueItem,
    PullStateResponse,
    PushCheckpointResponse,
    RawCodexSharedBundle,
    RawFileArtifact,
    RawSessionBundle,
    SuperprojectManifest,
    ThreadSummary,
    ThreadCheckpoint,
)
from cws.utils import encode_b64, utc_now


class _FakeChannel:
    def recv_exit_status(self) -> int:
        return 0


class _FakeStream:
    def __init__(self, payload: str) -> None:
        self._payload = payload.encode("utf-8")
        self.channel = _FakeChannel()

    def read(self) -> bytes:
        return self._payload


class _FakeSSHClient:
    last_command: str | None = None
    last_connect_kwargs: dict | None = None

    def set_missing_host_key_policy(self, _policy) -> None:
        return None

    def connect(self, **kwargs) -> None:
        _FakeSSHClient.last_connect_kwargs = kwargs
        return None

    def exec_command(self, command: str):
        _FakeSSHClient.last_command = command
        response = json.dumps(
            {
                "device": {
                    "device_id": "device-123",
                    "device_name": "machine-a",
                },
                "device_secret": "secret-123",
            }
        )
        return None, _FakeStream(response), _FakeStream("")

    def close(self) -> None:
        return None


def test_register_device_over_ssh_prefers_server_venv_python(monkeypatch) -> None:
    monkeypatch.setattr("cws.client.sync.paramiko.SSHClient", _FakeSSHClient)
    monkeypatch.setattr(ClientService, "resolve_ssh_config", staticmethod(lambda host: {}))

    response = ClientService()._register_device_over_ssh(
        ssh_host="37.27.184.8",
        ssh_user="root",
        ssh_port=22,
        device_name="machine-a",
        secondary_passphrase="bootstrap-secret",
        metadata={"platform": "windows"},
        ssh_password=None,
        ssh_key_passphrase="ssh-key-secret",
    )

    assert response["device"]["device_id"] == "device-123"
    assert _FakeSSHClient.last_connect_kwargs is not None
    assert _FakeSSHClient.last_connect_kwargs["passphrase"] == "ssh-key-secret"
    assert _FakeSSHClient.last_connect_kwargs["key_filename"] is None
    assert _FakeSSHClient.last_command is not None
    assert "$APP_ROOT/.venv/bin/python" in _FakeSSHClient.last_command
    assert '--state-root "$STATE_ROOT"' in _FakeSSHClient.last_command
    assert "register-device" in _FakeSSHClient.last_command


@pytest.mark.parametrize(
    ("raw_host", "raw_user", "raw_port", "expected"),
    [
        ("37.27.184.8", "root", 22, ("37.27.184.8", "root", 22)),
        ("root@37.27.184.8", "root", 22, ("37.27.184.8", "root", 22)),
        ("ssh root@37.27.184.8", "root", 22, ("37.27.184.8", "root", 22)),
        ("ssh://root@37.27.184.8:22", "root", 2222, ("37.27.184.8", "root", 22)),
        ("http://37.27.184.8:8787", "root", 22, ("37.27.184.8", "root", 22)),
        ("37.27.184.8:22", "root", 8787, ("37.27.184.8", "root", 22)),
    ],
)
def test_normalize_ssh_target_accepts_common_input_formats(
    raw_host: str,
    raw_user: str,
    raw_port: int,
    expected: tuple[str, str, int],
) -> None:
    assert ClientService.normalize_ssh_target(raw_host, raw_user, raw_port) == expected


def test_register_device_over_ssh_uses_identity_file_from_matching_hostname(monkeypatch, tmp_path) -> None:
    ssh_dir = tmp_path / ".ssh"
    ssh_dir.mkdir()
    (ssh_dir / "config").write_text(
        "\n".join(
            [
                "Host hetzner",
                "    HostName 37.27.184.8",
                "    User root",
                "    Port 22",
                "    IdentityFile ~/.ssh/id_ed25519_hetzner",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("cws.client.sync.paramiko.SSHClient", _FakeSSHClient)
    monkeypatch.setattr("cws.client.sync.Path.home", lambda: tmp_path)

    ClientService()._register_device_over_ssh(
        ssh_host="37.27.184.8",
        ssh_user="root",
        ssh_port=22,
        device_name="machine-a",
        secondary_passphrase="bootstrap-secret",
        metadata={"platform": "windows"},
        ssh_password=None,
        ssh_key_passphrase="ssh-key-secret",
    )

    assert _FakeSSHClient.last_connect_kwargs is not None
    assert _FakeSSHClient.last_connect_kwargs["key_filename"].endswith("id_ed25519_hetzner")


def test_write_shared_skills_accepts_model_instances(tmp_path) -> None:
    service = ClientService(codex_root=tmp_path / ".codex")
    artifact = RawFileArtifact(
        relative_path="workspace-sync-operator/SKILL.md",
        sha256="unused",
        content_b64=encode_b64(b"hello"),
    )

    service._write_shared_skills([artifact])

    saved = tmp_path / ".codex" / "skills" / "codex-workspace-sync-shared" / "workspace-sync-operator" / "SKILL.md"
    assert saved.read_text(encoding="utf-8") == "hello"


class _FakeApiClient:
    def __init__(self, state: PullStateResponse) -> None:
        self._state = state

    def pull_state(self, _slug: str) -> PullStateResponse:
        return self._state


def test_update_from_server_adopts_server_managed_file_ids(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    config = ClientConfig(
        superprojects={
            "telegram-bots-suite": ClientSuperprojectState(
                slug="telegram-bots-suite",
                name="telegram-bots-suite",
                managed_root=str(managed_root),
                managed_file_ids={"baseline/base_rules.md": "local-file-id"},
                last_alignment_action=AlignmentAction.NONE,
            )
        }
    )
    state_store.save_config(config)

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    server_state = PullStateResponse(
        manifest=SuperprojectManifest(
            slug="telegram-bots-suite",
            name="telegram-bots-suite",
            created_at=utc_now(),
            updated_at=utc_now(),
            managed_files=[server_document.record],
        ),
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=None,
        pending_resolutions=[],
    )
    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    diff = service.update_from_server("telegram-bots-suite", assume_yes=True)
    updated_config = state_store.load_config()
    updated_state = updated_config.superprojects["telegram-bots-suite"]

    assert diff.new_on_server == []
    assert diff.new_local == []
    assert diff.changed == ["baseline/base_rules.md"]
    assert updated_state.managed_file_ids["baseline/base_rules.md"] == "server-file-id"
    assert baseline_path.read_text(encoding="utf-8") == "# server\n"


def test_update_from_server_applies_latest_checkpoint_per_thread(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    managed_file_ids={"baseline/base_rules.md": "server-file-id"},
                    last_alignment_action=AlignmentAction.NONE,
                )
            }
        )
    )

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    manifest = SuperprojectManifest(
        slug="telegram-bots-suite",
        name="telegram-bots-suite",
        created_at=utc_now(),
        updated_at=utc_now(),
        revision=8,
        managed_files=[server_document.record],
    )
    explicit_thread_checkpoint = ThreadCheckpoint(
        checkpoint_id="checkpoint-thread-a",
        superproject_slug="telegram-bots-suite",
        thread_id="thread-a",
        revision=4,
        created_at=utc_now(),
        source_device_id="device-a",
        canonical=True,
        base_revision=3,
        turn_hashes=["turn-a"],
        summary="thread-a",
        manifest=manifest,
        managed_documents=[server_document],
        raw_bundle=RawSessionBundle(
            bundle_id="bundle-thread-a",
            captured_at=utc_now(),
            thread_id="thread-a",
            session_ids=["thread-a", "thread-b"],
            files=[
                RawFileArtifact(
                    relative_path="sessions/2026/03/17/thread-a.jsonl",
                    sha256="session-a",
                    content_b64=encode_b64(b"thread-a-session"),
                ),
            ],
        ),
        snapshot_hash="snapshot-thread-a",
    )
    default_checkpoint = ThreadCheckpoint(
        checkpoint_id="checkpoint-default",
        superproject_slug="telegram-bots-suite",
        thread_id=None,
        revision=8,
        created_at=utc_now(),
        source_device_id="device-b",
        canonical=True,
        base_revision=7,
        turn_hashes=[],
        summary="default",
        manifest=manifest,
        managed_documents=[server_document],
        shared_bundle=RawCodexSharedBundle(
            bundle_id="bundle-default",
            captured_at=utc_now(),
            files=[
                RawFileArtifact(
                    relative_path="session_index.jsonl",
                    sha256="index-new",
                    content_b64=encode_b64(b"new-index"),
                )
            ],
        ),
        snapshot_hash="snapshot-default",
    )
    server_state = PullStateResponse(
        manifest=manifest,
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=default_checkpoint,
        shared_checkpoint=default_checkpoint,
        thread_checkpoints=[explicit_thread_checkpoint, default_checkpoint],
        pending_resolutions=[],
    )
    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    service.update_from_server("telegram-bots-suite", assume_yes=True)

    codex_root = tmp_path / ".codex"
    updated_state = state_store.load_config().superprojects["telegram-bots-suite"]

    assert (codex_root / "sessions" / "2026" / "03" / "17" / "thread-a.jsonl").read_text(encoding="utf-8") == (
        "thread-a-session"
    )
    assert (codex_root / "session_index.jsonl").read_text(encoding="utf-8") == "new-index"
    assert updated_state.pending_thread_refreshes["thread-a"] == 4
    assert updated_state.pending_thread_refreshes["thread-b"] == 4
    assert updated_state.last_shared_bundle_revision == 8


def test_build_checkpoint_snapshot_hash_ignores_volatile_shared_runtime_artifacts(monkeypatch, tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            device_id="device-a",
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    workspace_roots=[str(tmp_path / "workspace")],
                    tracked_thread_ids=["thread-a"],
                    managed_file_ids={"baseline/base_rules.md": "server-file-id"},
                )
            },
        )
    )
    manifest = SuperprojectManifest(
        slug="telegram-bots-suite",
        name="telegram-bots-suite",
        created_at=utc_now(),
        updated_at=utc_now(),
        revision=2,
        managed_files=[
            ManagedFileRecord(
                file_id="server-file-id",
                relative_path="baseline/base_rules.md",
                sha256="unused",
                size_bytes=len("# local\n"),
                line_count=1,
                classification=ManagedFileClass.PROTECTED,
            )
        ],
    )

    class _ManifestOnlyApiClient:
        def pull_state(self, _slug: str) -> PullStateResponse:
            return PullStateResponse(
                manifest=manifest,
                latest_checkpoint=None,
                thread_checkpoints=[],
                pending_resolutions=[],
                managed_documents=[],
                shared_skills=[],
            )

    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")
    service.api_client = lambda: _ManifestOnlyApiClient()  # type: ignore[method-assign]

    shared_bundle_a = RawCodexSharedBundle(
        captured_at=utc_now(),
        files=[
            RawFileArtifact(
                relative_path="session_index.jsonl",
                sha256="index-sha",
                content_b64=encode_b64(b"shared-index"),
            ),
            RawFileArtifact(
                relative_path="state_5.sqlite-wal",
                sha256="wal-old",
                content_b64=encode_b64(b"old-wal"),
            ),
        ],
    )
    shared_bundle_b = RawCodexSharedBundle(
        captured_at=utc_now(),
        files=[
            RawFileArtifact(
                relative_path="session_index.jsonl",
                sha256="index-sha",
                content_b64=encode_b64(b"shared-index"),
            ),
            RawFileArtifact(
                relative_path="state_5.sqlite-wal",
                sha256="wal-new",
                content_b64=encode_b64(b"new-wal"),
            ),
        ],
    )
    bundles = iter([shared_bundle_a, shared_bundle_b])
    monkeypatch.setattr("cws.client.sync.build_shared_codex_bundle", lambda *_args, **_kwargs: next(bundles))

    checkpoint_a = service.build_checkpoint(
        "telegram-bots-suite",
        canonical=True,
        show_progress=False,
    )
    checkpoint_b = service.build_checkpoint(
        "telegram-bots-suite",
        canonical=True,
        show_progress=False,
    )

    assert checkpoint_a.snapshot_hash == checkpoint_b.snapshot_hash


def test_sync_worker_promotes_repeated_stable_checkpoint_to_canonical_push() -> None:
    class _FakeHeartbeat:
        def __init__(self, accepted: bool) -> None:
            self.accepted = accepted

    class _FakeApi:
        def __init__(self) -> None:
            self.heartbeat_calls = 0
            self.pushes: list[tuple[str, object]] = []

        def heartbeat(self):
            self.heartbeat_calls += 1
            return _FakeHeartbeat(self.heartbeat_calls < 10)

        def push_checkpoint(self, slug, request):
            self.pushes.append((slug, request))
            return PushCheckpointResponse(accepted=True, revision=7)

    class _FakeService:
        heartbeat_interval_seconds = 0

        def __init__(self) -> None:
            self.api = _FakeApi()
            self.prepared = object()
            self.checkpoints = iter(
                [
                    ThreadCheckpoint(
                        checkpoint_id="checkpoint-1",
                        superproject_slug="telegram-bots-suite",
                        thread_id="thread-a",
                        revision=0,
                        created_at=utc_now(),
                        source_device_id="device-a",
                        canonical=True,
                        base_revision=0,
                        turn_hashes=["turn-a"],
                        summary="checkpoint",
                        manifest=SuperprojectManifest(
                            slug="telegram-bots-suite",
                            name="telegram-bots-suite",
                            created_at=utc_now(),
                            updated_at=utc_now(),
                            managed_files=[],
                        ),
                        managed_documents=[],
                        raw_bundle=None,
                        snapshot_hash="stable-hash",
                    ),
                    ThreadCheckpoint(
                        checkpoint_id="checkpoint-2",
                        superproject_slug="telegram-bots-suite",
                        thread_id="thread-a",
                        revision=0,
                        created_at=utc_now(),
                        source_device_id="device-a",
                        canonical=True,
                        base_revision=0,
                        turn_hashes=["turn-a"],
                        summary="checkpoint",
                        manifest=SuperprojectManifest(
                            slug="telegram-bots-suite",
                            name="telegram-bots-suite",
                            created_at=utc_now(),
                            updated_at=utc_now(),
                            managed_files=[],
                        ),
                        managed_documents=[],
                        raw_bundle=None,
                        snapshot_hash="stable-hash",
                    ),
                ]
            )
            self.progress_messages: list[str] = []
            self.sync_inactive_marked = False

        def api_client(self):
            return self.api

        def flush_outbound_queue(self, _api, *, heartbeat=None):
            return True

        def prepare_live_checkpoint_inputs(self, _slug, *, show_progress):
            assert show_progress is False
            return self.prepared

        def build_live_checkpoints(self, _slug, *, canonical, show_progress, prepared=None):
            assert canonical is True
            assert show_progress is False
            assert prepared is self.prepared
            return [next(self.checkpoints)]

        def report_progress(self, message: str) -> None:
            self.progress_messages.append(message)

        def enqueue_checkpoint(self, _checkpoint) -> None:
            raise AssertionError("Queueing was not expected in the stable checkpoint case.")

        def mark_sync_inactive(self) -> None:
            self.sync_inactive_marked = True

        def _checkpoint_session_ids(self, checkpoint: ThreadCheckpoint) -> list[str]:
            return ClientService._checkpoint_session_ids(checkpoint)

        def _format_thread_labels(self, thread_ids: list[str], checkpoint: ThreadCheckpoint | None = None) -> str:
            return ClientService._format_thread_labels(thread_ids, checkpoint)

    service = _FakeService()
    worker = SyncWorker(service, "telegram-bots-suite")
    worker.run()

    assert len(service.api.pushes) == 1
    slug, request = service.api.pushes[0]
    assert slug == "telegram-bots-suite"
    assert request.checkpoint.canonical is True
    assert request.checkpoint.snapshot_hash == "stable-hash"
    assert service.sync_inactive_marked is True
    assert service.progress_messages == [
        "Detected a finished Codex turn for 'telegram-bots-suite' in thread(s): thread-a.",
        "Pushing the latest checkpoint for 'telegram-bots-suite' to the server...",
        "Server updated for 'telegram-bots-suite' at revision 7 for thread(s): thread-a.",
        "Live sync stopped because this device no longer owns the active lease.",
    ]


def test_sync_worker_retries_after_transient_heartbeat_http_error() -> None:
    class _FakeHeartbeat:
        def __init__(self, accepted: bool) -> None:
            self.accepted = accepted

    class _FakeApi:
        def __init__(self) -> None:
            self.heartbeat_calls = 0

        def heartbeat(self):
            self.heartbeat_calls += 1
            if self.heartbeat_calls == 1:
                raise httpx.ReadTimeout("temporary timeout")
            return _FakeHeartbeat(False)

    class _FakeService:
        heartbeat_interval_seconds = 0

        def __init__(self) -> None:
            self.api = _FakeApi()
            self.progress_messages: list[str] = []
            self.sync_inactive_marked = False

        def api_client(self):
            return self.api

        def flush_outbound_queue(self, _api, *, heartbeat=None):
            return True

        def report_progress(self, message: str) -> None:
            self.progress_messages.append(message)

        def mark_sync_inactive(self) -> None:
            self.sync_inactive_marked = True

    service = _FakeService()
    worker = SyncWorker(service, "telegram-bots-suite")

    worker.run()

    assert service.sync_inactive_marked is True
    assert service.progress_messages == [
        "Heartbeat request failed for 'telegram-bots-suite': temporary timeout. Retrying...",
        "Live sync stopped because this device no longer owns the active lease.",
    ]


def test_flush_outbound_queue_heartbeats_between_retry_pushes(tmp_path) -> None:
    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    checkpoint = ThreadCheckpoint(
        superproject_slug="telegram-bots-suite",
        thread_id="thread-a",
        revision=0,
        created_at=utc_now(),
        source_device_id="device-a",
        canonical=True,
        base_revision=0,
        turn_hashes=[],
        summary="checkpoint",
        manifest=SuperprojectManifest(
            slug="telegram-bots-suite",
            name="telegram-bots-suite",
            created_at=utc_now(),
            updated_at=utc_now(),
            managed_files=[],
        ),
        managed_documents=[],
        raw_bundle=None,
        snapshot_hash="snapshot-a",
    )
    queue = [
        OutboundQueueItem(
            superproject_slug="telegram-bots-suite",
            created_at=utc_now(),
            checkpoint=checkpoint,
        ),
        OutboundQueueItem(
            superproject_slug="telegram-bots-suite",
            created_at=utc_now(),
            checkpoint=checkpoint.model_copy(update={"checkpoint_id": "checkpoint-b", "snapshot_hash": "snapshot-b"}),
        ),
    ]
    state_store.save_queue(queue)

    class _QueueApi:
        def __init__(self) -> None:
            self.pushes = 0

        def push_checkpoint(self, _slug, _request):
            self.pushes += 1
            return PushCheckpointResponse(accepted=True, revision=7)

    heartbeat_calls: list[int] = []

    def heartbeat(_api) -> bool:
        heartbeat_calls.append(1)
        return True

    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")

    success = service.flush_outbound_queue(_QueueApi(), heartbeat=heartbeat)

    assert success is True
    assert heartbeat_calls == [1, 1, 1, 1]
    assert state_store.load_queue() == []


def test_update_from_server_reports_progress_steps(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    managed_file_ids={"baseline/base_rules.md": "local-file-id"},
                    last_alignment_action=AlignmentAction.NONE,
                )
            }
        )
    )

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    server_state = PullStateResponse(
        manifest=SuperprojectManifest(
            slug="telegram-bots-suite",
            name="telegram-bots-suite",
            created_at=utc_now(),
            updated_at=utc_now(),
            managed_files=[server_document.record],
        ),
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=None,
        pending_resolutions=[],
    )
    progress_messages: list[str] = []
    service = ClientService(
        state_store=state_store,
        codex_root=tmp_path / ".codex",
        progress_callback=progress_messages.append,
    )
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    service.update_from_server("telegram-bots-suite", assume_yes=True)

    assert progress_messages == [
        "Connecting to the server for 'telegram-bots-suite'...",
        "Comparing local Markdown for 'telegram-bots-suite' with the server copy...",
        "Applying server updates for 'telegram-bots-suite'...",
        "Syncing shared skills for 'telegram-bots-suite'...",
        "Update from server finished for 'telegram-bots-suite'.",
    ]


def test_update_from_server_reports_updated_thread_ids(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    managed_file_ids={"baseline/base_rules.md": "server-file-id"},
                )
            }
        )
    )

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    manifest = SuperprojectManifest(
        slug="telegram-bots-suite",
        name="telegram-bots-suite",
        created_at=utc_now(),
        updated_at=utc_now(),
        revision=6,
        managed_files=[server_document.record],
    )
    thread_checkpoint = ThreadCheckpoint(
        checkpoint_id="checkpoint-thread-a",
        superproject_slug="telegram-bots-suite",
        thread_id="thread-a",
        revision=6,
        created_at=utc_now(),
        source_device_id="device-a",
        canonical=True,
        base_revision=5,
        turn_hashes=["turn-a"],
        summary="thread-a",
        manifest=manifest,
        managed_documents=[server_document],
        raw_bundle=RawSessionBundle(
            bundle_id="bundle-thread-a",
            captured_at=utc_now(),
            thread_id="thread-a",
            session_ids=["thread-a", "thread-b", "thread-a"],
            files=[
                RawFileArtifact(
                    relative_path="sessions/2026/03/17/thread-a.jsonl",
                    sha256="session-a",
                    content_b64=encode_b64(b"thread-a-session"),
                )
            ],
        ),
        snapshot_hash="snapshot-thread-a",
    )
    server_state = PullStateResponse(
        manifest=manifest,
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=thread_checkpoint,
        thread_checkpoints=[thread_checkpoint],
        pending_resolutions=[],
    )
    progress_messages: list[str] = []
    service = ClientService(
        state_store=state_store,
        codex_root=tmp_path / ".codex",
        progress_callback=progress_messages.append,
    )
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    service.update_from_server("telegram-bots-suite", assume_yes=True)

    assert progress_messages == [
        "Connecting to the server for 'telegram-bots-suite'...",
        "Comparing local Markdown for 'telegram-bots-suite' with the server copy...",
        "Applying server updates for 'telegram-bots-suite'...",
        "Syncing shared skills for 'telegram-bots-suite'...",
        "Applying the latest Codex session bundle for 'telegram-bots-suite'...",
        "Updated Codex thread(s) for 'telegram-bots-suite': thread-a, thread-b.",
        "Update from server finished for 'telegram-bots-suite'.",
    ]


def test_update_from_server_skips_already_applied_thread_revisions(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    managed_file_ids={"baseline/base_rules.md": "server-file-id"},
                    pending_thread_refreshes={"thread-a": 6, "thread-b": 6},
                )
            }
        )
    )

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    manifest = SuperprojectManifest(
        slug="telegram-bots-suite",
        name="telegram-bots-suite",
        created_at=utc_now(),
        updated_at=utc_now(),
        revision=6,
        managed_files=[server_document.record],
    )
    thread_checkpoint = ThreadCheckpoint(
        checkpoint_id="checkpoint-thread-a",
        superproject_slug="telegram-bots-suite",
        thread_id="thread-a",
        revision=6,
        created_at=utc_now(),
        source_device_id="device-a",
        canonical=True,
        base_revision=5,
        turn_hashes=["turn-a"],
        summary="thread-a",
        manifest=manifest,
        managed_documents=[server_document],
        raw_bundle=RawSessionBundle(
            bundle_id="bundle-thread-a",
            captured_at=utc_now(),
            thread_id="thread-a",
            session_ids=["thread-a", "thread-b"],
            files=[
                RawFileArtifact(
                    relative_path="sessions/2026/03/17/thread-a.jsonl",
                    sha256="session-a",
                    content_b64=encode_b64(b"thread-a-session"),
                )
            ],
        ),
        snapshot_hash="snapshot-thread-a",
    )
    shared_checkpoint = ThreadCheckpoint(
        checkpoint_id="checkpoint-shared",
        superproject_slug="telegram-bots-suite",
        thread_id=None,
        revision=5,
        created_at=utc_now(),
        source_device_id="device-a",
        canonical=True,
        base_revision=4,
        turn_hashes=[],
        summary="shared runtime",
        manifest=manifest,
        managed_documents=[server_document],
        shared_bundle=RawCodexSharedBundle(
            bundle_id="bundle-shared",
            captured_at=utc_now(),
            files=[
                RawFileArtifact(
                    relative_path="session_index.jsonl",
                    sha256="index",
                    content_b64=encode_b64(b"new-index"),
                )
            ],
        ),
        snapshot_hash="snapshot-shared",
    )
    server_state = PullStateResponse(
        manifest=manifest,
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=thread_checkpoint,
        shared_checkpoint=shared_checkpoint,
        thread_checkpoints=[thread_checkpoint],
        pending_resolutions=[],
    )
    progress_messages: list[str] = []
    service = ClientService(
        state_store=state_store,
        codex_root=tmp_path / ".codex",
        progress_callback=progress_messages.append,
    )
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    diff = service.update_from_server("telegram-bots-suite", assume_yes=True)

    assert diff.thread_updates == []
    assert progress_messages == [
        "Connecting to the server for 'telegram-bots-suite'...",
        "Comparing local Markdown for 'telegram-bots-suite' with the server copy...",
        "Applying server updates for 'telegram-bots-suite'...",
        "Syncing shared skills for 'telegram-bots-suite'...",
        "Applying the shared Codex runtime bundle for 'telegram-bots-suite'...",
        "Update from server finished for 'telegram-bots-suite'.",
    ]


def test_force_thread_updates_pushes_tracked_threads_and_records_revision(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    baseline_path = managed_root / "baseline" / "base_rules.md"
    baseline_path.parent.mkdir(parents=True, exist_ok=True)
    baseline_path.write_text("# local\n", encoding="utf-8")
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)

    codex_root = tmp_path / ".codex"
    session_dir = codex_root / "sessions" / "2026" / "03" / "18"
    session_dir.mkdir(parents=True, exist_ok=True)
    session_dir.joinpath("rollout-thread-a.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"id": "thread-a", "cwd": str(workspace_root)}}),
                json.dumps(
                    {
                        "type": "event_msg",
                        "payload": {"type": "user_message", "message": "hello world"},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            device_id="device-a",
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    managed_root=str(managed_root),
                    workspace_roots=[str(workspace_root)],
                    tracked_thread_ids=["thread-a"],
                    managed_file_ids={"baseline/base_rules.md": "server-file-id"},
                )
            },
        )
    )

    manifest = SuperprojectManifest(
        slug="telegram-bots-suite",
        name="telegram-bots-suite",
        created_at=utc_now(),
        updated_at=utc_now(),
        revision=2,
        managed_files=[
            ManagedFileRecord(
                file_id="server-file-id",
                relative_path="baseline/base_rules.md",
                sha256="unused",
                size_bytes=len("# local\n"),
                line_count=1,
                classification=ManagedFileClass.PROTECTED,
            )
        ],
    )

    class _ForcePushApiClient:
        def __init__(self) -> None:
            self.acquired = 0
            self.released = 0
            self.pushed: list[str] = []

        def acquire_lease(self, steal: bool = False):
            self.acquired += 1
            return type("Lease", (), {"granted": True, "conflict_device_id": None})()

        def release_lease(self):
            self.released += 1
            return None

        def get_manifest(self, _slug: str):
            return manifest

        def push_checkpoint(self, _slug: str, request):
            self.pushed.append(request.checkpoint.thread_id)
            return PushCheckpointResponse(accepted=True, revision=7)

    api = _ForcePushApiClient()
    service = ClientService(state_store=state_store, codex_root=codex_root)
    service.api_client = lambda: api  # type: ignore[method-assign]

    pushed = service.force_thread_updates("telegram-bots-suite")
    updated_state = state_store.load_config().superprojects["telegram-bots-suite"]

    assert pushed == [{"thread_id": "thread-a", "thread_name": "hello world", "revision": 7}]
    assert api.acquired == 1
    assert api.released == 1
    assert api.pushed == ["thread-a"]
    assert updated_state.pending_thread_refreshes["thread-a"] == 7


def test_threadlist_backfills_preview_from_local_thread_cache(tmp_path) -> None:
    codex_root = tmp_path / ".codex"
    session_dir = codex_root / "sessions" / "2026" / "03" / "16"
    session_dir.mkdir(parents=True, exist_ok=True)
    session_dir.joinpath("rollout-thread-a.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"type": "session_meta", "payload": {"id": "thread-a", "cwd": str(tmp_path)}}),
                json.dumps(
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "user_message",
                            "message": "# Context from my IDE setup:\n\n## My request for Codex:\nfirst line\nsecond line",
                        },
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(
        ClientConfig(
            superprojects={
                "telegram-bots-suite": ClientSuperprojectState(
                    slug="telegram-bots-suite",
                    name="telegram-bots-suite",
                    tracked_thread_ids=["thread-a"],
                )
            }
        )
    )

    class _ThreadListApiClient:
        def list_threads(self, _slug: str):
            return [
                ThreadSummary(
                    thread_id="thread-a",
                    thread_name="fallback name",
                    updated_at=utc_now(),
                    last_user_turn_preview=None,
                    tracked=False,
                    source="server",
                )
            ]

    service = ClientService(state_store=state_store, codex_root=codex_root)
    service.api_client = lambda: _ThreadListApiClient()  # type: ignore[method-assign]

    threads = service.threadlist("telegram-bots-suite")

    assert len(threads) == 1
    assert threads[0].last_user_turn_preview == "first line\nsecond line"
    assert threads[0].tracked is True


def test_apply_shared_bundle_skips_locked_runtime_artifacts(monkeypatch, tmp_path) -> None:
    service = ClientService(codex_root=tmp_path / ".codex")
    bundle = RawCodexSharedBundle(
        captured_at=utc_now(),
        files=[
            RawFileArtifact(
                relative_path="session_index.jsonl",
                sha256="index",
                content_b64=encode_b64(b"index"),
            ),
            RawFileArtifact(
                relative_path="state_5.sqlite-shm",
                sha256="sidecar",
                content_b64=encode_b64(b"volatile"),
            ),
            RawFileArtifact(
                relative_path="skills/custom-skill/SKILL.md",
                sha256="skill",
                content_b64=encode_b64(b"skill-body"),
            ),
        ],
    )

    def fake_atomic_write_bytes(path, data):
        if str(path).endswith(".sqlite-shm"):
            raise OSError(22, "Invalid argument")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(data)

    monkeypatch.setattr("cws.client.sync.atomic_write_bytes", fake_atomic_write_bytes)

    service._apply_shared_bundle(bundle)

    assert (tmp_path / ".codex" / "session_index.jsonl").read_text(encoding="utf-8") == "index"
    assert not (tmp_path / ".codex" / "state_5.sqlite-shm").exists()
    assert (tmp_path / ".codex" / "skills" / "custom-skill" / "SKILL.md").read_text(encoding="utf-8") == (
        "skill-body"
    )


def test_disconnect_superproject_wipes_local_managed_root(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    (managed_root / "baseline").mkdir(parents=True, exist_ok=True)
    (managed_root / "baseline" / "base_rules.md").write_text("rules", encoding="utf-8")

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    config = ClientConfig(
        superprojects={
            "telegram-bots-suite": ClientSuperprojectState(
                slug="telegram-bots-suite",
                name="telegram-bots-suite",
                managed_root=str(managed_root),
            )
        }
    )
    state_store.save_config(config)

    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")
    result = service.disconnect_superproject("telegram-bots-suite")
    updated = state_store.load_config()

    assert result["slug"] == "telegram-bots-suite"
    assert result["managed_root_deleted"] is True
    assert "telegram-bots-suite" not in updated.superprojects
    assert not managed_root.exists()



def test_attach_superproject_registers_local_state_and_pulls_server_docs(tmp_path) -> None:
    managed_root = tmp_path / "managed"
    workspace_root = tmp_path / "workspace"
    workspace_root.mkdir(parents=True, exist_ok=True)

    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(ClientConfig())

    server_document = ManagedDocument(
        record=ManagedFileRecord(
            file_id="server-file-id",
            relative_path="baseline/base_rules.md",
            sha256="unused",
            size_bytes=len("# server\n"),
            line_count=1,
            classification=ManagedFileClass.PROTECTED,
        ),
        content="# server\n",
    )
    server_state = PullStateResponse(
        manifest=SuperprojectManifest(
            slug="telegram-bots-suite",
            name="Telegram Bots Suite",
            created_at=utc_now(),
            updated_at=utc_now(),
            managed_files=[server_document.record],
        ),
        managed_documents=[server_document],
        shared_skills=[],
        latest_checkpoint=None,
        pending_resolutions=[],
    )
    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")
    service.api_client = lambda: _FakeApiClient(server_state)  # type: ignore[method-assign]

    diff = service.attach_superproject(
        "telegram-bots-suite",
        managed_root=managed_root,
        workspace_roots=[workspace_root],
        assume_yes=True,
    )
    updated_state = state_store.load_config().superprojects["telegram-bots-suite"]

    assert diff.new_on_server == ["baseline/base_rules.md"]
    assert diff.new_local == []
    assert diff.changed == []
    assert updated_state.name == "Telegram Bots Suite"
    assert updated_state.managed_root == str(managed_root)
    assert updated_state.workspace_roots == [str(workspace_root)]
    assert updated_state.last_alignment_action == AlignmentAction.UPDATE_FROM_SERVER
    assert (managed_root / "baseline" / "base_rules.md").read_text(encoding="utf-8") == "# server\n"


def test_update_from_server_reports_attach_hint_for_unknown_local_superproject(tmp_path) -> None:
    state_store = ClientStateStore(ClientPaths.default(tmp_path / "client"))
    state_store.save_config(ClientConfig())
    service = ClientService(state_store=state_store, codex_root=tmp_path / ".codex")

    with pytest.raises(RuntimeError, match="attach-superproject or create-superproject first"):
        service.update_from_server("telegram-bots-suite", assume_yes=True)
