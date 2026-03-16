from __future__ import annotations

import json
from pathlib import Path

import pytest

from cws.client.state import ClientStateStore
from cws.client.sync import ClientService
from cws.config import ClientPaths
from cws.models import (
    AlignmentAction,
    ClientConfig,
    ClientSuperprojectState,
    ManagedDocument,
    ManagedFileClass,
    ManagedFileRecord,
    PullStateResponse,
    RawFileArtifact,
    RawSessionBundle,
    SuperprojectManifest,
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


def test_apply_raw_bundle_skips_locked_runtime_artifacts(monkeypatch, tmp_path) -> None:
    service = ClientService(codex_root=tmp_path / ".codex")
    bundle = RawSessionBundle(
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
                relative_path="sessions/2026/03/16/test-session.jsonl",
                sha256="session",
                content_b64=encode_b64(b"session"),
            ),
        ],
    )

    def fake_atomic_write_bytes(path, data):
        if str(path).endswith(".sqlite-shm"):
            raise OSError(22, "Invalid argument")
        if str(path).endswith("test-session.jsonl"):
            raise PermissionError(5, "Access is denied")
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(data)

    monkeypatch.setattr("cws.client.sync.atomic_write_bytes", fake_atomic_write_bytes)

    service._apply_raw_bundle(bundle)

    assert (tmp_path / ".codex" / "session_index.jsonl").read_text(encoding="utf-8") == "index"
    assert not (tmp_path / ".codex" / "state_5.sqlite-shm").exists()
    assert not (tmp_path / ".codex" / "sessions" / "2026" / "03" / "16" / "test-session.jsonl").exists()


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
