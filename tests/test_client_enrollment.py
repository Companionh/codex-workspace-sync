from __future__ import annotations

import json

import pytest

from cws.client.sync import ClientService


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
