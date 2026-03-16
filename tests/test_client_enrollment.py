from __future__ import annotations

import json

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

    def set_missing_host_key_policy(self, _policy) -> None:
        return None

    def connect(self, **kwargs) -> None:
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

    response = ClientService()._register_device_over_ssh(
        ssh_host="37.27.184.8",
        ssh_user="root",
        ssh_port=22,
        device_name="machine-a",
        secondary_passphrase="bootstrap-secret",
        metadata={"platform": "windows"},
        ssh_password=None,
    )

    assert response["device"]["device_id"] == "device-123"
    assert _FakeSSHClient.last_command is not None
    assert "$APP_ROOT/.venv/bin/python" in _FakeSSHClient.last_command
    assert "register-device" in _FakeSSHClient.last_command
