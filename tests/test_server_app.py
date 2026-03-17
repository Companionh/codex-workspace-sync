from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from cws.models import CreateSuperprojectRequest, RegisterDeviceRequest
from cws.server.app import get_service
from cws.server.app import create_app
from cws.server.app import get_service as get_service_dependency
from cws.server.service import ServerService
from cws.config import ServerPaths


def test_get_service_honors_env_state_root(monkeypatch, tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    monkeypatch.setenv("CWS_STATE_ROOT", str(state_root))
    get_service.cache_clear()
    try:
        service = get_service()
        assert service.paths.state_root == state_root
    finally:
        get_service.cache_clear()


def test_manifest_endpoint_returns_lightweight_manifest(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    service = ServerService(paths=ServerPaths.default(app_root=repo_root, state_root=tmp_path / "state"))
    service.init_state("secondary-passphrase")
    registration = service.register_device(
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

    app = create_app()
    app.dependency_overrides[get_service_dependency] = lambda: service
    client = TestClient(app)
    response = client.get(
        "/api/superprojects/telegram-suite/manifest",
        headers={
            "X-CWS-Device-Id": registration.device.device_id,
            "X-CWS-Device-Secret": registration.device_secret,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["manifest"]["slug"] == "telegram-suite"
    assert "managed_files" in payload["manifest"]
