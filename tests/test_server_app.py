from __future__ import annotations

from pathlib import Path

from cws.server.app import get_service


def test_get_service_honors_env_state_root(monkeypatch, tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    monkeypatch.setenv("CWS_STATE_ROOT", str(state_root))
    get_service.cache_clear()
    try:
        service = get_service()
        assert service.paths.state_root == state_root
    finally:
        get_service.cache_clear()
