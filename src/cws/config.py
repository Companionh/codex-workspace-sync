from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _default_client_root() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "CodexWorkspaceSync"
    return Path.home() / ".codex-workspace-sync"


@dataclass(frozen=True)
class ClientPaths:
    root: Path
    config_file: Path
    queue_file: Path
    secrets_file: Path
    cache_dir: Path
    workspace_file: Path

    @classmethod
    def default(cls, root: Path | None = None) -> "ClientPaths":
        selected_root = root or _default_client_root()
        return cls(
            root=selected_root,
            config_file=selected_root / "client-state.json",
            queue_file=selected_root / "outbound-queue.json",
            secrets_file=selected_root / "secrets.json",
            cache_dir=selected_root / "cache",
            workspace_file=selected_root / "workspace-bindings.json",
        )


@dataclass(frozen=True)
class ServerPaths:
    app_root: Path
    state_root: Path
    db_file: Path
    shared_skills_root: Path

    @classmethod
    def default(
        cls,
        app_root: Path | None = None,
        state_root: Path | None = None,
    ) -> "ServerPaths":
        selected_app_root = app_root or Path(
            os.environ.get("CWS_APP_ROOT", "/opt/codex-workspace-sync/app")
        )
        selected_state_root = state_root or Path(
            os.environ.get("CWS_STATE_ROOT", "/opt/codex-workspace-sync/state")
        )
        return cls(
            app_root=selected_app_root,
            state_root=selected_state_root,
            db_file=selected_state_root / "metadata.sqlite3",
            shared_skills_root=selected_app_root / "skills" / "shared",
        )

