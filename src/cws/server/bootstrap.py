from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn

from cws.config import ServerPaths
from cws.models import RegisterDeviceRequest
from cws.server.service import ServerService

app = typer.Typer(help="Server management for Codex Workspace Sync.")


def _service(app_root: Path | None, state_root: Path | None) -> ServerService:
    if app_root is None and state_root is None:
        repo_root = Path(__file__).resolve().parents[3]
        return ServerService(paths=ServerPaths.default(app_root=repo_root, state_root=repo_root / "state"))
    return ServerService(paths=ServerPaths.default(app_root=app_root, state_root=state_root))


@app.command("init")
def init_server(
    bootstrap_passphrase: str = typer.Option(..., prompt=True, hide_input=True),
    app_root: Path | None = typer.Option(None),
    state_root: Path | None = typer.Option(None),
) -> None:
    service = _service(app_root, state_root)
    service.init_state(bootstrap_passphrase)
    typer.echo(f"Initialized server state at {service.paths.state_root}")


@app.command("register-device")
def register_device(
    device_name: str = typer.Option(...),
    secondary_passphrase: str = typer.Option(..., prompt=True, hide_input=True),
    metadata_json: str = typer.Option("{}", help="JSON object with device metadata."),
    app_root: Path | None = typer.Option(None),
    state_root: Path | None = typer.Option(None),
) -> None:
    service = _service(app_root, state_root)
    response = service.register_device(
        RegisterDeviceRequest(
            device_name=device_name,
            secondary_passphrase=secondary_passphrase,
            metadata=json.loads(metadata_json),
        )
    )
    typer.echo(json.dumps(response.model_dump(mode="json"), indent=2))


@app.command("serve")
def serve(
    host: str = typer.Option("0.0.0.0"),
    port: int = typer.Option(8787),
) -> None:
    uvicorn.run("cws.server.app:app", host=host, port=port, reload=False)


@app.command("analyze-state")
def analyze_state(
    slug: str | None = typer.Option(None, "--slug"),
    app_root: Path | None = typer.Option(None),
    state_root: Path | None = typer.Option(None),
) -> None:
    service = _service(app_root, state_root)
    typer.echo(json.dumps(service.analyze_state(slug), indent=2))


@app.command("compact-state")
def compact_state(
    slug: str | None = typer.Option(None, "--slug"),
    vacuum: bool = typer.Option(True, "--vacuum/--no-vacuum"),
    app_root: Path | None = typer.Option(None),
    state_root: Path | None = typer.Option(None),
) -> None:
    service = _service(app_root, state_root)
    typer.echo(json.dumps(service.compact_state(slug, vacuum=vacuum), indent=2))


if __name__ == "__main__":
    app()
