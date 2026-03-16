from __future__ import annotations

import json
import time
import traceback
from pathlib import Path

import typer

from cws.client.sync import ClientService
from cws.shell import CWSShell

app = typer.Typer(help="Codex Workspace Sync client.")


def service() -> ClientService:
    return ClientService()


def enroll_device_interactive(client: ClientService) -> None:
    server_url = typer.prompt("Server URL", default="http://127.0.0.1:8787")
    ssh_host = typer.prompt("SSH host")
    ssh_user = typer.prompt("SSH user")
    ssh_port = int(typer.prompt("SSH port", default="22"))
    device_name = typer.prompt("Device name")
    secondary_passphrase = typer.prompt("Secondary passphrase", hide_input=True)
    ssh_password = typer.prompt("SSH password", hide_input=True, default="", show_default=False) or None
    github_pat = typer.prompt("GitHub PAT (optional)", hide_input=True, default="", show_default=False) or None
    response = client.enroll_device(
        server_url=server_url,
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        device_name=device_name,
        secondary_passphrase=secondary_passphrase,
        ssh_password=ssh_password,
        github_pat=github_pat,
    )
    typer.echo(json.dumps(response, indent=2))


def create_superproject_interactive(client: ClientService) -> None:
    name = typer.prompt("Superproject name")
    repo_urls: list[str] = []
    typer.echo("Enter GitHub repo URLs one by one. Type 'done' when finished.")
    while True:
        value = typer.prompt("Repo URL")
        if value.strip().lower() == "done":
            break
        repo_urls.append(value)
    managed_root = Path(typer.prompt("Local managed docs root"))
    workspace_roots: list[Path] = []
    typer.echo("Enter workspace roots used for Codex session matching. Type 'done' when finished.")
    while True:
        value = typer.prompt("Workspace root")
        if value.strip().lower() == "done":
            break
        workspace_roots.append(Path(value))
    slug = client.create_superproject(
        name=name,
        repo_urls=repo_urls,
        managed_root=managed_root,
        workspace_roots=workspace_roots,
    )
    typer.echo(f"Created superproject '{slug}'.")


def run_shell_command(client: ClientService, command: str, args: list[str]) -> None:
    def arg_value(name: str, *, required: bool = True) -> str | None:
        if name in args:
            index = args.index(name)
            if index + 1 < len(args):
                return args[index + 1]
        if required:
            raise RuntimeError(f"Missing {name}")
        return None

    if command == "update-from-server":
        slug = arg_value("--superproject")
        diff = client.update_from_server(slug)
        typer.echo(json.dumps(diff.__dict__, indent=2))
        return
    if command == "override-current-state":
        slug = arg_value("--superproject")
        thread_id = arg_value("--thread", required=False)
        checkpoint = client.override_current_state(slug, thread_id=thread_id)
        typer.echo(f"Override pushed for {checkpoint.superproject_slug} at revision {checkpoint.base_revision + 1}.")
        return
    if command == "refresh-thread":
        slug = arg_value("--superproject")
        thread_id = arg_value("--thread")
        client.refresh_thread(slug, thread_id)
        typer.echo("Thread refresh payload applied locally. Reopen the thread in VS Code.")
        return
    if command == "turn-on-sync":
        slug = arg_value("--superproject")
        try:
            client.turn_on_sync(slug)
            typer.echo(f"Live sync is active for '{slug}'.")
            return
        except RuntimeError as exc:
            message = str(exc)
            if "update-from-server or override-current-state first" in message:
                typer.echo(message)
                choice = typer.prompt("Type 'update' or 'override'")
                if choice.strip().lower().startswith("u"):
                    client.update_from_server(slug)
                else:
                    client.override_current_state(slug)
                client.turn_on_sync(slug)
                typer.echo(f"Live sync is active for '{slug}'.")
                return
            if "Another device currently holds the active lease" in message:
                if typer.confirm("Steal the global lease from the other device?"):
                    client.turn_on_sync(slug, steal=True)
                    typer.echo(f"Live sync is active for '{slug}'.")
                    return
            raise


@app.command("shell")
def shell() -> None:
    CWSShell(service()).run()


@app.command("status")
def status() -> None:
    typer.echo(json.dumps(service().status(), indent=2))


@app.command("enroll-device")
def enroll_device() -> None:
    try:
        enroll_device_interactive(service())
    except Exception as exc:
        typer.echo("")
        typer.echo(f"Enrollment failed: {exc}", err=True)
        typer.echo("")
        traceback.print_exc()
        raise typer.Exit(code=1)


@app.command("create-superproject")
def create_superproject() -> None:
    create_superproject_interactive(service())


@app.command("update-from-server")
def update_from_server(superproject: str = typer.Option(..., "--superproject")) -> None:
    diff = service().update_from_server(superproject)
    typer.echo(json.dumps(diff.__dict__, indent=2))


@app.command("override-current-state")
def override_current_state(
    superproject: str = typer.Option(..., "--superproject"),
    thread: str | None = typer.Option(None, "--thread"),
) -> None:
    checkpoint = service().override_current_state(superproject, thread_id=thread)
    typer.echo(f"Override pushed for {checkpoint.superproject_slug}.")


@app.command("turn-on-sync")
def turn_on_sync(superproject: str = typer.Option(..., "--superproject")) -> None:
    client = service()
    run_shell_command(client, "turn-on-sync", ["--superproject", superproject])
    while True:
        try:
            typer.echo("Sync worker running. Press Ctrl+C to stop.")
            time.sleep(60)
        except KeyboardInterrupt:
            client.turn_off_sync()
            raise typer.Exit()


@app.command("turn-off-sync")
def turn_off_sync() -> None:
    service().turn_off_sync()
    typer.echo("Sync stopped.")


@app.command("refresh-thread")
def refresh_thread(
    superproject: str = typer.Option(..., "--superproject"),
    thread: str = typer.Option(..., "--thread"),
) -> None:
    service().refresh_thread(superproject, thread)
    typer.echo("Thread refresh payload applied locally. Reopen the thread in VS Code.")
