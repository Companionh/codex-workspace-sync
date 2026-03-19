from __future__ import annotations

import json
import time
import traceback
from pathlib import Path

import typer

from cws.client.sync import ClientService
from cws.config import ClientPaths
from cws.shell import CWSShell
from cws.utils import utc_now

app = typer.Typer(
    help="Codex Workspace Sync client.",
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
)


def emit_progress(message: str) -> None:
    timestamp = time.strftime("%H:%M:%S")
    typer.secho(f"[cws {timestamp}] {message}", fg=typer.colors.CYAN)


def service() -> ClientService:
    return ClientService(progress_callback=emit_progress)


def enrollment_log_file() -> Path:
    return ClientPaths.default().root / "logs" / "cws-enroll-device.log"


def append_enrollment_log(message: str) -> None:
    log_path = enrollment_log_file()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{utc_now().isoformat()}] {message}\n")


def enroll_device_interactive(client: ClientService) -> None:
    append_enrollment_log("Starting interactive device enrollment.")
    server_url = typer.prompt("Server URL", default="http://127.0.0.1:8787")
    ssh_host = typer.prompt("SSH host (hostname/IP only is best; pasted ssh targets are accepted)")
    ssh_user = typer.prompt("SSH user")
    ssh_port = int(typer.prompt("SSH port", default="22"))
    ssh_host, ssh_user, ssh_port = client.normalize_ssh_target(ssh_host, ssh_user, ssh_port)
    typer.echo(f"Using SSH target: {ssh_user}@{ssh_host}:{ssh_port}")
    append_enrollment_log(f"Normalized SSH target to {ssh_user}@{ssh_host}:{ssh_port}")
    device_name = typer.prompt("Device name")
    typer.echo("Secondary passphrase = the passphrase you set when you ran `cws-server init` on Hetzner.")
    secondary_passphrase = typer.prompt("Secondary passphrase", hide_input=True)
    typer.echo("SSH password = your Linux account password. Leave it blank if you log in with an SSH key.")
    ssh_password = typer.prompt(
        "SSH password (leave blank for SSH key login)",
        hide_input=True,
        default="",
        show_default=False,
    ) or None
    typer.echo("SSH key passphrase = the passphrase that unlocks your local private key. Leave it blank if your key has no passphrase or your ssh-agent already has it loaded.")
    ssh_key_passphrase = typer.prompt(
        "SSH key passphrase (optional)",
        hide_input=True,
        default="",
        show_default=False,
    ) or None
    typer.echo("GitHub PAT is optional here. Press Enter to skip unless you want private-repo metadata lookups.")
    github_pat = typer.prompt(
        "GitHub PAT (optional)",
        hide_input=True,
        default="",
        show_default=False,
    ) or None
    response = client.enroll_device(
        server_url=server_url,
        ssh_host=ssh_host,
        ssh_user=ssh_user,
        ssh_port=ssh_port,
        device_name=device_name,
        secondary_passphrase=secondary_passphrase,
        ssh_password=ssh_password,
        ssh_key_passphrase=ssh_key_passphrase,
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


def attach_superproject_interactive(client: ClientService) -> None:
    slug = typer.prompt("Existing superproject slug")
    managed_root = Path(typer.prompt("Local managed docs root"))
    workspace_roots: list[Path] = []
    typer.echo("Enter workspace roots used for Codex session matching. Type 'done' when finished.")
    while True:
        value = typer.prompt("Workspace root", default="done")
        if value.strip().lower() == "done":
            break
        workspace_roots.append(Path(value))
    diff = client.attach_superproject(
        slug,
        managed_root=managed_root,
        workspace_roots=workspace_roots,
        assume_yes=True,
    )
    typer.echo(f"Attached local superproject '{slug}'.")
    typer.echo(json.dumps(diff.__dict__, indent=2))


def _positional_args(args: list[str]) -> list[str]:
    values: list[str] = []
    skip_next = False
    for index, arg in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        if arg.startswith("--"):
            if index + 1 < len(args) and not args[index + 1].startswith("--"):
                skip_next = True
            continue
        values.append(arg)
    return values


def _resolve_shell_superproject(args: list[str]) -> str:
    if "--superproject" in args:
        index = args.index("--superproject")
        if index + 1 < len(args):
            return args[index + 1]
        raise RuntimeError("Missing value for --superproject")
    positional = _positional_args(args)
    if positional:
        return positional[0]
    raise RuntimeError("Missing superproject slug")


def _resolve_cli_superproject(
    positional_superproject: str | None,
    option_superproject: str | None,
) -> str:
    slug = option_superproject or positional_superproject
    if slug:
        return slug
    raise typer.BadParameter("Missing superproject slug.")


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
        slug = _resolve_shell_superproject(args)
        diff = client.update_from_server(slug)
        typer.echo(json.dumps(diff.__dict__, indent=2))
        return
    if command == "threadlist":
        slug = _resolve_shell_superproject(args)
        typer.echo(json.dumps([thread.model_dump(mode="json") for thread in client.threadlist(slug)], indent=2))
        return
    if command == "localthreads":
        typer.echo(json.dumps([thread.model_dump(mode="json") for thread in client.local_threads()], indent=2))
        return
    if command == "addthread":
        slug = _resolve_shell_superproject(args)
        positional = _positional_args(args)
        if len(positional) < 2:
            raise RuntimeError("Missing thread name or ID")
        thread_ref = positional[1]
        thread = client.add_thread(slug, thread_ref)
        typer.echo(json.dumps(thread.model_dump(mode="json"), indent=2))
        return
    if command == "rename-superproject":
        slug = _resolve_shell_superproject(args)
        positional = _positional_args(args)
        if len(positional) < 2:
            raise RuntimeError("Missing new superproject name")
        new_name = " ".join(positional[1:])
        typer.echo(json.dumps(client.rename_superproject(slug, new_name), indent=2))
        return
    if command == "rename-thread":
        slug = _resolve_shell_superproject(args)
        positional = _positional_args(args)
        if len(positional) < 3:
            raise RuntimeError("Missing thread reference or new thread name")
        thread_ref = positional[1]
        new_name = " ".join(positional[2:])
        typer.echo(json.dumps(client.rename_thread(slug, thread_ref, new_name), indent=2))
        return
    if command == "override-current-state":
        slug = _resolve_shell_superproject(args)
        thread_id = arg_value("--thread", required=False)
        checkpoint = client.override_current_state(slug, thread_id=thread_id)
        typer.echo(f"Override pushed for {checkpoint.superproject_slug} at revision {checkpoint.base_revision + 1}.")
        return
    if command == "force-thread-updates":
        slug = _resolve_shell_superproject(args)
        steal = "--steal" in args
        typer.echo(json.dumps(client.force_thread_updates(slug, steal=steal), indent=2))
        return
    if command == "refresh-thread":
        slug = _resolve_shell_superproject(args)
        thread_id = arg_value("--thread")
        client.refresh_thread(slug, thread_id)
        typer.echo("Thread refresh payload applied locally. Reopen the thread in VS Code.")
        return
    if command == "disconnect-superproject":
        slug = _resolve_shell_superproject(args)
        typer.echo(json.dumps(client.disconnect_superproject(slug), indent=2))
        return
    if command == "delete-superproject-server":
        slug = _resolve_shell_superproject(args)
        force = "--force" in args
        if not typer.confirm(
            f"Delete superproject '{slug}' from the server and erase its server-side state?",
            default=False,
        ):
            raise RuntimeError("Server deletion aborted by user.")
        typer.echo(json.dumps(client.delete_superproject_from_server(slug, force=force), indent=2))
        return
    if command == "turn-on-sync":
        slug = _resolve_shell_superproject(args)
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
        append_enrollment_log("Enroll command invoked.")
        enroll_device_interactive(service())
        append_enrollment_log("Enrollment completed successfully.")
    except Exception as exc:
        trace = traceback.format_exc()
        append_enrollment_log(f"Enrollment failed: {exc}")
        append_enrollment_log(trace.rstrip())
        typer.echo("")
        typer.echo(f"Enrollment failed: {exc}", err=True)
        typer.echo(f"Debug log: {enrollment_log_file()}", err=True)
        typer.echo("")
        typer.echo(trace, err=True)
        raise typer.Exit(code=1)


@app.command("create-superproject")
def create_superproject() -> None:
    create_superproject_interactive(service())


@app.command("attach-superproject")
def attach_superproject() -> None:
    attach_superproject_interactive(service())


@app.command("disconnect-superproject")
def disconnect_superproject(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps(service().disconnect_superproject(slug), indent=2))


@app.command("delete-superproject-server")
def delete_superproject_server(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
    force: bool = typer.Option(False, "--force"),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    if not typer.confirm(
        f"Delete superproject '{slug}' from the server and erase its server-side state?",
        default=False,
    ):
        raise typer.Exit(code=1)
    typer.echo(json.dumps(service().delete_superproject_from_server(slug, force=force), indent=2))


@app.command("update-from-server")
def update_from_server(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    diff = service().update_from_server(slug)
    typer.echo(json.dumps(diff.__dict__, indent=2))


@app.command("threadlist")
def threadlist(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps([thread.model_dump(mode="json") for thread in service().threadlist(slug)], indent=2))


@app.command("localthreads")
def localthreads() -> None:
    typer.echo(json.dumps([thread.model_dump(mode="json") for thread in service().local_threads()], indent=2))


@app.command("addthread")
def addthread(
    superproject: str | None = typer.Argument(None),
    thread_ref: str = typer.Argument(...),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps(service().add_thread(slug, thread_ref).model_dump(mode="json"), indent=2))


@app.command("rename-superproject")
def rename_superproject(
    superproject: str | None = typer.Argument(None),
    new_name: str = typer.Argument(...),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps(service().rename_superproject(slug, new_name), indent=2))


@app.command("rename-thread")
def rename_thread(
    superproject: str | None = typer.Argument(None),
    thread_ref: str = typer.Argument(...),
    new_name: str = typer.Argument(...),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps(service().rename_thread(slug, thread_ref, new_name), indent=2))


@app.command("override-current-state")
def override_current_state(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
    thread: str | None = typer.Option(None, "--thread"),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    checkpoint = service().override_current_state(slug, thread_id=thread)
    typer.echo(f"Override pushed for {checkpoint.superproject_slug}.")


@app.command("force-thread-updates")
def force_thread_updates(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
    steal: bool = typer.Option(False, "--steal"),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    typer.echo(json.dumps(service().force_thread_updates(slug, steal=steal), indent=2))


@app.command("turn-on-sync")
def turn_on_sync(
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    client = service()
    run_shell_command(client, "turn-on-sync", [slug])
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
    superproject: str | None = typer.Argument(None),
    superproject_option: str | None = typer.Option(None, "--superproject", hidden=True),
    thread: str = typer.Option(..., "--thread"),
) -> None:
    slug = _resolve_cli_superproject(superproject, superproject_option)
    service().refresh_thread(slug, thread)
    typer.echo("Thread refresh payload applied locally. Reopen the thread in VS Code.")
