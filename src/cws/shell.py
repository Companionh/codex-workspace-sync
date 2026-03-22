from __future__ import annotations

import json
import shlex

import typer

from cws.client.sync import ClientService


class CWSShell:
    def __init__(self, service: ClientService) -> None:
        self.service = service

    def run(self) -> None:
        typer.echo("Codex Workspace Sync shell. Type 'help' for commands.")
        while True:
            try:
                raw = input("cws> ").strip()
            except (EOFError, KeyboardInterrupt):
                typer.echo("")
                break
            if not raw:
                continue
            if raw in {"exit", "quit"}:
                break
            if raw == "help":
                self._print_help()
                continue
            try:
                self._dispatch(raw)
            except Exception as exc:
                typer.echo(f"Command failed: {self._format_error(exc)}", err=True)

    def _print_help(self) -> None:
        typer.echo(
            "\n".join(
                [
                    "State and safety:",
                    "  status",
                    "  doctor [slug]",
                    "  queue-status",
                    "  set-lease-scope <global|superproject>",
                    "",
                    "Superprojects:",
                    "  enroll-device",
                    "  create-superproject",
                    "  attach-superproject",
                    "  rename-superproject <slug> <new-name>",
                    "  disconnect-superproject <slug>",
                    "  delete-superproject-server <slug> [--force]",
                    "",
                    "Threads:",
                    "  localthreads",
                    "  threadlist <slug>",
                    "  addthread <slug> <thread-id-or-name>",
                    "  rename-thread <slug> <thread-id-or-name> <new-name>",
                    "  untrack-thread <slug> <thread-id-or-name>",
                    "  remove-thread <slug> <thread-id-or-name>",
                    "  refresh-thread <slug> --thread <id>",
                    "",
                    "Sync:",
                    "  update-from-server <slug> [--dry-run]",
                    "  override-current-state <slug> [--thread <id>]",
                    "  force-thread-updates <slug> [--steal]",
                    "  turn-on-sync <slug>",
                    "  turn-off-sync",
                    "",
                    "exit",
                ]
            )
        )

    @staticmethod
    def _format_error(exc: Exception) -> str:
        if isinstance(exc, KeyError) and exc.args:
            return str(exc.args[0])
        return str(exc)

    def _dispatch(self, raw: str) -> None:
        parts = shlex.split(raw)
        command = parts[0]
        args = parts[1:]
        if command == "status":
            typer.echo(json.dumps(self.service.status(), indent=2))
            return
        if command == "turn-off-sync":
            self.service.turn_off_sync()
            typer.echo("Sync stopped.")
            return
        if command == "enroll-device":
            from cws.cli import enroll_device_interactive

            enroll_device_interactive(self.service)
            return
        if command == "create-superproject":
            from cws.cli import create_superproject_interactive

            create_superproject_interactive(self.service)
            return
        if command == "attach-superproject":
            from cws.cli import attach_superproject_interactive

            attach_superproject_interactive(self.service)
            return
        if command in {
            "doctor",
            "queue-status",
            "localthreads",
            "threadlist",
            "addthread",
            "rename-superproject",
            "rename-thread",
            "untrack-thread",
            "remove-thread",
            "set-lease-scope",
            "update-from-server",
            "override-current-state",
            "force-thread-updates",
            "turn-on-sync",
            "refresh-thread",
            "disconnect-superproject",
            "delete-superproject-server",
        }:
            from cws.cli import run_shell_command

            run_shell_command(self.service, command, args)
            return
        typer.echo(f"Unknown command: {command}")
