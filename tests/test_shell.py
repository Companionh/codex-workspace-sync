from __future__ import annotations

from cws.cli import run_shell_command
from cws.shell import CWSShell


class _BoomService:
    def status(self):
        raise RuntimeError("boom")


class _RecordingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def update_from_server(self, slug: str):
        self.calls.append(("update_from_server", (slug,)))
        return type("Diff", (), {"__dict__": {"new_on_server": [], "new_local": [], "changed": []}})()

    def turn_on_sync(self, slug: str, *, steal: bool = False):
        self.calls.append(("turn_on_sync", (slug, steal)))
        return slug


def test_shell_reports_command_errors_and_keeps_running(monkeypatch, capsys) -> None:
    inputs = iter(["status", "quit"])
    monkeypatch.setattr("builtins.input", lambda _prompt='': next(inputs))

    CWSShell(_BoomService()).run()

    captured = capsys.readouterr()
    assert "Command failed: boom" in captured.err


def test_run_shell_command_accepts_positional_superproject(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "update-from-server", ["telegram-bots-suite"])

    captured = capsys.readouterr()
    assert service.calls == [("update_from_server", ("telegram-bots-suite",))]
    assert '"changed": []' in captured.out


def test_run_shell_command_keeps_flagged_superproject_compatibility(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "update-from-server", ["--superproject", "telegram-bots-suite"])

    captured = capsys.readouterr()
    assert service.calls == [("update_from_server", ("telegram-bots-suite",))]
    assert '"new_on_server": []' in captured.out
