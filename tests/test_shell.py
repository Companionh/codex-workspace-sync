from __future__ import annotations

from cws.shell import CWSShell


class _BoomService:
    def status(self):
        raise RuntimeError("boom")


def test_shell_reports_command_errors_and_keeps_running(monkeypatch, capsys) -> None:
    inputs = iter(["status", "quit"])
    monkeypatch.setattr("builtins.input", lambda _prompt='': next(inputs))

    CWSShell(_BoomService()).run()

    captured = capsys.readouterr()
    assert "Command failed: boom" in captured.err
