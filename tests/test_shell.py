from __future__ import annotations

from cws.cli import run_shell_command, service
from cws.shell import CWSShell


class _BoomService:
    def status(self):
        raise RuntimeError("boom")


class _RecordingService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def update_from_server(self, slug: str):
        self.calls.append(("update_from_server", (slug,)))
        return type(
            "Diff",
            (),
            {"__dict__": {"new_on_server": [], "new_local": [], "changed": [], "thread_updates": []}},
        )()

    def turn_on_sync(self, slug: str, *, steal: bool = False):
        self.calls.append(("turn_on_sync", (slug, steal)))
        return slug

    def local_threads(self):
        self.calls.append(("local_threads", ()))
        return [type("Thread", (), {"model_dump": lambda self, mode="json": {"thread_id": "thread-a"}})()]

    def threadlist(self, slug: str):
        self.calls.append(("threadlist", (slug,)))
        return [type("Thread", (), {"model_dump": lambda self, mode="json": {"thread_id": "thread-a"}})()]

    def add_thread(self, slug: str, thread_ref: str):
        self.calls.append(("add_thread", (slug, thread_ref)))
        return type("Thread", (), {"model_dump": lambda self, mode="json": {"thread_id": "thread-a"}})()

    def rename_superproject(self, slug: str, new_name: str):
        self.calls.append(("rename_superproject", (slug, new_name)))
        return {"slug": slug, "name": new_name}

    def rename_thread(self, slug: str, thread_ref: str, new_name: str):
        self.calls.append(("rename_thread", (slug, thread_ref, new_name)))
        return {"slug": slug, "thread_id": "thread-a", "name": new_name, "name_manually_set": True}

    def force_thread_updates(self, slug: str, *, steal: bool = False):
        self.calls.append(("force_thread_updates", (slug, steal)))
        return [{"thread_id": "thread-a", "revision": 7}]


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


def test_cli_service_factory_wires_progress_callback(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeClientService:
        def __init__(self, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr("cws.cli.ClientService", _FakeClientService)

    service()

    assert callable(captured["progress_callback"])


def test_run_shell_command_lists_local_threads(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "localthreads", [])

    captured = capsys.readouterr()
    assert service.calls == [("local_threads", ())]
    assert '"thread_id": "thread-a"' in captured.out


def test_run_shell_command_adds_thread_by_name(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "addthread", ["telegram-bots-suite", "Clone Companionh repos"])

    captured = capsys.readouterr()
    assert service.calls == [("add_thread", ("telegram-bots-suite", "Clone Companionh repos"))]
    assert '"thread_id": "thread-a"' in captured.out


def test_run_shell_command_renames_superproject(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "rename-superproject", ["telegram-bots-suite", "Telegram", "Bots", "Suite"])

    captured = capsys.readouterr()
    assert service.calls == [("rename_superproject", ("telegram-bots-suite", "Telegram Bots Suite"))]
    assert '"name": "Telegram Bots Suite"' in captured.out


def test_run_shell_command_renames_thread(capsys) -> None:
    service = _RecordingService()

    run_shell_command(
        service,
        "rename-thread",
        ["telegram-bots-suite", "thread-a", "My", "Manual", "Thread"],
    )

    captured = capsys.readouterr()
    assert service.calls == [("rename_thread", ("telegram-bots-suite", "thread-a", "My Manual Thread"))]
    assert '"name_manually_set": true' in captured.out.lower()


def test_run_shell_command_force_pushes_tracked_threads(capsys) -> None:
    service = _RecordingService()

    run_shell_command(service, "force-thread-updates", ["telegram-bots-suite", "--steal"])

    captured = capsys.readouterr()
    assert service.calls == [("force_thread_updates", ("telegram-bots-suite", True))]
    assert '"revision": 7' in captured.out
