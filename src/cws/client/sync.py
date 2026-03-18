from __future__ import annotations

from collections.abc import Callable
import json
import shlex
import shutil
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import paramiko

from cws.client.api import ApiClient
from cws.client.codex import (
    build_managed_documents,
    build_shared_codex_bundle,
    build_raw_session_bundle,
    extract_turn_hashes,
    list_local_threads,
)
from cws.client.github import fetch_repo_metadata
from cws.client.state import ClientStateStore
from cws.models import (
    AlignmentAction,
    ClientConfig,
    ClientSuperprojectState,
    CreateSuperprojectRequest,
    ManagedDocument,
    MismatchResolution,
    OutboundQueueItem,
    PushCheckpointRequest,
    RawCodexSharedBundle,
    SubprojectRecord,
    ThreadSummary,
    ThreadCheckpoint,
)
from cws.utils import atomic_write_bytes, atomic_write_text, decode_b64, dump_json_file, sha256_text, slugify, utc_now


@dataclass
class DiffSummary:
    new_on_server: list[str]
    new_local: list[str]
    changed: list[str]
    thread_updates: list[str] = field(default_factory=list)

    @property
    def has_mismatch(self) -> bool:
        return bool(self.new_on_server or self.new_local or self.changed)


@dataclass
class PreparedCheckpointInputs:
    local_state: ClientSuperprojectState
    incoming_manifest: Any
    base_revision: int
    workspace_roots: list[Path]
    documents: list[ManagedDocument]
    shared_bundle: RawCodexSharedBundle


class TransientHeartbeatError(RuntimeError):
    pass


class SyncWorker(threading.Thread):
    def __init__(self, service: "ClientService", superproject_slug: str) -> None:
        super().__init__(daemon=True)
        self.service = service
        self.superproject_slug = superproject_slug
        self.stop_event = threading.Event()
        self.pending_checkpoints: dict[str, ThreadCheckpoint] = {}
        self.last_pushed_hashes: dict[str, str] = {}

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:  # pragma: no cover - exercised by integration flow
        api = self.service.api_client()
        while not self.stop_event.is_set():
            try:
                if not self._heartbeat(api):
                    return
                if not self.service.flush_outbound_queue(api, heartbeat=self._heartbeat):
                    return
                if not self._heartbeat(api):
                    return
                prepared = self.service.prepare_live_checkpoint_inputs(
                    self.superproject_slug,
                    show_progress=False,
                )
                if not self._heartbeat(api):
                    return
                checkpoints = self.service.build_live_checkpoints(
                    self.superproject_slug,
                    canonical=True,
                    show_progress=False,
                    prepared=prepared,
                )
            except TransientHeartbeatError as exc:
                self.service.report_progress(
                    f"Heartbeat request failed for '{self.superproject_slug}': {exc}. Retrying..."
                )
                time.sleep(self.service.heartbeat_interval_seconds)
                continue
            except Exception as exc:
                self.service.report_progress(
                    f"Checkpoint build failed for '{self.superproject_slug}': {exc}. Retrying..."
                )
                time.sleep(self.service.heartbeat_interval_seconds)
                continue
            for checkpoint in checkpoints:
                if not self._heartbeat(api):
                    return
                key = checkpoint.thread_id or "__docs__"
                if checkpoint.snapshot_hash == self.last_pushed_hashes.get(key):
                    continue
                pending_checkpoint = self.pending_checkpoints.get(key)
                if pending_checkpoint is None:
                    self.pending_checkpoints[key] = checkpoint
                    continue
                if pending_checkpoint.snapshot_hash == checkpoint.snapshot_hash:
                    thread_labels = self.service._format_thread_labels(
                        self.service._checkpoint_session_ids(checkpoint),
                        checkpoint,
                    )
                    try:
                        self.service.report_progress(
                            f"Detected a finished Codex turn for '{self.superproject_slug}' in thread(s): {thread_labels}."
                        )
                        self.service.report_progress(
                            f"Pushing the latest checkpoint for '{self.superproject_slug}' to the server..."
                        )
                        response = api.push_checkpoint(
                            self.superproject_slug,
                            PushCheckpointRequest(checkpoint=checkpoint),
                        )
                        if not self._heartbeat(api):
                            return
                        try:
                            self.service._record_thread_revision(
                                self.superproject_slug,
                                checkpoint,
                                response.revision,
                            )
                        except Exception:
                            pass
                        self.last_pushed_hashes[key] = checkpoint.snapshot_hash
                        self.service.report_progress(
                            f"Server updated for '{self.superproject_slug}' at revision {response.revision} for thread(s): {thread_labels}."
                        )
                    except Exception:
                        if not self._heartbeat(api):
                            return
                        self.service.report_progress(
                            f"Server push failed for '{self.superproject_slug}'. Queueing the checkpoint for retry."
                        )
                        self.service.enqueue_checkpoint(checkpoint)
                    self.pending_checkpoints.pop(key, None)
                    continue

                scratch = pending_checkpoint.model_copy(update={"canonical": False})
                try:
                    api.push_checkpoint(
                        self.superproject_slug,
                        PushCheckpointRequest(checkpoint=scratch),
                    )
                    if not self._heartbeat(api):
                        return
                except Exception:
                    pass
                self.pending_checkpoints[key] = checkpoint
            time.sleep(self.service.heartbeat_interval_seconds)
        self.service.mark_sync_inactive()

    def _heartbeat(self, api: ApiClient) -> bool:
        try:
            heartbeat = api.heartbeat()
        except httpx.HTTPError as exc:
            raise TransientHeartbeatError(str(exc)) from exc
        if heartbeat.accepted:
            return True
        self.service.report_progress("Live sync stopped because this device no longer owns the active lease.")
        self.service.mark_sync_inactive()
        return False


class ClientService:
    heartbeat_interval_seconds = 15

    def __init__(
        self,
        state_store: ClientStateStore | None = None,
        codex_root: Path | None = None,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.state_store = state_store or ClientStateStore()
        self.codex_root = codex_root or (Path.home() / ".codex")
        self.worker: SyncWorker | None = None
        self.progress_callback = progress_callback

    def report_progress(self, message: str) -> None:
        if self.progress_callback is not None:
            self.progress_callback(message)

    def config(self) -> ClientConfig:
        return self.state_store.load_config()

    def save_config(self, config: ClientConfig) -> None:
        self.state_store.save_config(config)

    def api_client(self) -> ApiClient:
        config = self.config()
        if not config.server_url or not config.device_id:
            raise RuntimeError("Device is not enrolled.")
        secret = self.state_store.get_device_secret()
        if not secret:
            raise RuntimeError("Device secret is missing.")
        return ApiClient(config.server_url, config.device_id, secret)

    def enroll_device(
        self,
        *,
        server_url: str,
        ssh_host: str,
        ssh_user: str,
        ssh_port: int,
        device_name: str,
        secondary_passphrase: str,
        ssh_password: str | None = None,
        ssh_key_passphrase: str | None = None,
        github_pat: str | None = None,
    ) -> dict[str, Any]:
        ssh_host, ssh_user, ssh_port = self.normalize_ssh_target(ssh_host, ssh_user, ssh_port)
        metadata = {
            "platform": "windows",
            "enrolled_at": utc_now().isoformat(),
        }
        response = self._register_device_over_ssh(
            ssh_host=ssh_host,
            ssh_user=ssh_user,
            ssh_port=ssh_port,
            device_name=device_name,
            secondary_passphrase=secondary_passphrase,
            metadata=metadata,
            ssh_password=ssh_password,
            ssh_key_passphrase=ssh_key_passphrase,
        )
        config = self.config()
        config.server_url = server_url.rstrip("/")
        config.device_id = response["device"]["device_id"]
        config.device_name = response["device"]["device_name"]
        config.ssh_host = ssh_host
        config.ssh_user = ssh_user
        config.ssh_port = ssh_port
        self.save_config(config)
        self.state_store.set_device_secret(response["device_secret"])
        self.state_store.set_secondary_passphrase(secondary_passphrase)
        if ssh_password:
            self.state_store.set_ssh_password(ssh_password)
        if ssh_key_passphrase:
            self.state_store.set_ssh_key_passphrase(ssh_key_passphrase)
        if github_pat:
            self.state_store.set_github_token(github_pat)
        return response

    @staticmethod
    def normalize_ssh_target(ssh_host: str, ssh_user: str, ssh_port: int) -> tuple[str, str, int]:
        raw_host = ssh_host.strip()
        normalized_user = ssh_user.strip()
        normalized_port = ssh_port

        if raw_host.lower().startswith("ssh "):
            raw_host = raw_host[4:].strip()

        normalized_host = raw_host
        if "://" in raw_host:
            parsed = urlsplit(raw_host)
            normalized_host = parsed.hostname or raw_host
            if parsed.username and not normalized_user:
                normalized_user = parsed.username
            if parsed.scheme == "ssh" and parsed.port:
                normalized_port = parsed.port
        else:
            if "@" in raw_host:
                maybe_user, maybe_host = raw_host.split("@", 1)
                if maybe_user and not normalized_user:
                    normalized_user = maybe_user
                normalized_host = maybe_host
            if normalized_host.count(":") == 1:
                host_part, port_part = normalized_host.rsplit(":", 1)
                if host_part and port_part.isdigit():
                    normalized_host = host_part
                    normalized_port = int(port_part)

        return normalized_host.strip(), normalized_user, normalized_port

    @staticmethod
    def resolve_ssh_config(ssh_host: str) -> dict[str, str]:
        config_path = Path.home() / ".ssh" / "config"
        if not config_path.exists():
            return {}

        ssh_config = paramiko.SSHConfig()
        with config_path.open("r", encoding="utf-8") as handle:
            ssh_config.parse(handle)
        resolved = ssh_config.lookup(ssh_host)
        if resolved.get("identityfile"):
            return resolved

        current_patterns: list[str] = []
        current_values: dict[str, str] = {}
        with config_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                stripped = raw_line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                parts = stripped.split(None, 1)
                if len(parts) != 2:
                    continue
                key, value = parts[0].lower(), parts[1].strip()
                if key == "host":
                    if current_values.get("hostname") == ssh_host and current_values.get("identityfile"):
                        return current_values
                    current_patterns = value.split()
                    current_values = {"host": " ".join(current_patterns)}
                    continue
                if not current_patterns:
                    continue
                current_values[key] = value

        if current_values.get("hostname") == ssh_host and current_values.get("identityfile"):
            return current_values
        return {}

    def _register_device_over_ssh(
        self,
        *,
        ssh_host: str,
        ssh_user: str,
        ssh_port: int,
        device_name: str,
        secondary_passphrase: str,
        metadata: dict[str, Any],
        ssh_password: str | None,
        ssh_key_passphrase: str | None,
    ) -> dict[str, Any]:
        ssh_config = self.resolve_ssh_config(ssh_host)
        key_filename = ssh_config.get("identityfile")
        resolved_user = ssh_config.get("user", ssh_user) or ssh_user
        resolved_port = int(ssh_config.get("port", ssh_port) or ssh_port)
        if key_filename:
            key_filename = str(Path(key_filename).expanduser())
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=ssh_host,
            username=resolved_user,
            port=resolved_port,
            password=ssh_password,
            key_filename=key_filename,
            passphrase=ssh_key_passphrase,
            timeout=20.0,
        )
        remote_script = (
            'APP_ROOT="${CWS_APP_ROOT:-/opt/codex-workspace-sync/app}"; '
            'STATE_ROOT="${CWS_STATE_ROOT:-/opt/codex-workspace-sync/state}"; '
            'if [ -x "$APP_ROOT/.venv/bin/python" ]; then '
            '  PY_BIN="$APP_ROOT/.venv/bin/python"; '
            "else "
            '  PY_BIN="python3"; '
            "fi; "
            'if [ -x "$APP_ROOT/.venv/bin/cws-server" ]; then '
            '  "$APP_ROOT/.venv/bin/cws-server" register-device '
            '--app-root "$APP_ROOT" '
            '--state-root "$STATE_ROOT" '
            f"--device-name {shlex.quote(device_name)} "
            f"--secondary-passphrase {shlex.quote(secondary_passphrase)} "
            f"--metadata-json {shlex.quote(json.dumps(metadata))}; "
            "else "
            '  "$PY_BIN" -m cws.server.bootstrap register-device '
            '--app-root "$APP_ROOT" '
            '--state-root "$STATE_ROOT" '
            f"--device-name {shlex.quote(device_name)} "
            f"--secondary-passphrase {shlex.quote(secondary_passphrase)} "
            f"--metadata-json {shlex.quote(json.dumps(metadata))}; "
            "fi"
        )
        command = f"bash -lc {shlex.quote(remote_script)}"
        _, stdout, stderr = client.exec_command(command)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode("utf-8")
        error_output = stderr.read().decode("utf-8")
        client.close()
        if exit_code != 0:
            raise RuntimeError(error_output or output or "SSH enrollment failed.")
        if not output.strip():
            raise RuntimeError("SSH enrollment did not return a device payload.")
        return json.loads(output)

    def status(self) -> dict[str, Any]:
        config = self.config()
        queue = self.state_store.load_queue()
        return {
            "device_name": config.device_name,
            "server_url": config.server_url,
            "active_superproject": config.sync_active_superproject,
            "queued_checkpoints": len(queue),
            "superprojects": {
                slug: state.model_dump(mode="json")
                for slug, state in config.superprojects.items()
            },
        }

    def local_threads(self) -> list[ThreadSummary]:
        return list_local_threads(self.codex_root)

    def _tracked_thread_ids(self, slug: str) -> list[str]:
        local_state = self._get_superproject_state(slug)
        return list(dict.fromkeys(local_state.tracked_thread_ids))

    def _thread_lookup(self) -> dict[str, ThreadSummary]:
        return {thread.thread_id: thread for thread in self.local_threads()}

    @staticmethod
    def _dedupe_thread_ids(thread_ids: list[str]) -> list[str]:
        return list(dict.fromkeys(thread_id for thread_id in thread_ids if thread_id))

    def _record_thread_revision(
        self,
        slug: str,
        checkpoint: ThreadCheckpoint,
        revision: int,
    ) -> None:
        local_state = self._get_superproject_state(slug)
        if checkpoint.thread_id is None:
            local_state.last_shared_bundle_revision = max(local_state.last_shared_bundle_revision, revision)
        thread_ids = self._dedupe_thread_ids(self._checkpoint_session_ids(checkpoint))
        if checkpoint.thread_id and checkpoint.thread_id not in thread_ids:
            thread_ids.append(checkpoint.thread_id)
        for thread_id in thread_ids:
            current_revision = local_state.pending_thread_refreshes.get(thread_id, 0)
            local_state.pending_thread_refreshes[thread_id] = max(current_revision, revision)
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)

    def _checkpoint_needs_local_refresh(
        self,
        local_state: ClientSuperprojectState,
        checkpoint: ThreadCheckpoint,
    ) -> bool:
        if checkpoint.thread_id is None:
            return checkpoint.revision > local_state.last_shared_bundle_revision
        thread_ids = self._dedupe_thread_ids(self._checkpoint_session_ids(checkpoint))
        if checkpoint.thread_id and checkpoint.thread_id not in thread_ids:
            thread_ids.append(checkpoint.thread_id)
        if not thread_ids:
            return True
        return any(checkpoint.revision > local_state.pending_thread_refreshes.get(thread_id, 0) for thread_id in thread_ids)

    def _match_local_thread(self, thread_ref: str) -> ThreadSummary:
        normalized = thread_ref.strip()
        if not normalized:
            raise RuntimeError("Thread reference cannot be empty.")
        threads = self.local_threads()
        by_id = {thread.thread_id: thread for thread in threads}
        if normalized in by_id:
            return by_id[normalized]
        matches = [
            thread
            for thread in threads
            if thread.thread_name.casefold() == normalized.casefold()
        ]
        if len(matches) == 1:
            return matches[0]
        if len(matches) > 1:
            raise RuntimeError(
                f"Multiple local threads match '{thread_ref}'. Use the thread ID instead."
            )
        partial_matches = [
            thread
            for thread in threads
            if normalized.casefold() in thread.thread_name.casefold()
        ]
        if len(partial_matches) == 1:
            return partial_matches[0]
        raise RuntimeError(f"No local Codex thread matched '{thread_ref}'.")

    def add_thread(self, slug: str, thread_ref: str) -> ThreadSummary:
        local_state = self._get_superproject_state(slug)
        thread = self._match_local_thread(thread_ref)
        tracked = list(dict.fromkeys(local_state.tracked_thread_ids + [thread.thread_id]))
        local_state.tracked_thread_ids = tracked
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)
        return thread.model_copy(update={"tracked": True})

    @staticmethod
    def _adopt_server_name(local_state: ClientSuperprojectState, server_name: str) -> None:
        if not local_state.name or not local_state.name_manually_set:
            local_state.name = server_name

    def rename_superproject(self, slug: str, new_name: str) -> dict[str, Any]:
        cleaned_name = new_name.strip()
        if not cleaned_name:
            raise RuntimeError("Superproject name cannot be empty.")
        manifest = self.api_client().rename_superproject(slug, cleaned_name).manifest
        local_state = self._get_superproject_state(slug)
        local_state.name = manifest.name
        local_state.name_manually_set = True
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)
        return {
            "slug": slug,
            "name": manifest.name,
        }

    def create_superproject(
        self,
        *,
        name: str,
        repo_urls: list[str],
        managed_root: Path,
        workspace_roots: list[Path],
    ) -> str:
        github_token = self.state_store.get_github_token()
        subprojects = []
        for repo_url in repo_urls:
            metadata = fetch_repo_metadata(repo_url, github_token)
            subprojects.append(
                SubprojectRecord(
                    repo_url=metadata.repo_url,
                    repo_name=metadata.repo_name,
                    default_branch=metadata.default_branch,
                    description=metadata.description,
                )
            )
        slug = slugify(name)
        manifest = self.api_client().create_superproject(
            CreateSuperprojectRequest(name=name, slug=slug, subprojects=subprojects)
        ).manifest
        config = self.config()
        config.superprojects[slug] = ClientSuperprojectState(
            slug=slug,
            name=name,
            name_manually_set=True,
            managed_root=str(managed_root),
            workspace_roots=[str(path) for path in workspace_roots],
        )
        self.save_config(config)
        managed_root.mkdir(parents=True, exist_ok=True)
        self.update_from_server(slug, assume_yes=True)
        return manifest.slug

    def attach_superproject(
        self,
        slug: str,
        *,
        managed_root: Path,
        workspace_roots: list[Path],
        assume_yes: bool = True,
    ) -> DiffSummary:
        server_state = self.api_client().pull_state(slug)
        config = self.config()
        local_state = config.superprojects.get(slug)
        if local_state is None:
            local_state = ClientSuperprojectState(
                slug=server_state.manifest.slug,
                name=server_state.manifest.name,
            )
        local_state.slug = server_state.manifest.slug
        self._adopt_server_name(local_state, server_state.manifest.name)
        local_state.managed_root = str(managed_root)
        local_state.workspace_roots = [str(path) for path in workspace_roots]
        config.superprojects[slug] = local_state
        self.save_config(config)
        managed_root.mkdir(parents=True, exist_ok=True)
        return self.update_from_server(server_state.manifest.slug, assume_yes=assume_yes)

    def _get_superproject_state(self, slug: str) -> ClientSuperprojectState:
        config = self.config()
        if slug not in config.superprojects:
            raise RuntimeError(
                f"Unknown local superproject: {slug}. This device is enrolled, but that superproject is not attached locally yet. Use attach-superproject or create-superproject first."
            )
        return config.superprojects[slug]

    def _write_shared_skills(self, artifacts: list[dict[str, Any]]) -> None:
        target_root = self.codex_root / "skills" / "codex-workspace-sync-shared"
        for artifact in artifacts:
            if hasattr(artifact, "relative_path"):
                relative_path = artifact.relative_path
                content_b64 = artifact.content_b64
            else:
                relative_path = artifact["relative_path"]
                content_b64 = artifact["content_b64"]
            target_path = target_root / relative_path
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(decode_b64(content_b64))

    @staticmethod
    def _is_volatile_runtime_artifact(relative_path: str) -> bool:
        return (
            relative_path.startswith("state_")
            or relative_path.startswith("logs_")
            or relative_path.endswith((".sqlite", ".sqlite-shm", ".sqlite-wal"))
        )

    @classmethod
    def _stable_thread_snapshot_entries(cls, raw_bundle) -> list[dict[str, str]]:
        return [
            {
                "relative_path": artifact.relative_path,
                "sha256": artifact.sha256,
            }
            for artifact in raw_bundle.files
        ]

    @classmethod
    def _stable_shared_snapshot_entries(cls, shared_bundle: RawCodexSharedBundle) -> list[dict[str, str]]:
        return [
            {
                "relative_path": artifact.relative_path,
                "sha256": artifact.sha256,
            }
            for artifact in shared_bundle.files
            if not cls._is_volatile_runtime_artifact(artifact.relative_path)
        ]

    @staticmethod
    def _session_checkpoints_from_state(server_state) -> list[ThreadCheckpoint]:
        checkpoints_by_id: dict[str, ThreadCheckpoint] = {}
        if server_state.latest_checkpoint is not None:
            checkpoints_by_id[server_state.latest_checkpoint.checkpoint_id] = server_state.latest_checkpoint
        for checkpoint in getattr(server_state, "thread_checkpoints", []) or []:
            checkpoints_by_id[checkpoint.checkpoint_id] = checkpoint
        return sorted(checkpoints_by_id.values(), key=lambda checkpoint: checkpoint.revision)

    def _newer_thread_checkpoints(
        self,
        local_state: ClientSuperprojectState,
        server_state: Any,
    ) -> list[ThreadCheckpoint]:
        session_checkpoints = self._session_checkpoints_from_state(server_state)
        checkpoints_with_raw_bundles = [
            checkpoint
            for checkpoint in session_checkpoints
            if checkpoint.raw_bundle is not None and checkpoint.raw_bundle.files
        ]
        return [
            checkpoint
            for checkpoint in checkpoints_with_raw_bundles
            if self._checkpoint_needs_local_refresh(local_state, checkpoint)
        ]

    @staticmethod
    def _checkpoint_session_ids(checkpoint: ThreadCheckpoint) -> list[str]:
        session_ids: list[str] = []
        if checkpoint.raw_bundle is not None:
            session_ids.extend(checkpoint.raw_bundle.session_ids)
        if checkpoint.thread_id and checkpoint.thread_id not in session_ids:
            session_ids.append(checkpoint.thread_id)
        return session_ids

    def _summary_from_checkpoint(self, checkpoint: ThreadCheckpoint) -> ThreadSummary:
        thread_id = checkpoint.thread_id
        if not thread_id:
            raise RuntimeError("Checkpoint does not refer to a named thread.")
        thread_name = (
            (checkpoint.raw_bundle.thread_name if checkpoint.raw_bundle else None)
            or checkpoint.summary
            or thread_id
        )
        updated_at = (
            (checkpoint.raw_bundle.thread_updated_at if checkpoint.raw_bundle else None)
            or checkpoint.created_at
        )
        return ThreadSummary(
            thread_id=thread_id,
            thread_name=thread_name,
            updated_at=updated_at,
            last_user_turn_preview=(
                checkpoint.raw_bundle.last_user_turn_preview if checkpoint.raw_bundle else None
            ),
            tracked=thread_id in self._tracked_thread_ids(checkpoint.superproject_slug),
            source="server",
        )

    def threadlist(self, slug: str) -> list[ThreadSummary]:
        summaries = self.api_client().list_threads(slug)
        local_lookup = self._thread_lookup()
        for summary in summaries:
            local_match = local_lookup.get(summary.thread_id)
            if local_match and local_match.thread_name:
                summary.thread_name = local_match.thread_name
            if local_match and not summary.last_user_turn_preview and local_match.last_user_turn_preview:
                summary.last_user_turn_preview = local_match.last_user_turn_preview
            summary.tracked = summary.thread_id in self._tracked_thread_ids(slug)
        return sorted(summaries, key=lambda item: (item.updated_at, item.thread_name, item.thread_id), reverse=True)

    @classmethod
    def _format_thread_labels(
        cls,
        thread_ids: list[str],
        checkpoint: ThreadCheckpoint | None = None,
    ) -> str:
        labels: list[str] = []
        seen: set[str] = set()
        preferred_name = checkpoint.raw_bundle.thread_name if checkpoint and checkpoint.raw_bundle else None
        for thread_id in thread_ids:
            if thread_id in seen:
                continue
            seen.add(thread_id)
            if preferred_name and checkpoint and checkpoint.thread_id == thread_id:
                labels.append(preferred_name)
            else:
                labels.append(thread_id)
        if not labels:
            return "shared Codex runtime files"
        return ", ".join(labels)

    @staticmethod
    def _load_manifest(api_client: Any, slug: str):
        if hasattr(api_client, "get_manifest"):
            return api_client.get_manifest(slug)
        return api_client.pull_state(slug).manifest

    def compare_with_server(self, slug: str) -> DiffSummary:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        managed_root = Path(local_state.managed_root)
        local_documents, _ = build_managed_documents(managed_root, local_state.managed_file_ids)
        manifest = self._load_manifest(self.api_client(), slug)
        server_by_path = {record.relative_path: record for record in manifest.managed_files}
        local_by_path = {document.record.relative_path: document.record for document in local_documents}
        new_on_server = sorted(set(server_by_path) - set(local_by_path))
        new_local = sorted(set(local_by_path) - set(server_by_path))
        changed = sorted(
            path
            for path in set(server_by_path) & set(local_by_path)
            if server_by_path[path].sha256 != local_by_path[path].sha256
        )
        return DiffSummary(new_on_server=new_on_server, new_local=new_local, changed=changed)

    def _prompt_thread_update_mode(
        self,
        slug: str,
        diff: DiffSummary,
        thread_summaries: list[ThreadSummary],
    ) -> str:
        typer_message = (
            f"Server has updates for '{slug}': "
            f"{len(diff.thread_updates)} thread(s), "
            f"{len(diff.new_on_server)} new server doc(s), "
            f"{len(diff.new_local)} local-only doc(s), "
            f"{len(diff.changed)} changed doc(s). "
            "Type 'update', 'select', or 'abort': "
        )
        while True:
            choice = input(typer_message).strip().lower()
            if choice in {"update", "select", "abort"}:
                return choice

    def _select_server_thread_checkpoints(
        self,
        slug: str,
        checkpoints: list[ThreadCheckpoint],
        *,
        assume_yes: bool,
        diff: DiffSummary,
    ) -> list[ThreadCheckpoint] | None:
        thread_summaries = [
            self._summary_from_checkpoint(checkpoint)
            for checkpoint in checkpoints
            if checkpoint.thread_id
        ]
        if not thread_summaries:
            if diff.has_mismatch and not assume_yes:
                message = (
                    "Server has updates. Apply them now? "
                    f"new_on_server={len(diff.new_on_server)}, new_local={len(diff.new_local)}, changed={len(diff.changed)}"
                )
                if input(f"{message} [y/N]: ").strip().lower() not in {"y", "yes"}:
                    return None
            return checkpoints
        if assume_yes:
            return checkpoints

        choice = self._prompt_thread_update_mode(slug, diff, thread_summaries)
        if choice == "abort":
            return None
        if choice == "update":
            return checkpoints

        selected_ids: set[str] = set()
        for summary in sorted(thread_summaries, key=lambda item: (item.updated_at, item.thread_name), reverse=True):
            prompt = (
                f"Overwrite local thread '{summary.thread_name}' "
                f"({summary.thread_id}, last updated {summary.updated_at.isoformat()})? [y/N]: "
            )
            if input(prompt).strip().lower() in {"y", "yes"}:
                selected_ids.add(summary.thread_id)
        return [checkpoint for checkpoint in checkpoints if checkpoint.thread_id in selected_ids]

    def update_from_server(self, slug: str, *, assume_yes: bool = False) -> DiffSummary:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        self.report_progress(f"Connecting to the server for '{slug}'...")
        managed_root = Path(local_state.managed_root)
        server_state = self.api_client().pull_state(slug)
        self._adopt_server_name(local_state, server_state.manifest.name)
        self.report_progress(f"Comparing local Markdown for '{slug}' with the server copy...")
        server_by_path = {
            document.record.relative_path: document.record
            for document in server_state.managed_documents
        }
        local_documents, _ = build_managed_documents(managed_root, local_state.managed_file_ids)
        local_by_path = {document.record.relative_path: document.record for document in local_documents}
        diff = DiffSummary(
            new_on_server=sorted(set(server_by_path) - set(local_by_path)),
            new_local=sorted(set(local_by_path) - set(server_by_path)),
            changed=sorted(
                path
                for path in set(server_by_path) & set(local_by_path)
                if server_by_path[path].sha256 != local_by_path[path].sha256
            ),
        )
        checkpoints_with_raw_bundles = self._newer_thread_checkpoints(local_state, server_state)
        diff.thread_updates = self._dedupe_thread_ids(
            [checkpoint.thread_id for checkpoint in checkpoints_with_raw_bundles if checkpoint.thread_id]
        )
        shared_checkpoint = getattr(server_state, "shared_checkpoint", None)
        apply_shared_checkpoint = (
            shared_checkpoint is not None
            and shared_checkpoint.shared_bundle is not None
            and self._checkpoint_needs_local_refresh(local_state, shared_checkpoint)
        )
        selected_checkpoints = self._select_server_thread_checkpoints(
            slug,
            checkpoints_with_raw_bundles,
            assume_yes=assume_yes,
            diff=diff,
        )
        if selected_checkpoints is None:
            self.report_progress(f"Update from server aborted for '{slug}'.")
            return diff
        self.report_progress(f"Applying server updates for '{slug}'...")
        quarantine_root = self.state_store.paths.cache_dir / "quarantine" / slug / utc_now().strftime("%Y%m%d%H%M%S")
        for path in diff.new_local:
            source = managed_root / path
            if source.exists():
                target = quarantine_root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read_bytes())
                source.unlink()
        for document in server_state.managed_documents:
            target = managed_root / document.record.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            atomic_write_text(target, document.content)
        self.report_progress(f"Syncing shared skills for '{slug}'...")
        self._write_shared_skills(server_state.shared_skills)
        if apply_shared_checkpoint:
            self.report_progress(f"Applying the shared Codex runtime bundle for '{slug}'...")
            self._apply_shared_bundle(shared_checkpoint.shared_bundle)
            self._record_thread_revision(slug, shared_checkpoint, shared_checkpoint.revision)
            local_state = self._get_superproject_state(slug)
        if selected_checkpoints:
            if len(selected_checkpoints) == 1:
                self.report_progress(f"Applying the latest Codex session bundle for '{slug}'...")
            else:
                self.report_progress(
                    f"Applying {len(selected_checkpoints)} Codex session bundles for '{slug}'..."
                )
            refreshed_session_ids: list[str] = []
            for checkpoint in selected_checkpoints:
                self._apply_raw_bundle(checkpoint.raw_bundle)
                session_ids = self._checkpoint_session_ids(checkpoint)
                refreshed_session_ids.extend(session_ids)
                self._record_thread_revision(slug, checkpoint, checkpoint.revision)
            thread_labels = self._format_thread_labels(refreshed_session_ids)
            if refreshed_session_ids:
                self.report_progress(f"Updated Codex thread(s) for '{slug}': {thread_labels}.")
            else:
                self.report_progress(f"Updated {thread_labels} for '{slug}'.")
            local_state = self._get_superproject_state(slug)
        local_state.last_alignment_action = AlignmentAction.UPDATE_FROM_SERVER
        local_state.last_aligned_revision = server_state.manifest.revision
        server_file_ids = {
            document.record.relative_path: document.record.file_id
            for document in server_state.managed_documents
        }
        local_documents, updated_ids = build_managed_documents(managed_root, server_file_ids)
        local_state.managed_file_ids = updated_ids
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)
        self.report_progress(f"Update from server finished for '{slug}'.")
        return diff

    @staticmethod
    def _can_skip_locked_raw_artifact(relative_path: str) -> bool:
        return (
            relative_path == "session_index.jsonl"
            or relative_path.startswith("sessions/")
            or relative_path.endswith((".sqlite", ".sqlite-shm", ".sqlite-wal"))
        )

    def _apply_bundle_artifacts(self, artifacts) -> None:
        for artifact in artifacts:
            target = self.codex_root / artifact.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                atomic_write_bytes(target, decode_b64(artifact.content_b64))
            except (OSError, PermissionError):
                if self._can_skip_locked_raw_artifact(artifact.relative_path):
                    continue
                raise

    def _apply_raw_bundle(self, bundle) -> None:
        self._apply_bundle_artifacts(bundle.files)

    def _apply_shared_bundle(self, bundle: RawCodexSharedBundle) -> None:
        self._apply_bundle_artifacts(bundle.files)

    def prepare_live_checkpoint_inputs(
        self,
        slug: str,
        *,
        show_progress: bool = True,
    ) -> PreparedCheckpointInputs:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        if show_progress:
            self.report_progress(f"Scanning managed Markdown for '{slug}'...")
        managed_root = Path(local_state.managed_root)
        documents, updated_ids = build_managed_documents(managed_root, local_state.managed_file_ids)
        local_state.managed_file_ids = updated_ids
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)
        if show_progress:
            self.report_progress(f"Loading the current server manifest for '{slug}'...")
        manifest = self._load_manifest(self.api_client(), slug)
        incoming_manifest = manifest.model_copy(
            update={
                "managed_files": [document.record for document in documents],
            }
        )
        workspace_roots = [Path(path) for path in local_state.workspace_roots if Path(path).exists()]
        shared_bundle = build_shared_codex_bundle(self.codex_root)
        return PreparedCheckpointInputs(
            local_state=local_state,
            incoming_manifest=incoming_manifest,
            base_revision=manifest.revision,
            workspace_roots=workspace_roots,
            documents=documents,
            shared_bundle=shared_bundle,
        )

    def _build_checkpoint_from_inputs(
        self,
        slug: str,
        *,
        prepared: PreparedCheckpointInputs,
        canonical: bool,
        show_progress: bool = True,
        thread_id: str | None = None,
    ) -> ThreadCheckpoint:
        raw_bundle = None
        shared_bundle = None
        turn_hashes: list[str] = []
        if thread_id is not None:
            if show_progress:
                self.report_progress(f"Capturing Codex session artifacts for '{slug}'...")
            raw_bundle = build_raw_session_bundle(self.codex_root, prepared.workspace_roots, thread_id=thread_id)
            session_files = [
                self.codex_root / artifact.relative_path
                for artifact in raw_bundle.files
                if artifact.relative_path.startswith("sessions/")
            ]
            turn_hashes = extract_turn_hashes([path for path in session_files if path.exists()])
        else:
            shared_bundle = prepared.shared_bundle
        if show_progress:
            self.report_progress(f"Computing the checkpoint summary for '{slug}'...")
        snapshot_hash = sha256_text(
            json.dumps(
                {
                    "documents": (
                        [document.record.model_dump(mode="json") for document in prepared.documents]
                        if thread_id is None
                        else []
                    ),
                    "raw_files": self._stable_thread_snapshot_entries(raw_bundle) if raw_bundle else [],
                    "shared_files": self._stable_shared_snapshot_entries(shared_bundle) if shared_bundle else [],
                    "turn_hashes": turn_hashes,
                    "thread_id": thread_id,
                },
                sort_keys=True,
            )
        )
        if raw_bundle is not None:
            summary = (
                f"Synced {len(raw_bundle.files)} thread Codex artifacts for "
                f"{raw_bundle.thread_name or thread_id}."
            )
        else:
            summary = (
                f"Synced {len(prepared.documents)} managed Markdown files and "
                f"{len(shared_bundle.files) if shared_bundle else 0} shared Codex artifacts."
            )
        return ThreadCheckpoint(
            superproject_slug=slug,
            thread_id=thread_id or (raw_bundle.thread_id if raw_bundle else None),
            revision=prepared.incoming_manifest.revision,
            created_at=utc_now(),
            source_device_id=self.config().device_id or "unknown-device",
            canonical=canonical,
            base_revision=prepared.base_revision,
            turn_hashes=turn_hashes,
            summary=summary,
            manifest=prepared.incoming_manifest,
            managed_documents=prepared.documents if thread_id is None else [],
            raw_bundle=raw_bundle,
            shared_bundle=shared_bundle,
            snapshot_hash=snapshot_hash,
        )

    def build_checkpoint(
        self,
        slug: str,
        *,
        canonical: bool,
        show_progress: bool = True,
        thread_id: str | None = None,
    ) -> ThreadCheckpoint:
        prepared = self.prepare_live_checkpoint_inputs(slug, show_progress=show_progress)
        return self._build_checkpoint_from_inputs(
            slug,
            prepared=prepared,
            canonical=canonical,
            show_progress=show_progress,
            thread_id=thread_id,
        )

    def build_live_checkpoints(
        self,
        slug: str,
        *,
        canonical: bool,
        show_progress: bool = True,
        prepared: PreparedCheckpointInputs | None = None,
    ) -> list[ThreadCheckpoint]:
        prepared_inputs = prepared or self.prepare_live_checkpoint_inputs(slug, show_progress=show_progress)
        shared_checkpoint = self._build_checkpoint_from_inputs(
            slug,
            prepared=prepared_inputs,
            canonical=canonical,
            show_progress=show_progress,
            thread_id=None,
        )
        tracked_thread_ids = self._tracked_thread_ids(slug)
        if not tracked_thread_ids:
            return [shared_checkpoint]
        return [
            shared_checkpoint,
            *[
                self._build_checkpoint_from_inputs(
                    slug,
                    prepared=prepared_inputs,
                    canonical=canonical,
                    show_progress=show_progress,
                    thread_id=thread_id,
                )
                for thread_id in tracked_thread_ids
            ],
        ]

    def enqueue_checkpoint(self, checkpoint: ThreadCheckpoint) -> None:
        queue = self.state_store.load_queue()
        queue.append(
            OutboundQueueItem(
                superproject_slug=checkpoint.superproject_slug,
                created_at=utc_now(),
                checkpoint=checkpoint,
            )
        )
        self.state_store.save_queue(queue)

    def flush_outbound_queue(
        self,
        api: ApiClient | None = None,
        *,
        heartbeat: Callable[[ApiClient], bool] | None = None,
    ) -> bool:
        client = api or self.api_client()
        queue = self.state_store.load_queue()
        if queue:
            self.report_progress(f"Retrying {len(queue)} queued checkpoint(s)...")
        remaining: list[OutboundQueueItem] = []
        for index, item in enumerate(queue):
            if heartbeat is not None and not heartbeat(client):
                remaining.extend(queue[index:])
                self.state_store.save_queue(remaining)
                return False
            try:
                response = client.push_checkpoint(
                    item.superproject_slug,
                    PushCheckpointRequest(checkpoint=item.checkpoint),
                )
                try:
                    self._record_thread_revision(
                        item.superproject_slug,
                        item.checkpoint,
                        response.revision,
                    )
                except Exception:
                    pass
            except Exception:
                remaining.append(item)
            if heartbeat is not None and not heartbeat(client):
                remaining.extend(queue[index + 1 :])
                self.state_store.save_queue(remaining)
                return False
        self.state_store.save_queue(remaining)
        return True

    def force_thread_updates(self, slug: str, *, steal: bool = False) -> list[dict[str, Any]]:
        config = self.config()
        if config.sync_active_superproject:
            raise RuntimeError("turn-off-sync before force-thread-updates.")
        tracked_thread_ids = self._tracked_thread_ids(slug)
        if not tracked_thread_ids:
            raise RuntimeError(f"No tracked threads are configured for '{slug}'.")

        api = self.api_client()
        self.report_progress(f"Acquiring the global live-sync lease for a forced thread push to '{slug}'...")
        lease = api.acquire_lease(steal=steal)
        if not lease.granted:
            raise RuntimeError(
                f"Another device currently holds the active lease: {lease.conflict_device_id}"
            )

        pushed: list[dict[str, Any]] = []
        try:
            prepared = self.prepare_live_checkpoint_inputs(slug, show_progress=False)
            checkpoints = self.build_live_checkpoints(
                slug,
                canonical=True,
                show_progress=False,
                prepared=prepared,
            )
            for checkpoint in checkpoints:
                if checkpoint.thread_id is None:
                    continue
                thread_labels = self._format_thread_labels(
                    self._checkpoint_session_ids(checkpoint),
                    checkpoint,
                )
                self.report_progress(f"Force-pushing tracked thread(s) for '{slug}': {thread_labels}.")
                response = api.push_checkpoint(
                    slug,
                    PushCheckpointRequest(checkpoint=checkpoint),
                )
                self._record_thread_revision(slug, checkpoint, response.revision)
                pushed.append(
                    {
                        "thread_id": checkpoint.thread_id,
                        "thread_name": (
                            checkpoint.raw_bundle.thread_name if checkpoint.raw_bundle else checkpoint.thread_id
                        ),
                        "revision": response.revision,
                    }
                )
        finally:
            api.release_lease()
        return pushed

    def override_current_state(
        self,
        slug: str,
        *,
        thread_id: str | None = None,
        assume_yes: bool = False,
    ) -> ThreadCheckpoint:
        config = self.config()
        if config.sync_active_superproject:
            raise RuntimeError("turn-off-sync before overriding current state.")
        if not assume_yes:
            if input("Override server state with this machine's version? [y/N]: ").strip().lower() not in {"y", "yes"}:
                raise RuntimeError("Override aborted by user.")
        checkpoint = self.build_checkpoint(slug, canonical=True, thread_id=thread_id)
        self.api_client().override_state(slug, PushCheckpointRequest(checkpoint=checkpoint, override=True))
        local_state = self._get_superproject_state(slug)
        local_state.last_alignment_action = AlignmentAction.OVERRIDE_CURRENT_STATE
        if checkpoint.thread_id is None:
            local_state.last_shared_bundle_revision = max(
                local_state.last_shared_bundle_revision,
                checkpoint.base_revision + 1,
            )
            local_state.last_aligned_revision = checkpoint.base_revision + 1
        config.superprojects[slug] = local_state
        self.save_config(config)
        return checkpoint

    def mark_sync_inactive(self) -> None:
        config = self.config()
        config.sync_active_superproject = None
        self.save_config(config)

    def turn_on_sync(self, slug: str, *, steal: bool = False) -> str:
        local_state = self._get_superproject_state(slug)
        self.report_progress(f"Checking whether '{slug}' is aligned with the server...")
        diff = self.compare_with_server(slug)
        if diff.has_mismatch and local_state.last_alignment_action == AlignmentAction.NONE:
            raise RuntimeError(
                "Local state does not match the server. Run update-from-server or override-current-state first."
            )
        self.report_progress("Acquiring the global live-sync lease from the server...")
        lease = self.api_client().acquire_lease(steal=steal)
        if not lease.granted:
            raise RuntimeError(
                f"Another device currently holds the active lease: {lease.conflict_device_id}"
            )
        config = self.config()
        config.sync_active_superproject = slug
        self.save_config(config)
        self.worker = SyncWorker(self, slug)
        self.worker.start()
        self.report_progress(f"Live sync started for '{slug}'.")
        return slug

    def turn_off_sync(self) -> None:
        if self.worker is not None:
            self.worker.stop()
            self.worker.join(timeout=5)
            self.worker = None
        try:
            self.api_client().release_lease()
        finally:
            self.mark_sync_inactive()

    def refresh_thread(self, slug: str, thread_id: str) -> None:
        checkpoint = self.api_client().get_thread_checkpoint(slug, thread_id)
        if checkpoint.raw_bundle is None:
            raise RuntimeError("Requested thread does not have a raw session bundle on the server.")
        self._apply_raw_bundle(checkpoint.raw_bundle)
        self._record_thread_revision(slug, checkpoint, checkpoint.revision)

    def disconnect_superproject(self, slug: str, *, wipe_managed_root: bool = True) -> dict[str, Any]:
        config = self.config()
        local_state = config.superprojects.get(slug)
        if local_state is None:
            raise KeyError(f"Unknown local superproject: {slug}")

        if config.sync_active_superproject == slug:
            self.turn_off_sync()
            config = self.config()
            local_state = config.superprojects.get(slug)
            if local_state is None:
                raise KeyError(f"Unknown local superproject: {slug}")

        managed_root = Path(local_state.managed_root) if local_state.managed_root else None
        managed_root_deleted = False
        if wipe_managed_root and managed_root and managed_root.exists():
            shutil.rmtree(managed_root, ignore_errors=False)
            managed_root_deleted = True

        quarantine_root = self.state_store.paths.cache_dir / "quarantine" / slug
        if quarantine_root.exists():
            shutil.rmtree(quarantine_root, ignore_errors=False)

        queue = [item for item in self.state_store.load_queue() if item.superproject_slug != slug]
        self.state_store.save_queue(queue)

        del config.superprojects[slug]
        if config.sync_active_superproject == slug:
            config.sync_active_superproject = None
        self.save_config(config)

        return {
            "slug": slug,
            "managed_root_deleted": managed_root_deleted,
            "managed_root": str(managed_root) if managed_root else None,
        }

    def delete_superproject_from_server(self, slug: str, *, force: bool = False) -> dict[str, Any]:
        config = self.config()
        if config.sync_active_superproject == slug:
            self.turn_off_sync()
        return self.api_client().delete_superproject(slug, force=force)
