from __future__ import annotations

import json
import shlex
import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import paramiko

from cws.client.api import ApiClient
from cws.client.codex import build_managed_documents, build_raw_session_bundle, extract_turn_hashes
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
    SubprojectRecord,
    ThreadCheckpoint,
)
from cws.utils import atomic_write_bytes, atomic_write_text, decode_b64, dump_json_file, sha256_text, slugify, utc_now


@dataclass
class DiffSummary:
    new_on_server: list[str]
    new_local: list[str]
    changed: list[str]

    @property
    def has_mismatch(self) -> bool:
        return bool(self.new_on_server or self.new_local or self.changed)


class SyncWorker(threading.Thread):
    def __init__(self, service: "ClientService", superproject_slug: str) -> None:
        super().__init__(daemon=True)
        self.service = service
        self.superproject_slug = superproject_slug
        self.stop_event = threading.Event()
        self.pending_checkpoint: ThreadCheckpoint | None = None
        self.last_pushed_hash: str | None = None

    def stop(self) -> None:
        self.stop_event.set()

    def run(self) -> None:  # pragma: no cover - exercised by integration flow
        api = self.service.api_client()
        while not self.stop_event.is_set():
            self.service.flush_outbound_queue(api)
            heartbeat = api.heartbeat()
            if not heartbeat.accepted:
                self.service.mark_sync_inactive()
                return
            checkpoint = self.service.build_checkpoint(self.superproject_slug, canonical=True)
            if checkpoint.snapshot_hash == self.last_pushed_hash:
                time.sleep(self.service.heartbeat_interval_seconds)
                continue
            if self.pending_checkpoint is None:
                self.pending_checkpoint = checkpoint
            elif self.pending_checkpoint.snapshot_hash == checkpoint.snapshot_hash:
                try:
                    api.push_checkpoint(
                        self.superproject_slug,
                        PushCheckpointRequest(checkpoint=checkpoint),
                    )
                    self.last_pushed_hash = checkpoint.snapshot_hash
                except Exception:
                    self.service.enqueue_checkpoint(checkpoint)
                self.pending_checkpoint = None
            else:
                scratch = self.pending_checkpoint.model_copy(update={"canonical": False})
                try:
                    api.push_checkpoint(
                        self.superproject_slug,
                        PushCheckpointRequest(checkpoint=scratch),
                    )
                except Exception:
                    pass
                self.pending_checkpoint = checkpoint
            time.sleep(self.service.heartbeat_interval_seconds)
        self.service.mark_sync_inactive()


class ClientService:
    heartbeat_interval_seconds = 15

    def __init__(self, state_store: ClientStateStore | None = None, codex_root: Path | None = None) -> None:
        self.state_store = state_store or ClientStateStore()
        self.codex_root = codex_root or (Path.home() / ".codex")
        self.worker: SyncWorker | None = None

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
        local_state.name = server_state.manifest.name
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

    def compare_with_server(self, slug: str) -> DiffSummary:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        managed_root = Path(local_state.managed_root)
        state = self.api_client().pull_state(slug)
        local_documents, _ = build_managed_documents(managed_root, local_state.managed_file_ids)
        server_by_path = {document.record.relative_path: document.record for document in state.managed_documents}
        local_by_path = {document.record.relative_path: document.record for document in local_documents}
        new_on_server = sorted(set(server_by_path) - set(local_by_path))
        new_local = sorted(set(local_by_path) - set(server_by_path))
        changed = sorted(
            path
            for path in set(server_by_path) & set(local_by_path)
            if server_by_path[path].sha256 != local_by_path[path].sha256
        )
        return DiffSummary(new_on_server=new_on_server, new_local=new_local, changed=changed)

    def update_from_server(self, slug: str, *, assume_yes: bool = False) -> DiffSummary:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        managed_root = Path(local_state.managed_root)
        server_state = self.api_client().pull_state(slug)
        diff = self.compare_with_server(slug)
        if diff.has_mismatch and not assume_yes:
            message = (
                "Server has updates. Apply them now? "
                f"new_on_server={len(diff.new_on_server)}, new_local={len(diff.new_local)}, changed={len(diff.changed)}"
            )
            if input(f"{message} [y/N]: ").strip().lower() not in {"y", "yes"}:
                return diff
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
        self._write_shared_skills(server_state.shared_skills)
        if server_state.latest_checkpoint and server_state.latest_checkpoint.raw_bundle:
            self._apply_raw_bundle(server_state.latest_checkpoint.raw_bundle)
            if server_state.latest_checkpoint.thread_id:
                local_state.pending_thread_refreshes[server_state.latest_checkpoint.thread_id] = (
                    server_state.latest_checkpoint.revision
                )
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
        return diff

    @staticmethod
    def _can_skip_locked_raw_artifact(relative_path: str) -> bool:
        return (
            relative_path == "session_index.jsonl"
            or relative_path.startswith("sessions/")
            or relative_path.endswith((".sqlite", ".sqlite-shm", ".sqlite-wal"))
        )

    def _apply_raw_bundle(self, bundle) -> None:
        for artifact in bundle.files:
            target = self.codex_root / artifact.relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                atomic_write_bytes(target, decode_b64(artifact.content_b64))
            except (OSError, PermissionError):
                if self._can_skip_locked_raw_artifact(artifact.relative_path):
                    continue
                raise

    def build_checkpoint(self, slug: str, *, canonical: bool) -> ThreadCheckpoint:
        local_state = self._get_superproject_state(slug)
        if not local_state.managed_root:
            raise RuntimeError("Managed root is not configured for this superproject.")
        managed_root = Path(local_state.managed_root)
        documents, updated_ids = build_managed_documents(managed_root, local_state.managed_file_ids)
        local_state.managed_file_ids = updated_ids
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)
        manifest = self.api_client().pull_state(slug).manifest
        incoming_manifest = manifest.model_copy(
            update={
                "managed_files": [document.record for document in documents],
            }
        )
        workspace_roots = [Path(path) for path in local_state.workspace_roots if Path(path).exists()]
        raw_bundle = build_raw_session_bundle(self.codex_root, workspace_roots)
        session_files = [
            self.codex_root / artifact.relative_path
            for artifact in raw_bundle.files
            if artifact.relative_path.startswith("sessions/")
        ]
        turn_hashes = extract_turn_hashes([path for path in session_files if path.exists()])
        snapshot_hash = sha256_text(
            json.dumps(
                {
                    "documents": [document.record.model_dump(mode="json") for document in documents],
                    "raw_files": [artifact.sha256 for artifact in raw_bundle.files],
                    "turn_hashes": turn_hashes,
                },
                sort_keys=True,
            )
        )
        return ThreadCheckpoint(
            superproject_slug=slug,
            thread_id=raw_bundle.thread_id,
            revision=incoming_manifest.revision,
            created_at=utc_now(),
            source_device_id=self.config().device_id or "unknown-device",
            canonical=canonical,
            base_revision=manifest.revision,
            turn_hashes=turn_hashes,
            summary=f"Synced {len(documents)} managed Markdown files and {len(raw_bundle.files)} Codex artifacts.",
            manifest=incoming_manifest,
            managed_documents=documents,
            raw_bundle=raw_bundle,
            snapshot_hash=snapshot_hash,
        )

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

    def flush_outbound_queue(self, api: ApiClient | None = None) -> None:
        client = api or self.api_client()
        queue = self.state_store.load_queue()
        remaining: list[OutboundQueueItem] = []
        for item in queue:
            try:
                client.push_checkpoint(
                    item.superproject_slug,
                    PushCheckpointRequest(checkpoint=item.checkpoint),
                )
            except Exception:
                remaining.append(item)
        self.state_store.save_queue(remaining)

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
        checkpoint = self.build_checkpoint(slug, canonical=True)
        if thread_id:
            checkpoint = checkpoint.model_copy(update={"thread_id": thread_id})
        self.api_client().override_state(slug, PushCheckpointRequest(checkpoint=checkpoint, override=True))
        local_state = self._get_superproject_state(slug)
        local_state.last_alignment_action = AlignmentAction.OVERRIDE_CURRENT_STATE
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
        diff = self.compare_with_server(slug)
        if diff.has_mismatch and local_state.last_alignment_action == AlignmentAction.NONE:
            raise RuntimeError(
                "Local state does not match the server. Run update-from-server or override-current-state first."
            )
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
        local_state = self._get_superproject_state(slug)
        local_state.pending_thread_refreshes[thread_id] = checkpoint.revision
        config = self.config()
        config.superprojects[slug] = local_state
        self.save_config(config)

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
