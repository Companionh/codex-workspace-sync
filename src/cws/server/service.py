from __future__ import annotations

import json
import os
import secrets
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from string import Template
from typing import Any
from uuid import uuid4

from cws.config import ServerPaths
from cws.models import (
    AcquireLeaseRequest,
    AcquireLeaseResponse,
    BackupRecord,
    CreateSuperprojectRequest,
    CreateSuperprojectResponse,
    DeviceRecord,
    HeartbeatResponse,
    LeaseRecord,
    LeaseState,
    ManagedDocument,
    ManagedFileClass,
    ManagedFileRecord,
    MismatchResolution,
    PullStateResponse,
    PushCheckpointRequest,
    PushCheckpointResponse,
    RawFileArtifact,
    RegisterDeviceRequest,
    RegisterDeviceResponse,
    SuperprojectManifest,
    ThreadSummary,
    ThreadCheckpoint,
)
from cws.server.db import ServerDatabase
from cws.server.security import hash_secret, verify_secret
from cws.utils import (
    atomic_write_text,
    decode_b64,
    dump_json_file,
    encode_b64,
    relative_posix,
    sha256_file,
    sha256_text,
    slugify,
    utc_now,
)


class ServerService:
    heartbeat_timeout_seconds = int(os.environ.get("CWS_HEARTBEAT_TIMEOUT_SECONDS", "120"))

    def __init__(self, paths: ServerPaths | None = None) -> None:
        if paths is None:
            repo_root = Path(__file__).resolve().parents[3]
            paths = ServerPaths.default(app_root=repo_root, state_root=repo_root / "state")
        self.paths = paths
        self.db = ServerDatabase(self.paths.db_file)
        self.db.init_schema()
        self.paths.state_root.mkdir(parents=True, exist_ok=True)

    def init_state(self, bootstrap_passphrase: str) -> None:
        self.db.set_config("bootstrap_passphrase_hash", hash_secret(bootstrap_passphrase))

    def validate_bootstrap_passphrase(self, candidate: str) -> bool:
        stored = self.db.get_config("bootstrap_passphrase_hash")
        if stored is None:
            raise RuntimeError("Server bootstrap passphrase is not configured.")
        return verify_secret(stored, candidate)

    def register_device(self, request: RegisterDeviceRequest) -> RegisterDeviceResponse:
        if not self.validate_bootstrap_passphrase(request.secondary_passphrase):
            raise PermissionError("Secondary passphrase is invalid.")
        device_secret = secrets.token_urlsafe(32)
        device = DeviceRecord(
            device_id=str(uuid4()),
            device_name=request.device_name,
            created_at=utc_now(),
            metadata=request.metadata,
        )
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO devices (
                    device_id,
                    device_name,
                    secret_hash,
                    created_at,
                    status,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    device.device_id,
                    device.device_name,
                    hash_secret(device_secret),
                    device.created_at.isoformat(),
                    device.status,
                    json.dumps(device.metadata),
                ),
            )
            connection.commit()
        return RegisterDeviceResponse(device=device, device_secret=device_secret)

    def authenticate_device(self, device_id: str, device_secret: str) -> DeviceRecord:
        with self.db.connect() as connection:
            row = connection.execute(
                """
                SELECT device_id, device_name, secret_hash, created_at, status, metadata_json
                FROM devices
                WHERE device_id = ?
                """,
                (device_id,),
            ).fetchone()
        if row is None:
            raise PermissionError("Unknown device.")
        if not verify_secret(row["secret_hash"], device_secret):
            raise PermissionError("Invalid device secret.")
        return DeviceRecord(
            device_id=row["device_id"],
            device_name=row["device_name"],
            created_at=datetime.fromisoformat(row["created_at"]),
            status=row["status"],
            metadata=json.loads(row["metadata_json"]),
        )

    def _load_lease(self) -> LeaseRecord:
        with self.db.connect() as connection:
            row = connection.execute(
                """
                SELECT resource_id, device_id, acquired_at, last_heartbeat_at, heartbeat_timeout_seconds
                FROM leases WHERE resource_id = 'global'
                """
            ).fetchone()
        if row is None:
            return LeaseRecord(heartbeat_timeout_seconds=self.heartbeat_timeout_seconds)
        lease = LeaseRecord(
            resource_id=row["resource_id"],
            device_id=row["device_id"],
            acquired_at=datetime.fromisoformat(row["acquired_at"]) if row["acquired_at"] else None,
            last_heartbeat_at=datetime.fromisoformat(row["last_heartbeat_at"])
            if row["last_heartbeat_at"]
            else None,
            heartbeat_timeout_seconds=row["heartbeat_timeout_seconds"],
            state=LeaseState.ACTIVE if row["device_id"] else LeaseState.AVAILABLE,
        )
        if lease.device_id and lease.last_heartbeat_at:
            expiration = lease.last_heartbeat_at + timedelta(seconds=lease.heartbeat_timeout_seconds)
            if expiration <= utc_now():
                expired = LeaseRecord(
                    state=LeaseState.EXPIRED,
                    heartbeat_timeout_seconds=lease.heartbeat_timeout_seconds,
                )
                self._write_lease(expired)
                return expired
        return lease

    def _write_lease(self, lease: LeaseRecord) -> None:
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO leases (
                    resource_id,
                    device_id,
                    acquired_at,
                    last_heartbeat_at,
                    heartbeat_timeout_seconds
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(resource_id) DO UPDATE SET
                    device_id = excluded.device_id,
                    acquired_at = excluded.acquired_at,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    heartbeat_timeout_seconds = excluded.heartbeat_timeout_seconds
                """,
                (
                    lease.resource_id,
                    lease.device_id,
                    lease.acquired_at.isoformat() if lease.acquired_at else None,
                    lease.last_heartbeat_at.isoformat() if lease.last_heartbeat_at else None,
                    lease.heartbeat_timeout_seconds,
                ),
            )
            connection.commit()

    def acquire_lease(self, request: AcquireLeaseRequest) -> AcquireLeaseResponse:
        lease = self._load_lease()
        if lease.device_id and lease.device_id != request.device_id and not request.steal:
            lease.state = LeaseState.ACTIVE
            return AcquireLeaseResponse(
                lease=lease,
                granted=False,
                conflict_device_id=lease.device_id,
            )
        now = utc_now()
        lease.device_id = request.device_id
        lease.acquired_at = now
        lease.last_heartbeat_at = now
        lease.heartbeat_timeout_seconds = self.heartbeat_timeout_seconds
        lease.state = LeaseState.ACTIVE
        self._write_lease(lease)
        return AcquireLeaseResponse(lease=lease, granted=True)

    def heartbeat(self, device_id: str) -> HeartbeatResponse:
        lease = self._load_lease()
        if lease.device_id != device_id:
            return HeartbeatResponse(lease=lease, accepted=False)
        lease.last_heartbeat_at = utc_now()
        lease.state = LeaseState.ACTIVE
        self._write_lease(lease)
        return HeartbeatResponse(lease=lease, accepted=True)

    def release_lease(self, device_id: str, *, force: bool = False) -> LeaseRecord:
        lease = self._load_lease()
        if lease.device_id != device_id and not force:
            return lease
        lease.device_id = None
        lease.acquired_at = None
        lease.last_heartbeat_at = None
        lease.heartbeat_timeout_seconds = self.heartbeat_timeout_seconds
        lease.state = LeaseState.AVAILABLE
        self._write_lease(lease)
        return lease

    def _superproject_root(self, slug: str) -> Path:
        return self.paths.state_root / "superprojects" / slug

    def _render_template(self, template_name: str, context: dict[str, str]) -> str:
        template_path = Path(__file__).resolve().parents[1] / "templates" / template_name
        template = Template(template_path.read_text(encoding="utf-8"))
        return template.substitute(context)

    def _scaffold_manifest(
        self,
        slug: str,
        name: str,
        subprojects: list[Any],
    ) -> SuperprojectManifest:
        now = utc_now()
        root = self._superproject_root(slug)
        managed_files: list[ManagedFileRecord] = []
        for directory in (
            "baseline",
            "ecosystem",
            "subprojects",
            "threads",
            "generated",
            "manifests",
            "raw_codex",
            "backups",
        ):
            (root / directory).mkdir(parents=True, exist_ok=True)
        baseline_text = self._render_template(
            "baseline/base_rules.md.tmpl",
            {"superproject_name": name},
        )
        baseline_path = root / "baseline" / "base_rules.md"
        atomic_write_text(baseline_path, baseline_text)
        managed_files.append(
            ManagedFileRecord(
                relative_path="baseline/base_rules.md",
                sha256=sha256_text(baseline_text),
                size_bytes=len(baseline_text.encode("utf-8")),
                line_count=baseline_text.count("\n") + 1,
                classification=ManagedFileClass.PROTECTED,
            )
        )
        ecosystem_text = self._render_template(
            "ecosystem/sibling_repos.md.tmpl",
            {"superproject_name": name},
        )
        ecosystem_path = root / "ecosystem" / "sibling_repos.md"
        atomic_write_text(ecosystem_path, ecosystem_text)
        managed_files.append(
            ManagedFileRecord(
                relative_path="ecosystem/sibling_repos.md",
                sha256=sha256_text(ecosystem_text),
                size_bytes=len(ecosystem_text.encode("utf-8")),
                line_count=ecosystem_text.count("\n") + 1,
                classification=ManagedFileClass.PROTECTED,
            )
        )
        registry: list[dict[str, Any]] = []
        for subproject in subprojects:
            subproject_dir = root / "subprojects" / slugify(subproject.repo_name)
            subproject_dir.mkdir(parents=True, exist_ok=True)
            rules_text = self._render_template(
                "subproject/rules.md.tmpl",
                {
                    "superproject_name": name,
                    "subproject_name": subproject.repo_name,
                },
            )
            rules_path = subproject_dir / "rules.md"
            atomic_write_text(rules_path, rules_text)
            managed_files.append(
                ManagedFileRecord(
                    relative_path=relative_posix(rules_path, root),
                    sha256=sha256_text(rules_text),
                    size_bytes=len(rules_text.encode("utf-8")),
                    line_count=rules_text.count("\n") + 1,
                    classification=ManagedFileClass.NORMAL,
                )
            )
            registry.append(subproject.model_dump(mode="json"))
        dump_json_file(root / "subprojects" / "registry.json", registry)
        manifest = SuperprojectManifest(
            slug=slug,
            name=name,
            created_at=now,
            updated_at=now,
            revision=0,
            subprojects=subprojects,
            managed_files=managed_files,
        )
        dump_json_file(root / "manifests" / "current.json", manifest.model_dump(mode="json"))
        return manifest

    def create_superproject(self, request: CreateSuperprojectRequest) -> CreateSuperprojectResponse:
        manifest = self._scaffold_manifest(request.slug, request.name, request.subprojects)
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO superprojects (slug, name, created_at, updated_at, revision, manifest_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    manifest.slug,
                    manifest.name,
                    manifest.created_at.isoformat(),
                    manifest.updated_at.isoformat(),
                    manifest.revision,
                    json.dumps(manifest.model_dump(mode="json")),
                ),
            )
            connection.commit()
        return CreateSuperprojectResponse(manifest=manifest)

    def get_manifest(self, slug: str) -> SuperprojectManifest:
        with self.db.connect() as connection:
            row = connection.execute(
                "SELECT manifest_json FROM superprojects WHERE slug = ?",
                (slug,),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Unknown superproject: {slug}")
        return SuperprojectManifest.model_validate(json.loads(row["manifest_json"]))

    def _save_manifest(self, manifest: SuperprojectManifest) -> None:
        with self.db.connect() as connection:
            connection.execute(
                """
                UPDATE superprojects
                SET updated_at = ?, revision = ?, manifest_json = ?
                WHERE slug = ?
                """,
                (
                    manifest.updated_at.isoformat(),
                    manifest.revision,
                    json.dumps(manifest.model_dump(mode="json")),
                    manifest.slug,
                ),
            )
            connection.commit()
        dump_json_file(
            self._superproject_root(manifest.slug) / "manifests" / "current.json",
            manifest.model_dump(mode="json"),
        )

    def _latest_checkpoint(self, slug: str, thread_id: str | None = None) -> ThreadCheckpoint | None:
        query = """
            SELECT payload_json
            FROM checkpoints
            WHERE superproject_slug = ?
              AND canonical = 1
        """
        params: list[Any] = [slug]
        if thread_id is not None:
            query += " AND thread_id = ?"
            params.append(thread_id)
        query += " ORDER BY revision DESC LIMIT 1"
        with self.db.connect() as connection:
            row = connection.execute(query, tuple(params)).fetchone()
        if row is None:
            return None
        return ThreadCheckpoint.model_validate(json.loads(row["payload_json"]))

    def _latest_shared_checkpoint(self, slug: str) -> ThreadCheckpoint | None:
        query = """
            SELECT payload_json
            FROM checkpoints
            WHERE superproject_slug = ?
              AND canonical = 1
              AND thread_id IS NULL
            ORDER BY revision DESC
            LIMIT 1
        """
        with self.db.connect() as connection:
            row = connection.execute(query, (slug,)).fetchone()
        if row is None:
            return None
        return ThreadCheckpoint.model_validate(json.loads(row["payload_json"]))

    def _latest_thread_checkpoints(self, slug: str) -> list[ThreadCheckpoint]:
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM checkpoints
                WHERE superproject_slug = ?
                  AND canonical = 1
                ORDER BY revision DESC
                """,
                (slug,),
            ).fetchall()
        checkpoints: list[ThreadCheckpoint] = []
        seen_thread_ids: set[str] = set()
        for row in rows:
            checkpoint = ThreadCheckpoint.model_validate(json.loads(row["payload_json"]))
            thread_key = checkpoint.thread_id or ""
            if thread_key in seen_thread_ids:
                continue
            seen_thread_ids.add(thread_key)
            checkpoints.append(checkpoint)
        return sorted(checkpoints, key=lambda checkpoint: checkpoint.revision)

    def _pending_resolutions(self, slug: str) -> list[MismatchResolution]:
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json
                FROM mismatch_resolutions
                WHERE superproject_slug = ?
                ORDER BY created_at DESC
                """,
                (slug,),
            ).fetchall()
        return [MismatchResolution.model_validate(json.loads(row["payload_json"])) for row in rows]

    def _shared_skills(self) -> list[RawFileArtifact]:
        root = self.paths.shared_skills_root
        if not root.exists():
            return []
        artifacts: list[RawFileArtifact] = []
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            artifacts.append(
                RawFileArtifact(
                    relative_path=relative_posix(path, root),
                    sha256=sha256_file(path),
                    content_b64=encode_b64(path.read_bytes()),
                )
            )
        return artifacts

    def pull_state(self, slug: str) -> PullStateResponse:
        manifest = self.get_manifest(slug)
        return PullStateResponse(
            manifest=manifest,
            latest_checkpoint=self._latest_checkpoint(slug),
            shared_checkpoint=self._latest_shared_checkpoint(slug),
            thread_checkpoints=self._latest_thread_checkpoints(slug),
            pending_resolutions=self._pending_resolutions(slug),
            managed_documents=[
                ManagedDocument(
                    record=record,
                    content=(self._superproject_root(slug) / record.relative_path).read_text(encoding="utf-8"),
                )
                for record in manifest.managed_files
                if (self._superproject_root(slug) / record.relative_path).exists()
            ],
            shared_skills=self._shared_skills(),
        )

    @staticmethod
    def _clean_user_message(message: str) -> str | None:
        cleaned = message.strip()
        if not cleaned:
            return None

        if cleaned.startswith("<environment_context>"):
            marker = "</environment_context>"
            marker_index = cleaned.find(marker)
            if marker_index != -1:
                cleaned = cleaned[marker_index + len(marker) :].strip()
        if not cleaned:
            return None

        lines = cleaned.splitlines()
        for index, line in enumerate(lines):
            if line.strip() == "## My request for Codex:":
                request_lines = [value.rstrip() for value in lines[index + 1 :] if value.strip()]
                request_text = "\n".join(request_lines).strip()
                if request_text:
                    return request_text

        filtered_lines: list[str] = []
        skip_bullets = False
        boilerplate_headers = {
            "# Context from my IDE setup:",
            "## Open tabs:",
            "## Active file:",
        }
        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                continue
            if line in boilerplate_headers:
                skip_bullets = line == "## Open tabs:"
                continue
            if skip_bullets and line.startswith("- "):
                continue
            skip_bullets = False
            filtered_lines.append(line)
        fallback_text = "\n".join(filtered_lines).strip()
        return fallback_text or None

    def _thread_name_from_raw_bundle(self, checkpoint: ThreadCheckpoint) -> str | None:
        raw_bundle = checkpoint.raw_bundle
        thread_id = checkpoint.thread_id
        if raw_bundle is None or thread_id is None:
            return None
        for artifact in raw_bundle.files:
            if artifact.relative_path != "session_index.jsonl":
                continue
            content = decode_b64(artifact.content_b64).decode("utf-8", errors="ignore")
            for line in content.splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("id") != thread_id:
                    continue
                thread_name = payload.get("thread_name")
                if isinstance(thread_name, str) and thread_name.strip():
                    return thread_name.strip()
        return None

    def _preview_from_raw_bundle(self, checkpoint: ThreadCheckpoint) -> str | None:
        raw_bundle = checkpoint.raw_bundle
        thread_id = checkpoint.thread_id
        if raw_bundle is None:
            return None
        user_messages: list[str] = []
        for artifact in raw_bundle.files:
            if not artifact.relative_path.startswith("sessions/") or not artifact.relative_path.endswith(".jsonl"):
                continue
            content = decode_b64(artifact.content_b64).decode("utf-8", errors="ignore")
            session_matches = thread_id is None
            for line in content.splitlines():
                if not line.strip():
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("type") == "session_meta":
                    meta = payload.get("payload", {})
                    session_matches = thread_id is None or meta.get("id") == thread_id
                    continue
                if not session_matches or payload.get("type") != "event_msg":
                    continue
                message_payload = payload.get("payload", {})
                if message_payload.get("type") != "user_message":
                    continue
                message = str(message_payload.get("message", "")).strip()
                if not message:
                    continue
                cleaned = self._clean_user_message(message)
                if cleaned:
                    user_messages.append(cleaned)
        if not user_messages:
            return None
        lines = [line.strip() for line in user_messages[-1].splitlines() if line.strip()]
        if not lines:
            return None
        return "\n".join(lines[:2])

    def list_threads(self, slug: str) -> list[ThreadSummary]:
        summaries: list[ThreadSummary] = []
        for checkpoint in self._latest_thread_checkpoints(slug):
            if not checkpoint.thread_id:
                continue
            derived_name = self._thread_name_from_raw_bundle(checkpoint)
            derived_preview = self._preview_from_raw_bundle(checkpoint)
            summaries.append(
                ThreadSummary(
                    thread_id=checkpoint.thread_id,
                    thread_name=(
                        derived_name
                        or (checkpoint.raw_bundle.thread_name if checkpoint.raw_bundle else None)
                        or derived_preview
                        or checkpoint.summary
                        or checkpoint.thread_id
                    ),
                    updated_at=(
                        (checkpoint.raw_bundle.thread_updated_at if checkpoint.raw_bundle else None)
                        or checkpoint.created_at
                    ),
                    last_user_turn_preview=(
                        (checkpoint.raw_bundle.last_user_turn_preview if checkpoint.raw_bundle else None)
                        or derived_preview
                    ),
                    tracked=True,
                    source="server",
                )
            )
        return sorted(summaries, key=lambda item: (item.updated_at, item.thread_name, item.thread_id), reverse=True)

    def delete_superproject(self, slug: str, *, requesting_device_id: str, force: bool = False) -> dict[str, object]:
        manifest = self.get_manifest(slug)
        lease = self._load_lease()
        if lease.device_id and lease.device_id != requesting_device_id and not force:
            raise PermissionError(
                f"Another device currently holds the active lease: {lease.device_id}"
            )
        if lease.device_id and (lease.device_id == requesting_device_id or force):
            self.release_lease(lease.device_id, force=True)

        with self.db.connect() as connection:
            connection.execute("DELETE FROM checkpoints WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM mismatch_resolutions WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM backups WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM superprojects WHERE slug = ?", (slug,))
            connection.commit()

        root = self._superproject_root(slug)
        if root.exists():
            shutil.rmtree(root, ignore_errors=False)
        return {
            "deleted": True,
            "slug": slug,
            "name": manifest.name,
        }

    def get_thread_checkpoint(self, slug: str, thread_id: str) -> ThreadCheckpoint:
        checkpoint = self._latest_checkpoint(slug, thread_id)
        if checkpoint is None:
            raise FileNotFoundError(f"No checkpoint found for thread '{thread_id}'.")
        return checkpoint

    def _create_backup(self, slug: str, thread_id: str | None = None) -> BackupRecord:
        manifest = self.get_manifest(slug)
        latest_checkpoint = self._latest_checkpoint(slug, thread_id)
        snapshot = {
            "manifest": manifest.model_dump(mode="json"),
            "latest_checkpoint": latest_checkpoint.model_dump(mode="json") if latest_checkpoint else None,
        }
        backup = BackupRecord(
            superproject_slug=slug,
            thread_id=thread_id,
            created_at=utc_now(),
            snapshot=snapshot,
        )
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO backups (backup_id, superproject_slug, thread_id, created_at, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    backup.backup_id,
                    backup.superproject_slug,
                    backup.thread_id,
                    backup.created_at.isoformat(),
                    json.dumps(backup.model_dump(mode="json")),
                ),
            )
            connection.commit()
        dump_json_file(
            self._superproject_root(slug) / "backups" / f"{backup.backup_id}.json",
            backup.model_dump(mode="json"),
        )
        return backup

    def _reject_suspicious_manifest_change(
        self,
        current: SuperprojectManifest,
        incoming: SuperprojectManifest,
    ) -> None:
        current_by_id = {record.file_id: record for record in current.managed_files}
        incoming_ids = {record.file_id for record in incoming.managed_files}
        for record in current.managed_files:
            if (
                record.classification == ManagedFileClass.PROTECTED
                and record.file_id not in incoming_ids
            ):
                raise ValueError(
                    f"Protected file '{record.relative_path}' is missing from incoming manifest."
                )
        for record in incoming.managed_files:
            previous = current_by_id.get(record.file_id)
            if previous is None:
                continue
            if previous.relative_path != record.relative_path and previous.sha256 != record.sha256:
                raise ValueError(
                    f"Managed file '{previous.relative_path}' changed path and content together."
                )
            if previous.size_bytes > 0 and record.size_bytes <= max(16, previous.size_bytes // 5):
                raise ValueError(
                    f"Managed file '{record.relative_path}' shrank unexpectedly and was quarantined."
                )

    def _write_managed_document(self, superproject_slug: str, document: ManagedDocument) -> None:
        target_path = self._superproject_root(superproject_slug) / document.record.relative_path
        atomic_write_text(target_path, document.content)

    def push_checkpoint(
        self,
        device_id: str,
        request: PushCheckpointRequest,
    ) -> PushCheckpointResponse:
        lease = self._load_lease()
        if lease.device_id != device_id and not request.override:
            raise PermissionError("Device does not own the active lease.")
        current_manifest = self.get_manifest(request.checkpoint.superproject_slug)
        incoming_manifest = request.checkpoint.manifest
        self._reject_suspicious_manifest_change(current_manifest, incoming_manifest)
        backup = (
            self._create_backup(
                request.checkpoint.superproject_slug,
                request.checkpoint.thread_id,
            )
            if request.override
            else None
        )
        revision = current_manifest.revision + 1
        checkpoint = request.checkpoint.model_copy(
            update={
                "revision": revision,
                "base_revision": current_manifest.revision,
                "created_at": utc_now(),
            }
        )
        for document in checkpoint.managed_documents:
            self._write_managed_document(checkpoint.superproject_slug, document)
        if checkpoint.raw_bundle is not None:
            raw_root = self._superproject_root(checkpoint.superproject_slug) / "raw_codex"
            raw_root.mkdir(parents=True, exist_ok=True)
            dump_json_file(
                raw_root / f"{checkpoint.raw_bundle.bundle_id}.json",
                checkpoint.raw_bundle.model_dump(mode="json"),
            )
        if checkpoint.shared_bundle is not None:
            shared_root = self._superproject_root(checkpoint.superproject_slug) / "raw_codex" / "shared"
            shared_root.mkdir(parents=True, exist_ok=True)
            dump_json_file(
                shared_root / f"{checkpoint.shared_bundle.bundle_id}.json",
                checkpoint.shared_bundle.model_dump(mode="json"),
            )
        thread_dir = (
            self._superproject_root(checkpoint.superproject_slug)
            / "threads"
            / (checkpoint.thread_id or "default")
            / "checkpoints"
        )
        thread_dir.mkdir(parents=True, exist_ok=True)
        dump_json_file(thread_dir / f"{revision}.json", checkpoint.model_dump(mode="json"))
        updated_manifest = incoming_manifest.model_copy(
            update={
                "revision": revision,
                "updated_at": checkpoint.created_at,
                "managed_files": [
                    record.model_copy(update={"last_known_good_revision": revision})
                    for record in incoming_manifest.managed_files
                ],
            }
        )
        self._save_manifest(updated_manifest)
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO checkpoints (
                    checkpoint_id,
                    superproject_slug,
                    thread_id,
                    revision,
                    created_at,
                    source_device_id,
                    canonical,
                    base_revision,
                    turn_hashes_json,
                    snapshot_hash,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    checkpoint.checkpoint_id,
                    checkpoint.superproject_slug,
                    checkpoint.thread_id,
                    checkpoint.revision,
                    checkpoint.created_at.isoformat(),
                    checkpoint.source_device_id,
                    1 if checkpoint.canonical else 0,
                    checkpoint.base_revision,
                    json.dumps(checkpoint.turn_hashes),
                    checkpoint.snapshot_hash,
                    json.dumps(checkpoint.model_dump(mode="json")),
                ),
            )
            connection.commit()
        return PushCheckpointResponse(
            accepted=True,
            revision=revision,
            backup_id=backup.backup_id if backup else None,
        )

    def record_mismatch_resolution(self, resolution: MismatchResolution) -> MismatchResolution:
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO mismatch_resolutions (
                    resolution_id,
                    superproject_slug,
                    thread_id,
                    created_at,
                    chosen_source,
                    base_revision,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    resolution.resolution_id,
                    resolution.superproject_slug,
                    resolution.thread_id,
                    resolution.created_at.isoformat(),
                    resolution.chosen_source,
                    resolution.base_revision,
                    json.dumps(resolution.model_dump(mode="json")),
                ),
            )
            connection.commit()
        return resolution

    def restore_backup(self, slug: str, backup_id: str) -> BackupRecord:
        with self.db.connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM backups WHERE backup_id = ? AND superproject_slug = ?",
                (backup_id, slug),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Unknown backup: {backup_id}")
        backup = BackupRecord.model_validate(json.loads(row["payload_json"]))
        manifest = SuperprojectManifest.model_validate(backup.snapshot["manifest"])
        self._save_manifest(manifest)
        latest_checkpoint = backup.snapshot.get("latest_checkpoint")
        if latest_checkpoint is not None:
            checkpoint = ThreadCheckpoint.model_validate(latest_checkpoint)
            thread_dir = (
                self._superproject_root(slug)
                / "threads"
                / (checkpoint.thread_id or "default")
                / "checkpoints"
            )
            thread_dir.mkdir(parents=True, exist_ok=True)
            dump_json_file(thread_dir / f"{checkpoint.revision}.json", checkpoint.model_dump(mode="json"))
        return backup
