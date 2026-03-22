from __future__ import annotations

import json
import os
import secrets
import shutil
import sqlite3
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
    RawCodexSharedBundle,
    RawFileArtifact,
    RawSessionBundle,
    RegisterDeviceRequest,
    RegisterDeviceResponse,
    RenameThreadResponse,
    RenameSuperprojectResponse,
    ServerInfoResponse,
    SharedCheckpointMetadata,
    SuperprojectManifest,
    ThreadSummary,
    ThreadCheckpoint,
    UpdateMetadataResponse,
    UpdatePackageRequest,
    UpdatePackageResponse,
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
    checkpoint_retention_per_thread = int(os.environ.get("CWS_CHECKPOINT_RETENTION_PER_THREAD", "5"))
    backup_retention_per_superproject = int(os.environ.get("CWS_BACKUP_RETENTION_PER_SUPERPROJECT", "20"))

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

    def _load_lease(self, resource_id: str = "global") -> LeaseRecord:
        with self.db.connect() as connection:
            row = connection.execute(
                """
                SELECT resource_id, device_id, acquired_at, last_heartbeat_at, heartbeat_timeout_seconds
                FROM leases WHERE resource_id = ?
                """
                ,
                (resource_id,),
            ).fetchone()
        if row is None:
            return LeaseRecord(resource_id=resource_id, heartbeat_timeout_seconds=self.heartbeat_timeout_seconds)
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
                    resource_id=resource_id,
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
        lease = self._load_lease(request.resource_id)
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

    def heartbeat(self, device_id: str, *, resource_id: str = "global") -> HeartbeatResponse:
        lease = self._load_lease(resource_id)
        if lease.device_id != device_id:
            return HeartbeatResponse(lease=lease, accepted=False)
        lease.last_heartbeat_at = utc_now()
        lease.state = LeaseState.ACTIVE
        self._write_lease(lease)
        return HeartbeatResponse(lease=lease, accepted=True)

    def release_lease(self, device_id: str, *, resource_id: str = "global", force: bool = False) -> LeaseRecord:
        lease = self._load_lease(resource_id)
        if lease.device_id != device_id and not force:
            return lease
        lease.device_id = None
        lease.acquired_at = None
        lease.last_heartbeat_at = None
        lease.heartbeat_timeout_seconds = self.heartbeat_timeout_seconds
        lease.state = LeaseState.AVAILABLE
        self._write_lease(lease)
        return lease

    def current_lease(self, resource_id: str = "global") -> LeaseRecord:
        return self._load_lease(resource_id)

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
        _, shared_skill_revision = self._shared_skill_catalog()
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
            shared_skill_catalog_revision=shared_skill_revision or "v1",
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

    def rename_superproject(self, slug: str, new_name: str) -> RenameSuperprojectResponse:
        cleaned_name = new_name.strip()
        if not cleaned_name:
            raise ValueError("Superproject name cannot be empty.")
        manifest = self.get_manifest(slug)
        updated_manifest = manifest.model_copy(
            update={
                "name": cleaned_name,
                "updated_at": utc_now(),
            }
        )
        self._save_manifest(updated_manifest)
        return RenameSuperprojectResponse(manifest=updated_manifest)

    def _thread_name_overrides(self, slug: str) -> dict[str, dict[str, Any]]:
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT thread_id, custom_name, updated_at
                FROM thread_names
                WHERE superproject_slug = ?
                ORDER BY updated_at DESC
                """,
                (slug,),
            ).fetchall()
        return {
            row["thread_id"]: {
                "name": row["custom_name"],
                "updated_at": datetime.fromisoformat(row["updated_at"]),
            }
            for row in rows
        }

    def rename_thread(self, slug: str, thread_id: str, new_name: str) -> RenameThreadResponse:
        self.get_manifest(slug)
        cleaned_name = new_name.strip()
        if not cleaned_name:
            raise ValueError("Thread name cannot be empty.")
        normalized_thread_id = thread_id.strip()
        if not normalized_thread_id:
            raise ValueError("Thread ID cannot be empty.")
        updated_at = utc_now()
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO thread_names (superproject_slug, thread_id, custom_name, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(superproject_slug, thread_id) DO UPDATE SET
                    custom_name = excluded.custom_name,
                    updated_at = excluded.updated_at
                """,
                (slug, normalized_thread_id, cleaned_name, updated_at.isoformat()),
            )
            connection.commit()
        thread = self._thread_summary(slug, normalized_thread_id, updated_at=updated_at)
        return RenameThreadResponse(thread=thread)

    def _thread_metadata_cache(self, slug: str) -> dict[str, dict[str, Any]]:
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT thread_id, cached_thread_name, cached_last_user_turn_preview, updated_at, revision, source_device_id
                FROM thread_metadata
                WHERE superproject_slug = ?
                ORDER BY updated_at DESC
                """,
                (slug,),
            ).fetchall()
        return {
            row["thread_id"]: {
                "thread_name": row["cached_thread_name"],
                "last_user_turn_preview": row["cached_last_user_turn_preview"],
                "updated_at": datetime.fromisoformat(row["updated_at"]),
                "revision": row["revision"],
                "source_device_id": row["source_device_id"],
            }
            for row in rows
        }

    def _cache_thread_metadata(
        self,
        slug: str,
        checkpoint: ThreadCheckpoint,
        *,
        thread_name: str | None,
        last_user_turn_preview: str | None,
        updated_at: datetime,
    ) -> None:
        if not checkpoint.thread_id:
            return
        with self.db.connect() as connection:
            connection.execute(
                """
                INSERT INTO thread_metadata (
                    superproject_slug,
                    thread_id,
                    cached_thread_name,
                    cached_last_user_turn_preview,
                    updated_at,
                    revision,
                    source_device_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(superproject_slug, thread_id) DO UPDATE SET
                    cached_thread_name = excluded.cached_thread_name,
                    cached_last_user_turn_preview = excluded.cached_last_user_turn_preview,
                    updated_at = excluded.updated_at,
                    revision = excluded.revision,
                    source_device_id = excluded.source_device_id
                """,
                (
                    slug,
                    checkpoint.thread_id,
                    thread_name,
                    last_user_turn_preview,
                    updated_at.isoformat(),
                    checkpoint.revision,
                    checkpoint.source_device_id,
                ),
            )
            connection.commit()

    def get_manifest(self, slug: str) -> SuperprojectManifest:
        with self.db.connect() as connection:
            row = connection.execute(
                "SELECT manifest_json FROM superprojects WHERE slug = ?",
                (slug,),
            ).fetchone()
        if row is None:
            raise FileNotFoundError(f"Unknown superproject: {slug}")
        manifest = SuperprojectManifest.model_validate(json.loads(row["manifest_json"]))
        _, shared_skill_revision = self._shared_skill_catalog()
        if shared_skill_revision and manifest.shared_skill_catalog_revision != shared_skill_revision:
            manifest = manifest.model_copy(update={"shared_skill_catalog_revision": shared_skill_revision})
        return manifest

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

    @staticmethod
    def _bundle_storage_id(
        bundle: RawSessionBundle | RawCodexSharedBundle,
        *,
        thread_id: str | None = None,
    ) -> str:
        digest_entries = [
            {
                "relative_path": artifact.relative_path,
                "sha256": artifact.sha256,
            }
            for artifact in bundle.files
        ]
        return sha256_text(
            json.dumps(
                {
                    "thread_id": thread_id,
                    "thread_name": getattr(bundle, "thread_name", None),
                    "thread_updated_at": (
                        getattr(bundle, "thread_updated_at", None).isoformat()
                        if getattr(bundle, "thread_updated_at", None) is not None
                        else None
                    ),
                    "last_user_turn_preview": getattr(bundle, "last_user_turn_preview", None),
                    "files": digest_entries,
                },
                sort_keys=True,
            )
        )

    def _raw_bundle_path(self, slug: str, bundle_id: str) -> Path:
        return self._superproject_root(slug) / "raw_codex" / f"{bundle_id}.json"

    def _shared_bundle_path(self, slug: str, bundle_id: str) -> Path:
        return self._superproject_root(slug) / "raw_codex" / "shared" / f"{bundle_id}.json"

    def _write_bundle_once(
        self,
        slug: str,
        *,
        raw_bundle: RawSessionBundle | None = None,
        shared_bundle: RawCodexSharedBundle | None = None,
        thread_id: str | None = None,
    ) -> tuple[str | None, str | None]:
        raw_bundle_id: str | None = None
        shared_bundle_id: str | None = None
        if raw_bundle is not None:
            raw_bundle_id = self._bundle_storage_id(raw_bundle, thread_id=thread_id)
            raw_path = self._raw_bundle_path(slug, raw_bundle_id)
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            if not raw_path.exists():
                dump_json_file(
                    raw_path,
                    raw_bundle.model_dump(mode="json", exclude={"bundle_id"}),
                )
        if shared_bundle is not None:
            shared_bundle_id = self._bundle_storage_id(shared_bundle)
            shared_path = self._shared_bundle_path(slug, shared_bundle_id)
            shared_path.parent.mkdir(parents=True, exist_ok=True)
            if not shared_path.exists():
                dump_json_file(
                    shared_path,
                    shared_bundle.model_dump(mode="json", exclude={"bundle_id"}),
                )
        return raw_bundle_id, shared_bundle_id

    def _load_raw_bundle(self, slug: str, bundle_id: str) -> RawSessionBundle | None:
        path = self._raw_bundle_path(slug, bundle_id)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.setdefault("bundle_id", bundle_id)
        return RawSessionBundle.model_validate(payload)

    def _load_shared_bundle(self, slug: str, bundle_id: str) -> RawCodexSharedBundle | None:
        path = self._shared_bundle_path(slug, bundle_id)
        if not path.exists():
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload.setdefault("bundle_id", bundle_id)
        return RawCodexSharedBundle.model_validate(payload)

    def _thin_checkpoint(self, checkpoint: ThreadCheckpoint) -> ThreadCheckpoint:
        raw_bundle_id = checkpoint.raw_bundle_id
        shared_bundle_id = checkpoint.shared_bundle_id
        if checkpoint.raw_bundle is not None:
            raw_bundle_id, _ = self._write_bundle_once(
                checkpoint.superproject_slug,
                raw_bundle=checkpoint.raw_bundle,
                thread_id=checkpoint.thread_id,
            )
        if checkpoint.shared_bundle is not None:
            _, shared_bundle_id = self._write_bundle_once(
                checkpoint.superproject_slug,
                shared_bundle=checkpoint.shared_bundle,
            )
        return checkpoint.model_copy(
            update={
                "raw_bundle_id": raw_bundle_id,
                "raw_bundle": None,
                "shared_bundle_id": shared_bundle_id,
                "shared_bundle": None,
            }
        )

    def _hydrate_checkpoint_bundles(self, checkpoint: ThreadCheckpoint) -> ThreadCheckpoint:
        updates: dict[str, Any] = {}
        if checkpoint.raw_bundle is None and checkpoint.raw_bundle_id:
            raw_bundle = self._load_raw_bundle(checkpoint.superproject_slug, checkpoint.raw_bundle_id)
            if raw_bundle is not None:
                updates["raw_bundle"] = raw_bundle
        if checkpoint.shared_bundle is None and checkpoint.shared_bundle_id:
            shared_bundle = self._load_shared_bundle(checkpoint.superproject_slug, checkpoint.shared_bundle_id)
            if shared_bundle is not None:
                updates["shared_bundle"] = shared_bundle
        return checkpoint.model_copy(update=updates) if updates else checkpoint

    def _checkpoint_from_payload(
        self,
        slug: str,
        payload_json: str,
        *,
        raw_bundle_id: str | None = None,
        shared_bundle_id: str | None = None,
        hydrate: bool = False,
    ) -> ThreadCheckpoint:
        checkpoint = ThreadCheckpoint.model_validate(json.loads(payload_json))
        if raw_bundle_id and checkpoint.raw_bundle_id is None:
            checkpoint = checkpoint.model_copy(update={"raw_bundle_id": raw_bundle_id})
        if shared_bundle_id and checkpoint.shared_bundle_id is None:
            checkpoint = checkpoint.model_copy(update={"shared_bundle_id": shared_bundle_id})
        if hydrate:
            checkpoint = self._hydrate_checkpoint_bundles(checkpoint)
        return checkpoint

    def _latest_checkpoint(
        self,
        slug: str,
        thread_id: str | None = None,
        *,
        hydrate: bool = False,
    ) -> ThreadCheckpoint | None:
        query = """
            SELECT payload_json, raw_bundle_id, shared_bundle_id
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
        return self._checkpoint_from_payload(
            slug,
            row["payload_json"],
            raw_bundle_id=row["raw_bundle_id"],
            shared_bundle_id=row["shared_bundle_id"],
            hydrate=hydrate,
        )

    def _latest_shared_checkpoint(self, slug: str, *, hydrate: bool = False) -> ThreadCheckpoint | None:
        query = """
            SELECT payload_json, raw_bundle_id, shared_bundle_id
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
        return self._checkpoint_from_payload(
            slug,
            row["payload_json"],
            raw_bundle_id=row["raw_bundle_id"],
            shared_bundle_id=row["shared_bundle_id"],
            hydrate=hydrate,
        )

    def _latest_thread_checkpoints(self, slug: str, *, hydrate: bool = False) -> list[ThreadCheckpoint]:
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json, raw_bundle_id, shared_bundle_id
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
            checkpoint = self._checkpoint_from_payload(
                slug,
                row["payload_json"],
                raw_bundle_id=row["raw_bundle_id"],
                shared_bundle_id=row["shared_bundle_id"],
                hydrate=hydrate,
            )
            thread_key = checkpoint.thread_id or ""
            if thread_key in seen_thread_ids:
                continue
            seen_thread_ids.add(thread_key)
            checkpoints.append(checkpoint)
        return sorted(checkpoints, key=lambda checkpoint: checkpoint.revision)

    def _latest_thread_checkpoint_map(self, slug: str, *, hydrate: bool = False) -> dict[str, ThreadCheckpoint]:
        return {
            checkpoint.thread_id: checkpoint
            for checkpoint in self._latest_thread_checkpoints(slug, hydrate=hydrate)
            if checkpoint.thread_id
        }

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
        artifacts, _ = self._shared_skill_catalog()
        return artifacts

    def _shared_skill_catalog(self) -> tuple[list[RawFileArtifact], str | None]:
        root = self.paths.shared_skills_root
        if not root.exists():
            return [], None
        artifacts: list[RawFileArtifact] = []
        digest_entries: list[dict[str, str]] = []
        for path in sorted(root.rglob("*")):
            if path.is_dir():
                continue
            sha = sha256_file(path)
            artifacts.append(
                RawFileArtifact(
                    relative_path=relative_posix(path, root),
                    sha256=sha,
                    content_b64=encode_b64(path.read_bytes()),
                )
            )
            digest_entries.append(
                {
                    "relative_path": relative_posix(path, root),
                    "sha256": sha,
                }
            )
        revision = sha256_text(json.dumps(digest_entries, sort_keys=True)) if digest_entries else None
        return artifacts, revision

    def server_info(self) -> ServerInfoResponse:
        artifacts, revision = self._shared_skill_catalog()
        return ServerInfoResponse(
            schema_version=self.db.schema_version(),
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            scoped_leases_supported=True,
            shared_skills_revision=revision,
            shared_skills_count=len(artifacts),
        )

    def pull_state(self, slug: str) -> PullStateResponse:
        manifest = self.get_manifest(slug)
        shared_skills, _ = self._shared_skill_catalog()
        return PullStateResponse(
            manifest=manifest,
            latest_checkpoint=self._latest_checkpoint(slug, hydrate=True),
            shared_checkpoint=self._latest_shared_checkpoint(slug, hydrate=True),
            thread_checkpoints=self._latest_thread_checkpoints(slug, hydrate=True),
            pending_resolutions=self._pending_resolutions(slug),
            managed_documents=[
                ManagedDocument(
                    record=record,
                    content=(self._superproject_root(slug) / record.relative_path).read_text(encoding="utf-8"),
                )
                for record in manifest.managed_files
                if (self._superproject_root(slug) / record.relative_path).exists()
            ],
            shared_skills=shared_skills,
        )

    def update_metadata(self, slug: str) -> UpdateMetadataResponse:
        manifest = self.get_manifest(slug)
        shared_checkpoint = self._latest_shared_checkpoint(slug)
        shared_skills, shared_skills_revision = self._shared_skill_catalog()
        return UpdateMetadataResponse(
            manifest=manifest,
            shared_checkpoint=(
                SharedCheckpointMetadata(
                    revision=shared_checkpoint.revision,
                    updated_at=shared_checkpoint.created_at,
                )
                if shared_checkpoint is not None
                else None
            ),
            shared_skills_revision=shared_skills_revision,
            shared_skills_count=len(shared_skills),
            threads=self.list_threads(slug),
            pending_resolutions=self._pending_resolutions(slug),
        )

    def update_package(self, slug: str, request: UpdatePackageRequest) -> UpdatePackageResponse:
        manifest = self.get_manifest(slug)
        checkpoint_map = self._latest_thread_checkpoint_map(slug, hydrate=True)
        thread_ids = list(dict.fromkeys(thread_id for thread_id in request.thread_ids if thread_id))
        shared_skills, shared_skills_revision = self._shared_skill_catalog()
        thread_checkpoints = [
            checkpoint_map[thread_id]
            for thread_id in thread_ids
            if thread_id in checkpoint_map
        ]
        return UpdatePackageResponse(
            manifest=manifest,
            shared_checkpoint=self._latest_shared_checkpoint(slug, hydrate=True) if request.include_shared_checkpoint else None,
            shared_skills_revision=shared_skills_revision,
            thread_checkpoints=thread_checkpoints,
            managed_documents=[
                ManagedDocument(
                    record=record,
                    content=(self._superproject_root(slug) / record.relative_path).read_text(encoding="utf-8"),
                )
                for record in manifest.managed_files
                if request.include_managed_documents
                and (self._superproject_root(slug) / record.relative_path).exists()
            ],
            shared_skills=shared_skills if request.include_shared_skills else [],
            pending_resolutions=self._pending_resolutions(slug),
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

    def _thread_summary(
        self,
        slug: str,
        thread_id: str,
        *,
        checkpoint: ThreadCheckpoint | None = None,
        overrides: dict[str, dict[str, Any]] | None = None,
        cached: dict[str, Any] | None = None,
        updated_at: datetime | None = None,
    ) -> ThreadSummary:
        effective_overrides = overrides if overrides is not None else self._thread_name_overrides(slug)
        checkpoint_value = checkpoint or self._latest_checkpoint(slug, thread_id)
        override = effective_overrides.get(thread_id)
        cached_metadata = cached or self._thread_metadata_cache(slug).get(thread_id)
        if cached_metadata is None and checkpoint_value is not None:
            derived_name = self._thread_name_from_raw_bundle(checkpoint_value)
            derived_preview = self._preview_from_raw_bundle(checkpoint_value)
            cached_updated_at = (
                (checkpoint_value.raw_bundle.thread_updated_at if checkpoint_value.raw_bundle else None)
                or checkpoint_value.created_at
            )
            self._cache_thread_metadata(
                slug,
                checkpoint_value,
                thread_name=derived_name or (checkpoint_value.raw_bundle.thread_name if checkpoint_value.raw_bundle else None),
                last_user_turn_preview=(
                    (checkpoint_value.raw_bundle.last_user_turn_preview if checkpoint_value.raw_bundle else None)
                    or derived_preview
                ),
                updated_at=cached_updated_at,
            )
            cached_metadata = self._thread_metadata_cache(slug).get(thread_id)
        derived_name = cached_metadata["thread_name"] if cached_metadata else None
        derived_preview = cached_metadata["last_user_turn_preview"] if cached_metadata else None
        override_updated_at = override["updated_at"] if override else None
        return ThreadSummary(
            thread_id=thread_id,
            thread_name=(
                (override["name"] if override else None)
                or derived_name
                or (checkpoint_value.raw_bundle.thread_name if checkpoint_value and checkpoint_value.raw_bundle else None)
                or derived_preview
                or (checkpoint_value.summary if checkpoint_value else None)
                or thread_id
            ),
            updated_at=(
                (cached_metadata["updated_at"] if cached_metadata else None)
                or (checkpoint_value.raw_bundle.thread_updated_at if checkpoint_value and checkpoint_value.raw_bundle else None)
                or (checkpoint_value.created_at if checkpoint_value is not None else None)
                or updated_at
                or override_updated_at
                or utc_now()
            ),
            last_user_turn_preview=(
                (cached_metadata["last_user_turn_preview"] if cached_metadata else None)
                or (checkpoint_value.raw_bundle.last_user_turn_preview if checkpoint_value and checkpoint_value.raw_bundle else None)
                or derived_preview
            ),
            revision=(
                int(cached_metadata["revision"])
                if cached_metadata and cached_metadata.get("revision") is not None
                else (checkpoint_value.revision if checkpoint_value is not None else None)
            ),
            name_manually_set=override is not None,
            tracked=checkpoint_value is not None or override is not None,
            source="server",
        )

    def list_threads(self, slug: str) -> list[ThreadSummary]:
        checkpoint_map = self._latest_thread_checkpoint_map(slug)
        overrides = self._thread_name_overrides(slug)
        metadata_cache = self._thread_metadata_cache(slug)
        thread_ids = list(
            dict.fromkeys(
                [thread_id for thread_id in checkpoint_map if thread_id]
                + list(overrides.keys())
                + list(metadata_cache.keys())
            )
        )
        summaries = [
            self._thread_summary(
                slug,
                thread_id,
                checkpoint=checkpoint_map.get(thread_id),
                overrides=overrides,
                cached=metadata_cache.get(thread_id),
            )
            for thread_id in thread_ids
        ]
        return sorted(summaries, key=lambda item: (item.updated_at, item.thread_name, item.thread_id), reverse=True)

    def delete_superproject(self, slug: str, *, requesting_device_id: str, force: bool = False) -> dict[str, object]:
        manifest = self.get_manifest(slug)
        leases = [
            self._load_lease(),
            self._load_lease(f"superproject:{slug}"),
        ]
        blocking_lease = next(
            (
                lease
                for lease in leases
                if lease.device_id and lease.device_id != requesting_device_id and not force
            ),
            None,
        )
        if blocking_lease is not None:
            raise PermissionError(
                f"Another device currently holds the active lease: {blocking_lease.device_id}"
            )
        for lease in leases:
            if lease.device_id and (lease.device_id == requesting_device_id or force):
                self.release_lease(lease.device_id, resource_id=lease.resource_id, force=True)

        with self.db.connect() as connection:
            connection.execute("DELETE FROM checkpoints WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM mismatch_resolutions WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM backups WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM thread_names WHERE superproject_slug = ?", (slug,))
            connection.execute("DELETE FROM thread_metadata WHERE superproject_slug = ?", (slug,))
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

    def _superproject_slugs(self) -> list[str]:
        with self.db.connect() as connection:
            rows = connection.execute("SELECT slug FROM superprojects ORDER BY slug").fetchall()
        return [str(row["slug"]) for row in rows]

    @staticmethod
    def _directory_size_bytes(root: Path) -> int:
        if not root.exists():
            return 0
        return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())

    def analyze_state(self, slug: str | None = None, *, top_n: int = 15) -> dict[str, Any]:
        target_slugs = [slug] if slug else self._superproject_slugs()
        superprojects: list[dict[str, Any]] = []
        for target_slug in target_slugs:
            root = self._superproject_root(target_slug)
            file_count = sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0
            superprojects.append(
                {
                    "slug": target_slug,
                    "size_bytes": self._directory_size_bytes(root),
                    "file_count": file_count,
                }
            )
        with self.db.connect() as connection:
            checkpoint_stats = connection.execute(
                """
                SELECT COUNT(*) AS checkpoint_count, COALESCE(SUM(LENGTH(payload_json)), 0) AS payload_bytes
                FROM checkpoints
                """
                + (" WHERE superproject_slug = ?" if slug else ""),
                ((slug,) if slug else ()),
            ).fetchone()
            backup_stats = connection.execute(
                """
                SELECT COUNT(*) AS backup_count, COALESCE(SUM(LENGTH(payload_json)), 0) AS payload_bytes
                FROM backups
                """
                + (" WHERE superproject_slug = ?" if slug else ""),
                ((slug,) if slug else ()),
            ).fetchone()
        state_root = self.paths.state_root if slug is None else self._superproject_root(slug)
        biggest_files = []
        if state_root.exists():
            files = sorted(
                (path for path in state_root.rglob("*") if path.is_file()),
                key=lambda path: path.stat().st_size,
                reverse=True,
            )
            biggest_files = [
                {
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                }
                for path in files[:top_n]
            ]
        return {
            "state_root": str(state_root),
            "size_bytes": self._directory_size_bytes(state_root),
            "db_file": str(self.paths.db_file),
            "db_size_bytes": self.paths.db_file.stat().st_size if self.paths.db_file.exists() else 0,
            "checkpoint_count": int(checkpoint_stats["checkpoint_count"]) if checkpoint_stats else 0,
            "checkpoint_payload_bytes": int(checkpoint_stats["payload_bytes"]) if checkpoint_stats else 0,
            "backup_count": int(backup_stats["backup_count"]) if backup_stats else 0,
            "backup_payload_bytes": int(backup_stats["payload_bytes"]) if backup_stats else 0,
            "superprojects": superprojects,
            "largest_files": biggest_files,
        }

    def _compact_checkpoints(self, slug: str) -> int:
        connection = self.db.connect()
        try:
            rows = connection.execute(
                """
                SELECT checkpoint_id, payload_json, raw_bundle_id, shared_bundle_id
                FROM checkpoints
                WHERE superproject_slug = ?
                ORDER BY revision DESC
                """,
                (slug,),
            ).fetchall()
            rewritten = 0
            metadata_updates: list[dict[str, Any]] = []
            for row in rows:
                checkpoint = self._checkpoint_from_payload(
                    slug,
                    row["payload_json"],
                    raw_bundle_id=row["raw_bundle_id"],
                    shared_bundle_id=row["shared_bundle_id"],
                )
                if checkpoint.thread_id and checkpoint.raw_bundle is not None:
                    derived_name = self._thread_name_from_raw_bundle(checkpoint)
                    derived_preview = self._preview_from_raw_bundle(checkpoint)
                    metadata_updated_at = checkpoint.raw_bundle.thread_updated_at or checkpoint.created_at
                    metadata_updates.append(
                        {
                            "checkpoint": checkpoint,
                            "thread_name": derived_name or checkpoint.raw_bundle.thread_name,
                            "last_user_turn_preview": checkpoint.raw_bundle.last_user_turn_preview or derived_preview,
                            "updated_at": metadata_updated_at,
                        }
                    )
                thin_checkpoint = self._thin_checkpoint(checkpoint)
                if (
                    checkpoint.raw_bundle is None
                    and checkpoint.shared_bundle is None
                    and row["raw_bundle_id"] == thin_checkpoint.raw_bundle_id
                    and row["shared_bundle_id"] == thin_checkpoint.shared_bundle_id
                ):
                    continue
                connection.execute(
                    """
                    UPDATE checkpoints
                    SET raw_bundle_id = ?, shared_bundle_id = ?, payload_json = ?
                    WHERE checkpoint_id = ?
                    """,
                    (
                        thin_checkpoint.raw_bundle_id,
                        thin_checkpoint.shared_bundle_id,
                        json.dumps(thin_checkpoint.model_dump(mode="json")),
                        thin_checkpoint.checkpoint_id,
                    ),
                )
                dump_json_file(
                    self._checkpoint_json_path(slug, thin_checkpoint.thread_id, thin_checkpoint.revision),
                    thin_checkpoint.model_dump(mode="json"),
                )
                rewritten += 1
            connection.commit()
        finally:
            connection.close()
        for metadata_update in metadata_updates:
            self._cache_thread_metadata(
                slug,
                metadata_update["checkpoint"],
                thread_name=metadata_update["thread_name"],
                last_user_turn_preview=metadata_update["last_user_turn_preview"],
                updated_at=metadata_update["updated_at"],
            )
        return rewritten

    def _compact_backups(self, slug: str) -> int:
        connection = self.db.connect()
        try:
            rows = connection.execute(
                """
                SELECT backup_id, payload_json
                FROM backups
                WHERE superproject_slug = ?
                ORDER BY created_at DESC
                """,
                (slug,),
            ).fetchall()
            rewritten = 0
            for row in rows:
                backup = BackupRecord.model_validate(json.loads(row["payload_json"]))
                latest_checkpoint_payload = backup.snapshot.get("latest_checkpoint")
                if latest_checkpoint_payload is None:
                    continue
                checkpoint = ThreadCheckpoint.model_validate(latest_checkpoint_payload)
                thin_checkpoint = self._thin_checkpoint(checkpoint)
                if checkpoint.raw_bundle is None and checkpoint.shared_bundle is None:
                    continue
                backup.snapshot["latest_checkpoint"] = thin_checkpoint.model_dump(mode="json")
                payload_json = json.dumps(backup.model_dump(mode="json"))
                connection.execute(
                    "UPDATE backups SET payload_json = ? WHERE backup_id = ?",
                    (payload_json, backup.backup_id),
                )
                dump_json_file(
                    self._superproject_root(slug) / "backups" / f"{backup.backup_id}.json",
                    backup.model_dump(mode="json"),
                )
                rewritten += 1
            connection.commit()
        finally:
            connection.close()
        return rewritten

    def compact_state(self, slug: str | None = None, *, vacuum: bool = True) -> dict[str, Any]:
        before = self.analyze_state(slug)
        target_slugs = [slug] if slug else self._superproject_slugs()
        rewritten_checkpoints = 0
        rewritten_backups = 0
        warnings: list[str] = []
        for target_slug in target_slugs:
            rewritten_checkpoints += self._compact_checkpoints(target_slug)
            rewritten_backups += self._compact_backups(target_slug)
            self._prune_superproject_state(target_slug)
        vacuumed = False
        if vacuum:
            try:
                connection = sqlite3.connect(self.paths.db_file, timeout=5)
                try:
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    connection.execute("PRAGMA journal_mode=DELETE")
                    connection.execute("VACUUM")
                    connection.execute("PRAGMA journal_mode=WAL")
                    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                    vacuumed = True
                finally:
                    connection.close()
            except sqlite3.OperationalError as exc:
                warnings.append(f"SQLite vacuum skipped: {exc}")
        after = self.analyze_state(slug)
        result = {
            "slug": slug,
            "rewritten_checkpoints": rewritten_checkpoints,
            "rewritten_backups": rewritten_backups,
            "vacuum_requested": vacuum,
            "vacuumed": vacuumed,
            "before": before,
            "after": after,
            "bytes_reclaimed": max(0, int(before["size_bytes"]) - int(after["size_bytes"])),
        }
        if warnings:
            result["warnings"] = warnings
        return result

    def get_thread_checkpoint(self, slug: str, thread_id: str) -> ThreadCheckpoint:
        checkpoint = self._latest_checkpoint(slug, thread_id, hydrate=True)
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

    @staticmethod
    def _checkpoint_dir_name(thread_id: str | None) -> str:
        return thread_id or "default"

    def _checkpoint_json_path(self, slug: str, thread_id: str | None, revision: int) -> Path:
        return (
            self._superproject_root(slug)
            / "threads"
            / self._checkpoint_dir_name(thread_id)
            / "checkpoints"
            / f"{revision}.json"
        )

    @staticmethod
    def _bundle_ids_from_checkpoint(checkpoint: ThreadCheckpoint) -> tuple[str | None, str | None]:
        raw_bundle_id = checkpoint.raw_bundle_id or (checkpoint.raw_bundle.bundle_id if checkpoint.raw_bundle else None)
        shared_bundle_id = checkpoint.shared_bundle_id or (
            checkpoint.shared_bundle.bundle_id if checkpoint.shared_bundle else None
        )
        return raw_bundle_id, shared_bundle_id

    def _backup_bundle_ids(self, slug: str) -> tuple[set[str], set[str]]:
        raw_bundle_ids: set[str] = set()
        shared_bundle_ids: set[str] = set()
        with self.db.connect() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM backups WHERE superproject_slug = ?",
                (slug,),
            ).fetchall()
        for row in rows:
            backup = BackupRecord.model_validate(json.loads(row["payload_json"]))
            latest_checkpoint_payload = backup.snapshot.get("latest_checkpoint")
            if latest_checkpoint_payload is None:
                continue
            checkpoint = ThreadCheckpoint.model_validate(latest_checkpoint_payload)
            raw_bundle_id, shared_bundle_id = self._bundle_ids_from_checkpoint(checkpoint)
            if raw_bundle_id:
                raw_bundle_ids.add(raw_bundle_id)
            if shared_bundle_id:
                shared_bundle_ids.add(shared_bundle_id)
        return raw_bundle_ids, shared_bundle_ids

    def _prune_checkpoint_history(self, slug: str) -> None:
        keep_count = max(1, self.checkpoint_retention_per_thread)
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT checkpoint_id, thread_id, revision, canonical, payload_json, raw_bundle_id, shared_bundle_id
                FROM checkpoints
                WHERE superproject_slug = ?
                ORDER BY revision DESC
                """,
                (slug,),
            ).fetchall()

            retained_checkpoints: list[ThreadCheckpoint] = []
            delete_rows: list[tuple[str, str | None, int]] = []
            seen_per_bucket: dict[tuple[str, int], int] = {}
            for row in rows:
                thread_key = row["thread_id"] or "__default__"
                canonical = int(row["canonical"])
                bucket = (thread_key, canonical)
                bucket_limit = keep_count if canonical else 1
                count = seen_per_bucket.get(bucket, 0)
                if count < bucket_limit:
                    seen_per_bucket[bucket] = count + 1
                    retained_checkpoints.append(
                        self._checkpoint_from_payload(
                            slug,
                            row["payload_json"],
                            raw_bundle_id=row["raw_bundle_id"],
                            shared_bundle_id=row["shared_bundle_id"],
                        )
                    )
                    continue
                delete_rows.append((row["checkpoint_id"], row["thread_id"], row["revision"]))

            if delete_rows:
                connection.executemany(
                    "DELETE FROM checkpoints WHERE checkpoint_id = ?",
                    [(checkpoint_id,) for checkpoint_id, _, _ in delete_rows],
                )
                connection.commit()

        for _, thread_id, revision in delete_rows:
            checkpoint_path = self._checkpoint_json_path(slug, thread_id, revision)
            if checkpoint_path.exists():
                checkpoint_path.unlink()

        retained_raw_bundle_ids = set()
        retained_shared_bundle_ids = set()
        for checkpoint in retained_checkpoints:
            raw_bundle_id, shared_bundle_id = self._bundle_ids_from_checkpoint(checkpoint)
            if raw_bundle_id:
                retained_raw_bundle_ids.add(raw_bundle_id)
            if shared_bundle_id:
                retained_shared_bundle_ids.add(shared_bundle_id)
        backup_raw_bundle_ids, backup_shared_bundle_ids = self._backup_bundle_ids(slug)
        retained_raw_bundle_ids.update(backup_raw_bundle_ids)
        retained_shared_bundle_ids.update(backup_shared_bundle_ids)
        raw_root = self._superproject_root(slug) / "raw_codex"
        if raw_root.exists():
            for path in raw_root.glob("*.json"):
                if path.stem not in retained_raw_bundle_ids:
                    path.unlink()
        shared_root = raw_root / "shared"
        if shared_root.exists():
            for path in shared_root.glob("*.json"):
                if path.stem not in retained_shared_bundle_ids:
                    path.unlink()

    def _prune_backups(self, slug: str) -> None:
        keep_count = max(1, self.backup_retention_per_superproject)
        with self.db.connect() as connection:
            rows = connection.execute(
                """
                SELECT backup_id
                FROM backups
                WHERE superproject_slug = ?
                ORDER BY created_at DESC
                """,
                (slug,),
            ).fetchall()
            delete_ids = [row["backup_id"] for row in rows[keep_count:]]
            if delete_ids:
                connection.executemany(
                    "DELETE FROM backups WHERE backup_id = ?",
                    [(backup_id,) for backup_id in delete_ids],
                )
                connection.commit()

        backup_root = self._superproject_root(slug) / "backups"
        for backup_id in delete_ids:
            backup_path = backup_root / f"{backup_id}.json"
            if backup_path.exists():
                backup_path.unlink()

    def _prune_superproject_state(self, slug: str) -> None:
        self._prune_checkpoint_history(slug)
        self._prune_backups(slug)

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
        if not request.override and request.checkpoint.canonical:
            latest_checkpoint = self._latest_checkpoint(
                request.checkpoint.superproject_slug,
                request.checkpoint.thread_id,
            )
            if latest_checkpoint is not None and latest_checkpoint.snapshot_hash == request.checkpoint.snapshot_hash:
                return PushCheckpointResponse(
                    accepted=True,
                    revision=latest_checkpoint.revision,
                )
        backup = (
            self._create_backup(
                request.checkpoint.superproject_slug,
                request.checkpoint.thread_id,
            )
            if request.override
            else None
        )
        revision = current_manifest.revision + 1
        _, shared_skills_revision = self._shared_skill_catalog()
        checkpoint = request.checkpoint.model_copy(
            update={
                "revision": revision,
                "base_revision": current_manifest.revision,
                "created_at": utc_now(),
            }
        )
        for document in checkpoint.managed_documents:
            self._write_managed_document(checkpoint.superproject_slug, document)
        checkpoint_for_storage = self._thin_checkpoint(checkpoint)
        thread_dir = (
            self._superproject_root(checkpoint_for_storage.superproject_slug)
            / "threads"
            / (checkpoint_for_storage.thread_id or "default")
            / "checkpoints"
        )
        thread_dir.mkdir(parents=True, exist_ok=True)
        dump_json_file(thread_dir / f"{revision}.json", checkpoint_for_storage.model_dump(mode="json"))
        updated_manifest = incoming_manifest.model_copy(
            update={
                "revision": revision,
                "updated_at": checkpoint_for_storage.created_at,
                "shared_skill_catalog_revision": shared_skills_revision or incoming_manifest.shared_skill_catalog_revision,
                "managed_files": [
                    record.model_copy(update={"last_known_good_revision": revision})
                    for record in incoming_manifest.managed_files
                ],
            }
        )
        self._save_manifest(updated_manifest)
        if checkpoint.canonical and checkpoint.thread_id and checkpoint.raw_bundle is not None:
            derived_name = self._thread_name_from_raw_bundle(checkpoint)
            derived_preview = self._preview_from_raw_bundle(checkpoint)
            metadata_updated_at = checkpoint.raw_bundle.thread_updated_at or checkpoint.created_at
            self._cache_thread_metadata(
                checkpoint_for_storage.superproject_slug,
                checkpoint_for_storage,
                thread_name=derived_name or checkpoint.raw_bundle.thread_name,
                last_user_turn_preview=checkpoint.raw_bundle.last_user_turn_preview or derived_preview,
                updated_at=metadata_updated_at,
            )
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
                    raw_bundle_id,
                    shared_bundle_id,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    checkpoint_for_storage.checkpoint_id,
                    checkpoint_for_storage.superproject_slug,
                    checkpoint_for_storage.thread_id,
                    checkpoint_for_storage.revision,
                    checkpoint_for_storage.created_at.isoformat(),
                    checkpoint_for_storage.source_device_id,
                    1 if checkpoint_for_storage.canonical else 0,
                    checkpoint_for_storage.base_revision,
                    json.dumps(checkpoint_for_storage.turn_hashes),
                    checkpoint_for_storage.snapshot_hash,
                    checkpoint_for_storage.raw_bundle_id,
                    checkpoint_for_storage.shared_bundle_id,
                    json.dumps(checkpoint_for_storage.model_dump(mode="json")),
                ),
            )
            connection.commit()
        self._prune_superproject_state(checkpoint_for_storage.superproject_slug)
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
