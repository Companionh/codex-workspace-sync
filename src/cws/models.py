from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class ManagedFileClass(StrEnum):
    PROTECTED = "protected"
    NORMAL = "normal"
    GENERATED = "generated"


class AlignmentAction(StrEnum):
    NONE = "none"
    UPDATE_FROM_SERVER = "update-from-server"
    OVERRIDE_CURRENT_STATE = "override-current-state"


class LeaseState(StrEnum):
    ACTIVE = "active"
    EXPIRED = "expired"
    AVAILABLE = "available"


class LeaseScope(StrEnum):
    GLOBAL = "global"
    SUPERPROJECT = "superproject"


class DoctorStatus(StrEnum):
    OK = "ok"
    WARNING = "warning"
    ERROR = "error"


class DeviceRecord(BaseModel):
    device_id: str
    device_name: str
    created_at: datetime
    status: Literal["active", "stale", "revoked"] = "active"
    metadata: dict[str, Any] = Field(default_factory=dict)


class LeaseRecord(BaseModel):
    resource_id: str = "global"
    device_id: str | None = None
    acquired_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    heartbeat_timeout_seconds: int = 120
    state: LeaseState = LeaseState.AVAILABLE


class ManagedFileRecord(BaseModel):
    file_id: str = Field(default_factory=lambda: str(uuid4()))
    relative_path: str
    sha256: str
    size_bytes: int
    line_count: int
    classification: ManagedFileClass
    last_known_good_revision: int = 0


class ManagedDocument(BaseModel):
    record: ManagedFileRecord
    content: str


class SubprojectRecord(BaseModel):
    repo_url: str
    repo_name: str
    default_branch: str | None = None
    description: str | None = None


class SuperprojectManifest(BaseModel):
    slug: str
    name: str
    created_at: datetime
    updated_at: datetime
    revision: int = 0
    shared_skill_catalog_revision: str = "v1"
    subprojects: list[SubprojectRecord] = Field(default_factory=list)
    managed_files: list[ManagedFileRecord] = Field(default_factory=list)


class RawFileArtifact(BaseModel):
    relative_path: str
    sha256: str
    content_b64: str


class RawSessionBundle(BaseModel):
    bundle_id: str = Field(default_factory=lambda: str(uuid4()))
    captured_at: datetime
    thread_id: str | None = None
    thread_name: str | None = None
    thread_updated_at: datetime | None = None
    last_user_turn_preview: str | None = None
    session_ids: list[str] = Field(default_factory=list)
    files: list[RawFileArtifact] = Field(default_factory=list)


class RawCodexSharedBundle(BaseModel):
    bundle_id: str = Field(default_factory=lambda: str(uuid4()))
    captured_at: datetime
    files: list[RawFileArtifact] = Field(default_factory=list)


class ThreadSummary(BaseModel):
    thread_id: str
    thread_name: str
    updated_at: datetime
    last_user_turn_preview: str | None = None
    revision: int | None = None
    name_manually_set: bool = False
    tracked: bool = False
    source: Literal["local", "server"] = "local"


class ThreadCheckpoint(BaseModel):
    checkpoint_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    thread_id: str | None = None
    revision: int
    created_at: datetime
    source_device_id: str
    canonical: bool = True
    base_revision: int = 0
    turn_hashes: list[str] = Field(default_factory=list)
    summary: str | None = None
    manifest: SuperprojectManifest
    managed_documents: list[ManagedDocument] = Field(default_factory=list)
    raw_bundle_id: str | None = None
    raw_bundle: RawSessionBundle | None = None
    shared_bundle_id: str | None = None
    shared_bundle: RawCodexSharedBundle | None = None
    snapshot_hash: str


class MismatchResolution(BaseModel):
    resolution_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    thread_id: str
    created_at: datetime
    chosen_source: Literal["server", "local"]
    base_revision: int
    details: dict[str, Any] = Field(default_factory=dict)


class RegisterDeviceRequest(BaseModel):
    device_name: str
    secondary_passphrase: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class RegisterDeviceResponse(BaseModel):
    device: DeviceRecord
    device_secret: str


class AcquireLeaseRequest(BaseModel):
    device_id: str
    resource_id: str = "global"
    steal: bool = False


class AcquireLeaseResponse(BaseModel):
    lease: LeaseRecord
    granted: bool
    conflict_device_id: str | None = None


class HeartbeatRequest(BaseModel):
    device_id: str
    resource_id: str = "global"


class HeartbeatResponse(BaseModel):
    lease: LeaseRecord
    accepted: bool


class CurrentLeaseResponse(BaseModel):
    lease: LeaseRecord


class CreateSuperprojectRequest(BaseModel):
    name: str
    slug: str
    subprojects: list[SubprojectRecord]


class CreateSuperprojectResponse(BaseModel):
    manifest: SuperprojectManifest


class RenameSuperprojectRequest(BaseModel):
    name: str


class RenameSuperprojectResponse(BaseModel):
    manifest: SuperprojectManifest


class RenameThreadRequest(BaseModel):
    name: str


class RenameThreadResponse(BaseModel):
    thread: ThreadSummary


class PushCheckpointRequest(BaseModel):
    checkpoint: ThreadCheckpoint
    override: bool = False


class PushCheckpointResponse(BaseModel):
    accepted: bool
    revision: int
    backup_id: str | None = None


class SharedCheckpointMetadata(BaseModel):
    revision: int
    updated_at: datetime


class UpdateMetadataResponse(BaseModel):
    manifest: SuperprojectManifest
    shared_checkpoint: SharedCheckpointMetadata | None = None
    shared_skills_revision: str | None = None
    shared_skills_count: int = 0
    threads: list[ThreadSummary] = Field(default_factory=list)
    pending_resolutions: list[MismatchResolution] = Field(default_factory=list)


class UpdatePackageRequest(BaseModel):
    thread_ids: list[str] = Field(default_factory=list)
    include_shared_checkpoint: bool = False
    include_managed_documents: bool = True
    include_shared_skills: bool = True


class UpdatePackageResponse(BaseModel):
    manifest: SuperprojectManifest
    shared_checkpoint: ThreadCheckpoint | None = None
    shared_skills_revision: str | None = None
    thread_checkpoints: list[ThreadCheckpoint] = Field(default_factory=list)
    managed_documents: list[ManagedDocument] = Field(default_factory=list)
    shared_skills: list[RawFileArtifact] = Field(default_factory=list)
    pending_resolutions: list[MismatchResolution] = Field(default_factory=list)


class PullStateResponse(BaseModel):
    manifest: SuperprojectManifest
    latest_checkpoint: ThreadCheckpoint | None = None
    shared_checkpoint: ThreadCheckpoint | None = None
    thread_checkpoints: list[ThreadCheckpoint] = Field(default_factory=list)
    pending_resolutions: list[MismatchResolution] = Field(default_factory=list)
    managed_documents: list[ManagedDocument] = Field(default_factory=list)
    shared_skills: list[RawFileArtifact] = Field(default_factory=list)


class ResolveMismatchRequest(BaseModel):
    resolution: MismatchResolution


class OverrideStateRequest(BaseModel):
    checkpoint: ThreadCheckpoint


class ServerInfoResponse(BaseModel):
    schema_version: int
    heartbeat_timeout_seconds: int
    scoped_leases_supported: bool = True
    shared_skills_revision: str | None = None
    shared_skills_count: int = 0


class BackupRecord(BaseModel):
    backup_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    thread_id: str | None = None
    created_at: datetime
    snapshot: dict[str, Any]


class QueueHealth(BaseModel):
    queued_count: int = 0
    oldest_item_age_seconds: float | None = None
    retry_count: int = 0
    last_error: str | None = None


class DoctorCheck(BaseModel):
    name: str
    status: DoctorStatus
    detail: str


class DoctorReport(BaseModel):
    ok: bool
    superproject_slug: str | None = None
    lease_scope: LeaseScope = LeaseScope.GLOBAL
    checks: list[DoctorCheck] = Field(default_factory=list)
    queue_health: QueueHealth = Field(default_factory=QueueHealth)
    stale_docs: bool = False
    stale_shared_runtime: bool = False
    stale_threads: list[str] = Field(default_factory=list)
    stale_shared_skills: bool = False


class ClientSuperprojectState(BaseModel):
    slug: str
    name: str
    name_manually_set: bool = False
    managed_root: str | None = None
    workspace_roots: list[str] = Field(default_factory=list)
    tracked_thread_ids: list[str] = Field(default_factory=list)
    last_alignment_action: AlignmentAction = AlignmentAction.NONE
    last_aligned_revision: int = 0
    last_shared_bundle_revision: int = 0
    last_shared_skill_catalog_revision: str | None = None
    last_local_snapshot_hash: str | None = None
    pending_thread_refreshes: dict[str, int] = Field(default_factory=dict)
    managed_file_ids: dict[str, str] = Field(default_factory=dict)


class ClientConfig(BaseModel):
    server_url: str | None = None
    device_id: str | None = None
    device_name: str | None = None
    ssh_host: str | None = None
    ssh_user: str | None = None
    ssh_port: int = 22
    lease_scope: LeaseScope = LeaseScope.GLOBAL
    superprojects: dict[str, ClientSuperprojectState] = Field(default_factory=dict)
    sync_active_superproject: str | None = None


class OutboundQueueItem(BaseModel):
    queue_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    created_at: datetime
    checkpoint: ThreadCheckpoint
    retry_count: int = 0
    last_attempt_at: datetime | None = None
    last_error: str | None = None
