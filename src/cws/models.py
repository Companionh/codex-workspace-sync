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
    heartbeat_timeout_seconds: int = 60
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
    session_ids: list[str] = Field(default_factory=list)
    files: list[RawFileArtifact] = Field(default_factory=list)


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
    raw_bundle: RawSessionBundle | None = None
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
    steal: bool = False


class AcquireLeaseResponse(BaseModel):
    lease: LeaseRecord
    granted: bool
    conflict_device_id: str | None = None


class HeartbeatRequest(BaseModel):
    device_id: str


class HeartbeatResponse(BaseModel):
    lease: LeaseRecord
    accepted: bool


class CreateSuperprojectRequest(BaseModel):
    name: str
    slug: str
    subprojects: list[SubprojectRecord]


class CreateSuperprojectResponse(BaseModel):
    manifest: SuperprojectManifest


class PushCheckpointRequest(BaseModel):
    checkpoint: ThreadCheckpoint
    override: bool = False


class PushCheckpointResponse(BaseModel):
    accepted: bool
    revision: int
    backup_id: str | None = None


class PullStateResponse(BaseModel):
    manifest: SuperprojectManifest
    latest_checkpoint: ThreadCheckpoint | None = None
    thread_checkpoints: list[ThreadCheckpoint] = Field(default_factory=list)
    pending_resolutions: list[MismatchResolution] = Field(default_factory=list)
    managed_documents: list[ManagedDocument] = Field(default_factory=list)
    shared_skills: list[RawFileArtifact] = Field(default_factory=list)


class ResolveMismatchRequest(BaseModel):
    resolution: MismatchResolution


class OverrideStateRequest(BaseModel):
    checkpoint: ThreadCheckpoint


class BackupRecord(BaseModel):
    backup_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    thread_id: str | None = None
    created_at: datetime
    snapshot: dict[str, Any]


class ClientSuperprojectState(BaseModel):
    slug: str
    name: str
    managed_root: str | None = None
    workspace_roots: list[str] = Field(default_factory=list)
    last_alignment_action: AlignmentAction = AlignmentAction.NONE
    last_aligned_revision: int = 0
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
    superprojects: dict[str, ClientSuperprojectState] = Field(default_factory=dict)
    sync_active_superproject: str | None = None


class OutboundQueueItem(BaseModel):
    queue_id: str = Field(default_factory=lambda: str(uuid4()))
    superproject_slug: str
    created_at: datetime
    checkpoint: ThreadCheckpoint
