from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from cws.config import ServerPaths
from cws.models import (
    AcquireLeaseRequest,
    CreateSuperprojectRequest,
    ManagedDocument,
    PushCheckpointRequest,
    RegisterDeviceRequest,
    SubprojectRecord,
    ThreadCheckpoint,
)
from cws.server.service import ServerService
from cws.utils import sha256_text, utc_now


def make_service(tmp_path: Path) -> ServerService:
    repo_root = Path(__file__).resolve().parents[1]
    paths = ServerPaths.default(app_root=repo_root, state_root=tmp_path / "state")
    service = ServerService(paths=paths)
    service.init_state("secondary-passphrase")
    return service


def test_register_device_and_authenticate(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    response = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
            metadata={"platform": "windows"},
        )
    )

    authenticated = service.authenticate_device(
        response.device.device_id,
        response.device_secret,
    )

    assert authenticated.device_name == "machine-a"
    assert authenticated.metadata["platform"] == "windows"


def test_global_lease_expires_after_sixty_seconds(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    response = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    lease_response = service.acquire_lease(
        AcquireLeaseRequest(device_id=response.device.device_id)
    )
    assert lease_response.granted is True

    stale_timestamp = (utc_now() - timedelta(seconds=61)).isoformat()
    with service.db.connect() as connection:
        connection.execute(
            "UPDATE leases SET last_heartbeat_at = ? WHERE resource_id = 'global'",
            (stale_timestamp,),
        )
        connection.commit()

    heartbeat = service.heartbeat(response.device.device_id)
    assert heartbeat.accepted is False
    assert heartbeat.lease.device_id is None


def test_create_superproject_scaffolds_expected_directories(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[
                SubprojectRecord(
                    repo_url="https://github.com/example/bot-a",
                    repo_name="bot-a",
                ),
                SubprojectRecord(
                    repo_url="https://github.com/example/bot-b",
                    repo_name="bot-b",
                ),
            ],
        )
    ).manifest

    root = tmp_path / "state" / "superprojects" / "telegram-suite"
    assert (root / "baseline" / "base_rules.md").exists()
    assert (root / "ecosystem" / "sibling_repos.md").exists()
    assert (root / "subprojects" / "registry.json").exists()
    assert len(manifest.managed_files) >= 4


def test_push_checkpoint_rejects_missing_protected_file(tmp_path: Path) -> None:
    service = make_service(tmp_path)
    device = service.register_device(
        RegisterDeviceRequest(
            device_name="machine-a",
            secondary_passphrase="secondary-passphrase",
        )
    )
    service.acquire_lease(AcquireLeaseRequest(device_id=device.device.device_id))
    manifest = service.create_superproject(
        CreateSuperprojectRequest(
            name="Telegram Suite",
            slug="telegram-suite",
            subprojects=[],
        )
    ).manifest

    broken_manifest = manifest.model_copy(update={"managed_files": []})
    documents = [
        ManagedDocument(
            record=record,
            content="replacement",
        )
        for record in manifest.managed_files
    ]
    snapshot_hash = sha256_text(json.dumps({"documents": len(documents)}))
    request = PushCheckpointRequest(
        checkpoint=ThreadCheckpoint(
            superproject_slug="telegram-suite",
            revision=0,
            created_at=utc_now(),
            source_device_id=device.device.device_id,
            canonical=True,
            base_revision=manifest.revision,
            turn_hashes=[],
            summary="test checkpoint",
            manifest=broken_manifest,
            managed_documents=documents,
            raw_bundle=None,
            snapshot_hash=snapshot_hash,
        )
    )

    with pytest.raises(ValueError):
        service.push_checkpoint(device.device.device_id, request)
