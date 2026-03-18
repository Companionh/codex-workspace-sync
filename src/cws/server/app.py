from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from cws.config import ServerPaths
from cws.models import (
    AcquireLeaseRequest,
    AcquireLeaseResponse,
    CreateSuperprojectRequest,
    CreateSuperprojectResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    MismatchResolution,
    PullStateResponse,
    PushCheckpointRequest,
    PushCheckpointResponse,
    RenameSuperprojectRequest,
    RenameSuperprojectResponse,
    ResolveMismatchRequest,
    SuperprojectManifest,
    ThreadSummary,
)
from cws.server.service import ServerService


@lru_cache(maxsize=1)
def get_service() -> ServerService:
    repo_root = Path(__file__).resolve().parents[3]
    return ServerService(paths=ServerPaths.default(app_root=repo_root))


def authenticate(
    x_cws_device_id: str = Header(...),
    x_cws_device_secret: str = Header(...),
    service: ServerService = Depends(get_service),
) -> str:
    try:
        service.authenticate_device(x_cws_device_id, x_cws_device_secret)
    except PermissionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    return x_cws_device_id


def create_app() -> FastAPI:
    app = FastAPI(title="Codex Workspace Sync")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/skills/shared")
    def shared_skills(
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, object]:
        return {"artifacts": [artifact.model_dump(mode="json") for artifact in service._shared_skills()]}

    @app.post("/api/lease/acquire", response_model=AcquireLeaseResponse)
    def acquire_lease(
        request: AcquireLeaseRequest,
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> AcquireLeaseResponse:
        if device_id != request.device_id:
            raise HTTPException(status_code=403, detail="Device mismatch.")
        return service.acquire_lease(request)

    @app.post("/api/lease/heartbeat", response_model=HeartbeatResponse)
    def heartbeat(
        request: HeartbeatRequest,
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> HeartbeatResponse:
        if device_id != request.device_id:
            raise HTTPException(status_code=403, detail="Device mismatch.")
        try:
            return service.heartbeat(device_id)
        except Exception as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.post("/api/lease/release", response_model=HeartbeatResponse)
    def release_lease(
        request: HeartbeatRequest,
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> HeartbeatResponse:
        if device_id != request.device_id:
            raise HTTPException(status_code=403, detail="Device mismatch.")
        lease = service.release_lease(device_id)
        return HeartbeatResponse(lease=lease, accepted=True)

    @app.post("/api/superprojects", response_model=CreateSuperprojectResponse)
    def create_superproject(
        request: CreateSuperprojectRequest,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> CreateSuperprojectResponse:
        try:
            return service.create_superproject(request)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/superprojects/{slug}/rename", response_model=RenameSuperprojectResponse)
    def rename_superproject(
        slug: str,
        request: RenameSuperprojectRequest,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> RenameSuperprojectResponse:
        try:
            return service.rename_superproject(slug, request.name)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/superprojects/{slug}/state", response_model=PullStateResponse)
    def pull_state(
        slug: str,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> PullStateResponse:
        try:
            return service.pull_state(slug)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/superprojects/{slug}/manifest", response_model=dict[str, SuperprojectManifest])
    def get_manifest(
        slug: str,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, SuperprojectManifest]:
        try:
            manifest = service.get_manifest(slug)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"manifest": manifest}

    @app.get("/api/superprojects/{slug}/threads", response_model=dict[str, list[ThreadSummary]])
    def list_threads(
        slug: str,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, list[ThreadSummary]]:
        try:
            threads = service.list_threads(slug)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"threads": threads}

    @app.post("/api/superprojects/{slug}/checkpoints", response_model=PushCheckpointResponse)
    def push_checkpoint(
        slug: str,
        request: PushCheckpointRequest,
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> PushCheckpointResponse:
        if slug != request.checkpoint.superproject_slug:
            raise HTTPException(status_code=400, detail="Superproject slug mismatch.")
        try:
            return service.push_checkpoint(device_id, request)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.post("/api/superprojects/{slug}/mismatch-resolutions", response_model=MismatchResolution)
    def resolve_mismatch(
        slug: str,
        request: ResolveMismatchRequest,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> MismatchResolution:
        if slug != request.resolution.superproject_slug:
            raise HTTPException(status_code=400, detail="Superproject slug mismatch.")
        return service.record_mismatch_resolution(request.resolution)

    @app.post("/api/superprojects/{slug}/override", response_model=PushCheckpointResponse)
    def override_state(
        slug: str,
        request: PushCheckpointRequest,
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> PushCheckpointResponse:
        if slug != request.checkpoint.superproject_slug:
            raise HTTPException(status_code=400, detail="Superproject slug mismatch.")
        request = request.model_copy(update={"override": True})
        try:
            return service.push_checkpoint(device_id, request)
        except Exception as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.delete("/api/superprojects/{slug}")
    def delete_superproject(
        slug: str,
        force: bool = Query(False),
        device_id: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, object]:
        try:
            return service.delete_superproject(slug, requesting_device_id=device_id, force=force)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/api/superprojects/{slug}/threads/{thread_id}/checkpoint")
    def get_thread_checkpoint(
        slug: str,
        thread_id: str,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, object]:
        try:
            checkpoint = service.get_thread_checkpoint(slug, thread_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"checkpoint": checkpoint.model_dump(mode="json")}

    @app.post("/api/superprojects/{slug}/backups/{backup_id}/restore")
    def restore_backup(
        slug: str,
        backup_id: str,
        _: str = Depends(authenticate),
        service: ServerService = Depends(get_service),
    ) -> dict[str, object]:
        try:
            backup = service.restore_backup(slug, backup_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"backup": backup.model_dump(mode="json")}

    return app


app = create_app()
