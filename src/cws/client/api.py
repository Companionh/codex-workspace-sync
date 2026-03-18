from __future__ import annotations

import os
from typing import Any

import httpx

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
    SuperprojectManifest,
    ThreadSummary,
    ThreadCheckpoint,
)


class ApiClient:
    def __init__(self, server_url: str, device_id: str, device_secret: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.device_id = device_id
        self.device_secret = device_secret
        timeout_seconds = float(os.environ.get("CWS_HTTP_TIMEOUT_SECONDS", "300"))
        self.timeout = httpx.Timeout(
            timeout_seconds,
            connect=min(30.0, timeout_seconds),
            read=timeout_seconds,
            write=timeout_seconds,
            pool=timeout_seconds,
        )

    def _headers(self) -> dict[str, str]:
        return {
            "X-CWS-Device-Id": self.device_id,
            "X-CWS-Device-Secret": self.device_secret,
        }

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = httpx.request(
            method,
            f"{self.server_url}{path}",
            headers=self._headers(),
            timeout=self.timeout,
            **kwargs,
        )
        response.raise_for_status()
        return response

    def acquire_lease(self, steal: bool = False) -> AcquireLeaseResponse:
        payload = AcquireLeaseRequest(device_id=self.device_id, steal=steal)
        response = self._request("POST", "/api/lease/acquire", json=payload.model_dump(mode="json"))
        return AcquireLeaseResponse.model_validate(response.json())

    def heartbeat(self) -> HeartbeatResponse:
        payload = HeartbeatRequest(device_id=self.device_id)
        response = self._request("POST", "/api/lease/heartbeat", json=payload.model_dump(mode="json"))
        return HeartbeatResponse.model_validate(response.json())

    def release_lease(self) -> HeartbeatResponse:
        payload = HeartbeatRequest(device_id=self.device_id)
        response = self._request("POST", "/api/lease/release", json=payload.model_dump(mode="json"))
        return HeartbeatResponse.model_validate(response.json())

    def create_superproject(self, request: CreateSuperprojectRequest) -> CreateSuperprojectResponse:
        response = self._request("POST", "/api/superprojects", json=request.model_dump(mode="json"))
        return CreateSuperprojectResponse.model_validate(response.json())

    def rename_superproject(self, slug: str, name: str) -> RenameSuperprojectResponse:
        payload = RenameSuperprojectRequest(name=name)
        response = self._request(
            "POST",
            f"/api/superprojects/{slug}/rename",
            json=payload.model_dump(mode="json"),
        )
        return RenameSuperprojectResponse.model_validate(response.json())

    def pull_state(self, slug: str) -> PullStateResponse:
        response = self._request("GET", f"/api/superprojects/{slug}/state")
        return PullStateResponse.model_validate(response.json())

    def get_manifest(self, slug: str) -> SuperprojectManifest:
        try:
            response = self._request("GET", f"/api/superprojects/{slug}/manifest")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            return self.pull_state(slug).manifest
        return SuperprojectManifest.model_validate(response.json()["manifest"])

    def list_threads(self, slug: str) -> list[ThreadSummary]:
        try:
            response = self._request("GET", f"/api/superprojects/{slug}/threads")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            return [
                ThreadSummary(
                    thread_id=checkpoint.thread_id,
                    thread_name=(
                        (checkpoint.raw_bundle.thread_name if checkpoint.raw_bundle else None)
                        or checkpoint.summary
                        or checkpoint.thread_id
                    ),
                    updated_at=(
                        (checkpoint.raw_bundle.thread_updated_at if checkpoint.raw_bundle else None)
                        or checkpoint.created_at
                    ),
                    last_user_turn_preview=(
                        checkpoint.raw_bundle.last_user_turn_preview if checkpoint.raw_bundle else None
                    ),
                    tracked=True,
                    source="server",
                )
                for checkpoint in self.pull_state(slug).thread_checkpoints
                if checkpoint.thread_id
            ]
        return [ThreadSummary.model_validate(item) for item in response.json()["threads"]]

    def push_checkpoint(self, slug: str, request: PushCheckpointRequest) -> PushCheckpointResponse:
        response = self._request(
            "POST",
            f"/api/superprojects/{slug}/checkpoints",
            json=request.model_dump(mode="json"),
        )
        return PushCheckpointResponse.model_validate(response.json())

    def override_state(self, slug: str, request: PushCheckpointRequest) -> PushCheckpointResponse:
        response = self._request(
            "POST",
            f"/api/superprojects/{slug}/override",
            json=request.model_dump(mode="json"),
        )
        return PushCheckpointResponse.model_validate(response.json())

    def resolve_mismatch(self, slug: str, resolution: MismatchResolution) -> MismatchResolution:
        response = self._request(
            "POST",
            f"/api/superprojects/{slug}/mismatch-resolutions",
            json={"resolution": resolution.model_dump(mode="json")},
        )
        return MismatchResolution.model_validate(response.json())

    def shared_skills(self) -> list[dict[str, Any]]:
        response = self._request("GET", "/api/skills/shared")
        return response.json().get("artifacts", [])

    def get_thread_checkpoint(self, slug: str, thread_id: str) -> ThreadCheckpoint:
        response = self._request(
            "GET",
            f"/api/superprojects/{slug}/threads/{thread_id}/checkpoint",
        )
        return ThreadCheckpoint.model_validate(response.json()["checkpoint"])

    def delete_superproject(self, slug: str, *, force: bool = False) -> dict[str, Any]:
        response = self._request(
            "DELETE",
            f"/api/superprojects/{slug}",
            params={"force": str(force).lower()},
        )
        return response.json()
