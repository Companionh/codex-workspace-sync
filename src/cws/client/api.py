from __future__ import annotations

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
    ThreadCheckpoint,
)


class ApiClient:
    def __init__(self, server_url: str, device_id: str, device_secret: str) -> None:
        self.server_url = server_url.rstrip("/")
        self.device_id = device_id
        self.device_secret = device_secret

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
            timeout=30.0,
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

    def pull_state(self, slug: str) -> PullStateResponse:
        response = self._request("GET", f"/api/superprojects/{slug}/state")
        return PullStateResponse.model_validate(response.json())

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
