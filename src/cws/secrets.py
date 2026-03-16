from __future__ import annotations

import base64
import json
from pathlib import Path

from cws.utils import atomic_write_text

try:
    import keyring
except Exception:  # pragma: no cover - optional dependency behavior
    keyring = None


class SecretStore:
    def __init__(self, namespace: str, fallback_path: Path) -> None:
        self.namespace = namespace
        self.fallback_path = fallback_path

    def get(self, key: str) -> str | None:
        if keyring is not None:
            value = keyring.get_password(self.namespace, key)
            if value is not None:
                return value
        if not self.fallback_path.exists():
            return None
        payload = json.loads(self.fallback_path.read_text(encoding="utf-8"))
        encoded = payload.get(key)
        if encoded is None:
            return None
        return base64.b64decode(encoded.encode("ascii")).decode("utf-8")

    def set(self, key: str, value: str) -> None:
        if keyring is not None:
            try:
                keyring.set_password(self.namespace, key, value)
                return
            except Exception:
                pass
        payload: dict[str, str] = {}
        if self.fallback_path.exists():
            payload = json.loads(self.fallback_path.read_text(encoding="utf-8"))
        payload[key] = base64.b64encode(value.encode("utf-8")).decode("ascii")
        atomic_write_text(self.fallback_path, json.dumps(payload, indent=2, sort_keys=True))

