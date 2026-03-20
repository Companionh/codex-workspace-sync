from __future__ import annotations

import base64
import ctypes
import json
import os
from pathlib import Path

from cws.utils import atomic_write_text

try:
    import keyring
except Exception:  # pragma: no cover - optional dependency behavior
    keyring = None


if os.name == "nt":  # pragma: no branch - windows-only fallback path
    from ctypes import wintypes

    _crypt32 = ctypes.windll.crypt32
    _kernel32 = ctypes.windll.kernel32

    class _DataBlob(ctypes.Structure):
        _fields_ = [
            ("cbData", wintypes.DWORD),
            ("pbData", ctypes.POINTER(ctypes.c_byte)),
        ]

    def _make_blob(data: bytes) -> tuple[_DataBlob, ctypes.Array[ctypes.c_char]]:
        buffer = ctypes.create_string_buffer(data)
        blob = _DataBlob(
            cbData=len(data),
            pbData=ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)),
        )
        return blob, buffer

    def _blob_to_bytes(blob: _DataBlob) -> bytes:
        if not blob.cbData or not blob.pbData:
            return b""
        return ctypes.string_at(blob.pbData, blob.cbData)

    def _dpapi_encrypt(data: bytes) -> bytes:
        input_blob, input_buffer = _make_blob(data)
        output_blob = _DataBlob()
        if not _crypt32.CryptProtectData(
            ctypes.byref(input_blob),
            "codex-workspace-sync",
            None,
            None,
            None,
            0,
            ctypes.byref(output_blob),
        ):
            raise RuntimeError("Windows DPAPI encryption failed.")
        try:
            return _blob_to_bytes(output_blob)
        finally:
            if output_blob.pbData:
                _kernel32.LocalFree(output_blob.pbData)
            del input_buffer

    def _dpapi_decrypt(data: bytes) -> bytes:
        input_blob, input_buffer = _make_blob(data)
        output_blob = _DataBlob()
        if not _crypt32.CryptUnprotectData(
            ctypes.byref(input_blob),
            None,
            None,
            None,
            None,
            0,
            ctypes.byref(output_blob),
        ):
            raise RuntimeError("Windows DPAPI decryption failed.")
        try:
            return _blob_to_bytes(output_blob)
        finally:
            if output_blob.pbData:
                _kernel32.LocalFree(output_blob.pbData)
            del input_buffer
else:
    def _dpapi_encrypt(data: bytes) -> bytes:
        raise RuntimeError("Windows DPAPI fallback is unavailable on this platform.")

    def _dpapi_decrypt(data: bytes) -> bytes:
        raise RuntimeError("Windows DPAPI fallback is unavailable on this platform.")


class SecretStore:
    WINDOWS_FALLBACK_FORMAT = "windows-dpapi-v1"

    def __init__(self, namespace: str, fallback_path: Path) -> None:
        self.namespace = namespace
        self.fallback_path = fallback_path

    def _load_fallback_payload(self) -> dict[str, object]:
        if not self.fallback_path.exists():
            return {"_format": self.WINDOWS_FALLBACK_FORMAT, "values": {}}
        payload = json.loads(self.fallback_path.read_text(encoding="utf-8"))
        if payload.get("_format") == self.WINDOWS_FALLBACK_FORMAT:
            payload.setdefault("values", {})
            return payload
        # Migrate legacy insecure payloads in memory on first write.
        migrated: dict[str, str] = {}
        for key, encoded in payload.items():
            if not isinstance(encoded, str):
                continue
            migrated[key] = encoded
        return {"_format": self.WINDOWS_FALLBACK_FORMAT, "values": migrated}

    def get(self, key: str) -> str | None:
        if keyring is not None:
            value = keyring.get_password(self.namespace, key)
            if value is not None:
                return value
        if not self.fallback_path.exists():
            return None
        payload = json.loads(self.fallback_path.read_text(encoding="utf-8"))
        if payload.get("_format") == self.WINDOWS_FALLBACK_FORMAT:
            values = payload.get("values", {})
            if not isinstance(values, dict):
                return None
            encoded = values.get(key)
            if encoded is None:
                return None
            decrypted = _dpapi_decrypt(base64.b64decode(str(encoded).encode("ascii")))
            return decrypted.decode("utf-8")
        encoded = payload.get(key)
        if encoded is None:
            return None
        return base64.b64decode(str(encoded).encode("ascii")).decode("utf-8")

    def set(self, key: str, value: str) -> None:
        if keyring is not None:
            try:
                keyring.set_password(self.namespace, key, value)
                return
            except Exception:
                pass
        if os.name != "nt":
            raise RuntimeError(
                "Secure secret storage is unavailable. Install a working keyring backend on this platform."
            )
        payload = self._load_fallback_payload()
        values = payload.setdefault("values", {})
        if not isinstance(values, dict):
            raise RuntimeError("Secret store payload is invalid.")
        values[key] = base64.b64encode(_dpapi_encrypt(value.encode("utf-8"))).decode("ascii")
        atomic_write_text(self.fallback_path, json.dumps(payload, indent=2, sort_keys=True))
