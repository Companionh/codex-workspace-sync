from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", lowered)
    collapsed = re.sub(r"-{2,}", "-", normalized).strip("-")
    if not collapsed:
        raise ValueError("Cannot derive slug from empty value.")
    return collapsed


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def encode_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def decode_b64(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True)


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_path = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        for attempt in range(10):
            try:
                os.replace(temp_path, path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.05)
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def dump_json_file(path: Path, value: Any) -> None:
    atomic_write_text(path, json_dumps(value))


def is_relative_to(path: Path, base: Path) -> bool:
    try:
        path.resolve().relative_to(base.resolve())
        return True
    except ValueError:
        return False


def relative_posix(path: Path, base: Path) -> str:
    return path.resolve().relative_to(base.resolve()).as_posix()
