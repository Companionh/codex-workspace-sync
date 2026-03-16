from __future__ import annotations

import base64
import hashlib
import hmac
import secrets


def hash_secret(secret: str, *, iterations: int = 200_000) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, iterations)
    return "pbkdf2_sha256${}${}${}".format(
        iterations,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_secret(stored_value: str, candidate: str) -> bool:
    algorithm, iterations, salt_b64, digest_b64 = stored_value.split("$", 3)
    if algorithm != "pbkdf2_sha256":
        raise ValueError(f"Unsupported secret hash algorithm: {algorithm}")
    salt = base64.b64decode(salt_b64.encode("ascii"))
    expected = base64.b64decode(digest_b64.encode("ascii"))
    actual = hashlib.pbkdf2_hmac(
        "sha256",
        candidate.encode("utf-8"),
        salt,
        int(iterations),
    )
    return hmac.compare_digest(expected, actual)

