from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class ServerDatabase:
    MIGRATIONS: tuple[tuple[int, str, str], ...] = (
        (
            1,
            "initial_core_schema",
            """
            CREATE TABLE IF NOT EXISTS server_config (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS devices (
                device_id TEXT PRIMARY KEY,
                device_name TEXT NOT NULL,
                secret_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS leases (
                resource_id TEXT PRIMARY KEY,
                device_id TEXT,
                acquired_at TEXT,
                last_heartbeat_at TEXT,
                heartbeat_timeout_seconds INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS superprojects (
                slug TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                revision INTEGER NOT NULL,
                manifest_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint_id TEXT PRIMARY KEY,
                superproject_slug TEXT NOT NULL,
                thread_id TEXT,
                revision INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                source_device_id TEXT NOT NULL,
                canonical INTEGER NOT NULL,
                base_revision INTEGER NOT NULL,
                turn_hashes_json TEXT NOT NULL,
                snapshot_hash TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mismatch_resolutions (
                resolution_id TEXT PRIMARY KEY,
                superproject_slug TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                chosen_source TEXT NOT NULL,
                base_revision INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backups (
                backup_id TEXT PRIMARY KEY,
                superproject_slug TEXT NOT NULL,
                thread_id TEXT,
                created_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_checkpoints_superproject_thread_revision
                ON checkpoints (superproject_slug, thread_id, revision DESC);

            CREATE INDEX IF NOT EXISTS idx_backups_superproject_created
                ON backups (superproject_slug, created_at DESC);
            """,
        ),
        (
            2,
            "thread_name_overrides",
            """
            CREATE TABLE IF NOT EXISTS thread_names (
                superproject_slug TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                custom_name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (superproject_slug, thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_thread_names_superproject_updated
                ON thread_names (superproject_slug, updated_at DESC);
            """,
        ),
        (
            3,
            "thread_metadata_cache",
            """
            CREATE TABLE IF NOT EXISTS thread_metadata (
                superproject_slug TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                cached_thread_name TEXT,
                cached_last_user_turn_preview TEXT,
                updated_at TEXT NOT NULL,
                revision INTEGER NOT NULL,
                source_device_id TEXT,
                PRIMARY KEY (superproject_slug, thread_id)
            );

            CREATE INDEX IF NOT EXISTS idx_thread_metadata_superproject_updated
                ON thread_metadata (superproject_slug, updated_at DESC);
            """,
        ),
    )

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def init_schema(self) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            applied_versions = {
                row["version"]
                for row in connection.execute("SELECT version FROM schema_migrations").fetchall()
            }
            for version, name, sql in self.MIGRATIONS:
                if version in applied_versions:
                    continue
                connection.executescript(sql)
                connection.execute(
                    """
                    INSERT INTO schema_migrations (version, name, applied_at)
                    VALUES (?, ?, ?)
                    """,
                (version, name, datetime.now(UTC).isoformat().replace("+00:00", "Z")),
                )
            connection.commit()

    def schema_version(self) -> int:
        with self.connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            row = connection.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
        if row is None or row["version"] is None:
            return 0
        return int(row["version"])

    def get_config(self, key: str, default: Any = None) -> Any:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value_json FROM server_config WHERE key = ?",
                (key,),
            ).fetchone()
        if row is None:
            return default
        return json.loads(row["value_json"])

    def set_config(self, key: str, value: Any) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO server_config (key, value_json)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json
                """,
                (key, json.dumps(value)),
            )
            connection.commit()
