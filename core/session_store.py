from __future__ import annotations

import logging
from pathlib import Path

from core.observability import audit_event

logger = logging.getLogger(__name__)


class PostgresSessionStore:
    def __init__(self, database_url: str | None) -> None:
        self.database_url = (database_url or "").strip()

    @property
    def enabled(self) -> bool:
        return bool(self.database_url)

    def initialize(self) -> None:
        if not self.enabled:
            return
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS telethon_sessions (
                    session_name TEXT PRIMARY KEY,
                    file_data BYTEA NOT NULL,
                    file_size INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            conn.commit()
        audit_event("session_store.initialized", message="Postgres session store initialized")

    def save_session_file(self, session_name: str, file_path: Path) -> None:
        self.save_session_bytes(session_name, file_path.read_bytes())

    def save_session_bytes(self, session_name: str, file_data: bytes) -> None:
        if not self.enabled:
            return
        normalized = session_name.replace(".session", "").strip()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO telethon_sessions (session_name, file_data, file_size, created_at, updated_at)
                VALUES (%s, %s, %s, NOW(), NOW())
                ON CONFLICT (session_name) DO UPDATE
                SET file_data = EXCLUDED.file_data,
                    file_size = EXCLUDED.file_size,
                    updated_at = NOW()
                """,
                (normalized, file_data, len(file_data)),
            )
            conn.commit()
        audit_event(
            "session_store.saved",
            message="Session stored in Postgres",
            session=normalized,
            file_size=len(file_data),
        )

    def load_session_bytes(self, session_name: str) -> bytes | None:
        if not self.enabled:
            return None
        normalized = session_name.replace(".session", "").strip()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT file_data FROM telethon_sessions WHERE session_name = %s",
                (normalized,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return bytes(row[0])

    def list_session_names(self) -> list[str]:
        if not self.enabled:
            return []
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT session_name FROM telethon_sessions ORDER BY session_name")
            rows = cur.fetchall()
        return [str(item[0]) for item in rows]

    def delete_session(self, session_name: str) -> bool:
        if not self.enabled:
            return False
        normalized = session_name.replace(".session", "").strip()
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM telethon_sessions WHERE session_name = %s",
                (normalized,),
            )
            deleted = cur.rowcount > 0
            conn.commit()
        if deleted:
            audit_event(
                "session_store.deleted",
                message="Session removed from Postgres",
                session=normalized,
            )
        return deleted

    def hydrate_to_directory(self, target_dir: Path, *, overwrite: bool = False) -> int:
        if not self.enabled:
            return 0
        target_dir.mkdir(parents=True, exist_ok=True)
        restored = 0
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT session_name, file_data FROM telethon_sessions ORDER BY session_name")
            rows = cur.fetchall()
        for session_name, file_data in rows:
            path = target_dir / f"{session_name}.session"
            if path.exists() and not overwrite:
                continue
            path.write_bytes(bytes(file_data))
            restored += 1
        audit_event(
            "session_store.hydrated",
            message="Session files restored from Postgres",
            restored=restored,
            overwrite=overwrite,
            target_dir=str(target_dir),
        )
        return restored

    def sync_directory_to_db(self, source_dir: Path) -> int:
        if not self.enabled or not source_dir.exists():
            return 0
        synced = 0
        for path in sorted(source_dir.glob("*.session")):
            try:
                self.save_session_file(path.stem, path)
                synced += 1
            except Exception:
                logger.exception("Failed to sync session %s to Postgres", path.name)
        audit_event(
            "session_store.synced",
            message="Session files synced to Postgres",
            synced=synced,
            source_dir=str(source_dir),
        )
        return synced

    def _connect(self):
        if not self.enabled:
            raise RuntimeError("DATABASE_URL is not configured.")
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("Для Postgres session store нужен пакет psycopg.") from exc
        return psycopg.connect(self.database_url)
