from __future__ import annotations

import asyncio
import hmac
import logging
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

from core.access_manager import AccessKey, AccessManager, AccessUser
from core.accounts import AccountManager
from core.observability import (
    audit_event,
    list_log_files,
    process_uptime_seconds,
    tail_jsonl_file,
)
from core.queue import TaskQueue
from core.session_store import PostgresSessionStore

logger = logging.getLogger(__name__)


class AdminApiError(Exception):
    def __init__(self, message: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        super().__init__(message)
        self.status = int(status)


def _iso_datetime(value: float | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def _format_display_datetime(value: float | None) -> str | None:
    if not value:
        return None
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class AdminApiService:
    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        access_manager: AccessManager,
        account_manager: AccountManager,
        task_queue: TaskQueue,
        session_store: PostgresSessionStore | None,
        logs_dir: Path,
        token: str,
    ) -> None:
        self.loop = loop
        self.access_manager = access_manager
        self.account_manager = account_manager
        self.task_queue = task_queue
        self.session_store = session_store
        self.logs_dir = logs_dir
        self.token = token

    def run_coro(self, coro: Any, *, timeout: float = 30.0) -> Any:
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def health(self) -> dict[str, Any]:
        return {
            "ok": True,
            "uptime_seconds": process_uptime_seconds(),
            "logs": list_log_files(self.logs_dir),
        }

    def dashboard(self) -> dict[str, Any]:
        users: list[AccessUser] = self.run_coro(self.access_manager.list_users())
        keys: list[AccessKey] = self.run_coro(self.access_manager.list_keys())
        sessions: list[dict[str, Any]] = self.run_coro(
            self.account_manager.list_accounts_status(force_refresh=False),
            timeout=60.0,
        )
        tasks: list[dict[str, Any]] = self.run_coro(self.task_queue.list_tasks(limit=500))
        task_stats: dict[str, Any] = self.run_coro(self.task_queue.stats())

        today = datetime.now().date()
        tasks_done_today = 0
        for item in tasks:
            finished_at = item.get("finished_at")
            if not finished_at:
                continue
            if datetime.fromtimestamp(float(finished_at)).date() == today:
                tasks_done_today += 1

        return {
            "total_users": len(users),
            "total_keys": len(keys),
            "total_sessions": len(sessions),
            "total_tasks": len(tasks),
            "sessions_alive": sum(1 for item in sessions if item.get("health_status") == "alive"),
            "sessions_limited": sum(1 for item in sessions if item.get("health_status") == "limited"),
            "sessions_banned": sum(1 for item in sessions if item.get("health_status") == "banned"),
            "sessions_in_pool": sum(1 for item in sessions if bool(item.get("in_pool"))),
            "sessions_out_pool": sum(1 for item in sessions if not bool(item.get("in_pool"))),
            "tasks_active": int(task_stats.get("active_total", 0)),
            "tasks_done_today": tasks_done_today,
            "uptime_seconds": process_uptime_seconds(),
        }

    def users(self) -> list[dict[str, Any]]:
        users: list[AccessUser] = self.run_coro(self.access_manager.list_users())
        keys: list[AccessKey] = self.run_coro(self.access_manager.list_keys())
        sessions: list[dict[str, Any]] = self.run_coro(
            self.account_manager.list_accounts_status(force_refresh=False),
            timeout=60.0,
        )

        latest_key_by_user: dict[int, AccessKey] = {}
        for key in keys:
            current = latest_key_by_user.get(key.telegram_id)
            if current is None or (key.created_at or 0) > (current.created_at or 0):
                latest_key_by_user[key.telegram_id] = key

        sessions_by_owner: dict[int, int] = {}
        for item in sessions:
            owner_id = _safe_int(item.get("owner_id"))
            sessions_by_owner[owner_id] = sessions_by_owner.get(owner_id, 0) + 1

        result: list[dict[str, Any]] = []
        for user in users:
            latest_key = latest_key_by_user.get(user.telegram_id)
            if user.role in {"owner", "admin", "internal"}:
                session_count = sessions_by_owner.get(self.access_manager.owner_user_id, 0)
            else:
                session_count = sessions_by_owner.get(user.owner_scope_id, 0)

            display_name = user.username or f"id:{user.telegram_id}"
            if user.first_name:
                display_name = f"{user.first_name} ({display_name})"

            result.append(
                {
                    "telegram_id": str(user.telegram_id),
                    "display_name": display_name,
                    "role": user.role,
                    "tariff": user.tariff,
                    "status": user.status,
                    "owner_scope_id": str(user.owner_scope_id),
                    "key_issued": _format_display_datetime(latest_key.created_at) if latest_key else None,
                    "key_expires": _format_display_datetime(latest_key.expires_at) if latest_key else None,
                    "sessions": session_count,
                    "online": user.is_active,
                    "created_at": _iso_datetime(user.created_at),
                    "activated_at": _iso_datetime(user.activated_at),
                }
            )

        return result

    def keys(self) -> list[dict[str, Any]]:
        items: list[AccessKey] = self.run_coro(self.access_manager.list_keys())
        return [
            {
                "key": item.key,
                "issued_to": str(item.telegram_id),
                "role": item.role,
                "tariff": item.tariff,
                "issued_at": _format_display_datetime(item.created_at),
                "expires_at": _format_display_datetime(item.expires_at),
                "status": item.status,
                "activated_by_id": str(item.activated_by_id) if item.activated_by_id is not None else None,
            }
            for item in items
        ]

    def sessions(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = self.run_coro(
            self.account_manager.list_accounts_status(force_refresh=False),
            timeout=60.0,
        )

        result: list[dict[str, Any]] = []
        for item in rows:
            result.append(
                {
                    "id": item.get("id"),
                    "name": item.get("session"),
                    "owner": str(item.get("owner_id")),
                    "status": str(item.get("health_label", "")).lower(),
                    "state": str(item.get("account_state_label", "")).lower(),
                    "pool": "in_pool" if item.get("in_pool") else "out_of_pool",
                    "added": _format_display_datetime(item.get("last_checked_ts")),
                    "last_error": item.get("reason") or None,
                    "username": item.get("username"),
                    "first_name": item.get("first_name"),
                    "available_for_tasks": bool(item.get("available_for_tasks")),
                }
            )
        return result

    def tasks(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = self.run_coro(self.task_queue.list_tasks(limit=200))
        return [
            {
                "id": str(item.get("id")),
                "type": item.get("kind"),
                "title": item.get("title"),
                "started_by": str(item.get("requested_by_user_id")),
                "status": item.get("status"),
                "started_at": _format_display_datetime(item.get("started_at") or item.get("created_at")),
                "finished_at": _format_display_datetime(item.get("finished_at")),
                "result": item.get("result_text") or None,
                "progress": item.get("progress_text") or None,
                "queue_position": item.get("queue_position"),
            }
            for item in items
        ]

    def audit(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = tail_jsonl_file(self.logs_dir / "events.jsonl", limit=max(1, min(limit, 1000)))
        result: list[dict[str, Any]] = []
        for index, row in enumerate(reversed(rows), start=1):
            payload = row.get("extra", {}).get("payload", {}) if isinstance(row.get("extra"), dict) else {}
            event_type = row.get("extra", {}).get("event_type") if isinstance(row.get("extra"), dict) else None
            target = "-"
            if isinstance(payload, dict):
                target = (
                    payload.get("session")
                    or payload.get("task_id")
                    or payload.get("telegram_id")
                    or payload.get("key")
                    or "-"
                )

            result.append(
                {
                    "id": str(index),
                    "action": str(event_type or row.get("message") or "event"),
                    "who": str(payload.get("requested_by_user_id") or payload.get("owner_id") or "system")
                    if isinstance(payload, dict)
                    else "system",
                    "target": str(target),
                    "timestamp": str(row.get("ts") or ""),
                    "level": str(row.get("level") or "INFO"),
                    "message": str(row.get("message") or ""),
                }
            )
        return result

    def export_session(self, session_name: str) -> tuple[bytes, str, str]:
        normalized = session_name.replace(".session", "").strip()
        if not normalized:
            raise AdminApiError("Session name is required.", HTTPStatus.BAD_REQUEST)

        try:
            path = self.account_manager.session_file_path(normalized)
            payload = path.read_bytes()
            filename = path.name
        except ValueError:
            if self.session_store and self.session_store.enabled:
                payload = self.session_store.load_session_bytes(normalized)
                if payload:
                    filename = f"{normalized}.session"
                else:
                    raise AdminApiError("Session file was not found.", HTTPStatus.NOT_FOUND) from None
            else:
                raise AdminApiError("Session file was not found.", HTTPStatus.NOT_FOUND) from None

        audit_event(
            "admin.session_exported",
            message="Session exported from admin API",
            session=normalized,
            size=len(payload),
        )
        return payload, filename, "application/octet-stream"

    def command(self, payload: dict[str, Any]) -> dict[str, Any]:
        command_type = str(payload.get("type", "")).strip()
        data = payload.get("data") or {}
        if not command_type:
            raise AdminApiError("Command type is required.")
        if not isinstance(data, dict):
            raise AdminApiError("Command data must be an object.")

        if command_type == "clear_finished":
            removed = self.run_coro(self.task_queue.clear_finished())
            return {"ok": True, "message": f"Removed {removed} finished task(s)."}

        if command_type in {"pause_task", "resume_task", "cancel_task", "remove_task"}:
            task_id = _safe_int(data.get("task_id"))
            if task_id < 1:
                raise AdminApiError("Valid task_id is required.")
            action_map = {
                "pause_task": self.task_queue.pause_task,
                "resume_task": self.task_queue.resume_task,
                "cancel_task": self.task_queue.cancel_task,
                "remove_task": self.task_queue.remove_task,
            }
            ok, message = self.run_coro(action_map[command_type](task_id))
            status = HTTPStatus.OK if ok else HTTPStatus.BAD_REQUEST
            if not ok:
                raise AdminApiError(message, status)
            audit_event(
                f"admin.{command_type}",
                message=f"Admin command executed: {command_type}",
                task_id=task_id,
            )
            return {"ok": True, "message": message, "command_id": f"{command_type}:{task_id}"}

        if command_type == "run_task":
            raise AdminApiError(
                "run_task requires real task parameters and is intentionally blocked in admin API.",
                HTTPStatus.NOT_IMPLEMENTED,
            )

        if command_type == "export_session":
            session_id = str(data.get("session_id", "")).strip()
            if not session_id:
                raise AdminApiError("session_id is required.")
            return {
                "ok": True,
                "message": "Use the dedicated download endpoint for session export.",
                "download_url": f"/sessions/{session_id}/export",
                "command_id": f"export_session:{session_id}",
            }

        raise AdminApiError(f"Unsupported command: {command_type}", HTTPStatus.NOT_IMPLEMENTED)


def create_app(service: AdminApiService) -> FastAPI:
    app = FastAPI(title="BotoFerma Admin API", version="1.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.exception_handler(AdminApiError)
    async def handle_admin_error(_request: Request, exc: AdminApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.status, content={"detail": str(exc)})

    def require_admin_auth(request: Request) -> None:
        bearer = request.headers.get("Authorization", "")
        custom = request.headers.get("X-Admin-Token", "")
        candidate = custom.strip()
        if bearer.lower().startswith("bearer "):
            candidate = bearer[7:].strip()
        if not candidate or not hmac.compare_digest(candidate, service.token):
            raise AdminApiError("Unauthorized", HTTPStatus.UNAUTHORIZED)

    @app.get("/health")
    def get_health() -> dict[str, Any]:
        return service.health()

    @app.get("/auth/check")
    def get_auth_check(request: Request) -> dict[str, Any]:
        require_admin_auth(request)
        return {"ok": True}

    @app.get("/dashboard")
    def get_dashboard(request: Request) -> dict[str, Any]:
        require_admin_auth(request)
        return service.dashboard()

    @app.get("/users")
    def get_users(request: Request) -> list[dict[str, Any]]:
        require_admin_auth(request)
        return service.users()

    @app.get("/keys")
    def get_keys(request: Request) -> list[dict[str, Any]]:
        require_admin_auth(request)
        return service.keys()

    @app.get("/sessions")
    def get_sessions(request: Request) -> list[dict[str, Any]]:
        require_admin_auth(request)
        return service.sessions()

    @app.get("/tasks")
    def get_tasks(request: Request) -> list[dict[str, Any]]:
        require_admin_auth(request)
        return service.tasks()

    @app.get("/audit")
    def get_audit(request: Request, limit: int = Query(default=200, ge=1, le=1000)) -> list[dict[str, Any]]:
        require_admin_auth(request)
        return service.audit(limit=limit)

    @app.get("/sessions/{session_name}/export")
    def export_session(session_name: str, request: Request) -> Response:
        require_admin_auth(request)
        payload, filename, content_type = service.export_session(session_name)
        return Response(
            content=payload,
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.post("/commands")
    def post_command(payload: dict[str, Any], request: Request) -> dict[str, Any]:
        require_admin_auth(request)
        return service.command(payload)

    return app


class AdminApiServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        token: str,
        loop: asyncio.AbstractEventLoop,
        access_manager: AccessManager,
        account_manager: AccountManager,
        task_queue: TaskQueue,
        session_store: PostgresSessionStore | None,
        logs_dir: Path,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.token = token
        self._service = AdminApiService(
            loop=loop,
            access_manager=access_manager,
            account_manager=account_manager,
            task_queue=task_queue,
            session_store=session_store,
            logs_dir=logs_dir,
            token=self.token,
        )
        self._server = uvicorn.Server(
            uvicorn.Config(
                app=create_app(self._service),
                host=self.host,
                port=self.port,
                log_level="warning",
                access_log=False,
            )
        )
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._thread = threading.Thread(
            target=self._server.run,
            name="admin-api-server",
            daemon=True,
        )
        self._thread.start()
        audit_event(
            "admin.server_started",
            message="Admin API server started",
            host=self.host,
            port=self.port,
        )

    def stop(self) -> None:
        self._server.should_exit = True
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        audit_event(
            "admin.server_stopped",
            message="Admin API server stopped",
            host=self.host,
            port=self.port,
        )

