from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import sqlite3
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from telethon import TelegramClient
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest

from core.observability import audit_event

if TYPE_CHECKING:
    from core.session_store import PostgresSessionStore

logger = logging.getLogger(__name__)

ACCOUNT_STATE_ACTIVE = "ACTIVE"
ACCOUNT_STATE_LIMITED = "LIMITED"
ACCOUNT_STATE_DEAD = "DEAD"
TIMEOUT_LIMIT_THRESHOLD = 3
SPAMBOT_CACHE_TTL_SECONDS = 6 * 60 * 60


@dataclass(slots=True)
class ManagedClient:
    account_id: int
    owner_id: int
    session_name: str
    api_id: int
    api_hash: str
    client: TelegramClient
    session_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass(slots=True)
class HealthState:
    status: str = "unknown"
    reason: str = ""
    in_pool: bool = True
    dc_id: int | None = None
    last_checked_ts: float = 0.0
    consecutive_timeouts: int = 0


class AccountManager:
    def __init__(
        self,
        sessions_dir: Path,
        accounts_file: Path,
        shared_owner_id: int,
        default_api_id: int | None = None,
        default_api_hash: str | None = None,
        health_notifier: Callable[[str, int | None], Awaitable[None]] | None = None,
    ) -> None:
        self.sessions_dir = sessions_dir
        self.accounts_file = accounts_file
        self.shared_owner_id = int(shared_owner_id)
        self.default_api_id = int(default_api_id) if default_api_id is not None else None
        self.default_api_hash = str(default_api_hash).strip() if default_api_hash else None
        self._clients: list[ManagedClient] = []
        self._cursor = 0
        self._lock = asyncio.Lock()
        self._session_locks: dict[str, asyncio.Lock] = {}
        self._status_cache: list[dict] = []
        self._status_cache_ts = 0.0
        self._status_cache_ttl = 45.0
        self._profile_cache: dict[str, tuple[str, str, str, int | None]] = {}
        self._health_states: dict[str, HealthState] = {}
        self._auto_remove_from_pool = True
        self._health_notifications_enabled = True
        self._health_notifier = health_notifier
        self._health_monitor_task: asyncio.Task | None = None
        self._health_monitor_stop = asyncio.Event()
        self._spambot_cache: dict[str, tuple[str, str, float]] = {}

    @property
    def size(self) -> int:
        return len(self._clients)

    def set_health_notifier(self, notifier: Callable[[str, int | None], Awaitable[None]] | None) -> None:
        self._health_notifier = notifier

    async def start_health_monitor(self, interval_seconds: int = 90) -> None:
        if self._health_monitor_task and not self._health_monitor_task.done():
            return
        self._health_monitor_stop = asyncio.Event()
        self._health_monitor_task = asyncio.create_task(
            self._health_monitor_loop(interval_seconds),
            name="account-health-monitor",
        )

    async def stop_health_monitor(self) -> None:
        if not self._health_monitor_task:
            return
        self._health_monitor_stop.set()
        try:
            await self._health_monitor_task
        finally:
            self._health_monitor_task = None

    async def _health_monitor_loop(self, interval_seconds: int) -> None:
        while not self._health_monitor_stop.is_set():
            try:
                await self.list_accounts_status(force_refresh=True, include_spam_check=False)
            except Exception:
                logger.exception("Health monitor iteration failed")
            try:
                await asyncio.wait_for(self._health_monitor_stop.wait(), timeout=interval_seconds)
            except asyncio.TimeoutError:
                continue

    def health_settings(self) -> dict[str, bool]:
        return {
            "auto_remove_from_pool": self._auto_remove_from_pool,
            "notifications_enabled": self._health_notifications_enabled,
        }

    async def toggle_auto_remove_from_pool(self) -> bool:
        self._auto_remove_from_pool = not self._auto_remove_from_pool
        if self._auto_remove_from_pool:
            for state in self._health_states.values():
                if state.status in {"limited", "banned"}:
                    state.in_pool = False
            self._invalidate_status_cache()
        audit_event(
            "accounts.auto_remove_toggled",
            message="Auto remove from pool toggled",
            enabled=self._auto_remove_from_pool,
        )
        return self._auto_remove_from_pool

    async def toggle_health_notifications(self) -> bool:
        self._health_notifications_enabled = not self._health_notifications_enabled
        audit_event(
            "accounts.health_notifications_toggled",
            message="Health notifications toggled",
            enabled=self._health_notifications_enabled,
        )
        return self._health_notifications_enabled

    async def toggle_pool(self, session: str) -> bool:
        state = self._health_states.setdefault(session.replace(".session", "").strip(), HealthState())
        state.in_pool = not state.in_pool
        state.last_checked_ts = time.time()
        self._invalidate_status_cache()
        audit_event(
            "accounts.pool_toggled",
            message="Account pool membership toggled",
            session=session,
            in_pool=state.in_pool,
        )
        return state.in_pool

    async def mark_alive(self, session: str, *, dc_id: int | None = None) -> None:
        session_name = session.replace(".session", "").strip()
        state = self._health_states.setdefault(session_name, HealthState())
        previous_status = state.status
        previous_in_pool = state.in_pool
        state.status = "alive"
        state.reason = ""
        state.dc_id = dc_id if dc_id is not None else state.dc_id
        state.last_checked_ts = time.time()
        state.consecutive_timeouts = 0
        self._invalidate_status_cache()
        await self._maybe_notify_health_change(
            session_name=session_name,
            state=state,
            previous_status=previous_status,
            previous_in_pool=previous_in_pool,
        )

    async def mark_limited(self, session: str, reason: str, *, dc_id: int | None = None) -> None:
        await self._set_health_state(session, "limited", reason, dc_id=dc_id)

    async def mark_banned(self, session: str, reason: str, *, dc_id: int | None = None) -> None:
        await self._set_health_state(session, "banned", reason, dc_id=dc_id)

    async def apply_runtime_health(self, session: str, exc: Exception, *, dc_id: int | None = None) -> None:
        if self._is_timeout_exception(exc):
            await self._apply_timeout_health(session, exc, dc_id=dc_id)
            return
        status, reason = self._classify_health_exception(exc)
        if status == "alive":
            await self.mark_alive(session, dc_id=dc_id)
        elif status == "limited":
            await self.mark_limited(session, reason, dc_id=dc_id)
        else:
            await self.mark_banned(session, reason, dc_id=dc_id)

    async def load_clients(self) -> None:
        async with self._lock:
            await self._disconnect_all_unlocked()
            self._clients.clear()
            self._cursor = 0
            self._invalidate_status_cache()
            config = self._load_accounts_config()
            default_api_id = config.get("default", {}).get("api_id") or self.default_api_id
            default_api_hash = config.get("default", {}).get("api_hash") or self.default_api_hash
            account_entries = config.get("accounts", [])
            if not account_entries:
                account_entries = [
                    {"id": index, "owner_id": self.shared_owner_id, "session": path.stem}
                    for index, path in enumerate(sorted(self.sessions_dir.glob("*.session")), start=1)
                ]
            for entry in account_entries:
                managed = self._build_managed_client(
                    entry,
                    default_api_id=default_api_id,
                    default_api_hash=default_api_hash,
                )
                if managed is None:
                    continue
                self._clients.append(managed)
                logger.info("Registered session: %s", managed.session_name)
            alive_sessions = {item.session_name for item in self._clients}
            self._profile_cache = {k: v for k, v in self._profile_cache.items() if k in alive_sessions}
            self._health_states = {session: self._health_states.get(session, HealthState()) for session in alive_sessions}
            self._spambot_cache = {session: self._spambot_cache.get(session, ("", "", 0.0)) for session in alive_sessions if session in self._spambot_cache}
            logger.info("Registered %s session(s).", len(self._clients))
            audit_event(
                "accounts.loaded",
                message="Accounts loaded from runtime config",
                sessions_total=len(self._clients),
                sessions_dir=str(self.sessions_dir),
                accounts_file=str(self.accounts_file),
            )

    def accounts_total(self) -> int:
        return len(self._load_accounts_config().get("accounts", []))

    def has_account_session(self, session: str) -> bool:
        session_name = session.replace(".session", "").strip()
        if not session_name:
            return False
        return any(
            str(item.get("session", "")).replace(".session", "").strip() == session_name
            for item in self._load_accounts_config().get("accounts", [])
        )

    async def next_client(self, owner_ids: set[int] | None = None) -> TelegramClient:
        return (await self.next_managed_client(owner_ids=owner_ids)).client

    async def next_managed_client(self, owner_ids: set[int] | None = None) -> ManagedClient:
        return (await self.get_authorized_clients(limit=1, owner_ids=owner_ids))[0]

    async def get_authorized_clients(
        self,
        limit: int | None = None,
        owner_ids: set[int] | None = None,
    ) -> list[ManagedClient]:
        if not self._clients:
            raise RuntimeError("Нет доступных Telethon-клиентов.")
        if limit is not None and limit < 1:
            raise ValueError("Лимит аккаунтов должен быть не меньше 1.")
        pool_candidates = [
            managed
            for managed in self._filter_clients(owner_ids)
            if self._health_states.setdefault(managed.session_name, HealthState()).in_pool
            and self._is_available_for_tasks(managed.session_name)
        ]
        if not pool_candidates:
            raise RuntimeError("РќРµС‚ РґРѕСЃС‚СѓРїРЅС‹С… ACTIVE-Р°РєРєР°СѓРЅС‚РѕРІ РІ РїСѓР»Рµ.")
        target_count = len(pool_candidates) if limit is None else min(limit, len(pool_candidates))
        selected: list[ManagedClient] = []
        candidates = list(pool_candidates)
        random.shuffle(candidates)
        for managed in candidates:
            if len(selected) >= target_count:
                break
            try:
                if not managed.client.is_connected():
                    await asyncio.wait_for(managed.client.connect(), timeout=10)
                if not await asyncio.wait_for(managed.client.is_user_authorized(), timeout=8):
                    logger.warning("Session is not authorized: %s", managed.session_name)
                    await self.mark_banned(managed.session_name, "Сессия не авторизована.")
                    continue
                await self.mark_alive(managed.session_name, dc_id=self._extract_dc_id(managed))
                selected.append(managed)
            except Exception as exc:
                logger.warning("Failed to prepare session %s: %s", managed.session_name, exc)
                await self.apply_runtime_health(managed.session_name, exc, dc_id=self._extract_dc_id(managed))
        if not selected:
            raise RuntimeError("Нет доступных авторизованных Telethon-клиентов.")
        return selected

    async def list_accounts_status(
        self,
        *,
        force_refresh: bool = False,
        progress_cb: Callable[[int, int, str], Awaitable[None]] | None = None,
        owner_ids: set[int] | None = None,
        include_spam_check: bool = False,
    ) -> list[dict]:
        now = time.monotonic()
        if owner_ids is None and not force_refresh and self._status_cache and now - self._status_cache_ts <= self._status_cache_ttl:
            return [dict(item) for item in self._status_cache]
        if owner_ids is not None and not force_refresh and self._status_cache and now - self._status_cache_ts <= self._status_cache_ttl:
            return self._filter_rows(self._status_cache, owner_ids)
        rows: list[dict] = []
        visible_clients = self._filter_clients(owner_ids)
        total = len(visible_clients)
        for index, managed in enumerate(visible_clients, start=1):
            row = await self._collect_single_status(index, managed, include_spam_check=include_spam_check)
            rows.append(row)
            if progress_cb is not None:
                await progress_cb(index, total, f"{row['username']} - {row['first_name']}")
        if owner_ids is None:
            self._status_cache = [dict(item) for item in rows]
            self._status_cache_ts = time.monotonic()
        return rows

    def get_cached_statuses(self) -> list[dict]:
        return [dict(item) for item in self._status_cache]

    def build_health_summary(self, rows: list[dict]) -> dict[str, int]:
        counter = Counter(item.get("health_status", "unknown") for item in rows)
        in_pool = sum(1 for item in rows if item.get("in_pool", True))
        return {
            "total": len(rows),
            "alive": counter.get("alive", 0),
            "limited": counter.get("limited", 0),
            "banned": counter.get("banned", 0),
            "in_pool": in_pool,
        }

    def build_dc_summary(self, rows: list[dict]) -> list[tuple[str, int]]:
        counter: Counter[str] = Counter()
        for item in rows:
            dc_id = item.get("dc_id")
            label = f"DC {dc_id}" if dc_id is not None else "DC ?"
            counter[label] += 1
        return sorted(counter.items(), key=lambda item: item[0])

    async def relogin_sessions(self) -> dict[str, int]:
        await self.load_clients()
        rows = await self.list_accounts_status(force_refresh=True)
        summary = self.build_health_summary(rows)
        audit_event("accounts.relogin_completed", message="Sessions reloaded from files", summary=summary)
        return summary

    async def mark_session_dialogs_read(self, session: str) -> dict[str, int]:
        managed = self._find_managed_by_session(session)
        summary = {"dialogs_marked": 0, "unread_messages": 0}
        try:
            client = managed.client
            if not client.is_connected():
                await client.connect()
            if not await client.is_user_authorized():
                raise ValueError("Сессия не авторизована.")
            async for dialog in client.iter_dialogs():
                unread_count = int(getattr(dialog, "unread_count", 0) or 0)
                unread_mentions = int(getattr(dialog, "unread_mentions_count", 0) or 0)
                if unread_count < 1 and unread_mentions < 1:
                    continue
                top_message = getattr(dialog, "message", None)
                max_id = int(getattr(top_message, "id", 0) or 0)
                await client.send_read_acknowledge(dialog.input_entity, max_id=max_id or None, clear_mentions=True)
                summary["dialogs_marked"] += 1
                summary["unread_messages"] += unread_count
            await self.mark_alive(managed.session_name, dc_id=self._extract_dc_id(managed))
            audit_event("accounts.read_session_completed", message="Session dialogs marked as read", session=managed.session_name, **summary)
            return summary
        except Exception as exc:
            await self.apply_runtime_health(managed.session_name, exc, dc_id=self._extract_dc_id(managed))
            logger.exception("Failed to mark dialogs as read for session %s", managed.session_name)
            raise

    async def mark_all_dialogs_read(self, *, owner_ids: set[int] | None = None) -> dict[str, int]:
        visible_clients = self._filter_clients(owner_ids)
        if not visible_clients:
            raise RuntimeError("Нет доступных аккаунтов для прочтения.")
        summary = {"accounts_total": len(visible_clients), "accounts_processed": 0, "accounts_failed": 0, "dialogs_marked": 0, "unread_messages": 0}
        for managed in visible_clients:
            try:
                client = managed.client
                if not client.is_connected():
                    await client.connect()
                if not await client.is_user_authorized():
                    await self.mark_banned(managed.session_name, "Сессия не авторизована.", dc_id=self._extract_dc_id(managed))
                    summary["accounts_failed"] += 1
                    continue
                dialogs_marked = 0
                unread_messages = 0
                async for dialog in client.iter_dialogs():
                    unread_count = int(getattr(dialog, "unread_count", 0) or 0)
                    unread_mentions = int(getattr(dialog, "unread_mentions_count", 0) or 0)
                    if unread_count < 1 and unread_mentions < 1:
                        continue
                    top_message = getattr(dialog, "message", None)
                    max_id = int(getattr(top_message, "id", 0) or 0)
                    await client.send_read_acknowledge(dialog.input_entity, max_id=max_id or None, clear_mentions=True)
                    dialogs_marked += 1
                    unread_messages += unread_count
                summary["accounts_processed"] += 1
                summary["dialogs_marked"] += dialogs_marked
                summary["unread_messages"] += unread_messages
                await self.mark_alive(managed.session_name, dc_id=self._extract_dc_id(managed))
            except Exception as exc:
                summary["accounts_failed"] += 1
                await self.apply_runtime_health(managed.session_name, exc, dc_id=self._extract_dc_id(managed))
                logger.exception("Failed to mark dialogs as read for session %s", managed.session_name)
        audit_event("accounts.read_all_completed", message="All dialogs marked as read", owner_ids=sorted(owner_ids) if owner_ids else None, **summary)
        return summary

    async def ensure_account_entry(self, session: str, api_id: int | None = None, api_hash: str | None = None, owner_id: int | None = None) -> None:
        session_name = session.replace(".session", "").strip()
        if not session_name:
            raise ValueError("Нужно указать имя сессии.")
        session_file = self.sessions_dir / f"{session_name}.session"
        if not session_file.exists():
            raise ValueError(f"Файл сессии не найден: {session_file}")
        config = self._load_accounts_config()
        config.setdefault("default", {})
        entries = config.setdefault("accounts", [])
        if any(str(item.get("session", "")).replace(".session", "").strip() == session_name for item in entries):
            return
        default_api_id = api_id if api_id is not None else config.get("default", {}).get("api_id") or self.default_api_id
        default_api_hash = api_hash if api_hash is not None else config.get("default", {}).get("api_hash") or self.default_api_hash
        if not default_api_id or not default_api_hash:
            raise ValueError("Не найдены default api_id/api_hash ни в env, ни в accounts.json.")
        entries.append({
            "id": self._next_account_id(entries),
            "owner_id": int(owner_id or self.shared_owner_id),
            "session": session_name,
            "api_id": int(default_api_id),
            "api_hash": str(default_api_hash),
            **self._initial_lifecycle_fields(),
        })
        self._save_accounts_config(config)
        async with self._lock:
            if not any(managed.session_name == session_name for managed in self._clients):
                managed = self._build_managed_client(
                    entries[-1],
                    default_api_id=config.get("default", {}).get("api_id") or self.default_api_id,
                    default_api_hash=config.get("default", {}).get("api_hash") or self.default_api_hash,
                )
                if managed is not None:
                    self._clients.append(managed)
                    self._cursor = min(self._cursor, len(self._clients))
                    self._invalidate_status_cache()
                    logger.info("Registered session: %s", managed.session_name)
        audit_event("accounts.entry_created", message="Account entry created from existing session", session=session_name, owner_id=int(owner_id or self.shared_owner_id))

    def session_file_path(self, session: str) -> Path:
        session_name = session.replace(".session", "").strip()
        path = self.sessions_dir / f"{session_name}.session"
        if not path.exists():
            raise ValueError("Р¤Р°Р№Р» session РЅРµ РЅР°Р№РґРµРЅ.")
        return path

    def get_account_owner_id(self, session: str) -> int:
        session_name = session.replace(".session", "").strip()
        entry = self._find_config_entry(session_name)
        if entry is not None:
            return int(entry.get("owner_id", self.shared_owner_id))
        if self._session_exists_locally(session_name) or any(managed.session_name == session_name for managed in self._clients):
            return self.shared_owner_id
        raise ValueError("РђРєРєР°СѓРЅС‚ РЅРµ РЅР°Р№РґРµРЅ.")

    async def add_account(self, session: str, api_id: int, api_hash: str, *, owner_id: int | None = None) -> None:
        session_name = session.replace(".session", "").strip()
        if not session_name:
            raise ValueError("Нужно указать имя сессии.")
        session_file = self.sessions_dir / f"{session_name}.session"
        if not session_file.exists():
            raise ValueError(f"Файл сессии не найден: {session_file}")
        config = self._load_accounts_config()
        config.setdefault("default", {})
        entries = config.setdefault("accounts", [])
        if any(str(item.get("session", "")).replace(".session", "") == session_name for item in entries):
            raise ValueError("РђРєРєР°СѓРЅС‚ СѓР¶Рµ СЃСѓС‰РµСЃС‚РІСѓРµС‚.")
        entries.append({
            "id": self._next_account_id(entries),
            "owner_id": int(owner_id or self.shared_owner_id),
            "session": session_name,
            "api_id": int(api_id),
            "api_hash": api_hash.strip(),
            **self._initial_lifecycle_fields(),
        })
        self._save_accounts_config(config)
        await self.load_clients()
        audit_event("accounts.created", message="Account added", session=session_name, owner_id=int(owner_id or self.shared_owner_id))

    async def edit_account(self, session: str, *, new_session: str | None = None, new_api_id: int | None = None, new_api_hash: str | None = None) -> None:
        session_name = session.replace(".session", "").strip()
        config = self._load_accounts_config()
        entries = config.setdefault("accounts", [])
        target = None
        for item in entries:
            item_name = str(item.get("session", "")).replace(".session", "").strip()
            if item_name == session_name:
                target = item
                break
        if target is None:
            raise ValueError("Запись аккаунта не найдена в accounts.json.")
        if new_session:
            clean_session = new_session.replace(".session", "").strip()
            if not (self.sessions_dir / f"{clean_session}.session").exists():
                raise ValueError("Новый файл сессии не существует.")
            target["session"] = clean_session
        if new_api_id is not None:
            target["api_id"] = int(new_api_id)
        if new_api_hash is not None:
            cleaned_hash = new_api_hash.strip()
            if not cleaned_hash:
                raise ValueError("api_hash РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ РїСѓСЃС‚С‹Рј.")
            target["api_hash"] = cleaned_hash
        self._save_accounts_config(config)
        await self.load_clients()
        audit_event("accounts.edited", message="Account edited", session=session_name, new_session=new_session, api_id_updated=new_api_id is not None, api_hash_updated=new_api_hash is not None)

    async def delete_account(self, session: str, *, session_store: PostgresSessionStore | None = None, delete_session_file: bool = True) -> dict[str, int]:
        summary = await self.delete_accounts([session], session_store=session_store, delete_session_file=delete_session_file)
        if summary["accounts_deleted"] < 1:
            raise ValueError("Запись аккаунта не найдена в accounts.json.")
        return summary

    async def delete_accounts(self, sessions: list[str], *, session_store: PostgresSessionStore | None = None, delete_session_file: bool = True) -> dict[str, int]:
        normalized_sessions = self._normalize_session_names(sessions)
        if not normalized_sessions:
            raise ValueError("Не выбраны аккаунты для удаления.")
        config = self._load_accounts_config()
        entries = config.setdefault("accounts", [])
        existing_sessions = {str(item.get("session", "")).replace(".session", "").strip() for item in entries}
        target_sessions = [session_name for session_name in normalized_sessions if session_name in existing_sessions or self._session_exists_locally(session_name) or any(managed.session_name == session_name for managed in self._clients)]
        if not target_sessions:
            raise ValueError("Аккаунты не найдены ни в accounts.json, ни среди локальных session-файлов.")
        target_set = set(target_sessions)
        async with self._lock:
            managed_to_disconnect = [managed for managed in self._clients if managed.session_name in target_set]
            for managed in managed_to_disconnect:
                await self._disconnect_managed_client(managed)
            self._clients = [managed for managed in self._clients if managed.session_name not in target_set]
            self._cursor = min(self._cursor, len(self._clients))
            self._invalidate_status_cache()
        deleted_from_store = 0
        if session_store and session_store.enabled:
            for session_name in target_sessions:
                if await asyncio.to_thread(session_store.delete_session, session_name):
                    deleted_from_store += 1
        deleted_files = 0
        if delete_session_file:
            for session_name in target_sessions:
                path = self.sessions_dir / f"{session_name}.session"
                if path.exists():
                    path.unlink()
                    deleted_files += 1
        config["accounts"] = [item for item in entries if str(item.get("session", "")).replace(".session", "").strip() not in target_set]
        self._save_accounts_config(config)
        for session_name in target_sessions:
            self._profile_cache.pop(session_name, None)
            self._health_states.pop(session_name, None)
        await self.load_clients()
        summary = {
            "accounts_deleted": len(target_sessions),
            "config_entries_deleted": len([name for name in target_sessions if name in existing_sessions]),
            "session_files_deleted": deleted_files,
            "store_records_deleted": deleted_from_store,
        }
        audit_event("accounts.deleted", message="Accounts deleted", sessions=target_sessions, **summary)
        return summary

    async def update_first_name(self, session: str, first_name: str) -> None:
        client = await self._authorized_client_for_session(session)
        value = first_name.strip()
        if not value:
            raise ValueError("РРјСЏ РЅРµ РјРѕР¶РµС‚ Р±С‹С‚СЊ РїСѓСЃС‚С‹Рј.")
        await client(UpdateProfileRequest(first_name=value))
        self._invalidate_status_cache()
        audit_event("accounts.profile_first_name_updated", message="Account first name updated", session=session)

    async def update_last_name(self, session: str, last_name: str) -> None:
        client = await self._authorized_client_for_session(session)
        value = "" if last_name.strip() == "-" else last_name.strip()
        await client(UpdateProfileRequest(last_name=value))
        self._invalidate_status_cache()
        audit_event("accounts.profile_last_name_updated", message="Account last name updated", session=session)

    async def update_bio(self, session: str, about: str) -> None:
        client = await self._authorized_client_for_session(session)
        value = "" if about.strip() == "-" else about.strip()
        await client(UpdateProfileRequest(about=value))
        audit_event("accounts.profile_bio_updated", message="Account bio updated", session=session)

    async def update_username(self, session: str, username: str) -> None:
        client = await self._authorized_client_for_session(session)
        value = username.strip()
        if value in {"-", "none", "None"}:
            value = ""
        if value.startswith("@"):
            value = value[1:]
        await client(UpdateUsernameRequest(username=value))
        self._invalidate_status_cache()
        audit_event("accounts.profile_username_updated", message="Account username updated", session=session)

    async def update_avatar(self, session: str, image_path: Path) -> None:
        client = await self._authorized_client_for_session(session)
        if not image_path.exists():
            raise ValueError("Файл изображения не найден.")
        uploaded = await client.upload_file(str(image_path))
        await client(UploadProfilePhotoRequest(file=uploaded))
        audit_event("accounts.profile_avatar_updated", message="Account avatar updated", session=session, image_path=str(image_path))

    async def update_birthday(self, session: str, day: int, month: int, year: int) -> None:
        client = await self._authorized_client_for_session(session)
        if not (1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100):
            raise ValueError("Неверная дата рождения.")
        try:
            from telethon.tl.functions.account import UpdateBirthdayRequest
            from telethon.tl.types import Birthday
        except Exception as exc:
            raise ValueError("В этой версии Telethon недоступно изменение даты рождения.") from exc
        await client(UpdateBirthdayRequest(birthday=Birthday(day=day, month=month, year=year)))
        audit_event("accounts.profile_birthday_updated", message="Account birthday updated", session=session, birthday=f"{year:04d}-{month:02d}-{day:02d}")

    async def disconnect_all(self) -> None:
        async with self._lock:
            await self._disconnect_all_unlocked()

    async def _disconnect_all_unlocked(self) -> None:
        for managed in self._clients:
            await self._disconnect_managed_client(managed)

    async def _disconnect_managed_client(self, managed: ManagedClient) -> None:
        disconnect_error: Exception | None = None
        async with managed.session_lock:
            for attempt in range(3):
                try:
                    await managed.client.disconnect()
                    disconnect_error = None
                    break
                except sqlite3.OperationalError as exc:
                    disconnect_error = exc
                    error_text = str(exc).lower()
                    if "no such table" in error_text:
                        logger.warning("Skipping Telethon state flush for corrupted session %s: %s", managed.session_name, exc)
                        disconnect_error = None
                        break
                    if "database is locked" not in error_text or attempt == 2:
                        break
                    await asyncio.sleep(0.35 * (attempt + 1))
                except Exception as exc:
                    disconnect_error = exc
                    break
            try:
                managed.client.session.close()
            except Exception:
                logger.debug("Failed to close session handle for %s", managed.session_name, exc_info=True)
        if disconnect_error is not None:
            logger.exception("Error while disconnecting %s", managed.session_name, exc_info=disconnect_error)

    def _load_accounts_config(self) -> dict:
        if not self.accounts_file.exists():
            self.accounts_file.parent.mkdir(parents=True, exist_ok=True)
            bootstrap = {"default": {}, "accounts": []}
            if self.default_api_id is not None and self.default_api_hash:
                bootstrap["default"] = {"api_id": self.default_api_id, "api_hash": self.default_api_hash}
            self._save_accounts_config(bootstrap)
            return bootstrap
        raw = json.loads(self.accounts_file.read_text(encoding="utf-8-sig"))
        normalized = self._normalize_accounts_config(raw)
        if normalized != raw:
            self._save_accounts_config(normalized)
        return normalized

    def _save_accounts_config(self, data: dict) -> None:
        self.accounts_file.parent.mkdir(parents=True, exist_ok=True)
        self.accounts_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def _find_managed_by_session(self, session: str) -> ManagedClient:
        session_name = session.replace(".session", "").strip()
        for managed in self._clients:
            if managed.session_name == session_name:
                return managed
        raise ValueError("РђРєРєР°СѓРЅС‚ РЅРµ РЅР°Р№РґРµРЅ.")

    def _filter_clients(self, owner_ids: set[int] | None) -> list[ManagedClient]:
        if owner_ids is None:
            return list(self._clients)
        normalized_owner_ids = {int(item) for item in owner_ids}
        return [managed for managed in self._clients if managed.owner_id in normalized_owner_ids]

    def _filter_rows(self, rows: list[dict], owner_ids: set[int] | None) -> list[dict]:
        if owner_ids is None:
            return [dict(item) for item in rows]
        normalized_owner_ids = {int(item) for item in owner_ids}
        return self._reindex_rows([dict(item) for item in rows if int(item.get("owner_id", self.shared_owner_id)) in normalized_owner_ids])

    @staticmethod
    def _reindex_rows(rows: list[dict]) -> list[dict]:
        reindexed: list[dict] = []
        for index, item in enumerate(rows, start=1):
            row = dict(item)
            row["index"] = index
            reindexed.append(row)
        return reindexed

    def _next_account_id(self, entries: list[dict]) -> int:
        max_id = 0
        for item in entries:
            try:
                max_id = max(max_id, int(item.get("id", 0)))
            except (TypeError, ValueError):
                continue
        return max_id + 1

    def _normalize_accounts_config(self, data: dict) -> dict:
        if not isinstance(data, dict):
            raise ValueError("accounts.json должен содержать JSON-объект.")
        default = data.get("default") if isinstance(data.get("default"), dict) else {}
        normalized_accounts: list[dict] = []
        used_ids: set[int] = set()
        next_id = 1
        raw_accounts = data.get("accounts", [])
        if not isinstance(raw_accounts, list):
            raw_accounts = []
        for item in raw_accounts:
            if not isinstance(item, dict):
                continue
            session_name = str(item.get("session", "")).replace(".session", "").strip()
            if not session_name:
                continue
            try:
                raw_id = int(item.get("id", 0))
            except (TypeError, ValueError):
                raw_id = 0
            if raw_id < 1 or raw_id in used_ids:
                while next_id in used_ids:
                    next_id += 1
                raw_id = next_id
            used_ids.add(raw_id)
            next_id = max(next_id, raw_id + 1)
            try:
                owner_id = int(item.get("owner_id", self.shared_owner_id))
            except (TypeError, ValueError):
                owner_id = self.shared_owner_id
            normalized_item = {"id": raw_id, "owner_id": owner_id, "session": session_name, "state": self._normalize_account_state(item.get("state"))}
            api_id = item.get("api_id")
            if api_id not in {None, ""}:
                try:
                    normalized_item["api_id"] = int(api_id)
                except (TypeError, ValueError):
                    pass
            api_hash = str(item.get("api_hash", "")).strip()
            if api_hash:
                normalized_item["api_hash"] = api_hash
            normalized_accounts.append(normalized_item)
        return {"default": default, "accounts": normalized_accounts}

    def _session_lock_for(self, session_name: str) -> asyncio.Lock:
        normalized = session_name.replace(".session", "").strip()
        lock = self._session_locks.get(normalized)
        if lock is None:
            lock = asyncio.Lock()
            self._session_locks[normalized] = lock
        return lock

    def _build_managed_client(self, entry: dict, *, default_api_id: int | None, default_api_hash: str | None) -> ManagedClient | None:
        account_id = int(entry.get("id", 0))
        owner_id = int(entry.get("owner_id", self.shared_owner_id))
        session_name = str(entry.get("session", "")).replace(".session", "").strip()
        if not session_name:
            logger.warning("Skipped account entry without session name: %s", entry)
            return None
        api_id = entry.get("api_id", default_api_id)
        api_hash = entry.get("api_hash", default_api_hash)
        if not api_id or not api_hash:
            logger.warning("Skipped %s: api_id/api_hash not set.", session_name)
            return None
        session_sqlite = self.sessions_dir / f"{session_name}.session"
        if not session_sqlite.exists():
            logger.warning("Session file is missing: %s", session_sqlite)
            return None
        client = TelegramClient(str(self.sessions_dir / session_name), int(api_id), str(api_hash))
        return ManagedClient(account_id=account_id, owner_id=owner_id, session_name=session_name, api_id=int(api_id), api_hash=str(api_hash), client=client, session_lock=self._session_lock_for(session_name))

    async def _authorized_client_for_session(self, session: str) -> TelegramClient:
        managed = self._find_managed_by_session(session)
        if not self._is_available_for_tasks(managed.session_name):
            raise ValueError("РђРєРєР°СѓРЅС‚ РЅРµРґРѕСЃС‚СѓРїРµРЅ РґР»СЏ Р·Р°РґР°С‡.")
        if not managed.client.is_connected():
            await managed.client.connect()
        if not await managed.client.is_user_authorized():
            raise ValueError("Сессия не авторизована.")
        return managed.client

    def _invalidate_status_cache(self) -> None:
        self._status_cache = []
        self._status_cache_ts = 0.0

    def _initial_lifecycle_fields(self) -> dict[str, object]:
        return {"state": ACCOUNT_STATE_ACTIVE}

    def _find_config_entry(self, session: str) -> dict | None:
        session_name = session.replace(".session", "").strip()
        for item in self._load_accounts_config().get("accounts", []):
            if str(item.get("session", "")).replace(".session", "").strip() == session_name:
                return item
        return None

    def _is_available_for_tasks(self, session: str) -> bool:
        session_name = session.replace(".session", "").strip()
        entry = self._find_config_entry(session_name)
        if entry is None:
            if not any(managed.session_name == session_name for managed in self._clients):
                return False
            health_status = self._health_states.setdefault(session_name, HealthState()).status
            return self._effective_account_state(ACCOUNT_STATE_ACTIVE, health_status) == ACCOUNT_STATE_ACTIVE
        health_status = self._health_states.setdefault(session_name, HealthState()).status
        return self._effective_account_state(str(entry.get("state", ACCOUNT_STATE_ACTIVE)).upper(), health_status) == ACCOUNT_STATE_ACTIVE

    def _account_lifecycle_snapshot(self, session: str, *, health_status: str) -> dict[str, object]:
        entry = self._find_config_entry(session)
        if entry is None:
            has_runtime_session = any(managed.session_name == session.replace(".session", "").strip() for managed in self._clients)
            raw_state = ACCOUNT_STATE_ACTIVE if has_runtime_session else ACCOUNT_STATE_DEAD
            effective_state = self._effective_account_state(raw_state, health_status)
            return {
                "state": effective_state,
                "raw_state": raw_state,
                "state_label": self._account_state_label(effective_state),
                "state_icon": self._account_state_icon(effective_state),
                "available_for_tasks": effective_state == ACCOUNT_STATE_ACTIVE,
            }
        raw_state = str(entry.get("state", ACCOUNT_STATE_ACTIVE)).upper()
        effective_state = self._effective_account_state(raw_state, health_status)
        return {"state": effective_state, "raw_state": raw_state, "state_label": self._account_state_label(effective_state), "state_icon": self._account_state_icon(effective_state), "available_for_tasks": effective_state == ACCOUNT_STATE_ACTIVE}

    @staticmethod
    def _effective_account_state(raw_state: str, health_status: str) -> str:
        if health_status == "banned":
            return ACCOUNT_STATE_DEAD
        if health_status == "limited":
            return ACCOUNT_STATE_LIMITED
        return AccountManager._normalize_account_state(raw_state)

    @staticmethod
    def _account_state_label(state: str) -> str:
        return {ACCOUNT_STATE_ACTIVE: "ACTIVE", ACCOUNT_STATE_LIMITED: "LIMITED", ACCOUNT_STATE_DEAD: "DEAD"}.get(state, ACCOUNT_STATE_ACTIVE)

    @staticmethod
    def _account_state_icon(state: str) -> str:
        return {ACCOUNT_STATE_ACTIVE: "✅", ACCOUNT_STATE_LIMITED: "🟡", ACCOUNT_STATE_DEAD: "🔴"}.get(state, "вљЄ")

    @staticmethod
    def _normalize_account_state(value: object) -> str:
        state = str(value or ACCOUNT_STATE_ACTIVE).strip().upper()
        return state if state in {ACCOUNT_STATE_ACTIVE, ACCOUNT_STATE_LIMITED, ACCOUNT_STATE_DEAD} else ACCOUNT_STATE_ACTIVE

    async def _apply_timeout_health(self, session: str, exc: Exception, *, dc_id: int | None = None) -> None:
        session_name = session.replace(".session", "").strip()
        state = self._health_states.setdefault(session_name, HealthState())
        state.dc_id = dc_id if dc_id is not None else state.dc_id
        state.last_checked_ts = time.time()
        state.consecutive_timeouts += 1
        state.reason = self._format_health_reason(exc)
        if state.consecutive_timeouts < TIMEOUT_LIMIT_THRESHOLD:
            self._invalidate_status_cache()
            logger.info(
                "Transient timeout for %s (%s/%s before LIMITED)",
                session_name,
                state.consecutive_timeouts,
                TIMEOUT_LIMIT_THRESHOLD,
            )
            return
        await self.mark_limited(
            session_name,
            f"{self._format_health_reason(exc)} x{state.consecutive_timeouts}",
            dc_id=dc_id,
        )

    async def _collect_single_status(self, index: int, managed: ManagedClient, *, include_spam_check: bool = False) -> dict:
        username, first_name = self._progress_identity(managed.session_name)
        dc_id = self._extract_dc_id(managed)
        try:
            if not managed.client.is_connected():
                await asyncio.wait_for(managed.client.connect(), timeout=6)
            is_authorized = await asyncio.wait_for(managed.client.is_user_authorized(), timeout=5)
            if not is_authorized:
                await self.mark_banned(managed.session_name, "Сессия не авторизована.", dc_id=dc_id)
                state = self._health_states.setdefault(managed.session_name, HealthState())
                return self._build_status_row(index, managed, username, first_name, state)
            me = await asyncio.wait_for(managed.client.get_me(), timeout=5)
            if me and me.username:
                username = f"@{me.username}"
            elif me:
                username = f"id:{me.id}"
            phone = f"+{me.phone}" if me and getattr(me, "phone", None) else ""
            if me and me.first_name:
                first_name = me.first_name.strip() or "-"
            profile_id = int(getattr(me, "id", 0) or 0) if me else 0
            self._profile_cache[managed.session_name] = (username, first_name, phone, profile_id or None)
            state = self._health_states.setdefault(managed.session_name, HealthState())
            if include_spam_check:
                spam_status, spam_reason = await self._check_spamblock_state(managed)
                if spam_status == "limited":
                    await self.mark_limited(
                        managed.session_name,
                        spam_reason or "SpamBot: account is temporarily limited.",
                        dc_id=dc_id,
                    )
                elif spam_status == "banned":
                    await self.mark_banned(
                        managed.session_name,
                        spam_reason or "SpamBot: account is blocked.",
                        dc_id=dc_id,
                    )
                elif spam_status == "alive":
                    await self.mark_alive(managed.session_name, dc_id=dc_id)
                elif state.status not in {"limited", "banned"}:
                    await self.mark_alive(managed.session_name, dc_id=dc_id)
                else:
                    state.dc_id = dc_id if dc_id is not None else state.dc_id
                    state.last_checked_ts = time.time()
                    self._invalidate_status_cache()
            elif state.status != "limited":
                await self.mark_alive(managed.session_name, dc_id=dc_id)
            else:
                state.dc_id = dc_id if dc_id is not None else state.dc_id
                state.last_checked_ts = time.time()
                self._invalidate_status_cache()
            return self._build_status_row(
                index,
                managed,
                username,
                first_name,
                self._health_states.setdefault(managed.session_name, HealthState()),
            )
        except Exception as exc:
            await self.apply_runtime_health(managed.session_name, exc, dc_id=dc_id)
            return self._build_status_row(
                index,
                managed,
                username,
                first_name,
                self._health_states.setdefault(managed.session_name, HealthState()),
            )

    async def _check_spamblock_state(self, managed: ManagedClient) -> tuple[str, str]:
        session_name = managed.session_name
        now = time.time()
        cached = self._spambot_cache.get(session_name)
        if cached and now - cached[2] <= SPAMBOT_CACHE_TTL_SECONDS:
            return cached[0], cached[1]

        try:
            await asyncio.wait_for(managed.client.send_message("@SpamBot", "/start"), timeout=10)
            await asyncio.sleep(0.8)
            messages = await asyncio.wait_for(managed.client.get_messages("@SpamBot", limit=5), timeout=10)
        except Exception as exc:
            logger.debug("SpamBot check failed for %s: %s", session_name, exc)
            if cached:
                return cached[0], cached[1]
            return "unknown", ""

        response_text = ""
        for item in messages or []:
            candidate = (getattr(item, "raw_text", None) or getattr(item, "message", None) or "").strip()
            if candidate and candidate != "/start":
                response_text = candidate
                break
        if not response_text:
            if cached:
                return cached[0], cached[1]
            return "unknown", ""

        parsed = self._parse_spambot_response(response_text)
        if parsed is None:
            if cached:
                return cached[0], cached[1]
            return "unknown", ""

        status, reason = parsed
        self._spambot_cache[session_name] = (status, reason, now)
        return status, reason

    @staticmethod
    def _parse_spambot_response(text: str) -> tuple[str, str] | None:
        clean = re.sub(r"\s+", " ", (text or "").strip())
        normalized = clean.lower()
        if not normalized:
            return None

        free_markers = (
            "ваш аккаунт свободен от каких-либо ограничений",
            "your account is free of any limitations",
            "good news, no limits are currently applied to your account",
        )
        if any(marker in normalized for marker in free_markers):
            return "alive", ""

        # Stable signal across locales for hard block messages.
        if "telegram.org/tos" in normalized:
            blocked_markers = (
                "blocked",
                "banned",
                "заблокирован",
                "заблокировали",
            )
            if any(marker in normalized for marker in blocked_markers):
                return "banned", f"SpamBot: {clean[:240]}"

        limited_markers = (
            "ваш аккаунт временно ограничен",
            "к сожалению, это невозможно",
            "ограничения будут сняты",
            "your account is currently limited",
            "you can only send messages to mutual contacts",
            "limitations will be automatically lifted",
        )
        if any(marker in normalized for marker in limited_markers):
            reason = f"SpamBot: {clean}"
            return "limited", reason[:240]

        return None

    def _build_status_row(
        self,
        index: int,
        managed: ManagedClient,
        username: str,
        first_name: str,
        state: HealthState,
    ) -> dict:
        health_status = state.status if state.status != "unknown" else "limited"
        lifecycle = self._account_lifecycle_snapshot(managed.session_name, health_status=health_status)
        return {
            "index": index,
            "id": managed.account_id,
            "profile_id": self._profile_cache.get(managed.session_name, ("", "", "", None))[3],
            "owner_id": managed.owner_id,
            "session": managed.session_name,
            "username": username,
            "first_name": first_name,
            "is_working": health_status == "alive",
            "health_status": health_status,
            "health_label": self._health_label(health_status),
            "health_icon": self._health_icon(health_status),
            "in_pool": state.in_pool,
            "dc_id": state.dc_id,
            "reason": state.reason,
            "last_checked_ts": state.last_checked_ts,
            "account_state": lifecycle["state"],
            "account_state_icon": lifecycle["state_icon"],
            "account_state_label": lifecycle["state_label"],
            "available_for_tasks": lifecycle["available_for_tasks"] and state.in_pool,
        }

    async def _set_health_state(
        self,
        session: str,
        status: str,
        reason: str,
        *,
        dc_id: int | None = None,
    ) -> None:
        session_name = session.replace(".session", "").strip()
        state = self._health_states.setdefault(session_name, HealthState())
        previous_status = state.status
        previous_in_pool = state.in_pool
        state.status = status
        state.reason = reason.strip()
        state.dc_id = dc_id if dc_id is not None else state.dc_id
        state.last_checked_ts = time.time()
        if status != "limited" or "timeout" not in state.reason.lower():
            state.consecutive_timeouts = 0
        if status in {"limited", "banned"} and self._auto_remove_from_pool:
            state.in_pool = False
        self._invalidate_status_cache()
        await self._maybe_notify_health_change(
            session_name=session_name,
            state=state,
            previous_status=previous_status,
            previous_in_pool=previous_in_pool,
        )

    async def _maybe_notify_health_change(
        self,
        *,
        session_name: str,
        state: HealthState,
        previous_status: str,
        previous_in_pool: bool,
    ) -> None:
        status_changed = state.status != previous_status and state.status != "unknown"
        pool_changed = state.in_pool != previous_in_pool
        if not status_changed and not pool_changed:
            return
        should_notify_status = state.status in {"limited", "banned"} or previous_status in {"limited", "banned"}
        if not should_notify_status and not pool_changed:
            return
        audit_event(
            "accounts.health_changed",
            level=logging.WARNING if state.status in {"limited", "banned"} else logging.INFO,
            message="Account health changed",
            session=session_name,
            status=state.status,
            previous_status=previous_status,
            in_pool=state.in_pool,
            previous_in_pool=previous_in_pool,
            dc_id=state.dc_id,
            reason=state.reason,
        )
        if not self._health_notifications_enabled or self._health_notifier is None:
            return
        parts = [
            "📉 <b>Health Check</b>",
            f"🗂 <b>Аккаунт:</b> <code>{self._health_account_label(session_name)}</code>",
            f"👤 <b>User:</b> <code>{self._health_identity(session_name)}</code>",
            f"📦 <b>Pool:</b> <b>{'pool' if state.in_pool else 'none'}</b>",
        ]
        if state.dc_id is not None:
            parts.append(f"🌐 <b>DC:</b> <b>{state.dc_id}</b>")
        if state.reason and state.status != "alive":
            parts.append(f"⚠️ <b>Причина:</b> <code>{self._display_health_reason(state.reason)}</code>")
        try:
            owner_id = self.get_account_owner_id(session_name)
        except ValueError:
            logger.info("Skipping health notification for removed account %s", session_name)
            owner_id = None
        await self._safe_notify_health("\n".join(parts), owner_id=owner_id)

    async def _safe_notify_health(self, text: str, *, owner_id: int | None = None) -> None:
        if self._health_notifier is None:
            return
        try:
            await self._health_notifier(text, owner_id)
        except Exception:
            logger.exception("Failed to send health notification")

    def _progress_identity(self, session_name: str) -> tuple[str, str]:
        if session_name in self._profile_cache:
            username, first_name, _, _ = self._profile_cache[session_name]
            return username, first_name
        return self._format_session_fallback(session_name), "-"

    @staticmethod
    def _format_session_fallback(session_name: str) -> str:
        if session_name.startswith("id:") or session_name.startswith("@"):
            return session_name
        if session_name.isdigit():
            return f"+{session_name}"
        return session_name

    @staticmethod
    def _normalize_session_names(values: list[str]) -> list[str]:
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            session_name = str(value or "").replace(".session", "").strip()
            if not session_name or session_name in seen:
                continue
            normalized.append(session_name)
            seen.add(session_name)
        return normalized

    def _session_exists_locally(self, session: str) -> bool:
        session_name = session.replace(".session", "").strip()
        return bool(session_name) and (self.sessions_dir / f"{session_name}.session").exists()

    @staticmethod
    def _extract_dc_id(managed: ManagedClient) -> int | None:
        try:
            return getattr(managed.client.session, "dc_id", None)
        except Exception:
            return None

    @staticmethod
    def _health_label(status: str) -> str:
        return {"alive": "ALIVE", "limited": "LIMITED", "banned": "BANNED"}.get(status, "LIMITED")

    @staticmethod
    def _health_icon(status: str) -> str:
        return {"alive": "🟢", "limited": "🟡", "banned": "🔴"}.get(status, "вљЄ")

    @staticmethod
    def _classify_health_exception(exc: Exception) -> tuple[str, str]:
        reason = AccountManager._format_health_reason(exc)
        text = f"{exc.__class__.__name__}: {exc}".lower()
        banned_markers = (
            "authkeyunregistered",
            "sessionrevoked",
            "userdeactivated",
            "phonebanned",
            "banned",
            "deactivated",
            "not authorized",
            "не авторизована",
        )
        limited_markers = ("peerflood", "flood", "spam", "rate", "timeout", "timed out")
        if any(marker in text for marker in banned_markers):
            return "banned", reason
        if any(marker in text for marker in limited_markers):
            return "limited", reason
        return "limited", reason

    @staticmethod
    def _is_timeout_exception(exc: Exception) -> bool:
        text = f"{exc.__class__.__name__}: {exc}".lower()
        return "timeout" in text or "timed out" in text

    @staticmethod
    def _format_health_reason(exc: Exception) -> str:
        text = str(exc).strip()
        return text or exc.__class__.__name__

    @staticmethod
    def _display_health_reason(reason: str) -> str:
        text = (reason or "").strip()
        if not text:
            return "-"
        if "caused by" in text:
            text = text.split("(caused by", maxsplit=1)[0].strip()
        return text[:120]

    def _health_account_label(self, session_name: str) -> str:
        try:
            managed = self._find_managed_by_session(session_name)
            profile_id = self._profile_cache.get(session_name, ("", "", "", None))[3]
            profile_id_label = str(profile_id) if profile_id is not None else "-"
            return f"#{managed.account_id} | ID: {profile_id_label}"
        except Exception:
            return "#- | ID: -"

    def _health_identity(self, session_name: str) -> str:
        username, _, phone, _ = self._profile_cache.get(
            session_name,
            (self._format_session_fallback(session_name), "-", "", None),
        )
        identity = (username or "").strip()
        if identity.startswith("@"):
            return f"{identity} | {phone}" if phone else identity
        if identity.startswith("id:"):
            id_value = f"id{identity[3:]}"
            return f"{id_value} | {phone}" if phone else id_value
        if phone:
            return f"session:{session_name} | {phone}"
        return f"session:{session_name}"

