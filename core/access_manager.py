from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
import string
import time
from dataclasses import dataclass, field
from pathlib import Path

from core.observability import audit_event

logger = logging.getLogger(__name__)

ROLE_OWNER = "owner"
ROLE_ADMIN = "admin"
ROLE_INTERNAL = "internal"
ROLE_EXTERNAL = "external"
ROLE_CLIENT = ROLE_INTERNAL
ROLE_PRIVATE = ROLE_EXTERNAL
ROLE_PRIVATE_CLIENT = "private_client"

TARIFF_TRIAL = "trial"
TARIFF_STANDARD = "standard"
TARIFF_PRO = "pro"
TARIFF_ENTERPRISE = "enterprise"

USER_STATUS_PENDING = "pending"
USER_STATUS_ACTIVE = "active"
USER_STATUS_BLOCKED = "blocked"

KEY_STATUS_INACTIVE = "inactive"
KEY_STATUS_ACTIVE = "active"
KEY_STATUS_EXPIRED = "expired"

VALID_ROLES = {ROLE_OWNER, ROLE_ADMIN, ROLE_INTERNAL, ROLE_EXTERNAL}
VALID_TARIFFS = {TARIFF_TRIAL, TARIFF_STANDARD, TARIFF_PRO, TARIFF_ENTERPRISE}
ACCESS_KEY_BLOCK_LENGTH = 4
ACCESS_KEY_BLOCKS_COUNT = 4
ACCESS_KEY_ALPHABET = string.ascii_letters + string.digits
ACCESS_KEY_PATTERN = re.compile(r"^[A-Za-z0-9]{4}(?:-[A-Za-z0-9]{4}){3}$")
UNASSIGNED_ROLE = ""
UNASSIGNED_TARIFF = ""


@dataclass(slots=True)
class AccessUser:
    telegram_id: int
    role: str
    owner_scope_id: int
    tariff: str = TARIFF_TRIAL
    status: str = USER_STATUS_PENDING
    created_at: float | None = None
    activated_at: float | None = None
    username: str = ""
    first_name: str = ""
    last_name: str = ""
    start_count: int = 0
    last_start_at: float | None = None
    blocked_reason: str = ""
    blocked_at: float | None = None
    access_notice_sent: bool = False
    blocked_notice_sent: bool = False
    start_notice_message_ids: dict[str, int] = field(default_factory=dict)

    @property
    def uses_shared_pool(self) -> bool:
        return self.role in {ROLE_OWNER, ROLE_ADMIN, ROLE_INTERNAL}

    @property
    def is_global(self) -> bool:
        return self.role in {ROLE_OWNER, ROLE_ADMIN}

    @property
    def is_active(self) -> bool:
        return self.status == USER_STATUS_ACTIVE


@dataclass(slots=True)
class AccessKey:
    key: str
    telegram_id: int
    role: str
    tariff: str
    status: str = KEY_STATUS_INACTIVE
    created_at: float | None = None
    expires_at: float | None = None
    activated_at: float | None = None
    activated_by_id: int | None = None

    @property
    def is_active(self) -> bool:
        return self.status == KEY_STATUS_ACTIVE


class AccessManager:
    def __init__(self, access_file: Path, owner_user_id: int, support_username: str = "mattersless") -> None:
        self.access_file = access_file
        self.owner_user_id = int(owner_user_id)
        self.support_username = str(support_username).lstrip("@").strip() or "mattersless"
        self._users: dict[int, AccessUser] = {}
        self._keys: dict[str, AccessKey] = {}
        self._lock = asyncio.Lock()
        self._activation_attempts: dict[int, list[float]] = {}

    async def load(self) -> None:
        async with self._lock:
            raw_data = self._read_file()
            self._users, self._keys = self._parse_data(raw_data)
            self._ensure_primary_owner_unlocked()
            self._expire_outdated_keys_unlocked()
            self._write_file()
            audit_event("access.loaded", message="Access registry loaded", users_total=len(self._users), keys_total=len(self._keys))

    async def is_allowed(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active)

    async def is_registered(self, user_id: int) -> bool:
        async with self._lock:
            return int(user_id) in self._users

    async def is_owner(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.role == ROLE_OWNER and user.is_active)

    async def get_user(self, user_id: int) -> AccessUser | None:
        async with self._lock:
            return self._copy_user(self._users.get(int(user_id)))

    async def get_key(self, key: str) -> AccessKey | None:
        async with self._lock:
            return self._copy_key(self._keys.get(key))

    async def has_inactive_key(self, telegram_id: int) -> bool:
        async with self._lock:
            normalized_id = int(telegram_id)
            return any(item.telegram_id == normalized_id and item.status == KEY_STATUS_INACTIVE for item in self._keys.values())

    async def list_users(self) -> list[AccessUser]:
        async with self._lock:
            return [self._copy_user(user) for user in sorted(self._users.values(), key=self._sort_key)]

    async def list_active_user_ids(self) -> list[int]:
        async with self._lock:
            return sorted(user.telegram_id for user in self._users.values() if user.is_active)

    async def list_active_owner_ids(self) -> list[int]:
        async with self._lock:
            owners = [user for user in self._users.values() if user.is_active and user.role == ROLE_OWNER]
            owners.sort(key=lambda user: (0 if user.telegram_id == self.owner_user_id else 1, user.created_at or 0, user.telegram_id))
            return [user.telegram_id for user in owners]

    async def list_health_notification_user_ids(self, owner_scope_id: int) -> list[int]:
        async with self._lock:
            normalized_owner_scope_id = int(owner_scope_id)
            recipients: set[int] = set()
            for user in self._users.values():
                if not user.is_active:
                    continue
                if user.telegram_id == normalized_owner_scope_id:
                    recipients.add(user.telegram_id)
                elif user.role in {ROLE_OWNER, ROLE_ADMIN} and user.owner_scope_id == normalized_owner_scope_id:
                    recipients.add(user.telegram_id)
            return sorted(recipients)

    async def role_display_label(self, telegram_id: int, role: str) -> str:
        if not str(role or "").strip():
            return "-"
        normalized_role = self._normalize_role(role)
        if normalized_role != ROLE_OWNER:
            return self._role_title(normalized_role)
        owner_ids = await self.list_active_owner_ids()
        if not owner_ids:
            return "OWNER"
        try:
            index = owner_ids.index(int(telegram_id)) + 1
        except ValueError:
            index = 1
        return "OWNER" if index == 1 else f"OWNER{index}"

    async def get_start_notice_message_id(self, *, telegram_id: int, owner_id: int) -> int | None:
        async with self._lock:
            user = self._users.get(int(telegram_id))
            if user is None:
                return None
            value = user.start_notice_message_ids.get(str(int(owner_id)))
            return int(value) if value is not None else None

    async def set_start_notice_message_id(self, *, telegram_id: int, owner_id: int, message_id: int) -> None:
        async with self._lock:
            user = self._users.get(int(telegram_id))
            if user is None:
                return
            user.start_notice_message_ids[str(int(owner_id))] = int(message_id)
            self._write_file()

    async def register_start_attempt(
        self,
        *,
        telegram_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        event_ts: float | None,
    ) -> dict[str, object]:
        normalized_id = int(telegram_id)
        now = event_ts or time.time()
        async with self._lock:
            user = self._users.get(normalized_id)
            if user is None:
                user = AccessUser(
                    telegram_id=normalized_id,
                    role=UNASSIGNED_ROLE,
                    owner_scope_id=self.owner_user_id,
                    tariff=UNASSIGNED_TARIFF,
                    status=USER_STATUS_PENDING,
                    created_at=now,
                )
                self._users[normalized_id] = user
            user.username = (username or "").strip().lstrip("@")
            user.first_name = (first_name or "").strip()
            user.last_name = (last_name or "").strip()
            user.last_start_at = now
            blocked_now = False
            if user.status != USER_STATUS_BLOCKED:
                user.start_count = max(0, int(user.start_count)) + 1
                if user.start_count > 20:
                    user.status = USER_STATUS_BLOCKED
                    user.blocked_reason = "частый вызов /start"
                    user.blocked_at = now
                    user.blocked_notice_sent = False
                    blocked_now = True
            self._write_file()
            audit_event("access.start_received", message="Start command received", telegram_id=normalized_id, start_count=user.start_count, status=user.status, blocked_now=blocked_now)
            return {
                "count": user.start_count,
                "blocked": user.status == USER_STATUS_BLOCKED,
                "blocked_now": blocked_now,
                "access_notice_sent": user.access_notice_sent,
                "blocked_notice_sent": user.blocked_notice_sent,
                "user": self._copy_user(user),
            }

    async def list_user_ids(self) -> list[int]:
        async with self._lock:
            return sorted(self._users)

    async def list_keys(self, *, telegram_id: int | None = None) -> list[AccessKey]:
        async with self._lock:
            self._expire_outdated_keys_unlocked()
            items = list(self._keys.values())
            if telegram_id is not None:
                items = [item for item in items if item.telegram_id == int(telegram_id)]
            items.sort(key=lambda item: (item.telegram_id, -(item.created_at or 0), item.key))
            return [self._copy_key(item) for item in items]

    async def upsert_user(self, telegram_id: int, *, role: str, owner_scope_id: int | None = None, status: str | None = None) -> tuple[AccessUser, bool]:
        normalized_id = int(telegram_id)
        normalized_role = self._normalize_role(role)
        normalized_scope = self._normalize_scope(telegram_id=normalized_id, role=normalized_role, owner_scope_id=owner_scope_id)
        normalized_status = self._normalize_user_status(status)
        async with self._lock:
            created = normalized_id not in self._users
            existing = self._users.get(normalized_id)
            user = AccessUser(
                telegram_id=normalized_id,
                role=normalized_role,
                owner_scope_id=normalized_scope,
                tariff=existing.tariff if existing else self._default_tariff_for_role(normalized_role),
                status=normalized_status or (existing.status if existing else USER_STATUS_ACTIVE),
                created_at=existing.created_at if existing else time.time(),
                activated_at=existing.activated_at if existing else None,
                username=existing.username if existing else "",
                first_name=existing.first_name if existing else "",
                last_name=existing.last_name if existing else "",
                start_count=existing.start_count if existing else 0,
                last_start_at=existing.last_start_at if existing else None,
                blocked_reason=existing.blocked_reason if existing else "",
                blocked_at=existing.blocked_at if existing else None,
                access_notice_sent=existing.access_notice_sent if existing else False,
                blocked_notice_sent=existing.blocked_notice_sent if existing else False,
                start_notice_message_ids=dict(existing.start_notice_message_ids) if existing else {},
            )
            self._users[normalized_id] = user
            self._write_file()
            audit_event("access.user_upserted", message="Access user upserted", telegram_id=normalized_id, role=normalized_role, owner_scope_id=normalized_scope, status=user.status, created=created)
            return self._copy_user(user), created

    async def create_access_key(self, telegram_id: int, role: str, tariff: str) -> AccessKey:
        normalized_id = int(telegram_id)
        normalized_role = self._normalize_role(role)
        normalized_tariff = self._normalize_tariff(tariff)
        async with self._lock:
            self._expire_outdated_keys_unlocked()
            existing_user = self._users.get(normalized_id)
            if existing_user and existing_user.is_active:
                raise ValueError("Пользователь уже активирован.")
            now = time.time()
            generated_key = self._generate_unique_key_unlocked()
            owner_scope_id = self._normalize_scope(telegram_id=normalized_id, role=normalized_role, owner_scope_id=existing_user.owner_scope_id if existing_user else None)
            self._users[normalized_id] = AccessUser(
                telegram_id=normalized_id,
                role=normalized_role,
                owner_scope_id=owner_scope_id,
                tariff=normalized_tariff,
                status=USER_STATUS_PENDING,
                created_at=existing_user.created_at if existing_user else now,
                activated_at=None,
                username=existing_user.username if existing_user else "",
                first_name=existing_user.first_name if existing_user else "",
                last_name=existing_user.last_name if existing_user else "",
                start_count=existing_user.start_count if existing_user else 0,
                last_start_at=existing_user.last_start_at if existing_user else None,
                blocked_reason="",
                blocked_at=None,
                access_notice_sent=False,
                blocked_notice_sent=existing_user.blocked_notice_sent if existing_user else False,
                start_notice_message_ids=dict(existing_user.start_notice_message_ids) if existing_user else {},
            )
            for key, item in list(self._keys.items()):
                if item.telegram_id == normalized_id and item.status == KEY_STATUS_INACTIVE:
                    self._keys.pop(key, None)
            access_key = AccessKey(key=generated_key, telegram_id=normalized_id, role=normalized_role, tariff=normalized_tariff, status=KEY_STATUS_INACTIVE, created_at=now)
            self._keys[generated_key] = access_key
            self._write_file()
            audit_event("access.key_created", message="Access key created", telegram_id=normalized_id, role=normalized_role, tariff=normalized_tariff, key=generated_key)
            return self._copy_key(access_key)

    async def activate_key(self, telegram_id: int, raw_key: str) -> AccessUser:
        normalized_id = int(telegram_id)
        key = raw_key.strip()
        async with self._lock:
            self._expire_outdated_keys_unlocked()
            self._check_rate_limit_unlocked(normalized_id)
            user = self._users.get(normalized_id)
            if user is None:
                self._register_failed_attempt_unlocked(normalized_id, "user_not_registered")
                raise PermissionError(self.not_registered_message())
            if user.is_active:
                raise ValueError("Доступ уже активирован.")
            if user.status == USER_STATUS_BLOCKED:
                raise PermissionError(self.blocked_message(user))
            if not self._is_valid_key_format(key):
                self._register_failed_attempt_unlocked(normalized_id, "invalid_format")
                raise ValueError("Ключ должен быть в формате xxxx-xxxx-xxxx-xxxx (A-Z, a-z, 0-9).")
            access_key = self._keys.get(key)
            if access_key is None:
                self._register_failed_attempt_unlocked(normalized_id, "key_not_found")
                raise ValueError("Ключ не найден.")
            if access_key.telegram_id != normalized_id:
                self._register_failed_attempt_unlocked(normalized_id, "wrong_owner")
                raise ValueError("Этот ключ привязан к другому Telegram ID.")
            if access_key.status != KEY_STATUS_INACTIVE:
                self._register_failed_attempt_unlocked(normalized_id, "already_used")
                raise ValueError("Ключ уже активирован.")
            if any(item.key != access_key.key and item.status == KEY_STATUS_ACTIVE and item.activated_by_id == normalized_id for item in self._keys.values()):
                self._register_failed_attempt_unlocked(normalized_id, "duplicate_active")
                raise ValueError("Для этого Telegram ID уже активирован другой ключ.")
            now = time.time()
            access_key.status = KEY_STATUS_ACTIVE
            access_key.activated_at = now
            access_key.activated_by_id = normalized_id
            user.status = USER_STATUS_ACTIVE
            user.role = access_key.role
            user.tariff = access_key.tariff
            user.owner_scope_id = self._normalize_scope(telegram_id=normalized_id, role=access_key.role, owner_scope_id=user.owner_scope_id)
            user.activated_at = now
            self._activation_attempts.pop(normalized_id, None)
            self._write_file()
            audit_event("access.key_activated", message="Access key activated", telegram_id=normalized_id, role=access_key.role, key=access_key.key, owner_scope_id=user.owner_scope_id)
            return self._copy_user(user)

    async def remove_user(self, user_id: int) -> bool:
        normalized = int(user_id)
        if normalized == self.owner_user_id:
            raise ValueError("Основной владелец не может быть удалён из доступа.")
        async with self._lock:
            if normalized not in self._users:
                return False
            self._users.pop(normalized, None)
            self._keys = {key: item for key, item in self._keys.items() if item.telegram_id != normalized}
            self._write_file()
            audit_event("access.user_removed", message="Access user removed", telegram_id=normalized)
            return True

    async def unblock_user(self, user_id: int) -> bool:
        async with self._lock:
            user = self._users.get(int(user_id))
            if user is None or user.status != USER_STATUS_BLOCKED:
                return False
            user.status = USER_STATUS_PENDING
            user.start_count = 0
            user.blocked_reason = ""
            user.blocked_at = None
            user.access_notice_sent = False
            user.blocked_notice_sent = False
            self._write_file()
            audit_event("access.user_unblocked", message="Access user unblocked", telegram_id=int(user_id))
            return True

    async def block_user(self, user_id: int, *, reason: str) -> bool:
        async with self._lock:
            user = self._users.get(int(user_id))
            if user is None:
                return False
            user.status = USER_STATUS_BLOCKED
            user.blocked_reason = reason.strip() or "manual block"
            user.blocked_at = time.time()
            user.blocked_notice_sent = False
            self._write_file()
            audit_event("access.user_blocked", message="Access user blocked", telegram_id=int(user_id), reason=user.blocked_reason)
            return True

    async def change_user_role(self, user_id: int, role: str) -> AccessUser:
        normalized = int(user_id)
        normalized_role = self._normalize_role(role)
        async with self._lock:
            user = self._users.get(normalized)
            if user is None:
                raise ValueError("Пользователь не найден.")
            if not user.is_active:
                raise ValueError("Роль можно менять только у пользователя с активированным доступом.")
            user.role = normalized_role
            user.tariff = self._default_tariff_for_role(normalized_role)
            user.owner_scope_id = self._normalize_scope(telegram_id=normalized, role=normalized_role, owner_scope_id=user.owner_scope_id)
            self._write_file()
            audit_event("access.user_role_changed", message="Access user role changed", telegram_id=normalized, role=normalized_role)
            return self._copy_user(user)

    async def change_user_tariff(self, user_id: int, tariff: str) -> AccessUser:
        normalized = int(user_id)
        normalized_tariff = self._normalize_tariff(tariff)
        async with self._lock:
            user = self._users.get(normalized)
            if user is None:
                raise ValueError("Пользователь не найден.")
            if not user.is_active:
                raise ValueError("Тариф можно менять только у пользователя с активированным доступом.")
            user.tariff = normalized_tariff
            self._write_file()
            audit_event("access.user_tariff_changed", message="Access user tariff changed", telegram_id=normalized, tariff=normalized_tariff)
            return self._copy_user(user)

    async def access_state(self, user_id: int) -> str:
        user = await self.get_user(user_id)
        if user is None:
            return "missing"
        if user.status == USER_STATUS_BLOCKED:
            return USER_STATUS_BLOCKED
        return USER_STATUS_ACTIVE if user.is_active else USER_STATUS_PENDING

    async def pending_key_message(self, user_id: int) -> str:
        normalized_id = int(user_id)
        async with self._lock:
            user = self._users.get(normalized_id)
            if user is None:
                return self.not_registered_message()
            if user.status == USER_STATUS_BLOCKED:
                return self.blocked_message(self._copy_user(user))
            if user.is_active:
                return "Доступ уже активирован."
            has_key = any(item.telegram_id == normalized_id for item in self._keys.values())
            if not has_key:
                return self.not_registered_message()
            return (
                "<b>🔐 Ключ успешно зарегистрирован</b>\n\n"
                "Ваш ключ активирован и закреплён за вашим Telegram ID.\n\n"
                "<b>📌 Формат:</b>\n"
                "<code>xxxx-xxxx-xxxx-xxxx</code>"
            )

    def not_registered_message(self) -> str:
        return "<b>🚫 Доступ ограничен</b>\n\nВы не зарегистрированы\n" f"📩 Контакт: @{self.support_username}"

    def blocked_message(self, user: AccessUser | None) -> str:
        reason = (user.blocked_reason if user else "") or "частый вызов /start"
        identity = "—" if user is None else (f"@{user.username}" if user.username else f"id{user.telegram_id}")
        return "<b>⛔ Вы заблокированы</b>\n\n" f"Причина: {reason}\n👤 {identity}\n\n📅 {self._format_public_dt(user.blocked_at if user else None)}\n" f"📩 Для разблокировки: @{self.support_username}"

    async def mark_access_notice_sent(self, user_id: int) -> None:
        async with self._lock:
            user = self._users.get(int(user_id))
            if user is not None:
                user.access_notice_sent = True
                self._write_file()

    async def mark_blocked_notice_sent(self, user_id: int) -> None:
        async with self._lock:
            user = self._users.get(int(user_id))
            if user is not None:
                user.blocked_notice_sent = True
                self._write_file()

    async def can_manage_access(self, user_id: int) -> bool:
        return await self.is_owner(user_id)

    async def can_manage_roles(self, user_id: int) -> bool:
        return await self.is_owner(user_id)

    async def can_manage_owner_settings(self, user_id: int) -> bool:
        return await self.is_owner(user_id)

    async def can_access_accounts_menu(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active and user.role in {ROLE_OWNER, ROLE_ADMIN, ROLE_INTERNAL, ROLE_EXTERNAL})

    async def can_add_accounts(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active and user.role in {ROLE_OWNER, ROLE_ADMIN, ROLE_EXTERNAL})

    async def can_export_accounts(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active and user.role in {ROLE_OWNER, ROLE_ADMIN, ROLE_INTERNAL, ROLE_EXTERNAL})

    async def can_use_manual_account_add(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active and user.role in {ROLE_OWNER, ROLE_ADMIN})

    async def can_view_all_tasks(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user and user.is_active and user.role in {ROLE_OWNER, ROLE_ADMIN})

    async def visible_account_owner_ids(self, user_id: int) -> set[int] | None:
        user = await self.get_user(user_id)
        if user is None or not user.is_active:
            return set()
        if user.role in {ROLE_OWNER, ROLE_ADMIN}:
            return None
        if user.role == ROLE_INTERNAL:
            return {self.owner_user_id}
        return {user.owner_scope_id}

    async def can_manage_account_owner(self, user_id: int, account_owner_id: int) -> bool:
        user = await self.get_user(user_id)
        if user is None or not user.is_active:
            return False
        if user.role in {ROLE_OWNER, ROLE_ADMIN}:
            return True
        return user.role == ROLE_EXTERNAL and user.owner_scope_id == int(account_owner_id)

    async def account_owner_for_new_account(self, user_id: int) -> int | None:
        user = await self.get_user(user_id)
        if user is None or not user.is_active:
            return None
        if user.role in {ROLE_OWNER, ROLE_ADMIN}:
            return self.owner_user_id
        if user.role == ROLE_EXTERNAL:
            return user.owner_scope_id
        return None

    async def validate_action_delay(self, user_id: int, value: float) -> float:
        user = await self.get_user(user_id)
        if user is None or not user.is_active:
            raise PermissionError("Пользователь не активирован.")
        delay = float(value)
        if delay < 0:
            raise ValueError("Задержка не может быть меньше 0 секунд.")
        if user.role in {ROLE_OWNER, ROLE_ADMIN}:
            return delay
        if delay > 86400:
            raise ValueError("Задержка должна быть в диапазоне от 0 до 86400 секунд.")
        if user.role == ROLE_EXTERNAL:
            return delay
        min_delay = {
            TARIFF_TRIAL: 2.0,
            TARIFF_STANDARD: 2.0,
            TARIFF_PRO: 1.5,
            TARIFF_ENTERPRISE: 1.0,
        }[self._normalize_tariff(user.tariff)]
        if delay < min_delay:
            raise ValueError(
                f"Для роли Internal с тарифом {self._tariff_title(user.tariff)} минимальная задержка {min_delay:g} сек."
            )
        return delay

    async def get_active_key_for_user(self, user_id: int) -> AccessKey | None:
        normalized = int(user_id)
        async with self._lock:
            for item in self._keys.values():
                if item.activated_by_id == normalized and item.status == KEY_STATUS_ACTIVE:
                    return self._copy_key(item)
        return None

    async def has_any_key_for_user(self, user_id: int) -> bool:
        normalized = int(user_id)
        async with self._lock:
            return any(item.telegram_id == normalized for item in self._keys.values())


def _read_file(self) -> dict:
    if not self.access_file.exists():
        return {}
    try:
        raw_data = json.loads(self.access_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return raw_data if isinstance(raw_data, dict) else {}


def _write_file(self) -> None:
    self.access_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "owner_user_id": self.owner_user_id,
        "users": [
            {
                "telegram_id": user.telegram_id,
                "role": user.role,
                "owner_scope_id": user.owner_scope_id,
                "tariff": user.tariff,
                "status": user.status,
                "created_at": user.created_at,
                "activated_at": user.activated_at,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "start_count": user.start_count,
                "last_start_at": user.last_start_at,
                "blocked_reason": user.blocked_reason,
                "blocked_at": user.blocked_at,
                "access_notice_sent": user.access_notice_sent,
                "blocked_notice_sent": user.blocked_notice_sent,
                "start_notice_message_ids": user.start_notice_message_ids,
            }
            for user in sorted(self._users.values(), key=self._sort_key)
        ],
        "keys": [
            {
                "telegram_id": item.telegram_id,
                "role": item.role,
                "tariff": item.tariff,
                "key": item.key,
                "status": item.status,
                "created_at": item.created_at,
                "expires_at": item.expires_at,
                "activated_at": item.activated_at,
                "activated_by_id": item.activated_by_id,
            }
            for item in sorted(
                self._keys.values(),
                key=lambda current: (current.telegram_id, -(current.created_at or 0), current.key),
            )
        ],
    }
    self.access_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_data(self, data: dict) -> tuple[dict[int, AccessUser], dict[str, AccessKey]]:
    users: dict[int, AccessUser] = {}
    keys: dict[str, AccessKey] = {}
    raw_users = data.get("users")
    if isinstance(raw_users, list):
        for item in raw_users:
            if not isinstance(item, dict):
                continue
            telegram_id = self._safe_int(item.get("telegram_id"))
            if telegram_id is None:
                continue
            raw_role = item.get("role", UNASSIGNED_ROLE)
            role = UNASSIGNED_ROLE if raw_role in {None, ""} else self._normalize_role(raw_role)
            owner_scope_id = self._normalize_scope(
                telegram_id=telegram_id,
                role=role or ROLE_INTERNAL,
                owner_scope_id=item.get("owner_scope_id"),
            )
            users[telegram_id] = AccessUser(
                telegram_id=telegram_id,
                role=role,
                owner_scope_id=owner_scope_id,
                tariff=(
                    UNASSIGNED_TARIFF
                    if role == UNASSIGNED_ROLE
                    else self._normalize_tariff(item.get("tariff", self._default_tariff_for_role(role)))
                ),
                status=self._normalize_user_status(item.get("status")) or USER_STATUS_ACTIVE,
                created_at=self._safe_ts(item.get("created_at")),
                activated_at=self._safe_ts(item.get("activated_at")),
                username=str(item.get("username", "")).strip().lstrip("@"),
                first_name=str(item.get("first_name", "")).strip(),
                last_name=str(item.get("last_name", "")).strip(),
                start_count=max(0, int(item.get("start_count", 0) or 0)),
                last_start_at=self._safe_ts(item.get("last_start_at")),
                blocked_reason=str(item.get("blocked_reason", "")).strip(),
                blocked_at=self._safe_ts(item.get("blocked_at")),
                access_notice_sent=bool(item.get("access_notice_sent", False)),
                blocked_notice_sent=bool(item.get("blocked_notice_sent", False)),
                start_notice_message_ids={
                    str(key): int(value)
                    for key, value in (item.get("start_notice_message_ids", {}) or {}).items()
                    if self._safe_int(value) is not None
                },
            )
    else:
        now = time.time()
        for telegram_id in _normalize_ids(data.get("allowed_user_ids", [])):
            role = ROLE_OWNER if telegram_id == self.owner_user_id else ROLE_INTERNAL
            users[telegram_id] = AccessUser(
                telegram_id=telegram_id,
                role=role,
                owner_scope_id=self.owner_user_id,
                tariff=self._default_tariff_for_role(role),
                status=USER_STATUS_ACTIVE,
                created_at=now,
                activated_at=now,
            )
    raw_keys = data.get("keys", [])
    if isinstance(raw_keys, list):
        for item in raw_keys:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "")).strip()
            telegram_id = self._safe_int(item.get("telegram_id"))
            if telegram_id is None or not self._is_valid_key_format(key):
                continue
            role = self._normalize_role(item.get("role", ROLE_INTERNAL))
            tariff = self._normalize_tariff(
                item.get("tariff", item.get("duration", self._default_tariff_for_role(role)))
            )
            keys[key] = AccessKey(
                key=key,
                telegram_id=telegram_id,
                role=role,
                tariff=tariff,
                status=self._normalize_key_status(item.get("status")),
                created_at=self._safe_ts(item.get("created_at")),
                expires_at=self._safe_ts(item.get("expires_at")),
                activated_at=self._safe_ts(item.get("activated_at")),
                activated_by_id=self._safe_int(item.get("activated_by_id")),
            )
    return users, keys


def _ensure_primary_owner_unlocked(self) -> None:
    now = time.time()
    existing = self._users.get(self.owner_user_id)
    self._users[self.owner_user_id] = AccessUser(
        telegram_id=self.owner_user_id,
        role=ROLE_OWNER,
        owner_scope_id=self.owner_user_id,
        tariff=existing.tariff if existing else TARIFF_ENTERPRISE,
        status=USER_STATUS_ACTIVE,
        created_at=existing.created_at if existing and existing.created_at else now,
        activated_at=existing.activated_at if existing and existing.activated_at else now,
        username=existing.username if existing else "",
        first_name=existing.first_name if existing else "",
        last_name=existing.last_name if existing else "",
        start_count=existing.start_count if existing else 0,
        last_start_at=existing.last_start_at if existing else None,
        blocked_reason="",
        blocked_at=None,
        access_notice_sent=existing.access_notice_sent if existing else False,
        blocked_notice_sent=existing.blocked_notice_sent if existing else False,
        start_notice_message_ids=dict(existing.start_notice_message_ids) if existing else {},
    )


def _expire_outdated_keys_unlocked(self) -> None:
    now = time.time()
    changed = False
    for item in self._keys.values():
        if item.status == KEY_STATUS_INACTIVE and item.expires_at is not None and item.expires_at < now:
            item.status = KEY_STATUS_EXPIRED
            changed = True
    if changed:
        self._write_file()


def _generate_unique_key_unlocked(self) -> str:
    while True:
        blocks = [
            "".join(secrets.choice(ACCESS_KEY_ALPHABET) for _ in range(ACCESS_KEY_BLOCK_LENGTH))
            for _ in range(ACCESS_KEY_BLOCKS_COUNT)
        ]
        key = "-".join(blocks)
        if key not in self._keys:
            return key


def _check_rate_limit_unlocked(self, telegram_id: int) -> None:
    now = time.time()
    attempts = [ts for ts in self._activation_attempts.get(telegram_id, []) if now - ts <= 300]
    self._activation_attempts[telegram_id] = attempts
    if len(attempts) >= 5:
        raise ValueError("Слишком много попыток. Подождите 5 минут.")


def _register_failed_attempt_unlocked(self, telegram_id: int, reason: str) -> None:
    now = time.time()
    attempts = [ts for ts in self._activation_attempts.get(telegram_id, []) if now - ts <= 300]
    attempts.append(now)
    self._activation_attempts[telegram_id] = attempts
    logger.warning("Access key activation failed for telegram_id=%s reason=%s", telegram_id, reason)
    audit_event(
        "access.activation_failed",
        level=logging.WARNING,
        message="Access key activation failed",
        telegram_id=telegram_id,
        reason=reason,
        attempts_last_5m=len(attempts),
    )


def _normalize_ids(values: list[object]) -> set[int]:
    normalized: set[int] = set()
    for value in values:
        try:
            normalized.add(int(str(value).strip()))
        except (TypeError, ValueError):
            continue
    return normalized


def _sort_key(user: AccessUser) -> tuple[int, int, int]:
    role_order = {
        ROLE_OWNER: 0,
        ROLE_ADMIN: 1,
        ROLE_EXTERNAL: 2,
        ROLE_INTERNAL: 3,
    }
    status_order = {
        USER_STATUS_ACTIVE: 0,
        USER_STATUS_PENDING: 1,
        USER_STATUS_BLOCKED: 2,
    }
    return status_order.get(user.status, 99), role_order.get(user.role, 99), user.telegram_id


def _normalize_role(value: object) -> str:
    role = str(value or ROLE_INTERNAL).strip().lower()
    if role in {"client"}:
        role = ROLE_INTERNAL
    elif role in {ROLE_PRIVATE_CLIENT, "private"}:
        role = ROLE_EXTERNAL
    if role not in VALID_ROLES:
        raise ValueError("Role must be one of: owner, admin, internal, external.")
    return role


def _normalize_tariff(value: object) -> str:
    tariff = str(value or TARIFF_TRIAL).strip().lower()
    tariff = {
        "7d": TARIFF_TRIAL,
        "14d": TARIFF_STANDARD,
        "1m": TARIFF_PRO,
        "6m": TARIFF_ENTERPRISE,
        "1y": TARIFF_ENTERPRISE,
        "lt": TARIFF_ENTERPRISE,
    }.get(tariff, tariff)
    if tariff not in VALID_TARIFFS:
        raise ValueError("Tariff must be one of: Trial, Standard, Pro, Enterprise.")
    return tariff


def _normalize_user_status(value: object | None) -> str | None:
    if value in {None, ""}:
        return None
    status = str(value).strip().lower()
    if status not in {USER_STATUS_PENDING, USER_STATUS_ACTIVE, USER_STATUS_BLOCKED}:
        raise ValueError("User status must be pending, active or blocked.")
    return status


def _normalize_key_status(value: object | None) -> str:
    status = str(value or KEY_STATUS_INACTIVE).strip().lower()
    if status not in {KEY_STATUS_INACTIVE, KEY_STATUS_ACTIVE, KEY_STATUS_EXPIRED}:
        return KEY_STATUS_INACTIVE
    return status


def _is_valid_key_format(value: str) -> bool:
    return bool(ACCESS_KEY_PATTERN.fullmatch((value or "").strip()))


def _safe_int(value: object | None) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _safe_ts(value: object | None) -> float | None:
    if value in {None, ""}:
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    return normalized if normalized > 0 else None


def _normalize_scope(
    self,
    *,
    telegram_id: int,
    role: str,
    owner_scope_id: object | None,
) -> int:
    if role == ROLE_OWNER:
        return self.owner_user_id
    if role in {ROLE_ADMIN, ROLE_INTERNAL}:
        return self.owner_user_id
    if owner_scope_id is None:
        return telegram_id
    try:
        return int(str(owner_scope_id).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("owner_scope_id must be a number.") from exc


def _copy_user(user: AccessUser | None) -> AccessUser | None:
    if user is None:
        return None
    return AccessUser(
        telegram_id=user.telegram_id,
        role=user.role,
        owner_scope_id=user.owner_scope_id,
        tariff=user.tariff,
        status=user.status,
        created_at=user.created_at,
        activated_at=user.activated_at,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        start_count=user.start_count,
        last_start_at=user.last_start_at,
        blocked_reason=user.blocked_reason,
        blocked_at=user.blocked_at,
        access_notice_sent=user.access_notice_sent,
        blocked_notice_sent=user.blocked_notice_sent,
        start_notice_message_ids=dict(user.start_notice_message_ids),
    )


def _format_public_dt(value: float | None) -> str:
    if not value:
        return "-"
    return time.strftime("%d.%m.%Y • %H:%M", time.localtime(value))


def _copy_key(item: AccessKey | None) -> AccessKey | None:
    if item is None:
        return None
    return AccessKey(
        key=item.key,
        telegram_id=item.telegram_id,
        role=item.role,
        tariff=item.tariff,
        status=item.status,
        created_at=item.created_at,
        expires_at=item.expires_at,
        activated_at=item.activated_at,
        activated_by_id=item.activated_by_id,
    )


def _role_title(role: str) -> str:
    mapping = {
        ROLE_OWNER: "OWNER",
        ROLE_ADMIN: "Admin",
        ROLE_INTERNAL: "Internal",
        ROLE_EXTERNAL: "External",
    }
    return mapping.get(role, role)


def _tariff_title(tariff: str) -> str:
    mapping = {
        TARIFF_TRIAL: "Trial",
        TARIFF_STANDARD: "Standard",
        TARIFF_PRO: "Pro",
        TARIFF_ENTERPRISE: "Enterprise",
    }
    return mapping.get(tariff, tariff)


def _default_tariff_for_role(role: str) -> str:
    if role in {ROLE_OWNER, ROLE_ADMIN}:
        return TARIFF_ENTERPRISE
    return TARIFF_TRIAL


AccessManager._read_file = _read_file
AccessManager._write_file = _write_file
AccessManager._parse_data = _parse_data
AccessManager._ensure_primary_owner_unlocked = _ensure_primary_owner_unlocked
AccessManager._expire_outdated_keys_unlocked = _expire_outdated_keys_unlocked
AccessManager._generate_unique_key_unlocked = _generate_unique_key_unlocked
AccessManager._check_rate_limit_unlocked = _check_rate_limit_unlocked
AccessManager._register_failed_attempt_unlocked = _register_failed_attempt_unlocked
AccessManager._normalize_ids = staticmethod(_normalize_ids)
AccessManager._sort_key = staticmethod(_sort_key)
AccessManager._normalize_role = staticmethod(_normalize_role)
AccessManager._normalize_tariff = staticmethod(_normalize_tariff)
AccessManager._normalize_user_status = staticmethod(_normalize_user_status)
AccessManager._normalize_key_status = staticmethod(_normalize_key_status)
AccessManager._is_valid_key_format = staticmethod(_is_valid_key_format)
AccessManager._safe_int = staticmethod(_safe_int)
AccessManager._safe_ts = staticmethod(_safe_ts)
AccessManager._normalize_scope = _normalize_scope
AccessManager._copy_user = staticmethod(_copy_user)
AccessManager._format_public_dt = staticmethod(_format_public_dt)
AccessManager._copy_key = staticmethod(_copy_key)
AccessManager._role_title = staticmethod(_role_title)
AccessManager._tariff_title = staticmethod(_tariff_title)
AccessManager._default_tariff_for_role = staticmethod(_default_tariff_for_role)
