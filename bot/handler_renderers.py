from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from html import escape
from math import ceil
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.types import CallbackQuery, Message

from bot.handlers import PendingAction, SESSION_MANAGER_PAGE_SIZE
from bot.keyboards import (
    access_manager_keyboard,
    access_user_actions_keyboard,
    access_user_role_keyboard,
    access_user_tariff_keyboard,
    access_users_keyboard,
    admin_confirm_stop_all_keyboard,
    accounts_menu_keyboard,
    dc_control_keyboard,
    health_settings_keyboard,
    healthcheck_keyboard,
    help_back_keyboard,
    profile_keyboard,
    session_manager_keyboard,
    task_actions_keyboard,
    task_created_keyboard,
    task_manager_keyboard,
)
from config import LOGS_DIR
from core.access_manager import AccessManager, AccessUser, ROLE_ADMIN, ROLE_EXTERNAL, ROLE_INTERNAL, ROLE_OWNER
from core.accounts import AccountManager
from core.observability import tail_jsonl_file
from core.queue import TaskQueue

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def progress_text(current: int, total: int, session: str) -> str:
    return (
        f"<b>🔎 Проверка аккаунтов: {current} / {total}</b>\n"
        f"Текущий: {escape(session)}"
    )


def format_session_manager(
    accounts: list[dict],
    account_manager: AccountManager,
    *,
    page: int,
    page_size: int,
) -> str:
    summary = account_manager.build_health_summary(accounts)
    dc_summary = account_manager.build_dc_summary(accounts)
    total_pages = max(1, (len(accounts) + page_size - 1) // page_size)
    current_page = min(max(1, page), total_pages)

    dc_parts = [f"🌍 {label}: {count}" for label, count in dc_summary] or ["🌍 DC: нет данных"]
    lines = [
        "<b>🗂 Сессии</b>",
        "",
        f"Всего: {summary['total']}",
        f"В пуле: {summary['in_pool']}",
        "",
        f"🟢 Живые: {summary['alive']}",
        f"🔴 Баны: {summary['banned']}",
        "",
        "",
        *dc_parts,
        "",
        f"Стр. {current_page}/{total_pages}",
    ]
    return "\n".join(lines)


def access_identity(user: AccessUser) -> str:
    return f"id{user.telegram_id}"


def role_label(role: str) -> str:
    mapping = {
        ROLE_OWNER: "Владелец",
        ROLE_ADMIN: "Админ",
        ROLE_INTERNAL: "Internal",
        ROLE_EXTERNAL: "External",
    }
    return mapping.get(role, role or "-")


def access_full_name(user: AccessUser) -> str:
    return " ".join(part for part in [user.first_name, user.last_name] if part).strip() or "-"


def display_user_role(user: AccessUser, users: list[AccessUser], owner_user_id: int) -> str:
    if user.status == "blocked":
        return "Заблокирован"
    if not str(user.role or "").strip():
        return "-"
    if user.role != ROLE_OWNER:
        return role_label(user.role)

    owners = [item for item in users if item.role == ROLE_OWNER]
    owners.sort(key=lambda item: (0 if item.telegram_id == owner_user_id else 1, item.created_at or 0, item.telegram_id))
    for index, item in enumerate(owners, start=1):
        if item.telegram_id == user.telegram_id:
            return "Владелец" if index == 1 else f"Владелец {index}"
    return "Владелец"


def serialize_access_user(user: AccessUser) -> dict:
    return {
        "telegram_id": user.telegram_id,
        "role": user.role,
        "owner_scope_id": user.owner_scope_id,
    }


def format_dt(value: float | None) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value).strftime("%d.%m.%Y %H:%M:%S")


def format_public_dt(value: float | None) -> str:
    if not value:
        return "-"
    return datetime.fromtimestamp(value, tz=MOSCOW_TZ).strftime("%d.%m.%Y • %H:%M")


def truncate(value: str, limit: int) -> str:
    return value if len(value) <= limit else f"{value[: limit - 3]}..."


def render_unblocked_notice(user: AccessUser) -> str:
    username = f"@{user.username}" if user.username else f"id{user.telegram_id}"
    return (
        "<b>✅ Доступ восстановлен</b>\n\n"
        f"Пользователь: {escape(username)}\n"
        f"Когда: {format_public_dt(time.time())}"
    )


def format_access_manager(users: list[AccessUser], owner_user_id: int) -> str:
    return (
        "<b>🔐 Админ-панель</b>\n\n"
        f"👤 <code>{owner_user_id}</code>\n"
        f"👥 {len(users)} пользователей\n\n"
        "Выберите действие:"
    )


def format_access_users_hub(users: list[AccessUser], owner_user_id: int) -> str:
    return (
        "<b>👥 Пользователи</b>\n\n"
        f"Всего: {len(users)}\n\n"
        "Выберите пользователя:"
    )


def format_access_keys_info(owner_user_id: int) -> str:
    return (
        "<b>➕ Выдать доступ</b>\n\n"
        "📌 Формат:\n"
        "<code>/creatkey id роль тариф</code>\n\n"
        "📝 Пример:\n"
        "<code>/creatkey 123456789 internal Trial</code>\n\n"
        "👤 Роли: <code>owner</code>, <code>admin</code>, <code>internal</code>, <code>external</code>\n"
        "💎 Тарифы: <code>Trial</code>, <code>Standard</code>, <code>Pro</code>, <code>Enterprise</code>"
    )


def format_access_users_info(users: list[AccessUser], owner_user_id: int) -> str:
    return format_access_users_hub(users, owner_user_id)


async def format_access_user_detail(
    user: AccessUser,
    access_manager,
    *,
    accounts_count: int = 0,
    tasks_count: int = 0,
) -> str:
    role_display = (
        "Заблокирован"
        if user.status == "blocked"
        else await access_manager.role_display_label(user.telegram_id, user.role) if user.role else "-"
    )
    lines = [
        "<b>👤 Пользователь</b>",
        "",
        f"🆔 <code>{user.telegram_id}</code>",
    ]
    if user.username:
        lines.append(f"👤 <code>@{escape(user.username)}</code>")
    lines.extend(
        [
            "",
            f"🎭 Роль: {escape(role_display)}",
            f"💼 Тариф: {escape(user.tariff.title() if user.tariff else '-')}",
            "",
            f"📂 Аккаунты: {accounts_count}",
            f"🗂 Активные задачи: {tasks_count}",
        ]
    )
    if user.blocked_reason:
        lines.extend(["", f"Причина: <code>{escape(user.blocked_reason)}</code>"])
    return "\n".join(lines)


def format_healthcheck(accounts: list[dict], account_manager: AccountManager) -> str:
    summary = account_manager.build_health_summary(accounts)
    settings = account_manager.health_settings()
    return "\n".join(
        [
            "<b>📊 Аккаунты</b>",
            "",
            f"Всего: {summary['total']} | 🟢 {summary['alive']} 🟡 {summary['limited']} 🔴 {summary['banned']}",
            f"В пуле: {summary['in_pool']}",
            f"⚙️ Авто-вывод: {'ВКЛ' if settings['auto_remove_from_pool'] else 'ВЫКЛ'} | "
            f"🔔 Увед: {'ВКЛ' if settings['notifications_enabled'] else 'ВЫКЛ'}",
        ]
    )


def format_health_settings(account_manager: AccountManager) -> str:
    settings = account_manager.health_settings()
    return (
        "<b>⚙️ Настройки</b>\n\n"
        f"🛡 Авто-вывод: {'ВКЛ' if settings['auto_remove_from_pool'] else 'ВЫКЛ'}\n"
        f"🔔 Уведомления: {'ВКЛ' if settings['notifications_enabled'] else 'ВЫКЛ'}"
    )


def format_dc_overview(accounts: list[dict], account_manager: AccountManager) -> str:
    dc_summary = account_manager.build_dc_summary(accounts)
    lines = ["<b>🌍 DC</b>", ""]
    if not dc_summary:
        lines.append("Нет данных.")
    else:
        lines.extend(f"{label}: <b>{count}</b>" for label, count in dc_summary)
    return "\n".join(lines)


def format_account_view(item: dict) -> str:
    pool_text = "в пуле" if item.get("in_pool", True) else "вне пула"
    reason = (item.get("reason") or "").strip() or "-"
    dc_label = item["dc_id"] if item.get("dc_id") is not None else "?"
    username = item.get("username") or "-"
    first_name = item.get("first_name") or "-"
    return (
        "<b>👤 Аккаунт</b>\n\n"
        f"🆔 <code>{item.get('id', '-')}</code>\n"
        f"🗂 <code>{escape(item['session'])}</code>\n"
        f"👤 <code>{escape(username)}</code>\n"
        f"📝 <code>{escape(first_name)}</code>\n\n"
        f"Состояние: <b>{escape(item.get('account_state_label', '-'))}</b>\n"
        f"Здоровье: <b>{escape(item.get('health_label', 'ALIVE'))}</b>\n"
        f"Пул: <b>{pool_text}</b>\n"
        f"DC: <b>{dc_label}</b>\n"
        f"Причина: <code>{escape(reason)}</code>"
    )


def task_status_label(status: str) -> str:
    mapping = {
        "queued": "в очереди",
        "paused": "на паузе",
        "running": "выполняется",
        "cancel_requested": "останавливается",
        "completed": "завершена",
        "failed": "ошибка",
        "canceled": "отменена",
    }
    return mapping.get(status, status)


def format_task_manager(tasks: list[dict], stats: dict) -> str:
    lines = [
        "<b>🗂 Задачи</b>",
        "",
        f"▶️ Активные: <b>{stats['running']}</b>",
        f"⏸ На паузе: <b>{stats['paused']}</b>",
        f"🕒 Ожидание: <b>{stats['queued']}</b>",
        "",
        "📌 Выберите задачу ниже:" if tasks else "📌 Нет активных задач.",
    ]
    return "\n".join(lines)


def format_task_detail(item: dict) -> str:
    delay = item.get("delay")
    if isinstance(delay, tuple):
        delay_text = f"{delay[0]}-{delay[1]}"
    elif delay is None:
        delay_text = ""
    else:
        delay_text = str(delay)
    lines = [
        f"<b>🗂 Задача #{item['id']}</b>",
        "",
        f"Тип: {escape(item['kind'])}",
        f"Статус: {escape(task_status_label(item['status']))}",
        "",
        f"👥 Аккаунтов: {item.get('accounts_count') or 0}",
    ]
    if delay_text:
        lines.append(f"⏱ Задержка: {escape(delay_text)}")
    return "\n".join(lines)


def format_admin_stats(*, tasks: int, alive: int, limited: int, banned: int, errors: int) -> str:
    return (
        "<b>📊 Статистика</b>\n\n"
        f"🗂 Активные задачи: {tasks}\n\n"
        "📂 Аккаунты:\n"
        f"🟢 Активные: {alive}\n"
        f"🟡 Ограничены: {limited}\n"
        f"🔴 Ошибки: {banned}\n\n"
        f"⚠️ Ошибки за 5 мин: {errors}"
    )


def format_admin_stop_all() -> str:
    return (
        "<b>🛑 Остановить все задачи?</b>\n\n"
        "Это действие остановит все активные процессы."
    )


async def safe_edit_message(
    message: Message,
    text: str,
    *,
    reply_markup=None,
    retries: int = 3,
) -> bool:
    for attempt in range(1, retries + 1):
        try:
            await message.edit_text(text, reply_markup=reply_markup)
            return True
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return True
            if attempt == retries:
                raise
            await asyncio.sleep(0.5 * attempt)
        except TelegramNetworkError:
            if attempt == retries:
                return False
            await asyncio.sleep(attempt)
    return False


async def set_pending_edit(
    callback: CallbackQuery,
    pending_actions: dict[int, PendingAction],
    action: str,
    prompt_text: str,
    safe_callback_answer,
) -> None:
    if not callback.data or not callback.from_user:
        await safe_callback_answer(callback)
        return
    parts = callback.data.rsplit(":", maxsplit=1)
    if len(parts) != 2 or not parts[1]:
        await safe_callback_answer(callback, text="Некорректные данные кнопки.", show_alert=True)
        return

    session = parts[1]
    pending_actions[callback.from_user.id] = PendingAction(action=action, session=session)
    if callback.message:
        await callback.message.answer(
            "<b>✏️ Редактирование аккаунта</b>\n"
            f"Сессия: <code>{escape(session)}</code>\n"
            f"{prompt_text}"
        )
    await safe_callback_answer(callback)


async def render_accounts_menu(
    message: Message,
    *,
    access_manager: AccessManager,
    current_user_id: int,
) -> None:
    user = await access_manager.get_user(current_user_id)
    is_private_tenant = bool(user and user.role == ROLE_EXTERNAL)
    description = (
        "<b>👤 Аккаунты</b>\n\n"
        "📂 Сессии: список, релогин, DC, действия\n"
        "📥 Импорт: загрузка .session\n"
        "📤 Экспорт: выгрузка сессий\n\n"
        "📊 Статус: 🟢 живые / 🟡 лимит / 🔴 бан"
    )
    await safe_edit_message(
        message,
        description,
        reply_markup=accounts_menu_keyboard(
            can_import_accounts=await access_manager.can_add_accounts(current_user_id),
            can_export_accounts=await access_manager.can_export_accounts(current_user_id),
            is_private_tenant=is_private_tenant,
        ),
    )


async def render_access_manager(message: Message, access_manager: AccessManager) -> None:
    users = await access_manager.list_users()
    text = format_access_manager(users, access_manager.owner_user_id)
    keyboard = access_manager_keyboard(
        [serialize_access_user(user) for user in users],
        owner_user_id=access_manager.owner_user_id,
    )
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_access_grant(message: Message, access_manager: AccessManager) -> None:
    text = format_access_keys_info(access_manager.owner_user_id)
    updated = await safe_edit_message(message, text, reply_markup=help_back_keyboard("back"))
    if not updated:
        await message.answer(text, reply_markup=help_back_keyboard("back"))


async def render_access_users(message: Message, access_manager: AccessManager) -> None:
    await render_access_users_page(message, access_manager, page=1)


async def render_access_users_page(message: Message, access_manager: AccessManager, *, page: int) -> None:
    users = await access_manager.list_users()
    page_size = 10
    total_pages = max(1, ceil(len(users) / page_size))
    current_page = min(max(1, page), total_pages)
    page_users = users[(current_page - 1) * page_size: current_page * page_size]
    serialized = [{"telegram_id": user.telegram_id, "username": user.username or ""} for user in page_users]
    text = format_access_users_hub(users, access_manager.owner_user_id)
    keyboard = access_users_keyboard(serialized, page=current_page, total_pages=total_pages)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_access_user_detail(
    message: Message,
    access_manager: AccessManager,
    task_queue: TaskQueue,
    account_manager: AccountManager,
    *,
    user_id: int,
) -> None:
    user = await access_manager.get_user(user_id)
    if user is None:
        await safe_edit_message(message, "Пользователь не найден.", reply_markup=help_back_keyboard("back"))
        return

    owner_ids = None if user.role in {"owner", "admin", "internal"} else {user.owner_scope_id}
    accounts = await account_manager.list_accounts_status(force_refresh=False, owner_ids=owner_ids)
    accounts_count = len(accounts)
    tasks_count = task_queue.qsize(
        requested_by_user_id=user.telegram_id,
        include_all=await access_manager.can_view_all_tasks(user.telegram_id),
    )
    text = await format_access_user_detail(
        user,
        access_manager,
        accounts_count=accounts_count,
        tasks_count=tasks_count,
    )
    keyboard = access_user_actions_keyboard(telegram_id=user.telegram_id, is_blocked=user.status == "blocked")
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_access_user_role_menu(message: Message, access_manager: AccessManager, *, user_id: int) -> None:
    user = await access_manager.get_user(user_id)
    if user is None:
        await safe_edit_message(message, "Пользователь не найден.", reply_markup=help_back_keyboard("back"))
        return
    text = (
        "<b>🎭 Сменить роль</b>\n\n"
        f"ID: <code>{user.telegram_id}</code>\n"
        f"Текущая роль: {escape(user.role or '-')}"
    )
    keyboard = access_user_role_keyboard(telegram_id=user.telegram_id)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_access_user_tariff_menu(message: Message, access_manager: AccessManager, *, user_id: int) -> None:
    user = await access_manager.get_user(user_id)
    if user is None:
        await safe_edit_message(message, "Пользователь не найден.", reply_markup=help_back_keyboard("back"))
        return
    text = (
        "<b>💼 Сменить тариф</b>\n\n"
        f"ID: <code>{user.telegram_id}</code>\n"
        f"Текущий тариф: {escape(user.tariff.title() if user.tariff else '-')}"
    )
    keyboard = access_user_tariff_keyboard(telegram_id=user.telegram_id)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def _render_accounts_with_progress(
    message: Message,
    account_manager: AccountManager,
    access_manager: AccessManager,
    requester_user_id: int,
    force_refresh: bool,
    loading_label: str,
    *,
    include_spam_check: bool = False,
) -> list[dict]:
    owner_ids = await access_manager.visible_account_owner_ids(requester_user_id)
    last_update_ts = 0.0

    async def on_progress(current: int, total: int, session: str) -> None:
        nonlocal last_update_ts
        now = time.monotonic()
        if current != total and now - last_update_ts < 0.6:
            return
        last_update_ts = now
        await safe_edit_message(message, progress_text(current, total, session))

    await safe_edit_message(message, progress_text(0, 0, loading_label))
    return await account_manager.list_accounts_status(
        force_refresh=force_refresh,
        progress_cb=on_progress,
        owner_ids=owner_ids,
        include_spam_check=include_spam_check,
    )


async def render_session_manager_with_progress(
    message: Message,
    account_manager: AccountManager,
    access_manager: AccessManager,
    requester_user_id: int,
    force_refresh: bool,
    page: int = 1,
) -> None:
    accounts = await _render_accounts_with_progress(
        message,
        account_manager,
        access_manager,
        requester_user_id,
        force_refresh,
        "загрузка...",
    )
    text = format_session_manager(accounts, account_manager, page=page, page_size=SESSION_MANAGER_PAGE_SIZE)
    keyboard = session_manager_keyboard(
        accounts,
        can_add_accounts=await access_manager.can_add_accounts(requester_user_id),
        page=page,
        page_size=SESSION_MANAGER_PAGE_SIZE,
    )
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_healthcheck_with_progress(
    message: Message,
    account_manager: AccountManager,
    access_manager: AccessManager,
    requester_user_id: int,
    force_refresh: bool,
    page: int = 1,
) -> None:
    accounts = await _render_accounts_with_progress(
        message,
        account_manager,
        access_manager,
        requester_user_id,
        force_refresh,
        "проверка...",
        include_spam_check=True,
    )
    settings = account_manager.health_settings()
    keyboard = healthcheck_keyboard(
        accounts=accounts,
        auto_remove_enabled=settings["auto_remove_from_pool"],
        notifications_enabled=settings["notifications_enabled"],
        can_manage_owner_settings=await access_manager.can_manage_owner_settings(requester_user_id),
        page=page,
        page_size=SESSION_MANAGER_PAGE_SIZE,
    )
    text = format_healthcheck(accounts, account_manager)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_health_settings(
    message: Message,
    *,
    account_manager: AccountManager,
    page: int,
) -> None:
    text = format_health_settings(account_manager)
    keyboard = health_settings_keyboard(page=page)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_dc_overview(
    message: Message,
    account_manager: AccountManager,
    access_manager: AccessManager,
    requester_user_id: int,
    force_refresh: bool,
) -> None:
    accounts = await account_manager.list_accounts_status(
        force_refresh=force_refresh,
        owner_ids=await access_manager.visible_account_owner_ids(requester_user_id),
    )
    text = format_dc_overview(accounts, account_manager)
    updated = await safe_edit_message(message, text, reply_markup=dc_control_keyboard())
    if not updated:
        await message.answer(text, reply_markup=dc_control_keyboard())


async def render_task_manager(
    message: Message,
    task_queue: TaskQueue,
    *,
    requester_user_id: int,
    include_all: bool,
    back_callback: str = "menu:home",
) -> None:
    tasks = await task_queue.list_tasks(limit=20, requested_by_user_id=requester_user_id, include_all=include_all)
    stats = await task_queue.stats(requested_by_user_id=requester_user_id, include_all=include_all)
    text = format_task_manager(tasks, stats)
    keyboard = task_manager_keyboard(tasks, has_finished=bool(stats["has_finished"]), back_callback=back_callback)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_admin_stats(
    message: Message,
    task_queue: TaskQueue,
    account_manager: AccountManager,
) -> None:
    task_stats = await task_queue.stats(requested_by_user_id=None, include_all=True)
    accounts = await account_manager.list_accounts_status(force_refresh=False, owner_ids=None)
    health = account_manager.build_health_summary(accounts)
    now = datetime.now(timezone.utc)
    errors = 0
    for row in tail_jsonl_file(LOGS_DIR / "app.jsonl", limit=1000):
        if str(row.get("level", "")).upper() != "ERROR":
            continue
        ts = row.get("ts")
        try:
            dt = datetime.fromisoformat(str(ts))
        except Exception:
            continue
        if (now - dt.astimezone(timezone.utc)).total_seconds() <= 300:
            errors += 1
    text = format_admin_stats(
        tasks=task_stats["active_total"],
        alive=health["alive"],
        limited=health["limited"],
        banned=health["banned"],
        errors=errors,
    )
    updated = await safe_edit_message(message, text, reply_markup=help_back_keyboard("back"))
    if not updated:
        await message.answer(text, reply_markup=help_back_keyboard("back"))


async def render_admin_stop_all(message: Message) -> None:
    text = format_admin_stop_all()
    updated = await safe_edit_message(message, text, reply_markup=admin_confirm_stop_all_keyboard())
    if not updated:
        await message.answer(text, reply_markup=admin_confirm_stop_all_keyboard())


async def render_profile(
    message: Message,
    *,
    account_manager: AccountManager,
    access_manager: AccessManager,
    task_queue: TaskQueue,
    requester_user_id: int,
    telegram_username: str | None,
) -> None:
    user = await access_manager.get_user(requester_user_id)
    if user is None:
        await safe_edit_message(message, "Профиль не найден.", reply_markup=help_back_keyboard("menu:home"))
        return
    accounts = await account_manager.list_accounts_status(
        force_refresh=False,
        owner_ids=await access_manager.visible_account_owner_ids(requester_user_id),
    )
    active_key = await access_manager.get_active_key_for_user(requester_user_id)
    expires_text = format_public_dt(active_key.expires_at) if active_key and active_key.expires_at else "Без срока"
    username = f"@{telegram_username}" if telegram_username else (f"@{user.username}" if user.username else "-")
    loaded_accounts = len(accounts)
    active_accounts = sum(1 for item in accounts if item.get("available_for_tasks"))
    active_tasks = task_queue.qsize(
        requested_by_user_id=requester_user_id,
        include_all=await access_manager.can_view_all_tasks(requester_user_id),
    )
    text = (
        "<b>👤 Профиль</b>\n\n"
        f"🆔 {requester_user_id}\n"
        f"👤 {escape(username)}\n\n"
        f"💼 Тариф: {escape(user.tariff.title())}\n"
        f"⏳ {escape(expires_text)}\n\n"
        "📂 Аккаунты\n"
        f"Всего: {loaded_accounts} • Активные: {active_accounts}\n\n"
        "🗂 Задачи\n"
        f"▶️ {active_tasks} / 1"
    )
    keyboard = profile_keyboard(show_accounts=await access_manager.can_access_accounts_menu(requester_user_id))
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def render_task_detail(
    message: Message,
    task_queue: TaskQueue,
    task_id: int,
    *,
    requester_user_id: int,
    include_all: bool,
    back_callback: str = "menu:tasks",
) -> None:
    item = await task_queue.get_task(task_id, requested_by_user_id=requester_user_id, include_all=include_all)
    if item is None:
        await safe_edit_message(
            message,
            "Задача не найдена или у вас нет к ней доступа.",
            reply_markup=help_back_keyboard("back"),
        )
        return
    text = format_task_detail(item)
    keyboard = task_actions_keyboard(task_id, item["status"], back_callback=back_callback)
    updated = await safe_edit_message(message, text, reply_markup=keyboard)
    if not updated:
        await message.answer(text, reply_markup=keyboard)


async def format_task_created_message(
    task_queue: TaskQueue,
    access_manager: AccessManager,
    *,
    requester_user_id: int,
    task_id: int,
    command: str,
    accounts_count: int,
    extra_lines: list[str] | None = None,
) -> str:
    include_all = await access_manager.can_view_all_tasks(requester_user_id)
    active_tasks = task_queue.qsize(requested_by_user_id=requester_user_id, include_all=include_all)
    lines = [
        f"Задача <b>#{task_id}</b> добавлена в менеджер задач.",
        f"Тип: <b>{escape(command)}</b>",
        f"Аккаунтов: <b>{accounts_count}</b>",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    lines.extend(["", f"Активных задач: <b>{active_tasks}</b>"])
    return "\n".join(lines)


async def reply_task_created(
    message: Message,
    task_queue: TaskQueue,
    access_manager: AccessManager,
    *,
    task_id: int,
    command: str,
    accounts_count: int,
    extra_lines: list[str] | None = None,
) -> None:
    response = await message.answer(
        await format_task_created_message(
            task_queue,
            access_manager,
            requester_user_id=message.from_user.id,
            task_id=task_id,
            command=command,
            accounts_count=accounts_count,
            extra_lines=extra_lines,
        ),
        reply_markup=task_created_keyboard(task_id),
    )
    await task_queue.bind_status_message(task_id, chat_id=response.chat.id, message_id=response.message_id)


async def rerender_account_view_or_session_manager(
    message: Message,
    *,
    account_manager: AccountManager,
    access_manager: AccessManager,
    requester_user_id: int,
    session: str,
    page: int,
    format_account_view=format_account_view,
    account_actions_keyboard,
) -> None:
    accounts = await account_manager.list_accounts_status(
        force_refresh=True,
        owner_ids=await access_manager.visible_account_owner_ids(requester_user_id),
    )
    target = next((item for item in accounts if item["session"] == session), None)
    if target is None:
        await render_session_manager_with_progress(
            message,
            account_manager,
            access_manager=access_manager,
            requester_user_id=requester_user_id,
            force_refresh=False,
            page=page,
        )
        return
    await message.edit_text(
        format_account_view(target),
        reply_markup=account_actions_keyboard(
            session,
            in_pool=bool(target.get("in_pool", True)),
            account_state=str(target.get("account_state", "")),
            page=page,
        ),
    )


