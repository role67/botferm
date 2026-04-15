from __future__ import annotations

import asyncio
from dataclasses import dataclass
from zoneinfo import ZoneInfo

from aiogram import Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import BaseFilter
from aiogram.types import CallbackQuery, Message, TelegramObject

from bot.keyboards import main_menu_keyboard
from core.access_manager import AccessManager
from core.accounts import AccountManager
from core.queue import TaskQueue
from core.session_store import PostgresSessionStore
from task_application import TaskApplicationService

SESSION_MANAGER_PAGE_SIZE = 10
MOSCOW_TZ = ZoneInfo("Europe/Moscow")


class AllowedUserFilter(BaseFilter):
    def __init__(self, access_manager: AccessManager) -> None:
        self.access_manager = access_manager

    async def __call__(self, event: TelegramObject) -> bool:
        user = getattr(event, "from_user", None)
        return bool(user and await self.access_manager.is_allowed(user.id))


@dataclass(slots=True)
class PendingAction:
    action: str
    session: str | None = None
    mode: str | None = None


@dataclass(slots=True)
class HandlerContext:
    task_queue: TaskQueue
    task_service: TaskApplicationService
    account_manager: AccountManager
    access_manager: AccessManager
    session_store: PostgresSessionStore | None
    pending_actions: dict[int, PendingAction]
    task_manager_live_views: dict[tuple[int, int], asyncio.Task]
    task_manager_live_state: set[tuple[int, int]]
    admin_nav_state: dict[tuple[int, int], str]


def main_panel_text() -> str:
    return "<b>🎛 Панель управления</b>\n\nВыберите раздел ниже."


def task_view_key(message: Message) -> tuple[int, int] | None:
    if message.chat is None or message.message_id is None:
        return None
    return message.chat.id, message.message_id


def stop_task_manager_live_view(ctx: HandlerContext, message: Message | None) -> None:
    if message is None:
        return
    key = task_view_key(message)
    if key is None:
        return
    ctx.task_manager_live_state.discard(key)
    task = ctx.task_manager_live_views.pop(key, None)
    if task is not None:
        task.cancel()


def set_admin_nav_state(ctx: HandlerContext, message: Message | None, state: str | None) -> None:
    if message is None:
        return
    key = task_view_key(message)
    if key is None:
        return
    if state is None:
        ctx.admin_nav_state.pop(key, None)
        return
    ctx.admin_nav_state[key] = state


def get_admin_nav_state(ctx: HandlerContext, message: Message | None) -> str | None:
    if message is None:
        return None
    key = task_view_key(message)
    if key is None:
        return None
    return ctx.admin_nav_state.get(key)


async def safe_callback_answer(
    callback: CallbackQuery,
    text: str | None = None,
    show_alert: bool = False,
) -> None:
    try:
        await callback.answer(text=text, show_alert=show_alert)
    except TelegramBadRequest:
        pass


async def ensure_active_message_access(message: Message, access_manager: AccessManager) -> bool:
    user_id = getattr(message.from_user, "id", None)
    if user_id is None:
        await message.answer("Не удалось определить Telegram ID.")
        return False
    state = await access_manager.access_state(user_id)
    if state == "missing":
        await message.answer(access_manager.not_registered_message())
        return False
    if state != "active":
        await message.answer(await access_manager.pending_key_message(user_id))
        return False
    return True


async def ensure_access_owner(callback: CallbackQuery, access_manager: AccessManager) -> bool:
    user_id = getattr(callback.from_user, "id", None)
    if user_id is None or not await access_manager.can_manage_access(user_id):
        await safe_callback_answer(callback, text="Управление доступом доступно только владельцу.", show_alert=True)
        return False
    return True


async def ensure_owner_level_settings(callback: CallbackQuery, access_manager: AccessManager) -> bool:
    user_id = getattr(callback.from_user, "id", None)
    if user_id is None or not await access_manager.can_manage_owner_settings(user_id):
        await safe_callback_answer(callback, text="Эта настройка доступна только владельцу.", show_alert=True)
        return False
    return True


async def ensure_accounts_menu_access(callback: CallbackQuery, access_manager: AccessManager) -> bool:
    user_id = getattr(callback.from_user, "id", None)
    if user_id is None or not await access_manager.can_access_accounts_menu(user_id):
        await safe_callback_answer(callback, text="Раздел аккаунтов недоступен для вашей роли.", show_alert=True)
        return False
    return True


async def ensure_account_creation_access(callback: CallbackQuery, access_manager: AccessManager) -> bool:
    user_id = getattr(callback.from_user, "id", None)
    if user_id is None or not await access_manager.can_add_accounts(user_id):
        await safe_callback_answer(callback, text="У вас нет прав на добавление аккаунтов.", show_alert=True)
        return False
    return True


async def ensure_account_access(
    callback: CallbackQuery,
    access_manager: AccessManager,
    account_manager: AccountManager,
    session: str,
) -> bool:
    user_id = getattr(callback.from_user, "id", None)
    if user_id is None:
        await safe_callback_answer(callback, text="Пользователь не определён.", show_alert=True)
        return False
    try:
        account_owner_id = account_manager.get_account_owner_id(session)
    except Exception as exc:
        await safe_callback_answer(callback, text=str(exc), show_alert=True)
        return False
    if not await access_manager.can_manage_account_owner(user_id, account_owner_id):
        await safe_callback_answer(callback, text="У вас нет доступа к этому аккаунту.", show_alert=True)
        return False
    return True


async def render_main_panel(message: Message, access_manager: AccessManager, *, user_id: int) -> None:
    await message.answer(
        main_panel_text(),
        reply_markup=main_menu_keyboard(
            show_accounts=await access_manager.can_access_accounts_menu(user_id),
            show_owner_panel=await access_manager.can_manage_access(user_id),
            show_profile=True,
        ),
    )


def build_router(
    task_queue: TaskQueue,
    task_service: TaskApplicationService,
    account_manager: AccountManager,
    access_manager: AccessManager,
    *,
    session_store: PostgresSessionStore | None,
) -> Router:
    from bot.handler_access import register_access_handlers
    from bot.handler_accounts import register_account_handlers
    from bot.handler_tasks import register_task_handlers

    router = Router()
    ctx = HandlerContext(
        task_queue=task_queue,
        task_service=task_service,
        account_manager=account_manager,
        access_manager=access_manager,
        session_store=session_store,
        pending_actions={},
        task_manager_live_views={},
        task_manager_live_state=set(),
        admin_nav_state={},
    )
    register_access_handlers(router, ctx)
    register_account_handlers(router, ctx)
    register_task_handlers(router, ctx)
    return router
