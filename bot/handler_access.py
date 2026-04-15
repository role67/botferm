from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html import escape

from aiogram import F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from bot.command_parsing import extract_payload as _extract_payload
from bot.command_parsing import parse_user_id as _parse_user_id
from bot.handlers import (
    MOSCOW_TZ,
    HandlerContext,
    ensure_access_owner,
    get_admin_nav_state,
    main_panel_text,
    render_main_panel,
    safe_callback_answer,
    set_admin_nav_state,
    stop_task_manager_live_view,
)
from bot.handler_renderers import (
    render_access_grant,
    render_access_manager,
    render_access_user_detail,
    render_access_user_role_menu,
    render_access_user_tariff_menu,
    render_access_users,
    render_access_users_page,
    render_accounts_menu,
    render_admin_stats,
    render_admin_stop_all,
    render_profile,
    render_task_detail,
    render_task_manager,
    safe_edit_message,
)
from bot.keyboards import (
    OWNER_PANEL_CALLBACK,
    OWNER_PANEL_HELP_CALLBACK,
    OWNER_PANEL_REFRESH_CALLBACK,
    main_menu_keyboard,
    menu_section_keyboard,
)
from bot.handler_renderers import access_full_name, render_unblocked_notice
from core.access_manager import AccessUser, ROLE_ADMIN, ROLE_EXTERNAL, ROLE_INTERNAL, ROLE_OWNER


async def _notify_user_unblocked(message: Message, user: AccessUser) -> None:
    try:
        await message.bot.send_message(user.telegram_id, render_unblocked_notice(user))
    except Exception:
        pass


@dataclass(slots=True)
class _StartNoticeUser:
    telegram_id: int
    username: str
    first_name: str
    last_name: str


async def _notify_owner_about_new_start(
    message: Message,
    access_manager,
    *,
    start_count: int,
    moscow_tz=None,
) -> None:
    user = message.from_user
    if user is None:
        return
    notice_user = _StartNoticeUser(
        telegram_id=user.id,
        username=f"@{user.username}" if user.username else "-",
        first_name=user.first_name or "",
        last_name=user.last_name or "",
    )
    full_name = access_full_name(
        AccessUser(
            telegram_id=notice_user.telegram_id,
            role="",
            owner_scope_id=access_manager.owner_user_id,
            username=notice_user.username.removeprefix("@") if notice_user.username != "-" else "",
            first_name=notice_user.first_name,
            last_name=notice_user.last_name,
        )
    )
    tz = moscow_tz or getattr(message.date, "tzinfo", None)
    message_dt = message.date.astimezone(tz) if message.date and tz else (message.date or datetime.now())
    formatted_dt = message_dt.strftime("%d.%m.%Y • %H:%M")
    text = (
        "<b>🚨 Активность</b>\n\n"
        "<blockquote>"
        f"Запуск команды /start ×{max(1, start_count)}\n\n"
        f"👤 Пользователь: {escape(notice_user.username)}\n"
        f"🆔 ID: id{notice_user.telegram_id}\n"
        f"📛 Имя: {escape(full_name)}\n\n"
        f"🕒 {formatted_dt}"
        "</blockquote>"
    )
    owner_ids = await access_manager.list_active_owner_ids()
    for owner_id in owner_ids:
        if owner_id == notice_user.telegram_id:
            continue
        try:
            message_id = await access_manager.get_start_notice_message_id(
                telegram_id=notice_user.telegram_id,
                owner_id=owner_id,
            )
            if message_id is not None:
                await message.bot.edit_message_text(chat_id=owner_id, message_id=message_id, text=text)
            else:
                sent = await message.bot.send_message(owner_id, text)
                await access_manager.set_start_notice_message_id(
                    telegram_id=notice_user.telegram_id,
                    owner_id=owner_id,
                    message_id=sent.message_id,
                )
        except Exception:
            try:
                sent = await message.bot.send_message(owner_id, text)
                await access_manager.set_start_notice_message_id(
                    telegram_id=notice_user.telegram_id,
                    owner_id=owner_id,
                    message_id=sent.message_id,
                )
            except Exception:
                pass


async def _notify_owner_about_start_block(message: Message, access_manager, *, moscow_tz) -> None:
    user = message.from_user
    if user is None:
        return
    username = f"@{user.username}" if user.username else "-"
    message_dt = message.date.astimezone(moscow_tz) if message.date else datetime.now(moscow_tz)
    formatted_dt = message_dt.strftime("%d.%m.%Y • %H:%M")
    text = (
        "<b>⛔ Блокировка</b>\n\n"
        "> 🚀 Спам команды /start\n\n"
        f"> 👤 {escape(username)} (id{user.id})\n\n"
        f"🗓 {formatted_dt}\n"
        f"📩 Для разблокировки: @{access_manager.support_username}"
    )
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔓 Разблокировать", callback_data=f"access:unblock:{user.id}")]
        ]
    )
    owner_ids = await access_manager.list_active_owner_ids()
    for owner_id in owner_ids:
        if owner_id == user.id:
            continue
        try:
            await message.bot.send_message(owner_id, text, reply_markup=keyboard)
        except Exception:
            pass


def register_access_handlers(router: Router, ctx: HandlerContext) -> None:
    access_manager = ctx.access_manager
    account_manager = ctx.account_manager
    task_queue = ctx.task_queue

    @router.message(CommandStart())
    async def start_handler(message: Message) -> None:
        user_id = getattr(message.from_user, "id", 0)
        state = await access_manager.access_state(user_id)
        if state == "blocked":
            user = await access_manager.get_user(user_id)
            await message.answer(access_manager.blocked_message(user))
            return
        if state != "active":
            start_info = await access_manager.register_start_attempt(
                telegram_id=user_id,
                username=getattr(message.from_user, "username", None),
                first_name=getattr(message.from_user, "first_name", None),
                last_name=getattr(message.from_user, "last_name", None),
                event_ts=message.date.timestamp() if message.date else None,
            )
            await _notify_owner_about_new_start(
                message,
                access_manager,
                start_count=int(start_info["count"]),
                moscow_tz=MOSCOW_TZ,
            )
            if bool(start_info["blocked_now"]):
                await _notify_owner_about_start_block(message, access_manager, moscow_tz=MOSCOW_TZ)
                if not bool(start_info["blocked_notice_sent"]):
                    await message.answer(await access_manager.pending_key_message(user_id))
                    await access_manager.mark_blocked_notice_sent(user_id)
                return
            if not bool(start_info["access_notice_sent"]):
                await message.answer(await access_manager.pending_key_message(user_id))
                await access_manager.mark_access_notice_sent(user_id)
            return

        await render_main_panel(message, access_manager, user_id=user_id)

    @router.message(Command("creatkey"))
    async def creatkey_handler(message: Message) -> None:
        user_id = getattr(message.from_user, "id", 0)
        if not message.from_user or not await access_manager.can_manage_access(user_id):
            await message.answer("Команда доступна только OWNER.")
            return

        payload = _extract_payload(message.text)
        if not payload:
            await message.answer(
                "<b>➕ Выдать доступ</b>\n\n"
                "📌 Формат:\n"
                "<code>/creatkey id роль тариф</code>\n\n"
                "📝 Пример:\n"
                "<code>/creatkey 123456789 internal Trial</code>\n\n"
                "👤 Роли: owner, admin, internal, external\n"
                "💎 Тарифы: Trial, Standard, Pro, Enterprise"
            )
            return

        try:
            telegram_id_raw, role_raw, tariff_raw = payload.split(maxsplit=2)
            access_key = await access_manager.create_access_key(
                telegram_id=int(telegram_id_raw),
                role=role_raw,
                tariff=tariff_raw,
            )
            role_label = await access_manager.role_display_label(access_key.telegram_id, access_key.role)
            await message.answer(
                "<b>✅ Ключ создан</b>\n\n"
                f"🆔 Telegram ID: <code>{access_key.telegram_id}</code>\n"
                f"👤 Роль: <b>{escape(role_label)}</b>\n"
                f"💎 Тариф: <b>{escape(access_key.tariff.title())}</b>\n"
                f"🔑 Ключ: <code>{access_key.key}</code>"
            )
        except Exception as exc:
            await message.answer(f"❌ Ошибка: {escape(str(exc))}")

    @router.callback_query(F.data == "menu:home")
    async def menu_home(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        set_admin_nav_state(ctx, callback.message, None)
        if callback.message:
            await callback.message.edit_text(
                main_panel_text(),
                reply_markup=main_menu_keyboard(
                    show_accounts=await access_manager.can_access_accounts_menu(getattr(callback.from_user, "id", 0)),
                    show_owner_panel=await access_manager.can_manage_access(getattr(callback.from_user, "id", 0)),
                    show_profile=True,
                ),
            )

    @router.callback_query(F.data == "menu:profile")
    async def menu_profile(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        set_admin_nav_state(ctx, callback.message, None)
        if callback.message:
            await render_profile(
                callback.message,
                account_manager=account_manager,
                access_manager=access_manager,
                task_queue=task_queue,
                requester_user_id=getattr(callback.from_user, "id", 0),
                telegram_username=getattr(callback.from_user, "username", None),
            )

    @router.callback_query(F.data == "menu:messaging")
    async def menu_messaging(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        set_admin_nav_state(ctx, callback.message, None)
        if callback.message:
            await callback.message.edit_text(
                "<b>💬 Сообщения</b>\n\nВыберите действие:",
                reply_markup=menu_section_keyboard("messaging"),
            )

    @router.callback_query(F.data == "menu:engagement")
    async def menu_engagement(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        set_admin_nav_state(ctx, callback.message, None)
        if callback.message:
            await callback.message.edit_text(
                "<b>🚀 Активность</b>\n\nВыберите действие:",
                reply_markup=menu_section_keyboard("engagement"),
            )

    @router.callback_query(F.data == "menu:accounts")
    async def menu_accounts(callback: CallbackQuery) -> None:
        if not await access_manager.can_access_accounts_menu(getattr(callback.from_user, "id", 0)):
            await safe_callback_answer(callback, text="Для вашей роли раздел аккаунтов недоступен.", show_alert=True)
            return
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        set_admin_nav_state(ctx, callback.message, None)
        if callback.message:
            await render_accounts_menu(
                callback.message,
                access_manager=access_manager,
                current_user_id=getattr(callback.from_user, "id", 0),
            )

    @router.callback_query(F.data.in_({OWNER_PANEL_CALLBACK, OWNER_PANEL_REFRESH_CALLBACK}))
    async def access_menu(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "menu")
            await render_access_manager(callback.message, access_manager)

    @router.callback_query(F.data.in_({OWNER_PANEL_HELP_CALLBACK, "admin_grant_access"}))
    async def access_grant(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "grant")
            await render_access_grant(callback.message, access_manager)

    @router.callback_query(F.data == "admin_users")
    async def access_users(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "users:1")
            await render_access_users(callback.message, access_manager)

    @router.callback_query(F.data.startswith("page:"))
    async def access_users_page(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        page = int(callback.data.split(":", maxsplit=1)[1])
        await safe_callback_answer(callback)
        set_admin_nav_state(ctx, callback.message, f"users:{page}")
        await render_access_users_page(callback.message, access_manager, page=page)

    @router.callback_query(F.data.startswith("user:"))
    async def access_user_card(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        user_id = _parse_user_id(callback.data.split(":", maxsplit=1)[1])
        page_state = get_admin_nav_state(ctx, callback.message) or "users:1"
        page = 1
        if page_state.startswith("users:"):
            page = int(page_state.split(":", maxsplit=1)[1])
        await safe_callback_answer(callback)
        set_admin_nav_state(ctx, callback.message, f"user:{user_id}:{page}")
        await render_access_user_detail(
            callback.message,
            access_manager,
            task_queue,
            account_manager,
            user_id=user_id,
        )

    @router.callback_query(F.data.startswith("user_role:"))
    async def access_user_role(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        user_id = _parse_user_id(callback.data.split(":", maxsplit=1)[1])
        prev_state = get_admin_nav_state(ctx, callback.message) or f"user:{user_id}:1"
        await safe_callback_answer(callback)
        set_admin_nav_state(ctx, callback.message, f"role:{user_id}:{prev_state}")
        await render_access_user_role_menu(callback.message, access_manager, user_id=user_id)

    @router.callback_query(F.data.startswith("user_tariff:"))
    async def access_user_tariff(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        user_id = _parse_user_id(callback.data.split(":", maxsplit=1)[1])
        prev_state = get_admin_nav_state(ctx, callback.message) or f"user:{user_id}:1"
        await safe_callback_answer(callback)
        set_admin_nav_state(ctx, callback.message, f"tariff:{user_id}:{prev_state}")
        await render_access_user_tariff_menu(callback.message, access_manager, user_id=user_id)

    @router.callback_query(F.data.startswith("user_role_set:"))
    async def access_user_role_set(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        _, user_id_raw, role = callback.data.split(":", maxsplit=2)
        user_id = _parse_user_id(user_id_raw)
        try:
            await access_manager.change_user_role(user_id, role)
            await safe_callback_answer(callback, text="Роль обновлена.")
        except Exception as exc:
            await safe_callback_answer(callback, text=str(exc), show_alert=True)
            return
        set_admin_nav_state(ctx, callback.message, f"user:{user_id}:1")
        await render_access_user_detail(callback.message, access_manager, task_queue, account_manager, user_id=user_id)

    @router.callback_query(F.data.startswith("user_tariff_set:"))
    async def access_user_tariff_set(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        _, user_id_raw, tariff = callback.data.split(":", maxsplit=2)
        user_id = _parse_user_id(user_id_raw)
        try:
            await access_manager.change_user_tariff(user_id, tariff)
            await safe_callback_answer(callback, text="Тариф обновлён.")
        except Exception as exc:
            await safe_callback_answer(callback, text=str(exc), show_alert=True)
            return
        set_admin_nav_state(ctx, callback.message, f"user:{user_id}:1")
        await render_access_user_detail(callback.message, access_manager, task_queue, account_manager, user_id=user_id)

    @router.callback_query(F.data.startswith("user_block:"))
    async def access_user_block(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        user_id = _parse_user_id(callback.data.split(":", maxsplit=1)[1])
        try:
            target_user = await access_manager.get_user(user_id)
            if target_user is None:
                raise ValueError("Пользователь не найден.")
            if target_user.status == "blocked":
                changed = await access_manager.unblock_user(user_id)
                target_user = await access_manager.get_user(user_id)
                if changed and target_user is not None:
                    await _notify_user_unblocked(callback.message, target_user)
                text = "Пользователь разблокирован."
            else:
                if user_id == access_manager.owner_user_id:
                    raise ValueError("Основного owner нельзя блокировать.")
                await access_manager.block_user(user_id, reason="manual block")
                text = "Пользователь заблокирован."
            await safe_callback_answer(callback, text=text)
        except Exception as exc:
            await safe_callback_answer(callback, text=str(exc), show_alert=True)
            return
        set_admin_nav_state(ctx, callback.message, f"user:{user_id}:1")
        await render_access_user_detail(callback.message, access_manager, task_queue, account_manager, user_id=user_id)

    @router.callback_query(F.data.startswith("user_delete:"))
    async def access_user_delete(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        user_id = _parse_user_id(callback.data.split(":", maxsplit=1)[1])
        page = 1
        state = get_admin_nav_state(ctx, callback.message) or ""
        if state.startswith("user:"):
            parts = state.split(":")
            if len(parts) >= 3 and parts[2].isdigit():
                page = int(parts[2])
        try:
            removed = await access_manager.remove_user(user_id)
            if not removed:
                await safe_callback_answer(callback, text="Пользователь уже удалён.")
            else:
                await safe_callback_answer(callback, text="Пользователь удалён из БД.")
        except Exception as exc:
            await safe_callback_answer(callback, text=str(exc), show_alert=True)
            return
        set_admin_nav_state(ctx, callback.message, f"users:{page}")
        await render_access_users_page(callback.message, access_manager, page=page)

    @router.callback_query(F.data == "admin_tasks")
    async def admin_tasks(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "tasks")
            await render_task_manager(callback.message, task_queue, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data.regexp(r"^task:\d+$"))
    async def admin_task_card(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=1)[1])
        await safe_callback_answer(callback)
        set_admin_nav_state(ctx, callback.message, f"task:{task_id}")
        await render_task_detail(callback.message, task_queue, task_id, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data.startswith("task_pause:"))
    async def admin_task_pause(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=1)[1])
        ok, text = await task_queue.pause_task(task_id, requested_by_user_id=None, include_all=True)
        await safe_callback_answer(callback, text=text, show_alert=not ok)
        await render_task_detail(callback.message, task_queue, task_id, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data.startswith("task_stop:"))
    async def admin_task_stop(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=1)[1])
        ok, text = await task_queue.cancel_task(task_id, requested_by_user_id=None, include_all=True)
        await safe_callback_answer(callback, text=text, show_alert=not ok)
        await render_task_detail(callback.message, task_queue, task_id, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data.startswith("task_restart:"))
    async def admin_task_restart(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=1)[1])
        ok, text, new_task_id = await task_queue.restart_task(task_id, requested_by_user_id=None, include_all=True)
        await safe_callback_answer(callback, text=text, show_alert=not ok)
        if ok and new_task_id is not None:
            set_admin_nav_state(ctx, callback.message, f"task:{new_task_id}")
            await render_task_detail(callback.message, task_queue, new_task_id, requester_user_id=0, include_all=True, back_callback="back")
            return
        await render_task_detail(callback.message, task_queue, task_id, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data == "admin_stats")
    async def admin_stats(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "stats")
            await render_admin_stats(callback.message, task_queue, account_manager)

    @router.callback_query(F.data == "admin_stop_all")
    async def admin_stop_all(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            set_admin_nav_state(ctx, callback.message, "stop_all")
            await render_admin_stop_all(callback.message)

    @router.callback_query(F.data == "confirm_stop_all")
    async def admin_stop_all_confirm(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        if not callback.message:
            await safe_callback_answer(callback)
            return
        stopped = await task_queue.stop_all_tasks(requested_by_user_id=None, include_all=True)
        await safe_callback_answer(callback, text=f"Остановлено задач: {stopped}")
        set_admin_nav_state(ctx, callback.message, "tasks")
        await render_task_manager(callback.message, task_queue, requester_user_id=0, include_all=True, back_callback="back")

    @router.callback_query(F.data == "back")
    async def admin_back(callback: CallbackQuery) -> None:
        if not await ensure_access_owner(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if not callback.message:
            return
        state = get_admin_nav_state(ctx, callback.message) or "menu"
        if state == "menu":
            set_admin_nav_state(ctx, callback.message, None)
            await callback.message.edit_text(
                main_panel_text(),
                reply_markup=main_menu_keyboard(
                    show_accounts=await access_manager.can_access_accounts_menu(getattr(callback.from_user, "id", 0)),
                    show_owner_panel=await access_manager.can_manage_access(getattr(callback.from_user, "id", 0)),
                    show_profile=True,
                ),
            )
            return
        if state.startswith("users:"):
            set_admin_nav_state(ctx, callback.message, "menu")
            await render_access_manager(callback.message, access_manager)
            return
        if state.startswith("user:"):
            page = int(state.rsplit(":", maxsplit=1)[1])
            set_admin_nav_state(ctx, callback.message, f"users:{page}")
            await render_access_users_page(callback.message, access_manager, page=page)
            return
        if state.startswith("role:") or state.startswith("tariff:"):
            parts = state.split(":", maxsplit=3)
            user_id = int(parts[1])
            set_admin_nav_state(ctx, callback.message, f"user:{user_id}:1")
            await render_access_user_detail(callback.message, access_manager, task_queue, account_manager, user_id=user_id)
            return
        if state in {"grant", "tasks", "stats", "stop_all"} or state.startswith("task:"):
            set_admin_nav_state(ctx, callback.message, "menu")
            await render_access_manager(callback.message, access_manager)
            return

