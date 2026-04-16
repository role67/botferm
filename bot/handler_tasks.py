from __future__ import annotations

import asyncio
import tempfile
import uuid
from html import escape
from pathlib import Path

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from bot.command_parsing import (
    extract_payload as _extract_payload,
    looks_like_access_key as _looks_like_access_key,
    parse_birthday as _parse_birthday,
    parse_likep_payload as _parse_likep_payload,
    parse_msg_payload as _parse_msg_payload,
    parse_msgbot_payload as _parse_msgbot_payload,
    parse_msgchat_payload as _parse_msgchat_payload,
    parse_refp_payload as _parse_refp_payload,
    parse_vote_payload as _parse_vote_payload,
)
from bot.handlers import HandlerContext, ensure_active_message_access, render_main_panel, safe_callback_answer, stop_task_manager_live_view
from bot.handler_renderers import (
    format_session_manager as _format_session_manager,
    render_task_detail,
    render_task_manager,
    reply_task_created,
)
from bot.handler_accounts import export_sessions_for_user as _export_sessions_for_user, resolve_export_sessions as _resolve_export_sessions
from bot.keyboards import activation_policies_keyboard, session_manager_keyboard
from config import MESSAGE_MEDIA_DIR, PRIVACY_POLICY_URL, TERMS_OF_SERVICE_URL, USAGE_POLICY_URL
from core.queue import LikeTask, MsgChatTask, MsgTask, RefTask, VoteTask

TASK_HELP_TEXTS = {
    "menu:call": "<b>📞 Звонки</b>\n\n<code>/call @username 5 3 5</code>\n<code>/call @username 3 10 5-7</code>\n\n<blockquote>/call ссылка аккаунты циклы задержка</blockquote>",
    "menu:msg": "<b>💬 Массовая отправка</b>\n\n<code>/msg @user1 @user2 hello 3 5 3</code>\n<code>/msg @user1 @user2 3 5 3</code> + фото\n<code>/msg @user1 @user2 secret 3 5 3 -h</code>\n\n<blockquote>/msg ссылка текст аккаунты циклы задержка флаг</blockquote>",
    "menu:msgbot": "<b>📩 Отправка боту</b>\n\n<code>/msgbot @testbot /start 10 5 3</code>\n\n<blockquote>/msgbot ссылка текст аккаунты циклы задержка</blockquote>",
    "menu:msgchat": "<b>💥 Спам в чат</b>\n\n<code>/msgchat https://t.me/publicchat hello guys 10 5 3</code>\n<code>/msgchat @publicchat 10 5 3</code> + фото\n<code>/msgchat @publicchat secret 10 5 3 -h</code>\n\n<blockquote>/msgchat ссылка текст аккаунты циклы задержка флаг</blockquote>",
    "menu:join": "<b>🔗 Вступление</b>\n\n<code>/join https://t.me/ab123cd 5 3-3.5</code>\n\n<blockquote>/join ссылка аккаунты задержка</blockquote>",
    "menu:leave": "<b>🚪 Выход</b>\n\n<code>/leave https://t.me/ab123cd 5 3-3.5</code>\n\n<blockquote>/leave ссылка аккаунты задержка</blockquote>",
    "menu:likep": "<b>👍 Реакции</b>\n\n<code>/likep https://t.me/channel/123 10 2 ❤️</code>\n<code>/likep https://t.me/channel/123 10 2 👍❤️</code>\n\n<blockquote>/likep ссылка аккаунты задержка реакции (от 1 до 5)</blockquote>",
    "menu:vote": "<b>🗳 Голосование</b>\n\n<code>/vote https://t.me/publicchannel/123 2 10 3-3.5</code>\n\n<blockquote>/vote ссылка вариант аккаунты задержка</blockquote>",
    "menu:refp": "<b>🎃 Рефералы</b>\n\n<code>/refp https://t.me/testbot?start=ref123 5 3-3.5</code>\n\n<blockquote>/refp ссылка аккаунты задержка</blockquote>",
}


def register_task_handlers(router: Router, ctx: HandlerContext) -> None:
    _register_task_callbacks(router, ctx)
    _register_task_help_callbacks(router)
    _register_task_command_slices(router, ctx)
    _register_task_commands(router, ctx)
    _register_pending_input(router, ctx)


def _register_task_command_slices(router: Router, ctx: HandlerContext) -> None:
    task_queue = ctx.task_queue
    task_service = ctx.task_service

    @router.message(Command("msg"))
    async def msg_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text or message.caption)
        msg_presenter = task_service.presenter_for("msg")
        if not payload:
            usage_text = '❌ <b>Пример рабочей команды:</b>\n<code>/msg @user1 @user2 "hello guys" 10 5 1.1</code>\n<code>/msg @user1 @user2 10 5 1.1</code> + фото'
            if msg_presenter is not None:
                usage_text = msg_presenter.usage_text()
            await message.answer(usage_text)
            return
        has_photo = bool(message.photo)
        try:
            spec = task_service.parse_task_payload("msg", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        if has_photo:
            spec.photo_path = await _download_message_photo(message)
        try:
            record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        command = msg_presenter.command if msg_presenter is not None else "/msg"
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.accounts_count, extra_lines=[f"Повторов: <b>{spec.repeat_count}</b>"])

    @router.message(Command("msgbot"))
    async def msgbot_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        msgbot_presenter = task_service.presenter_for("msgbot")
        if not payload:
            usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/msgbot @testbot /start 10 5 0.7</code>"
            if msgbot_presenter is not None:
                usage_text = msgbot_presenter.usage_text()
            await message.answer(usage_text)
            return
        try:
            spec = task_service.parse_task_payload("msgbot", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        command = msgbot_presenter.command if msgbot_presenter is not None else "/msgbot"
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.accounts_count, extra_lines=[f"Повторов: <b>{spec.repeat_count}</b>"])

    @router.message(Command("msgchat"))
    async def msgchat_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text or message.caption)
        msgchat_presenter = task_service.presenter_for("msgchat")
        if not payload:
            usage_text = '❌ <b>Пример рабочей команды:</b>\n<code>/msgchat https://t.me/publicchat "hello guys" 10 5 1.1</code>\n<code>/msgchat @publicchat 10 5 1.1</code> + фото'
            if msgchat_presenter is not None:
                usage_text = msgchat_presenter.usage_text()
            await message.answer(usage_text)
            return
        has_photo = bool(message.photo)
        try:
            spec = task_service.parse_task_payload("msgchat", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        if has_photo:
            spec.photo_path = await _download_message_photo(message)
        try:
            record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        command = msgchat_presenter.command if msgchat_presenter is not None else "/msgchat"
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.accounts_count, extra_lines=[f"Повторов: <b>{spec.repeat_count}</b>"])

    @router.message(Command("call"))
    async def call_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        call_presenter = task_service.presenter_for("call")
        if not payload:
            usage_text = "❌ <b>Example:</b>\n<code>/call @username 5 3 5</code>"
            if call_presenter is not None:
                usage_text = call_presenter.usage_text()
            await message.answer(usage_text)
            return
        try:
            spec = task_service.parse_task_payload("call", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        try:
            record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        command = call_presenter.command if call_presenter is not None else "/call"
        await reply_task_created(
            message,
            task_queue,
            ctx.access_manager,
            task_id=record.id,
            command=command,
            accounts_count=spec.accounts_count,
            extra_lines=[f"Cycles: <b>{spec.repeat_count}</b>"],
        )

    @router.message(Command("join"))
    async def join_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        join_presenter = task_service.presenter_for("join")
        if not payload:
            usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/join https://t.me/+abcd 5 1.7</code>"
            if join_presenter is not None:
                usage_text = join_presenter.usage_text()
            await message.answer(usage_text)
            return
        try:
            spec = task_service.parse_task_payload("join", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        command = join_presenter.command if join_presenter is not None else "/join"
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)

    @router.message(Command("leave"))
    async def leave_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        leave_presenter = task_service.presenter_for("leave")
        if not payload:
            usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/leave https://t.me/+abcd 5 1.7</code>"
            if leave_presenter is not None:
                usage_text = leave_presenter.usage_text()
            await message.answer(usage_text)
            return
        try:
            spec = task_service.parse_task_payload("leave", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(task_queue, spec, requested_by_user_id=message.from_user.id)
        command = leave_presenter.command if leave_presenter is not None else "/leave"
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)


def _register_task_callbacks(router: Router, ctx: HandlerContext) -> None:
    access_manager = ctx.access_manager
    task_queue = ctx.task_queue

    async def render_and_track(callback: CallbackQuery) -> None:
        if callback.message:
            requester_user_id = getattr(callback.from_user, "id", 0)
            include_all = await access_manager.can_view_all_tasks(requester_user_id)
            await render_task_manager(
                callback.message,
                task_queue,
                requester_user_id=requester_user_id,
                include_all=include_all,
            )
            key = (callback.message.chat.id, callback.message.message_id)
            existing = ctx.task_manager_live_views.pop(key, None)
            if existing is not None:
                existing.cancel()
            ctx.task_manager_live_state.add(key)
            ctx.task_manager_live_views[key] = asyncio.create_task(
                _task_manager_live_loop(ctx, callback.message, requester_user_id=requester_user_id, include_all=include_all),
                name=f"task-manager-live-{key[0]}-{key[1]}",
            )

    @router.callback_query(F.data == "menu:tasks")
    async def menu_tasks(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        await render_and_track(callback)

    @router.callback_query(F.data == "task:refresh")
    async def tasks_refresh(callback: CallbackQuery) -> None:
        await safe_callback_answer(callback)
        await render_and_track(callback)

    @router.callback_query(F.data == "task:clear_finished")
    async def tasks_clear_finished(callback: CallbackQuery) -> None:
        requester_user_id = getattr(callback.from_user, "id", 0)
        include_all = await access_manager.can_view_all_tasks(requester_user_id)
        removed = await task_queue.clear_finished(requested_by_user_id=requester_user_id, include_all=include_all)
        await safe_callback_answer(callback, text=f"Удалено из истории: {removed}")
        await render_and_track(callback)

    @router.callback_query(F.data.startswith("task:view:"))
    async def task_view(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=2)[2])
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        requester_user_id = getattr(callback.from_user, "id", 0)
        await render_task_detail(
            callback.message,
            task_queue,
            task_id,
            requester_user_id=requester_user_id,
            include_all=await access_manager.can_view_all_tasks(requester_user_id),
        )

    @router.callback_query(F.data.startswith("task:"))
    async def task_view_short(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        tail = callback.data.split(":", maxsplit=1)[1]
        if not tail.isdigit():
            return
        task_id = int(tail)
        await safe_callback_answer(callback)
        stop_task_manager_live_view(ctx, callback.message)
        requester_user_id = getattr(callback.from_user, "id", 0)
        await render_task_detail(
            callback.message,
            task_queue,
            task_id,
            requester_user_id=requester_user_id,
            include_all=await access_manager.can_view_all_tasks(requester_user_id),
        )

    @router.callback_query(F.data.startswith("task:pause:"))
    async def task_pause(callback: CallbackQuery) -> None:
        await _handle_task_action(callback, ctx, task_queue.pause_task)

    @router.callback_query(F.data.startswith("task_pause:"))
    async def task_pause_short(callback: CallbackQuery) -> None:
        await _handle_task_action(callback, ctx, task_queue.pause_task, prefix="task_pause:")

    @router.callback_query(F.data.startswith("task:resume:"))
    async def task_resume(callback: CallbackQuery) -> None:
        await _handle_task_action(callback, ctx, task_queue.resume_task)

    @router.callback_query(F.data.startswith("task:cancel:"))
    async def task_cancel(callback: CallbackQuery) -> None:
        await _handle_task_action(callback, ctx, task_queue.cancel_task)

    @router.callback_query(F.data.startswith("task_stop:"))
    async def task_stop_short(callback: CallbackQuery) -> None:
        await _handle_task_action(callback, ctx, task_queue.cancel_task, prefix="task_stop:")

    @router.callback_query(F.data.startswith("task_restart:"))
    async def task_restart_short(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data[len("task_restart:"):])
        requester_user_id = getattr(callback.from_user, "id", 0)
        include_all = await ctx.access_manager.can_view_all_tasks(requester_user_id)
        ok, text, new_task_id = await task_queue.restart_task(task_id, requested_by_user_id=requester_user_id, include_all=include_all)
        await safe_callback_answer(callback, text=text, show_alert=not ok)
        stop_task_manager_live_view(ctx, callback.message)
        await render_task_detail(
            callback.message,
            ctx.task_queue,
            new_task_id or task_id,
            requester_user_id=requester_user_id,
            include_all=include_all,
        )

    @router.callback_query(F.data.startswith("task:remove:"))
    async def task_remove(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        task_id = int(callback.data.split(":", maxsplit=2)[2])
        requester_user_id = getattr(callback.from_user, "id", 0)
        include_all = await access_manager.can_view_all_tasks(requester_user_id)
        ok, text = await task_queue.remove_task(task_id, requested_by_user_id=requester_user_id, include_all=include_all)
        await safe_callback_answer(callback, text=text, show_alert=not ok)
        if ok:
            await render_and_track(callback)
            return
        stop_task_manager_live_view(ctx, callback.message)
        await render_task_detail(
            callback.message,
            task_queue,
            task_id,
            requester_user_id=requester_user_id,
            include_all=include_all,
        )


async def _task_manager_live_loop(
    ctx: HandlerContext,
    message: Message,
    *,
    requester_user_id: int,
    include_all: bool,
) -> None:
    key = (message.chat.id, message.message_id)
    try:
        while key in ctx.task_manager_live_state:
            await asyncio.sleep(4)
            if key not in ctx.task_manager_live_state:
                break
            await render_task_manager(
                message,
                ctx.task_queue,
                requester_user_id=requester_user_id,
                include_all=include_all,
            )
    except asyncio.CancelledError:
        return
    finally:
        ctx.task_manager_live_views.pop(key, None)


def _register_task_help_callbacks(router: Router) -> None:
    for callback_data, text in TASK_HELP_TEXTS.items():
        @router.callback_query(F.data == callback_data)
        async def help_callback(callback: CallbackQuery, _text=text) -> None:
            await safe_callback_answer(callback)
            if callback.message:
                await callback.message.answer(_text)


def _register_task_commands(router: Router, ctx: HandlerContext) -> None:
    task_queue = ctx.task_queue
    task_service = ctx.task_service

    @router.message(Command("__legacy_disabled_msg"))
    async def msg_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text or message.caption)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/msg @user1 @user2 \"hello guys\" 10 5 1.1</code>\n<code>/msg @user1 @user2 10 5 1.1</code> + фото")
            return
        has_photo = bool(message.photo)
        try:
            targets, text, accounts_count, repeat_count, delay, hide_content = _parse_msg_payload(payload, allow_empty_text=has_photo)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        if not text and not has_photo:
            await message.answer("❌ Нужен текст или прикрепленное фото.")
            return
        photo_path = await _download_message_photo(message) if has_photo else ""
        record = await task_service.enqueue_task(
            task_queue,
            MsgTask(
                chat_id=message.chat.id,
                targets=targets,
                text=text,
                accounts_count=accounts_count,
                repeat_count=repeat_count,
                delay=delay,
                photo_path=photo_path,
                hide_content=hide_content,
            ),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/msg", accounts_count=accounts_count, extra_lines=[f"Повторов: <b>{repeat_count}</b>"])

    @router.message(Command("__legacy_disabled_msgbot"))
    async def msgbot_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        if True:
            if not payload:
                await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/msgbot @testbot /start 10 5 0.7</code>")
                return
            try:
                bot_username, command, accounts_count, repeat_count, delay = _parse_msgbot_payload(payload)
            except ValueError as exc:
                await message.answer(f"❌ {escape(str(exc))}")
                return
            record = await task_service.enqueue_task(
                task_queue,
                MsgBotTask(chat_id=message.chat.id, bot_username=bot_username, command=command, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay),
                requested_by_user_id=message.from_user.id,
            )
            await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/msgbot", accounts_count=accounts_count, extra_lines=[f"Повторов: <b>{repeat_count}</b>"])
            return
        payload = _extract_payload(message.text)
        leave_presenter = task_service.presenter_for("leave")
        if True:
            if not payload:
                usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/leave https://t.me/+abcd 5 1.7</code>"
                if leave_presenter is not None:
                    usage_text = leave_presenter.usage_text()
                await message.answer(usage_text)
                return
            try:
                spec = task_service.parse_task_payload("leave", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
            except ValueError as exc:
                await message.answer(f"❌ {escape(str(exc))}")
                return
            record = await task_service.enqueue_task(
                task_queue,
                spec,
                requested_by_user_id=message.from_user.id,
            )
            command = "/leave"
            if leave_presenter is not None:
                command = leave_presenter.command
            await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)
            return
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/msgbot @testbot /start 10 5 0.7</code>")
            return
        try:
            bot_username, command, accounts_count, repeat_count, delay = _parse_msgbot_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            MsgBotTask(chat_id=message.chat.id, bot_username=bot_username, command=command, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/msgbot", accounts_count=accounts_count, extra_lines=[f"Повторов: <b>{repeat_count}</b>"])

    @router.message(Command("__legacy_disabled_msgchat"))
    async def msgchat_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text or message.caption)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/msgchat https://t.me/publicchat \"hello guys\" 10 5 1.1</code>\n<code>/msgchat @publicchat 10 5 1.1</code> + фото")
            return
        has_photo = bool(message.photo)
        try:
            target, text, accounts_count, repeat_count, delay, hide_content = _parse_msgchat_payload(payload, allow_empty_text=has_photo)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        if not text and not has_photo:
            await message.answer("❌ Нужен текст или прикрепленное фото.")
            return
        photo_path = await _download_message_photo(message) if has_photo else ""
        record = await task_service.enqueue_task(
            task_queue,
            MsgChatTask(
                chat_id=message.chat.id,
                target=target,
                text=text,
                accounts_count=accounts_count,
                repeat_count=repeat_count,
                delay=delay,
                photo_path=photo_path,
                hide_content=hide_content,
            ),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/msgchat", accounts_count=accounts_count, extra_lines=[f"Повторов: <b>{repeat_count}</b>"])

    @router.message(Command("__legacy_disabled_join"))
    async def join_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        join_presenter = task_service.presenter_for("join")
        if True:
            if not payload:
                usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/join https://t.me/+abcd 5 1.7</code>"
                if join_presenter is not None:
                    usage_text = join_presenter.usage_text()
                await message.answer(usage_text)
                return
            try:
                spec = task_service.parse_task_payload("join", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
            except ValueError as exc:
                await message.answer(f"❌ {escape(str(exc))}")
                return
            record = await task_service.enqueue_task(
                task_queue,
                spec,
                requested_by_user_id=message.from_user.id,
            )
            command = "/join"
            if join_presenter is not None:
                command = join_presenter.command
            await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)
            return
        if not payload:
            usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/join https://t.me/+abcd 5 1.7</code>"
            if join_presenter is not None:
                usage_text = join_presenter.usage_text()
            await message.answer(usage_text)
            return
        try:
            spec = task_service.parse_task_payload("join", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            spec,
            requested_by_user_id=message.from_user.id,
        )
        command = "/join"
        if join_presenter is not None:
            command = join_presenter.command
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)
        return
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/join https://t.me/+abcd 5 1.7</code>")
            return
        try:
            link, count, delay_cap = _parse_join_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            JoinTask(chat_id=message.chat.id, link=link, count=count, delay_cap=delay_cap),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/join", accounts_count=count)

    @router.message(Command("__legacy_disabled_leave"))
    async def leave_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        leave_presenter = task_service.presenter_for("leave")
        if True:
            if not payload:
                usage_text = "❌ <b>Пример рабочей команды:</b>\n<code>/leave https://t.me/+abcd 5 1.7</code>"
                if leave_presenter is not None:
                    usage_text = leave_presenter.usage_text()
                await message.answer(usage_text)
                return
            try:
                spec = task_service.parse_task_payload("leave", payload, chat_id=message.chat.id, requested_by_user_id=message.from_user.id)
            except ValueError as exc:
                await message.answer(f"❌ {escape(str(exc))}")
                return
            record = await task_service.enqueue_task(
                task_queue,
                spec,
                requested_by_user_id=message.from_user.id,
            )
            command = "/leave"
            if leave_presenter is not None:
                command = leave_presenter.command
            await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command=command, accounts_count=spec.count)
            return
        payload = _extract_payload(message.text)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/leave https://t.me/+abcd 5 1.7</code>")
            return
        try:
            link, count, delay_cap = _parse_leave_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            LeaveTask(chat_id=message.chat.id, link=link, count=count, delay_cap=delay_cap),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/leave", accounts_count=count)

    @router.message(Command("likep"))
    async def likep_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/likep https://t.me/channel/123 10 2 ❤️</code>\n<code>/likep https://t.me/channel/123 10 2 👍❤️</code>")
            return
        try:
            link, count, delay, emojis = _parse_likep_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            LikeTask(chat_id=message.chat.id, link=link, count=count, delay=delay, emojis=emojis),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(
            message,
            task_queue,
            ctx.access_manager,
            task_id=record.id,
            command="/likep",
            accounts_count=count,
            extra_lines=[f"Задержка: <b>{escape(str(delay))}</b>"],
        )

    @router.message(Command("refp"))
    async def refp_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/refp https://t.me/testbot?start=ref123 5 1.2</code>")
            return
        try:
            link, count, delay = _parse_refp_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            RefTask(chat_id=message.chat.id, link=link, count=count, delay=delay),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/refp", accounts_count=count)

    @router.message(Command("vote"))
    async def vote_handler(message: Message) -> None:
        if not await ensure_active_message_access(message, ctx.access_manager):
            return
        payload = _extract_payload(message.text)
        if not payload:
            await message.answer("❌ <b>Пример рабочей команды:</b>\n<code>/vote https://t.me/publicchannel/123 2 10 1.2</code>")
            return
        try:
            link, option_index, count, delay = _parse_vote_payload(payload)
        except ValueError as exc:
            await message.answer(f"❌ {escape(str(exc))}")
            return
        record = await task_service.enqueue_task(
            task_queue,
            VoteTask(chat_id=message.chat.id, link=link, option_index=option_index, count=count, delay=delay),
            requested_by_user_id=message.from_user.id,
        )
        await reply_task_created(message, task_queue, ctx.access_manager, task_id=record.id, command="/vote", accounts_count=count)


def _register_pending_input(router: Router, ctx: HandlerContext) -> None:
    access_manager = ctx.access_manager
    account_manager = ctx.account_manager
    pending_actions = ctx.pending_actions
    session_store = ctx.session_store

    @router.message()
    async def pending_input_handler(message: Message) -> None:
        if not message.from_user:
            return
        user_id = message.from_user.id
        raw_text = (message.text or "").strip()
        if raw_text.startswith("/start"):
            return
        access_state = await access_manager.access_state(user_id)
        if access_state == "missing":
            if raw_text or message.document:
                await message.answer(access_manager.not_registered_message())
            return
        if access_state == "blocked":
            user = await access_manager.get_user(user_id)
            await message.answer(access_manager.blocked_message(user))
            return
        if access_state != "active":
            if not raw_text or raw_text.startswith("/") or not _looks_like_access_key(raw_text) or not await access_manager.has_inactive_key(user_id):
                await message.answer(await access_manager.pending_key_message(user_id))
                return
            try:
                user = await access_manager.activate_key(user_id, raw_text)
                support_username = access_manager.support_username
                welcome_keyboard = activation_policies_keyboard(
                    usage_policy_url=USAGE_POLICY_URL,
                    privacy_policy_url=PRIVACY_POLICY_URL,
                    terms_of_service_url=TERMS_OF_SERVICE_URL,
                )
                welcome_text = (
                    "🎉 <b>Авторизация выполнена успешно!</b>\n\n"
                    "Ваш ключ подтверждён и привязан к аккаунту. Доступ к системе активирован.\n\n"
                    "📌 Перед началом использования ознакомьтесь с важной информацией:\n\n"
                    "<blockquote>✅ Это сообщение показывается один раз после первого входа.</blockquote>\n\n"
                    f"💬 Поддержка: @{escape(support_username)}"
                )
                await message.answer(welcome_text, reply_markup=welcome_keyboard)
                await render_main_panel(message, access_manager, user_id=user_id)
            except Exception:
                return
            return

        pending = pending_actions.get(user_id)
        if not pending:
            return
        try:
            await _handle_pending_action(message, ctx, pending, session_store)
        except Exception as exc:
            await message.answer(f"❌ Ошибка: {escape(str(exc))}")
        finally:
            pending_actions.pop(user_id, None)


async def _handle_task_action(callback: CallbackQuery, ctx: HandlerContext, action, *, prefix: str | None = None) -> None:
    if not callback.data or not callback.message:
        await safe_callback_answer(callback)
        return
    if prefix is not None:
        task_id = int(callback.data[len(prefix):])
    else:
        task_id = int(callback.data.split(":", maxsplit=2)[2])
    requester_user_id = getattr(callback.from_user, "id", 0)
    include_all = await ctx.access_manager.can_view_all_tasks(requester_user_id)
    ok, text = await action(task_id, requested_by_user_id=requester_user_id, include_all=include_all)
    if (
        not ok
        and "не найдена" in str(text).lower()
        and callback.message is not None
    ):
        resolved_task_id = await ctx.task_queue.find_task_id_by_status_message(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            requested_by_user_id=requester_user_id,
            include_all=include_all,
        )
        if resolved_task_id is not None and resolved_task_id != task_id:
            task_id = resolved_task_id
            ok, text = await action(task_id, requested_by_user_id=requester_user_id, include_all=include_all)
    await safe_callback_answer(callback, text=text, show_alert=not ok)
    stop_task_manager_live_view(ctx, callback.message)
    await render_task_detail(callback.message, ctx.task_queue, task_id, requester_user_id=requester_user_id, include_all=include_all)


async def _download_message_photo(message: Message) -> str:
    if not message.photo:
        return ""
    MESSAGE_MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    largest = message.photo[-1]
    file = await message.bot.get_file(largest.file_id)
    suffix = Path(file.file_path or "message.jpg").suffix or ".jpg"
    destination = MESSAGE_MEDIA_DIR / f"msg_{message.chat.id}_{message.message_id}_{uuid.uuid4().hex}{suffix}"
    await message.bot.download(largest, destination=destination)
    return str(destination)


async def _handle_pending_action(message: Message, ctx: HandlerContext, pending, session_store) -> None:
    access_manager = ctx.access_manager
    account_manager = ctx.account_manager
    if pending.action == "import_session":
        import_limit = 50
        owner_id = await access_manager.account_owner_for_new_account(message.from_user.id)
        if owner_id is None:
            raise PermissionError("У вас нет прав на добавление аккаунтов.")
        if not message.document:
            await message.answer("❌ Пришлите именно файл <code>.session</code> как документ.")
            return
        file_name = message.document.file_name or ""
        if not file_name.endswith(".session"):
            raise ValueError("Нужен файл с расширением .session")
        session_name = Path(file_name).stem
        if account_manager.accounts_total() >= import_limit:
            raise ValueError(f"Достигнут лимит импорта: не больше {import_limit} аккаунтов.")
        if account_manager.has_account_session(session_name):
            raise ValueError("Такая session уже зарегистрирована в аккаунтах.")
        target_path = account_manager.sessions_dir / file_name
        if target_path.exists():
            raise ValueError("Такой session-файл уже существует.")
        try:
            await message.bot.download(message.document, destination=target_path)
            if session_store and session_store.enabled:
                await asyncio.to_thread(session_store.save_session_file, session_name, target_path)
            await account_manager.ensure_account_entry(session_name, owner_id=owner_id)
        except Exception:
            try:
                target_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        await message.answer(
            "✅ Session "
            f"<code>{escape(file_name)}</code> импортирован "
            "and added to the account list."
        )
    elif pending.action == "export_session":
        if not pending.mode:
            raise ValueError("Не выбран режим экспорта.")
        scope, export_format = pending.mode.split(":", maxsplit=1)
        sessions = await _resolve_export_sessions(
            message=message,
            account_manager=account_manager,
            access_manager=access_manager,
            scope=scope,
            raw_value=(message.text or "").strip(),
        )
        await _export_sessions_for_user(
            message,
            requester_user_id=message.from_user.id,
            account_manager=account_manager,
            access_manager=access_manager,
            session_store=session_store,
            sessions=sessions,
            export_format=export_format,
        )
    elif pending.action == "edit_first_name":
        await account_manager.update_first_name(pending.session or "", (message.text or "").strip())
        await message.answer("✅ First name updated.")
    elif pending.action == "edit_last_name":
        await account_manager.update_last_name(pending.session or "", (message.text or "").strip())
        await message.answer("✅ Фамилия обновлена.")
    elif pending.action == "edit_bio":
        await account_manager.update_bio(pending.session or "", (message.text or "").strip())
        await message.answer("✅ Описание обновлено.")
    elif pending.action == "edit_username":
        await account_manager.update_username(pending.session or "", (message.text or "").strip())
        await message.answer("✅ Юзернейм обновлён.")
    elif pending.action == "edit_birthday":
        day, month, year = _parse_birthday((message.text or "").strip())
        await account_manager.update_birthday(pending.session or "", day=day, month=month, year=year)
        await message.answer("✅ Дата рождения обновлена.")
    elif pending.action == "edit_avatar":
        if not message.photo:
            await message.answer("❌ Пришлите именно фотографию, не файл.")
            return
        largest = message.photo[-1]
        file = await message.bot.get_file(largest.file_id)
        suffix = Path(file.file_path or "avatarka.jpg").suffix or ".jpg"
        temp_path = Path(tempfile.gettempdir()) / f"tg_avatar_{message.from_user.id}_{pending.session}{suffix}"
        await message.bot.download(largest, destination=temp_path)
        try:
            await account_manager.update_avatar(pending.session or "", temp_path)
        finally:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
        await message.answer("✅ Аватар обновлён.")

    requester_user_id = message.from_user.id
    accounts = await account_manager.list_accounts_status(
        force_refresh=True,
        owner_ids=await access_manager.visible_account_owner_ids(requester_user_id),
    )
    await message.answer(
        _format_session_manager(accounts, account_manager, page=1, page_size=10),
        reply_markup=session_manager_keyboard(
            accounts,
            can_add_accounts=await access_manager.can_add_accounts(requester_user_id),
            page=1,
            page_size=10,
        ),
    )

