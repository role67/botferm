from __future__ import annotations

import asyncio
import re
import tempfile
import time
import zipfile
from html import escape
from pathlib import Path

from aiogram import F, Router
from aiogram.types import CallbackQuery, FSInputFile

from bot.command_parsing import (
    parse_positive_int as _parse_positive_int,
    parse_session_page_callback as _parse_session_page_callback,
)
from bot.handlers import (
    HandlerContext,
    PendingAction,
    ensure_account_access,
    ensure_account_creation_access,
    ensure_accounts_menu_access,
    ensure_owner_level_settings,
    safe_callback_answer,
)
from bot.handler_renderers import (
    format_account_view as _format_account_view,
    render_dc_overview,
    render_health_settings,
    render_healthcheck_with_progress,
    render_session_manager_with_progress,
    rerender_account_view_or_session_manager,
    set_pending_edit,
)
from bot.keyboards import (
    account_actions_keyboard,
    account_add_mode_keyboard,
    account_edit_keyboard,
    help_back_keyboard,
    selective_account_delete_keyboard,
    session_export_format_keyboard,
    session_export_scope_keyboard,
)
from core.access_manager import AccessManager
from core.accounts import AccountManager
from core.session_store import PostgresSessionStore


async def resolve_export_sessions(
    *,
    message,
    account_manager: AccountManager,
    access_manager: AccessManager,
    scope: str,
    raw_value: str,
) -> list[str]:
    owner_ids = await access_manager.visible_account_owner_ids(getattr(getattr(message, "from_user", None), "id", 0))
    accounts = await account_manager.list_accounts_status(force_refresh=False, owner_ids=owner_ids)
    if not accounts:
        raise ValueError("Нет доступных сессий для экспорта.")

    if not raw_value:
        raise ValueError("Укажите номер или имя сессии.")

    by_session = {str(item["session"]): str(item["session"]) for item in accounts}
    by_index = {str(item["index"]): str(item["session"]) for item in accounts}
    tokens = [token.strip() for token in re.split(r"[\s,;]+", raw_value) if token.strip()]
    if not tokens:
        raise ValueError("Не удалось разобрать список сессий.")

    resolved: list[str] = []
    for token in tokens:
        session = by_session.get(token) or by_index.get(token)
        if session is None:
            raise ValueError(f"Сессия не найдена: {token}")
        if session not in resolved:
            resolved.append(session)

    if scope == "single" and len(resolved) != 1:
        raise ValueError("Нужна ровно одна сессия.")

    return resolved


async def export_sessions_for_user(
    message,
    *,
    requester_user_id: int,
    account_manager: AccountManager,
    access_manager: AccessManager,
    session_store: PostgresSessionStore | None,
    sessions: list[str] | None,
    export_format: str,
) -> None:
    owner_ids = await access_manager.visible_account_owner_ids(requester_user_id)
    accounts = await account_manager.list_accounts_status(force_refresh=False, owner_ids=owner_ids)
    visible_sessions = [str(item["session"]) for item in accounts]
    if sessions is None:
        sessions = visible_sessions
    else:
        hidden = [session for session in sessions if session not in visible_sessions]
        if hidden:
            raise ValueError("Часть сессий недоступна для экспорта.")

    if not sessions:
        raise ValueError("Нет сессий для экспорта.")

    if export_format == "zip":
        await message.answer(f"🗜 Готовлю ZIP: <b>{len(sessions)}</b> шт.")
        await send_sessions_zip(
            message,
            account_manager=account_manager,
            session_store=session_store,
            sessions=sessions,
        )
        return

    await message.answer(f"📤 Отправляю .session: <b>{len(sessions)}</b> шт.")
    for session in sessions:
        path = await ensure_session_file_materialized(
            account_manager=account_manager,
            session_store=session_store,
            session=session,
        )
        await message.answer_document(
            document=FSInputFile(path=path),
            caption=f"📄 <code>{escape(path.name)}</code>",
        )


async def send_sessions_zip(
    message,
    *,
    account_manager: AccountManager,
    session_store: PostgresSessionStore | None,
    sessions: list[str],
) -> None:
    archive_path = Path(tempfile.gettempdir()) / f"sessions_export_{int(time.time() * 1000)}.zip"
    try:
        with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for session in sessions:
                path = await ensure_session_file_materialized(
                    account_manager=account_manager,
                    session_store=session_store,
                    session=session,
                )
                archive.write(path, arcname=path.name)

        await message.answer_document(
            document=FSInputFile(path=archive_path),
            caption=f"🗜 Сессий: <b>{len(sessions)}</b>",
        )
    finally:
        try:
            archive_path.unlink(missing_ok=True)
        except Exception:
            pass


async def ensure_session_file_materialized(
    *,
    account_manager: AccountManager,
    session_store: PostgresSessionStore | None,
    session: str,
) -> Path:
    target_path = account_manager.sessions_dir / f"{session}.session"
    if not target_path.exists() and session_store and session_store.enabled:
        payload = await asyncio.to_thread(session_store.load_session_bytes, session)
        if payload:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_bytes(payload)
    return account_manager.session_file_path(session)


def register_account_handlers(router: Router, ctx: HandlerContext) -> None:
    account_manager = ctx.account_manager
    access_manager = ctx.access_manager
    pending_actions = ctx.pending_actions
    session_store = ctx.session_store
    selective_delete_selection: dict[int, set[str]] = {}

    async def _visible_accounts(callback: CallbackQuery) -> list[dict]:
        return await account_manager.list_accounts_status(
            force_refresh=False,
            owner_ids=await access_manager.visible_account_owner_ids(getattr(callback.from_user, "id", 0)),
        )

    async def _render_selective_delete(callback: CallbackQuery, *, page: int) -> None:
        if not callback.message:
            return
        accounts = await _visible_accounts(callback)
        if not accounts:
            await callback.message.edit_text(
                "<b>Выборочное удаление</b>\n\n"
                "Сейчас нет доступных аккаунтов для удаления.",
                reply_markup=help_back_keyboard("acc:session_manager"),
            )
            return
        user_id = getattr(callback.from_user, "id", 0)
        selected = selective_delete_selection.setdefault(user_id, set())
        available_sessions = {str(item.get("session") or "") for item in accounts}
        selected.intersection_update(available_sessions)
        await callback.message.edit_text(
            "<b>🧹 Выборочное удаление аккаунтов</b>\n\n"
            "Отметьте нужные аккаунты кнопками ниже.\n"
            "☑️ — выбран для удаления\n"
            "⬜ — не выбран\n\n"
            f"Сейчас выбрано: <b>{len(selected)}</b>\n"
            "После подтверждения удалятся записи из accounts.json, локальные .session и данные из Postgres (если включён).",
            reply_markup=selective_account_delete_keyboard(
                accounts,
                selected_sessions=selected,
                page=page,
                page_size=10,
            ),
        )

    @router.callback_query(F.data == "acc:session_manager")
    async def session_manager_menu(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback, text="Открываю менеджер сессий...")
        if callback.message:
            await render_session_manager_with_progress(
                message=callback.message,
                account_manager=account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=False,
                page=1,
            )

    @router.callback_query(F.data.startswith("acc:session_manager:page:"))
    async def session_manager_page(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        await safe_callback_answer(callback)
        await render_session_manager_with_progress(
            message=callback.message,
            account_manager=account_manager,
            access_manager=access_manager,
            requester_user_id=getattr(callback.from_user, "id", 0),
            force_refresh=False,
            page=page,
        )

    @router.callback_query(F.data.startswith("acc:session_manager:refresh"))
    async def session_manager_refresh(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        page = 1
        if callback.data and callback.data.count(":") >= 3:
            page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        await safe_callback_answer(callback, text="Обновляю менеджер сессий...")
        if callback.message:
            await render_session_manager_with_progress(
                message=callback.message,
                account_manager=account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=True,
                page=page,
            )

    @router.callback_query(F.data == "acc:health")
    async def accounts_health(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback, text="Запускаю проверку здоровья...")
        if callback.message:
            await render_healthcheck_with_progress(
                message=callback.message,
                account_manager=account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=True,
                page=1,
            )

    @router.callback_query(F.data.startswith("acc:health:refresh"))
    async def accounts_health_refresh(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        page = 1
        if callback.data and callback.data.count(":") >= 3:
            page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        await safe_callback_answer(callback, text="Обновляю проверку здоровья...")
        if callback.message:
            await render_healthcheck_with_progress(
                message=callback.message,
                account_manager=account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=True,
                page=page,
            )

    @router.callback_query(F.data.startswith("acc:health:page:"))
    async def accounts_health_page(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        await safe_callback_answer(callback)
        await render_healthcheck_with_progress(
            message=callback.message,
            account_manager=account_manager,
            access_manager=access_manager,
            requester_user_id=getattr(callback.from_user, "id", 0),
            force_refresh=False,
            page=page,
        )

    @router.callback_query(F.data.startswith("acc:health:settings:"))
    async def accounts_health_settings(callback: CallbackQuery) -> None:
        if not await ensure_owner_level_settings(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        await safe_callback_answer(callback)
        await render_health_settings(
            callback.message,
            account_manager=account_manager,
            page=page,
        )

    @router.callback_query(F.data.startswith("acc:health:toggle_auto_remove"))
    async def accounts_health_toggle_auto_remove(callback: CallbackQuery) -> None:
        if not await ensure_owner_level_settings(callback, access_manager):
            return
        page = 1
        if callback.data and callback.data.count(":") >= 4:
            page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        enabled = await account_manager.toggle_auto_remove_from_pool()
        await safe_callback_answer(callback, text=f"Автоудаление {'включено' if enabled else 'выключено'}")
        if callback.message:
            await render_health_settings(
                callback.message,
                account_manager=account_manager,
                page=page,
            )

    @router.callback_query(F.data.startswith("acc:health:toggle_notifications"))
    async def accounts_health_toggle_notifications(callback: CallbackQuery) -> None:
        if not await ensure_owner_level_settings(callback, access_manager):
            return
        page = 1
        if callback.data and callback.data.count(":") >= 4:
            page = _parse_positive_int(callback.data.rsplit(":", maxsplit=1)[1], default=1)
        enabled = await account_manager.toggle_health_notifications()
        await safe_callback_answer(callback, text=f"Уведомления {'включены' if enabled else 'выключены'}")
        if callback.message:
            await render_health_settings(
                callback.message,
                account_manager=account_manager,
                page=page,
            )

    @router.callback_query(F.data == "acc:relogin")
    async def accounts_relogin(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback, text="Перезагружаю сессии...")
        if not callback.message:
            return
        await account_manager.load_clients()
        owner_ids = await access_manager.visible_account_owner_ids(getattr(callback.from_user, "id", 0))
        rows = await account_manager.list_accounts_status(force_refresh=True, owner_ids=owner_ids)
        summary = account_manager.build_health_summary(rows)
        await callback.message.edit_text(
            "<b>Автоперелогин</b>\n"
            "Сессии перезагружены и заново прочитаны из текущих файлов сессий.\n\n"
            f"Всего: <b>{summary['total']}</b>\n"
            f"Живы: <b>{summary['alive']}</b>\n"
            f"Ограничены: <b>{summary['limited']}</b>\n"
            f"Забанены: <b>{summary['banned']}</b>\n"
            f"В пуле: <b>{summary['in_pool']}</b>",
            reply_markup=help_back_keyboard("acc:session_manager"),
        )

    @router.callback_query(F.data == "acc:dc")
    async def accounts_dc(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback, text="Собираю данные по DC...")
        if callback.message:
            await render_dc_overview(
                callback.message,
                account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=False,
            )

    @router.callback_query(F.data == "acc:dc:refresh")
    async def accounts_dc_refresh(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback, text="Обновляю данные по DC...")
        if callback.message:
            await render_dc_overview(
                callback.message,
                account_manager,
                access_manager=access_manager,
                requester_user_id=getattr(callback.from_user, "id", 0),
                force_refresh=True,
            )

    @router.callback_query(F.data.startswith("acc:view:"))
    async def account_view(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        await safe_callback_answer(callback, text="Загружаю аккаунт...")
        session, page = _parse_session_page_callback(callback.data, prefix="acc:view:")
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        accounts = await account_manager.list_accounts_status(
            owner_ids=await access_manager.visible_account_owner_ids(getattr(callback.from_user, "id", 0))
        )
        target = next((item for item in accounts if item["session"] == session), None)
        if not target:
            await safe_callback_answer(callback, text="Аккаунт не найден", show_alert=True)
            return
        await callback.message.edit_text(
            _format_account_view(target),
            reply_markup=account_actions_keyboard(
                session,
                in_pool=bool(target.get("in_pool", True)),
                account_state=str(target.get("account_state", "")),
                page=page,
            ),
        )

    @router.callback_query(F.data.startswith("acc:pool:"))
    async def account_toggle_pool(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        session, page = _parse_session_page_callback(callback.data, prefix="acc:pool:")
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        in_pool = await account_manager.toggle_pool(session)
        await safe_callback_answer(callback, text="Возвращён в пул" if in_pool else "Убран из пула")
        await rerender_account_view_or_session_manager(
            callback.message,
            account_manager=account_manager,
            access_manager=access_manager,
            requester_user_id=getattr(callback.from_user, "id", 0),
            session=session,
            page=page,
            format_account_view=_format_account_view,
            account_actions_keyboard=account_actions_keyboard,
        )

    @router.callback_query(F.data.startswith("acc:read_all:"))
    async def account_read_all(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        session, page = _parse_session_page_callback(callback.data, prefix="acc:read_all:")
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        await safe_callback_answer(callback, text="Читаю сообщения...")
        try:
            summary = await account_manager.mark_session_dialogs_read(session)
        except Exception as exc:
            await safe_callback_answer(callback, text=str(exc), show_alert=True)
            return
        await rerender_account_view_or_session_manager(
            callback.message,
            account_manager=account_manager,
            access_manager=access_manager,
            requester_user_id=getattr(callback.from_user, "id", 0),
            session=session,
            page=page,
            format_account_view=_format_account_view,
            account_actions_keyboard=account_actions_keyboard,
        )
        await safe_callback_answer(
            callback,
            text=f"Чатов: {summary['dialogs_marked']} | Сообщений: {summary['unread_messages']}",
            show_alert=True,
        )

    @router.callback_query(F.data.startswith("acc:export:"))
    async def account_export(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        session = callback.data.split(":", maxsplit=2)[2]
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        if session_store and session_store.enabled:
            target_path = account_manager.sessions_dir / f"{session}.session"
            if not target_path.exists():
                payload = await asyncio.to_thread(session_store.load_session_bytes, session)
                if payload:
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    target_path.write_bytes(payload)
        path = account_manager.session_file_path(session)
        await safe_callback_answer(callback, text="Отправляю сессию...")
        await callback.message.answer_document(
            document=FSInputFile(path=path),
            caption=f"Экспорт сессии <code>{escape(path.name)}</code>",
        )

    @router.callback_query(F.data.startswith("acc:edit:"))
    async def account_edit_menu(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        await safe_callback_answer(callback)
        session, page = _parse_session_page_callback(callback.data, prefix="acc:edit:")
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        await callback.message.edit_text(
            "<b>Редактирование аккаунта</b>\n"
            f"Сессия: <code>{escape(session)}</code>\n"
            "Выберите, что изменить:",
            reply_markup=account_edit_keyboard(session, page),
        )

    @router.callback_query(F.data.startswith("acc:delete:"))
    async def account_delete(callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        await safe_callback_answer(callback, text="Удаляю...")
        session = callback.data.split(":", maxsplit=2)[2]
        if not await ensure_account_access(callback, access_manager, account_manager, session):
            return
        try:
            summary = await account_manager.delete_account(session, session_store=session_store)
            text = (
                f"<b>Готово</b>\n"
                f"Сессия: <code>{escape(session)}</code>\n"
                f"Аккаунтов удалено: <b>{summary['accounts_deleted']}</b>\n"
                f"Записей в accounts.json удалено: <b>{summary['config_entries_deleted']}</b>\n"
                f"Локальных файлов .session удалено: <b>{summary['session_files_deleted']}</b>\n"
                f"Записей в хранилище сессий удалено: <b>{summary['store_records_deleted']}</b>"
            )
        except Exception as exc:
            text = f"<b>Ошибка</b>: {escape(str(exc))}"
        await callback.message.edit_text(text, reply_markup=help_back_keyboard("acc:session_manager"))

    @router.callback_query(F.data.startswith("acc:select_delete:"))
    async def accounts_select_delete(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return

        parts = callback.data.split(":")
        if len(parts) == 3:
            page = _parse_positive_int(parts[2], default=1)
            await safe_callback_answer(callback, text="Открываю выборочное удаление...")
            selective_delete_selection.pop(getattr(callback.from_user, "id", 0), None)
            await _render_selective_delete(callback, page=page)
            return

        action = parts[2] if len(parts) >= 3 else ""
        page = _parse_positive_int(parts[3], default=1) if len(parts) >= 4 else 1
        user_id = getattr(callback.from_user, "id", 0)
        selected = selective_delete_selection.setdefault(user_id, set())

        if action in {"page", "cancel"}:
            await safe_callback_answer(callback)
            if action == "cancel":
                selective_delete_selection.pop(user_id, None)
                await render_session_manager_with_progress(
                    message=callback.message,
                    account_manager=account_manager,
                    access_manager=access_manager,
                    requester_user_id=user_id,
                    force_refresh=False,
                    page=page,
                )
                return
            await _render_selective_delete(callback, page=page)
            return

        if action == "toggle":
            if len(parts) < 5:
                await safe_callback_answer(callback, text="Не удалось определить аккаунт.", show_alert=True)
                return
            session = parts[4]
            accounts = await _visible_accounts(callback)
            available_sessions = {str(item.get("session") or "") for item in accounts}
            if session not in available_sessions:
                await safe_callback_answer(callback, text="Аккаунт недоступен.", show_alert=True)
                return
            if session in selected:
                selected.remove(session)
                await safe_callback_answer(callback, text="Убрано из выбора")
            else:
                selected.add(session)
                await safe_callback_answer(callback, text="Добавлено в выбор")
            await _render_selective_delete(callback, page=page)
            return

        if action == "confirm":
            accounts = await _visible_accounts(callback)
            available_sessions = {str(item.get("session") or "") for item in accounts}
            targets = [session for session in selected if session in available_sessions]
            if not targets:
                await safe_callback_answer(callback, text="Выберите хотя бы один аккаунт.", show_alert=True)
                await _render_selective_delete(callback, page=page)
                return
            await safe_callback_answer(callback, text="Удаляю выбранные аккаунты...")
            try:
                summary = await account_manager.delete_accounts(
                    targets,
                    session_store=session_store,
                )
            except Exception as exc:
                await callback.message.edit_text(
                    f"<b>Ошибка</b>: {escape(str(exc))}",
                    reply_markup=help_back_keyboard(f"acc:select_delete:{page}"),
                )
                return
            selective_delete_selection.pop(user_id, None)
            await callback.message.edit_text(
                "<b>🧹 Выборочное удаление завершено</b>\n"
                f"Выбрано к удалению: <b>{len(targets)}</b>\n"
                f"Аккаунтов удалено: <b>{summary['accounts_deleted']}</b>\n"
                f"Записей в accounts.json удалено: <b>{summary['config_entries_deleted']}</b>\n"
                f"Локальных файлов .session удалено: <b>{summary['session_files_deleted']}</b>\n"
                f"Записей в хранилище сессий удалено: <b>{summary['store_records_deleted']}</b>",
                reply_markup=help_back_keyboard("acc:session_manager"),
            )
            return

        await safe_callback_answer(callback, text="Некорректная команда удаления.", show_alert=True)

    @router.callback_query(F.data == "acc:import")
    async def account_import(callback: CallbackQuery) -> None:
        if not await ensure_account_creation_access(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            await callback.message.edit_text(
                "<b>Импорт сессий</b>\n"
                "Выберите режим добавления аккаунта.",
                reply_markup=account_add_mode_keyboard("import"),
            )

    @router.callback_query(F.data == "acc:export_menu")
    async def account_export_menu(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        await safe_callback_answer(callback)
        if callback.message:
            await callback.message.edit_text(
                "<b>Экспорт сессий</b>\n"
                "Что вы хотите экспортировать?",
                reply_markup=session_export_scope_keyboard(),
            )

    @router.callback_query(F.data.startswith("acc:export_scope:"))
    async def account_export_scope(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        scope = callback.data.rsplit(":", maxsplit=1)[1]
        scope_label = {"single": "one account", "multi": "multiple accounts", "all": "all accounts"}.get(
            scope,
            "выбранные аккаунты",
        )
        await safe_callback_answer(callback)
        await callback.message.edit_text(
            "<b>Экспорт сессий</b>\n"
            f"Выбрано: <b>{scope_label}</b>\n"
            "Как отправить?",
            reply_markup=session_export_format_keyboard(scope),
        )

    @router.callback_query(F.data.startswith("acc:export_format:"))
    async def account_export_format(callback: CallbackQuery) -> None:
        if not await ensure_accounts_menu_access(callback, access_manager):
            return
        if not callback.data or not callback.message:
            await safe_callback_answer(callback)
            return
        _, _, scope, export_format = callback.data.split(":", maxsplit=3)
        requester_user_id = getattr(callback.from_user, "id", 0)
        if scope == "all":
            await safe_callback_answer(callback, text="Подготавливаю экспорт...")
            await export_sessions_for_user(
                callback.message,
                requester_user_id=requester_user_id,
                account_manager=account_manager,
                access_manager=access_manager,
                session_store=session_store,
                sessions=None,
                export_format=export_format,
            )
            return
        if callback.from_user:
            pending_actions[callback.from_user.id] = PendingAction(action="export_session", mode=f"{scope}:{export_format}")
        prompt = (
            "Отправьте номер сессии из списка или её имя."
            if scope == "single"
            else "Отправьте номера или имена сессий через пробел, запятую или с новой строки."
        )
        await safe_callback_answer(callback, text="Ожидаю выбор сессии...")
        await callback.message.answer(
            "<b>Экспорт сессий</b>\n"
            f"Format: <b>{'ZIP' if export_format == 'zip' else '.session'}</b>\n"
            f"{prompt}"
        )

    @router.callback_query(F.data == "acc:add")
    async def account_add(callback: CallbackQuery) -> None:
        await safe_callback_answer(
            callback,
            text="Отдельное добавление аккаунта отключено. Используйте импорт .session.",
            show_alert=True,
        )

    @router.callback_query(F.data.startswith("acc:import:mode:"))
    async def account_import_mode(callback: CallbackQuery) -> None:
        if not await ensure_account_creation_access(callback, access_manager):
            return
        if not callback.data:
            await safe_callback_answer(callback)
            return
        mode = callback.data.rsplit(":", maxsplit=1)[1]
        if callback.from_user:
            pending_actions[callback.from_user.id] = PendingAction(action="import_session", mode=mode)
        await safe_callback_answer(callback, text="Ожидаю файл .session...")
        if callback.message:
            await callback.message.answer(
                "<b>Импорт сессий</b>\n"
                "Отправьте файл <code>.session</code> как документ."
            )

    @router.callback_query(F.data.startswith("acc:add:mode:"))
    async def account_add_mode(callback: CallbackQuery) -> None:
        await safe_callback_answer(
            callback,
            text="Ручное добавление отключено. Используйте импорт .session.",
            show_alert=True,
        )

    @router.callback_query(F.data.startswith("acc:edit_first_name:"))
    async def account_edit_first_name(callback: CallbackQuery) -> None:
        await set_pending_edit(callback, pending_actions, "edit_first_name", "Отправьте новое имя.", safe_callback_answer)

    @router.callback_query(F.data.startswith("acc:edit_last_name:"))
    async def account_edit_last_name(callback: CallbackQuery) -> None:
        await set_pending_edit(
            callback,
            pending_actions,
            "edit_last_name",
            "Отправьте новую фамилию. Отправьте <code>-</code>, чтобы очистить.",
            safe_callback_answer,
        )

    @router.callback_query(F.data.startswith("acc:edit_bio:"))
    async def account_edit_bio(callback: CallbackQuery) -> None:
        await set_pending_edit(
            callback,
            pending_actions,
            "edit_bio",
            "Отправьте новую биографию. Отправьте <code>-</code>, чтобы очистить.",
            safe_callback_answer,
        )

    @router.callback_query(F.data.startswith("acc:edit_username:"))
    async def account_edit_username(callback: CallbackQuery) -> None:
        await set_pending_edit(
            callback,
            pending_actions,
            "edit_username",
            "Отправьте новое имя пользователя, с @ или без. Отправьте <code>-</code>, чтобы очистить.",
            safe_callback_answer,
        )

    @router.callback_query(F.data.startswith("acc:edit_avatar:"))
    async def account_edit_avatar(callback: CallbackQuery) -> None:
        await set_pending_edit(
            callback,
            pending_actions,
            "edit_avatar",
            "Отправьте изображение как фото. Новый аватар будет добавлен, старые не удаляются.",
            safe_callback_answer,
        )

    @router.callback_query(F.data.startswith("acc:edit_birthday:"))
    async def account_edit_birthday(callback: CallbackQuery) -> None:
        await set_pending_edit(
            callback,
            pending_actions,
            "edit_birthday",
            "Отправьте дату рождения в формате <code>ДД.ММ.ГГГГ</code>.",
            safe_callback_answer,
        )

