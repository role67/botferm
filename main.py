from __future__ import annotations

import asyncio
import logging

from admin import AdminApiServer
from task_application import task_application_service
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

from bot.handlers import build_router
from config import (
    ACCOUNTS_FILE,
    ACCESS_USERS_FILE,
    ADMIN_API_ENABLED,
    ADMIN_API_HOST,
    ADMIN_API_PORT,
    ADMIN_API_TOKEN,
    BOT_TOKEN,
    DATABASE_URL,
    DEFAULT_API_HASH,
    DEFAULT_API_ID,
    HEALTH_NOTIFY_CHAT_ID,
    LOGS_DIR,
    MAX_COUNT,
    MAX_RETRIES,
    MESSAGE_MEDIA_DIR,
    MIN_DELAY_SECONDS,
    OWNER_USER_ID,
    SESSIONS_DIR,
    SUPPORT_USERNAME,
)
from core.access_manager import AccessManager
from core.accounts import AccountManager
from core.observability import audit_event, configure_logging
from core.queue import TaskQueue, Worker
from core.session_store import PostgresSessionStore
from core.sender import Sender
from telegram_gateway import TelegramTaskGateway


def setup_logging() -> None:
    configure_logging(LOGS_DIR)
    logging.getLogger("telethon").setLevel(logging.ERROR)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)
    logging.getLogger("aiogram.dispatcher").setLevel(logging.WARNING)


async def run() -> None:
    setup_logging()
    audit_event("system.starting", message="Запуск приложения начат")

    ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    ACCESS_USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    MESSAGE_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    access_manager = AccessManager(
        access_file=ACCESS_USERS_FILE,
        owner_user_id=OWNER_USER_ID,
        support_username=SUPPORT_USERNAME,
    )
    await access_manager.load()

    session_store = PostgresSessionStore(DATABASE_URL)
    if session_store.enabled:
        session_store.initialize()
        synced = session_store.sync_directory_to_db(SESSIONS_DIR)
        restored = session_store.hydrate_to_directory(SESSIONS_DIR, overwrite=False)
        logging.getLogger(__name__).info(
            "Postgres-хранилище сессий включено. Синхронизировано=%s восстановлено=%s",
            synced,
            restored,
        )

    account_manager = AccountManager(
        sessions_dir=SESSIONS_DIR,
        accounts_file=ACCOUNTS_FILE,
        shared_owner_id=OWNER_USER_ID,
        default_api_id=DEFAULT_API_ID,
        default_api_hash=DEFAULT_API_HASH or None,
    )
    await account_manager.load_clients()
    if account_manager.size == 0:
        logging.getLogger(__name__).warning("Сессии Telethon пока не загружены. Бот запустится в ограниченном режиме.")

    sender = Sender(
        account_manager=account_manager,
        access_manager=access_manager,
        min_delay_seconds=MIN_DELAY_SECONDS,
        max_count=MAX_COUNT,
        max_retries=MAX_RETRIES,
    )
    task_gateway = TelegramTaskGateway(sender)
    task_queue = TaskQueue()

    async def notifier(chat_id: int, text: str, message_id: int | None = None, reply_markup=None) -> int | None:
        if message_id is not None:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
                return message_id
            except TelegramBadRequest as exc:
                if "message is not modified" in str(exc).lower():
                    return message_id

        sent = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        return sent.message_id

    async def health_notifier(text: str, owner_id: int | None = None) -> None:
        recipients: list[int] = []
        if owner_id is not None:
            recipients = await access_manager.list_health_notification_user_ids(owner_id)

        if not recipients and owner_id is not None:
            user = await access_manager.get_user(owner_id)
            if user and user.is_active:
                recipients = [owner_id]

        if not recipients:
            recipients = [HEALTH_NOTIFY_CHAT_ID]
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                logging.getLogger(__name__).warning("Не удалось отправить уведомление о здоровье в chat_id=%s", chat_id)

    account_manager.set_health_notifier(health_notifier)
    await account_manager.start_health_monitor(interval_seconds=90)
    dp = Dispatcher()
    dp.include_router(build_router(task_queue, task_application_service, account_manager, access_manager, session_store=session_store))

    worker = Worker(task_queue=task_queue, sender=task_gateway, notifier=notifier, task_service=task_application_service)
    worker_task = asyncio.create_task(worker.run(), name="telethon-worker")
    admin_api: AdminApiServer | None = None

    if ADMIN_API_ENABLED:
        admin_api = AdminApiServer(
            host=ADMIN_API_HOST,
            port=ADMIN_API_PORT,
            token=ADMIN_API_TOKEN,
            loop=asyncio.get_running_loop(),
            access_manager=access_manager,
            account_manager=account_manager,
            task_queue=task_queue,
            session_store=session_store,
            logs_dir=LOGS_DIR,
        )
        admin_api.start()

    audit_event(
        "system.started",
        message="Запуск приложения завершён",
        accounts_loaded=account_manager.size,
    )

    try:
        await dp.start_polling(bot)
    finally:
        audit_event("system.stopping", message="Остановка приложения начата")
        if admin_api is not None:
            await asyncio.to_thread(admin_api.stop)
        await worker.stop()
        await worker_task
        await account_manager.stop_health_monitor()
        await account_manager.disconnect_all()
        await bot.session.close()
        audit_event("system.stopped", message="Остановка приложения завершена")


if __name__ == "__main__":
    asyncio.run(run())
    
    

