from __future__ import annotations

import asyncio
import copy
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from html import escape
from typing import TYPE_CHECKING

from bot.keyboards import task_actions_keyboard
from task_application import TaskApplicationService, TaskSenderGateway, task_application_service
from task_domain import task_accounts_count, task_delay
from task_domain import JoinTask, LeaveTask, LikeTask, MsgBotTask, MsgChatTask, MsgTask, RefTask, TaskSpec, VoteTask, task_kind, task_title
from core.observability import audit_event

logger = logging.getLogger(__name__)

ACTIVE_TASK_STATUSES = {"queued", "paused", "running", "cancel_requested"}
FINISHED_TASK_STATUSES = {"completed", "failed", "canceled"}


class TaskCancelledError(Exception):
    pass


@dataclass(slots=True)
class StopTask:
    pass


TaskItem = TaskSpec | StopTask


@dataclass(slots=True)
class TaskControl:
    paused: bool = False
    cancel_requested: bool = False
    _resume_event: asyncio.Event = field(default_factory=asyncio.Event, repr=False)

    def __post_init__(self) -> None:
        self._resume_event.set()

    def pause(self) -> None:
        self.paused = True
        self._resume_event.clear()

    def resume(self) -> None:
        self.paused = False
        self._resume_event.set()

    def cancel(self) -> None:
        self.cancel_requested = True
        self._resume_event.set()

    async def checkpoint(self) -> None:
        await self._resume_event.wait()
        if self.cancel_requested:
            raise TaskCancelledError("Task was canceled by user.")

    async def controlled_sleep(self, seconds: float) -> None:
        remaining = max(0.0, seconds)
        while remaining > 0:
            await self.checkpoint()
            chunk = min(0.25, remaining)
            await asyncio.sleep(chunk)
            remaining -= chunk


@dataclass(slots=True)
class TaskRecord:
    id: int
    kind: str
    title: str
    chat_id: int
    requested_by_user_id: int
    payload: TaskItem
    status: str
    created_at: float
    updated_at: float
    control: TaskControl = field(default_factory=TaskControl, repr=False)
    started_at: float | None = None
    finished_at: float | None = None
    result_text: str = ""
    progress_text: str = ""
    status_message_id: int | None = None
    status_message_chat_id: int | None = None
    last_notified_text: str = ""


def status_icon(status: str) -> str:
    mapping = {
        "queued": "🕒",
        "paused": "⏸",
        "running": "▶️",
        "cancel_requested": "🛑",
        "completed": "✅",
        "failed": "❌",
        "canceled": "🚫",
    }
    return mapping.get(status, "⚪")


def task_sort_key(record: TaskRecord) -> tuple[int, float, int]:
    priority = {
        "running": 0,
        "cancel_requested": 1,
        "paused": 2,
        "queued": 3,
        "failed": 4,
        "completed": 5,
        "canceled": 6,
    }
    timestamp = record.started_at or record.updated_at or record.created_at or time.time()
    return (priority.get(record.status, 99), -timestamp, -record.id)


class TaskQueue:
    def __init__(self) -> None:
        self._condition = asyncio.Condition()
        self._records: dict[int, TaskRecord] = {}
        self._order: list[int] = []
        self._next_id = 1
        self._current_task_id: int | None = None
        self._stop_after_drain = False

    async def put(self, task: TaskItem, *, requested_by_user_id: int) -> TaskRecord:
        async with self._condition:
            task_id = self._next_id
            self._next_id += 1

            now = time.time()
            record = TaskRecord(
                id=task_id,
                kind=task_kind(task),
                title=task_title(task),
                chat_id=getattr(task, "chat_id", 0),
                requested_by_user_id=int(requested_by_user_id),
                payload=task,
                status="queued",
                created_at=now,
                updated_at=now,
            )
            self._records[task_id] = record
            self._order.append(task_id)
            self._condition.notify_all()
            audit_event(
                "task.queued",
                message="Задача добавлена в очередь",
                task_id=task_id,
                kind=record.kind,
                title=record.title,
                chat_id=record.chat_id,
                requested_by_user_id=record.requested_by_user_id,
            )
            return record

    async def get(self) -> TaskRecord | StopTask:
        async with self._condition:
            while True:
                for task_id in self._order:
                    record = self._records.get(task_id)
                    if record is None or record.status != "queued":
                        continue

                    record.status = "running"
                    record.started_at = time.time()
                    record.updated_at = time.time()
                    self._current_task_id = task_id
                    self._condition.notify_all()
                    audit_event(
                        "task.started",
                        message="Задача запущена",
                        task_id=record.id,
                        kind=record.kind,
                        title=record.title,
                        requested_by_user_id=record.requested_by_user_id,
                    )
                    return record

                if self._stop_after_drain and self._current_task_id is None:
                    return StopTask()

                await self._condition.wait()

    def task_done(self) -> None:
        return None

    def qsize(self, *, requested_by_user_id: int | None = None, include_all: bool = False) -> int:
        return sum(
            1
            for record in self._records.values()
            if record.status in ACTIVE_TASK_STATUSES
            and self._record_visible_to(
                record,
                requested_by_user_id=requested_by_user_id,
                include_all=include_all,
            )
        )

    async def join(self) -> None:
        async with self._condition:
            while self._current_task_id is not None or any(
                record.status in ACTIVE_TASK_STATUSES for record in self._records.values()
            ):
                await self._condition.wait()

    async def stop(self) -> None:
        async with self._condition:
            self._stop_after_drain = True
            self._condition.notify_all()

    async def finish_task(self, task_id: int, status: str, result_text: str) -> None:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return

            now = time.time()
            record.status = status
            record.result_text = result_text
            record.progress_text = result_text
            record.finished_at = now
            record.updated_at = now
            if self._current_task_id == task_id:
                self._current_task_id = None
            self._condition.notify_all()
            audit_event(
                "task.finished",
                message="Задача завершена",
                task_id=record.id,
                kind=record.kind,
                status=status,
                requested_by_user_id=record.requested_by_user_id,
            )

    async def update_progress(self, task_id: int, text: str) -> None:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return
            record.progress_text = text
            record.updated_at = time.time()
            self._condition.notify_all()

    async def bind_status_message(self, task_id: int, *, chat_id: int, message_id: int, text: str = "") -> None:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return
            if record.status_message_id is not None and record.status_message_chat_id is not None:
                self._condition.notify_all()
                return
            record.status_message_chat_id = chat_id
            record.status_message_id = message_id
            if text:
                record.last_notified_text = text
            self._condition.notify_all()

    async def pause_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> tuple[bool, str]:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return False, "Задача не найдена."
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return False, "Нет доступа к задаче."

            if record.status == "queued":
                record.status = "paused"
                record.updated_at = time.time()
                self._condition.notify_all()
                audit_event("task.paused", message="Задача поставлена на паузу до старта", task_id=record.id, requested_by_user_id=requested_by_user_id)
                return True, "Задача поставлена на паузу."

            if record.status == "running":
                record.control.pause()
                record.status = "paused"
                record.updated_at = time.time()
                self._condition.notify_all()
                audit_event("task.paused", message="Запрошена пауза задачи", task_id=record.id, requested_by_user_id=requested_by_user_id)
                return True, "Пауза будет применена на ближайшем шаге."

            if record.status == "paused":
                return False, "Задача уже на паузе."

            return False, "Эту задачу нельзя поставить на паузу."

    async def resume_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> tuple[bool, str]:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return False, "Задача не найдена."
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return False, "Нет доступа к задаче."

            if record.status != "paused":
                return False, "Эта задача сейчас не на паузе."

            record.updated_at = time.time()
            if self._current_task_id == task_id:
                record.control.resume()
                record.status = "running"
                self._condition.notify_all()
                audit_event("task.resumed", message="Задача продолжена", task_id=record.id, requested_by_user_id=requested_by_user_id)
                return True, "Задача продолжит выполнение."

            record.status = "queued"
            self._condition.notify_all()
            audit_event("task.requeued", message="Задача возвращена в очередь", task_id=record.id, requested_by_user_id=requested_by_user_id)
            return True, "Задача возвращена в очередь."

    async def cancel_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> tuple[bool, str]:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return False, "Задача не найдена."
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return False, "Нет доступа к задаче."

            if record.status in {"completed", "failed", "canceled"}:
                return False, "Задача уже завершена."

            if self._current_task_id == task_id:
                record.control.cancel()
                record.status = "cancel_requested"
                record.updated_at = time.time()
                self._condition.notify_all()
                audit_event("task.cancel_requested", message="Запрошена остановка задачи", task_id=record.id, requested_by_user_id=requested_by_user_id)
                return True, "Остановка запрошена."

            record.status = "canceled"
            record.result_text = "🛑 Задача отменена до запуска."
            record.progress_text = record.result_text
            record.finished_at = time.time()
            record.updated_at = record.finished_at
            self._condition.notify_all()
            audit_event("task.canceled", message="Задача отменена до старта", task_id=record.id, requested_by_user_id=requested_by_user_id)
            return True, "Задача отменена."

    async def remove_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> tuple[bool, str]:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return False, "Задача не найдена."
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return False, "Нет доступа к задаче."
            if record.status in ACTIVE_TASK_STATUSES:
                return False, "Нельзя удалять активную задачу."

            self._records.pop(task_id, None)
            self._order = [item for item in self._order if item != task_id]
            self._condition.notify_all()
            audit_event("task.removed", message="Задача удалена из истории", task_id=record.id, requested_by_user_id=requested_by_user_id)
            return True, "Задача удалена из истории."

    async def clear_finished(
        self,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> int:
        async with self._condition:
            removable = [
                task_id
                for task_id, record in self._records.items()
                if record.status in FINISHED_TASK_STATUSES
                and self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all)
            ]
            removable_set = set(removable)
            for task_id in removable:
                self._records.pop(task_id, None)
            self._order = [task_id for task_id in self._order if task_id not in removable_set]
            self._condition.notify_all()
            if removable:
                audit_event("task.history_cleared", message="История завершённых задач очищена", removed=len(removable), requested_by_user_id=requested_by_user_id)
            return len(removable)

    async def restart_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> tuple[bool, str, int | None]:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return False, "Задача не найдена.", None
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return False, "Нет доступа к задаче.", None

            new_task_id = self._next_id
            self._next_id += 1
            now = time.time()
            new_record = TaskRecord(
                id=new_task_id,
                kind=record.kind,
                title=record.title,
                chat_id=record.chat_id,
                requested_by_user_id=record.requested_by_user_id,
                payload=copy.deepcopy(record.payload),
                status="queued",
                created_at=now,
                updated_at=now,
            )
            self._records[new_task_id] = new_record
            self._order.append(new_task_id)

            if record.status == "running":
                record.control.cancel()
                record.status = "cancel_requested"
                record.updated_at = now
            elif record.status in {"queued", "paused"}:
                record.status = "canceled"
                record.result_text = f"🔄 Перезапущена как задача #{new_task_id}."
                record.progress_text = record.result_text
                record.finished_at = now
                record.updated_at = now

            self._condition.notify_all()
            audit_event(
                "task.restarted",
                message="Задача перезапущена",
                task_id=task_id,
                new_task_id=new_task_id,
                requested_by_user_id=requested_by_user_id,
            )
            return True, f"Задача перезапущена: #{new_task_id}.", new_task_id

    async def stop_all_tasks(
        self,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> int:
        async with self._condition:
            now = time.time()
            stopped = 0
            for record in self._records.values():
                if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                    continue
                if record.status == "running":
                    record.control.cancel()
                    record.status = "cancel_requested"
                    record.updated_at = now
                    stopped += 1
                elif record.status in {"queued", "paused"}:
                    record.status = "canceled"
                    record.result_text = "🛑 Остановлена через глобальную команду."
                    record.progress_text = record.result_text
                    record.finished_at = now
                    record.updated_at = now
                    stopped += 1

            self._condition.notify_all()
            if stopped:
                audit_event(
                    "task.stop_all_requested",
                    message="Global stop requested for tasks",
                    stopped=stopped,
                    requested_by_user_id=requested_by_user_id,
                )
            return stopped

    async def get_task(
        self,
        task_id: int,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> dict | None:
        async with self._condition:
            record = self._records.get(task_id)
            if record is None:
                return None
            if not self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all):
                return None
            return self._serialize_record(record)

    async def list_tasks(
        self,
        limit: int = 20,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> list[dict]:
        async with self._condition:
            records = [
                self._records[task_id]
                for task_id in self._order
                if task_id in self._records
                and self._record_visible_to(self._records[task_id], requested_by_user_id=requested_by_user_id, include_all=include_all)
            ]
            records.sort(key=task_sort_key)
            return [self._serialize_record(record) for record in records[:limit]]

    async def stats(
        self,
        *,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> dict:
        async with self._condition:
            records = [
                record
                for record in self._records.values()
                if self._record_visible_to(record, requested_by_user_id=requested_by_user_id, include_all=include_all)
            ]
            return {
                "running": sum(1 for record in records if record.status == "running"),
                "paused": sum(1 for record in records if record.status == "paused"),
                "queued": sum(1 for record in records if record.status == "queued"),
                "cancel_requested": sum(1 for record in records if record.status == "cancel_requested"),
                "finished": sum(1 for record in records if record.status in FINISHED_TASK_STATUSES),
                "active_total": sum(1 for record in records if record.status in ACTIVE_TASK_STATUSES),
                "has_finished": any(record.status in FINISHED_TASK_STATUSES for record in records),
                "current_task_id": next((record.id for record in records if record.id == self._current_task_id), None),
            }

    async def find_task_id_by_status_message(
        self,
        *,
        chat_id: int,
        message_id: int,
        requested_by_user_id: int | None = None,
        include_all: bool = False,
    ) -> int | None:
        async with self._condition:
            for record in self._records.values():
                if int(record.status_message_chat_id or 0) != int(chat_id):
                    continue
                if int(record.status_message_id or 0) != int(message_id):
                    continue
                if not self._record_visible_to(
                    record,
                    requested_by_user_id=requested_by_user_id,
                    include_all=include_all,
                ):
                    continue
                return int(record.id)
            return None

    def _serialize_record(self, record: TaskRecord) -> dict:
        active_queue_ids = [
            task_id
            for task_id in self._order
            if task_id in self._records and self._records[task_id].status in {"queued", "paused"}
        ]
        queue_position = None
        if record.status in {"queued", "paused"} and record.id in active_queue_ids:
            queue_position = active_queue_ids.index(record.id) + 1

        payload = record.payload
        accounts_count = task_accounts_count(payload)
        delay = task_delay(payload)

        return {
            "id": record.id,
            "kind": record.kind,
            "title": record.title,
            "chat_id": record.chat_id,
            "requested_by_user_id": record.requested_by_user_id,
            "status": record.status,
            "status_icon": status_icon(record.status),
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "started_at": record.started_at,
            "finished_at": record.finished_at,
            "result_text": record.result_text,
            "progress_text": record.progress_text,
            "queue_position": queue_position,
            "accounts_count": accounts_count,
            "delay": delay,
        }

    @staticmethod
    def _record_visible_to(
        record: TaskRecord,
        *,
        requested_by_user_id: int | None,
        include_all: bool,
    ) -> bool:
        if include_all or requested_by_user_id is None:
            return True
        return record.requested_by_user_id == int(requested_by_user_id)


class Worker:
    def __init__(
        self,
        task_queue: "TaskQueue",
        sender: "TaskSenderGateway",
        notifier: Callable[[int, str, int | None, object | None], Awaitable[int | None]],
        task_service: TaskApplicationService | None = None,
    ) -> None:
        self.task_queue = task_queue
        self.sender = sender
        self.notifier = notifier
        self.task_service = task_service or task_application_service
        self._running = True

    async def run(self) -> None:
        while self._running:
            item = await self.task_queue.get()
            if isinstance(item, StopTask):
                self._running = False
                continue

            record = item
            try:
                await self.task_queue.update_progress(record.id, "▶️ Задача запущена.")
                result_text = await self._process(record)
                await self.task_queue.finish_task(record.id, "completed", result_text)
                await self._safe_notify(record, result_text)
            except TaskCancelledError:
                text = f"🛑 Задача #{record.id} остановлена пользователем."
                await self.task_queue.finish_task(record.id, "canceled", text)
                await self._safe_notify(record, text)
            except Exception as exc:
                logger.exception("Unhandled worker error for task_id=%s", record.id)
                text = f"❌ Задача #{record.id} завершилась с ошибкой: <code>{escape(str(exc) or exc.__class__.__name__)}</code>"
                await self.task_queue.finish_task(record.id, "failed", text)
                await self._safe_notify(record, text)

    async def stop(self) -> None:
        await self.task_queue.stop()

    async def _process(self, record: TaskRecord) -> str:
        async def task_progress(text: str) -> None:
            await self.task_queue.update_progress(record.id, text)
            await self._safe_notify(record, text)

        return await self.task_service.execute_task(
            record,
            sender=self.sender,
            progress_cb=task_progress,
        )

    async def _safe_notify(self, record: TaskRecord, text: str) -> None:
        if record.last_notified_text == text:
            return
        if record.status_message_id is None:
            for _ in range(15):
                if record.status_message_id is not None:
                    break
                await asyncio.sleep(0.1)
        for attempt in range(1, 4):
            try:
                message_id = await self.notifier(
                    record.status_message_chat_id or record.chat_id,
                    text,
                    record.status_message_id,
                    task_actions_keyboard(record.id, record.status),
                )
                if message_id is not None:
                    record.status_message_id = message_id
                    record.status_message_chat_id = record.status_message_chat_id or record.chat_id
                record.last_notified_text = text
                return
            except Exception:
                if attempt == 3:
                    logger.exception("Не удалось отправить обновление в chat_id=%s", record.chat_id)
                    return
                await asyncio.sleep(attempt)


__all__ = [
    "ACTIVE_TASK_STATUSES",
    "FINISHED_TASK_STATUSES",
    "JoinTask",
    "LeaveTask",
    "LikeTask",
    "MsgBotTask",
    "MsgChatTask",
    "MsgTask",
    "RefTask",
    "TaskCancelledError",
    "TaskControl",
    "TaskItem",
    "TaskQueue",
    "TaskRecord",
    "StopTask",
    "status_icon",
    "task_sort_key",
    "VoteTask",
    "Worker",
]
