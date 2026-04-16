from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, Protocol

from config import MAX_COUNT
from task_domain import (
    CALL_TASK,
    JOIN_TASK,
    LEAVE_TASK,
    LIKE_TASK,
    MSGBOT_TASK,
    MSGCHAT_TASK,
    MSG_TASK,
    REF_TASK,
    VOTE_TASK,
    JoinTask,
    CallTask,
    LeaveTask,
    LikeTask,
    MsgBotTask,
    MsgChatTask,
    MsgTask,
    RefTask,
    TaskSpec,
    TaskType,
    VoteTask,
    task_catalog,
)

if TYPE_CHECKING:
    from core.queue import TaskQueue
    from core.queue import TaskControl, TaskRecord


class TaskSenderGateway(Protocol):
    async def send_messages(self, targets: list[str], text: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], photo_path: str = "", hide_content: bool = False, *, requester_user_id: int, task_control: "TaskControl | None" = None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> tuple[int, int]: ...
    async def send_to_bot(self, bot_username: str, command: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], *, requester_user_id: int, task_control: "TaskControl | None" = None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> tuple[int, int]: ...
    async def call_user(self, target: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], *, requester_user_id: int, task_control: "TaskControl | None" = None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> str: ...
    async def join_chat(self, link: str, count: int = 1, delay_cap: float | tuple[float, float] = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control: "TaskControl | None" = None) -> str: ...
    async def leave_chat(self, link: str, count: int = 1, delay_cap: float | tuple[float, float] = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control: "TaskControl | None" = None) -> str: ...
    async def react_to_post(self, link: str, count: int, delay: float | tuple[float, float], emojis: list[str], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control: "TaskControl | None" = None) -> str: ...
    async def follow_referral(self, link: str, count: int, delay: float | tuple[float, float], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control: "TaskControl | None" = None) -> str: ...
    async def vote_in_poll(self, link: str, option_index: int, count: int, delay: float | tuple[float, float], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control: "TaskControl | None" = None) -> str: ...


@dataclass(slots=True)
class ParseTaskContext:
    chat_id: int
    requester_user_id: int


@dataclass(slots=True)
class TaskRuntimeContext:
    sender: TaskSenderGateway
    requester_user_id: int
    task_control: "TaskControl | None"
    progress_cb: Callable[[str], Awaitable[None]] | None = None


class TaskParser(Protocol):
    task_type: TaskType
    def parse(self, payload: str, *, context: ParseTaskContext) -> TaskSpec: ...


class TaskHandler(Protocol):
    task_type: TaskType
    spec_type: type[TaskSpec]
    def validate(self, spec: TaskSpec) -> None: ...
    async def execute(self, spec: TaskSpec, runtime: TaskRuntimeContext) -> str: ...


class TaskPresenter(Protocol):
    task_type: TaskType
    command: str
    def usage_text(self) -> str: ...
    def describe(self, spec: TaskSpec) -> str: ...


@dataclass(frozen=True, slots=True)
class RegisteredTask:
    task_type: TaskType
    spec_type: type[TaskSpec]
    parser: TaskParser | None = None
    handler: TaskHandler | None = None
    presenter: TaskPresenter | None = None


class TaskRegistry:
    def __init__(self) -> None:
        self._by_type: dict[str, RegisteredTask] = {}
        self._by_spec_type: dict[type[object], RegisteredTask] = {}

    def register(self, task: RegisteredTask) -> None:
        self._by_type[task.task_type] = task
        self._by_spec_type[task.spec_type] = task

    def maybe_get(self, task_type: str) -> RegisteredTask | None:
        return self._by_type.get(task_type)

    def get(self, task_type: str) -> RegisteredTask:
        task = self.maybe_get(task_type)
        if task is None:
            raise KeyError(f"Task type '{task_type}' is not registered.")
        return task

    def definition_for_spec(self, spec: TaskSpec) -> RegisteredTask | None:
        return self._by_spec_type.get(type(spec))


DelayValue = float | tuple[float, float]


def parse_delay_value(raw_value: str, field_name: str) -> float:
    normalized = raw_value.strip().replace(",", ".")
    try:
        value = float(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a number not less than 0. Example: 1,1 or 1.7.") from exc
    if value < 0:
        raise ValueError(f"{field_name} must be a number not less than 0.")
    return value


def parse_delay_input(raw_value: str, field_name: str) -> DelayValue:
    normalized = raw_value.strip().replace(",", ".")
    if not normalized:
        raise ValueError(f"{field_name} must be a number not less than 0.")
    range_match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)", normalized)
    if range_match:
        min_value = float(range_match.group(1))
        max_value = float(range_match.group(2))
        if min_value < 0 or max_value < 0:
            raise ValueError(f"{field_name} must be a number not less than 0.")
        if min_value > max_value:
            raise ValueError(f"In range {field_name}, the left value cannot be greater than the right one.")
        return (min_value, max_value)
    return parse_delay_value(normalized, field_name)


def parse_link_count_delay(payload: str, *, command_name: str) -> tuple[str, int, DelayValue]:
    parts = payload.split()
    if len(parts) == 1:
        return parts[0].strip(), 1, 1.5
    if len(parts) != 3:
        raise ValueError(f"Format: /{command_name} <link> <N> <T>")
    if not parts[1].isdigit():
        raise ValueError(f"N for /{command_name} must be an integer greater than 0.")
    count = int(parts[1])
    if count < 1:
        raise ValueError(f"N for /{command_name} must be an integer greater than 0.")
    return parts[0].strip(), count, parse_delay_input(parts[2], field_name=f"T for /{command_name}")


def split_command_parts(payload: str) -> list[str]:
    try:
        return shlex.split(payload)
    except ValueError:
        return payload.split()


def normalize_username(value: str) -> str:
    username = value.strip().strip(",").strip("/")
    username = re.sub(r"^https?://(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = re.sub(r"^(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = username.split("?", maxsplit=1)[0].strip("/")
    cleaned = username[1:] if username.startswith("@") else username
    if not re.fullmatch(r"[A-Za-z0-9_]{5,64}", cleaned):
        raise ValueError(f"Invalid username: {value}")
    return f"@{cleaned}"


def normalize_chat_target(value: str) -> str:
    raw = value.strip().strip(",")
    prepared = prepare_telegram_url(raw)
    invite_match = re.search(r"(?:(?:t|telegram)\.me/(?:joinchat/|\+))([A-Za-z0-9_-]+)", prepared, flags=re.IGNORECASE)
    if invite_match:
        return f"https://t.me/+{invite_match.group(1)}"
    if raw.startswith("@"):
        return normalize_username(raw)
    raw = re.sub(r"^https?://(?:www\.)?(?:t|telegram)\.me/", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^(?:www\.)?(?:t|telegram)\.me/", "", raw, flags=re.IGNORECASE)
    raw = raw.strip("/")
    if not raw:
        raise ValueError("You need to provide a chat link or username.")
    if "/" in raw:
        raise ValueError("/msgchat expects a chat/channel link, not a single message link.")
    return normalize_username(raw)


def _is_explicit_msg_target(token: str) -> bool:
    normalized = token.strip().strip(",")
    if normalized.startswith("@"):
        return True
    return bool(
        re.match(
            r"^(?:https?://)?(?:www\.)?(?:t|telegram)\.me/",
            normalized,
            flags=re.IGNORECASE,
        )
    )


def _extract_hide_flag(parts: list[str]) -> tuple[list[str], bool]:
    if parts and parts[-1].lower() == "-h":
        return parts[:-1], True
    return parts, False


def validate_accounts_repeat_delay(raw_accounts: str, raw_repeats: str, raw_delay: str) -> tuple[int, int, DelayValue]:
    if not raw_accounts.isdigit():
        raise ValueError("Account count must be an integer greater than 0.")
    accounts_count = int(raw_accounts)
    if accounts_count < 1 or accounts_count > MAX_COUNT:
        raise ValueError(f"Account count must be between 1 and {MAX_COUNT}.")
    if not raw_repeats.isdigit():
        raise ValueError("Repeat count must be an integer from 1 to 100.")
    repeat_count = int(raw_repeats)
    if repeat_count < 1 or repeat_count > 100:
        raise ValueError("Repeat count must be an integer from 1 to 100.")
    return accounts_count, repeat_count, parse_delay_input(raw_delay, field_name="T")


def parse_msg_payload(payload: str, *, allow_empty_text: bool = False) -> tuple[list[str], str, int, int, DelayValue, bool]:
    parts = split_command_parts(payload)
    parts, hide_content = _extract_hide_flag(parts)
    if len(parts) < 4:
        raise ValueError("Format: /msg @user1 @user2 text accounts repeats delay")
    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(parts[-3], parts[-2], parts[-1])
    target_tokens: list[str] = []
    index = 0
    while index < len(parts) - 3:
        token = parts[index]
        if not _is_explicit_msg_target(token):
            break
        try:
            normalize_username(token)
        except ValueError:
            break
        target_tokens.append(token)
        index += 1
    if not target_tokens:
        raise ValueError("You need to specify at least one target at the start of the command.")
    text = " ".join(parts[index:-3]).strip()
    if not text and not allow_empty_text:
        raise ValueError("You need to provide message text.")
    return [normalize_username(item) for item in target_tokens], text, accounts_count, repeat_count, delay, hide_content


def parse_msgchat_payload(payload: str, *, allow_empty_text: bool = False) -> tuple[str, str, int, int, DelayValue, bool]:
    parts = split_command_parts(payload)
    parts, hide_content = _extract_hide_flag(parts)
    if len(parts) < 4:
        raise ValueError("Format: /msgchat <chat> text accounts repeats delay")
    target = normalize_chat_target(parts[0])
    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(parts[-3], parts[-2], parts[-1])
    text = " ".join(parts[1:-3]).strip()
    if not text and not allow_empty_text:
        raise ValueError("You need to provide message text.")
    return target, text, accounts_count, repeat_count, delay, hide_content


def _ensure_call_delay_minimum(delay: DelayValue, minimum_seconds: float = 5.0) -> DelayValue:
    if isinstance(delay, tuple):
        min_value, max_value = delay
        if min_value < minimum_seconds:
            raise ValueError(f"T for /call must be at least {int(minimum_seconds)} seconds.")
        return delay
    if delay < minimum_seconds:
        raise ValueError(f"T for /call must be at least {int(minimum_seconds)} seconds.")
    return delay


def parse_call_payload(payload: str) -> tuple[str, int, int, DelayValue]:
    parts = split_command_parts(payload)
    if len(parts) != 4:
        raise ValueError("Format: /call @username accounts repeats delay")
    target = normalize_username(parts[0])
    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(parts[1], parts[2], parts[3])
    return target, accounts_count, repeat_count, _ensure_call_delay_minimum(delay)


class MsgTaskParser:
    task_type = MSG_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> MsgTask:
        targets, text, accounts_count, repeat_count, delay, hide_content = parse_msg_payload(payload, allow_empty_text=True)
        return MsgTask(chat_id=context.chat_id, targets=targets, text=text, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay, hide_content=hide_content)


class MsgTaskHandler:
    task_type = MSG_TASK
    spec_type = MsgTask
    def validate(self, spec: MsgTask) -> None:
        if not spec.targets:
            raise ValueError("You need to specify at least one target at the start of the command.")
        if not spec.text and not spec.photo_path:
            raise ValueError("Message text or an attached photo is required.")
        if spec.accounts_count < 1:
            raise ValueError("Account count must be greater than 0.")
        if spec.repeat_count < 1:
            raise ValueError("Repeat count must be greater than 0.")
    async def execute(self, spec: MsgTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        ok, bad = await runtime.sender.send_messages(targets=spec.targets, text=spec.text, accounts_count=spec.accounts_count, repeat_count=spec.repeat_count, delay=spec.delay, photo_path=spec.photo_path, hide_content=spec.hide_content, requester_user_id=runtime.requester_user_id, task_control=runtime.task_control, progress_cb=runtime.progress_cb)
        return f"OK /msg finished. Success: {ok}, errors: {bad}."


class MsgTaskPresenter:
    task_type = MSG_TASK
    command = "/msg"
    def usage_text(self) -> str:
        return (
            "Example:\n"
            "<code>/msg @user1 @user2 \"hello guys\" 10 5 1.1</code>\n"
            "<code>/msg @user1 @user2 10 5 1.1</code> + photo"
        )
    def describe(self, spec: MsgTask) -> str:
        return f"{self.command} -> {', '.join(spec.targets)}"


class MsgBotTaskParser:
    task_type = MSGBOT_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> MsgBotTask:
        parts = split_command_parts(payload)
        if len(parts) < 5:
            raise ValueError("Format: /msgbot @bot_username command accounts repeats delay")
        bot_username = normalize_username(parts[0])
        accounts_count, repeat_count, delay = validate_accounts_repeat_delay(parts[-3], parts[-2], parts[-1])
        command = " ".join(parts[1:-3]).strip()
        if not command:
            raise ValueError("You need to provide a command or text for the bot.")
        return MsgBotTask(chat_id=context.chat_id, bot_username=bot_username, command=command, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay)


class MsgBotTaskHandler:
    task_type = MSGBOT_TASK
    spec_type = MsgBotTask
    def validate(self, spec: MsgBotTask) -> None:
        if not spec.command.strip():
            raise ValueError("You need to provide a command or text for the bot.")
        if spec.accounts_count < 1:
            raise ValueError("Account count must be greater than 0.")
        if spec.repeat_count < 1:
            raise ValueError("Repeat count must be greater than 0.")
    async def execute(self, spec: MsgBotTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        ok, bad = await runtime.sender.send_to_bot(bot_username=spec.bot_username, command=spec.command, accounts_count=spec.accounts_count, repeat_count=spec.repeat_count, delay=spec.delay, requester_user_id=runtime.requester_user_id, task_control=runtime.task_control, progress_cb=runtime.progress_cb)
        return f"OK /msgbot finished. Success: {ok}, errors: {bad}."


class MsgBotTaskPresenter:
    task_type = MSGBOT_TASK
    command = "/msgbot"
    def usage_text(self) -> str:
        return "Example:\n<code>/msgbot @testbot /start 10 5 0.7</code>"
    def describe(self, spec: MsgBotTask) -> str:
        return f"{self.command} -> {spec.bot_username}"


class CallTaskParser:
    task_type = CALL_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> CallTask:
        target, accounts_count, repeat_count, delay = parse_call_payload(payload)
        return CallTask(
            chat_id=context.chat_id,
            target=target,
            accounts_count=accounts_count,
            repeat_count=repeat_count,
            delay=delay,
        )


class CallTaskHandler:
    task_type = CALL_TASK
    spec_type = CallTask
    def validate(self, spec: CallTask) -> None:
        if not spec.target.strip():
            raise ValueError("You need to provide a target username.")
        if spec.accounts_count < 1:
            raise ValueError("Account count must be greater than 0.")
        if spec.repeat_count < 1:
            raise ValueError("Repeat count must be greater than 0.")
        _ensure_call_delay_minimum(spec.delay)

    async def execute(self, spec: CallTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        return await runtime.sender.call_user(
            target=spec.target,
            accounts_count=spec.accounts_count,
            repeat_count=spec.repeat_count,
            delay=spec.delay,
            requester_user_id=runtime.requester_user_id,
            task_control=runtime.task_control,
            progress_cb=runtime.progress_cb,
        )


class CallTaskPresenter:
    task_type = CALL_TASK
    command = "/call"
    def usage_text(self) -> str:
        return "Example:\n<code>/call @username 5 3 5</code>"

    def describe(self, spec: CallTask) -> str:
        return f"{self.command} -> {spec.target}"


class MsgChatTaskParser:
    task_type = MSGCHAT_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> MsgChatTask:
        target, text, accounts_count, repeat_count, delay, hide_content = parse_msgchat_payload(payload, allow_empty_text=True)
        return MsgChatTask(chat_id=context.chat_id, target=target, text=text, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay, hide_content=hide_content)


class MsgChatTaskHandler:
    task_type = MSGCHAT_TASK
    spec_type = MsgChatTask
    def validate(self, spec: MsgChatTask) -> None:
        if not spec.target.strip():
            raise ValueError("You need to provide a chat link or username.")
        if not spec.text and not spec.photo_path:
            raise ValueError("Message text or an attached photo is required.")
        if spec.accounts_count < 1:
            raise ValueError("Account count must be greater than 0.")
        if spec.repeat_count < 1:
            raise ValueError("Repeat count must be greater than 0.")
    async def execute(self, spec: MsgChatTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        ok, bad = await runtime.sender.send_messages(targets=[spec.target], text=spec.text, accounts_count=spec.accounts_count, repeat_count=spec.repeat_count, delay=spec.delay, photo_path=spec.photo_path, hide_content=spec.hide_content, requester_user_id=runtime.requester_user_id, task_control=runtime.task_control, progress_cb=runtime.progress_cb)
        return f"OK /msgchat finished. Success: {ok}, errors: {bad}."


class MsgChatTaskPresenter:
    task_type = MSGCHAT_TASK
    command = "/msgchat"
    def usage_text(self) -> str:
        return (
            "Example:\n"
            "<code>/msgchat https://t.me/publicchat \"hello guys\" 10 5 1.1</code>\n"
            "<code>/msgchat @publicchat 10 5 1.1</code> + photo"
        )
    def describe(self, spec: MsgChatTask) -> str:
        return f"{self.command} -> {spec.target}"


class JoinTaskParser:
    task_type = JOIN_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> JoinTask:
        link, count, delay_cap = parse_link_count_delay(payload, command_name="join")
        return JoinTask(chat_id=context.chat_id, link=link, count=count, delay_cap=delay_cap)


class JoinTaskHandler:
    task_type = JOIN_TASK
    spec_type = JoinTask
    def validate(self, spec: JoinTask) -> None:
        if not spec.link.strip():
            raise ValueError("Link for /join cannot be empty.")
        if spec.count < 1:
            raise ValueError("N for /join must be an integer greater than 0.")
    async def execute(self, spec: JoinTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        return await runtime.sender.join_chat(link=spec.link, count=spec.count, delay_cap=spec.delay_cap, requester_user_id=runtime.requester_user_id, progress_cb=runtime.progress_cb, task_control=runtime.task_control)


class JoinTaskPresenter:
    task_type = JOIN_TASK
    command = "/join"
    def usage_text(self) -> str:
        return "Example:\n<code>/join https://t.me/+abcd 5 1.7</code>"
    def describe(self, spec: JoinTask) -> str:
        return f"{self.command} -> {spec.link}"


class LeaveTaskParser:
    task_type = LEAVE_TASK
    def parse(self, payload: str, *, context: ParseTaskContext) -> LeaveTask:
        link, count, delay_cap = parse_link_count_delay(payload, command_name="leave")
        return LeaveTask(chat_id=context.chat_id, link=link, count=count, delay_cap=delay_cap)


class LeaveTaskHandler:
    task_type = LEAVE_TASK
    spec_type = LeaveTask
    def validate(self, spec: LeaveTask) -> None:
        if not spec.link.strip():
            raise ValueError("Link for /leave cannot be empty.")
        if spec.count < 1:
            raise ValueError("N for /leave must be an integer greater than 0.")
    async def execute(self, spec: LeaveTask, runtime: TaskRuntimeContext) -> str:
        self.validate(spec)
        return await runtime.sender.leave_chat(link=spec.link, count=spec.count, delay_cap=spec.delay_cap, requester_user_id=runtime.requester_user_id, progress_cb=runtime.progress_cb, task_control=runtime.task_control)


class LeaveTaskPresenter:
    task_type = LEAVE_TASK
    command = "/leave"
    def usage_text(self) -> str:
        return "Example:\n<code>/leave https://t.me/+abcd 5 1.7</code>"
    def describe(self, spec: LeaveTask) -> str:
        return f"{self.command} -> {spec.link}"


@dataclass(frozen=True, slots=True)
class TaskExecutionDefinition:
    task_type: str
    spec_type: type[TaskSpec]
    execute: Callable[[TaskSpec, TaskRuntimeContext], Awaitable[str]]


class TaskExecutorRegistry:
    def __init__(self) -> None:
        self._by_type: dict[str, TaskExecutionDefinition] = {}

    def register(self, definition: TaskExecutionDefinition) -> None:
        self._by_type[definition.task_type] = definition

    def for_type(self, task_type: str) -> TaskExecutionDefinition:
        return self._by_type[task_type]


task_executor_registry = TaskExecutorRegistry()
task_registry = TaskRegistry()


async def _execute_msg(spec: MsgTask, runtime: TaskRuntimeContext) -> str:
    return await MsgTaskHandler().execute(spec, runtime)


async def _execute_msgbot(spec: MsgBotTask, runtime: TaskRuntimeContext) -> str:
    return await MsgBotTaskHandler().execute(spec, runtime)


async def _execute_call(spec: CallTask, runtime: TaskRuntimeContext) -> str:
    return await CallTaskHandler().execute(spec, runtime)


async def _execute_msgchat(spec: MsgChatTask, runtime: TaskRuntimeContext) -> str:
    return await MsgChatTaskHandler().execute(spec, runtime)


async def _execute_join(spec: JoinTask, runtime: TaskRuntimeContext) -> str:
    return await JoinTaskHandler().execute(spec, runtime)


async def _execute_leave(spec: LeaveTask, runtime: TaskRuntimeContext) -> str:
    return await LeaveTaskHandler().execute(spec, runtime)


async def _execute_like(spec: LikeTask, runtime: TaskRuntimeContext) -> str:
    return await runtime.sender.react_to_post(
        link=spec.link,
        count=spec.count,
        delay=spec.delay,
        emojis=spec.emojis,
        requester_user_id=runtime.requester_user_id,
        progress_cb=runtime.progress_cb,
        task_control=runtime.task_control,
    )


async def _execute_ref(spec: RefTask, runtime: TaskRuntimeContext) -> str:
    return await runtime.sender.follow_referral(link=spec.link, count=spec.count, delay=spec.delay, requester_user_id=runtime.requester_user_id, progress_cb=runtime.progress_cb, task_control=runtime.task_control)


async def _execute_vote(spec: VoteTask, runtime: TaskRuntimeContext) -> str:
    return await runtime.sender.vote_in_poll(link=spec.link, option_index=spec.option_index, count=spec.count, delay=spec.delay, requester_user_id=runtime.requester_user_id, progress_cb=runtime.progress_cb, task_control=runtime.task_control)


for definition in (
    TaskExecutionDefinition(MSG_TASK, MsgTask, _execute_msg),
    TaskExecutionDefinition(MSGBOT_TASK, MsgBotTask, _execute_msgbot),
    TaskExecutionDefinition(CALL_TASK, CallTask, _execute_call),
    TaskExecutionDefinition(MSGCHAT_TASK, MsgChatTask, _execute_msgchat),
    TaskExecutionDefinition(JOIN_TASK, JoinTask, _execute_join),
    TaskExecutionDefinition(LEAVE_TASK, LeaveTask, _execute_leave),
    TaskExecutionDefinition(LIKE_TASK, LikeTask, _execute_like),
    TaskExecutionDefinition(REF_TASK, RefTask, _execute_ref),
    TaskExecutionDefinition(VOTE_TASK, VoteTask, _execute_vote),
):
    task_executor_registry.register(definition)


for task in (
    RegisteredTask(MSG_TASK, MsgTask, MsgTaskParser(), MsgTaskHandler(), MsgTaskPresenter()),
    RegisteredTask(MSGBOT_TASK, MsgBotTask, MsgBotTaskParser(), MsgBotTaskHandler(), MsgBotTaskPresenter()),
    RegisteredTask(CALL_TASK, CallTask, CallTaskParser(), CallTaskHandler(), CallTaskPresenter()),
    RegisteredTask(MSGCHAT_TASK, MsgChatTask, MsgChatTaskParser(), MsgChatTaskHandler(), MsgChatTaskPresenter()),
    RegisteredTask(JOIN_TASK, JoinTask, JoinTaskParser(), JoinTaskHandler(), JoinTaskPresenter()),
    RegisteredTask(LEAVE_TASK, LeaveTask, LeaveTaskParser(), LeaveTaskHandler(), LeaveTaskPresenter()),
):
    task_registry.register(task)


class TaskApplicationService:
    def parse_task_payload(self, task_type: str, payload: str, *, chat_id: int, requested_by_user_id: int) -> TaskSpec:
        registered_task = task_registry.get(task_type)
        if registered_task.parser is None:
            raise KeyError(f"Task type '{task_type}' does not provide a parser.")
        return registered_task.parser.parse(payload, context=ParseTaskContext(chat_id=chat_id, requester_user_id=requested_by_user_id))

    def presenter_for(self, task_type: str) -> TaskPresenter | None:
        registered_task = task_registry.maybe_get(task_type)
        return None if registered_task is None else registered_task.presenter

    async def enqueue_task(self, task_queue: "TaskQueue", spec: TaskSpec, *, requested_by_user_id: int):
        task_catalog.metadata_for_spec(spec)
        registered_task = task_registry.definition_for_spec(spec)
        if registered_task is not None and registered_task.handler is not None:
            registered_task.handler.validate(spec)
        return await task_queue.put(spec, requested_by_user_id=requested_by_user_id)

    async def execute_task(self, record: "TaskRecord", *, sender: TaskSenderGateway, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> str:
        runtime = TaskRuntimeContext(sender=sender, requester_user_id=record.requested_by_user_id, task_control=record.control, progress_cb=progress_cb)
        try:
            registered_task = task_registry.maybe_get(record.kind)
            if registered_task is not None and registered_task.handler is not None:
                registered_task.handler.validate(record.payload)
                return await registered_task.handler.execute(record.payload, runtime)
            return await task_executor_registry.for_type(record.kind).execute(record.payload, runtime)
        finally:
            await self._cleanup_task_artifacts(record.payload)

    async def _cleanup_task_artifacts(self, spec: TaskSpec) -> None:
        photo_path = getattr(spec, "photo_path", "")
        if not photo_path:
            return
        try:
            Path(photo_path).unlink(missing_ok=True)
        except Exception:
            return


task_application_service = TaskApplicationService()

