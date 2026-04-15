from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, TypeAlias


TaskType: TypeAlias = Literal["msg", "msgbot", "msgchat", "call", "join", "leave", "likep", "refp", "vote"]

MSG_TASK: TaskType = "msg"
MSGBOT_TASK: TaskType = "msgbot"
MSGCHAT_TASK: TaskType = "msgchat"
CALL_TASK: TaskType = "call"
JOIN_TASK: TaskType = "join"
LEAVE_TASK: TaskType = "leave"
LIKE_TASK: TaskType = "likep"
REF_TASK: TaskType = "refp"
VOTE_TASK: TaskType = "vote"


@dataclass(slots=True)
class MsgTask:
    chat_id: int
    targets: list[str]
    text: str
    accounts_count: int
    repeat_count: int
    delay: float | tuple[float, float]
    photo_path: str = ""
    hide_content: bool = False


@dataclass(slots=True)
class MsgBotTask:
    chat_id: int
    bot_username: str
    command: str
    accounts_count: int
    repeat_count: int
    delay: float | tuple[float, float]


@dataclass(slots=True)
class MsgChatTask:
    chat_id: int
    target: str
    text: str
    accounts_count: int
    repeat_count: int
    delay: float | tuple[float, float]
    photo_path: str = ""
    hide_content: bool = False


@dataclass(slots=True)
class CallTask:
    chat_id: int
    target: str
    accounts_count: int
    repeat_count: int
    delay: float | tuple[float, float]


@dataclass(slots=True)
class JoinTask:
    chat_id: int
    link: str
    count: int
    delay_cap: float | tuple[float, float]


@dataclass(slots=True)
class LeaveTask:
    chat_id: int
    link: str
    count: int
    delay_cap: float | tuple[float, float]


@dataclass(slots=True)
class LikeTask:
    chat_id: int
    link: str
    count: int
    emojis: list[str]
    delay: float | tuple[float, float] = 1.5


@dataclass(slots=True)
class RefTask:
    chat_id: int
    link: str
    count: int
    delay: float | tuple[float, float]


@dataclass(slots=True)
class VoteTask:
    chat_id: int
    link: str
    option_index: int
    count: int
    delay: float | tuple[float, float]


TaskSpec = MsgTask | MsgBotTask | MsgChatTask | CallTask | JoinTask | LeaveTask | LikeTask | RefTask | VoteTask


@dataclass(frozen=True, slots=True)
class TaskMetadata:
    task_type: str
    spec_type: type[TaskSpec]
    title_builder: Callable[[TaskSpec], str]
    accounts_count_getter: Callable[[TaskSpec], int | None]
    delay_getter: Callable[[TaskSpec], float | tuple[float, float] | None]


class TaskCatalog:
    def __init__(self) -> None:
        self._by_type: dict[str, TaskMetadata] = {}
        self._by_spec_type: dict[type[object], TaskMetadata] = {}

    def register(self, metadata: TaskMetadata) -> None:
        self._by_type[metadata.task_type] = metadata
        self._by_spec_type[metadata.spec_type] = metadata

    def metadata_for_spec(self, spec: TaskSpec) -> TaskMetadata:
        for spec_type, metadata in self._by_spec_type.items():
            if isinstance(spec, spec_type):
                return metadata
        raise KeyError(f"Task metadata is not registered for spec type: {type(spec).__name__}")

    def metadata_for_type(self, task_type: str) -> TaskMetadata:
        return self._by_type[task_type]


task_catalog = TaskCatalog()


def _msg_title(spec: MsgTask) -> str:
    target_label = ", ".join(spec.targets[:2])
    if len(spec.targets) > 2:
        target_label = f"{target_label} +{len(spec.targets) - 2}"
    return f"/msg -> {target_label}"


task_catalog.register(TaskMetadata(MSG_TASK, MsgTask, _msg_title, lambda spec: spec.accounts_count, lambda spec: spec.delay))
task_catalog.register(TaskMetadata(MSGBOT_TASK, MsgBotTask, lambda spec: f"/msgbot -> {spec.bot_username}", lambda spec: spec.accounts_count, lambda spec: spec.delay))
task_catalog.register(TaskMetadata(MSGCHAT_TASK, MsgChatTask, lambda spec: f"/msgchat -> {spec.target}", lambda spec: spec.accounts_count, lambda spec: spec.delay))
task_catalog.register(TaskMetadata(CALL_TASK, CallTask, lambda spec: f"/call -> {spec.target}", lambda spec: spec.accounts_count, lambda spec: spec.delay))
task_catalog.register(TaskMetadata(JOIN_TASK, JoinTask, lambda spec: f"/join -> {spec.link}", lambda spec: spec.count, lambda spec: spec.delay_cap))
task_catalog.register(TaskMetadata(LEAVE_TASK, LeaveTask, lambda spec: f"/leave -> {spec.link}", lambda spec: spec.count, lambda spec: spec.delay_cap))
task_catalog.register(
    TaskMetadata(
        LIKE_TASK,
        LikeTask,
        lambda spec: f"/likep -> {'/'.join(spec.emojis[:2])}{'...' if len(spec.emojis) > 2 else ''}",
        lambda spec: spec.count,
        lambda spec: spec.delay,
    )
)
task_catalog.register(TaskMetadata(REF_TASK, RefTask, lambda spec: f"/refp -> {spec.link}", lambda spec: spec.count, lambda spec: spec.delay))
task_catalog.register(TaskMetadata(VOTE_TASK, VoteTask, lambda spec: f"/vote -> пункт {spec.option_index}", lambda spec: spec.count, lambda spec: spec.delay))


def task_kind(spec: TaskSpec) -> str:
    return task_catalog.metadata_for_spec(spec).task_type


def task_title(spec: TaskSpec) -> str:
    return task_catalog.metadata_for_spec(spec).title_builder(spec)


def task_accounts_count(spec: TaskSpec) -> int | None:
    return task_catalog.metadata_for_spec(spec).accounts_count_getter(spec)


def task_delay(spec: TaskSpec) -> float | tuple[float, float] | None:
    return task_catalog.metadata_for_spec(spec).delay_getter(spec)
