from __future__ import annotations

from typing import Awaitable, Callable

from core.sender import Sender


class TelegramTaskGateway:
    def __init__(self, sender: Sender) -> None:
        self._sender = sender

    async def send_messages(self, targets: list[str], text: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], photo_path: str = "", hide_content: bool = False, *, requester_user_id: int, task_control=None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> tuple[int, int]:
        return await self._sender.send_messages(targets=targets, text=text, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay, photo_path=photo_path, hide_content=hide_content, requester_user_id=requester_user_id, task_control=task_control, progress_cb=progress_cb)

    async def send_to_bot(self, bot_username: str, command: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], *, requester_user_id: int, task_control=None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> tuple[int, int]:
        return await self._sender.send_to_bot(bot_username=bot_username, command=command, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay, requester_user_id=requester_user_id, task_control=task_control, progress_cb=progress_cb)

    async def call_user(self, target: str, accounts_count: int, repeat_count: int, delay: float | tuple[float, float], *, requester_user_id: int, task_control=None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> str:
        return await self._sender.call_user(target=target, accounts_count=accounts_count, repeat_count=repeat_count, delay=delay, requester_user_id=requester_user_id, task_control=task_control, progress_cb=progress_cb)

    async def join_chat(self, link: str, count: int = 1, delay_cap: float | tuple[float, float] = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
        return await self._sender.join_chat(link=link, count=count, delay_cap=delay_cap, requester_user_id=requester_user_id, progress_cb=progress_cb, task_control=task_control)

    async def leave_chat(self, link: str, count: int = 1, delay_cap: float | tuple[float, float] = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
        return await self._sender.leave_chat(link=link, count=count, delay_cap=delay_cap, requester_user_id=requester_user_id, progress_cb=progress_cb, task_control=task_control)

    async def react_to_post(self, link: str, count: int, delay: float | tuple[float, float], emojis: list[str], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
        return await self._sender.react_to_post(link=link, count=count, delay=delay, emojis=emojis, requester_user_id=requester_user_id, progress_cb=progress_cb, task_control=task_control)

    async def follow_referral(self, link: str, count: int, delay: float | tuple[float, float], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
        return await self._sender.follow_referral(link=link, count=count, delay=delay, requester_user_id=requester_user_id, progress_cb=progress_cb, task_control=task_control)

    async def vote_in_poll(self, link: str, option_index: int, count: int, delay: float | tuple[float, float], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
        return await self._sender.vote_in_poll(link=link, option_index=option_index, count=count, delay=delay, requester_user_id=requester_user_id, progress_cb=progress_cb, task_control=task_control)
