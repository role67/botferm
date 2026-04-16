from __future__ import annotations

import asyncio
import logging
import os
import random
import re
from collections.abc import Awaitable, Callable
from html import escape
from urllib.parse import parse_qsl, urlparse

from telethon.errors import (
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    PeerFloodError,
    RPCError,
    UserPrivacyRestrictedError,
    UserAlreadyParticipantError,
    UserNotParticipantError,
)
from telethon import events
from telethon.tl import functions, types
from telethon.tl.functions.account import UpdateNotifySettingsRequest
from telethon.tl.functions.channels import GetParticipantRequest, JoinChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    SendReactionRequest,
    SendVoteRequest,
    StartBotRequest,
)
from telethon.tl.types import MessageEntitySpoiler

from core.access_manager import AccessManager
from core.accounts import AccountManager, ManagedClient
from core.observability import audit_event
from core.queue import TaskCancelledError

logger = logging.getLogger(__name__)

DelayWindow = float | tuple[float, float]
_TELEGRAM_HOSTS = {"t.me", "telegram.me", "www.t.me", "www.telegram.me"}


def prepare_telegram_url(raw_value: str) -> str:
    raw = raw_value.strip()
    if not raw:
        return raw
    if raw.startswith("@") or raw.startswith("tg://"):
        return raw
    if re.match(r"^https?://", raw, flags=re.IGNORECASE):
        return raw
    if re.match(r"^(?:www\.)?(?:t|telegram)\.me/", raw, flags=re.IGNORECASE):
        return f"https://{raw}"
    return raw


async def describe_account(managed: ManagedClient) -> tuple[str, str]:
    fallback = managed.session_name
    try:
        if not managed.client.is_connected():
            await managed.client.connect()
        me = await asyncio.wait_for(managed.client.get_me(), timeout=5)
    except Exception:
        return fallback, fallback
    if me is None:
        return fallback, fallback
    if getattr(me, "first_name", None):
        account_name = me.first_name.strip() or fallback
    elif getattr(me, "username", None):
        account_name = f"@{me.username}"
    else:
        account_name = fallback
    if getattr(me, "phone", None):
        account_ref = str(me.phone)
    elif getattr(me, "id", None):
        account_ref = str(me.id)
    elif getattr(me, "username", None):
        account_ref = f"@{me.username}"
    else:
        account_ref = fallback
    return account_name, account_ref


async def resolve_owner_ids(access_manager: AccessManager, requester_user_id: int) -> set[int] | None:
    owner_ids = await access_manager.visible_account_owner_ids(requester_user_id)
    if owner_ids == set():
        raise PermissionError("No visible accounts available.")
    return owner_ids


def describe_message_target(link: str) -> str:
    return re.sub(r"^https?://", "", prepare_telegram_url(link), flags=re.IGNORECASE).strip("/")


def describe_join_target(link: str, invite_hash: str | None, *, extract_public_channel) -> tuple[str, str]:
    if invite_hash:
        return f"t.me/+{invite_hash}", "invite"
    try:
        channel = extract_public_channel(link).lstrip("@")
        return f"t.me/{channel}", "public"
    except ValueError:
        normalized = re.sub(r"^https?://", "", prepare_telegram_url(link), flags=re.IGNORECASE).strip("/")
        return normalized, "unknown"


def format_account_ref(account_ref: str) -> str:
    clean_ref = account_ref.strip()
    if clean_ref.isdigit():
        return f"+{clean_ref}"
    return clean_ref


def format_join_progress_message(*, index: int, total: int, account_name: str, account_ref: str, target_label: str, join_type: str, status_icon: str, status_text: str, error_text: str | None = None) -> str:
    lines = [
        f"<b>JOIN [{index}/{total}]</b>",
        f"Account: <b>{escape(account_name)}</b> ({escape(format_account_ref(account_ref))})",
        f"Target: <code>{escape(target_label)}</code>",
        f"Type: <b>{escape(join_type)}</b>",
        f"{escape(status_icon)} Status: <b>{escape(status_text)}</b>",
    ]
    if error_text:
        lines.append(f"Error: <code>{escape(error_text)}</code>")
    return "\n".join(lines)


def format_leave_progress_message(*, index: int, total: int, account_name: str, account_ref: str, target_label: str, join_type: str, status_icon: str, status_text: str, error_text: str | None = None) -> str:
    lines = [
        f"<b>LEAVE [{index}/{total}]</b>",
        f"Account: <b>{escape(account_name)}</b> ({escape(format_account_ref(account_ref))})",
        f"Target: <code>{escape(target_label)}</code>",
        f"Type: <b>{escape(join_type)}</b>",
        f"{escape(status_icon)} Status: <b>{escape(status_text)}</b>",
    ]
    if error_text:
        lines.append(f"Error: <code>{escape(error_text)}</code>")
    return "\n".join(lines)


def format_reaction_progress_message(*, index: int, total: int, account_name: str, account_ref: str, target_label: str, status_text: str, error_text: str | None = None) -> str:
    lines = [
        f"<b>REACT [{index}/{total}]</b>",
        f"Account: <b>{escape(account_name)}</b> ({escape(format_account_ref(account_ref))})",
        f"Target: <code>{escape(target_label)}</code>",
        f"<b>{escape(status_text)}</b>",
    ]
    if error_text:
        lines.append(f"Error: <code>{escape(error_text)}</code>")
    return "\n".join(lines)


def format_referral_progress_message(*, index: int, total: int, account_name: str, account_ref: str, target_label: str, status_text: str, error_text: str | None = None) -> str:
    lines = [
        f"<b>REF [{index}/{total}]</b>",
        f"Account: <b>{escape(account_name)}</b> ({escape(format_account_ref(account_ref))})",
        f"Target: <code>{escape(target_label)}</code>",
        f"<b>{escape(status_text)}</b>",
    ]
    if error_text:
        lines.append(f"Error: <code>{escape(error_text)}</code>")
    return "\n".join(lines)


def format_vote_progress_message(*, index: int, total: int, account_name: str, account_ref: str, target_label: str, option_index: int, option_label: str, status_text: str, error_text: str | None = None) -> str:
    choice_text = f"{option_index}"
    if option_label and option_label != str(option_index):
        choice_text = f"{option_index} ({option_label})"
    lines = [
        f"<b>VOTE [{index}/{total}]</b>",
        f"Account: <b>{escape(account_name)}</b> ({escape(format_account_ref(account_ref))})",
        f"Target: <code>{escape(target_label)}</code>",
        f"Option: <b>{escape(choice_text)}</b>",
        f"<b>{escape(status_text)}</b>",
    ]
    if error_text:
        lines.append(f"Error: <code>{escape(error_text)}</code>")
    return "\n".join(lines)


def format_error(exc: Exception) -> str:
    text = str(exc).strip()
    return text or exc.__class__.__name__


def normalize_user_delay(value: DelayWindow) -> DelayWindow:
    if isinstance(value, tuple):
        min_value, max_value = value
        if min_value < 0 or max_value < 0:
            raise ValueError("Delay cannot be less than 0 seconds.")
        if min_value > max_value:
            raise ValueError("Minimum delay cannot be greater than maximum delay.")
        return (float(min_value), float(max_value))
    if value < 0:
        raise ValueError("Delay cannot be less than 0 seconds.")
    return float(value)


def normalize_call_delay(value: DelayWindow, *, minimum_seconds: float = 5.0) -> DelayWindow:
    normalized = normalize_user_delay(value)
    if isinstance(normalized, tuple):
        min_value, max_value = normalized
        min_value = max(min_value, minimum_seconds)
        max_value = max(max_value, min_value)
        return (min_value, max_value)
    return max(float(normalized), minimum_seconds)


def normalize_username(value: str) -> str:
    username = prepare_telegram_url(value).strip().strip("/")
    username = re.sub(r"^https?://(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = re.sub(r"^(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = username.split("?", maxsplit=1)[0].strip("/")
    if not username.startswith("@"):
        username = f"@{username}"
    return username


def normalize_join_delay(value: DelayWindow) -> DelayWindow:
    return normalize_user_delay(value)


def sample_delay_value(value: DelayWindow) -> float:
    if isinstance(value, tuple):
        min_value, max_value = value
        if min_value == max_value:
            return float(min_value)
        return random.uniform(min_value, max_value)
    return float(value)


def extract_invite_hash(link: str) -> str | None:
    pattern = r"(?:(?:t|telegram)\.me/(?:joinchat/|\+))([A-Za-z0-9_-]+)"
    match = re.search(pattern, prepare_telegram_url(link), flags=re.IGNORECASE)
    return match.group(1) if match else None


def extract_public_channel(link: str) -> str:
    raw = str(link or "").strip()
    if raw.startswith("@"):
        return raw
    if re.fullmatch(r"-?\d{5,20}", raw):
        return raw
    parsed = urlparse(prepare_telegram_url(raw))
    if parsed.netloc not in _TELEGRAM_HOSTS:
        raise ValueError("Unsupported Telegram link.")
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError("Invalid Telegram link format.")
    if parts[0] in {"joinchat", "+"}:
        raise ValueError("Use invite link format with invite hash.")
    return parts[0]


def parse_referral_link(link: str) -> tuple[str, str]:
    raw_link = link.strip()
    if not raw_link:
        raise ValueError("Telegram link cannot be empty.")
    parsed = urlparse(prepare_telegram_url(raw_link))
    if parsed.scheme == "tg" and parsed.netloc == "resolve":
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        domain = (query.get("domain") or "").strip()
        start_param = (query.get("start") or "").strip()
        if not domain or not start_param:
            raise ValueError("Referral link must include bot username and start parameter.")
        return normalize_username(domain), start_param
    if parsed.netloc not in _TELEGRAM_HOSTS:
        raise ValueError("Use https://t.me/<bot>?start=... or tg://resolve?domain=<bot>&start=...")
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError("Invalid Telegram link format.")
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    start_param = (query.get("start") or "").strip()
    if not start_param:
        raise ValueError("Referral link must include start parameter.")
    return normalize_username(parts[0]), start_param


def parse_message_link(link: str) -> tuple[str | int, int]:
    parsed = urlparse(prepare_telegram_url(link))
    if parsed.netloc not in _TELEGRAM_HOSTS:
        raise ValueError("Unsupported message link.")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        raise ValueError("Invalid message link format.")
    if parts[0] == "c":
        if len(parts) < 3:
            raise ValueError("Invalid private message link format.")
        peer_id = int(f"-100{parts[1]}")
        msg_id = int(parts[2])
        return peer_id, msg_id
    return parts[0], int(parts[1])


def parse_public_message_link(link: str) -> tuple[str, int]:
    parsed = urlparse(prepare_telegram_url(link))
    if parsed.netloc not in _TELEGRAM_HOSTS:
        raise ValueError("A public poll message link is required.")
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        raise ValueError("Invalid public message link format.")
    if parts[0] == "c":
        raise ValueError("For /vote use a public link like https://t.me/channel/123.")
    try:
        msg_id = int(parts[1])
    except ValueError as exc:
        raise ValueError("Invalid message ID in poll link.") from exc
    return parts[0], msg_id


def extract_dc_id(managed: ManagedClient) -> int | None:
    try:
        return getattr(managed.client.session, "dc_id", None)
    except Exception:
        return None


def normalize_reaction_token(value: str) -> str:
    token = (value or "").strip()
    if token.lower().startswith("custom:"):
        suffix = token.split(":", maxsplit=1)[1].strip()
        return f"custom:{suffix}"
    return token


def format_allowed_reactions(allowed: set[str], *, limit: int = 12) -> str:
    if not allowed:
        return "-"
    ordered = sorted(allowed)
    if len(ordered) > limit:
        shown = ", ".join(ordered[:limit])
        return f"{shown}, ... (+{len(ordered) - limit})"
    return ", ".join(ordered)


def is_reaction_invalid_error(exc: RPCError) -> bool:
    text = str(exc).lower()
    return "reactioninvalid" in text or "invalid reaction" in text


async def load_allowed_reactions(client, peer_ref: str, msg_id: int) -> set[str]:
    try:
        message = await client.get_messages(peer_ref, ids=msg_id)
    except Exception:
        return set()
    reactions_info = getattr(message, "reactions", None)
    results = getattr(reactions_info, "results", None) or []
    allowed: set[str] = set()
    for item in results:
        reaction = getattr(item, "reaction", None)
        if isinstance(reaction, types.ReactionEmoji):
            emoticon = (getattr(reaction, "emoticon", "") or "").strip()
            if emoticon:
                allowed.add(emoticon)
        elif isinstance(reaction, types.ReactionCustomEmoji):
            document_id = getattr(reaction, "document_id", None)
            if document_id is not None:
                allowed.add(f"custom:{int(document_id)}")
    return allowed

class Sender:
    def __init__(
        self,
        account_manager: AccountManager,
        access_manager: AccessManager,
        min_delay_seconds: int,
        max_count: int,
        max_retries: int,
    ) -> None:
        self.account_manager = account_manager
        self.access_manager = access_manager
        self.min_delay_seconds = min_delay_seconds
        self.max_count = max_count
        self.max_retries = max_retries

async def send_messages(
    self,
    targets: list[str],
    text: str,
    accounts_count: int,
    repeat_count: int,
    delay: float,
    photo_path: str = "",
    hide_content: bool = False,
    *,
    requester_user_id: int,
    task_control=None,
    progress_cb: Callable[[str], Awaitable[None]] | None = None,
) -> tuple[int, int]:
    normalized_delay = await self._validated_delay_window(requester_user_id, delay)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    if accounts_count < 1:
        raise ValueError("Account count must be an integer greater than 0.")
    if repeat_count < 1 or repeat_count > 100:
        raise ValueError("Repeat count must be an integer from 1 to 100.")
    audit_event("sender.messages_started", message="Bulk message sending started", targets=targets, accounts_count=accounts_count, repeat_count=repeat_count, delay=normalized_delay, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    success = 0
    failed = 0
    used_accounts = min(accounts_count, len(managed_clients))
    total_steps = used_accounts * repeat_count * len(targets)
    current_step = 0
    permanent_failures: set[tuple[str, str]] = set()
    step_specs: list[tuple[str, int]] = []
    for repeat_index in range(1, repeat_count + 1):
        shuffled_targets = list(targets)
        random.shuffle(shuffled_targets)
        for target in shuffled_targets:
            step_specs.append((target, repeat_index))
    account_batches = self._build_balanced_account_batches(
        managed_clients=managed_clients,
        batch_size=used_accounts,
        batch_count=len(step_specs),
    )
    raw_steps: list[tuple[ManagedClient, str, int]] = []
    for (target, repeat_index), batch in zip(step_specs, account_batches):
        for managed in batch:
            raw_steps.append((managed, target, repeat_index))
    for managed, target, repeat_index in raw_steps:
        await self._checkpoint(task_control)
        current_step += 1
        failure_key = (managed.session_name, target)
        if failure_key in permanent_failures:
            status_text = "SKIP after permanent failure"
            if progress_cb is not None:
                await progress_cb(f"MSG [{current_step}/{total_steps}] {target} | repeat {repeat_index}/{repeat_count} - {status_text}")
            continue
        status_text = "OK sent"
        try:
            await self._run_with_retry_on_client(
                client=managed.client,
                operation_name=f"send message to {target}",
                coro_factory=lambda current_client, t=target, p=text, media=photo_path, hidden=hide_content: _send_message_payload(current_client, t, p, media, hidden),
                managed=managed,
                task_control=task_control,
            )
            success += 1
        except TaskCancelledError:
            raise
        except PeerFloodError as exc:
            failed += 1
            status_text = f"WARN {format_error(exc)}"
            logger.warning("Send blocked by PeerFlood for %s: %s", target, exc)
        except RPCError as exc:
            failed += 1
            if self._is_non_retryable_rpc_error(exc):
                permanent_failures.add(failure_key)
            status_text = f"ERR {format_error(exc)}"
            logger.exception("Failed to send message to %s", target)
        except Exception:
            failed += 1
            status_text = "ERR not sent"
            logger.exception("Failed to send message to %s", target)
        if progress_cb is not None:
            await progress_cb(f"MSG [{current_step}/{total_steps}] {target} | repeat {repeat_index}/{repeat_count} - {status_text}")
        if current_step < total_steps:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))
    audit_event("sender.messages_finished", message="Bulk message sending finished", targets=targets, used_accounts=used_accounts, repeat_count=repeat_count, success=success, failed=failed, requester_user_id=requester_user_id)
    return success, failed


async def _send_message_payload(current_client, target: str, text: str, photo_path: str, hide_content: bool):
    resolved_target = await _resolve_message_target(current_client, target)
    await _mute_dialog_notifications(current_client, resolved_target)
    if photo_path:
        # For media messages, -h should hide media via Telegram's media spoiler.
        # Caption text remains plain unless it is a text-only message.
        parsed_text = (text or "").strip()
        if hide_content:
            result = await _send_photo_with_spoiler(current_client, resolved_target, photo_path, parsed_text)
            await _mute_dialog_notifications(current_client, resolved_target)
            return result
        send_kwargs = {"caption": parsed_text or None}
        result = await current_client.send_file(resolved_target, photo_path, **send_kwargs)
        await _mute_dialog_notifications(current_client, resolved_target)
        return result
    parsed_text, entities = _prepare_hidden_text(text, hide_content)
    if parsed_text:
        if entities:
            result = await current_client.send_message(resolved_target, parsed_text, formatting_entities=entities)
        else:
            result = await current_client.send_message(resolved_target, parsed_text)
        await _mute_dialog_notifications(current_client, resolved_target)
        return result
    raise ValueError("Text or photo is required for sending.")


async def _resolve_message_target(current_client, target: str):
    invite_hash = extract_invite_hash(str(target))
    if not invite_hash:
        return target
    invite_info = await current_client(functions.messages.CheckChatInviteRequest(hash=invite_hash))
    if isinstance(invite_info, types.ChatInviteAlready):
        return invite_info.chat
    raise ValueError("Для отправки по приватной ссылке аккаунт должен быть уже в этом чате.")


async def _mute_dialog_notifications(current_client, target) -> None:
    peer = await current_client.get_input_entity(target)
    # Keep notifications disabled as long as Telegram accepts large mute_until values.
    settings = types.InputPeerNotifySettings(
        show_previews=False,
        silent=True,
        mute_until=2_147_483_647,
    )
    await current_client(
        UpdateNotifySettingsRequest(
            peer=types.InputNotifyPeer(peer=peer),
            settings=settings,
        )
    )


async def _mute_join_target_notifications(current_client, *, link: str, invite_hash: str | None) -> None:
    try:
        if invite_hash:
            invite_info = await current_client(functions.messages.CheckChatInviteRequest(hash=invite_hash))
            if isinstance(invite_info, types.ChatInviteAlready):
                await _mute_dialog_notifications(current_client, invite_info.chat)
            return
        channel = extract_public_channel(link)
        await _mute_dialog_notifications(current_client, channel)
    except Exception:
        logger.warning("Failed to mute notifications after join for %s", link, exc_info=True)


def _prepare_hidden_text(text: str, hide_content: bool) -> tuple[str, list[MessageEntitySpoiler]]:
    cleaned = (text or "").strip()
    if not cleaned:
        return "", []
    if hide_content:
        return cleaned, [MessageEntitySpoiler(offset=0, length=len(cleaned))]
    return cleaned, []


async def _send_photo_with_spoiler(current_client, target: str, photo_path: str, caption: str):
    uploaded = await current_client.upload_file(photo_path)
    media = types.InputMediaUploadedPhoto(
        file=uploaded,
        spoiler=True,
    )
    return await current_client(
        functions.messages.SendMediaRequest(
            peer=target,
            media=media,
            message=caption or "",
            random_id=random.randrange(1, 2**63),
        )
    )


def _extract_phone_call(update_result) -> object | None:
    if update_result is None:
        return None
    maybe_call = getattr(update_result, "phone_call", None)
    return maybe_call if maybe_call is not None else update_result


async def place_call_and_hangup(
    self,
    current_client,
    target: str,
    *,
    ring_timeout: float = 30.0,
    phase_cb: Callable[[str], Awaitable[None]] | None = None,
) -> bool:
    peer = await current_client.get_input_entity(target)
    if phase_cb is not None:
        await phase_cb("dialing")
    protocol = types.PhoneCallProtocol(
        min_layer=92,
        max_layer=166,
        library_versions=["telethon"],
        udp_p2p=True,
        udp_reflector=True,
    )
    result = await current_client(
        functions.phone.RequestCallRequest(
            user_id=peer,
            g_a_hash=os.urandom(32),
            protocol=protocol,
            video=False,
            # RequestCallRequest expects int32 random_id in Telethon.
            random_id=random.randint(0, 2**31 - 1),
        )
    )
    phone_call = _extract_phone_call(result)
    if phone_call is None or getattr(phone_call, "id", None) is None or getattr(phone_call, "access_hash", None) is None:
        raise RuntimeError("Call request did not return call identifiers.")
    call_id = int(phone_call.id)
    input_call = types.InputPhoneCall(id=call_id, access_hash=int(phone_call.access_hash))

    answered_event = asyncio.Event()
    answered = isinstance(phone_call, types.PhoneCallAccepted)
    accepted_reported = False
    if answered and phase_cb is not None:
        accepted_reported = True
        await phase_cb("answered")
    elif phase_cb is not None:
        await phase_cb("ringing")

    async def _on_raw(event: events.Raw) -> None:
        nonlocal answered, accepted_reported
        update = getattr(event, "update", event)
        if not isinstance(update, types.UpdatePhoneCall):
            return
        updated_call = update.phone_call
        if getattr(updated_call, "id", None) != call_id:
            return
        if isinstance(updated_call, types.PhoneCallAccepted):
            answered = True
            if phase_cb is not None and not accepted_reported:
                accepted_reported = True
                await phase_cb("answered")
            answered_event.set()
            return
        if isinstance(updated_call, types.PhoneCallDiscarded):
            if phase_cb is not None and not answered:
                await phase_cb("not answered")
            answered_event.set()

    current_client.add_event_handler(_on_raw, events.Raw)
    started_at = asyncio.get_running_loop().time()
    try:
        if not answered:
            try:
                await asyncio.wait_for(answered_event.wait(), timeout=max(float(ring_timeout), 1.0))
            except asyncio.TimeoutError:
                if phase_cb is not None:
                    await phase_cb("no answer (timeout)")
                pass
    finally:
        if phase_cb is not None:
            await phase_cb("hanging up")
        current_client.remove_event_handler(_on_raw, events.Raw)
        duration = max(0, int(asyncio.get_running_loop().time() - started_at))
        try:
            await current_client(
                functions.phone.DiscardCallRequest(
                    peer=input_call,
                    duration=duration,
                    reason=types.PhoneCallDiscardReasonHangup(),
                    connection_id=0,
                    video=False,
                )
            )
        except Exception:
            # The call may already be closed by peer or server.
            pass
    return answered


async def send_to_bot(self, bot_username: str, command: str, accounts_count: int, repeat_count: int, delay: float, *, requester_user_id: int, task_control=None, progress_cb: Callable[[str], Awaitable[None]] | None = None) -> tuple[int, int]:
    normalized_delay = await self._validated_delay_window(requester_user_id, delay)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    if accounts_count < 1:
        raise ValueError("Account count must be an integer greater than 0.")
    if repeat_count < 1 or repeat_count > 100:
        raise ValueError("Repeat count must be an integer from 1 to 100.")
    audit_event("sender.bot_messages_started", message="Bot messaging started", bot_username=bot_username, command=command, accounts_count=accounts_count, repeat_count=repeat_count, delay=normalized_delay, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    username = normalize_username(bot_username)
    managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    success = 0
    failed = 0
    used_accounts = min(accounts_count, len(managed_clients))
    selected_accounts = self._shuffle_managed_clients(managed_clients)[:used_accounts]
    total_steps = used_accounts * repeat_count
    progress_lock = asyncio.Lock()
    progress_step = 0

    async def _next_step() -> int:
        nonlocal progress_step
        async with progress_lock:
            progress_step += 1
            return progress_step

    async def _send_for_account(managed: ManagedClient) -> tuple[int, int]:
        account_success = 0
        account_failed = 0
        permanent_failure = False
        for repeat_index in range(1, repeat_count + 1):
            await self._checkpoint(task_control)
            step = await _next_step()
            if permanent_failure:
                if progress_cb is not None:
                    await progress_cb(
                        f"MSGBOT [{step}/{total_steps}] {username} | repeat {repeat_index}/{repeat_count} | {managed.session_name} - SKIP after permanent failure"
                    )
                continue
            status_text = "OK sent"
            try:
                await self._run_with_retry_on_client(
                    client=managed.client,
                    operation_name=f"send to bot {username}",
                    coro_factory=lambda current_client, u=username, c=command: _send_message_payload(current_client, u, c, "", False),
                    managed=managed,
                    task_control=task_control,
                )
                account_success += 1
            except TaskCancelledError:
                raise
            except PeerFloodError as exc:
                account_failed += 1
                status_text = f"WARN {format_error(exc)}"
                logger.warning("Command send blocked by PeerFlood for %s: %s", username, exc)
            except RPCError as exc:
                account_failed += 1
                if self._is_non_retryable_rpc_error(exc):
                    permanent_failure = True
                status_text = f"ERR {format_error(exc)}"
                logger.exception("Failed to send command to bot %s", username)
            except Exception:
                account_failed += 1
                status_text = "ERR not sent"
                logger.exception("Failed to send command to bot %s", username)
            if progress_cb is not None:
                await progress_cb(
                    f"MSGBOT [{step}/{total_steps}] {username} | repeat {repeat_index}/{repeat_count} | {managed.session_name} - {status_text}"
                )
            if repeat_index < repeat_count:
                await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))
        return account_success, account_failed

    results = await asyncio.gather(*[_send_for_account(managed) for managed in selected_accounts])
    for account_success, account_failed in results:
        success += account_success
        failed += account_failed
    audit_event("sender.bot_messages_finished", message="Bot messaging finished", bot_username=username, used_accounts=used_accounts, repeat_count=repeat_count, success=success, failed=failed, requester_user_id=requester_user_id)
    return success, failed


async def call_user(
    self,
    target: str,
    accounts_count: int,
    repeat_count: int,
    delay: float | tuple[float, float],
    *,
    requester_user_id: int,
    task_control=None,
    progress_cb: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    normalized_delay = normalize_call_delay(await self._validated_delay_window(requester_user_id, delay))
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    if accounts_count < 1:
        raise ValueError("Account count must be an integer greater than 0.")
    if repeat_count < 1 or repeat_count > 100:
        raise ValueError("Repeat count must be an integer from 1 to 100.")

    username = normalize_username(target)
    audit_event(
        "sender.call_started",
        message="Call task started",
        target=username,
        accounts_count=accounts_count,
        repeat_count=repeat_count,
        delay=normalized_delay,
        requester_user_id=requester_user_id,
        owner_ids=sorted(owner_ids) if owner_ids is not None else None,
    )
    managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    used_accounts = min(accounts_count, len(managed_clients))
    if used_accounts < 1:
        return "No available accounts for /call."

    total_steps = used_accounts * repeat_count
    current_step = 0
    success = 0
    answered = 0
    failed = 0
    account_batches = self._build_balanced_account_batches(
        managed_clients=managed_clients,
        batch_size=used_accounts,
        batch_count=repeat_count,
    )
    raw_steps: list[tuple[ManagedClient, int]] = []
    for repeat_index, batch in enumerate(account_batches, start=1):
        for managed in batch:
            raw_steps.append((managed, repeat_index))

    for managed, repeat_index in raw_steps:
        await self._checkpoint(task_control)
        current_step += 1
        account_name, account_ref = await describe_account(managed)
        account_label = f"{account_name} ({format_account_ref(account_ref)})"
        progress_prefix = (
            f"CALL [{current_step}/{total_steps}] {username} | repeat {repeat_index}/{repeat_count} | account {account_label}"
        )
        async def _report_phase(phase: str) -> None:
            if progress_cb is not None:
                await progress_cb(f"{progress_prefix} - {phase}")

        status_text = "OK ring finished"
        try:
            accepted = await self._run_with_retry_on_client(
                client=managed.client,
                operation_name=f"call {username}",
                coro_factory=lambda current_client, t=username: self._place_call_and_hangup(
                    current_client,
                    t,
                    phase_cb=_report_phase,
                ),
                managed=managed,
                task_control=task_control,
            )
            success += 1
            if accepted:
                answered += 1
                status_text = "OK answered and hung up"
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except PeerFloodError as exc:
            failed += 1
            status_text = f"WARN {format_error(exc)}"
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.warning("Call blocked by PeerFlood for target %s: %s", username, exc)
        except RPCError as exc:
            failed += 1
            error_text = format_error(exc)
            if self._is_call_target_unavailable_error(exc):
                status_text = f"STOP target unavailable: {error_text}"
                if progress_cb is not None:
                    await progress_cb(
                        f"{progress_prefix} - {status_text}"
                    )
                audit_event(
                    "sender.call_target_unavailable",
                    level=logging.WARNING,
                    message="Call target unavailable, stopping task",
                    target=username,
                    error=error_text,
                    requester_user_id=requester_user_id,
                )
                return (
                    "ERR /call stopped.\n"
                    f"Target: {username}\n"
                    f"Reason: {error_text}\n"
                    "Calls to this user are unavailable (privacy/settings/restriction)."
                )
            status_text = f"ERR {error_text}"
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Call failed for target %s", username)
        except Exception as exc:
            failed += 1
            status_text = f"ERR {format_error(exc)}"
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Call failed for target %s", username)
        if progress_cb is not None:
            await progress_cb(
                f"{progress_prefix} - {status_text}"
            )
        if current_step < total_steps:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))

    audit_event(
        "sender.call_finished",
        message="Call task finished",
        target=username,
        used_accounts=used_accounts,
        repeat_count=repeat_count,
        success=success,
        answered=answered,
        failed=failed,
        requester_user_id=requester_user_id,
    )
    return (
        "OK /call finished.\n"
        f"Target: {username}\n"
        f"Requested accounts: {accounts_count}\n"
        f"Used accounts: {used_accounts}\n"
        f"Cycles: {repeat_count}\n"
        f"Calls sent: {success}\n"
        f"Answered and hung up: {answered}\n"
        f"Errors: {failed}"
    )


async def join_chat(self, link: str, count: int = 1, delay_cap: float = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
    normalized_delay_cap = await self._validated_delay_window(requester_user_id, normalize_join_delay(delay_cap))
    invite_hash = extract_invite_hash(link)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    audit_event("sender.join_started", message="Join operation started", link=link, count=count, delay_cap=normalized_delay_cap, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    if count < 1:
        return "N for /join must be an integer greater than 0."
    requested_count = count
    target_label, join_type = describe_join_target(link=link, invite_hash=invite_hash, extract_public_channel=extract_public_channel)
    try:
        managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    except Exception as exc:
        logger.exception("Failed to prepare clients for join")
        return f"Failed to prepare accounts for join: {exc}"

    joined = 0
    already_joined = 0
    failed = 0
    attempted_accounts = 0
    total_available = len(managed_clients)
    for managed in self._shuffle_managed_clients(managed_clients):
        await self._checkpoint(task_control)
        if joined >= requested_count:
            break
        attempted_accounts += 1
        account_name, account_ref = await describe_account(managed)
        progress_index = min(joined + 1, requested_count)
        status_icon = "OK"
        status_text = "success"
        error_text: str | None = None
        should_report = True
        try:
            if await self._is_already_joined(client=managed.client, link=link, invite_hash=invite_hash, managed=managed, task_control=task_control):
                already_joined += 1
                should_report = False
                await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
                await _mute_join_target_notifications(managed.client, link=link, invite_hash=invite_hash)
                if joined < requested_count and attempted_accounts < total_available:
                    await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay_cap))
                continue
            await self._join_with_retry(client=managed.client, link=link, invite_hash=invite_hash, managed=managed, task_control=task_control)
            await _mute_join_target_notifications(managed.client, link=link, invite_hash=invite_hash)
            joined += 1
            progress_index = joined
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except UserAlreadyParticipantError:
            already_joined += 1
            should_report = False
            await _mute_join_target_notifications(managed.client, link=link, invite_hash=invite_hash)
        except (InviteHashInvalidError, InviteHashExpiredError):
            status_icon = "ERR"
            status_text = "invite invalid"
            if progress_cb is not None:
                await progress_cb(format_join_progress_message(index=progress_index, total=requested_count, account_name=account_name, account_ref=account_ref, target_label=target_label, join_type=join_type, status_icon=status_icon, status_text=status_text))
            audit_event("sender.join_invalid_invite", level=logging.WARNING, message="Join failed because invite is invalid", link=link, requester_user_id=requester_user_id)
            return "Invite link is invalid or expired."
        except Exception as exc:
            failed += 1
            status_icon = "ERR"
            status_text = "error"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Join failed for session %s", managed.session_name)
        if should_report and progress_cb is not None:
            await progress_cb(format_join_progress_message(index=progress_index, total=requested_count, account_name=account_name, account_ref=account_ref, target_label=target_label, join_type=join_type, status_icon=status_icon, status_text=status_text, error_text=error_text))
        if joined < requested_count and attempted_accounts < total_available:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay_cap))

    result_text = (
        "JOIN completed.\n"
        f"Link: {link}\n"
        f"Requested new joins: {requested_count}\n"
        f"Accounts checked: {attempted_accounts}\n"
        f"New joins: {joined}\n"
        f"Skipped (already inside): {already_joined}\n"
        f"Errors: {failed}"
    )
    audit_event("sender.join_finished", message="Join operation finished", link=link, requested_count=requested_count, attempted_accounts=attempted_accounts, joined=joined, already_joined=already_joined, failed=failed, requester_user_id=requester_user_id)
    return result_text


async def react_to_post(self, link: str, count: int, delay: DelayWindow, emojis: list[str], progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
    if count < 1:
        return "N for /likep must be an integer greater than 0."
    if not emojis:
        return "Нужно указать хотя бы один emoji для /likep."
    normalized_delay = await self._validated_delay_window(requester_user_id, delay)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    reaction_pool = [normalize_reaction_token(item) for item in emojis if (item or "").strip()]
    if not reaction_pool:
        return "Не удалось распознать emoji для /likep."
    audit_event("sender.reactions_started", message="Reaction sending started", link=link, count=count, delay=normalized_delay, emojis=reaction_pool, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    try:
        peer_ref, msg_id = parse_message_link(link)
    except ValueError as exc:
        return f"ERR {exc}"
    try:
        managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    except Exception as exc:
        logger.exception("Failed to prepare clients for reaction")
        return f"Failed to prepare accounts for reaction: {exc}"

    success = 0
    failed = 0
    used_accounts = min(count, len(managed_clients))
    selected_batches = self._build_balanced_account_batches(managed_clients=managed_clients, batch_size=used_accounts, batch_count=1)
    selected_accounts = selected_batches[0] if selected_batches else []
    usable_pool = list(reaction_pool)
    target_label = describe_message_target(link)
    for index, managed in enumerate(selected_accounts, start=1):
        await self._checkpoint(task_control)
        account_name, account_ref = await describe_account(managed)
        chosen_reaction = random.choice(usable_pool)
        status_text = f"{chosen_reaction} reaction sent"
        error_text: str | None = None
        try:
            await self._run_with_retry_on_client(client=managed.client, operation_name=f"send reaction from {managed.session_name}", coro_factory=lambda current_client, p=peer_ref, m=msg_id, e=chosen_reaction: self._send_reaction(current_client, p, m, e), managed=managed, task_control=task_control)
            success += 1
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except RPCError as exc:
            failed += 1
            if is_reaction_invalid_error(exc):
                allowed_reactions = await load_allowed_reactions(managed.client, peer_ref, msg_id)
                return (
                    "ERR /likep остановлен.\n"
                    f"Запрошенная реакция: {chosen_reaction}\n"
                    "Telegram отклонил эту реакцию для целевого поста.\n"
                    f"Доступные реакции: {format_allowed_reactions(allowed_reactions)}"
                )
            status_text = "ERR reaction not sent"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Reaction failed for session %s", managed.session_name)
        except Exception as exc:
            failed += 1
            status_text = "ERR reaction not sent"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Reaction failed for session %s", managed.session_name)
        if progress_cb is not None:
            await progress_cb(format_reaction_progress_message(index=index, total=used_accounts, account_name=account_name, account_ref=account_ref, target_label=target_label, status_text=status_text, error_text=error_text))
        if index < used_accounts:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))

    result_text = (
        "OK /likep finished.\n"
        f"Link: {link}\n"
        f"Reaction pool: {', '.join(reaction_pool)}\n"
        f"Requested accounts: {count}\n"
        f"Delay: {normalized_delay}\n"
        f"Used accounts: {used_accounts}\n"
        f"Reactions sent: {success}\n"
        f"Errors: {failed}"
    )
    audit_event("sender.reactions_finished", message="Reaction sending finished", link=link, count=count, delay=normalized_delay, emojis=reaction_pool, used_accounts=used_accounts, success=success, failed=failed, requester_user_id=requester_user_id)
    return result_text


async def leave_chat(self, link: str, count: int = 1, delay_cap: float = 1.5, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
    normalized_delay_cap = await self._validated_delay_window(requester_user_id, normalize_join_delay(delay_cap))
    invite_hash = extract_invite_hash(link)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    audit_event("sender.leave_started", message="Leave operation started", link=link, count=count, delay_cap=normalized_delay_cap, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    if count < 1:
        return "N for /leave must be an integer greater than 0."
    requested_count = count
    target_label, join_type = describe_join_target(link=link, invite_hash=invite_hash, extract_public_channel=extract_public_channel)
    try:
        managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    except Exception as exc:
        logger.exception("Failed to prepare clients for leave")
        return f"Failed to prepare accounts for leave: {exc}"

    left = 0
    already_left = 0
    failed = 0
    attempted_accounts = 0
    total_available = len(managed_clients)
    for managed in self._shuffle_managed_clients(managed_clients):
        await self._checkpoint(task_control)
        if left >= requested_count:
            break
        attempted_accounts += 1
        account_name, account_ref = await describe_account(managed)
        progress_index = min(left + 1, requested_count)
        status_icon = "OK"
        status_text = "success"
        error_text: str | None = None
        should_report = True
        try:
            if not await self._is_already_joined(client=managed.client, link=link, invite_hash=invite_hash, managed=managed, task_control=task_control):
                already_left += 1
                should_report = False
                await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
                if left < requested_count and attempted_accounts < total_available:
                    await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay_cap))
                continue
            await self._leave_with_retry(client=managed.client, link=link, invite_hash=invite_hash, managed=managed, task_control=task_control)
            left += 1
            progress_index = left
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except (InviteHashInvalidError, InviteHashExpiredError):
            status_icon = "ERR"
            status_text = "invite invalid"
            if progress_cb is not None:
                await progress_cb(format_leave_progress_message(index=progress_index, total=requested_count, account_name=account_name, account_ref=account_ref, target_label=target_label, join_type=join_type, status_icon=status_icon, status_text=status_text))
            audit_event("sender.leave_invalid_invite", level=logging.WARNING, message="Leave failed because invite is invalid", link=link, requester_user_id=requester_user_id)
            return "Invite link is invalid or expired."
        except Exception as exc:
            failed += 1
            status_icon = "ERR"
            status_text = "error"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Leave failed for session %s", managed.session_name)
        if should_report and progress_cb is not None:
            await progress_cb(format_leave_progress_message(index=progress_index, total=requested_count, account_name=account_name, account_ref=account_ref, target_label=target_label, join_type=join_type, status_icon=status_icon, status_text=status_text, error_text=error_text))
        if left < requested_count and attempted_accounts < total_available:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay_cap))

    result_text = (
        "LEAVE completed.\n"
        f"Link: {link}\n"
        f"Requested leaves: {requested_count}\n"
        f"Accounts checked: {attempted_accounts}\n"
        f"New leaves: {left}\n"
        f"Skipped (already outside): {already_left}\n"
        f"Errors: {failed}"
    )
    audit_event("sender.leave_finished", message="Leave operation finished", link=link, requested_count=requested_count, attempted_accounts=attempted_accounts, left=left, already_left=already_left, failed=failed, requester_user_id=requester_user_id)
    return result_text


async def follow_referral(self, link: str, count: int, delay: float, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
    if count < 1:
        return "N for /refp must be an integer greater than 0."
    normalized_delay = await self._validated_delay_window(requester_user_id, delay)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    try:
        bot_username, start_param = parse_referral_link(link)
    except ValueError as exc:
        return f"ERR {exc}"
    audit_event("sender.referral_started", message="Referral start started", link=link, bot_username=bot_username, count=count, delay=normalized_delay, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    try:
        managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    except Exception as exc:
        logger.exception("Failed to prepare clients for referral start")
        return f"Failed to prepare accounts for referral start: {exc}"

    success = 0
    failed = 0
    used_accounts = min(count, len(managed_clients))
    selected_batches = self._build_balanced_account_batches(managed_clients=managed_clients, batch_size=used_accounts, batch_count=1)
    selected_accounts = selected_batches[0] if selected_batches else []
    target_label = f"{bot_username}?start={start_param}"
    for index, managed in enumerate(selected_accounts, start=1):
        await self._checkpoint(task_control)
        account_name, account_ref = await describe_account(managed)
        status_text = "OK referral opened"
        error_text: str | None = None
        try:
            await self._run_with_retry_on_client(client=managed.client, operation_name=f"start bot referral {bot_username}", coro_factory=lambda current_client, b=bot_username, p=start_param: current_client(StartBotRequest(bot=b, peer=b, start_param=p, random_id=random.randrange(1, 2**63))), managed=managed, task_control=task_control)
            success += 1
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except Exception as exc:
            failed += 1
            status_text = "ERR referral not opened"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Referral start failed for session %s", managed.session_name)
        if progress_cb is not None:
            await progress_cb(format_referral_progress_message(index=index, total=used_accounts, account_name=account_name, account_ref=account_ref, target_label=target_label, status_text=status_text, error_text=error_text))
        if index < used_accounts:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))

    result_text = (
        "OK /refp finished.\n"
        f"Link: {link}\n"
        f"Bot: {bot_username}\n"
        f"Start param: {start_param}\n"
        f"Requested accounts: {count}\n"
        f"Used accounts: {used_accounts}\n"
        f"Successful referrals: {success}\n"
        f"Errors: {failed}"
    )
    audit_event("sender.referral_finished", message="Referral start finished", link=link, bot_username=bot_username, start_param=start_param, count=count, used_accounts=used_accounts, success=success, failed=failed, requester_user_id=requester_user_id)
    return result_text


async def vote_in_poll(self, link: str, option_index: int, count: int, delay: float, progress_cb: Callable[[str], Awaitable[None]] | None = None, *, requester_user_id: int, task_control=None) -> str:
    if count < 1:
        return "N for /vote must be an integer greater than 0."
    if option_index < 1 or option_index > 12:
        return "Option index for /vote must be between 1 and 12."
    normalized_delay = await self._validated_delay_window(requester_user_id, delay)
    owner_ids = await resolve_owner_ids(self.access_manager, requester_user_id)
    try:
        peer_ref, msg_id = parse_public_message_link(link)
    except ValueError as exc:
        return f"ERR {exc}"
    audit_event("sender.vote_started", message="Poll voting started", link=link, option_index=option_index, count=count, delay=normalized_delay, requester_user_id=requester_user_id, owner_ids=sorted(owner_ids) if owner_ids is not None else None)
    try:
        managed_clients = await self.account_manager.get_authorized_clients(owner_ids=owner_ids)
    except Exception as exc:
        logger.exception("Failed to prepare clients for voting")
        return f"Failed to prepare accounts for voting: {exc}"

    used_accounts = min(count, len(managed_clients))
    selected_batches = self._build_balanced_account_batches(managed_clients=managed_clients, batch_size=used_accounts, batch_count=1)
    selected_accounts = selected_batches[0] if selected_batches else []
    success = 0
    failed = 0
    invalid_poll = False
    option_label = str(option_index)
    target_label = describe_message_target(link)
    for index, managed in enumerate(selected_accounts, start=1):
        await self._checkpoint(task_control)
        account_name, account_ref = await describe_account(managed)
        status_text = f"OK vote sent for option {option_index}"
        error_text: str | None = None
        try:
            option_bytes, option_label = await self._load_poll_option(managed.client, peer_ref, msg_id, option_index)
            await self._run_with_retry_on_client(client=managed.client, operation_name=f"vote in poll from {managed.session_name}", coro_factory=lambda current_client, p=peer_ref, m=msg_id, o=option_bytes: self._send_vote(current_client, p, m, o), managed=managed, task_control=task_control)
            success += 1
            await self.account_manager.mark_alive(managed.session_name, dc_id=extract_dc_id(managed))
        except TaskCancelledError:
            raise
        except ValueError as exc:
            failed += 1
            invalid_poll = True
            status_text = "ERR voting unavailable"
            error_text = str(exc)
        except Exception as exc:
            failed += 1
            status_text = "ERR vote not sent"
            error_text = format_error(exc)
            await self.account_manager.apply_runtime_health(managed.session_name, exc, dc_id=extract_dc_id(managed))
            logger.exception("Poll vote failed for session %s", managed.session_name)
        if progress_cb is not None:
            await progress_cb(format_vote_progress_message(index=index, total=used_accounts, account_name=account_name, account_ref=account_ref, target_label=target_label, option_index=option_index, option_label=option_label, status_text=status_text, error_text=error_text))
        if invalid_poll:
            break
        if index < used_accounts:
            await self._cooperative_sleep(task_control, self._sample_delay(normalized_delay))

    result_text = (
        "OK /vote finished.\n"
        f"Link: {link}\n"
        f"Option: {option_index}{f' ({option_label})' if option_label and option_label != str(option_index) else ''}\n"
        f"Requested accounts: {count}\n"
        f"Used accounts: {used_accounts if not invalid_poll else min(used_accounts, success + failed)}\n"
        f"Votes sent: {success}\n"
        f"Errors: {failed}"
    )
    audit_event("sender.vote_finished", message="Poll voting finished", link=link, option_index=option_index, count=count, used_accounts=used_accounts, success=success, failed=failed, requester_user_id=requester_user_id)
    return result_text


async def validated_delay_window(self, requester_user_id: int, value: DelayWindow) -> DelayWindow:
    normalized = normalize_user_delay(value)
    minimum = max(float(self.min_delay_seconds), 0.0)
    if isinstance(normalized, tuple):
        low = max(float(normalized[0]), minimum)
        high = max(float(normalized[1]), low)
        return (low, high)
    return max(float(normalized), minimum)


def sample_delay(self, value: DelayWindow) -> float:
    return sample_delay_value(value)


def shuffle_managed_clients(self, managed_clients: list[ManagedClient]) -> list[ManagedClient]:
    shuffled = list(managed_clients)
    random.shuffle(shuffled)
    return shuffled


def build_non_repeating_account_sequence(self, managed_clients: list[ManagedClient], total_steps: int) -> list[ManagedClient]:
    if not managed_clients or total_steps <= 0:
        return []
    shuffled = self._shuffle_managed_clients(managed_clients)
    result: list[ManagedClient] = []
    cursor = 0
    while len(result) < total_steps:
        result.append(shuffled[cursor % len(shuffled)])
        cursor += 1
    return result


def build_non_repeating_steps(self, raw_steps: list[tuple[ManagedClient, object, int]]) -> list[tuple[ManagedClient, object, int]]:
    grouped: dict[int, list[tuple[ManagedClient, object, int]]] = {}
    for step in raw_steps:
        grouped.setdefault(step[2], []).append(step)
    ordered: list[tuple[ManagedClient, object, int]] = []
    for repeat_index in sorted(grouped):
        group = list(grouped[repeat_index])
        random.shuffle(group)
        ordered.extend(group)
    return ordered


def build_balanced_account_batches(
    self,
    managed_clients: list[ManagedClient],
    batch_size: int,
    batch_count: int,
) -> list[list[ManagedClient]]:
    if not managed_clients or batch_size <= 0 or batch_count <= 0:
        return []
    pool = self._shuffle_managed_clients(managed_clients)
    pool_len = len(pool)
    batch_size = min(batch_size, pool_len)
    cursor = 0
    last_session: str | None = None
    batches: list[list[ManagedClient]] = []
    for _ in range(batch_count):
        batch: list[ManagedClient] = []
        used_in_batch: set[str] = set()
        for _ in range(batch_size):
            candidate_idx: int | None = None
            for offset in range(pool_len):
                idx = (cursor + offset) % pool_len
                managed = pool[idx]
                if managed.session_name in used_in_batch:
                    continue
                if pool_len > 1 and last_session is not None and managed.session_name == last_session:
                    continue
                candidate_idx = idx
                break
            if candidate_idx is None:
                for offset in range(pool_len):
                    idx = (cursor + offset) % pool_len
                    managed = pool[idx]
                    if pool_len > 1 and last_session is not None and managed.session_name == last_session:
                        continue
                    candidate_idx = idx
                    break
            if candidate_idx is None:
                candidate_idx = cursor % pool_len
            managed = pool[candidate_idx]
            cursor = (candidate_idx + 1) % pool_len
            if cursor == 0 and pool_len > 1:
                pool = self._shuffle_managed_clients(pool)
            batch.append(managed)
            used_in_batch.add(managed.session_name)
            last_session = managed.session_name
        batches.append(batch)
    return batches


async def send_reaction(client, peer_ref: str, msg_id: int, emoji: str):
    token = (emoji or "").strip()
    if not token:
        raise ValueError("Emoji for reaction cannot be empty.")
    if token.lower().startswith("custom:"):
        custom_id_raw = token.split(":", maxsplit=1)[1].strip()
        if not custom_id_raw.isdigit():
            raise ValueError("Custom emoji format: custom:<document_id> (digits only).")
        reaction = [types.ReactionCustomEmoji(document_id=int(custom_id_raw))]
    else:
        reaction = [types.ReactionEmoji(emoticon=token)]
    return await client(
        SendReactionRequest(
            peer=peer_ref,
            msg_id=msg_id,
            reaction=reaction,
        )
    )


async def send_vote(client, peer_ref: str, msg_id: int, option_bytes: bytes):
    return await client(
        SendVoteRequest(
            peer=peer_ref,
            msg_id=msg_id,
            options=[option_bytes],
        )
    )


def normalize_poll_option_label(text: str | None) -> str:
    return (text or "").strip() or "option"


async def load_poll_option(client, peer_ref: str, msg_id: int, option_index: int) -> tuple[bytes, str]:
    message = await client.get_messages(peer_ref, ids=msg_id)
    poll = getattr(getattr(message, "media", None), "poll", None)
    answers = list(getattr(poll, "answers", []) or [])
    if not answers:
        raise ValueError("Poll was not found or is no longer available.")
    answer_idx = option_index - 1
    if answer_idx < 0 or answer_idx >= len(answers):
        raise ValueError("This poll option does not exist.")
    selected = answers[answer_idx]
    return selected.option, normalize_poll_option_label(getattr(selected, "text", None))


async def join_with_retry(self, client, link: str, invite_hash: str | None, managed: ManagedClient, task_control=None) -> None:
    if invite_hash:
        await self._run_with_retry_on_client(
            client=client,
            operation_name=f"join invite {invite_hash}",
            coro_factory=lambda current_client, invite=invite_hash: current_client(ImportChatInviteRequest(invite)),
            managed=managed,
            task_control=task_control,
        )
        return
    channel = extract_public_channel(link)
    await self._run_with_retry_on_client(
        client=client,
        operation_name=f"join channel {channel}",
        coro_factory=lambda current_client, target=channel: current_client(JoinChannelRequest(target)),
        managed=managed,
        task_control=task_control,
    )


async def leave_with_retry(self, client, link: str, invite_hash: str | None, managed: ManagedClient, task_control=None) -> None:
    if invite_hash:
        invite_info = await self._run_with_retry_on_client(
            client=client,
            operation_name=f"resolve invite {invite_hash}",
            coro_factory=lambda current_client, invite=invite_hash: current_client(functions.messages.CheckChatInviteRequest(hash=invite)),
            managed=managed,
            task_control=task_control,
        )
        if isinstance(invite_info, types.ChatInviteAlready):
            await self._run_with_retry_on_client(
                client=client,
                operation_name="leave invite chat",
                coro_factory=lambda current_client, target=invite_info.chat: current_client(LeaveChannelRequest(target)),
                managed=managed,
                task_control=task_control,
            )
            return
        raise ValueError("Аккаунт не состоит в чате по этой приватной ссылке.")
    channel = extract_public_channel(link)
    await self._run_with_retry_on_client(
        client=client,
        operation_name=f"leave channel {channel}",
        coro_factory=lambda current_client, target=channel: current_client(LeaveChannelRequest(target)),
        managed=managed,
        task_control=task_control,
    )


async def is_already_joined(self, client, link: str, invite_hash: str | None, managed: ManagedClient, task_control=None) -> bool:
    if invite_hash:
        try:
            invite_info = await self._run_with_retry_on_client(
                client=client,
                operation_name=f"check invite membership {invite_hash}",
                coro_factory=lambda current_client, invite=invite_hash: current_client(functions.messages.CheckChatInviteRequest(hash=invite)),
                managed=managed,
                task_control=task_control,
            )
            return isinstance(invite_info, types.ChatInviteAlready)
        except Exception:
            return False
    channel = extract_public_channel(link)
    try:
        me = await client.get_me()
        await self._run_with_retry_on_client(
            client=client,
            operation_name=f"check membership {channel}",
            coro_factory=lambda current_client, target=channel, participant=me: current_client(
                GetParticipantRequest(channel=target, participant=participant)
            ),
            managed=managed,
            task_control=task_control,
        )
        return True
    except UserNotParticipantError:
        return False
    except RPCError:
        return False


async def run_with_retry(self, operation_name: str, coro_factory: Callable[[], Awaitable], task_control=None):
    last_exc: Exception | None = None
    for attempt in range(1, self.max_retries + 1):
        await self._checkpoint(task_control)
        try:
            return await coro_factory()
        except TaskCancelledError:
            raise
        except FloodWaitError as exc:
            last_exc = exc
            await self._cooperative_sleep(task_control, max(float(exc.seconds), float(self.min_delay_seconds)))
        except RPCError as exc:
            last_exc = exc
            if self._is_non_retryable_rpc_error(exc) or attempt >= self.max_retries:
                raise
            await self._cooperative_sleep(task_control, attempt)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"{operation_name} failed without exception")


async def run_with_retry_on_client(self, client, operation_name: str, coro_factory: Callable, managed: ManagedClient, task_control=None):
    async with managed.session_lock:
        if not client.is_connected():
            await client.connect()
        return await self._run_with_retry(
            operation_name=operation_name,
            coro_factory=lambda: coro_factory(client),
            task_control=task_control,
        )


def is_non_retryable_rpc_error(exc: RPCError) -> bool:
    text = str(exc).lower()
    non_retryable_markers = (
        "chat admin required",
        "channel private",
        "invite hash invalid",
        "invite hash expired",
        "username invalid",
        "username not occupied",
        "username is unacceptable",
        "nobody is using this username",
        "entity bounds invalid",
        "message id invalid",
        "peer id invalid",
        "user not participant",
        "reactioninvalid",
        "invalid reaction",
    )
    return any(marker in text for marker in non_retryable_markers)


def is_call_target_unavailable_error(exc: RPCError) -> bool:
    if isinstance(exc, UserPrivacyRestrictedError):
        return True
    text = str(exc).lower()
    unavailable_markers = (
        "user privacy restricted",
        "privacy settings do not allow",
        "user_not_mutual_contact",
        "user_is_blocked",
        "participant_version_outdated",
        "call_peer_invalid",
        "call_protocol_flags_invalid",
        "participant_call_failed",
    )
    return any(marker in text for marker in unavailable_markers)


async def cooperative_sleep(self, task_control, delay: DelayWindow | float) -> None:
    sleep_for = sample_delay_value(delay) if isinstance(delay, tuple) else float(delay)
    if sleep_for <= 0:
        return
    remaining = sleep_for
    while remaining > 0:
        await self._checkpoint(task_control)
        step = min(remaining, 1.0)
        await asyncio.sleep(step)
        remaining -= step


async def checkpoint(self, task_control) -> None:
    if task_control is None:
        return
    await task_control.checkpoint()


# Bind module-level implementations to Sender.
# This keeps behavior stable after file cleanup and restores expected methods.
Sender.send_messages = send_messages
Sender.send_to_bot = send_to_bot
Sender.call_user = call_user
Sender.join_chat = join_chat
Sender.react_to_post = react_to_post
Sender.leave_chat = leave_chat
Sender.follow_referral = follow_referral
Sender.vote_in_poll = vote_in_poll

Sender._validated_delay_window = validated_delay_window
Sender._sample_delay = sample_delay
Sender._shuffle_managed_clients = shuffle_managed_clients
Sender._build_non_repeating_account_sequence = build_non_repeating_account_sequence
Sender._build_non_repeating_steps = build_non_repeating_steps
Sender._build_balanced_account_batches = build_balanced_account_batches
Sender._join_with_retry = join_with_retry
Sender._leave_with_retry = leave_with_retry
Sender._is_already_joined = is_already_joined
Sender._run_with_retry = run_with_retry
Sender._run_with_retry_on_client = run_with_retry_on_client
Sender._cooperative_sleep = cooperative_sleep
Sender._checkpoint = checkpoint

Sender._send_reaction = staticmethod(send_reaction)
Sender._send_vote = staticmethod(send_vote)
Sender._load_poll_option = staticmethod(load_poll_option)
Sender._is_non_retryable_rpc_error = staticmethod(is_non_retryable_rpc_error)
Sender._is_call_target_unavailable_error = staticmethod(is_call_target_unavailable_error)
Sender._place_call_and_hangup = place_call_and_hangup

