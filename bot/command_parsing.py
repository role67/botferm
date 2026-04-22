from __future__ import annotations

import re
import shlex
from typing import TypeAlias

from config import MAX_COUNT
from core.access_manager import ROLE_ADMIN, ROLE_EXTERNAL, ROLE_INTERNAL, ROLE_OWNER

DelayValue: TypeAlias = float | tuple[float, float]


def parse_access_user_payload(raw_value: str, *, owner_user_id: int) -> tuple[int, str, int | None]:
    parts = raw_value.split()
    if not parts:
        raise ValueError("Формат: telegram_id role [owner_scope_id]")

    telegram_id = parse_user_id(parts[0])
    if len(parts) == 1:
        return telegram_id, ROLE_INTERNAL, owner_user_id

    role = parts[1].strip().lower()
    if role == "client":
        role = ROLE_INTERNAL
    elif role in {"private_client", "private"}:
        role = ROLE_EXTERNAL
    if role not in {ROLE_OWNER, ROLE_ADMIN, ROLE_INTERNAL, ROLE_EXTERNAL}:
        raise ValueError("Роль должна быть одной из: owner, admin, internal, external.")

    if len(parts) > 3:
        raise ValueError("Формат: telegram_id role [owner_scope_id]")

    if len(parts) == 3:
        owner_scope_id = parse_user_id(parts[2])
    elif role == ROLE_EXTERNAL:
        owner_scope_id = telegram_id
    else:
        owner_scope_id = owner_user_id

    return telegram_id, role, owner_scope_id


def parse_likep_payload(payload: str) -> tuple[str, int, DelayValue, list[str]]:
    parts = _split_command_parts(payload)
    if len(parts) < 2:
        raise ValueError("Формат: /likep <ссылка> <N> <T> <emoji|emoji...>")

    link = parts[0].strip()
    count = 1
    delay: DelayValue = 1.5
    emoji_start = 1

    if len(parts) >= 2 and parts[1].isdigit():
        count = int(parts[1])
        emoji_start = 2
    if len(parts) >= 3 and _looks_like_delay_token(parts[2]):
        delay = parse_delay_input(parts[2], field_name="T для /likep")
        emoji_start = 3

    emoji_tokens = parts[emoji_start:]

    if count < 1:
        raise ValueError("N для /likep должен быть целым числом больше 0.")
    if not emoji_tokens:
        raise ValueError("Нужен emoji. Формат: /likep <ссылка> <N> <T> <emoji|emoji...>")

    emoji = " ".join(emoji_tokens).strip()
    if not emoji:
        raise ValueError("Нужен emoji.")
    if emoji.isdigit():
        raise ValueError("Нужен emoji, а не число. Формат: /likep <ссылка> <N> <T> <emoji|emoji...>")
    reactions = _parse_reaction_list(emoji)
    if not reactions:
        raise ValueError("Не удалось распознать emoji. Пример: 👍❤️ или 👍 ❤️")
    if len(reactions) > 5:
        raise ValueError("Для /likep можно указать не более 5 эмодзи.")

    return link, count, delay, reactions


def _looks_like_delay_token(value: str) -> bool:
    normalized = value.strip().replace(",", ".")
    return bool(re.fullmatch(r"\d+(?:\.\d+)?(?:\s*-\s*\d+(?:\.\d+)?)?", normalized))


def _parse_reaction_list(raw_value: str) -> list[str]:
    text = raw_value.strip()
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[\s,|;/]+", text) if part.strip()]
    reactions: list[str] = []
    if len(parts) > 1:
        for part in parts:
            reactions.extend(_split_compact_emoji_sequence(part))
        return _dedupe_keep_order(reactions)
    return _dedupe_keep_order(_split_compact_emoji_sequence(text))


def _split_compact_emoji_sequence(value: str) -> list[str]:
    if value.lower().startswith("custom:"):
        return [value]
    clusters: list[str] = []
    current = ""
    for ch in value:
        if not current:
            current = ch
            continue
        if _is_emoji_joiner(ch):
            current += ch
            continue
        if _is_emoji_modifier(ch):
            current += ch
            continue
        if current.endswith("\u200d"):
            current += ch
            continue
        clusters.append(current)
        current = ch
    if current:
        clusters.append(current)
    return [item for item in clusters if _looks_like_reaction_token(item)]


def _is_emoji_joiner(ch: str) -> bool:
    return ch in {"\u200d", "\ufe0f", "\ufe0e", "\u20e3"}


def _is_emoji_modifier(ch: str) -> bool:
    code = ord(ch)
    return 0x1F3FB <= code <= 0x1F3FF


def _looks_like_reaction_token(token: str) -> bool:
    stripped = token.strip()
    if not stripped:
        return False
    if stripped.lower().startswith("custom:"):
        suffix = stripped.split(":", maxsplit=1)[1].strip()
        return suffix.isdigit()
    if stripped.isdigit():
        return False
    return True


def _dedupe_keep_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def parse_delay_value(raw_value: str, field_name: str) -> float:
    normalized = raw_value.strip().replace(",", ".")
    try:
        value = float(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} должен быть числом не меньше 0. Можно 1,1 или 1.7.") from exc

    if value < 0:
        raise ValueError(f"{field_name} должен быть числом не меньше 0.")
    return value


def parse_delay_input(raw_value: str, field_name: str) -> DelayValue:
    normalized = raw_value.strip().replace(",", ".")
    if not normalized:
        raise ValueError(f"{field_name} должен быть числом не меньше 0.")

    range_match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)", normalized)
    if range_match:
        min_value = float(range_match.group(1))
        max_value = float(range_match.group(2))
        if min_value < 0 or max_value < 0:
            raise ValueError(f"{field_name} должен быть числом не меньше 0.")
        if min_value > max_value:
            raise ValueError(f"В диапазоне {field_name} левая граница не может быть больше правой.")
        return (min_value, max_value)

    return parse_delay_value(normalized, field_name)


def parse_join_payload(payload: str) -> tuple[str, int, DelayValue]:
    parts = payload.split()
    if len(parts) == 1:
        return parts[0].strip(), 1, 1.5
    if len(parts) != 3:
        raise ValueError("Формат: /join <ссылка> <N> <T>")

    link = parts[0].strip()

    if not parts[1].isdigit():
        raise ValueError("N для /join должен быть целым числом больше 0.")
    count = int(parts[1])
    if count < 1:
        raise ValueError("N для /join должен быть целым числом больше 0.")

    delay_cap = parse_delay_input(parts[2], field_name="T РґР»СЏ /join")
    return link, count, delay_cap


def parse_leave_payload(payload: str) -> tuple[str, int, DelayValue]:
    parts = payload.split()
    if len(parts) == 1:
        return parts[0].strip(), 1, 1.5
    if len(parts) != 3:
        raise ValueError("Формат: /leave <ссылка> <N> <T>")

    link = parts[0].strip()

    if not parts[1].isdigit():
        raise ValueError("N для /leave должен быть целым числом больше 0.")
    count = int(parts[1])
    if count < 1:
        raise ValueError("N для /leave должен быть целым числом больше 0.")

    delay_cap = parse_delay_input(parts[2], field_name="T РґР»СЏ /leave")
    return link, count, delay_cap


def parse_refp_payload(payload: str) -> tuple[str, int, DelayValue]:
    parts = payload.split()
    if len(parts) != 3:
        raise ValueError("Формат: /refp <ссылка> <N> <T>")

    link = parts[0].strip()
    raw_count = parts[1]
    raw_delay = parts[2]
    count, delay = validate_count_delay(raw_count, raw_delay, max_count=MAX_COUNT)
    return link, count, delay


def parse_vote_payload(payload: str) -> tuple[str, int, int, DelayValue]:
    parts = payload.split()
    if len(parts) != 4:
        raise ValueError("Формат: /vote <ссылка> <пункт> <N> <T>")

    link = parts[0].strip()

    if not parts[1].isdigit():
        raise ValueError("Пункт для /vote должен быть целым числом от 1 до 12.")
    option_index = int(parts[1])
    if option_index < 1 or option_index > 12:
        raise ValueError("Пункт для /vote должен быть целым числом от 1 до 12.")

    count, delay = validate_count_delay(parts[2], parts[3], max_count=MAX_COUNT)
    return link, option_index, count, delay


def parse_birthday(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", value.strip())
    if not match:
        raise ValueError("Формат даты: ДД.ММ.ГГГГ")

    day = int(match.group(1))
    month = int(match.group(2))
    year = int(match.group(3))
    return day, month, year


def extract_payload(text: str | None) -> str:
    if not text:
        return ""
    parts = text.split(maxsplit=1)
    if len(parts) == 1:
        return ""
    return parts[1].strip()


def _extract_hide_flag(parts: list[str]) -> tuple[list[str], bool]:
    if parts and parts[-1].lower() == "-h":
        return parts[:-1], True
    return parts, False


def _split_command_parts(payload: str) -> list[str]:
    try:
        return shlex.split(payload)
    except ValueError:
        return payload.split()


def parse_msg_payload(payload: str, *, allow_empty_text: bool = False) -> tuple[list[str], str, int, int, float, bool]:
    parts = _split_command_parts(payload)
    parts, hide_content = _extract_hide_flag(parts)
    if len(parts) < 4:
        raise ValueError("Формат: /msg @user1 @user2 текст аккаунты повторы delay")

    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(
        parts[-3],
        parts[-2],
        parts[-1],
    )

    target_tokens: list[str] = []
    index = 0
    while index < len(parts) - 3:
        token = parts[index]
        try:
            normalize_username(token)
            target_tokens.append(token)
            index += 1
        except ValueError:
            break

    if not target_tokens:
        raise ValueError("РќСѓР¶РЅРѕ СѓРєР°Р·Р°С‚СЊ С…РѕС‚СЏ Р±С‹ РѕРґРЅСѓ С†РµР»СЊ РІ РЅР°С‡Р°Р»Рµ РєРѕРјР°РЅРґС‹.")

    text = " ".join(parts[index:-3]).strip()
    if not text and not allow_empty_text:
        raise ValueError("Нужно указать текст сообщения.")

    targets = parse_targets(" ".join(target_tokens))
    return targets, text, accounts_count, repeat_count, delay, hide_content


def parse_msgbot_payload(payload: str) -> tuple[str, str, int, int, float]:
    parts = _split_command_parts(payload)
    if len(parts) < 5:
        raise ValueError("Формат: /msgbot @bot_username команда аккаунты повторы delay")

    bot_username = normalize_username(parts[0])
    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(
        parts[-3],
        parts[-2],
        parts[-1],
    )

    command = " ".join(parts[1:-3]).strip()
    if not command:
        raise ValueError("Нужно указать команду или текст для бота.")

    return bot_username, command, accounts_count, repeat_count, delay


def parse_msgchat_payload(payload: str, *, allow_empty_text: bool = False) -> tuple[str, str, int, int, float, bool]:
    parts = _split_command_parts(payload)
    parts, hide_content = _extract_hide_flag(parts)
    if len(parts) < 4:
        raise ValueError("Формат: /msgchat <чат> текст аккаунты повторы delay")

    target = normalize_chat_target(parts[0])
    accounts_count, repeat_count, delay = validate_accounts_repeat_delay(
        parts[-3],
        parts[-2],
        parts[-1],
    )
    text = " ".join(parts[1:-3]).strip()
    if not text and not allow_empty_text:
        raise ValueError("Нужно указать текст сообщения.")
    return target, text, accounts_count, repeat_count, delay, hide_content


def parse_account_add_payload(payload: str) -> tuple[str, str, str]:
    parts = payload.split(maxsplit=2)
    if len(parts) != 3 or any(not part.strip() for part in parts):
        raise ValueError("Формат: session api_id api_hash")
    return parts[0].strip(), parts[1].strip(), parts[2].strip()


def parse_user_id(raw_value: str) -> int:
    value = raw_value.strip()
    if not re.fullmatch(r"\d{3,20}", value):
        raise ValueError("Нужно отправить Telegram user ID числом.")
    return int(value)


def parse_targets(raw: str) -> list[str]:
    targets = [normalize_username(item) for item in raw.split() if item.strip()]
    if not targets:
        raise ValueError("РќСѓР¶РЅРѕ СѓРєР°Р·Р°С‚СЊ С…РѕС‚СЏ Р±С‹ РѕРґРЅСѓ С†РµР»СЊ.")
    return targets


def normalize_username(value: str) -> str:
    username = value.strip().strip(",").strip("/")
    username = re.sub(r"^https?://(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = re.sub(r"^(?:www\.)?(?:t|telegram)\.me/", "", username, flags=re.IGNORECASE)
    username = username.split("?", maxsplit=1)[0].strip("/")
    if username.startswith("@"):
        cleaned = username[1:]
    else:
        cleaned = username

    if not re.fullmatch(r"[A-Za-z0-9_]{5,64}", cleaned):
        raise ValueError(f"Некорректный юзернейм: {value}")

    return f"@{cleaned}"


def normalize_chat_target(value: str) -> str:
    raw = value.strip().strip(",")
    if raw.startswith("@"):
        return normalize_username(raw)
    raw = re.sub(r"^https?://(?:www\.)?(?:t|telegram)\.me/", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^(?:www\.)?(?:t|telegram)\.me/", "", raw, flags=re.IGNORECASE)
    raw = raw.strip("/")
    if not raw:
        raise ValueError("Нужно указать ссылку или username чата.")
    if "/" in raw:
        raise ValueError("Для /msgchat нужна ссылка на чат или канал, а не на отдельное сообщение.")
    return normalize_username(raw)


def validate_accounts_repeat_delay(
    raw_accounts: str,
    raw_repeats: str,
    raw_delay: str,
) -> tuple[int, int, DelayValue]:
    if not raw_accounts.isdigit():
        raise ValueError("Количество аккаунтов должно быть целым числом больше 0.")
    accounts_count = int(raw_accounts)
    if accounts_count < 1 or accounts_count > MAX_COUNT:
        raise ValueError(f"Количество аккаунтов должно быть от 1 до {MAX_COUNT}.")

    if not raw_repeats.isdigit():
        raise ValueError("Количество повторений должно быть целым числом от 1 до 100.")
    repeat_count = int(raw_repeats)
    if repeat_count < 1 or repeat_count > 100:
        raise ValueError("Количество повторений должно быть целым числом от 1 до 100.")

    delay = parse_delay_input(raw_delay, field_name="T")
    return accounts_count, repeat_count, delay


def validate_count_delay(raw_count: str, raw_delay: str, *, max_count: int) -> tuple[int, DelayValue]:
    if not raw_count.isdigit():
        raise ValueError(f"count должен быть целым числом в диапазоне 1..{max_count}.")

    count = int(raw_count)
    if count < 1 or count > max_count:
        raise ValueError(f"count должен быть в диапазоне 1..{max_count}.")

    delay = parse_delay_input(raw_delay, field_name="delay")
    return count, delay


def parse_positive_int(value: str, *, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def looks_like_access_key(value: str) -> bool:
    text = (value or "").strip()
    return bool(re.fullmatch(r"[A-Za-z0-9]{4}(?:-[A-Za-z0-9]{4}){3}", text))


def parse_session_page_callback(data: str, *, prefix: str) -> tuple[str, int]:
    payload = data[len(prefix):]
    session, _, page_raw = payload.rpartition(":")
    if not session:
        return payload, 1
    return session, parse_positive_int(page_raw, default=1)

