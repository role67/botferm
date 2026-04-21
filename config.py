from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data")).expanduser()


def _normalize_path(raw_value: str | None, default_path: Path) -> Path:
    if not raw_value:
        return default_path
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = BASE_DIR / candidate
    return candidate


def _resolve_runtime_file(env_name: str, default_path: Path, legacy_paths: list[Path]) -> Path:
    from_env = os.getenv(env_name, "").strip()
    if from_env:
        return _normalize_path(from_env, default_path)
    for candidate in legacy_paths:
        if candidate.exists():
            return candidate
    return default_path


def _env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _env_int(name: str, default: int | None = None) -> int | None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> list[str]:
    raw = _env_str(name, default)
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_required_str(name: str) -> str:
    value = _env_str(name)
    if not value:
        raise RuntimeError(f"Не задана обязательная переменная окружения {name}.")
    return value


def _env_required_int(name: str) -> int:
    value = _env_int(name)
    if value is None:
        raise RuntimeError(f"Не задана обязательная переменная окружения {name}.")
    return value


def _normalize_username(value: str) -> str:
    clean = value.strip()
    if clean.startswith("@"):
        clean = clean[1:]
    return clean or "mattersless"


_legacy_root_sessions = BASE_DIR / "sessions"
_legacy_parent_sessions = BASE_DIR.parent / "sessions"
_default_sessions = DATA_DIR / "sessions"
SESSIONS_DIR = _resolve_runtime_file(
    "SESSIONS_DIR",
    _default_sessions,
    [_legacy_root_sessions, _legacy_parent_sessions],
)
ACCOUNTS_FILE = _resolve_runtime_file(
    "ACCOUNTS_FILE",
    DATA_DIR / "accounts.json",
    [BASE_DIR / "accounts.json"],
)
ACCESS_USERS_FILE = _resolve_runtime_file(
    "ACCESS_USERS_FILE",
    DATA_DIR / "access_users.json",
    [BASE_DIR / "access_users.json"],
)
LOGS_DIR = _resolve_runtime_file(
    "LOGS_DIR",
    DATA_DIR / "logs",
    [],
)
MESSAGE_MEDIA_DIR = _resolve_runtime_file(
    "MESSAGE_MEDIA_DIR",
    DATA_DIR / "message_media",
    [],
)

BOT_TOKEN = _env_required_str("BOT_TOKEN")
OWNER_USER_ID = _env_required_int("OWNER_USER_ID")
HEALTH_NOTIFY_CHAT_ID = _env_int("HEALTH_NOTIFY_CHAT_ID", OWNER_USER_ID)
SUPPORT_USERNAME = _normalize_username(_env_str("SUPPORT_USERNAME", "mattersless"))
USAGE_POLICY_URL = _env_str("USAGE_POLICY_URL")
PRIVACY_POLICY_URL = _env_str("PRIVACY_POLICY_URL")
TERMS_OF_SERVICE_URL = _env_str("TERMS_OF_SERVICE_URL")

DEFAULT_API_ID = _env_int("DEFAULT_API_ID")
DEFAULT_API_HASH = _env_str("DEFAULT_API_HASH")
DATABASE_URL = _env_str("DATABASE_URL")

MIN_DELAY_SECONDS = _env_int("MIN_DELAY_SECONDS", 2) or 2
MAX_COUNT = _env_int("MAX_COUNT", 100) or 100
MAX_RETRIES = _env_int("MAX_RETRIES", 3) or 3

ADMIN_API_ENABLED = _env_bool("ADMIN_API_ENABLED", True)
ADMIN_API_HOST = _env_str("ADMIN_API_HOST", "0.0.0.0")
ADMIN_API_PORT = _env_int("ADMIN_API_PORT", _env_int("PORT", 8000)) or 8000
ADMIN_API_TOKEN = _env_required_str("ADMIN_API_TOKEN") if ADMIN_API_ENABLED else ""
ADMIN_API_AUTH_TOKENS = _env_csv("ADMIN_API_TOKENS")
if ADMIN_API_ENABLED and not ADMIN_API_AUTH_TOKENS:
    ADMIN_API_AUTH_TOKENS = [ADMIN_API_TOKEN]
ADMIN_API_ALLOWED_ORIGINS = _env_csv(
    "ADMIN_API_ALLOWED_ORIGINS",
    "http://localhost:5173,http://127.0.0.1:5173",
)
ADMIN_API_HEALTH_INCLUDE_LOGS = _env_bool("ADMIN_API_HEALTH_INCLUDE_LOGS", False)
ADMIN_API_ALLOWED_IPS = _env_csv("ADMIN_API_ALLOWED_IPS")
ADMIN_API_ENFORCE_HTTPS = _env_bool("ADMIN_API_ENFORCE_HTTPS", False)
ADMIN_API_RATE_LIMIT_ENABLED = _env_bool("ADMIN_API_RATE_LIMIT_ENABLED", True)
ADMIN_API_RATE_LIMIT_WINDOW_SECONDS = _env_int("ADMIN_API_RATE_LIMIT_WINDOW_SECONDS", 60) or 60
ADMIN_API_RATE_LIMIT_MAX_REQUESTS = _env_int("ADMIN_API_RATE_LIMIT_MAX_REQUESTS", 120) or 120
ADMIN_API_AUTH_RATE_LIMIT_MAX_ATTEMPTS = _env_int("ADMIN_API_AUTH_RATE_LIMIT_MAX_ATTEMPTS", 20) or 20
ADMIN_API_CSP = _env_str(
    "ADMIN_API_CSP",
    "default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'",
)
