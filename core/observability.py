from __future__ import annotations

import json
import logging
import os
import platform
import socket
import time
from collections import deque
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

APP_START_TS = time.time()

_DEFAULT_TEXT_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
_DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
_RESERVED_LOG_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__)
_EVENT_RECORDER: EventRecorder | None = None


def _sanitize_for_json(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return {"type": "bytes", "size": len(value)}
    if isinstance(value, dict):
        return {str(key): _sanitize_for_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_for_json(item) for item in value]
    if isinstance(value, BaseException):
        text = str(value).strip()
        return {
            "type": value.__class__.__name__,
            "message": text or value.__class__.__name__,
        }
    if hasattr(value, "__dict__"):
        return {
            key: _sanitize_for_json(item)
            for key, item in vars(value).items()
            if not key.startswith("_")
        }
    return str(value)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.threadName,
        }

        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _RESERVED_LOG_RECORD_FIELDS and not key.startswith("_")
        }
        if extras:
            payload["extra"] = _sanitize_for_json(extras)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = record.stack_info
        return json.dumps(payload, ensure_ascii=False)


class EventRecorder:
    def __init__(self, logger: logging.Logger, logs_dir: Path) -> None:
        self._logger = logger
        self.logs_dir = logs_dir

    def record(
        self,
        event_type: str,
        *,
        level: int = logging.INFO,
        message: str | None = None,
        category: str | None = None,
        **data: Any,
    ) -> None:
        payload = _sanitize_for_json(data)
        self._logger.log(
            level,
            message or event_type,
            extra={
                "event_type": event_type,
                "event_category": category or event_type.split(".", maxsplit=1)[0],
                "payload": payload,
            },
        )


def configure_logging(logs_dir: Path) -> EventRecorder:
    global _EVENT_RECORDER

    logs_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    text_formatter = logging.Formatter(_DEFAULT_TEXT_FORMAT, datefmt=_DEFAULT_DATE_FORMAT)
    json_formatter = JsonFormatter()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(text_formatter)

    app_handler = RotatingFileHandler(
        logs_dir / "app.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    app_handler.setLevel(logging.INFO)
    app_handler.setFormatter(text_formatter)

    json_handler = RotatingFileHandler(
        logs_dir / "app.jsonl",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    json_handler.setLevel(logging.INFO)
    json_handler.setFormatter(json_formatter)

    error_handler = RotatingFileHandler(
        logs_dir / "error.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(text_formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(app_handler)
    root_logger.addHandler(json_handler)
    root_logger.addHandler(error_handler)

    audit_logger = logging.getLogger("audit")
    audit_logger.handlers.clear()
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = True

    events_handler = RotatingFileHandler(
        logs_dir / "events.jsonl",
        maxBytes=5 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    events_handler.setLevel(logging.INFO)
    events_handler.setFormatter(json_formatter)
    audit_logger.addHandler(events_handler)

    _EVENT_RECORDER = EventRecorder(audit_logger, logs_dir)
    audit_event(
        "system.logging_configured",
        message="Logging configured",
        logs_dir=str(logs_dir),
        hostname=socket.gethostname(),
        pid=os.getpid(),
        platform=platform.platform(),
        python=platform.python_version(),
    )
    return _EVENT_RECORDER


def get_event_recorder() -> EventRecorder | None:
    return _EVENT_RECORDER


def audit_event(
    event_type: str,
    *,
    level: int = logging.INFO,
    message: str | None = None,
    category: str | None = None,
    **data: Any,
) -> None:
    recorder = get_event_recorder()
    if recorder is None:
        return
    recorder.record(
        event_type,
        level=level,
        message=message,
        category=category,
        **data,
    )


def tail_text_file(path: Path, limit: int = 200) -> list[str]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        return list(deque((line.rstrip("\n") for line in handle), maxlen=max(1, limit)))


def tail_jsonl_file(path: Path, limit: int = 200) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in tail_text_file(path, limit=limit):
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            rows.append({"raw": line, "parse_error": True})
            continue
        if isinstance(payload, dict):
            rows.append(payload)
        else:
            rows.append({"raw": payload})
    return rows


def list_log_files(logs_dir: Path) -> list[dict[str, Any]]:
    logs_dir.mkdir(parents=True, exist_ok=True)
    items: list[dict[str, Any]] = []
    for path in sorted(logs_dir.glob("*")):
        if not path.is_file():
            continue
        stat = path.stat()
        items.append(
            {
                "name": path.name,
                "path": str(path),
                "size_bytes": stat.st_size,
                "updated_at": stat.st_mtime,
            }
        )
    return items


def process_uptime_seconds() -> int:
    return max(0, int(time.time() - APP_START_TS))
