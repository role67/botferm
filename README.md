# botoferma

Telegram bot project with account management, access keys, task queue, and Telegram automation runtime.

## Environment

Use `.env` locally and environment variables on Render.

Required:

```env
BOT_TOKEN=...
OWNER_USER_ID=...
DATABASE_URL=...
```

Recommended:

```env
SUPPORT_USERNAME=mattersless
DATA_DIR=./data
LOGS_DIR=./data/logs
DEFAULT_API_ID=...
DEFAULT_API_HASH=...
HEALTH_NOTIFY_CHAT_ID=...
MIN_DELAY_SECONDS=2
MAX_COUNT=100
MAX_RETRIES=3
```

## Runtime Data

The app now stores mutable runtime data outside the main code flow:

- sessions: `DATA_DIR/sessions`
- accounts config: `DATA_DIR/accounts.json`
- access users and keys: `DATA_DIR/access_users.json`
- logs: `LOGS_DIR` (`app.log`, `app.jsonl`, `events.jsonl`, `error.log`)
- Telethon session backups in Postgres: `telethon_sessions`

If old files already exist in the project root, the app can still read them as a fallback.

## Local Run

```bash
pip install -r requirements.txt
python main.py
```

## Encoding Check

Run this check before commits to catch broken text encoding (mojibake):

```bash
python tools/check_mojibake.py
```

## Logging

The runtime now keeps multiple log streams:

- `app.log`: readable operational log
- `app.jsonl`: structured application log
- `events.jsonl`: audit trail for queue, access, health, sessions, and profile/account changes
- `error.log`: error-only stream

## Render Notes

- Set all required variables in the Render dashboard.
- Use a persistent disk for `DATA_DIR`, otherwise session files and runtime JSON will be lost after redeploy/restart.
- If `DATABASE_URL` is set, uploaded `.session` files are also stored in Postgres and restored on startup.
- Start command: `python main.py`

## GitHub Safety

`.gitignore` excludes:

- `.env`
- `data/`
- `sessions/`
- `accounts.json`
- `access_users.json`

Before pushing, make sure real secrets and runtime files are not included in the repository history.
