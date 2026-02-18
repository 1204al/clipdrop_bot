from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from env import env_flag_is_true, load_env_file


@dataclass(frozen=True)
class AppConfig:
    debug: bool
    max_attempts: int
    downloads_dir: Path
    queue_file: Path
    results_file: Path
    queue_lock_file: Path
    worker_poll_seconds: float
    service_host: str
    service_port: int
    telegram_bot_token: str | None
    telegram_auth_password: str | None
    telegram_authorized_chats_file: Path
    telegram_whitelist_file: Path
    telegram_access_lock_file: Path
    telegram_callback_host: str
    telegram_callback_port: int
    telegram_lock_file: Path
    telegram_upload_limit_mb: int
    telegram_very_large_threshold_mb: int
    telegram_resize_timeout_sec: int
    bot_service_url: str
    worker_bot_callback_url: str
    bot_callback_secret: str
    log_file: Path | None


def _env_int(key: str, default: int, minimum: int) -> int:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _log_file_path() -> Path | None:
    raw = os.getenv("LOG_FILE")
    if not raw or not raw.strip():
        return None
    return Path(raw.strip())


def _env_float(key: str, default: float, minimum: float) -> float:
    raw = os.getenv(key)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(minimum, value)


def load_config(env_path: Path | None = None) -> AppConfig:
    load_env_file(env_path or Path(".env"))
    upload_limit_mb = _env_int("TELEGRAM_UPLOAD_LIMIT_MB", default=50, minimum=1)

    return AppConfig(
        debug=env_flag_is_true(os.getenv("DEBUG")),
        max_attempts=_env_int("MAX_ATTEMPTS", default=2, minimum=1),
        downloads_dir=Path(os.getenv("DOWNLOADS_DIR", "downloads")),
        queue_file=Path(os.getenv("QUEUE_FILE", "queue.jsonl")),
        results_file=Path(os.getenv("RESULTS_FILE", "results.jsonl")),
        queue_lock_file=Path(os.getenv("QUEUE_LOCK_FILE", ".queue.lock")),
        worker_poll_seconds=_env_float("WORKER_POLL_SECONDS", default=2.0, minimum=0.2),
        service_host=os.getenv("SERVICE_HOST", "0.0.0.0"),
        service_port=_env_int("SERVICE_PORT", default=8000, minimum=1),
        telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
        telegram_auth_password=os.getenv("TELEGRAM_AUTH_PASSWORD"),
        telegram_authorized_chats_file=Path(
            os.getenv("TELEGRAM_AUTHORIZED_CHATS_FILE", "telegram_authorized_chats.json")
        ),
        telegram_whitelist_file=Path(os.getenv("TELEGRAM_WHITELIST_FILE", "telegram_whitelist.txt")),
        telegram_access_lock_file=Path(os.getenv("TELEGRAM_ACCESS_LOCK_FILE", ".telegram_access.lock")),
        telegram_callback_host=os.getenv("TELEGRAM_CALLBACK_HOST", "127.0.0.1"),
        telegram_callback_port=_env_int("TELEGRAM_CALLBACK_PORT", default=8090, minimum=1),
        telegram_lock_file=Path(os.getenv("TELEGRAM_LOCK_FILE", ".telegram_bot.lock")),
        telegram_upload_limit_mb=upload_limit_mb,
        telegram_very_large_threshold_mb=_env_int(
            "TELEGRAM_VERY_LARGE_THRESHOLD_MB",
            default=150,
            minimum=upload_limit_mb,
        ),
        telegram_resize_timeout_sec=_env_int("TELEGRAM_RESIZE_TIMEOUT_SEC", default=180, minimum=10),
        bot_service_url=os.getenv("BOT_SERVICE_URL", "http://127.0.0.1:8000"),
        worker_bot_callback_url=os.getenv(
            "WORKER_BOT_CALLBACK_URL",
            "http://127.0.0.1:8090/internal/job-events",
        ),
        bot_callback_secret=os.getenv("BOT_CALLBACK_SECRET", "change-me"),
        log_file=_log_file_path(),
    )
