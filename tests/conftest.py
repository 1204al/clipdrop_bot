from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from config import AppConfig


@pytest.fixture
def app_config(tmp_path: Path) -> AppConfig:
    return AppConfig(
        debug=False,
        max_attempts=2,
        downloads_dir=tmp_path / "downloads",
        queue_file=tmp_path / "queue.jsonl",
        results_file=tmp_path / "results.jsonl",
        queue_lock_file=tmp_path / ".queue.lock",
        worker_poll_seconds=0.01,
        service_host="127.0.0.1",
        service_port=8000,
        telegram_bot_token="test-token",
        telegram_auth_password="123",
        telegram_authorized_chats_file=tmp_path / "telegram_authorized_chats.json",
        telegram_whitelist_file=tmp_path / "telegram_whitelist.txt",
        telegram_access_lock_file=tmp_path / ".telegram_access.lock",
        telegram_callback_host="127.0.0.1",
        telegram_callback_port=8090,
        telegram_lock_file=tmp_path / ".telegram_bot.lock",
        telegram_upload_limit_mb=50,
        telegram_very_large_threshold_mb=150,
        telegram_resize_timeout_sec=180,
        bot_service_url="http://127.0.0.1:8000",
        worker_bot_callback_url="http://127.0.0.1:8090/internal/job-events",
        bot_callback_secret="secret",
    )


@pytest.fixture
def debug_app_config(app_config: AppConfig) -> AppConfig:
    return replace(app_config, debug=True)
