from __future__ import annotations

import asyncio
from pathlib import Path
import tempfile

from telegram_access_store import TelegramAccessStore
from telegram_bot import TelegramBotRuntime


class FakeBot:
    def __init__(self) -> None:
        self.video_calls = 0
        self.document_calls = 0
        self.messages: list[str] = []
        self.fail_video = False
        self.fail_document = False

    async def send_video(self, **kwargs) -> None:
        self.video_calls += 1
        if self.fail_video:
            raise RuntimeError("video failed")

    async def send_document(self, **kwargs) -> None:
        self.document_calls += 1
        if self.fail_document:
            raise RuntimeError("document failed")

    async def send_message(self, **kwargs) -> None:
        self.messages.append(str(kwargs.get("text") or ""))


class FakeApp:
    def __init__(self, bot: FakeBot) -> None:
        self.bot = bot


def _store(tmp_path: Path | None = None) -> TelegramAccessStore:
    base = tmp_path or Path(tempfile.mkdtemp(prefix="clipdrop-test-access-"))
    return TelegramAccessStore(
        authorized_chats_file=base / "authorized.json",
        whitelist_file=base / "whitelist.txt",
        lock_file=base / ".access.lock",
    )


def test_done_event_falls_back_to_document(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    bot.fail_video = True

    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(tmp_path),
        auth_password="123",
        logger=__import__("logging").getLogger("test-bot"),
    )
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    asyncio.run(
        runtime.handle_job_event(
            {
                "event_id": "1:done:1",
                "job_id": "1",
                "status": "done",
                "platform": "x",
                "input_url": "https://x.com/u/status/1",
                "result": {"file_path": str(file_path)},
                "error": None,
                "subscribers": [{"chat_id": 1, "message_id": 1, "thread_id": None}],
            }
        )
    )

    assert bot.video_calls == 1
    assert bot.document_calls == 1


def test_callback_rejects_invalid_token() -> None:
    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(),
        auth_password="123",
        logger=__import__("logging").getLogger("test-bot"),
    )

    status, body = runtime.handle_callback_request(
        token="bad-token",
        payload={"event_id": "1:done:1", "status": "done"},
    )

    assert status == 401
    assert body["ok"] is False


def test_callback_idempotency() -> None:
    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(),
        auth_password="123",
        logger=__import__("logging").getLogger("test-bot"),
    )

    first_status, first_body = runtime.handle_callback_request(
        token="secret",
        payload={"event_id": "1:done:1", "status": "done", "subscribers": []},
    )
    second_status, second_body = runtime.handle_callback_request(
        token="secret",
        payload={"event_id": "1:done:1", "status": "done", "subscribers": []},
    )

    assert first_status == 200
    assert first_body["ok"] is True
    assert second_status == 200
    assert second_body["duplicate"] is True
