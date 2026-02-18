from __future__ import annotations

import asyncio
import logging
from pathlib import Path
import tempfile

from telegram import ReactionTypeEmoji
from telegram_access_store import TelegramAccessStore
from telegram_bot import TelegramBotRuntime


class FakeBot:
    def __init__(self) -> None:
        self.video_calls = 0
        self.document_calls = 0
        self.reaction_calls = 0
        self.messages: list[str] = []
        self.video_paths: list[str] = []
        self.document_paths: list[str] = []
        self.reaction_payloads: list[dict] = []
        self.fail_video = False
        self.fail_document = False
        self.fail_reaction = False

    async def send_video(self, **kwargs) -> None:
        self.video_calls += 1
        self.video_paths.append(str(getattr(kwargs.get("video"), "name", "")))
        if self.fail_video:
            raise RuntimeError("video failed")

    async def send_document(self, **kwargs) -> None:
        self.document_calls += 1
        self.document_paths.append(str(getattr(kwargs.get("document"), "name", "")))
        if self.fail_document:
            raise RuntimeError("document failed")

    async def send_message(self, **kwargs) -> None:
        self.messages.append(str(kwargs.get("text") or ""))

    async def set_message_reaction(self, **kwargs) -> None:
        self.reaction_calls += 1
        self.reaction_payloads.append(dict(kwargs))
        if self.fail_reaction:
            raise RuntimeError("reaction failed")


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


def _runtime(tmp_path: Path) -> TelegramBotRuntime:
    return TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(tmp_path),
        auth_password="123",
        logger=logging.getLogger("test-bot"),
    )


def _done_payload(file_path: Path) -> dict[str, object]:
    return {
        "event_id": "1:done:1",
        "job_id": "1",
        "status": "done",
        "platform": "x",
        "input_url": "https://x.com/u/status/1",
        "result": {"file_path": str(file_path)},
        "error": None,
        "subscribers": [{"chat_id": 1, "message_id": 1, "thread_id": None}],
    }


def _started_payload(*, event_id: str = "1:started:1") -> dict[str, object]:
    return {
        "event_id": event_id,
        "job_id": "1",
        "status": "started",
        "platform": "x",
        "input_url": "https://x.com/u/status/1",
        "result": None,
        "error": None,
        "subscribers": [{"chat_id": 1, "message_id": 1, "thread_id": None}],
    }


def _run_done(runtime: TelegramBotRuntime, file_path: Path) -> None:
    asyncio.run(runtime.handle_job_event(_done_payload(file_path)))


def test_started_event_sets_thumbs_up_reaction(tmp_path: Path) -> None:
    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    asyncio.run(runtime.handle_job_event(_started_payload()))

    assert bot.reaction_calls == 1
    reaction = bot.reaction_payloads[0]["reaction"]
    assert len(reaction) == 1
    assert isinstance(reaction[0], ReactionTypeEmoji)
    assert reaction[0].emoji == "👍"
    assert bot.reaction_payloads[0]["chat_id"] == 1
    assert bot.reaction_payloads[0]["message_id"] == 1


def test_started_event_deduplicates_reaction_calls_for_same_message(tmp_path: Path) -> None:
    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    asyncio.run(runtime.handle_job_event(_started_payload(event_id="1:started:1")))
    asyncio.run(runtime.handle_job_event(_started_payload(event_id="1:started:2")))

    assert bot.reaction_calls == 1


def test_done_event_sends_original_when_size_under_limit(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 1
    assert bot.document_calls == 0
    assert bot.video_paths[0].endswith("video.mp4")
    assert bot.messages == []


def test_done_event_falls_back_to_document(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    bot.fail_video = True

    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 1
    assert bot.document_calls == 1


def test_done_event_resizes_when_between_50_and_150_and_sends_resized(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    resized_path = runtime._make_resized_path(file_path)

    def fake_size(path: Path) -> float:
        if path == file_path:
            return 70.0
        if path == resized_path:
            return 40.0
        return 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        assert input_path == file_path
        assert output_path == resized_path
        assert target_mb == 50
        output_path.write_bytes(b"resized")
        return True, "ok"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 1
    assert bot.document_calls == 0
    assert bot.video_paths[0].endswith("_tg50.mp4")
    assert any("Стискаю до Telegram-ліміту" in text for text in bot.messages)


def test_done_event_rejects_when_over_150_with_user_message(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    resize_called = {"value": False}

    def fake_size(path: Path) -> float:
        return 160.0 if path == file_path else 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        resize_called["value"] = True
        return False, "should not run"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert resize_called["value"] is False
    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert any("Telegram Bot API дозволяє надсилати файли до 50MB" in text for text in bot.messages)


def test_done_event_resize_failure_notifies_user(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    def fake_size(path: Path) -> float:
        return 70.0 if path == file_path else 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        return False, "ffmpeg failed"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert any("Не вдалося стиснути файл до 50MB" in text for text in bot.messages)


def test_done_event_ffmpeg_missing_notifies_user(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    def fake_size(path: Path) -> float:
        return 70.0 if path == file_path else 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        return False, "ffmpeg not found in PATH"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert any("Не вдалося стиснути файл до 50MB" in text for text in bot.messages)


def test_done_event_resize_timeout_notifies_user(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    def fake_size(path: Path) -> float:
        return 70.0 if path == file_path else 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        return False, "ffmpeg timed out"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert any("Не вдалося стиснути файл до 50MB" in text for text in bot.messages)


def test_done_event_after_resize_still_over_limit_notifies_user(tmp_path: Path) -> None:
    file_path = tmp_path / "video.mp4"
    file_path.write_bytes(b"123")

    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]
    resized_path = runtime._make_resized_path(file_path)

    def fake_size(path: Path) -> float:
        if path == file_path:
            return 70.0
        if path == resized_path:
            return 52.0
        return 1.0

    async def fake_resize_video_to_limit(*, input_path: Path, output_path: Path, target_mb: int, timeout_sec: int) -> tuple[bool, str]:
        output_path.write_bytes(b"resized")
        return True, "ok"

    runtime._size_mb = fake_size  # type: ignore[assignment]
    runtime._resize_video_to_limit = fake_resize_video_to_limit  # type: ignore[assignment]

    _run_done(runtime, file_path)

    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert any("Не вдалося стиснути файл до 50MB" in text for text in bot.messages)


def test_callback_rejects_invalid_token() -> None:
    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(),
        auth_password="123",
        logger=logging.getLogger("test-bot"),
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
        logger=logging.getLogger("test-bot"),
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


def test_callback_accepts_started_status() -> None:
    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=_store(),
        auth_password="123",
        logger=logging.getLogger("test-bot"),
    )

    status, body = runtime.handle_callback_request(
        token="secret",
        payload={"event_id": "1:started:1", "status": "started", "subscribers": [{"chat_id": 1, "message_id": 1}]},
    )

    assert status == 200
    assert body["ok"] is True


def test_failed_event_sends_generic_user_message(tmp_path: Path) -> None:
    bot = FakeBot()
    runtime = _runtime(tmp_path)
    runtime.application = FakeApp(bot)  # type: ignore[assignment]

    asyncio.run(
        runtime.handle_job_event(
            {
                "event_id": "1:failed:2",
                "job_id": "1",
                "status": "failed",
                "platform": "x",
                "input_url": "https://x.com/u/status/1",
                "error": "Traceback: sensitive details",
                "subscribers": [{"chat_id": 1, "message_id": 1, "thread_id": None}],
            }
        )
    )

    assert bot.video_calls == 0
    assert bot.document_calls == 0
    assert len(bot.messages) == 1
    assert "can't download this video right now" in bot.messages[0]
    assert "Traceback:" not in bot.messages[0]
