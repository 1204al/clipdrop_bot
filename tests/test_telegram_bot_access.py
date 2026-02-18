from __future__ import annotations

import asyncio
from pathlib import Path

from telegram_access_store import TelegramAccessStore
from telegram_bot import TelegramBotRuntime, handle_message


class FakeUser:
    def __init__(self, user_id: int) -> None:
        self.id = user_id


class FakeChat:
    def __init__(self, chat_type: str) -> None:
        self.type = chat_type


class FakeMessage:
    def __init__(self, *, chat_id: int, chat_type: str, user_id: int, text: str) -> None:
        self.chat_id = chat_id
        self.chat = FakeChat(chat_type)
        self.message_id = 1
        self.message_thread_id = None
        self.text = text
        self.caption = None
        self.replies: list[str] = []
        self._user_id = user_id

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class FakeUpdate:
    def __init__(self, message: FakeMessage, user: FakeUser) -> None:
        self.message = message
        self.effective_user = user


class FakeApplication:
    def __init__(self, runtime: TelegramBotRuntime) -> None:
        self.bot_data = {"runtime": runtime}


class FakeContext:
    def __init__(self, runtime: TelegramBotRuntime) -> None:
        self.application = FakeApplication(runtime)


def _runtime(tmp_path: Path) -> tuple[TelegramBotRuntime, TelegramAccessStore]:
    store = TelegramAccessStore(
        authorized_chats_file=tmp_path / "authorized.json",
        whitelist_file=tmp_path / "whitelist.txt",
        lock_file=tmp_path / ".access.lock",
    )
    runtime = TelegramBotRuntime(
        service_base_url="http://127.0.0.1:8000",
        callback_secret="secret",
        callback_host="127.0.0.1",
        callback_port=8090,
        access_store=store,
        auth_password="123",
        logger=__import__("logging").getLogger("test-access"),
    )
    return runtime, store


def test_unauthorized_group_message_is_ignored(tmp_path: Path) -> None:
    runtime, _ = _runtime(tmp_path)
    called = {"enqueue": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        return {"jobs": []}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=-1001,
        chat_type="group",
        user_id=10,
        text="https://x.com/test/status/123",
    )
    update = FakeUpdate(msg, FakeUser(10))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 0
    assert msg.replies == []


def test_authorized_group_message_allowed_and_user_whitelisted(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    store.authorize_chat(-1002)
    called = {"enqueue": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        return {"jobs": [{"job_id": "1"}]}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=-1002,
        chat_type="group",
        user_id=21,
        text="https://www.instagram.com/p/DUIvX5LEUZp/",
    )
    update = FakeUpdate(msg, FakeUser(21))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 1
    assert store.is_user_whitelisted(21) is True


def test_private_whitelisted_user_allowed(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    store.add_user_to_whitelist(31)
    called = {"enqueue": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        return {"jobs": [{"job_id": "1"}]}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=31,
        chat_type="private",
        user_id=31,
        text="https://vt.tiktok.com/ZSmDoVEBm",
    )
    update = FakeUpdate(msg, FakeUser(31))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 1


def test_private_non_whitelist_user_denied(tmp_path: Path) -> None:
    runtime, _ = _runtime(tmp_path)
    called = {"enqueue": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        return {"jobs": [{"job_id": "1"}]}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=41,
        chat_type="private",
        user_id=41,
        text="https://x.com/FrontendMasters/status/2023797282978607430",
    )
    update = FakeUpdate(msg, FakeUser(41))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 0
    assert any("access denied" in reply.lower() for reply in msg.replies)


def test_authorized_group_message_limits_to_first_five_links(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    store.authorize_chat(-1003)
    called = {"enqueue": 0, "url_count": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        called["url_count"] = len(urls)
        return {"jobs": [{"job_id": str(idx)} for idx, _ in enumerate(urls, start=1)]}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=-1003,
        chat_type="group",
        user_id=22,
        text="\n".join(
            [
                "https://x.com/u1/status/101",
                "https://x.com/u2/status/102",
                "https://x.com/u3/status/103",
                "https://x.com/u4/status/104",
                "https://x.com/u5/status/105",
                "https://x.com/u6/status/106",
            ]
        ),
    )
    update = FakeUpdate(msg, FakeUser(22))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 1
    assert called["url_count"] == 5
    assert any("downloading first 5 only" in reply.lower() for reply in msg.replies)


def test_private_whitelisted_message_limits_to_first_five_links(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    store.add_user_to_whitelist(31)
    called = {"enqueue": 0, "url_count": 0}

    async def fake_enqueue_jobs(*, urls, message):
        called["enqueue"] += 1
        called["url_count"] = len(urls)
        return {"jobs": [{"job_id": str(idx)} for idx, _ in enumerate(urls, start=1)]}

    runtime.enqueue_jobs = fake_enqueue_jobs  # type: ignore[assignment]

    msg = FakeMessage(
        chat_id=31,
        chat_type="private",
        user_id=31,
        text="\n".join(
            [
                "https://vt.tiktok.com/ZSmDoVEBm",
                "https://www.instagram.com/p/DUIvX5LEUZp/",
                "https://x.com/FrontendMasters/status/2023797282978607430",
                "https://x.com/u4/status/204",
                "https://x.com/u5/status/205",
                "https://x.com/u6/status/206",
                "https://x.com/u7/status/207",
            ]
        ),
    )
    update = FakeUpdate(msg, FakeUser(31))
    context = FakeContext(runtime)

    asyncio.run(handle_message(update, context))

    assert called["enqueue"] == 1
    assert called["url_count"] == 5
    assert any("downloading first 5 only" in reply.lower() for reply in msg.replies)
