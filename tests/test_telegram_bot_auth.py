from __future__ import annotations

import asyncio
from pathlib import Path

from telegram_access_store import TelegramAccessStore
from telegram_bot import TelegramBotRuntime, auth_command


class FakeUser:
    def __init__(self, user_id: int, *, is_bot: bool = False) -> None:
        self.id = user_id
        self.is_bot = is_bot


class FakeChat:
    def __init__(self, chat_type: str) -> None:
        self.type = chat_type


class FakeMessage:
    def __init__(self, *, chat_id: int, chat_type: str, user_id: int) -> None:
        self.chat_id = chat_id
        self.chat = FakeChat(chat_type)
        self.message_id = 1
        self.message_thread_id = None
        self._user_id = user_id
        self.replies: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class FakeUpdate:
    def __init__(self, message: FakeMessage, user: FakeUser) -> None:
        self.message = message
        self.effective_user = user


class FakeMember:
    def __init__(self, status: str, user: FakeUser | None = None) -> None:
        self.status = status
        self.user = user or FakeUser(0)


class FakeBot:
    def __init__(self, *, caller_status: str = "administrator", admin_ids: list[int] | None = None) -> None:
        self.caller_status = caller_status
        self.admin_ids = admin_ids or [11, 12]

    async def get_chat_member(self, *, chat_id: int, user_id: int) -> FakeMember:
        return FakeMember(self.caller_status, FakeUser(user_id))

    async def get_chat_administrators(self, *, chat_id: int) -> list[FakeMember]:
        return [FakeMember("administrator", FakeUser(user_id)) for user_id in self.admin_ids]


class FakeApplication:
    def __init__(self, runtime: TelegramBotRuntime) -> None:
        self.bot_data = {"runtime": runtime}


class FakeContext:
    def __init__(self, *, runtime: TelegramBotRuntime, bot: FakeBot, args: list[str]) -> None:
        self.application = FakeApplication(runtime)
        self.bot = bot
        self.args = args


def _runtime(tmp_path: Path, password: str = "123") -> tuple[TelegramBotRuntime, TelegramAccessStore]:
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
        auth_password=password,
        logger=__import__("logging").getLogger("test-auth"),
    )
    return runtime, store


def test_auth_success_for_admin(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path, password="123")
    message = FakeMessage(chat_id=-10055, chat_type="group", user_id=11)
    user = FakeUser(11)
    update = FakeUpdate(message, user)
    context = FakeContext(runtime=runtime, bot=FakeBot(caller_status="administrator", admin_ids=[11, 22]), args=["123"])

    asyncio.run(auth_command(update, context))

    assert store.is_chat_authorized(-10055) is True
    assert store.is_user_whitelisted(11) is True
    assert store.is_user_whitelisted(22) is True
    assert any("authorized" in reply.lower() for reply in message.replies)


def test_auth_rejected_for_non_admin(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    message = FakeMessage(chat_id=-10066, chat_type="group", user_id=10)
    update = FakeUpdate(message, FakeUser(10))
    context = FakeContext(runtime=runtime, bot=FakeBot(caller_status="member"), args=["123"])

    asyncio.run(auth_command(update, context))

    assert store.is_chat_authorized(-10066) is False
    assert any("only chat admins" in reply.lower() for reply in message.replies)


def test_auth_rejected_for_wrong_password(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    message = FakeMessage(chat_id=-10077, chat_type="group", user_id=10)
    update = FakeUpdate(message, FakeUser(10))
    context = FakeContext(runtime=runtime, bot=FakeBot(caller_status="administrator"), args=["bad"])

    asyncio.run(auth_command(update, context))

    assert store.is_chat_authorized(-10077) is False
    assert any("wrong password" in reply.lower() for reply in message.replies)


def test_auth_rejected_in_private_chat(tmp_path: Path) -> None:
    runtime, store = _runtime(tmp_path)
    message = FakeMessage(chat_id=999, chat_type="private", user_id=10)
    update = FakeUpdate(message, FakeUser(10))
    context = FakeContext(runtime=runtime, bot=FakeBot(caller_status="administrator"), args=["123"])

    asyncio.run(auth_command(update, context))

    assert store.is_chat_authorized(999) is False
    assert any("only in group" in reply.lower() for reply in message.replies)
