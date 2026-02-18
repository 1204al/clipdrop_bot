from __future__ import annotations

from pathlib import Path

from telegram_access_store import TelegramAccessStore


def _store(tmp_path: Path) -> TelegramAccessStore:
    return TelegramAccessStore(
        authorized_chats_file=tmp_path / "telegram_authorized_chats.json",
        whitelist_file=tmp_path / "telegram_whitelist.txt",
        lock_file=tmp_path / ".telegram_access.lock",
    )


def test_authorized_chats_persist_and_load(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert store.authorize_chat(-1001) is True
    assert store.authorize_chat(-1001) is False
    assert store.is_chat_authorized(-1001) is True

    reloaded = _store(tmp_path)
    assert reloaded.is_chat_authorized(-1001) is True


def test_whitelist_persist_and_load(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert store.add_user_to_whitelist(10) is True
    assert store.add_user_to_whitelist(10) is False
    assert store.is_user_whitelisted(10) is True

    reloaded = _store(tmp_path)
    assert reloaded.is_user_whitelisted(10) is True


def test_add_users_to_whitelist_is_idempotent(tmp_path: Path) -> None:
    store = _store(tmp_path)

    assert store.add_users_to_whitelist({1, 2, 3}) == 3
    assert store.add_users_to_whitelist({2, 3, 4}) == 1

    counts = store.snapshot_counts()
    assert counts["whitelisted_users"] == 4
