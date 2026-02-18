from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from file_lock import file_lock


class TelegramAccessStore:
    def __init__(
        self,
        *,
        authorized_chats_file: Path,
        whitelist_file: Path,
        lock_file: Path,
    ) -> None:
        self.authorized_chats_file = authorized_chats_file
        self.whitelist_file = whitelist_file
        self.lock_file = lock_file

    def _read_authorized_locked(self) -> set[int]:
        path = self.authorized_chats_file
        if not path.exists():
            return set()

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return set()

        raw_ids: Any = payload.get("authorized_chat_ids") if isinstance(payload, dict) else []
        if not isinstance(raw_ids, list):
            return set()

        result: set[int] = set()
        for value in raw_ids:
            try:
                result.add(int(value))
            except (TypeError, ValueError):
                continue
        return result

    def _write_authorized_locked(self, chat_ids: set[int]) -> None:
        payload = {
            "authorized_chat_ids": sorted(chat_ids),
        }
        self.authorized_chats_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.authorized_chats_file.with_name(f"{self.authorized_chats_file.name}.tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.authorized_chats_file)

    def _read_whitelist_locked(self) -> set[int]:
        path = self.whitelist_file
        if not path.exists():
            return set()

        result: set[int] = set()
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            return set()

        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            try:
                result.add(int(stripped))
            except ValueError:
                continue
        return result

    def _write_whitelist_locked(self, user_ids: set[int]) -> None:
        self.whitelist_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.whitelist_file.with_name(f"{self.whitelist_file.name}.tmp")
        lines = [str(user_id) for user_id in sorted(user_ids)]
        tmp_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
        tmp_path.replace(self.whitelist_file)

    def is_chat_authorized(self, chat_id: int) -> bool:
        with file_lock(self.lock_file):
            return int(chat_id) in self._read_authorized_locked()

    def authorize_chat(self, chat_id: int) -> bool:
        with file_lock(self.lock_file):
            current = self._read_authorized_locked()
            normalized = int(chat_id)
            if normalized in current:
                return False
            current.add(normalized)
            self._write_authorized_locked(current)
            return True

    def is_user_whitelisted(self, user_id: int) -> bool:
        with file_lock(self.lock_file):
            return int(user_id) in self._read_whitelist_locked()

    def add_user_to_whitelist(self, user_id: int) -> bool:
        with file_lock(self.lock_file):
            current = self._read_whitelist_locked()
            normalized = int(user_id)
            if normalized in current:
                return False
            current.add(normalized)
            self._write_whitelist_locked(current)
            return True

    def add_users_to_whitelist(self, user_ids: set[int]) -> int:
        with file_lock(self.lock_file):
            current = self._read_whitelist_locked()
            before = len(current)
            for user_id in user_ids:
                current.add(int(user_id))
            added = len(current) - before
            if added > 0:
                self._write_whitelist_locked(current)
            return added

    def snapshot_counts(self) -> dict[str, int]:
        with file_lock(self.lock_file):
            return {
                "authorized_chats": len(self._read_authorized_locked()),
                "whitelisted_users": len(self._read_whitelist_locked()),
            }
