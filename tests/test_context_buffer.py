from __future__ import annotations

from datetime import datetime, timezone

from telegram_bot import ChatHistoryBuffer, _trim_context_photos


class FakePhoto:
    def __init__(self, file_id: str) -> None:
        self.file_id = file_id


class FakeMessage:
    def __init__(
        self,
        *,
        message_id: int,
        text: str | None = None,
        caption: str | None = None,
        photo_ids: list[str] | None = None,
        has_video: bool = False,
    ) -> None:
        self.message_id = message_id
        self.text = text
        self.caption = caption
        self.photo = [FakePhoto(value) for value in (photo_ids or [])]
        self.video = object() if has_video else None
        self.video_note = None
        self.animation = None
        self.date = datetime.now(timezone.utc)


def test_buffer_keeps_only_text_or_photo() -> None:
    buffer = ChatHistoryBuffer(max_per_chat=50)

    buffer.push(chat_id=1, message=FakeMessage(message_id=1, text="hi"), user_id=10)
    buffer.push(chat_id=1, message=FakeMessage(message_id=2, has_video=True), user_id=10)
    buffer.push(chat_id=1, message=FakeMessage(message_id=3, photo_ids=["p1", "p2"]), user_id=10)

    rows = buffer.get_recent(chat_id=1, limit=10)

    assert len(rows) == 2
    assert rows[0]["message_id"] == 1
    assert rows[1]["message_id"] == 3
    assert rows[1]["photo_file_id"] == "p2"


def test_buffer_returns_last_ten_in_order() -> None:
    buffer = ChatHistoryBuffer(max_per_chat=50)

    for idx in range(1, 15):
        buffer.push(chat_id=2, message=FakeMessage(message_id=idx, text=f"m{idx}"), user_id=20)

    rows = buffer.get_recent(chat_id=2, limit=10)

    assert len(rows) == 10
    assert rows[0]["message_id"] == 5
    assert rows[-1]["message_id"] == 14


def test_trim_context_photos_keeps_newest_ten() -> None:
    rows = [
        {
            "message_id": idx,
            "user_id": 1,
            "created_at": "2026-02-18T00:00:00+00:00",
            "text": None,
            "photo_file_id": f"photo_{idx}",
            "has_video": False,
        }
        for idx in range(1, 13)
    ]

    trimmed = _trim_context_photos(rows, limit=10)

    assert len(trimmed) == 10
    assert trimmed[0]["message_id"] == 3
    assert trimmed[-1]["message_id"] == 12
    assert sum(1 for row in trimmed if row.get("photo_file_id")) == 10
