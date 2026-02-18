from __future__ import annotations

import argparse
import asyncio
from collections import deque
import fcntl
import hmac
import json
import logging
import os
from pathlib import Path
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
from telegram import Update
from telegram.error import Conflict
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from config import load_config
from env import load_env_file
from logging_utils import setup_logger
from telegram_access_store import TelegramAccessStore
from url_extractors import ExtractedUrl, extract_supported_urls

GROUP_CHAT_TYPES = {"group", "supergroup"}
ADMIN_STATUSES = {"administrator", "creator"}
MAX_LINKS_PER_MESSAGE = 5


class TelegramBotRuntime:
    def __init__(
        self,
        *,
        service_base_url: str,
        callback_secret: str,
        callback_host: str,
        callback_port: int,
        access_store: TelegramAccessStore,
        auth_password: str,
        logger: logging.Logger,
        upload_limit_mb: int = 50,
        very_large_threshold_mb: int = 150,
        resize_timeout_sec: int = 180,
    ) -> None:
        self.service_base_url = service_base_url.rstrip("/")
        self.callback_secret = callback_secret
        self.callback_host = callback_host
        self.callback_port = callback_port
        self.access_store = access_store
        self.auth_password = auth_password
        self.logger = logger
        self.upload_limit_mb = max(1, int(upload_limit_mb))
        self.very_large_threshold_mb = max(self.upload_limit_mb, int(very_large_threshold_mb))
        self.resize_timeout_sec = max(10, int(resize_timeout_sec))

        self.application: Application | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        self._event_queue: deque[dict[str, Any]] = deque()
        self._event_queue_lock = threading.Lock()
        self._event_signal: asyncio.Event | None = None
        self._event_consumer_task: asyncio.Task[None] | None = None

        self._event_ids: set[str] = set()
        self._event_id_order: deque[str] = deque(maxlen=5000)
        self._event_id_lock = threading.Lock()

        self._http_server: ThreadingHTTPServer | None = None
        self._http_thread: threading.Thread | None = None

    async def start_background_components(self, app: Application) -> None:
        self.application = app
        self._loop = asyncio.get_running_loop()
        self._event_signal = asyncio.Event()
        self._event_consumer_task = asyncio.create_task(self._consume_events(), name="job-event-consumer")
        self._start_callback_server()

    async def stop_background_components(self) -> None:
        self._stop_callback_server()
        if self._event_consumer_task:
            self._event_consumer_task.cancel()
            try:
                await self._event_consumer_task
            except asyncio.CancelledError:
                pass
            self._event_consumer_task = None

    @staticmethod
    def _build_subscriber(message: Any) -> dict[str, Any]:
        return {
            "chat_id": int(message.chat_id),
            "message_id": int(message.message_id),
            "chat_type": str(message.chat.type),
            "thread_id": message.message_thread_id,
        }

    async def enqueue_jobs(self, *, urls: list[ExtractedUrl], message: Any) -> dict[str, Any]:
        url = f"{self.service_base_url}/jobs"
        payload = {
            "urls": [item.input_url for item in urls],
            "subscriber": self._build_subscriber(message),
        }
        timeout = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=20.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return dict(response.json())

    def _mark_event_seen(self, event_id: str) -> bool:
        with self._event_id_lock:
            if event_id in self._event_ids:
                return True
            if len(self._event_id_order) == self._event_id_order.maxlen:
                dropped = self._event_id_order.popleft()
                self._event_ids.discard(dropped)
            self._event_id_order.append(event_id)
            self._event_ids.add(event_id)
            return False

    def handle_callback_request(
        self,
        *,
        token: str | None,
        payload: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        if token != self.callback_secret:
            return 401, {"ok": False, "error": "unauthorized"}

        event_id = str(payload.get("event_id") or "").strip()
        if not event_id:
            return 400, {"ok": False, "error": "missing event_id"}

        status = str(payload.get("status") or "").lower()
        if status not in {"done", "failed"}:
            return 400, {"ok": False, "error": "invalid status"}

        if self._mark_event_seen(event_id):
            return 200, {"ok": True, "duplicate": True}

        with self._event_queue_lock:
            self._event_queue.append(payload)

        if self._loop and self._event_signal:
            self._loop.call_soon_threadsafe(self._event_signal.set)

        return 200, {"ok": True}

    async def _consume_events(self) -> None:
        assert self._event_signal is not None
        while True:
            await self._event_signal.wait()
            self._event_signal.clear()

            while True:
                with self._event_queue_lock:
                    event = self._event_queue.popleft() if self._event_queue else None
                if event is None:
                    break
                try:
                    await self.handle_job_event(event)
                except Exception as exc:  # noqa: BLE001
                    self.logger.exception("Failed handling callback event error=%s", exc)

    async def handle_job_event(self, payload: dict[str, Any]) -> None:
        if not self.application:
            raise RuntimeError("Telegram application is not initialized")

        status = str(payload.get("status") or "").lower()
        subscribers = list(payload.get("subscribers") or [])
        if not subscribers:
            self.logger.warning("Callback has no subscribers job_id=%s", payload.get("job_id"))
            return

        if status == "done":
            await self._handle_done_event(payload, subscribers)
        else:
            await self._handle_failed_event(payload, subscribers)

    @staticmethod
    def _size_mb(file_path: Path) -> float:
        return file_path.stat().st_size / (1024 * 1024)

    def _make_resized_path(self, file_path: Path) -> Path:
        return file_path.with_name(f"{file_path.stem}_tg{self.upload_limit_mb}.mp4")

    @staticmethod
    async def _probe_duration_sec(file_path: Path) -> float | None:
        try:
            process = await asyncio.create_subprocess_exec(
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(file_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            return None

        stdout, _ = await process.communicate()
        if process.returncode != 0:
            return None

        raw = stdout.decode("utf-8", errors="ignore").strip()
        if not raw:
            return None
        try:
            duration = float(raw)
        except ValueError:
            return None
        if duration <= 0:
            return None
        return duration

    async def _resize_video_to_limit(
        self,
        *,
        input_path: Path,
        output_path: Path,
        target_mb: int,
        timeout_sec: int,
    ) -> tuple[bool, str]:
        duration = await self._probe_duration_sec(input_path)
        target_bytes = int(target_mb * 1024 * 1024 * 0.95)
        if duration and duration > 0:
            total_bitrate = int((target_bytes * 8) / duration)
        else:
            total_bitrate = 800_000

        audio_bitrate = min(128_000, max(64_000, int(total_bitrate * 0.15)))
        video_bitrate = max(200_000, total_bitrate - audio_bitrate)
        maxrate = int(video_bitrate * 1.2)
        bufsize = maxrate * 2

        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            str(input_path),
            "-c:v",
            "libx264",
            "-b:v",
            str(video_bitrate),
            "-maxrate",
            str(maxrate),
            "-bufsize",
            str(bufsize),
            "-preset",
            "veryfast",
            "-c:a",
            "aac",
            "-b:a",
            str(audio_bitrate),
            "-movflags",
            "+faststart",
            str(output_path),
        ]

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            return False, "ffmpeg not found in PATH"

        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=float(timeout_sec))
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            return False, f"ffmpeg timed out after {timeout_sec}s"

        if process.returncode != 0:
            short = stderr.decode("utf-8", errors="ignore")[-500:]
            return False, f"ffmpeg failed: {short}"

        if not output_path.exists() or not output_path.is_file():
            return False, "ffmpeg did not produce output file"

        if self._size_mb(output_path) > target_mb:
            return False, "resized file is still above Telegram upload limit"

        return True, "ok"

    async def _handle_done_event(self, payload: dict[str, Any], subscribers: list[dict[str, Any]]) -> None:
        assert self.application is not None

        result = dict(payload.get("result") or {})
        file_path_raw = str(result.get("file_path") or "")
        file_path = Path(file_path_raw) if file_path_raw else None
        if not file_path or not file_path.exists() or not file_path.is_file():
            await self._broadcast_text(
                subscribers,
                f"Downloaded file missing for job {payload.get('job_id')}",
            )
            return

        final_file_path = file_path
        original_size_mb = self._size_mb(file_path)
        if original_size_mb > self.very_large_threshold_mb:
            await self._broadcast_text(
                subscribers,
                (
                    "Файл дуже великий. Telegram Bot API дозволяє надсилати файли до "
                    f"{self.upload_limit_mb}MB, цей файл перевищує поріг для авто-стискання."
                ),
            )
            return

        if original_size_mb > self.upload_limit_mb:
            await self._broadcast_text(
                subscribers,
                f"Файл більший за {self.upload_limit_mb}MB. Стискаю до Telegram-ліміту, зачекайте.",
            )
            resized_path = self._make_resized_path(file_path)
            ok, details = await self._resize_video_to_limit(
                input_path=file_path,
                output_path=resized_path,
                target_mb=self.upload_limit_mb,
                timeout_sec=self.resize_timeout_sec,
            )
            if not ok:
                self.logger.warning(
                    "Resize failed job_id=%s path=%s error=%s",
                    payload.get("job_id"),
                    file_path,
                    details,
                )
                await self._broadcast_text(
                    subscribers,
                    "Не вдалося стиснути файл до 50MB для відправки в Telegram.",
                )
                return

            if self._size_mb(resized_path) > self.upload_limit_mb:
                await self._broadcast_text(
                    subscribers,
                    "Не вдалося стиснути файл до 50MB для відправки в Telegram.",
                )
                return
            final_file_path = resized_path

        caption = f"{payload.get('platform')}: {payload.get('input_url')}"
        for sub in subscribers:
            chat_id = int(sub["chat_id"])
            thread_id = sub.get("thread_id")
            try:
                with final_file_path.open("rb") as fh:
                    await self.application.bot.send_video(
                        chat_id=chat_id,
                        video=fh,
                        caption=caption,
                        message_thread_id=thread_id,
                        supports_streaming=True,
                    )
                continue
            except Exception as video_exc:  # noqa: BLE001
                self.logger.warning(
                    "sendVideo failed chat_id=%s job_id=%s error=%s",
                    chat_id,
                    payload.get("job_id"),
                    video_exc,
                )

            try:
                with final_file_path.open("rb") as fh:
                    await self.application.bot.send_document(
                        chat_id=chat_id,
                        document=fh,
                        caption=caption,
                        message_thread_id=thread_id,
                    )
            except Exception as doc_exc:  # noqa: BLE001
                short_error = str(doc_exc)[-700:]
                text = (
                    f"Failed to upload downloaded media for job {payload.get('job_id')}.\n"
                    f"{short_error}"
                )
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    message_thread_id=thread_id,
                )

    async def _handle_failed_event(self, payload: dict[str, Any], subscribers: list[dict[str, Any]]) -> None:
        error_text = str(payload.get("error") or "Unknown error")[-1200:]
        text = f"Failed to download: {payload.get('input_url')}\n{error_text}"
        await self._broadcast_text(subscribers, text)

    async def _broadcast_text(self, subscribers: list[dict[str, Any]], text: str) -> None:
        assert self.application is not None
        for sub in subscribers:
            await self.application.bot.send_message(
                chat_id=int(sub["chat_id"]),
                text=text,
                message_thread_id=sub.get("thread_id"),
            )

    def _start_callback_server(self) -> None:
        if self._http_server is not None:
            return

        runtime = self

        class CallbackHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                if self.path != "/internal/job-events":
                    self.send_response(404)
                    self.end_headers()
                    return

                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length)
                try:
                    payload = json.loads(raw_body.decode("utf-8"))
                except Exception:
                    self._send_json(400, {"ok": False, "error": "invalid JSON"})
                    return

                token = self.headers.get("X-Internal-Token")
                status_code, response = runtime.handle_callback_request(token=token, payload=payload)
                self._send_json(status_code, response)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
                return

            def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._http_server = ThreadingHTTPServer((self.callback_host, self.callback_port), CallbackHandler)
        self._http_thread = threading.Thread(
            target=self._http_server.serve_forever,
            kwargs={"poll_interval": 0.5},
            daemon=True,
            name="bot-callback-http",
        )
        self._http_thread.start()
        self.logger.info("Callback HTTP server started on %s:%s", self.callback_host, self.callback_port)

    def _stop_callback_server(self) -> None:
        if self._http_server is None:
            return
        self._http_server.shutdown()
        self._http_server.server_close()
        if self._http_thread:
            self._http_thread.join(timeout=3)
        self._http_server = None
        self._http_thread = None
        self.logger.info("Callback HTTP server stopped")


def acquire_single_instance_lock(lock_file: Path) -> object:
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_fd = lock_file.open("a+", encoding="utf-8")
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise RuntimeError(f"Another telegram-bot instance is already running (lock: {lock_file}).") from exc
    return lock_fd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telegram bot for TikTok/Instagram/X downloader")
    parser.add_argument("--service-url", default=os.getenv("BOT_SERVICE_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--callback-host", default=os.getenv("TELEGRAM_CALLBACK_HOST", "127.0.0.1"))
    parser.add_argument("--callback-port", type=int, default=int(os.getenv("TELEGRAM_CALLBACK_PORT", "8090")))
    parser.add_argument("--callback-secret", default=os.getenv("BOT_CALLBACK_SECRET", "change-me"))
    parser.add_argument("--lock-file", default=os.getenv("TELEGRAM_LOCK_FILE", ".telegram_bot.lock"))
    return parser.parse_args()


async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime: TelegramBotRuntime = context.application.bot_data["runtime"]
    message = update.message
    user = update.effective_user
    if message is None or user is None:
        return

    chat_type = str(message.chat.type)
    if chat_type not in GROUP_CHAT_TYPES:
        await message.reply_text("/auth works only in group or supergroup chats.")
        return

    args = list(context.args or [])
    if len(args) != 1:
        await message.reply_text("Usage: /auth <password>")
        return

    given_password = args[0]
    if not hmac.compare_digest(given_password, runtime.auth_password):
        await message.reply_text("Wrong password.")
        return

    try:
        member = await context.bot.get_chat_member(chat_id=message.chat_id, user_id=user.id)
    except Exception as exc:  # noqa: BLE001
        runtime.logger.error("Failed to verify admin status chat_id=%s user_id=%s error=%s", message.chat_id, user.id, exc)
        await message.reply_text("Failed to verify admin status. Try again later.")
        return

    status = str(getattr(member, "status", "")).lower()
    if status not in ADMIN_STATUSES:
        await message.reply_text("Only chat admins can authorize this chat.")
        return

    try:
        newly_authorized = runtime.access_store.authorize_chat(int(message.chat_id))
    except Exception as exc:  # noqa: BLE001
        runtime.logger.error("Failed to save authorized chat chat_id=%s error=%s", message.chat_id, exc)
        await message.reply_text("Failed to persist authorization state.")
        return

    try:
        admins = await context.bot.get_chat_administrators(chat_id=message.chat_id)
        admin_user_ids = {
            int(admin.user.id)
            for admin in admins
            if getattr(admin, "user", None) is not None and not bool(getattr(admin.user, "is_bot", False))
        }
        added_admins = runtime.access_store.add_users_to_whitelist(admin_user_ids)
        counts = runtime.access_store.snapshot_counts()
    except Exception as exc:  # noqa: BLE001
        runtime.logger.error("Failed to sync admin whitelist chat_id=%s error=%s", message.chat_id, exc)
        await message.reply_text(
            "Chat authorized, but failed to sync admins into whitelist. "
            "Users will be added when they send messages in this chat."
        )
        return

    prefix = "Chat authorized." if newly_authorized else "Chat already authorized."
    await message.reply_text(
        f"{prefix}\n"
        f"Added admins to whitelist: {added_admins}\n"
        f"Authorized chats: {counts['authorized_chats']}\n"
        f"Whitelisted users: {counts['whitelisted_users']}"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime: TelegramBotRuntime = context.application.bot_data["runtime"]
    message = update.message
    user = update.effective_user
    if message is None or user is None:
        return

    chat_type = str(message.chat.type)
    if chat_type == "private":
        try:
            if not runtime.access_store.is_user_whitelisted(user.id):
                await message.reply_text("Access denied.")
                return
        except Exception as exc:  # noqa: BLE001
            runtime.logger.error("Access store read failed user_id=%s error=%s", user.id, exc)
            await message.reply_text("Access check failed. Try later.")
            return

    if chat_type in GROUP_CHAT_TYPES:
        try:
            if not runtime.access_store.is_chat_authorized(message.chat_id):
                return
        except Exception as exc:  # noqa: BLE001
            runtime.logger.error("Access store read failed chat_id=%s error=%s", message.chat_id, exc)
            return

    await message.reply_text(
        "Send TikTok/Instagram/X links. "
        "I will download and return media when ready."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime: TelegramBotRuntime = context.application.bot_data["runtime"]

    message = update.message
    user = update.effective_user
    if message is None or user is None:
        return

    chat_type = str(message.chat.type)
    if chat_type == "private":
        try:
            if not runtime.access_store.is_user_whitelisted(user.id):
                await message.reply_text("Access denied.")
                return
        except Exception as exc:  # noqa: BLE001
            runtime.logger.error("Access store read failed user_id=%s error=%s", user.id, exc)
            await message.reply_text("Access check failed. Try later.")
            return
    elif chat_type in GROUP_CHAT_TYPES:
        try:
            if not runtime.access_store.is_chat_authorized(message.chat_id):
                return
            runtime.access_store.add_user_to_whitelist(user.id)
        except Exception as exc:  # noqa: BLE001
            runtime.logger.error(
                "Access store failure chat_id=%s user_id=%s error=%s",
                message.chat_id,
                user.id,
                exc,
            )
            await message.reply_text("Access check failed. Try later.")
            return
    else:
        return

    text_candidates = [message.text or "", message.caption or ""]
    extracted: list[ExtractedUrl] = []
    for text in text_candidates:
        extracted.extend(extract_supported_urls(text))

    if not extracted:
        return

    by_norm: dict[str, ExtractedUrl] = {}
    for item in extracted:
        by_norm.setdefault(item.normalized_url, item)
    deduped = list(by_norm.values())
    selected = deduped

    if len(deduped) > MAX_LINKS_PER_MESSAGE:
        await message.reply_text(
            f"Found {len(deduped)} links. Downloading first {MAX_LINKS_PER_MESSAGE} only."
        )
        selected = deduped[:MAX_LINKS_PER_MESSAGE]

    try:
        response = await runtime.enqueue_jobs(urls=selected, message=message)
        runtime.logger.info(
            "Message queued chat_id=%s message_id=%s found_links=%s selected_links=%s jobs=%s",
            message.chat_id,
            message.message_id,
            len(deduped),
            len(selected),
            len(response.get("jobs") or []),
        )
    except Exception as exc:  # noqa: BLE001
        runtime.logger.error("Failed enqueue chat_id=%s error=%s", message.chat_id, exc)
        await message.reply_text(f"Failed to enqueue links: {exc}")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    runtime: TelegramBotRuntime = context.application.bot_data["runtime"]
    if isinstance(context.error, Conflict):
        runtime.logger.error(
            "Telegram Conflict: another getUpdates consumer uses this token. "
            "Stop the other bot instance and restart."
        )
        context.application.stop_running()
        return
    runtime.logger.exception("Unhandled telegram bot error: %s", context.error)


def main() -> None:
    load_env_file(Path(".env"))
    args = parse_args()
    cfg = load_config()

    token = cfg.telegram_bot_token
    if not token:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN in environment or .env")
    if not cfg.telegram_auth_password:
        raise RuntimeError("Set TELEGRAM_AUTH_PASSWORD in environment or .env")

    logger = setup_logger("telegram-bot", cfg.debug)

    access_store = TelegramAccessStore(
        authorized_chats_file=cfg.telegram_authorized_chats_file,
        whitelist_file=cfg.telegram_whitelist_file,
        lock_file=cfg.telegram_access_lock_file,
    )

    runtime = TelegramBotRuntime(
        service_base_url=args.service_url,
        callback_secret=args.callback_secret,
        callback_host=args.callback_host,
        callback_port=int(args.callback_port),
        access_store=access_store,
        auth_password=cfg.telegram_auth_password,
        logger=logger,
        upload_limit_mb=cfg.telegram_upload_limit_mb,
        very_large_threshold_mb=cfg.telegram_very_large_threshold_mb,
        resize_timeout_sec=cfg.telegram_resize_timeout_sec,
    )

    lock_fd = acquire_single_instance_lock(Path(args.lock_file))

    async def post_init(app: Application) -> None:
        await runtime.start_background_components(app)

    async def post_shutdown(_: Application) -> None:
        await runtime.stop_background_components()

    app = (
        ApplicationBuilder()
        .token(token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["runtime"] = runtime

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("auth", auth_command))
    app.add_handler(MessageHandler((filters.TEXT | filters.CAPTION) & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        try:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fd.close()


if __name__ == "__main__":
    main()
