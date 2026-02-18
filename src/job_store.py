from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
import uuid

from file_lock import file_lock
from url_extractors import ExtractedUrl

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_DONE = "done"
STATUS_FAILED = "failed"
ACTIVE_STATUSES = {STATUS_QUEUED, STATUS_RUNNING}


class JobStore:
    def __init__(
        self,
        *,
        queue_file: Path,
        results_file: Path,
        lock_file: Path,
        max_attempts: int,
        compact_after_lines: int = 1000,
    ) -> None:
        self.queue_file = queue_file
        self.results_file = results_file
        self.lock_file = lock_file
        self.max_attempts = max(1, int(max_attempts))
        self.compact_after_lines = max(100, int(compact_after_lines))

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []

        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
        return rows

    def _append_jsonl(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _count_lines(self, path: Path) -> int:
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as f:
            return sum(1 for _ in f)

    def _materialize_jobs_locked(self) -> dict[str, dict[str, Any]]:
        jobs: dict[str, dict[str, Any]] = {}
        for row in self._read_jsonl(self.queue_file):
            job_id = str(row.get("job_id") or "")
            if not job_id:
                continue
            jobs[job_id] = row
        return jobs

    def _compact_latest_by_job_id(self, path: Path) -> None:
        rows = self._read_jsonl(path)
        if not rows:
            return

        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            job_id = str(row.get("job_id") or "")
            if not job_id:
                continue
            latest[job_id] = row

        compacted = sorted(
            latest.values(),
            key=lambda row: (
                str(row.get("created_at") or ""),
                str(row.get("updated_at") or ""),
                str(row.get("job_id") or ""),
            ),
        )

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            for row in compacted:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        tmp_path.replace(path)

    def _maybe_compact_locked(self) -> None:
        if self._count_lines(self.queue_file) > self.compact_after_lines:
            self._compact_latest_by_job_id(self.queue_file)
        if self._count_lines(self.results_file) > self.compact_after_lines:
            self._compact_latest_by_job_id(self.results_file)

    @staticmethod
    def _same_subscriber(a: dict[str, Any], b: dict[str, Any]) -> bool:
        return (
            int(a.get("chat_id") or 0) == int(b.get("chat_id") or 0)
            and int(a.get("message_id") or 0) == int(b.get("message_id") or 0)
            and (a.get("thread_id") if a.get("thread_id") is not None else None)
            == (b.get("thread_id") if b.get("thread_id") is not None else None)
        )

    def _append_subscriber_if_missing(
        self,
        subscribers: list[dict[str, Any]],
        subscriber: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], bool]:
        for existing in subscribers:
            if self._same_subscriber(existing, subscriber):
                return subscribers, False
        updated = list(subscribers)
        updated.append(subscriber)
        return updated, True

    def enqueue_many(
        self,
        inputs: list[ExtractedUrl],
        *,
        subscriber: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if not inputs:
            return []

        now = self._now()
        subscriber_row = {
            "chat_id": int(subscriber["chat_id"]),
            "message_id": int(subscriber["message_id"]),
            "thread_id": subscriber.get("thread_id"),
            "requested_at": now,
        }

        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            active_by_url: dict[str, dict[str, Any]] = {
                str(job.get("normalized_url") or ""): job
                for job in jobs_by_id.values()
                if str(job.get("status") or "") in ACTIVE_STATUSES
            }

            output_rows: list[dict[str, Any]] = []
            for item in inputs:
                existing = active_by_url.get(item.normalized_url)
                if existing:
                    existing_subscribers = list(existing.get("subscribers") or [])
                    merged_subscribers, changed = self._append_subscriber_if_missing(
                        existing_subscribers,
                        subscriber_row,
                    )
                    if changed:
                        updated = dict(existing)
                        updated["subscribers"] = merged_subscribers
                        updated["updated_at"] = self._now()
                        self._append_jsonl(self.queue_file, updated)
                        active_by_url[item.normalized_url] = updated
                        existing = updated

                    output_rows.append(
                        {
                            "job_id": str(existing["job_id"]),
                            "status": str(existing["status"]),
                            "deduplicated": True,
                            "input_url": str(existing["input_url"]),
                            "normalized_url": str(existing["normalized_url"]),
                            "platform": str(existing["platform"]),
                        }
                    )
                    continue

                job_id = str(uuid.uuid4())
                job = {
                    "job_id": job_id,
                    "input_url": item.input_url,
                    "normalized_url": item.normalized_url,
                    "platform": item.platform.value,
                    "status": STATUS_QUEUED,
                    "attempts": 0,
                    "max_attempts": self.max_attempts,
                    "created_at": now,
                    "updated_at": now,
                    "result": None,
                    "error": None,
                    "subscribers": [subscriber_row],
                    "notification": {
                        "last_event_id": None,
                        "callback_attempts": 0,
                        "callback_error": None,
                    },
                }
                self._append_jsonl(self.queue_file, job)
                active_by_url[item.normalized_url] = job
                output_rows.append(
                    {
                        "job_id": job_id,
                        "status": STATUS_QUEUED,
                        "deduplicated": False,
                        "input_url": item.input_url,
                        "normalized_url": item.normalized_url,
                        "platform": item.platform.value,
                    }
                )

            self._maybe_compact_locked()
            return output_rows

    def claim_next(self, *, worker_id: str) -> dict[str, Any] | None:
        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            queued = [job for job in jobs_by_id.values() if str(job.get("status")) == STATUS_QUEUED]
            if not queued:
                return None

            queued.sort(key=lambda row: (str(row.get("created_at") or ""), str(row.get("job_id") or "")))
            job = dict(queued[0])
            job["status"] = STATUS_RUNNING
            job["attempts"] = int(job.get("attempts") or 0) + 1
            job["updated_at"] = self._now()
            job["claimed_by"] = worker_id
            job["error"] = None

            self._append_jsonl(self.queue_file, job)
            self._maybe_compact_locked()
            return job

    def mark_done(self, *, job_id: str, result: dict[str, Any]) -> dict[str, Any] | None:
        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            current = jobs_by_id.get(job_id)
            if not current:
                return None

            job = dict(current)
            now = self._now()
            job["status"] = STATUS_DONE
            job["updated_at"] = now
            job["result"] = result
            job["error"] = None

            self._append_jsonl(self.queue_file, job)
            self._append_jsonl(
                self.results_file,
                {
                    "job_id": job_id,
                    "status": STATUS_DONE,
                    "result": result,
                    "created_at": job.get("created_at"),
                    "updated_at": now,
                },
            )
            self._maybe_compact_locked()
            return job

    def mark_failed_or_retry(self, *, job_id: str, error: str) -> tuple[dict[str, Any] | None, str | None]:
        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            current = jobs_by_id.get(job_id)
            if not current:
                return None, None

            job = dict(current)
            now = self._now()
            attempts = int(job.get("attempts") or 0)
            max_attempts = int(job.get("max_attempts") or self.max_attempts)

            job["error"] = error
            job["updated_at"] = now

            if attempts < max_attempts:
                job["status"] = STATUS_QUEUED
                self._append_jsonl(self.queue_file, job)
                self._maybe_compact_locked()
                return job, STATUS_QUEUED

            job["status"] = STATUS_FAILED
            self._append_jsonl(self.queue_file, job)
            self._append_jsonl(
                self.results_file,
                {
                    "job_id": job_id,
                    "status": STATUS_FAILED,
                    "error": error,
                    "attempts": attempts,
                    "max_attempts": max_attempts,
                    "created_at": job.get("created_at"),
                    "updated_at": now,
                },
            )
            self._maybe_compact_locked()
            return job, STATUS_FAILED

    def mark_notification(self, *, job_id: str, event_id: str, callback_error: str | None) -> dict[str, Any] | None:
        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            current = jobs_by_id.get(job_id)
            if not current:
                return None

            job = dict(current)
            notification = dict(job.get("notification") or {})
            notification["last_event_id"] = event_id
            notification["callback_attempts"] = int(notification.get("callback_attempts") or 0) + 1
            notification["callback_error"] = callback_error

            job["notification"] = notification
            job["updated_at"] = self._now()

            self._append_jsonl(self.queue_file, job)
            self._maybe_compact_locked()
            return job

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with file_lock(self.lock_file):
            jobs_by_id = self._materialize_jobs_locked()
            job = jobs_by_id.get(job_id)
            if not job:
                return None
            return dict(job)
