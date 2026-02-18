from __future__ import annotations

import os
import socket
import time
import traceback
from typing import Any

import httpx

from config import AppConfig
from downloader import download_url
from job_store import JobStore, STATUS_FAILED, STATUS_QUEUED
from logging_utils import setup_logger
from url_extractors import Platform


def build_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def build_job_store(config: AppConfig) -> JobStore:
    return JobStore(
        queue_file=config.queue_file,
        results_file=config.results_file,
        lock_file=config.queue_lock_file,
        max_attempts=config.max_attempts,
    )


def _build_event_payload(job: dict[str, Any], status: str) -> dict[str, Any]:
    attempts = int(job.get("attempts") or 0)
    return {
        "event_id": f"{job['job_id']}:{status}:{attempts}",
        "job_id": str(job["job_id"]),
        "status": status,
        "platform": str(job.get("platform") or ""),
        "input_url": str(job.get("input_url") or ""),
        "result": job.get("result"),
        "error": job.get("error"),
        "subscribers": list(job.get("subscribers") or []),
    }


def send_job_event_callback(
    *,
    callback_url: str,
    callback_secret: str,
    payload: dict[str, Any],
) -> None:
    timeout = httpx.Timeout(connect=10.0, read=20.0, write=20.0, pool=20.0)
    headers = {"X-Internal-Token": callback_secret}
    with httpx.Client(timeout=timeout) as client:
        response = client.post(callback_url, json=payload, headers=headers)
        response.raise_for_status()


def _send_callback_with_retries(
    *,
    callback_url: str,
    callback_secret: str,
    payload: dict[str, Any],
    retries: int = 3,
    delay_seconds: float = 0.8,
) -> None:
    attempt = 0
    last_error: Exception | None = None
    while attempt < max(1, retries):
        attempt += 1
        try:
            send_job_event_callback(
                callback_url=callback_url,
                callback_secret=callback_secret,
                payload=payload,
            )
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries:
                time.sleep(delay_seconds)
    if last_error is not None:
        raise last_error


def run_worker(config: AppConfig, *, run_once: bool = False) -> None:
    logger = setup_logger("worker", config.debug, config.log_file)
    store = build_job_store(config)
    worker_id = build_worker_id()

    logger.info(
        "Worker started worker_id=%s poll=%.2fs max_attempts=%s",
        worker_id,
        config.worker_poll_seconds,
        config.max_attempts,
    )

    processed = 0
    while True:
        job = store.claim_next(worker_id=worker_id)
        if not job:
            if run_once:
                logger.info("No queued jobs. Exiting.")
                return
            time.sleep(config.worker_poll_seconds)
            continue

        processed += 1
        job_id = str(job["job_id"])
        attempts = int(job.get("attempts") or 0)
        max_attempts = int(job.get("max_attempts") or config.max_attempts)
        logger.info(
            "Claimed job job_id=%s platform=%s attempt=%s/%s",
            job_id,
            job.get("platform"),
            attempts,
            max_attempts,
        )

        started_payload = _build_event_payload(job, "started")
        started_callback_error: str | None = None
        try:
            _send_callback_with_retries(
                callback_url=config.worker_bot_callback_url,
                callback_secret=config.bot_callback_secret,
                payload=started_payload,
            )
        except Exception as exc:  # noqa: BLE001
            started_callback_error = str(exc)
            logger.warning("Start callback failed job_id=%s error=%s", job_id, started_callback_error)
        finally:
            store.mark_notification(
                job_id=job_id,
                event_id=str(started_payload["event_id"]),
                callback_error=started_callback_error,
            )

        try:
            platform = Platform(str(job["platform"]))
            result = download_url(
                input_url=str(job["input_url"]),
                platform=platform,
                downloads_dir=config.downloads_dir,
                debug=config.debug,
            )
            finished = store.mark_done(job_id=job_id, result=result)
            if not finished:
                logger.error("Job disappeared before mark_done job_id=%s", job_id)
                continue

            payload = _build_event_payload(finished, "done")
            callback_error: str | None = None
            try:
                _send_callback_with_retries(
                    callback_url=config.worker_bot_callback_url,
                    callback_secret=config.bot_callback_secret,
                    payload=payload,
                )
            except Exception as exc:  # noqa: BLE001
                callback_error = str(exc)
                logger.error("Callback failed job_id=%s error=%s", job_id, callback_error)
            finally:
                store.mark_notification(
                    job_id=job_id,
                    event_id=str(payload["event_id"]),
                    callback_error=callback_error,
                )
            logger.info("Job done job_id=%s", job_id)
        except Exception:
            error = traceback.format_exc()
            logger.exception("Download failed job_id=%s", job_id)
            failed_job, next_status = store.mark_failed_or_retry(job_id=job_id, error=error)
            if next_status == STATUS_QUEUED:
                logger.warning("Job failed and re-queued job_id=%s", job_id)
            elif next_status == STATUS_FAILED and failed_job:
                logger.error("Job failed permanently job_id=%s", job_id)
                payload = _build_event_payload(failed_job, "failed")
                callback_error = None
                try:
                    _send_callback_with_retries(
                        callback_url=config.worker_bot_callback_url,
                        callback_secret=config.bot_callback_secret,
                        payload=payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    callback_error = str(exc)
                    logger.error("Callback failed for failed job job_id=%s error=%s", job_id, callback_error)
                finally:
                    store.mark_notification(
                        job_id=job_id,
                        event_id=str(payload["event_id"]),
                        callback_error=callback_error,
                    )
            else:
                logger.error("Job update failed after error job_id=%s", job_id)

        if run_once and processed >= 1:
            logger.info("Processed one job. Exiting due to --once.")
            return
