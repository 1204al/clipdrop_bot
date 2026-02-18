from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from config import AppConfig, load_config
from job_store import JobStore
from logging_utils import setup_logger
from url_extractors import ExtractedUrl, classify_url


class SubscriberRequest(BaseModel):
    chat_id: int
    message_id: int
    chat_type: str = Field(..., min_length=1)
    thread_id: int | None = None


class EnqueueRequest(BaseModel):
    urls: list[str] = Field(..., min_length=1)
    subscriber: SubscriberRequest


class JobResponse(BaseModel):
    job_id: str
    status: str
    attempts: int
    max_attempts: int
    platform: str
    input_url: str
    normalized_url: str
    result: dict[str, Any] | None = None
    error: str | None = None
    subscribers_count: int = 0


@dataclass
class ServiceRuntime:
    config: AppConfig
    store: JobStore


def _build_store(config: AppConfig) -> JobStore:
    return JobStore(
        queue_file=config.queue_file,
        results_file=config.results_file,
        lock_file=config.queue_lock_file,
        max_attempts=config.max_attempts,
    )


def _parse_supported_urls(urls: list[str]) -> list[ExtractedUrl]:
    rows: list[ExtractedUrl] = []
    seen: set[str] = set()
    for raw in urls:
        item = classify_url(raw)
        if item is None:
            continue
        if item.normalized_url in seen:
            continue
        seen.add(item.normalized_url)
        rows.append(item)
    return rows


def _to_job_payload(job: dict[str, Any]) -> dict[str, Any]:
    subscribers = list(job.get("subscribers") or [])
    return {
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or "unknown"),
        "attempts": int(job.get("attempts") or 0),
        "max_attempts": int(job.get("max_attempts") or 0),
        "platform": str(job.get("platform") or ""),
        "input_url": str(job.get("input_url") or ""),
        "normalized_url": str(job.get("normalized_url") or ""),
        "result": job.get("result"),
        "error": job.get("error"),
        "subscribers_count": len(subscribers),
    }


def create_app(config: AppConfig | None = None) -> FastAPI:
    cfg = config or load_config()
    logger = setup_logger("service", cfg.debug)
    runtime = ServiceRuntime(config=cfg, store=_build_store(cfg))

    app = FastAPI(title="clipdrop", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/jobs")
    def enqueue_jobs(payload: EnqueueRequest) -> dict[str, Any]:
        parsed_urls = _parse_supported_urls(payload.urls)
        if not parsed_urls:
            raise HTTPException(status_code=400, detail="No supported URLs found")

        subscriber = {
            "chat_id": payload.subscriber.chat_id,
            "message_id": payload.subscriber.message_id,
            "chat_type": payload.subscriber.chat_type,
            "thread_id": payload.subscriber.thread_id,
        }
        rows = runtime.store.enqueue_many(parsed_urls, subscriber=subscriber)

        logger.info("Enqueued jobs count=%s", len(rows))
        return {
            "ok": True,
            "jobs": [
                {
                    "input_url": row["input_url"],
                    "normalized_url": row["normalized_url"],
                    "platform": row["platform"],
                    "job_id": row["job_id"],
                    "status": row["status"],
                    "deduplicated": bool(row["deduplicated"]),
                }
                for row in rows
            ],
        }

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: str) -> dict[str, Any]:
        job = runtime.store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return _to_job_payload(job)

    return app


app = create_app()
