from __future__ import annotations

import worker as worker_module
from config import AppConfig
from job_store import JobStore
from url_extractors import ExtractedUrl, Platform


def _store_for_config(config: AppConfig) -> JobStore:
    return JobStore(
        queue_file=config.queue_file,
        results_file=config.results_file,
        lock_file=config.queue_lock_file,
        max_attempts=config.max_attempts,
    )


def _enqueue(store: JobStore, url: str) -> str:
    row = ExtractedUrl(input_url=url, normalized_url=url, platform=Platform.X)
    return store.enqueue_many(
        [row],
        subscriber={"chat_id": 1, "message_id": 1, "chat_type": "private", "thread_id": None},
    )[0]["job_id"]


def test_worker_processes_job_and_marks_done(app_config: AppConfig, monkeypatch) -> None:
    store = _store_for_config(app_config)
    job_id = _enqueue(store, "https://x.com/u/status/1")

    sent_payloads: list[dict] = []

    def fake_download_url(**kwargs):
        return {
            "file_path": "/tmp/video.mp4",
            "file_size_bytes": 10,
            "duration_sec": 2.3,
            "platform": kwargs["platform"].value,
        }

    def fake_send_callback(**kwargs):
        sent_payloads.append(kwargs["payload"])

    monkeypatch.setattr(worker_module, "download_url", fake_download_url)
    monkeypatch.setattr(worker_module, "send_job_event_callback", fake_send_callback)

    worker_module.run_worker(app_config, run_once=True)

    job = store.get_job(job_id)
    assert job is not None
    assert job["status"] == "done"
    assert [payload["status"] for payload in sent_payloads] == ["started", "done"]


def test_worker_retries_then_marks_failed_and_sends_callback(app_config: AppConfig, monkeypatch) -> None:
    store = _store_for_config(app_config)
    job_id = _enqueue(store, "https://x.com/u/status/2")

    sent_payloads: list[dict] = []

    def fake_download_url(**kwargs):
        raise RuntimeError("failed")

    def fake_send_callback(**kwargs):
        sent_payloads.append(kwargs["payload"])

    monkeypatch.setattr(worker_module, "download_url", fake_download_url)
    monkeypatch.setattr(worker_module, "send_job_event_callback", fake_send_callback)

    worker_module.run_worker(app_config, run_once=True)
    first = store.get_job(job_id)
    assert first is not None
    assert first["status"] == "queued"

    worker_module.run_worker(app_config, run_once=True)
    second = store.get_job(job_id)
    assert second is not None
    assert second["status"] == "failed"
    assert [payload["status"] for payload in sent_payloads] == ["started", "started", "failed"]
