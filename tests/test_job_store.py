from __future__ import annotations

from pathlib import Path

from job_store import JobStore, STATUS_FAILED, STATUS_QUEUED, STATUS_RUNNING
from url_extractors import Platform, ExtractedUrl


def _make_store(tmp_path: Path, max_attempts: int = 2) -> JobStore:
    return JobStore(
        queue_file=tmp_path / "queue.jsonl",
        results_file=tmp_path / "results.jsonl",
        lock_file=tmp_path / ".queue.lock",
        max_attempts=max_attempts,
    )


def _sub(chat_id: int, message_id: int) -> dict:
    return {
        "chat_id": chat_id,
        "message_id": message_id,
        "chat_type": "private",
        "thread_id": None,
    }


def _url(url: str) -> ExtractedUrl:
    return ExtractedUrl(input_url=url, normalized_url=url, platform=Platform.X)


def test_enqueue_deduplicates_active_and_merges_subscribers(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    url = _url("https://x.com/user/status/1")

    first = store.enqueue_many([url], subscriber=_sub(1, 11))
    second = store.enqueue_many([url], subscriber=_sub(2, 22))

    assert first[0]["job_id"] == second[0]["job_id"]
    assert second[0]["deduplicated"] is True

    job = store.get_job(first[0]["job_id"])
    assert job is not None
    assert len(job["subscribers"]) == 2


def test_claim_next_marks_running_fifo(tmp_path: Path) -> None:
    store = _make_store(tmp_path)
    a = store.enqueue_many([_url("https://x.com/u/status/1")], subscriber=_sub(1, 1))[0]["job_id"]
    b = store.enqueue_many([_url("https://x.com/u/status/2")], subscriber=_sub(1, 2))[0]["job_id"]

    claimed1 = store.claim_next(worker_id="w1")
    claimed2 = store.claim_next(worker_id="w1")

    assert claimed1 is not None and claimed1["job_id"] == a
    assert claimed1["status"] == STATUS_RUNNING
    assert claimed2 is not None and claimed2["job_id"] == b


def test_retry_then_fail(tmp_path: Path) -> None:
    store = _make_store(tmp_path, max_attempts=2)
    job_id = store.enqueue_many([_url("https://x.com/u/status/3")], subscriber=_sub(1, 1))[0]["job_id"]

    store.claim_next(worker_id="w1")
    _, status1 = store.mark_failed_or_retry(job_id=job_id, error="boom")
    assert status1 == STATUS_QUEUED

    store.claim_next(worker_id="w1")
    _, status2 = store.mark_failed_or_retry(job_id=job_id, error="boom2")
    assert status2 == STATUS_FAILED
