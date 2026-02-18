from __future__ import annotations

from fastapi.testclient import TestClient

from config import AppConfig
from service_api import create_app


def _payload(urls: list[str], chat_id: int = 1, message_id: int = 2) -> dict:
    return {
        "urls": urls,
        "subscriber": {
            "chat_id": chat_id,
            "message_id": message_id,
            "chat_type": "private",
            "thread_id": None,
        },
    }


def test_post_jobs_and_get_job(app_config: AppConfig) -> None:
    client = TestClient(create_app(app_config))

    response = client.post(
        "/jobs",
        json=_payload(["https://x.com/aaa/status/123456"]),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert len(payload["jobs"]) == 1

    job_id = payload["jobs"][0]["job_id"]
    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    assert status.json()["status"] == "queued"


def test_post_jobs_rejects_invalid_urls(app_config: AppConfig) -> None:
    client = TestClient(create_app(app_config))
    response = client.post("/jobs", json=_payload(["https://www.youtube.com/watch?v=abc"]))
    assert response.status_code == 400


def test_post_jobs_global_dedup_and_subscribers_count(app_config: AppConfig) -> None:
    client = TestClient(create_app(app_config))

    first = client.post("/jobs", json=_payload(["https://instagram.com/reel/ABC123/"]))
    second = client.post("/jobs", json=_payload(["https://instagram.com/reel/ABC123/?igshid=123"], chat_id=2, message_id=20))

    assert first.status_code == 200
    assert second.status_code == 200

    first_job = first.json()["jobs"][0]
    second_job = second.json()["jobs"][0]
    assert first_job["job_id"] == second_job["job_id"]
    assert second_job["deduplicated"] is True

    status = client.get(f"/jobs/{first_job['job_id']}")
    assert status.status_code == 200
    assert status.json()["subscribers_count"] == 2
