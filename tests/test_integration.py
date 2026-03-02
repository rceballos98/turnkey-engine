"""
Integration tests — run against real local Postgres (turnkey-pg container).

    pytest tests/ -v
"""
import uuid
from unittest.mock import patch

from app.models import JobQueue, Report
from app.worker import claim_job, process_job


# ── Health ───────────────────────────────────────────────────────────

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


# ── Auth ─────────────────────────────────────────────────────────────

def test_create_report_unauthorized(client):
    """No auth header at all → 401."""
    resp = client.post("/reports", json={"query": "test"})
    assert resp.status_code in (401, 403)


def test_create_report_invalid_key(client):
    """Wrong bearer token → 401."""
    resp = client.post(
        "/reports",
        json={"query": "test"},
        headers={"Authorization": "Bearer wrong-key"},
    )
    assert resp.status_code == 401


# ── Report CRUD ──────────────────────────────────────────────────────

def test_create_report(client, db, auth_headers):
    """Valid auth + query → 201, status=queued, result=null."""
    resp = client.post(
        "/reports",
        json={"query": "revenue by region"},
        headers=auth_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "queued"
    assert body["result"] is None
    assert body["id"]


def test_get_report_not_found(client, db, auth_headers):
    """GET /reports/<random-uuid> → 404."""
    fake_id = str(uuid.uuid4())
    resp = client.get(f"/reports/{fake_id}", headers=auth_headers)
    assert resp.status_code == 404


# ── Full lifecycle ───────────────────────────────────────────────────

def test_full_lifecycle(client, db, auth_headers):
    """
    POST → enqueue → claim → process → GET completed report.
    We call claim_job / process_job directly (no subprocess worker).
    run_agent is patched to avoid the 2-second sleep.
    """
    query_text = "full lifecycle test query"

    # 1. Create the report
    resp = client.post(
        "/reports",
        json={"query": query_text},
        headers=auth_headers,
    )
    assert resp.status_code == 201
    report_id = resp.json()["id"]

    # 2. Verify job_queue row exists with status=pending
    job = db.query(JobQueue).filter(JobQueue.report_id == report_id).first()
    assert job is not None
    assert job.status == "pending"

    # 3. Claim the job (uses the real SQL query)
    claimed = claim_job(db)
    assert claimed is not None
    assert str(claimed.report_id) == report_id

    # 4. Process the job (mock the agent to avoid sleep)
    fake_result = f"Test result for: {query_text}"
    with patch("app.worker.run_agent", return_value=fake_result):
        process_job(db, claimed.id, claimed.report_id)

    # 5. Verify the report is completed in DB
    db.expire_all()
    report = db.query(Report).filter(Report.id == report_id).first()
    assert report.status == "completed"
    assert report.result is not None

    # 6. GET the report via API
    resp = client.get(f"/reports/{report_id}", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert query_text in body["result"]
