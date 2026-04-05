"""
Integration tests — run against real local Postgres (turnkey-pg container).

    pytest tests/ -v
"""
import uuid
from unittest.mock import patch, AsyncMock

from app.models import JobQueue, Report, Payment
from app.worker import claim_job, process_job


# ── Health ───────────────────────────────────────────────────────────

def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


# ── Auth ─────────────────────────────────────────────────────────────

def test_create_report_unauthorized(client):
    """No auth header at all → 401."""
    resp = client.post("/reports", json={"address": "test"})
    assert resp.status_code == 401


def test_create_report_invalid_key(client):
    """Wrong bearer token → 401."""
    resp = client.post(
        "/reports",
        json={"address": "test"},
        headers={"Authorization": "Bearer wrong-key"},
    )
    assert resp.status_code == 401


# ── Report CRUD ──────────────────────────────────────────────────────

def test_create_report(client, db, auth_headers):
    """Valid auth + address → 201, status=queued, pdf_path=null."""
    resp = client.post(
        "/reports",
        json={"address": "301 E 79th St, New York, NY 10075"},
        headers=auth_headers,
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "queued"
    assert body["pdf_path"] is None
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
    run_report_pipeline is patched to avoid real API calls.
    """
    address_text = "301 E 79th St, Unit 3B, New York, NY 10075"

    # 1. Create the report
    resp = client.post(
        "/reports",
        json={"address": address_text},
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

    # 4. Process the job (mock the pipeline to avoid real API calls)
    fake_result = {
        "raw_data": {"pluto": {"bbl": "1234567890"}},
        "transformed": {"address": address_text, "yearBuilt": 1960},
        "pdf_path": "/data/pdfs/test.pdf",
    }
    with patch("app.worker.asyncio.run", return_value=fake_result):
        process_job(db, claimed.id, claimed.report_id)

    # 5. Verify the report is completed in DB
    db.expire_all()
    report = db.query(Report).filter(Report.id == report_id).first()
    assert report.status == "completed"
    assert report.raw_data is not None
    assert report.pdf_path is not None

    # 6. GET the report via API
    resp = client.get(f"/reports/{report_id}", headers=auth_headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert body["address"] == address_text


# ── Payment auth ────────────────────────────────────────────────────

def test_payment_token_auth(client, db):
    """X-Payment-Token with valid paid payment → 200."""
    # Create a payment + report
    payment = Payment(
        stripe_session_id="cs_test_abc123",
        address="123 Main St",
        address_hash="abc123",
        amount_cents=2500,
        currency="usd",
        status="paid",
    )
    db.add(payment)
    db.flush()

    report = Report(address="123 Main St", status="completed", payment_id=payment.id)
    db.add(report)
    db.commit()
    db.refresh(report)

    resp = client.get(
        f"/reports/{report.id}",
        headers={"X-Payment-Token": "cs_test_abc123"},
    )
    assert resp.status_code == 200
    assert resp.json()["address"] == "123 Main St"


def test_payment_token_invalid(client):
    """X-Payment-Token with unknown session → 402."""
    fake_id = str(uuid.uuid4())
    resp = client.get(
        f"/reports/{fake_id}",
        headers={"X-Payment-Token": "cs_test_nonexistent"},
    )
    assert resp.status_code == 402


# ── Status endpoint ─────────────────────────────────────────────────

def test_report_status_by_session(client, db):
    """GET /reports/status?session_id= returns report info."""
    payment = Payment(
        stripe_session_id="cs_test_status",
        address="456 Oak Ave",
        address_hash="def456",
        amount_cents=2500,
        currency="usd",
        status="paid",
    )
    db.add(payment)
    db.flush()

    report = Report(address="456 Oak Ave", status="queued", payment_id=payment.id)
    db.add(report)
    db.commit()
    db.refresh(report)

    resp = client.get("/reports/status?session_id=cs_test_status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["report_id"] == str(report.id)
    assert body["status"] == "queued"


def test_report_status_not_found(client):
    """GET /reports/status with unknown session_id → 404."""
    resp = client.get("/reports/status?session_id=cs_test_unknown")
    assert resp.status_code == 404


# ── Checkout ────────────────────────────────────────────────────────

def test_checkout_no_stripe(client):
    """POST /checkout when Stripe not configured → 503."""
    resp = client.post("/checkout", json={"address": "301 E 79th St"})
    assert resp.status_code == 503
