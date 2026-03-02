"""
Background worker that polls the job queue and processes reports.

Handles SIGTERM gracefully for clean shutdown during Render deploys.
"""

import signal
import time
from datetime import datetime, timezone

from sqlalchemy import text

from app.agent import run_agent
from app.database import SessionLocal
from app.models import JobQueue, Report

shutdown_requested = False


def handle_sigterm(signum, frame):
    global shutdown_requested
    print("SIGTERM received, shutting down gracefully...")
    shutdown_requested = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def claim_job(db):
    """Claim the oldest pending job using SELECT ... FOR UPDATE SKIP LOCKED."""
    result = db.execute(
        text(
            """
            UPDATE job_queue
            SET status = 'claimed', claimed_at = now()
            WHERE id = (
                SELECT id FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, report_id
            """
        )
    )
    row = result.fetchone()
    db.commit()
    return row


def process_job(db, job_id, report_id):
    """Run the agent and update the report with the result."""
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        print(f"Report {report_id} not found, skipping job {job_id}")
        return

    try:
        report.status = "processing"
        db.commit()

        result = run_agent(report.query)

        report.status = "completed"
        report.result = result
        report.updated_at = datetime.now(timezone.utc)

        db.query(JobQueue).filter(JobQueue.id == job_id).update(
            {"status": "completed", "completed_at": datetime.now(timezone.utc)}
        )
        db.commit()
        print(f"Completed job {job_id} for report {report_id}")

    except Exception as e:
        db.rollback()
        report.status = "failed"
        report.error = str(e)
        report.updated_at = datetime.now(timezone.utc)

        db.query(JobQueue).filter(JobQueue.id == job_id).update(
            {"status": "failed", "completed_at": datetime.now(timezone.utc)}
        )
        db.commit()
        print(f"Failed job {job_id}: {e}")


def poll_loop():
    """Main polling loop. Sleeps in 1s increments for fast SIGTERM response."""
    print("Worker started, polling for jobs...")
    while not shutdown_requested:
        db = SessionLocal()
        try:
            row = claim_job(db)
            if row:
                job_id, report_id = row
                print(f"Claimed job {job_id} for report {report_id}")
                process_job(db, job_id, report_id)
            else:
                # Sleep 5s total, but in 1s increments so SIGTERM is caught quickly
                for _ in range(5):
                    if shutdown_requested:
                        break
                    time.sleep(1)
        except Exception as e:
            print(f"Worker error: {e}")
            time.sleep(5)
        finally:
            db.close()

    print("Worker shut down.")


if __name__ == "__main__":
    poll_loop()
