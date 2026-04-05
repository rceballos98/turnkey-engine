import stripe
from fastapi import APIRouter, Request, HTTPException, Depends
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import Payment, Report, JobQueue

router = APIRouter()


@router.post("/webhook/stripe")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """Receives Stripe webhook events. Verifies signature, then processes."""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    if not sig_header or not settings.stripe_webhook_secret:
        raise HTTPException(status_code=400, detail="Missing signature or webhook secret")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, settings.stripe_webhook_secret
        )
    except stripe.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        _handle_checkout_completed(session, db)

    return {"status": "ok"}


def _handle_checkout_completed(session: dict, db: Session):
    """Process a completed checkout: record payment, create report, enqueue job."""
    metadata = session.get("metadata", {})
    address = metadata.get("address", "")
    address_hash = metadata.get("address_hash", "")

    # Record payment
    payment = Payment(
        stripe_session_id=session["id"],
        address=address,
        address_hash=address_hash,
        amount_cents=session["amount_total"],
        currency=session.get("currency", "usd"),
        status="paid",
    )
    db.add(payment)
    db.flush()

    # Idempotency: if a completed report already exists for this address, link and skip
    existing = db.query(Report).filter(
        Report.address == address,
        Report.status == "completed",
    ).first()

    if existing:
        existing.payment_id = payment.id
        db.commit()
        return

    # Create report + enqueue job
    report = Report(
        address=address,
        status="queued",
        payment_id=payment.id,
    )
    db.add(report)
    db.flush()

    job = JobQueue(report_id=report.id, status="pending")
    db.add(job)
    db.commit()
