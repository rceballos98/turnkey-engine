import hashlib

import stripe
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings

router = APIRouter()


class CheckoutRequest(BaseModel):
    address: str


def hash_address(address: str) -> str:
    """Normalize and hash an address for idempotency."""
    normalized = address.strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()


@router.post("/checkout")
def create_checkout(req: CheckoutRequest):
    """
    Public endpoint — creates a Stripe Checkout Session for a report.
    Returns the checkout URL where the user pays $25.
    """
    if not settings.stripe_secret_key:
        raise HTTPException(status_code=503, detail="Payments not configured")

    stripe.api_key = settings.stripe_secret_key
    address_hash = hash_address(req.address)

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "unit_amount": settings.report_price_cents,
                    "product_data": {
                        "name": "Property Report",
                        "description": f"AI-generated property report for {req.address}",
                    },
                },
                "quantity": 1,
            }],
            mode="payment",
            success_url=f"{settings.base_url}/reports/status?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{settings.base_url}/checkout/cancelled",
            metadata={
                "address": req.address,
                "address_hash": address_hash,
            },
        )
    except stripe.StripeError as e:
        raise HTTPException(status_code=502, detail=f"Stripe error: {str(e)}")

    return {
        "checkout_url": session.url,
        "session_id": session.id,
        "address_hash": address_hash,
    }
