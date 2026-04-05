from dataclasses import dataclass
from uuid import UUID

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.models import Payment


@dataclass
class AuthContext:
    tier: str  # "internal" or "paid"
    payment_id: UUID | None = None


def get_auth_context(
    request: Request,
    db: Session = Depends(get_db),
) -> AuthContext:
    # Path 1: Bearer token → internal access
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if token == settings.internal_api_key:
            return AuthContext(tier="internal")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )

    # Path 2: X-Payment-Token → paid access
    payment_token = request.headers.get("x-payment-token")
    if payment_token:
        payment = db.query(Payment).filter(
            Payment.stripe_session_id == payment_token,
            Payment.status == "paid",
        ).first()
        if not payment:
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail="Invalid or unpaid payment token",
            )
        return AuthContext(tier="paid", payment_id=payment.id)

    # No credentials at all
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing authentication",
    )
