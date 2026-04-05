import uuid
from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime, Text, Index, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB

from app.database import Base


class Payment(Base):
    __tablename__ = "payments"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    stripe_session_id = Column(String(255), unique=True, nullable=False)
    address = Column(Text, nullable=False)
    address_hash = Column(String(64), nullable=False, index=True)
    amount_cents = Column(Integer, nullable=False)
    currency = Column(String(10), nullable=False, default="usd")
    status = Column(String(50), nullable=False, default="pending")
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class Report(Base):
    __tablename__ = "reports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String(50), nullable=False, default="queued")
    address = Column(Text, nullable=False)
    raw_data = Column(JSONB, nullable=True)
    result_json = Column(JSONB, nullable=True)
    pdf_path = Column(Text, nullable=True)
    error = Column(Text, nullable=True)
    payment_id = Column(UUID(as_uuid=True), ForeignKey("payments.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class JobQueue(Base):
    __tablename__ = "job_queue"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    report_id = Column(UUID(as_uuid=True), nullable=False)
    status = Column(String(50), nullable=False, default="pending")
    claimed_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_job_queue_status_created", "status", "created_at"),
    )
