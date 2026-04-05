"""add payments table and payment_id to reports

Revision ID: 003
Revises: 002
Create Date: 2026-04-05
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "payments",
        sa.Column("id", UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("stripe_session_id", sa.String(255), unique=True, nullable=False),
        sa.Column("address", sa.Text, nullable=False),
        sa.Column("address_hash", sa.String(64), nullable=False),
        sa.Column("amount_cents", sa.Integer, nullable=False),
        sa.Column("currency", sa.String(10), nullable=False, server_default="usd"),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_payments_address_hash", "payments", ["address_hash"])

    op.add_column("reports", sa.Column("payment_id", UUID(as_uuid=True), nullable=True))
    op.create_foreign_key("fk_reports_payment_id", "reports", "payments", ["payment_id"], ["id"])


def downgrade():
    op.drop_constraint("fk_reports_payment_id", "reports", type_="foreignkey")
    op.drop_column("reports", "payment_id")
    op.drop_index("ix_payments_address_hash", table_name="payments")
    op.drop_table("payments")
