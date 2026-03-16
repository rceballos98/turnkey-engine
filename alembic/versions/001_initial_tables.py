"""initial tables

Revision ID: 001
Revises:
Create Date: 2026-03-16
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "reports",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("status", sa.String(50), nullable=False, server_default="queued"),
        sa.Column("query", sa.Text, nullable=False),
        sa.Column("result", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_table(
        "job_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("report_id", UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_job_queue_status_created", "job_queue", ["status", "created_at"])


def downgrade():
    op.drop_index("ix_job_queue_status_created", table_name="job_queue")
    op.drop_table("job_queue")
    op.drop_table("reports")
