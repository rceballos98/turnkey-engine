"""report pipeline fields

Revision ID: 002
Revises: 001
Create Date: 2026-03-15
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade():
    # Add new columns
    op.add_column("reports", sa.Column("address", sa.Text, nullable=True))
    op.add_column("reports", sa.Column("raw_data", JSONB, nullable=True))
    op.add_column("reports", sa.Column("result_json", JSONB, nullable=True))
    op.add_column("reports", sa.Column("pdf_path", sa.Text, nullable=True))

    # Migrate data: copy query -> address
    op.execute("UPDATE reports SET address = query WHERE address IS NULL")

    # Make address non-nullable
    op.alter_column("reports", "address", nullable=False)

    # Drop old columns
    op.drop_column("reports", "query")
    op.drop_column("reports", "result")


def downgrade():
    # Re-add old columns
    op.add_column("reports", sa.Column("query", sa.Text, nullable=True))
    op.add_column("reports", sa.Column("result", sa.Text, nullable=True))

    # Migrate data back
    op.execute("UPDATE reports SET query = address WHERE query IS NULL")
    op.alter_column("reports", "query", nullable=False)

    # Drop new columns
    op.drop_column("reports", "pdf_path")
    op.drop_column("reports", "result_json")
    op.drop_column("reports", "raw_data")
    op.drop_column("reports", "address")
