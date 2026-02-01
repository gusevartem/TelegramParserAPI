"""Every one hour parsing

Revision ID: 14187dfc65ec
Revises: 57bb93ae0b22
Create Date: 2026-02-01 23:05:50.380014

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "14187dfc65ec"
down_revision: str | Sequence[str] | None = "57bb93ae0b22"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint("check_bucket_range", "parsing_task", type_="check")
    op.create_check_constraint(
        "check_bucket_range", "parsing_task", "bucket >= 0 AND bucket < 60"
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("check_bucket_range", "parsing_task", type_="check")
    op.create_check_constraint(
        "check_bucket_range", "parsing_task", "bucket >= 0 AND bucket < 1440"
    )
