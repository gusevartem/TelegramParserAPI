"""Fix message id unique

Revision ID: c5603e70cc81
Revises: 14187dfc65ec
Create Date: 2026-02-02 20:13:22.282294

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c5603e70cc81"
down_revision: str | Sequence[str] | None = "14187dfc65ec"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_constraint(
        "message_media_link_message_id_fkey", "message_media_link", type_="foreignkey"
    )
    op.drop_constraint(
        "channel_message_statistic_message_id_fkey",
        "channel_message_statistic",
        type_="foreignkey",
    )

    op.add_column(
        "channel_message",
        sa.Column("channel_message_id", sa.BigInteger(), nullable=False),
    )

    op.alter_column(
        "channel_message",
        "id",
        existing_type=sa.BigInteger(),
        type_=sa.Uuid(),
        existing_nullable=False,
        postgresql_using="gen_random_uuid()",
    )

    op.alter_column(
        "message_media_link",
        "message_id",
        existing_type=sa.BigInteger(),
        type_=sa.Uuid(),
        existing_nullable=False,
        postgresql_using="gen_random_uuid()",
    )

    op.add_column(
        "channel_message_statistic",
        sa.Column("channel_message_id", sa.Uuid(), nullable=False),
    )
    op.drop_index(
        op.f("ix_channel_message_statistic_message_id"),
        table_name="channel_message_statistic",
    )
    op.create_index(
        op.f("ix_channel_message_statistic_channel_message_id"),
        "channel_message_statistic",
        ["channel_message_id"],
        unique=False,
    )

    op.create_foreign_key(
        None,
        "channel_message_statistic",
        "channel_message",
        ["channel_message_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("channel_message_statistic", "message_id")

    op.create_index(
        op.f("ix_channel_message_channel_message_id"),
        "channel_message",
        ["channel_message_id"],
        unique=False,
    )
    op.create_unique_constraint(
        "uq_channel_message_channel",
        "channel_message",
        ["channel_message_id", "channel_id"],
    )

    op.create_foreign_key(
        None,
        "message_media_link",
        "channel_message",
        ["message_id"],
        ["id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        "message_media_link_message_id_fkey", "message_media_link", type_="foreignkey"
    )
    op.drop_constraint(
        "channel_message_statistic_channel_message_id_fkey",
        "channel_message_statistic",
        type_="foreignkey",
    )

    op.drop_constraint("uq_channel_message_channel", "channel_message", type_="unique")
    op.drop_index(
        op.f("ix_channel_message_channel_message_id"), table_name="channel_message"
    )

    op.add_column(
        "channel_message_statistic",
        sa.Column("message_id", sa.BigInteger(), autoincrement=False, nullable=False),
    )

    op.drop_index(
        op.f("ix_channel_message_statistic_channel_message_id"),
        table_name="channel_message_statistic",
    )
    op.create_index(
        "ix_channel_message_statistic_message_id",
        "channel_message_statistic",
        ["message_id"],
        unique=False,
    )
    op.drop_column("channel_message_statistic", "channel_message_id")

    op.alter_column(
        "message_media_link",
        "message_id",
        existing_type=sa.Uuid(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )

    op.alter_column(
        "channel_message",
        "id",
        existing_type=sa.Uuid(),
        type_=sa.BigInteger(),
        existing_nullable=False,
    )

    op.drop_column("channel_message", "channel_message_id")

    op.create_foreign_key(
        None,
        "message_media_link",
        "channel_message",
        ["message_id"],
        ["id"],
    )
    op.create_foreign_key(
        None,
        "channel_message_statistic",
        "channel_message",
        ["message_id"],
        ["id"],
        ondelete="CASCADE",
    )
