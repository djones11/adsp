"""Initial migration

Revision ID: a7a0e66e995f
Revises:
Create Date: 2025-12-06 15:10:24.760707

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7a0e66e995f"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "failed_rows",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("raw_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "stop_searches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("force", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("involved_person", sa.Boolean(), nullable=False),
        sa.Column("datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operation", sa.Boolean(), nullable=True),
        sa.Column("operation_name", sa.String(), nullable=True),
        sa.Column("latitude", sa.String(), nullable=True),
        sa.Column("longitude", sa.String(), nullable=True),
        sa.Column("street_id", sa.Integer(), nullable=True),
        sa.Column("street_name", sa.String(), nullable=True),
        sa.Column("gender", sa.String(), nullable=True),
        sa.Column("age_range", sa.String(), nullable=True),
        sa.Column("self_defined_ethnicity", sa.String(), nullable=True),
        sa.Column("officer_defined_ethnicity", sa.String(), nullable=True),
        sa.Column("legislation", sa.String(), nullable=True),
        sa.Column("object_of_search", sa.String(), nullable=True),
        sa.Column("outcome", sa.String(), nullable=True),
        sa.Column("outcome_linked_to_object_of_search", sa.Boolean(), nullable=True),
        sa.Column("removal_of_more_than_outer_clothing", sa.Boolean(), nullable=True),
        sa.Column("outcome_object_id", sa.String(), nullable=True),
        sa.Column("outcome_object_name", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_stop_searches_datetime"), "stop_searches", ["datetime"], unique=False
    )
    op.create_index(
        op.f("ix_stop_searches_force"), "stop_searches", ["force"], unique=False
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_stop_searches_force"), table_name="stop_searches")
    op.drop_index(op.f("ix_stop_searches_datetime"), table_name="stop_searches")
    op.drop_table("stop_searches")
    op.drop_table("failed_rows")
