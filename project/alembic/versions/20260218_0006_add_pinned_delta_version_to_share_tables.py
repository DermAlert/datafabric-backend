"""add pinned_delta_version to share_tables

Allows pinning a specific Delta Lake version on a shared Bronze/Silver table,
enabling time-travel sharing so consumers always see a fixed historical snapshot.

Revision ID: 20260218_0006
Revises: 20260215_0005
Create Date: 2026-02-18 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = "20260218_0006"
down_revision: Union[str, None] = "20260215_0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    existing = {
        col["name"]
        for col in inspect(op.get_bind()).get_columns('share_tables', schema='delta_sharing')
    }
    if 'pinned_delta_version' not in existing:
        op.add_column(
            'share_tables',
            sa.Column('pinned_delta_version', sa.Integer(), nullable=True),
            schema='delta_sharing'
        )


def downgrade() -> None:
    op.drop_column(
        'share_tables',
        'pinned_delta_version',
        schema='delta_sharing'
    )
