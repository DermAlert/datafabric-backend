"""add bronze/silver persistent config IDs to share_tables

Allows share_tables of type BRONZE and SILVER to hold a direct reference to
their source config so that name and description can always be resolved live
from the config, reflecting any renames or edits without manual sync.

Revision ID: 20260218_0008
Revises: 20260218_0007
Create Date: 2026-02-18 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = "20260218_0008"
down_revision: Union[str, None] = "20260218_0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    existing = {
        col["name"]
        for col in inspect(op.get_bind()).get_columns('share_tables', schema='delta_sharing')
    }
    if 'bronze_persistent_config_id' not in existing:
        op.add_column(
            'share_tables',
            sa.Column('bronze_persistent_config_id', sa.Integer(), nullable=True),
            schema='delta_sharing'
        )
    if 'silver_persistent_config_id' not in existing:
        op.add_column(
            'share_tables',
            sa.Column('silver_persistent_config_id', sa.Integer(), nullable=True),
            schema='delta_sharing'
        )


def downgrade() -> None:
    op.drop_column('share_tables', 'silver_persistent_config_id', schema='delta_sharing')
    op.drop_column('share_tables', 'bronze_persistent_config_id', schema='delta_sharing')
