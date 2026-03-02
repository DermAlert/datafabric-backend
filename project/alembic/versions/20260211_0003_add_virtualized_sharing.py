"""add virtualized sharing support to share_tables

Adds columns to delta_sharing.share_tables to support virtualized (on-demand)
data access through the Data API:
- source_type: delta | bronze_virtualized | silver_virtualized
- bronze_virtualized_config_id: FK to bronze virtualized config
- silver_virtualized_config_id: FK to silver virtualized config
- Makes dataset_id nullable (virtualized tables don't need one)

Revision ID: 20260211_0003
Revises: 20260211_0002
Create Date: 2026-02-11 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

# revision identifiers, used by Alembic.
revision: str = "20260211_0003"
down_revision: Union[str, None] = "20260211_0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(table: str, schema: str) -> set[str]:
    inspector = inspect(op.get_bind())
    return {col["name"] for col in inspector.get_columns(table, schema=schema)}


def upgrade() -> None:
    # Create the enum type in public schema (so PostgreSQL finds it in default search_path)
    # SQLAlchemy uses enum NAMES (uppercase), not values, when persisting to PG
    source_type_enum = sa.Enum(
        'DELTA', 'BRONZE_VIRTUALIZED', 'SILVER_VIRTUALIZED',
        name='sharetablesourcetype',
    )
    source_type_enum.create(op.get_bind(), checkfirst=True)

    existing = _existing_columns('share_tables', 'delta_sharing')

    if 'source_type' not in existing:
        op.add_column(
            'share_tables',
            sa.Column(
                'source_type',
                source_type_enum,
                nullable=False,
                server_default='DELTA'
            ),
            schema='delta_sharing'
        )

    if 'bronze_virtualized_config_id' not in existing:
        op.add_column(
            'share_tables',
            sa.Column('bronze_virtualized_config_id', sa.Integer(), nullable=True),
            schema='delta_sharing'
        )

    if 'silver_virtualized_config_id' not in existing:
        op.add_column(
            'share_tables',
            sa.Column('silver_virtualized_config_id', sa.Integer(), nullable=True),
            schema='delta_sharing'
        )

    # Make dataset_id nullable (virtualized tables don't need one).
    # Safe to run multiple times – ALTER COLUMN is idempotent for nullability.
    op.alter_column(
        'share_tables',
        'dataset_id',
        existing_type=sa.Integer(),
        nullable=True,
        schema='delta_sharing'
    )


def downgrade() -> None:
    # Remove added columns
    op.drop_column('share_tables', 'silver_virtualized_config_id', schema='delta_sharing')
    op.drop_column('share_tables', 'bronze_virtualized_config_id', schema='delta_sharing')
    op.drop_column('share_tables', 'source_type', schema='delta_sharing')

    # Drop the enum type from public schema
    sa.Enum(
        name='sharetablesourcetype',
    ).drop(op.get_bind(), checkfirst=True)

    # Restore dataset_id NOT NULL (only safe if no NULL values exist)
    op.alter_column(
        'share_tables',
        'dataset_id',
        existing_type=sa.Integer(),
        nullable=False,
        schema='delta_sharing'
    )
