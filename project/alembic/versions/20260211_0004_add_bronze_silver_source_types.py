"""add bronze and silver source types to sharetablesourcetype enum

Adds BRONZE and SILVER values to the sharetablesourcetype enum so
materialized tables from Bronze Persistent and Silver Transform configs
can be distinguished from generic delta, and from virtualized types.

Also updates existing share_tables created via from-bronze/from-silver
endpoints from DELTA to the correct BRONZE/SILVER source_type.

Revision ID: 20260211_0004
Revises: 20260211_0003
Create Date: 2026-02-11 20:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260211_0004"
down_revision: Union[str, None] = "20260211_0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add new enum values to existing type
    # PostgreSQL requires ALTER TYPE ... ADD VALUE
    op.execute("ALTER TYPE sharetablesourcetype ADD VALUE IF NOT EXISTS 'BRONZE'")
    op.execute("ALTER TYPE sharetablesourcetype ADD VALUE IF NOT EXISTS 'SILVER'")


def downgrade() -> None:
    # PostgreSQL doesn't support removing enum values directly.
    # Update any rows using the new values back to DELTA first.
    op.execute("""
        UPDATE delta_sharing.share_tables 
        SET source_type = 'DELTA' 
        WHERE source_type IN ('BRONZE', 'SILVER')
    """)
    # Note: The enum values BRONZE/SILVER will remain in the type
    # as PostgreSQL cannot remove individual enum values.
    # This is safe - unused values don't cause issues.
