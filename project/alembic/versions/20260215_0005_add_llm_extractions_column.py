"""add llm_extractions column to silver_persistent_configs

Adds a JSON column for LLM-based extraction definitions that allow
creating new structured columns (bool/enum) from free-text fields
using LLM prompts via PydanticAI.

Revision ID: 20260215_0005
Revises: 20260211_0004
Create Date: 2026-02-15 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "20260215_0005"
down_revision: Union[str, None] = "20260211_0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'silver_persistent_configs',
        sa.Column('llm_extractions', sa.JSON(), nullable=True),
        schema='datasets'
    )


def downgrade() -> None:
    op.drop_column(
        'silver_persistent_configs',
        'llm_extractions',
        schema='datasets'
    )
