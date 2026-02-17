"""add missing silver tables

Revision ID: 20260211_0002
Revises: 20260211_0001
Create Date: 2026-02-11 00:20:00.000000
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import inspect

from app.database.session import Base
from app.database.models import silver  # noqa: F401

# revision identifiers, used by Alembic.
revision: str = "20260211_0002"
down_revision: Union[str, None] = "20260211_0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SILVER_TABLES = (
    "datasets.silver_normalization_rules",
    "datasets.silver_virtualized_configs",
    "datasets.silver_persistent_configs",
    "datasets.silver_executions",
)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    op.execute("CREATE SCHEMA IF NOT EXISTS datasets")

    for full_name in SILVER_TABLES:
        schema_name, table_name = full_name.split(".", 1)
        if not inspector.has_table(table_name, schema=schema_name):
            Base.metadata.tables[full_name].create(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = inspect(bind)

    for full_name in reversed(SILVER_TABLES):
        schema_name, table_name = full_name.split(".", 1)
        if inspector.has_table(table_name, schema=schema_name):
            Base.metadata.tables[full_name].drop(bind=bind, checkfirst=True)
