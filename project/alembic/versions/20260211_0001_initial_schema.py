"""initial schema

Revision ID: 20260211_0001
Revises:
Create Date: 2026-02-11 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

from app.database.session import Base

# Import model modules so all tables are registered before create_all/drop_all.
from app.database.models import (  # noqa: F401
    bronze,
    core,
    delta_sharing,
    equivalence,
    metadata,
    relationships,
    silver,
    storage,
    workflow,
)

# revision identifiers, used by Alembic.
revision: str = "20260211_0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMAS = (
    "core",
    "equivalence",
    "metadata",
    "storage",
    "workflow",
    "delta_sharing",
    "datasets",
)


def upgrade() -> None:
    bind = op.get_bind()

    for schema_name in SCHEMAS:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    Base.metadata.create_all(bind=bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()

    Base.metadata.drop_all(bind=bind, checkfirst=True)

    for schema_name in reversed(SCHEMAS):
        op.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
