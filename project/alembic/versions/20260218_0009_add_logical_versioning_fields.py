"""add logical versioning fields to bronze configs and executions

Adds fields for logical (execution-based) versioning:
- BronzePersistentConfig.known_output_paths: stable registry of all output paths
- BronzeExecution.version_number: sequential logical version per config
- BronzeExecution.path_delta_versions: per-path Delta Lake version snapshot

Revision ID: 20260218_0009
Revises: 20260218_0008
Create Date: 2026-02-18 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision: str = "20260218_0009"
down_revision: Union[str, None] = "20260218_0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bronze_configs_cols = {
        col["name"]
        for col in inspect(op.get_bind()).get_columns('bronze_persistent_configs', schema='datasets')
    }
    if 'known_output_paths' not in bronze_configs_cols:
        op.add_column(
            'bronze_persistent_configs',
            sa.Column('known_output_paths', sa.JSON(), nullable=True),
            schema='datasets'
        )

    bronze_exec_cols = {
        col["name"]
        for col in inspect(op.get_bind()).get_columns('bronze_executions', schema='datasets')
    }
    if 'version_number' not in bronze_exec_cols:
        op.add_column(
            'bronze_executions',
            sa.Column('version_number', sa.Integer(), nullable=True),
            schema='datasets'
        )
    if 'path_delta_versions' not in bronze_exec_cols:
        op.add_column(
            'bronze_executions',
            sa.Column('path_delta_versions', sa.JSON(), nullable=True),
            schema='datasets'
        )


def downgrade() -> None:
    op.drop_column('bronze_executions', 'path_delta_versions', schema='datasets')
    op.drop_column('bronze_executions', 'version_number', schema='datasets')
    op.drop_column('bronze_persistent_configs', 'known_output_paths', schema='datasets')
