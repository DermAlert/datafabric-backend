"""fix recipient_access_logs FK to SET NULL on table/schema/share delete

Without ON DELETE SET NULL, deleting a share_table (or share/schema) that
was ever accessed raises an IntegrityError because the audit log still
references its id. Since table_id/schema_id/share_id are all nullable, SET
NULL is the correct behaviour: the audit record is kept but the reference
is cleared.

Revision ID: 20260218_0007
Revises: 20260218_0006
Create Date: 2026-02-18 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "20260218_0007"
down_revision: Union[str, None] = "20260218_0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # table_id
    op.drop_constraint(
        'recipient_access_logs_table_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_table_id_fkey',
        'recipient_access_logs', 'share_tables',
        ['table_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing',
        ondelete='SET NULL'
    )

    # schema_id
    op.drop_constraint(
        'recipient_access_logs_schema_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_schema_id_fkey',
        'recipient_access_logs', 'share_schemas',
        ['schema_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing',
        ondelete='SET NULL'
    )

    # share_id
    op.drop_constraint(
        'recipient_access_logs_share_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_share_id_fkey',
        'recipient_access_logs', 'shares',
        ['share_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing',
        ondelete='SET NULL'
    )


def downgrade() -> None:
    op.drop_constraint(
        'recipient_access_logs_table_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_table_id_fkey',
        'recipient_access_logs', 'share_tables',
        ['table_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing'
    )

    op.drop_constraint(
        'recipient_access_logs_schema_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_schema_id_fkey',
        'recipient_access_logs', 'share_schemas',
        ['schema_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing'
    )

    op.drop_constraint(
        'recipient_access_logs_share_id_fkey',
        'recipient_access_logs',
        schema='delta_sharing',
        type_='foreignkey'
    )
    op.create_foreign_key(
        'recipient_access_logs_share_id_fkey',
        'recipient_access_logs', 'shares',
        ['share_id'], ['id'],
        source_schema='delta_sharing', referent_schema='delta_sharing'
    )
