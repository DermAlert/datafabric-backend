from sqlalchemy import Column, TIMESTAMP, Boolean, Integer, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declared_attr
from datetime import datetime, timezone

def _utcnow() -> datetime:
    """Python-side callable for onupdate.

    Using a Python callable (instead of func.now() SQL expression) ensures
    SQLAlchemy sets the value in Python *before* the UPDATE flush, so the
    ORM object keeps the fresh timestamp without needing an extra SELECT.
    This avoids the async greenlet lazy-load error when accessing
    data_atualizacao after commit in async routes.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)

class AuditMixin:
    @declared_attr
    def data_criacao(cls):
        return Column(TIMESTAMP, server_default=func.now(), nullable=False)

    @declared_attr
    def data_atualizacao(cls):
        return Column(TIMESTAMP, server_default=func.now(), onupdate=_utcnow, nullable=False)

    @declared_attr
    def id_usuario_criacao(cls):
        return Column(Integer, ForeignKey('core.users.id'), nullable=True)

    @declared_attr
    def id_usuario_atualizacao(cls):
        return Column(Integer, ForeignKey('core.users.id'), nullable=True)

    @declared_attr
    def fl_ativo(cls):
        return Column(Boolean, default=True, nullable=False)
