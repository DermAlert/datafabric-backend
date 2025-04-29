from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, TIMESTAMP, Boolean, Enum, DATE, CheckConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base
from .baseMixin import AuditMixin

# Association tables
user_roles = Table(
    'user_roles', Base.metadata,
    Column('user_id', Integer, ForeignKey('core.users.id')),
    Column('role_id', Integer, ForeignKey('core.roles.id')),
    schema='core'
)

class Organization(Base):
    __tablename__ = "organizacoes"
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True)
    nome = Column(String, nullable=False)

    usuarios = relationship("Usuario", back_populates="organizacao")

# Models
class User(AuditMixin, Base):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True, index=True)
    nome_usuario = Column(String(50), index=True, nullable=True)
    email = Column(String(100), unique=True, index=True, nullable=False)
    cpf = Column(String(11), unique=True, index=True, nullable=False)
    senha_hash = Column(String(255), nullable=True)
    password_reset_token = Column(String(255), nullable=True)
    password_reset_token_used = Column(Boolean, default=False)
    email_invite_token = Column(String(255), nullable=True)
    email_invite_token_used = Column(Boolean, default=False)

    roles = relationship('Role', secondary=user_roles, back_populates='users')

class Role(Base):
    __tablename__ = 'roles'
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, index=True, nullable=False)
    nivel_acesso = Column(Integer, nullable=False, default=1)
    users = relationship('User', secondary=user_roles, back_populates='roles')

class ConnectionType(Base):
    __tablename__ = "connection_types"
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(String, nullable=True)
    icon = Column(String, nullable=True)
    color_hex = Column(String(7), nullable=True)
    connection_params_schema = Column(JSON, nullable=False)  # JSON Schema for connection parameters
    metadata_extraction_method = Column(String, nullable=False)  # e.g., airflow_job, direct_query, api_call

class DataConnection(Base):
    __tablename__ = "data_connections"
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(String, nullable=True)
    connection_type_id = Column(Integer, ForeignKey("core.connection_types.id"), nullable=False)
    connection_params = Column(JSON, nullable=False)
    # credentials_id = Column(Integer, ForeignKey("security.credentials.id"), nullable=True)  -> ainda não existe security.credentials.id
    status = Column(String, nullable=False, default='inactive')  # inactive, active, error
    sync_status = Column(String, nullable=False, default='success')  # success, partial, failed
    last_sync_time = Column(TIMESTAMP(timezone=True), nullable=True)
    next_sync_time = Column(TIMESTAMP(timezone=True), nullable=True)
    # sync_frequency = Column(String, nullable=False, default='weekly')  # daily, weekly, monthly    -> tirei porque o cron ja pode definir isso sem precisar dessa variavel
    cron_expression = Column(String, nullable=True)  # Weekly on Sunday at midnight "0 0 * * 0"
    sync_settings = Column(JSON, nullable=False, default={})  # Additional sync settings

