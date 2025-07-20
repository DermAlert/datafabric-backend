from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, TIMESTAMP, Boolean, Enum, DATE, CheckConstraint, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM
from ..database import Base
from ..baseMixin import AuditMixin
import enum

class DatasetAccessLevel(enum.Enum):
    """Define os níveis de permissão para acesso a um Dataset."""
    LER = "ler"              # Permite visualizar os dados e metadados do dataset.
    EDITAR = "editar"        # Permite modificar metadados (descrição, etc.), e potencialmente a definição (colunas, fontes) ou acionar atualizações.
    # EXCLUIR = "excluir"      # Permite apagar o dataset (a definição, não necessariamente os dados brutos originais).
    ADMINISTRAR = "administrar" # Permite gerenciar as permissões de outros usuários/grupos para este dataset (conceder/revogar LER, EDITAR, EXCLUIR).

class ContentType(enum.Enum):
    """Define o tipo de conteúdo armazenado na conexão de dados."""
    METADATA = "metadata"    # Conexão contém metadados estruturados
    IMAGE = "image"          # Conexão contém imagens referenciadas pelos metadados

# Create PostgreSQL enum type with proper handling
content_type_enum = ENUM(
    'metadata', 'image',
    name='contenttype',
    create_type=True  # Auto-create the enum type
)

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

    usuarios = relationship("User", back_populates="organizacao")
    data_connections = relationship("DataConnection", back_populates="organization")

# Models        
class User(AuditMixin, Base):
    __tablename__ = 'users'
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True)
    organization_id = Column(Integer, ForeignKey('core.organizacoes.id'), nullable=False) # every user MUST belong to an org
    nome_usuario = Column(String(50), nullable=True)
    email = Column(String(100), unique=True, index=True, nullable=False)
    cpf = Column(String(11), unique=True, index=True, nullable=False)
    senha_hash = Column(String(255), nullable=True)
    password_reset_token = Column(String(255), nullable=True)
    password_reset_token_used = Column(Boolean, default=False)
    email_invite_token = Column(String(255), nullable=True)
    email_invite_token_used = Column(Boolean, default=False)

    roles = relationship('Role', secondary=user_roles, back_populates='users')

    groups = relationship('Group', secondary='core.user_groups', back_populates='users')
    dataset_permissions = relationship("UserDatasetPermission", back_populates="user")

    organizacao = relationship("Organization", back_populates="usuarios")


class Role(Base):
    __tablename__ = 'roles'
    __table_args__ = {'schema': 'core'}
    id = Column(Integer, primary_key=True)
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
    organization_id = Column(Integer, ForeignKey("core.organizacoes.id"), nullable=False) # Made non-nullable assuming every connection must belong to an org
    name = Column(String(255), nullable=False)
    description = Column(String, nullable=True)
    connection_type_id = Column(Integer, ForeignKey("core.connection_types.id"), nullable=False)
    connection_params = Column(JSON, nullable=False)
    content_type = Column(content_type_enum, nullable=False, default='metadata')
    # credentials_id = Column(Integer, ForeignKey("security.credentials.id"), nullable=True)  -> ainda não existe security.credentials.id
    status = Column(String, nullable=False, default='inactive')  # inactive, active, error
    sync_status = Column(String, nullable=False, default='success')  # success, partial, failed
    last_sync_time = Column(TIMESTAMP(timezone=True), nullable=True)
    next_sync_time = Column(TIMESTAMP(timezone=True), nullable=True)
    cron_expression = Column(String, nullable=True)  # Weekly on Sunday at midnight "0 0 * * 0"
    sync_settings = Column(JSON, nullable=False, default={})  # Additional sync settings

    organization = relationship("Organization", back_populates="data_connections")



user_groups = Table(
    'user_groups', Base.metadata,
    Column('user_id', Integer, ForeignKey('core.users.id', ondelete='CASCADE'), primary_key=True),
    Column('group_id', Integer, ForeignKey('core.groups.id', ondelete='CASCADE'), primary_key=True),
    schema='core' # Or 'auth' schema
)

class Group(Base):
    __tablename__ = 'groups'
    __table_args__ = {'schema': 'core'}

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    organization_id = Column(Integer, ForeignKey('core.organizacoes.id'), nullable=False)
    description = Column(String, nullable=True)

    # Relationship: Users in this group
    users = relationship("User", secondary="core.user_groups", back_populates="groups")
    # Relationship: Datasets this group has access to
    dataset_permissions = relationship("GroupDatasetPermission", back_populates="group")


class UserDatasetPermission(Base):
    __tablename__ = 'user_dataset_permissions'
    __table_args__ = (
        UniqueConstraint('user_id', 'dataset_id'), # User can only have one permission level per dataset
        {'schema': 'core'} # Or 'auth'
    )

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('core.users.id', ondelete='CASCADE'), nullable=False)
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    access_level = Column(Enum(DatasetAccessLevel), nullable=False, default=DatasetAccessLevel.LER)

    # Relationships
    user = relationship("User", back_populates="dataset_permissions")
    dataset = relationship("Dataset", back_populates="user_permissions")


class GroupDatasetPermission(Base):
    __tablename__ = 'group_dataset_permissions'
    __table_args__ = (
        UniqueConstraint('group_id', 'dataset_id'), # Group can only have one permission level per dataset
        {'schema': 'core'} # Or 'auth'
    )

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, ForeignKey('core.groups.id', ondelete='CASCADE'), nullable=False)
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    access_level = Column(Enum(DatasetAccessLevel), nullable=False, default=DatasetAccessLevel.LER)

    # Relationships
    group = relationship("Group", back_populates="dataset_permissions")
    dataset = relationship("Dataset", back_populates="group_permissions")


class DatasetSource(Base):
    __tablename__ = "dataset_sources"
    __table_args__ = (
        UniqueConstraint('dataset_id', 'table_id'),
        {'schema': 'core'}
    )
    
    id = Column(Integer, primary_key=True)  # Antiga dataset_source_id UUID
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    table_id = Column(Integer, ForeignKey('metadata.external_tables.id', ondelete='CASCADE'), nullable=False)
    join_type = Column(String(50), default='primary')  # primary, left, inner, etc.
    join_condition = Column(String)  # Para fontes secundárias, a condição de junção
    filter_condition = Column(String)  # Condição de filtro opcional



class DatasetColumnSource(Base):
    __tablename__ = "dataset_column_sources"
    __table_args__ = {'schema': 'core'}
    
    id = Column(Integer, primary_key=True)  # Antiga column_source_id UUID
    dataset_column_id = Column(Integer, ForeignKey('core.dataset_columns.id', ondelete='CASCADE'), nullable=False)
    source_column_id = Column(Integer, ForeignKey('metadata.external_columns.id', ondelete='CASCADE'), nullable=True)
    transformation_type = Column(String(50), default='direct')  # direct, calculated, aggregated
    transformation_expression = Column(String)  # Fórmula, cálculo ou expressão de agregação
    is_primary_source = Column(Boolean, default=True)



class DatasetColumn(Base):
    __tablename__ = "dataset_columns"
    __table_args__ = (
        UniqueConstraint('dataset_id', 'name'),
        {'schema': 'core'}
    )
    
    id = Column(Integer, primary_key=True)  # Antiga dataset_column_id UUID
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String)
    # column_group_id = Column(Integer, ForeignKey('equivalence.column_groups.id'))  # Mapeamento para o grupo de equivalência -> tirou porque ainda n implementou
    data_type = Column(String(50), nullable=False)
    is_nullable = Column(Boolean, default=True, nullable=False)
    column_position = Column(Integer, nullable=False)
    transformation_expression = Column(String)  # Expressão personalizada para transformações complexas
    is_visible = Column(Boolean, default=True, nullable=False)
    format_pattern = Column(String(100))  # Formato de exibição para a coluna
    properties = Column(JSON, default={})  # Substituto para JSONB


class Dataset(AuditMixin, Base):
    __tablename__ = "datasets"
    __table_args__ = {'schema': 'core'}
    
    id = Column(Integer, primary_key=True)  # Antiga dataset_id UUID
    name = Column(String(255), nullable=False)
    description = Column(String)
    # category_id = Column(Integer, ForeignKey('metadata.categories.id'))  -> não temos um metadata categories
    storage_type = Column(String(50), nullable=False)  # virtual_view, materialized, copy_to_minio
    refresh_type = Column(String(50), default='on_demand', nullable=False)  # on_demand, scheduled, real_time
    refresh_schedule = Column(String(100))  # Expressão cron para atualizações programadas
    status = Column(String(50), default='draft', nullable=False)  # draft, active, deprecated
    version = Column(String(50), default='1.0')
    properties = Column(JSON, default={})  # Substituto para JSONB
    # owner_id = Column(Integer, ForeignKey('security.users.id')) -> ainda n implementado

    user_permissions = relationship("UserDatasetPermission", back_populates="dataset")
    group_permissions = relationship("GroupDatasetPermission", back_populates="dataset")

class DatasetVersion(Base):
    __tablename__ = "dataset_versions"
    __table_args__ = (
        UniqueConstraint('dataset_id', 'version_number'),
        {'schema': 'core'}
    )

    id = Column(Integer, primary_key=True)  
    # Assuming 'core.datasets' table has an Integer primary key named 'id'
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    version_number = Column(String(50), nullable=False)
    version_note = Column(String)
    # Assuming 'security.users' table has an Integer primary key named 'id'
    # created_by_id = Column("created_by", Integer, ForeignKey('security.users.id'))
    is_current = Column(Boolean, default=True, nullable=False) # Assuming non-nullable based on default
    previous_version_id = Column(Integer, ForeignKey('core.dataset_versions.id'))
    job_id = Column(Integer, ForeignKey('workflow.dataset_jobs.id'))
    row_count = Column(Integer)
    schema_hash = Column(String(64))