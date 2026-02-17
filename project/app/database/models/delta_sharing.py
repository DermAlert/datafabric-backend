from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, TIMESTAMP, Boolean, Enum, TEXT, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ENUM
from ..session import Base
from ..base import AuditMixin
import enum

class ShareStatus(enum.Enum):
    """Status of a Delta Sharing share"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"

class TableShareStatus(enum.Enum):
    """Status of a table share"""
    ACTIVE = "active"
    INACTIVE = "inactive"

class ShareTableSourceType(enum.Enum):
    """Source type of a shared table"""
    DELTA = "delta"                                # Generic Delta Lake table (legacy/direct)
    BRONZE = "bronze"                              # Materialized from Bronze Persistent Config
    SILVER = "silver"                              # Materialized from Silver Transform Config
    BRONZE_VIRTUALIZED = "bronze_virtualized"      # On-demand query via Bronze VirtualizedConfig
    SILVER_VIRTUALIZED = "silver_virtualized"      # On-demand query via Silver VirtualizedConfig

# Association tables for many-to-many relationships
recipient_shares = Table(
    'recipient_shares', Base.metadata,
    Column('recipient_id', Integer, ForeignKey('delta_sharing.recipients.id'), primary_key=True),
    Column('share_id', Integer, ForeignKey('delta_sharing.shares.id'), primary_key=True),
    schema='delta_sharing'
)

class Share(AuditMixin, Base):
    """
    A Delta Sharing Share represents a logical grouping of schemas and tables
    that can be shared with recipients.
    """
    __tablename__ = "shares"
    __table_args__ = (
        UniqueConstraint('name'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(TEXT, nullable=True)
    organization_id = Column(Integer, ForeignKey('core.organizacoes.id'), nullable=False)
    status = Column(Enum(ShareStatus), nullable=False, default=ShareStatus.ACTIVE)
    
    # Delta Sharing specific metadata
    owner_email = Column(String(255), nullable=True)
    contact_info = Column(JSON, nullable=True)  # Additional contact information
    terms_of_use = Column(TEXT, nullable=True)  # Terms of use for this share
    
    # Relationships
    schemas = relationship("ShareSchema", back_populates="share", cascade="all, delete-orphan")
    recipients = relationship("Recipient", secondary=recipient_shares, back_populates="shares")

class ShareSchema(AuditMixin, Base):
    """
    A schema within a Delta Sharing Share. Schemas organize tables within a share.
    """
    __tablename__ = "share_schemas"
    __table_args__ = (
        UniqueConstraint('share_id', 'name'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    share_id = Column(Integer, ForeignKey('delta_sharing.shares.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(TEXT, nullable=True)
    
    # Relationships
    share = relationship("Share", back_populates="schemas")
    tables = relationship("ShareTable", back_populates="schema", cascade="all, delete-orphan")

class ShareTable(AuditMixin, Base):
    """
    A table within a Delta Sharing Schema. This represents the actual shared dataset.
    """
    __tablename__ = "share_tables"
    __table_args__ = (
        UniqueConstraint('schema_id', 'name'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    schema_id = Column(Integer, ForeignKey('delta_sharing.share_schemas.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(TEXT, nullable=True)
    
    # Reference to the actual dataset (nullable for virtualized tables)
    dataset_id = Column(Integer, ForeignKey('core.datasets.id'), nullable=True)
    
    # Source type: delta (materialized), bronze_virtualized, silver_virtualized
    source_type = Column(
        Enum(ShareTableSourceType), 
        nullable=False, 
        default=ShareTableSourceType.DELTA,
        server_default='DELTA'
    )
    
    # Reference to virtualized config (only one should be set, based on source_type)
    bronze_virtualized_config_id = Column(Integer, nullable=True)
    silver_virtualized_config_id = Column(Integer, nullable=True)
    
    # Delta Sharing specific configuration
    status = Column(Enum(TableShareStatus), nullable=False, default=TableShareStatus.ACTIVE)
    share_mode = Column(String(50), nullable=False, default='full')  # full, filtered, aggregated
    filter_condition = Column(TEXT, nullable=True)  # SQL WHERE clause for filtered sharing
    
    # Table versioning for Delta Sharing protocol
    current_version = Column(Integer, nullable=False, default=1)
    min_reader_version = Column(Integer, nullable=False, default=1)
    min_writer_version = Column(Integer, nullable=False, default=1)
    
    # Table metadata for Delta protocol
    table_format = Column(String(50), nullable=False, default='parquet')
    partition_columns = Column(JSON, nullable=True)  # List of partition column names
    schema_string = Column(TEXT, nullable=True)  # JSON schema string for Delta protocol
    table_properties = Column(JSON, nullable=True)  # Additional table properties
    
    # Storage information
    storage_location = Column(String(500), nullable=True)  # S3/MinIO location
    storage_credentials_id = Column(Integer, nullable=True)  # Reference to storage credentials
    
    # Relationships
    schema = relationship("ShareSchema", back_populates="tables")
    # Note: We'll add relationship to Dataset when needed

class Recipient(AuditMixin, Base):
    """
    A recipient represents an entity (user, organization, or system) that can access shared data.
    """
    __tablename__ = "recipients"
    __table_args__ = (
        UniqueConstraint('identifier'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    identifier = Column(String(255), nullable=False, unique=True)  # Unique identifier for recipient
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    organization_name = Column(String(255), nullable=True)
    
    # Authentication and access control
    authentication_type = Column(String(50), nullable=False, default='bearer_token')  # bearer_token, basic_auth, etc.
    bearer_token = Column(String(500), nullable=True)  # For bearer token authentication
    token_expiry = Column(TIMESTAMP(timezone=True), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    
    # Rate limiting and usage tracking
    max_requests_per_hour = Column(Integer, nullable=True)
    max_downloads_per_day = Column(Integer, nullable=True)
    
    # Audit and compliance
    access_logged = Column(Boolean, nullable=False, default=True)
    data_usage_agreement_accepted = Column(Boolean, nullable=False, default=False)
    agreement_accepted_at = Column(TIMESTAMP(timezone=True), nullable=True)
    
    # Additional metadata
    contact_info = Column(JSON, nullable=True)
    notes = Column(TEXT, nullable=True)
    
    # Relationships
    shares = relationship("Share", secondary=recipient_shares, back_populates="recipients")
    access_logs = relationship("RecipientAccessLog", back_populates="recipient")

class RecipientAccessLog(Base):
    """
    Log of recipient access to shared data for audit and compliance purposes.
    """
    __tablename__ = "recipient_access_logs"
    __table_args__ = {'schema': 'delta_sharing'}
    
    id = Column(Integer, primary_key=True)
    recipient_id = Column(Integer, ForeignKey('delta_sharing.recipients.id'), nullable=False)
    share_id = Column(Integer, ForeignKey('delta_sharing.shares.id'), nullable=True)
    schema_id = Column(Integer, ForeignKey('delta_sharing.share_schemas.id'), nullable=True)
    table_id = Column(Integer, ForeignKey('delta_sharing.share_tables.id'), nullable=True)
    
    # Request details
    operation = Column(String(100), nullable=False)  # list_shares, get_table_metadata, query_table, etc.
    request_method = Column(String(10), nullable=False)  # GET, POST
    request_path = Column(String(500), nullable=False)
    request_query_params = Column(JSON, nullable=True)
    request_body = Column(TEXT, nullable=True)
    
    # Response details
    response_status_code = Column(Integer, nullable=False)
    response_size_bytes = Column(Integer, nullable=True)
    processing_time_ms = Column(Integer, nullable=True)
    
    # Client information
    client_ip = Column(String(45), nullable=True)  # IPv6 support
    user_agent = Column(String(500), nullable=True)
    
    # Timestamp
    accessed_at = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    
    # Error information
    error_message = Column(TEXT, nullable=True)
    
    # Relationships
    recipient = relationship("Recipient", back_populates="access_logs")

class ShareTableVersion(Base):
    """
    Track versions of shared tables for Delta Sharing protocol compliance.
    """
    __tablename__ = "share_table_versions"
    __table_args__ = (
        UniqueConstraint('table_id', 'version'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('delta_sharing.share_tables.id', ondelete='CASCADE'), nullable=False)
    version = Column(Integer, nullable=False)
    
    # Version metadata
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False, default=func.now())
    operation = Column(String(50), nullable=False)  # CREATE, UPDATE, DELETE, etc.
    description = Column(TEXT, nullable=True)
    
    # Delta protocol specific
    protocol_version = Column(JSON, nullable=True)  # Protocol version info
    table_metadata = Column(JSON, nullable=True)  # Table metadata for this version
    schema_string = Column(TEXT, nullable=True)  # Schema at this version
    
    # File information
    add_files = Column(JSON, nullable=True)  # Files added in this version
    remove_files = Column(JSON, nullable=True)  # Files removed in this version
    
    # Statistics
    num_files = Column(Integer, nullable=True)
    total_size_bytes = Column(Integer, nullable=True)
    num_records = Column(Integer, nullable=True)

class ShareTableFile(Base):
    """
    Track individual files that make up a shared table for Delta Sharing protocol.
    """
    __tablename__ = "share_table_files"
    __table_args__ = (
        UniqueConstraint('table_id', 'file_path'),
        {'schema': 'delta_sharing'}
    )
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('delta_sharing.share_tables.id', ondelete='CASCADE'), nullable=False)
    file_path = Column(String(1000), nullable=False)  # Relative path within table storage
    
    # File metadata
    file_size_bytes = Column(Integer, nullable=False)
    modification_time = Column(TIMESTAMP(timezone=True), nullable=False)
    
    # Delta protocol specific
    data_change = Column(Boolean, nullable=False, default=True)
    partition_values = Column(JSON, nullable=True)  # Partition values for this file
    
    # Statistics (for Delta protocol)
    num_records = Column(Integer, nullable=True)
    min_values = Column(JSON, nullable=True)  # Min values per column
    max_values = Column(JSON, nullable=True)  # Max values per column
    null_count = Column(JSON, nullable=True)  # Null count per column
    
    # Version tracking
    created_at_version = Column(Integer, nullable=False)
    removed_at_version = Column(Integer, nullable=True)
    
    # Storage information
    storage_url = Column(String(1000), nullable=True)  # Full URL for file access
    content_length = Column(Integer, nullable=True)
    etag = Column(String(100), nullable=True)
