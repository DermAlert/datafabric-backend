from sqlalchemy import Column, Integer, String, ForeignKey, JSON, TIMESTAMP
from sqlalchemy.sql import func
from ..session import Base

class SyncJob(Base):
    __tablename__ = "metadata_sync_jobs"
    __table_args__ = {'schema': 'workflow'}
    id = Column(Integer, primary_key=True)
    connection_id = Column(Integer, ForeignKey("core.data_connections.id", ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    airflow_dag_id = Column(String(255), nullable=True)
    job_type = Column(String, nullable=False)  # full, incremental, schemas_only, stats_update
    status = Column(String, nullable=False, default='pending')  # pending, running, success, failed
    last_run_at = Column(TIMESTAMP(timezone=True), nullable=True)
    last_run_duration_seconds = Column(Integer, nullable=True)
    error_message = Column(String, nullable=True)
    job_params = Column(JSON, nullable=False, default={})
    job_results = Column(JSON, nullable=False, default={})  # LIKE core.base_entity INCLUDING DEFAULTS

class SyncExecutionHistory(Base):
    __tablename__ = "sync_execution_history"
    __table_args__ = {'schema': 'workflow'}
    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("workflow.metadata_sync_jobs.id", ondelete='CASCADE'), nullable=False)
    connection_id = Column(Integer, ForeignKey("core.data_connections.id", ondelete='CASCADE'), nullable=False)
    execution_start = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    execution_end = Column(TIMESTAMP(timezone=True), nullable=True)
    status = Column(String, nullable=False, default='running')  # running, success, failed
    catalog_count = Column(Integer, nullable=False, default=0)
    schemas_count = Column(Integer, nullable=False, default=0)
    table_count = Column(Integer, nullable=False, default=0)
    column_count = Column(Integer, nullable=False, default=0)
    new_objects_count = Column(Integer, nullable=False, default=0)
    updated_objects_count = Column(Integer, nullable=False, default=0)
    deleted_objects_count = Column(Integer, nullable=False, default=0)
    error_log = Column(String, nullable=True)
    execution_details = Column(JSON, nullable=False, default={})  # LIKE core.base_entity INCLUDING DEFAULTS

class DatasetJob(Base):
    __tablename__ = "dataset_jobs"
    __table_args__ = {'schema': 'workflow'}

    id = Column(Integer, primary_key=True)  # Replaced job_id UUID
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)
    job_type = Column(String(50), nullable=False) # e.g., materialize, refresh, validate
    airflow_dag_id = Column(String(255))
    status = Column(String(50), nullable=False, default='pending')
    last_run_start = Column(TIMESTAMP(timezone=True))
    last_run_end = Column(TIMESTAMP(timezone=True))
    last_run_duration_seconds = Column(Integer)
    row_count = Column(Integer)
    size_bytes = Column(Integer)
    error_message = Column(String)
    job_params = Column(JSON, default={}) # Changed from JSONB to JSON for broader compatibility
