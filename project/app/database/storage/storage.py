from sqlalchemy import Column, Integer, String, ForeignKey, Table, JSON, ARRAY, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class DatasetStorage(Base):
    __tablename__ = "dataset_storage"
    __table_args__ = {'schema': 'storage'}
    
    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey('core.datasets.id', ondelete='CASCADE'), nullable=False)  # Referenciando datasets.id
    storage_type = Column(String(50), nullable=False)  # minio, postgres, parquet_files, etc.
    storage_location = Column(String(1024), nullable=False)  # URI, path, or connection details, s3://my-bucket/dataset | postgres://my-connection/dataset 
    file_format = Column(String(50))  # csv, parquet, json, etc.
    compression = Column(String(50))  # gzip, snappy, none, etc.
    partition_columns = Column(ARRAY(Text))  # Lista de colunas para particionar, generalmente usado no minio
    storage_properties = Column(JSON, default={})  # "access_key": "minio-key", "secret_key": "minio-secret", etc. | "connection_url": "postgres://user:password@host:port/dbname", etc.