from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import asyncio

from ..database.database import get_db
from ..database.core import core
from ..database.metadata import metadata
from ..utils.logger import logger

# Import specific extractors
from .extractors.postgres_extractor import PostgresExtractor
from .extractors.delta_extractor import DeltaExtractor
# from .extractors.mongodb_extractor import MongoDBExtractor
# from .extractors.minio_extractor import MinioExtractor
# from .extractors.mysql_extractor import MySQLExtractor
# from .extractors.bigquery_extractor import BigQueryExtractor

# Map connection types to their extractors
EXTRACTORS = {
    "postgresql": PostgresExtractor,
    "postgres": PostgresExtractor,
    "deltalake": DeltaExtractor,
    "delta": DeltaExtractor,
    # "mongodb": MongoDBExtractor,
    # "minio": MinioExtractor,
    # "s3": MinioExtractor,
    # "mysql": MySQLExtractor,
    # "bigquery": BigQueryExtractor,
    # Add more extractors as needed
}

async def extract_metadata(connection_id: int) -> bool:
    """
    Extract metadata from a data connection and store it in the database.
    """
    try:
        # Create a new session
        async for db in get_db():
            try:
                # Get connection details
                result = await db.execute(
                    select(core.DataConnection, core.ConnectionType).join(
                        core.ConnectionType, 
                        core.DataConnection.connection_type_id == core.ConnectionType.id
                    ).where(
                        core.DataConnection.id == connection_id
                    )
                )

                # Check if the result is not empty
                if not result:
                    logger.error(f"No connection found with ID (result) {connection_id}")
                    return False


                record = result.first()
                
                if not record:
                    logger.error(f"No connection found with ID (record) {connection_id}")
                    return False
                    
                connection = record[0]  # DataConnection
                print("connection", record[0])
                connection_type = record[1]  # ConnectionType
                print("connection_type", record[1])
                
                # Update connection status
                connection.sync_status = "running"
                connection.last_sync_time = datetime.now()
                await db.commit()
                
                # Get the appropriate extractor
                extractor_class = EXTRACTORS.get(connection_type.name.lower())
                if not extractor_class:
                    logger.error(f"No extractor available for connection type {connection_type.name}")
                    connection.sync_status = "failed"
                    connection.status = "error"
                    await db.commit()
                    return False
                
                # Initialize extractor
                extractor = extractor_class(connection.connection_params)
                
                # Extract metadata
                try:
                    # Extract catalogs (for sources that support them)
                    catalogs = await extractor.extract_catalogs()
                    catalog_map = {}
                    
                    for catalog_data in catalogs:
                        catalog = await save_catalog(
                            db, 
                            connection_id=connection_id, 
                            catalog_data=catalog_data
                        )
                        catalog_map[catalog_data.get("catalog_name")] = catalog.id
                    
                    # Extract schemas
                    schemas = await extractor.extract_schemas()
                    schema_map = {}
                    
                    for schema_data in schemas:
                        catalog_id = None
                        if schema_data.get("catalog_name") and schema_data["catalog_name"] in catalog_map:
                            catalog_id = catalog_map[schema_data["catalog_name"]]
                            
                        schema = await save_schema(
                            db,
                            connection_id=connection_id,
                            catalog_id=catalog_id,
                            schema_data=schema_data
                        )
                        schema_map[schema_data.get("schema_name")] = schema.id
                    
                    # Extract tables and columns
                    success = True
                    for schema_name, schema_id in schema_map.items():
                        tables = await extractor.extract_tables(schema_name)
                        
                        for table_data in tables:
                            table = await save_table(
                                db,
                                schema_id=schema_id,
                                connection_id=connection_id,
                                table_data=table_data
                            )
                            
                            # Extract columns for this table
                            columns = await extractor.extract_columns(
                                schema_name=schema_name,
                                table_name=table_data.get("table_name")
                            )

                            print(f"Extracted columns for table {table_data.get('table_name')}: {columns}")
                            
                            for column_data in columns:
                                await save_column(
                                    db,
                                    table_id=table.id,
                                    column_data=column_data
                                )
                    
                    # Update connection status
                    connection.sync_status = "success"
                    connection.status = "active"
                    connection.last_sync_time = datetime.now()
                    # Calculate next sync time based on cron expression if provided
                    # This would require a cron parser library
                    
                    await db.commit()
                    return True
                    
                except Exception as e:
                    logger.error(f"Error extracting metadata for connection {connection_id}: {str(e)}")
                    connection.sync_status = "failed"
                    connection.status = "error"
                    await db.commit()
                    return False
                
            except Exception as e:
                logger.error(f"Error in extract_metadata for connection {connection_id}: {str(e)}")
                await db.rollback()
                return False
    except Exception as e:
        logger.error(f"Failed to create session for metadata extraction: {str(e)}")
        return False

async def save_catalog(
    db: AsyncSession, 
    connection_id: int, 
    catalog_data: Dict[str, Any]
) -> metadata.ExternalCatalogs:
    """
    Save a catalog to the database, update if it already exists.
    """
    result = await db.execute(
        select(metadata.ExternalCatalogs).where(
            (metadata.ExternalCatalogs.connection_id == connection_id) &
            (metadata.ExternalCatalogs.catalog_name == catalog_data.get("catalog_name"))
        )
    )
    catalog = result.scalars().first()
    
    if catalog:
        # Update existing catalog
        for key, value in catalog_data.items():
            if key != "catalog_id" and hasattr(catalog, key):
                setattr(catalog, key, value)
    else:
        # Create new catalog
        catalog = metadata.ExternalCatalogs(
            connection_id=connection_id,
            catalog_name=catalog_data.get("catalog_name"),
            catalog_type=catalog_data.get("catalog_type"),
            external_reference=catalog_data.get("external_reference"),
            properties=catalog_data.get("properties", {})
        )
        db.add(catalog)
    
    await db.flush()
    return catalog

async def save_schema(
    db: AsyncSession, 
    connection_id: int, 
    catalog_id: Optional[int],
    schema_data: Dict[str, Any]
) -> metadata.ExternalSchema:
    """
    Save a schema to the database, update if it already exists.
    """
    result = await db.execute(
        select(metadata.ExternalSchema).where(
            (metadata.ExternalSchema.connection_id == connection_id) &
            (metadata.ExternalSchema.schema_name == schema_data.get("schema_name"))
        )
    )
    schema = result.scalars().first()
    
    if schema:
        # Update existing schema
        for key, value in schema_data.items():
            if key not in ["schema_id", "catalog_name"] and hasattr(schema, key):
                setattr(schema, key, value)
        
        schema.catalog_id = catalog_id
    else:
        # Create new schema
        schema = metadata.ExternalSchema(
            connection_id=connection_id,
            catalog_id=catalog_id,
            schema_name=schema_data.get("schema_name"),
            external_reference=schema_data.get("external_reference"),
            properties=schema_data.get("properties", {})
        )
        db.add(schema)
    
    await db.flush()
    return schema

async def save_table(
    db: AsyncSession, 
    schema_id: int,
    connection_id: int,
    table_data: Dict[str, Any]
) -> metadata.ExternalTables:
    """
    Save a table to the database, update if it already exists.
    """
    result = await db.execute(
        select(metadata.ExternalTables).where(
            (metadata.ExternalTables.schema_id == schema_id) &
            (metadata.ExternalTables.table_name == table_data.get("table_name"))
        )
    )
    table = result.scalars().first()
    
    if table:
        # Update existing table
        for key, value in table_data.items():
            if key != "table_id" and hasattr(table, key):
                setattr(table, key, value)
    else:
        # Create new table
        table = metadata.ExternalTables(
            schema_id=schema_id,
            connection_id=connection_id,
            table_name=table_data.get("table_name"),
            external_reference=table_data.get("external_reference"),
            table_type=table_data.get("table_type", "table"),
            estimated_row_count=table_data.get("estimated_row_count"),
            total_size_bytes=table_data.get("total_size_bytes"),
            last_analyzed=table_data.get("last_analyzed"),
            properties=table_data.get("properties", {}),
            description=table_data.get("description")
        )
        db.add(table)
    
    await db.flush()
    return table

async def save_column(
    db: AsyncSession, 
    table_id: int,
    column_data: Dict[str, Any]
) -> metadata.ExternalColumn:
    """
    Save a column to the database, update if it already exists.
    """
    result = await db.execute(
        select(metadata.ExternalColumn).where(
            (metadata.ExternalColumn.table_id == table_id) &
            (metadata.ExternalColumn.column_name == column_data.get("column_name"))
        )
    )
    column = result.scalars().first()
    
    if column:
        # Update existing column
        for key, value in column_data.items():
            if key != "column_id" and hasattr(column, key):
                setattr(column, key, value)
    else:
        # Create new column
        column = metadata.ExternalColumn(
            table_id=table_id,
            column_name=column_data.get("column_name"),
            external_reference=column_data.get("external_reference"),
            data_type=column_data.get("data_type", "unknown"),
            is_nullable=column_data.get("is_nullable", True),
            column_position=column_data.get("column_position", 0),
            max_length=column_data.get("max_length"),
            numeric_precision=column_data.get("numeric_precision"),
            numeric_scale=column_data.get("numeric_scale"),
            is_primary_key=column_data.get("is_primary_key", False),
            is_unique=column_data.get("is_unique", False),
            is_indexed=column_data.get("is_indexed", False),
            default_value=column_data.get("default_value"),
            description=column_data.get("description"),
            statistics=column_data.get("statistics", {}),
            sample_values=column_data.get("sample_values", []),
            properties=column_data.get("properties", {})
        )
        db.add(column)
    
    await db.flush()
    return column