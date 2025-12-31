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

# Import generic Trino extractor
from .extractors.trino_extractor import TrinoExtractor

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

                record = result.first()
                
                if not record:
                    logger.error(f"No connection found with ID {connection_id}")
                    return False
                    
                connection = record[0]  # DataConnection
                connection_type = record[1]  # ConnectionType
                
                # Validate that connection is not of type IMAGE
                if connection.content_type == 'image':
                    logger.error(f"Cannot extract metadata from connection {connection_id} of type 'image'")
                    connection.sync_status = "failed"
                    connection.status = "error"
                    await db.commit()
                    return False
                
                # Update connection status
                connection.sync_status = "running"
                await db.commit()
                
                logger.info(f"Starting metadata extraction for connection {connection.name} ({connection_type.name})")
                
                # Use TrinoExtractor for all supported types
                try:
                    extractor = TrinoExtractor(
                        connection_params=connection.connection_params,
                        connection_type=connection_type.name,
                        connection_name=connection.name,
                        connection_id=connection.id  # Pass ID for unique catalog name
                    )
                    
                    # Extract Catalogs (usually just the one we created)
                    catalogs = await extractor.extract_catalogs()
                    
                    # Map catalog_name -> catalog_id
                    catalog_map = {}

                    for cat_info in catalogs:
                        cat_name = cat_info["catalog_name"]
                        stmt_cat = select(metadata.ExternalCatalogs).where(
                            (metadata.ExternalCatalogs.connection_id == connection.id) &
                            (metadata.ExternalCatalogs.catalog_name == cat_name)
                        )
                        existing_cat = (await db.execute(stmt_cat)).scalars().first()
                        
                        if existing_cat:
                            cat_obj = existing_cat
                            cat_obj.last_scanned = datetime.now()
                        else:
                            cat_obj = metadata.ExternalCatalogs(
                                connection_id=connection.id,
                                catalog_name=cat_name,
                                catalog_type=cat_info.get("catalog_type"),
                                external_reference=cat_info.get("external_reference"),
                                properties=cat_info.get("properties", {})
                            )
                            db.add(cat_obj)
                            await db.flush()
                        
                        catalog_map[cat_name] = cat_obj.id
                    
                    # Extract Schemas
                    schemas = await extractor.extract_schemas()
                    logger.info(f"Extracted {len(schemas)} schemas")
                    
                    # Store schemas and tables
                    for schema_info in schemas:
                        # Store Schema
                        schema_name = schema_info["schema_name"]
                        
                        # Check if schema exists
                        stmt = select(metadata.ExternalSchema).where(
                            (metadata.ExternalSchema.connection_id == connection.id) &
                            (metadata.ExternalSchema.schema_name == schema_name)
                        )
                        existing_schema = (await db.execute(stmt)).scalars().first()
                        
                        if existing_schema:
                            schema_obj = existing_schema
                            schema_obj.last_scanned = datetime.now()
                        else:
                            # Look up catalog_id
                            catalog_name_extracted = schema_info.get("catalog_name")
                            catalog_id = catalog_map.get(catalog_name_extracted)
                            
                            schema_obj = metadata.ExternalSchema(
                                connection_id=connection.id,
                                schema_name=schema_name,
                                catalog_id=catalog_id,
                                external_reference=schema_info.get("external_reference"),
                                properties=schema_info.get("properties", {})
                            )
                            db.add(schema_obj)
                            await db.flush() # Get ID
                        
                        # Extract Tables for this schema
                        tables = await extractor.extract_tables(schema_name)
                        logger.info(f"Extracted {len(tables)} tables for schema {schema_name}")
                        
                        for table_info in tables:
                            table_name = table_info["table_name"]
                            
                            # Check if table exists
                            stmt_table = select(metadata.ExternalTables).where(
                                (metadata.ExternalTables.schema_id == schema_obj.id) &
                                (metadata.ExternalTables.table_name == table_name)
                            )
                            existing_table = (await db.execute(stmt_table)).scalars().first()
                            
                            if existing_table:
                                table_obj = existing_table
                                table_obj.last_scanned = datetime.now()
                                table_obj.estimated_row_count = table_info.get("estimated_row_count")
                                table_obj.total_size_bytes = table_info.get("total_size_bytes")
                            else:
                                table_obj = metadata.ExternalTables(
                                    schema_id=schema_obj.id,
                                    connection_id=connection.id,
                                    table_name=table_name,
                                    external_reference=table_info.get("external_reference"),
                                    table_type=table_info.get("table_type", "table"),
                                    estimated_row_count=table_info.get("estimated_row_count"),
                                    total_size_bytes=table_info.get("total_size_bytes"),
                                    description=table_info.get("description"),
                                    properties=table_info.get("properties", {})
                                )
                                db.add(table_obj)
                                await db.flush()
                            
                            # Extract Columns
                            columns = await extractor.extract_columns(schema_name, table_name)
                            
                            # Update columns
                            # For simplicity, we might want to delete existing and recreate, or update
                            # Here we'll update/insert
                            existing_col_names = set()
                            if existing_table:
                                stmt_cols = select(metadata.ExternalColumn).where(metadata.ExternalColumn.table_id == table_obj.id)
                                existing_cols = (await db.execute(stmt_cols)).scalars().all()
                                existing_col_names = {c.column_name: c for c in existing_cols}
                            
                            for col_info in columns:
                                col_name = col_info["column_name"]
                                
                                if col_name in existing_col_names:
                                    col_obj = existing_col_names[col_name]
                                    col_obj.data_type = col_info["data_type"]
                                    col_obj.is_nullable = col_info["is_nullable"]
                                    col_obj.column_position = col_info.get("column_position", col_obj.column_position)
                                    col_obj.external_reference = col_info.get("external_reference")
                                    col_obj.sample_values = col_info.get("sample_values", [])
                                    col_obj.max_length = col_info.get("max_length")
                                    col_obj.numeric_precision = col_info.get("numeric_precision")
                                    col_obj.numeric_scale = col_info.get("numeric_scale")
                                    col_obj.is_primary_key = col_info.get("is_primary_key", False)
                                    col_obj.is_unique = col_info.get("is_unique", False)
                                    col_obj.is_indexed = col_info.get("is_indexed", False)
                                    col_obj.default_value = col_info.get("default_value")
                                    col_obj.description = col_info.get("description")
                                    col_obj.statistics = col_info.get("statistics", {})
                                    col_obj.properties = col_info.get("properties", {})
                                else:
                                    col_obj = metadata.ExternalColumn(
                                        table_id=table_obj.id,
                                        column_name=col_name,
                                        external_reference=col_info.get("external_reference"),
                                        data_type=col_info["data_type"],
                                        is_nullable=col_info["is_nullable"],
                                        column_position=col_info["column_position"],
                                        max_length=col_info.get("max_length"),
                                        numeric_precision=col_info.get("numeric_precision"),
                                        numeric_scale=col_info.get("numeric_scale"),
                                        is_primary_key=col_info.get("is_primary_key", False),
                                        is_unique=col_info.get("is_unique", False),
                                        is_indexed=col_info.get("is_indexed", False),
                                        default_value=col_info.get("default_value"),
                                        description=col_info.get("description"),
                                        statistics=col_info.get("statistics", {}),
                                        sample_values=col_info.get("sample_values", []),
                                        properties=col_info.get("properties", {})
                                    )
                                    db.add(col_obj)
                            
                    connection.sync_status = "success"
                    connection.last_sync_time = datetime.now()
                    connection.status = "active"
                    await db.commit()
                    logger.info(f"Metadata extraction completed successfully for {connection.name}")
                    return True
                    
                except Exception as ext_error:
                    logger.error(f"Error during extraction logic: {str(ext_error)}")
                    connection.sync_status = "failed"
                    connection.status = "error"
                    await db.commit()
                    return False
                finally:
                    if 'extractor' in locals():
                        await extractor.close()

            except Exception as e:
                logger.error(f"Database error during metadata extraction: {str(e)}")
                if 'connection' in locals():
                     connection.sync_status = "failed"
                     await db.commit()
                return False
                
    except Exception as e:
        logger.error(f"Fatal error in extract_metadata: {str(e)}")
        return False
