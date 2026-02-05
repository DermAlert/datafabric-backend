from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from typing import Dict, Any, List, Optional
from datetime import datetime

from ..database.database import get_db
from ..database.core import core
from ..database.metadata import metadata
from ..utils.logger import logger
from .credential_service import get_credential_service

# Import generic Trino extractor
from .extractors.trino_extractor import TrinoExtractor


async def _update_progress(
    db: AsyncSession,
    connection: core.DataConnection,
    progress: int,
    phase: str,
    message: str,
    current_item: Optional[str] = None,
    items_done: int = 0,
    total_items: int = 0
):
    """
    Update sync progress in the database.
    Progress phases:
    - 0-5%: Setup
    - 5-15%: Catalogs
    - 15-30%: Schemas
    - 30-70%: Tables
    - 70-100%: Columns
    """
    connection.sync_progress = min(progress, 100)
    connection.sync_progress_details = {
        "phase": phase,
        "message": message,
        "current_item": current_item,
        "items_done": items_done,
        "total_items": total_items,
        "updated_at": datetime.now().isoformat()
    }
    await db.commit()
    logger.debug(f"Progress: {progress}% - {phase}: {message}")


async def _get_or_create_catalog(
    db: AsyncSession,
    connection_id: int,
    cat_info: Dict[str, Any]
) -> int:
    """
    Get existing catalog or create new one. Handles race conditions gracefully.
    Returns catalog_id.
    """
    cat_name = cat_info["catalog_name"]
    
    # First try to find existing
    stmt = select(metadata.ExternalCatalogs).where(
        (metadata.ExternalCatalogs.connection_id == connection_id) &
        (metadata.ExternalCatalogs.catalog_name == cat_name)
    )
    existing = (await db.execute(stmt)).scalars().first()
    
    if existing:
        existing.last_scanned = datetime.now()
        return existing.id
    
    # Try to create
    try:
        cat_obj = metadata.ExternalCatalogs(
            connection_id=connection_id,
            catalog_name=cat_name,
            catalog_type=cat_info.get("catalog_type"),
            external_reference=cat_info.get("external_reference"),
            properties=cat_info.get("properties", {})
        )
        db.add(cat_obj)
        await db.flush()
        return cat_obj.id
    except IntegrityError:
        # Race condition - another process created it, rollback and fetch
        await db.rollback()
        existing = (await db.execute(stmt)).scalars().first()
        if existing:
            return existing.id
        raise  # Re-raise if still not found


async def _get_or_create_schema(
    db: AsyncSession,
    connection_id: int,
    catalog_id: Optional[int],
    schema_info: Dict[str, Any]
) -> metadata.ExternalSchema:
    """
    Get existing schema or create new one. Handles race conditions gracefully.
    Returns schema object.
    """
    schema_name = schema_info["schema_name"]
    
    # First try to find existing
    stmt = select(metadata.ExternalSchema).where(
        (metadata.ExternalSchema.connection_id == connection_id) &
        (metadata.ExternalSchema.schema_name == schema_name)
    )
    existing = (await db.execute(stmt)).scalars().first()
    
    if existing:
        existing.last_scanned = datetime.now()
        return existing
    
    # Try to create
    try:
        schema_obj = metadata.ExternalSchema(
            connection_id=connection_id,
            schema_name=schema_name,
            catalog_id=catalog_id,
            external_reference=schema_info.get("external_reference"),
            properties=schema_info.get("properties", {})
        )
        db.add(schema_obj)
        await db.flush()
        return schema_obj
    except IntegrityError:
        # Race condition - another process created it, rollback and fetch
        await db.rollback()
        existing = (await db.execute(stmt)).scalars().first()
        if existing:
            return existing
        raise  # Re-raise if still not found


async def _get_or_create_table(
    db: AsyncSession,
    schema_id: int,
    connection_id: int,
    table_info: Dict[str, Any]
) -> metadata.ExternalTables:
    """
    Get existing table or create new one. Handles race conditions gracefully.
    Returns table object.
    """
    table_name = table_info["table_name"]
    
    # First try to find existing
    stmt = select(metadata.ExternalTables).where(
        (metadata.ExternalTables.schema_id == schema_id) &
        (func.lower(metadata.ExternalTables.table_name) == table_name.lower())
    )
    existing = (await db.execute(stmt)).scalars().first()
    
    if existing:
        # Update existing
        existing.last_scanned = datetime.now()
        existing.estimated_row_count = table_info.get("estimated_row_count")
        existing.total_size_bytes = table_info.get("total_size_bytes")
        existing.table_name = table_name
        existing.external_reference = table_info.get("external_reference")
        return existing
    
    # Try to create
    try:
        table_obj = metadata.ExternalTables(
            schema_id=schema_id,
            connection_id=connection_id,
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
        return table_obj
    except IntegrityError:
        # Race condition - another process created it, rollback and fetch
        await db.rollback()
        existing = (await db.execute(stmt)).scalars().first()
        if existing:
            return existing
        raise


async def extract_metadata(connection_id: int) -> bool:
    """
    Extract metadata from a data connection and store it in the database.
    Handles race conditions gracefully for concurrent syncs.
    Tracks progress in sync_progress and sync_progress_details fields.
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
                    connection.sync_progress = 0
                    connection.sync_progress_details = {"phase": "error", "message": "Cannot sync image connections"}
                    await db.commit()
                    return False
                
                # Update connection status - starting
                connection.sync_status = "running"
                connection.sync_progress = 0
                await _update_progress(db, connection, 0, "setup", "Initializing metadata extraction...")
                
                logger.info(f"Starting metadata extraction for connection {connection.name} ({connection_type.name})")
                
                # Use TrinoExtractor for all supported types
                try:
                    # Progress: 0-5% - Setup
                    await _update_progress(db, connection, 2, "setup", "Creating Trino extractor...")
                    
                    # SEGURANÇA: Descriptografar credenciais antes de usar
                    credential_service = get_credential_service()
                    decrypted_params = credential_service.decrypt_for_use(
                        connection.connection_params,
                        connection_id=connection.id,
                        purpose="metadata_extraction"
                    )
                    
                    extractor = TrinoExtractor(
                        connection_params=decrypted_params,
                        connection_type=connection_type.name,
                        connection_name=connection.name,
                        connection_id=connection.id  # Pass ID for unique catalog name
                    )
                    
                    await _update_progress(db, connection, 4, "setup", "Ensuring Trino catalog exists...")
                    
                    # Ensure catalog exists (async to not block event loop)
                    await extractor.ensure_connection()
                    
                    await _update_progress(db, connection, 5, "setup", "Setup complete")
                    
                    # Progress: 5-15% - Extract Catalogs
                    await _update_progress(db, connection, 6, "catalogs", "Extracting catalogs...")
                    
                    catalogs = await extractor.extract_catalogs()
                    
                    # Map catalog_name -> catalog_id
                    catalog_map = {}
                    total_catalogs = len(catalogs)

                    for i, cat_info in enumerate(catalogs):
                        progress = 6 + int((i + 1) / max(total_catalogs, 1) * 9)  # 6-15%
                        await _update_progress(
                            db, connection, progress, "catalogs",
                            f"Processing catalog: {cat_info['catalog_name']}",
                            current_item=cat_info['catalog_name'],
                            items_done=i + 1,
                            total_items=total_catalogs
                        )
                        cat_id = await _get_or_create_catalog(db, connection.id, cat_info)
                        catalog_map[cat_info["catalog_name"]] = cat_id
                    
                    # Progress: 15-50% - Extract all metadata in parallel using Trino
                    await _update_progress(db, connection, 15, "extracting", "Extracting metadata in parallel...")
                    
                    # Use parallel extraction for better performance
                    full_metadata = await extractor.extract_full_metadata()
                    
                    schemas = full_metadata["schemas"]
                    schema_tables = full_metadata["tables"]
                    all_columns = full_metadata["columns"]
                    
                    total_schemas = len(schemas)
                    total_tables = sum(len(tables) for tables in schema_tables.values())
                    logger.info(f"Extracted {total_schemas} schemas, {total_tables} tables in parallel")
                    
                    await _update_progress(db, connection, 50, "storing", "Storing metadata in database...")
                    
                    # Now store everything in the database
                    tables_processed = 0
                    
                    for schema_idx, schema_info in enumerate(schemas):
                        schema_name = schema_info["schema_name"]
                        catalog_name = schema_info.get("catalog_name")
                        catalog_id = catalog_map.get(catalog_name)
                        
                        # Progress: 50-70% for schemas and tables
                        schema_progress = 50 + int((schema_idx + 1) / max(total_schemas, 1) * 20)
                        await _update_progress(
                            db, connection, schema_progress, "storing",
                            f"Storing schema: {schema_name}",
                            current_item=schema_name,
                            items_done=schema_idx + 1,
                            total_items=total_schemas
                        )
                        
                        # Get or create schema (handles race conditions)
                        schema_obj = await _get_or_create_schema(
                            db, connection.id, catalog_id, schema_info
                        )
                        
                        # Get tables for this schema from pre-extracted data
                        tables = schema_tables.get(schema_name, [])
                        
                        for table_info in tables:
                            tables_processed += 1
                            table_name = table_info["table_name"]
                            
                            # Get or create table (handles race conditions)
                            table_obj = await _get_or_create_table(
                                db, schema_obj.id, connection.id, table_info
                            )
                            
                            # Get columns from pre-extracted data
                            columns = all_columns.get((schema_name, table_name), [])
                            
                            # Get existing columns for this table
                            stmt_cols = select(metadata.ExternalColumn).where(
                                metadata.ExternalColumn.table_id == table_obj.id
                            )
                            existing_cols = (await db.execute(stmt_cols)).scalars().all()
                            existing_col_map = {c.column_name: c for c in existing_cols}
                            
                            for col_info in columns:
                                col_name = col_info["column_name"]
                                
                                if col_name in existing_col_map:
                                    # Update existing column
                                    col_obj = existing_col_map[col_name]
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
                                    # Create new column
                                    try:
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
                                    except IntegrityError:
                                        # Column already exists (race condition), skip
                                        await db.rollback()
                                        logger.debug(f"Column {col_name} already exists, skipping")
                            
                            # Progress: 70-99% for columns storage
                            col_progress = 70 + int(tables_processed / max(total_tables, 1) * 29)
                            col_progress = min(col_progress, 99)
                            await _update_progress(
                                db, connection, col_progress, "storing",
                                f"Stored columns for: {schema_name}.{table_name}",
                                current_item=f"{schema_name}.{table_name}",
                                items_done=tables_processed,
                                total_items=total_tables
                            )
                            
                    # Complete!
                    connection.sync_status = "success"
                    connection.sync_progress = 100
                    connection.sync_progress_details = {
                        "phase": "complete",
                        "message": "Metadata extraction completed successfully",
                        "schemas_processed": total_schemas,
                        "tables_processed": tables_processed,
                        "completed_at": datetime.now().isoformat()
                    }
                    connection.last_sync_time = datetime.now()
                    connection.status = "active"
                    await db.commit()
                    logger.info(f"Metadata extraction completed successfully for {connection.name}")
                    return True
                    
                except Exception as ext_error:
                    logger.error(f"Error during extraction logic: {str(ext_error)}")
                    try:
                        await db.rollback()
                    except:
                        pass
                    # Try to update connection status in a fresh transaction
                    try:
                        connection.sync_status = "failed"
                        connection.status = "error"
                        connection.sync_progress_details = {
                            "phase": "error",
                            "message": f"Extraction failed: {str(ext_error)}",
                            "failed_at": datetime.now().isoformat()
                        }
                        await db.commit()
                    except:
                        pass
                    return False
                finally:
                    if 'extractor' in locals():
                        await extractor.close()

            except Exception as e:
                logger.error(f"Database error during metadata extraction: {str(e)}")
                try:
                    await db.rollback()
                except:
                    pass
                return False
                
    except Exception as e:
        logger.error(f"Fatal error in extract_metadata: {str(e)}")
        return False
