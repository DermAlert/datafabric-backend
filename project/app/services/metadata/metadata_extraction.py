from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from typing import Dict, Any, List, Optional
from datetime import datetime

from ...database.session import db_session
from ...database.models import core
from ...database.models import metadata
from ...utils.logger import logger
from ..infrastructure.credential_service import get_credential_service

# Import generic Trino extractor
from ..infrastructure.trino_extractor import TrinoExtractor


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
    
    is_system = schema_info.get("is_system_schema", False)
    
    if existing:
        existing.last_scanned = datetime.now()
        existing.is_system_schema = is_system
        return existing
    
    # Try to create
    try:
        schema_obj = metadata.ExternalSchema(
            connection_id=connection_id,
            schema_name=schema_name,
            catalog_id=catalog_id,
            external_reference=schema_info.get("external_reference"),
            is_system_schema=is_system,
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

    Uses the 3-phase connection pattern to avoid holding a DB connection
    during the long Trino extraction (which can take 30–300 s):

      Phase 1 — read config + mark RUNNING  (DB connection held briefly)
      Phase 2 — Trino extraction            (NO DB connection held)
      Phase 3 — batch-store results         (DB connection held briefly)
    """
    # ── Phase 1: read connection config, mark as running ─────────────────────
    connection_name = None
    connection_type_name = None
    connection_params_encrypted = None
    content_type = None

    try:
        async with db_session() as db:
            result = await db.execute(
                select(core.DataConnection, core.ConnectionType).join(
                    core.ConnectionType,
                    core.DataConnection.connection_type_id == core.ConnectionType.id
                ).where(core.DataConnection.id == connection_id)
            )
            record = result.first()

            if not record:
                logger.error(f"No connection found with ID {connection_id}")
                return False

            connection = record[0]
            connection_type = record[1]

            if connection.content_type == 'image':
                logger.error(f"Cannot extract metadata from image connection {connection_id}")
                connection.sync_status = "failed"
                connection.status = "error"
                connection.sync_progress = 0
                connection.sync_progress_details = {
                    "phase": "error",
                    "message": "Cannot sync image connections"
                }
                # db_session commits on exit
                return False

            # Capture everything needed for Phase 2 as plain Python values
            connection_name = connection.name
            connection_type_name = connection_type.name
            connection_params_encrypted = connection.connection_params
            content_type = connection.content_type

            connection.sync_status = "running"
            connection.sync_progress = 5
            connection.sync_progress_details = {
                "phase": "setup",
                "message": "Initializing metadata extraction...",
                "updated_at": datetime.now().isoformat()
            }
            # db_session() commits and releases connection on exit
    except Exception as e:
        logger.error(f"Fatal error in extract_metadata (phase 1): {e}")
        return False

    # ── Phase 2: Trino extraction — NO DB connection held ────────────────────
    extractor = None
    catalogs = []
    full_metadata: dict = {"schemas": [], "tables": {}, "columns": {}}

    try:
        credential_service = get_credential_service()
        decrypted_params = credential_service.decrypt_for_use(
            connection_params_encrypted,
            connection_id=connection_id,
            purpose="metadata_extraction"
        )

        extractor = TrinoExtractor(
            connection_params=decrypted_params,
            connection_type=connection_type_name,
            connection_name=connection_name,
            connection_id=connection_id
        )

        await extractor.ensure_connection()
        catalogs = await extractor.extract_catalogs()
        full_metadata = await extractor.extract_full_metadata()

        logger.info(
            f"Trino extraction done for {connection_name}: "
            f"{len(full_metadata['schemas'])} schemas, "
            f"{sum(len(t) for t in full_metadata['tables'].values())} tables"
        )

    except Exception as ext_error:
        logger.error(f"Error during Trino extraction for {connection_name}: {ext_error}")
        # Phase 3-error: mark failed
        try:
            async with db_session() as db:
                res = await db.execute(
                    select(core.DataConnection).where(core.DataConnection.id == connection_id)
                )
                conn_obj = res.scalar_one_or_none()
                if conn_obj:
                    conn_obj.sync_status = "failed"
                    conn_obj.status = "error"
                    conn_obj.sync_progress_details = {
                        "phase": "error",
                        "message": f"Extraction failed: {ext_error}",
                        "failed_at": datetime.now().isoformat()
                    }
        except Exception:
            pass
        return False
    finally:
        if extractor:
            await extractor.close()

    # ── Phase 3: batch-store results ─────────────────────────────────────────
    schemas = full_metadata["schemas"]
    schema_tables = full_metadata["tables"]
    all_columns = full_metadata["columns"]
    total_schemas = len(schemas)
    total_tables = sum(len(t) for t in schema_tables.values())

    try:
        async with db_session() as db:
            # Re-fetch the connection object for mutation
            res = await db.execute(
                select(core.DataConnection).where(core.DataConnection.id == connection_id)
            )
            connection = res.scalar_one_or_none()
            if not connection:
                return False

            # Store catalogs
            catalog_map: dict[str, int] = {}
            for cat_info in catalogs:
                cat_id = await _get_or_create_catalog(db, connection_id, cat_info)
                catalog_map[cat_info["catalog_name"]] = cat_id
            await db.flush()

            # Store schemas, tables and columns in one transaction
            tables_processed = 0
            PROGRESS_BATCH = 10  # update progress every N tables (reduces commits)

            for schema_idx, schema_info in enumerate(schemas):
                schema_name = schema_info["schema_name"]
                catalog_id = catalog_map.get(schema_info.get("catalog_name"))

                schema_obj = await _get_or_create_schema(
                    db, connection_id, catalog_id, schema_info
                )
                await db.flush()

                for table_info in schema_tables.get(schema_name, []):
                    tables_processed += 1
                    table_name = table_info["table_name"]

                    table_obj = await _get_or_create_table(
                        db, schema_obj.id, connection_id, table_info
                    )
                    await db.flush()

                    columns = all_columns.get((schema_name, table_name), [])
                    stmt_cols = select(metadata.ExternalColumn).where(
                        metadata.ExternalColumn.table_id == table_obj.id
                    )
                    existing_col_map = {
                        c.column_name: c
                        for c in (await db.execute(stmt_cols)).scalars().all()
                    }

                    for col_info in columns:
                        col_name = col_info["column_name"]
                        if col_name in existing_col_map:
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
                            try:
                                db.add(metadata.ExternalColumn(
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
                                ))
                            except IntegrityError:
                                await db.rollback()
                                logger.debug(f"Column {col_name} already exists, skipping")

                    # Batch progress update: commit every PROGRESS_BATCH tables
                    # instead of once per table, reducing DB round-trips by 10×
                    if tables_processed % PROGRESS_BATCH == 0:
                        progress = 50 + int(tables_processed / max(total_tables, 1) * 49)
                        connection.sync_progress = min(progress, 99)
                        connection.sync_progress_details = {
                            "phase": "storing",
                            "message": f"Stored {tables_processed}/{total_tables} tables",
                            "items_done": tables_processed,
                            "total_items": total_tables,
                            "updated_at": datetime.now().isoformat()
                        }
                        await db.commit()

            # Final commit with success status
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
            # db_session() commits on exit

        logger.info(f"Metadata extraction completed for {connection_name}")
        return True

    except Exception as e:
        logger.error(f"Fatal error in extract_metadata (phase 3): {e}")
        try:
            async with db_session() as db:
                res = await db.execute(
                    select(core.DataConnection).where(core.DataConnection.id == connection_id)
                )
                conn_obj = res.scalar_one_or_none()
                if conn_obj:
                    conn_obj.sync_status = "failed"
                    conn_obj.status = "error"
                    conn_obj.sync_progress_details = {
                        "phase": "error",
                        "message": f"Store failed: {e}",
                        "failed_at": datetime.now().isoformat()
                    }
        except Exception:
            pass
        return False
