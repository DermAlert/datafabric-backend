"""
Constraint Extraction Service
=============================

Extracts Primary Key and Foreign Key metadata from databases via Trino query passthrough.
This allows automatic detection of 1:N relationships for JOIN aggregation in Silver layer.

Supported databases:
- MySQL / MariaDB
- PostgreSQL
- SQL Server (future)
- Oracle (future)

Usage:
    service = ConstraintExtractionService(trino_client, db_session)
    await service.extract_constraints_for_connection(connection_id)
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func

from ...database.models.metadata import ExternalColumn, ExternalTables, ExternalSchema
from ...database.models.core import DataConnection, ConnectionType
from ..infrastructure.credential_service import get_credential_service

logger = logging.getLogger(__name__)


# ============================================================================
# SQL QUERIES FOR EACH DATABASE TYPE
# ============================================================================

# MySQL / MariaDB - Uses information_schema.KEY_COLUMN_USAGE
MYSQL_PK_FK_QUERY = """
SELECT 
    TABLE_NAME as table_name,
    COLUMN_NAME as column_name,
    CONSTRAINT_NAME as constraint_name,
    CASE 
        WHEN CONSTRAINT_NAME = 'PRIMARY' THEN 'PRIMARY KEY'
        WHEN REFERENCED_TABLE_NAME IS NOT NULL THEN 'FOREIGN KEY'
        ELSE 'UNIQUE'
    END as constraint_type,
    REFERENCED_TABLE_NAME as referenced_table,
    REFERENCED_COLUMN_NAME as referenced_column
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = '{schema}'
"""

# PostgreSQL - Uses information_schema with joins
POSTGRESQL_PK_FK_QUERY = """
SELECT 
    tc.table_name,
    kcu.column_name,
    tc.constraint_name,
    tc.constraint_type,
    ccu.table_name AS referenced_table,
    ccu.column_name AS referenced_column
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name 
    AND tc.table_schema = kcu.table_schema
LEFT JOIN information_schema.constraint_column_usage ccu 
    ON tc.constraint_name = ccu.constraint_name 
    AND tc.table_schema = ccu.table_schema
WHERE tc.table_schema = '{schema}' 
    AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE')
"""

# SQL Server - Uses sys tables (future implementation)
SQLSERVER_PK_FK_QUERY = """
SELECT 
    t.name AS table_name,
    c.name AS column_name,
    kc.name AS constraint_name,
    CASE 
        WHEN kc.type = 'PK' THEN 'PRIMARY KEY'
        WHEN kc.type = 'FK' THEN 'FOREIGN KEY'
        WHEN kc.type = 'UQ' THEN 'UNIQUE'
    END AS constraint_type,
    rt.name AS referenced_table,
    rc.name AS referenced_column
FROM sys.key_constraints kc
JOIN sys.index_columns ic ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
JOIN sys.tables t ON kc.parent_object_id = t.object_id
LEFT JOIN sys.foreign_key_columns fkc ON kc.object_id = fkc.constraint_object_id
LEFT JOIN sys.tables rt ON fkc.referenced_object_id = rt.object_id
LEFT JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
WHERE SCHEMA_NAME(t.schema_id) = '{schema}'
"""

# Database type to query mapping
DB_CONSTRAINT_QUERIES = {
    'mysql': MYSQL_PK_FK_QUERY,
    'mariadb': MYSQL_PK_FK_QUERY,
    'postgresql': POSTGRESQL_PK_FK_QUERY,
    'postgres': POSTGRESQL_PK_FK_QUERY,
    'sqlserver': SQLSERVER_PK_FK_QUERY,
    'mssql': SQLSERVER_PK_FK_QUERY,
}


class ConstraintExtractionService:
    """
    Service to extract PK/FK constraints from databases via Trino.
    
    Uses Trino's query passthrough feature (TABLE(catalog.system.query(...)))
    to execute native SQL on each database and extract constraint metadata.
    """
    
    def __init__(self, trino_client, db: AsyncSession):
        """
        Initialize the service.
        
        Args:
            trino_client: Trino client for executing queries
            db: SQLAlchemy async session for metadata storage
        """
        self.trino = trino_client
        self.db = db
    
    async def extract_constraints_for_connection(
        self, 
        connection_id: int,
        schemas: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Extract PK/FK constraints for all tables in a connection.
        
        Args:
            connection_id: ID of the data connection
            schemas: Optional list of schemas to extract (default: all)
            
        Returns:
            Dict with extraction results and statistics
        """
        # Get connection info with connection type
        conn_result = await self.db.execute(
            select(DataConnection, ConnectionType)
            .join(ConnectionType, DataConnection.connection_type_id == ConnectionType.id)
            .where(DataConnection.id == connection_id)
        )
        result = conn_result.first()
        
        if not result:
            raise ValueError(f"Connection {connection_id} not found")
        
        connection, conn_type = result
        
        # Determine database type from connection type name
        db_type = self._normalize_db_type(conn_type.name)
        if db_type not in DB_CONSTRAINT_QUERIES:
            logger.warning(f"Unsupported database type for constraint extraction: {db_type}")
            return {"status": "unsupported", "db_type": db_type}
        
        # Get catalog name (Trino catalog) using same format as TrinoManager
        catalog_name = self.trino.generate_catalog_name(connection.name, connection.id)
        
        # IMPORTANTE: Garantir que o catálogo existe antes de executar queries
        # Descriptografar credenciais antes de usar
        credential_service = get_credential_service()
        decrypted_params = credential_service.decrypt_for_use(
            connection.connection_params,
            connection_id=connection.id,
            purpose="constraint_extraction"
        )
        
        # Garantir que o catálogo existe no Trino
        catalog_created = await self.trino.ensure_catalog_exists_async(
            connection_name=connection.name,
            connection_type=conn_type.name,
            params=decrypted_params,
            connection_id=connection.id
        )
        
        if not catalog_created:
            raise ValueError(f"Failed to create/verify Trino catalog for connection {connection_id}")
        
        # Get schemas to process
        if not schemas:
            schema_result = await self.db.execute(
                select(ExternalSchema).where(
                    ExternalSchema.connection_id == connection_id
                )
            )
            schemas_to_process = [s.schema_name for s in schema_result.scalars()]
        else:
            schemas_to_process = schemas
        
        results = {
            "connection_id": connection_id,
            "catalog": catalog_name,
            "db_type": db_type,
            "schemas_processed": [],
            "primary_keys_found": 0,
            "foreign_keys_found": 0,
            "errors": []
        }
        
        for schema_name in schemas_to_process:
            try:
                schema_results = await self._extract_schema_constraints(
                    catalog_name, schema_name, db_type, connection_id
                )
                results["schemas_processed"].append(schema_name)
                results["primary_keys_found"] += schema_results["pk_count"]
                results["foreign_keys_found"] += schema_results["fk_count"]
            except Exception as e:
                logger.error(f"Error extracting constraints for {catalog_name}.{schema_name}: {e}")
                results["errors"].append({
                    "schema": schema_name,
                    "error": str(e)
                })
        
        await self.db.commit()
        
        logger.info(
            f"Constraint extraction complete for connection {connection_id}: "
            f"{results['primary_keys_found']} PKs, {results['foreign_keys_found']} FKs"
        )
        
        return results
    
    async def _extract_schema_constraints(
        self,
        catalog: str,
        schema: str,
        db_type: str,
        connection_id: int
    ) -> Dict[str, int]:
        """Extract constraints for a specific schema."""
        
        # Build the query
        native_query = DB_CONSTRAINT_QUERIES[db_type].format(schema=schema)
        trino_query = f"""
            SELECT * FROM TABLE({catalog}.system.query(
                query => '{native_query.replace("'", "''")}'
            ))
        """
        
        logger.info(f"Extracting constraints from {catalog}.{schema}")
        
        # Execute via Trino
        try:
            conn = await self.trino.get_connection()
            cursor = await conn.cursor()
            await cursor.execute(trino_query)
            rows = await cursor.fetchall()
            await conn.close()
        except Exception as e:
            logger.error(f"Trino query failed: {e}")
            raise
        
        pk_count = 0
        fk_count = 0
        
        # Process each constraint
        for row in rows:
            table_name, column_name, constraint_name, constraint_type, ref_table, ref_column = row[:6]
            
            # Find the column in our metadata
            column = await self._find_column(connection_id, schema, table_name, column_name)
            if not column:
                continue
            
            if constraint_type == 'PRIMARY KEY':
                await self._update_column_pk(column.id)
                pk_count += 1
                
            elif constraint_type == 'FOREIGN KEY' and ref_table and ref_column:
                # Find referenced column
                ref_col = await self._find_column(connection_id, schema, ref_table, ref_column)
                ref_table_obj = await self._find_table(connection_id, schema, ref_table)
                
                await self._update_column_fk(
                    column.id,
                    constraint_name,
                    ref_table_obj.id if ref_table_obj else None,
                    ref_col.id if ref_col else None
                )
                fk_count += 1
        
        return {"pk_count": pk_count, "fk_count": fk_count}
    
    async def _find_column(
        self, 
        connection_id: int, 
        schema: str, 
        table: str, 
        column: str
    ) -> Optional[ExternalColumn]:
        """Find a column in our metadata (case-insensitive)."""
        result = await self.db.execute(
            select(ExternalColumn)
            .join(ExternalTables, ExternalColumn.table_id == ExternalTables.id)
            .join(ExternalSchema, ExternalTables.schema_id == ExternalSchema.id)
            .where(
                ExternalSchema.connection_id == connection_id,
                func.lower(ExternalSchema.schema_name) == func.lower(schema),
                func.lower(ExternalTables.table_name) == func.lower(table),
                func.lower(ExternalColumn.column_name) == func.lower(column)
            )
        )
        return result.scalar_one_or_none()
    
    async def _find_table(
        self,
        connection_id: int,
        schema: str,
        table: str
    ) -> Optional[ExternalTables]:
        """Find a table in our metadata (case-insensitive)."""
        result = await self.db.execute(
            select(ExternalTables)
            .join(ExternalSchema, ExternalTables.schema_id == ExternalSchema.id)
            .where(
                ExternalSchema.connection_id == connection_id,
                func.lower(ExternalSchema.schema_name) == func.lower(schema),
                func.lower(ExternalTables.table_name) == func.lower(table)
            )
        )
        return result.scalar_one_or_none()
    
    async def _update_column_pk(self, column_id: int):
        """Mark column as primary key."""
        await self.db.execute(
            update(ExternalColumn)
            .where(ExternalColumn.id == column_id)
            .values(is_primary_key=True)
        )
    
    async def _update_column_fk(
        self,
        column_id: int,
        constraint_name: str,
        ref_table_id: Optional[int],
        ref_column_id: Optional[int]
    ):
        """Mark column as foreign key with references."""
        await self.db.execute(
            update(ExternalColumn)
            .where(ExternalColumn.id == column_id)
            .values(
                is_foreign_key=True,
                fk_constraint_name=constraint_name,
                fk_referenced_table_id=ref_table_id,
                fk_referenced_column_id=ref_column_id
            )
        )
    
    def _normalize_db_type(self, conn_type_name: str) -> str:
        """
        Normalize connection type name to a standard database type.
        
        Args:
            conn_type_name: Name of the connection type (e.g., 'postgresql', 'MySQL')
            
        Returns:
            Normalized database type string
        """
        conn_type = conn_type_name.lower()
        
        if 'mysql' in conn_type or 'mariadb' in conn_type:
            return 'mysql'
        elif 'postgres' in conn_type or 'postgresql' in conn_type:
            return 'postgresql'
        elif 'sqlserver' in conn_type or 'mssql' in conn_type:
            return 'sqlserver'
        elif 'oracle' in conn_type:
            return 'oracle'
        elif 'delta' in conn_type:
            return 'delta'
        
        return conn_type or 'unknown'
    
    async def _get_db_type(self, connection: DataConnection) -> str:
        """
        Determine database type from connection.
        
        DEPRECATED: Use _normalize_db_type with conn_type.name instead.
        Kept for backwards compatibility.
        """
        # Get connection type name from ConnectionType table
        conn_type_result = await self.db.execute(
            select(ConnectionType).where(ConnectionType.id == connection.connection_type_id)
        )
        conn_type_obj = conn_type_result.scalar_one_or_none()
        conn_type_name = conn_type_obj.name if conn_type_obj else ''
        
        return self._normalize_db_type(conn_type_name)
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    async def get_table_cardinality_info(
        self,
        table_id: int
    ) -> Dict[str, Any]:
        """
        Get cardinality information for a table based on PK/FK analysis.
        
        Returns info about which columns are PKs and which are FKs,
        useful for determining JOIN behavior.
        """
        result = await self.db.execute(
            select(ExternalColumn).where(ExternalColumn.table_id == table_id)
        )
        columns = result.scalars().all()
        
        pk_columns = [c for c in columns if c.is_primary_key]
        fk_columns = [c for c in columns if c.is_foreign_key]
        
        return {
            "table_id": table_id,
            "primary_keys": [
                {"column_id": c.id, "column_name": c.column_name}
                for c in pk_columns
            ],
            "foreign_keys": [
                {
                    "column_id": c.id,
                    "column_name": c.column_name,
                    "references_table_id": c.fk_referenced_table_id,
                    "references_column_id": c.fk_referenced_column_id,
                    "constraint_name": c.fk_constraint_name
                }
                for c in fk_columns
            ],
            "has_composite_pk": len(pk_columns) > 1,
            "fk_count": len(fk_columns)
        }
    
    async def infer_join_cardinality(
        self,
        left_table_id: int,
        right_table_id: int,
        join_column_name: str
    ) -> str:
        """
        Infer the cardinality of a JOIN between two tables.
        
        Args:
            left_table_id: ID of the left table
            right_table_id: ID of the right table
            join_column_name: Name of the column used for joining
            
        Returns:
            Cardinality string: 'ONE_TO_ONE', 'ONE_TO_MANY', 'MANY_TO_ONE', 'MANY_TO_MANY'
        """
        # Check if join column is PK in left table (case-insensitive)
        left_col = await self.db.execute(
            select(ExternalColumn).where(
                ExternalColumn.table_id == left_table_id,
                func.lower(ExternalColumn.column_name) == func.lower(join_column_name)
            )
        )
        left_column = left_col.scalar_one_or_none()
        
        # Check if join column is PK in right table (case-insensitive)
        right_col = await self.db.execute(
            select(ExternalColumn).where(
                ExternalColumn.table_id == right_table_id,
                func.lower(ExternalColumn.column_name) == func.lower(join_column_name)
            )
        )
        right_column = right_col.scalar_one_or_none()
        
        left_is_pk = left_column.is_primary_key if left_column else False
        right_is_pk = right_column.is_primary_key if right_column else False
        
        if left_is_pk and right_is_pk:
            return 'ONE_TO_ONE'
        elif left_is_pk and not right_is_pk:
            return 'ONE_TO_MANY'  # Left has 1, Right has many
        elif not left_is_pk and right_is_pk:
            return 'MANY_TO_ONE'  # Left has many, Right has 1
        else:
            return 'MANY_TO_MANY'  # Neither is PK

