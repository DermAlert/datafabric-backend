from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update
from typing import List, Optional, Dict, Any
from datetime import datetime

from ...database.database import get_db
from ...database.core import core
from ...database.metadata import metadata
from ...crud.token import get_current_user
from ..schemas.metadata_schemas import (
    CatalogResponse,
    SchemaResponse,
    TableResponse,
    ColumnResponse,
    TableDetailsResponse,
    DataPreviewResponse,
    UpdateFlAtivoRequest,
    BulkUpdateFlAtivoRequest,
    UpdateFlAtivoResponse,
    BulkUpdateFlAtivoResponse,
    ConstraintExtractionRequest,
    ConstraintExtractionResponse,
    TableCardinalityInfo,
    JoinCardinalityResponse
)
# from ...services.data_preview import get_data_preview
from ...services.distinct_values_service import DistinctValuesService
from ...services.constraint_extraction_service import ConstraintExtractionService
from ...services.trino_client import get_trino_client
from ...services.trino_manager import TrinoManager

router = APIRouter()

@router.get("/connections/{connection_id}/catalogs", response_model=List[CatalogResponse])
async def list_catalogs(
    connection_id: int,
    fl_ativo: Optional[bool] = Query(None, description="Filter by fl_ativo status. If not provided, returns all catalogs."),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all catalogs for a specific data connection.
    """
    try:
        # Verify connection exists and user has access
        conn_result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = conn_result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query with fl_ativo filter
        query = select(metadata.ExternalCatalogs).where(metadata.ExternalCatalogs.connection_id == connection_id)
        
        if fl_ativo is not None:
            query = query.where(metadata.ExternalCatalogs.fl_ativo == fl_ativo)
            
        query = query.order_by(metadata.ExternalCatalogs.catalog_name)
        
        # Get catalogs
        result = await db.execute(query)
        catalogs = result.scalars().all()
        
        return catalogs
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve catalogs: {str(e)}"
        )

@router.get("/connections/{connection_id}/schemas", response_model=List[SchemaResponse])
async def list_schemas(
    connection_id: int,
    catalog_id: Optional[int] = None,
    fl_ativo: Optional[bool] = Query(None, description="Filter by fl_ativo status. If not provided, returns all schemas."),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all schemas for a specific data connection.
    """
    try:
        # Verify connection exists and user has access
        conn_result = await db.execute(
            select(core.DataConnection).where(core.DataConnection.id == connection_id)
        )
        connection = conn_result.scalars().first()
        
        if not connection:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Data connection with ID {connection_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query
        query = select(metadata.ExternalSchema).where(metadata.ExternalSchema.connection_id == connection_id)
        
        if catalog_id:
            query = query.where(metadata.ExternalSchema.catalog_id == catalog_id)
            
        if fl_ativo is not None:
            query = query.where(metadata.ExternalSchema.fl_ativo == fl_ativo)
            
        query = query.order_by(metadata.ExternalSchema.schema_name)
        
        # Get schemas
        result = await db.execute(query)
        schemas = result.scalars().all()
        
        return schemas
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve schemas: {str(e)}"
        )

@router.get("/schemas/{schema_id}/tables", response_model=List[TableResponse])
async def list_tables(
    schema_id: int,
    table_type: Optional[str] = None,
    search: Optional[str] = None,
    fl_ativo: Optional[bool] = Query(None, description="Filter by fl_ativo status. If not provided, returns all tables."),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all tables for a specific schema.
    """
    try:
        # Verify schema exists
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id == schema_id)
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with ID {schema_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query
        query = select(metadata.ExternalTables).where(metadata.ExternalTables.schema_id == schema_id)
        
        if table_type:
            query = query.where(metadata.ExternalTables.table_type == table_type)
            
        if search:
            query = query.where(metadata.ExternalTables.table_name.ilike(f"%{search}%"))
            
        if fl_ativo is not None:
            query = query.where(metadata.ExternalTables.fl_ativo == fl_ativo)
            
        query = query.order_by(metadata.ExternalTables.table_name)
        
        # Get tables
        result = await db.execute(query)
        tables = result.scalars().all()
        
        return tables
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tables: {str(e)}"
        )

@router.get("/tables/{table_id}/columns", response_model=List[ColumnResponse])
async def list_columns(
    table_id: int,
    fl_ativo: Optional[bool] = Query(None, description="Filter by fl_ativo status. If not provided, returns all columns."),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    List all columns for a specific table.
    """
    try:
        # Verify table exists
        table_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Build query
        query = select(metadata.ExternalColumn).where(metadata.ExternalColumn.table_id == table_id)
        
        if fl_ativo is not None:
            query = query.where(metadata.ExternalColumn.fl_ativo == fl_ativo)
            
        query = query.order_by(metadata.ExternalColumn.column_position)
        
        # Get columns
        result = await db.execute(query)
        columns_db = result.scalars().all()
        
        # Convert ORM objects to Pydantic model instances
        columns = [
            ColumnResponse(
                id=col.id,
                table_id=col.table_id,
                column_name=col.column_name,
                column_position=col.column_position,
                data_type=col.data_type,
                description=col.description,
                is_nullable=col.is_nullable,
                is_primary_key=col.is_primary_key,
                is_unique=col.is_unique,
                is_foreign_key=getattr(col, 'is_foreign_key', False),
                fk_referenced_table_id=getattr(col, 'fk_referenced_table_id', None),
                fk_referenced_column_id=getattr(col, 'fk_referenced_column_id', None),
                fk_constraint_name=getattr(col, 'fk_constraint_name', None),
                properties=col.properties,
                is_indexed=getattr(col, 'is_indexed', False),
                statistics=getattr(col, 'statistics', {}),
                sample_values=getattr(col, 'sample_values', []),
                fl_ativo=col.fl_ativo
            ) for col in columns_db
        ]
        
        return columns
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve columns: {str(e)}"
        )
    
@router.get("/tables/{table_id}", response_model=TableDetailsResponse)
async def get_table_details(
    table_id: int,
    fl_ativo_columns: Optional[bool] = Query(None, description="Filter columns by fl_ativo status. If not provided, returns all columns."),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get detailed information about a specific table including its columns.
    """
    try:
        # Get table
        table_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
        
        # TODO: Check if user has access to this connection's organization
        
        # Get schema
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id == table.schema_id)
        )
        schema = schema_result.scalars().first()
        
        # Build columns query with optional fl_ativo filter
        columns_query = select(metadata.ExternalColumn).where(metadata.ExternalColumn.table_id == table_id)
        
        if fl_ativo_columns is not None:
            columns_query = columns_query.where(metadata.ExternalColumn.fl_ativo == fl_ativo_columns)
            
        columns_query = columns_query.order_by(metadata.ExternalColumn.column_position)
        
        # Get columns
        columns_result = await db.execute(columns_query)
        columns_db = columns_result.scalars().all()
        
        # Convert ORM objects to Pydantic model instances with all required fields
        columns = [
            ColumnResponse(
                id=col.id,
                table_id=col.table_id,
                column_name=col.column_name,
                column_position=col.column_position,
                data_type=col.data_type,
                description=col.description,
                is_nullable=col.is_nullable,
                is_primary_key=col.is_primary_key,
                is_unique=col.is_unique,
                is_foreign_key=getattr(col, 'is_foreign_key', False),
                fk_referenced_table_id=getattr(col, 'fk_referenced_table_id', None),
                fk_referenced_column_id=getattr(col, 'fk_referenced_column_id', None),
                fk_constraint_name=getattr(col, 'fk_constraint_name', None),
                properties=col.properties,
                # Adding the missing required fields
                is_indexed=getattr(col, 'is_indexed', False),  # Default to False if not available
                statistics=getattr(col, 'statistics', {}),     # Default to empty dict if not available
                sample_values=getattr(col, 'sample_values', []),  # Default to empty list if not available
                fl_ativo=col.fl_ativo
            ) for col in columns_db
        ]
        
        # Count primary keys
        primary_key_count = sum(1 for col in columns_db if col.is_primary_key)
        
        return TableDetailsResponse(
            id=table.id,
            schema_id=table.schema_id,
            connection_id=table.connection_id,
            table_name=table.table_name,
            external_reference=table.external_reference,
            table_type=table.table_type,
            estimated_row_count=table.estimated_row_count,
            total_size_bytes=table.total_size_bytes,
            last_analyzed=table.last_analyzed,
            properties=table.properties,
            description=table.description,
            fl_ativo=table.fl_ativo,
            schema_name=schema.schema_name if schema else None,
            columns=columns,
            primary_key_count=primary_key_count,
            column_count=len(columns)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve table details: {str(e)}"
        )


@router.get("/tables/{table_id}/sample")
async def get_table_sample(
    table_id: int,
    max_samples: int = Query(5, ge=1, le=100, description="Maximum number of sample rows to return"),
    db: AsyncSession = Depends(get_db),
):
    """
    Returns sample rows from a table using Trino.
    Works with any connection type that has a Trino catalog configured (MySQL, PostgreSQL, Delta Lake, etc.).
    
    Args:
        table_id: ID of the table to sample
        max_samples: Maximum number of sample rows to return (default: 5, max: 100)
    
    Returns:
        Sample rows with all columns from the table
    """
    try:
        # Buscar a tabela
        table_result = await db.execute(
            select(metadata.ExternalTables)
            .where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tabela com ID {table_id} não encontrada"
            )
        
        # Buscar o schema
        schema_result = await db.execute(
            select(metadata.ExternalSchema)
            .where(metadata.ExternalSchema.id == table.schema_id)
        )
        schema = schema_result.scalars().first()
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema com ID {table.schema_id} não encontrado"
            )
        
        # Buscar o catalog (para Delta Lake)
        catalog_db = None
        if schema.catalog_id:
            catalog_result = await db.execute(
                select(metadata.ExternalCatalogs)
                .where(metadata.ExternalCatalogs.id == schema.catalog_id)
            )
            catalog_db = catalog_result.scalars().first()
        
        # Buscar a conexão
        conn_result = await db.execute(
            select(core.DataConnection)
            .where(core.DataConnection.id == table.connection_id)
        )
        conn = conn_result.scalars().first()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection com ID {table.connection_id} não encontrada"
            )
        
        # Buscar o tipo da conexão
        conn_type = None
        if hasattr(conn, 'connection_type_id'):
            type_result = await db.execute(
                select(core.ConnectionType)
                .where(core.ConnectionType.id == conn.connection_type_id)
            )
            conn_type_obj = type_result.scalars().first()
            if conn_type_obj:
                conn_type = conn_type_obj.name.lower()
        
        if not conn_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tipo de conexão não encontrado"
            )
        
        # Usar TrinoManager para gerar o nome do catalog
        trino_manager = TrinoManager()
        catalog_name = trino_manager.generate_catalog_name(conn.name, conn.id)
        
        # Garantir que o catálogo existe no Trino
        await trino_manager.ensure_catalog_exists_async(
            connection_name=conn.name,
            connection_type=conn_type,
            params=conn.connection_params,
            connection_id=conn.id
        )
        
        schema_name = schema.schema_name
        table_name = table.table_name
        
        # Buscar as colunas da tabela para informação adicional
        columns_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.table_id == table_id)
            .order_by(metadata.ExternalColumn.column_position)
        )
        columns_db = columns_result.scalars().all()
        column_info = [
            {
                "column_name": col.column_name,
                "data_type": col.data_type,
                "is_nullable": col.is_nullable,
                "is_primary_key": col.is_primary_key,
            }
            for col in columns_db
        ]
        
        # Executar query via Trino
        import aiotrino
        
        trino_conn = aiotrino.dbapi.connect(
            host=trino_manager.host,
            port=trino_manager.port,
            user=trino_manager.user,
            catalog=catalog_name,
            schema=schema_name,
        )
        
        try:
            cur = await trino_conn.cursor()
            
            # Query para buscar samples - usar quoted identifiers para preservar case
            query = f'SELECT * FROM "{catalog_name}"."{schema_name}"."{table_name}" LIMIT {max_samples}'
            
            await cur.execute(query)
            rows = await cur.fetchall()
            
            # Obter nomes das colunas do cursor
            description = await cur.get_description() if hasattr(cur, 'get_description') else None
            column_names = []
            if description:
                column_names = [
                    col.name if hasattr(col, 'name') else (col.get('name') if isinstance(col, dict) else col[0])
                    for col in description
                ]
            
            # Converter rows para dicts
            sample_data = []
            for row in rows:
                if column_names:
                    row_dict = {}
                    for i, val in enumerate(row):
                        col_name = column_names[i] if i < len(column_names) else f"col_{i}"
                        # Converter valores para tipos serializáveis
                        if val is None:
                            row_dict[col_name] = None
                        elif isinstance(val, (int, float, str, bool)):
                            row_dict[col_name] = val
                        else:
                            row_dict[col_name] = str(val)
                    sample_data.append(row_dict)
                else:
                    sample_data.append(list(row))
            
        finally:
            await trino_conn.close()
        
        return {
            "table_id": table_id,
            "table_name": table_name,
            "schema_name": schema_name,
            "catalog_name": catalog_name,
            "connection_type": conn_type,
            "columns": column_info,
            "sample_data": sample_data,
            "total_returned": len(sample_data),
            "max_samples": max_samples
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar sample da tabela: {str(e)}"
        )

    
@router.get("/columns/{column_id}/distinct-values")
async def get_distinct_values_for_column(
    column_id: int,
    limit: int = 100,
    search: Optional[str] = Query(None, description="Search/filter distinct values by text. Case insensitive partial match."),
    db: AsyncSession = Depends(get_db),
):
    """
    Retorna os valores distintos encontrados para uma coluna de metadados,
    consultando via Trino (suporta MySQL, PostgreSQL, Delta Lake, etc.).
    """
    try:
        # Buscar a coluna
        column_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id == column_id)
        )
        column = column_result.scalars().first()
        if not column:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Coluna com ID {column_id} não encontrada"
            )
        
        # Buscar a tabela
        table_result = await db.execute(
            select(metadata.ExternalTables)
            .where(metadata.ExternalTables.id == column.table_id)
        )
        table = table_result.scalars().first()
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tabela com ID {column.table_id} não encontrada"
            )
        
        # Buscar o schema
        schema_result = await db.execute(
            select(metadata.ExternalSchema)
            .where(metadata.ExternalSchema.id == table.schema_id)
        )
        schema = schema_result.scalars().first()
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema com ID {table.schema_id} não encontrado"
            )
        
        # Buscar a conexão
        conn_result = await db.execute(
            select(core.DataConnection)
            .where(core.DataConnection.id == table.connection_id)
        )
        conn = conn_result.scalars().first()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection com ID {table.connection_id} não encontrada"
            )
        
        # Buscar o tipo da conexão
        conn_type = None
        if hasattr(conn, 'connection_type_id'):
            type_result = await db.execute(
                select(core.ConnectionType)
                .where(core.ConnectionType.id == conn.connection_type_id)
            )
            conn_type_obj = type_result.scalars().first()
            if conn_type_obj:
                conn_type = conn_type_obj.name.lower()
        
        if not conn_type:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tipo de conexão não encontrado"
            )
        
        # Usar TrinoManager para gerar o nome do catalog e executar query
        trino_manager = TrinoManager()
        catalog_name = trino_manager.generate_catalog_name(conn.name, conn.id)
        
        # Garantir que o catálogo existe no Trino
        await trino_manager.ensure_catalog_exists_async(
            connection_name=conn.name,
            connection_type=conn_type,
            params=conn.connection_params,
            connection_id=conn.id
        )
        
        schema_name = schema.schema_name
        table_name = table.table_name
        column_name = column.column_name
        
        # Executar query via Trino
        import aiotrino
        
        trino_conn = aiotrino.dbapi.connect(
            host=trino_manager.host,
            port=trino_manager.port,
            user=trino_manager.user,
            catalog=catalog_name,
            schema=schema_name,
        )
        
        try:
            cur = await trino_conn.cursor()
            
            # First, get approximate count of distinct values (using HyperLogLog - very fast)
            if search:
                safe_search = search.replace("'", "''")
                count_query = f'''
                    SELECT approx_distinct("{column_name}") 
                    FROM "{catalog_name}"."{schema_name}"."{table_name}" 
                    WHERE lower(cast("{column_name}" as varchar)) LIKE '%{safe_search.lower()}%'
                '''
            else:
                count_query = f'''
                    SELECT approx_distinct("{column_name}") 
                    FROM "{catalog_name}"."{schema_name}"."{table_name}"
                '''
            
            await cur.execute(count_query)
            count_result = await cur.fetchone()
            total_distinct = count_result[0] if count_result else 0
            
            # Build query with optional search filter
            if search:
                # Case-insensitive search using lower()
                query = f'''
                    SELECT DISTINCT "{column_name}" 
                    FROM "{catalog_name}"."{schema_name}"."{table_name}" 
                    WHERE lower(cast("{column_name}" as varchar)) LIKE '%{safe_search.lower()}%'
                    ORDER BY "{column_name}" 
                    LIMIT {limit}
                '''
            else:
                query = f'''
                    SELECT DISTINCT "{column_name}" 
                    FROM "{catalog_name}"."{schema_name}"."{table_name}" 
                    ORDER BY "{column_name}" 
                    LIMIT {limit}
                '''
            
            await cur.execute(query)
            rows = await cur.fetchall()
            
            # Extract values
            values = []
            for row in rows:
                val = row[0]
                if val is None:
                    values.append(None)
                elif isinstance(val, (int, float, str, bool)):
                    values.append(val)
                else:
                    values.append(str(val))
            
        finally:
            await trino_conn.close()
        
        return {
            "column_id": column_id,
            "column_name": column_name,
            "connection_type": conn_type,
            "distinct_values": values,
            "total_distinct": total_distinct,
            "total_returned": len(values),
            "search_filter": search,
            "limit": limit
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar valores distintos: {str(e)}"
        )

# APIs para editar fl_ativo

# APIs para atualização em lote (devem vir ANTES das rotas com parâmetros dinâmicos)

@router.patch("/catalogs/bulk/fl_ativo", response_model=BulkUpdateFlAtivoResponse)
async def bulk_update_catalogs_fl_ativo(
    request: BulkUpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Bulk update the fl_ativo status of multiple catalogs.
    When deactivating (fl_ativo=False), also deactivates all schemas, tables and columns in cascade.
    """
    try:
        # Verify catalogs exist
        catalogs_result = await db.execute(
            select(metadata.ExternalCatalogs).where(metadata.ExternalCatalogs.id.in_(request.ids))
        )
        catalogs = catalogs_result.scalars().all()
        
        existing_ids = [catalog.id for catalog in catalogs]
        missing_ids = set(request.ids) - set(existing_ids)
        
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalogs with IDs {list(missing_ids)} not found"
            )
        
        # Update catalogs fl_ativo
        result = await db.execute(
            update(metadata.ExternalCatalogs)
            .where(metadata.ExternalCatalogs.id.in_(request.ids))
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating catalogs, cascade to schemas, tables and columns
        if not request.fl_ativo:
            # Get all schemas in these catalogs
            schemas_result = await db.execute(
                select(metadata.ExternalSchema.id)
                .where(metadata.ExternalSchema.catalog_id.in_(request.ids))
            )
            schema_ids = [row.id for row in schemas_result.fetchall()]
            
            if schema_ids:
                # Deactivate all schemas in these catalogs
                await db.execute(
                    update(metadata.ExternalSchema)
                    .where(metadata.ExternalSchema.catalog_id.in_(request.ids))
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
                
                # Get all tables in these schemas
                tables_result = await db.execute(
                    select(metadata.ExternalTables.id)
                    .where(metadata.ExternalTables.schema_id.in_(schema_ids))
                )
                table_ids = [row.id for row in tables_result.fetchall()]
                
                if table_ids:
                    # Deactivate all tables in these schemas
                    await db.execute(
                        update(metadata.ExternalTables)
                        .where(metadata.ExternalTables.schema_id.in_(schema_ids))
                        .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                    )
                    
                    # Deactivate all columns in these tables
                    await db.execute(
                        update(metadata.ExternalColumn)
                        .where(metadata.ExternalColumn.table_id.in_(table_ids))
                        .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                    )
        
        await db.commit()
        
        return BulkUpdateFlAtivoResponse(
            updated_count=result.rowcount,
            updated_ids=request.ids,
            fl_ativo=request.fl_ativo
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk update catalogs fl_ativo: {str(e)}"
        )

@router.patch("/schemas/bulk/fl_ativo", response_model=BulkUpdateFlAtivoResponse)
async def bulk_update_schemas_fl_ativo(
    request: BulkUpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Bulk update the fl_ativo status of multiple schemas.
    When deactivating (fl_ativo=False), also deactivates all tables and columns in cascade.
    """
    try:
        # Verify schemas exist
        schemas_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id.in_(request.ids))
        )
        schemas = schemas_result.scalars().all()
        
        existing_ids = [schema.id for schema in schemas]
        missing_ids = set(request.ids) - set(existing_ids)
        
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schemas with IDs {list(missing_ids)} not found"
            )
        
        # Update schemas fl_ativo
        result = await db.execute(
            update(metadata.ExternalSchema)
            .where(metadata.ExternalSchema.id.in_(request.ids))
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating schemas, cascade to tables and columns
        if not request.fl_ativo:
            # Get all tables in these schemas
            tables_result = await db.execute(
                select(metadata.ExternalTables.id)
                .where(metadata.ExternalTables.schema_id.in_(request.ids))
            )
            table_ids = [row.id for row in tables_result.fetchall()]
            
            if table_ids:
                # Deactivate all tables in these schemas
                await db.execute(
                    update(metadata.ExternalTables)
                    .where(metadata.ExternalTables.schema_id.in_(request.ids))
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
                
                # Deactivate all columns in these tables
                await db.execute(
                    update(metadata.ExternalColumn)
                    .where(metadata.ExternalColumn.table_id.in_(table_ids))
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
        
        await db.commit()
        
        return BulkUpdateFlAtivoResponse(
            updated_count=result.rowcount,
            updated_ids=request.ids,
            fl_ativo=request.fl_ativo
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk update schemas fl_ativo: {str(e)}"
        )

@router.patch("/tables/bulk/fl_ativo", response_model=BulkUpdateFlAtivoResponse)
async def bulk_update_tables_fl_ativo(
    request: BulkUpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Bulk update the fl_ativo status of multiple tables.
    When deactivating (fl_ativo=False), also deactivates all columns in cascade.
    """
    try:
        # Verify tables exist
        tables_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id.in_(request.ids))
        )
        tables = tables_result.scalars().all()
        
        existing_ids = [table.id for table in tables]
        missing_ids = set(request.ids) - set(existing_ids)
        
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tables with IDs {list(missing_ids)} not found"
            )
        
        # Update tables fl_ativo
        result = await db.execute(
            update(metadata.ExternalTables)
            .where(metadata.ExternalTables.id.in_(request.ids))
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating tables, cascade to columns
        if not request.fl_ativo:
            # Deactivate all columns in these tables
            await db.execute(
                update(metadata.ExternalColumn)
                .where(metadata.ExternalColumn.table_id.in_(request.ids))
                .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
            )
        
        await db.commit()
        
        return BulkUpdateFlAtivoResponse(
            updated_count=result.rowcount,
            updated_ids=request.ids,
            fl_ativo=request.fl_ativo
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk update tables fl_ativo: {str(e)}"
        )

@router.patch("/columns/bulk/fl_ativo", response_model=BulkUpdateFlAtivoResponse)
async def bulk_update_columns_fl_ativo(
    request: BulkUpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Bulk update the fl_ativo status of multiple columns.
    """
    try:
        # Verify columns exist
        columns_result = await db.execute(
            select(metadata.ExternalColumn).where(metadata.ExternalColumn.id.in_(request.ids))
        )
        columns = columns_result.scalars().all()
        
        existing_ids = [column.id for column in columns]
        missing_ids = set(request.ids) - set(existing_ids)
        
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Columns with IDs {list(missing_ids)} not found"
            )
        
        # Update fl_ativo
        result = await db.execute(
            update(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id.in_(request.ids))
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        await db.commit()
        
        return BulkUpdateFlAtivoResponse(
            updated_count=result.rowcount,
            updated_ids=request.ids,
            fl_ativo=request.fl_ativo
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to bulk update columns fl_ativo: {str(e)}"
        )

# APIs para atualização individual

@router.patch("/catalogs/{catalog_id}/fl_ativo", response_model=UpdateFlAtivoResponse)
async def update_catalog_fl_ativo(
    catalog_id: int,
    request: UpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update the fl_ativo status of a catalog.
    When deactivating (fl_ativo=False), also deactivates all schemas, tables and columns in cascade.
    """
    try:
        # Verify catalog exists
        catalog_result = await db.execute(
            select(metadata.ExternalCatalogs).where(metadata.ExternalCatalogs.id == catalog_id)
        )
        catalog = catalog_result.scalars().first()
        
        if not catalog:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Catalog with ID {catalog_id} not found"
            )
        
        # Update catalog fl_ativo
        await db.execute(
            update(metadata.ExternalCatalogs)
            .where(metadata.ExternalCatalogs.id == catalog_id)
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating catalog, cascade to schemas, tables and columns
        if not request.fl_ativo:
            # Get all schemas in this catalog
            schemas_result = await db.execute(
                select(metadata.ExternalSchema.id)
                .where(metadata.ExternalSchema.catalog_id == catalog_id)
            )
            schema_ids = [row.id for row in schemas_result.fetchall()]
            
            if schema_ids:
                # Deactivate all schemas in this catalog
                await db.execute(
                    update(metadata.ExternalSchema)
                    .where(metadata.ExternalSchema.catalog_id == catalog_id)
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
                
                # Get all tables in these schemas
                tables_result = await db.execute(
                    select(metadata.ExternalTables.id)
                    .where(metadata.ExternalTables.schema_id.in_(schema_ids))
                )
                table_ids = [row.id for row in tables_result.fetchall()]
                
                if table_ids:
                    # Deactivate all tables in these schemas
                    await db.execute(
                        update(metadata.ExternalTables)
                        .where(metadata.ExternalTables.schema_id.in_(schema_ids))
                        .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                    )
                    
                    # Deactivate all columns in these tables
                    await db.execute(
                        update(metadata.ExternalColumn)
                        .where(metadata.ExternalColumn.table_id.in_(table_ids))
                        .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                    )
        
        await db.commit()
        
        return UpdateFlAtivoResponse(
            id=catalog_id,
            fl_ativo=request.fl_ativo,
            data_atualizacao=datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update catalog fl_ativo: {str(e)}"
        )

@router.patch("/schemas/{schema_id}/fl_ativo", response_model=UpdateFlAtivoResponse)
async def update_schema_fl_ativo(
    schema_id: int,
    request: UpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update the fl_ativo status of a schema.
    When deactivating (fl_ativo=False), also deactivates all tables and columns in cascade.
    """
    try:
        # Verify schema exists
        schema_result = await db.execute(
            select(metadata.ExternalSchema).where(metadata.ExternalSchema.id == schema_id)
        )
        schema = schema_result.scalars().first()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with ID {schema_id} not found"
            )
        
        # Update schema fl_ativo
        await db.execute(
            update(metadata.ExternalSchema)
            .where(metadata.ExternalSchema.id == schema_id)
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating schema, cascade to tables and columns
        if not request.fl_ativo:
            # Get all tables in this schema
            tables_result = await db.execute(
                select(metadata.ExternalTables.id)
                .where(metadata.ExternalTables.schema_id == schema_id)
            )
            table_ids = [row.id for row in tables_result.fetchall()]
            
            if table_ids:
                # Deactivate all tables in this schema
                await db.execute(
                    update(metadata.ExternalTables)
                    .where(metadata.ExternalTables.schema_id == schema_id)
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
                
                # Deactivate all columns in these tables
                await db.execute(
                    update(metadata.ExternalColumn)
                    .where(metadata.ExternalColumn.table_id.in_(table_ids))
                    .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
                )
        
        await db.commit()
        
        return UpdateFlAtivoResponse(
            id=schema_id,
            fl_ativo=request.fl_ativo,
            data_atualizacao=datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update schema fl_ativo: {str(e)}"
        )

@router.patch("/tables/{table_id}/fl_ativo", response_model=UpdateFlAtivoResponse)
async def update_table_fl_ativo(
    table_id: int,
    request: UpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update the fl_ativo status of a table.
    When deactivating (fl_ativo=False), also deactivates all columns in cascade.
    """
    try:
        # Verify table exists
        table_result = await db.execute(
            select(metadata.ExternalTables).where(metadata.ExternalTables.id == table_id)
        )
        table = table_result.scalars().first()
        
        if not table:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
        
        # Update table fl_ativo
        await db.execute(
            update(metadata.ExternalTables)
            .where(metadata.ExternalTables.id == table_id)
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        # If deactivating table, cascade to columns
        if not request.fl_ativo:
            # Deactivate all columns in this table
            await db.execute(
                update(metadata.ExternalColumn)
                .where(metadata.ExternalColumn.table_id == table_id)
                .values(fl_ativo=False, data_atualizacao=datetime.utcnow())
            )
        
        await db.commit()
        
        return UpdateFlAtivoResponse(
            id=table_id,
            fl_ativo=request.fl_ativo,
            data_atualizacao=datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update table fl_ativo: {str(e)}"
        )

@router.patch("/columns/{column_id}/fl_ativo", response_model=UpdateFlAtivoResponse)
async def update_column_fl_ativo(
    column_id: int,
    request: UpdateFlAtivoRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Update the fl_ativo status of a column.
    """
    try:
        # Verify column exists
        column_result = await db.execute(
            select(metadata.ExternalColumn).where(metadata.ExternalColumn.id == column_id)
        )
        column = column_result.scalars().first()
        
        if not column:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Column with ID {column_id} not found"
            )
        
        # Update fl_ativo
        await db.execute(
            update(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.id == column_id)
            .values(fl_ativo=request.fl_ativo, data_atualizacao=datetime.utcnow())
        )
        
        await db.commit()
        
        return UpdateFlAtivoResponse(
            id=column_id,
            fl_ativo=request.fl_ativo,
            data_atualizacao=datetime.utcnow()
        )
    
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update column fl_ativo: {str(e)}"
        )


# ============================================================================
# CONSTRAINT EXTRACTION ENDPOINTS (PK/FK)
# ============================================================================

@router.post(
    "/connections/{connection_id}/extract-constraints",
    response_model=ConstraintExtractionResponse,
    summary="Extract PK/FK Constraints",
    description="""
    Extract Primary Key and Foreign Key metadata from a database via Trino query passthrough.
    
    **Supported databases:**
    - MySQL / MariaDB
    - PostgreSQL
    - SQL Server (experimental)
    
    **What it does:**
    1. Queries the source database's information_schema
    2. Identifies PRIMARY KEY and FOREIGN KEY constraints
    3. Updates metadata.external_columns with:
       - `is_primary_key`: True if column is a PK
       - `is_foreign_key`: True if column is a FK
       - `fk_referenced_table_id`: ID of the referenced table
       - `fk_referenced_column_id`: ID of the referenced column
       - `fk_constraint_name`: Name of the FK constraint
    
    **Use case:**
    This information enables automatic detection of 1:N relationships for JOIN aggregation in the Silver layer.
    """
)
async def extract_constraints(
    connection_id: int,
    request: ConstraintExtractionRequest = ConstraintExtractionRequest(),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Extract PK/FK constraints from a database connection.
    """
    try:
        trino_client = await get_trino_client()
        service = ConstraintExtractionService(trino_client, db)
        
        result = await service.extract_constraints_for_connection(
            connection_id=connection_id,
            schemas=request.schemas
        )
        
        if result.get("status") == "unsupported":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Database type '{result.get('db_type')}' is not supported for constraint extraction"
            )
        
        return ConstraintExtractionResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to extract constraints: {str(e)}"
        )


@router.get(
    "/tables/{table_id}/cardinality-info",
    response_model=TableCardinalityInfo,
    summary="Get Table Cardinality Info",
    description="""
    Get PK/FK information for a table to help determine JOIN cardinality.
    
    Returns:
    - List of primary key columns
    - List of foreign key columns with their references
    - Whether the table has a composite primary key
    """
)
async def get_table_cardinality_info(
    table_id: int,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get cardinality information for a specific table.
    """
    try:
        trino_client = await get_trino_client()
        service = ConstraintExtractionService(trino_client, db)
        
        result = await service.get_table_cardinality_info(table_id)
        return TableCardinalityInfo(**result)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get cardinality info: {str(e)}"
        )


@router.get(
    "/tables/{left_table_id}/join-cardinality/{right_table_id}",
    response_model=JoinCardinalityResponse,
    summary="Infer JOIN Cardinality",
    description="""
    Infer the cardinality of a JOIN between two tables based on PK/FK analysis.
    
    **Cardinality types:**
    - `ONE_TO_ONE`: Both columns are PKs
    - `ONE_TO_MANY`: Left is PK, Right is not
    - `MANY_TO_ONE`: Left is not PK, Right is PK
    - `MANY_TO_MANY`: Neither is PK
    
    **Note:** Requires that `extract-constraints` has been run first.
    """
)
async def infer_join_cardinality(
    left_table_id: int,
    right_table_id: int,
    join_column: str = Query(..., description="Name of the column used for joining"),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Infer the cardinality of a JOIN between two tables.
    """
    try:
        trino_client = await get_trino_client()
        service = ConstraintExtractionService(trino_client, db)
        
        cardinality = await service.infer_join_cardinality(
            left_table_id=left_table_id,
            right_table_id=right_table_id,
            join_column_name=join_column
        )
        
        return JoinCardinalityResponse(
            left_table_id=left_table_id,
            right_table_id=right_table_id,
            join_column_name=join_column,
            cardinality=cardinality
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to infer cardinality: {str(e)}"
        )