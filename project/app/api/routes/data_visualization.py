from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from typing import List, Optional, Dict, Any, Union
import json
from datetime import datetime, date

from ...database.session import get_db
from ...database.models import core
from ...database.models import metadata
from ...core.auth import get_current_user
from ..schemas.data_visualization_schemas import (
    DataVisualizationResponse,
    DataVisualizationRequest,
    QueryExecutionResponse
)
from ...services.visualization.data_visualization import get_visualized_data, execute_custom_query

router = APIRouter()

@router.post("/tables/{table_id}/data", response_model=DataVisualizationResponse)
async def visualize_table_data(
    table_id: int,
    request: DataVisualizationRequest,
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Visualize data from a table with advanced filtering, sorting and pagination.
    
    This endpoint provides virtualized access to the data, allowing users to browse
    large tables efficiently.
    """
    try:
        # Get table
        table_result = await db.execute(
            select(metadata.ExternalTables, metadata.ExternalSchema)
            .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
            .where(metadata.ExternalTables.id == table_id)
        )
        result = table_result.first()
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
            
        table = result[0]  # ExternalTables
        schema = result[1]  # ExternalSchema
        
        # Get connection
        conn_result = await db.execute(
            select(core.DataConnection, core.ConnectionType)
            .join(core.ConnectionType, core.DataConnection.connection_type_id == core.ConnectionType.id)
            .where(core.DataConnection.id == table.connection_id)
        )
        conn_result = conn_result.first()
        
        if not conn_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection for table with ID {table_id} not found"
            )
            
        connection = conn_result[0]  # DataConnection
        connection_type = conn_result[1]  # ConnectionType

        print("DEBUG - AASDASDSAd", connection.name, connection_type.name)
        
        # Get columns for the table (needed for filtering and sorting validation)
        columns_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.table_id == table_id)
            .order_by(metadata.ExternalColumn.column_position)
        )
        columns = columns_result.scalars().all()
        column_info = {col.column_name: col for col in columns}
        
        # Log access to the data for audit purposes
        # You might want to implement a more robust logging/audit system
        # print(f"User {current_user.email} accessed data from table {schema.schema_name}.{table.table_name}")
        
        # Get visualized data with pagination, filtering and sorting
        total_count, rows, column_names, execution_time = await get_visualized_data(
            connection_type=connection_type.name,
            connection_params=connection.connection_params,
            schema_name=schema.schema_name,
            table_name=table.table_name,
            page=request.page,
            page_size=request.page_size,
            filters=request.filters,
            sort_by=request.sort_by,
            selected_columns=request.columns,
            column_info=column_info
        )
        
        # Calculate total pages
        total_pages = (total_count + request.page_size - 1) // request.page_size if total_count else 0
        
        # Prepare response
        return DataVisualizationResponse(
            table_id=table_id,
            table_name=table.table_name,
            schema_name=schema.schema_name,
            column_names=column_names,
            rows=rows,
            page=request.page,
            page_size=request.page_size,
            total_rows=total_count,
            total_pages=total_pages,
            execution_time_ms=execution_time,
            applied_filters=request.filters,
            applied_sorts=request.sort_by
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to visualize table data: {str(e)}"
        )

@router.post("/execute-query", response_model=QueryExecutionResponse)
async def execute_sql_query(
    request: Dict[str, Any],
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Execute a custom SQL query on a data source.
    
    Only available for users with the appropriate permissions.
    The query will be executed against the specified connection.
    """
    # Check if user has permission to execute custom queries (e.g., admin or data analyst role)
    # This is a placeholder - implement your own permission logic
    # if current_user.role not in ["ADMIN", "DATA_ANALYST"]:
    #     raise HTTPException(
    #         status_code=status.HTTP_403_FORBIDDEN,
    #         detail="You don't have permission to execute custom queries"
    #     )
    
    try:
        connection_id = request.get("connection_id")
        if not connection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="connection_id is required"
            )
            
        # Get the query
        query = request.get("query")
        if not query or not isinstance(query, str) or not query.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A valid SQL query is required"
            )
            
        # Basic security check - block potentially destructive queries
        # This is very basic and should be enhanced with proper SQL parsing
        query_lower = query.lower()
        if any(word in query_lower for word in ["drop", "delete", "truncate", "alter", "update", "insert", "create"]):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Data modification queries are not allowed"
            )
        
        # Get connection - CORRIGIDO
        conn_result = await db.execute(
            select(core.DataConnection, core.ConnectionType)
            .join(core.ConnectionType, core.DataConnection.connection_type_id == core.ConnectionType.id)
            .where(core.DataConnection.id == connection_id)
        )
        conn_result = conn_result.first()
        
        if not conn_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection with ID {connection_id} not found"
            )
            
        connection = conn_result[0]  # DataConnection
        connection_type = conn_result[1]  # ConnectionType
        
        # Log the query for audit purposes
        # print(f"User {current_user.email} executed custom query on connection {connection.name}: {query}")
        
        # Execute the query
        rows, column_names, execution_time = await execute_custom_query(
            connection_type=connection_type.name,
            connection_params=connection.connection_params,
            query=query,
            max_rows=request.get("max_rows", 1000)  # Limit results to prevent huge data transfers
        )
        
        # Prepare response
        return QueryExecutionResponse(
            connection_id=connection_id,
            column_names=column_names,
            rows=rows,
            row_count=len(rows),
            execution_time_ms=execution_time,
            query=query,
            truncated=len(rows) >= request.get("max_rows", 1000)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to execute query: {str(e)}"
        )

@router.get("/tables/{table_id}/sample", response_model=Dict[str, Any])
async def get_table_sample_stats(
    table_id: int,
    columns: Optional[str] = None,  # Comma-separated list of columns to analyze
    sample_size: int = Query(default=1000, le=10000),
    db: AsyncSession = Depends(get_db),
    # current_user: core.User = Depends(get_current_user),
):
    """
    Get statistical insights from a sample of the table data.
    
    This endpoint provides useful statistics about the data without loading the entire table:
    - For numeric columns: min, max, avg, median, std deviation
    - For text columns: most frequent values, unique count
    - For date columns: earliest, latest, distribution by year/month
    """
    try:
        # Get table - CORRIGIDO
        table_result = await db.execute(
            select(metadata.ExternalTables, metadata.ExternalSchema)
            .join(metadata.ExternalSchema, metadata.ExternalTables.schema_id == metadata.ExternalSchema.id)
            .where(metadata.ExternalTables.id == table_id)
        )
        result = table_result.first()
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table with ID {table_id} not found"
            )
            
        table = result[0]
        schema = result[1]
        
        # Get connection - CORRIGIDO
        conn_result = await db.execute(
            select(core.DataConnection, core.ConnectionType)
            .join(core.ConnectionType, core.DataConnection.connection_type_id == core.ConnectionType.id)
            .where(core.DataConnection.id == table.connection_id)
        )
        conn_result = conn_result.first()
        
        if not conn_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Connection for table with ID {table_id} not found"
            )
            
        connection = conn_result[0]
        connection_type = conn_result[1]
        
        # Parse column list if provided
        column_list = None
        if columns:
            column_list = [col.strip() for col in columns.split(",")]
        
        # Get columns for the table - CORRIGIDO (mudando de core.ExternalColumn para metadata.ExternalColumn)
        columns_result = await db.execute(
            select(metadata.ExternalColumn)
            .where(metadata.ExternalColumn.table_id == table_id)
            .order_by(metadata.ExternalColumn.column_position)
        )
        table_columns = columns_result.scalars().all()
        
        # Filter columns if a specific list was provided
        if column_list:
            table_columns = [col for col in table_columns if col.column_name in column_list]
            if not table_columns:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="None of the specified columns were found in the table"
                )
        
        # Process statistical analysis logic here based on connection_type
        # This would involve querying the database with statistical functions
        # For now, returning a placeholder response with sample stats
        
        # In a real implementation, you'd call a service function like:
        # stats = await analyze_table_sample(
        #     connection_type=connection_type.name,
        #     connection_params=connection.connection_params,
        #     schema_name=schema.schema_name,
        #     table_name=table.table_name,
        #     columns=table_columns,
        #     sample_size=sample_size
        # )
        
        # Placeholder response
        stats = {
            "table_id": table_id,
            "table_name": table.table_name,
            "schema_name": schema.schema_name, 
            "sample_size": sample_size,
            "total_rows": table.estimated_row_count,
            "column_stats": {
                col.column_name: {
                    "data_type": col.data_type,
                    "sample_values": col.sample_values[:5] if col.sample_values else [],
                    # These would be populated by the actual analysis
                    "stats": {}
                } for col in table_columns
            }
        }
        
        return stats
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get table sample stats: {str(e)}"
        )