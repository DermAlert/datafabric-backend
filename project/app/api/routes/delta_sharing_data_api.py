"""
Delta Sharing Data API Routes

REST-based access to shared data (both materialized and virtualized tables)
using the same authentication as Delta Sharing protocol (bearer tokens + recipients).

This extends Delta Sharing with on-demand query support for virtualized datasets,
enabling external applications to consume data that isn't materialized in Delta Lake.

Endpoints:
- POST /shares/{share}/schemas/{schema}/tables/{table}/data
    Query table data (JSON response) - works for virtualized tables
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query, Request, Header, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
import time
import csv
import io
import json

from ...database.session import get_db
from ...api.schemas.delta_sharing_schemas import (
    DataAPIRequest, DataAPIResponse, DataAPIFormat, DataAPIErrorResponse
)
from ...services.delta_sharing.data_api_service import DataAPIService

router = APIRouter()


# ==================== Data API Routes ====================

@router.post(
    "/shares/{share}/schemas/{schema}/tables/{table}/data",
    response_model=DataAPIResponse,
    responses={
        401: {"model": DataAPIErrorResponse, "description": "Unauthorized"},
        404: {"model": DataAPIErrorResponse, "description": "Not found"},
        429: {"model": DataAPIErrorResponse, "description": "Rate limited"},
    },
    summary="Query shared table data",
    description="""
Query data from a shared table via REST API. Supports both materialized (Delta Lake)
and virtualized (on-demand via Trino) tables.

**Authentication:** Bearer token (same as Delta Sharing protocol).

**Virtualized tables** execute the query in real-time via Trino and return fresh data.
**Materialized tables** should use the standard Delta Sharing protocol instead.

**Response formats:** JSON (default), CSV, NDJSON via the `format` field in the request body.

**Pagination:** Use `limit` and `offset` in the request body. The response includes
`has_more` to indicate if more data is available.

**Rate limiting:** Respects the recipient's `max_requests_per_hour` and
`max_downloads_per_day` limits. Returns 429 when exceeded.
"""
)
async def query_table_data(
    share: str,
    schema: str,
    table: str,
    query_request: DataAPIRequest,
    request: Request,
    authorization: str = Header(None, description="Bearer token for authentication"),
    db: AsyncSession = Depends(get_db),
):
    """Query table data through the Data API."""
    service = DataAPIService(db)
    
    # Authenticate
    recipient = await service.authenticate_recipient(authorization)
    
    # Execute query
    result = await service.query_table_data(
        recipient=recipient,
        share_name=share,
        schema_name=schema,
        table_name=table,
        query_request=query_request,
        request=request
    )
    
    # Return based on requested format
    if query_request.format == DataAPIFormat.CSV:
        return _build_csv_response(result)
    elif query_request.format == DataAPIFormat.NDJSON:
        return _build_ndjson_response(result)
    else:
        return result


@router.get(
    "/shares/{share}/schemas/{schema}/tables/{table}/data",
    response_model=DataAPIResponse,
    responses={
        401: {"model": DataAPIErrorResponse, "description": "Unauthorized"},
        404: {"model": DataAPIErrorResponse, "description": "Not found"},
        429: {"model": DataAPIErrorResponse, "description": "Rate limited"},
    },
    summary="Query shared table data (GET)",
    description="""
GET variant of the Data API for simple queries.
Supports limit and offset as query parameters.
Always returns JSON format.
"""
)
async def query_table_data_get(
    share: str,
    schema: str,
    table: str,
    request: Request,
    limit: int = Query(default=1000, ge=1, le=50000, description="Maximum rows to return"),
    offset: int = Query(default=0, ge=0, description="Rows to skip"),
    authorization: str = Header(None, description="Bearer token for authentication"),
    db: AsyncSession = Depends(get_db),
):
    """Query table data through the Data API (GET variant)."""
    service = DataAPIService(db)
    
    # Authenticate
    recipient = await service.authenticate_recipient(authorization)
    
    # Build request from query params
    query_request = DataAPIRequest(
        format=DataAPIFormat.JSON,
        limit=limit,
        offset=offset
    )
    
    # Execute query
    return await service.query_table_data(
        recipient=recipient,
        share_name=share,
        schema_name=schema,
        table_name=table,
        query_request=query_request,
        request=request
    )


# ==================== Response Formatters ====================

def _build_csv_response(result: DataAPIResponse) -> StreamingResponse:
    """Build a CSV streaming response from query results."""
    def generate_csv():
        output = io.StringIO()
        if result.columns:
            writer = csv.DictWriter(output, fieldnames=[c.name for c in result.columns])
            writer.writeheader()
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
            
            for row in result.data:
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
    
    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={result.table}.csv",
            "X-Row-Count": str(result.row_count),
            "X-Has-More": str(result.has_more).lower()
        }
    )


def _build_ndjson_response(result: DataAPIResponse) -> StreamingResponse:
    """Build an NDJSON (newline-delimited JSON) streaming response."""
    def generate_ndjson():
        for row in result.data:
            yield json.dumps(row, default=str) + "\n"
    
    return StreamingResponse(
        generate_ndjson(),
        media_type="application/x-ndjson",
        headers={
            "X-Row-Count": str(result.row_count),
            "X-Has-More": str(result.has_more).lower()
        }
    )
