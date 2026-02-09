from fastapi import APIRouter, Depends, HTTPException, status, Query, Request, Header, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Optional
import time
from fastapi.responses import JSONResponse

from ...database.session import get_db
from ...api.schemas.delta_sharing_schemas import (
    ListSharesResponse, ShareResponse, ListSchemasResponse, ListTablesResponse,
    TableVersionResponse, QueryTableRequest, DeltaSharingError
)
from ...services.delta_sharing.protocol_service import DeltaSharingProtocolService

router = APIRouter()

# ==================== DELTA SHARING PROTOCOL ROUTES ====================

async def get_recipient_from_auth(
    authorization: str = Header(None),
    db: AsyncSession = Depends(get_db)
):
    """Extract and authenticate recipient from authorization header"""
    service = DeltaSharingProtocolService(db)
    return await service.authenticate_recipient(authorization)

@router.get("/shares", response_model=ListSharesResponse)
async def list_shares(
    request: Request,
    maxResults: Optional[int] = Query(None, description="Maximum number of results to return"),
    pageToken: Optional[str] = Query(None, description="Token for pagination"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """
    List the shares accessible to the recipient
    
    Without any query parameters the request will return the first page
    of "shares" available to the authenticated recipient
    """
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.list_shares(recipient, maxResults, pageToken)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_shares", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_shares", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}", response_model=ShareResponse)
async def get_share(
    share: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """Get the metadata of a share"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.get_share(recipient, share)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_share", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_share", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/schemas", response_model=ListSchemasResponse)
async def list_schemas(
    share: str,
    request: Request,
    maxResults: Optional[int] = Query(None, description="Maximum number of results to return"),
    pageToken: Optional[str] = Query(None, description="Token for pagination"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """List the schemas in a share"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.list_schemas(recipient, share, maxResults, pageToken)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_schemas", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_schemas", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/schemas/{schema}/tables", response_model=ListTablesResponse)
async def list_tables(
    share: str,
    schema: str,
    request: Request,
    maxResults: Optional[int] = Query(None, description="Maximum number of results to return"),
    pageToken: Optional[str] = Query(None, description="Token for pagination"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """List the tables in a given share's schema"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.list_tables(recipient, share, schema, maxResults, pageToken)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_tables", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_tables", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/all-tables", response_model=ListTablesResponse)
async def list_all_tables(
    share: str,
    request: Request,
    maxResults: Optional[int] = Query(None, description="Maximum number of results to return"),
    pageToken: Optional[str] = Query(None, description="Token for pagination"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """List the tables under all schemas in a share"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.list_all_tables(recipient, share, maxResults, pageToken)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_all_tables", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "list_all_tables", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/schemas/{schema}/tables/{table}/version", response_model=TableVersionResponse)
async def get_table_version(
    share: str,
    schema: str,
    table: str,
    request: Request,
    response: Response,
    startingTimestamp: Optional[str] = Query(None, description="Starting timestamp for time travel"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """
    Return the table version
    
    This is the API for clients to get a table version without any other
    extra information. The server usually can implement this API
    effectively. If a client caches information about a shared table
    locally, it can store the table version and use this cheap API to
    quickly check whether their cache is stale and they should re-fetch the
    data.
    """
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        result = await service.get_table_version(recipient, share, schema, table, startingTimestamp)
        
        # Add Delta-Table-Version header
        response.headers["Delta-Table-Version"] = str(result.version)
        
        # Log access
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_version", 
            response_status=200, processing_time=processing_time
        )
        
        return result
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_version", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/schemas/{schema}/tables/{table}/metadata")
async def get_table_metadata(
    share: str,
    schema: str,
    table: str,
    request: Request,
    response: Response,
    startingTimestamp: Optional[str] = Query(None, description="Starting timestamp for time travel"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """Query the metadata and schema of the given table"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        # First get table version for header
        version_info = await service.get_table_version(recipient, share, schema, table, startingTimestamp)
        
        async def generate_metadata():
            async for line in service.get_table_metadata(recipient, share, schema, table, startingTimestamp):
                yield line
        
        # Log access (will be done in streaming)
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_metadata", 
            response_status=200, processing_time=processing_time
        )
        
        # Include Delta-Table-Version header in StreamingResponse headers
        headers = {
            "Content-Type": "application/x-ndjson; charset=utf-8",
            "Delta-Table-Version": str(version_info.version)
        }
        
        return StreamingResponse(
            generate_metadata(),
            media_type="application/x-ndjson",
            headers=headers
        )
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_metadata", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.post("/shares/{share}/schemas/{schema}/tables/{table}/query")
async def query_table(
    share: str,
    schema: str,
    table: str,
    query_request: QueryTableRequest,
    request: Request,
    response: Response,
    startingTimestamp: Optional[str] = Query(None, description="Starting timestamp for time travel"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """Query the table"""
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        # First get table version for header
        version_info = await service.get_table_version(recipient, share, schema, table, startingTimestamp)
        
        async def generate_query_response():
            async for line in service.query_table(recipient, share, schema, table, query_request):
                yield line
        
        # Log access (will be done in streaming)
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "query_table", 
            response_status=200, processing_time=processing_time
        )
        
        # Include Delta-Table-Version header in StreamingResponse headers
        headers = {
            "Content-Type": "application/x-ndjson; charset=utf-8",
            "Delta-Table-Version": str(version_info.version)
        }
        
        return StreamingResponse(
            generate_query_response(),
            media_type="application/x-ndjson",
            headers=headers
        )
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "query_table", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise

@router.get("/shares/{share}/schemas/{schema}/tables/{table}/changes")
async def get_table_changes(
    share: str,
    schema: str,
    table: str,
    request: Request,
    startingTimestamp: Optional[str] = Query(None, description="Starting timestamp"),
    startingVersion: Optional[int] = Query(None, description="Starting version"),
    endingVersion: Optional[int] = Query(None, description="Ending version"),
    endingTimestamp: Optional[str] = Query(None, description="Ending timestamp"),
    includeHistoricalMetadata: Optional[bool] = Query(None, description="Include historical metadata"),
    db: AsyncSession = Depends(get_db),
    recipient = Depends(get_recipient_from_auth)
):
    """
    Read Change Data Feed from a Table
    
    This is the API for clients to read change data feed from a table.
    The API supports a start parameter and and an end parameter. The
    start/end parameter can either be a version or a timestamp. The start
    parameter must be provided. If the end parameter is not provided, the
    API will use the latest table version for it. The parameter range is
    inclusive in the query. You can specify a version as a Long or a
    timestamp as a string in the Timestamp Format.
    The change data feed represents row-level changes between versions of
    a Delta table. It records change data for UPDATE, DELETE, and MERGE operations.
    If you leverage the connectors provided by this library to read change data feed,
    it results in three metadata columns that identify the type of change event, in
    addition to the data columns:
    _change_type (type: String): There are four values: insert, update_preimage, update_postimage, delete. preimage is the value before the udpate, postimage is the value after the update.
    _commit_version (type: Long): The table version containing the change.
    _commit_timestamp (type: Long): The unix timestamp associated when the commit of the change was created, in milliseconds.
    """
    start_time = time.time()
    service = DeltaSharingProtocolService(db)
    
    try:
        async def generate_changes():
            async for line in service.get_table_changes(
                recipient, share, schema, table, 
                startingVersion, startingTimestamp, endingVersion, endingTimestamp, 
                includeHistoricalMetadata
            ):
                yield line
        
        # Log access (will be done in streaming)
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_changes", 
            response_status=200, processing_time=processing_time
        )
        
        return StreamingResponse(
            generate_changes(),
            media_type="application/x-ndjson",
            headers={"Content-Type": "application/x-ndjson; charset=utf-8"}
        )
    except HTTPException as e:
        processing_time = int((time.time() - start_time) * 1000)
        await service.log_access(
            recipient, request, "get_table_changes", 
            response_status=e.status_code, processing_time=processing_time,
            error_message=e.detail
        )
        raise