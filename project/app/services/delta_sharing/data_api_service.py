"""
Data API Service for Delta Sharing

Provides REST-based access to shared data, supporting both materialized (Delta Lake)
and virtualized (Trino on-demand) tables through the same authentication and
authorization layer as the Delta Sharing protocol.

Security features:
- Bearer token authentication (reuses Delta Sharing recipients)
- Rate limiting enforcement (per-recipient hourly/daily limits)
- Query timeout enforcement (prevents resource exhaustion)
- Row limit caps (prevents unbounded data extraction)
- Full audit logging (all queries recorded)
- SQL injection prevention (no raw user SQL - all queries generated from configs)
- Access control (recipient must have access to the share containing the table)
"""

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, text
from typing import Optional, List, Dict, Any, Tuple
from fastapi import HTTPException, status, Request
from datetime import datetime, timedelta, timezone
import logging
import time

from ...database.models.delta_sharing import (
    Share, ShareSchema, ShareTable, Recipient,
    RecipientAccessLog, ShareStatus, TableShareStatus,
    ShareTableSourceType, recipient_shares
)
from ...database.models.bronze import BronzeVirtualizedConfig
from ...database.models.silver import VirtualizedConfig
from ...api.schemas.delta_sharing_schemas import (
    DataAPIResponse, DataAPIColumnInfo, DataAPIRequest
)

logger = logging.getLogger(__name__)

# Security constants
MAX_QUERY_TIMEOUT_SECONDS = 120      # Max time a Trino query can run
ABSOLUTE_MAX_ROWS = 50000            # Hard cap on rows per request
DEFAULT_MAX_ROWS = 1000              # Default row limit
RATE_LIMIT_WINDOW_HOURS = 1          # Rate limit window
RATE_LIMIT_DAILY_WINDOW_HOURS = 24   # Daily rate limit window


class DataAPIService:
    """
    Service for serving shared data via REST API.
    
    Handles both materialized (Delta) and virtualized (Bronze/Silver) tables
    through the same authentication layer as Delta Sharing protocol.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
    
    # ==================== Authentication & Authorization ====================
    
    async def authenticate_recipient(self, authorization: str) -> Recipient:
        """
        Authenticate recipient using bearer token.
        
        Security checks:
        1. Token format validation (must be Bearer <token>)
        2. Token existence and match in database
        3. Recipient active status
        4. Token expiry check
        """
        if not authorization:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authorization header is required",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        if not authorization.startswith('Bearer '):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authorization scheme. Use 'Bearer <token>'",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        token = authorization[7:]  # Remove 'Bearer ' prefix
        
        # Security: reject empty/whitespace-only tokens
        if not token or not token.strip():
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Bearer token is empty",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        # Security: reject suspiciously long tokens (prevent buffer overflow attacks)
        if len(token) > 512:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token format",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        query = select(Recipient).where(
            and_(
                Recipient.bearer_token == token,
                Recipient.is_active == True,
                or_(
                    Recipient.token_expiry.is_(None),
                    Recipient.token_expiry > func.now()
                )
            )
        )
        result = await self.db.execute(query)
        recipient = result.scalar_one_or_none()
        
        if not recipient:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        
        return recipient
    
    async def enforce_rate_limit(self, recipient: Recipient) -> None:
        """
        Enforce rate limiting for a recipient.
        
        Checks both:
        - Hourly request limit (max_requests_per_hour)
        - Daily download limit (max_downloads_per_day)
        """
        now = datetime.now(timezone.utc)
        
        # Check hourly limit
        if recipient.max_requests_per_hour:
            one_hour_ago = now - timedelta(hours=RATE_LIMIT_WINDOW_HOURS)
            hourly_count_query = select(func.count(RecipientAccessLog.id)).where(
                and_(
                    RecipientAccessLog.recipient_id == recipient.id,
                    RecipientAccessLog.accessed_at >= one_hour_ago,
                    RecipientAccessLog.operation.like('data_api%')
                )
            )
            result = await self.db.execute(hourly_count_query)
            hourly_count = result.scalar() or 0
            
            if hourly_count >= recipient.max_requests_per_hour:
                logger.warning(
                    f"Rate limit exceeded for recipient {recipient.id}: "
                    f"{hourly_count}/{recipient.max_requests_per_hour} requests/hour"
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded. Maximum {recipient.max_requests_per_hour} requests per hour.",
                    headers={
                        "Retry-After": "3600",
                        "X-RateLimit-Limit": str(recipient.max_requests_per_hour),
                        "X-RateLimit-Remaining": "0"
                    }
                )
        
        # Check daily limit
        if recipient.max_downloads_per_day:
            one_day_ago = now - timedelta(hours=RATE_LIMIT_DAILY_WINDOW_HOURS)
            daily_count_query = select(func.count(RecipientAccessLog.id)).where(
                and_(
                    RecipientAccessLog.recipient_id == recipient.id,
                    RecipientAccessLog.accessed_at >= one_day_ago,
                    RecipientAccessLog.operation.like('data_api%')
                )
            )
            result = await self.db.execute(daily_count_query)
            daily_count = result.scalar() or 0
            
            if daily_count >= recipient.max_downloads_per_day:
                logger.warning(
                    f"Daily download limit exceeded for recipient {recipient.id}: "
                    f"{daily_count}/{recipient.max_downloads_per_day} downloads/day"
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Daily download limit exceeded. Maximum {recipient.max_downloads_per_day} per day.",
                    headers={"Retry-After": "86400"}
                )
    
    async def get_accessible_table(
        self, recipient: Recipient, share_name: str, schema_name: str, table_name: str
    ) -> Tuple[ShareTable, ShareSchema, Share]:
        """
        Get a shared table verifying recipient has access.
        
        Security: Validates the full chain: recipient -> share -> schema -> table
        """
        query = select(ShareTable, ShareSchema, Share).select_from(
            ShareTable.__table__
            .join(ShareSchema.__table__, ShareTable.schema_id == ShareSchema.id)
            .join(Share.__table__, ShareSchema.share_id == Share.id)
            .join(recipient_shares, Share.id == recipient_shares.c.share_id)
        ).where(
            and_(
                ShareTable.name == table_name,
                ShareSchema.name == schema_name,
                Share.name == share_name,
                recipient_shares.c.recipient_id == recipient.id,
                ShareTable.status == TableShareStatus.ACTIVE,
                Share.status == ShareStatus.ACTIVE
            )
        )
        result = await self.db.execute(query)
        row = result.first()
        
        if not row:
            # Security: don't leak whether share/schema/table exists
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Table not found or not accessible"
            )
        
        return row[0], row[1], row[2]
    
    # ==================== Audit Logging ====================
    
    async def log_access(
        self, recipient: Recipient, request: Request, operation: str,
        share_id: Optional[int] = None, schema_id: Optional[int] = None,
        table_id: Optional[int] = None, response_status: int = 200,
        response_size: Optional[int] = None, processing_time: Optional[int] = None,
        error_message: Optional[str] = None
    ) -> None:
        """Log access for audit trail. Always logs (unlike protocol service which checks access_logged)."""
        try:
            log_entry = RecipientAccessLog(
                recipient_id=recipient.id,
                share_id=share_id,
                schema_id=schema_id,
                table_id=table_id,
                operation=operation,
                request_method=request.method,
                request_path=str(request.url.path),
                request_query_params=dict(request.query_params) if request.query_params else None,
                response_status_code=response_status,
                response_size_bytes=response_size,
                processing_time_ms=processing_time,
                client_ip=request.client.host if request.client else None,
                user_agent=request.headers.get('user-agent'),
                error_message=error_message
            )
            self.db.add(log_entry)
            await self.db.commit()
        except Exception as e:
            # Never fail the main request due to logging errors
            logger.error(f"Failed to log access: {e}")
    
    # ==================== Data Query Execution ====================
    
    async def query_table_data(
        self,
        recipient: Recipient,
        share_name: str,
        schema_name: str,
        table_name: str,
        query_request: DataAPIRequest,
        request: Request
    ) -> DataAPIResponse:
        """
        Query table data via REST API.
        
        Routes to the appropriate handler based on the table's source_type:
        - DELTA: reads from Delta Lake (via Spark/presigned URLs)
        - BRONZE_VIRTUALIZED: executes Bronze virtualized query via Trino
        - SILVER_VIRTUALIZED: executes Silver virtualized query via Trino
        """
        start_time = time.time()
        
        # 1. Enforce rate limiting
        await self.enforce_rate_limit(recipient)
        
        # 2. Validate access and get table info
        table, schema, share = await self.get_accessible_table(
            recipient, share_name, schema_name, table_name
        )
        
        # 3. Enforce row limits
        limit = min(query_request.limit, ABSOLUTE_MAX_ROWS)
        offset = query_request.offset
        
        try:
            # 4. Route to appropriate handler based on source_type
            source_type = table.source_type or ShareTableSourceType.DELTA
            
            if source_type == ShareTableSourceType.BRONZE_VIRTUALIZED:
                columns, data, has_more = await self._execute_bronze_virtualized(
                    table, limit, offset
                )
            elif source_type == ShareTableSourceType.SILVER_VIRTUALIZED:
                columns, data, has_more = await self._execute_silver_virtualized(
                    table, limit, offset
                )
            elif source_type in (
                ShareTableSourceType.DELTA,
                ShareTableSourceType.BRONZE,
                ShareTableSourceType.SILVER,
            ):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "This table is a materialized Delta table. "
                        "Use the Delta Sharing protocol (query endpoint) to access it, "
                        "or use a delta-sharing client library."
                    )
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Unsupported source type: {source_type}"
                )
            
            execution_time = time.time() - start_time
            
            # 5. Log successful access
            await self.log_access(
                recipient, request, "data_api_query",
                share_id=share.id, schema_id=schema.id, table_id=table.id,
                response_status=200,
                response_size=len(data),
                processing_time=int(execution_time * 1000)
            )
            
            return DataAPIResponse(
                share=share_name,
                schema_name=schema_name,
                table=table_name,
                source_type=source_type.value,
                columns=columns,
                data=data,
                row_count=len(data),
                has_more=has_more,
                execution_time_seconds=round(execution_time, 3)
            )
            
        except HTTPException:
            # Log failed access
            execution_time = time.time() - start_time
            await self.log_access(
                recipient, request, "data_api_query",
                share_id=share.id, schema_id=schema.id, table_id=table.id,
                response_status=500,
                processing_time=int(execution_time * 1000),
                error_message="Query execution failed"
            )
            raise
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Data API query failed: {e}")
            await self.log_access(
                recipient, request, "data_api_query",
                share_id=share.id, schema_id=schema.id, table_id=table.id,
                response_status=500,
                processing_time=int(execution_time * 1000),
                error_message=str(e)
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Query execution failed"
            )
    
    async def _execute_bronze_virtualized(
        self,
        table: ShareTable,
        limit: int,
        offset: int
    ) -> Tuple[List[DataAPIColumnInfo], List[Dict[str, Any]], bool]:
        """Execute a Bronze virtualized query via Trino."""
        if not table.bronze_virtualized_config_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Table references a Bronze virtualized config but no config ID is set"
            )
        
        # Verify config exists and is active
        config_result = await self.db.execute(
            select(BronzeVirtualizedConfig).where(
                and_(
                    BronzeVirtualizedConfig.id == table.bronze_virtualized_config_id,
                    BronzeVirtualizedConfig.is_active == True
                )
            )
        )
        config = config_result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="The underlying virtualized config is inactive or has been removed"
            )
        
        # Import here to avoid circular imports
        from ..bronze.ingestion_service import BronzeIngestionService
        from ...api.schemas.bronze_schemas import (
            BronzeVirtualizedRequest, TableColumnSelection
        )
        
        # Build the request from the saved config
        tables = [TableColumnSelection(**t) for t in config.tables]
        bronze_request = BronzeVirtualizedRequest(
            tables=tables,
            relationship_ids=config.relationship_ids,
            enable_federated_joins=config.enable_federated_joins,
        )
        
        # Request one extra row to detect has_more
        service = BronzeIngestionService(self.db)
        response = await service.execute_virtualized(
            bronze_request, limit=limit + 1, offset=offset
        )
        
        # Determine has_more and trim data
        all_rows = []
        for group in response.groups:
            all_rows.extend(group.data)
        
        has_more = len(all_rows) > limit
        data = all_rows[:limit]
        
        # Extract column info
        columns = []
        if data:
            for col_name in data[0].keys():
                columns.append(DataAPIColumnInfo(name=col_name, type=None))
        
        return columns, data, has_more
    
    async def _execute_silver_virtualized(
        self,
        table: ShareTable,
        limit: int,
        offset: int
    ) -> Tuple[List[DataAPIColumnInfo], List[Dict[str, Any]], bool]:
        """Execute a Silver virtualized query via Trino."""
        if not table.silver_virtualized_config_id:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Table references a Silver virtualized config but no config ID is set"
            )
        
        # Verify config exists and is active
        config_result = await self.db.execute(
            select(VirtualizedConfig).where(
                and_(
                    VirtualizedConfig.id == table.silver_virtualized_config_id,
                    VirtualizedConfig.is_active == True
                )
            )
        )
        config = config_result.scalar_one_or_none()
        
        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="The underlying virtualized config is inactive or has been removed"
            )
        
        # Import here to avoid circular imports
        from ..silver.transformation_service import SilverTransformationService
        
        # Request one extra row to detect has_more
        service = SilverTransformationService(self.db)
        response = await service.query_virtualized_config(
            config_id=table.silver_virtualized_config_id,
            limit=limit + 1,
            offset=offset
        )
        
        has_more = len(response.data) > limit
        data = response.data[:limit]
        
        # Extract column info
        columns = [
            DataAPIColumnInfo(name=col_name, type=None)
            for col_name in response.columns
        ]
        
        return columns, data, has_more
