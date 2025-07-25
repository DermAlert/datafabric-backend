from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, desc, text
from sqlalchemy.orm import selectinload, joinedload
from typing import Optional, List, Dict, Any, AsyncGenerator
from fastapi import HTTPException, status, Request
import json
import boto3
from botocore.exceptions import ClientError
from urllib.parse import quote
from datetime import datetime, timedelta
import logging
import os
import pyspark
from delta import configure_spark_with_delta_pip

# Optional imports for schema inference
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

from ...database.delta_sharing.delta_sharing import (
    Share, ShareSchema, ShareTable, Recipient, 
    RecipientAccessLog, ShareTableVersion, ShareTableFile,
    ShareStatus, TableShareStatus, recipient_shares
)
from ...database.core.core import Dataset
from ...api.schemas.delta_sharing_schemas import (
    DeltaSharingProtocol, TableMetadata, FileAction, RemoveFile,
    ShareResponse, ListSharesResponse, SchemaResponse, ListSchemasResponse,
    TableResponse, ListTablesResponse, TableVersionResponse,
    QueryTableRequest, DeltaSharingError
)

logger = logging.getLogger(__name__)

class DeltaSharingProtocolService:
    """Service for Delta Sharing protocol operations"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._minio_client = None  # Lazy initialization through property
        self._current_table_version = None  # Store current table version for headers
        self._spark = None  # Lazy initialization for Spark session
    
    def get_current_table_version(self) -> Optional[int]:
        """Get the current table version for response headers"""
        return self._current_table_version
    
    @property
    def minio_client(self):
        """Lazy initialization of MinIO client"""
        if self._minio_client is None:
            endpoint_url = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
            access_key = os.getenv('MINIO_ACCESS_KEY', 'minio')
            secret_key = os.getenv('MINIO_SECRET_KEY', 'minio123')
            
            # Ensure endpoint URL has protocol
            if not endpoint_url.startswith(('http://', 'https://')):
                endpoint_url = f'http://{endpoint_url}'
            
            self._minio_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name='us-east-1',
                use_ssl=False,  # Disable SSL for development
                verify=False    # Disable SSL verification for development
            )
        return self._minio_client
    
    @property
    def spark(self):
        """Lazy initialization of Spark session for Delta Lake"""
        if self._spark is None:
            try:
                endpoint_url = os.getenv('MINIO_ENDPOINT', 'localhost:9000')
                access_key = os.getenv('MINIO_ACCESS_KEY', 'minio')
                secret_key = os.getenv('MINIO_SECRET_KEY', 'minio123')
                
                # Remove protocol from endpoint for Spark configuration
                if endpoint_url.startswith('http://'):
                    endpoint_url = endpoint_url[7:]
                elif endpoint_url.startswith('https://'):
                    endpoint_url = endpoint_url[8:]
                
                endpoint_with_protocol = f"http://{endpoint_url}"
                
                builder = pyspark.sql.SparkSession.builder \
                    .appName("DeltaSharingProtocolService") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.hadoop.fs.s3a.endpoint", endpoint_with_protocol) \
                    .config("spark.hadoop.fs.s3a.access.key", access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.executor.memory", "512m") \
                    .config("spark.driver.memory", "256m")
                
                packages = [
                    "io.delta:delta-spark_2.12:3.2.0",
                    "org.apache.hadoop:hadoop-aws:3.3.4"
                ]
                
                builder = builder.config("spark.jars.packages", ",".join(packages))
                self._spark = configure_spark_with_delta_pip(builder).getOrCreate()
                
                # Set log level to reduce noise
                self._spark.sparkContext.setLogLevel("WARN")
                
                logger.info("Spark session initialized for Delta Sharing")
                
            except Exception as e:
                logger.error(f"Failed to initialize Spark session: {e}")
                # Return None if Spark fails to initialize
                self._spark = None
                
        return self._spark
    
    async def authenticate_recipient(self, authorization: str) -> Recipient:
        """Authenticate recipient using bearer token"""
        if not authorization or not authorization.startswith('Bearer '):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authorization header"
            )
        
        token = authorization[7:]  # Remove 'Bearer ' prefix
        
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
                detail="Invalid or expired token"
            )
        
        return recipient
    
    async def log_access(self, recipient: Recipient, request: Request, operation: str, 
                        share_id: Optional[int] = None, schema_id: Optional[int] = None, 
                        table_id: Optional[int] = None, response_status: int = 200, 
                        response_size: Optional[int] = None, processing_time: Optional[int] = None,
                        error_message: Optional[str] = None) -> None:
        """Log recipient access for audit purposes"""
        if not recipient.access_logged:
            return
        
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
    
    # ==================== Protocol API Implementation ====================
    
    async def list_shares(self, recipient: Recipient, max_results: Optional[int] = None, 
                         page_token: Optional[str] = None) -> ListSharesResponse:
        """List shares accessible to the recipient"""
        # Get shares assigned to this recipient
        query = select(Share).select_from(
            Share.__table__.join(recipient_shares, Share.id == recipient_shares.c.share_id)
        ).where(
            and_(
                recipient_shares.c.recipient_id == recipient.id,
                Share.status == ShareStatus.ACTIVE
            )
        ).order_by(Share.name)
        
        # Apply pagination
        if max_results:
            query = query.limit(max_results)
        
        if page_token:
            # Decode page token (simple implementation - in production use proper pagination)
            try:
                offset = int(page_token)
                query = query.offset(offset)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid page token"
                )
        
        result = await self.db.execute(query)
        shares = result.scalars().all()
        
        # Build response
        items = [ShareResponse(name=share.name, id=str(share.id)) for share in shares]
        
        # Calculate next page token
        next_page_token = None
        if max_results and len(shares) == max_results:
            current_offset = int(page_token or 0)
            next_page_token = str(current_offset + max_results)
        
        return ListSharesResponse(items=items, nextPageToken=next_page_token)
    
    async def get_share(self, recipient: Recipient, share_name: str) -> ShareResponse:
        """Get share metadata"""
        share = await self._get_accessible_share(recipient, share_name)
        return ShareResponse(name=share.name, id=str(share.id))
    
    async def list_schemas(self, recipient: Recipient, share_name: str, 
                          max_results: Optional[int] = None, page_token: Optional[str] = None) -> ListSchemasResponse:
        """List schemas in a share"""
        share = await self._get_accessible_share(recipient, share_name)
        
        query = select(ShareSchema).where(ShareSchema.share_id == share.id).order_by(ShareSchema.name)
        
        # Apply pagination
        if max_results:
            query = query.limit(max_results)
        
        if page_token:
            try:
                offset = int(page_token)
                query = query.offset(offset)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid page token"
                )
        
        result = await self.db.execute(query)
        schemas = result.scalars().all()
        
        # Build response
        items = [SchemaResponse(name=schema.name, share=share.name) for schema in schemas]
        
        # Calculate next page token
        next_page_token = None
        if max_results and len(schemas) == max_results:
            current_offset = int(page_token or 0)
            next_page_token = str(current_offset + max_results)
        
        return ListSchemasResponse(items=items, nextPageToken=next_page_token)
    
    async def list_tables(self, recipient: Recipient, share_name: str, schema_name: str,
                         max_results: Optional[int] = None, page_token: Optional[str] = None) -> ListTablesResponse:
        """List tables in a schema"""
        share = await self._get_accessible_share(recipient, share_name)
        schema = await self._get_schema_in_share(share, schema_name)
        
        query = select(ShareTable).where(
            and_(
                ShareTable.schema_id == schema.id,
                ShareTable.status == TableShareStatus.ACTIVE
            )
        ).order_by(ShareTable.name)
        
        # Apply pagination
        if max_results:
            query = query.limit(max_results)
        
        if page_token:
            try:
                offset = int(page_token)
                query = query.offset(offset)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid page token"
                )
        
        result = await self.db.execute(query)
        tables = result.scalars().all()
        
        # Build response
        items = [
            TableResponse(
                name=table.name, 
                schema=schema.name, 
                share=share.name,
                shareId=str(share.id),
                id=str(table.id)
            ) 
            for table in tables
        ]
        
        # Calculate next page token
        next_page_token = None
        if max_results and len(tables) == max_results:
            current_offset = int(page_token or 0)
            next_page_token = str(current_offset + max_results)
        
        return ListTablesResponse(items=items, nextPageToken=next_page_token)
    
    async def list_all_tables(self, recipient: Recipient, share_name: str,
                             max_results: Optional[int] = None, page_token: Optional[str] = None) -> ListTablesResponse:
        """List all tables under all schemas in a share"""
        share = await self._get_accessible_share(recipient, share_name)
        
        query = select(ShareTable, ShareSchema).join(ShareSchema).where(
            and_(
                ShareSchema.share_id == share.id,
                ShareTable.status == TableShareStatus.ACTIVE
            )
        ).order_by(ShareSchema.name, ShareTable.name)
        
        # Apply pagination
        if max_results:
            query = query.limit(max_results)
        
        if page_token:
            try:
                offset = int(page_token)
                query = query.offset(offset)
            except (ValueError, TypeError):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid page token"
                )
        
        result = await self.db.execute(query)
        table_data = result.all()
        
        # Build response
        items = [
            TableResponse(
                name=table.name, 
                schema=schema.name, 
                share=share.name,
                shareId=str(share.id),
                id=str(table.id)
            ) 
            for table, schema in table_data
        ]
        
        # Calculate next page token
        next_page_token = None
        if max_results and len(table_data) == max_results:
            current_offset = int(page_token or 0)
            next_page_token = str(current_offset + max_results)
        
        return ListTablesResponse(items=items, nextPageToken=next_page_token)
    
    async def get_table_version(self, recipient: Recipient, share_name: str, schema_name: str, 
                               table_name: str, starting_timestamp: Optional[str] = None) -> TableVersionResponse:
        """Get table version"""
        table = await self._get_accessible_table(recipient, share_name, schema_name, table_name)
        
        # For simplicity, return current version. In a full implementation,
        # you would handle time travel queries with starting_timestamp
        return TableVersionResponse(
            version=table.current_version,
            timestamp=table.data_atualizacao.isoformat() if table.data_atualizacao else None
        )
    
    async def get_table_metadata(self, recipient: Recipient, share_name: str, schema_name: str, 
                                table_name: str, starting_timestamp: Optional[str] = None) -> AsyncGenerator[str, None]:
        """Get table metadata (streaming response)"""
        table = await self._get_accessible_table(recipient, share_name, schema_name, table_name)
        
        # Store table version for header (can be accessed by caller)
        self._current_table_version = table.current_version
        
        # Yield protocol information
        protocol = DeltaSharingProtocol(
            minReaderVersion=table.min_reader_version,
            minWriterVersion=table.min_writer_version
        )
        yield json.dumps({"protocol": protocol.dict()}) + "\n"
        
        # Yield table metadata
        metadata = await self._build_table_metadata(table)
        yield json.dumps({"metaData": metadata.dict()}) + "\n"
    
    async def query_table(self, recipient: Recipient, share_name: str, schema_name: str, 
                         table_name: str, query_request: QueryTableRequest) -> AsyncGenerator[str, None]:
        """Query table data (streaming response)"""
        table = await self._get_accessible_table(recipient, share_name, schema_name, table_name)
        
        # Store table version for header (can be accessed by caller)
        self._current_table_version = table.current_version
        
        # Yield protocol information
        protocol = DeltaSharingProtocol(
            minReaderVersion=table.min_reader_version,
            minWriterVersion=table.min_writer_version
        )
        yield json.dumps({"protocol": protocol.dict()}) + "\n"
        
        # Yield table metadata
        metadata = await self._build_table_metadata(table)
        yield json.dumps({"metaData": metadata.dict()}) + "\n"
        
        # Get files for this table
        files = await self._get_table_files(table, query_request)
        
        # Yield file actions
        for file_action in files:
            yield json.dumps({"file": file_action.dict()}) + "\n"
    
    async def get_table_changes(self, recipient: Recipient, share_name: str, schema_name: str, 
                               table_name: str, starting_version: Optional[int] = None,
                               starting_timestamp: Optional[str] = None, ending_version: Optional[int] = None,
                               ending_timestamp: Optional[str] = None, 
                               include_historical_metadata: Optional[bool] = None) -> AsyncGenerator[str, None]:
        """Get table changes (Change Data Feed)"""
        table = await self._get_accessible_table(recipient, share_name, schema_name, table_name)
        
        # Yield protocol information
        protocol = DeltaSharingProtocol(
            minReaderVersion=table.min_reader_version,
            minWriterVersion=table.min_writer_version
        )
        yield json.dumps({"protocol": protocol.dict()}) + "\n"
        
        # For this implementation, we'll return current state
        # In a full implementation, you would track actual changes
        files = await self._get_table_files(table, QueryTableRequest())
        
        # Yield file actions with change metadata
        for file_action in files:
            # Add change data feed specific fields
            file_dict = file_action.dict()
            file_dict.update({
                "version": table.current_version,
                "timestamp": int(table.data_atualizacao.timestamp() * 1000) if table.data_atualizacao else None
            })
            yield json.dumps({"file": file_dict}) + "\n"
    
    # ==================== Helper Methods ====================
    
    async def _get_accessible_share(self, recipient: Recipient, share_name: str) -> Share:
        """Get share that is accessible to the recipient"""
        query = select(Share).select_from(
            Share.__table__.join(recipient_shares, Share.id == recipient_shares.c.share_id)
        ).where(
            and_(
                Share.name == share_name,
                recipient_shares.c.recipient_id == recipient.id,
                Share.status == ShareStatus.ACTIVE
            )
        )
        result = await self.db.execute(query)
        share = result.scalar_one_or_none()
        
        if not share:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Share '{share_name}' not found or not accessible"
            )
        
        return share
    
    async def _get_schema_in_share(self, share: Share, schema_name: str) -> ShareSchema:
        """Get schema within a share"""
        query = select(ShareSchema).where(
            and_(
                ShareSchema.share_id == share.id,
                ShareSchema.name == schema_name
            )
        )
        result = await self.db.execute(query)
        schema = result.scalar_one_or_none()
        
        if not schema:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema '{schema_name}' not found in share '{share.name}'"
            )
        
        return schema
    
    async def _get_accessible_table(self, recipient: Recipient, share_name: str, 
                                   schema_name: str, table_name: str) -> ShareTable:
        """Get table that is accessible to the recipient"""
        query = select(ShareTable, ShareSchema, Share).select_from(
            ShareTable.__table__.join(ShareSchema.__table__, ShareTable.schema_id == ShareSchema.id)
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
        table_data = result.first()
        
        if not table_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Table '{table_name}' not found or not accessible"
            )
        
        table, _, _ = table_data
        return table
    
    async def _build_table_metadata(self, table: ShareTable) -> TableMetadata:
        """Build table metadata for Delta Sharing protocol"""
        # Get dataset information
        dataset_query = select(Dataset).where(Dataset.id == table.dataset_id)
        dataset_result = await self.db.execute(dataset_query)
        dataset = dataset_result.scalar_one_or_none()
        
        # Read schema dynamically from the actual Delta Lake data
        schema_string = await self._get_dynamic_schema_from_delta(table)
        
        # Ensure we always have a valid schema string
        if not schema_string:
            # This should not happen if Delta Lake data exists
            logger.error(f"No schema found for table {table.id} with storage_location: {table.storage_location}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unable to determine schema for table {table.name}"
            )
        
        return TableMetadata(
            id=str(table.id),
            name=table.name,
            description=table.description,
            format={"provider": table.table_format},
            schemaString=schema_string,
            partitionColumns=table.partition_columns or [],
            configuration=table.table_properties or {},
            version=table.current_version,
            size=None,  # Would be calculated from actual files
            numFiles=None  # Would be calculated from actual files
        )
    
    async def _get_table_files(self, table: ShareTable, query_request: QueryTableRequest) -> List[FileAction]:
        """Get files for a table using Delta Lake to avoid data duplication"""
        files = []
        
        # If table has a storage location, read from Delta Lake
        if table.storage_location and self.spark:
            try:
                # Build Delta Lake path
                bucket_name, prefix = self._parse_storage_location(table.storage_location)
                delta_path = f"s3a://{bucket_name}/{prefix}"
                
                logger.info(f"Reading Delta table from: {delta_path}")
                
                # Read Delta Lake table to get current files only (no duplicates)
                delta_table = self.spark.read.format("delta").load(delta_path)
                
                # Get the underlying files from the Delta Lake transaction log
                # This ensures we only get the current version of files, no duplicates
                table_details = self.spark.sql(f"DESCRIBE DETAIL delta.`{delta_path}`").collect()
                
                if table_details:
                    # Get table location and list current files
                    table_location = table_details[0]['location']
                    current_version = table_details[0]['minReaderVersion']
                    
                    # Use Delta Lake's internal file listing to get current files only
                    try:
                        # Build base query
                        base_query = f"SELECT input_file_name() as file_path, count(*) as row_count FROM delta.`{delta_path}`"
                        
                        # Apply filters if provided
                        where_clause = ""
                        if query_request.jsonPredicateHints:
                            logger.info(f"Raw jsonPredicateHints received: {query_request.jsonPredicateHints}")
                            
                            # Handle both string and dict formats
                            predicate_dict = query_request.jsonPredicateHints
                            if isinstance(predicate_dict, str):
                                try:
                                    predicate_dict = json.loads(predicate_dict)
                                    logger.info(f"Parsed jsonPredicateHints to dict: {predicate_dict}")
                                except json.JSONDecodeError as e:
                                    logger.warning(f"Failed to parse jsonPredicateHints as JSON: {e}")
                                    predicate_dict = None
                            
                            if predicate_dict:
                                logger.info(f"Converting predicate to SQL: {predicate_dict}")
                                logger.info(f"Converting predicate to SQL: {predicate_dict}")
                                sql_condition = self._convert_json_predicate_to_sql(predicate_dict)
                                logger.info(f"SQL condition result: {sql_condition}")
                                if sql_condition:
                                    where_clause = f" WHERE {sql_condition}"
                                    logger.info(f"Generated WHERE clause: {where_clause}")
                                else:
                                    logger.warning("Failed to convert predicate to SQL condition")
                            else:
                                logger.warning("Predicate dict is None after processing")
                        
                        # Build base query to find files that contain data matching the filter
                        # IMPORTANT: Delta Sharing protocol limitation workaround
                        # Standard Delta Sharing works at file level, but for mixed data files,
                        # we need to actually filter the data and create temporary filtered files
                        
                        if where_clause:
                            logger.info("Applying data-level filtering using PySpark")
                            
                            try:
                                # Read the full Delta table and apply the filter
                                filtered_df = self.spark.sql(f"""
                                    SELECT * FROM delta.`{delta_path}`
                                    {where_clause}
                                """)
                                
                                # Check if filtered data exists
                                filtered_count = filtered_df.count()
                                logger.info(f"Filter matched {filtered_count} rows")
                                
                                if filtered_count == 0:
                                    logger.info("No data matches the filter")
                                    return []
                                
                                # Create a temporary location for filtered data
                                import uuid
                                temp_suffix = str(uuid.uuid4())[:8]
                                temp_location = f"{prefix}_filtered_{temp_suffix}"
                                temp_delta_path = f"s3a://{bucket_name}/{temp_location}"
                                
                                logger.info(f"Saving filtered data to temporary location: {temp_delta_path}")
                                
                                # Save filtered data as Delta format to temporary location
                                filtered_df.write.format("delta").mode("overwrite").save(temp_delta_path)
                                
                                # Now get the files from the filtered Delta table
                                filtered_files_query = f"SELECT DISTINCT input_file_name() as file_path FROM delta.`{temp_delta_path}`"
                                filtered_files_df = self.spark.sql(filtered_files_query)
                                active_files = [row['file_path'] for row in filtered_files_df.collect()]
                                
                                logger.info(f"Created {len(active_files)} filtered files")
                                
                                # Note: In production, you'd want to clean up temp files after some time
                                # For now, we'll let them accumulate (they're relatively small)
                                
                            except Exception as e:
                                logger.error(f"Error creating filtered data: {e}")
                                # Fallback to standard file-level filtering
                                files_with_data_query = f"""
                                    SELECT DISTINCT input_file_name() as file_path
                                    FROM delta.`{delta_path}`
                                    {where_clause}
                                """
                                logger.info(f"Falling back to file-level filtering: {files_with_data_query}")
                                relevant_files_df = self.spark.sql(files_with_data_query)
                                active_files = [row['file_path'] for row in relevant_files_df.collect()]
                            
                        else:
                            # No filter, get all files
                            all_files_query = f"SELECT DISTINCT input_file_name() as file_path FROM delta.`{delta_path}`"
                            logger.info(f"Getting all files: {all_files_query}")
                            active_files_df = self.spark.sql(all_files_query)
                            active_files = [row['file_path'] for row in active_files_df.collect()]
                        
                        logger.info(f"Found {len(active_files)} active files in Delta table")
                        
                        # Create file actions for each active file
                        for file_path in active_files:
                            # Extract the key from the full path
                            if bucket_name in file_path:
                                # Remove the s3a:// prefix and bucket name
                                key = file_path.replace(f"s3a://{bucket_name}/", "")
                            else:
                                # Fallback: extract just the filename
                                key = file_path.split('/')[-1]
                            
                            # Generate presigned URL
                            try:
                                presigned_url = self.minio_client.generate_presigned_url(
                                    'get_object',
                                    Params={'Bucket': bucket_name, 'Key': key},
                                    ExpiresIn=3600  # 1 hour
                                )
                                
                                # Get file size (try to get from MinIO)
                                file_size = 0
                                try:
                                    response = self.minio_client.head_object(bucket_name, key)
                                    file_size = response.get('ContentLength', 0)
                                except:
                                    file_size = 0  # Default if we can't get size
                                
                                # Create file action
                                file_action = FileAction(
                                    url=presigned_url,
                                    id=key.split('/')[-1],  # Use filename as ID
                                    partitionValues={},  # Would be parsed from path in production
                                    size=file_size,
                                    stats=None,  # Would include min/max/null stats in production
                                    version=table.current_version,
                                    timestamp=int(datetime.now().timestamp() * 1000)
                                )
                                files.append(file_action)
                                
                            except Exception as e:
                                logger.warning(f"Could not generate presigned URL for {key}: {e}")
                                continue
                    
                    except Exception as e:
                        logger.warning(f"Could not get active files from Delta table, falling back to file listing: {e}")
                        # Fallback to original method but filter for current version only
                        return await self._get_table_files_fallback(table, query_request)
                
            except Exception as e:
                logger.error(f"Error reading Delta table for {table.id}: {e}")
                # Fallback to original method
                return await self._get_table_files_fallback(table, query_request)
        else:
            # Fallback if no Spark or no storage location
            return await self._get_table_files_fallback(table, query_request)
        
        logger.info(f"Found {len(files)} files for table {table.id} using Delta Lake")
        return files
    
    async def _get_table_files_fallback(self, table: ShareTable, query_request: QueryTableRequest) -> List[FileAction]:
        """Fallback method to get table files using MinIO listing (may have duplicates)"""
        files = []
        
        if table.storage_location:
            try:
                # List objects in the storage location - Look for parquet files
                bucket_name, prefix = self._parse_storage_location(table.storage_location)
                
                response = self.minio_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )
                
                # Keep track of parquet files we've seen
                parquet_files = set()
                
                for obj in response.get('Contents', []):
                    key = obj['Key']
                    
                    # Look for parquet files (MinIO stores them as directories)
                    if 'part-' in key and '.snappy.parquet' in key:
                        # Extract the parquet file path
                        if key.endswith('/xl.meta'):
                            parquet_path = key.replace('/xl.meta', '')
                        else:
                            parquet_path = key
                            
                        # Avoid duplicates
                        if parquet_path in parquet_files:
                            continue
                        parquet_files.add(parquet_path)
                        
                        # Skip delta log files
                        if '_delta_log' in parquet_path:
                            continue
                        
                        # Generate presigned URL for the actual parquet file
                        try:
                            presigned_url = self.minio_client.generate_presigned_url(
                                'get_object',
                                Params={'Bucket': bucket_name, 'Key': parquet_path},
                                ExpiresIn=3600  # 1 hour
                            )
                            
                            # Create file action
                            file_action = FileAction(
                                url=presigned_url,
                                id=parquet_path.split('/')[-1],  # Use filename as ID
                                partitionValues={},  # Would be parsed from path in production
                                size=obj.get('Size', 0),
                                stats=None,  # Would include min/max/null stats in production
                                version=table.current_version,
                                timestamp=int(obj.get('LastModified', obj.get('last_modified', datetime.now())).timestamp() * 1000)
                            )
                            files.append(file_action)
                        except Exception as e:
                            logger.warning(f"Could not generate presigned URL for {parquet_path}: {e}")
                            continue
                        
            except ClientError as e:
                logger.error(f"Error accessing storage for table {table.id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error accessing storage for table {table.id}: {e}")
        
        logger.info(f"Found {len(files)} files for table {table.id} using fallback method")
        return files
    
    async def _get_dynamic_schema_from_delta(self, table: ShareTable) -> Optional[str]:
        """Read the actual schema from Delta Lake data using Spark"""
        try:
            # First try to read directly from Delta Lake using Spark
            if table.storage_location and self.spark:
                bucket_name, prefix = self._parse_storage_location(table.storage_location)
                delta_path = f"s3a://{bucket_name}/{prefix}"
                
                logger.info(f"Reading schema from Delta table: {delta_path}")
                
                try:
                    # Read Delta table and get schema
                    delta_table = self.spark.read.format("delta").load(delta_path)
                    spark_schema = delta_table.schema
                    
                    # Convert Spark schema to Delta Sharing format
                    return self._convert_spark_schema_to_delta_sharing(spark_schema)
                except Exception as e:
                    logger.warning(f"Failed to read schema using Spark: {e}")
            
            # Fallback: try to read from Delta Lake transaction log files
            if table.storage_location:
                schema_from_log = await self._read_delta_schema(table.storage_location)
                if schema_from_log:
                    return schema_from_log
            
            # Try to get from database table schema_string
            if table.schema_string:
                return table.schema_string
            
            # Get dataset information and check properties
            dataset_query = select(Dataset).where(Dataset.id == table.dataset_id)
            dataset_result = await self.db.execute(dataset_query)
            dataset = dataset_result.scalar_one_or_none()
            
            if dataset and dataset.properties and isinstance(dataset.properties, dict):
                if 'schema' in dataset.properties:
                    return dataset.properties['schema']
                if 'schemaString' in dataset.properties:
                    return dataset.properties['schemaString']
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get dynamic schema for table {table.id}: {e}")
            return None
    
    async def _infer_schema_from_parquet_files(self, storage_location: str) -> Optional[str]:
        """Infer schema from actual parquet files"""
        if not PYARROW_AVAILABLE:
            logger.warning("PyArrow not available, cannot infer schema from parquet files")
            return None
            
        try:
            bucket_name, prefix = self._parse_storage_location(storage_location)
            
            # List parquet files
            response = self.minio_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            # Find a parquet file to read schema from
            for obj in response.get('Contents', []):
                key = obj['Key']
                if 'part-' in key and '.snappy.parquet' in key and not '_delta_log' in key:
                    try:
                        # For MinIO structure, we need the directory name (without /xl.meta)
                        parquet_path = key.replace('/xl.meta', '') if key.endswith('/xl.meta') else key
                        
                        # Try to read parquet metadata using boto3
                        import json
                        
                        # Get the parquet file from MinIO
                        obj_response = self.minio_client.get_object(Bucket=bucket_name, Key=parquet_path)
                        parquet_data = obj_response['Body'].read()
                        
                        # Read schema from parquet
                        parquet_file = pq.ParquetFile(pa.BufferReader(parquet_data))
                        arrow_schema = parquet_file.schema.to_arrow_schema()
                        
                        # Convert Arrow schema to Delta Lake schema format
                        delta_schema = self._convert_arrow_schema_to_delta(arrow_schema)
                        return json.dumps(delta_schema)
                        
                    except Exception as e:
                        logger.warning(f"Could not read schema from parquet file {parquet_path}: {e}")
                        continue
            
            return None
            
        except Exception as e:
            logger.warning(f"Could not infer schema from parquet files at {storage_location}: {e}")
            return None
    
    def _convert_arrow_schema_to_delta(self, arrow_schema) -> dict:
        """Convert PyArrow schema to Delta Lake schema format"""
        import pyarrow as pa
        
        def arrow_type_to_delta_type(arrow_type):
            """Convert Arrow data type to Delta Lake data type"""
            if pa.types.is_string(arrow_type):
                return "string"
            elif pa.types.is_integer(arrow_type):
                if arrow_type == pa.int64():
                    return "long"
                else:
                    return "integer"
            elif pa.types.is_floating(arrow_type):
                if arrow_type == pa.float64():
                    return "double"
                else:
                    return "float"
            elif pa.types.is_boolean(arrow_type):
                return "boolean"
            elif pa.types.is_timestamp(arrow_type):
                return "timestamp"
            elif pa.types.is_date(arrow_type):
                return "date"
            else:
                return "string"  # Default fallback
        
        fields = []
        for field in arrow_schema:
            delta_field = {
                "name": field.name,
                "type": arrow_type_to_delta_type(field.type),
                "nullable": field.nullable,
                "metadata": {}
            }
            fields.append(delta_field)
        
    async def _read_delta_schema(self, storage_location: str) -> Optional[str]:
        """Read schema from Delta Lake transaction log"""
        try:
            bucket_name, prefix = self._parse_storage_location(storage_location)
            
            # Look for Delta log files
            response = self.minio_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=f"{prefix}/_delta_log"
            )
            
            # Find Delta log JSON files (look for the directory structure in MinIO)
            json_files = []
            for obj in response.get('Contents', []):
                if '.json' in obj['Key'] and not obj['Key'].endswith('/xl.meta'):
                    json_files.append(obj['Key'])
                elif '.json/xl.meta' in obj['Key']:
                    # Extract the json file path (remove /xl.meta)
                    json_file = obj['Key'].replace('/xl.meta', '')
                    json_files.append(json_file)
            
            if json_files:
                # Sort to get the first file which should have metadata
                json_files.sort()
                metadata_file = json_files[0]  # Start with the first file which should have metadata
                
                # Try to read the metadata file
                try:
                    obj_response = self.minio_client.get_object(
                        Bucket=bucket_name,
                        Key=metadata_file
                    )
                    content = obj_response['Body'].read().decode('utf-8')
                    
                    # Parse Delta log entries (NDJSON format - one JSON per line)
                    import json as json_module
                    lines = content.strip().split('\n')
                    
                    for line in lines:
                        if line.strip():
                            try:
                                log_entry = json_module.loads(line)
                                
                                # Look for metaData entry
                                if 'metaData' in log_entry:
                                    schema_string = log_entry['metaData'].get('schemaString')
                                    if schema_string:
                                        return schema_string
                            except json_module.JSONDecodeError:
                                continue
                        
                except Exception as e:
                    logger.warning(f"Could not read Delta log file {metadata_file}: {e}")
                    
        except Exception as e:
            logger.warning(f"Could not read Delta schema from {storage_location}: {e}")
            
        return None
    
    def _convert_arrow_schema_to_delta(self, arrow_schema) -> dict:
        """Convert PyArrow schema to Delta Lake schema format"""
        if not PYARROW_AVAILABLE:
            return {"type": "struct", "fields": []}
            
        def arrow_type_to_delta_type(arrow_type):
            """Convert Arrow data type to Delta Lake data type"""
            if pa.types.is_string(arrow_type):
                return "string"
            elif pa.types.is_integer(arrow_type):
                if arrow_type == pa.int64():
                    return "long"
                else:
                    return "integer"
            elif pa.types.is_floating(arrow_type):
                if arrow_type == pa.float64():
                    return "double"
                else:
                    return "float"
            elif pa.types.is_boolean(arrow_type):
                return "boolean"
            elif pa.types.is_timestamp(arrow_type):
                return "timestamp"
            elif pa.types.is_date(arrow_type):
                return "date"
            else:
                return "string"  # Default fallback
        
        fields = []
        for field in arrow_schema:
            delta_field = {
                "name": field.name,
                "type": arrow_type_to_delta_type(field.type),
                "nullable": field.nullable,
                "metadata": {}
            }
            fields.append(delta_field)
        
        return {
            "type": "struct",
            "fields": fields
        }
    
    def _parse_storage_location(self, storage_location: str) -> tuple[str, str]:
        """Parse storage location to extract bucket and prefix"""
        # Handle different storage location formats
        if storage_location.startswith('s3://'):
            path = storage_location[5:]
        elif storage_location.startswith('minio://'):
            path = storage_location[8:]
        else:
            path = storage_location
        
        parts = path.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        return bucket_name, prefix
    
    def _convert_spark_schema_to_delta_sharing(self, spark_schema) -> str:
        """Convert Spark DataFrame schema to Delta Sharing schema format"""
        try:
            fields = []
            
            for field in spark_schema.fields:
                # Map Spark types to Delta Sharing types
                field_type = self._map_spark_type_to_delta_sharing(str(field.dataType))
                
                delta_field = {
                    "name": field.name,
                    "type": field_type,
                    "nullable": field.nullable,
                    "metadata": {}
                }
                fields.append(delta_field)
            
            schema = {
                "type": "struct",
                "fields": fields
            }
            
            import json
            return json.dumps(schema)
            
        except Exception as e:
            logger.error(f"Error converting Spark schema to Delta Sharing format: {e}")
            # Return a basic schema as fallback
            return '{"type": "struct", "fields": []}'
    
    def _map_spark_type_to_delta_sharing(self, spark_type: str) -> str:
        """Map Spark data types to Delta Sharing data types"""
        spark_type = spark_type.lower()
        
        # Basic type mappings
        type_mappings = {
            'stringtype': 'string',
            'integertype': 'integer', 
            'longtype': 'long',
            'doubletype': 'double',
            'floattype': 'float',
            'booleantype': 'boolean',
            'timestamptype': 'timestamp',
            'datetype': 'date',
            'binarytype': 'binary'
        }
        
        # Check for exact matches first
        for spark_name, delta_name in type_mappings.items():
            if spark_name in spark_type:
                return delta_name
        
        # Default to string for unknown types
        return 'string'
    
    def _convert_json_predicate_to_sql(self, predicate: Dict[str, Any]) -> str:
        """Convert JSON predicate hints to SQL WHERE clause"""
        logger.info(f"Converting predicate: {predicate}")
        
        if not predicate or 'op' not in predicate:
            logger.warning("Predicate is empty or missing 'op' field")
            return None
        
        op = predicate['op']
        logger.info(f"Processing operation: {op}")
        
        try:
            if op == 'column':
                column_name = predicate.get('name', '')
                logger.info(f"Column operation: {column_name}")
                return column_name
            
            elif op == 'literal':
                value = predicate.get('value', '')
                value_type = predicate.get('valueType', 'string')
                logger.info(f"Literal operation: value={value}, type={value_type}")
                
                if value_type == 'string':
                    return f"'{value}'"
                else:
                    return str(value)
            
            elif op == 'equal':
                children = predicate.get('children', [])
                logger.info(f"Equal operation with {len(children)} children: {children}")
                
                if len(children) == 2:
                    left = self._convert_json_predicate_to_sql(children[0])
                    right = self._convert_json_predicate_to_sql(children[1])
                    logger.info(f"Equal parts: left='{left}', right='{right}'")
                    
                    if left and right:
                        result = f"{left} = {right}"
                        logger.info(f"Generated equal condition: {result}")
                        return result
                else:
                    logger.warning(f"Equal operation expects 2 children, got {len(children)}")
            
            elif op == 'lessThan':
                children = predicate.get('children', [])
                if len(children) == 2:
                    left = self._convert_json_predicate_to_sql(children[0])
                    right = self._convert_json_predicate_to_sql(children[1])
                    if left and right:
                        return f"{left} < {right}"
            
            elif op == 'lessThanOrEqual':
                children = predicate.get('children', [])
                if len(children) == 2:
                    left = self._convert_json_predicate_to_sql(children[0])
                    right = self._convert_json_predicate_to_sql(children[1])
                    if left and right:
                        return f"{left} <= {right}"
            
            elif op == 'greaterThan':
                children = predicate.get('children', [])
                if len(children) == 2:
                    left = self._convert_json_predicate_to_sql(children[0])
                    right = self._convert_json_predicate_to_sql(children[1])
                    if left and right:
                        return f"{left} > {right}"
            
            elif op == 'greaterThanOrEqual':
                children = predicate.get('children', [])
                if len(children) == 2:
                    left = self._convert_json_predicate_to_sql(children[0])
                    right = self._convert_json_predicate_to_sql(children[1])
                    if left and right:
                        return f"{left} >= {right}"
            
            elif op == 'isNull':
                children = predicate.get('children', [])
                if len(children) == 1:
                    operand = self._convert_json_predicate_to_sql(children[0])
                    if operand:
                        return f"{operand} IS NULL"
            
            elif op == 'and':
                children = predicate.get('children', [])
                conditions = []
                for child in children:
                    condition = self._convert_json_predicate_to_sql(child)
                    if condition:
                        conditions.append(condition)
                if conditions:
                    return " AND ".join([f"({cond})" for cond in conditions])
            
            elif op == 'or':
                children = predicate.get('children', [])
                conditions = []
                for child in children:
                    condition = self._convert_json_predicate_to_sql(child)
                    if condition:
                        conditions.append(condition)
                if conditions:
                    return " OR ".join([f"({cond})" for cond in conditions])
            
            elif op == 'not':
                children = predicate.get('children', [])
                if len(children) == 1:
                    operand = self._convert_json_predicate_to_sql(children[0])
                    if operand:
                        return f"NOT ({operand})"
        
        except Exception as e:
            logger.error(f"Error converting predicate to SQL: {e}")
            return None
        
        logger.warning(f"Unhandled operation: {op}")
        return None
