"""
Bronze Ingestion Service

This service implements the "Smart Bronze" architecture for data ingestion.
It handles:
1. Grouping tables by connection (for pushdown optimization)
2. Generating optimized SQL for each group
3. Executing the ingestion via Trino
4. Storing metadata for Silver layer processing

Key principles:
- No value transformations (raw data only)
- Joins within same DB are pushed down to source
- Inter-DB relationships are metadata-only (for Silver)
- Column selection is respected (no unnecessary data transfer)
"""

import os
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict
from datetime import datetime
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_
from fastapi import HTTPException, status

from ..database.core.core import DataConnection, Dataset
from ..database.metadata.metadata import ExternalTables, ExternalColumn, ExternalSchema
from ..database.metadata.relationships import (
    TableRelationship,
    RelationshipScope,
    RelationshipSource,
)
from ..database.datasets.bronze import (
    DatasetBronzeConfig,
    DatasetIngestionGroup,
    IngestionGroupTable,
    InterSourceLink,
    IngestionStatus,
    JoinStrategy,
    BronzeColumnMapping,
)
from ..api.schemas.bronze_schemas import (
    DatasetBronzeCreate,
    DatasetBronzeCreateRequest,
    DatasetBronzeCreateSimplified,
    TableColumnSelection,
    IntraDBRelationship,
    InterDBRelationship,
    BronzeIngestionPreview,
    IngestionGroupPreview,
    BronzeIngestionResult,
    IngestionGroupResult,
    IngestionStatusEnum,
    JoinStrategyEnum,
    RelationshipUsagePreview,
    BronzeVirtualizedRequest,
    BronzeVirtualizedResponse,
    BronzeVirtualizedGroupResult,
)
from .trino_manager import TrinoManager
from minio import Minio
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class BronzeIngestionService:
    """
    Service for managing Bronze layer ingestion using Smart Bronze architecture.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.trino = TrinoManager()
        self.bronze_bucket = os.getenv("BRONZE_BUCKET", "datafabric-bronze")
        self.bronze_catalog = "bronze"
        self.bronze_schema = "default"
        
        # Ensure bronze infrastructure exists
        self._ensure_bronze_bucket()
        self._ensure_bronze_schema()
    
    def _ensure_bronze_bucket(self):
        """Create the bronze bucket if it doesn't exist"""
        try:
            endpoint = os.getenv("INTERNAL_S3_ENDPOINT", "http://minio:9000")
            access_key = os.getenv("INTERNAL_S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio"))
            secret_key = os.getenv("INTERNAL_S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123"))
            
            parsed = urlparse(endpoint)
            endpoint_host = parsed.netloc if parsed.netloc else parsed.path
            secure = parsed.scheme == "https"
            
            client = Minio(
                endpoint=endpoint_host,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure
            )
            
            if not client.bucket_exists(self.bronze_bucket):
                client.make_bucket(self.bronze_bucket)
                logger.info(f"Created bronze bucket: {self.bronze_bucket}")
            else:
                logger.debug(f"Bronze bucket already exists: {self.bronze_bucket}")
        except Exception as e:
            logger.warning(f"Could not ensure bronze bucket exists: {e}")
    
    def _ensure_bronze_schema(self):
        """Create the bronze schema in Trino if it doesn't exist"""
        try:
            conn = self.trino.get_connection()
            cur = conn.cursor()
            
            # Check if schema exists
            cur.execute(f"SHOW SCHEMAS FROM {self.bronze_catalog} LIKE '{self.bronze_schema}'")
            schemas = cur.fetchall()
            
            if not schemas:
                # Create schema with location pointing to bronze bucket
                create_schema_sql = f"""
                    CREATE SCHEMA IF NOT EXISTS {self.bronze_catalog}.{self.bronze_schema}
                    WITH (location = 's3a://{self.bronze_bucket}/')
                """
                cur.execute(create_schema_sql)
                logger.info(f"Created bronze schema: {self.bronze_catalog}.{self.bronze_schema}")
            else:
                logger.debug(f"Bronze schema already exists: {self.bronze_catalog}.{self.bronze_schema}")
            
            conn.close()
        except Exception as e:
            logger.warning(f"Could not ensure bronze schema exists: {e}")
    
    def _get_dataset_folder_name(self, dataset_name: str, dataset_id: Optional[int] = None) -> str:
        """
        Generate dataset folder name in format: {id}-{name}
        
        If dataset_id is not available (during preview), uses placeholder format.
        
        Args:
            dataset_name: Name of the dataset
            dataset_id: Optional ID of the dataset
            
        Returns:
            Folder name (e.g., "15-melanoma_study" or "{id}-melanoma_study")
        """
        if dataset_id is not None:
            return f"{dataset_id}-{dataset_name}"
        else:
            # During preview, ID is not available yet - use placeholder
            return f"{{id}}-{dataset_name}"
    
    async def generate_ingestion_plan(
        self, 
        dataset_data: DatasetBronzeCreate
    ) -> BronzeIngestionPreview:
        """
        Generate an ingestion plan without executing it.
        
        This method:
        1. Groups tables by their connection
        2. Resolves column selections (expanding select_all)
        3. Generates SQL for each group
        4. Identifies inter-DB links for Silver layer (or processes them if federated joins enabled)
        
        Returns a preview of what will be created.
        """
        # Step 1: Fetch table and column metadata
        table_metadata = await self._fetch_table_metadata(dataset_data.tables)
        
        # Step 2: Resolve column selections
        resolved_columns = await self._resolve_column_selections(
            dataset_data.tables,
            table_metadata
        )
        
        # Step 3: Check if federated joins are enabled
        enable_federated = getattr(dataset_data, 'enable_federated_joins', False)
        
        ingestion_groups = []
        warnings = []
        inter_db_links = []
        
        if enable_federated and dataset_data.inter_db_relationships:
            # FEDERATED MODE: Create a single unified group with all tables joined via Trino
            logger.info("Federated joins enabled - generating unified SQL with cross-database JOINs")
            
            # Combine all relationships (intra + inter)
            all_relationships = []
            if dataset_data.intra_db_relationships:
                all_relationships.extend(dataset_data.intra_db_relationships)
            if dataset_data.inter_db_relationships:
                # Convert InterDBRelationship to IntraDBRelationship format
                for inter_rel in dataset_data.inter_db_relationships:
                    all_relationships.append(IntraDBRelationship(
                        left_table_id=inter_rel.left_table_id,
                        left_column_id=inter_rel.left_column_id,
                        right_table_id=inter_rel.right_table_id,
                        right_column_id=inter_rel.right_column_id,
                        join_strategy=inter_rel.join_strategy,
                        custom_condition=None
                    ))
            
            # Get all table IDs
            all_table_ids = [t.table_id for t in dataset_data.tables]
            
            # Generate federated SQL
            sql, group_columns, column_mappings = await self._generate_federated_sql(
                all_table_ids,
                all_relationships,
                resolved_columns,
                table_metadata
            )
            
            # Generate dataset folder name (preview mode - no ID yet)
            dataset_folder = self._get_dataset_folder_name(dataset_data.name)
            output_path = f"s3a://{self.bronze_bucket}/{dataset_folder}/unified/"
            
            # Build table info for preview
            tables_preview = []
            for table_id in all_table_ids:
                table_info = table_metadata[table_id]
                cols = resolved_columns.get(table_id, [])
                tables_preview.append({
                    'table_id': table_id,
                    'table_name': table_info['table_name'],
                    'schema_name': table_info['schema_name'],
                    'connection_name': table_info['connection_name'],
                    'selected_columns': [c['column_name'] for c in cols],
                    'column_count': len(cols)
                })
            
            # Use the first table's connection_id for the federated group
            # This is needed for foreign key constraint, even though it's a cross-connection query
            first_connection_id = table_metadata[all_table_ids[0]]['connection_id']
            
            ingestion_groups.append(IngestionGroupPreview(
                group_name="unified_federated",
                connection_id=first_connection_id,  # Use first connection for FK constraint
                connection_name="federated",
                tables=tables_preview,
                has_joins=len(all_relationships) > 0,
                estimated_sql=sql,
                output_path=output_path,
                column_mappings=column_mappings
            ))
            
            warnings.append(
                "Federated joins enabled: All tables will be joined in a single query across databases. "
                "This may be slower than the default Bronze architecture for large datasets."
            )
            
        else:
            # STANDARD MODE: Group by connection, separate inter-DB relationships
            # Step 2: Group tables by connection
            connection_groups = self._group_tables_by_connection(
                table_metadata, 
                dataset_data.intra_db_relationships or []
            )
            
            # Step 4: Generate SQL for each group
            # Important: Tables without relationships should be in SEPARATE groups
            for conn_id, group_info in connection_groups.items():
                conn_name = group_info['connection_name']
                tables_in_group = group_info['tables']
                relationships = group_info['relationships']
                
                # Split tables into sub-groups based on relationships
                # Tables that are connected via relationships go together
                # Tables without relationships become their own groups
                sub_groups = self._split_tables_by_relationships(
                    tables_in_group,
                    relationships,
                    table_metadata
                )
                
                for sub_group_idx, sub_group in enumerate(sub_groups):
                    sub_tables = sub_group['tables']
                    sub_relationships = sub_group['relationships']
                    
                    # Generate SQL for this sub-group
                    sql, group_columns, column_mappings = await self._generate_group_sql(
                        conn_id,
                        conn_name,
                        sub_tables,
                        sub_relationships,
                        resolved_columns,
                        table_metadata
                    )
                    
                    # Create unique output path for each sub-group
                    if len(sub_tables) == 1:
                        # Single table - use table name
                        table_info = table_metadata[sub_tables[0]]
                        group_suffix = f"{conn_name}_{table_info['table_name']}"
                    else:
                        # Multiple tables with relationship
                        group_suffix = f"{conn_name}_joined_{sub_group_idx}"
                    
                    # Generate dataset folder name (preview mode - no ID yet)
                    dataset_folder = self._get_dataset_folder_name(dataset_data.name)
                    output_path = f"s3a://{self.bronze_bucket}/{dataset_folder}/part_{group_suffix}/"
                    
                    # Build table info for preview
                    tables_preview = []
                    for table_id in sub_tables:
                        table_info = table_metadata[table_id]
                        cols = resolved_columns.get(table_id, [])
                        tables_preview.append({
                            'table_id': table_id,
                            'table_name': table_info['table_name'],
                            'schema_name': table_info['schema_name'],
                            'selected_columns': [c['column_name'] for c in cols],
                            'column_count': len(cols)
                        })
                    
                    ingestion_groups.append(IngestionGroupPreview(
                        group_name=f"part_{group_suffix}",
                        connection_id=conn_id,
                        connection_name=conn_name,
                        tables=tables_preview,
                        has_joins=len(sub_relationships) > 0,
                        estimated_sql=sql,
                        output_path=output_path,
                        column_mappings=column_mappings
                    ))
            
            # Step 5: Process inter-DB relationships (metadata only)
            if dataset_data.inter_db_relationships:
                for rel in dataset_data.inter_db_relationships:
                    left_table = table_metadata.get(rel.left_table_id, {})
                    right_table = table_metadata.get(rel.right_table_id, {})
                    
                    # Get column names
                    left_col = await self._get_column_info(rel.left_column_id)
                    right_col = await self._get_column_info(rel.right_column_id)
                    
                    inter_db_links.append({
                        'left_table': left_table.get('table_name'),
                        'left_column': left_col.get('column_name') if left_col else None,
                        'left_connection': left_table.get('connection_name'),
                        'right_table': right_table.get('table_name'),
                        'right_column': right_col.get('column_name') if right_col else None,
                        'right_connection': right_table.get('connection_name'),
                        'join_strategy': rel.join_strategy.value,
                        'description': rel.description
                    })
        
        # Calculate totals
        total_columns = sum(
            len(resolved_columns.get(t.table_id, [])) 
            for t in dataset_data.tables
        )
        
        return BronzeIngestionPreview(
            dataset_name=dataset_data.name,
            total_tables=len(dataset_data.tables),
            total_columns=total_columns,
            ingestion_groups=ingestion_groups,
            inter_db_links=inter_db_links,
            estimated_output_paths=[g.output_path for g in ingestion_groups],
            warnings=warnings,
            federated_joins_enabled=enable_federated
        )
    
    async def execute_ingestion(
        self,
        dataset_data: DatasetBronzeCreate,
        preview: Optional[BronzeIngestionPreview] = None
    ) -> BronzeIngestionResult:
        """
        Execute the Bronze ingestion plan.
        
        This method:
        1. Creates the Dataset record in the database
        2. Creates the BronzeConfig and IngestionGroups
        3. Executes each group's SQL via Trino
        4. Updates status and returns results
        """
        start_time = datetime.now()
        
        # Generate plan if not provided
        if not preview:
            preview = await self.generate_ingestion_plan(dataset_data)
        
        # Step 1: Create Dataset record
        dataset = Dataset(
            name=dataset_data.name,
            description=dataset_data.description,
            storage_type='copy_to_minio',
            refresh_type='on_demand',
            status='draft',
            version='1.0',
            properties=dataset_data.properties
        )
        self.db.add(dataset)
        await self.db.flush()
        
        # Step 2: Create Bronze Config
        # Use format: {id}-{name} for better organization and uniqueness
        dataset_folder = self._get_dataset_folder_name(dataset_data.name, dataset.id)
        
        bronze_config = DatasetBronzeConfig(
            dataset_id=dataset.id,
            name=dataset_data.name,
            description=dataset_data.description,
            bronze_bucket=self.bronze_bucket,
            bronze_path_prefix=f"{dataset_folder}/",
            output_format=dataset_data.output_format.value,
            partition_columns=dataset_data.partition_columns,
            config_snapshot=dataset_data.dict()
        )
        self.db.add(bronze_config)
        await self.db.flush()
        
        # Step 3: Update preview paths to use actual dataset ID
        # The preview was generated with placeholder {id}, now we update with the real ID
        for group_preview in preview.ingestion_groups:
            # Replace {id} placeholder with actual dataset ID
            group_preview.output_path = group_preview.output_path.replace("{id}", str(dataset.id))
        
        # Step 4: Execute each ingestion group
        group_results = []
        total_rows = 0
        all_success = True
        
        for group_preview in preview.ingestion_groups:
            group_start = datetime.now()
            
            # Create IngestionGroup record
            ingestion_group = DatasetIngestionGroup(
                bronze_config_id=bronze_config.id,
                connection_id=group_preview.connection_id,
                group_name=group_preview.group_name,
                group_order=len(group_results),
                output_path=group_preview.output_path,
                generated_sql=group_preview.estimated_sql,
                status=IngestionStatus.RUNNING
            )
            self.db.add(ingestion_group)
            await self.db.flush()
            
            # Save column mappings for this group
            if group_preview.column_mappings:
                await self._save_column_mappings(
                    ingestion_group.id,
                    group_preview.column_mappings
                )
            
            # Execute the SQL via Trino
            try:
                rows_ingested = await self._execute_trino_ingestion(
                    group_preview.connection_name,
                    group_preview.estimated_sql,
                    group_preview.output_path,
                    dataset_data.output_format.value
                )
                
                # Update group status
                ingestion_group.status = IngestionStatus.SUCCESS
                ingestion_group.rows_ingested = rows_ingested
                ingestion_group.last_execution_time = datetime.now()
                
                group_results.append(IngestionGroupResult(
                    group_name=group_preview.group_name,
                    connection_name=group_preview.connection_name,
                    status=IngestionStatusEnum.SUCCESS,
                    output_path=group_preview.output_path,
                    rows_ingested=rows_ingested,
                    execution_time_seconds=(datetime.now() - group_start).total_seconds()
                ))
                
                total_rows += rows_ingested
                
            except Exception as e:
                logger.error(f"Failed to ingest group {group_preview.group_name}: {str(e)}")
                
                ingestion_group.status = IngestionStatus.FAILED
                ingestion_group.error_message = str(e)
                ingestion_group.last_execution_time = datetime.now()
                
                group_results.append(IngestionGroupResult(
                    group_name=group_preview.group_name,
                    connection_name=group_preview.connection_name,
                    status=IngestionStatusEnum.FAILED,
                    error_message=str(e),
                    execution_time_seconds=(datetime.now() - group_start).total_seconds()
                ))
                
                all_success = False
        
        # Step 5: Copy images to Bronze bucket if there are image columns
        images_copied = 0
        if all_success or any(r.status == IngestionStatusEnum.SUCCESS for r in group_results):
            try:
                images_copied = await self._copy_images_to_bronze(
                    dataset.id,
                    dataset_folder,  # Use dataset_folder (id-name format)
                    dataset_data.tables,
                    self.bronze_bucket
                )
                if images_copied > 0:
                    logger.info(f"Copied {images_copied} images to Bronze bucket")
            except Exception as img_error:
                logger.warning(f"Failed to copy images to Bronze: {img_error}")
                # Don't fail the entire ingestion if image copy fails
        
        # Step 6: Store inter-DB links for Silver layer
        if dataset_data.inter_db_relationships:
            await self._store_inter_source_links(
                bronze_config.id,
                dataset_data.inter_db_relationships,
                preview.ingestion_groups
            )
        
        # Step 7: Update final status
        if all_success:
            bronze_config.last_ingestion_status = IngestionStatus.SUCCESS
            dataset.status = 'active'
            final_status = IngestionStatusEnum.SUCCESS
            message = f"Bronze ingestion completed successfully. {total_rows} rows ingested."
        elif any(r.status == IngestionStatusEnum.SUCCESS for r in group_results):
            bronze_config.last_ingestion_status = IngestionStatus.PARTIAL
            dataset.status = 'active'
            final_status = IngestionStatusEnum.PARTIAL
            message = f"Bronze ingestion partially completed. {total_rows} rows ingested. Some groups failed."
        else:
            bronze_config.last_ingestion_status = IngestionStatus.FAILED
            dataset.status = 'draft'
            final_status = IngestionStatusEnum.FAILED
            message = "Bronze ingestion failed. No data was ingested."
        
        bronze_config.last_ingestion_time = datetime.now()
        
        await self.db.commit()
        
        return BronzeIngestionResult(
            dataset_id=dataset.id,
            dataset_name=dataset_data.name,
            status=final_status,
            groups=group_results,
            total_rows_ingested=total_rows,
            total_execution_time_seconds=(datetime.now() - start_time).total_seconds(),
            bronze_paths=[g.output_path for g in group_results if g.status == IngestionStatusEnum.SUCCESS],
            message=message
        )
    
    # ==================== PRIVATE METHODS ====================
    
    async def _fetch_table_metadata(
        self, 
        tables: List[TableColumnSelection]
    ) -> Dict[int, Dict[str, Any]]:
        """Fetch metadata for all selected tables"""
        table_ids = [t.table_id for t in tables]
        
        query = select(
            ExternalTables.id,
            ExternalTables.table_name,
            ExternalTables.connection_id,
            ExternalSchema.schema_name,
            DataConnection.name.label('connection_name')
        ).join(
            ExternalSchema, ExternalTables.schema_id == ExternalSchema.id
        ).join(
            DataConnection, ExternalTables.connection_id == DataConnection.id
        ).where(
            ExternalTables.id.in_(table_ids)
        )
        
        result = await self.db.execute(query)
        rows = result.fetchall()
        
        return {
            row.id: {
                'table_id': row.id,
                'table_name': row.table_name,
                'schema_name': row.schema_name,
                'connection_id': row.connection_id,
                'connection_name': row.connection_name
            }
            for row in rows
        }
    
    async def _save_column_mappings(
        self,
        ingestion_group_id: int,
        column_mappings: List[Dict[str, Any]]
    ) -> None:
        """
        Save column mappings for an ingestion group.
        
        This persists the mapping between external_column.id and the
        actual column name in the Bronze Delta Lake output.
        
        This enables the Silver layer to:
        1. Use ColumnGroup from Equivalence (which references external_column.id)
        2. Resolve to actual Bronze column names for transformations
        """
        for mapping in column_mappings:
            if not mapping.get('external_column_id'):
                continue  # Skip if no column ID
            
            bronze_mapping = BronzeColumnMapping(
                ingestion_group_id=ingestion_group_id,
                external_column_id=mapping['external_column_id'],
                external_table_id=mapping['external_table_id'],
                original_column_name=mapping['original_column_name'],
                original_table_name=mapping['original_table_name'],
                original_schema_name=mapping.get('original_schema_name'),
                bronze_column_name=mapping['bronze_column_name'],
                data_type=mapping.get('data_type'),
                column_position=mapping.get('column_position'),
                is_prefixed=mapping.get('is_prefixed', False)
            )
            self.db.add(bronze_mapping)
        
        await self.db.flush()
        logger.info(f"Saved {len(column_mappings)} column mappings for ingestion group {ingestion_group_id}")
    
    def _group_tables_by_connection(
        self,
        table_metadata: Dict[int, Dict[str, Any]],
        intra_db_relationships: List[IntraDBRelationship]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Group tables by their connection ID.
        
        Also associates intra-DB relationships with their respective groups.
        """
        groups = defaultdict(lambda: {
            'connection_name': None,
            'tables': [],
            'relationships': []
        })
        
        # Group tables
        for table_id, metadata in table_metadata.items():
            conn_id = metadata['connection_id']
            groups[conn_id]['connection_name'] = metadata['connection_name']
            groups[conn_id]['tables'].append(table_id)
        
        # Associate relationships with groups
        for rel in intra_db_relationships:
            left_conn = table_metadata.get(rel.left_table_id, {}).get('connection_id')
            right_conn = table_metadata.get(rel.right_table_id, {}).get('connection_id')
            
            if left_conn and left_conn == right_conn:
                groups[left_conn]['relationships'].append(rel)
            else:
                logger.warning(
                    f"Intra-DB relationship between tables {rel.left_table_id} and {rel.right_table_id} "
                    f"spans different connections. This should be an inter-DB relationship."
                )
        
        return dict(groups)
    
    def _split_tables_by_relationships(
        self,
        table_ids: List[int],
        relationships: List[IntraDBRelationship],
        table_metadata: Dict[int, Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Split tables into sub-groups based on relationships.
        
        Tables that are connected via relationships go together.
        Tables without relationships become their own separate groups.
        
        Returns a list of sub-groups, each with 'tables' and 'relationships'.
        """
        if not relationships:
            # No relationships - each table is its own group
            return [
                {'tables': [table_id], 'relationships': []}
                for table_id in table_ids
            ]
        
        # Build a graph of connected tables
        from collections import defaultdict
        
        # Create adjacency list
        connected = defaultdict(set)
        rel_by_tables = defaultdict(list)
        
        for rel in relationships:
            left_id = rel.left_table_id
            right_id = rel.right_table_id
            
            if left_id in table_ids and right_id in table_ids:
                connected[left_id].add(right_id)
                connected[right_id].add(left_id)
                # Store relationship for both directions
                rel_by_tables[(left_id, right_id)].append(rel)
                rel_by_tables[(right_id, left_id)].append(rel)
        
        # Find connected components using BFS
        visited = set()
        sub_groups = []
        
        for table_id in table_ids:
            if table_id in visited:
                continue
            
            # BFS to find all connected tables
            component_tables = []
            component_rels = []
            queue = [table_id]
            
            while queue:
                current = queue.pop(0)
                if current in visited:
                    continue
                
                visited.add(current)
                component_tables.append(current)
                
                for neighbor in connected[current]:
                    if neighbor not in visited:
                        queue.append(neighbor)
                        # Add the relationship
                        for rel in rel_by_tables[(current, neighbor)]:
                            if rel not in component_rels:
                                component_rels.append(rel)
            
            sub_groups.append({
                'tables': component_tables,
                'relationships': component_rels
            })
        
        return sub_groups
    
    async def _resolve_column_selections(
        self,
        tables: List[TableColumnSelection],
        table_metadata: Dict[int, Dict[str, Any]]
    ) -> Dict[int, List[Dict[str, Any]]]:
        """
        Resolve column selections, expanding select_all to actual columns.
        """
        resolved = {}
        
        for table_sel in tables:
            table_id = table_sel.table_id
            
            if table_sel.select_all:
                # Fetch all columns for this table
                query = select(
                    ExternalColumn.id,
                    ExternalColumn.column_name,
                    ExternalColumn.data_type,
                    ExternalColumn.column_position
                ).where(
                    ExternalColumn.table_id == table_id
                ).order_by(
                    ExternalColumn.column_position
                )
                
                result = await self.db.execute(query)
                columns = result.fetchall()
                
                resolved[table_id] = [
                    {
                        'column_id': col.id,
                        'column_name': col.column_name,
                        'data_type': col.data_type,
                        'position': col.column_position
                    }
                    for col in columns
                ]
            else:
                # Fetch only selected columns
                query = select(
                    ExternalColumn.id,
                    ExternalColumn.column_name,
                    ExternalColumn.data_type,
                    ExternalColumn.column_position
                ).where(
                    and_(
                        ExternalColumn.table_id == table_id,
                        ExternalColumn.id.in_(table_sel.column_ids or [])
                    )
                ).order_by(
                    ExternalColumn.column_position
                )
                
                result = await self.db.execute(query)
                columns = result.fetchall()
                
                resolved[table_id] = [
                    {
                        'column_id': col.id,
                        'column_name': col.column_name,
                        'data_type': col.data_type,
                        'position': col.column_position
                    }
                    for col in columns
                ]
        
        return resolved
    
    async def _get_column_info(self, column_id: int) -> Optional[Dict[str, Any]]:
        """Get column information by ID"""
        query = select(
            ExternalColumn.id,
            ExternalColumn.column_name,
            ExternalColumn.data_type,
            ExternalColumn.table_id
        ).where(
            ExternalColumn.id == column_id
        )
        
        result = await self.db.execute(query)
        col = result.first()
        
        if col:
            return {
                'column_id': col.id,
                'column_name': col.column_name,
                'data_type': col.data_type,
                'table_id': col.table_id
            }
        return None
    
    def _convert_to_delta_compatible_type(self, column_expr: str, data_type: str) -> str:
        """
        Convert column expression to Delta Lake compatible type.
        
        Delta Lake has limitations on supported types:
        - No TIMESTAMP WITH TIME ZONE
        - No JSON (must be VARCHAR)
        - No INTERVAL types
        - Limited precision on some types
        
        This method wraps the column expression with appropriate CAST if needed.
        """
        if not data_type:
            return column_expr
        
        data_type_lower = data_type.lower()
        
        # Handle JSON type -> convert to VARCHAR
        # Delta Lake doesn't support JSON natively, store as text
        # Use JSON_FORMAT() for proper serialization (works with MySQL JSON type)
        if data_type_lower == 'json' or data_type_lower == 'jsonb':
            logger.debug(f"Converting {data_type} to VARCHAR using JSON_FORMAT for Delta Lake compatibility")
            return f"JSON_FORMAT({column_expr})"
        
        # Handle timestamp with time zone -> convert to timestamp (without timezone)
        if 'timestamp' in data_type_lower and 'time zone' in data_type_lower:
            logger.debug(f"Converting {data_type} to TIMESTAMP for Delta Lake compatibility")
            return f"CAST({column_expr} AS TIMESTAMP)"
        
        # Handle timestamp with precision -> remove precision for Delta Lake
        if 'timestamp(' in data_type_lower:
            logger.debug(f"Converting {data_type} to TIMESTAMP for Delta Lake compatibility")
            return f"CAST({column_expr} AS TIMESTAMP)"
        
        # Handle time with time zone
        if 'time' in data_type_lower and 'time zone' in data_type_lower:
            logger.debug(f"Converting {data_type} to TIME for Delta Lake compatibility")
            return f"CAST({column_expr} AS TIME)"
        
        # Handle interval types (not supported by Delta Lake)
        if 'interval' in data_type_lower:
            logger.debug(f"Converting {data_type} to VARCHAR for Delta Lake compatibility")
            return f"CAST({column_expr} AS VARCHAR)"
        
        # Handle UUID (some connectors return this, Delta Lake prefers VARCHAR)
        if data_type_lower == 'uuid':
            logger.debug(f"Converting UUID to VARCHAR for Delta Lake compatibility")
            return f"CAST({column_expr} AS VARCHAR)"
        
        # Handle XML type (not supported by Delta Lake)
        if data_type_lower == 'xml':
            logger.debug(f"Converting XML to VARCHAR for Delta Lake compatibility")
            return f"CAST({column_expr} AS VARCHAR)"
        
        # Handle PostgreSQL-specific types
        if data_type_lower in ['money', 'cidr', 'inet', 'macaddr', 'bit', 'varbit', 'box', 'circle', 'line', 'lseg', 'path', 'point', 'polygon']:
            logger.debug(f"Converting PostgreSQL type {data_type} to VARCHAR for Delta Lake compatibility")
            return f"CAST({column_expr} AS VARCHAR)"
        
        # Handle MySQL-specific types
        if data_type_lower in ['enum', 'set', 'geometry', 'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection']:
            logger.debug(f"Converting MySQL type {data_type} to VARCHAR for Delta Lake compatibility")
            return f"CAST({column_expr} AS VARCHAR)"
        
        # No conversion needed
        return column_expr
    
    async def _generate_group_sql(
        self,
        connection_id: int,
        connection_name: str,
        table_ids: List[int],
        relationships: List[IntraDBRelationship],
        resolved_columns: Dict[int, List[Dict[str, Any]]],
        table_metadata: Dict[int, Dict[str, Any]]
    ) -> Tuple[str, List[str], List[Dict[str, Any]]]:
        """
        Generate SQL for an ingestion group.
        
        Handles:
        - Single table (simple SELECT)
        - Multiple tables with joins (pushdown JOIN)
        - Column aliasing for conflict resolution
        
        Returns:
            Tuple of (sql, column_names, column_mappings)
            - sql: The generated SQL query
            - column_names: List of output column names
            - column_mappings: List of dicts with mapping info for BronzeColumnMapping
        """
        # Use the same catalog name format as TrinoManager (with connection_id suffix)
        catalog = self.trino.generate_catalog_name(connection_name, connection_id)
        
        # Build table aliases
        table_aliases = {}
        for i, table_id in enumerate(table_ids):
            table_aliases[table_id] = f"t{i}"
        
        # Determine primary table (first table or the one with most relationships)
        if relationships:
            # Find the table that appears most often on the left side of joins
            left_counts = defaultdict(int)
            for rel in relationships:
                left_counts[rel.left_table_id] += 1
            primary_table_id = max(table_ids, key=lambda t: left_counts.get(t, 0))
        else:
            primary_table_id = table_ids[0]
        
        # Build SELECT columns with aliasing for conflicts
        select_columns = []
        all_column_names = set()
        column_name_counts = defaultdict(int)
        column_mappings = []  # Track column mappings for persistence
        
        # First pass: count column name occurrences
        for table_id in table_ids:
            for col in resolved_columns.get(table_id, []):
                column_name_counts[col['column_name']] += 1
        
        # Second pass: build SELECT with aliases where needed
        column_position = 0
        for table_id in table_ids:
            alias = table_aliases[table_id]
            table_info = table_metadata[table_id]
            
            for col in resolved_columns.get(table_id, []):
                col_name = col['column_name']
                col_data_type = col.get('data_type', '')
                is_prefixed = False
                
                # If column name appears in multiple tables, prefix with table name
                if column_name_counts[col_name] > 1:
                    output_name = f"{table_info['table_name']}_{col_name}"
                    is_prefixed = True
                else:
                    output_name = col_name
                
                # Convert to Delta Lake compatible type if needed
                column_expr = f'{alias}."{col_name}"'
                column_expr = self._convert_to_delta_compatible_type(column_expr, col_data_type)
                
                select_columns.append(f'{column_expr} AS "{output_name}"')
                all_column_names.add(output_name)
                
                # Track the column mapping
                column_mappings.append({
                    'external_column_id': col.get('column_id'),
                    'external_table_id': table_id,
                    'original_column_name': col_name,
                    'original_table_name': table_info['table_name'],
                    'original_schema_name': table_info.get('schema_name'),
                    'bronze_column_name': output_name,
                    'data_type': col_data_type,
                    'column_position': column_position,
                    'is_prefixed': is_prefixed
                })
                column_position += 1
        
        # Add metadata columns
        select_columns.append(f"'{table_metadata[primary_table_id]['table_name']}' AS _source_table")
        select_columns.append("CURRENT_TIMESTAMP AS _ingestion_timestamp")
        
        # Build FROM clause
        primary_table = table_metadata[primary_table_id]
        from_clause = f'"{catalog}"."{primary_table["schema_name"]}"."{primary_table["table_name"]}" {table_aliases[primary_table_id]}'
        
        # Build JOIN clauses
        join_clauses = []
        joined_tables = {primary_table_id}
        
        for rel in relationships:
            # Determine which table to join
            if rel.left_table_id in joined_tables and rel.right_table_id not in joined_tables:
                join_table_id = rel.right_table_id
                left_alias = table_aliases[rel.left_table_id]
                right_alias = table_aliases[rel.right_table_id]
            elif rel.right_table_id in joined_tables and rel.left_table_id not in joined_tables:
                join_table_id = rel.left_table_id
                left_alias = table_aliases[rel.right_table_id]
                right_alias = table_aliases[rel.left_table_id]
            else:
                continue  # Both already joined or neither available
            
            join_table = table_metadata[join_table_id]
            
            # Get column names for the join condition
            left_col = await self._get_column_info(rel.left_column_id)
            right_col = await self._get_column_info(rel.right_column_id)
            
            if not left_col or not right_col:
                logger.warning(f"Could not resolve columns for relationship")
                continue
            
            # Build join type
            join_type_map = {
                'inner': 'INNER JOIN',
                'left': 'LEFT JOIN',
                'right': 'RIGHT JOIN',
                'full': 'FULL OUTER JOIN'
            }
            join_type = join_type_map.get(rel.join_strategy.value, 'INNER JOIN')
            
            # Build join condition
            if rel.custom_condition:
                condition = rel.custom_condition
            else:
                condition = f'{table_aliases[rel.left_table_id]}."{left_col["column_name"]}" = {table_aliases[rel.right_table_id]}."{right_col["column_name"]}"'
            
            join_clause = f'{join_type} "{catalog}"."{join_table["schema_name"]}"."{join_table["table_name"]}" {right_alias} ON {condition}'
            join_clauses.append(join_clause)
            joined_tables.add(join_table_id)
        
        # Note: Tables without relationships are now handled by _split_tables_by_relationships
        # They become their own separate groups, so we don't need to handle them here
        
        # Build final SQL
        sql = f"""SELECT 
    {', '.join(select_columns)}
FROM {from_clause}
{chr(10).join(join_clauses)}"""
        
        return sql.strip(), list(all_column_names), column_mappings
    
    async def _generate_federated_sql(
        self,
        table_ids: List[int],
        relationships: List[IntraDBRelationship],
        resolved_columns: Dict[int, List[Dict[str, Any]]],
        table_metadata: Dict[int, Dict[str, Any]]
    ) -> Tuple[str, List[str], List[Dict[str, Any]]]:
        """
        Generate federated SQL that JOINs tables across different databases using Trino.
        
        This is used when enable_federated_joins=True.
        Unlike _generate_group_sql which only handles tables from the same connection,
        this method can JOIN tables from different catalogs/connections.
        
        Handles:
        - Multiple catalogs (different databases)
        - Cross-database JOINs via Trino
        - Column aliasing for conflict resolution
        
        Returns:
            Tuple of (sql, column_names, column_mappings)
        """
        # Build table aliases
        table_aliases = {}
        for i, table_id in enumerate(table_ids):
            table_aliases[table_id] = f"t{i}"
        
        # Determine primary table (first table or the one with most relationships)
        if relationships:
            # Find the table that appears most often on the left side of joins
            left_counts = defaultdict(int)
            for rel in relationships:
                left_counts[rel.left_table_id] += 1
            primary_table_id = max(table_ids, key=lambda t: left_counts.get(t, 0))
        else:
            primary_table_id = table_ids[0]
        
        # Build SELECT columns with aliasing for conflicts
        select_columns = []
        all_column_names = set()
        column_name_counts = defaultdict(int)
        column_mappings = []  # Track column mappings for persistence
        
        # First pass: count column name occurrences
        for table_id in table_ids:
            for col in resolved_columns.get(table_id, []):
                column_name_counts[col['column_name']] += 1
        
        # Second pass: build SELECT with aliases where needed
        column_position = 0
        for table_id in table_ids:
            alias = table_aliases[table_id]
            table_info = table_metadata[table_id]
            
            for col in resolved_columns.get(table_id, []):
                col_name = col['column_name']
                col_data_type = col.get('data_type', '')
                is_prefixed = False
                
                # If column name appears in multiple tables, prefix with table name
                if column_name_counts[col_name] > 1:
                    output_name = f"{table_info['table_name']}_{col_name}"
                    is_prefixed = True
                else:
                    output_name = col_name
                
                # Convert to Delta Lake compatible type if needed
                column_expr = f'{alias}."{col_name}"'
                column_expr = self._convert_to_delta_compatible_type(column_expr, col_data_type)
                
                select_columns.append(f'{column_expr} AS "{output_name}"')
                all_column_names.add(output_name)
                
                # Track the column mapping
                column_mappings.append({
                    'external_column_id': col.get('column_id'),
                    'external_table_id': table_id,
                    'original_column_name': col_name,
                    'original_table_name': table_info['table_name'],
                    'original_schema_name': table_info.get('schema_name'),
                    'bronze_column_name': output_name,
                    'data_type': col_data_type,
                    'column_position': column_position,
                    'is_prefixed': is_prefixed
                })
                column_position += 1
        
        # Add metadata columns
        select_columns.append("'federated' AS _source_table")
        select_columns.append("CURRENT_TIMESTAMP AS _ingestion_timestamp")
        
        # Build FROM clause with FULL CATALOG PATH
        primary_table = table_metadata[primary_table_id]
        # Use the same catalog name format as TrinoManager (with connection_id suffix)
        primary_catalog = self.trino.generate_catalog_name(primary_table['connection_name'], primary_table['connection_id'])
        from_clause = f'"{primary_catalog}"."{primary_table["schema_name"]}"."{primary_table["table_name"]}" {table_aliases[primary_table_id]}'
        
        # Build JOIN clauses (cross-database)
        join_clauses = []
        joined_tables = {primary_table_id}
        
        # Sort relationships to ensure we join in a logical order
        # Try to join tables that are already in the query first
        remaining_rels = list(relationships)
        max_iterations = len(remaining_rels) * 2  # Prevent infinite loop
        iteration = 0
        
        while remaining_rels and iteration < max_iterations:
            iteration += 1
            made_progress = False
            
            for rel in list(remaining_rels):
                # Determine which table to join
                left_in = rel.left_table_id in joined_tables
                right_in = rel.right_table_id in joined_tables
                
                if left_in and not right_in:
                    join_table_id = rel.right_table_id
                    left_table_id = rel.left_table_id
                    right_table_id = rel.right_table_id
                elif right_in and not left_in:
                    join_table_id = rel.left_table_id
                    left_table_id = rel.right_table_id
                    right_table_id = rel.left_table_id
                elif left_in and right_in:
                    # Both already joined, skip
                    remaining_rels.remove(rel)
                    made_progress = True
                    continue
                else:
                    # Neither joined yet, skip for now
                    continue
                
                join_table = table_metadata[join_table_id]
                # Use the same catalog name format as TrinoManager (with connection_id suffix)
                join_catalog = self.trino.generate_catalog_name(join_table['connection_name'], join_table['connection_id'])
                
                # Get column names for the join condition
                left_col = await self._get_column_info(rel.left_column_id)
                right_col = await self._get_column_info(rel.right_column_id)
                
                if not left_col or not right_col:
                    logger.warning(f"Could not resolve columns for relationship")
                    remaining_rels.remove(rel)
                    made_progress = True
                    continue
                
                # Build join type
                join_type_map = {
                    'inner': 'INNER JOIN',
                    'left': 'LEFT JOIN',
                    'right': 'RIGHT JOIN',
                    'full': 'FULL OUTER JOIN'
                }
                join_type = join_type_map.get(rel.join_strategy.value, 'INNER JOIN')
                
                # Build join condition
                if rel.custom_condition:
                    condition = rel.custom_condition
                else:
                    # Use the original relationship direction for the condition
                    condition = f'{table_aliases[rel.left_table_id]}."{left_col["column_name"]}" = {table_aliases[rel.right_table_id]}."{right_col["column_name"]}"'
                
                # Build join clause with FULL CATALOG PATH (cross-database!)
                join_clause = f'{join_type} "{join_catalog}"."{join_table["schema_name"]}"."{join_table["table_name"]}" {table_aliases[join_table_id]} ON {condition}'
                join_clauses.append(join_clause)
                joined_tables.add(join_table_id)
                
                remaining_rels.remove(rel)
                made_progress = True
            
            if not made_progress:
                # No progress made in this iteration, break to avoid infinite loop
                if remaining_rels:
                    logger.warning(f"Could not apply {len(remaining_rels)} relationships - they may form disconnected components")
                break
        
        # Handle tables that couldn't be joined (disconnected components)
        unjoined_tables = set(table_ids) - joined_tables
        if unjoined_tables:
            logger.warning(
                f"Tables {unjoined_tables} could not be joined - they have no relationships to the main query. "
                f"Consider using standard Bronze mode instead of federated joins."
            )
        
        # Build final SQL
        sql = f"""SELECT 
    {', '.join(select_columns)}
FROM {from_clause}
{chr(10).join(join_clauses)}"""
        
        return sql.strip(), list(all_column_names), column_mappings
    
    async def _execute_trino_ingestion(
        self,
        connection_name: str,
        select_sql: str,
        output_path: str,
        output_format: str
    ) -> int:
        """
        Execute the ingestion SQL via Trino.
        
        Creates a table in the bronze catalog pointing to MinIO.
        """
        # Extract table name from output path
        # e.g., "s3a://datafabric-bronze/my_dataset/part_postgres/" -> "my_dataset_part_postgres"
        path_parts = output_path.rstrip('/').split('/')
        table_name = '_'.join(path_parts[-2:])  # dataset_name + part_name
        table_name = self.trino._sanitize_identifier(table_name)
        
        # Build CREATE TABLE statement
        # Note: Delta Lake connector only supports 'location' property
        # The data is always stored in Delta format (Parquet + transaction log)
        create_sql = f"""
CREATE TABLE IF NOT EXISTS "{self.bronze_catalog}"."{self.bronze_schema}"."{table_name}"
WITH (
    location = '{output_path}'
)
AS
{select_sql}
"""
        
        logger.info(f"Executing Bronze ingestion SQL:\n{create_sql}")
        
        try:
            conn = self.trino.get_connection()
            cur = conn.cursor()
            cur.execute(create_sql)
            
            # Get row count
            cur.execute(f'SELECT COUNT(*) FROM "{self.bronze_catalog}"."{self.bronze_schema}"."{table_name}"')
            result = cur.fetchone()
            rows_ingested = result[0] if result else 0
            
            conn.close()
            return rows_ingested
            
        except Exception as e:
            logger.error(f"Trino ingestion failed: {str(e)}")
            raise
    
    async def _store_inter_source_links(
        self,
        bronze_config_id: int,
        inter_db_relationships: List[InterDBRelationship],
        ingestion_groups: List[IngestionGroupPreview]
    ):
        """
        Store inter-source links for Silver layer processing.
        """
        # Create a mapping from connection_id to group_id
        # Note: We need to fetch the actual IngestionGroup IDs from the database
        
        group_query = select(
            DatasetIngestionGroup.id,
            DatasetIngestionGroup.connection_id,
            DatasetIngestionGroup.group_name
        ).where(
            DatasetIngestionGroup.bronze_config_id == bronze_config_id
        )
        
        result = await self.db.execute(group_query)
        groups = {row.connection_id: row.id for row in result.fetchall()}
        
        # For each inter-DB relationship, create an InterSourceLink
        for rel in inter_db_relationships:
            # Get connection IDs for the tables
            left_table_query = select(ExternalTables.connection_id).where(ExternalTables.id == rel.left_table_id)
            right_table_query = select(ExternalTables.connection_id).where(ExternalTables.id == rel.right_table_id)
            
            left_result = await self.db.execute(left_table_query)
            right_result = await self.db.execute(right_table_query)
            
            left_conn = left_result.scalar()
            right_conn = right_result.scalar()
            
            if left_conn and right_conn and left_conn in groups and right_conn in groups:
                # Get column names
                left_col = await self._get_column_info(rel.left_column_id)
                right_col = await self._get_column_info(rel.right_column_id)
                
                if left_col and right_col:
                    link = InterSourceLink(
                        bronze_config_id=bronze_config_id,
                        left_group_id=groups[left_conn],
                        left_column_name=left_col['column_name'],
                        right_group_id=groups[right_conn],
                        right_column_name=right_col['column_name'],
                        join_strategy=JoinStrategy(rel.join_strategy.value),
                        description=rel.description
                    )
                    self.db.add(link)
        
        await self.db.flush()

    async def _copy_images_to_bronze(
        self,
        dataset_id: int,
        dataset_folder: str,
        tables: List[TableColumnSelection],
        bronze_bucket: str
    ) -> int:
        """
        Copy images from external MinIO to Bronze bucket.
        
        Images are organized within the dataset folder:
        s3://bronze-bucket/{id}-{name}/images/{filename}
        
        Example: s3://datafabric-bronze/15-melanoma_study/images/ISIC_7874486.jpg
        
        This keeps all dataset data (metadata + images) together in one place.
        
        This method:
        1. Identifies image columns in the selected tables
        2. Reads the Bronze Delta tables to get image paths
        3. Downloads images from external MinIO
        4. Uploads images to Bronze bucket under dataset folder
        5. Updates Delta tables with new image paths
        
        Args:
            dataset_id: ID of the dataset
            dataset_folder: Folder name in format "{id}-{name}"
            tables: List of table selections
            bronze_bucket: Bronze bucket name
        
        Returns:
            Number of images successfully copied
        """
        try:
            from ..services.image_service import ImageService
            from minio import Minio
            
            # Get all table IDs
            table_ids = [t.table_id for t in tables]
            
            # Find image columns in these tables
            image_columns_query = select(
                ExternalColumn.id,
                ExternalColumn.column_name,
                ExternalColumn.table_id,
                ExternalColumn.image_connection_id,
                ExternalTables.table_name
            ).join(
                ExternalTables, ExternalColumn.table_id == ExternalTables.id
            ).where(
                and_(
                    ExternalColumn.table_id.in_(table_ids),
                    ExternalColumn.is_image_path == True,
                    ExternalColumn.image_connection_id.isnot(None)
                )
            )
            
            result = await self.db.execute(image_columns_query)
            image_columns = result.fetchall()
            
            if not image_columns:
                logger.info("No image columns found in selected tables")
                return 0
            
            logger.info(f"Found {len(image_columns)} image columns to process")
            
            # Initialize ImageService
            image_service = ImageService(self.db)
            
            # Create MinIO client for Bronze bucket
            bronze_minio = Minio(
                endpoint=os.getenv("INTERNAL_S3_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", ""),
                access_key=os.getenv("INTERNAL_S3_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minio")),
                secret_key=os.getenv("INTERNAL_S3_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio123")),
                secure=False
            )
            
            # Ensure Bronze bucket exists
            if not bronze_minio.bucket_exists(bronze_bucket):
                bronze_minio.make_bucket(bronze_bucket)
                logger.info(f"Created Bronze bucket: {bronze_bucket}")
            
            total_images_copied = 0
            
            # Process each image column
            for img_col in image_columns:
                try:
                    column_name = img_col.column_name
                    table_name = img_col.table_name
                    image_connection_id = img_col.image_connection_id
                    
                    logger.info(f"Processing image column: {table_name}.{column_name}")
                    
                    # Read Bronze Delta table using Trino
                    # Find all tables in Bronze schema that might contain this data
                    conn = self.trino.get_connection()
                    cur = conn.cursor()
                    
                    # List all tables in Bronze schema
                    try:
                        cur.execute(f'SHOW TABLES FROM "{self.bronze_catalog}"."{self.bronze_schema}"')
                        bronze_tables = [row[0] for row in cur.fetchall()]
                        logger.debug(f"Found {len(bronze_tables)} tables in Bronze schema")
                    except Exception as list_error:
                        logger.warning(f"Could not list Bronze tables: {list_error}")
                        conn.close()
                        continue
                    
                    # Try to find the table containing this column
                    image_paths = []
                    for bronze_table in bronze_tables:
                        try:
                            # Check if this table has the image column
                            query = f"""
                                SELECT DISTINCT "{column_name}"
                                FROM "{self.bronze_catalog}"."{self.bronze_schema}"."{bronze_table}"
                                WHERE "{column_name}" IS NOT NULL
                                LIMIT 1000
                            """
                            cur.execute(query)
                            paths = cur.fetchall()
                            if paths:
                                image_paths.extend(paths)
                                logger.info(f"Found {len(paths)} image paths in table {bronze_table}")
                                break  # Found the right table
                        except Exception as query_error:
                            # Column doesn't exist in this table, try next
                            continue
                    
                    conn.close()
                    
                    if not image_paths:
                        logger.info(f"No image paths found in {table_name}.{column_name}")
                        continue
                    
                    # Get source MinIO client
                    source_minio = await image_service._get_minio_client(image_connection_id)
                    if not source_minio:
                        logger.error(f"Could not create MinIO client for connection {image_connection_id}")
                        continue
                    
                    # Copy each unique image
                    for image_path_row in image_paths:
                        image_path = image_path_row[0]
                        
                        try:
                            # Parse S3 path
                            if image_path.startswith('s3://'):
                                path_without_protocol = image_path[5:]
                                path_parts = path_without_protocol.split('/', 1)
                                source_bucket = path_parts[0]
                                source_key = path_parts[1] if len(path_parts) > 1 else ""
                            else:
                                # Get bucket from connection
                                source_bucket = await image_service._get_bucket_from_connection(image_connection_id)
                                source_key = image_path.strip('/')
                            
                            if not source_bucket or not source_key:
                                logger.warning(f"Invalid image path: {image_path}")
                                continue
                            
                            # Download from source
                            response = source_minio.get_object(source_bucket, source_key)
                            image_data = response.read()
                            response.close()
                            response.release_conn()
                            
                            # Generate destination path
                            # Format: bronze-bucket/{id}-{name}/images/{filename}
                            # This keeps images organized within the same dataset folder as metadata
                            filename = os.path.basename(source_key)
                            dest_key = f"{dataset_folder}/images/{filename}"
                            
                            # Upload to Bronze bucket
                            from io import BytesIO
                            bronze_minio.put_object(
                                bronze_bucket,
                                dest_key,
                                BytesIO(image_data),
                                length=len(image_data),
                                content_type=image_service._get_mime_type(filename.lower().split('.')[-1])
                            )
                            
                            total_images_copied += 1
                            
                            if total_images_copied % 10 == 0:
                                logger.info(f"Copied {total_images_copied} images so far...")
                        
                        except Exception as copy_error:
                            logger.warning(f"Failed to copy image {image_path}: {copy_error}")
                            continue
                
                except Exception as col_error:
                    logger.error(f"Error processing image column: {col_error}")
                    continue
            
            logger.info(f"Successfully copied {total_images_copied} images to Bronze bucket")
            return total_images_copied
        
        except Exception as e:
            logger.error(f"Error copying images to Bronze: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return 0
    
    # ==================== PRE-DEFINED RELATIONSHIPS METHODS ====================
    
    async def get_applicable_relationships(
        self,
        table_ids: List[int],
        relationship_ids: Optional[List[int]] = None
    ) -> Tuple[List[TableRelationship], List[TableRelationship]]:
        """
        Get pre-defined relationships that apply to the selected tables.
        
        Returns two lists:
        - intra_connection: Relationships within the same connection (for pushdown)
        - inter_connection: Relationships across connections (for Silver layer)
        """
        from sqlalchemy import or_
        
        # Build query for relationships involving the selected tables
        query = select(TableRelationship).where(
            and_(
                TableRelationship.is_active == True,
                or_(
                    TableRelationship.left_table_id.in_(table_ids),
                    TableRelationship.right_table_id.in_(table_ids)
                )
            )
        )
        
        # Filter by specific relationship IDs if provided
        if relationship_ids:
            query = query.where(TableRelationship.id.in_(relationship_ids))
        
        result = await self.db.execute(query)
        relationships = result.scalars().all()
        
        # Filter to only include relationships where BOTH tables are selected
        table_id_set = set(table_ids)
        applicable = [
            r for r in relationships 
            if r.left_table_id in table_id_set and r.right_table_id in table_id_set
        ]
        
        # Separate by scope
        intra_connection = [r for r in applicable if r.scope == RelationshipScope.INTRA_CONNECTION]
        inter_connection = [r for r in applicable if r.scope == RelationshipScope.INTER_CONNECTION]
        
        return intra_connection, inter_connection
    
    async def generate_ingestion_plan_simplified(
        self, 
        dataset_data: DatasetBronzeCreateRequest
    ) -> BronzeIngestionPreview:
        """
        Generate an ingestion plan using pre-defined relationships from metadata.
        
        This method:
        1. Looks up relationships between the selected tables
        2. Separates intra-connection (for Trino pushdown) from inter-connection (for Silver)
        3. Generates optimized SQL for each connection group
        """
        # Get table IDs from selection
        table_ids = [t.table_id for t in dataset_data.tables]
        
        # Fetch applicable relationships from metadata
        intra_rels, inter_rels = await self.get_applicable_relationships(
            table_ids,
            dataset_data.relationship_ids  # Optional filter
        )
        
        # Convert to the format expected by generate_ingestion_plan
        intra_db_relationships = []
        for rel in intra_rels:
            intra_db_relationships.append(IntraDBRelationship(
                left_table_id=rel.left_table_id,
                left_column_id=rel.left_column_id,
                right_table_id=rel.right_table_id,
                right_column_id=rel.right_column_id,
                join_strategy=JoinStrategyEnum(rel.default_join_type.value),
                custom_condition=None
            ))
        
        inter_db_relationships = []
        for rel in inter_rels:
            inter_db_relationships.append(InterDBRelationship(
                left_table_id=rel.left_table_id,
                left_column_id=rel.left_column_id,
                right_table_id=rel.right_table_id,
                right_column_id=rel.right_column_id,
                join_strategy=JoinStrategyEnum(rel.default_join_type.value),
                description=rel.description
            ))
        
        # Create full DatasetBronzeCreate from simplified version
        full_data = DatasetBronzeCreate(
            name=dataset_data.name,
            description=dataset_data.description,
            tables=dataset_data.tables,
            intra_db_relationships=intra_db_relationships if intra_db_relationships else None,
            inter_db_relationships=inter_db_relationships if inter_db_relationships else None,
            output_format=dataset_data.output_format,
            output_bucket=dataset_data.output_bucket,
            partition_columns=dataset_data.partition_columns,
            enable_federated_joins=getattr(dataset_data, 'enable_federated_joins', False),
            properties=dataset_data.properties
        )
        
        # Use the standard method
        preview = await self.generate_ingestion_plan(full_data)
        
        return preview
    
    async def execute_ingestion_simplified(
        self,
        dataset_data: DatasetBronzeCreateRequest
    ) -> BronzeIngestionResult:
        """
        Execute Bronze ingestion using pre-defined relationships from metadata.
        
        This is the main entry point for Bronze ingestion.
        Relationships are fetched from the metadata layer automatically.
        """
        # Generate plan with relationships
        preview = await self.generate_ingestion_plan_simplified(dataset_data)
        
        # Get table IDs and relationships for full data construction
        table_ids = [t.table_id for t in dataset_data.tables]
        intra_rels, inter_rels = await self.get_applicable_relationships(
            table_ids,
            dataset_data.relationship_ids  # Optional filter
        )
        
        # Convert relationships
        intra_db_relationships = [
            IntraDBRelationship(
                left_table_id=rel.left_table_id,
                left_column_id=rel.left_column_id,
                right_table_id=rel.right_table_id,
                right_column_id=rel.right_column_id,
                join_strategy=JoinStrategyEnum(rel.default_join_type.value),
                custom_condition=None
            ) for rel in intra_rels
        ]
        
        inter_db_relationships = [
            InterDBRelationship(
                left_table_id=rel.left_table_id,
                left_column_id=rel.left_column_id,
                right_table_id=rel.right_table_id,
                right_column_id=rel.right_column_id,
                join_strategy=JoinStrategyEnum(rel.default_join_type.value),
                description=rel.description
            ) for rel in inter_rels
        ]
        
        # Create full data
        full_data = DatasetBronzeCreate(
            name=dataset_data.name,
            description=dataset_data.description,
            tables=dataset_data.tables,
            intra_db_relationships=intra_db_relationships if intra_db_relationships else None,
            inter_db_relationships=inter_db_relationships if inter_db_relationships else None,
            output_format=dataset_data.output_format,
            output_bucket=dataset_data.output_bucket,
            partition_columns=dataset_data.partition_columns,
            enable_federated_joins=getattr(dataset_data, 'enable_federated_joins', False),
            properties=dataset_data.properties
        )
        
        # Execute using standard method
        return await self.execute_ingestion(full_data, preview)
    
    async def preview_relationship_usage(
        self,
        table_ids: List[int],
        relationship_ids: Optional[List[int]] = None
    ) -> List[RelationshipUsagePreview]:
        """
        Preview how relationships will be used for the selected tables.
        
        Shows which relationships will be used for pushdown joins
        and which will be stored for Silver layer.
        """
        intra_rels, inter_rels = await self.get_applicable_relationships(
            table_ids,
            relationship_ids
        )
        
        previews = []
        
        for rel in intra_rels:
            # Get column and table names
            left_col = await self._get_column_info(rel.left_column_id)
            right_col = await self._get_column_info(rel.right_column_id)
            
            if left_col and right_col:
                previews.append(RelationshipUsagePreview(
                    relationship_id=rel.id,
                    left_table=left_col.get('table_name', 'unknown'),
                    left_column=left_col.get('column_name', 'unknown'),
                    right_table=right_col.get('table_name', 'unknown'),
                    right_column=right_col.get('column_name', 'unknown'),
                    scope='intra_connection',
                    usage='pushdown_join',
                    join_type=rel.default_join_type.value
                ))
        
        for rel in inter_rels:
            left_col = await self._get_column_info(rel.left_column_id)
            right_col = await self._get_column_info(rel.right_column_id)
            
            if left_col and right_col:
                previews.append(RelationshipUsagePreview(
                    relationship_id=rel.id,
                    left_table=left_col.get('table_name', 'unknown'),
                    left_column=left_col.get('column_name', 'unknown'),
                    right_table=right_col.get('table_name', 'unknown'),
                    right_column=right_col.get('column_name', 'unknown'),
                    scope='inter_connection',
                    usage='silver_layer_link',
                    join_type=rel.default_join_type.value
                ))
        
        return previews

    # ==================== VIRTUALIZED QUERY ====================
    
    async def execute_virtualized(
        self,
        request: BronzeVirtualizedRequest,
        limit: int = 1000,
        offset: int = 0
    ) -> BronzeVirtualizedResponse:
        """
        Execute a virtualized Bronze query without saving to Delta.
        
        This method:
        1. Generates the same SQL as the ingestion plan
        2. Executes the SQL via Trino
        3. Returns the data directly as JSON
        
        Useful for:
        - Data exploration/preview with actual data
        - Light API consumption (small datasets)
        - Testing queries before materializing
        
        Args:
            request: The virtualized query request (tables, columns, relationships)
            limit: Maximum rows per group (default 1000)
            offset: Offset for pagination (default 0)
            
        Returns:
            BronzeVirtualizedResponse with data from each group
        """
        start_time = datetime.now()
        warnings = []
        
        # Convert request to internal format for plan generation
        # We create a "fake" DatasetBronzeCreateRequest just to reuse the plan logic
        dataset_data = DatasetBronzeCreateRequest(
            name="__virtualized_query__",
            tables=request.tables,
            relationship_ids=request.relationship_ids,
            enable_federated_joins=request.enable_federated_joins
        )
        
        # Generate the plan (this gives us the SQL for each group)
        preview = await self.generate_ingestion_plan_simplified(dataset_data)
        
        # Execute each group's SQL and collect results
        group_results = []
        total_rows = 0
        
        for group_preview in preview.ingestion_groups:
            try:
                # Modify SQL to add LIMIT/OFFSET
                select_sql = group_preview.estimated_sql
                if select_sql:
                    # Remove trailing semicolon if present
                    select_sql = select_sql.rstrip(';').strip()
                    
                    # Check if query already has LIMIT (case-insensitive)
                    sql_upper = select_sql.upper()
                    if ' LIMIT ' not in sql_upper:
                        # Add LIMIT (required for OFFSET in Trino)
                        select_sql = f"{select_sql} LIMIT {limit}"
                        # Only add OFFSET if > 0
                        if offset > 0:
                            select_sql = f"{select_sql} OFFSET {offset}"
                
                # Execute the query
                data, columns = await self._execute_virtualized_query(select_sql)
                
                group_results.append(BronzeVirtualizedGroupResult(
                    group_name=group_preview.group_name,
                    connection_name=group_preview.connection_name,
                    columns=columns,
                    data=data,
                    row_count=len(data),
                    sql_executed=select_sql
                ))
                
                total_rows += len(data)
                
            except Exception as e:
                logger.error(f"Failed to execute virtualized query for group {group_preview.group_name}: {str(e)}")
                warnings.append(f"Group {group_preview.group_name} failed: {str(e)}")
                
                # Add empty result for failed group
                group_results.append(BronzeVirtualizedGroupResult(
                    group_name=group_preview.group_name,
                    connection_name=group_preview.connection_name,
                    columns=[],
                    data=[],
                    row_count=0,
                    sql_executed=group_preview.estimated_sql
                ))
        
        # Add warnings from preview
        warnings.extend(preview.warnings)
        
        execution_time = (datetime.now() - start_time).total_seconds()
        
        return BronzeVirtualizedResponse(
            total_tables=preview.total_tables,
            total_columns=preview.total_columns,
            groups=group_results,
            total_rows=total_rows,
            execution_time_seconds=execution_time,
            warnings=warnings,
            message=f"Virtualized query completed. {total_rows} rows returned from {len(group_results)} groups."
        )
    
    async def _execute_virtualized_query(
        self,
        select_sql: str
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """
        Execute a SELECT query via Trino and return the data.
        
        Args:
            select_sql: The SELECT SQL to execute
            
        Returns:
            Tuple of (data as list of dicts, column names)
        """
        try:
            conn = self.trino.get_connection()
            cur = conn.cursor()
            
            logger.info(f"Executing virtualized query:\n{select_sql}")
            cur.execute(select_sql)
            
            # Get column names from cursor description
            columns = [desc[0] for desc in cur.description] if cur.description else []
            
            # Fetch all rows
            rows = cur.fetchall()
            
            # Convert to list of dicts
            data = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    value = row[i]
                    # Convert non-serializable types to string
                    if value is not None and not isinstance(value, (str, int, float, bool, list, dict)):
                        value = str(value)
                    row_dict[col_name] = value
                data.append(row_dict)
            
            conn.close()
            return data, columns
            
        except Exception as e:
            logger.error(f"Virtualized query failed: {str(e)}")
            raise

