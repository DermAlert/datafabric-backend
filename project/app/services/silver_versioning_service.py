"""
Silver Versioning Service

This service handles Delta Lake versioning for Silver layer datasets.
It provides:
1. Auto-detection of merge keys from Bronze source PKs
2. MERGE (upsert) operations for incremental updates without duplicates
3. Time travel queries to access historical versions
4. Version history tracking

This is similar to BronzeVersioningService but tailored for Silver layer.
"""

import os
import hashlib
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_

from ..database.metadata.metadata import ExternalColumn
from ..database.datasets.bronze import BronzeColumnMapping, DatasetBronzeConfig
from ..database.datasets.silver import (
    TransformConfig,
    TransformExecution,
    SilverWriteMode,
)
from ..api.schemas.silver_schemas import (
    SilverVersionInfo,
    SilverVersionHistoryResponse,
)

logger = logging.getLogger(__name__)


class SilverVersioningService:
    """
    Service for managing Delta Lake versioning in Silver layer.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.silver_bucket = os.getenv("SILVER_BUCKET", "datafabric-silver")
    
    async def auto_detect_merge_keys(
        self, 
        bronze_dataset_id: int
    ) -> Tuple[Optional[List[str]], str]:
        """
        Auto-detect merge keys from Bronze source primary keys.
        
        Args:
            bronze_dataset_id: ID of the Bronze dataset
        
        Returns:
            Tuple of (merge_keys, source) where:
            - merge_keys: List of column names to use for merge, or None if not found
            - source: 'auto_detected_pk' if found, None if not
        """
        # Get Bronze config to find the tables
        bronze_config_result = await self.db.execute(
            select(DatasetBronzeConfig).where(
                DatasetBronzeConfig.dataset_id == bronze_dataset_id
            )
        )
        bronze_config = bronze_config_result.scalar_one_or_none()
        
        if not bronze_config:
            logger.warning(f"Bronze config not found for dataset {bronze_dataset_id}")
            return None, None
        
        # Get column mappings for this Bronze config
        from ..database.datasets.bronze import DatasetIngestionGroup
        
        groups_result = await self.db.execute(
            select(DatasetIngestionGroup.id).where(
                DatasetIngestionGroup.bronze_config_id == bronze_config.id
            )
        )
        group_ids = [row[0] for row in groups_result.fetchall()]
        
        if not group_ids:
            logger.warning(f"No ingestion groups found for Bronze config {bronze_config.id}")
            return None, None
        
        # Get PKs from column mappings
        detected_pks = []
        
        mappings_result = await self.db.execute(
            select(BronzeColumnMapping).where(
                BronzeColumnMapping.ingestion_group_id.in_(group_ids)
            )
        )
        mappings = mappings_result.scalars().all()
        
        for mapping in mappings:
            # Check if the external column is a PK
            pk_result = await self.db.execute(
                select(ExternalColumn.is_primary_key).where(
                    ExternalColumn.id == mapping.external_column_id
                )
            )
            is_pk = pk_result.scalar_one_or_none()
            
            if is_pk:
                # Use the bronze column name (which is what Silver sees)
                detected_pks.append(mapping.bronze_column_name)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_pks = []
        for pk in detected_pks:
            if pk.lower() not in seen:
                seen.add(pk.lower())
                unique_pks.append(pk)
        
        if unique_pks:
            logger.info(f"Auto-detected merge_keys from Bronze PKs: {unique_pks}")
            return unique_pks, 'auto_detected_pk'
        
        logger.warning("No primary keys found in Bronze source")
        return None, None
    
    async def resolve_write_mode(
        self,
        write_mode: str,
        merge_keys: Optional[List[str]],
        bronze_dataset_id: int
    ) -> Tuple[str, Optional[List[str]], Optional[str], List[str]]:
        """
        Resolve the effective write mode and merge keys.
        
        If write_mode='merge' and merge_keys not provided:
        - Try to auto-detect from Bronze source PKs
        - If not found, fallback to 'overwrite'
        
        Args:
            write_mode: Requested write mode ('overwrite', 'append', 'merge')
            merge_keys: User-provided merge keys (optional)
            bronze_dataset_id: ID of the Bronze dataset
        
        Returns:
            Tuple of (effective_write_mode, effective_merge_keys, merge_keys_source, warnings)
        """
        warnings = []
        
        # Not merge mode - no merge keys needed
        if write_mode != 'merge':
            return write_mode, None, None, warnings
        
        # User provided merge_keys
        if merge_keys:
            return 'merge', merge_keys, 'user_defined', warnings
        
        # Try to auto-detect from Bronze
        detected_keys, source = await self.auto_detect_merge_keys(bronze_dataset_id)
        
        if detected_keys:
            return 'merge', detected_keys, source, warnings
        
        # No PKs found - fallback to overwrite
        warnings.append(
            "No primary keys found in Bronze source. "
            "Falling back to 'overwrite' mode. "
            "Define merge_keys manually to use 'merge' mode."
        )
        return 'overwrite', None, None, warnings
    
    def calculate_schema_hash(self, config: dict) -> str:
        """
        Calculate a hash of the config for schema change detection.
        
        Args:
            config: Config dictionary
        
        Returns:
            SHA256 hash (first 16 chars)
        """
        # Normalize the config for consistent hashing
        normalized = {
            'source_bronze_dataset_id': config.get('source_bronze_dataset_id'),
            'column_group_ids': sorted(config.get('column_group_ids', []) or []),
            'filters': config.get('filters'),
            'column_transformations': config.get('column_transformations'),
            'exclude_unified_source_columns': config.get('exclude_unified_source_columns', False)
        }
        
        config_str = json.dumps(normalized, sort_keys=True, default=str)
        return hashlib.sha256(config_str.encode()).hexdigest()[:16]
    
    async def execute_merge(
        self,
        spark,
        source_df,
        output_path: str,
        merge_keys: List[str],
    ) -> Dict[str, Any]:
        """
        Execute MERGE (upsert) operation using Delta Lake.
        
        Same logic as BronzeVersioningService.execute_merge
        """
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        
        # Check if Delta table exists
        try:
            delta_table = DeltaTable.forPath(spark, output_path)
            table_exists = True
            logger.info(f"Delta table exists at {output_path}")
        except Exception:
            table_exists = False
            logger.info(f"Delta table does not exist at {output_path}, will create new")
        
        if not table_exists:
            # First execution: create table
            logger.info(f"Creating new Delta table at {output_path}")
            source_df.write.format("delta").save(output_path)
            
            # Get version info
            delta_table = DeltaTable.forPath(spark, output_path)
            history = delta_table.history(1).collect()
            version = history[0]["version"] if history else 0
            row_count = source_df.count()
            
            return {
                'delta_version': version,
                'rows_inserted': row_count,
                'rows_updated': 0,
                'rows_deleted': 0,
                'total_rows': row_count
            }
        
        # Build merge condition
        merge_condition = " AND ".join([
            f"target.`{key}` = source.`{key}`" for key in merge_keys
        ])
        logger.info(f"MERGE condition: {merge_condition}")
        
        # Get all columns except merge keys for update
        all_columns = source_df.columns
        update_columns = [c for c in all_columns if c not in merge_keys]
        
        # Build update expressions
        update_expr = {f"`{col}`": f"source.`{col}`" for col in update_columns}
        
        # Execute MERGE
        logger.info(f"Executing MERGE with keys: {merge_keys}")
        delta_table.alias("target").merge(
            source_df.alias("source"),
            merge_condition
        ).whenMatchedUpdate(
            set=update_expr
        ).whenNotMatchedInsertAll(
        ).execute()
        
        # Get version and statistics from history
        history = delta_table.history(1).collect()
        latest = history[0].asDict()  # Convert Row to dict for safe access
        version = latest["version"]
        metrics = latest.get("operationMetrics", {}) or {}
        
        # Read total rows
        result_df = spark.read.format("delta").load(output_path)
        total_rows = result_df.count()
        
        return {
            'delta_version': version,
            'rows_inserted': int(metrics.get("numTargetRowsInserted", 0)),
            'rows_updated': int(metrics.get("numTargetRowsUpdated", 0)),
            'rows_deleted': int(metrics.get("numTargetRowsDeleted", 0)),
            'total_rows': total_rows
        }
    
    async def execute_overwrite(
        self,
        spark,
        source_df,
        output_path: str,
    ) -> Dict[str, Any]:
        """
        Execute OVERWRITE operation.
        """
        from delta.tables import DeltaTable
        
        logger.info(f"Executing OVERWRITE at {output_path}")
        
        source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        # Get version info
        delta_table = DeltaTable.forPath(spark, output_path)
        history = delta_table.history(1).collect()
        version = history[0]["version"] if history else 0
        row_count = source_df.count()
        
        return {
            'delta_version': version,
            'rows_inserted': row_count,
            'rows_updated': 0,
            'rows_deleted': 0,
            'total_rows': row_count
        }
    
    async def execute_append(
        self,
        spark,
        source_df,
        output_path: str,
    ) -> Dict[str, Any]:
        """
        Execute APPEND operation.
        """
        from delta.tables import DeltaTable
        
        logger.info(f"Executing APPEND at {output_path}")
        
        source_df.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        # Get version info
        delta_table = DeltaTable.forPath(spark, output_path)
        history = delta_table.history(1).collect()
        version = history[0]["version"] if history else 0
        
        # Total rows
        result_df = spark.read.format("delta").load(output_path)
        total_rows = result_df.count()
        row_count = source_df.count()
        
        return {
            'delta_version': version,
            'rows_inserted': row_count,
            'rows_updated': 0,
            'rows_deleted': 0,
            'total_rows': total_rows
        }
    
    async def get_version_history(
        self,
        spark,
        output_path: str,
        config_id: int,
        config_name: str,
        limit: int = 100
    ) -> SilverVersionHistoryResponse:
        """
        Get Delta Lake version history for a Silver config.
        """
        from delta.tables import DeltaTable
        
        try:
            delta_table = DeltaTable.forPath(spark, output_path)
            history_df = delta_table.history(limit)
            history_rows = history_df.collect()
            
            versions = []
            for row in history_rows:
                row_dict = row.asDict()  # Convert Row to dict for safe access
                metrics = row_dict.get("operationMetrics", {}) or {}
                
                version_info = SilverVersionInfo(
                    version=row_dict["version"],
                    timestamp=row_dict["timestamp"],
                    operation=row_dict["operation"],
                    execution_id=None,
                    rows_inserted=int(metrics.get("numTargetRowsInserted", 0)) or int(metrics.get("numOutputRows", 0)),
                    rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
                    rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
                    total_rows=None,
                    num_files=int(metrics.get("numFiles", 0)) or int(metrics.get("numAddedFiles", 0)),
                    size_bytes=int(metrics.get("numOutputBytes", 0)) or None
                )
                versions.append(version_info)
            
            current_version = versions[0].version if versions else None
            
            return SilverVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=current_version,
                output_path=output_path,
                versions=versions
            )
        
        except Exception as e:
            logger.error(f"Error getting version history: {e}")
            return SilverVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=None,
                output_path=output_path,
                versions=[]
            )
    
    async def query_at_version(
        self,
        spark,
        output_path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> Tuple[List[str], List[Dict[str, Any]], int]:
        """
        Query Delta table at a specific version or timestamp.
        """
        reader = spark.read.format("delta")
        
        if version is not None:
            logger.info(f"Reading Delta table at version {version}")
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            logger.info(f"Reading Delta table at timestamp {timestamp}")
            reader = reader.option("timestampAsOf", timestamp)
        
        df = reader.load(output_path)
        
        columns = df.columns
        total_rows = df.count()
        
        # Apply pagination
        if offset > 0:
            from pyspark.sql import Window
            from pyspark.sql import functions as F
            
            window = Window.orderBy(F.monotonically_increasing_id())
            df = df.withColumn("_row_num", F.row_number().over(window))
            df = df.filter(F.col("_row_num") > offset).drop("_row_num")
        
        df = df.limit(limit)
        
        # Convert to list of dicts
        data = [row.asDict() for row in df.collect()]
        
        return columns, data, total_rows
