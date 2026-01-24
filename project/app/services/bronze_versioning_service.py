"""
Bronze Versioning Service

This service handles Delta Lake versioning for Bronze layer datasets.
It provides:
1. Auto-detection of merge keys from source table PKs
2. MERGE (upsert) operations for incremental updates without duplicates
3. Time travel queries to access historical versions
4. Version history tracking

Key concepts:
- write_mode='merge': Upsert based on merge_keys (recommended)
- write_mode='overwrite': Full refresh each execution
- write_mode='append': Add data without deduplication
- merge_keys: Columns used for deduplication (like a primary key)
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

from ..database.metadata.metadata import ExternalColumn, ExternalTables
from ..database.datasets.bronze import (
    BronzePersistentConfig,
    BronzeExecution,
    WriteMode,
)
from ..api.schemas.bronze_schemas import (
    TableColumnSelection,
    DeltaVersionInfo,
    BronzeVersionHistoryResponse,
)

logger = logging.getLogger(__name__)


class BronzeVersioningService:
    """
    Service for managing Delta Lake versioning in Bronze layer.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.bronze_bucket = os.getenv("BRONZE_BUCKET", "datafabric-bronze")
    
    async def auto_detect_merge_keys(
        self, 
        tables: List[Dict[str, Any]]
    ) -> Tuple[Optional[List[str]], str]:
        """
        Auto-detect merge keys from source table primary keys.
        
        Args:
            tables: List of table configurations [{table_id: 1, select_all: true}, ...]
        
        Returns:
            Tuple of (merge_keys, source) where:
            - merge_keys: List of column names to use for merge, or None if not found
            - source: 'auto_detected_pk' if found, None if not
        """
        detected_pks = []
        
        for table_config in tables:
            table_id = table_config.get('table_id')
            if not table_id:
                continue
            
            # Query PKs for this table
            result = await self.db.execute(
                select(ExternalColumn.column_name)
                .where(
                    and_(
                        ExternalColumn.table_id == table_id,
                        ExternalColumn.is_primary_key == True
                    )
                )
            )
            pks = [row[0] for row in result.fetchall()]
            detected_pks.extend(pks)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_pks = []
        for pk in detected_pks:
            if pk.lower() not in seen:
                seen.add(pk.lower())
                unique_pks.append(pk)
        
        if unique_pks:
            logger.info(f"Auto-detected merge_keys from PKs: {unique_pks}")
            return unique_pks, 'auto_detected_pk'
        
        logger.warning("No primary keys found in source tables")
        return None, None
    
    async def resolve_write_mode(
        self,
        write_mode: str,
        merge_keys: Optional[List[str]],
        tables: List[Dict[str, Any]]
    ) -> Tuple[str, Optional[List[str]], Optional[str], List[str]]:
        """
        Resolve the effective write mode and merge keys.
        
        If write_mode='merge' and merge_keys not provided:
        - Try to auto-detect from source PKs
        - If not found, fallback to 'overwrite'
        
        Args:
            write_mode: Requested write mode ('overwrite', 'append', 'merge')
            merge_keys: User-provided merge keys (optional)
            tables: Table configurations
        
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
        
        # Try to auto-detect
        detected_keys, source = await self.auto_detect_merge_keys(tables)
        
        if detected_keys:
            return 'merge', detected_keys, source, warnings
        
        # No PKs found - fallback to overwrite
        warnings.append(
            "No primary keys found in source tables. "
            "Falling back to 'overwrite' mode. "
            "Define merge_keys manually to use 'merge' mode."
        )
        return 'overwrite', None, None, warnings
    
    def calculate_schema_hash(self, tables: List[Dict[str, Any]]) -> str:
        """
        Calculate a hash of the schema configuration.
        
        This is used to detect schema changes between executions.
        The hash is normalized (sorted) so order doesn't matter.
        
        Args:
            tables: Table configurations
        
        Returns:
            SHA256 hash (first 16 chars)
        """
        # Normalize: sort by table_id, sort column_ids
        normalized = []
        for t in sorted(tables, key=lambda x: x.get("table_id", 0)):
            item = {"table_id": t.get("table_id")}
            
            if t.get("select_all"):
                item["select_all"] = True
            elif t.get("column_ids"):
                item["column_ids"] = sorted(t.get("column_ids", []))
            
            normalized.append(item)
        
        config_str = json.dumps(normalized, sort_keys=True)
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
        
        Args:
            spark: SparkSession
            source_df: Source DataFrame with new/updated data
            output_path: S3A path to Delta table
            merge_keys: Columns to use for matching
        
        Returns:
            Dict with statistics: {
                'delta_version': int,
                'rows_inserted': int,
                'rows_updated': int,
                'rows_deleted': int,
                'total_rows': int
            }
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
        latest = history[0]
        version = latest["version"]
        metrics = latest.get("operationMetrics", {})
        
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
        
        This replaces all data but Delta Lake still maintains version history.
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
        
        This adds data without checking for duplicates.
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
    ) -> BronzeVersionHistoryResponse:
        """
        Get Delta Lake version history for a config.
        
        Args:
            spark: SparkSession
            output_path: S3A path to Delta table
            config_id: Config ID
            config_name: Config name
            limit: Maximum versions to return
        
        Returns:
            BronzeVersionHistoryResponse with version list
        """
        from delta.tables import DeltaTable
        
        try:
            delta_table = DeltaTable.forPath(spark, output_path)
            history_df = delta_table.history(limit)
            history_rows = history_df.collect()
            
            versions = []
            for row in history_rows:
                metrics = row.get("operationMetrics", {}) or {}
                
                version_info = DeltaVersionInfo(
                    version=row["version"],
                    timestamp=row["timestamp"],
                    operation=row["operation"],
                    execution_id=None,  # TODO: link to BronzeExecution
                    rows_inserted=int(metrics.get("numTargetRowsInserted", 0)) or int(metrics.get("numOutputRows", 0)),
                    rows_updated=int(metrics.get("numTargetRowsUpdated", 0)),
                    rows_deleted=int(metrics.get("numTargetRowsDeleted", 0)),
                    total_rows=None,  # Would need to read table at each version
                    num_files=int(metrics.get("numFiles", 0)) or int(metrics.get("numAddedFiles", 0)),
                    size_bytes=int(metrics.get("numOutputBytes", 0)) or None
                )
                versions.append(version_info)
            
            current_version = versions[0].version if versions else None
            
            return BronzeVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=current_version,
                output_path=output_path,
                versions=versions
            )
        
        except Exception as e:
            logger.error(f"Error getting version history: {e}")
            return BronzeVersionHistoryResponse(
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
        
        Args:
            spark: SparkSession
            output_path: S3A path to Delta table
            version: Specific version number (optional)
            timestamp: Timestamp for time travel (optional)
            limit: Max rows to return
            offset: Rows to skip
        
        Returns:
            Tuple of (columns, data, total_rows)
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
            # Use row_number for pagination
            from pyspark.sql import Window
            from pyspark.sql import functions as F
            
            window = Window.orderBy(F.monotonically_increasing_id())
            df = df.withColumn("_row_num", F.row_number().over(window))
            df = df.filter(F.col("_row_num") > offset).drop("_row_num")
        
        df = df.limit(limit)
        
        # Convert to list of dicts
        data = [row.asDict() for row in df.collect()]
        
        return columns, data, total_rows
