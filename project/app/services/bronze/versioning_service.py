"""
Bronze Versioning Service (Simplified)

This service handles Delta Lake versioning for Bronze layer datasets.
It provides:
1. OVERWRITE operations for full refresh each execution
2. Time travel queries to access historical versions
3. Version history tracking

Note: Only OVERWRITE mode is supported for simplicity.
Delta Lake maintains version history automatically.
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from ...api.schemas.bronze_schemas import (
    DeltaVersionInfo,
    BronzeVersionHistoryResponse,
)
from ...database.models.bronze import BronzeExecution

logger = logging.getLogger(__name__)


class BronzeVersioningService:
    """
    Service for managing Delta Lake versioning in Bronze layer.
    
    Simplified to only support OVERWRITE mode.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.bronze_bucket = os.getenv("BRONZE_BUCKET", "datafabric-bronze")
    
    async def execute_overwrite(
        self,
        spark,
        source_df,
        output_path: str,
    ) -> Dict[str, Any]:
        """
        Execute OVERWRITE operation.
        
        This replaces all data but Delta Lake still maintains version history
        for time travel.
        
        Args:
            spark: SparkSession
            source_df: DataFrame to write
            output_path: S3A path for Delta table
            
        Returns:
            Dict with delta_version, rows_inserted, total_rows, size_bytes, num_files
        """
        from delta.tables import DeltaTable
        
        logger.info(f"Executing OVERWRITE at {output_path}")
        
        source_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .option("mergeSchema", "true") \
            .save(output_path)
        
        # Get version info and metrics from Delta history
        delta_table = DeltaTable.forPath(spark, output_path)
        history = delta_table.history(1).collect()
        
        version = 0
        size_bytes = None
        num_files = None
        
        if history:
            row_dict = history[0].asDict()
            version = row_dict["version"]
            metrics = row_dict.get("operationMetrics", {}) or {}
            size_bytes = int(metrics.get("numOutputBytes", 0)) or None
            num_files = int(metrics.get("numFiles", 0)) or int(metrics.get("numAddedFiles", 0)) or None
        
        row_count = source_df.count()
        
        return {
            'delta_version': version,
            'rows_inserted': row_count,
            'rows_updated': 0,
            'rows_deleted': 0,
            'total_rows': row_count,
            'size_bytes': size_bytes,
            'num_files': num_files
        }
    
    async def get_version_history(
        self,
        config_id: int,
        config_name: str,
        limit: int = 100
    ) -> BronzeVersionHistoryResponse:
        """
        Get version history for a config from execution records.
        
        Uses BronzeExecution table as source of truth for aggregated metrics
        across all output paths. This provides a unified view regardless of
        how many paths/sources the config has.
        
        Args:
            config_id: Config ID
            config_name: Config name
            limit: Maximum versions to return
        
        Returns:
            BronzeVersionHistoryResponse with version list including config_snapshot for each version
        """
        from ...database.models.bronze import BronzeExecutionStatus
        
        try:
            # Get all successful executions for this config (they have the aggregated metrics)
            executions_result = await self.db.execute(
                select(BronzeExecution)
                .where(
                    BronzeExecution.config_id == config_id,
                    BronzeExecution.status.in_([BronzeExecutionStatus.SUCCESS, BronzeExecutionStatus.PARTIAL])
                )
                .order_by(BronzeExecution.started_at.desc())
                .limit(limit)
            )
            executions = executions_result.scalars().all()
            
            if not executions:
                return BronzeVersionHistoryResponse(
                    config_id=config_id,
                    config_name=config_name,
                    current_version=None,
                    output_paths=[],
                    versions=[]
                )
            
            # Collect all unique output paths
            all_output_paths = set()
            for exec in executions:
                if exec.output_paths:
                    all_output_paths.update(exec.output_paths)
            
            versions = []
            for exec in executions:
                # Get size and file metrics from execution_details
                exec_details = exec.execution_details or {}
                
                # Use execution timestamp and metrics (already aggregated across all paths)
                version_info = DeltaVersionInfo(
                    version=exec.delta_version if exec.delta_version is not None else 0,
                    timestamp=exec.finished_at or exec.started_at,
                    operation=exec.write_mode_used.upper() if exec.write_mode_used else "WRITE",
                    execution_id=exec.id,
                    rows_inserted=exec.rows_inserted,
                    rows_updated=exec.rows_updated,
                    rows_deleted=exec.rows_deleted,
                    total_rows=exec.rows_ingested,
                    num_files=exec_details.get('num_files'),
                    size_bytes=exec_details.get('size_bytes'),
                    config_snapshot=exec.config_snapshot
                )
                versions.append(version_info)
            
            current_version = versions[0].version if versions else None
            
            return BronzeVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=current_version,
                output_paths=sorted(list(all_output_paths)),
                versions=versions
            )
        
        except Exception as e:
            logger.error(f"Error getting version history: {e}")
            return BronzeVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=None,
                output_paths=[],
                versions=[]
            )
    
    async def query_at_version(
        self,
        spark,
        output_path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
        column_filters: Optional[List] = None,
        column_filters_logic: str = "AND",
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
    ) -> Tuple[List[str], List[Dict[str, Any]], int]:
        """
        Query Delta table at a specific version or timestamp, with optional
        column filtering and sorting.
        
        Args:
            spark: SparkSession
            output_path: S3A path to Delta table
            version: Specific version number (optional)
            timestamp: Timestamp for time travel (optional)
            limit: Max rows to return
            offset: Rows to skip
            column_filters: List of ColumnFilter objects for filtering
            column_filters_logic: "AND" or "OR" logic for combining filters
            sort_by: Column name to sort by
            sort_order: "asc" or "desc"
        
        Returns:
            Tuple of (columns, data, total_rows)
        """
        from ..data_query_service import apply_filters_and_sorting
        from ...api.schemas.data_query_schemas import ColumnFilterLogic, SortOrder
        
        reader = spark.read.format("delta")
        
        if version is not None:
            logger.info(f"Reading Delta table at version {version}")
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            logger.info(f"Reading Delta table at timestamp {timestamp}")
            reader = reader.option("timestampAsOf", timestamp)
        
        df = reader.load(output_path)
        
        columns = df.columns
        
        # Apply column filters and sorting before counting/pagination
        filters_logic = ColumnFilterLogic(column_filters_logic) if column_filters_logic else ColumnFilterLogic.AND
        sort_ord = SortOrder(sort_order) if sort_order else SortOrder.asc
        
        df = apply_filters_and_sorting(
            df,
            filters=column_filters,
            filters_logic=filters_logic,
            sort_by=sort_by,
            sort_order=sort_ord,
        )
        
        # Count AFTER filtering (this is the filtered total)
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
