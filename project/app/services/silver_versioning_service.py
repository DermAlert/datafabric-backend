"""
Silver Versioning Service (Simplified)

This service handles Delta Lake versioning for Silver layer datasets.
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

from sqlalchemy.future import select

from ..api.schemas.silver_schemas import (
    SilverVersionInfo,
    SilverVersionHistoryResponse,
)
from ..database.datasets.silver import TransformExecution, TransformStatus

logger = logging.getLogger(__name__)


class SilverVersioningService:
    """
    Service for managing Delta Lake versioning in Silver layer.
    
    Simplified to only support OVERWRITE mode.
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.silver_bucket = os.getenv("SILVER_BUCKET", "datafabric-silver")
    
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
    ) -> SilverVersionHistoryResponse:
        """
        Get version history for a config from execution records.
        
        Uses TransformExecution table as source of truth for aggregated metrics.
        This provides a unified view and includes config_snapshot for each version.
        
        Args:
            config_id: Config ID
            config_name: Config name
            limit: Maximum versions to return
        
        Returns:
            SilverVersionHistoryResponse with version list including config_snapshot for each version
        """
        try:
            # Get all successful executions for this config (they have the aggregated metrics)
            executions_result = await self.db.execute(
                select(TransformExecution)
                .where(
                    TransformExecution.config_id == config_id,
                    TransformExecution.status == TransformStatus.SUCCESS
                )
                .order_by(TransformExecution.started_at.desc())
                .limit(limit)
            )
            executions = executions_result.scalars().all()
            
            if not executions:
                return SilverVersionHistoryResponse(
                    config_id=config_id,
                    config_name=config_name,
                    current_version=None,
                    output_paths=[],
                    versions=[]
                )
            
            # Collect all unique output paths
            all_output_paths = set()
            for exec in executions:
                if exec.output_path:
                    all_output_paths.add(exec.output_path)
            
            versions = []
            for exec in executions:
                # Get size and file metrics from execution_details
                exec_details = exec.execution_details or {}
                
                # Use execution timestamp and metrics (already aggregated)
                version_info = SilverVersionInfo(
                    version=exec.delta_version if exec.delta_version is not None else 0,
                    timestamp=exec.finished_at or exec.started_at,
                    operation=exec.write_mode_used.upper() if exec.write_mode_used else "OVERWRITE",
                    execution_id=exec.id,
                    rows_inserted=exec.rows_inserted,
                    rows_updated=exec.rows_updated,
                    rows_deleted=exec.rows_deleted,
                    total_rows=exec.rows_output,
                    num_files=exec_details.get('num_files'),
                    size_bytes=exec_details.get('size_bytes'),
                    config_snapshot=exec.config_snapshot
                )
                versions.append(version_info)
            
            current_version = versions[0].version if versions else None
            
            return SilverVersionHistoryResponse(
                config_id=config_id,
                config_name=config_name,
                current_version=current_version,
                output_paths=sorted(list(all_output_paths)),
                versions=versions
            )
        
        except Exception as e:
            logger.error(f"Error getting version history: {e}")
            return SilverVersionHistoryResponse(
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
            from pyspark.sql import Window
            from pyspark.sql import functions as F
            
            window = Window.orderBy(F.monotonically_increasing_id())
            df = df.withColumn("_row_num", F.row_number().over(window))
            df = df.filter(F.col("_row_num") > offset).drop("_row_num")
        
        df = df.limit(limit)
        
        # Convert to list of dicts
        data = [row.asDict() for row in df.collect()]
        
        return columns, data, total_rows
