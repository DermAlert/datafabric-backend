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

from ..api.schemas.silver_schemas import (
    SilverVersionInfo,
    SilverVersionHistoryResponse,
)

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
            Dict with delta_version, rows_inserted, total_rows
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
    
    async def get_version_history(
        self,
        spark,
        output_path: str,
        config_id: int,
        config_name: str,
        limit: int = 100
    ) -> SilverVersionHistoryResponse:
        """
        Get Delta Lake version history for a config.
        
        Args:
            spark: SparkSession
            output_path: S3A path to Delta table
            config_id: Config ID
            config_name: Config name
            limit: Maximum versions to return
        
        Returns:
            SilverVersionHistoryResponse with version list
        """
        from delta.tables import DeltaTable
        
        try:
            delta_table = DeltaTable.forPath(spark, output_path)
            history_df = delta_table.history(limit)
            history_rows = history_df.collect()
            
            versions = []
            for row in history_rows:
                row_dict = row.asDict()
                metrics = row_dict.get("operationMetrics", {}) or {}
                
                # Delta Lake registra como "WRITE", mas mostramos "OVERWRITE" 
                # para consistência com Bronze (ambos fazem overwrite)
                delta_operation = row_dict["operation"]
                display_operation = "OVERWRITE" if delta_operation == "WRITE" else delta_operation
                
                version_info = SilverVersionInfo(
                    version=row_dict["version"],
                    timestamp=row_dict["timestamp"],
                    operation=display_operation,
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
