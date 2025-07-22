"""Spark SQL job handler"""

import logging
from typing import Dict, Any
from datetime import datetime

from app.jobs.base import JobHandler
from app.core.spark_manager import SparkManager


logger = logging.getLogger(__name__)


class SparkSQLJobHandler(JobHandler):
    """Handler for Spark SQL jobs"""
    
    async def execute(self, spark_manager: SparkManager, config: Dict[str, Any], 
                     job_id: str) -> Dict[str, Any]:
        """Execute Spark SQL job"""
        logger.info(f"Executing Spark SQL job {job_id}")
        
        start_time = datetime.utcnow()
        
        try:
            # Validate configuration
            if not await self.validate_config(config):
                raise ValueError("Invalid job configuration")
                
            # Get SQL query
            query = config.get("query")
            if not query:
                raise ValueError("No SQL query provided")
                
            # Get output configuration
            output_path = config.get("output_path")
            output_format = config.get("output_format", "parquet")
            output_mode = config.get("output_mode", "overwrite")
            partition_by = config.get("partition_by", [])
            
            # Execute query
            result = await spark_manager.execute_sql(query)
            
            # If output path specified, write results
            if output_path:
                spark = spark_manager.get_spark()
                df = spark.sql(query)
                
                await spark_manager.write_data(
                    df=df,
                    path=output_path,
                    format=output_format,
                    mode=output_mode,
                    partition_by=partition_by
                )
                
                row_count = df.count()
                logger.info(f"Wrote {row_count} rows to {output_path}")
                
                result = {
                    "rows_written": row_count,
                    "output_path": output_path,
                    "output_format": output_format
                }
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "success",
                "job_id": job_id,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration,
                "result": result
            }
            
        except Exception as e:
            logger.error(f"Spark SQL job {job_id} failed: {e}")
            raise
            
    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate job configuration"""
        # Check required fields
        if "query" not in config:
            logger.error("Missing required field: query")
            return False
            
        # Validate output format if specified
        if "output_format" in config:
            valid_formats = ["parquet", "json", "csv", "orc", "avro"]
            if config["output_format"] not in valid_formats:
                logger.error(f"Invalid output format: {config['output_format']}")
                return False
                
        return True 