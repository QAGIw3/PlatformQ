"""File Processing job handler"""

import logging
from typing import Dict, Any
from datetime import datetime
import json
import subprocess
import os

from app.jobs.base import JobHandler
from app.core.spark_manager import SparkManager

logger = logging.getLogger(__name__)


class FileProcessingJobHandler(JobHandler):
    """Handler for file processing jobs"""
    
    async def execute(self, spark_manager: SparkManager, config: Dict[str, Any], 
                     job_id: str) -> Dict[str, Any]:
        """Execute file processing job"""
        logger.info(f"Executing file processing job {job_id}")
        
        start_time = datetime.utcnow()
        
        try:
            # Validate configuration
            if not await self.validate_config(config):
                raise ValueError("Invalid job configuration")
            
            # Get job parameters
            script_path = config.get("script")
            spark_conf = config.get("spark_conf", {})
            job_config = config.get("config", {})
            
            # Build Spark submit command
            spark_submit_cmd = [
                "spark-submit",
                "--master", spark_manager.spark_master
            ]
            
            # Add Spark configurations
            for key, value in spark_conf.items():
                spark_submit_cmd.extend(["--conf", f"{key}={value}"])
            
            # Add job config as environment variable
            env = os.environ.copy()
            env["SPARK_JOB_CONFIG"] = json.dumps(job_config)
            
            # Add script path
            spark_submit_cmd.append(script_path)
            
            # Submit job to Spark
            logger.info(f"Submitting Spark job: {' '.join(spark_submit_cmd)}")
            
            result = subprocess.run(
                spark_submit_cmd,
                capture_output=True,
                text=True,
                env=env
            )
            
            if result.returncode != 0:
                raise RuntimeError(f"Spark job failed: {result.stderr}")
            
            # Parse results if available
            results_path = job_config.get("output_path", f"/tmp/output/{job_id}")
            job_results = {}
            
            # Try to read results file
            results_file = os.path.join(results_path, "processing_results.json")
            if os.path.exists(results_file):
                with open(results_file, 'r') as f:
                    job_results = json.load(f)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            return {
                "status": "success",
                "job_id": job_id,
                "processor_type": job_config.get("processor_type", "unknown"),
                "files_processed": len(job_config.get("input_files", [])),
                "duration_seconds": duration,
                "results": job_results,
                "spark_output": result.stdout
            }
            
        except Exception as e:
            logger.error(f"File processing job {job_id} failed: {e}")
            raise
    
    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate job configuration"""
        if "script" not in config:
            logger.error("Missing required field: script")
            return False
        
        if "config" not in config:
            logger.error("Missing required field: config")
            return False
        
        return True 