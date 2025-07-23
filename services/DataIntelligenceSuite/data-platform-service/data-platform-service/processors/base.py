"""
Base processor class for batch processing
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import asyncio
import os
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseFileProcessor(ABC):
    """
    Abstract base class for all file processors.
    Integrates with Apache Spark for distributed processing.
    """
    
    @property
    @abstractmethod
    def processor_type(self) -> str:
        """Unique identifier for the processor"""
        pass
    
    @property
    @abstractmethod
    def supported_formats(self) -> List[str]:
        """List of supported file extensions"""
        pass
    
    @property
    def spark_config(self) -> Dict[str, Any]:
        """Spark configuration for this processor"""
        return {
            "spark.app.name": f"{self.processor_type}_processor",
            "spark.executor.memory": "4g",
            "spark.executor.cores": "2",
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "1",
            "spark.dynamicAllocation.maxExecutors": "10"
        }
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.tenant_id = config.get("tenant_id")
        self.job_id = config.get("job_id")
        self.input_path = config.get("input_path")
        self.output_path = config.get("output_path")
        logger.info(f"Initialized {self.processor_type} processor")
    
    @abstractmethod
    async def validate_input(self, file_path: str) -> bool:
        """Validate that the input file can be processed"""
        pass
    
    @abstractmethod
    async def extract_metadata(self, file_path: str) -> Dict[str, Any]:
        """Extract metadata from the file"""
        pass
    
    @abstractmethod
    def get_spark_job_script(self) -> str:
        """
        Get the Spark job script for this processor.
        This should return the path to the Python script that will be submitted to Spark.
        """
        pass
    
    async def process(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process a file using Spark
        """
        try:
            # Validate input
            if not await self.validate_input(file_path):
                raise ValueError(f"Invalid input file: {file_path}")
            
            # Extract metadata
            metadata = await self.extract_metadata(file_path)
            
            # Prepare Spark job
            job_config = {
                "input_path": file_path,
                "output_path": self.output_path or f"/tmp/output/{self.job_id}",
                "metadata": metadata,
                "options": options or {},
                "tenant_id": self.tenant_id,
                "job_id": self.job_id
            }
            
            # Get Spark job script
            spark_script = self.get_spark_job_script()
            
            # Submit Spark job
            result = await self._submit_spark_job(spark_script, job_config)
            
            return {
                "status": "success",
                "job_id": self.job_id,
                "processor": self.processor_type,
                "metadata": metadata,
                "spark_result": result
            }
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            raise
    
    async def _submit_spark_job(self, script_path: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit job to Spark cluster"""
        # This would integrate with the existing Spark submission logic
        # For now, return a placeholder
        return {
            "spark_app_id": f"spark_{self.job_id}",
            "status": "submitted",
            "script": script_path,
            "config": config
        }
    
    def supports_file(self, file_path: str) -> bool:
        """Check if this processor supports the given file"""
        extension = os.path.splitext(file_path)[1].lower().lstrip('.')
        return extension in self.supported_formats 