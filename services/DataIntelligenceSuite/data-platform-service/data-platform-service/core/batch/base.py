"""Base job handler interface"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

from app.core.spark_manager import SparkManager


logger = logging.getLogger(__name__)


class JobHandler(ABC):
    """Abstract base class for job handlers"""
    
    @abstractmethod
    async def execute(self, spark_manager: SparkManager, config: Dict[str, Any], 
                     job_id: str) -> Dict[str, Any]:
        """Execute the job
        
        Args:
            spark_manager: SparkManager instance
            config: Job configuration
            job_id: Unique job identifier
            
        Returns:
            Dictionary containing job results
        """
        pass
        
    async def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate job configuration
        
        Args:
            config: Job configuration
            
        Returns:
            True if configuration is valid
        """
        return True
        
    async def prepare(self, spark_manager: SparkManager, config: Dict[str, Any]):
        """Prepare for job execution
        
        Args:
            spark_manager: SparkManager instance
            config: Job configuration
        """
        pass
        
    async def cleanup(self, spark_manager: SparkManager, job_id: str):
        """Cleanup after job execution
        
        Args:
            spark_manager: SparkManager instance
            job_id: Unique job identifier
        """
        pass 