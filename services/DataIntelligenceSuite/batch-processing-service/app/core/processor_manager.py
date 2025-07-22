"""
Processor Manager for managing file processors
"""

import logging
from typing import Dict, Any, List, Optional
import os
import uuid
from datetime import datetime

from app.processors import get_processor_for_file, PROCESSOR_REGISTRY
from app.core.config import Settings
from app.core.job_scheduler import JobScheduler

logger = logging.getLogger(__name__)


class ProcessorManager:
    """Manages file processors and their execution"""
    
    def __init__(self, config: Settings, job_scheduler: JobScheduler):
        self.config = config
        self.job_scheduler = job_scheduler
        self.active_processors: Dict[str, Any] = {}
        
    async def process_file(self, file_path: str, options: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Process a file using the appropriate processor
        """
        # Get processor for file
        processor_class = get_processor_for_file(file_path)
        if not processor_class:
            raise ValueError(f"No processor available for file: {file_path}")
        
        # Generate job ID
        job_id = str(uuid.uuid4())
        
        # Create processor instance
        processor_config = {
            "tenant_id": options.get("tenant_id", "default"),
            "job_id": job_id,
            "input_path": file_path,
            "output_path": options.get("output_path", f"/tmp/output/{job_id}")
        }
        
        processor = processor_class(processor_config)
        self.active_processors[job_id] = processor
        
        try:
            # Process the file
            result = await processor.process(file_path, options)
            
            # Submit to Spark if needed
            if result.get("spark_result"):
                spark_job_id = await self._submit_spark_job(processor, result)
                result["spark_job_id"] = spark_job_id
            
            return result
            
        finally:
            # Clean up
            del self.active_processors[job_id]
    
    async def process_batch(self, file_paths: List[str], options: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Process multiple files in batch
        """
        results = []
        
        # Group files by processor type
        processor_groups = {}
        for file_path in file_paths:
            processor_class = get_processor_for_file(file_path)
            if processor_class:
                processor_type = processor_class({}).processor_type
                if processor_type not in processor_groups:
                    processor_groups[processor_type] = []
                processor_groups[processor_type].append(file_path)
        
        # Process each group
        for processor_type, files in processor_groups.items():
            # Create batch job
            batch_job_id = str(uuid.uuid4())
            
            # Submit batch job to Spark
            batch_config = {
                "processor_type": processor_type,
                "input_files": files,
                "output_path": options.get("output_path", f"/tmp/output/{batch_job_id}"),
                "tenant_id": options.get("tenant_id", "default"),
                "options": options or {}
            }
            
            spark_job_id = await self._submit_batch_spark_job(processor_type, batch_config)
            
            results.append({
                "batch_job_id": batch_job_id,
                "processor_type": processor_type,
                "num_files": len(files),
                "spark_job_id": spark_job_id,
                "status": "submitted"
            })
        
        return results
    
    async def _submit_spark_job(self, processor, result: Dict[str, Any]) -> str:
        """Submit processor job to Spark"""
        spark_config = processor.spark_config
        spark_script = processor.get_spark_job_script()
        
        # Prepare job configuration
        job_config = {
            "script": spark_script,
            "config": result.get("spark_result", {}).get("config", {}),
            "spark_conf": spark_config
        }
        
        # Submit through job scheduler
        job_id = await self.job_scheduler.submit_job(
            name=f"{processor.processor_type}_{processor.job_id}",
            job_type="file_processing",
            config=job_config,
            resource_profile=self._get_resource_profile(processor.processor_type)
        )
        
        return job_id
    
    async def _submit_batch_spark_job(self, processor_type: str, batch_config: Dict[str, Any]) -> str:
        """Submit batch processing job to Spark"""
        # Get processor class to get Spark config
        processor_class = PROCESSOR_REGISTRY.get(processor_type)
        if not processor_class:
            raise ValueError(f"Unknown processor type: {processor_type}")
        
        temp_processor = processor_class({})
        spark_config = temp_processor.spark_config
        spark_script = temp_processor.get_spark_job_script()
        
        # Prepare job configuration
        job_config = {
            "script": spark_script,
            "config": batch_config,
            "spark_conf": spark_config
        }
        
        # Submit through job scheduler
        job_id = await self.job_scheduler.submit_job(
            name=f"batch_{processor_type}_{batch_config.get('batch_job_id')}",
            job_type="file_processing",
            config=job_config,
            resource_profile=self._get_resource_profile(processor_type)
        )
        
        return job_id
    
    def list_supported_formats(self) -> Dict[str, List[str]]:
        """List all supported file formats by processor"""
        supported = {}
        
        for processor_name, processor_class in PROCESSOR_REGISTRY.items():
            temp_processor = processor_class({})
            supported[processor_name] = temp_processor.supported_formats
        
        return supported
    
    def get_processor_info(self, processor_type: str) -> Dict[str, Any]:
        """Get information about a specific processor"""
        processor_class = PROCESSOR_REGISTRY.get(processor_type)
        if not processor_class:
            raise ValueError(f"Unknown processor type: {processor_type}")
        
        temp_processor = processor_class({})
        
        return {
            "type": processor_type,
            "supported_formats": temp_processor.supported_formats,
            "spark_config": temp_processor.spark_config
        }
    
    def _get_resource_profile(self, processor_type: str) -> str:
        """Get resource profile for a processor type"""
        # Map processor types to resource profiles
        resource_map = {
            "blender": "high",      # GPU-intensive
            "openfoam": "high",     # CPU/Memory intensive
            "multimedia": "medium", # Variable based on content
            "freecad": "medium",    # Moderate resources
            "flightgear": "low"     # Light processing
        }
        return resource_map.get(processor_type, "medium") 