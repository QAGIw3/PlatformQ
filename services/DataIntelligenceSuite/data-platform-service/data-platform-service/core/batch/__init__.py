"""Job handlers for different batch processing job types"""

import logging
from typing import Optional, Dict, Type

from .base import JobHandler
from .spark_sql import SparkSQLJobHandler
from .ml_training import MLTrainingJobHandler
from .etl_pipeline import ETLPipelineJobHandler
from .feature_engineering import FeatureEngineeringJobHandler
from .graph_processing import GraphProcessingJobHandler
from .file_processing import FileProcessingJobHandler


logger = logging.getLogger(__name__)


# Job handler registry
_job_handlers: Dict[str, Type[JobHandler]] = {
    "spark_sql": SparkSQLJobHandler,
    "ml_training": MLTrainingJobHandler,
    "etl_pipeline": ETLPipelineJobHandler,
    "feature_engineering": FeatureEngineeringJobHandler,
    "graph_processing": GraphProcessingJobHandler,
    "file_processing": FileProcessingJobHandler
}


def get_job_handler(job_type: str) -> Optional[JobHandler]:
    """Get job handler for a specific job type"""
    handler_class = _job_handlers.get(job_type)
    if handler_class:
        return handler_class()
    return None


def register_job_handler(job_type: str, handler_class: Type[JobHandler]):
    """Register a new job handler"""
    _job_handlers[job_type] = handler_class
    logger.info(f"Registered job handler for type: {job_type}")


__all__ = ["JobHandler", "get_job_handler", "register_job_handler"] 