"""
API request and response models
"""

from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime


class StandardResponse(BaseModel):
    """Standard API response"""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None


class DataIngestionRequest(BaseModel):
    """Request for data ingestion to bronze layer"""
    dataset_name: str = Field(..., description="Dataset name")
    data: Any = Field(..., description="Data to ingest")
    source_type: str = Field(..., description="Source type (database, api, file, etc)")
    connection_info: Optional[Dict[str, Any]] = Field(None, description="Connection information")
    format: Optional[str] = Field("parquet", description="Storage format")


class TransformationConfig(BaseModel):
    """Transformation configuration"""
    type: str = Field(..., description="Transformation type (clean, enrich, filter)")
    config: Dict[str, Any] = Field(..., description="Transformation configuration")


class QualityRule(BaseModel):
    """Data quality rule"""
    type: str = Field(..., description="Rule type (completeness, uniqueness, etc)")
    config: Dict[str, Any] = Field(..., description="Rule configuration")


class TransformationRequest(BaseModel):
    """Request for bronze to silver transformation"""
    dataset_name: str = Field(..., description="Dataset name")
    transformations: List[TransformationConfig] = Field(..., description="List of transformations")
    quality_rules: Optional[List[QualityRule]] = Field(None, description="Quality check rules")


class AggregationConfig(BaseModel):
    """Aggregation configuration"""
    type: str = Field(..., description="Aggregation type (rollup, window, etc)")
    config: Dict[str, Any] = Field(..., description="Aggregation configuration")


class BusinessRule(BaseModel):
    """Business rule configuration"""
    type: str = Field(..., description="Rule type")
    config: Dict[str, Any] = Field(..., description="Rule configuration")


class AggregationRequest(BaseModel):
    """Request for silver to gold aggregation"""
    dataset_name: str = Field(..., description="Dataset name")
    aggregations: List[AggregationConfig] = Field(..., description="List of aggregations")
    business_rules: Optional[List[BusinessRule]] = Field(None, description="Business rules")


class CustomTieringPolicy(BaseModel):
    """Custom tiering policy configuration"""
    hot_days: int = Field(7, description="Days to keep in hot tier")
    warm_days: int = Field(30, description="Days to keep in warm tier")
    cold_days: int = Field(365, description="Days to keep in cold tier")
    delete_after_days: Optional[int] = Field(None, description="Days after which to delete")


class TieringPolicyRequest(BaseModel):
    """Request to apply tiering policy"""
    dataset_name: str = Field(..., description="Dataset name")
    data_type: str = Field(..., description="Data type (events, metrics, logs, etc)")
    custom_policy: Optional[CustomTieringPolicy] = Field(None, description="Custom policy override")


class CostReportResponse(BaseModel):
    """Cost report response"""
    success: bool
    message: str
    report: Dict[str, Any] 