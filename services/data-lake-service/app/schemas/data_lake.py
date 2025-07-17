from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional

class IngestionRequest(BaseModel):
    source_type: str = Field(..., description="Type of data source: pulsar, api, database, file, iot")
    source_config: Dict[str, Any] = Field(..., description="Source-specific configuration")
    target_dataset: str = Field(..., description="Target dataset name")
    ingestion_mode: str = Field(..., description="Ingestion mode: batch, streaming, incremental")
    schedule: Optional[str] = Field(None, description="Cron expression for scheduled runs")
    transformations: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    quality_checks: Optional[List[str]] = Field(default_factory=list)


class ProcessingRequest(BaseModel):
    source_layer: str = Field(..., description="Source layer: bronze, silver")
    target_layer: str = Field(..., description="Target layer: silver, gold")
    dataset_name: str = Field(..., description="Dataset name")
    transformations: List[Dict[str, Any]] = Field(..., description="List of transformations to apply")
    quality_threshold: float = Field(0.90, description="Minimum quality score required")
    partition_strategy: Optional[Dict[str, Any]] = Field(None)
    optimization_config: Optional[Dict[str, Any]] = Field(None)
    schedule: Optional[str] = Field(None, description="Cron expression for scheduled runs")


class QualityCheckRequest(BaseModel):
    dataset_name: str = Field(..., description="Dataset name")
    layer: str = Field(..., description="Data layer: bronze, silver, gold")
    rules: Optional[List[str]] = Field(None, description="Specific rules to apply")


class DataCatalogQuery(BaseModel):
    layer: Optional[str] = Field(None, description="Filter by layer")
    dataset_pattern: Optional[str] = Field(None, description="Dataset name pattern")
    include_schema: bool = Field(True, description="Include schema information") 