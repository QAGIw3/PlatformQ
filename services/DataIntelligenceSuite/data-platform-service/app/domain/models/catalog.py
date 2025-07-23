"""
Catalog Domain Models

Domain entities for data catalog functionality
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field

from data_intelligence_common.core.catalog import EntityStatus


class Dataset(BaseModel):
    """Dataset entity in the catalog"""
    id: str
    name: str
    description: str
    location: str
    format: str  # iceberg, delta, parquet, csv, etc.
    owner: str
    created_at: datetime
    updated_at: datetime
    status: EntityStatus = EntityStatus.ACTIVE
    schema_id: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    properties: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class Schema(BaseModel):
    """Schema definition for a dataset"""
    id: str
    dataset_id: str
    version: int
    definition: Dict[str, Any]  # JSON schema or Avro schema
    compatibility_mode: str = "BACKWARD"
    created_at: datetime
    
    
class Classification(BaseModel):
    """Data classification"""
    id: str
    name: str  # PII, SENSITIVE, PUBLIC, etc.
    dataset_id: str
    confidence: float = 1.0
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    
    
class Lineage(BaseModel):
    """Data lineage relationship"""
    id: str
    source_datasets: List[str]
    target_dataset: str
    process_name: str
    process_type: str  # batch, streaming, manual
    created_at: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    
class BusinessTerm(BaseModel):
    """Business glossary term"""
    id: str
    name: str
    definition: str
    owner: str
    synonyms: List[str] = Field(default_factory=list)
    related_terms: List[str] = Field(default_factory=list)
    mapped_datasets: List[str] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    
    
class DataQualityRule(BaseModel):
    """Data quality rule definition"""
    id: str
    name: str
    dataset_id: str
    rule_type: str  # completeness, accuracy, consistency, etc.
    expression: str
    severity: str = "warning"  # info, warning, error, critical
    enabled: bool = True
    created_at: datetime
    updated_at: datetime
    
    
class CatalogSearchResult(BaseModel):
    """Search result from catalog"""
    datasets: List[Dataset] = Field(default_factory=list)
    total: int = 0
    offset: int = 0
    limit: int = 20
    query: str = ""
    filters: Dict[str, Any] = Field(default_factory=dict)
    
    
class DatasetStatistics(BaseModel):
    """Statistics for a dataset"""
    dataset_id: str
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    column_count: Optional[int] = None
    null_count: Optional[Dict[str, int]] = None
    distinct_count: Optional[Dict[str, int]] = None
    min_values: Optional[Dict[str, Any]] = None
    max_values: Optional[Dict[str, Any]] = None
    last_updated: datetime
    
    
class ColumnMetadata(BaseModel):
    """Metadata for a dataset column"""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    statistics: Optional[Dict[str, Any]] = None
    
    
class TableMetadata(BaseModel):
    """Extended metadata for table-based datasets"""
    dataset_id: str
    database: str
    table_name: str
    columns: List[ColumnMetadata]
    partitions: Optional[List[str]] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime 