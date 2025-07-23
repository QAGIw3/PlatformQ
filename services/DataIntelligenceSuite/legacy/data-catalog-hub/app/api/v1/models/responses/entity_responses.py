"""
Entity API Response Models

Pydantic models for entity API responses.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from app.domain.catalog.entities import Entity


class EntityResponse(BaseModel):
    """Entity response model"""
    guid: str = Field(..., description="Entity GUID")
    type_name: str = Field(..., description="Entity type")
    qualified_name: str = Field(..., description="Qualified name")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Description")
    owner: Optional[str] = Field(None, description="Owner")
    status: str = Field(..., description="Entity status")
    created_time: Optional[datetime] = Field(None, description="Creation time")
    created_by: Optional[str] = Field(None, description="Created by")
    modified_time: Optional[datetime] = Field(None, description="Last modified time")
    modified_by: Optional[str] = Field(None, description="Modified by")
    version: int = Field(..., description="Version number")
    classifications: List[str] = Field(..., description="Classifications")
    tags: List[str] = Field(..., description="Tags")
    glossary_terms: List[str] = Field(..., description="Glossary terms")
    attributes: Dict[str, Any] = Field(..., description="Custom attributes")
    
    @classmethod
    def from_domain(cls, entity: Entity) -> 'EntityResponse':
        """Create from domain entity"""
        return cls(
            guid=entity.guid,
            type_name=entity.type_name,
            qualified_name=entity.qualified_name,
            name=entity.name,
            description=entity.description,
            owner=entity.owner,
            status=entity.status.value,
            created_time=entity.created_time,
            created_by=entity.created_by,
            modified_time=entity.modified_time,
            modified_by=entity.modified_by,
            version=entity.version,
            classifications=entity.classifications,
            tags=entity.tags,
            glossary_terms=entity.glossary_terms,
            attributes=entity.attributes
        )
    
    class Config:
        schema_extra = {
            "example": {
                "guid": "12345678-1234-1234-1234-123456789012",
                "type_name": "DataSet",
                "qualified_name": "sales_data_2024",
                "name": "Sales Data 2024",
                "description": "Annual sales data",
                "owner": "john.doe@example.com",
                "status": "ACTIVE",
                "created_time": "2024-01-01T00:00:00Z",
                "version": 1,
                "classifications": ["PII", "Financial"],
                "tags": ["sales", "2024"],
                "attributes": {
                    "format": "parquet",
                    "location": "s3://bucket/sales/2024"
                }
            }
        }


class EntityListResponse(BaseModel):
    """Response for entity list operations"""
    entities: List[EntityResponse] = Field(..., description="List of entities")
    total: int = Field(..., description="Total count")
    limit: int = Field(..., description="Result limit")
    offset: int = Field(..., description="Result offset")
    
    class Config:
        schema_extra = {
            "example": {
                "entities": [
                    {
                        "guid": "12345678-1234-1234-1234-123456789012",
                        "type_name": "DataSet",
                        "name": "Sales Data 2024"
                    }
                ],
                "total": 100,
                "limit": 20,
                "offset": 0
            }
        }


class BulkOperationResponse(BaseModel):
    """Response for bulk operations"""
    success_count: int = Field(..., description="Number of successful operations")
    failure_count: int = Field(..., description="Number of failed operations")
    results: List[Dict[str, Any]] = Field(..., description="Individual results")
    
    class Config:
        schema_extra = {
            "example": {
                "success_count": 8,
                "failure_count": 2,
                "results": [
                    {
                        "index": 0,
                        "success": True,
                        "guid": "12345678-1234-1234-1234-123456789012"
                    },
                    {
                        "index": 1,
                        "success": False,
                        "error": "Entity already exists"
                    }
                ]
            }
        } 