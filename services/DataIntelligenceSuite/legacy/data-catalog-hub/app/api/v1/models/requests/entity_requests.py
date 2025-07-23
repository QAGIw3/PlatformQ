"""
Entity API Request Models

Pydantic models for entity API requests.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class CreateEntityRequest(BaseModel):
    """Request to create an entity"""
    type_name: str = Field(..., description="Entity type name")
    qualified_name: str = Field(..., description="Unique qualified name")
    name: str = Field(..., description="Display name")
    description: Optional[str] = Field(None, description="Entity description")
    owner: Optional[str] = Field(None, description="Entity owner")
    classifications: List[str] = Field(default_factory=list, description="Classifications to apply")
    tags: List[str] = Field(default_factory=list, description="Tags to apply")
    attributes: Dict[str, Any] = Field(default_factory=dict, description="Custom attributes")
    schema: Optional[Dict[str, Any]] = Field(None, description="Schema definition")
    auto_classify: bool = Field(True, description="Enable auto-classification")
    
    class Config:
        schema_extra = {
            "example": {
                "type_name": "DataSet",
                "qualified_name": "sales_data_2024",
                "name": "Sales Data 2024",
                "description": "Annual sales data for 2024",
                "owner": "john.doe@example.com",
                "classifications": ["PII", "Financial"],
                "tags": ["sales", "2024"],
                "attributes": {
                    "format": "parquet",
                    "location": "s3://bucket/sales/2024"
                }
            }
        }


class UpdateEntityRequest(BaseModel):
    """Request to update an entity"""
    description: Optional[str] = Field(None, description="Updated description")
    owner: Optional[str] = Field(None, description="Updated owner")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Updated attributes")
    add_classifications: List[str] = Field(default_factory=list, description="Classifications to add")
    remove_classifications: List[str] = Field(default_factory=list, description="Classifications to remove")
    add_tags: List[str] = Field(default_factory=list, description="Tags to add")
    remove_tags: List[str] = Field(default_factory=list, description="Tags to remove")
    
    class Config:
        schema_extra = {
            "example": {
                "description": "Updated description",
                "add_tags": ["verified", "production"],
                "attributes": {
                    "last_updated": "2024-01-01"
                }
            }
        }


class BulkCreateEntitiesRequest(BaseModel):
    """Request to create multiple entities"""
    entities: List[CreateEntityRequest] = Field(..., description="List of entities to create")
    
    class Config:
        schema_extra = {
            "example": {
                "entities": [
                    {
                        "type_name": "DataSet",
                        "qualified_name": "dataset1",
                        "name": "Dataset 1"
                    },
                    {
                        "type_name": "DataSet",
                        "qualified_name": "dataset2",
                        "name": "Dataset 2"
                    }
                ]
            }
        } 