"""
Catalog API Endpoints

RESTful API for data catalog operations
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from data_intelligence_common import APIResponse, PaginatedResponse

from ...core.catalog_manager import CatalogManager
from ...domain.models.catalog import (
    Dataset,
    Schema,
    Classification,
    Lineage,
    BusinessTerm,
    DataQualityRule,
    CatalogSearchResult
)
from ..dependencies import get_catalog_manager, get_current_user

router = APIRouter(prefix="/catalog", tags=["Catalog"])


class RegisterDatasetRequest(BaseModel):
    """Request model for dataset registration"""
    name: str = Field(..., description="Dataset name")
    description: str = Field(..., description="Dataset description")
    location: str = Field(..., description="Storage location")
    format: str = Field(..., description="Data format (iceberg, delta, parquet, etc.)")
    schema: Optional[Dict[str, Any]] = Field(None, description="Schema definition")
    owner: Optional[str] = Field(None, description="Dataset owner")
    tags: Optional[List[str]] = Field(default_factory=list, description="Tags")
    properties: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional properties")


class CreateLineageRequest(BaseModel):
    """Request model for creating lineage"""
    source_datasets: List[str] = Field(..., description="Source dataset IDs")
    target_dataset: str = Field(..., description="Target dataset ID")
    process_name: str = Field(..., description="Process name")
    process_type: str = Field("batch", description="Process type")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")


class AddClassificationRequest(BaseModel):
    """Request model for adding classification"""
    dataset_id: str = Field(..., description="Dataset ID")
    classification_name: str = Field(..., description="Classification name")
    confidence: float = Field(1.0, ge=0.0, le=1.0, description="Confidence score")
    attributes: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Classification attributes")


class CreateBusinessTermRequest(BaseModel):
    """Request model for creating business term"""
    name: str = Field(..., description="Term name")
    definition: str = Field(..., description="Term definition")
    owner: str = Field(..., description="Term owner")
    synonyms: Optional[List[str]] = Field(default_factory=list, description="Synonyms")
    related_terms: Optional[List[str]] = Field(default_factory=list, description="Related terms")
    mapped_datasets: Optional[List[str]] = Field(default_factory=list, description="Mapped dataset IDs")


@router.post("/datasets", response_model=APIResponse[Dataset])
async def register_dataset(
    request: RegisterDatasetRequest,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Register a new dataset in the catalog"""
    try:
        dataset = await catalog_manager.register_dataset(
            name=request.name,
            description=request.description,
            location=request.location,
            format=request.format,
            schema=request.schema,
            owner=request.owner or current_user["id"],
            tags=request.tags,
            properties=request.properties
        )
        
        return APIResponse(
            success=True,
            data=dataset,
            message=f"Dataset '{request.name}' registered successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to register dataset: {str(e)}")


@router.get("/datasets", response_model=PaginatedResponse[Dataset])
async def search_datasets(
    query: str = Query("", description="Search query"),
    format: Optional[str] = Query(None, description="Filter by format"),
    owner: Optional[str] = Query(None, description="Filter by owner"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Search datasets in the catalog"""
    try:
        # Build filters
        filters = {}
        if format:
            filters["format"] = format
        if owner:
            filters["owner"] = owner
        if tags:
            filters["tags"] = tags.split(",")
            
        # Search datasets
        datasets, total = await catalog_manager.search_datasets(
            query=query,
            filters=filters,
            limit=page_size,
            offset=(page - 1) * page_size
        )
        
        return PaginatedResponse(
            success=True,
            data=datasets,
            total=total,
            page=page,
            page_size=page_size,
            pages=(total + page_size - 1) // page_size
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to search datasets: {str(e)}")


@router.get("/datasets/{dataset_id}", response_model=APIResponse[Dataset])
async def get_dataset(
    dataset_id: str,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get dataset details"""
    try:
        if dataset_id not in catalog_manager.datasets:
            raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")
            
        dataset = catalog_manager.datasets[dataset_id]
        
        return APIResponse(
            success=True,
            data=dataset,
            message="Dataset retrieved successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get dataset: {str(e)}")


@router.post("/lineage", response_model=APIResponse[Lineage])
async def create_lineage(
    request: CreateLineageRequest,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Create lineage relationship between datasets"""
    try:
        lineage = await catalog_manager.create_lineage(
            source_datasets=request.source_datasets,
            target_dataset=request.target_dataset,
            process_name=request.process_name,
            process_type=request.process_type,
            metadata=request.metadata
        )
        
        return APIResponse(
            success=True,
            data=lineage,
            message="Lineage created successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create lineage: {str(e)}")


@router.get("/datasets/{dataset_id}/lineage", response_model=APIResponse[Dict[str, Any]])
async def get_dataset_lineage(
    dataset_id: str,
    direction: str = Query("both", regex="^(upstream|downstream|both)$"),
    depth: int = Query(3, ge=1, le=10),
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get lineage graph for a dataset"""
    try:
        lineage = await catalog_manager.get_dataset_lineage(
            dataset_id=dataset_id,
            direction=direction,
            depth=depth
        )
        
        return APIResponse(
            success=True,
            data=lineage,
            message="Lineage retrieved successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get lineage: {str(e)}")


@router.post("/classifications", response_model=APIResponse[Classification])
async def add_classification(
    request: AddClassificationRequest,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Add classification to a dataset"""
    try:
        classification = await catalog_manager.add_classification(
            dataset_id=request.dataset_id,
            classification_name=request.classification_name,
            confidence=request.confidence,
            attributes=request.attributes
        )
        
        return APIResponse(
            success=True,
            data=classification,
            message=f"Classification '{request.classification_name}' added successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to add classification: {str(e)}")


@router.post("/business-terms", response_model=APIResponse[BusinessTerm])
async def create_business_term(
    request: CreateBusinessTermRequest,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Create a business glossary term"""
    try:
        term = await catalog_manager.create_business_term(
            name=request.name,
            definition=request.definition,
            owner=request.owner,
            synonyms=request.synonyms,
            related_terms=request.related_terms,
            mapped_datasets=request.mapped_datasets
        )
        
        return APIResponse(
            success=True,
            data=term,
            message=f"Business term '{request.name}' created successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create business term: {str(e)}")


@router.get("/business-terms", response_model=APIResponse[List[BusinessTerm]])
async def list_business_terms(
    search: Optional[str] = Query(None, description="Search in name or definition"),
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """List business glossary terms"""
    try:
        terms = list(catalog_manager.business_terms.values())
        
        # Filter by search if provided
        if search:
            search_lower = search.lower()
            terms = [
                term for term in terms
                if search_lower in term.name.lower() or search_lower in term.definition.lower()
            ]
            
        return APIResponse(
            success=True,
            data=terms,
            message=f"Found {len(terms)} business terms"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list business terms: {str(e)}")


@router.get("/datasets/{dataset_id}/impact", response_model=APIResponse[Dict[str, Any]])
async def get_impact_analysis(
    dataset_id: str,
    change_type: str = Query("schema", regex="^(schema|delete|rename)$"),
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Analyze impact of changes to a dataset"""
    try:
        impact = await catalog_manager.get_impact_analysis(
            dataset_id=dataset_id,
            change_type=change_type
        )
        
        return APIResponse(
            success=True,
            data=impact,
            message="Impact analysis completed"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to analyze impact: {str(e)}")


@router.get("/schemas/{dataset_id}", response_model=APIResponse[List[Schema]])
async def get_dataset_schemas(
    dataset_id: str,
    catalog_manager: CatalogManager = Depends(get_catalog_manager),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get all schema versions for a dataset"""
    try:
        schemas = [
            schema for schema in catalog_manager.schemas.values()
            if schema.dataset_id == dataset_id
        ]
        
        # Sort by version
        schemas.sort(key=lambda s: s.version)
        
        return APIResponse(
            success=True,
            data=schemas,
            message=f"Found {len(schemas)} schema versions"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get schemas: {str(e)}") 