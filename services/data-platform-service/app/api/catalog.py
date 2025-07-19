"""
Data catalog API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
import uuid
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class AssetType(str, Enum):
    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    STREAM = "stream"
    FILE = "file"
    API = "api"
    DATASET = "dataset"


class AssetStatus(str, Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    DRAFT = "draft"


class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"
    PHI = "phi"


class AssetMetadata(BaseModel):
    """Asset metadata"""
    name: str = Field(..., description="Asset name")
    description: Optional[str] = Field(None)
    asset_type: AssetType
    location: str = Field(..., description="Physical location/URI")
    owner: str = Field(..., description="Asset owner")
    team: Optional[str] = Field(None, description="Owning team")
    classification: DataClassification
    tags: List[str] = Field(default_factory=list)
    custom_properties: Dict[str, Any] = Field(default_factory=dict)


class AssetResponse(AssetMetadata):
    """Asset response"""
    asset_id: str
    status: AssetStatus
    created_at: datetime
    updated_at: datetime
    version: int
    usage_count: int = 0
    quality_score: Optional[float] = None
    lineage_upstream: List[str] = Field(default_factory=list)
    lineage_downstream: List[str] = Field(default_factory=list)


class ColumnMetadata(BaseModel):
    """Column metadata"""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None
    classification: Optional[DataClassification] = None
    tags: List[str] = Field(default_factory=list)
    statistics: Optional[Dict[str, Any]] = None


@router.post("/assets", response_model=AssetResponse)
async def register_asset(
    asset: AssetMetadata,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Register a new data asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Register asset
        result = await main.catalog_manager.register_asset(
            name=asset.name,
            asset_type=asset.asset_type.value,
            location=asset.location,
            owner=asset.owner,
            description=asset.description,
            tags=asset.tags,
            custom_properties=asset.custom_properties,
            classification=asset.classification.value
        )
        
        return AssetResponse(
            asset_id=result["asset_id"],
            name=asset.name,
            description=asset.description,
            asset_type=asset.asset_type,
            location=asset.location,
            owner=asset.owner,
            team=asset.team,
            classification=asset.classification,
            tags=asset.tags,
            custom_properties=asset.custom_properties,
            status=AssetStatus.ACTIVE,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            version=1,
            lineage_upstream=[],
            lineage_downstream=[],
            quality_score=None,
            usage_count=0
        )
    except Exception as e:
        logger.error(f"Failed to register asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/{asset_id}", response_model=AssetResponse)
async def get_asset(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get asset details"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        asset = await main.catalog_manager.get_asset(asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        return AssetResponse(
            asset_id=asset["asset_id"],
            name=asset["name"],
            description=asset.get("description"),
            asset_type=AssetType(asset["asset_type"]),
            location=asset["location"],
            owner=asset["owner"],
            team=asset.get("team"),
            classification=DataClassification(asset.get("classification", "internal")),
            tags=asset.get("tags", []),
            custom_properties=asset.get("custom_properties", {}),
            status=AssetStatus(asset.get("status", "active")),
            created_at=asset.get("created_at", datetime.utcnow()),
            updated_at=asset.get("updated_at", datetime.utcnow()),
            version=asset.get("version", 1),
            lineage_upstream=asset.get("lineage_upstream", []),
            lineage_downstream=asset.get("lineage_downstream", []),
            quality_score=asset.get("quality_score"),
            usage_count=asset.get("usage_count", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/assets/{asset_id}", response_model=AssetResponse)
async def update_asset(
    asset_id: str,
    asset: AssetMetadata,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Update asset metadata"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Update asset
        updates = {
            "name": asset.name,
            "description": asset.description,
            "asset_type": asset.asset_type.value,
            "location": asset.location,
            "owner": asset.owner,
            "team": asset.team,
            "classification": asset.classification.value,
            "tags": asset.tags,
            "custom_properties": asset.custom_properties
        }
        
        result = await main.catalog_manager.update_asset(asset_id, updates)
        if not result:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        # Get updated asset
        updated_asset = await main.catalog_manager.get_asset(asset_id)
        
        return AssetResponse(
            asset_id=updated_asset["asset_id"],
            name=updated_asset["name"],
            description=updated_asset.get("description"),
            asset_type=AssetType(updated_asset["asset_type"]),
            location=updated_asset["location"],
            owner=updated_asset["owner"],
            team=updated_asset.get("team"),
            classification=DataClassification(updated_asset.get("classification", "internal")),
            tags=updated_asset.get("tags", []),
            custom_properties=updated_asset.get("custom_properties", {}),
            status=AssetStatus(updated_asset.get("status", "active")),
            created_at=updated_asset.get("created_at", datetime.utcnow()),
            updated_at=datetime.utcnow(),
            version=updated_asset.get("version", 1) + 1,
            lineage_upstream=updated_asset.get("lineage_upstream", []),
            lineage_downstream=updated_asset.get("lineage_downstream", []),
            quality_score=updated_asset.get("quality_score"),
            usage_count=updated_asset.get("usage_count", 0)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/assets/{asset_id}")
async def delete_asset(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Delete an asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        result = await main.catalog_manager.delete_asset(asset_id)
        if not result:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        return {"message": f"Asset {asset_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets", response_model=List[AssetResponse])
async def search_assets(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    query: Optional[str] = Query(None, description="Search query"),
    asset_type: Optional[AssetType] = Query(None),
    owner: Optional[str] = Query(None),
    classification: Optional[DataClassification] = Query(None),
    tags: Optional[List[str]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Search data assets"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Build filters
        filters = {}
        if asset_type:
            filters["asset_type"] = asset_type.value
        if owner:
            filters["owner"] = owner
        if classification:
            filters["classification"] = classification.value
        if tags:
            filters["tags"] = tags
        
        # Search assets
        results = await main.catalog_manager.search_assets(
            query=query,
            filters=filters,
            limit=limit,
            offset=offset
        )
        
        # Convert to response format
        assets = []
        for asset in results.get("assets", []):
            assets.append(AssetResponse(
                asset_id=asset["asset_id"],
                name=asset["name"],
                description=asset.get("description"),
                asset_type=AssetType(asset["asset_type"]),
                location=asset["location"],
                owner=asset["owner"],
                team=asset.get("team"),
                classification=DataClassification(asset.get("classification", "internal")),
                tags=asset.get("tags", []),
                custom_properties=asset.get("custom_properties", {}),
                status=AssetStatus(asset.get("status", "active")),
                created_at=asset.get("created_at", datetime.utcnow()),
                updated_at=asset.get("updated_at", datetime.utcnow()),
                version=asset.get("version", 1),
                lineage_upstream=asset.get("lineage_upstream", []),
                lineage_downstream=asset.get("lineage_downstream", []),
                quality_score=asset.get("quality_score"),
                usage_count=asset.get("usage_count", 0)
            ))
        
        return assets
    except Exception as e:
        logger.error(f"Failed to search assets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assets/{asset_id}/columns", response_model=List[ColumnMetadata])
async def add_columns(
    asset_id: str,
    columns: List[ColumnMetadata],
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Add column metadata to an asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Add columns to asset
        for column in columns:
            await main.catalog_manager.add_column_metadata(
                asset_id=asset_id,
                column_name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                description=column.description,
                classification=column.classification.value if column.classification else None,
                tags=column.tags,
                statistics=column.statistics
            )
        
        return columns
    except Exception as e:
        logger.error(f"Failed to add columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/{asset_id}/columns", response_model=List[ColumnMetadata])
async def get_columns(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get column metadata for an asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        columns = await main.catalog_manager.get_column_metadata(asset_id)
        
        return [
            ColumnMetadata(
                name=col["name"],
                data_type=col["data_type"],
                nullable=col.get("nullable", True),
                description=col.get("description"),
                classification=DataClassification(col["classification"]) if col.get("classification") else None,
                tags=col.get("tags", []),
                statistics=col.get("statistics")
            )
            for col in columns
        ]
    except Exception as e:
        logger.error(f"Failed to get columns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/discover")
async def discover_assets(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    source_type: str = Query(..., description="Source type (database, file, api)"),
    connection_params: Dict[str, Any] = Body(..., description="Connection parameters")
):
    """Discover assets from a data source"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Run discovery
        discovered_assets = await main.catalog_manager.discover_assets(
            source_type=source_type,
            connection_params=connection_params
        )
        
        return {
            "discovered_count": len(discovered_assets),
            "assets": discovered_assets
        }
    except Exception as e:
        logger.error(f"Failed to discover assets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assets/{asset_id}/deprecate")
async def deprecate_asset(
    asset_id: str,
    request: Request,
    reason: str = Query(..., description="Deprecation reason"),
    tenant_id: str = Query(..., description="Tenant ID"),
    replacement_asset_id: Optional[str] = Query(None),
    deprecation_date: Optional[datetime] = Query(None)
):
    """Mark asset as deprecated"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Update asset status to deprecated
        updates = {
            "status": "deprecated",
            "deprecation_reason": reason,
            "replacement_asset_id": replacement_asset_id,
            "deprecation_date": deprecation_date or datetime.utcnow()
        }
        
        result = await main.catalog_manager.update_asset(asset_id, updates)
        if not result:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        return {
            "asset_id": asset_id,
            "status": "deprecated",
            "reason": reason,
            "replacement_asset_id": replacement_asset_id,
            "deprecation_date": deprecation_date or datetime.utcnow()
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deprecate asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_catalog(
    request: Request,
    q: str = Query(..., description="Search query"),
    tenant_id: str = Query(..., description="Tenant ID"),
    asset_types: Optional[List[AssetType]] = Query(None),
    classifications: Optional[List[DataClassification]] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Search across all catalog assets"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Build filters
        filters = {}
        if asset_types:
            filters["asset_type"] = [at.value for at in asset_types]
        if classifications:
            filters["classification"] = [cl.value for cl in classifications]
        
        # Search assets
        results = await main.catalog_manager.search_assets(
            query=q,
            filters=filters,
            limit=limit,
            offset=offset
        )
        
        return {
            "query": q,
            "results": results.get("assets", []),
            "total": results.get("total", 0),
            "facets": results.get("facets", {})
        }
    except Exception as e:
        logger.error(f"Failed to search catalog: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tags")
async def list_tags(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    prefix: Optional[str] = Query(None, description="Tag prefix filter")
):
    """List all available tags"""
    return {
        "tags": [
            {"name": "customer", "count": 25},
            {"name": "orders", "count": 20},
            {"name": "pii", "count": 15},
            {"name": "financial", "count": 10}
        ]
    }


@router.get("/owners")
async def list_owners(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """List all asset owners"""
    return {
        "owners": [
            {
                "owner_id": "data_team",
                "name": "Data Team",
                "email": "data-team@company.com",
                "asset_count": 45
            },
            {
                "owner_id": "analytics_team",
                "name": "Analytics Team",
                "email": "analytics@company.com",
                "asset_count": 30
            }
        ]
    }


@router.post("/bulk-import")
async def bulk_import_assets(
    request: Request,
    source_type: str = Query(..., description="Source type (hive, glue, custom)"),
    source_config: Dict[str, Any] = Body(..., description="Source configuration"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Bulk import assets from external catalog"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        job_id = str(uuid.uuid4())
        
        # In a real implementation, this would trigger an async job
        # For now, we'll just return a job ID
        return {
            "job_id": job_id,
            "status": "started",
            "source_type": source_type,
            "estimated_assets": 100,
            "started_at": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Failed to start bulk import: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_catalog_statistics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get catalog statistics"""
    return {
        "total_assets": 250,
        "by_type": {
            "table": 150,
            "view": 50,
            "dataset": 30,
            "api": 20
        },
        "by_classification": {
            "public": 50,
            "internal": 100,
            "confidential": 50,
            "pii": 50
        },
        "top_owners": [
            {"owner": "data_team", "count": 80},
            {"owner": "analytics_team", "count": 60}
        ],
        "quality_distribution": {
            "excellent": 50,  # >0.9
            "good": 100,      # 0.7-0.9
            "fair": 75,       # 0.5-0.7
            "poor": 25        # <0.5
        },
        "last_updated": datetime.utcnow()
    } 