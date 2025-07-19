"""
Data catalog API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
import uuid

router = APIRouter()


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
    lineage_upstream: List[str] = Field(default_factory=list)
    lineage_downstream: List[str] = Field(default_factory=list)
    quality_score: Optional[float] = None
    usage_count: int = 0


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
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Register a new data asset"""
    asset_id = str(uuid.uuid4())
    
    # Get catalog manager from app state
    catalog_manager = request.app.state.catalog_manager
    
    # In production, would register in catalog
    
    return AssetResponse(
        **asset.dict(),
        asset_id=asset_id,
        status=AssetStatus.ACTIVE,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        version=1
    )


@router.get("/assets", response_model=List[AssetResponse])
async def list_assets(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    asset_type: Optional[AssetType] = Query(None),
    classification: Optional[DataClassification] = Query(None),
    owner: Optional[str] = Query(None),
    tags: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None, description="Search in name/description"),
    include_deprecated: bool = Query(False),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List data assets with filtering"""
    # Mock response
    return [
        AssetResponse(
            asset_id="asset_123",
            name="customer_orders",
            description="Customer order data",
            asset_type=AssetType.TABLE,
            location="hive.analytics.customer_orders",
            owner="data_team",
            team="analytics",
            classification=DataClassification.INTERNAL,
            tags=["orders", "customers"],
            custom_properties={"refresh_frequency": "daily"},
            status=AssetStatus.ACTIVE,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            version=1,
            quality_score=0.95,
            usage_count=150
        )
    ]


@router.get("/assets/{asset_id}", response_model=AssetResponse)
async def get_asset(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get asset details"""
    # In production, would fetch from catalog
    raise HTTPException(status_code=404, detail="Asset not found")


@router.patch("/assets/{asset_id}")
async def update_asset(
    asset_id: str,
    request: Request,
    description: Optional[str] = Query(None),
    owner: Optional[str] = Query(None),
    classification: Optional[DataClassification] = Query(None),
    tags: Optional[List[str]] = Query(None),
    custom_properties: Optional[Dict[str, Any]] = None,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Update asset metadata"""
    return {
        "asset_id": asset_id,
        "status": "updated",
        "version": 2,
        "updated_at": datetime.utcnow()
    }


@router.post("/assets/{asset_id}/columns")
async def add_columns(
    asset_id: str,
    columns: List[ColumnMetadata],
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Add or update column metadata"""
    return {
        "asset_id": asset_id,
        "columns_added": len(columns),
        "status": "success"
    }


@router.get("/assets/{asset_id}/columns")
async def get_columns(
    asset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get column metadata"""
    return {
        "asset_id": asset_id,
        "columns": [
            {
                "name": "id",
                "data_type": "bigint",
                "nullable": False,
                "description": "Unique identifier",
                "classification": "internal",
                "tags": ["primary_key"],
                "statistics": {
                    "distinct_count": 1000000,
                    "null_count": 0
                }
            },
            {
                "name": "email",
                "data_type": "varchar(255)",
                "nullable": True,
                "description": "Customer email",
                "classification": "pii",
                "tags": ["email", "pii"],
                "statistics": {
                    "distinct_count": 950000,
                    "null_count": 50000
                }
            }
        ]
    }


@router.post("/assets/{asset_id}/deprecate")
async def deprecate_asset(
    asset_id: str,
    reason: str = Query(..., description="Deprecation reason"),
    replacement_asset_id: Optional[str] = Query(None),
    deprecation_date: Optional[datetime] = Query(None),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Mark asset as deprecated"""
    return {
        "asset_id": asset_id,
        "status": "deprecated",
        "reason": reason,
        "replacement_asset_id": replacement_asset_id,
        "deprecation_date": deprecation_date or datetime.utcnow(),
        "deprecated_by": user_id
    }


@router.get("/search")
async def search_catalog(
    q: str = Query(..., description="Search query"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    asset_types: Optional[List[AssetType]] = Query(None),
    classifications: Optional[List[DataClassification]] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Search across all catalog assets"""
    return {
        "query": q,
        "results": [
            {
                "asset_id": "asset_123",
                "name": "customer_orders",
                "asset_type": "table",
                "description": "Customer order data",
                "score": 0.95,
                "highlights": {
                    "description": ["Customer <em>order</em> data"]
                }
            }
        ],
        "total": 1,
        "facets": {
            "asset_type": {
                "table": 15,
                "view": 5,
                "dataset": 3
            },
            "classification": {
                "internal": 10,
                "pii": 8,
                "public": 5
            }
        }
    }


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
    source_type: str = Query(..., description="Source type (hive, glue, custom)"),
    source_config: Dict[str, Any] = Query(..., description="Source configuration"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Bulk import assets from external catalog"""
    job_id = str(uuid.uuid4())
    
    return {
        "job_id": job_id,
        "status": "started",
        "source_type": source_type,
        "estimated_assets": 100,
        "started_at": datetime.utcnow()
    }


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