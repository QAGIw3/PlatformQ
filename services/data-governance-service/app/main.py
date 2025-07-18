"""
Data Governance Service

Provides data governance, quality monitoring, lineage tracking, and compliance management.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
import networkx as nx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db

from .models import (
    DataAsset,
    DataLineage,
    DataQualityProfile,
    DataPolicy,
    ComplianceCheck,
    AssetType,
    PolicyType,
    ComplianceType,
    ComplianceStatus,
    DataClassification
)
from .repository import (
    DataAssetRepository,
    DataLineageRepository,
    DataQualityMetricRepository,
    DataQualityProfileRepository,
    DataPolicyRepository,
    AccessPolicyRepository,
    ComplianceCheckRepository,
    DataCatalogRepository
)
from .event_processors import DataGovernanceEventProcessor

# Setup logging
logger = logging.getLogger(__name__)

# Global instances
event_processor = None
scheduler = AsyncIOScheduler()
lineage_graph = nx.DiGraph()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor
    
    # Startup
    logger.info("Initializing Data Governance Service...")
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_processor = DataGovernanceEventProcessor(
        pulsar_client=pulsar_client,
        service_name="data-governance-service"
    )
    
    # Start event processor
    await event_processor.start()
    
    # Start scheduler for periodic tasks
    scheduler.add_job(
        run_daily_quality_checks,
        CronTrigger(hour=2, minute=0),  # Run at 2 AM
        id="daily_quality_checks"
    )
    
    scheduler.add_job(
        run_weekly_compliance_checks,
        CronTrigger(day_of_week="sun", hour=3, minute=0),  # Run Sunday at 3 AM
        id="weekly_compliance_checks"
    )
    
    scheduler.start()
    
    logger.info("Data Governance Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Governance Service...")
    
    scheduler.shutdown()
    
    if event_processor:
        await event_processor.stop()
    
    logger.info("Data Governance Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="PlatformQ Data Governance Service",
    description="Data governance, quality monitoring, and compliance management",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Pydantic models for API requests/responses
class AssetRegistrationRequest(BaseModel):
    asset_name: str
    asset_type: AssetType
    catalog: str
    schema: str
    table: Optional[str] = None
    owner: str
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PolicyRequest(BaseModel):
    policy_name: str
    policy_type: PolicyType
    description: Optional[str] = None
    rules: List[Dict[str, Any]]
    applies_to: Optional[List[Dict[str, Any]]] = None
    enforcement_level: str = "enforce"  # enforce, warn, monitor


class QualityProfileRequest(BaseModel):
    asset_id: str
    profile_type: str = "full"  # full, sample, incremental
    sample_size: Optional[int] = 10000


class ComplianceCheckRequest(BaseModel):
    asset_id: str
    compliance_types: Optional[List[ComplianceType]] = None


class LineageUpdateRequest(BaseModel):
    source_asset_id: str
    target_asset_id: str
    transformation_type: str
    transformation_details: Optional[Dict[str, Any]] = None
    pipeline_id: Optional[str] = None


# API Endpoints

@app.post("/api/v1/assets/register", response_model=Dict[str, Any])
async def register_asset(
    request: AssetRegistrationRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")  # TODO: Get from auth
):
    """Register a new data asset in the governance system"""
    try:
        asset_repo = DataAssetRepository(db)
        
        # Check if asset already exists
        existing = asset_repo.query().filter_by(
            catalog=request.catalog,
            schema=request.schema,
            table=request.table,
            tenant_id=tenant_id
        ).first()
        
        if existing:
            raise HTTPException(
                status_code=400,
                detail="Asset already registered"
            )
        
        # Create asset
        asset = asset_repo.create({
            "asset_id": f"{request.catalog}.{request.schema}.{request.table or request.asset_name}",
            "tenant_id": tenant_id,
            "asset_name": request.asset_name,
            "asset_type": request.asset_type,
            "catalog": request.catalog,
            "schema": request.schema,
            "table": request.table,
            "owner": request.owner,
            "description": request.description,
            "tags": request.tags,
            "metadata": request.metadata
        })
        
        # Publish asset created event
        event_publisher = EventPublisher()
        await event_publisher.publish_event(
            {
                "asset_id": asset.asset_id,
                "asset_type": asset.asset_type.value,
                "tenant_id": tenant_id,
                "metadata": {
                    "name": asset.asset_name,
                    "catalog": asset.catalog,
                    "schema": asset.schema,
                    "table": asset.table,
                    "owner": asset.owner,
                    "tags": asset.tags
                }
            },
            "persistent://public/default/asset-events"
        )
        
        return {
            "asset_id": asset.asset_id,
            "status": "registered",
            "created_at": asset.created_at.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/assets/search", response_model=List[Dict[str, Any]])
async def search_assets(
    query: Optional[str] = None,
    asset_type: Optional[AssetType] = None,
    classification: Optional[DataClassification] = None,
    owner: Optional[str] = None,
    tags: Optional[List[str]] = None,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Search for data assets"""
    try:
        asset_repo = DataAssetRepository(db)
        
        assets = asset_repo.search_assets(
            tenant_id=tenant_id,
            query=query,
            asset_type=asset_type,
            classification=classification,
            owner=owner,
            tags=tags
        )
        
        return [
            {
                "asset_id": asset.asset_id,
                "asset_name": asset.asset_name,
                "asset_type": asset.asset_type.value,
                "catalog": asset.catalog,
                "schema": asset.schema,
                "table": asset.table,
                "owner": asset.owner,
                "classification": asset.classification.value,
                "tags": asset.tags,
                "created_at": asset.created_at.isoformat()
            }
            for asset in assets
        ]
        
    except Exception as e:
        logger.error(f"Error searching assets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/quality/profile", response_model=Dict[str, Any])
async def create_quality_profile(
    request: QualityProfileRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Create a quality profile for an asset"""
    try:
        # Publish quality check request
        event_publisher = EventPublisher()
        await event_publisher.publish_event(
            {
                "asset_id": request.asset_id,
                "check_type": request.profile_type,
                "tenant_id": tenant_id,
                "metadata": {
                    "sample_size": request.sample_size
                }
            },
            "persistent://public/default/data-quality-events"
        )
        
        return {
            "status": "profiling_started",
            "asset_id": request.asset_id,
            "profile_type": request.profile_type
        }
        
    except Exception as e:
        logger.error(f"Error creating quality profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/quality/profile/{asset_id}", response_model=Dict[str, Any])
async def get_quality_profile(
    asset_id: str,
    db: Session = Depends(get_db)
):
    """Get the latest quality profile for an asset"""
    try:
        profile_repo = DataQualityProfileRepository(db)
        
        profile = profile_repo.get_latest_profile(asset_id)
        if not profile:
            raise HTTPException(status_code=404, detail="No quality profile found")
        
        return {
            "asset_id": profile.asset_id,
            "profile_date": profile.profile_date.isoformat(),
            "row_count": profile.row_count,
            "scores": {
                "completeness": profile.completeness_score,
                "validity": profile.validity_score,
                "consistency": profile.consistency_score,
                "accuracy": profile.accuracy_score,
                "uniqueness": profile.uniqueness_score,
                "timeliness": profile.timeliness_score,
                "overall": profile.overall_score
            },
            "column_profiles": profile.column_profiles,
            "anomalies": profile.anomalies
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting quality profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/policies", response_model=Dict[str, Any])
async def create_policy(
    request: PolicyRequest,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Create a new data governance policy"""
    try:
        policy_repo = DataPolicyRepository(db)
        
        policy = policy_repo.create({
            "policy_id": f"policy-{datetime.utcnow().timestamp()}",
            "tenant_id": tenant_id,
            "policy_name": request.policy_name,
            "policy_type": request.policy_type,
            "description": request.description,
            "rules": request.rules,
            "applies_to": request.applies_to,
            "enforcement_level": request.enforcement_level,
            "active": True,
            "created_by": "user"  # TODO: Get from auth
        })
        
        return {
            "policy_id": policy.policy_id,
            "policy_name": policy.policy_name,
            "status": "created",
            "created_at": policy.created_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/policies", response_model=List[Dict[str, Any]])
async def list_policies(
    policy_type: Optional[PolicyType] = None,
    active_only: bool = True,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """List data governance policies"""
    try:
        policy_repo = DataPolicyRepository(db)
        
        if active_only:
            policies = policy_repo.get_active_policies(tenant_id, policy_type)
        else:
            query = policy_repo.query().filter_by(tenant_id=tenant_id)
            if policy_type:
                query = query.filter_by(policy_type=policy_type)
            policies = query.all()
        
        return [
            {
                "policy_id": p.policy_id,
                "policy_name": p.policy_name,
                "policy_type": p.policy_type.value,
                "description": p.description,
                "active": p.active,
                "enforcement_level": p.enforcement_level,
                "created_at": p.created_at.isoformat()
            }
            for p in policies
        ]
        
    except Exception as e:
        logger.error(f"Error listing policies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/compliance/check", response_model=Dict[str, Any])
async def run_compliance_check(
    request: ComplianceCheckRequest,
    background_tasks: BackgroundTasks,
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Run compliance checks on an asset"""
    try:
        # Publish compliance check request
        event_publisher = EventPublisher()
        await event_publisher.publish_event(
            {
                "asset_id": request.asset_id,
                "compliance_types": [ct.value for ct in request.compliance_types] if request.compliance_types else None,
                "tenant_id": tenant_id
            },
            "persistent://public/default/compliance-events"
        )
        
        return {
            "status": "compliance_check_started",
            "asset_id": request.asset_id,
            "compliance_types": request.compliance_types
        }
        
    except Exception as e:
        logger.error(f"Error running compliance check: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/compliance/summary", response_model=Dict[str, Any])
async def get_compliance_summary(
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Get compliance summary for the tenant"""
    try:
        compliance_repo = ComplianceCheckRepository(db)
        
        summary = compliance_repo.get_compliance_summary(tenant_id)
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting compliance summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/lineage", response_model=Dict[str, Any])
async def update_lineage(
    request: LineageUpdateRequest,
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Update data lineage"""
    try:
        # Publish lineage update event
        event_publisher = EventPublisher()
        await event_publisher.publish_event(
            {
                "source_asset_id": request.source_asset_id,
                "target_asset_id": request.target_asset_id,
                "transformation_type": request.transformation_type,
                "transformation_details": request.transformation_details,
                "pipeline_id": request.pipeline_id,
                "tenant_id": tenant_id,
                "execution_time": datetime.utcnow().isoformat()
            },
            "persistent://public/default/lineage-events"
        )
        
        # Update local lineage graph
        lineage_graph.add_edge(
            request.source_asset_id,
            request.target_asset_id,
            transformation=request.transformation_type
        )
        
        return {
            "status": "lineage_updated",
            "source": request.source_asset_id,
            "target": request.target_asset_id
        }
        
    except Exception as e:
        logger.error(f"Error updating lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/lineage/{asset_id}", response_model=Dict[str, Any])
async def get_asset_lineage(
    asset_id: str,
    depth: int = 3,
    db: Session = Depends(get_db)
):
    """Get lineage for an asset"""
    try:
        lineage_repo = DataLineageRepository(db)
        
        lineage = lineage_repo.get_asset_lineage(asset_id, depth)
        
        # Also get from graph for visualization
        ancestors = list(nx.ancestors(lineage_graph, asset_id)) if asset_id in lineage_graph else []
        descendants = list(nx.descendants(lineage_graph, asset_id)) if asset_id in lineage_graph else []
        
        return {
            "asset_id": asset_id,
            "lineage": lineage,
            "graph": {
                "ancestors": ancestors,
                "descendants": descendants
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/dashboard", response_model=Dict[str, Any])
async def get_governance_dashboard(
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")
):
    """Get governance dashboard data"""
    try:
        asset_repo = DataAssetRepository(db)
        profile_repo = DataQualityProfileRepository(db)
        compliance_repo = ComplianceCheckRepository(db)
        policy_repo = DataPolicyRepository(db)
        
        # Get asset statistics
        total_assets = asset_repo.query().filter_by(tenant_id=tenant_id).count()
        sensitive_assets = len(asset_repo.get_sensitive_assets(tenant_id))
        
        # Get quality statistics
        low_quality = profile_repo.get_low_quality_assets(tenant_id)
        
        # Get compliance summary
        compliance_summary = compliance_repo.get_compliance_summary(tenant_id)
        
        # Get policy counts
        active_policies = len(policy_repo.get_active_policies(tenant_id))
        
        return {
            "assets": {
                "total": total_assets,
                "sensitive": sensitive_assets,
                "classification_rate": (sensitive_assets / total_assets * 100) if total_assets > 0 else 0
            },
            "quality": {
                "low_quality_assets": len(low_quality),
                "average_score": sum(a["overall_score"] for a in low_quality) / len(low_quality) if low_quality else 0
            },
            "compliance": compliance_summary,
            "policies": {
                "active": active_policies
            },
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Scheduled tasks
async def run_daily_quality_checks():
    """Run daily quality checks on all assets"""
    db = next(get_db())
    try:
        asset_repo = DataAssetRepository(db)
        event_publisher = EventPublisher()
        
        assets = asset_repo.query().all()
        for asset in assets:
            await event_publisher.publish_event(
                {
                    "asset_id": asset.asset_id,
                    "check_type": "incremental",
                    "tenant_id": asset.tenant_id
                },
                "persistent://public/default/data-quality-events"
            )
            
        logger.info(f"Triggered quality checks for {len(assets)} assets")
    except Exception as e:
        logger.error(f"Error in daily quality checks: {e}")
    finally:
        db.close()


async def run_weekly_compliance_checks():
    """Run weekly compliance checks"""
    db = next(get_db())
    try:
        asset_repo = DataAssetRepository(db)
        event_publisher = EventPublisher()
        
        # Get sensitive assets
        sensitive_assets = asset_repo.get_sensitive_assets("default-tenant")  # TODO: Multi-tenant
        
        for asset in sensitive_assets:
            await event_publisher.publish_event(
                {
                    "asset_id": asset.asset_id,
                    "compliance_types": ["gdpr", "ccpa"],  # Check main privacy regulations
                    "tenant_id": asset.tenant_id
                },
                "persistent://public/default/compliance-events"
            )
            
        logger.info(f"Triggered compliance checks for {len(sensitive_assets)} sensitive assets")
    except Exception as e:
        logger.error(f"Error in weekly compliance checks: {e}")
    finally:
        db.close()


# Health check handled by base service 