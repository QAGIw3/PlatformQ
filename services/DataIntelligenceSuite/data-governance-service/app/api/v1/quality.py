"""
Quality API endpoints
"""
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel

# Import from common library
from data_intelligence_common.core.api.base_router import BaseRouter
from data_intelligence_common.core.api.response_models import (
    SuccessResponse,
    ErrorResponse,
    PaginatedResponse
)
from data_intelligence_common.core.api.request_validators import validate_entity_id

# Import domain models
from ...domain.models.quality import (
    QualityCheckRequest,
    EnhancedQualityProfile,
    QualityRuleDefinition,
    QualityIncident,
    RemediationAction,
    QualityMetricHistory,
    QualityDashboard
)
from ...core.container import Container
from ..dependencies import get_container, get_current_user


router = APIRouter()


class QualityCheckRequestModel(BaseModel):
    """API model for quality check request"""
    entity_id: str
    entity_type: str = "dataset"
    rule_ids: Optional[List[str]] = None
    check_types: Optional[List[str]] = None
    sample_size: Optional[int] = None
    async_execution: bool = False


class QualityRuleCreateModel(BaseModel):
    """API model for creating quality rule"""
    name: str
    description: Optional[str] = None
    rule_type: str
    configuration: Dict[str, Any]
    remediation_strategy: str = "alert_only"
    auto_fix_enabled: bool = False
    schedule_cron: Optional[str] = None
    owner: str


class RemediationTriggerModel(BaseModel):
    """API model for triggering remediation"""
    incident_id: str
    strategy: Optional[str] = None
    notes: Optional[str] = None


@router.post("/check", response_model=SuccessResponse)
async def run_quality_check(
    request: QualityCheckRequestModel,
    background_tasks: BackgroundTasks,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Run quality checks on an entity"""
    quality_engine = await container.quality_engine()
    
    # Validate entity exists
    catalog_client = await container.catalog_service_client()
    entity = await catalog_client.get_entity(request.entity_id)
    if not entity:
        raise HTTPException(status_code=404, detail=f"Entity {request.entity_id} not found")
    
    # Create check request
    check_request = QualityCheckRequest(
        entity_id=request.entity_id,
        entity_type=request.entity_type,
        rule_ids=request.rule_ids,
        check_types=request.check_types,
        sample_size=request.sample_size,
        async_execution=request.async_execution,
        triggered_by=current_user
    )
    
    if request.async_execution:
        # Run asynchronously
        background_tasks.add_task(
            quality_engine.check_quality,
            check_request
        )
        return SuccessResponse(
            message="Quality check started",
            data={"request_id": check_request.correlation_id}
        )
    else:
        # Run synchronously
        result = await quality_engine.check_quality(check_request)
        return SuccessResponse(
            message="Quality check completed",
            data=result.dict()
        )


@router.get("/profile/{entity_id}", response_model=SuccessResponse)
async def get_quality_profile(
    entity_id: str,
    container: Container = Depends(get_container)
):
    """Get latest quality profile for an entity"""
    quality_engine = await container.quality_engine()
    
    # Get from cache or storage
    profile = await quality_engine.get_latest_profile(entity_id)
    if not profile:
        raise HTTPException(status_code=404, detail=f"No quality profile found for {entity_id}")
    
    return SuccessResponse(
        message="Quality profile retrieved",
        data=profile.dict()
    )


@router.get("/history/{entity_id}", response_model=SuccessResponse)
async def get_quality_history(
    entity_id: str,
    metric_type: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    container: Container = Depends(get_container)
):
    """Get quality metric history for an entity"""
    quality_engine = await container.quality_engine()
    
    history = await quality_engine.get_quality_history(
        entity_id=entity_id,
        metric_type=metric_type,
        start_date=start_date,
        end_date=end_date
    )
    
    return SuccessResponse(
        message="Quality history retrieved",
        data={"history": history}
    )


@router.post("/rules", response_model=SuccessResponse)
async def create_quality_rule(
    rule: QualityRuleCreateModel,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Create a new quality rule"""
    quality_engine = await container.quality_engine()
    
    # Create rule definition
    rule_def = QualityRuleDefinition(
        name=rule.name,
        description=rule.description,
        rule_type=rule.rule_type,
        configuration=rule.configuration,
        remediation_strategy=rule.remediation_strategy,
        auto_fix_enabled=rule.auto_fix_enabled,
        schedule_cron=rule.schedule_cron,
        owner=rule.owner,
        created_by=current_user
    )
    
    rule_id = await quality_engine.create_rule(rule_def)
    
    return SuccessResponse(
        message="Quality rule created",
        data={"rule_id": rule_id}
    )


@router.get("/rules", response_model=PaginatedResponse)
async def list_quality_rules(
    rule_type: Optional[str] = None,
    owner: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    container: Container = Depends(get_container)
):
    """List quality rules"""
    quality_engine = await container.quality_engine()
    
    rules = await quality_engine.list_rules(
        rule_type=rule_type,
        owner=owner,
        offset=(page - 1) * page_size,
        limit=page_size
    )
    
    total = await quality_engine.count_rules(rule_type=rule_type, owner=owner)
    
    return PaginatedResponse(
        data=rules,
        page=page,
        page_size=page_size,
        total=total,
        pages=(total + page_size - 1) // page_size
    )


@router.get("/rules/{rule_id}", response_model=SuccessResponse)
async def get_quality_rule(
    rule_id: str,
    container: Container = Depends(get_container)
):
    """Get quality rule details"""
    quality_engine = await container.quality_engine()
    
    rule = quality_engine.rule_registry.get(rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Rule {rule_id} not found")
    
    return SuccessResponse(
        message="Rule retrieved",
        data=rule.dict()
    )


@router.put("/rules/{rule_id}", response_model=SuccessResponse)
async def update_quality_rule(
    rule_id: str,
    updates: Dict[str, Any],
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Update quality rule"""
    quality_engine = await container.quality_engine()
    
    await quality_engine.update_rule(rule_id, updates, updated_by=current_user)
    
    return SuccessResponse(
        message="Rule updated",
        data={"rule_id": rule_id}
    )


@router.delete("/rules/{rule_id}", response_model=SuccessResponse)
async def delete_quality_rule(
    rule_id: str,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Delete quality rule"""
    quality_engine = await container.quality_engine()
    
    await quality_engine.delete_rule(rule_id, deleted_by=current_user)
    
    return SuccessResponse(
        message="Rule deleted",
        data={"rule_id": rule_id}
    )


@router.get("/incidents", response_model=PaginatedResponse)
async def list_quality_incidents(
    entity_id: Optional[str] = None,
    severity: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    container: Container = Depends(get_container)
):
    """List quality incidents"""
    quality_engine = await container.quality_engine()
    
    incidents = await quality_engine.list_incidents(
        entity_id=entity_id,
        severity=severity,
        status=status,
        offset=(page - 1) * page_size,
        limit=page_size
    )
    
    total = await quality_engine.count_incidents(
        entity_id=entity_id,
        severity=severity,
        status=status
    )
    
    return PaginatedResponse(
        data=incidents,
        page=page,
        page_size=page_size,
        total=total,
        pages=(total + page_size - 1) // page_size
    )


@router.get("/incidents/{incident_id}", response_model=SuccessResponse)
async def get_quality_incident(
    incident_id: str,
    container: Container = Depends(get_container)
):
    """Get incident details"""
    quality_engine = await container.quality_engine()
    
    incident = quality_engine.active_incidents.get(incident_id)
    if not incident:
        # Try loading from storage
        incident = await quality_engine.load_incident(incident_id)
        if not incident:
            raise HTTPException(status_code=404, detail=f"Incident {incident_id} not found")
    
    return SuccessResponse(
        message="Incident retrieved",
        data=incident.dict()
    )


@router.post("/remediation/trigger", response_model=SuccessResponse)
async def trigger_remediation(
    request: RemediationTriggerModel,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Trigger remediation for an incident"""
    quality_engine = await container.quality_engine()
    
    action = await quality_engine.trigger_remediation(
        incident_id=request.incident_id,
        strategy=request.strategy
    )
    
    return SuccessResponse(
        message="Remediation triggered",
        data=action.dict()
    )


@router.get("/remediation/{action_id}", response_model=SuccessResponse)
async def get_remediation_status(
    action_id: str,
    container: Container = Depends(get_container)
):
    """Get remediation action status"""
    quality_engine = await container.quality_engine()
    
    action = await quality_engine.get_remediation_action(action_id)
    if not action:
        raise HTTPException(status_code=404, detail=f"Remediation action {action_id} not found")
    
    return SuccessResponse(
        message="Remediation status retrieved",
        data=action.dict()
    )


@router.get("/dashboards", response_model=SuccessResponse)
async def list_quality_dashboards(
    owner: Optional[str] = None,
    shared: bool = False,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """List quality dashboards"""
    quality_engine = await container.quality_engine()
    
    dashboards = await quality_engine.list_dashboards(
        owner=owner if owner else current_user,
        include_shared=shared
    )
    
    return SuccessResponse(
        message="Dashboards retrieved",
        data={"dashboards": dashboards}
    )


@router.post("/dashboards", response_model=SuccessResponse)
async def create_quality_dashboard(
    dashboard: QualityDashboard,
    container: Container = Depends(get_container),
    current_user: str = Depends(get_current_user)
):
    """Create quality dashboard"""
    quality_engine = await container.quality_engine()
    
    dashboard.owner = current_user
    dashboard_id = await quality_engine.create_dashboard(dashboard)
    
    return SuccessResponse(
        message="Dashboard created",
        data={"dashboard_id": dashboard_id}
    ) 