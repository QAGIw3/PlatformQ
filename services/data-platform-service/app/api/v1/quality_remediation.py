"""
Data Quality Remediation API Endpoints

Provides endpoints for managing automated data quality remediation.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID

from app.core.deps import get_current_user, get_db
from app.quality.remediation_orchestrator import (
    DataQualityRemediationOrchestrator,
    RemediationPlan,
    RemediationResult
)
from app.core.data_quality import DataQuality
from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher

logger = get_logger(__name__)

router = APIRouter()


@router.post("/remediation/plans", response_model=Dict[str, Any])
async def create_remediation_plan(
    dataset_id: str,
    quality_assessment_id: Optional[str] = None,
    background_tasks: BackgroundTasks = BackgroundTasks(),
    current_user: Dict = Depends(get_current_user),
    db = Depends(get_db)
):
    """
    Create a remediation plan for data quality issues.
    
    Args:
        dataset_id: ID of the dataset to remediate
        quality_assessment_id: Optional ID of a specific quality assessment
        
    Returns:
        Remediation plan details
    """
    try:
        # Initialize orchestrator
        cache = IgniteClient()
        event_publisher = EventPublisher()
        orchestrator = DataQualityRemediationOrchestrator(cache, event_publisher)
        
        # Get latest quality assessment if not specified
        if not quality_assessment_id:
            quality_engine = DataQuality()
            assessment = await quality_engine.get_latest_assessment(dataset_id)
        else:
            assessment = await cache.get(f"quality:assessment:{quality_assessment_id}")
            
        if not assessment:
            raise HTTPException(status_code=404, detail="Quality assessment not found")
        
        # Create remediation plan
        plan = await orchestrator.create_remediation_plan(dataset_id, assessment)
        
        # If auto-remediation is enabled and doesn't require approval, execute in background
        if not plan.requires_approval and assessment.get("auto_remediate", False):
            background_tasks.add_task(
                orchestrator.execute_remediation_plan,
                plan.plan_id,
                current_user["user_id"]
            )
        
        return {
            "plan_id": plan.plan_id,
            "dataset_id": plan.dataset_id,
            "severity": plan.severity.value,
            "actions": [a.value for a in plan.actions],
            "requires_approval": plan.requires_approval,
            "estimated_duration_minutes": plan.estimated_duration.total_seconds() / 60,
            "created_at": plan.created_at.isoformat(),
            "quality_issues": plan.quality_issues,
            "auto_execution": not plan.requires_approval and assessment.get("auto_remediate", False)
        }
        
    except Exception as e:
        logger.error(f"Failed to create remediation plan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/remediation/plans/{plan_id}", response_model=Dict[str, Any])
async def get_remediation_plan(
    plan_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get details of a specific remediation plan."""
    try:
        cache = IgniteClient()
        plan_data = await cache.get(f"remediation:plan:{plan_id}")
        
        if not plan_data:
            raise HTTPException(status_code=404, detail="Remediation plan not found")
        
        return plan_data
        
    except Exception as e:
        logger.error(f"Failed to get remediation plan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediation/plans/{plan_id}/approve", response_model=Dict[str, Any])
async def approve_remediation_plan(
    plan_id: str,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """Approve a remediation plan that requires approval."""
    try:
        cache = IgniteClient()
        event_publisher = EventPublisher()
        orchestrator = DataQualityRemediationOrchestrator(cache, event_publisher)
        
        # Approve the plan
        plan = await orchestrator.approve_remediation_plan(plan_id, current_user["user_id"])
        
        # Execute in background
        background_tasks.add_task(
            orchestrator.execute_remediation_plan,
            plan_id,
            current_user["user_id"]
        )
        
        return {
            "plan_id": plan.plan_id,
            "approved_by": plan.approved_by,
            "approved_at": plan.approved_at.isoformat(),
            "status": "approved_and_executing"
        }
        
    except Exception as e:
        logger.error(f"Failed to approve remediation plan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediation/plans/{plan_id}/execute", response_model=Dict[str, Any])
async def execute_remediation_plan(
    plan_id: str,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """Manually execute a remediation plan."""
    try:
        cache = IgniteClient()
        event_publisher = EventPublisher()
        orchestrator = DataQualityRemediationOrchestrator(cache, event_publisher)
        
        # Check if plan exists
        plan_data = await cache.get(f"remediation:plan:{plan_id}")
        if not plan_data:
            raise HTTPException(status_code=404, detail="Remediation plan not found")
        
        # Execute in background
        background_tasks.add_task(
            orchestrator.execute_remediation_plan,
            plan_id,
            current_user["user_id"]
        )
        
        return {
            "plan_id": plan_id,
            "status": "execution_started",
            "triggered_by": current_user["user_id"],
            "triggered_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to execute remediation plan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/remediation/history/{dataset_id}", response_model=List[Dict[str, Any]])
async def get_remediation_history(
    dataset_id: str,
    limit: int = Query(default=10, ge=1, le=100),
    current_user: Dict = Depends(get_current_user)
):
    """Get remediation history for a dataset."""
    try:
        cache = IgniteClient()
        
        # Get all remediation plans for dataset
        # In production, this would query a proper database
        history = []
        
        # Scan cache for plans (simplified - would use proper query in production)
        pattern = f"remediation:plan:*{dataset_id}*"
        # Note: This is a simplified approach. In production, use proper indexing
        
        return history
        
    except Exception as e:
        logger.error(f"Failed to get remediation history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/remediation/recommendations/{dataset_id}", response_model=List[Dict[str, Any]])
async def get_remediation_recommendations(
    dataset_id: str,
    issue_type: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get ML-based remediation recommendations for specific issue types."""
    try:
        cache = IgniteClient()
        event_publisher = EventPublisher()
        orchestrator = DataQualityRemediationOrchestrator(cache, event_publisher)
        
        recommendations = await orchestrator.get_remediation_recommendations(
            dataset_id, issue_type
        )
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Failed to get remediation recommendations: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediation/rules", response_model=Dict[str, Any])
async def create_remediation_rule(
    rule_config: Dict[str, Any],
    current_user: Dict = Depends(get_current_user)
):
    """
    Create a custom remediation rule.
    
    Args:
        rule_config: Configuration for the remediation rule including:
            - issue_type: Type of quality issue
            - actions: List of remediation actions
            - conditions: Conditions for triggering
            - severity: Severity threshold
            
    Returns:
        Created rule details
    """
    try:
        cache = IgniteClient()
        
        # Validate rule configuration
        required_fields = ["issue_type", "actions", "severity"]
        for field in required_fields:
            if field not in rule_config:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Create rule
        rule_id = f"rule_{rule_config['issue_type']}_{datetime.utcnow().timestamp()}"
        rule = {
            "rule_id": rule_id,
            "created_by": current_user["user_id"],
            "created_at": datetime.utcnow().isoformat(),
            **rule_config
        }
        
        # Store rule
        await cache.put(f"remediation:rule:{rule_id}", rule)
        
        return rule
        
    except Exception as e:
        logger.error(f"Failed to create remediation rule: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/remediation/rules", response_model=List[Dict[str, Any]])
async def list_remediation_rules(
    issue_type: Optional[str] = None,
    current_user: Dict = Depends(get_current_user)
):
    """List all remediation rules, optionally filtered by issue type."""
    try:
        cache = IgniteClient()
        
        # Get all rules (simplified - would use proper query in production)
        rules = []
        
        # Filter by issue type if specified
        if issue_type:
            rules = [r for r in rules if r.get("issue_type") == issue_type]
        
        return rules
        
    except Exception as e:
        logger.error(f"Failed to list remediation rules: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/remediation/status/{plan_id}", response_model=Dict[str, Any])
async def get_remediation_status(
    plan_id: str,
    current_user: Dict = Depends(get_current_user)
):
    """Get real-time status of a remediation plan execution."""
    try:
        cache = IgniteClient()
        
        # Get plan
        plan_data = await cache.get(f"remediation:plan:{plan_id}")
        if not plan_data:
            raise HTTPException(status_code=404, detail="Remediation plan not found")
        
        # Get execution status
        execution_status = await cache.get(f"remediation:execution:{plan_id}")
        
        return {
            "plan_id": plan_id,
            "execution_status": plan_data.get("execution_status", "pending"),
            "progress": execution_status.get("progress", 0) if execution_status else 0,
            "current_action": execution_status.get("current_action") if execution_status else None,
            "actions_completed": execution_status.get("actions_completed", []) if execution_status else [],
            "estimated_completion": execution_status.get("estimated_completion") if execution_status else None,
            "execution_result": plan_data.get("execution_result")
        }
        
    except Exception as e:
        logger.error(f"Failed to get remediation status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/remediation/rollback/{plan_id}", response_model=Dict[str, Any])
async def rollback_remediation(
    plan_id: str,
    reason: str,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
):
    """Rollback a remediation that has been executed."""
    try:
        cache = IgniteClient()
        event_publisher = EventPublisher()
        
        # Get plan
        plan_data = await cache.get(f"remediation:plan:{plan_id}")
        if not plan_data:
            raise HTTPException(status_code=404, detail="Remediation plan not found")
        
        if plan_data.get("execution_status") != "completed":
            raise HTTPException(status_code=400, detail="Can only rollback completed remediations")
        
        # Check if rollback is available
        result = plan_data.get("execution_result", {})
        if not result.get("rollback_available", False):
            raise HTTPException(status_code=400, detail="Rollback not available for this remediation")
        
        # Create rollback task
        rollback_id = f"rollback_{plan_id}_{datetime.utcnow().timestamp()}"
        
        # Publish rollback event
        await event_publisher.publish("quality.remediation.rollback_requested", {
            "rollback_id": rollback_id,
            "plan_id": plan_id,
            "dataset_id": plan_data["dataset_id"],
            "reason": reason,
            "requested_by": current_user["user_id"],
            "requested_at": datetime.utcnow().isoformat()
        })
        
        return {
            "rollback_id": rollback_id,
            "plan_id": plan_id,
            "status": "rollback_initiated",
            "reason": reason
        }
        
    except Exception as e:
        logger.error(f"Failed to rollback remediation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 