"""
Rules API endpoints

Provides API for managing data quality rules.
"""

from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger
from ..rules import RuleType, ConditionOperator, ActionType

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/rules", tags=["rules"])


# Request/Response Models
class RuleConditionModel(BaseModel):
    """Rule condition model"""
    field: str
    operator: str
    value: Any
    case_sensitive: bool = True


class RuleActionModel(BaseModel):
    """Rule action model"""
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)


class RuleCreateRequest(BaseModel):
    """Request to create a rule"""
    name: str = Field(..., description="Rule name")
    description: str = Field("", description="Rule description")
    type: str = Field(..., description="Rule type")
    conditions: List[RuleConditionModel] = Field(..., description="Rule conditions")
    actions: List[RuleActionModel] = Field(..., description="Rule actions")
    enabled: bool = Field(default=True, description="Whether rule is enabled")
    priority: int = Field(default=0, description="Rule priority (higher executes first)")
    tags: List[str] = Field(default_factory=list, description="Rule tags")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    condition_logic: str = Field(default="AND", description="Logic for combining conditions")


class RuleUpdateRequest(BaseModel):
    """Request to update a rule"""
    name: Optional[str] = None
    description: Optional[str] = None
    conditions: Optional[List[RuleConditionModel]] = None
    actions: Optional[List[RuleActionModel]] = None
    enabled: Optional[bool] = None
    priority: Optional[int] = None
    tags: Optional[List[str]] = None
    metadata: Optional[Dict[str, Any]] = None
    condition_logic: Optional[str] = None


class RuleResponse(BaseModel):
    """Rule response model"""
    id: str
    name: str
    description: str
    type: str
    conditions: List[RuleConditionModel]
    actions: List[RuleActionModel]
    enabled: bool
    priority: int
    tags: List[str]
    metadata: Dict[str, Any]
    condition_logic: str
    created_at: datetime
    updated_at: datetime


class RuleExecutionRequest(BaseModel):
    """Request to execute rules"""
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(..., description="Data to validate")
    rule_ids: Optional[List[str]] = Field(None, description="Specific rules to execute")
    tags: Optional[List[str]] = Field(None, description="Execute rules with these tags")
    rule_types: Optional[List[str]] = Field(None, description="Execute rules of these types")


# API Endpoints
@router.post("/", response_model=RuleResponse)
async def create_rule(request: RuleCreateRequest):
    """
    Create a new quality rule
    """
    try:
        logger.info("create_rule_requested", name=request.name)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Create rule
        rule_id = f"rule_{datetime.utcnow().timestamp()}"
        rule_data = request.dict()
        
        # Save rule
        success = await service.rule_repository.save_rule({
            "id": rule_id,
            **rule_data
        })
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to save rule")
        
        # Get saved rule
        rule = await service.rule_repository.get_rule(rule_id)
        
        return RuleResponse(
            id=rule.id,
            name=rule.name,
            description=rule.description,
            type=rule.type.value,
            conditions=[
                RuleConditionModel(
                    field=c.field,
                    operator=c.operator.value,
                    value=c.value,
                    case_sensitive=c.case_sensitive
                )
                for c in rule.conditions
            ],
            actions=[
                RuleActionModel(
                    type=a.type.value,
                    params=a.params
                )
                for a in rule.actions
            ],
            enabled=rule.enabled,
            priority=rule.priority,
            tags=rule.tags,
            metadata=rule.metadata,
            condition_logic=rule.condition_logic,
            created_at=rule.created_at,
            updated_at=rule.updated_at
        )
        
    except Exception as e:
        logger.error("create_rule_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[RuleResponse])
async def list_rules(
    type: Optional[str] = Query(None, description="Filter by rule type"),
    tags: Optional[str] = Query(None, description="Filter by tags (comma-separated)"),
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    search: Optional[str] = Query(None, description="Search in name and description")
):
    """
    List quality rules
    """
    try:
        logger.info("list_rules_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get rules
        if search:
            rules = await service.rule_repository.search_rules(
                query=search,
                rule_type=RuleType(type) if type else None,
                tags=tags.split(",") if tags else None,
                enabled_only=enabled if enabled is not None else True
            )
        else:
            all_rules = await service.rule_repository.get_all_rules()
            rules = all_rules
            
            # Apply filters
            if type:
                rules = [r for r in rules if r.type == RuleType(type)]
            if tags:
                tag_list = tags.split(",")
                rules = [r for r in rules if any(t in r.tags for t in tag_list)]
            if enabled is not None:
                rules = [r for r in rules if r.enabled == enabled]
        
        # Convert to response models
        return [
            RuleResponse(
                id=rule.id,
                name=rule.name,
                description=rule.description,
                type=rule.type.value,
                conditions=[
                    RuleConditionModel(
                        field=c.field,
                        operator=c.operator.value,
                        value=c.value,
                        case_sensitive=c.case_sensitive
                    )
                    for c in rule.conditions
                ],
                actions=[
                    RuleActionModel(
                        type=a.type.value,
                        params=a.params
                    )
                    for a in rule.actions
                ],
                enabled=rule.enabled,
                priority=rule.priority,
                tags=rule.tags,
                metadata=rule.metadata,
                condition_logic=rule.condition_logic,
                created_at=rule.created_at,
                updated_at=rule.updated_at
            )
            for rule in rules
        ]
        
    except Exception as e:
        logger.error("list_rules_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{rule_id}", response_model=RuleResponse)
async def get_rule(rule_id: str = Path(..., description="Rule ID")):
    """
    Get a specific rule
    """
    try:
        logger.info("get_rule_requested", rule_id=rule_id)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get rule
        rule = await service.rule_repository.get_rule(rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        return RuleResponse(
            id=rule.id,
            name=rule.name,
            description=rule.description,
            type=rule.type.value,
            conditions=[
                RuleConditionModel(
                    field=c.field,
                    operator=c.operator.value,
                    value=c.value,
                    case_sensitive=c.case_sensitive
                )
                for c in rule.conditions
            ],
            actions=[
                RuleActionModel(
                    type=a.type.value,
                    params=a.params
                )
                for a in rule.actions
            ],
            enabled=rule.enabled,
            priority=rule.priority,
            tags=rule.tags,
            metadata=rule.metadata,
            condition_logic=rule.condition_logic,
            created_at=rule.created_at,
            updated_at=rule.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_rule_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{rule_id}", response_model=RuleResponse)
async def update_rule(
    rule_id: str = Path(..., description="Rule ID"),
    request: RuleUpdateRequest = Body(...)
):
    """
    Update a rule
    """
    try:
        logger.info("update_rule_requested", rule_id=rule_id)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get existing rule
        rule = await service.rule_repository.get_rule(rule_id)
        if not rule:
            raise HTTPException(status_code=404, detail="Rule not found")
        
        # Update fields
        update_data = request.dict(exclude_unset=True)
        for field, value in update_data.items():
            if value is not None:
                setattr(rule, field, value)
        
        # Save updated rule
        success = await service.rule_repository.save_rule(rule)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to update rule")
        
        # Get updated rule
        rule = await service.rule_repository.get_rule(rule_id)
        
        return RuleResponse(
            id=rule.id,
            name=rule.name,
            description=rule.description,
            type=rule.type.value,
            conditions=[
                RuleConditionModel(
                    field=c.field,
                    operator=c.operator.value,
                    value=c.value,
                    case_sensitive=c.case_sensitive
                )
                for c in rule.conditions
            ],
            actions=[
                RuleActionModel(
                    type=a.type.value,
                    params=a.params
                )
                for a in rule.actions
            ],
            enabled=rule.enabled,
            priority=rule.priority,
            tags=rule.tags,
            metadata=rule.metadata,
            condition_logic=rule.condition_logic,
            created_at=rule.created_at,
            updated_at=rule.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("update_rule_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{rule_id}")
async def delete_rule(rule_id: str = Path(..., description="Rule ID")):
    """
    Delete a rule
    """
    try:
        logger.info("delete_rule_requested", rule_id=rule_id)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Delete rule
        success = await service.rule_repository.delete_rule(rule_id)
        if not success:
            raise HTTPException(status_code=404, detail="Rule not found or failed to delete")
        
        return {"message": "Rule deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("delete_rule_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute", response_model=Dict[str, Any])
async def execute_rules(request: RuleExecutionRequest):
    """
    Execute rules against data
    """
    try:
        logger.info("execute_rules_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Execute rules
        rule_types = [RuleType(rt) for rt in request.rule_types] if request.rule_types else None
        
        results = await service.rule_engine.execute_rules(
            data=request.data,
            rule_ids=request.rule_ids,
            tags=request.tags,
            rule_types=rule_types
        )
        
        # Summarize results
        summary = {
            "total_rules_executed": len(results),
            "passed": sum(1 for r in results if r.passed),
            "failed": sum(1 for r in results if not r.passed),
            "errors": sum(1 for r in results if r.error),
            "total_execution_time_ms": sum(r.execution_time_ms for r in results),
            "results": [
                {
                    "rule_id": r.rule_id,
                    "rule_name": r.rule_name,
                    "passed": r.passed,
                    "conditions_met": r.conditions_met,
                    "actions_executed": r.actions_executed,
                    "execution_time_ms": r.execution_time_ms,
                    "error": r.error
                }
                for r in results
            ]
        }
        
        return summary
        
    except Exception as e:
        logger.error("execute_rules_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics", response_model=Dict[str, Any])
async def get_rule_statistics():
    """
    Get rule statistics
    """
    try:
        logger.info("get_statistics_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get statistics
        stats = await service.rule_repository.get_rule_statistics()
        
        return stats
        
    except Exception as e:
        logger.error("get_statistics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 