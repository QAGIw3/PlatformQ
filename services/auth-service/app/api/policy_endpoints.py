"""
Policy Evaluation API Endpoints

Provides unified authorization and policy management.
"""

import logging
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status

from ..schemas.policy import (
    PolicyEvaluationRequest,
    PolicyEvaluationResponse,
    PolicyDefinition,
    PolicyUpdateRequest
)
from ..services.policy_engine import get_policy_engine, PolicyEngine
from .deps import get_current_tenant_and_user, require_role

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/evaluate", response_model=PolicyEvaluationResponse)
async def evaluate_policy(
    request: PolicyEvaluationRequest,
    policy_engine: PolicyEngine = Depends(get_policy_engine)
) -> PolicyEvaluationResponse:
    """
    Evaluate an access control policy.
    
    This endpoint evaluates whether a subject can perform an action on a resource,
    taking into account the current context.
    """
    try:
        # Prepare input for policy engine
        input_data = {
            "subject": request.subject,
            "action": request.action,
            "resource": request.resource,
            "context": request.context
        }
        
        # Evaluate policy
        result = await policy_engine.evaluate(input_data)
        
        # Return response
        return PolicyEvaluationResponse(
            allow=result["allow"],
            reason=result.get("reason")
        )
        
    except Exception as e:
        logger.error(f"Error evaluating policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal error during policy evaluation"
        )


@router.post("/policies", response_model=Dict[str, str])
async def create_policy(
    policy: PolicyDefinition,
    context: dict = Depends(require_role("admin")),
    policy_engine: PolicyEngine = Depends(get_policy_engine)
) -> Dict[str, str]:
    """
    Create or update a policy (admin only).
    
    This endpoint allows administrators to add or update policies in the system.
    When OPA is integrated, this will update the OPA policy store.
    """
    try:
        success = await policy_engine.add_policy(policy.name, policy.content)
        
        if success:
            return {
                "status": "success",
                "message": f"Policy '{policy.name}' created/updated successfully"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Policy engine not available or policy update failed"
            )
            
    except Exception as e:
        logger.error(f"Error creating policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create policy: {str(e)}"
        )


@router.get("/policies/actions", response_model=Dict[str, list[str]])
async def get_available_actions(
    context: dict = Depends(get_current_tenant_and_user),
    policy_engine: PolicyEngine = Depends(get_policy_engine)
) -> Dict[str, list[str]]:
    """
    Get available actions and their required roles.
    
    This endpoint returns a mapping of actions to the roles that can perform them.
    """
    return {"actions": policy_engine.rbac_policies}


@router.post("/check-permission")
async def check_permission(
    action: str,
    resource: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user),
    policy_engine: PolicyEngine = Depends(get_policy_engine)
) -> PolicyEvaluationResponse:
    """
    Check if the current user has permission to perform an action.
    
    This is a convenience endpoint that automatically includes the current user's
    information in the policy evaluation.
    """
    try:
        user = context["user"]
        tenant_id = context["tenant_id"]
        
        # Get user roles
        from ..crud import crud_role
        from ..db.session import get_db_session
        
        # This is a workaround - in production, roles would be in the JWT
        roles = ["member"]  # Default role, should be fetched from DB
        
        # Prepare policy evaluation request
        input_data = {
            "subject": {
                "user_id": str(user.id),
                "roles": roles,
                "tenant_id": str(tenant_id)
            },
            "action": action,
            "resource": resource,
            "context": {
                "tenant_id": str(tenant_id)
            }
        }
        
        # Evaluate policy
        result = await policy_engine.evaluate(input_data)
        
        return PolicyEvaluationResponse(
            allow=result["allow"],
            reason=result.get("reason")
        )
        
    except Exception as e:
        logger.error(f"Error checking permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to check permission"
        ) 