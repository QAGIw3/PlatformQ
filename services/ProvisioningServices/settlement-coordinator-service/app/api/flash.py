"""
Flash Provisioning API endpoints for Settlement Coordinator
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException

from ..models import (
    FlashProvisioningRequest, FlashSwapRequest, 
    FlashSettlementResponse, Settlement
)
from ..services.flash_settlement import FlashSettlementService
from ..dependencies import get_flash_settlement_service, get_auth_user

router = APIRouter(prefix="/flash", tags=["flash-settlement"])


@router.post("/settlement", response_model=Settlement)
async def create_flash_settlement(
    request: FlashProvisioningRequest,
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service),
    user = Depends(get_auth_user)
):
    """
    Create a settlement using flash provisioning for instant resource access.
    
    This creates a settlement that leverages flash loans to provide
    immediate resource access without upfront payment.
    
    Args:
        request: Flash provisioning request details
        
    Returns:
        Created flash settlement
    """
    try:
        # Verify user is authorized consumer
        if user["address"] != request.consumer:
            raise HTTPException(status_code=403, detail="Unauthorized consumer")
            
        resource_request = {
            "resource_type": request.resource_type,
            "amount": request.amount,
            "tier": request.tier,
            "region": request.region
        }
        
        settlement = await flash_service.create_flash_settlement(
            resource_request=resource_request,
            provider=request.provider,
            consumer=request.consumer,
            duration=request.duration
        )
        
        return settlement
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create flash settlement: {str(e)}")


@router.post("/execute/{settlement_id}", response_model=FlashSettlementResponse)
async def execute_flash_provision(
    settlement_id: str,
    callback_data: bytes = b"",
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service),
    user = Depends(get_auth_user)
):
    """
    Execute flash provision for a settlement.
    
    This triggers the flash loan to instantly provision resources.
    
    Args:
        settlement_id: ID of the settlement to execute
        callback_data: Optional callback data
        
    Returns:
        Execution result
    """
    try:
        # Get settlement
        settlement = flash_service._active_flash_settlements.get(settlement_id)
        if not settlement:
            raise HTTPException(status_code=404, detail="Settlement not found")
            
        # Verify user is authorized
        if user["address"] != settlement.consumer:
            raise HTTPException(status_code=403, detail="Unauthorized")
            
        result = await flash_service.execute_flash_provision(
            settlement=settlement,
            callback_data=callback_data
        )
        
        return FlashSettlementResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@router.post("/swap", response_model=FlashSettlementResponse)
async def execute_atomic_swap(
    request: FlashSwapRequest,
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service),
    user = Depends(get_auth_user)
):
    """
    Execute atomic resource swap using flash loans.
    
    Atomically swaps resources from one type to another without
    holding intermediate assets.
    
    Args:
        request: Swap request details
        
    Returns:
        Swap result with new settlement
    """
    try:
        # Get source settlement
        from_settlement = flash_service._active_flash_settlements.get(request.settlement_id)
        if not from_settlement:
            raise HTTPException(status_code=404, detail="Settlement not found")
            
        # Verify user is authorized
        if user["address"] != from_settlement.consumer:
            raise HTTPException(status_code=403, detail="Unauthorized")
            
        result = await flash_service.execute_atomic_swap(
            from_settlement=from_settlement,
            to_resource_type=request.to_resource_type,
            to_amount=request.to_amount,
            pool_id=request.pool_id
        )
        
        return FlashSettlementResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Swap failed: {str(e)}")


@router.get("/settlement/{settlement_id}", response_model=Settlement)
async def get_flash_settlement(
    settlement_id: str,
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service),
    user = Depends(get_auth_user)
):
    """
    Get details of a flash settlement.
    
    Args:
        settlement_id: Settlement ID
        
    Returns:
        Settlement details
    """
    settlement = flash_service._active_flash_settlements.get(settlement_id)
    if not settlement:
        raise HTTPException(status_code=404, detail="Settlement not found")
        
    # Verify user is authorized
    if user["address"] not in [settlement.consumer, settlement.provider]:
        raise HTTPException(status_code=403, detail="Unauthorized")
        
    return settlement


@router.post("/callback")
async def handle_flash_callback(
    initiator: str,
    token_id: int,
    amount: int,
    fee: int,
    data: bytes,
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service)
):
    """
    Handle callback from flash loan provider.
    
    This endpoint is called by the smart contract during flash loan execution.
    
    Args:
        initiator: Address that initiated the flash loan
        token_id: Resource token ID
        amount: Amount borrowed
        fee: Fee to be paid
        data: Callback data
        
    Returns:
        Success response bytes
    """
    try:
        response = await flash_service.handle_flash_provision_callback(
            initiator=initiator,
            token_id=token_id,
            amount=amount,
            fee=fee,
            data=data
        )
        
        return {"response": response.hex()}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Callback failed: {str(e)}")


@router.get("/active")
async def get_active_flash_settlements(
    flash_service: FlashSettlementService = Depends(get_flash_settlement_service),
    user = Depends(get_auth_user)
):
    """
    Get all active flash settlements for the user.
    
    Returns:
        List of active settlements
    """
    user_settlements = []
    
    for settlement in flash_service._active_flash_settlements.values():
        if user["address"] in [settlement.consumer, settlement.provider]:
            user_settlements.append({
                "settlement_id": settlement.settlement_id,
                "resource_type": settlement.resource_type.value,
                "amount": settlement.amount,
                "status": settlement.status.value,
                "start_time": settlement.start_time.isoformat(),
                "end_time": settlement.end_time.isoformat(),
                "is_provider": user["address"] == settlement.provider,
                "flash_fee": settlement.flash_fee
            })
            
    return {"settlements": user_settlements} 