"""
Flash Provisioning API endpoints
"""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from decimal import Decimal

from ..models import (
    FlashProvisionRequest, FlashProvisionResponse,
    FlashSwapRequest, FlashSwapResponse,
    BurstProvisionRequest, BurstProvisionResponse,
    JITScalingConfig, FlashStatistics,
    ProvisionStatusResponse, FlashLiquidityDeposit,
    FlashFeeUpdate, TrustedReceiverUpdate
)
from ..protocols.flash_provisioning import FlashProvisioningProtocol
from ..dependencies import get_flash_protocol, get_auth_user
from platformq_shared.models import ResourceType

router = APIRouter(prefix="/flash", tags=["flash-provisioning"])


@router.post("/provision", response_model=FlashProvisionResponse)
async def flash_provision(
    request: FlashProvisionRequest,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Execute instant resource provisioning using flash loans.
    
    This endpoint allows users to instantly provision resources without upfront payment.
    The resources must be returned within the same transaction plus fees.
    
    Args:
        request: Flash provisioning request details
        
    Returns:
        Provisioning result with allocation details
    """
    try:
        result = await protocol.flash_provision(request)
        return FlashProvisionResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Flash provision failed: {str(e)}")


@router.post("/swap", response_model=FlashSwapResponse)
async def flash_swap(
    request: FlashSwapRequest,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Atomically swap one resource type for another using flash loans.
    
    This allows instant conversion between resource types without holding
    the intermediate assets.
    
    Args:
        request: Flash swap request details
        
    Returns:
        Swap result with exchange details
    """
    try:
        from_resource = {
            "token_id": request.from_token_id,
            "amount": request.from_amount
        }
        
        result = await protocol.flash_swap(
            from_resource=from_resource,
            to_resource_type=request.to_resource_type,
            to_amount=request.to_amount,
            max_slippage=Decimal(str(request.max_slippage))
        )
        
        return FlashSwapResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Flash swap failed: {str(e)}")


@router.post("/burst", response_model=BurstProvisionResponse)
async def provision_burst(
    request: BurstProvisionRequest,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Provision burst capacity for sudden demand spikes.
    
    This endpoint enables automatic provisioning of additional resources
    when demand exceeds normal capacity.
    
    Args:
        request: Burst provisioning request
        
    Returns:
        Burst provisioning result with multiple allocations
    """
    try:
        max_price = Decimal(str(request.max_price)) if request.max_price else None
        
        result = await protocol.provision_burst_capacity(
            resource_type=request.resource_type,
            burst_amount=request.burst_amount,
            duration=request.duration,
            max_price=max_price
        )
        
        return BurstProvisionResponse(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Burst provision failed: {str(e)}")


@router.post("/jit-scaling/{resource_type}")
async def configure_jit_scaling(
    resource_type: ResourceType,
    config: JITScalingConfig,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Configure just-in-time scaling for a resource type.
    
    JIT scaling automatically provisions resources based on utilization
    thresholds to maintain optimal capacity.
    
    Args:
        resource_type: Type of resource to configure
        config: JIT scaling configuration
        
    Returns:
        Success message
    """
    try:
        if config.resource_type != resource_type:
            raise ValueError("Resource type mismatch")
            
        await protocol.enable_jit_scaling(
            resource_type=resource_type,
            scaling_config=config.dict()
        )
        
        return {"message": f"JIT scaling configured for {resource_type.value}"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"JIT configuration failed: {str(e)}")


@router.get("/provision/{provision_id}", response_model=ProvisionStatusResponse)
async def get_provision_status(
    provision_id: str,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Get status of a flash provision.
    
    Args:
        provision_id: ID of the provision to check
        
    Returns:
        Current status and details of the provision
    """
    status = await protocol.get_provision_status(provision_id)
    if not status:
        raise HTTPException(status_code=404, detail="Provision not found")
        
    return ProvisionStatusResponse(**status)


@router.get("/statistics", response_model=FlashStatistics)
async def get_flash_statistics(
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Get flash provisioning statistics.
    
    Returns aggregate statistics about active provisions, resource usage,
    and JIT scaling status.
    
    Returns:
        Flash provisioning statistics
    """
    stats = await protocol.get_flash_statistics()
    return FlashStatistics(**stats)


@router.post("/liquidity/deposit")
async def deposit_liquidity(
    deposit: FlashLiquidityDeposit,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Deposit resources to provide flash loan liquidity.
    
    Liquidity providers earn fees from flash loans proportional to their
    contribution.
    
    Args:
        deposit: Liquidity deposit details
        
    Returns:
        Deposit confirmation
    """
    try:
        # This would interact with the smart contract
        # For now, return mock response
        return {
            "message": "Liquidity deposit submitted",
            "token_id": deposit.token_id,
            "amount": deposit.amount,
            "tx_hash": "0x123..."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Deposit failed: {str(e)}")


@router.put("/fees/{resource_type}")
async def update_flash_fees(
    resource_type: ResourceType,
    fee_update: FlashFeeUpdate,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Update flash loan fees for a resource type (admin only).
    
    Args:
        resource_type: Type of resource
        fee_update: New fee configuration
        
    Returns:
        Update confirmation
    """
    try:
        # Check admin permissions
        if not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
            
        if fee_update.resource_type != resource_type:
            raise ValueError("Resource type mismatch")
            
        # This would interact with the smart contract
        return {
            "message": f"Fee updated for {resource_type.value}",
            "new_fee": fee_update.fee_basis_points
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fee update failed: {str(e)}")


@router.put("/trusted-receivers")
async def update_trusted_receiver(
    update: TrustedReceiverUpdate,
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Update trusted receiver status (admin only).
    
    Trusted receivers can execute flash provisions on behalf of users.
    
    Args:
        update: Trusted receiver update
        
    Returns:
        Update confirmation
    """
    try:
        # Check admin permissions
        if not user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin access required")
            
        # This would interact with the smart contract
        return {
            "message": "Trusted receiver status updated",
            "receiver": update.receiver_address,
            "trusted": update.trusted
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")


@router.get("/available-capacity")
async def get_available_capacity(
    resource_type: ResourceType = Query(...),
    region: str = Query(...),
    tier: str = Query(default="standard"),
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol),
    user = Depends(get_auth_user)
):
    """
    Check available flash capacity for a resource type.
    
    Args:
        resource_type: Type of resource
        region: Target region
        tier: Service tier
        
    Returns:
        Available capacity information
    """
    try:
        # This would query the capacity monitor
        # For now, return mock data
        return {
            "resource_type": resource_type.value,
            "region": region,
            "tier": tier,
            "available_capacity": 5000,
            "max_flash_amount": 1000,
            "current_utilization": 0.65,
            "estimated_fee_rate": 10  # basis points
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")


@router.get("/fee-rates")
async def get_fee_rates(
    protocol: FlashProvisioningProtocol = Depends(get_flash_protocol)
):
    """
    Get current flash loan fee rates for all resource types.
    
    Returns:
        Fee rates in basis points by resource type
    """
    try:
        # This would query the smart contract
        # For now, return mock data
        return {
            "cpu": 10,  # 0.1%
            "gpu": 20,  # 0.2%
            "storage": 5,  # 0.05%
            "bandwidth": 15,  # 0.15%
            "memory": 10  # 0.1%
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}") 