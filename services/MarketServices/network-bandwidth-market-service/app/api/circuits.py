"""
Dedicated Circuit API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Header
from typing import Optional

from ..models import (
    CircuitProvisionRequest, CircuitResponse,
    DedicatedCircuit
)
from ..services import CircuitManagerService
from ..core.dependencies import get_circuit_manager


router = APIRouter(prefix="/circuits", tags=["Dedicated Circuits"])


@router.post("/", response_model=CircuitResponse)
async def provision_circuit(
    request: CircuitProvisionRequest,
    x_user_address: str = Header(..., description="User wallet address"),
    circuit_manager: CircuitManagerService = Depends(get_circuit_manager)
):
    """Provision a new dedicated circuit"""
    circuit, selected_paths, error = await circuit_manager.provision_circuit(
        request,
        x_user_address
    )
    
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return CircuitResponse(
        circuit=circuit,
        selected_paths=selected_paths,
        estimated_setup_time=circuit.sla_parameters.get("setup_time_seconds", 300),
        blockchain_tx_hash=None  # Would be set after blockchain tx
    )


@router.get("/{circuit_id}", response_model=DedicatedCircuit)
async def get_circuit(
    circuit_id: str,
    circuit_manager: CircuitManagerService = Depends(get_circuit_manager)
):
    """Get circuit details"""
    circuit = await circuit_manager.get_circuit(circuit_id)
    
    if not circuit:
        raise HTTPException(status_code=404, detail="Circuit not found")
    
    return circuit


@router.put("/{circuit_id}/modify")
async def modify_circuit(
    circuit_id: str,
    new_bandwidth_mbps: Optional[int] = None,
    extend_duration_days: Optional[int] = None,
    x_user_address: str = Header(..., description="User wallet address"),
    circuit_manager: CircuitManagerService = Depends(get_circuit_manager)
):
    """Modify circuit parameters"""
    if not new_bandwidth_mbps and not extend_duration_days:
        raise HTTPException(
            status_code=400,
            detail="Must specify bandwidth or duration change"
        )
    
    success, error = await circuit_manager.modify_circuit(
        circuit_id,
        x_user_address,
        new_bandwidth_mbps,
        extend_duration_days
    )
    
    if not success:
        raise HTTPException(status_code=400, detail=error or "Modification failed")
    
    return {
        "status": "modified",
        "circuit_id": circuit_id,
        "new_bandwidth_mbps": new_bandwidth_mbps,
        "extended_days": extend_duration_days
    }


@router.delete("/{circuit_id}")
async def decommission_circuit(
    circuit_id: str,
    x_user_address: str = Header(..., description="User wallet address"),
    circuit_manager: CircuitManagerService = Depends(get_circuit_manager)
):
    """Decommission a circuit"""
    success = await circuit_manager.decommission_circuit(
        circuit_id,
        x_user_address
    )
    
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Failed to decommission - not found or unauthorized"
        )
    
    return {"status": "decommissioned", "circuit_id": circuit_id}


@router.get("/{circuit_id}/health")
async def get_circuit_health(
    circuit_id: str,
    circuit_manager: CircuitManagerService = Depends(get_circuit_manager)
):
    """Get circuit health and SLA compliance status"""
    health = await circuit_manager.monitor_circuit_health(circuit_id)
    
    if not health:
        raise HTTPException(status_code=404, detail="Circuit not found")
    
    return health 