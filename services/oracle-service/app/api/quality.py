"""
Quality Score API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional

from ..models.measurements import (
    QualityScoreRequest, QualityScoreResponse,
    QuantumQualityScore, AIQualityScore, NetworkQualityScore
)
from ..oracles import QuantumOracle, AIOracle, NetworkOracle
from ..core.dependencies import (
    get_quantum_oracle, get_ai_oracle, get_network_oracle,
    verify_api_key
)
from ..utils.blockchain import BlockchainOracle
from ..config import settings


router = APIRouter(prefix="/quality", tags=["Quality Scores"])


@router.post("/quantum/{qpu_id}", response_model=QuantumQualityScore)
async def calculate_quantum_quality(
    qpu_id: str,
    time_window_hours: int = Query(24, ge=1, le=168),
    quantum_oracle: QuantumOracle = Depends(get_quantum_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Calculate quality score for a quantum processor"""
    try:
        quality_score = await quantum_oracle.calculate_quality_score(
            qpu_id, time_window_hours
        )
        
        # Submit to blockchain if configured
        if settings.BLOCKCHAIN_RPC_URL:
            blockchain = BlockchainOracle()
            await blockchain.initialize()
            await blockchain.update_quality_score(
                qpu_id,
                int(quality_score.overall_score),
                quality_score.last_updated
            )
        
        return quality_score
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ai/{accelerator_id}", response_model=AIQualityScore)
async def calculate_ai_quality(
    accelerator_id: str,
    accelerator_type: str = Query(..., regex="^(TPU|GPU|NPU|ASIC)$"),
    time_window_hours: int = Query(24, ge=1, le=168),
    ai_oracle: AIOracle = Depends(get_ai_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Calculate quality score for an AI accelerator"""
    try:
        quality_score = await ai_oracle.calculate_quality_score(
            accelerator_id, accelerator_type, time_window_hours
        )
        
        # Submit to blockchain if configured
        if settings.BLOCKCHAIN_RPC_URL:
            blockchain = BlockchainOracle()
            await blockchain.initialize()
            await blockchain.update_quality_score(
                accelerator_id,
                int(quality_score.overall_score),
                quality_score.last_updated
            )
        
        return quality_score
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/network/{path_id}", response_model=NetworkQualityScore)
async def calculate_network_quality(
    path_id: str,
    source_node: str,
    destination_node: str,
    time_window_hours: int = Query(24, ge=1, le=168),
    network_oracle: NetworkOracle = Depends(get_network_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Calculate quality score for a network path"""
    try:
        quality_score = await network_oracle.calculate_quality_score(
            path_id, source_node, destination_node, time_window_hours
        )
        
        # Submit to blockchain if configured
        if settings.BLOCKCHAIN_RPC_URL:
            blockchain = BlockchainOracle()
            await blockchain.initialize()
            await blockchain.update_quality_score(
                path_id,
                int(quality_score.overall_score),
                quality_score.last_updated
            )
        
        return quality_score
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quantum/{qpu_id}/health")
async def get_quantum_health(
    qpu_id: str,
    quantum_oracle: QuantumOracle = Depends(get_quantum_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Get real-time health status for a quantum processor"""
    try:
        health = await quantum_oracle.monitor_qpu_health(qpu_id)
        return health
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ai/{accelerator_id}/health")
async def get_ai_health(
    accelerator_id: str,
    accelerator_type: str = Query(..., regex="^(TPU|GPU|NPU|ASIC)$"),
    ai_oracle: AIOracle = Depends(get_ai_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Get real-time health status for an AI accelerator"""
    try:
        health = await ai_oracle.monitor_accelerator_health(
            accelerator_id, accelerator_type
        )
        return health
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/network/{path_id}/health")
async def get_network_health(
    path_id: str,
    source_node: str,
    destination_node: str,
    network_oracle: NetworkOracle = Depends(get_network_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Get real-time health status for a network path"""
    try:
        health = await network_oracle.monitor_path_health(
            path_id, source_node, destination_node
        )
        return health
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify/quantum")
async def verify_quantum_computation(
    qpu_id: str,
    algorithm_id: str,
    expected_result: dict,
    actual_result: dict,
    quantum_oracle: QuantumOracle = Depends(get_quantum_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Verify quantum computation result"""
    try:
        verified, confidence = await quantum_oracle.verify_quantum_computation(
            qpu_id, algorithm_id, expected_result, actual_result
        )
        return {
            "verified": verified,
            "confidence": confidence,
            "qpu_id": qpu_id,
            "algorithm_id": algorithm_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify/ai-training")
async def verify_ai_training(
    accelerator_id: str,
    training_id: str,
    expected_metrics: dict,
    ai_oracle: AIOracle = Depends(get_ai_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Verify AI training completion and results"""
    try:
        verified, result = await ai_oracle.verify_training_completion(
            accelerator_id, training_id, expected_metrics
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify/network-sla")
async def verify_network_sla(
    path_id: str,
    sla_parameters: dict,
    network_oracle: NetworkOracle = Depends(get_network_oracle),
    api_key: str = Depends(verify_api_key)
):
    """Verify network SLA compliance"""
    try:
        compliant, result = await network_oracle.verify_sla_compliance(
            path_id, sla_parameters
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 