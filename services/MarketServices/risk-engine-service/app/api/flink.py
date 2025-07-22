"""Flink job management API endpoints."""

from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.dependencies import get_flink_processor
from app.integrations.flink_integration import FlinkJobManager
from app.auth import verify_admin

router = APIRouter(prefix="/flink", tags=["Flink"])


class FlinkJobRequest(BaseModel):
    """Request to submit a Flink job."""
    job_jar_path: str
    job_class: str = "com.platformq.risk.RiskAnalyticsJob"
    parallelism: int = 4
    checkpoint_interval_ms: int = 60000


class FlinkJobResponse(BaseModel):
    """Flink job information."""
    job_id: str
    status: str
    start_time: str
    vertices: Dict[str, int]


@router.post("/jobs", response_model=Dict[str, str])
async def submit_job(
    request: FlinkJobRequest,
    _: str = Depends(verify_admin)
) -> Dict[str, str]:
    """Submit a new Flink job for risk analytics."""
    try:
        job_id = await FlinkJobManager.submit_risk_analytics_job(
            job_jar_path=request.job_jar_path
        )
        
        return {
            "job_id": job_id,
            "message": "Flink job submitted successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=FlinkJobResponse)
async def get_job_status(
    job_id: str,
    _: str = Depends(verify_admin)
) -> FlinkJobResponse:
    """Get status of a Flink job."""
    try:
        status = await FlinkJobManager.get_job_status(job_id)
        
        return FlinkJobResponse(
            job_id=status["job_id"],
            status=status["status"],
            start_time=status["start_time"],
            vertices=status["vertices"]
        )
        
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Job not found: {str(e)}")


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: str,
    _: str = Depends(verify_admin)
) -> Dict[str, str]:
    """Cancel a running Flink job."""
    try:
        success = await FlinkJobManager.cancel_job(job_id)
        
        if success:
            return {"message": f"Job {job_id} cancelled successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to cancel job")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status")
async def get_flink_status(
    _: str = Depends(verify_admin)
) -> Dict[str, Any]:
    """Get Flink integration status."""
    flink_processor = get_flink_processor()
    
    if not flink_processor:
        return {
            "status": "disabled",
            "message": "Flink integration is not enabled"
        }
    
    return {
        "status": "active",
        "producers": list(flink_processor.producers.keys()),
        "consumers": list(flink_processor.consumers.keys()),
        "running": flink_processor._running
    }


@router.post("/test-event")
async def send_test_event(
    event_type: str,
    user_id: str,
    _: str = Depends(verify_admin)
) -> Dict[str, str]:
    """Send a test event to Flink for debugging."""
    flink_processor = get_flink_processor()
    
    if not flink_processor:
        raise HTTPException(status_code=503, detail="Flink integration not available")
    
    try:
        if event_type == "trading":
            await flink_processor.send_trading_event({
                "event_type": "ORDER_FILLED",
                "user_id": user_id,
                "symbol": "COMPUTE_GPU_A100",
                "side": "buy",
                "quantity": 10,
                "price": 100.0,
                "order_id": "TEST_ORDER_001",
                "position_id": "TEST_POS_001"
            })
        elif event_type == "position":
            await flink_processor.send_position_update({
                "user_id": user_id,
                "positions": {
                    "TEST_POS_001": {
                        "symbol": "COMPUTE_GPU_A100",
                        "quantity": 10,
                        "avg_price": 100.0,
                        "market_value": 1000.0
                    }
                }
            })
        elif event_type == "market":
            await flink_processor.send_market_data({
                "symbol": "COMPUTE_GPU_A100",
                "price": 105.0,
                "volume": 1000,
                "bid": 104.5,
                "ask": 105.5
            })
        else:
            raise HTTPException(status_code=400, detail="Invalid event type")
            
        return {"message": f"Test {event_type} event sent successfully"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 