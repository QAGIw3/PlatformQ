from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid
import logging
import json

from platformq_shared.jwt import get_current_tenant_and_user
from ...schemas import (
    SimulationCreateRequest,
    SimulationResponse,
    SimulationUpdateRequest,
    AgentDefinition,
    SimulationStartRequest,
    MLSessionNotification
)
from ...services import SimulationService
from ...ignite_manager import SimulationIgniteManager
from ...collaboration import SimulationCollaborationManager

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/simulations/{simulation_id}/ml-session", response_model=Dict[str, Any])
async def notify_ml_session(
    simulation_id: str,
    notification: MLSessionNotification,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    ignite_manager: SimulationIgniteManager = Depends(lambda: router.app.state.ignite_manager)
):
    """
    Receive notification about ML training session for simulation
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Get simulation
        sim_cache = ignite_manager.client.get_or_create_cache("simulations")
        simulation = sim_cache.get(simulation_id)
        
        if not simulation:
            raise HTTPException(status_code=404, detail="Simulation not found")
            
        # Verify tenant access
        if simulation.get("tenant_id") != tenant_id:
            raise HTTPException(status_code=403, detail="Access denied")
            
        # Update simulation with ML session info
        if "ml_sessions" not in simulation:
            simulation["ml_sessions"] = []
            
        ml_session_info = {
            "fl_session_id": notification.fl_session_id,
            "model_type": notification.model_type,
            "status": notification.status,
            "created_at": datetime.utcnow().isoformat(),
            "metadata": notification.metadata
        }
        
        simulation["ml_sessions"].append(ml_session_info)
        simulation["last_ml_session"] = notification.fl_session_id
        simulation["ml_optimization_active"] = notification.status == "initiated"
        
        # Update in cache
        sim_cache.put(simulation_id, simulation)
        
        # Schedule background task to monitor ML session
        background_tasks.add_task(
            monitor_ml_session,
            simulation_id,
            notification.fl_session_id,
            ignite_manager
        )
        
        logger.info(f"ML session {notification.fl_session_id} registered for simulation {simulation_id}")
        
        return {
            "simulation_id": simulation_id,
            "fl_session_id": notification.fl_session_id,
            "status": "registered",
            "message": "ML session notification received"
        }
        
    except Exception as e:
        logger.error(f"Error processing ML session notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def monitor_ml_session(
    simulation_id: str,
    fl_session_id: str,
    ignite_manager: SimulationIgniteManager
):
    """
    Background task to monitor ML session progress
    """
    try:
        # This would poll the federated learning service for status updates
        # and apply the trained model when ready
        logger.info(f"Starting to monitor ML session {fl_session_id} for simulation {simulation_id}")
        
        # For now, just log - full implementation would poll FL service
        
    except Exception as e:
        logger.error(f"Error monitoring ML session: {e}") 

@router.post("/simulations/{simulation_id}/anomaly", response_model=Dict[str, Any])
async def handle_anomaly_notification(
    simulation_id: str,
    notification: Dict[str, Any],
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    ignite_manager: SimulationIgniteManager = Depends(lambda: router.app.state.ignite_manager)
):
    """
    Handle anomaly notification from analytics service
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Get simulation
        sim_cache = ignite_manager.client.get_or_create_cache("simulations")
        simulation = sim_cache.get(simulation_id)
        
        if not simulation:
            raise HTTPException(status_code=404, detail="Simulation not found")
            
        # Verify tenant access
        if simulation.get("tenant_id") != tenant_id:
            raise HTTPException(status_code=403, detail="Access denied")
            
        # Update simulation with anomaly info
        if "anomalies" not in simulation:
            simulation["anomalies"] = []
            
        anomaly_info = {
            "timestamp": datetime.utcnow().isoformat(),
            "type": notification.get("anomaly_type"),
            "severity": notification.get("severity"),
            "actions": notification.get("recommended_actions", [])
        }
        
        simulation["anomalies"].append(anomaly_info)
        simulation["has_anomaly"] = True
        simulation["last_anomaly_time"] = anomaly_info["timestamp"]
        
        # Update in cache
        sim_cache.put(simulation_id, simulation)
        
        # Take action based on severity
        severity = notification.get("severity", 0)
        
        if severity > 0.8:
            # Critical anomaly - pause simulation
            logger.warning(f"Critical anomaly in simulation {simulation_id}, pausing...")
            background_tasks.add_task(
                pause_simulation_for_anomaly,
                simulation_id,
                anomaly_info,
                ignite_manager
            )
        elif severity > 0.5:
            # Medium severity - log and monitor
            logger.info(f"Medium severity anomaly in simulation {simulation_id}")
            
        return {
            "simulation_id": simulation_id,
            "anomaly_handled": True,
            "action_taken": "pause" if severity > 0.8 else "monitor"
        }
        
    except Exception as e:
        logger.error(f"Error handling anomaly notification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def pause_simulation_for_anomaly(
    simulation_id: str,
    anomaly_info: Dict[str, Any],
    ignite_manager: SimulationIgniteManager
):
    """
    Pause simulation due to critical anomaly
    """
    try:
        # Update simulation state
        state_cache = ignite_manager.client.get_or_create_cache("simulation_states")
        state_key = f"{simulation_id}:state:latest"
        
        state = state_cache.get(state_key)
        if state:
            state["status"] = "paused"
            state["pause_reason"] = f"Critical anomaly: {anomaly_info['type']}"
            state["paused_at"] = datetime.utcnow().isoformat()
            state_cache.put(state_key, state)
            
        logger.info(f"Simulation {simulation_id} paused due to anomaly")
        
    except Exception as e:
        logger.error(f"Error pausing simulation: {e}") 