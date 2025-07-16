from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from typing import Dict, List, Optional, Any
import json
import uuid
import logging
from datetime import datetime

from .deps import get_current_tenant_and_user
from ..ignite_manager import IgniteManager
from ..crdt_synchronizer import CRDTSynchronizer
from ..mesh_optimizer_client import MeshOptimizerClient
from platformq_shared.events import GeometryOperationEvent, MeshOptimizationResult

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/sessions/{session_id}/sync")
async def sync_geometry_operations(
    session_id: str,
    operations: List[Dict[str, Any]],
    context: dict = Depends(get_current_tenant_and_user),
):
    """Sync a batch of geometry operations"""
    from ..main import app
    crdt_sync: CRDTSynchronizer = app.state.crdt_sync
    
    results = []
    for op_data in operations:
        # Convert to event
        event = GeometryOperationEvent(
            tenant_id=context["tenant_id"],
            asset_id=op_data.get("asset_id", ""),
            session_id=session_id,
            user_id=context["user_id"],
            operation_id=op_data["operation_id"],
            operation_type=op_data["operation_type"],
            target_object_ids=op_data.get("target_object_ids", []),
            operation_data=json.dumps(op_data.get("data", {})),
            vector_clock=op_data.get("vector_clock", {}),
            parent_operations=op_data.get("parent_operations", []),
            timestamp=op_data.get("timestamp", int(datetime.utcnow().timestamp() * 1000))
        )
        
        result = await crdt_sync.handle_geometry_operation(session_id, event)
        results.append(result)
        
    return {"results": results}


@router.get("/sessions/{session_id}/state")
async def get_session_state(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Get current state of a CAD session"""
    from ..main import app
    crdt_sync: CRDTSynchronizer = app.state.crdt_sync
    
    state = await crdt_sync.get_session_state(session_id)
    if not state:
        raise HTTPException(status_code=404, detail="Session not found")
        
    return state


@router.post("/sessions/{session_id}/merge")
async def merge_remote_state(
    session_id: str,
    remote_state: str,  # Base64 encoded CRDT state
    context: dict = Depends(get_current_tenant_and_user),
):
    """Merge a remote CRDT state into the session"""
    from ..main import app
    crdt_sync: CRDTSynchronizer = app.state.crdt_sync
    
    import base64
    try:
        state_bytes = base64.b64decode(remote_state)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid base64 state: {e}")
        
    result = await crdt_sync.merge_remote_state(session_id, state_bytes)
    return result


@router.get("/sessions/{session_id}/presence")
async def get_session_presence(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Get presence information for all users in a session"""
    from ..main import app
    ignite: IgniteManager = app.state.ignite_manager
    
    presence = await ignite.get_session_presence(session_id)
    return {"session_id": session_id, "users": presence}


@router.post("/sessions/{session_id}/presence")
async def update_presence(
    session_id: str,
    presence_data: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user),
):
    """Update user presence in a session"""
    from ..main import app
    ignite: IgniteManager = app.state.ignite_manager
    
    await ignite.update_user_presence(
        session_id=session_id,
        user_id=context["user_id"],
        presence_data=presence_data
    )
    
    return {"status": "updated"}


@router.post("/optimize/mesh")
async def request_mesh_optimization(
    asset_id: str,
    mesh_uri: str,
    optimization_params: Dict[str, Any],
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Request mesh optimization for a 3D model"""
    from ..main import app
    optimizer: MeshOptimizerClient = app.state.mesh_optimizer
    
    # Define callback for when optimization completes
    async def on_optimization_complete(result: MeshOptimizationResult):
        logger.info(f"Optimization completed for asset {asset_id}: {result.status}")
        # TODO: Update asset with optimized mesh URI
        # TODO: Notify users via WebSocket
        
    request_id = await optimizer.request_optimization(
        asset_id=asset_id,
        mesh_data_uri=mesh_uri,
        optimization_params=optimization_params,
        callback=on_optimization_complete
    )
    
    return {
        "request_id": request_id,
        "status": "submitted",
        "estimated_time": 60  # seconds
    }


@router.post("/optimize/lod")
async def generate_lods(
    asset_id: str,
    mesh_uri: str,
    lod_levels: Optional[List[float]] = None,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Generate Level of Detail (LOD) versions of a mesh"""
    from ..main import app
    optimizer: MeshOptimizerClient = app.state.mesh_optimizer
    
    request_id = await optimizer.request_lod_generation(
        asset_id=asset_id,
        mesh_data_uri=mesh_uri,
        lod_levels=lod_levels
    )
    
    return {
        "request_id": request_id,
        "status": "submitted",
        "lod_levels": lod_levels or [0.5, 0.25, 0.1]
    }


@router.get("/optimize/status/{request_id}")
async def get_optimization_status(
    request_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Get status of an optimization request"""
    from ..main import app
    optimizer: MeshOptimizerClient = app.state.mesh_optimizer
    
    status = await optimizer.get_optimization_status(request_id)
    if not status:
        raise HTTPException(status_code=404, detail="Request not found")
        
    return status


@router.get("/cache/stats")
async def get_cache_statistics(
    context: dict = Depends(get_current_tenant_and_user),
):
    """Get Ignite cache statistics"""
    from ..main import app
    ignite: IgniteManager = app.state.ignite_manager
    
    stats = await ignite.get_cache_stats()
    return stats


@router.post("/sessions/{session_id}/clear")
async def clear_session_cache(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Clear all cached data for a session"""
    from ..main import app
    ignite: IgniteManager = app.state.ignite_manager
    
    await ignite.clear_session_data(session_id)
    
    # Also remove from CRDT synchronizer
    crdt_sync: CRDTSynchronizer = app.state.crdt_sync
    if session_id in crdt_sync.active_sessions:
        del crdt_sync.active_sessions[session_id]
        
    return {"status": "cleared"}


@router.websocket("/ws/sync/{session_id}")
async def websocket_sync_endpoint(
    websocket: WebSocket,
    session_id: str,
    user_id: str = "anonymous",
):
    """WebSocket endpoint for real-time CRDT synchronization"""
    await websocket.accept()
    
    from ..main import app
    crdt_sync: CRDTSynchronizer = app.state.crdt_sync
    ignite: IgniteManager = app.state.ignite_manager
    
    try:
        # Send initial state
        state = await crdt_sync.get_session_state(session_id)
        if state:
            await websocket.send_json({
                "type": "state_sync",
                "data": state
            })
            
        while True:
            # Receive operations from client
            data = await websocket.receive_json()
            
            if data["type"] == "geometry_operation":
                # Process operation
                event = GeometryOperationEvent(
                    tenant_id="default",
                    asset_id=data.get("asset_id", ""),
                    session_id=session_id,
                    user_id=user_id,
                    operation_id=data["operation_id"],
                    operation_type=data["operation_type"],
                    target_object_ids=data.get("target_object_ids", []),
                    operation_data=json.dumps(data.get("data", {})),
                    vector_clock=data.get("vector_clock", {}),
                    parent_operations=data.get("parent_operations", []),
                    timestamp=data.get("timestamp", int(datetime.utcnow().timestamp() * 1000))
                )
                
                result = await crdt_sync.handle_geometry_operation(session_id, event)
                
                # Send acknowledgment
                await websocket.send_json({
                    "type": "operation_ack",
                    "operation_id": data["operation_id"],
                    "result": result
                })
                
            elif data["type"] == "presence_update":
                # Update user presence
                await ignite.update_user_presence(
                    session_id=session_id,
                    user_id=user_id,
                    presence_data=data["presence"]
                )
                
            elif data["type"] == "request_sync":
                # Send current state
                state = await crdt_sync.get_session_state(session_id)
                if state:
                    await websocket.send_json({
                        "type": "state_sync",
                        "data": state
                    })
                    
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for session {session_id}, user {user_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await websocket.close() 