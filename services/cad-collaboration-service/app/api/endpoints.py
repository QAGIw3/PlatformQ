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
from ..collaboration_engine import CollaborationEngine, GeometryOperation
from platformq_shared.events import GeometryOperationEvent, MeshOptimizationResult

logger = logging.getLogger(__name__)
router = APIRouter()


# Collaboration session endpoints
@router.post("/sessions")
async def create_collaboration_session(
    project_id: str,
    user_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a new collaboration session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    user_id = context["user"]["id"]
    
    session = await collaboration_engine.create_session(
        project_id=project_id,
        user_id=user_id,
        user_name=user_name
    )
    
    return {
        "session_id": session.session_id,
        "project_id": session.project_id,
        "created_at": session.created_at,
        "join_url": f"/ws/collaborate/{session.session_id}/{user_id}"
    }


@router.post("/sessions/{session_id}/join")
async def join_collaboration_session(
    session_id: str,
    user_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Join an existing collaboration session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    user_id = context["user"]["id"]
    
    try:
        presence = await collaboration_engine.join_session(
            session_id=session_id,
            user_id=user_id,
            user_name=user_name
        )
        
        return {
            "session_id": session_id,
            "user_id": user_id,
            "color": presence.color,
            "join_url": f"/ws/collaborate/{session_id}/{user_id}"
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/sessions/{session_id}/leave")
async def leave_collaboration_session(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Leave a collaboration session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    user_id = context["user"]["id"]
    
    await collaboration_engine.leave_session(session_id, user_id)
    
    return {"status": "left", "session_id": session_id}


@router.get("/sessions/{session_id}")
async def get_session_info(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get information about a collaboration session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    if session_id not in collaboration_engine.sessions:
        raise HTTPException(status_code=404, detail="Session not found")
        
    session = collaboration_engine.sessions[session_id]
    
    return {
        "session_id": session.session_id,
        "project_id": session.project_id,
        "created_at": session.created_at,
        "users": [
            {
                "id": u.user_id,
                "name": u.name,
                "color": u.color,
                "status": u.status,
                "last_seen": u.last_seen
            }
            for u in session.users.values()
        ],
        "operation_count": len(session.operations),
        "checkpoint_count": len(session.checkpoints)
    }


@router.get("/sessions/{session_id}/operations")
async def get_session_operations(
    session_id: str,
    limit: int = 100,
    offset: int = 0,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get operation history for a session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    if session_id not in collaboration_engine.sessions:
        raise HTTPException(status_code=404, detail="Session not found")
        
    session = collaboration_engine.sessions[session_id]
    
    # Get operations with pagination
    operations = session.operations[offset:offset + limit]
    
    return {
        "operations": [
            {
                "id": op.operation_id,
                "type": op.operation_type,
                "user_id": op.user_id,
                "timestamp": op.timestamp,
                "target_objects": op.target_objects,
                "parameters": op.parameters
            }
            for op in operations
        ],
        "total": len(session.operations),
        "limit": limit,
        "offset": offset
    }


@router.post("/sessions/{session_id}/checkpoints")
async def create_checkpoint(
    session_id: str,
    name: str,
    description: str = "",
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a checkpoint in the session"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    try:
        checkpoint = await collaboration_engine.create_checkpoint(
            session_id=session_id,
            name=name,
            description=description
        )
        
        return checkpoint
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/sessions/{session_id}/branch")
async def branch_from_checkpoint(
    session_id: str,
    checkpoint_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a new branch from a checkpoint"""
    from ..main import app
    collaboration_engine: CollaborationEngine = app.state.collaboration_engine
    
    try:
        branch_session = await collaboration_engine.branch_from_checkpoint(
            session_id=session_id,
            checkpoint_id=checkpoint_id
        )
        
        return {
            "branch_session_id": branch_session.session_id,
            "parent_session_id": session_id,
            "checkpoint_id": checkpoint_id,
            "created_at": branch_session.created_at
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


# Original CRDT sync endpoints
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
    """Get the current state of a CAD collaboration session"""
    from ..main import app
    ignite_manager: IgniteManager = app.state.ignite_manager
    
    # Get session state from Ignite
    session_key = f"session:{context['tenant_id']}:{session_id}"
    session_state = await ignite_manager.get_cache_value("cad_sessions", session_key)
    
    if not session_state:
        raise HTTPException(status_code=404, detail="Session not found")
        
    # Get geometry state
    geometry_key = f"geometry:{context['tenant_id']}:{session_id}"
    geometry_state = await ignite_manager.get_cache_value("cad_geometry", geometry_key)
    
    # Get connected users
    users_key = f"users:{context['tenant_id']}:{session_id}"
    connected_users = await ignite_manager.get_cache_value("cad_users", users_key) or []
    
    return {
        "session_id": session_id,
        "state": session_state,
        "geometry": geometry_state,
        "connected_users": connected_users,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/optimize-mesh")
async def optimize_mesh(
    mesh_data: Dict[str, Any],
    optimization_level: str = "medium",
    preserve_features: bool = True,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Optimize a 3D mesh for better performance"""
    from ..main import app
    mesh_optimizer: MeshOptimizerClient = app.state.mesh_optimizer
    
    try:
        # Call mesh optimization service
        result = await mesh_optimizer.optimize_mesh(
            vertices=mesh_data.get("vertices", []),
            faces=mesh_data.get("faces", []),
            normals=mesh_data.get("normals"),
            uvs=mesh_data.get("uvs"),
            optimization_level=optimization_level,
            preserve_features=preserve_features
        )
        
        # Publish optimization result event
        event = MeshOptimizationResult(
            tenant_id=context["tenant_id"],
            mesh_id=mesh_data.get("mesh_id", str(uuid.uuid4())),
            original_vertex_count=len(mesh_data.get("vertices", [])),
            optimized_vertex_count=result["vertex_count"],
            original_face_count=len(mesh_data.get("faces", [])),
            optimized_face_count=result["face_count"],
            optimization_level=optimization_level,
            processing_time_ms=result["processing_time"],
            quality_score=result.get("quality_score", 1.0)
        )
        
        event_publisher: EventPublisher = app.state.event_publisher
        await event_publisher.publish_event(event)
        
        return result
        
    except Exception as e:
        logger.error(f"Mesh optimization failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Mesh optimization failed: {str(e)}")


@router.websocket("/ws/{session_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    session_id: str,
    tenant_id: str,
    user_id: str,
):
    """WebSocket endpoint for real-time CAD collaboration"""
    from ..main import app
    
    await websocket.accept()
    
    # Store connection info
    connection_key = f"ws:{tenant_id}:{session_id}:{user_id}"
    ignite_manager: IgniteManager = app.state.ignite_manager
    
    try:
        # Register user connection
        await ignite_manager.put_cache_value(
            "cad_connections",
            connection_key,
            {
                "user_id": user_id,
                "session_id": session_id,
                "connected_at": datetime.utcnow().isoformat()
            }
        )
        
        # Main message loop
        while True:
            data = await websocket.receive_json()
            
            # Handle different message types
            if data["type"] == "geometry_operation":
                # Process and broadcast geometry operation
                event = GeometryOperationEvent(
                    tenant_id=tenant_id,
                    asset_id=data.get("asset_id", ""),
                    session_id=session_id,
                    user_id=user_id,
                    operation_id=data["operation_id"],
                    operation_type=data["operation_type"],
                    target_object_ids=data.get("target_object_ids", []),
                    operation_data=json.dumps(data.get("data", {})),
                    vector_clock=data.get("vector_clock", {}),
                    parent_operations=data.get("parent_operations", []),
                    timestamp=int(datetime.utcnow().timestamp() * 1000)
                )
                
                # Handle through CRDT synchronizer
                crdt_sync: CRDTSynchronizer = app.state.crdt_sync
                result = await crdt_sync.handle_geometry_operation(session_id, event)
                
                # Send result back
                await websocket.send_json({
                    "type": "operation_result",
                    "operation_id": data["operation_id"],
                    "result": result
                })
                
            elif data["type"] == "cursor_update":
                # Broadcast cursor position to other users
                await broadcast_to_session(
                    session_id,
                    {
                        "type": "cursor_update",
                        "user_id": user_id,
                        "position": data["position"]
                    },
                    exclude_user=user_id
                )
                
            elif data["type"] == "selection_update":
                # Broadcast selection changes
                await broadcast_to_session(
                    session_id,
                    {
                        "type": "selection_update",
                        "user_id": user_id,
                        "selected_objects": data["selected_objects"]
                    },
                    exclude_user=user_id
                )
                
    except WebSocketDisconnect:
        # Clean up on disconnect
        await ignite_manager.delete_cache_value("cad_connections", connection_key)
        
        # Notify other users
        await broadcast_to_session(
            session_id,
            {
                "type": "user_disconnected",
                "user_id": user_id
            }
        )


async def broadcast_to_session(session_id: str, message: Dict[str, Any], exclude_user: Optional[str] = None):
    """Broadcast a message to all users in a session"""
    # This is a placeholder - in production, you'd maintain WebSocket connections
    # and broadcast through them
    logger.info(f"Broadcasting to session {session_id}: {message}") 