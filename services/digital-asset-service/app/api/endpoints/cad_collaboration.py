from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, Request, status, BackgroundTasks
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from uuid import UUID
import uuid
import json
import asyncio
import time
import logging
from datetime import datetime

from ....platformq_shared.db_models import User
from ....platformq_shared.api.deps import get_db_session, get_current_tenant_and_user
from ....platformq_shared.events import GeometryOperationEvent, CADSessionEvent
from ....platformq_shared.event_publisher import EventPublisher
from ....platformq_shared.crdts.geometry_crdt import Geometry3DCRDT, GeometryOperation
from ...db import models
from ...schemas import cad_session as schemas
import numpy as np

logger = logging.getLogger(__name__)
router = APIRouter()

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Dict[str, WebSocket]] = {}  # session_id -> {user_id: websocket}
        self.user_sessions: Dict[str, str] = {}  # user_id -> session_id
        
    async def connect(self, websocket: WebSocket, session_id: str, user_id: str):
        await websocket.accept()
        if session_id not in self.active_connections:
            self.active_connections[session_id] = {}
        self.active_connections[session_id][user_id] = websocket
        self.user_sessions[user_id] = session_id
        
    def disconnect(self, session_id: str, user_id: str):
        if session_id in self.active_connections:
            self.active_connections[session_id].pop(user_id, None)
            if not self.active_connections[session_id]:
                del self.active_connections[session_id]
        self.user_sessions.pop(user_id, None)
        
    async def broadcast_to_session(self, session_id: str, message: dict, exclude_user: Optional[str] = None):
        if session_id in self.active_connections:
            disconnected = []
            for user_id, websocket in self.active_connections[session_id].items():
                if user_id != exclude_user:
                    try:
                        await websocket.send_json(message)
                    except:
                        disconnected.append(user_id)
            
            # Clean up disconnected clients
            for user_id in disconnected:
                self.disconnect(session_id, user_id)
                
    async def send_to_user(self, user_id: str, message: dict):
        session_id = self.user_sessions.get(user_id)
        if session_id and session_id in self.active_connections:
            websocket = self.active_connections[session_id].get(user_id)
            if websocket:
                await websocket.send_json(message)


manager = ConnectionManager()


@router.post("/assets/{asset_id}/cad-sessions", response_model=schemas.CADSession)
async def create_cad_session(
    asset_id: UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """Initialize a collaborative CAD editing session"""
    tenant_id = context["tenant_id"]
    user_id = context["user"].id if hasattr(context["user"], "id") else str(context["user_id"])
    
    # Verify asset exists and user has access
    asset = db.query(models.DigitalAsset).filter(
        models.DigitalAsset.asset_id == asset_id
    ).first()
    
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    # Create new session
    session = models.CADSession(
        asset_id=asset_id,
        tenant_id=tenant_id,
        created_by=UUID(user_id),
        active_users=[user_id],
        vector_clock={user_id: 0}
    )
    
    db.add(session)
    db.commit()
    db.refresh(session)
    
    # Publish session created event
    publisher: EventPublisher = request.app.state.event_publisher
    if publisher:
        event = CADSessionEvent(
            tenant_id=str(tenant_id),
            session_id=str(session.session_id),
            asset_id=str(asset_id),
            event_type="CREATED",
            user_id=user_id,
            active_users=[user_id],
            session_state={
                "operation_count": 0,
                "crdt_state_size": 0,
                "last_checkpoint_id": None,
                "conflict_count": 0
            },
            timestamp=int(time.time() * 1000)
        )
        
        background_tasks.add_task(
            publisher.publish,
            topic_base='cad-session-events',
            tenant_id=str(tenant_id),
            schema_class=CADSessionEvent,
            data=event
        )
    
    return session


@router.get("/assets/{asset_id}/cad-sessions/active", response_model=Optional[schemas.CADSession])
async def get_active_cad_session(
    asset_id: UUID,
    db: Session = Depends(get_db_session),
):
    """Get the active CAD session for an asset if one exists"""
    session = db.query(models.CADSession).filter(
        models.CADSession.asset_id == asset_id,
        models.CADSession.closed_at.is_(None)
    ).first()
    
    return session


@router.post("/cad-sessions/{session_id}/join")
async def join_cad_session(
    session_id: UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """Join an existing CAD session"""
    user_id = context["user"].id if hasattr(context["user"], "id") else str(context["user_id"])
    
    session = db.query(models.CADSession).filter(
        models.CADSession.session_id == session_id,
        models.CADSession.closed_at.is_(None)
    ).first()
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found or closed")
    
    # Add user to active users if not already present
    active_users = session.active_users or []
    if user_id not in active_users:
        active_users.append(user_id)
        session.active_users = active_users
        
        # Initialize user's vector clock entry
        vector_clock = session.vector_clock or {}
        vector_clock[user_id] = 0
        session.vector_clock = vector_clock
        
        db.commit()
    
    # Publish user joined event
    publisher: EventPublisher = request.app.state.event_publisher
    if publisher:
        event = CADSessionEvent(
            tenant_id=str(session.tenant_id),
            session_id=str(session_id),
            asset_id=str(session.asset_id),
            event_type="USER_JOINED",
            user_id=user_id,
            active_users=active_users,
            timestamp=int(time.time() * 1000)
        )
        
        background_tasks.add_task(
            publisher.publish,
            topic_base='cad-session-events',
            tenant_id=str(session.tenant_id),
            schema_class=CADSessionEvent,
            data=event
        )
    
    return {
        "session_id": str(session_id),
        "active_users": active_users,
        "crdt_state": session.crdt_state,
        "vector_clock": session.vector_clock
    }


@router.websocket("/ws/cad-sessions/{session_id}")
async def cad_collaboration_websocket(
    websocket: WebSocket,
    session_id: UUID,
    user_id: str,  # Should come from auth headers in production
    db: Session = Depends(get_db_session),
):
    """WebSocket endpoint for real-time CAD collaboration"""
    
    # Verify session exists
    session = db.query(models.CADSession).filter(
        models.CADSession.session_id == session_id,
        models.CADSession.closed_at.is_(None)
    ).first()
    
    if not session:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    
    await manager.connect(websocket, str(session_id), user_id)
    
    # Send initial state
    await websocket.send_json({
        "type": "initial_state",
        "crdt_state": session.crdt_state.hex() if session.crdt_state else None,
        "vector_clock": session.vector_clock,
        "active_users": session.active_users
    })
    
    # Notify others of new user
    await manager.broadcast_to_session(
        str(session_id),
        {
            "type": "user_joined",
            "user_id": user_id,
            "timestamp": int(time.time() * 1000)
        },
        exclude_user=user_id
    )
    
    try:
        # Initialize CRDT for this session if needed
        if session.crdt_state:
            crdt = Geometry3DCRDT.deserialize(session.crdt_state)
        else:
            crdt = Geometry3DCRDT(user_id)
        
        while True:
            # Receive geometry operations
            data = await websocket.receive_json()
            
            if data["type"] == "geometry_operation":
                operation_data = data["operation"]
                
                # Apply operation to CRDT
                operation = GeometryOperation(
                    id=operation_data["id"],
                    replica_id=user_id,
                    operation_type=operation_data["operation_type"],
                    timestamp=time.time(),
                    vector_clock=operation_data.get("vector_clock", {}),
                    target_id=operation_data.get("target_id"),
                    data=operation_data.get("data", {}),
                    parent_operations=operation_data.get("parent_operations", [])
                )
                
                # Broadcast to other users
                await manager.broadcast_to_session(
                    str(session_id),
                    {
                        "type": "geometry_operation",
                        "operation": operation_data,
                        "from_user": user_id
                    },
                    exclude_user=user_id
                )
                
                # Update session state periodically (every 10 operations)
                session.operation_count += 1
                if session.operation_count % 10 == 0:
                    session.crdt_state = crdt.serialize()
                    db.commit()
                    
            elif data["type"] == "request_sync":
                # Send current CRDT state
                await websocket.send_json({
                    "type": "sync_state",
                    "crdt_state": crdt.serialize().hex(),
                    "vector_clock": crdt.vector_clock.clock
                })
                
            elif data["type"] == "cursor_position":
                # Broadcast cursor position to others
                await manager.broadcast_to_session(
                    str(session_id),
                    {
                        "type": "cursor_position",
                        "user_id": user_id,
                        "position": data["position"]
                    },
                    exclude_user=user_id
                )
                
    except WebSocketDisconnect:
        manager.disconnect(str(session_id), user_id)
        
        # Update session active users
        session = db.query(models.CADSession).filter(
            models.CADSession.session_id == session_id
        ).first()
        
        if session and session.active_users:
            active_users = [u for u in session.active_users if u != user_id]
            session.active_users = active_users
            db.commit()
        
        # Notify others
        await manager.broadcast_to_session(
            str(session_id),
            {
                "type": "user_left",
                "user_id": user_id,
                "timestamp": int(time.time() * 1000)
            }
        )


@router.post("/cad-sessions/{session_id}/checkpoint")
async def create_checkpoint(
    session_id: UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a checkpoint of the current session state"""
    session = db.query(models.CADSession).filter(
        models.CADSession.session_id == session_id
    ).first()
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # TODO: Store snapshot in MinIO
    snapshot_uri = f"s3://cad-snapshots/{session.tenant_id}/{session_id}/{uuid.uuid4()}.geo"
    
    checkpoint = models.CADCheckpoint(
        session_id=session_id,
        snapshot_uri=snapshot_uri,
        operation_range_start=str(session.operation_count - 100),  # Last 100 ops
        operation_range_end=str(session.operation_count),
        operation_count=100,
        crdt_state_size=len(session.crdt_state) if session.crdt_state else 0
    )
    
    db.add(checkpoint)
    session.last_checkpoint_id = checkpoint.checkpoint_id
    db.commit()
    
    # Publish checkpoint event
    publisher: EventPublisher = request.app.state.event_publisher
    if publisher:
        event = CADSessionEvent(
            tenant_id=str(session.tenant_id),
            session_id=str(session_id),
            asset_id=str(session.asset_id),
            event_type="CHECKPOINT_CREATED",
            active_users=session.active_users,
            checkpoint_data={
                "checkpoint_id": str(checkpoint.checkpoint_id),
                "snapshot_uri": snapshot_uri,
                "operation_range": {
                    "start_operation_id": checkpoint.operation_range_start,
                    "end_operation_id": checkpoint.operation_range_end,
                    "operation_count": checkpoint.operation_count
                }
            },
            timestamp=int(time.time() * 1000)
        )
        
        background_tasks.add_task(
            publisher.publish,
            topic_base='cad-session-events',
            tenant_id=str(session.tenant_id),
            schema_class=CADSessionEvent,
            data=event
        )
    
    return {"checkpoint_id": str(checkpoint.checkpoint_id)}


@router.post("/assets/{asset_id}/geometry-versions")
async def create_geometry_version(
    asset_id: UUID,
    version_data: schemas.CreateGeometryVersion,
    db: Session = Depends(get_db_session),
    context: dict = Depends(get_current_tenant_and_user),
):
    """Create a new version of the geometry"""
    user_id = context["user"].id if hasattr(context["user"], "id") else str(context["user_id"])
    
    # Get latest version number
    latest_version = db.query(models.GeometryVersion).filter(
        models.GeometryVersion.asset_id == asset_id
    ).order_by(models.GeometryVersion.version_number.desc()).first()
    
    next_version_number = 1 if not latest_version else latest_version.version_number + 1
    
    version = models.GeometryVersion(
        asset_id=asset_id,
        version_number=next_version_number,
        created_by=UUID(user_id),
        geometry_snapshot_uri=version_data.snapshot_uri,
        parent_version_id=latest_version.version_id if latest_version else None,
        vertex_count=version_data.vertex_count,
        edge_count=version_data.edge_count,
        face_count=version_data.face_count,
        file_size_bytes=version_data.file_size_bytes,
        commit_message=version_data.commit_message,
        version_metadata=version_data.metadata or {}
    )
    
    db.add(version)
    db.commit()
    db.refresh(version)
    
    return version 