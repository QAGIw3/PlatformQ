"""
Collaboration Platform Service

Unified real-time collaboration platform for simulations, CAD, and other domains.
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from platformq_shared.events import EventPublisher
from platformq_shared.metrics import MetricsCollector
from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .domains.base import DomainRegistry
from .domains.simulation_adapter import SimulationAdapter
from .domains.cad_adapter import CADAdapter
from .core.session_manager import SessionManager
from .clients import get_state_client, get_compute_client, cleanup_clients

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Collaboration Platform Service")
    
    # Initialize domain registry
    domain_registry = DomainRegistry()
    domain_registry.register(SimulationAdapter())
    domain_registry.register(CADAdapter())
    app.state.domain_registry = domain_registry
    
    # Initialize clients
    app.state.state_client = await get_state_client()
    app.state.compute_client = await get_compute_client()
    
    # Initialize event publisher
    pulsar_url = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    app.state.event_publisher = EventPublisher(
        service_name="collaboration-platform",
        pulsar_url=pulsar_url
    )
    await app.state.event_publisher.start()
    
    # Initialize session manager
    app.state.session_manager = SessionManager(
        domain_registry=domain_registry,
        state_client=app.state.state_client,
        compute_client=app.state.compute_client,
        event_publisher=app.state.event_publisher
    )
    await app.state.session_manager.start()
    
    # Initialize metrics
    app.state.metrics = MetricsCollector("collaboration_platform")
    
    # Create required caches
    try:
        await app.state.state_client.create_cache({
            "name": "collaboration_states",
            "mode": "PARTITIONED",
            "backups": 1,
            "atomicity": "TRANSACTIONAL",
            "eviction_policy": "LRU",
            "eviction_max_size": 100000
        })
        logger.info("Created collaboration_states cache")
    except Exception as e:
        logger.warning(f"Cache might already exist: {e}")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Collaboration Platform Service")
    await app.state.session_manager.stop()
    await app.state.event_publisher.stop()
    await cleanup_clients()


# Create FastAPI app
app = FastAPI(
    title="Collaboration Platform Service",
    description="Unified real-time collaboration platform",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "collaboration-platform",
        "version": "1.0.0"
    }


# Session management endpoints
@app.post("/api/v1/sessions")
async def create_session(
    domain_type: str,
    project_name: str = None,
    description: str = None,
    current_user=Depends(get_current_user)
):
    """Create a new collaboration session"""
    # Validate domain type
    if domain_type not in app.state.domain_registry.list_domains():
        raise HTTPException(status_code=400, detail=f"Unknown domain type: {domain_type}")
    
    # Create session
    metadata = {
        "created_by": current_user["user_id"],
        "tenant_id": current_user["tenant_id"],
        "project_name": project_name,
        "description": description
    }
    
    session_id = await app.state.session_manager.create_session(
        domain_type=domain_type,
        metadata=metadata
    )
    
    # Track metric
    app.state.metrics.increment("sessions_created", tags={"domain": domain_type})
    
    return {
        "session_id": session_id,
        "domain_type": domain_type,
        "status": "created"
    }


@app.get("/api/v1/sessions/{session_id}")
async def get_session(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """Get session information"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check access
    if session.metadata.get("tenant_id") != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    return {
        "session_id": session_id,
        "domain_type": session.domain_type,
        "created_at": session.created_at.isoformat(),
        "active_users": len([u for u in session.users.values() if u.is_active()]),
        "total_users": len(session.users),
        "state_version": session.state_version,
        "resource_allocated": session.resource_allocated,
        "metadata": session.metadata
    }


@app.get("/api/v1/sessions")
async def list_sessions(
    domain_type: str = Query(None),
    active_only: bool = Query(True),
    current_user=Depends(get_current_user)
):
    """List sessions for the current tenant"""
    all_sessions = await app.state.session_manager.list_sessions(
        domain_type=domain_type,
        active_only=active_only
    )
    
    # Filter by tenant
    tenant_sessions = [
        s for s in all_sessions
        if s.get("metadata", {}).get("tenant_id") == current_user["tenant_id"]
    ]
    
    return {
        "sessions": tenant_sessions,
        "total": len(tenant_sessions)
    }


@app.delete("/api/v1/sessions/{session_id}")
async def delete_session(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """Delete a session"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check ownership
    if session.metadata.get("created_by") != current_user["user_id"]:
        raise HTTPException(status_code=403, detail="Only session creator can delete")
    
    await app.state.session_manager.delete_session(session_id)
    
    return {"status": "deleted"}


# WebSocket endpoint for collaboration
@app.websocket("/ws/collaborate/{session_id}")
async def websocket_collaborate(
    websocket: WebSocket,
    session_id: str,
    user_id: str = Query(...),
    user_name: str = Query("User")
):
    """WebSocket endpoint for real-time collaboration"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        await websocket.close(code=4004, reason="Session not found")
        return
    
    try:
        # Accept connection
        await websocket.accept()
        
        # Add user to session
        await session.add_user(user_id, user_name, websocket)
        
        # Track metric
        app.state.metrics.increment("websocket_connections", tags={"domain": session.domain_type})
        
        # Handle messages
        while True:
            message = await websocket.receive_json()
            await session.handle_user_message(user_id, message)
            
    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Remove user from session
        await session.remove_user(user_id)
        app.state.metrics.decrement("websocket_connections", tags={"domain": session.domain_type})


# Domain information endpoints
@app.get("/api/v1/domains")
async def list_domains():
    """List available collaboration domains"""
    capabilities = app.state.domain_registry.get_capabilities()
    return {
        "domains": [
            {
                "name": name,
                "capabilities": caps
            }
            for name, caps in capabilities.items()
        ]
    }


@app.get("/api/v1/domains/{domain_name}/capabilities")
async def get_domain_capabilities(domain_name: str):
    """Get capabilities of a specific domain"""
    try:
        adapter = app.state.domain_registry.get(domain_name)
        return adapter.get_capabilities()
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Domain not found: {domain_name}")


# Resource management endpoints
@app.post("/api/v1/sessions/{session_id}/allocate-resources")
async def allocate_resources(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """Manually trigger resource allocation for a session"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check access
    if session.metadata.get("tenant_id") != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Get current state
    state = await session._load_state()
    
    # Check resource requirements
    await session._check_resource_requirements(state)
    
    return {
        "resource_allocated": session.resource_allocated,
        "allocation_id": session.allocation_id
    }


@app.get("/api/v1/sessions/{session_id}/resource-usage")
async def get_resource_usage(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """Get resource usage for a session"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check access
    if session.metadata.get("tenant_id") != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Get allocation details
    if session.allocation_id:
        allocation = await app.state.compute_client.get_allocation(session.allocation_id)
        return {
            "allocated": True,
            "allocation": allocation
        }
    else:
        # Get requirements
        state = await session._load_state()
        requirements = session.domain_adapter.get_resource_requirements(state)
        return {
            "allocated": False,
            "requirements": requirements
        }


# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Get Prometheus metrics"""
    return app.state.metrics.generate_metrics()


# Session state endpoints
@app.get("/api/v1/sessions/{session_id}/state")
async def get_session_state(
    session_id: str,
    current_user=Depends(get_current_user)
):
    """Get current session state (admin/debug endpoint)"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check access
    if session.metadata.get("tenant_id") != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Load state
    state = await session._load_state()
    
    return {
        "session_id": session_id,
        "version": state.version,
        "domain_type": state.domain_type,
        "metadata": state.metadata,
        "data_size": len(str(state.data))
    }


@app.post("/api/v1/sessions/{session_id}/checkpoint")
async def create_checkpoint(
    session_id: str,
    name: str = Query(...),
    description: str = Query(""),
    current_user=Depends(get_current_user)
):
    """Create a checkpoint of the current session state"""
    session = await app.state.session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Check access
    if session.metadata.get("tenant_id") != current_user["tenant_id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    
    # Create checkpoint operation
    await session.handle_operation(
        user_id=current_user["user_id"],
        operation_data={
            "type": "custom",
            "subtype": "create_checkpoint",
            "data": {
                "checkpoint_id": f"ckpt_{session_id}_{session.state_version}",
                "name": name,
                "description": description
            }
        }
    )
    
    return {
        "status": "checkpoint_created",
        "checkpoint_id": f"ckpt_{session_id}_{session.state_version}"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 