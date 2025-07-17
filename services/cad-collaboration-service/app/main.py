"""
CAD Collaboration Service

Provides real-time collaborative CAD editing with:
- Real-time mesh decimation and LOD management
- Conflict resolution with Operational Transform
- Multi-user awareness and presence
- Version control and branching
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from platformq_shared.base_service import create_base_app
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from .api import endpoints
from .ignite_manager import IgniteManager
from .crdt_synchronizer import CRDTSynchronizer
from .mesh_optimizer_client import MeshOptimizerClient
from .mesh_decimator import MeshDecimator
from .collaboration_engine import CollaborationEngine
from .simulation_consumer import SimulationConsumer
from .quantum_optimization import QuantumMeshOptimizer, QuantumCADIntegration
from .api.deps import get_db_session, get_api_key_crud_placeholder, get_user_crud_placeholder, get_password_verifier_placeholder
import asyncio
import logging
import json
import pulsar
from .mesh_optimizer_client import MeshOptimizationRequest

logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="cad-collaboration-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Add CORS middleware for WebSocket support
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["cad-collaboration"])

# Initialize services
@app.on_event("startup")
async def startup_event():
    """Initialize connections to Ignite, Pulsar, etc."""
    logger.info("Starting CAD Collaboration Service")
    
    # Initialize Ignite connection
    app.state.ignite_manager = IgniteManager()
    await app.state.ignite_manager.connect()
    
    # Initialize CRDT synchronizer
    app.state.crdt_sync = CRDTSynchronizer(
        ignite_manager=app.state.ignite_manager,
        event_publisher=app.state.event_publisher
    )
    
    # Initialize mesh optimizer client
    app.state.mesh_optimizer = MeshOptimizerClient()
    
    # Initialize mesh decimator
    app.state.mesh_decimator = MeshDecimator(
        algorithm="quadric",
        preserve_features=True,
        preserve_boundaries=True
    )
    
    # Initialize collaboration engine
    app.state.collaboration_engine = CollaborationEngine(
        conflict_resolution="operational_transform",
        history_limit=1000
    )
    
    # Initialize quantum optimization
    config_loader = ConfigLoader()
    quantum_service_url = config_loader.get_setting("QUANTUM_SERVICE_URL", "http://quantum-optimization-service:8000")
    app.state.quantum_optimizer = QuantumMeshOptimizer(
        quantum_service_url,
        app.state.ignite_manager.client,
        app.state.event_publisher
    )
    
    app.state.quantum_integration = QuantumCADIntegration(
        app.state.quantum_optimizer,
        app.state.ignite_manager.client,
        app.state.event_publisher
    )

    # Initialize Pulsar client for consumer
    pulsar_client = pulsar.Client('pulsar://pulsar:6650')

    # Initialize and start simulation consumer
    app.state.simulation_consumer = SimulationConsumer(
        pulsar_client,
        app.state.collaboration_engine
    )
    await app.state.simulation_consumer.start()
    
    # Start background tasks
    asyncio.create_task(app.state.crdt_sync.start_sync_loop())
    asyncio.create_task(process_quantum_optimization_queue(app.state))
    
    logger.info("CAD Collaboration Service started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources"""
    if hasattr(app.state, "quantum_optimizer"):
        await app.state.quantum_optimizer.close()
    
    if hasattr(app.state, "simulation_consumer"):
        await app.state.simulation_consumer.stop()
    
    if hasattr(app.state, "ignite_manager"):
        await app.state.ignite_manager.disconnect()
    
    logger.info("CAD Collaboration Service shutdown complete")


async def process_quantum_optimization_queue(app_state):
    """Background task to process queued quantum optimizations"""
    while True:
        try:
            queue_cache = app_state.ignite_manager.client.get_or_create_cache("quantum_optimization_queue")
            
            # Process queued optimizations
            query = queue_cache.scan()
            for queue_id, item in query:
                if item["priority"] > 0.5:  # Process medium-high priority items
                    request = MeshOptimizationRequest(**item["request"])
                    result = await app_state.quantum_optimizer.optimize_mesh(
                        item["session_id"],
                        request
                    )
                    
                    # Remove from queue
                    queue_cache.remove(queue_id)
                    
                    # Broadcast result
                    await app_state.quantum_integration._broadcast_optimization_result(
                        item["session_id"],
                        result
                    )
                    
            await asyncio.sleep(5)  # Check every 5 seconds
            
        except Exception as e:
            logger.error(f"Error processing quantum optimization queue: {e}")
            await asyncio.sleep(10)


# WebSocket endpoint for real-time collaboration
@app.websocket("/ws/collaborate/{session_id}/{user_id}")
async def websocket_collaborate(websocket: WebSocket, session_id: str, user_id: str):
    """WebSocket endpoint for real-time CAD collaboration"""
    await websocket.accept()
    
    # Store user_id with websocket for filtering
    websocket.user_id = user_id
    
    # Register websocket with collaboration engine
    app.state.collaboration_engine.register_websocket(session_id, websocket)
    
    try:
        # Send initial session state
        if session_id in app.state.collaboration_engine.sessions:
            session = app.state.collaboration_engine.sessions[session_id]
            
            await websocket.send_json({
                "type": "session_state",
                "session": {
                    "id": session.session_id,
                    "users": [
                        {
                            "id": u.user_id,
                            "name": u.name,
                            "color": u.color,
                            "status": u.status
                        }
                        for u in session.users.values()
                    ],
                    "operation_count": len(session.operations),
                    "checkpoints": [
                        {
                            "id": cp["id"],
                            "name": cp["name"],
                            "timestamp": cp["timestamp"]
                        }
                        for cp in session.checkpoints
                    ]
                }
            })
            
        # Handle incoming messages
        while True:
            data = await websocket.receive_json()
            
            if data["type"] == "presence_update":
                # Update user presence
                await app.state.collaboration_engine.update_presence(
                    session_id,
                    user_id,
                    data["presence"]
                )
                
            elif data["type"] == "operation":
                # Handle geometry operation
                from .collaboration_engine import GeometryOperation
                
                operation = GeometryOperation(
                    operation_id=data["operation"]["id"],
                    operation_type=data["operation"]["type"],
                    user_id=user_id,
                    session_id=session_id,
                    timestamp=data["operation"]["timestamp"],
                    target_objects=data["operation"]["target_objects"],
                    parameters=data["operation"]["parameters"],
                    parent_operations=data["operation"].get("parent_operations", [])
                )
                
                # Apply operation with conflict resolution
                result = await app.state.collaboration_engine.apply_operation(
                    session_id,
                    operation
                )
                
                # Send result back to user
                await websocket.send_json({
                    "type": "operation_result",
                    "operation_id": operation.operation_id,
                    "status": "applied" if not result["conflicts"] else "resolved",
                    "conflicts": result["conflicts"]
                })
                
            elif data["type"] == "mesh_sync":
                # Handle mesh LOD synchronization
                lod_data = await app.state.collaboration_engine.sync_mesh_lod(
                    session_id,
                    data["object_id"],
                    data["mesh_data"],
                    data.get("viewport", {})
                )
                
                await websocket.send_json({
                    "type": "mesh_lod",
                    "object_id": data["object_id"],
                    "lod_data": lod_data
                })
                
            elif data["type"] == "create_checkpoint":
                # Create checkpoint
                checkpoint = await app.state.collaboration_engine.create_checkpoint(
                    session_id,
                    data["name"],
                    data.get("description", "")
                )
                
                await websocket.send_json({
                    "type": "checkpoint_created",
                    "checkpoint": checkpoint
                })
                
            elif data["type"] == "ping":
                # Keep-alive
                await websocket.send_json({"type": "pong"})
                
    except WebSocketDisconnect:
        logger.info(f"User {user_id} disconnected from session {session_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        # Clean up
        app.state.collaboration_engine.unregister_websocket(session_id, websocket)
        await app.state.collaboration_engine.leave_session(session_id, user_id)


# Additional endpoints for mesh decimation
@app.post("/api/v1/mesh/decimate")
async def decimate_mesh(mesh_data: dict):
    """Decimate a mesh to reduce complexity"""
    try:
        import numpy as np
        
        vertices = np.array(mesh_data["vertices"])
        faces = np.array(mesh_data["faces"])
        target_ratio = mesh_data.get("target_ratio", 0.5)
        
        result = await app.state.mesh_decimator.decimate_mesh(
            vertices,
            faces,
            target_ratio=target_ratio
        )
        
        return {
            "vertices": result["vertices"].tolist(),
            "faces": result["faces"].tolist(),
            "original_vertices": result["original_vertices"],
            "original_faces": result["original_faces"],
            "decimation_ratio": result["decimation_ratio"],
            "processing_time": result["processing_time"]
        }
        
    except Exception as e:
        logger.error(f"Mesh decimation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/mesh/generate-lods")
async def generate_lods(mesh_data: dict):
    """Generate multiple LOD levels for a mesh"""
    try:
        import numpy as np
        
        vertices = np.array(mesh_data["vertices"])
        faces = np.array(mesh_data["faces"])
        lod_levels = mesh_data.get("lod_levels", [1.0, 0.5, 0.25, 0.1])
        
        lods = await app.state.mesh_decimator.generate_lods(
            vertices,
            faces,
            lod_levels=lod_levels
        )
        
        return {
            "lods": [
                {
                    "level": lod.level,
                    "vertices": lod.vertices.tolist(),
                    "faces": lod.faces.tolist(),
                    "vertex_count": lod.vertex_count,
                    "face_count": lod.face_count,
                    "error_metric": lod.error_metric
                }
                for lod in lods
            ]
        }
        
    except Exception as e:
        logger.error(f"LOD generation error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check
@app.get("/health")
async def health_check():
    """Service health check"""
    ignite_status = "healthy"
    try:
        if hasattr(app.state, "ignite_manager"):
            # Check Ignite connection
            pass  # Add actual health check
    except:
        ignite_status = "unhealthy"
        
    return {
        "status": "healthy" if ignite_status == "healthy" else "degraded",
        "service": "cad-collaboration-service",
        "components": {
            "ignite": ignite_status,
            "mesh_decimator": hasattr(app.state, "mesh_decimator"),
            "collaboration_engine": hasattr(app.state, "collaboration_engine"),
            "active_sessions": len(app.state.collaboration_engine.sessions) if hasattr(app.state, "collaboration_engine") else 0
        }
    } 