from platformq_shared.base_service import create_base_app
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from .api import endpoints
from .ignite_manager import IgniteManager
from .crdt_synchronizer import CRDTSynchronizer
from .mesh_optimizer_client import MeshOptimizerClient
from .api.deps import get_db_session, get_api_key_crud_placeholder, get_user_crud_placeholder, get_password_verifier_placeholder
import asyncio
import logging

logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="cad-collaboration-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
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
    
    # Start background tasks
    asyncio.create_task(app.state.crdt_sync.start_sync_loop())
    
    logger.info("CAD Collaboration Service started successfully")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections"""
    logger.info("Shutting down CAD Collaboration Service")
    
    if hasattr(app.state, 'ignite_manager'):
        await app.state.ignite_manager.disconnect()
    
    if hasattr(app.state, 'crdt_sync'):
        await app.state.crdt_sync.stop()

@app.get("/")
def read_root():
    return {"message": "cad-collaboration-service is running"} 