import importlib
import pkgutil
from fastapi import FastAPI, Request, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Dict
import asyncio
import logging
from contextlib import asynccontextmanager

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import connector_pb2, connector_pb2_grpc
from . import plugins
from .plugins.k8s_utils import K8sJobBuilder
from platformq.shared.base_service import create_base_app
from .core.config import settings

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The scheduler is used to run connectors that have a cron schedule defined.
scheduler = AsyncIOScheduler()

# This dictionary acts as a plugin registry, holding instantiated connector plugins,
# mapping their unique 'connector_type' string to the class instance.
connector_plugins: Dict[str, plugins.base.BaseConnector] = {}

# gRPC Server implementation
class ConnectorServiceServicer(connector_pb2_grpc.ConnectorServiceServicer):
    """
    Implements the gRPC service interface for the Connector Service.
    """
    async def CreateAssetFromURI(self, request, context):
        logger.info(f"gRPC: Received CreateAssetFromURI request for URI: {request.uri} in tenant: {request.tenant_id}")
        
        # Find the appropriate connector based on file extension or URI pattern
        connector = None
        uri_lower = request.uri.lower()
        
        # Map file extensions to connectors
        extension_map = {
            '.blend': 'blender',
            '.fcstd': 'freecad',
            '.aup3': 'audacity',
            '.osp': 'openshot',
            '.xcf': 'gimp',
            '.foam': 'openfoam'
        }
        
        for ext, connector_type in extension_map.items():
            if uri_lower.endswith(ext):
                connector = connector_plugins.get(connector_type)
                break
        
        if connector:
            # Trigger the connector with the URI
            context_data = {
                "file_uri": request.uri,
                "tenant_id": request.tenant_id,
                "asset_id": request.asset_id if request.asset_id else None
            }
            
            # Run asynchronously
            asyncio.create_task(connector.run(context=context_data))
            
            return connector_pb2.CreateAssetFromURIResponse(
                asset_id=context_data.get("asset_id", "processing"),
                success=True,
                message=f"Asset creation initiated using {connector.connector_type} connector"
            )
        else:
            return connector_pb2.CreateAssetFromURIResponse(
                asset_id="",
                success=False,
                message=f"No suitable connector found for URI: {request.uri}"
            )

# Database dependency placeholders
async def get_db_session():
    # TODO: Implement actual database session when needed
    yield None

async def get_api_key_crud_placeholder():
    return None

async def get_user_crud_placeholder():
    return None

async def get_password_verifier_placeholder():
    return None


def discover_and_schedule_plugins():
    """
    Dynamically discovers, imports, and instantiates all connector plugins
    from the 'plugins' directory. This is the core of the plugin architecture.
    
    If a connector has a 'schedule' property, it is automatically added to
    the APScheduler job queue.
    """
    logger.info("Discovering connector plugins...")
    # pkgutil.iter_modules is used to find all modules in the plugins package.
    # This is more robust than walking the filesystem as it works with different
    # packaging formats.
    for (_, name, _) in pkgutil.iter_modules(plugins.__path__):
        # We skip the 'base' module as it only contains the abstract class.
        # Also skip the k8s_utils module
        if name not in ["base", "k8s_utils"]:
            try:
                plugin_module = importlib.import_module(f".{name}", plugins.__name__)
                
                # We iterate through the attributes of the loaded module to find
                # the class that inherits from our BaseConnector.
                for attribute_name in dir(plugin_module):
                    attribute = getattr(plugin_module, attribute_name)
                    if isinstance(attribute, type) and issubclass(attribute, plugins.base.BaseConnector) and attribute is not plugins.base.BaseConnector:
                        # Instantiate the connector (with config from settings)
                        config = settings.connector_plugins.get(name, {})
                        connector_instance = attribute(config=config)
                        connector_plugins[connector_instance.connector_type] = connector_instance
                        logger.info(f"Loaded connector: {connector_instance.connector_type}")
                        
                        # If a schedule is provided (e.g., '0 * * * *'), parse the cron
                        # string and add the connector's 'run' method to the scheduler.
                        if connector_instance.schedule:
                            logger.info(f"Scheduling '{connector_instance.connector_type}' with cron: {connector_instance.schedule}")
                            scheduler.add_job(
                                connector_instance.run,
                                'cron',
                                **{field: val for field, val in zip(['minute', 'hour', 'day', 'month', 'day_of_week'], connector_instance.schedule.split())}
                            )
            except Exception as e:
                logger.error(f"Failed to load plugin {name}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Connector Service...")
    
    # Initialize Kubernetes PVCs if running in cluster
    try:
        K8sJobBuilder.create_pvcs_if_not_exist()
        logger.info("Kubernetes PVCs initialized")
    except Exception as e:
        logger.warning(f"Could not initialize Kubernetes PVCs: {e}")
    
    # Discover and schedule plugins
    discover_and_schedule_plugins()
    
    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Connector Service...")
    scheduler.shutdown()


app = create_base_app(
    service_name="connector-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
    grpc_servicer=ConnectorServiceServicer(),
    grpc_add_servicer_func=connector_pb2_grpc.add_ConnectorServiceServicer_to_server,
    grpc_port=50051,
    lifespan=lifespan
)


# ============= REST API Endpoints =============

@app.get("/connectors")
async def list_connectors():
    """
    Returns a list of all available connector types that have been discovered
    and loaded. This can be used by administrators to verify which connectors
    are operational.
    """
    return {
        "connectors": list(connector_plugins.keys()),
        "count": len(connector_plugins)
    }


@app.post("/connectors/{connector_name}/run")
async def run_connector(connector_name: str, context: Dict = None):
    """
    Manually triggers a specific connector. The request body can contain any
    JSON data, which is passed as 'context' to the connector's 'run' method.
    
    This is useful for testing connectors during development or triggering
    one-off imports.
    """
    if connector_name not in connector_plugins:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    
    connector = connector_plugins[connector_name]
    
    # The connector's logic is run in the background to avoid blocking.
    asyncio.create_task(connector.run(context=context))
    
    return {
        "message": f"Connector '{connector_name}' triggered successfully.",
        "context": context
    }


@app.post("/webhooks/{connector_name}")
async def receive_webhook(connector_name: str, request: Request):
    """
    A generic endpoint to receive real-time events via webhooks and trigger
    the corresponding connector. The entire request body is passed as the
    'payload' in the context dictionary.
    """
    if connector_name not in connector_plugins:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    
    connector = connector_plugins[connector_name]
    
    try:
        payload = await request.json()
        # The connector's logic is run in the background to immediately
        # return a 200 OK to the webhook source.
        asyncio.create_task(connector.run(context={"payload": payload}))
        return {"message": "Webhook received and is being processed."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")


@app.get("/health")
async def health_check():
    """Health check endpoint for Kubernetes probes"""
    return {
        "status": "healthy",
        "service": "connector-service",
        "plugins_loaded": len(connector_plugins),
        "scheduler_running": scheduler.running
    }
