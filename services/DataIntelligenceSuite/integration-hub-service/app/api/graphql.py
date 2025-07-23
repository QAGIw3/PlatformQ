"""
GraphQL API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse
from strawberry.fastapi import GraphQLRouter
from typing import Dict, Any

from data_intelligence_common import StructuredLogger
from ..core.dependencies import get_graphql_gateway
from ..engines.graphql import GraphQLGateway

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()

# GraphQL gateway instance (will be injected)
graphql_gateway: GraphQLGateway = None


def set_graphql_deps(gateway: GraphQLGateway):
    """Set GraphQL dependencies"""
    global graphql_gateway
    graphql_gateway = gateway


# Create GraphQL router
def create_graphql_router() -> GraphQLRouter:
    """Create the GraphQL router with the gateway schema"""
    if not graphql_gateway or not graphql_gateway.schema:
        raise RuntimeError("GraphQL gateway not initialized")
    
    return GraphQLRouter(
        graphql_gateway.schema,
        context_getter=lambda: graphql_gateway.get_context()
    )


@router.get("/schema", response_model=Dict[str, Any])
async def get_schema():
    """Get the GraphQL schema"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        schema_sdl = await graphql_gateway.get_schema_sdl()
        
        return {
            "schema": schema_sdl,
            "version": graphql_gateway.federation_manager.get_schema_version(),
            "federated_services": list(graphql_gateway.federation_manager.get_service_schemas().keys())
        }
        
    except Exception as e:
        logger.error(f"Failed to get schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/federation/status", response_model=Dict[str, Any])
async def get_federation_status():
    """Get federation status"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        health = await graphql_gateway.federation_manager.health_check()
        
        return {
            "status": "healthy" if health["healthy"] else "unhealthy",
            "schema_version": health["schema_version"],
            "federated_services": health["federated_services"],
            "issues": health.get("issues", [])
        }
        
    except Exception as e:
        logger.error(f"Failed to get federation status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/federation/register", response_model=Dict[str, str])
async def register_service_schema(
    service_name: str,
    schema: str,
    endpoint: str
):
    """Register a service schema for federation"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        await graphql_gateway.federation_manager.register_service_schema(
            service_name, schema, endpoint
        )
        
        return {
            "status": "registered",
            "service": service_name,
            "message": f"Schema registered for {service_name}"
        }
        
    except Exception as e:
        logger.error(f"Failed to register schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/federation/unregister/{service_name}")
async def unregister_service_schema(service_name: str):
    """Unregister a service schema"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        await graphql_gateway.federation_manager.unregister_service_schema(service_name)
        
        return {
            "status": "unregistered",
            "service": service_name,
            "message": f"Schema unregistered for {service_name}"
        }
        
    except Exception as e:
        logger.error(f"Failed to unregister schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cache/clear")
async def clear_cache(loader_name: str = None):
    """Clear DataLoader cache"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        graphql_gateway.dataloader_registry.clear_cache(loader_name)
        
        return {
            "status": "cleared",
            "loader": loader_name or "all",
            "message": f"Cache cleared for {loader_name or 'all loaders'}"
        }
        
    except Exception as e:
        logger.error(f"Failed to clear cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=Dict[str, Any])
async def get_metrics():
    """Get GraphQL gateway metrics"""
    try:
        if not graphql_gateway:
            raise HTTPException(status_code=503, detail="GraphQL gateway not available")
        
        return {
            "gateway_metrics": graphql_gateway.metrics,
            "dataloader_stats": graphql_gateway.dataloader_registry.get_statistics(),
            "federation_health": await graphql_gateway.federation_manager.health_check()
        }
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/playground", response_class=HTMLResponse)
async def graphql_playground():
    """GraphQL Playground UI"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>GraphQL Playground</title>
        <link rel="stylesheet" href="https://unpkg.com/graphql-playground-react/build/static/css/index.css" />
        <script src="https://unpkg.com/graphql-playground-react/build/static/js/middleware.js"></script>
    </head>
    <body>
        <div id="root"></div>
        <script>
            window.addEventListener('load', function (event) {
                GraphQLPlayground.init(document.getElementById('root'), {
                    endpoint: '/api/v1/graphql'
                })
            })
        </script>
    </body>
    </html>
    """ 