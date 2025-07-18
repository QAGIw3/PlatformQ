"""
Graph Intelligence Service

JanusGraph-based knowledge graph and intelligence platform.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

from platformq_shared import (
    create_base_app,
    EventProcessor,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader

from .api import endpoints
from .api.endpoints import graph_api
from .api.deps import (
    get_db_session, 
    get_api_key_crud, 
    get_user_crud, 
    get_password_verifier,
    get_current_tenant_and_user
)
from .repository import (
    GraphNodeRepository,
    GraphEdgeRepository,
    GraphAnalyticsRepository
)
from .event_processors import (
    GraphUpdateProcessor,
    LineageProcessor,
    TrustNetworkProcessor,
    GraphQueryProcessor
)
from .db.janusgraph import JanusGraph
from .db.schema_manager import SchemaManager
from .services.graph_processor import GraphProcessor
from .services.lineage_tracker import LineageTracker
from .services.trust_network import TrustNetworkManager

# gRPC imports
from .grpc_generated import graph_intelligence_pb2, graph_intelligence_pb2_grpc

logger = logging.getLogger(__name__)

# Service components
graph_update_processor = None
lineage_processor = None
trust_processor = None
query_processor = None
graph_processor = None
lineage_tracker = None
trust_manager = None
service_clients = None
gremlin_url = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global graph_update_processor, lineage_processor, trust_processor, query_processor
    global graph_processor, lineage_tracker, trust_manager, service_clients, gremlin_url
    
    # Startup
    logger.info("Starting Graph Intelligence Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize JanusGraph connection
    gremlin_url = settings.get("gremlin_server", "ws://janusgraph:8182/gremlin")
    
    # Initialize and create schema
    janus_graph = JanusGraph()
    schema_manager = SchemaManager(janus_graph)
    try:
        schema_manager.create_schema()
        # Wait for critical indexes to be ready
        schema_manager.wait_for_index_status('by_asset_id')
        schema_manager.wait_for_index_status('by_user_id')
        logger.info("Graph schema initialized")
    except Exception as e:
        logger.error(f"Could not initialize graph schema: {e}")
        # Continue anyway - schema might already exist
    
    # Initialize repositories
    app.state.node_repo = GraphNodeRepository(
        gremlin_url,
        event_publisher=app.state.event_publisher
    )
    app.state.edge_repo = GraphEdgeRepository(
        gremlin_url,
        event_publisher=app.state.event_publisher
    )
    app.state.analytics_repo = GraphAnalyticsRepository(
        gremlin_url,
        event_publisher=app.state.event_publisher
    )
    
    # Initialize services
    graph_processor = GraphProcessor(gremlin_url)
    app.state.graph_processor = graph_processor
    
    lineage_tracker = LineageTracker(
        app.state.node_repo,
        app.state.edge_repo
    )
    app.state.lineage_tracker = lineage_tracker
    
    trust_manager = TrustNetworkManager(
        app.state.edge_repo,
        vc_service_url=settings.get("vc_service_url", "http://verifiable-credential-service:8000")
    )
    await trust_manager.initialize()
    app.state.trust_manager = trust_manager
    
    # Initialize event processors
    graph_update_processor = GraphUpdateProcessor(
        service_name="graph-intelligence-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        node_repo=app.state.node_repo,
        edge_repo=app.state.edge_repo,
        analytics_repo=app.state.analytics_repo,
        graph_processor=graph_processor
    )
    
    lineage_processor = LineageProcessor(
        service_name="graph-intelligence-lineage",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        node_repo=app.state.node_repo,
        edge_repo=app.state.edge_repo,
        lineage_tracker=lineage_tracker
    )
    
    trust_processor = TrustNetworkProcessor(
        service_name="graph-intelligence-trust",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        edge_repo=app.state.edge_repo,
        trust_manager=trust_manager,
        service_clients=service_clients
    )
    
    query_processor = GraphQueryProcessor(
        service_name="graph-intelligence-query",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        analytics_repo=app.state.analytics_repo,
        graph_processor=graph_processor
    )
    
    # Start event processors
    await asyncio.gather(
        graph_update_processor.start(),
        lineage_processor.start(),
        trust_processor.start(),
        query_processor.start()
    )
    
    # Start trust network sync
    app.state.trust_sync_task = asyncio.create_task(
        trust_manager.sync_trust_network()
    )
    
    logger.info("Graph Intelligence Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Graph Intelligence Service...")
    
    # Cancel trust sync
    if hasattr(app.state, "trust_sync_task"):
        app.state.trust_sync_task.cancel()
        
    # Stop event processors
    await asyncio.gather(
        graph_update_processor.stop() if graph_update_processor else asyncio.sleep(0),
        lineage_processor.stop() if lineage_processor else asyncio.sleep(0),
        trust_processor.stop() if trust_processor else asyncio.sleep(0),
        query_processor.stop() if query_processor else asyncio.sleep(0)
    )
    
    # Cleanup resources
    if trust_manager:
        await trust_manager.cleanup()
        
    logger.info("Graph Intelligence Service shutdown complete")


# gRPC Service implementation
class GraphIntelligenceServiceServicer(graph_intelligence_pb2_grpc.GraphIntelligenceServiceServicer):
    async def GetCommunityInsights(self, request, context):
        """gRPC endpoint for community insights"""
        logging.info(f"gRPC: Received GetCommunityInsights request for tenant: {request.tenant_id}")
        
        # Use analytics repository
        analytics_repo = context.app.state.analytics_repo
        communities = analytics_repo.find_communities(
            tenant_id=request.tenant_id,
            algorithm="label_propagation"
        )
        
        # Transform to protobuf format
        response = graph_intelligence_pb2.GetCommunityInsightsResponse()
        for comm in communities:
            community_proto = response.communities.add()
            community_proto.community_id = comm["community_id"]
            community_proto.user_ids.extend(comm["members"])
        
        return response


# Create app with enhanced patterns
app = create_base_app(
    service_name="graph-intelligence-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[
        graph_update_processor, lineage_processor,
        trust_processor, query_processor
    ] if all([graph_update_processor, lineage_processor, trust_processor, query_processor]) else [],
    # gRPC Configuration
    grpc_servicer=GraphIntelligenceServiceServicer(),
    grpc_add_servicer_func=graph_intelligence_pb2_grpc.add_GraphIntelligenceServiceServicer_to_server,
    grpc_port=50052
)

# Set lifespan
app.router.lifespan_context = lifespan

# Include service-specific routers
app.include_router(graph_api.router, prefix="/api/v1/graph", tags=["graph"])
app.include_router(endpoints.router, prefix="/api/v1", tags=["graph-intelligence"])

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "graph-intelligence-service",
        "version": "2.0",
        "features": [
            "janusgraph",
            "knowledge-graph",
            "data-lineage",
            "trust-network",
            "community-detection",
            "graph-analytics",
            "event-driven",
            "grpc-support"
        ]
    }


# Health check with graph connectivity
@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check including graph connectivity"""
    health = {
        "status": "healthy",
        "checks": {}
    }
    
    # Check JanusGraph connectivity
    try:
        g = Graph().traversal().withRemote(DriverRemoteConnection(gremlin_url, 'g'))
        node_count = g.V().count().next()
        health["checks"]["janusgraph"] = {
            "status": "connected",
            "node_count": node_count
        }
    except Exception as e:
        health["status"] = "unhealthy"
        health["checks"]["janusgraph"] = {
            "status": "down",
            "error": str(e)
        }
        
    # Check trust network sync
    if hasattr(app.state, "trust_manager"):
        health["checks"]["trust_network"] = {
            "status": "active" if app.state.trust_manager.is_syncing else "inactive",
            "last_sync": app.state.trust_manager.last_sync_time
        }
        
    return health


# API Endpoints using new patterns
@app.post("/api/v1/graph/nodes")
async def create_node(
    node_type: str,
    properties: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a new node in the graph"""
    tenant_id = context["tenant_id"]
    
    node_repo = app.state.node_repo
    node = node_repo.create_node(
        node_type=node_type,
        properties=properties,
        tenant_id=tenant_id
    )
    
    return {
        "node_id": node["node_id"],
        "type": node_type,
        "created": True
    }


@app.get("/api/v1/graph/nodes/{node_id}")
async def get_node(
    node_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get node by ID"""
    tenant_id = context["tenant_id"]
    
    node_repo = app.state.node_repo
    node = node_repo.get_node(node_id, tenant_id)
    
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
        
    return node


@app.post("/api/v1/graph/edges")
async def create_edge(
    from_node_id: str,
    to_node_id: str,
    edge_type: str,
    properties: Optional[Dict[str, Any]] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create edge between nodes"""
    tenant_id = context["tenant_id"]
    
    edge_repo = app.state.edge_repo
    edge = edge_repo.create_edge(
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        edge_type=edge_type,
        properties=properties or {},
        tenant_id=tenant_id
    )
    
    return {
        "edge_id": edge["edge_id"],
        "type": edge_type,
        "created": True
    }


@app.get("/api/v1/graph/nodes/{node_id}/neighbors")
async def get_node_neighbors(
    node_id: str,
    hops: int = 1,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get node neighborhood"""
    tenant_id = context["tenant_id"]
    
    analytics_repo = app.state.analytics_repo
    neighborhood = analytics_repo.get_node_neighborhood(
        node_id=node_id,
        tenant_id=tenant_id,
        hops=hops
    )
    
    return neighborhood


@app.post("/api/v1/graph/analytics/communities")
async def detect_communities(
    algorithm: str = "label_propagation",
    context: dict = Depends(get_current_tenant_and_user)
):
    """Detect communities in the graph"""
    tenant_id = context["tenant_id"]
    
    analytics_repo = app.state.analytics_repo
    communities = analytics_repo.find_communities(
        tenant_id=tenant_id,
        algorithm=algorithm
    )
    
    return {
        "communities": communities,
        "algorithm": algorithm,
        "total": len(communities)
    }


@app.post("/api/v1/graph/analytics/paths")
async def find_paths(
    from_node_id: str,
    to_node_id: str,
    max_depth: int = 5,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Find paths between two nodes"""
    tenant_id = context["tenant_id"]
    
    analytics_repo = app.state.analytics_repo
    paths = analytics_repo.find_paths(
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        tenant_id=tenant_id,
        max_depth=max_depth
    )
    
    return {
        "paths": paths,
        "count": len(paths)
    }


@app.post("/api/v1/graph/analytics/centrality")
async def calculate_centrality(
    centrality_type: str = "degree",
    context: dict = Depends(get_current_tenant_and_user)
):
    """Calculate node centrality scores"""
    tenant_id = context["tenant_id"]
    
    analytics_repo = app.state.analytics_repo
    scores = analytics_repo.calculate_centrality(
        tenant_id=tenant_id,
        centrality_type=centrality_type
    )
    
    # Get top nodes
    sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    
    return {
        "centrality_type": centrality_type,
        "top_nodes": sorted_scores[:10],
        "total_nodes": len(scores)
    }


@app.get("/api/v1/graph/lineage/{asset_id}")
async def get_asset_lineage(
    asset_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get full lineage for an asset"""
    tenant_id = context["tenant_id"]
    
    lineage_tracker = app.state.lineage_tracker
    lineage = await lineage_tracker.get_full_lineage(
        asset_id=asset_id,
        tenant_id=tenant_id
    )
    
    return {
        "asset_id": asset_id,
        "lineage": lineage
    }


# Trust network endpoints
@app.get("/api/v1/trust/scores/{entity_id}")
async def get_trust_score(
    entity_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get trust score for an entity"""
    tenant_id = context["tenant_id"]
    
    trust_manager = app.state.trust_manager
    score = await trust_manager.get_trust_score(
        entity_id=entity_id,
        tenant_id=tenant_id
    )
    
    return {
        "entity_id": entity_id,
        "trust_score": score
    }


@app.get("/api/v1/trust/network")
async def get_trust_network(
    min_score: float = 0.0,
    limit: int = 100,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get trust network for tenant"""
    tenant_id = context["tenant_id"]
    
    edge_repo = app.state.edge_repo
    
    # Get all trust edges
    g = Graph().traversal().withRemote(DriverRemoteConnection(gremlin_url, 'g'))
    trust_edges = g.E().hasLabel('has_trust_score').has('tenant_id', tenant_id) \
                      .has('score', P.gte(min_score)) \
                      .limit(limit) \
                      .elementMap().toList()
                      
    return {
        "trust_network": [dict(e) for e in trust_edges],
        "count": len(trust_edges)
    }
