"""
Graph Intelligence Service

JanusGraph-based knowledge graph and intelligence platform.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.traversal import P
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import consul
import json

from platformq_shared import (
    create_base_app,
    EventProcessor,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader

from .vault_consul_integration import VaultConsulIntegration
from .api import endpoints
from .api.endpoints import graph_api
from .api import compute_market_endpoints
from .api import trading_risk_api
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
from .compute_market_insights import (
    ComputeMarketIntelligence,
    MarketParticipant,
    MarketRelationship,
    MarketInsight,
    MarketParticipantType
)


# gRPC imports
from .grpc_generated import graph_intelligence_pb2, graph_intelligence_pb2_grpc

logger = logging.getLogger(__name__)

# Service components
vault_consul = None
graph_update_processor = None
lineage_processor = None
trust_processor = None
query_processor = None
graph_processor = None
lineage_tracker = None
trust_network_manager = None
compute_market_intelligence = None


async def get_vault_consul() -> VaultConsulIntegration:
    """Get Vault/Consul integration instance"""
    global vault_consul
    if not vault_consul:
        raise RuntimeError("Vault/Consul integration not initialized")
    return vault_consul


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    global vault_consul, graph_update_processor, lineage_processor
    global trust_processor, query_processor, graph_processor
    global lineage_tracker, trust_network_manager, compute_market_intelligence
    
    logger.info("Starting Graph Intelligence Service...")
    
    # Initialize Vault/Consul integration
    vault_consul = VaultConsulIntegration()
    await vault_consul.initialize()
    
    # Initialize JanusGraph connection
    graph_db = JanusGraph()
    graph_db.connect()
    
    # Initialize repositories
    node_repo = GraphNodeRepository(
        gremlin_url=vault_consul.janusgraph_config.get('gremlin_url', 'ws://janusgraph:8182/gremlin')
    )
    edge_repo = GraphEdgeRepository(
        gremlin_url=vault_consul.janusgraph_config.get('gremlin_url', 'ws://janusgraph:8182/gremlin')
    )
    analytics_repo = GraphAnalyticsRepository(
        gremlin_url=vault_consul.janusgraph_config.get('gremlin_url', 'ws://janusgraph:8182/gremlin')
    )
    
    # Initialize services
    graph_processor = GraphProcessor(graph_db)
    lineage_tracker = LineageTracker(graph_db)
    trust_network_manager = TrustNetworkManager(graph_db)
    
    # Initialize compute market intelligence
    compute_market_intelligence = ComputeMarketIntelligence(
        janusgraph_client=graph_db,
        derivatives_engine_url=await vault_consul.get_service_endpoint('derivatives-engine-service')
    )
    

    
    # Initialize service clients
    service_clients = ServiceClients()
    
    # Initialize event processors
    graph_update_processor = GraphUpdateProcessor(
        service_name="graph-intelligence-service",
        pulsar_url=vault_consul.pulsar_config.get('url', 'pulsar://pulsar:6650'),
        node_repo=node_repo,
        edge_repo=edge_repo,
        analytics_repo=analytics_repo,
        graph_processor=graph_processor
    )
    
    lineage_processor = LineageProcessor(
        service_name="graph-intelligence-service",
        pulsar_url=vault_consul.pulsar_config.get('url', 'pulsar://pulsar:6650'),
        node_repo=node_repo,
        edge_repo=edge_repo,
        lineage_tracker=lineage_tracker
    )
    
    trust_processor = TrustNetworkProcessor(
        service_name="graph-intelligence-service",
        pulsar_url=vault_consul.pulsar_config.get('url', 'pulsar://pulsar:6650'),
        edge_repo=edge_repo,
        trust_manager=trust_network_manager,
        service_clients=service_clients
    )
    
    query_processor = GraphQueryProcessor(
        service_name="graph-intelligence-service",
        pulsar_url=vault_consul.pulsar_config.get('url', 'pulsar://pulsar:6650'),
        analytics_repo=analytics_repo,
        graph_processor=graph_processor
    )
    
    # Store in app state
    app.state.vault_consul = vault_consul
    app.state.graph_db = graph_db
    app.state.node_repo = node_repo
    app.state.edge_repo = edge_repo
    app.state.analytics_repo = analytics_repo
    app.state.graph_processor = graph_processor
    app.state.lineage_tracker = lineage_tracker
    app.state.trust_network_manager = trust_network_manager
    app.state.compute_market_intelligence = compute_market_intelligence
    
    # Start event processors
    await graph_update_processor.start()
    await lineage_processor.start()
    await trust_processor.start()
    await query_processor.start()
    
    # Initialize schema
    schema_manager = SchemaManager(graph_db)
    schema_manager.create_schema()
    
    logger.info("Graph Intelligence Service started successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Graph Intelligence Service...")
    
    await graph_update_processor.stop()
    await lineage_processor.stop()
    await trust_processor.stop()
    await query_processor.stop()
    
    await compute_market_intelligence.close()
    await vault_consul.close()
    
    graph_db.close()
    
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


# Create FastAPI app
app = create_base_app(
    title="Graph Intelligence Service",
    description="JanusGraph-based knowledge graph and intelligence platform",
    version="1.0.0",
    lifespan=lifespan
)

# Add error handlers
add_error_handlers(app)

# Include routers
app.include_router(endpoints.router, prefix="/api/v1/graph")
app.include_router(graph_api.router, prefix="/api/v1/graph")
app.include_router(compute_market_endpoints.router, prefix="/api/v1/graph")


# Health check endpoints remain the same...

# ... existing code ...

@app.get("/")
def read_root():
    """Service information endpoint"""
    return {
        "service": "graph-intelligence-service",
        "version": "1.0.0",
        "status": "operational",
        "description": "JanusGraph-based knowledge graph and intelligence platform",
        "features": [
            "graph-analytics",
            "community-detection",
            "trust-networks",
            "fraud-detection",
            "recommendations",
            "lineage-tracking",
            "compute-market-intelligence"
        ]
    }

# ... rest of the existing code remains the same ...
