from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks
from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from .api import endpoints
from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder,
    get_current_tenant_and_user
)
import logging
import os
import httpx
import asyncio
from typing import Dict, List, Any
from datetime import datetime
import threading
import time

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import graph_intelligence_pb2, graph_intelligence_pb2_grpc

# In a real app, this would come from config/vault
JANUSGRAPH_URL = 'ws://platformq-janusgraph:8182/gremlin'
VC_SERVICE_URL = os.environ.get("VC_SERVICE_URL", "http://verifiable-credential-service:8000")

logger = logging.getLogger(__name__)

class GraphIntelligenceServiceServicer(graph_intelligence_pb2_grpc.GraphIntelligenceServiceServicer):
    async def GetCommunityInsights(self, request, context):
        logging.info(f"gRPC: Received GetCommunityInsights request for tenant: {request.tenant_id}")
        g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
        
        # Enhanced with trust data
        community_data = g.V().has('tenant_id', request.tenant_id) \
                           .group().by('community').by('name').toList()

        # Transform the Gremlin output into the protobuf message format
        response = graph_intelligence_pb2.GetCommunityInsightsResponse()
        for comm in community_data:
            for comm_id, user_list in comm.items():
                community_proto = response.communities.add()
                community_proto.community_id = str(comm_id)
                community_proto.user_ids.extend(user_list)
        
        return response

app = create_base_app(
    service_name="graph-intelligence-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
    # --- gRPC Configuration ---
    grpc_servicer=GraphIntelligenceServiceServicer(),
    grpc_add_servicer_func=graph_intelligence_pb2_grpc.add_GraphIntelligenceServiceServicer_to_server,
    grpc_port=50052
)

# Trust Network Synchronization
class TrustNetworkSync:
    """Synchronizes trust network data from VC service to JanusGraph"""
    
    def __init__(self):
        self.g = None
        self.running = False
        self.sync_interval = 300  # 5 minutes
        
    def connect(self):
        """Connect to JanusGraph"""
        self.g = Graph().traversal().withRemote(
            DriverRemoteConnection(JANUSGRAPH_URL, 'g')
        )
        
    def sync_trust_data(self):
        """Fetch trust network data and update graph"""
        try:
            # Fetch trust network stats
            with httpx.Client() as client:
                response = client.get(f"{VC_SERVICE_URL}/api/v1/trust/network/stats")
                if response.status_code == 200:
                    stats = response.json()
                    logger.info(f"Trust network stats: {stats}")
                    
            # TODO: Fetch individual entities and relationships
            # This would require pagination through all entities
            
        except Exception as e:
            logger.error(f"Error syncing trust data: {e}")
            
    def update_entity_in_graph(self, entity_id: str, trust_score: float, trust_level: str):
        """Update or create entity node with trust data"""
        try:
            # Check if entity exists
            existing = self.g.V().has('entity_id', entity_id).toList()
            
            if existing:
                # Update existing node
                self.g.V().has('entity_id', entity_id) \
                    .property('trust_score', trust_score) \
                    .property('trust_level', trust_level) \
                    .property('last_updated', datetime.utcnow().isoformat()) \
                    .iterate()
            else:
                # Create new node
                self.g.addV('TrustEntity') \
                    .property('entity_id', entity_id) \
                    .property('trust_score', trust_score) \
                    .property('trust_level', trust_level) \
                    .property('created_at', datetime.utcnow().isoformat()) \
                    .property('last_updated', datetime.utcnow().isoformat()) \
                    .iterate()
                    
        except Exception as e:
            logger.error(f"Error updating entity {entity_id} in graph: {e}")
            
    def update_trust_relationship(self, from_entity: str, to_entity: str, trust_value: float):
        """Update or create trust edge between entities"""
        try:
            # Ensure both entities exist
            for entity_id in [from_entity, to_entity]:
                if not self.g.V().has('entity_id', entity_id).hasNext():
                    self.g.addV('TrustEntity').property('entity_id', entity_id).iterate()
            
            # Check if edge exists
            existing_edge = self.g.V().has('entity_id', from_entity) \
                .outE('TRUSTS').where(__.inV().has('entity_id', to_entity)) \
                .toList()
                
            if existing_edge:
                # Update existing edge
                self.g.E(existing_edge[0].id) \
                    .property('trust_value', trust_value) \
                    .property('last_updated', datetime.utcnow().isoformat()) \
                    .iterate()
            else:
                # Create new edge
                self.g.V().has('entity_id', from_entity).as_('from') \
                    .V().has('entity_id', to_entity).as_('to') \
                    .addE('TRUSTS').from_('from').to('to') \
                    .property('trust_value', trust_value) \
                    .property('created_at', datetime.utcnow().isoformat()) \
                    .iterate()
                    
        except Exception as e:
            logger.error(f"Error updating trust relationship {from_entity} -> {to_entity}: {e}")
            
    def run_sync_loop(self):
        """Run continuous synchronization"""
        self.running = True
        while self.running:
            try:
                self.sync_trust_data()
                time.sleep(self.sync_interval)
            except Exception as e:
                logger.error(f"Sync loop error: {e}")
                time.sleep(60)  # Wait before retry
                
    def stop(self):
        """Stop the sync loop"""
        self.running = False

# Global sync instance
trust_sync = TrustNetworkSync()

@app.on_event("startup")
async def startup_event():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting up graph-intelligence-service...")
    
    # Start trust network sync
    trust_sync.connect()
    sync_thread = threading.Thread(target=trust_sync.run_sync_loop, daemon=True)
    sync_thread.start()
    app.state.trust_sync = trust_sync

@app.on_event("shutdown")
async def shutdown_event():
    logging.info("Shutting down graph-intelligence-service...")
    if hasattr(app.state, 'trust_sync'):
        app.state.trust_sync.stop()

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["graph-intelligence-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "graph-intelligence-service is running"}

@app.get("/api/v1/insights/{insight_type}")
def get_graph_insight(
    insight_type: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    tenant_id = str(context["tenant_id"])
    g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
    
    if insight_type == "community-detection":
        # This logic is now handled by the gRPC endpoint.
        # This REST endpoint could be deprecated or kept for administrative purposes.
        # For now, we'll return a message pointing to the new method.
        return {"message": "Community detection is now performed via the gRPC GetCommunityInsights method."}
    
    elif insight_type == "centrality":
        # This query finds the most 'central' documents by calculating their
        # in-degree (how many users have edited them).
        centrality = g.V().has('tenant_id', tenant_id).hasLabel('Document') \
                          .order().by(__.inE('EDITED').count(), decr) \
                          .limit(10).valueMap('name', 'path').toList()
        return {"insight": "centrality", "data": centrality}
        
    else:
        raise HTTPException(status_code=404, detail="Insight type not found")

# New Trust-Enhanced Endpoints
@app.get("/api/v1/insights/trust-clusters")
async def get_trust_clusters(
    context: dict = Depends(get_current_tenant_and_user),
):
    """Identify clusters of high-trust entities"""
    g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
    
    # Find clusters of entities with high mutual trust
    clusters = g.V().has('trust_score', __.gte(0.7)) \
        .group().by().by(
            __.both('TRUSTS').has('trust_score', __.gte(0.7)).values('entity_id').fold()
        ).toList()
    
    # Format results
    trust_clusters = []
    for cluster_data in clusters:
        for entity, connected in cluster_data.items():
            if len(connected) >= 2:  # At least 3 entities in cluster
                trust_clusters.append({
                    "core_entity": entity.get('entity_id', 'unknown'),
                    "cluster_members": connected,
                    "cluster_size": len(connected) + 1,
                    "avg_trust_score": entity.get('trust_score', 0)
                })
    
    return {
        "tenant_id": context["tenant_id"],
        "cluster_count": len(trust_clusters),
        "clusters": trust_clusters
    }

@app.get("/api/v1/insights/trust-paths")
async def find_trust_paths(
    from_entity: str,
    to_entity: str,
    max_depth: int = 6,
    context: dict = Depends(get_current_tenant_and_user),
):
    """Find trust paths between two entities"""
    g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
    
    # Find paths with trust scores
    paths = g.V().has('entity_id', from_entity) \
        .repeat(
            __.outE('TRUSTS').has('trust_value', __.gte(0.1)).inV()
        ).until(
            __.has('entity_id', to_entity).or_().loops().is_(max_depth)
        ).has('entity_id', to_entity) \
        .path().by('entity_id').by('trust_value') \
        .toList()
    
    # Calculate cumulative trust for each path
    trust_paths = []
    for path in paths:
        entities = [p for i, p in enumerate(path) if i % 2 == 0]
        trust_values = [p for i, p in enumerate(path) if i % 2 == 1]
        
        # Calculate path trust (product of edge trusts)
        path_trust = 1.0
        for tv in trust_values:
            path_trust *= tv
            
        trust_paths.append({
            "path": entities,
            "trust_values": trust_values,
            "path_trust": path_trust,
            "path_length": len(entities)
        })
    
    # Sort by trust score
    trust_paths.sort(key=lambda x: x['path_trust'], reverse=True)
    
    return {
        "from_entity": from_entity,
        "to_entity": to_entity,
        "paths_found": len(trust_paths),
        "trust_paths": trust_paths[:5]  # Top 5 paths
    }

@app.post("/api/v1/graph/ingest-trust-event")
async def ingest_trust_event(
    event_type: str,
    entity_id: str,
    trust_delta: float,
    metadata: Dict[str, Any] = {},
    background_tasks: BackgroundTasks = BackgroundTasks(),
):
    """Ingest a trust-affecting event into the graph"""
    
    def update_graph():
        g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
        
        # Create event node
        g.addV('TrustEvent') \
            .property('event_type', event_type) \
            .property('entity_id', entity_id) \
            .property('trust_delta', trust_delta) \
            .property('timestamp', datetime.utcnow().isoformat()) \
            .property('metadata', str(metadata)) \
            .iterate()
            
        # Update entity trust score
        current_score = g.V().has('entity_id', entity_id) \
            .values('trust_score').next()
        new_score = max(0.0, min(1.0, current_score + trust_delta))
        
        g.V().has('entity_id', entity_id) \
            .property('trust_score', new_score) \
            .iterate()
    
    background_tasks.add_task(update_graph)
    
    return {
        "status": "accepted",
        "event_type": event_type,
        "entity_id": entity_id,
        "trust_impact": trust_delta
    }
