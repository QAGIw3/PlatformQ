from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
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

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import graph_intelligence_pb2, graph_intelligence_pb2_grpc

# In a real app, this would come from config/vault
JANUSGRAPH_URL = 'ws://platformq-janusgraph:8182/gremlin'

class GraphIntelligenceServiceServicer(graph_intelligence_pb2_grpc.GraphIntelligenceServiceServicer):
    async def GetCommunityInsights(self, request, context):
        logging.info(f"gRPC: Received GetCommunityInsights request for tenant: {request.tenant_id}")
        g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
        
        # This is the same logic from the original REST endpoint.
        # A background Spark GraphComputer job would periodically calculate and assign this.
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

@app.on_event("startup")
async def startup_event():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting up graph-intelligence-service...")

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
