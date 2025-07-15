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

app = create_base_app(
    service_name="graph-intelligence-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["graph-intelligence-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "graph-intelligence-service is running"}

# In a real app, this would come from config/vault
JANUSGRAPH_URL = 'ws://platformq-janusgraph:8182/gremlin'

@app.get("/api/v1/insights/{insight_type}")
def get_graph_insight(
    insight_type: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    tenant_id = str(context["tenant_id"])
    g = Graph().traversal().withRemote(DriverRemoteConnection(JANUSGRAPH_URL, 'g'))
    
    if insight_type == "community-detection":
        # This conceptual query uses a property 'community' which a background
        # Spark GraphComputer job would periodically calculate and assign to vertices.
        communities = g.V().has('tenant_id', tenant_id) \
                           .group().by('community').by('name').toList()
        return {"insight": "community-detection", "data": communities}
    
    elif insight_type == "centrality":
        # This query finds the most 'central' documents by calculating their
        # in-degree (how many users have edited them).
        centrality = g.V().has('tenant_id', tenant_id).hasLabel('Document') \
                          .order().by(__.inE('EDITED').count(), decr) \
                          .limit(10).valueMap('name', 'path').toList()
        return {"insight": "centrality", "data": centrality}
        
    else:
        raise HTTPException(status_code=404, detail="Insight type not found")
