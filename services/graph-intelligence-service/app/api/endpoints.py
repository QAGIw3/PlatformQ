from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from cassandra.cluster import Session
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

from .deps import get_current_tenant_and_user
from ..spark_integration import SparkGraphAnalytics, GraphXEnhancedQueries
from ..services.trust_score import trust_score_service

router = APIRouter()

# Initialize Spark integration
spark_analytics = SparkGraphAnalytics()


class PageRankRequest(BaseModel):
    """Request model for PageRank analysis"""
    vertex_label: Optional[str] = Field(None, description="Filter vertices by label")
    edge_label: Optional[str] = Field(None, description="Filter edges by label")
    tolerance: float = Field(0.001, description="Convergence tolerance")
    max_iterations: int = Field(20, description="Maximum iterations")
    top_k: int = Field(100, description="Number of top results to return")


class CommunityDetectionRequest(BaseModel):
    """Request model for community detection"""
    algorithm: str = Field("label_propagation", description="Algorithm to use")
    max_iterations: int = Field(10, description="Maximum iterations")


class ShortestPathRequest(BaseModel):
    """Request model for shortest path analysis"""
    source_vertices: List[str] = Field(..., description="Source vertex IDs")
    target_vertices: Optional[List[str]] = Field(None, description="Target vertex IDs")


class GraphAnalysisResponse(BaseModel):
    """Response model for graph analysis jobs"""
    job_id: str
    status: str
    algorithm: str
    parameters: Dict[str, Any]


@router.get("/example")
def example_endpoint(context: dict = Depends(get_current_tenant_and_user)):
    """
    An example protected endpoint.
    """
    tenant_id = context["tenant_id"]
    user = context["user"]
    
    return {
        "message": f"Hello {user.full_name} from tenant {tenant_id}!",
        "service": "graph-intelligence-service"
    }


@router.post("/analyze/pagerank", response_model=GraphAnalysisResponse)
async def run_pagerank_analysis(
    request: PageRankRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Run PageRank analysis on the graph to find influential nodes
    """
    tenant_id = context["tenant_id"]
    
    try:
        result = await spark_analytics.run_pagerank(
            tenant_id=tenant_id,
            vertex_label=request.vertex_label,
            edge_label=request.edge_label,
            tolerance=request.tolerance,
            max_iterations=request.max_iterations,
            top_k=request.top_k
        )
        return GraphAnalysisResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/communities", response_model=GraphAnalysisResponse)
async def detect_communities(
    request: CommunityDetectionRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Detect communities in the graph using various algorithms
    """
    tenant_id = context["tenant_id"]
    
    try:
        result = await spark_analytics.detect_communities(
            tenant_id=tenant_id,
            algorithm=request.algorithm,
            max_iterations=request.max_iterations
        )
        return GraphAnalysisResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/shortest-paths", response_model=GraphAnalysisResponse)
async def find_shortest_paths(
    request: ShortestPathRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Find shortest paths between vertices in the graph
    """
    tenant_id = context["tenant_id"]
    
    try:
        result = await spark_analytics.find_shortest_paths(
            tenant_id=tenant_id,
            source_vertices=request.source_vertices,
            target_vertices=request.target_vertices
        )
        return GraphAnalysisResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/structure")
async def analyze_graph_structure(
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Run comprehensive structural analysis on the graph
    """
    tenant_id = context["tenant_id"]
    
    try:
        result = await spark_analytics.analyze_graph_structure(tenant_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/status")
async def get_job_status(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get the status of a Spark graph analysis job
    """
    try:
        status = await spark_analytics.get_job_status(job_id)
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/result")
async def get_job_result(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get the result of a completed Spark graph analysis job
    """
    try:
        result = await spark_analytics.get_job_result(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/influencers")
async def get_top_influencers(
    limit: int = 10,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get top influencers based on PageRank scores
    """
    tenant_id = context["tenant_id"]
    
    # This assumes PageRank has already been computed
    # In production, check if analysis is up-to-date
    try:
        # TODO: Initialize janusgraph client properly
        enhanced_queries = GraphXEnhancedQueries(None, spark_analytics)
        influencers = await enhanced_queries.get_top_influencers(tenant_id, limit)
        return {"influencers": influencers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/communities/{community_id}/members")
async def get_community_members(
    community_id: int,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get all members of a specific community
    """
    tenant_id = context["tenant_id"]
    
    try:
        # TODO: Initialize janusgraph client properly
        enhanced_queries = GraphXEnhancedQueries(None, spark_analytics)
        members = await enhanced_queries.get_community_members(tenant_id, community_id)
        return {"community_id": community_id, "members": members}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/influence-path")
async def get_influence_path(
    source_id: str,
    target_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get the influence path between two nodes
    """
    tenant_id = context["tenant_id"]
    
    try:
        # TODO: Initialize janusgraph client properly
        enhanced_queries = GraphXEnhancedQueries(None, spark_analytics)
        path = await enhanced_queries.get_influence_path(tenant_id, source_id, target_id)
        return {"source": source_id, "target": target_id, "path": path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch/analyze-new-connections", status_code=202)
async def analyze_new_connections(
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Trigger batch analysis when new connections are added to the graph
    """
    tenant_id = context["tenant_id"]
    
    async def run_batch_analysis():
        # Run PageRank to update influence scores
        await spark_analytics.run_pagerank(tenant_id, top_k=1000)
        
        # Update community detection
        await spark_analytics.detect_communities(tenant_id)
    
    background_tasks.add_task(run_batch_analysis)
    
    return {"message": "Batch analysis triggered", "status": "accepted"} 


@router.get("/trust-score/{user_id}")
def get_trust_score(user_id: str, context: dict = Depends(get_current_tenant_and_user)):
    """
    Get the trust score for a user.
    """
    # In a real implementation, you might want to pre-populate the graph
    # with historical data. For now, we'll add some dummy data.
    trust_score_service.add_user_activity(user_id, "project_milestone_completed", "milestone1")
    trust_score_service.add_user_activity(user_id, "asset_peer_reviewed", "review1")
    
    score = trust_score_service.calculate_trust_score(user_id)
    return {"user_id": user_id, "trust_score": score} 