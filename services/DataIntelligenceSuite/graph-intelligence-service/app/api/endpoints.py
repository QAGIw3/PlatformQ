from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Request
from cassandra.cluster import Session
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import logging
from datetime import datetime, timedelta

from .deps import get_current_tenant_and_user
from ..spark_integration import SparkGraphAnalytics, GraphXEnhancedQueries
from ..services.trust_score import trust_score_service

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize Spark integration
spark_analytics = SparkGraphAnalytics()


def get_graph_processor(request: Request):
    """Get graph processor from app state"""
    return request.app.state.graph_processor


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


class FraudCheckRequest(BaseModel):
    """Request model for fraud detection check"""
    entity_ids: List[str] = Field(..., description="List of entity IDs to check")
    check_depth: int = Field(2, description="Graph traversal depth for analysis")
    include_network_analysis: bool = Field(True, description="Include network-based fraud indicators")


class RecommendationRequest(BaseModel):
    """Request model for recommendations"""
    user_id: str = Field(..., description="User ID for recommendations")
    recommendation_type: str = Field("general", description="Type of recommendations")
    limit: int = Field(10, description="Number of recommendations")
    context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class TrustScoreRequest(BaseModel):
    """Request model for trust score calculation"""
    source_id: str = Field(..., description="Source entity ID")
    target_id: str = Field(..., description="Target entity ID")
    path_limit: int = Field(3, description="Maximum path length for trust propagation")


class GraphAnalyticsRequest(BaseModel):
    """Generic request for graph analytics"""
    algorithm: str = Field(..., description="Algorithm to run")
    parameters: Dict[str, Any] = Field({}, description="Algorithm parameters")
    entity_filter: Optional[Dict[str, Any]] = Field(None, description="Filter for entities")
    save_results: bool = Field(True, description="Save results to graph")


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


@router.post("/recommendations")
async def get_recommendations(
    request: RecommendationRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Get personalized recommendations based on graph analysis
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Try to get cached recommendations first
        from pyignite import Client
        ignite_client = Client()
        ignite_client.connect([('ignite-0.ignite', 10800)])
        
        cache_key = f"recommendations:{request.user_id}"
        cached = ignite_client.get(cache_key)
        
        if cached and (datetime.utcnow() - datetime.fromisoformat(cached['generated_at'])).seconds < 300:
            ignite_client.close()
            return {
                "recommendations": cached['recommendations'],
                "cached": True,
                "generated_at": cached['generated_at']
            }
        
        ignite_client.close()
        
        # Generate new recommendations using Spark job
        spark_params = {
            "user_id": request.user_id,
            "recommendation_type": request.recommendation_type,
            "limit": request.limit,
            "context": request.context or {}
        }
        
        # Submit Spark job asynchronously
        background_tasks.add_task(
            spark_analytics.submit_job,
            "recommendation_engine",
            tenant_id,
            spark_params
        )
        
        # Return immediate response
        return {
            "status": "generating",
            "message": "Recommendations are being generated",
            "check_after_seconds": 5
        }
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recommendations/{user_id}")
async def get_recommendation_results(
    user_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get previously generated recommendations
    """
    try:
        from pyignite import Client
        ignite_client = Client()
        ignite_client.connect([('ignite-0.ignite', 10800)])
        
        cache_key = f"recommendations:{user_id}"
        result = ignite_client.get(cache_key)
        
        ignite_client.close()
        
        if result:
            return {
                "recommendations": result['recommendations'],
                "generated_at": result['generated_at']
            }
        else:
            return {"message": "No recommendations found"}
            
    except Exception as e:
        logger.error(f"Error retrieving recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trust/calculate")
async def calculate_trust_score(
    request: TrustScoreRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Calculate trust score between two entities using graph paths
    """
    tenant_id = context["tenant_id"]
    
    try:
        trust_score = await trust_score_service.calculate_trust_score(
            tenant_id=tenant_id,
            source_id=request.source_id,
            target_id=request.target_id,
            max_path_length=request.path_limit
        )
        
        return {
            "source_id": request.source_id,
            "target_id": request.target_id,
            "trust_score": trust_score['score'],
            "confidence": trust_score['confidence'],
            "paths_analyzed": trust_score['paths_analyzed'],
            "calculation_method": trust_score['method']
        }
        
    except Exception as e:
        logger.error(f"Error calculating trust score: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trust/network/{entity_id}")
async def get_trust_network(
    entity_id: str,
    depth: int = 2,
    min_trust_score: float = 0.5,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get trust network for an entity
    """
    tenant_id = context["tenant_id"]
    
    try:
        network = await trust_score_service.get_trust_network(
            tenant_id=tenant_id,
            entity_id=entity_id,
            depth=depth,
            min_trust_score=min_trust_score
        )
        
        return {
            "entity_id": entity_id,
            "network_size": len(network['nodes']),
            "average_trust": network['average_trust'],
            "nodes": network['nodes'],
            "edges": network['edges']
        }
        
    except Exception as e:
        logger.error(f"Error getting trust network: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fraud/check")
async def check_fraud(
    request: FraudCheckRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Check entities for fraud indicators using graph analysis
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Submit fraud detection Spark job
        spark_params = {
            "entity_ids": request.entity_ids,
            "check_depth": request.check_depth,
            "include_network_analysis": request.include_network_analysis,
            "fraud_threshold": 0.7
        }
        
        # Run fraud detection
        job_id = await spark_analytics.submit_job(
            "fraud_detection",
            tenant_id,
            spark_params
        )
        
        # For immediate checks, also query current graph state
        immediate_results = []
        
        from gremlin_python.driver import client, serializer
        gremlin_client = client.Client(
            'ws://janusgraph:8182/gremlin',
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        for entity_id in request.entity_ids[:5]:  # Quick check for first 5
            query = f"""
                g.V().has('entity_id', '{entity_id}')
                .project('id', 'fraud_score', 'is_suspicious', 'trust_score')
                .by('entity_id')
                .by(coalesce(values('fraud_score'), constant(0.0)))
                .by(coalesce(values('is_suspicious'), constant(false)))
                .by(coalesce(values('trust_score'), constant(0.5)))
            """
            
            result = gremlin_client.submit(query).all().result()
            if result:
                immediate_results.append(result[0])
        
        gremlin_client.close()
        
        return {
            "job_id": job_id,
            "status": "processing",
            "immediate_results": immediate_results,
            "total_entities": len(request.entity_ids),
            "message": "Full fraud analysis in progress"
        }
        
    except Exception as e:
        logger.error(f"Error checking fraud: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fraud/results/{job_id}")
async def get_fraud_results(
    job_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get results of fraud detection job
    """
    try:
        results = await spark_analytics.get_job_results(job_id)
        
        if results:
            return {
                "job_id": job_id,
                "status": results['status'],
                "fraud_analysis": results.get('pattern_analysis', {}),
                "suspicious_entities": results.get('suspicious_entities', []),
                "execution_time": results.get('execution_time')
            }
        else:
            return {"message": "Job still processing or not found"}
            
    except Exception as e:
        logger.error(f"Error getting fraud results: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/community/detect")
async def detect_communities(
    request: GraphAnalyticsRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Detect communities in the graph using various algorithms
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Validate algorithm
        valid_algorithms = ['label_propagation', 'louvain', 'connected_components']
        if request.algorithm not in valid_algorithms:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid algorithm. Must be one of: {valid_algorithms}"
            )
        
        # Submit community detection job
        spark_params = {
            "algorithm": request.algorithm,
            **request.parameters,
            "save_to_graph": request.save_results
        }
        
        job_id = await spark_analytics.submit_job(
            "community_detection",
            tenant_id,
            spark_params
        )
        
        return {
            "job_id": job_id,
            "status": "processing",
            "algorithm": request.algorithm,
            "message": "Community detection in progress"
        }
        
    except Exception as e:
        logger.error(f"Error detecting communities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/influence/{entity_id}")
async def get_influence_metrics(
    entity_id: str,
    metric_type: str = "pagerank",
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get influence metrics for an entity
    """
    tenant_id = context["tenant_id"]
    
    try:
        from gremlin_python.driver import client, serializer
        gremlin_client = client.Client(
            'ws://janusgraph:8182/gremlin',
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        # Get stored influence metrics
        query = f"""
            g.V().has('entity_id', '{entity_id}').has('tenant_id', '{tenant_id}')
            .project('entity_id', 'pagerank', 'betweenness', 'closeness', 
                     'in_degree', 'out_degree', 'clustering_coefficient')
            .by('entity_id')
            .by(coalesce(values('pagerank_score'), constant(0.0)))
            .by(coalesce(values('betweenness_centrality'), constant(0.0)))
            .by(coalesce(values('closeness_centrality'), constant(0.0)))
            .by(inE().count())
            .by(outE().count())
            .by(coalesce(values('clustering_coefficient'), constant(0.0)))
        """
        
        result = gremlin_client.submit(query).all().result()
        gremlin_client.close()
        
        if result:
            metrics = result[0]
            
            # Calculate composite influence score
            influence_score = (
                metrics['pagerank'] * 0.3 +
                metrics['betweenness'] * 0.2 +
                metrics['closeness'] * 0.2 +
                (metrics['in_degree'] / max(metrics['in_degree'] + metrics['out_degree'], 1)) * 0.3
            )
            
            return {
                "entity_id": entity_id,
                "influence_score": influence_score,
                "metrics": metrics,
                "percentile": await _calculate_influence_percentile(tenant_id, influence_score)
            }
        else:
            raise HTTPException(status_code=404, detail="Entity not found")
            
    except Exception as e:
        logger.error(f"Error getting influence metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/trends")
async def get_graph_trends(
    time_range: str = "7d",
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Get graph growth and evolution trends
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Parse time range
        time_map = {
            "1d": timedelta(days=1),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30),
            "90d": timedelta(days=90)
        }
        
        if time_range not in time_map:
            raise HTTPException(status_code=400, detail="Invalid time range")
        
        start_time = datetime.utcnow() - time_map[time_range]
        
        # Query graph evolution metrics
        from pyignite import Client
        ignite_client = Client()
        ignite_client.connect([('ignite-0.ignite', 10800)])
        
        # Get cached evolution metrics
        cache_key = f"graph_evolution:{tenant_id}:{time_range}"
        evolution_data = ignite_client.get(cache_key)
        
        ignite_client.close()
        
        if evolution_data:
            return evolution_data
        
        # If not cached, trigger computation
        return {
            "message": "Trend analysis initiated",
            "time_range": time_range,
            "check_after_seconds": 10
        }
        
    except Exception as e:
        logger.error(f"Error getting graph trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def _calculate_influence_percentile(tenant_id: str, influence_score: float) -> float:
    """Calculate percentile rank of influence score"""
    try:
        # This would query a pre-computed distribution
        # For now, return a mock calculation
        return min(99.0, influence_score * 100)
    except:
        return 50.0


@router.post("/graph/algorithm/run")
async def run_custom_algorithm(
    request: GraphAnalyticsRequest,
    context: dict = Depends(get_current_tenant_and_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """
    Run a custom graph algorithm
    """
    tenant_id = context["tenant_id"]
    
    try:
        # Map algorithm names to Spark jobs
        algorithm_map = {
            "pagerank": "pagerank",
            "community_detection": "community_detection",
            "fraud_detection": "fraud_detection",
            "shortest_path": "shortest_path",
            "centrality": "centrality_measures",
            "similarity": "node_similarity"
        }
        
        if request.algorithm not in algorithm_map:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown algorithm: {request.algorithm}"
            )
        
        # Submit job to Spark
        job_id = await spark_analytics.submit_job(
            algorithm_map[request.algorithm],
            tenant_id,
            request.parameters
        )
        
        return {
            "job_id": job_id,
            "algorithm": request.algorithm,
            "status": "submitted",
            "parameters": request.parameters
        }
        
    except Exception as e:
        logger.error(f"Error running algorithm: {e}")
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
    context: dict = Depends(get_current_tenant_and_user),
    graph_processor = Depends(get_graph_processor)
):
    """
    Get top influencers based on PageRank scores
    """
    tenant_id = context["tenant_id"]
    
    # This assumes PageRank has already been computed
    # In production, check if analysis is up-to-date
    try:
        enhanced_queries = GraphXEnhancedQueries(graph_processor, spark_analytics)
        influencers = await enhanced_queries.get_top_influencers(tenant_id, limit)
        return {"influencers": influencers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/communities/{community_id}/members")
async def get_community_members(
    community_id: int,
    context: dict = Depends(get_current_tenant_and_user),
    graph_processor = Depends(get_graph_processor)
):
    """
    Get all members of a specific community
    """
    tenant_id = context["tenant_id"]
    
    try:
        enhanced_queries = GraphXEnhancedQueries(graph_processor, spark_analytics)
        members = await enhanced_queries.get_community_members(tenant_id, community_id)
        return {"community_id": community_id, "members": members}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/insights/influence-path")
async def get_influence_path(
    source_id: str,
    target_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    graph_processor = Depends(get_graph_processor)
):
    """
    Get the influence path between two nodes
    """
    tenant_id = context["tenant_id"]
    
    try:
        enhanced_queries = GraphXEnhancedQueries(graph_processor, spark_analytics)
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


@router.post("/trust-score/{user_id}/calculate-verifiable")
async def calculate_verifiable_trust_score(
    user_id: str,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    blockchain_address: Optional[str] = None
):
    """
    Calculate verifiable trust score based on user's VCs and request issuance of TrustScoreCredential
    """
    # Generate user DID
    tenant_id = str(context["tenant_id"])
    user_did = f"did:platformq:{tenant_id}:user:{user_id}"
    
    # Calculate trust score
    trust_result = await trust_score_service.calculate_verifiable_trust_score(
        user_did=user_did,
        blockchain_address=blockchain_address
    )
    
    # Request TrustScoreCredential issuance
    background_tasks.add_task(
        request_trust_score_credential,
        tenant_id=tenant_id,
        user_did=user_did,
        trust_result=trust_result,
        blockchain_address=blockchain_address
    )
    
    return {
        "user_id": user_id,
        "user_did": user_did,
        "trust_score": trust_result.score,
        "evidence_count": len(trust_result.evidence),
        "calculated_at": trust_result.calculated_at.isoformat(),
        "valid_until": trust_result.valid_until.isoformat(),
        "credential_requested": True
    }


async def request_trust_score_credential(
    tenant_id: str,
    user_did: str,
    trust_result,
    blockchain_address: Optional[str]
):
    """Background task to request TrustScoreCredential issuance"""
    try:
        # Import event classes
        from pulsar.schema import Record, String, Double, Long, Array
        
        class TrustScoreCredentialRequested(Record):
            tenant_id = String()
            user_did = String()
            trust_score = Double()
            evidence_vc_ids = Array(String())
            calculation_timestamp = Long()
            valid_until = Long()
            blockchain_address = String(required=False)
        
        # Extract VC IDs from evidence
        evidence_vc_ids = [e["vc_id"] for e in trust_result.evidence if e["vc_id"] != "unknown"]
        
        # Create event
        event = TrustScoreCredentialRequested(
            tenant_id=tenant_id,
            user_did=user_did,
            trust_score=trust_result.score,
            evidence_vc_ids=evidence_vc_ids,
            calculation_timestamp=int(trust_result.calculated_at.timestamp() * 1000),
            valid_until=int(trust_result.valid_until.timestamp() * 1000),
            blockchain_address=blockchain_address
        )
        
        # Publish event (would need event publisher)
        # For now, just log
        logger.info(f"Trust score credential requested for {user_did}: score={trust_result.score}")
        
    except Exception as e:
        logger.error(f"Failed to request trust score credential: {e}") 