"""
Graph API endpoints
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel

from data_intelligence_common import StructuredLogger
from ..engines.graph import GraphManager

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()

# Graph manager instance (will be injected)
graph_manager: GraphManager = None


class CreateVertexRequest(BaseModel):
    """Request model for creating a vertex"""
    vertex_type: str
    properties: Dict[str, Any]


class CreateEdgeRequest(BaseModel):
    """Request model for creating an edge"""
    edge_type: str
    source_id: str
    target_id: str
    properties: Optional[Dict[str, Any]] = None


class GraphQueryRequest(BaseModel):
    """Request model for graph queries"""
    query: str
    bindings: Optional[Dict[str, Any]] = None


class AnalyticsRequest(BaseModel):
    """Request model for analytics"""
    algorithm: str
    graph_id: str
    params: Optional[Dict[str, Any]] = {}


class TemporalEventRequest(BaseModel):
    """Request model for temporal events"""
    event_type: str
    entity_id: str
    timestamp: datetime
    properties: Optional[Dict[str, Any]] = None


def set_graph_deps(manager: GraphManager):
    """Set graph dependencies"""
    global graph_manager
    graph_manager = manager


# Core graph operations
@router.post("/vertices", response_model=Dict[str, str])
async def create_vertex(request: CreateVertexRequest):
    """Create a new vertex"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        vertex_id = await graph_manager.create_vertex(
            request.vertex_type,
            request.properties
        )
        
        return {
            "vertex_id": vertex_id,
            "status": "created",
            "type": request.vertex_type
        }
        
    except Exception as e:
        logger.error(f"Failed to create vertex: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/edges", response_model=Dict[str, str])
async def create_edge(request: CreateEdgeRequest):
    """Create a new edge"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        edge_id = await graph_manager.create_edge(
            request.edge_type,
            request.source_id,
            request.target_id,
            request.properties
        )
        
        return {
            "edge_id": edge_id,
            "status": "created",
            "type": request.edge_type
        }
        
    except Exception as e:
        logger.error(f"Failed to create edge: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query", response_model=List[Dict[str, Any]])
async def query_graph(request: GraphQueryRequest):
    """Execute a Gremlin query"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        results = await graph_manager.query_graph(
            request.query,
            request.bindings
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vertices/{vertex_id}")
async def get_vertex(vertex_id: str):
    """Get a vertex by ID"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        result = await graph_manager.janusgraph_client.get_vertex(vertex_id)
        
        if not result:
            raise HTTPException(status_code=404, detail="Vertex not found")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get vertex: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vertices/{vertex_id}/neighbors")
async def get_neighbors(
    vertex_id: str,
    edge_label: Optional[str] = None,
    direction: str = Query("both", regex="^(in|out|both)$"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get neighbors of a vertex"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        neighbors = await graph_manager.get_neighbors(
            vertex_id,
            edge_label,
            direction,
            limit
        )
        
        return {
            "vertex_id": vertex_id,
            "direction": direction,
            "edge_label": edge_label,
            "neighbors": neighbors,
            "count": len(neighbors)
        }
        
    except Exception as e:
        logger.error(f"Failed to get neighbors: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shortest-path")
async def find_shortest_path(
    source_id: str = Query(..., description="Source vertex ID"),
    target_id: str = Query(..., description="Target vertex ID"),
    max_depth: int = Query(10, ge=1, le=20)
):
    """Find shortest path between two vertices"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        path = await graph_manager.find_shortest_path(
            source_id,
            target_id,
            max_depth
        )
        
        if not path:
            return {
                "source": source_id,
                "target": target_id,
                "path": None,
                "message": "No path found"
            }
        
        return {
            "source": source_id,
            "target": target_id,
            "path": path,
            "length": len(path) - 1
        }
        
    except Exception as e:
        logger.error(f"Failed to find shortest path: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Analytics operations
@router.post("/analytics", response_model=Dict[str, Any])
async def run_analytics(request: AnalyticsRequest):
    """Run graph analytics algorithm"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        result = await graph_manager.run_analytics(
            request.algorithm,
            request.graph_id,
            request.params
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Analytics failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analytics/algorithms")
async def list_algorithms():
    """List available analytics algorithms"""
    return {
        "algorithms": [
            {
                "name": "pagerank",
                "description": "Compute PageRank scores",
                "params": {
                    "resetProbability": "float (0-1)",
                    "maxIter": "int",
                    "tol": "float"
                }
            },
            {
                "name": "community_detection",
                "description": "Detect communities in the graph",
                "params": {
                    "algorithm": "string (louvain, label_propagation)",
                    "resolution": "float"
                }
            },
            {
                "name": "centrality",
                "description": "Compute centrality measures",
                "params": {
                    "measure": "string (betweenness, closeness, degree)"
                }
            },
            {
                "name": "clustering",
                "description": "Compute clustering coefficients",
                "params": {}
            },
            {
                "name": "shortest_paths",
                "description": "Compute shortest paths",
                "params": {
                    "source": "string (vertex ID)",
                    "targets": "list of vertex IDs (optional)"
                }
            },
            {
                "name": "influence_propagation",
                "description": "Simulate influence propagation",
                "params": {
                    "seeds": "list of vertex IDs",
                    "probability": "float (0-1)",
                    "iterations": "int"
                }
            }
        ]
    }


# Temporal operations
@router.post("/temporal/events", response_model=Dict[str, str])
async def create_temporal_event(request: TemporalEventRequest):
    """Create a temporal event"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        event_id = await graph_manager.create_temporal_event(
            request.event_type,
            request.entity_id,
            request.timestamp,
            request.properties
        )
        
        return {
            "event_id": event_id,
            "status": "created",
            "entity_id": request.entity_id
        }
        
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create temporal event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/temporal/events/{entity_id}")
async def query_temporal_events(
    entity_id: str,
    start_time: datetime = Query(..., description="Start time"),
    end_time: datetime = Query(..., description="End time")
):
    """Query temporal events for an entity"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        events = await graph_manager.query_temporal_range(
            entity_id,
            start_time,
            end_time
        )
        
        return {
            "entity_id": entity_id,
            "time_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "events": events,
            "count": len(events)
        }
        
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to query temporal events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/temporal/patterns/{entity_id}")
async def detect_patterns(
    entity_id: str,
    pattern_type: str = Query("periodic", regex="^(periodic|trend|anomaly)$")
):
    """Detect temporal patterns"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        patterns = await graph_manager.detect_temporal_patterns(
            entity_id,
            pattern_type
        )
        
        return {
            "entity_id": entity_id,
            "pattern_type": pattern_type,
            "patterns": patterns
        }
        
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to detect patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Trust operations
@router.get("/trust/score")
async def calculate_trust(
    source_id: str = Query(..., description="Source entity ID"),
    target_id: str = Query(..., description="Target entity ID")
):
    """Calculate trust score between entities"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        score = await graph_manager.calculate_trust_score(source_id, target_id)
        
        return {
            "source": source_id,
            "target": target_id,
            "trust_score": score
        }
        
    except Exception as e:
        logger.error(f"Failed to calculate trust: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trust/path")
async def get_trust_path(
    source_id: str = Query(..., description="Source entity ID"),
    target_id: str = Query(..., description="Target entity ID"),
    min_trust: float = Query(0.5, ge=0, le=1)
):
    """Get trust path between entities"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        path = await graph_manager.get_trust_path(source_id, target_id, min_trust)
        
        if not path:
            return {
                "source": source_id,
                "target": target_id,
                "path": None,
                "message": "No trust path found"
            }
        
        return {
            "source": source_id,
            "target": target_id,
            "path": path,
            "min_trust": min_trust
        }
        
    except Exception as e:
        logger.error(f"Failed to get trust path: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/trust/propagate")
async def propagate_trust(
    entity_id: str = Query(..., description="Starting entity ID"),
    initial_trust: float = Query(1.0, ge=0, le=1),
    decay_factor: float = Query(0.8, ge=0, le=1),
    max_hops: int = Query(3, ge=1, le=5)
):
    """Propagate trust from an entity"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        trust_scores = await graph_manager.propagate_trust(
            entity_id,
            initial_trust,
            decay_factor,
            max_hops
        )
        
        return {
            "source": entity_id,
            "initial_trust": initial_trust,
            "decay_factor": decay_factor,
            "max_hops": max_hops,
            "trust_scores": trust_scores
        }
        
    except Exception as e:
        logger.error(f"Failed to propagate trust: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Lineage operations
@router.get("/lineage/{entity_id}")
async def get_lineage(
    entity_id: str,
    direction: str = Query("both", regex="^(upstream|downstream|both)$"),
    max_depth: int = Query(5, ge=1, le=10)
):
    """Get data lineage for an entity"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        lineage = await graph_manager.get_lineage(entity_id, direction, max_depth)
        
        return lineage
        
    except Exception as e:
        logger.error(f"Failed to get lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/{entity_id}/impact")
async def analyze_impact(entity_id: str):
    """Analyze impact of changes to an entity"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        impact = await graph_manager.get_impact_analysis(entity_id)
        
        return impact
        
    except Exception as e:
        logger.error(f"Failed to analyze impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lineage/{entity_id}/validate")
async def validate_lineage(entity_id: str):
    """Validate lineage consistency"""
    try:
        if not graph_manager:
            raise HTTPException(status_code=503, detail="Graph manager not available")
        
        validation = await graph_manager.validate_lineage(entity_id)
        
        return validation
        
    except Exception as e:
        logger.error(f"Failed to validate lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check
@router.get("/health")
async def health_check():
    """Check graph service health"""
    try:
        if not graph_manager:
            return {
                "status": "unhealthy",
                "message": "Graph manager not available"
            }
        
        health = await graph_manager.health_check()
        
        return {
            "status": "healthy" if health["healthy"] else "unhealthy",
            "details": health
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        } 