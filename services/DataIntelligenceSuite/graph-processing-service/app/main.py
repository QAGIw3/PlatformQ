"""
Graph Processing Service

Unified service for graph analytics, trust scoring, community detection,
and real-time graph updates using JanusGraph and GraphX.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from app.core.config import settings
from app.core.janusgraph_client import JanusGraphClient
from app.core.graphx_manager import GraphXManager
from app.core.trust_engine import TrustEngine
from app.core.community_detector import CommunityDetector
from app.api import graph, analytics, trust, health, metrics
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
janusgraph: Optional[JanusGraphClient] = None
graphx_manager: Optional[GraphXManager] = None
trust_engine: Optional[TrustEngine] = None
community_detector: Optional[CommunityDetector] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global janusgraph, graphx_manager, trust_engine, community_detector
    
    # Startup
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize components
    janusgraph = JanusGraphClient(settings)
    await janusgraph.connect()
    
    graphx_manager = GraphXManager(settings)
    await graphx_manager.initialize()
    
    trust_engine = TrustEngine(settings, janusgraph)
    await trust_engine.initialize()
    
    community_detector = CommunityDetector(settings, graphx_manager)
    await community_detector.initialize()
    
    # Register with service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import register_service
        await register_service(settings)
    
    logger.info("Graph Processing Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Graph Processing Service")
    
    # Stop components
    await janusgraph.disconnect()
    await graphx_manager.cleanup()
    
    # Deregister from service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import deregister_service
        await deregister_service(settings)
    
    logger.info("Graph Processing Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    description="Unified graph processing service for analytics, trust scoring, and community detection",
    version=settings.service_version,
    lifespan=lifespan
)

# Add middleware
app.middleware("http")(error_handler_middleware)
app.middleware("http")(logging_middleware)

# Include routers
app.include_router(graph.router, prefix="/api/v1/graph", tags=["graph"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(trust.router, prefix="/api/v1/trust", tags=["trust"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/v1/info")
async def service_info():
    """Get service information"""
    return {
        "service": {
            "name": settings.service_name,
            "version": settings.service_version,
            "environment": settings.environment
        },
        "capabilities": {
            "janusgraph": True,
            "graphx": True,
            "trust_scoring": True,
            "community_detection": True,
            "real_time_updates": True
        },
        "analytics_types": [
            "pagerank",
            "community_detection",
            "shortest_path",
            "centrality",
            "trust_propagation"
        ],
        "graph_operations": {
            "crud": ["create_vertex", "create_edge", "update", "delete"],
            "traversal": ["bfs", "dfs", "pattern_matching"],
            "analytics": ["batch", "incremental", "streaming"]
        }
    }


class VertexCreateRequest(BaseModel):
    """Vertex creation request"""
    label: str
    properties: Dict[str, Any]
    

class EdgeCreateRequest(BaseModel):
    """Edge creation request"""
    label: str
    from_vertex_id: str
    to_vertex_id: str
    properties: Optional[Dict[str, Any]] = {}
    

@app.post("/api/v1/vertices")
async def create_vertex(request: VertexCreateRequest):
    """Create a new vertex"""
    try:
        vertex_id = await janusgraph.create_vertex(
            label=request.label,
            properties=request.properties
        )
        
        # Update trust score if applicable
        if request.label in ["user", "entity", "asset"]:
            await trust_engine.calculate_initial_trust(vertex_id)
        
        return {
            "vertex_id": vertex_id,
            "status": "created",
            "message": f"Vertex created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create vertex: {e}")
        raise HTTPException(500, f"Failed to create vertex: {str(e)}")


@app.post("/api/v1/edges")
async def create_edge(request: EdgeCreateRequest):
    """Create a new edge"""
    try:
        edge_id = await janusgraph.create_edge(
            label=request.label,
            from_vertex_id=request.from_vertex_id,
            to_vertex_id=request.to_vertex_id,
            properties=request.properties
        )
        
        # Recalculate trust scores for connected vertices
        await trust_engine.update_trust_scores([request.from_vertex_id, request.to_vertex_id])
        
        return {
            "edge_id": edge_id,
            "status": "created",
            "message": f"Edge created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create edge: {e}")
        raise HTTPException(500, f"Failed to create edge: {str(e)}")


class GraphAnalyticsRequest(BaseModel):
    """Graph analytics request"""
    algorithm: str  # pagerank, community, centrality
    parameters: Optional[Dict[str, Any]] = {}
    scope: Optional[str] = "full"  # full, subgraph, incremental
    

@app.post("/api/v1/analytics/run")
async def run_analytics(request: GraphAnalyticsRequest, background_tasks: BackgroundTasks):
    """Run graph analytics algorithm"""
    try:
        # Submit to GraphX
        job_id = await graphx_manager.submit_job(
            algorithm=request.algorithm,
            parameters=request.parameters,
            scope=request.scope
        )
        
        # Run in background
        background_tasks.add_task(graphx_manager.execute_job, job_id)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": f"Analytics job {request.algorithm} submitted"
        }
        
    except Exception as e:
        logger.error(f"Failed to run analytics: {e}")
        raise HTTPException(500, f"Failed to run analytics: {str(e)}")


@app.get("/api/v1/trust/{vertex_id}")
async def get_trust_score(vertex_id: str):
    """Get trust score for a vertex"""
    try:
        score = await trust_engine.get_trust_score(vertex_id)
        if score is None:
            raise HTTPException(404, f"Vertex {vertex_id} not found")
        
        breakdown = await trust_engine.get_trust_breakdown(vertex_id)
        
        return {
            "vertex_id": vertex_id,
            "trust_score": score,
            "breakdown": breakdown,
            "last_updated": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to get trust score: {e}")
        raise HTTPException(500, f"Failed to get trust score: {str(e)}")


@app.post("/api/v1/communities/detect")
async def detect_communities(algorithm: str = "louvain"):
    """Detect communities in the graph"""
    try:
        communities = await community_detector.detect(algorithm)
        
        return {
            "algorithm": algorithm,
            "num_communities": len(communities),
            "communities": communities,
            "modularity": await community_detector.calculate_modularity(communities)
        }
    except Exception as e:
        logger.error(f"Failed to detect communities: {e}")
        raise HTTPException(500, f"Failed to detect communities: {str(e)}")


class GraphQueryRequest(BaseModel):
    """Graph query request"""
    query: str  # Gremlin query
    bindings: Optional[Dict[str, Any]] = {}
    

@app.post("/api/v1/query")
async def execute_query(request: GraphQueryRequest):
    """Execute a Gremlin query"""
    try:
        results = await janusgraph.execute_query(
            query=request.query,
            bindings=request.bindings
        )
        
        return {
            "query": request.query,
            "results": results,
            "count": len(results)
        }
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise HTTPException(500, f"Failed to execute query: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    ) 