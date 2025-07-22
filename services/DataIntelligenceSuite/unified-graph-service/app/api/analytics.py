"""Analytics API endpoints"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.analytics.graphx_engine import GraphXEngine, AnalyticsJobStatus
from app.core.cache_manager import CacheManager


router = APIRouter(prefix="/api/v1/analytics", tags=["analytics"])

# Global instances (will be injected)
graphx_engine: Optional[GraphXEngine] = None
cache_manager: Optional[CacheManager] = None


class PageRankRequest(BaseModel):
    """PageRank request"""
    max_iterations: Optional[int] = Field(None, ge=1, le=100)
    damping_factor: Optional[float] = Field(None, ge=0.0, le=1.0)


class CommunityDetectionRequest(BaseModel):
    """Community detection request"""
    algorithm: str = Field("louvain", regex="^(louvain|label_propagation)$")
    resolution: Optional[float] = Field(None, ge=0.1, le=10.0)


class CentralityRequest(BaseModel):
    """Centrality calculation request"""
    centrality_type: str = Field("betweenness", regex="^(betweenness|closeness|degree)$")


class ShortestPathsRequest(BaseModel):
    """Shortest paths request"""
    source_id: str = Field(..., description="Source node ID")
    target_ids: Optional[List[str]] = Field(None, description="Target node IDs")


@router.post("/pagerank")
async def run_pagerank(request: PageRankRequest,
                      settings: Settings = Depends(get_settings)):
    """Run PageRank algorithm on the graph"""
    try:
        job_id = await graphx_engine.run_pagerank(
            request.max_iterations,
            request.damping_factor
        )
        
        return {
            "job_id": job_id,
            "status": "running",
            "message": "PageRank job started successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/communities")
async def detect_communities(request: CommunityDetectionRequest,
                           settings: Settings = Depends(get_settings)):
    """Detect communities in the graph"""
    try:
        job_id = await graphx_engine.detect_communities(
            request.algorithm,
            request.resolution
        )
        
        return {
            "job_id": job_id,
            "status": "running",
            "algorithm": request.algorithm,
            "message": "Community detection job started successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/centrality")
async def calculate_centrality(request: CentralityRequest,
                             settings: Settings = Depends(get_settings)):
    """Calculate node centrality"""
    try:
        job_id = await graphx_engine.calculate_centrality(
            request.centrality_type
        )
        
        return {
            "job_id": job_id,
            "status": "running",
            "centrality_type": request.centrality_type,
            "message": "Centrality calculation job started successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/shortest-path")
async def find_shortest_paths(request: ShortestPathsRequest,
                            settings: Settings = Depends(get_settings)):
    """Find shortest paths from source to targets"""
    try:
        job_id = await graphx_engine.find_shortest_paths(
            request.source_id,
            request.target_ids
        )
        
        return {
            "job_id": job_id,
            "status": "running",
            "source": request.source_id,
            "message": "Shortest paths job started successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str,
                        settings: Settings = Depends(get_settings)):
    """Get analytics job status"""
    try:
        # Check cache first
        cached = await cache_manager.get_cached_analytics(job_id)
        if cached and cached.get('status') == AnalyticsJobStatus.COMPLETED.value:
            return cached
            
        # Get current status
        status = await graphx_engine.get_job_status(job_id)
        
        # Cache if completed
        if status['status'] == AnalyticsJobStatus.COMPLETED.value:
            await cache_manager.cache_analytics_result(job_id, status)
            
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/results")
async def get_job_results(job_id: str,
                         limit: int = Query(100, ge=1, le=1000),
                         offset: int = Query(0, ge=0),
                         settings: Settings = Depends(get_settings)):
    """Get analytics job results"""
    try:
        # Get job status first
        status = await graphx_engine.get_job_status(job_id)
        
        if status['status'] != AnalyticsJobStatus.COMPLETED.value:
            raise HTTPException(
                status_code=400,
                detail=f"Job is not completed. Current status: {status['status']}"
            )
            
        # Get results
        results = await graphx_engine.get_job_results(job_id, limit)
        
        # Apply offset
        if offset > 0:
            results = results[offset:]
            
        return {
            "job_id": job_id,
            "results": results,
            "count": len(results),
            "limit": limit,
            "offset": offset
        }
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_jobs(status: Optional[str] = Query(None, regex="^(pending|running|completed|failed|cancelled)$"),
                   limit: int = Query(50, ge=1, le=200),
                   settings: Settings = Depends(get_settings)):
    """List analytics jobs"""
    try:
        # This would normally query a job tracking system
        # For now, return jobs from the engine's memory
        all_jobs = []
        
        for job_id, job_info in graphx_engine.running_jobs.items():
            if not status or job_info['status'].value == status:
                all_jobs.append({
                    'job_id': job_id,
                    'type': job_info['type'],
                    'status': job_info['status'].value,
                    'started_at': job_info['started_at'].isoformat(),
                    'completed_at': job_info.get('completed_at', {}).isoformat() if 'completed_at' in job_info else None
                })
                
        # Sort by start time descending
        all_jobs.sort(key=lambda x: x['started_at'], reverse=True)
        
        return {
            'jobs': all_jobs[:limit],
            'total': len(all_jobs),
            'limit': limit
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/influence-analysis")
async def analyze_influence(node_ids: List[str] = Query(..., description="Node IDs to analyze"),
                          depth: int = Query(2, ge=1, le=5),
                          settings: Settings = Depends(get_settings)):
    """Analyze influence of nodes (combines PageRank and network metrics)"""
    try:
        # This would run a custom influence analysis job
        # For now, trigger PageRank as a proxy
        job_id = await graphx_engine.run_pagerank()
        
        return {
            "job_id": job_id,
            "nodes": node_ids,
            "depth": depth,
            "message": "Influence analysis job started"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/subgraph-mining")
async def mine_subgraphs(min_support: float = Query(0.1, ge=0.01, le=1.0),
                        max_size: int = Query(5, ge=2, le=10),
                        settings: Settings = Depends(get_settings)):
    """Mine frequent subgraph patterns"""
    try:
        # This would run subgraph mining algorithms
        # Placeholder for the API structure
        return {
            "message": "Subgraph mining not yet implemented",
            "parameters": {
                "min_support": min_support,
                "max_size": max_size
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def set_dependencies(ge: GraphXEngine, cm: CacheManager):
    """Set global dependencies"""
    global graphx_engine, cache_manager
    graphx_engine = ge
    cache_manager = cm 