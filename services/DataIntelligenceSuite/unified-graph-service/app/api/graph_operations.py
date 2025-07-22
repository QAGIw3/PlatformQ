"""Graph operations API endpoints"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from app.core.config import Settings, get_settings
from app.graph.janusgraph_client import JanusGraphClient
from app.core.cache_manager import CacheManager


router = APIRouter(prefix="/api/v1/graph", tags=["graph"])

# Global instances (will be injected)
graph_client: Optional[JanusGraphClient] = None
cache_manager: Optional[CacheManager] = None


class NodeCreate(BaseModel):
    """Node creation request"""
    label: str = Field(..., description="Node label/type")
    properties: Dict[str, Any] = Field(..., description="Node properties")
    node_id: Optional[str] = Field(None, description="Optional node ID")


class NodeUpdate(BaseModel):
    """Node update request"""
    properties: Dict[str, Any] = Field(..., description="Properties to update")


class EdgeCreate(BaseModel):
    """Edge creation request"""
    label: str = Field(..., description="Edge label/type")
    from_id: str = Field(..., description="Source node ID")
    to_id: str = Field(..., description="Target node ID")
    properties: Optional[Dict[str, Any]] = Field(None, description="Edge properties")


class QueryRequest(BaseModel):
    """Gremlin query request"""
    query: str = Field(..., description="Gremlin query")
    bindings: Optional[Dict[str, Any]] = Field(None, description="Query parameters")
    timeout: Optional[int] = Field(30000, description="Query timeout in milliseconds")


class BatchOperation(BaseModel):
    """Batch operation request"""
    operations: List[Dict[str, Any]] = Field(..., description="List of operations")


@router.post("/nodes", response_model=Dict[str, str])
async def create_node(request: NodeCreate,
                     settings: Settings = Depends(get_settings)):
    """Create a new node in the graph"""
    try:
        # Check cache for duplicate
        if request.node_id:
            cached = await cache_manager.get_cached_node(request.node_id)
            if cached:
                raise HTTPException(status_code=409, detail="Node already exists")
                
        # Create node
        node_id = await graph_client.create_node(
            request.label,
            request.properties,
            request.node_id
        )
        
        # Cache the node
        await cache_manager.cache_node(node_id, {
            'id': node_id,
            'label': request.label,
            **request.properties
        })
        
        return {"id": node_id, "message": "Node created successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nodes/{node_id}")
async def get_node(node_id: str,
                  settings: Settings = Depends(get_settings)):
    """Get node details by ID"""
    try:
        # Check cache first
        cached = await cache_manager.get_cached_node(node_id)
        if cached:
            return cached
            
        # Get from database
        node = await graph_client.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
            
        # Cache for next time
        await cache_manager.cache_node(node_id, node)
        
        return node
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/nodes/{node_id}")
async def update_node(node_id: str,
                     request: NodeUpdate,
                     settings: Settings = Depends(get_settings)):
    """Update node properties"""
    try:
        # Update node
        success = await graph_client.update_node(node_id, request.properties)
        if not success:
            raise HTTPException(status_code=404, detail="Node not found")
            
        # Invalidate cache
        await cache_manager.invalidate_node_cache(node_id)
        
        return {"message": "Node updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/nodes/{node_id}")
async def delete_node(node_id: str,
                     settings: Settings = Depends(get_settings)):
    """Delete a node and its edges"""
    try:
        # Delete node
        success = await graph_client.delete_node(node_id)
        if not success:
            raise HTTPException(status_code=404, detail="Node not found")
            
        # Invalidate cache
        await cache_manager.invalidate_node_cache(node_id)
        
        return {"message": "Node deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/edges", response_model=Dict[str, str])
async def create_edge(request: EdgeCreate,
                     settings: Settings = Depends(get_settings)):
    """Create an edge between two nodes"""
    try:
        # Verify nodes exist
        from_node = await graph_client.get_node(request.from_id)
        to_node = await graph_client.get_node(request.to_id)
        
        if not from_node or not to_node:
            raise HTTPException(status_code=404, detail="One or both nodes not found")
            
        # Create edge
        edge_id = await graph_client.create_edge(
            request.label,
            request.from_id,
            request.to_id,
            request.properties
        )
        
        # Invalidate node caches
        await cache_manager.invalidate_node_cache(request.from_id)
        await cache_manager.invalidate_node_cache(request.to_id)
        
        return {"id": edge_id, "message": "Edge created successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def execute_query(request: QueryRequest,
                       settings: Settings = Depends(get_settings)):
    """Execute a Gremlin query"""
    try:
        # Check cache for query results
        cached = await cache_manager.get_cached_query(request.query, request.bindings or {})
        if cached:
            return {"results": cached, "cached": True}
            
        # Execute query
        results = await graph_client.execute_query(request.query, request.bindings)
        
        # Cache results for future
        await cache_manager.cache_graph_query(
            request.query,
            request.bindings or {},
            results
        )
        
        return {"results": results, "cached": False}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch")
async def batch_operations(request: BatchOperation,
                          settings: Settings = Depends(get_settings)):
    """Execute batch operations"""
    try:
        results = []
        errors = []
        
        for i, op in enumerate(request.operations):
            try:
                op_type = op.get('type')
                
                if op_type == 'create_node':
                    nodes = op.get('nodes', [])
                    node_ids = await graph_client.batch_create_nodes(nodes)
                    results.append({
                        'operation': i,
                        'type': op_type,
                        'success': True,
                        'node_ids': node_ids
                    })
                    
                elif op_type == 'create_edge':
                    edge_id = await graph_client.create_edge(
                        op['label'],
                        op['from_id'],
                        op['to_id'],
                        op.get('properties')
                    )
                    results.append({
                        'operation': i,
                        'type': op_type,
                        'success': True,
                        'edge_id': edge_id
                    })
                    
                else:
                    errors.append({
                        'operation': i,
                        'error': f"Unknown operation type: {op_type}"
                    })
                    
            except Exception as e:
                errors.append({
                    'operation': i,
                    'error': str(e)
                })
                
        return {
            'successful': len(results),
            'failed': len(errors),
            'results': results,
            'errors': errors
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nodes/{node_id}/neighbors")
async def get_neighbors(node_id: str,
                       direction: str = Query("both", regex="^(in|out|both)$"),
                       edge_label: Optional[str] = None,
                       limit: int = Query(100, ge=1, le=1000),
                       settings: Settings = Depends(get_settings)):
    """Get neighbors of a node"""
    try:
        neighbors = await graph_client.get_neighbors(
            node_id,
            direction,
            edge_label,
            limit
        )
        
        return {
            'node_id': node_id,
            'direction': direction,
            'neighbors': neighbors,
            'count': len(neighbors)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/shortest-path")
async def find_shortest_path(from_id: str = Query(..., description="Source node ID"),
                           to_id: str = Query(..., description="Target node ID"),
                           max_depth: Optional[int] = Query(None, ge=1, le=20),
                           settings: Settings = Depends(get_settings)):
    """Find shortest path between two nodes"""
    try:
        path = await graph_client.find_shortest_path(from_id, to_id, max_depth)
        
        if not path:
            return {
                'found': False,
                'message': "No path found between nodes"
            }
            
        return {
            'found': True,
            'path': path,
            'length': len(path) - 1
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_graph_statistics(settings: Settings = Depends(get_settings)):
    """Get graph statistics"""
    try:
        node_count = await graph_client.count_nodes()
        edge_count = await graph_client.count_edges()
        
        # Get cache stats
        cache_stats = await cache_manager.get_stats()
        
        return {
            'nodes': node_count,
            'edges': edge_count,
            'cache': cache_stats
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def set_dependencies(gc: JanusGraphClient, cm: CacheManager):
    """Set global dependencies"""
    global graph_client, cache_manager
    graph_client = gc
    cache_manager = cm 