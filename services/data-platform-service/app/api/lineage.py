"""
Data lineage API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
import uuid
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class LineageType(str, Enum):
    DATA = "data"
    SCHEMA = "schema"
    COLUMN = "column"
    PROCESS = "process"


class NodeType(str, Enum):
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    API = "api"
    PROCESS = "process"
    REPORT = "report"
    MODEL = "model"


class EdgeType(str, Enum):
    DERIVES_FROM = "derives_from"
    TRANSFORMS = "transforms"
    COPIES = "copies"
    READS = "reads"
    WRITES = "writes"
    DEPENDS_ON = "depends_on"


class LineageNode(BaseModel):
    """Lineage graph node"""
    node_id: str
    node_type: NodeType
    name: str
    namespace: str
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


class LineageEdge(BaseModel):
    """Lineage graph edge"""
    edge_id: str
    source_id: str
    target_id: str
    edge_type: EdgeType
    attributes: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class LineageGraph(BaseModel):
    """Lineage graph"""
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ImpactAnalysis(BaseModel):
    """Impact analysis result"""
    source_node: str
    impact_type: str  # upstream, downstream
    affected_nodes: List[Dict[str, Any]]
    depth: int
    critical_paths: List[List[str]]


class LineageNodeCreate(BaseModel):
    """Create lineage node request"""
    node_type: NodeType
    name: str
    namespace: str
    attributes: Dict[str, Any] = Field(default_factory=dict)


class LineageEdgeCreate(BaseModel):
    """Create lineage edge request"""
    source_id: str
    target_id: str
    edge_type: EdgeType
    attributes: Dict[str, Any] = Field(default_factory=dict)


@router.post("/nodes", response_model=LineageNode)
async def create_lineage_node(
    node: LineageNodeCreate,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a lineage node"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Add node
        node_data = await main.lineage_tracker.add_node(
            node_type=node.node_type.value,
            name=node.name,
            namespace=node.namespace,
            attributes=node.attributes
        )
        
        return LineageNode(
            node_id=node_data["node_id"],
            node_type=node.node_type,
            name=node.name,
            namespace=node.namespace,
            attributes=node.attributes,
            created_at=node_data.get("created_at", datetime.utcnow()),
            updated_at=node_data.get("updated_at", datetime.utcnow())
        )
    except Exception as e:
        logger.error(f"Failed to create lineage node: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/nodes/{node_id}", response_model=LineageNode)
async def get_lineage_node(
    node_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get lineage node details"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        node = await main.lineage_tracker.get_node(node_id)
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        
        return LineageNode(
            node_id=node["node_id"],
            node_type=NodeType(node["node_type"]),
            name=node["name"],
            namespace=node["namespace"],
            attributes=node.get("attributes", {}),
            created_at=node.get("created_at", datetime.utcnow()),
            updated_at=node.get("updated_at", datetime.utcnow())
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get lineage node: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/edges", response_model=LineageEdge)
async def create_lineage_edge(
    edge: LineageEdgeCreate,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a lineage edge"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Add edge
        edge_data = await main.lineage_tracker.add_edge(
            source_id=edge.source_id,
            target_id=edge.target_id,
            edge_type=edge.edge_type.value,
            attributes=edge.attributes
        )
        
        return LineageEdge(
            edge_id=edge_data["edge_id"],
            source_id=edge.source_id,
            target_id=edge.target_id,
            edge_type=edge.edge_type,
            attributes=edge.attributes,
            created_at=edge_data.get("created_at", datetime.utcnow())
        )
    except Exception as e:
        logger.error(f"Failed to create lineage edge: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/{node_id}")
async def get_node_lineage(
    node_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    direction: str = Query("both", description="upstream, downstream, or both"),
    depth: int = Query(3, ge=1, le=10),
    include_columns: bool = Query(False)
):
    """Get lineage for a node"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Get lineage
        lineage_data = await main.lineage_tracker.get_lineage(
            node_id=node_id,
            direction=direction,
            depth=depth
        )
        
        # Convert to response format
        nodes = []
        edges = []
        
        for node in lineage_data.get("nodes", []):
            nodes.append(LineageNode(
                node_id=node["node_id"],
                node_type=NodeType(node["node_type"]),
                name=node["name"],
                namespace=node["namespace"],
                attributes=node.get("attributes", {}),
                created_at=node.get("created_at", datetime.utcnow()),
                updated_at=node.get("updated_at", datetime.utcnow())
            ))
        
        for edge in lineage_data.get("edges", []):
            edges.append(LineageEdge(
                edge_id=edge["edge_id"],
                source_id=edge["source_id"],
                target_id=edge["target_id"],
                edge_type=EdgeType(edge["edge_type"]),
                attributes=edge.get("attributes", {}),
                created_at=edge.get("created_at", datetime.utcnow())
            ))
        
        return LineageGraph(
            nodes=nodes,
            edges=edges,
            metadata=lineage_data.get("metadata", {})
        )
    except Exception as e:
        logger.error(f"Failed to get node lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/impact-analysis/{node_id}")
async def analyze_impact(
    node_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    impact_type: str = Query("downstream", description="upstream or downstream"),
    max_depth: int = Query(5, ge=1, le=10),
    include_indirect: bool = Query(True)
):
    """Analyze impact of changes to a node"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Perform impact analysis
        impact_results = await main.lineage_tracker.analyze_impact(
            node_id=node_id,
            direction=impact_type,
            max_depth=max_depth
        )
        
        return ImpactAnalysis(
            source_node=node_id,
            impact_type=impact_type,
            affected_nodes=impact_results.get("affected_nodes", []),
            depth=impact_results.get("depth", 0),
            critical_paths=impact_results.get("critical_paths", [])
        )
    except Exception as e:
        logger.error(f"Failed to analyze impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/column-lineage/{table_id}/{column_name}")
async def get_column_lineage(
    table_id: str,
    column_name: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    depth: int = Query(3, ge=1, le=10)
):
    """Get column-level lineage"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Get column lineage
        column_lineage = await main.lineage_tracker.get_column_lineage(
            table_id=table_id,
            column_name=column_name,
            depth=depth
        )
        
        return {
            "table_id": table_id,
            "column_name": column_name,
            "lineage": column_lineage.get("lineage", []),
            "transformations": column_lineage.get("transformations", []),
            "data_flow": column_lineage.get("data_flow", [])
        }
    except Exception as e:
        logger.error(f"Failed to get column lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/events")
async def record_lineage_event(
    request: Request,
    event_type: str = Query(..., description="Event type (e.g., transformation, load, query)"),
    source_nodes: List[str] = Body(..., description="Source node IDs"),
    target_nodes: List[str] = Body(..., description="Target node IDs"),
    tenant_id: str = Query(..., description="Tenant ID"),
    metadata: Dict[str, Any] = Body({}, description="Event metadata")
):
    """Record a lineage event"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Create event
        event_data = {
            "event_type": event_type,
            "source_nodes": source_nodes,
            "target_nodes": target_nodes,
            "metadata": metadata,
            "timestamp": datetime.utcnow(),
            "tenant_id": tenant_id
        }
        
        # Record event
        event_id = await main.lineage_tracker.record_lineage_event(event_data)
        
        # Create edges for the event
        for source in source_nodes:
            for target in target_nodes:
                await main.lineage_tracker.add_edge(
                    source_id=source,
                    target_id=target,
                    edge_type="transforms",
                    attributes={"event_id": event_id, "event_type": event_type}
                )
        
        return {
            "event_id": event_id,
            "status": "recorded",
            "edges_created": len(source_nodes) * len(target_nodes)
        }
    except Exception as e:
        logger.error(f"Failed to record lineage event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/provenance/{node_id}")
async def get_data_provenance(
    node_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    include_transformations: bool = Query(True),
    include_quality_scores: bool = Query(True)
):
    """Get complete data provenance for a node"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Get provenance
        provenance = await main.lineage_tracker.get_provenance(
            node_id=node_id,
            include_transformations=include_transformations,
            include_quality_scores=include_quality_scores
        )
        
        return {
            "node_id": node_id,
            "provenance": provenance.get("provenance", {}),
            "data_sources": provenance.get("data_sources", []),
            "transformations": provenance.get("transformations", []) if include_transformations else [],
            "quality_history": provenance.get("quality_history", []) if include_quality_scores else [],
            "confidence_score": provenance.get("confidence_score", 0.0)
        }
    except Exception as e:
        logger.error(f"Failed to get data provenance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search")
async def search_lineage(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    query: Optional[str] = Query(None, description="Search query"),
    node_type: Optional[NodeType] = Query(None),
    namespace: Optional[str] = Query(None),
    created_after: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000)
):
    """Search lineage nodes"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        # Build search criteria
        criteria = {}
        if node_type:
            criteria["node_type"] = node_type.value
        if namespace:
            criteria["namespace"] = namespace
        if created_after:
            criteria["created_after"] = created_after
        
        # Search nodes
        results = await main.lineage_tracker.search_nodes(
            query=query,
            criteria=criteria,
            limit=limit
        )
        
        # Convert to response format
        nodes = []
        for node in results.get("nodes", []):
            nodes.append(LineageNode(
                node_id=node["node_id"],
                node_type=NodeType(node["node_type"]),
                name=node["name"],
                namespace=node["namespace"],
                attributes=node.get("attributes", {}),
                created_at=node.get("created_at", datetime.utcnow()),
                updated_at=node.get("updated_at", datetime.utcnow())
            ))
        
        return {
            "nodes": nodes,
            "total": results.get("total", 0),
            "query": query
        }
    except Exception as e:
        logger.error(f"Failed to search lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_lineage_statistics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get lineage statistics"""
    if not main.lineage_tracker:
        raise HTTPException(status_code=503, detail="Lineage tracker not available")
    
    try:
        stats = await main.lineage_tracker.get_statistics()
        
        return {
            "total_nodes": stats.get("total_nodes", 0),
            "total_edges": stats.get("total_edges", 0),
            "nodes_by_type": stats.get("nodes_by_type", {}),
            "edges_by_type": stats.get("edges_by_type", {}),
            "avg_node_degree": stats.get("avg_node_degree", 0.0),
            "max_lineage_depth": stats.get("max_lineage_depth", 0),
            "orphan_nodes": stats.get("orphan_nodes", 0),
            "recent_updates": stats.get("recent_updates", [])
        }
    except Exception as e:
        logger.error(f"Failed to get lineage statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 