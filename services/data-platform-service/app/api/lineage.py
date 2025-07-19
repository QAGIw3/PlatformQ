"""
Data lineage API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
import uuid

router = APIRouter()


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


@router.post("/nodes")
async def create_lineage_node(
    node_type: NodeType,
    name: str = Query(..., description="Node name"),
    namespace: str = Query(..., description="Node namespace"),
    attributes: Optional[Dict[str, Any]] = None,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a lineage node"""
    node_id = str(uuid.uuid4())
    
    # Get lineage manager from app state
    lineage_manager = request.app.state.lineage_manager
    
    return {
        "node_id": node_id,
        "node_type": node_type,
        "name": name,
        "namespace": namespace,
        "attributes": attributes or {},
        "created_at": datetime.utcnow()
    }


@router.post("/edges")
async def create_lineage_edge(
    source_id: str = Query(..., description="Source node ID"),
    target_id: str = Query(..., description="Target node ID"),
    edge_type: EdgeType = Query(..., description="Edge type"),
    attributes: Optional[Dict[str, Any]] = None,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a lineage edge"""
    edge_id = str(uuid.uuid4())
    
    return {
        "edge_id": edge_id,
        "source_id": source_id,
        "target_id": target_id,
        "edge_type": edge_type,
        "attributes": attributes or {},
        "created_at": datetime.utcnow()
    }


@router.get("/nodes/{node_id}/lineage", response_model=LineageGraph)
async def get_node_lineage(
    node_id: str,
    direction: str = Query("both", description="upstream, downstream, both"),
    depth: int = Query(3, ge=1, le=10),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get lineage for a node"""
    # Mock lineage graph
    return LineageGraph(
        nodes=[
            LineageNode(
                node_id=node_id,
                node_type=NodeType.TABLE,
                name="customer_orders",
                namespace="analytics",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            LineageNode(
                node_id="upstream_1",
                node_type=NodeType.TABLE,
                name="raw_orders",
                namespace="raw",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            ),
            LineageNode(
                node_id="downstream_1",
                node_type=NodeType.REPORT,
                name="daily_revenue_report",
                namespace="reports",
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        ],
        edges=[
            LineageEdge(
                edge_id="edge_1",
                source_id="upstream_1",
                target_id=node_id,
                edge_type=EdgeType.TRANSFORMS,
                created_at=datetime.utcnow()
            ),
            LineageEdge(
                edge_id="edge_2",
                source_id=node_id,
                target_id="downstream_1",
                edge_type=EdgeType.DERIVES_FROM,
                created_at=datetime.utcnow()
            )
        ],
        metadata={
            "direction": direction,
            "depth": depth,
            "total_nodes": 3,
            "total_edges": 2
        }
    )


@router.post("/impact-analysis")
async def analyze_impact(
    node_id: str = Query(..., description="Node to analyze"),
    impact_type: str = Query("downstream", description="upstream or downstream"),
    max_depth: int = Query(5, ge=1, le=10),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Analyze impact of changes"""
    return ImpactAnalysis(
        source_node=node_id,
        impact_type=impact_type,
        affected_nodes=[
            {
                "node_id": "affected_1",
                "name": "monthly_summary",
                "type": "report",
                "distance": 1,
                "impact_score": 0.9
            },
            {
                "node_id": "affected_2",
                "name": "executive_dashboard",
                "type": "dashboard",
                "distance": 2,
                "impact_score": 0.7
            }
        ],
        depth=max_depth,
        critical_paths=[
            [node_id, "transform_1", "affected_1"],
            [node_id, "aggregate_1", "affected_2"]
        ]
    )


@router.get("/column-lineage/{table_id}/{column_name}")
async def get_column_lineage(
    table_id: str,
    column_name: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get column-level lineage"""
    return {
        "table_id": table_id,
        "column_name": column_name,
        "lineage": {
            "upstream": [
                {
                    "table_id": "source_table_1",
                    "column_name": "original_column",
                    "transformation": "CAST(original_column AS VARCHAR)"
                }
            ],
            "downstream": [
                {
                    "table_id": "derived_table_1",
                    "column_name": "derived_column",
                    "usage": "JOIN key"
                }
            ]
        },
        "data_type_changes": [
            {
                "from_type": "INT",
                "to_type": "VARCHAR",
                "location": "transform_step_1"
            }
        ]
    }


@router.post("/bulk-import")
async def bulk_import_lineage(
    source_type: str = Query(..., description="Source type (dbt, airflow, spark, custom)"),
    lineage_data: Dict[str, Any] = Query(..., description="Lineage data to import"),
    merge_strategy: str = Query("update", description="merge, update, or replace"),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Bulk import lineage from external sources"""
    import_id = str(uuid.uuid4())
    
    return {
        "import_id": import_id,
        "status": "processing",
        "source_type": source_type,
        "merge_strategy": merge_strategy,
        "nodes_to_import": 50,
        "edges_to_import": 75,
        "started_at": datetime.utcnow()
    }


@router.get("/search")
async def search_lineage(
    q: str = Query(..., description="Search query"),
    node_types: Optional[List[NodeType]] = Query(None),
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0)
):
    """Search lineage graph"""
    return {
        "query": q,
        "results": [
            {
                "node_id": "node_123",
                "name": "customer_data",
                "type": "table",
                "namespace": "analytics",
                "score": 0.95,
                "highlights": ["customer <em>data</em> processing"]
            }
        ],
        "total": 1
    }


@router.get("/data-flow/{source_id}/{target_id}")
async def get_data_flow(
    source_id: str,
    target_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get data flow between two nodes"""
    return {
        "source": source_id,
        "target": target_id,
        "paths": [
            {
                "path": [source_id, "transform_1", "aggregate_1", target_id],
                "distance": 3,
                "transformations": [
                    "Filter WHERE active = true",
                    "GROUP BY customer_id",
                    "JOIN with dimensions"
                ]
            }
        ],
        "shortest_distance": 3
    }


@router.get("/dependencies/{node_id}")
async def get_dependencies(
    node_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    recursive: bool = Query(True, description="Include transitive dependencies")
):
    """Get node dependencies"""
    return {
        "node_id": node_id,
        "direct_dependencies": [
            {
                "node_id": "dep_1",
                "name": "users_table",
                "type": "table",
                "criticality": "high"
            },
            {
                "node_id": "dep_2",
                "name": "config_api",
                "type": "api",
                "criticality": "medium"
            }
        ],
        "transitive_dependencies": 15 if recursive else 0,
        "dependency_graph": {
            "max_depth": 4,
            "critical_path": ["dep_1", "transform_1", "dep_3", node_id]
        }
    }


@router.post("/validate")
async def validate_lineage(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    check_cycles: bool = Query(True),
    check_orphans: bool = Query(True)
):
    """Validate lineage consistency"""
    return {
        "validation_id": str(uuid.uuid4()),
        "status": "completed",
        "issues": [
            {
                "type": "orphan_node",
                "severity": "warning",
                "node_id": "orphan_1",
                "message": "Node has no upstream or downstream connections"
            },
            {
                "type": "circular_dependency",
                "severity": "error",
                "cycle": ["node_a", "node_b", "node_c", "node_a"],
                "message": "Circular dependency detected"
            }
        ],
        "summary": {
            "total_nodes": 150,
            "total_edges": 200,
            "orphan_nodes": 1,
            "cycles_found": 1
        },
        "validated_at": datetime.utcnow()
    } 