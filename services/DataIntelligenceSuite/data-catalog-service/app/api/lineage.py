"""
Data Lineage API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel, Field

from app.core import LineageProcessor, AtlasClient, LineageDirection, ProcessType
from platformq_events import EventStream
from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/lineage", tags=["lineage"])

# Global dependencies
lineage_processor: Optional[LineageProcessor] = None
atlas_client: Optional[AtlasClient] = None
event_stream: Optional[EventStream] = None


def set_dependencies(processor: LineageProcessor, atlas: AtlasClient, events: EventStream):
    """Set the global dependencies for this router"""
    global lineage_processor, atlas_client, event_stream
    lineage_processor = processor
    atlas_client = atlas
    event_stream = events


# Request/Response Models
class LineageEntity(BaseModel):
    """Entity in lineage"""
    guid: Optional[str] = None
    qualified_name: Optional[str] = None
    type_name: str
    name: str
    attributes: Optional[Dict[str, Any]] = None


class ProcessInfo(BaseModel):
    """Process information for lineage"""
    type_name: str = Field("Process", description="Process entity type")
    attributes: Dict[str, Any] = Field(..., description="Process attributes")
    
    class Config:
        schema_extra = {
            "example": {
                "type_name": "spark_job",
                "attributes": {
                    "name": "customer_aggregation",
                    "qualifiedName": "spark.jobs.customer_aggregation",
                    "processType": "BATCH",
                    "description": "Customer data aggregation job"
                }
            }
        }


class LineageCreateRequest(BaseModel):
    """Create lineage relationship request"""
    process: ProcessInfo
    inputs: List[LineageEntity]
    outputs: List[LineageEntity]
    execution_time: Optional[datetime] = None
    properties: Optional[Dict[str, Any]] = None


class LineageNode(BaseModel):
    """Node in lineage graph"""
    guid: str
    type_name: str
    qualified_name: str
    name: str
    display_name: str
    status: str
    attributes: Dict[str, Any]
    depth: int


class LineageEdge(BaseModel):
    """Edge in lineage graph"""
    from_guid: str
    to_guid: str
    relationship_type: str
    process_guid: Optional[str]
    attributes: Optional[Dict[str, Any]]


class LineageGraph(BaseModel):
    """Lineage graph response"""
    base_entity_guid: str
    direction: LineageDirection
    depth: int
    nodes: List[LineageNode]
    edges: List[LineageEdge]
    total_nodes: int
    total_edges: int


class ImpactAnalysisResult(BaseModel):
    """Impact analysis result"""
    entity_guid: str
    entity_name: str
    impacted_entities: List[Dict[str, Any]]
    impact_paths: List[List[str]]
    risk_score: float
    recommendations: List[str]


class LineageMetrics(BaseModel):
    """Lineage processing metrics"""
    total_entities: int
    total_processes: int
    total_relationships: int
    avg_lineage_depth: float
    orphaned_entities: int
    processing_lag_seconds: float
    last_updated: datetime


class TransformationTrackRequest(BaseModel):
    """Track data transformation request"""
    source_entities: List[LineageEntity] = Field(..., description="Source entities")
    target_entities: List[LineageEntity] = Field(..., description="Target entities")
    transformation: Dict[str, Any] = Field(..., description="Transformation details")
    execution_context: Optional[Dict[str, Any]] = Field(None, description="Execution context")
    track_compliance: bool = Field(False, description="Track for compliance")
    
    class Config:
        schema_extra = {
            "example": {
                "source_entities": [
                    {"qualified_name": "table.sales.raw", "type_name": "hive_table"}
                ],
                "target_entities": [
                    {"qualified_name": "table.sales.aggregated", "type_name": "hive_table"}
                ],
                "transformation": {
                    "type": "aggregation",
                    "logic": "SUM(amount) GROUP BY customer_id",
                    "job_id": "spark-job-123",
                    "column_mappings": [
                        {"source": "amount", "target": "total_amount", "transformation": "SUM"}
                    ]
                }
            }
        }


class ImpactAnalysisRequest(BaseModel):
    """Impact analysis request"""
    entity_guid: str = Field(..., description="Entity to analyze")
    change_type: str = Field("schema_change", description="Type of change")
    changes: Optional[List[Dict[str, Any]]] = Field(None, description="Specific changes")
    max_depth: int = Field(5, ge=1, le=10)
    include_indirect: bool = Field(True)
    
    class Config:
        schema_extra = {
            "example": {
                "entity_guid": "abc-123",
                "change_type": "schema_change",
                "changes": [
                    {"column": "email", "action": "modify", "from": "varchar(100)", "to": "varchar(255)"},
                    {"column": "phone", "action": "drop"}
                ]
            }
        }


class SensitiveDataFlowQuery(BaseModel):
    """Query for sensitive data flows"""
    classification: str = Field("PII", description="Data classification to track")
    start_entity: Optional[str] = Field(None, description="Starting entity pattern")
    include_third_party: bool = Field(True, description="Include third-party sharing")


class ComplianceAuditQuery(BaseModel):
    """Compliance audit trail query"""
    entity_guid: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    include_lineage: bool = True
    compliance_type: Optional[str] = Field(None, description="GDPR, CCPA, etc.")


# API Endpoints
@router.post("/", response_model=Dict[str, Any])
async def create_lineage(request: LineageCreateRequest):
    """
    Create lineage relationship between inputs and outputs through a process
    
    This endpoint creates:
    1. Process entity (if not exists)
    2. Input/Output entities (if not exist)
    3. Lineage relationships between them
    """
    try:
        # Create or get process entity
        process_guid = await lineage_processor.create_process(
            process_type=request.process.type_name,
            attributes=request.process.attributes,
            execution_time=request.execution_time
        )
        
        # Create lineage relationships
        result = await lineage_processor.create_lineage(
            process_guid=process_guid,
            inputs=request.inputs,
            outputs=request.outputs,
            properties=request.properties
        )
        
        # Emit lineage event
        await event_stream.publish(
            topic="catalog-lineage-created",
            event={
                "process_guid": process_guid,
                "input_count": len(request.inputs),
                "output_count": len(request.outputs),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {
            "process_guid": process_guid,
            "lineage_created": result,
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"Failed to create lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{guid}", response_model=LineageGraph)
async def get_lineage(
    guid: str = Path(..., description="Entity GUID"),
    direction: LineageDirection = Query(LineageDirection.BOTH, description="Lineage direction"),
    depth: int = Query(3, ge=1, le=10, description="Traversal depth")
):
    """
    Get lineage graph for an entity
    
    - **guid**: Entity GUID to get lineage for
    - **direction**: UPSTREAM (sources), DOWNSTREAM (targets), or BOTH
    - **depth**: How many levels to traverse (max 10)
    """
    try:
        # Get lineage from processor
        lineage_data = await lineage_processor.get_lineage(
            entity_guid=guid,
            direction=direction,
            depth=depth
        )
        
        # Build graph response
        nodes = []
        edges = []
        
        for node_data in lineage_data["nodes"]:
            nodes.append(LineageNode(
                guid=node_data["guid"],
                type_name=node_data["typeName"],
                qualified_name=node_data["qualifiedName"],
                name=node_data["name"],
                display_name=node_data.get("displayName", node_data["name"]),
                status=node_data.get("status", "ACTIVE"),
                attributes=node_data.get("attributes", {}),
                depth=node_data["depth"]
            ))
        
        for edge_data in lineage_data["edges"]:
            edges.append(LineageEdge(
                from_guid=edge_data["fromGuid"],
                to_guid=edge_data["toGuid"],
                relationship_type=edge_data["relationshipType"],
                process_guid=edge_data.get("processGuid"),
                attributes=edge_data.get("attributes")
            ))
        
        return LineageGraph(
            base_entity_guid=guid,
            direction=direction,
            depth=depth,
            nodes=nodes,
            edges=edges,
            total_nodes=len(nodes),
            total_edges=len(edges)
        )
        
    except Exception as e:
        logger.error(f"Failed to get lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/impact/{guid}", response_model=ImpactAnalysisResult)
async def analyze_impact(
    guid: str = Path(..., description="Entity GUID to analyze impact for"),
    max_depth: int = Query(5, ge=1, le=10, description="Maximum analysis depth"),
    include_indirect: bool = Query(True, description="Include indirect impacts")
):
    """
    Analyze the impact of changes to an entity
    
    Returns:
    - Directly and indirectly impacted entities
    - Impact propagation paths
    - Risk assessment
    - Recommendations for safe changes
    """
    try:
        # Perform impact analysis
        impact_data = await lineage_processor.analyze_impact(
            entity_guid=guid,
            max_depth=max_depth,
            include_indirect=include_indirect
        )
        
        # Get entity details
        entity = await atlas_client.get_entity_by_guid(guid)
        
        return ImpactAnalysisResult(
            entity_guid=guid,
            entity_name=entity["attributes"]["name"],
            impacted_entities=impact_data["impacted_entities"],
            impact_paths=impact_data["impact_paths"],
            risk_score=impact_data["risk_score"],
            recommendations=impact_data["recommendations"]
        )
        
    except Exception as e:
        logger.error(f"Failed to analyze impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/graph/{guid}/export")
async def export_lineage_graph(
    guid: str = Path(..., description="Entity GUID"),
    direction: LineageDirection = Query(LineageDirection.BOTH),
    depth: int = Query(3, ge=1, le=10),
    format: str = Query("dot", pattern="^(dot|json|graphml)$", description="Export format")
):
    """
    Export lineage graph in various formats
    
    Supported formats:
    - dot: GraphViz DOT format
    - json: JSON graph representation
    - graphml: GraphML XML format
    """
    try:
        # Get lineage data
        lineage_data = await lineage_processor.get_lineage(
            entity_guid=guid,
            direction=direction,
            depth=depth
        )
        
        # Export in requested format
        if format == "dot":
            export_data = await lineage_processor.export_as_dot(lineage_data)
            return {"format": "dot", "data": export_data}
        elif format == "graphml":
            export_data = await lineage_processor.export_as_graphml(lineage_data)
            return {"format": "graphml", "data": export_data}
        else:  # json
            return {"format": "json", "data": lineage_data}
            
    except Exception as e:
        logger.error(f"Failed to export lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validate/{guid}")
async def validate_lineage(
    guid: str = Path(..., description="Entity GUID to validate lineage for"),
    fix_issues: bool = Query(False, description="Attempt to fix identified issues")
):
    """
    Validate lineage integrity for an entity
    
    Checks for:
    - Broken relationships
    - Circular dependencies
    - Orphaned entities
    - Missing process information
    """
    try:
        # Validate lineage
        validation_result = await lineage_processor.validate_lineage(
            entity_guid=guid,
            fix_issues=fix_issues
        )
        
        return {
            "entity_guid": guid,
            "is_valid": validation_result["is_valid"],
            "issues": validation_result["issues"],
            "fixes_applied": validation_result.get("fixes_applied", []),
            "validation_time": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to validate lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=LineageMetrics)
async def get_lineage_metrics():
    """Get lineage processing metrics and statistics"""
    try:
        metrics = await lineage_processor.get_metrics()
        
        return LineageMetrics(
            total_entities=metrics["total_entities"],
            total_processes=metrics["total_processes"],
            total_relationships=metrics["total_relationships"],
            avg_lineage_depth=metrics["avg_lineage_depth"],
            orphaned_entities=metrics["orphaned_entities"],
            processing_lag_seconds=metrics["processing_lag_seconds"],
            last_updated=metrics["last_updated"]
        )
        
    except Exception as e:
        logger.error(f"Failed to get lineage metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/refresh")
async def refresh_lineage_cache():
    """
    Refresh lineage cache from Atlas
    
    This operation may take time for large catalogs
    """
    try:
        await lineage_processor.refresh_cache()
        
        return {
            "status": "success",
            "message": "Lineage cache refresh initiated",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to refresh lineage cache: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 


@router.post("/transform", response_model=Dict[str, Any])
async def track_transformation(request: TransformationTrackRequest):
    """
    Track a data transformation with detailed lineage
    
    Captures transformation logic, column mappings, and execution context
    """
    try:
        result = await lineage_processor.track_transformation(
            source_entities=[e.dict() for e in request.source_entities],
            target_entities=[e.dict() for e in request.target_entities],
            transformation=request.transformation,
            execution_context=request.execution_context
        )
        
        # Publish transformation event
        if event_stream:
            await event_stream.publish(
                topic="catalog-transformation-tracked",
                event={
                    "process_guid": result["process_guid"],
                    "transformation_type": request.transformation.get("type"),
                    "source_count": result["inputs"],
                    "target_count": result["outputs"],
                    "compliance_tracked": request.track_compliance,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to track transformation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/impact/simulate", response_model=Dict[str, Any])
async def simulate_impact(request: ImpactAnalysisRequest):
    """
    Simulate the impact of changes without applying them
    
    Useful for change management and risk assessment
    """
    try:
        impact_result = await lineage_processor.analyze_impact(
            entity_guid=request.entity_guid,
            change_type=request.change_type,
            changes=request.changes,
            max_depth=request.max_depth,
            include_indirect=request.include_indirect
        )
        
        # Enhance with additional analysis
        if impact_result["risk_level"] in ["high", "critical"]:
            # Get mitigation suggestions
            impact_result["mitigation_plan"] = {
                "suggested_phases": _calculate_migration_phases(impact_result),
                "rollback_strategy": _generate_rollback_strategy(impact_result),
                "testing_recommendations": _get_testing_recommendations(impact_result)
            }
        
        return impact_result
        
    except Exception as e:
        logger.error(f"Failed to simulate impact: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{guid}/visualization", response_model=Dict[str, Any])
async def get_lineage_visualization(
    guid: str = Path(..., description="Entity GUID"),
    depth: int = Query(3, ge=1, le=10),
    direction: LineageDirection = Query(LineageDirection.BOTH),
    include_columns: bool = Query(False, description="Include column-level lineage")
):
    """
    Get lineage data formatted for visualization (D3.js compatible)
    
    Returns graph data with nodes and edges suitable for interactive visualization
    """
    try:
        visualization_data = await lineage_processor.visualize_lineage(
            entity_guid=guid,
            depth=depth,
            direction=direction,
            include_columns=include_columns
        )
        
        return visualization_data
        
    except Exception as e:
        logger.error(f"Failed to get lineage visualization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compliance/audit-trail", response_model=Dict[str, Any])
async def get_compliance_audit_trail(request: ComplianceAuditQuery):
    """
    Get complete audit trail for compliance reporting
    
    Supports GDPR, CCPA, and other compliance requirements
    """
    try:
        audit_trail = await lineage_processor.get_compliance_audit_trail(
            entity_guid=request.entity_guid,
            start_date=request.start_date,
            end_date=request.end_date,
            include_lineage=request.include_lineage
        )
        
        # Add compliance-specific information
        if request.compliance_type == "GDPR":
            audit_trail["gdpr_info"] = {
                "data_controller": "PlatformQ",
                "legal_basis": "legitimate_interest",
                "retention_period": audit_trail["retention_info"]["retention_period_days"],
                "data_portability": True,
                "erasure_eligible": not audit_trail["retention_info"].get("legal_hold", False)
            }
        
        return audit_trail
        
    except Exception as e:
        logger.error(f"Failed to get compliance audit trail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compliance/sensitive-data-flows", response_model=Dict[str, Any])
async def find_sensitive_data_flows(request: SensitiveDataFlowQuery):
    """
    Find all flows containing sensitive data
    
    Essential for privacy compliance and data governance
    """
    try:
        flows = await lineage_processor.find_sensitive_data_flows(
            classification=request.classification,
            start_entity=request.start_entity
        )
        
        # Generate compliance report
        compliance_report = {
            "flows": flows,
            "summary": {
                "total_sources": len(flows["data_sources"]),
                "total_destinations": len(flows["data_destinations"]),
                "third_party_sharing": len(flows["third_party_sharing"]) if request.include_third_party else 0,
                "processing_activities": len(flows["processing_activities"])
            },
            "recommendations": _generate_privacy_recommendations(flows),
            "report_timestamp": datetime.utcnow().isoformat()
        }
        
        return compliance_report
        
    except Exception as e:
        logger.error(f"Failed to find sensitive data flows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/transformations", response_model=Dict[str, Any])
async def get_transformation_statistics():
    """Get statistics about data transformations"""
    try:
        # This would aggregate transformation metrics
        stats = {
            "total_transformations": 0,
            "transformations_by_type": {},
            "average_execution_time_ms": 0,
            "failed_transformations": 0,
            "most_active_pipelines": [],
            "column_mapping_stats": {}
        }
        
        # TODO: Implement actual statistics gathering
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get transformation statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions
def _calculate_migration_phases(impact_result: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Calculate suggested migration phases based on impact"""
    phases = []
    
    # Group impacted entities by severity
    by_severity = {}
    for entity in impact_result["impacted_entities"]:
        severity = entity["severity"]
        if severity not in by_severity:
            by_severity[severity] = []
        by_severity[severity].append(entity)
    
    # Create phases
    phase_num = 1
    for severity in ["low", "medium", "high"]:
        if severity in by_severity:
            phases.append({
                "phase": phase_num,
                "name": f"Phase {phase_num}: {severity.title()} Impact Changes",
                "entities": by_severity[severity],
                "estimated_duration": f"{len(by_severity[severity]) * 2} hours"
            })
            phase_num += 1
    
    return phases


def _generate_rollback_strategy(impact_result: Dict[str, Any]) -> Dict[str, Any]:
    """Generate rollback strategy"""
    return {
        "strategy_type": "snapshot_based" if impact_result["risk_level"] == "critical" else "incremental",
        "checkpoints": [
            "Before changes",
            "After each phase",
            "After validation"
        ],
        "validation_steps": [
            "Verify data integrity",
            "Check downstream systems",
            "Validate transformations"
        ]
    }


def _get_testing_recommendations(impact_result: Dict[str, Any]) -> List[str]:
    """Get testing recommendations based on impact"""
    recommendations = [
        "Test with sample data first",
        "Validate all transformation logic"
    ]
    
    if impact_result["breaking_changes"]:
        recommendations.append("Create comprehensive test suite for breaking changes")
        recommendations.append("Perform integration testing with downstream systems")
    
    return recommendations


def _generate_privacy_recommendations(flows: Dict[str, Any]) -> List[str]:
    """Generate privacy recommendations based on data flows"""
    recommendations = []
    
    if flows["third_party_sharing"]:
        recommendations.append("Review third-party data processing agreements")
        recommendations.append("Implement data minimization for external sharing")
    
    if len(flows["data_sources"]) > 5:
        recommendations.append("Consider consolidating data sources")
        recommendations.append("Implement centralized consent management")
    
    recommendations.append("Regularly audit sensitive data access")
    recommendations.append("Implement encryption for data at rest and in transit")
    
    return recommendations 