"""
Lineage API Router

RESTful API endpoints for data lineage operations.
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body

from app.api.v1.dependencies import get_lineage_service, get_current_user
from app.services.catalog import LineageService
from app.core.lineage_processor import LineageDirection, ProcessType

router = APIRouter()


@router.post("")
async def create_lineage(
    process_name: str = Body(..., description="Process name"),
    process_type: ProcessType = Body(..., description="Process type"),
    inputs: List[str] = Body(..., description="Input entity GUIDs"),
    outputs: List[str] = Body(..., description="Output entity GUIDs"),
    metadata: Optional[Dict[str, Any]] = Body(None, description="Additional metadata"),
    lineage_service: LineageService = Depends(get_lineage_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Create lineage relationship between entities.
    
    - **process_name**: Name of the process creating the lineage
    - **process_type**: Type of process (ETL, STREAMING, etc.)
    - **inputs**: List of input entity GUIDs
    - **outputs**: List of output entity GUIDs
    - **metadata**: Additional process metadata
    """
    result = await lineage_service.create_lineage(
        process_name=process_name,
        process_type=process_type,
        inputs=inputs,
        outputs=outputs,
        metadata=metadata
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/{entity_guid}")
async def get_lineage(
    entity_guid: str = Path(..., description="Entity GUID"),
    direction: LineageDirection = Query(LineageDirection.BOTH, description="Lineage direction"),
    depth: int = Query(3, ge=1, le=10, description="Traversal depth"),
    lineage_service: LineageService = Depends(get_lineage_service)
):
    """
    Get lineage graph for an entity.
    
    - **entity_guid**: Entity to get lineage for
    - **direction**: UPSTREAM (sources), DOWNSTREAM (targets), or BOTH
    - **depth**: Maximum traversal depth
    """
    result = await lineage_service.get_lineage(
        entity_guid=entity_guid,
        direction=direction,
        depth=depth
    )
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data


@router.post("/impact/{entity_guid}")
async def analyze_impact(
    entity_guid: str = Path(..., description="Entity GUID"),
    change_type: str = Body("schema_change", description="Type of change"),
    changes: Optional[List[Dict[str, Any]]] = Body(None, description="Specific changes"),
    max_depth: int = Body(5, ge=1, le=10, description="Maximum analysis depth"),
    lineage_service: LineageService = Depends(get_lineage_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Analyze impact of changes to an entity.
    
    - **entity_guid**: Entity to analyze impact for
    - **change_type**: Type of change (schema_change, deletion, etc.)
    - **changes**: Specific changes to analyze
    - **max_depth**: Maximum depth for impact analysis
    """
    result = await lineage_service.analyze_impact(
        entity_guid=entity_guid,
        change_type=change_type,
        changes=changes,
        max_depth=max_depth
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.post("/transformation")
async def track_transformation(
    source_entities: List[Dict[str, Any]] = Body(..., description="Source entities"),
    target_entities: List[Dict[str, Any]] = Body(..., description="Target entities"),
    transformation: Dict[str, Any] = Body(..., description="Transformation details"),
    execution_context: Optional[Dict[str, Any]] = Body(None, description="Execution context"),
    lineage_service: LineageService = Depends(get_lineage_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Track data transformation for compliance and audit.
    
    - **source_entities**: List of source entities in transformation
    - **target_entities**: List of target entities
    - **transformation**: Details about the transformation
    - **execution_context**: Additional execution context
    """
    result = await lineage_service.track_transformation(
        source_entities=source_entities,
        target_entities=target_entities,
        transformation=transformation,
        execution_context=execution_context
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/compliance/{entity_guid}")
async def get_compliance_audit_trail(
    entity_guid: str = Path(..., description="Entity GUID"),
    start_date: Optional[str] = Query(None, description="Start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="End date (ISO format)"),
    include_lineage: bool = Query(True, description="Include lineage information"),
    lineage_service: LineageService = Depends(get_lineage_service)
):
    """
    Get compliance audit trail for an entity.
    
    - **entity_guid**: Entity to get audit trail for
    - **start_date**: Filter by start date
    - **end_date**: Filter by end date
    - **include_lineage**: Whether to include lineage information
    """
    # Convert string dates to datetime if provided
    from datetime import datetime
    start_dt = datetime.fromisoformat(start_date) if start_date else None
    end_dt = datetime.fromisoformat(end_date) if end_date else None
    
    result = await lineage_service.get_compliance_audit_trail(
        entity_guid=entity_guid,
        start_date=start_dt,
        end_date=end_dt,
        include_lineage=include_lineage
    )
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data


@router.post("/sensitive-data-flows")
async def find_sensitive_data_flows(
    classification: str = Body("PII", description="Data classification"),
    start_entity: Optional[str] = Body(None, description="Starting entity GUID"),
    lineage_service: LineageService = Depends(get_lineage_service),
    current_user: dict = Depends(get_current_user)
):
    """
    Find flows of sensitive data through the system.
    
    - **classification**: Classification to track (PII, PCI, PHI, etc.)
    - **start_entity**: Optional starting entity to limit search
    """
    result = await lineage_service.find_sensitive_data_flows(
        classification=classification,
        start_entity=start_entity
    )
    
    if not result.success:
        raise HTTPException(status_code=400, detail=result.error)
        
    return result.data


@router.get("/visualize/{entity_guid}")
async def visualize_lineage(
    entity_guid: str = Path(..., description="Entity GUID"),
    depth: int = Query(3, ge=1, le=10, description="Visualization depth"),
    direction: LineageDirection = Query(LineageDirection.BOTH, description="Lineage direction"),
    include_columns: bool = Query(False, description="Include column-level lineage"),
    lineage_service: LineageService = Depends(get_lineage_service)
):
    """
    Get lineage visualization data.
    
    - **entity_guid**: Entity to visualize lineage for
    - **depth**: Maximum depth to visualize
    - **direction**: Direction to traverse
    - **include_columns**: Include column-level lineage details
    """
    result = await lineage_service.visualize_lineage(
        entity_guid=entity_guid,
        depth=depth,
        direction=direction,
        include_columns=include_columns
    )
    
    if not result.success:
        raise HTTPException(status_code=404, detail=result.error)
        
    return result.data 