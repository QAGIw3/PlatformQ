"""
Classification and Tagging API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from app.core import Classifier, AtlasClient, ClassificationType
from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["classifications"])

# Global dependencies
classifier: Optional[Classifier] = None
atlas_client: Optional[AtlasClient] = None


def set_dependencies(clf: Classifier, atlas: AtlasClient):
    """Set the global dependencies for this router"""
    global classifier, atlas_client
    classifier = clf
    atlas_client = atlas


# Request/Response Models
class ClassificationDef(BaseModel):
    """Classification definition"""
    name: str = Field(..., description="Classification name")
    display_name: Optional[str] = None
    description: Optional[str] = None
    parent: Optional[str] = None
    entity_types: Optional[List[str]] = None
    attribute_defs: Optional[List[Dict[str, Any]]] = None
    options: Optional[Dict[str, Any]] = None


class ClassificationAssignment(BaseModel):
    """Classification assignment request"""
    classification_name: str
    entity_guids: List[str]
    attributes: Optional[Dict[str, Any]] = None
    propagate: bool = Field(True, description="Propagate to related entities")


class TagAssignment(BaseModel):
    """Tag assignment request"""
    tags: List[str]
    entity_guids: List[str]
    operation: str = Field("add", pattern="^(add|remove|set)$")


class AutoClassifyRequest(BaseModel):
    """Auto-classification request"""
    entity_guid: Optional[str] = None
    entity_type: Optional[str] = None
    sample_size: int = Field(1000, ge=100, le=10000)
    classifiers: List[str] = Field(
        default=["pii", "financial", "healthcare"],
        description="Classifiers to run"
    )
    confidence_threshold: float = Field(0.8, ge=0.0, le=1.0)
    dry_run: bool = Field(False, description="Preview without applying")


class ClassificationResult(BaseModel):
    """Classification operation result"""
    entity_guid: str
    entity_name: str
    classifications_added: List[str]
    classifications_removed: List[str]
    tags_added: List[str]
    tags_removed: List[str]
    status: str


class ClassificationRule(BaseModel):
    """Classification rule definition"""
    name: str
    description: Optional[str]
    rule_type: str = Field(..., pattern="^(regex|contains|datatype|custom)$")
    pattern: Optional[str] = None
    classification: str
    confidence: float = Field(0.9, ge=0.0, le=1.0)
    entity_types: Optional[List[str]] = None
    enabled: bool = True


# API Endpoints
@router.get("/classifications", response_model=List[Dict[str, Any]])
async def list_classifications(
    include_sub: bool = Query(True, description="Include sub-classifications"),
    entity_type: Optional[str] = Query(None, description="Filter by applicable entity type")
):
    """
    List all available classifications
    
    Returns classification definitions with their hierarchies and applicable entity types
    """
    try:
        classifications = await classifier.list_classifications(
            include_sub=include_sub,
            entity_type=entity_type
        )
        
        return classifications
        
    except Exception as e:
        logger.error(f"Failed to list classifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/classifications", response_model=Dict[str, Any])
async def create_classification(classification: ClassificationDef):
    """
    Create a new classification definition
    
    Creates a classification that can be assigned to entities
    """
    try:
        result = await classifier.create_classification(
            name=classification.name,
            display_name=classification.display_name,
            description=classification.description,
            parent=classification.parent,
            entity_types=classification.entity_types,
            attribute_defs=classification.attribute_defs,
            options=classification.options
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to create classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/classifications/{name}", response_model=Dict[str, Any])
async def update_classification(
    name: str = Path(..., description="Classification name"),
    update: ClassificationDef = Body(...)
):
    """Update an existing classification definition"""
    try:
        result = await classifier.update_classification(
            name=name,
            updates=update.dict(exclude_none=True)
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to update classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/classifications/{name}")
async def delete_classification(
    name: str = Path(..., description="Classification name"),
    force: bool = Query(False, description="Force delete even if in use")
):
    """Delete a classification definition"""
    try:
        await classifier.delete_classification(name=name, force=force)
        
        return {"status": "success", "message": f"Classification {name} deleted"}
        
    except Exception as e:
        logger.error(f"Failed to delete classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/entities/{guid}/classifications", response_model=ClassificationResult)
async def assign_classification(
    guid: str = Path(..., description="Entity GUID"),
    classification_name: str = Body(..., embed=True),
    attributes: Optional[Dict[str, Any]] = Body(None, embed=True),
    propagate: bool = Body(True, embed=True)
):
    """
    Assign a classification to an entity
    
    Classifications can include attributes and can propagate to related entities
    """
    try:
        # Get entity details
        entity = await atlas_client.get_entity_by_guid(guid)
        
        # Assign classification
        await classifier.assign_classification(
            entity_guid=guid,
            classification_name=classification_name,
            attributes=attributes,
            propagate=propagate
        )
        
        return ClassificationResult(
            entity_guid=guid,
            entity_name=entity["attributes"]["name"],
            classifications_added=[classification_name],
            classifications_removed=[],
            tags_added=[],
            tags_removed=[],
            status="success"
        )
        
    except Exception as e:
        logger.error(f"Failed to assign classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/entities/{guid}/classifications/{classification}")
async def remove_classification(
    guid: str = Path(..., description="Entity GUID"),
    classification: str = Path(..., description="Classification name")
):
    """Remove a classification from an entity"""
    try:
        await classifier.remove_classification(
            entity_guid=guid,
            classification_name=classification
        )
        
        return {
            "status": "success",
            "message": f"Classification {classification} removed from entity {guid}"
        }
        
    except Exception as e:
        logger.error(f"Failed to remove classification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/entities/{guid}/tags", response_model=ClassificationResult)
async def manage_tags(
    guid: str = Path(..., description="Entity GUID"),
    tag_assignment: TagAssignment = Body(...)
):
    """
    Manage tags for an entity
    
    Operations:
    - add: Add tags to existing tags
    - remove: Remove specific tags
    - set: Replace all tags
    """
    try:
        # Get entity details
        entity = await atlas_client.get_entity_by_guid(guid)
        current_tags = entity.get("labels", [])
        
        # Apply tag operation
        if tag_assignment.operation == "add":
            new_tags = list(set(current_tags + tag_assignment.tags))
        elif tag_assignment.operation == "remove":
            new_tags = [t for t in current_tags if t not in tag_assignment.tags]
        else:  # set
            new_tags = tag_assignment.tags
        
        # Update entity
        await atlas_client.update_entity_labels(guid, new_tags)
        
        # Calculate changes
        added = list(set(new_tags) - set(current_tags))
        removed = list(set(current_tags) - set(new_tags))
        
        return ClassificationResult(
            entity_guid=guid,
            entity_name=entity["attributes"]["name"],
            classifications_added=[],
            classifications_removed=[],
            tags_added=added,
            tags_removed=removed,
            status="success"
        )
        
    except Exception as e:
        logger.error(f"Failed to manage tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/classify/auto", response_model=Dict[str, Any])
async def auto_classify(request: AutoClassifyRequest):
    """
    Automatically classify entities based on content analysis
    
    Uses ML models and rules to detect:
    - PII (Personal Identifiable Information)
    - Financial data
    - Healthcare information
    - Custom classifications
    """
    try:
        # Run auto-classification
        results = await classifier.auto_classify(
            entity_guid=request.entity_guid,
            entity_type=request.entity_type,
            sample_size=request.sample_size,
            classifiers=request.classifiers,
            confidence_threshold=request.confidence_threshold,
            dry_run=request.dry_run
        )
        
        return {
            "entities_analyzed": results["entities_analyzed"],
            "classifications_detected": results["classifications_detected"],
            "dry_run": request.dry_run,
            "results": results["details"]
        }
        
    except Exception as e:
        logger.error(f"Failed to auto-classify: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/classify/rules", response_model=List[ClassificationRule])
async def list_classification_rules(
    classification: Optional[str] = Query(None, description="Filter by classification"),
    enabled_only: bool = Query(True, description="Only show enabled rules")
):
    """List all classification rules"""
    try:
        rules = await classifier.list_rules(
            classification=classification,
            enabled_only=enabled_only
        )
        
        return [
            ClassificationRule(**rule)
            for rule in rules
        ]
        
    except Exception as e:
        logger.error(f"Failed to list classification rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/classify/rules", response_model=Dict[str, Any])
async def create_classification_rule(rule: ClassificationRule):
    """Create a new classification rule"""
    try:
        result = await classifier.create_rule(rule.dict())
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to create classification rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/classify/scan")
async def scan_for_classifications(
    entity_type: Optional[str] = Query(None, description="Limit scan to entity type"),
    limit: int = Query(100, ge=1, le=1000, description="Max entities to scan"),
    async_mode: bool = Query(True, description="Run scan asynchronously")
):
    """
    Scan catalog for entities needing classification
    
    Identifies entities without classifications or with outdated classifications
    """
    try:
        if async_mode:
            # Start async scan
            scan_id = await classifier.start_scan(
                entity_type=entity_type,
                limit=limit
            )
            
            return {
                "status": "started",
                "scan_id": scan_id,
                "message": "Classification scan started in background"
            }
        else:
            # Run synchronous scan
            results = await classifier.scan_entities(
                entity_type=entity_type,
                limit=limit
            )
            
            return {
                "status": "completed",
                "entities_scanned": results["entities_scanned"],
                "classifications_added": results["classifications_added"],
                "errors": results["errors"]
            }
            
    except Exception as e:
        logger.error(f"Failed to scan for classifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/classify/scan/{scan_id}")
async def get_scan_status(scan_id: str = Path(..., description="Scan ID")):
    """Get status of a classification scan"""
    try:
        status = await classifier.get_scan_status(scan_id)
        
        return status
        
    except Exception as e:
        logger.error(f"Failed to get scan status: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 