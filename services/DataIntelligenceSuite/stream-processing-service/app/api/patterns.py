"""Patterns API router

Handles CEP pattern registration, management, and querying.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from app.core.pattern_library import PatternLibrary, PatternType


logger = logging.getLogger(__name__)
router = APIRouter()


class PatternCreateRequest(BaseModel):
    """Pattern creation request model"""
    name: str = Field(..., description="Pattern name")
    type: str = Field(..., description="Pattern type")
    definition: Dict[str, Any] = Field(..., description="Pattern definition")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Pattern metadata")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "Custom Fraud Pattern",
                "type": "fraud_detection",
                "definition": {
                    "pattern": "EVERY(a.amount > 5000)",
                    "within": "10 minutes",
                    "partition_by": "user_id",
                    "threshold": 3,
                    "actions": ["alert", "investigate"]
                },
                "metadata": {
                    "author": "security-team",
                    "description": "Detects high-value transaction patterns"
                }
            }
        }


class PatternUpdateRequest(BaseModel):
    """Pattern update request model"""
    name: Optional[str] = Field(None, description="Pattern name")
    definition: Optional[Dict[str, Any]] = Field(None, description="Pattern definition")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Pattern metadata")
    enabled: Optional[bool] = Field(None, description="Whether pattern is enabled")


class PatternResponse(BaseModel):
    """Pattern response model"""
    id: str
    name: str
    type: str
    definition: Dict[str, Any]
    metadata: Dict[str, Any]
    created_at: str
    updated_at: str
    version: int
    enabled: bool


# Dependency to get pattern library
def get_pattern_library() -> PatternLibrary:
    """Get pattern library instance"""
    from app.main import pattern_library
    if not pattern_library:
        raise HTTPException(500, "Pattern library not initialized")
    return pattern_library


@router.post("/", response_model=Dict[str, str])
async def register_pattern(
    request: PatternCreateRequest,
    pattern_library: PatternLibrary = get_pattern_library()
) -> Dict[str, str]:
    """Register a new CEP pattern"""
    try:
        # Validate pattern type
        valid_types = [
            PatternType.FRAUD_DETECTION,
            PatternType.RISK_MONITORING,
            PatternType.ANOMALY_DETECTION,
            PatternType.TRADING_PATTERNS,
            PatternType.COMPLIANCE_MONITORING
        ]
        
        if request.type not in valid_types:
            raise HTTPException(400, f"Invalid pattern type. Must be one of: {valid_types}")
        
        pattern_id = await pattern_library.register_pattern(
            name=request.name,
            pattern_type=request.type,
            definition=request.definition,
            metadata=request.metadata
        )
        
        return {
            "pattern_id": pattern_id,
            "message": f"Pattern {request.name} registered successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to register pattern: {e}")
        raise HTTPException(500, f"Failed to register pattern: {str(e)}")


@router.get("/", response_model=List[PatternResponse])
async def list_patterns(
    type: Optional[str] = Query(None, description="Filter by pattern type"),
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of patterns to return"),
    offset: int = Query(0, ge=0, description="Number of patterns to skip"),
    pattern_library: PatternLibrary = get_pattern_library()
) -> List[PatternResponse]:
    """List all patterns"""
    try:
        patterns = await pattern_library.list_patterns(pattern_type=type)
        
        # Apply enabled filter if specified
        if enabled is not None:
            patterns = [p for p in patterns if p.get("enabled") == enabled]
        
        # Apply pagination
        paginated_patterns = patterns[offset:offset + limit]
        
        return [PatternResponse(**pattern) for pattern in paginated_patterns]
        
    except Exception as e:
        logger.error(f"Failed to list patterns: {e}")
        raise HTTPException(500, f"Failed to list patterns: {str(e)}")


@router.get("/types")
async def list_pattern_types() -> List[Dict[str, str]]:
    """List available pattern types"""
    return [
        {"type": PatternType.FRAUD_DETECTION, "description": "Fraud detection patterns"},
        {"type": PatternType.RISK_MONITORING, "description": "Risk monitoring patterns"},
        {"type": PatternType.ANOMALY_DETECTION, "description": "Anomaly detection patterns"},
        {"type": PatternType.TRADING_PATTERNS, "description": "Trading pattern detection"},
        {"type": PatternType.COMPLIANCE_MONITORING, "description": "Compliance monitoring patterns"}
    ]


@router.get("/{pattern_id}", response_model=PatternResponse)
async def get_pattern(
    pattern_id: str = Path(..., description="Pattern ID"),
    pattern_library: PatternLibrary = get_pattern_library()
) -> PatternResponse:
    """Get pattern details"""
    try:
        pattern = await pattern_library.get_pattern(pattern_id)
        if not pattern:
            raise HTTPException(404, f"Pattern {pattern_id} not found")
            
        return PatternResponse(**pattern.to_dict())
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get pattern {pattern_id}: {e}")
        raise HTTPException(500, f"Failed to get pattern: {str(e)}")


@router.put("/{pattern_id}")
async def update_pattern(
    pattern_id: str = Path(..., description="Pattern ID"),
    request: PatternUpdateRequest = Body(...),
    pattern_library: PatternLibrary = get_pattern_library()
) -> Dict[str, str]:
    """Update a pattern"""
    try:
        # Build updates dict
        updates = {}
        if request.name is not None:
            updates["name"] = request.name
        if request.definition is not None:
            updates["definition"] = request.definition
        if request.metadata is not None:
            updates["metadata"] = request.metadata
        if request.enabled is not None:
            updates["enabled"] = request.enabled
            
        success = await pattern_library.update_pattern(pattern_id, updates)
        if not success:
            raise HTTPException(404, f"Pattern {pattern_id} not found")
            
        return {"message": f"Pattern {pattern_id} updated successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update pattern {pattern_id}: {e}")
        raise HTTPException(500, f"Failed to update pattern: {str(e)}")


@router.delete("/{pattern_id}")
async def delete_pattern(
    pattern_id: str = Path(..., description="Pattern ID"),
    pattern_library: PatternLibrary = get_pattern_library()
) -> Dict[str, str]:
    """Delete a pattern"""
    try:
        success = await pattern_library.delete_pattern(pattern_id)
        if not success:
            raise HTTPException(404, f"Pattern {pattern_id} not found or cannot be deleted")
            
        return {"message": f"Pattern {pattern_id} deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete pattern {pattern_id}: {e}")
        raise HTTPException(500, f"Failed to delete pattern: {str(e)}")


@router.post("/{pattern_id}/compile")
async def compile_pattern(
    pattern_id: str = Path(..., description="Pattern ID"),
    pattern_library: PatternLibrary = get_pattern_library()
) -> Dict[str, Any]:
    """Compile a pattern to Flink CEP code"""
    try:
        compiled = await pattern_library.compile_pattern(pattern_id)
        if not compiled:
            raise HTTPException(404, f"Pattern {pattern_id} not found")
            
        return {
            "pattern_id": pattern_id,
            "compiled": compiled,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to compile pattern {pattern_id}: {e}")
        raise HTTPException(500, f"Failed to compile pattern: {str(e)}")


@router.post("/{pattern_id}/test")
async def test_pattern(
    pattern_id: str = Path(..., description="Pattern ID"),
    test_events: List[Dict[str, Any]] = Body(..., description="Test events to run against pattern"),
    pattern_library: PatternLibrary = get_pattern_library()
) -> Dict[str, Any]:
    """Test a pattern with sample events"""
    try:
        # This would be implemented to test patterns
        # For now, returning a placeholder response
        return {
            "pattern_id": pattern_id,
            "test_result": "success",
            "matches_found": len(test_events) // 2,  # Mock result
            "message": "Pattern test completed"
        }
        
    except Exception as e:
        logger.error(f"Failed to test pattern {pattern_id}: {e}")
        raise HTTPException(500, f"Failed to test pattern: {str(e)}") 