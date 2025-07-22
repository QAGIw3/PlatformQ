"""
Intelligent Data Mesh API endpoints
"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Request, Query, Body
from pydantic import BaseModel, Field
from datetime import datetime
import structlog

from app.mesh.intelligent_data_mesh import DataProduct, DataProductType, AccessPattern

logger = structlog.get_logger()
router = APIRouter()


class DataProductRequest(BaseModel):
    """Request to register a data product"""
    name: str = Field(..., description="Product name")
    type: DataProductType = Field(..., description="Product type")
    owner: str = Field(..., description="Product owner")
    location: str = Field(..., description="Data location")
    schema: Dict[str, Any] = Field(..., description="Data schema")
    quality_score: float = Field(0.8, ge=0, le=1, description="Quality score")
    dependencies: List[str] = Field(default_factory=list, description="Product dependencies")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


class PredictionRequest(BaseModel):
    """Request for data need predictions"""
    time_horizon: str = Field("24h", description="Prediction time horizon")
    confidence_threshold: float = Field(0.8, ge=0, le=1, description="Minimum confidence")
    limit: int = Field(50, ge=1, le=200, description="Maximum predictions")


@router.post("/products")
async def register_data_product(
    request: Request,
    product_request: DataProductRequest
) -> Dict[str, Any]:
    """Register a new data product in the mesh"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Create data product
        product = DataProduct(
            product_id=f"dp_{product_request.name}_{datetime.utcnow().timestamp()}",
            name=product_request.name,
            type=product_request.type,
            owner=product_request.owner,
            location=product_request.location,
            schema=product_request.schema,
            quality_score=product_request.quality_score,
            access_patterns=[],
            dependencies=product_request.dependencies,
            metadata=product_request.metadata
        )
        
        # Register in mesh
        product_id = await mesh.register_data_product(product)
        
        return {
            "product_id": product_id,
            "status": "registered",
            "message": f"Data product {product_request.name} registered successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to register data product: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products")
async def list_data_products(
    request: Request,
    product_type: Optional[DataProductType] = Query(None, description="Filter by type"),
    owner: Optional[str] = Query(None, description="Filter by owner"),
    limit: int = Query(100, ge=1, le=1000)
) -> Dict[str, Any]:
    """List data products in the mesh"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Get all products
        all_products = list(mesh.data_products.values())
        
        # Apply filters
        filtered = all_products
        if product_type:
            filtered = [p for p in filtered if p.type == product_type]
        if owner:
            filtered = [p for p in filtered if p.owner == owner]
            
        # Limit results
        filtered = filtered[:limit]
        
        return {
            "products": [
                {
                    "product_id": p.product_id,
                    "name": p.name,
                    "type": p.type.value,
                    "owner": p.owner,
                    "quality_score": p.quality_score,
                    "dependencies": p.dependencies
                }
                for p in filtered
            ],
            "total_count": len(all_products),
            "filtered_count": len(filtered)
        }
        
    except Exception as e:
        logger.error(f"Failed to list data products: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/products/{product_id}")
async def get_data_product(
    request: Request,
    product_id: str
) -> Dict[str, Any]:
    """Get detailed information about a data product"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        product = mesh.data_products.get(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Data product not found")
            
        # Get placement information
        placements = mesh.current_placements.get(product_id, ["lake"])
        
        return {
            "product_id": product.product_id,
            "name": product.name,
            "type": product.type.value,
            "owner": product.owner,
            "location": product.location,
            "schema": product.schema,
            "quality_score": product.quality_score,
            "access_patterns": product.access_patterns,
            "dependencies": product.dependencies,
            "current_placements": placements,
            "metadata": product.metadata
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get data product: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predict-needs")
async def predict_data_needs(
    request: Request,
    prediction_request: PredictionRequest
) -> Dict[str, Any]:
    """Predict future data access needs"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Get predictions
        predictions = await mesh.predict_data_needs(
            time_horizon=prediction_request.time_horizon,
            confidence_threshold=prediction_request.confidence_threshold
        )
        
        # Limit results
        predictions = predictions[:prediction_request.limit]
        
        return {
            "predictions": predictions,
            "time_horizon": prediction_request.time_horizon,
            "prediction_count": len(predictions)
        }
        
    except Exception as e:
        logger.error(f"Failed to predict data needs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/access-patterns")
async def get_access_patterns(
    request: Request,
    product_id: Optional[str] = Query(None, description="Filter by product")
) -> Dict[str, Any]:
    """Get learned access patterns"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Get patterns
        patterns = mesh.access_patterns
        
        # Filter if requested
        if product_id:
            patterns = {
                k: v for k, v in patterns.items()
                if product_id in v.data_products
            }
            
        return {
            "patterns": [
                {
                    "pattern_id": p.pattern_id,
                    "data_products": p.data_products,
                    "frequency": p.frequency,
                    "latency_requirement": p.latency_requirement,
                    "user_segments": p.user_segments,
                    "time_patterns": p.time_patterns
                }
                for p in patterns.values()
            ],
            "pattern_count": len(patterns)
        }
        
    except Exception as e:
        logger.error(f"Failed to get access patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize-placement")
async def optimize_data_placement(
    request: Request,
    product_ids: List[str] = Body(..., description="Products to optimize")
) -> Dict[str, Any]:
    """Manually trigger placement optimization"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Get predictions for specified products
        predictions = []
        for product_id in product_ids:
            if product_id in mesh.data_products:
                predictions.append({
                    "product_id": product_id,
                    "predicted_accesses": 100,  # Would use actual prediction
                    "confidence": 0.9,
                    "recommended_action": "optimize"
                })
                
        # Optimize placement
        await mesh.optimize_data_placement(predictions)
        
        # Get new placements
        optimized_placements = {
            pid: mesh.current_placements.get(pid, ["lake"])
            for pid in product_ids
        }
        
        return {
            "optimized_products": len(product_ids),
            "placements": optimized_placements
        }
        
    except Exception as e:
        logger.error(f"Failed to optimize placement: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def query_with_optimization(
    request: Request,
    query: str = Body(..., description="Query to execute"),
    user_context: Dict[str, Any] = Body(default_factory=dict, description="User context")
) -> Dict[str, Any]:
    """Execute query with intelligent optimization"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Execute with optimization
        start_time = datetime.utcnow()
        result = await mesh.query_with_optimization(query, user_context)
        execution_time = (datetime.utcnow() - start_time).total_seconds()
        
        return {
            "result": result,
            "execution_time": execution_time,
            "optimization_applied": True
        }
        
    except Exception as e:
        logger.error(f"Optimized query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_mesh_statistics(
    request: Request
) -> Dict[str, Any]:
    """Get data mesh statistics"""
    try:
        mesh = request.app.state.intelligent_mesh
        
        # Calculate statistics
        total_products = len(mesh.data_products)
        products_by_type = {}
        for product in mesh.data_products.values():
            type_key = product.type.value
            products_by_type[type_key] = products_by_type.get(type_key, 0) + 1
            
        # Access statistics
        total_accesses = len(mesh.access_history)
        patterns_learned = len(mesh.access_patterns)
        
        # Placement statistics
        placement_stats = {}
        for placements in mesh.current_placements.values():
            for placement in placements:
                placement_stats[placement] = placement_stats.get(placement, 0) + 1
                
        return {
            "total_products": total_products,
            "products_by_type": products_by_type,
            "total_accesses_tracked": total_accesses,
            "patterns_learned": patterns_learned,
            "placement_distribution": placement_stats,
            "mesh_status": "active" if mesh._running else "stopped"
        }
        
    except Exception as e:
        logger.error(f"Failed to get mesh statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 