"""
Data Lake API endpoints for medallion architecture
"""

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from app.core.medallion_architecture import MedallionArchitectureManager, DataLayer
from app.core.lifecycle_manager import DataLifecycleManager, TieringPolicy, StorageTier
from app.api.dependencies import get_current_user, get_medallion_manager, get_lifecycle_manager
from app.models.api import (
    StandardResponse,
    DataIngestionRequest,
    TransformationRequest,
    AggregationRequest,
    TieringPolicyRequest,
    CostReportResponse
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/lake", tags=["data-lake"])


@router.post("/ingest/bronze")
async def ingest_to_bronze(
    request: DataIngestionRequest,
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
) -> StandardResponse:
    """Ingest data to bronze layer"""
    try:
        result = await medallion_manager.ingest_to_bronze(
            data=request.data,
            dataset_name=request.dataset_name,
            source_info={
                "type": request.source_type,
                "connection": request.connection_info,
                "user": user["username"],
                "timestamp": datetime.utcnow()
            },
            format=request.format or "parquet"
        )
        
        # Schedule lifecycle check in background
        background_tasks.add_task(
            medallion_manager.apply_lifecycle_policies
        )
        
        return StandardResponse(
            success=True,
            message="Data ingested to bronze layer",
            data=result
        )
    except Exception as e:
        logger.error(f"Error ingesting to bronze: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transform/bronze-to-silver")
async def transform_bronze_to_silver(
    request: TransformationRequest,
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Transform data from bronze to silver layer"""
    try:
        result = await medallion_manager.transform_bronze_to_silver(
            dataset_name=request.dataset_name,
            transformations=request.transformations,
            quality_rules=request.quality_rules
        )
        
        return StandardResponse(
            success=True,
            message="Data transformed to silver layer",
            data=result
        )
    except Exception as e:
        logger.error(f"Error transforming to silver: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/aggregate/silver-to-gold")
async def aggregate_silver_to_gold(
    request: AggregationRequest,
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Aggregate data from silver to gold layer"""
    try:
        result = await medallion_manager.aggregate_silver_to_gold(
            dataset_name=request.dataset_name,
            aggregations=request.aggregations,
            business_rules=request.business_rules
        )
        
        return StandardResponse(
            success=True,
            message="Data aggregated to gold layer",
            data=result
        )
    except Exception as e:
        logger.error(f"Error aggregating to gold: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/layers/{dataset_name}")
async def get_dataset_layers(
    dataset_name: str,
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Get information about dataset across all layers"""
    try:
        layers_info = {}
        
        for layer in DataLayer:
            # Check if data exists in layer
            bucket = medallion_manager.layer_buckets[layer]
            prefix = f"{dataset_name}/"
            
            objects = list(medallion_manager.minio_client.list_objects(
                bucket,
                prefix=prefix,
                recursive=True
            ))
            
            if objects:
                layers_info[layer.value] = {
                    "exists": True,
                    "object_count": len(objects),
                    "total_size": sum(obj.size for obj in objects),
                    "last_modified": max(obj.last_modified for obj in objects)
                }
            else:
                layers_info[layer.value] = {
                    "exists": False
                }
        
        return StandardResponse(
            success=True,
            message=f"Layer information for dataset {dataset_name}",
            data=layers_info
        )
    except Exception as e:
        logger.error(f"Error getting layer info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/lifecycle/policy")
async def apply_tiering_policy(
    request: TieringPolicyRequest,
    lifecycle_manager: DataLifecycleManager = Depends(get_lifecycle_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Apply tiering policy to a dataset"""
    try:
        # Create custom policy if provided
        custom_policy = None
        if request.custom_policy:
            custom_policy = TieringPolicy(
                hot_duration_days=request.custom_policy.hot_days,
                warm_duration_days=request.custom_policy.warm_days,
                cold_duration_days=request.custom_policy.cold_days,
                delete_after_days=request.custom_policy.delete_after_days
            )
        
        result = await lifecycle_manager.apply_tiering_policy(
            data_type=request.data_type,
            dataset_name=request.dataset_name,
            custom_policy=custom_policy
        )
        
        return StandardResponse(
            success=True,
            message="Tiering policy applied",
            data=result
        )
    except Exception as e:
        logger.error(f"Error applying tiering policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lifecycle/cost-report")
async def get_cost_report(
    dataset_name: Optional[str] = Query(None, description="Dataset name for specific report"),
    lifecycle_manager: DataLifecycleManager = Depends(get_lifecycle_manager),
    user: Dict = Depends(get_current_user)
) -> CostReportResponse:
    """Generate storage cost report"""
    try:
        report = await lifecycle_manager.generate_cost_report(dataset_name)
        
        return CostReportResponse(
            success=True,
            message="Cost report generated",
            report=report
        )
    except Exception as e:
        logger.error(f"Error generating cost report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/optimize/{layer}/{dataset_name}")
async def optimize_storage(
    layer: DataLayer,
    dataset_name: str,
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
) -> StandardResponse:
    """Optimize storage for a dataset in a specific layer"""
    try:
        # Run optimization in background
        background_tasks.add_task(
            medallion_manager.optimize_storage,
            layer,
            dataset_name
        )
        
        return StandardResponse(
            success=True,
            message=f"Storage optimization started for {dataset_name} in {layer.value} layer",
            data={"task": "running"}
        )
    except Exception as e:
        logger.error(f"Error starting optimization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tiers/{dataset_name}")
async def get_data_distribution(
    dataset_name: str,
    lifecycle_manager: DataLifecycleManager = Depends(get_lifecycle_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Get data distribution across storage tiers"""
    try:
        distribution = await lifecycle_manager._get_data_distribution(dataset_name)
        
        # Format response
        tier_info = {}
        for tier, items in distribution.items():
            tier_info[tier.value] = {
                "item_count": len(items),
                "total_size_gb": sum(item.get("size_bytes", 0) for item in items) / (1024 ** 3),
                "oldest_item": min((item["timestamp"] for item in items), default=None),
                "newest_item": max((item["timestamp"] for item in items), default=None)
            }
        
        return StandardResponse(
            success=True,
            message=f"Data distribution for {dataset_name}",
            data=tier_info
        )
    except Exception as e:
        logger.error(f"Error getting data distribution: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload/bronze")
async def upload_to_bronze(
    file: UploadFile = File(...),
    dataset_name: str = Query(..., description="Dataset name"),
    medallion_manager: MedallionArchitectureManager = Depends(get_medallion_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Upload file directly to bronze layer"""
    try:
        # Read file content
        content = await file.read()
        
        # Determine format from filename
        format = file.filename.split('.')[-1].lower()
        if format not in ["csv", "json", "parquet", "avro"]:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {format}")
        
        # Process based on format
        import pandas as pd
        import io
        
        if format == "csv":
            data = pd.read_csv(io.BytesIO(content))
        elif format == "json":
            import json
            data = json.loads(content.decode('utf-8'))
        elif format == "parquet":
            data = pd.read_parquet(io.BytesIO(content))
        else:
            # For other formats, store as-is
            data = content
        
        # Ingest to bronze
        result = await medallion_manager.ingest_to_bronze(
            data=data,
            dataset_name=dataset_name,
            source_info={
                "type": "file_upload",
                "filename": file.filename,
                "user": user["username"],
                "timestamp": datetime.utcnow()
            },
            format=format
        )
        
        return StandardResponse(
            success=True,
            message=f"File uploaded to bronze layer",
            data=result
        )
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 