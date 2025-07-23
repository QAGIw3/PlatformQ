"""
Data Lake API endpoints for medallion architecture
"""

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from app.core.medallion_architecture import MedallionArchitectureManager, DataLayer
from app.core.lifecycle_manager import DataLifecycleManager, TieringPolicy, StorageTier
from app.api.dependencies import get_current_user, get_medallion_manager, get_lifecycle_manager, get_lakehouse_manager
from app.models.api import (
    StandardResponse,
    DataIngestionRequest,
    TransformationRequest,
    AggregationRequest,
    TieringPolicyRequest,
    CostReportResponse
)
from data_intelligence_common.core.lakehouse import (
    LakehouseManager,
    TableDefinition,
    TableSchema,
    PartitionSpec,
    DataType
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


# ================ New Lakehouse Endpoints ================

@router.post("/tables/create")
async def create_lakehouse_table(
    table_name: str,
    schema: Dict[str, str],
    format: str = "iceberg",
    partition_by: Optional[List[str]] = None,
    properties: Optional[Dict[str, str]] = None,
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Create a new table in the lakehouse"""
    try:
        # Convert schema dict to TableSchema
        columns = []
        for name, dtype in schema.items():
            columns.append({
                "name": name,
                "type": DataType[dtype.upper()],
                "nullable": True
            })
        
        table_schema = TableSchema(columns=columns)
        
        # Create partition spec if provided
        partition_spec = None
        if partition_by:
            partitions = []
            for col in partition_by:
                partitions.append({
                    "source_column": col,
                    "transform": "identity",
                    "name": f"{col}_partition"
                })
            partition_spec = PartitionSpec(partitions=partitions)
        
        # Create table definition
        table_def = TableDefinition(
            name=table_name,
            schema=table_schema,
            partition_spec=partition_spec,
            properties=properties or {}
        )
        
        # Create table
        await lakehouse_manager.create_table(table_def, format=format)
        
        return StandardResponse(
            success=True,
            message=f"Table {table_name} created successfully in {format} format",
            data={"table_name": table_name, "format": format}
        )
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/{table_name}")
async def get_table_info(
    table_name: str,
    format: str = "iceberg",
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager)
) -> StandardResponse:
    """Get information about a lakehouse table"""
    try:
        table_info = await lakehouse_manager.get_table(table_name, format=format)
        
        return StandardResponse(
            success=True,
            message=f"Table info retrieved",
            data=table_info
        )
    except Exception as e:
        logger.error(f"Error getting table info: {e}")
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/tables/{table_name}/write")
async def write_to_table(
    table_name: str,
    data: List[Dict[str, Any]],
    format: str = "iceberg",
    mode: str = "append",
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager),
    user: Dict = Depends(get_current_user)
) -> StandardResponse:
    """Write data to a lakehouse table"""
    try:
        # Convert data to DataFrame
        import pandas as pd
        df = pd.DataFrame(data)
        
        # Write to table
        await lakehouse_manager.write_data(
            table_name=table_name,
            data=df,
            format=format,
            mode=mode
        )
        
        return StandardResponse(
            success=True,
            message=f"Data written to table {table_name}",
            data={"rows_written": len(data), "mode": mode}
        )
    except Exception as e:
        logger.error(f"Error writing to table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/{table_name}/read")
async def read_from_table(
    table_name: str,
    format: str = "iceberg",
    filters: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager)
) -> StandardResponse:
    """Read data from a lakehouse table"""
    try:
        df = await lakehouse_manager.read_data(
            table_name=table_name,
            format=format,
            filters=filters,
            columns=columns
        )
        
        # Apply limit if specified
        if limit:
            df = df.head(limit)
        
        # Convert to dict for response
        data = df.to_dict(orient="records")
        
        return StandardResponse(
            success=True,
            message=f"Data read from table {table_name}",
            data={
                "rows": data,
                "count": len(data),
                "total_count": len(df) if not limit else None
            }
        )
    except Exception as e:
        logger.error(f"Error reading from table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tables/{table_name}/time-travel")
async def time_travel_query(
    table_name: str,
    timestamp: Optional[datetime] = None,
    version: Optional[int] = None,
    format: str = "iceberg",
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager)
) -> StandardResponse:
    """Query historical data using time travel"""
    try:
        if format not in ["iceberg", "delta"]:
            raise HTTPException(
                status_code=400,
                detail="Time travel only supported for Iceberg and Delta formats"
            )
        
        # Get the appropriate client
        if format == "iceberg":
            client = lakehouse_manager.iceberg_client
            df = await client.read_table(
                table_name,
                snapshot_id=version,
                as_of_timestamp=timestamp
            )
        else:  # delta
            client = lakehouse_manager.delta_client
            df = await client.read_table(
                table_name,
                version=version,
                timestamp=timestamp
            )
        
        data = df.head(100).to_dict(orient="records")
        
        return StandardResponse(
            success=True,
            message=f"Time travel query executed",
            data={
                "rows": data,
                "count": len(data),
                "query_time": timestamp.isoformat() if timestamp else f"version_{version}"
            }
        )
    except Exception as e:
        logger.error(f"Error in time travel query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tables/{table_name}/optimize")
async def optimize_table(
    table_name: str,
    format: str = "iceberg",
    options: Optional[Dict[str, Any]] = None,
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager),
    background_tasks: BackgroundTasks = BackgroundTasks()
) -> StandardResponse:
    """Optimize a lakehouse table (compaction, sorting, etc.)"""
    try:
        # Schedule optimization in background
        background_tasks.add_task(
            lakehouse_manager.optimize_table,
            table_name,
            format,
            options or {}
        )
        
        return StandardResponse(
            success=True,
            message=f"Table optimization scheduled for {table_name}",
            data={"table_name": table_name, "format": format}
        )
    except Exception as e:
        logger.error(f"Error scheduling optimization: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/formats")
async def get_supported_formats(
    lakehouse_manager: LakehouseManager = Depends(get_lakehouse_manager)
) -> StandardResponse:
    """Get supported lakehouse formats"""
    try:
        formats = []
        
        if lakehouse_manager.iceberg_client:
            formats.append({
                "format": "iceberg",
                "features": ["ACID", "time-travel", "schema-evolution", "partition-evolution"],
                "status": "available"
            })
        
        if lakehouse_manager.delta_client:
            formats.append({
                "format": "delta",
                "features": ["ACID", "time-travel", "schema-evolution", "z-ordering"],
                "status": "available"
            })
        
        return StandardResponse(
            success=True,
            message="Supported formats retrieved",
            data={"formats": formats}
        )
    except Exception as e:
        logger.error(f"Error getting formats: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 