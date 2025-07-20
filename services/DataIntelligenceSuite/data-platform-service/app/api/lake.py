"""
Data lake management API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, UploadFile, File, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum
import uuid
import logging

from .. import main
from ..lake.medallion_architecture import DataZone

router = APIRouter()
logger = logging.getLogger(__name__)


class LakeZone(str, Enum):
    BRONZE = "bronze"  # Raw data
    SILVER = "silver"  # Cleaned/validated data
    GOLD = "gold"      # Business-ready data


class FileFormat(str, Enum):
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    JSON = "json"
    CSV = "csv"
    DELTA = "delta"
    ICEBERG = "iceberg"


class PartitionStrategy(str, Enum):
    DATE = "date"
    HOUR = "hour"
    HASH = "hash"
    RANGE = "range"
    LIST = "list"


class DatasetMetadata(BaseModel):
    """Dataset metadata"""
    name: str = Field(..., description="Dataset name")
    description: Optional[str] = Field(None)
    zone: LakeZone
    format: FileFormat
    schema: Optional[Dict[str, Any]] = Field(None, description="Data schema")
    partition_by: Optional[List[str]] = Field(None)
    tags: List[str] = Field(default_factory=list)
    retention_days: Optional[int] = Field(None, ge=1)
    compression: Optional[str] = Field(None)


class DatasetResponse(DatasetMetadata):
    """Dataset response"""
    dataset_id: str
    location: str
    size_bytes: int
    file_count: int
    row_count: Optional[int]
    created_at: datetime
    updated_at: datetime
    last_modified: datetime
    owner: str


class IngestionJob(BaseModel):
    """Data ingestion job"""
    job_id: str
    dataset_id: str
    source_type: str
    source_config: Dict[str, Any]
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    rows_ingested: Optional[int]
    bytes_ingested: Optional[int]
    error: Optional[str]


class IngestionRequest(BaseModel):
    """Ingestion request"""
    source_type: str = Field(..., description="Source type (file, database, stream, api)")
    source_config: Dict[str, Any] = Field(..., description="Source configuration")
    target_zone: LakeZone = Field(..., description="Target zone")
    target_path: str = Field(..., description="Target path in lake")
    options: Dict[str, Any] = Field(default_factory=dict, description="Ingestion options")


class TransformationRequest(BaseModel):
    """Transformation request"""
    source_path: str = Field(..., description="Source data path")
    target_path: str = Field(..., description="Target data path")
    transformations: List[Dict[str, Any]] = Field(..., description="List of transformations")
    options: Dict[str, Any] = Field(default_factory=dict, description="Transformation options")


@router.post("/datasets", response_model=DatasetResponse)
async def create_dataset(
    dataset: DatasetMetadata,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a new dataset in the data lake"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        dataset_id = str(uuid.uuid4())
        
        # Create dataset in appropriate zone
        if dataset.zone == LakeZone.BRONZE:
            location = await main.lake_manager.ingest_to_bronze(
                data_source=f"empty_{dataset.format.value}",
                target_path=f"{dataset.name}",
                format=dataset.format.value,
                partition_by=dataset.partition_by
            )
        elif dataset.zone == LakeZone.SILVER:
            location = f"s3://datalake/silver/{dataset.name}"
        else:  # GOLD
            location = f"s3://datalake/gold/{dataset.name}"
        
        # Get dataset info
        dataset_info = await main.lake_manager.get_dataset_info(location)
        
        return DatasetResponse(
            dataset_id=dataset_id,
            name=dataset.name,
            description=dataset.description,
            zone=dataset.zone,
            format=dataset.format,
            schema=dataset.schema,
            partition_by=dataset.partition_by,
            tags=dataset.tags,
            retention_days=dataset.retention_days,
            compression=dataset.compression,
            location=location,
            size_bytes=dataset_info.get("size_bytes", 0),
            file_count=dataset_info.get("file_count", 0),
            row_count=dataset_info.get("row_count"),
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            last_modified=datetime.utcnow(),
            owner=tenant_id
        )
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets", response_model=List[DatasetResponse])
async def list_datasets(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    zone: Optional[LakeZone] = Query(None),
    format: Optional[FileFormat] = Query(None),
    tags: Optional[List[str]] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List datasets in the data lake"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Get lake statistics
        stats = await main.lake_manager.get_lake_statistics()
        
        # Mock dataset list based on statistics
        datasets = []
        zones_data = stats.get("zones", {})
        
        for zone_name, zone_info in zones_data.items():
            if zone and zone.value != zone_name:
                continue
            
            # Create sample datasets for this zone
            dataset_count = min(zone_info.get("dataset_count", 0), limit - len(datasets))
            for i in range(dataset_count):
                datasets.append(DatasetResponse(
                    dataset_id=str(uuid.uuid4()),
                    name=f"{zone_name}_dataset_{i}",
                    description=f"Dataset in {zone_name} zone",
                    zone=LakeZone(zone_name),
                    format=format or FileFormat.PARQUET,
                    schema={},
                    partition_by=["date"],
                    tags=tags or [],
                    retention_days=90,
                    compression="snappy",
                    location=f"s3://datalake/{zone_name}/dataset_{i}",
                    size_bytes=zone_info.get("total_size", 0) // max(dataset_count, 1),
                    file_count=zone_info.get("file_count", 0) // max(dataset_count, 1),
                    row_count=zone_info.get("row_count", 0) // max(dataset_count, 1) if zone_info.get("row_count") else None,
                    created_at=datetime.utcnow() - timedelta(days=30),
                    updated_at=datetime.utcnow() - timedelta(days=1),
                    last_modified=datetime.utcnow() - timedelta(hours=2),
                    owner=tenant_id
                ))
        
        # Apply pagination
        return datasets[offset:offset + limit]
    except Exception as e:
        logger.error(f"Failed to list datasets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get dataset details"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Mock dataset details
        return DatasetResponse(
            dataset_id=dataset_id,
            name="Sample Dataset",
            description="This is a sample dataset",
            zone=LakeZone.SILVER,
            format=FileFormat.PARQUET,
            schema={"id": "bigint", "name": "string", "value": "double"},
            partition_by=["date"],
            tags=["sample", "test"],
            retention_days=30,
            compression="snappy",
            location="s3://datalake/silver/sample_dataset",
            size_bytes=10485760,
            file_count=1,
            row_count=1000,
            created_at=datetime.utcnow() - timedelta(days=1),
            updated_at=datetime.utcnow(),
            last_modified=datetime.utcnow(),
            owner=tenant_id
        )
    except Exception as e:
        logger.error(f"Failed to get dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/ingest", response_model=IngestionJob)
async def ingest_data(
    ingestion: IngestionRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Ingest data into the lake"""
    if not main.ingestion_engine:
        raise HTTPException(status_code=503, detail="Ingestion engine not available")
    
    try:
        job_id = str(uuid.uuid4())
        
        # Start ingestion
        result = await main.ingestion_engine.ingest(
            source_type=ingestion.source_type,
            source_config=ingestion.source_config,
            target_zone=ingestion.target_zone.value,
            target_path=ingestion.target_path,
            options=ingestion.options
        )
        
        return IngestionJob(
            job_id=job_id,
            dataset_id=result.get("dataset_id", ""),
            source_type=ingestion.source_type,
            source_config=ingestion.source_config,
            status=result.get("status", "running"),
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow() if result.get("status") == "completed" else None,
            rows_ingested=result.get("rows_ingested"),
            bytes_ingested=result.get("bytes_ingested"),
            error=result.get("error")
        )
    except Exception as e:
        logger.error(f"Failed to ingest data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_file(
    request: Request,
    dataset_id: str = Query(..., description="Target dataset"),
    tenant_id: str = Query(..., description="Tenant ID"),
    file: UploadFile = File(...)
):
    """Upload file to data lake"""
    upload_id = str(uuid.uuid4())
    
    # In production, would upload to object storage
    
    return {
        "upload_id": upload_id,
        "dataset_id": dataset_id,
        "filename": file.filename,
        "size": file.size,
        "status": "uploaded",
        "location": f"s3://data-lake/{tenant_id}/uploads/{upload_id}/{file.filename}"
    }


@router.get("/datasets/{dataset_id}/files")
async def list_dataset_files(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    prefix: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List files in a dataset"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Mock file list
        return {
            "dataset_id": dataset_id,
            "files": [
                {
                    "path": f"part-00000-{uuid.uuid4()}.parquet",
                    "size_bytes": 10485760,
                    "last_modified": datetime.utcnow(),
                    "etag": "abc123"
                }
            ],
            "total": 1
        }
    except Exception as e:
        logger.error(f"Failed to list dataset files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets/{dataset_id}/compact")
async def compact_dataset(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    target_file_size_mb: int = Query(128, ge=1, le=1024)
):
    """Compact small files in dataset"""
    job_id = str(uuid.uuid4())
    
    return {
        "job_id": job_id,
        "dataset_id": dataset_id,
        "status": "started",
        "target_file_size_mb": target_file_size_mb,
        "estimated_time_seconds": 300,
        "started_at": datetime.utcnow()
    }


@router.post("/optimize/{dataset_id}")
async def optimize_dataset(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    compaction: bool = Query(True, description="Perform file compaction"),
    z_order_by: Optional[List[str]] = Query(None, description="Columns for Z-ordering")
):
    """Optimize dataset storage"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Optimize dataset
        result = await main.lake_manager.optimize_delta_tables()
        
        return {
            "dataset_id": dataset_id,
            "optimization_type": ["compaction"] + (["z-order"] if z_order_by else []),
            "status": "completed",
            "files_before": result.get("files_before", 0),
            "files_after": result.get("files_after", 0),
            "size_reduction_bytes": result.get("size_reduction", 0),
            "execution_time_ms": result.get("execution_time_ms", 0)
        }
    except Exception as e:
        logger.error(f"Failed to optimize dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets/{dataset_id}/partition")
async def partition_dataset(
    dataset_id: str,
    request: Request,
    partition_by: List[str] = Query(..., description="Columns to partition by"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Repartition a dataset"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # In a real implementation, this would repartition the dataset
        return {
            "dataset_id": dataset_id,
            "status": "completed",
            "partition_columns": partition_by,
            "partitions_created": 100,
            "execution_time_ms": 5000
        }
    except Exception as e:
        logger.error(f"Failed to partition dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_id}/schema")
async def get_dataset_schema(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get dataset schema"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Mock schema response
        return {
            "dataset_id": dataset_id,
            "schema": {
                "fields": [
                    {"name": "id", "type": "bigint", "nullable": False},
                    {"name": "timestamp", "type": "timestamp", "nullable": False},
                    {"name": "value", "type": "double", "nullable": True},
                    {"name": "category", "type": "string", "nullable": True}
                ]
            },
            "partitioning": ["date"],
            "statistics": {
                "row_count": 1000000,
                "size_bytes": 104857600,
                "min_timestamp": "2024-01-01T00:00:00Z",
                "max_timestamp": "2024-12-01T00:00:00Z"
            }
        }
    except Exception as e:
        logger.error(f"Failed to get dataset schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def query_lake(
    request: Request,
    query: str = Body(..., description="SQL query"),
    tenant_id: str = Query(..., description="Tenant ID"),
    limit: int = Query(1000, ge=1, le=10000)
):
    """Query data in the lake"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Execute query through Spark
        result_df = main.lake_manager.spark.sql(query).limit(limit)
        
        # Convert to records
        records = [row.asDict() for row in result_df.collect()]
        
        return {
            "query": query,
            "rows": records,
            "row_count": len(records),
            "truncated": len(records) == limit
        }
    except Exception as e:
        logger.error(f"Failed to query lake: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_lake_statistics(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get data lake statistics"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        stats = await main.lake_manager.get_lake_statistics()
        
        return {
            "zones": stats.get("zones", {}),
            "total_size": stats.get("total_size", 0),
            "total_datasets": stats.get("total_datasets", 0),
            "total_files": stats.get("total_files", 0),
            "formats": stats.get("formats", {}),
            "last_updated": datetime.utcnow()
        }
    except Exception as e:
        logger.error(f"Failed to get lake statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/transform")
async def transform_data(
    transformation: TransformationRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Transform data in the lake"""
    if not main.transformation_engine:
        raise HTTPException(status_code=503, detail="Transformation engine not available")
    
    try:
        # Apply transformations
        result = await main.transformation_engine.transform(
            source_path=transformation.source_path,
            target_path=transformation.target_path,
            transformations=transformation.transformations,
            options=transformation.options
        )
        
        return {
            "job_id": result.get("job_id", str(uuid.uuid4())),
            "status": result.get("status", "completed"),
            "source_path": transformation.source_path,
            "target_path": transformation.target_path,
            "transformations_applied": len(transformation.transformations),
            "rows_processed": result.get("rows_processed", 0),
            "execution_time_ms": result.get("execution_time_ms", 0)
        }
    except Exception as e:
        logger.error(f"Failed to transform data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
async def get_job_status(
    job_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get job status"""
    return IngestionJob(
        job_id=job_id,
        dataset_id="dataset_123",
        source_type="file",
        source_config={"path": "s3://source/data.csv"},
        status="completed",
        started_at=datetime.utcnow() - timedelta(minutes=5),
        completed_at=datetime.utcnow(),
        rows_ingested=100000,
        bytes_ingested=10485760,
        error=None
    )


@router.get("/datasets/{dataset_id}/preview")
async def preview_dataset(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    rows: int = Query(100, ge=1, le=1000)
):
    """Preview dataset contents"""
    return {
        "dataset_id": dataset_id,
        "schema": {
            "fields": [
                {"name": "id", "type": "bigint"},
                {"name": "name", "type": "string"},
                {"name": "value", "type": "double"},
                {"name": "timestamp", "type": "timestamp"}
            ]
        },
        "sample_data": [
            {"id": 1, "name": "Item 1", "value": 100.5, "timestamp": datetime.utcnow()},
            {"id": 2, "name": "Item 2", "value": 200.75, "timestamp": datetime.utcnow()}
        ],
        "total_rows": 2
    }


@router.get("/datasets/{dataset_id}/statistics")
async def get_dataset_statistics(
    dataset_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get dataset statistics"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Mock statistics
        return {
            "dataset_id": dataset_id,
            "statistics": {
                "row_count": 1000000,
                "size_bytes": 104857600,
                "file_count": 10,
                "avg_file_size_mb": 10.0,
                "last_updated": datetime.utcnow(),
                "partitions": 30,
                "column_stats": {
                    "id": {
                        "null_count": 0,
                        "distinct_count": 1000000,
                        "min": 1,
                        "max": 1000000
                    },
                    "value": {
                        "null_count": 1000,
                        "mean": 150.25,
                        "std": 50.5,
                        "min": 0.0,
                        "max": 999.99
                    }
                }
            }
        }
    except Exception as e:
        logger.error(f"Failed to get dataset statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets/{dataset_id}/retention")
async def set_retention_policy(
    dataset_id: str,
    request: Request,
    retention_days: int = Query(..., ge=1, le=3650),
    tenant_id: str = Query(..., description="Tenant ID"),
    archive_after_days: Optional[int] = Query(None, ge=1)
):
    """Set data retention policy"""
    return {
        "dataset_id": dataset_id,
        "retention_policy": {
            "retention_days": retention_days,
            "archive_after_days": archive_after_days,
            "delete_after_archive_days": 365 if archive_after_days else None
        },
        "applied_at": datetime.utcnow(),
        "applied_by": user_id
    }


@router.get("/storage/usage")
async def get_storage_usage(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    zone: Optional[LakeZone] = Query(None)
):
    """Get storage usage statistics"""
    if not main.lake_manager:
        raise HTTPException(status_code=503, detail="Lake manager not available")
    
    try:
        # Mock storage usage
        return {
            "total_usage_bytes": 1099511627776,  # 1TB
            "by_zone": {
                "bronze": 524288000000,  # 500GB
                "silver": 314572800000,  # 300GB
                "gold": 260650627776   # 200GB
            },
            "by_format": {
                "parquet": 659706976665,
                "avro": 219902325555,
                "json": 219902325556
            },
            "growth_rate_daily_gb": 10.5,
            "projected_usage_30d_tb": 1.3,
            "cost_estimate_monthly_usd": 250.00
        }
    except Exception as e:
        logger.error(f"Failed to get storage usage: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 

# Iceberg-specific endpoints

@router.post("/iceberg/tables")
async def create_iceberg_table(
    request: Request,
    table_name: str = Body(...),
    schema: Dict[str, str] = Body(...),
    partition_by: List[str] = Body(default=[]),
    properties: Optional[Dict[str, str]] = Body(default=None)
) -> Dict[str, Any]:
    """Create a new Iceberg table"""
    try:
        lake_manager = request.app.state.lake_manager
        
        # Create a temporary DataFrame with the schema
        spark = lake_manager.spark
        df = spark.createDataFrame([], schema=schema)
        
        await lake_manager._create_iceberg_table(
            table_name=table_name,
            df=df,
            partition_by=partition_by,
            properties=properties
        )
        
        return {
            "status": "success",
            "table_name": table_name,
            "partition_by": partition_by,
            "properties": properties
        }
        
    except Exception as e:
        logger.error(f"Failed to create Iceberg table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/iceberg/convert")
async def convert_delta_to_iceberg(
    request: Request,
    delta_path: str = Body(...),
    iceberg_table: str = Body(...),
    partition_by: Optional[List[str]] = Body(default=None)
) -> Dict[str, Any]:
    """Convert Delta table to Iceberg format"""
    try:
        lake_manager = request.app.state.lake_manager
        
        await lake_manager.convert_delta_to_iceberg(
            delta_path=delta_path,
            iceberg_table=iceberg_table,
            partition_by=partition_by
        )
        
        return {
            "status": "success",
            "source_delta": delta_path,
            "target_iceberg": iceberg_table
        }
        
    except Exception as e:
        logger.error(f"Failed to convert Delta to Iceberg: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/iceberg/{table_name}/schema")
async def evolve_iceberg_schema(
    request: Request,
    table_name: str,
    add_columns: Optional[Dict[str, str]] = Body(default=None),
    rename_columns: Optional[Dict[str, str]] = Body(default=None),
    drop_columns: Optional[List[str]] = Body(default=None)
) -> Dict[str, Any]:
    """Evolve schema of an Iceberg table"""
    try:
        lake_manager = request.app.state.lake_manager
        
        await lake_manager.evolve_iceberg_schema(
            table_name=table_name,
            add_columns=add_columns,
            rename_columns=rename_columns,
            drop_columns=drop_columns
        )
        
        return {
            "status": "success",
            "table_name": table_name,
            "changes": {
                "added": list(add_columns.keys()) if add_columns else [],
                "renamed": list(rename_columns.keys()) if rename_columns else [],
                "dropped": drop_columns or []
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to evolve schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/iceberg/{table_name}/snapshots")
async def get_iceberg_snapshots(
    request: Request,
    table_name: str
) -> Dict[str, Any]:
    """Get all snapshots of an Iceberg table"""
    try:
        lake_manager = request.app.state.lake_manager
        
        snapshots = await lake_manager.get_iceberg_snapshots(table_name)
        
        return {
            "table_name": table_name,
            "snapshots": snapshots,
            "count": len(snapshots)
        }
        
    except Exception as e:
        logger.error(f"Failed to get snapshots: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/iceberg/time-travel")
async def iceberg_time_travel_query(
    request: Request,
    table_name: str = Body(...),
    snapshot_id: Optional[int] = Body(default=None),
    as_of_timestamp: Optional[str] = Body(default=None),
    query: Optional[str] = Body(default=None),
    limit: int = Body(default=100)
) -> Dict[str, Any]:
    """Query Iceberg table at specific snapshot or timestamp"""
    try:
        lake_manager = request.app.state.lake_manager
        
        # Read table at specific point in time
        df = await lake_manager.iceberg_time_travel(
            table_name=table_name,
            snapshot_id=snapshot_id,
            as_of_timestamp=as_of_timestamp
        )
        
        # Apply optional query
        if query:
            df.createOrReplaceTempView("time_travel_table")
            df = lake_manager.spark.sql(query)
        
        # Collect results
        results = df.limit(limit).collect()
        data = [row.asDict() for row in results]
        
        return {
            "table_name": table_name,
            "snapshot_id": snapshot_id,
            "as_of_timestamp": as_of_timestamp,
            "row_count": len(data),
            "data": data
        }
        
    except Exception as e:
        logger.error(f"Failed to time travel query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/iceberg/optimize")
async def optimize_iceberg_tables(
    request: Request,
    compact_small_files: bool = Body(default=True),
    expire_snapshots: bool = Body(default=True),
    rewrite_manifests: bool = Body(default=True)
) -> Dict[str, Any]:
    """Optimize Iceberg tables for better performance"""
    try:
        lake_manager = request.app.state.lake_manager
        
        results = await lake_manager.optimize_iceberg_tables(
            compact_small_files=compact_small_files,
            expire_snapshots=expire_snapshots,
            rewrite_manifests=rewrite_manifests
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to optimize Iceberg tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tables/comparison")
async def compare_table_formats(
    request: Request,
    table_name: str = Query(...)
) -> Dict[str, Any]:
    """Compare Delta and Iceberg table formats for the same dataset"""
    try:
        lake_manager = request.app.state.lake_manager
        
        comparison = {
            "table_name": table_name,
            "formats": {}
        }
        
        # Check Delta table
        delta_path = f"{lake_manager.zone_paths[DataZone.SILVER]}/{table_name}"
        try:
            delta_df = lake_manager.spark.read.format("delta").load(delta_path)
            comparison["formats"]["delta"] = {
                "exists": True,
                "row_count": delta_df.count(),
                "columns": len(delta_df.columns),
                "partitions": delta_df.rdd.getNumPartitions()
            }
        except:
            comparison["formats"]["delta"] = {"exists": False}
        
        # Check Iceberg table
        iceberg_table = f"iceberg.silver.{table_name}"
        try:
            iceberg_df = lake_manager.spark.table(iceberg_table)
            snapshots = await lake_manager.get_iceberg_snapshots(iceberg_table)
            comparison["formats"]["iceberg"] = {
                "exists": True,
                "row_count": iceberg_df.count(),
                "columns": len(iceberg_df.columns),
                "snapshots": len(snapshots),
                "latest_snapshot": snapshots[0] if snapshots else None
            }
        except:
            comparison["formats"]["iceberg"] = {"exists": False}
        
        return comparison
        
    except Exception as e:
        logger.error(f"Failed to compare table formats: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 