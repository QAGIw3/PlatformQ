"""
API endpoints for Data Lake Service
"""

from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from typing import List, Optional, Dict, Any
from ..schemas import IngestionRequest, ProcessingRequest, QualityCheckRequest
from ..ingestion.data_ingestion import IngestionConfig
from ..processing.layer_processing import ProcessingConfig, DataLayer
from ..dependencies import get_ingestion_pipeline, get_processing_pipeline, get_quality_framework, get_medallion_architecture
from ..ingestion.data_ingestion import DataIngestionPipeline
from ..processing.layer_processing import LayerProcessingPipeline
from ..quality.data_quality_framework import DataQualityFramework
from ..models.medallion_architecture import MedallionArchitecture, DataQualityRule

router = APIRouter()

@router.post("/ingest")
async def start_ingestion(
    request: IngestionRequest,
    ingestion_pipeline: DataIngestionPipeline = Depends(get_ingestion_pipeline),
):
    """Start a new data ingestion job"""
    try:
        config = IngestionConfig(
            source_type=request.source_type,
            source_config=request.source_config,
            target_dataset=request.target_dataset,
            ingestion_mode=request.ingestion_mode,
            schedule=request.schedule,
            transformations=request.transformations,
            quality_checks=request.quality_checks
        )
        
        job_id = await ingestion_pipeline.start_ingestion(config)
        
        return {
            "job_id": job_id,
            "status": "started",
            "config": request.dict()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ingest/{job_id}")
async def get_ingestion_status(
    job_id: str,
    ingestion_pipeline: DataIngestionPipeline = Depends(get_ingestion_pipeline),
):
    """Get status of an ingestion job"""
    job = ingestion_pipeline.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "job_id": job.job_id,
        "status": job.status,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "records_processed": job.records_processed,
        "bronze_path": job.bronze_path,
        "errors": job.errors
    }


@router.post("/process")
async def start_processing(
    request: ProcessingRequest,
    processing_pipeline: LayerProcessingPipeline = Depends(get_processing_pipeline),
):
    """Start a layer processing job"""
    try:
        config = ProcessingConfig(
            source_layer=DataLayer(request.source_layer),
            target_layer=DataLayer(request.target_layer),
            dataset_name=request.dataset_name,
            transformations=request.transformations,
            quality_threshold=request.quality_threshold,
            partition_strategy=request.partition_strategy,
            optimization_config=request.optimization_config,
            schedule=request.schedule
        )
        
        job_id = await processing_pipeline.start_processing(config)
        
        return {
            "job_id": job_id,
            "status": "started",
            "config": request.dict()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/process/{job_id}")
async def get_processing_status(
    job_id: str,
    processing_pipeline: LayerProcessingPipeline = Depends(get_processing_pipeline),
):
    """Get status of a processing job"""
    job = processing_pipeline.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return {
        "job_id": job.job_id,
        "status": job.status,
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "source_paths": job.source_paths,
        "target_path": job.target_path,
        "quality_score": job.quality_score,
        "records_processed": job.records_processed,
        "errors": job.errors
    }


@router.post("/quality/check")
async def run_quality_check(
    request: QualityCheckRequest,
    quality_framework: DataQualityFramework = Depends(get_quality_framework),
    processing_pipeline: LayerProcessingPipeline = Depends(get_processing_pipeline),
):
    """Run quality check on a dataset"""
    try:
        # Get latest data path
        layer = DataLayer(request.layer)
        paths = processing_pipeline._get_latest_paths(layer, request.dataset_name)
        
        if not paths:
            raise HTTPException(status_code=404, detail="Dataset not found")
            
        # Read data
        df = processing_pipeline._read_and_union_data(paths)
        
        # Run quality profile
        profile = quality_framework.profile_dataset(
            df,
            f"{request.dataset_name}_{layer.value}",
            rules=request.rules
        )
        
        # Generate report
        report = quality_framework.generate_quality_report(
            request.dataset_name,
            profile
        )
        
        return report
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalog")
async def get_data_catalog(
    layer: Optional[str] = None,
    dataset_pattern: Optional[str] = None,
    include_schema: bool = True,
    medallion: MedallionArchitecture = Depends(get_medallion_architecture),
):
    """Get data catalog"""
    try:
        catalog = medallion.generate_data_catalog()
        
        # Filter by layer if specified
        if layer:
            catalog["layers"] = {
                k: v for k, v in catalog["layers"].items()
                if k == layer
            }
            
        # Filter by dataset pattern
        if dataset_pattern:
            for layer_name, layer_data in catalog["layers"].items():
                layer_data["datasets"] = [
                    d for d in layer_data["datasets"]
                    if dataset_pattern in d["name"]
                ]
                
        # Remove schema if not requested
        if not include_schema:
            for layer_name, layer_data in catalog["layers"].items():
                for dataset in layer_data["datasets"]:
                    dataset.pop("schema", None)
                    
        return catalog
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/lineage/{dataset_id}")
async def get_data_lineage(
    dataset_id: str,
    medallion: MedallionArchitecture = Depends(get_medallion_architecture),
):
    """Get data lineage for a dataset"""
    lineage = medallion.get_lineage(dataset_id)
    
    return {
        "dataset_id": dataset_id,
        "lineage": [
            {
                "dataset_id": l.dataset_id,
                "source_layer": l.source_layer.value,
                "target_layer": l.target_layer.value,
                "transformation_id": l.transformation_id,
                "timestamp": l.timestamp.isoformat(),
                "input_datasets": l.input_datasets,
                "output_dataset": l.output_dataset,
                "quality_checks_passed": l.quality_checks_passed,
                "quality_checks_failed": l.quality_checks_failed
            }
            for l in lineage
        ]
    }


@router.post("/quality/rules")
async def add_quality_rule(
    rule: Dict[str, Any],
    medallion: MedallionArchitecture = Depends(get_medallion_architecture),
):
    """Add a custom quality rule"""
    try:
        quality_rule = DataQualityRule(
            rule_id=rule["rule_id"],
            name=rule["name"],
            description=rule["description"],
            check_function=eval(rule["check_function"]),  # In production, use safe evaluation
            severity=rule.get("severity", "warning"),
            active=rule.get("active", True)
        )
        
        layer = DataLayer(rule["layer"])
        medallion.add_quality_rule(layer, quality_rule)
        
        return {"message": "Quality rule added successfully", "rule_id": rule["rule_id"]}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compact/{layer}")
async def compact_delta_tables(
    layer: str,
    background_tasks: BackgroundTasks,
    medallion: MedallionArchitecture = Depends(get_medallion_architecture),
):
    """Compact Delta tables in a layer"""
    try:
        data_layer = DataLayer(layer)
        background_tasks.add_task(medallion.compact_delta_tables, data_layer)
        
        return {
            "message": f"Compaction started for {layer} layer",
            "status": "started"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs")
async def list_active_jobs(
    ingestion_pipeline: DataIngestionPipeline = Depends(get_ingestion_pipeline),
    processing_pipeline: LayerProcessingPipeline = Depends(get_processing_pipeline),
):
    """List all active jobs"""
    ingestion_jobs = []
    processing_jobs = []
    
    if ingestion_pipeline:
        ingestion_jobs = [
            {
                "job_id": job.job_id,
                "type": "ingestion",
                "status": job.status,
                "dataset": job.config.target_dataset,
                "started_at": job.started_at.isoformat() if job.started_at else None
            }
            for job in ingestion_pipeline.list_active_jobs()
        ]
        
    if processing_pipeline:
        processing_jobs = [
            {
                "job_id": job.job_id,
                "type": "processing",
                "status": job.status,
                "dataset": job.config.dataset_name,
                "transition": f"{job.config.source_layer.value} -> {job.config.target_layer.value}",
                "started_at": job.started_at.isoformat() if job.started_at else None
            }
            for job in processing_pipeline.list_active_jobs()
        ]
        
    return {
        "total_active": len(ingestion_jobs) + len(processing_jobs),
        "ingestion_jobs": ingestion_jobs,
        "processing_jobs": processing_jobs
    } 