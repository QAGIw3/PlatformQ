"""
ML Data Lake API

Provides RESTful endpoints for ML data lake operations including
training data management, feature engineering, and model artifact storage.
"""

from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Query, BackgroundTasks
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import pandas as pd
import io
import json

from ..lake.ml_medallion import (
    MLMedallionArchitecture, TrainingDataset, FeatureSet, ModelArtifact
)

router = APIRouter(prefix="/api/v1/ml-lake", tags=["ML Data Lake"])


# Request/Response Models
class DataIngestionRequest(BaseModel):
    """Request model for data ingestion"""
    dataset_name: str
    source: str = "upload"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProcessingConfigRequest(BaseModel):
    """Request model for data processing configuration"""
    dataset_id: str
    handle_missing: bool = True
    missing_strategy: str = "mean"  # mean, median, drop, forward_fill
    normalize: bool = True
    encode_categorical: bool = True
    remove_outliers: bool = False
    outlier_threshold: float = 3.0


class FeatureEngineeringRequest(BaseModel):
    """Request model for feature engineering"""
    dataset_id: str
    polynomial_features: bool = False
    interaction_features: bool = False
    time_features: bool = False
    statistical_features: bool = False
    custom_features: Optional[List[Dict[str, Any]]] = None


class TrainingSplitRequest(BaseModel):
    """Request model for creating training splits"""
    feature_set_id: str
    target_column: str
    train_ratio: float = 0.7
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    stratify: bool = False
    random_seed: int = 42


class ModelArtifactRequest(BaseModel):
    """Request model for saving model artifacts"""
    name: str
    version: str = "v1.0"
    algorithm: str
    framework: str
    training_dataset_id: str
    feature_set_id: str
    metrics: Dict[str, float]
    parameters: Dict[str, Any]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class PredictionTrackingRequest(BaseModel):
    """Request model for tracking predictions"""
    model_id: str
    predictions: List[Dict[str, Any]]
    request_metadata: Dict[str, Any] = Field(default_factory=dict)


# Dependency to get ML medallion architecture
async def get_ml_medallion() -> MLMedallionArchitecture:
    """Get ML medallion architecture instance"""
    # In production, this would get the actual instance from app state
    from ..main import ml_medallion_architecture
    return ml_medallion_architecture


@router.post("/datasets/ingest")
async def ingest_dataset(
    file: UploadFile = File(...),
    request: DataIngestionRequest = Depends(),
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion),
    background_tasks: BackgroundTasks = BackgroundTasks()
) -> Dict[str, Any]:
    """Ingest a new training dataset"""
    try:
        # Read uploaded file
        contents = await file.read()
        
        # Determine file type and read accordingly
        if file.filename.endswith('.csv'):
            df = pd.read_csv(io.StringIO(contents.decode()))
        elif file.filename.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(contents))
        elif file.filename.endswith('.json'):
            df = pd.read_json(io.StringIO(contents.decode()))
        else:
            raise HTTPException(
                status_code=400,
                detail="Unsupported file format. Use CSV, Parquet, or JSON"
            )
            
        # Ingest data
        dataset = await ml_medallion.ingest_training_data(
            df,
            request.dataset_name,
            request.source
        )
        
        return {
            "status": "success",
            "dataset_id": dataset.dataset_id,
            "name": dataset.name,
            "version": dataset.version,
            "row_count": dataset.metadata["row_count"],
            "column_count": dataset.metadata["column_count"],
            "size_bytes": dataset.metadata["size_bytes"],
            "source_path": dataset.source_paths[0]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/datasets/{dataset_id}/process")
async def process_dataset(
    dataset_id: str,
    config: ProcessingConfigRequest,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Process raw dataset to Silver layer"""
    try:
        # Validate dataset ID matches
        if dataset_id != config.dataset_id:
            raise HTTPException(
                status_code=400,
                detail="Dataset ID mismatch"
            )
            
        # Process dataset
        processing_config = {
            "handle_missing": config.handle_missing,
            "missing_strategy": config.missing_strategy,
            "normalize": config.normalize,
            "encode_categorical": config.encode_categorical,
            "remove_outliers": config.remove_outliers,
            "outlier_threshold": config.outlier_threshold
        }
        
        dataset = await ml_medallion.process_training_data(
            dataset_id,
            processing_config
        )
        
        return {
            "status": "success",
            "dataset_id": dataset.dataset_id,
            "processed_path": dataset.source_paths[-1],
            "statistics": dataset.statistics,
            "processing_config": processing_config,
            "processed_at": dataset.metadata.get("processed_at")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features/engineer")
async def engineer_features(
    request: FeatureEngineeringRequest,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Create engineered features"""
    try:
        # Create feature configuration
        feature_config = {
            "polynomial_features": request.polynomial_features,
            "interaction_features": request.interaction_features,
            "time_features": request.time_features,
            "statistical_features": request.statistical_features
        }
        
        if request.custom_features:
            feature_config["custom_features"] = request.custom_features
            
        # Create feature set
        feature_set = await ml_medallion.create_feature_set(
            request.dataset_id,
            feature_config
        )
        
        return {
            "status": "success",
            "feature_set_id": feature_set.feature_set_id,
            "name": feature_set.name,
            "version": feature_set.version,
            "total_features": feature_set.statistics["total_features"],
            "engineered_features": feature_set.statistics["engineered_features"],
            "feature_path": feature_set.statistics["feature_path"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/training/splits")
async def create_training_splits(
    request: TrainingSplitRequest,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Create train/validation/test splits"""
    try:
        # Validate split ratios
        total_ratio = request.train_ratio + request.val_ratio + request.test_ratio
        if abs(total_ratio - 1.0) > 0.001:
            raise HTTPException(
                status_code=400,
                detail="Split ratios must sum to 1.0"
            )
            
        # Create splits
        split_config = {
            "train": request.train_ratio,
            "val": request.val_ratio,
            "test": request.test_ratio
        }
        
        split_paths = await ml_medallion.create_training_splits(
            request.feature_set_id,
            request.target_column,
            split_config
        )
        
        return {
            "status": "success",
            "feature_set_id": request.feature_set_id,
            "target_column": request.target_column,
            "split_paths": split_paths,
            "split_config": split_config
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/models/artifacts")
async def save_model_artifact(
    file: UploadFile = File(...),
    metadata: str = Query(..., description="JSON string of model metadata"),
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Save a trained model artifact"""
    try:
        # Parse metadata
        model_metadata = json.loads(metadata)
        
        # Save uploaded file temporarily
        temp_path = f"/tmp/{file.filename}"
        contents = await file.read()
        with open(temp_path, 'wb') as f:
            f.write(contents)
            
        # Save model artifact
        artifact = await ml_medallion.save_model_artifact(
            temp_path,
            model_metadata
        )
        
        # Clean up temp file
        import os
        os.remove(temp_path)
        
        return {
            "status": "success",
            "model_id": artifact.model_id,
            "name": artifact.name,
            "version": artifact.version,
            "artifact_path": artifact.artifact_path,
            "size_bytes": artifact.size_bytes,
            "metrics": artifact.metrics
        }
        
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON in metadata parameter"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/predictions/track")
async def track_predictions(
    request: PredictionTrackingRequest,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Track model predictions"""
    try:
        # Convert predictions to DataFrame
        predictions_df = pd.DataFrame(request.predictions)
        
        # Track predictions
        batch_id = await ml_medallion.track_predictions(
            request.model_id,
            predictions_df,
            request.request_metadata
        )
        
        return {
            "status": "success",
            "batch_id": batch_id,
            "model_id": request.model_id,
            "prediction_count": len(predictions_df),
            "tracked_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/models/{model_id}/metrics")
async def get_model_metrics(
    model_id: str,
    days: int = Query(default=7, ge=1, le=90),
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Get aggregated model metrics"""
    try:
        # Aggregate metrics for time window
        time_window = timedelta(days=days)
        metrics = await ml_medallion.aggregate_model_metrics(
            model_id,
            time_window
        )
        
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets")
async def list_datasets(
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """List available datasets"""
    try:
        # In production, implement proper listing from metadata store
        return {
            "datasets": [],
            "total": 0,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/datasets/{dataset_id}")
async def get_dataset_info(
    dataset_id: str,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Get dataset information"""
    try:
        # Load dataset metadata
        dataset = await ml_medallion._load_dataset_metadata(dataset_id)
        
        return {
            "dataset_id": dataset.dataset_id,
            "name": dataset.name,
            "version": dataset.version,
            "source_paths": dataset.source_paths,
            "split_ratios": dataset.split_ratios,
            "feature_columns": dataset.feature_columns,
            "target_column": dataset.target_column,
            "created_at": dataset.created_at.isoformat(),
            "metadata": dataset.metadata,
            "statistics": dataset.statistics
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/{feature_set_id}")
async def get_feature_set_info(
    feature_set_id: str,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Get feature set information"""
    try:
        # Load feature set metadata
        feature_set = await ml_medallion._load_feature_metadata(feature_set_id)
        
        return {
            "feature_set_id": feature_set.feature_set_id,
            "name": feature_set.name,
            "version": feature_set.version,
            "features": feature_set.features,
            "entity_type": feature_set.entity_type,
            "created_at": feature_set.created_at.isoformat(),
            "update_frequency": feature_set.update_frequency,
            "statistics": feature_set.statistics
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/query")
async def query_ml_data(
    query: str,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Execute SQL query on ML data"""
    try:
        # Execute query
        result_df = await ml_medallion.query_training_data(query)
        
        return {
            "status": "success",
            "row_count": len(result_df),
            "columns": list(result_df.columns),
            "data": result_df.to_dict(orient="records")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(
    dataset_id: str,
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Delete a dataset and its artifacts"""
    try:
        # In production, implement proper deletion with cascade
        return {
            "status": "success",
            "message": f"Dataset {dataset_id} marked for deletion"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_ml_lake_stats(
    ml_medallion: MLMedallionArchitecture = Depends(get_ml_medallion)
) -> Dict[str, Any]:
    """Get ML data lake statistics"""
    try:
        # In production, implement proper stats aggregation
        return {
            "total_datasets": 0,
            "total_feature_sets": 0,
            "total_models": 0,
            "total_predictions": 0,
            "storage_used_gb": 0.0,
            "generated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 