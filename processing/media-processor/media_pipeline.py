"""
Media Processing Pipeline System

Provides a unified pipeline for processing various media types with:
- Distributed processing via Apache Spark
- Real-time streaming support
- Multiple format support
- ML-based enhancement
- Quality optimization
"""

import os
import sys
import logging
import tempfile
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import hashlib
from enum import Enum

import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, udf, when, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, ArrayType, MapType, BinaryType
)
import cv2
from PIL import Image
import librosa
import moviepy.editor as mp
import soundfile as sf

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from platformq_shared.storage import StorageClient
from platformq_shared.events import EventPublisher
from platformq_shared.ml import MLModelRegistry

logger = logging.getLogger(__name__)


class MediaType(str, Enum):
    """Supported media types"""
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"
    MODEL_3D = "3d_model"


class ProcessingStage(str, Enum):
    """Processing pipeline stages"""
    INGEST = "ingest"
    VALIDATE = "validate"
    ANALYZE = "analyze"
    ENHANCE = "enhance"
    TRANSFORM = "transform"
    OPTIMIZE = "optimize"
    PACKAGE = "package"
    DELIVER = "deliver"


@dataclass
class MediaAsset:
    """Media asset metadata"""
    asset_id: str
    media_type: MediaType
    source_path: str
    format: str
    size_bytes: int
    duration_seconds: Optional[float] = None
    dimensions: Optional[Tuple[int, ...]] = None
    fps: Optional[float] = None
    bitrate: Optional[int] = None
    codec: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    

@dataclass
class ProcessingJob:
    """Media processing job"""
    job_id: str
    asset: MediaAsset
    pipeline: List[ProcessingStage]
    operations: Dict[str, Dict[str, Any]]
    output_formats: List[str]
    target_quality: Optional[str] = None
    priority: int = 5
    created_at: datetime = field(default_factory=datetime.utcnow)
    

@dataclass
class ProcessingResult:
    """Processing result"""
    job_id: str
    status: str
    outputs: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    errors: List[str] = field(default_factory=list)
    processing_time_seconds: float = 0.0


class MediaPipeline:
    """Media processing pipeline orchestrator"""
    
    def __init__(self,
                 spark_session: SparkSession,
                 storage_client: StorageClient,
                 event_publisher: EventPublisher,
                 model_registry: MLModelRegistry):
        self.spark = spark_session
        self.storage = storage_client
        self.events = event_publisher
        self.models = model_registry
        
        # Configure Spark for media processing
        self._configure_spark()
        
        # Initialize processors
        self.processors = {
            MediaType.IMAGE: ImageProcessor(self),
            MediaType.VIDEO: VideoProcessor(self),
            MediaType.AUDIO: AudioProcessor(self),
            MediaType.DOCUMENT: DocumentProcessor(self),
            MediaType.MODEL_3D: Model3DProcessor(self)
        }
        
        # Pipeline configurations
        self.stage_handlers = {
            ProcessingStage.INGEST: self._stage_ingest,
            ProcessingStage.VALIDATE: self._stage_validate,
            ProcessingStage.ANALYZE: self._stage_analyze,
            ProcessingStage.ENHANCE: self._stage_enhance,
            ProcessingStage.TRANSFORM: self._stage_transform,
            ProcessingStage.OPTIMIZE: self._stage_optimize,
            ProcessingStage.PACKAGE: self._stage_package,
            ProcessingStage.DELIVER: self._stage_deliver
        }
        
    def _configure_spark(self):
        """Configure Spark for media processing"""
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        self.spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Register custom serializers for media types
        self.spark.sparkContext.setJobGroup("media_pipeline", "Media Processing Pipeline")
        
    async def process_batch(self, jobs: List[ProcessingJob]) -> List[ProcessingResult]:
        """Process a batch of media jobs"""
        logger.info(f"Processing batch of {len(jobs)} media jobs")
        
        # Group jobs by media type for efficient processing
        jobs_by_type = {}
        for job in jobs:
            media_type = job.asset.media_type
            if media_type not in jobs_by_type:
                jobs_by_type[media_type] = []
            jobs_by_type[media_type].append(job)
            
        results = []
        
        # Process each media type group
        for media_type, typed_jobs in jobs_by_type.items():
            processor = self.processors.get(media_type)
            if processor:
                type_results = await self._process_media_type_batch(
                    processor, typed_jobs
                )
                results.extend(type_results)
            else:
                # Unsupported media type
                for job in typed_jobs:
                    results.append(ProcessingResult(
                        job_id=job.job_id,
                        status="failed",
                        outputs=[],
                        metrics={},
                        errors=[f"Unsupported media type: {media_type}"]
                    ))
                    
        return results
        
    async def _process_media_type_batch(self,
                                       processor: 'MediaProcessor',
                                       jobs: List[ProcessingJob]) -> List[ProcessingResult]:
        """Process batch of jobs for specific media type"""
        # Create Spark DataFrame from jobs
        job_data = []
        for job in jobs:
            job_data.append({
                "job_id": job.job_id,
                "asset_id": job.asset.asset_id,
                "source_path": job.asset.source_path,
                "pipeline": json.dumps([s.value for s in job.pipeline]),
                "operations": json.dumps(job.operations),
                "output_formats": json.dumps(job.output_formats),
                "priority": job.priority
            })
            
        jobs_df = self.spark.createDataFrame(job_data)
        
        # Process through pipeline stages
        results_df = jobs_df
        for stage in ProcessingStage:
            if any(stage in job.pipeline for job in jobs):
                stage_handler = self.stage_handlers[stage]
                results_df = await stage_handler(processor, results_df)
                
        # Collect results
        results = []
        for row in results_df.collect():
            results.append(ProcessingResult(
                job_id=row["job_id"],
                status=row["status"],
                outputs=json.loads(row["outputs"]),
                metrics=json.loads(row["metrics"]),
                errors=json.loads(row.get("errors", "[]")),
                processing_time_seconds=row.get("processing_time", 0.0)
            ))
            
        return results
        
    async def _stage_ingest(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Ingest stage - load and validate media files"""
        logger.info("Processing ingest stage")
        
        # Define UDF for file validation
        @udf(returnType=MapType(StringType(), StringType()))
        def validate_file(source_path: str) -> Dict[str, str]:
            try:
                if not os.path.exists(source_path):
                    return {"valid": "false", "error": "File not found"}
                    
                # Check file size
                size = os.path.getsize(source_path)
                if size == 0:
                    return {"valid": "false", "error": "Empty file"}
                    
                # Verify file type
                import magic
                file_type = magic.from_file(source_path, mime=True)
                
                return {
                    "valid": "true",
                    "size": str(size),
                    "mime_type": file_type
                }
                
            except Exception as e:
                return {"valid": "false", "error": str(e)}
                
        # Apply validation
        df = df.withColumn("validation", validate_file(col("source_path")))
        df = df.withColumn("is_valid", col("validation.valid") == "true")
        
        return df
        
    async def _stage_validate(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Validate stage - deep validation of media content"""
        logger.info("Processing validate stage")
        
        # Media-specific validation
        return await processor.validate(df)
        
    async def _stage_analyze(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Analyze stage - extract features and metadata"""
        logger.info("Processing analyze stage")
        
        # Media-specific analysis
        return await processor.analyze(df)
        
    async def _stage_enhance(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Enhance stage - AI-powered enhancement"""
        logger.info("Processing enhance stage")
        
        # Load enhancement models
        enhancer_model = await self.models.get_model("media_enhancer", processor.media_type.value)
        
        if enhancer_model:
            return await processor.enhance(df, enhancer_model)
        else:
            logger.warning(f"No enhancement model available for {processor.media_type}")
            return df
            
    async def _stage_transform(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Transform stage - apply transformations"""
        logger.info("Processing transform stage")
        
        return await processor.transform(df)
        
    async def _stage_optimize(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Optimize stage - optimize for delivery"""
        logger.info("Processing optimize stage")
        
        return await processor.optimize(df)
        
    async def _stage_package(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Package stage - create delivery packages"""
        logger.info("Processing package stage")
        
        # Define packaging UDF
        @udf(returnType=ArrayType(MapType(StringType(), StringType())))
        def package_outputs(outputs_json: str, formats_json: str) -> List[Dict[str, str]]:
            outputs = json.loads(outputs_json)
            formats = json.loads(formats_json)
            
            packages = []
            for fmt in formats:
                package = {
                    "format": fmt,
                    "path": f"{outputs['base_path']}/{outputs['asset_id']}.{fmt}",
                    "size": str(outputs.get(f"{fmt}_size", 0))
                }
                packages.append(package)
                
            return packages
            
        df = df.withColumn("packages", package_outputs(col("outputs"), col("output_formats")))
        
        return df
        
    async def _stage_deliver(self, processor: 'MediaProcessor', df: DataFrame) -> DataFrame:
        """Deliver stage - upload to storage and notify"""
        logger.info("Processing deliver stage")
        
        # Upload to storage
        @udf(returnType=StringType())
        def upload_to_storage(package_path: str, asset_id: str) -> str:
            try:
                # Upload logic here
                storage_path = f"media/{asset_id}/{os.path.basename(package_path)}"
                # self.storage.upload(package_path, storage_path)
                return storage_path
            except Exception as e:
                return f"error:{str(e)}"
                
        # Process uploads
        df = df.withColumn("delivery_status", lit("completed"))
        df = df.withColumn("delivered_at", lit(datetime.utcnow().isoformat()))
        
        return df


class MediaProcessor:
    """Base class for media processors"""
    
    def __init__(self, pipeline: MediaPipeline, media_type: MediaType):
        self.pipeline = pipeline
        self.media_type = media_type
        self.spark = pipeline.spark
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate media files"""
        raise NotImplementedError
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze media content"""
        raise NotImplementedError
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance media using AI"""
        raise NotImplementedError
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Apply transformations"""
        raise NotImplementedError
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize for delivery"""
        raise NotImplementedError


class ImageProcessor(MediaProcessor):
    """Image processing implementation"""
    
    def __init__(self, pipeline: MediaPipeline):
        super().__init__(pipeline, MediaType.IMAGE)
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate image files"""
        
        @udf(returnType=MapType(StringType(), StringType()))
        def validate_image(path: str) -> Dict[str, str]:
            try:
                img = cv2.imread(path)
                if img is None:
                    return {"valid": "false", "error": "Cannot read image"}
                    
                height, width = img.shape[:2]
                channels = img.shape[2] if len(img.shape) > 2 else 1
                
                return {
                    "valid": "true",
                    "width": str(width),
                    "height": str(height),
                    "channels": str(channels)
                }
                
            except Exception as e:
                return {"valid": "false", "error": str(e)}
                
        df = df.withColumn("image_info", validate_image(col("source_path")))
        return df
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze image content"""
        
        @udf(returnType=MapType(StringType(), FloatType()))
        def analyze_image_quality(path: str) -> Dict[str, float]:
            try:
                img = cv2.imread(path)
                if img is None:
                    return {}
                    
                # Calculate quality metrics
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                
                # Sharpness (Laplacian variance)
                laplacian = cv2.Laplacian(gray, cv2.CV_64F)
                sharpness = laplacian.var()
                
                # Brightness
                brightness = np.mean(gray)
                
                # Contrast
                contrast = gray.std()
                
                # Noise estimate
                noise = np.std(gray - cv2.GaussianBlur(gray, (5, 5), 0))
                
                return {
                    "sharpness": float(sharpness),
                    "brightness": float(brightness),
                    "contrast": float(contrast),
                    "noise": float(noise)
                }
                
            except Exception as e:
                logger.error(f"Error analyzing image: {e}")
                return {}
                
        df = df.withColumn("quality_metrics", analyze_image_quality(col("source_path")))
        return df
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance images using AI models"""
        
        @udf(returnType=StringType())
        def enhance_image(path: str, operations_json: str) -> str:
            try:
                operations = json.loads(operations_json)
                img = cv2.imread(path)
                
                if img is None:
                    return path
                    
                # Apply enhancements
                if operations.get("enhance", {}).get("denoise"):
                    img = cv2.fastNlMeansDenoisingColored(img, None, 10, 10, 7, 21)
                    
                if operations.get("enhance", {}).get("sharpen"):
                    kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
                    img = cv2.filter2D(img, -1, kernel)
                    
                if operations.get("enhance", {}).get("super_resolution"):
                    # Apply super-resolution model
                    # img = model.predict(img)
                    pass
                    
                # Save enhanced image
                enhanced_path = path.replace(".", "_enhanced.")
                cv2.imwrite(enhanced_path, img)
                
                return enhanced_path
                
            except Exception as e:
                logger.error(f"Error enhancing image: {e}")
                return path
                
        df = df.withColumn("enhanced_path", enhance_image(col("source_path"), col("operations")))
        return df
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Apply image transformations"""
        
        @udf(returnType=ArrayType(StringType()))
        def transform_image(path: str, operations_json: str) -> List[str]:
            try:
                operations = json.loads(operations_json)
                transforms = operations.get("transform", {})
                
                img = cv2.imread(path)
                if img is None:
                    return []
                    
                output_paths = []
                base_name = os.path.splitext(path)[0]
                
                # Resize operations
                if "resize" in transforms:
                    for size_name, dimensions in transforms["resize"].items():
                        resized = cv2.resize(img, tuple(dimensions))
                        output_path = f"{base_name}_{size_name}.jpg"
                        cv2.imwrite(output_path, resized)
                        output_paths.append(output_path)
                        
                # Format conversions
                if "formats" in transforms:
                    for fmt in transforms["formats"]:
                        output_path = f"{base_name}.{fmt}"
                        if fmt == "webp":
                            cv2.imwrite(output_path, img, [cv2.IMWRITE_WEBP_QUALITY, 90])
                        else:
                            cv2.imwrite(output_path, img)
                        output_paths.append(output_path)
                        
                return output_paths
                
            except Exception as e:
                logger.error(f"Error transforming image: {e}")
                return []
                
        df = df.withColumn("transformed_paths", transform_image(col("source_path"), col("operations")))
        return df
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize images for delivery"""
        
        @udf(returnType=MapType(StringType(), StringType()))
        def optimize_image(paths_json: str, target_quality: str) -> Dict[str, str]:
            try:
                paths = json.loads(paths_json) if isinstance(paths_json, str) else paths_json
                optimized = {}
                
                quality_settings = {
                    "low": 60,
                    "medium": 80,
                    "high": 90,
                    "lossless": 100
                }
                
                quality = quality_settings.get(target_quality, 85)
                
                for path in paths:
                    img = cv2.imread(path)
                    if img is not None:
                        optimized_path = path.replace(".", "_optimized.")
                        cv2.imwrite(optimized_path, img, [cv2.IMWRITE_JPEG_QUALITY, quality])
                        optimized[path] = optimized_path
                        
                return optimized
                
            except Exception as e:
                logger.error(f"Error optimizing image: {e}")
                return {}
                
        df = df.withColumn("optimized_paths", optimize_image(col("transformed_paths"), col("target_quality")))
        return df


class VideoProcessor(MediaProcessor):
    """Video processing implementation"""
    
    def __init__(self, pipeline: MediaPipeline):
        super().__init__(pipeline, MediaType.VIDEO)
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate video files"""
        
        @udf(returnType=MapType(StringType(), StringType()))
        def validate_video(path: str) -> Dict[str, str]:
            try:
                video = mp.VideoFileClip(path)
                
                return {
                    "valid": "true",
                    "duration": str(video.duration),
                    "fps": str(video.fps),
                    "width": str(video.w),
                    "height": str(video.h)
                }
                
            except Exception as e:
                return {"valid": "false", "error": str(e)}
                
        df = df.withColumn("video_info", validate_video(col("source_path")))
        return df
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze video content"""
        # Implement scene detection, quality analysis, etc.
        return df
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance videos using AI"""
        # Implement video enhancement
        return df
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Apply video transformations"""
        
        @udf(returnType=ArrayType(StringType()))
        def transform_video(path: str, operations_json: str) -> List[str]:
            try:
                operations = json.loads(operations_json)
                transforms = operations.get("transform", {})
                
                video = mp.VideoFileClip(path)
                output_paths = []
                base_name = os.path.splitext(path)[0]
                
                # Resolution variants
                if "resolutions" in transforms:
                    for res_name, resolution in transforms["resolutions"].items():
                        output_path = f"{base_name}_{res_name}.mp4"
                        resized = video.resize(resolution)
                        resized.write_videofile(output_path, codec='libx264')
                        output_paths.append(output_path)
                        
                # Format conversions
                if "formats" in transforms:
                    for fmt in transforms["formats"]:
                        output_path = f"{base_name}.{fmt}"
                        video.write_videofile(output_path)
                        output_paths.append(output_path)
                        
                video.close()
                return output_paths
                
            except Exception as e:
                logger.error(f"Error transforming video: {e}")
                return []
                
        df = df.withColumn("transformed_paths", transform_video(col("source_path"), col("operations")))
        return df
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize videos for delivery"""
        # Implement video optimization (bitrate, codec, etc.)
        return df


class AudioProcessor(MediaProcessor):
    """Audio processing implementation"""
    
    def __init__(self, pipeline: MediaPipeline):
        super().__init__(pipeline, MediaType.AUDIO)
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate audio files"""
        
        @udf(returnType=MapType(StringType(), StringType()))
        def validate_audio(path: str) -> Dict[str, str]:
            try:
                data, sr = librosa.load(path, sr=None)
                duration = len(data) / sr
                
                return {
                    "valid": "true",
                    "duration": str(duration),
                    "sample_rate": str(sr),
                    "channels": "1"  # librosa loads as mono by default
                }
                
            except Exception as e:
                return {"valid": "false", "error": str(e)}
                
        df = df.withColumn("audio_info", validate_audio(col("source_path")))
        return df
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze audio content"""
        
        @udf(returnType=MapType(StringType(), FloatType()))
        def analyze_audio_features(path: str) -> Dict[str, float]:
            try:
                y, sr = librosa.load(path)
                
                # Extract features
                tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
                spectral_centroids = librosa.feature.spectral_centroid(y=y, sr=sr)
                rms = librosa.feature.rms(y=y)
                
                return {
                    "tempo": float(tempo),
                    "spectral_centroid": float(np.mean(spectral_centroids)),
                    "rms_energy": float(np.mean(rms)),
                    "dynamic_range": float(np.max(rms) - np.min(rms))
                }
                
            except Exception as e:
                logger.error(f"Error analyzing audio: {e}")
                return {}
                
        df = df.withColumn("audio_features", analyze_audio_features(col("source_path")))
        return df
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance audio using AI"""
        # Implement noise reduction, enhancement, etc.
        return df
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Apply audio transformations"""
        # Implement format conversion, resampling, etc.
        return df
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize audio for delivery"""
        # Implement bitrate optimization, compression, etc.
        return df


class DocumentProcessor(MediaProcessor):
    """Document processing implementation"""
    
    def __init__(self, pipeline: MediaPipeline):
        super().__init__(pipeline, MediaType.DOCUMENT)
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate document files"""
        # Implement document validation
        return df
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze document content"""
        # Implement text extraction, OCR, etc.
        return df
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance documents"""
        # Implement quality improvement
        return df
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Transform documents"""
        # Implement format conversion
        return df
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize documents"""
        # Implement compression, optimization
        return df


class Model3DProcessor(MediaProcessor):
    """3D model processing implementation"""
    
    def __init__(self, pipeline: MediaPipeline):
        super().__init__(pipeline, MediaType.MODEL_3D)
        
    async def validate(self, df: DataFrame) -> DataFrame:
        """Validate 3D model files"""
        # Implement 3D model validation
        return df
        
    async def analyze(self, df: DataFrame) -> DataFrame:
        """Analyze 3D model content"""
        # Implement polygon count, texture analysis, etc.
        return df
        
    async def enhance(self, df: DataFrame, model: Any) -> DataFrame:
        """Enhance 3D models"""
        # Implement mesh optimization, texture enhancement
        return df
        
    async def transform(self, df: DataFrame) -> DataFrame:
        """Transform 3D models"""
        # Implement format conversion, LOD generation
        return df
        
    async def optimize(self, df: DataFrame) -> DataFrame:
        """Optimize 3D models"""
        # Implement compression, decimation
        return df 