"""Unified media processor with distributed processing capabilities."""
import os
import sys
import json
import time
import logging
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from platformq.shared.compute_orchestrator import (
    ComputeOrchestrator, 
    ComputeJob, 
    JobStatus
)
from processing.spark.processor_base import ProcessorBase

logger = logging.getLogger(__name__)


@dataclass
class MediaProcessingConfig:
    """Configuration for media processing."""
    mode: str = "metadata"  # "metadata", "process", or "distributed_process"
    media_type: str = "auto"  # "image", "video", "audio", or "auto"
    operation: Optional[str] = None
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class MediaProcessor(ProcessorBase):
    """Processor for media files with distributed processing capabilities."""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("MediaProcessor", tenant_id, config)
        self.orchestrator = ComputeOrchestrator("media-processor")
        
        # Register job completion handler
        self.orchestrator.register_job_handler("media", self._handle_job_completion)
        
        # Supported operations by media type
        self.supported_operations = {
            "image": [
                "resize", "crop", "filter", "color_correction", "batch_convert",
                "watermark", "composite", "enhance", "denoise", "style_transfer",
                "distributed_filter", "panorama_stitch", "hdr_merge"
            ],
            "video": [
                "transcode", "resize", "trim", "concatenate", "overlay",
                "stabilize", "color_grade", "add_effects", "extract_frames",
                "distributed_transcode", "motion_tracking", "object_removal"
            ],
            "audio": [
                "transcode", "normalize", "denoise", "mix", "effects",
                "pitch_shift", "time_stretch", "equalize", "compress",
                "distributed_process", "speech_enhance", "music_separation"
            ]
        }
    
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate processing configuration."""
        config = MediaProcessingConfig(**processing_config)
        
        if config.mode not in ["metadata", "process", "distributed_process"]:
            raise ValueError(f"Invalid mode: {config.mode}")
        
        if config.media_type not in ["image", "video", "audio", "auto"]:
            raise ValueError(f"Invalid media type: {config.media_type}")
        
        if config.mode in ["process", "distributed_process"] and not config.operation:
            raise ValueError("Operation must be specified for processing mode")
        
        return True
    
    def process(self, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process media file."""
        config = MediaProcessingConfig(**processing_config)
        
        if config.mode == "metadata":
            return self._extract_metadata(asset_uri)
        elif config.mode == "process":
            return self._process_local(asset_uri, config)
        else:  # distributed_process
            return self._submit_distributed_processing(asset_uri, config)
    
    def _extract_metadata(self, asset_uri: str) -> Dict[str, Any]:
        """Extract metadata from media file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download media file
            local_file = Path(temp_dir) / "media_file"
            self.download_from_s3(asset_uri, str(local_file))
            
            # Detect media type
            import mimetypes
            mime_type, _ = mimetypes.guess_type(str(local_file))
            
            if mime_type and mime_type.startswith('image/'):
                return self._extract_image_metadata(local_file)
            elif mime_type and mime_type.startswith('video/'):
                return self._extract_video_metadata(local_file)
            elif mime_type and mime_type.startswith('audio/'):
                return self._extract_audio_metadata(local_file)
            else:
                return {"error": "Unknown media type"}
    
    def _extract_image_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract image metadata."""
        try:
            from PIL import Image
            import exifread
            
            # Basic image info
            img = Image.open(file_path)
            metadata = {
                "media_type": "image",
                "format": img.format,
                "mode": img.mode,
                "size": {
                    "width": img.width,
                    "height": img.height
                },
                "info": img.info
            }
            
            # EXIF data
            with open(file_path, 'rb') as f:
                tags = exifread.process_file(f)
                exif_data = {}
                for tag, value in tags.items():
                    if tag not in ['JPEGThumbnail', 'TIFFThumbnail']:
                        exif_data[tag] = str(value)
                
                if exif_data:
                    metadata["exif"] = exif_data
            
            # Color statistics
            if img.mode in ['RGB', 'RGBA']:
                import numpy as np
                img_array = np.array(img)
                metadata["color_stats"] = {
                    "mean": img_array.mean(axis=(0, 1)).tolist(),
                    "std": img_array.std(axis=(0, 1)).tolist()
                }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting image metadata: {e}")
            return {"error": str(e)}
    
    def _extract_video_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract video metadata."""
        try:
            import subprocess
            import json
            
            # Use ffprobe to get metadata
            cmd = [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', '-show_streams', str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                
                # Extract key information
                metadata = {
                    "media_type": "video",
                    "format": data['format']['format_name'],
                    "duration": float(data['format'].get('duration', 0)),
                    "bit_rate": int(data['format'].get('bit_rate', 0)),
                    "size": int(data['format'].get('size', 0))
                }
                
                # Video stream info
                for stream in data['streams']:
                    if stream['codec_type'] == 'video':
                        metadata['video'] = {
                            "codec": stream['codec_name'],
                            "width": stream['width'],
                            "height": stream['height'],
                            "fps": eval(stream.get('r_frame_rate', '0/1')),
                            "duration": float(stream.get('duration', 0))
                        }
                    elif stream['codec_type'] == 'audio':
                        metadata['audio'] = {
                            "codec": stream['codec_name'],
                            "sample_rate": int(stream.get('sample_rate', 0)),
                            "channels": stream.get('channels', 0),
                            "bit_rate": int(stream.get('bit_rate', 0))
                        }
                
                return metadata
            else:
                return {"error": "Failed to extract video metadata"}
                
        except Exception as e:
            logger.error(f"Error extracting video metadata: {e}")
            return {"error": str(e)}
    
    def _extract_audio_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract audio metadata."""
        try:
            import librosa
            
            # Load audio file
            y, sr = librosa.load(str(file_path), sr=None)
            duration = len(y) / sr
            
            metadata = {
                "media_type": "audio",
                "duration": duration,
                "sample_rate": sr,
                "samples": len(y),
                "channels": 1 if len(y.shape) == 1 else y.shape[0]
            }
            
            # Audio features
            features = {
                "rms_energy": float(librosa.feature.rms(y=y).mean()),
                "zero_crossing_rate": float(librosa.feature.zero_crossing_rate(y).mean()),
                "spectral_centroid": float(librosa.feature.spectral_centroid(y=y, sr=sr).mean())
            }
            
            # Tempo estimation for music
            try:
                tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
                features["tempo"] = float(tempo)
            except:
                pass
            
            metadata["features"] = features
            
            return metadata
            
        except Exception as e:
            logger.error(f"Error extracting audio metadata: {e}")
            return {"error": str(e)}
    
    def _process_local(self, asset_uri: str, config: MediaProcessingConfig) -> Dict[str, Any]:
        """Process media locally (for small files)."""
        # This is a simplified version
        # In practice, would use the media worker directly
        return {
            "status": "completed",
            "mode": "local",
            "message": "Local processing not fully implemented"
        }
    
    def _submit_distributed_processing(self, asset_uri: str,
                                     config: MediaProcessingConfig) -> Dict[str, Any]:
        """Submit distributed media processing job."""
        # Auto-detect media type if needed
        if config.media_type == "auto":
            # Would detect from file extension or content
            config.media_type = self._detect_media_type(asset_uri)
        
        # Check if operation needs distribution
        if config.operation.startswith("distributed_"):
            return self._submit_distributed_job(asset_uri, config)
        else:
            # Single job
            job = ComputeJob(
                job_id="",
                job_type="media",
                input_data={
                    "media_file": asset_uri,
                    "media_type": config.media_type,
                    "operation": config.operation,
                    "parameters": config.parameters
                },
                output_location=f"output-data/media/{self.tenant_id}",
                parameters={}
            )
            
            job_id = self.orchestrator.submit_job(job)
            
            return {
                "status": "submitted",
                "job_id": job_id,
                "media_type": config.media_type,
                "operation": config.operation,
                "message": f"Media processing job submitted"
            }
    
    def _submit_distributed_job(self, asset_uri: str,
                               config: MediaProcessingConfig) -> Dict[str, Any]:
        """Submit distributed processing job."""
        jobs = []
        
        if config.media_type == "video" and config.operation == "distributed_transcode":
            # Split video into segments
            segment_duration = config.parameters.get("segment_duration", 60)
            num_segments = config.parameters.get("num_segments", 10)  # Estimate
            
            for i in range(num_segments):
                segment_job = ComputeJob(
                    job_id="",
                    job_type="media",
                    input_data={
                        "media_file": asset_uri,
                        "media_type": "video",
                        "operation": "transcode_segment",
                        "parameters": {
                            **config.parameters,
                            "segment_index": i,
                            "segment_duration": segment_duration
                        }
                    },
                    output_location=f"output-data/media/{self.tenant_id}/segments",
                    parameters={"segment": i}
                )
                
                job_id = self.orchestrator.submit_job(segment_job)
                jobs.append(job_id)
            
            # Submit merge job
            merge_job = ComputeJob(
                job_id="",
                job_type="media",
                input_data={
                    "segment_jobs": jobs,
                    "media_type": "video",
                    "operation": "merge_segments",
                    "parameters": config.parameters
                },
                output_location=f"output-data/media/{self.tenant_id}",
                parameters={"merge": True}
            )
            
            merge_job_id = self.orchestrator.submit_job(merge_job)
            
            return {
                "status": "submitted",
                "segment_jobs": jobs,
                "merge_job": merge_job_id,
                "num_segments": num_segments,
                "message": f"Distributed video transcoding submitted with {num_segments} segments"
            }
        
        elif config.media_type == "image" and config.operation == "distributed_filter":
            # Split image into tiles
            grid_size = config.parameters.get("grid_size", [2, 2])
            num_tiles = grid_size[0] * grid_size[1]
            
            for row in range(grid_size[1]):
                for col in range(grid_size[0]):
                    tile_job = ComputeJob(
                        job_id="",
                        job_type="media",
                        input_data={
                            "media_file": asset_uri,
                            "media_type": "image",
                            "operation": "filter_tile",
                            "parameters": {
                                **config.parameters,
                                "tile_row": row,
                                "tile_col": col,
                                "grid_size": grid_size
                            }
                        },
                        output_location=f"output-data/media/{self.tenant_id}/tiles",
                        parameters={"tile": f"{row}_{col}"}
                    )
                    
                    job_id = self.orchestrator.submit_job(tile_job)
                    jobs.append(job_id)
            
            return {
                "status": "submitted",
                "tile_jobs": jobs,
                "grid_size": grid_size,
                "num_tiles": num_tiles,
                "message": f"Distributed image processing submitted with {num_tiles} tiles"
            }
        
        return {"error": f"Distributed operation {config.operation} not implemented"}
    
    def _detect_media_type(self, asset_uri: str) -> str:
        """Detect media type from URI."""
        ext = Path(asset_uri).suffix.lower()
        
        if ext in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff']:
            return 'image'
        elif ext in ['.mp4', '.avi', '.mov', '.mkv', '.wmv']:
            return 'video'
        elif ext in ['.mp3', '.wav', '.flac', '.aac', '.ogg']:
            return 'audio'
        
        return 'auto'
    
    def _handle_job_completion(self, job: ComputeJob):
        """Handle completed media processing job."""
        logger.info(f"Media job {job.job_id} completed with status {job.status}")
        
        if job.status == JobStatus.COMPLETED:
            # Check if this is part of a distributed job
            if "segment" in job.parameters or "tile" in job.parameters:
                self._check_distributed_completion(job)
    
    def _check_distributed_completion(self, job: ComputeJob):
        """Check if all parts of a distributed job are complete."""
        # This would check if all segments/tiles are done
        # and trigger the merge operation
        pass
    
    def submit_batch_processing(self, media_files: List[str],
                               operation: str,
                               parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Submit batch media processing jobs."""
        jobs = []
        
        for media_file in media_files:
            config = MediaProcessingConfig(
                mode="distributed_process",
                media_type="auto",
                operation=operation,
                parameters=parameters
            )
            
            result = self._submit_distributed_processing(media_file, config)
            jobs.append(result.get("job_id") or result)
        
        return {
            "batch_size": len(media_files),
            "jobs": jobs,
            "operation": operation
        }
    
    def create_media_pipeline(self, pipeline_config: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a media processing pipeline with multiple stages."""
        pipeline_id = f"pipeline_{self.tenant_id}_{int(time.time())}"
        stages = []
        
        previous_output = None
        
        for i, stage in enumerate(pipeline_config):
            stage_input = previous_output or stage.get("input")
            
            stage_job = ComputeJob(
                job_id="",
                job_type="media",
                input_data={
                    "media_file": stage_input,
                    "media_type": stage.get("media_type", "auto"),
                    "operation": stage["operation"],
                    "parameters": stage.get("parameters", {})
                },
                output_location=f"output-data/media/{self.tenant_id}/pipeline/{pipeline_id}/stage_{i}",
                parameters={
                    "pipeline_id": pipeline_id,
                    "stage": i,
                    "next_stage": i + 1 if i < len(pipeline_config) - 1 else None
                }
            )
            
            job_id = self.orchestrator.submit_job(stage_job)
            stages.append({
                "stage": i,
                "operation": stage["operation"],
                "job_id": job_id
            })
            
            previous_output = f"${{{job_id}.output}}"
        
        return {
            "pipeline_id": pipeline_id,
            "stages": stages,
            "total_stages": len(stages)
        }


if __name__ == '__main__':
    # Example usage
    processor = MediaProcessor("test-tenant")
    
    # Extract metadata
    metadata_config = {
        "mode": "metadata"
    }
    
    # Resize image
    resize_config = {
        "mode": "distributed_process",
        "media_type": "image",
        "operation": "resize",
        "parameters": {
            "width": 1920,
            "height": 1080,
            "maintain_aspect_ratio": True
        }
    }
    
    # Distributed video transcoding
    transcode_config = {
        "mode": "distributed_process",
        "media_type": "video",
        "operation": "distributed_transcode",
        "parameters": {
            "format": "mp4",
            "video_codec": "h264",
            "audio_codec": "aac",
            "video_bitrate": "5M",
            "segment_duration": 60
        }
    }
    
    # Audio enhancement pipeline
    audio_pipeline = [
        {
            "operation": "denoise",
            "media_type": "audio",
            "parameters": {"sensitivity": 0.25}
        },
        {
            "operation": "normalize",
            "parameters": {"target_level": -23}
        },
        {
            "operation": "compress",
            "parameters": {"ratio": 4, "threshold": -20}
        }
    ] 