"""Unified media worker for distributed image, video, and audio processing."""
import os
import sys
import json
import time
import subprocess
import tempfile
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.worker_base import ComputeWorker
from media_utils import (
    detect_media_type,
    split_video_segments,
    split_audio_segments,
    split_image_regions,
    process_image_batch,
    process_video_segment,
    process_audio_segment,
    merge_video_segments,
    merge_audio_segments,
    merge_image_tiles
)

logger = logging.getLogger(__name__)


class MediaWorker(ComputeWorker):
    """Worker for distributed media processing tasks."""
    
    def __init__(self):
        super().__init__("media")
        self.supported_operations = {
            "image": [
                "resize", "crop", "filter", "color_correction", "batch_convert",
                "watermark", "composite", "enhance", "denoise", "style_transfer"
            ],
            "video": [
                "transcode", "resize", "trim", "concatenate", "overlay",
                "stabilize", "color_grade", "add_effects", "extract_frames"
            ],
            "audio": [
                "transcode", "normalize", "denoise", "mix", "effects",
                "pitch_shift", "time_stretch", "equalize", "compress"
            ]
        }
    
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate media processing job."""
        required_fields = ["media_file", "media_type", "operation", "parameters"]
        
        for field in required_fields:
            if field not in job_data:
                raise ValueError(f"Missing required field: {field}")
        
        media_type = job_data["media_type"]
        operation = job_data["operation"]
        
        # Validate media type
        if media_type not in ["image", "video", "audio", "auto"]:
            raise ValueError(f"Invalid media type: {media_type}")
        
        # Validate operation
        if media_type != "auto" and operation not in self.supported_operations.get(media_type, []):
            raise ValueError(f"Unsupported operation '{operation}' for media type '{media_type}'")
        
        return True
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a media processing job."""
        job_id = job_data["job_id"]
        media_file = job_data["media_file"]
        media_type = job_data["media_type"]
        operation = job_data["operation"]
        parameters = job_data["parameters"]
        
        # Create temporary working directory
        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)
            
            try:
                # Download media file
                logger.info(f"Downloading media from {media_file}")
                local_media = work_path / "input_media"
                self.download_input_data(media_file, str(local_media))
                
                # Auto-detect media type if needed
                if media_type == "auto":
                    media_type = detect_media_type(local_media)
                    logger.info(f"Detected media type: {media_type}")
                
                # Process based on media type
                if media_type == "image":
                    result = self._process_image(
                        local_media, work_path, operation, parameters
                    )
                elif media_type == "video":
                    result = self._process_video(
                        local_media, work_path, operation, parameters
                    )
                elif media_type == "audio":
                    result = self._process_audio(
                        local_media, work_path, operation, parameters
                    )
                else:
                    raise ValueError(f"Unknown media type: {media_type}")
                
                # Upload results
                output_path = job_data.get("output_location", f"output-data/{job_id}")
                self._upload_results(work_path / "output", output_path)
                
                return {
                    "status": "completed",
                    "media_type": media_type,
                    "operation": operation,
                    "output_path": output_path,
                    "processing_stats": result
                }
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
    
    def _process_image(self, input_file: Path, work_dir: Path,
                      operation: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Process image operations."""
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        stats = {"operation": operation}
        
        if operation == "resize":
            output_file = output_dir / f"resized_{input_file.name}"
            width = parameters.get("width")
            height = parameters.get("height")
            maintain_aspect = parameters.get("maintain_aspect_ratio", True)
            
            cmd = ["convert", str(input_file)]
            
            if maintain_aspect:
                cmd.extend(["-resize", f"{width}x{height}"])
            else:
                cmd.extend(["-resize", f"{width}x{height}!"])
            
            cmd.append(str(output_file))
            subprocess.run(cmd, check=True)
            
            stats["output_size"] = f"{width}x{height}"
            
        elif operation == "batch_convert":
            # For batch operations, split work
            if parameters.get("batch_files"):
                results = process_image_batch(
                    parameters["batch_files"],
                    output_dir,
                    parameters
                )
                stats["processed_count"] = len(results)
            
        elif operation == "filter":
            filter_type = parameters.get("filter", "blur")
            output_file = output_dir / f"filtered_{input_file.name}"
            
            if filter_type == "blur":
                radius = parameters.get("radius", 5)
                cmd = ["convert", str(input_file), "-blur", f"0x{radius}", str(output_file)]
            elif filter_type == "sharpen":
                radius = parameters.get("radius", 2)
                cmd = ["convert", str(input_file), "-sharpen", f"0x{radius}", str(output_file)]
            elif filter_type == "edge":
                cmd = ["convert", str(input_file), "-edge", "3", str(output_file)]
            else:
                cmd = ["convert", str(input_file), str(output_file)]
            
            subprocess.run(cmd, check=True)
            stats["filter_applied"] = filter_type
            
        elif operation == "color_correction":
            output_file = output_dir / f"corrected_{input_file.name}"
            
            cmd = ["convert", str(input_file)]
            
            if "brightness" in parameters:
                cmd.extend(["-modulate", f"{100 + parameters['brightness']},100,100"])
            if "contrast" in parameters:
                cmd.extend(["-contrast-stretch", f"{parameters['contrast']}%"])
            if "saturation" in parameters:
                cmd.extend(["-modulate", f"100,{100 + parameters['saturation']},100"])
            
            cmd.append(str(output_file))
            subprocess.run(cmd, check=True)
            
        elif operation == "style_transfer":
            # This would use a neural style transfer model
            # Simplified version using GIMP filters
            style = parameters.get("style", "impressionist")
            output_file = output_dir / f"styled_{input_file.name}"
            
            self._run_gimp_script(
                input_file, output_file,
                "style_transfer.py",
                {"style": style}
            )
            
            stats["style"] = style
        
        return stats
    
    def _process_video(self, input_file: Path, work_dir: Path,
                      operation: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Process video operations."""
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        stats = {"operation": operation}
        
        if operation == "transcode":
            output_format = parameters.get("format", "mp4")
            output_file = output_dir / f"transcoded.{output_format}"
            
            cmd = ["ffmpeg", "-i", str(input_file)]
            
            # Video codec
            if "video_codec" in parameters:
                cmd.extend(["-c:v", parameters["video_codec"]])
            
            # Audio codec
            if "audio_codec" in parameters:
                cmd.extend(["-c:a", parameters["audio_codec"]])
            
            # Bitrate
            if "video_bitrate" in parameters:
                cmd.extend(["-b:v", parameters["video_bitrate"]])
            
            # Frame rate
            if "fps" in parameters:
                cmd.extend(["-r", str(parameters["fps"])])
            
            cmd.extend(["-y", str(output_file)])
            subprocess.run(cmd, check=True)
            
            stats["output_format"] = output_format
            
        elif operation == "resize":
            output_file = output_dir / f"resized_{input_file.name}"
            width = parameters.get("width", 1920)
            height = parameters.get("height", 1080)
            
            cmd = [
                "ffmpeg", "-i", str(input_file),
                "-vf", f"scale={width}:{height}",
                "-c:a", "copy",
                "-y", str(output_file)
            ]
            subprocess.run(cmd, check=True)
            
            stats["output_resolution"] = f"{width}x{height}"
            
        elif operation == "extract_frames":
            # Extract frames at specified interval
            interval = parameters.get("interval", 1.0)  # seconds
            output_pattern = output_dir / "frame_%06d.png"
            
            cmd = [
                "ffmpeg", "-i", str(input_file),
                "-vf", f"fps=1/{interval}",
                str(output_pattern)
            ]
            subprocess.run(cmd, check=True)
            
            # Count extracted frames
            frame_count = len(list(output_dir.glob("frame_*.png")))
            stats["frames_extracted"] = frame_count
            
        elif operation == "stabilize":
            # Video stabilization using ffmpeg
            output_file = output_dir / f"stabilized_{input_file.name}"
            
            # First pass - analyze
            cmd1 = [
                "ffmpeg", "-i", str(input_file),
                "-vf", "vidstabdetect=stepsize=32:shakiness=10:accuracy=15",
                "-f", "null", "-"
            ]
            subprocess.run(cmd1, check=True)
            
            # Second pass - apply stabilization
            cmd2 = [
                "ffmpeg", "-i", str(input_file),
                "-vf", "vidstabtransform=input=transforms.trf:zoom=5:smoothing=30",
                "-y", str(output_file)
            ]
            subprocess.run(cmd2, check=True)
            
            stats["stabilization_applied"] = True
        
        elif operation == "distributed_transcode":
            # Split video for distributed processing
            segments = split_video_segments(
                input_file,
                work_dir / "segments",
                parameters.get("segment_duration", 60)
            )
            
            # Process each segment (in real distributed scenario, these would be separate jobs)
            processed_segments = []
            for segment in segments:
                processed = process_video_segment(
                    segment,
                    output_dir,
                    parameters
                )
                processed_segments.append(processed)
            
            # Merge segments
            final_output = merge_video_segments(
                processed_segments,
                output_dir / "final_output.mp4"
            )
            
            stats["segments_processed"] = len(segments)
        
        return stats
    
    def _process_audio(self, input_file: Path, work_dir: Path,
                      operation: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Process audio operations."""
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        stats = {"operation": operation}
        
        if operation == "transcode":
            output_format = parameters.get("format", "mp3")
            output_file = output_dir / f"transcoded.{output_format}"
            
            cmd = ["ffmpeg", "-i", str(input_file)]
            
            # Audio codec
            codec_map = {
                "mp3": "libmp3lame",
                "aac": "aac",
                "flac": "flac",
                "ogg": "libvorbis"
            }
            codec = codec_map.get(output_format, "copy")
            cmd.extend(["-c:a", codec])
            
            # Bitrate
            if "bitrate" in parameters:
                cmd.extend(["-b:a", parameters["bitrate"]])
            
            # Sample rate
            if "sample_rate" in parameters:
                cmd.extend(["-ar", str(parameters["sample_rate"])])
            
            cmd.extend(["-y", str(output_file)])
            subprocess.run(cmd, check=True)
            
            stats["output_format"] = output_format
            
        elif operation == "normalize":
            output_file = output_dir / f"normalized_{input_file.name}"
            target_level = parameters.get("target_level", -23.0)
            
            # Using sox for normalization
            cmd = [
                "sox", str(input_file), str(output_file),
                "norm", str(target_level)
            ]
            subprocess.run(cmd, check=True)
            
            stats["normalization_level"] = target_level
            
        elif operation == "denoise":
            output_file = output_dir / f"denoised_{input_file.name}"
            
            # Using sox with noise reduction
            # First, create noise profile from quiet section
            noise_profile = work_dir / "noise.prof"
            
            # Extract noise sample (first 0.5 seconds)
            cmd1 = [
                "sox", str(input_file), "-n",
                "trim", "0", "0.5",
                "noiseprof", str(noise_profile)
            ]
            subprocess.run(cmd1, check=True)
            
            # Apply noise reduction
            sensitivity = parameters.get("sensitivity", 0.21)
            cmd2 = [
                "sox", str(input_file), str(output_file),
                "noisered", str(noise_profile), str(sensitivity)
            ]
            subprocess.run(cmd2, check=True)
            
            stats["noise_reduction_applied"] = True
            
        elif operation == "effects":
            effect_type = parameters.get("effect", "reverb")
            output_file = output_dir / f"effect_{input_file.name}"
            
            cmd = ["sox", str(input_file), str(output_file)]
            
            if effect_type == "reverb":
                reverberance = parameters.get("reverberance", 50)
                cmd.extend(["reverb", str(reverberance)])
            elif effect_type == "echo":
                delay = parameters.get("delay", 0.5)
                decay = parameters.get("decay", 0.5)
                cmd.extend(["echo", "0.8", "0.9", str(delay * 1000), str(decay)])
            elif effect_type == "pitch_shift":
                semitones = parameters.get("semitones", 0)
                cmd.extend(["pitch", str(semitones * 100)])
            
            subprocess.run(cmd, check=True)
            stats["effect_applied"] = effect_type
        
        return stats
    
    def _run_gimp_script(self, input_file: Path, output_file: Path,
                        script_name: str, parameters: Dict[str, Any]):
        """Run a GIMP script for advanced image processing."""
        script_path = Path("/app/processing_scripts") / script_name
        
        gimp_cmd = [
            "gimp", "-i", "-b",
            f"(python-fu-run-script \"{input_file}\" \"{output_file}\" '{json.dumps(parameters)}')",
            "-b", "(gimp-quit 0)"
        ]
        
        subprocess.run(gimp_cmd, check=True)
    
    def _upload_results(self, output_dir: Path, output_path: str):
        """Upload processed media to MinIO."""
        bucket, prefix = output_path.split('/', 1)
        
        # Upload all output files
        for file_path in output_dir.glob('*'):
            if file_path.is_file():
                object_name = f"{prefix}/{file_path.name}"
                self.minio_client.fput_object(bucket, object_name, str(file_path))
                logger.info(f"Uploaded {file_path.name}")


if __name__ == "__main__":
    # Create and run worker
    worker = MediaWorker()
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        worker.cleanup() 