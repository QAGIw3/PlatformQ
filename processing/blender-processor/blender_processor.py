"""Blender processor with distributed rendering capabilities."""
import os
import sys
import json
import logging
import tempfile
import subprocess
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
class BlenderProcessingConfig:
    """Configuration for Blender processing."""
    mode: str = "metadata"  # "metadata", "render", or "distributed_render"
    output_format: str = "PNG"
    render_engine: str = "CYCLES"
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class BlenderProcessor(ProcessorBase):
    """Processor for Blender files with distributed rendering capabilities."""
    
    def __init__(self, tenant_id: str, config: Optional[Dict[str, Any]] = None):
        super().__init__("BlenderProcessor", tenant_id, config)
        self.orchestrator = ComputeOrchestrator("blender-processor")
        self.blender_path = config.get("blender_path", "blender")
        
        # Register job completion handler
        self.orchestrator.register_job_handler("blender", self._handle_job_completion)
    
    def validate_config(self, processing_config: Dict[str, Any]) -> bool:
        """Validate processing configuration."""
        config = BlenderProcessingConfig(**processing_config)
        
        if config.mode not in ["metadata", "render", "distributed_render"]:
            raise ValueError(f"Invalid mode: {config.mode}")
        
        if config.render_engine not in ["CYCLES", "EEVEE", "WORKBENCH"]:
            raise ValueError(f"Invalid render engine: {config.render_engine}")
        
        valid_formats = ["PNG", "JPEG", "EXR", "TIFF", "BMP", "TGA"]
        if config.output_format not in valid_formats:
            raise ValueError(f"Invalid output format: {config.output_format}")
        
        return True
    
    def process(self, asset_uri: str, processing_config: Dict[str, Any]) -> Dict[str, Any]:
        """Process Blender file."""
        config = BlenderProcessingConfig(**processing_config)
        
        if config.mode == "metadata":
            return self._extract_metadata(asset_uri)
        elif config.mode == "render":
            return self._render_local(asset_uri, config)
        else:  # distributed_render
            return self._submit_distributed_render(asset_uri, config)
    
    def _extract_metadata(self, asset_uri: str) -> Dict[str, Any]:
        """Extract metadata from Blender file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download blend file
            local_blend = Path(temp_dir) / "scene.blend"
            self.download_from_s3(asset_uri, str(local_blend))
            
            # Create metadata extraction script
            script = """
import bpy
import json

scene = bpy.context.scene

metadata = {
    "scenes": [],
    "objects": {
        "total": len(bpy.data.objects),
        "by_type": {}
    },
    "meshes": len(bpy.data.meshes),
    "materials": [mat.name for mat in bpy.data.materials],
    "textures": len(bpy.data.textures),
    "images": [img.name for img in bpy.data.images],
    "render_settings": {}
}

# Count objects by type
for obj in bpy.data.objects:
    obj_type = obj.type
    if obj_type not in metadata["objects"]["by_type"]:
        metadata["objects"]["by_type"][obj_type] = 0
    metadata["objects"]["by_type"][obj_type] += 1

# Scene information
for scene in bpy.data.scenes:
    scene_info = {
        "name": scene.name,
        "frame_start": scene.frame_start,
        "frame_end": scene.frame_end,
        "fps": scene.render.fps,
        "resolution_x": scene.render.resolution_x,
        "resolution_y": scene.render.resolution_y,
        "render_engine": scene.render.engine,
        "camera": scene.camera.name if scene.camera else None
    }
    metadata["scenes"].append(scene_info)
    
    # Get active scene render settings
    if scene == bpy.context.scene:
        metadata["render_settings"] = {
            "engine": scene.render.engine,
            "resolution_x": scene.render.resolution_x,
            "resolution_y": scene.render.resolution_y,
            "resolution_percentage": scene.render.resolution_percentage,
            "fps": scene.render.fps,
            "frame_start": scene.frame_start,
            "frame_end": scene.frame_end,
            "file_format": scene.render.image_settings.file_format
        }
        
        # Engine-specific settings
        if scene.render.engine == "CYCLES":
            metadata["render_settings"]["cycles_samples"] = scene.cycles.samples
        elif scene.render.engine == "EEVEE":
            metadata["render_settings"]["eevee_samples"] = scene.eevee.taa_render_samples

print(json.dumps(metadata))
"""
            
            # Run Blender to extract metadata
            cmd = [
                self.blender_path,
                str(local_blend),
                "--background",
                "--python-expr", script
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Parse JSON from output
            metadata = None
            for line in result.stdout.split('\n'):
                if line.strip().startswith('{'):
                    try:
                        metadata = json.loads(line)
                        break
                    except json.JSONDecodeError:
                        continue
            
            if metadata is None:
                raise RuntimeError(f"Failed to extract metadata: {result.stderr}")
            
            return metadata
    
    def _render_local(self, asset_uri: str, config: BlenderProcessingConfig) -> Dict[str, Any]:
        """Render locally (for small jobs)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Download blend file
            local_blend = temp_path / "scene.blend"
            self.download_from_s3(asset_uri, str(local_blend))
            
            # Setup output
            output_dir = temp_path / "output"
            output_dir.mkdir()
            
            # Build render command
            params = config.parameters
            cmd = [
                self.blender_path,
                str(local_blend),
                "--background",
                "--render-output", str(output_dir / "render"),
                "--render-format", config.output_format,
                "--engine", config.render_engine
            ]
            
            # Add frame range
            if params.get("animation", False):
                cmd.extend([
                    "--frame-start", str(params.get("frame_start", 1)),
                    "--frame-end", str(params.get("frame_end", 250)),
                    "--render-anim"
                ])
            else:
                cmd.extend([
                    "--render-frame", str(params.get("frame", 1))
                ])
            
            # Run render
            logger.info(f"Running local render: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                raise RuntimeError(f"Render failed: {result.stderr}")
            
            # Upload results
            output_s3 = f"s3://{self.config['output_bucket']}/renders/{self.tenant_id}/"
            for file in output_dir.glob("*"):
                if file.is_file():
                    self.upload_to_s3(str(file), output_s3 + file.name)
            
            return {
                "status": "completed",
                "mode": "local",
                "output_location": output_s3
            }
    
    def _submit_distributed_render(self, asset_uri: str, 
                                  config: BlenderProcessingConfig) -> Dict[str, Any]:
        """Submit distributed rendering job."""
        params = config.parameters
        
        # Determine number of workers based on frame count
        if params.get("animation", False):
            frame_start = params.get("frame_start", 1)
            frame_end = params.get("frame_end", 250)
            total_frames = frame_end - frame_start + 1
            
            # Auto-scale workers based on frame count
            if total_frames <= 10:
                num_workers = 1
            elif total_frames <= 50:
                num_workers = min(5, total_frames)
            elif total_frames <= 200:
                num_workers = min(20, total_frames // 5)
            else:
                num_workers = min(100, total_frames // 10)
            
            # Allow manual override
            num_workers = params.get("num_workers", num_workers)
            
            # Split frames among workers
            frames_per_worker = total_frames // num_workers
            remainder = total_frames % num_workers
            
            jobs = []
            current_frame = frame_start
            
            for i in range(num_workers):
                worker_frames = frames_per_worker + (1 if i < remainder else 0)
                
                if worker_frames > 0:
                    frame_subset = list(range(current_frame, current_frame + worker_frames))
                    
                    job_params = params.copy()
                    job_params["frame_subset"] = frame_subset
                    
                    job = ComputeJob(
                        job_id="",
                        job_type="blender",
                        input_data={
                            "blend_file": asset_uri,
                            "output_format": config.output_format,
                            "parameters": job_params
                        },
                        output_location=f"output-data/blender/{self.tenant_id}",
                        parameters={
                            "worker_id": i,
                            "total_workers": num_workers
                        }
                    )
                    
                    job_id = self.orchestrator.submit_job(job)
                    jobs.append(job_id)
                    
                    current_frame += worker_frames
            
            return {
                "status": "submitted",
                "mode": "distributed",
                "total_frames": total_frames,
                "num_workers": num_workers,
                "job_ids": jobs,
                "message": f"Distributed render submitted with {num_workers} workers"
            }
        
        else:
            # Single frame render
            job = ComputeJob(
                job_id="",
                job_type="blender",
                input_data={
                    "blend_file": asset_uri,
                    "output_format": config.output_format,
                    "parameters": params
                },
                output_location=f"output-data/blender/{self.tenant_id}",
                parameters={}
            )
            
            job_id = self.orchestrator.submit_job(job)
            
            return {
                "status": "submitted",
                "mode": "distributed",
                "job_id": job_id,
                "message": "Single frame render job submitted"
            }
    
    def _handle_job_completion(self, job: ComputeJob):
        """Handle completed Blender render job."""
        logger.info(f"Blender job {job.job_id} completed with status {job.status}")
        
        if job.status == JobStatus.COMPLETED:
            # Check if this is part of a multi-worker animation
            if "total_workers" in job.parameters and job.parameters["total_workers"] > 1:
                self._check_animation_completion(job)
    
    def _check_animation_completion(self, job: ComputeJob):
        """Check if all workers for an animation have completed."""
        # This would check if all related jobs are done and trigger
        # video creation or final assembly
        pass
    
    def create_render_preview(self, job_id: str, frame: int = 1) -> Dict[str, Any]:
        """Create a quick preview render of a specific frame."""
        job = self.orchestrator.get_job_status(job_id)
        
        if not job:
            return {"error": "Job not found"}
        
        # Submit a quick preview job with reduced settings
        preview_params = job.input_data["parameters"].copy()
        preview_params.update({
            "frame": frame,
            "animation": False,
            "samples": 16,  # Low samples for preview
            "resolution_percentage": 50,  # Half resolution
            "denoising": True
        })
        
        preview_job = ComputeJob(
            job_id="",
            job_type="blender",
            input_data={
                "blend_file": job.input_data["blend_file"],
                "output_format": "JPEG",
                "parameters": preview_params
            },
            output_location=f"output-data/blender/{self.tenant_id}/previews",
            parameters={"preview_for": job_id}
        )
        
        preview_id = self.orchestrator.submit_job(preview_job)
        
        return {
            "preview_job_id": preview_id,
            "frame": frame,
            "message": "Preview render submitted"
        }


# Backward compatibility
def extract_blender_metadata(filepath: str) -> Dict[str, Any]:
    """Legacy function for metadata extraction."""
    processor = BlenderProcessor("default")
    return processor._extract_metadata(f"file://{filepath}")


if __name__ == '__main__':
    # Example usage
    processor = BlenderProcessor("test-tenant")
    
    # Metadata extraction
    metadata_config = {
        "mode": "metadata"
    }
    
    # Single frame render
    render_config = {
        "mode": "distributed_render",
        "output_format": "PNG",
        "render_engine": "CYCLES",
        "parameters": {
            "frame": 1,
            "samples": 128,
            "resolution_x": 1920,
            "resolution_y": 1080
        }
    }
    
    # Animation render
    animation_config = {
        "mode": "distributed_render",
        "output_format": "PNG",
        "render_engine": "CYCLES",
        "parameters": {
            "animation": True,
            "frame_start": 1,
            "frame_end": 100,
            "samples": 64
        }
    } 