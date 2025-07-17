"""Blender worker for distributed rendering."""
import os
import sys
import json
import time
import shutil
import subprocess
import tempfile
import logging
from typing import Dict, Any, List, Tuple
from pathlib import Path

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from base.worker_base import ComputeWorker
from blender_utils import (
    validate_blend_file,
    split_frame_range,
    setup_render_settings,
    merge_render_outputs
)

logger = logging.getLogger(__name__)


class BlenderWorker(ComputeWorker):
    """Worker for distributed Blender rendering."""
    
    def __init__(self):
        super().__init__("blender")
        self.blender_path = os.getenv('BLENDER_PATH', '/opt/blender/blender')
        self.enable_gpu = os.getenv('ENABLE_GPU', 'false').lower() == 'true'
        
        # Supported render engines
        self.supported_engines = ['CYCLES', 'EEVEE', 'WORKBENCH']
        
        # Start Xvfb for headless rendering
        self._start_xvfb()
    
    def _start_xvfb(self):
        """Start X virtual framebuffer for headless rendering."""
        try:
            # Start Xvfb on display :99
            subprocess.Popen([
                'Xvfb', ':99', '-screen', '0', '1920x1080x24',
                '-ac', '+extension', 'GLX', '+render', '-noreset'
            ])
            os.environ['DISPLAY'] = ':99'
            time.sleep(2)  # Give Xvfb time to start
            logger.info("Started Xvfb for headless rendering")
        except Exception as e:
            logger.warning(f"Failed to start Xvfb: {e}")
    
    def validate_job(self, job_data: Dict[str, Any]) -> bool:
        """Validate Blender rendering job."""
        required_fields = ["blend_file", "output_format", "parameters"]
        
        for field in required_fields:
            if field not in job_data:
                raise ValueError(f"Missing required field: {field}")
        
        params = job_data["parameters"]
        
        # Validate render engine
        if "render_engine" in params:
            if params["render_engine"] not in self.supported_engines:
                raise ValueError(f"Unsupported render engine: {params['render_engine']}")
        
        # Validate frame range
        if "frame_start" in params and "frame_end" in params:
            if params["frame_start"] > params["frame_end"]:
                raise ValueError("frame_start must be <= frame_end")
        
        # Validate output format
        valid_formats = ['PNG', 'JPEG', 'EXR', 'TIFF', 'BMP', 'TGA']
        if job_data["output_format"] not in valid_formats:
            raise ValueError(f"Invalid output format: {job_data['output_format']}")
        
        return True
    
    def process_job(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a Blender rendering job."""
        job_id = job_data["job_id"]
        blend_file_path = job_data["blend_file"]
        output_format = job_data["output_format"]
        parameters = job_data["parameters"]
        
        # Create temporary working directory
        with tempfile.TemporaryDirectory() as work_dir:
            work_path = Path(work_dir)
            
            try:
                # Download blend file
                logger.info(f"Downloading blend file from {blend_file_path}")
                local_blend = work_path / "scene.blend"
                self.download_input_data(blend_file_path, str(local_blend))
                
                # Validate blend file
                if not validate_blend_file(local_blend):
                    raise ValueError("Invalid or corrupted blend file")
                
                # Determine render type
                if parameters.get("animation", False):
                    result = self._render_animation(
                        local_blend, work_path, output_format, parameters
                    )
                else:
                    result = self._render_still(
                        local_blend, work_path, output_format, parameters
                    )
                
                # Upload results
                output_path = job_data.get("output_location", f"output-data/{job_id}")
                self._upload_results(work_path / "output", output_path)
                
                return {
                    "status": "completed",
                    "render_engine": parameters.get("render_engine", "CYCLES"),
                    "output_format": output_format,
                    "frames_rendered": result.get("frames_rendered", 1),
                    "render_time": result.get("render_time", 0),
                    "output_path": output_path,
                    "gpu_used": self.enable_gpu
                }
                
            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                raise
    
    def _render_still(self, blend_file: Path, work_dir: Path, 
                     output_format: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Render a single frame."""
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Prepare render script
        render_script = self._create_render_script(
            output_dir, output_format, parameters, animation=False
        )
        script_path = work_dir / "render_script.py"
        with open(script_path, 'w') as f:
            f.write(render_script)
        
        # Build Blender command
        cmd = [
            self.blender_path,
            str(blend_file),
            "--background",
            "--python", str(script_path),
            "--render-frame", str(parameters.get("frame", 1))
        ]
        
        # Add GPU flag if enabled
        if self.enable_gpu:
            cmd.extend(["--", "--cycles-device", "GPU"])
        
        # Run render
        start_time = time.time()
        logger.info(f"Running Blender render: {' '.join(cmd)}")
        
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(work_dir)
        )
        
        if process.returncode != 0:
            raise RuntimeError(f"Blender render failed: {process.stderr}")
        
        render_time = time.time() - start_time
        
        return {
            "frames_rendered": 1,
            "render_time": render_time
        }
    
    def _render_animation(self, blend_file: Path, work_dir: Path,
                         output_format: str, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Render animation frames."""
        output_dir = work_dir / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Get frame range
        frame_start = parameters.get("frame_start", 1)
        frame_end = parameters.get("frame_end", 250)
        frame_step = parameters.get("frame_step", 1)
        
        # For distributed rendering, this worker might handle a subset of frames
        if "frame_subset" in parameters:
            frames_to_render = parameters["frame_subset"]
        else:
            frames_to_render = list(range(frame_start, frame_end + 1, frame_step))
        
        # Prepare render script
        render_script = self._create_render_script(
            output_dir, output_format, parameters, animation=True
        )
        script_path = work_dir / "render_script.py"
        with open(script_path, 'w') as f:
            f.write(render_script)
        
        # Render each frame
        start_time = time.time()
        frames_rendered = 0
        
        for frame in frames_to_render:
            cmd = [
                self.blender_path,
                str(blend_file),
                "--background",
                "--python", str(script_path),
                "--render-frame", str(frame)
            ]
            
            if self.enable_gpu:
                cmd.extend(["--", "--cycles-device", "GPU"])
            
            logger.info(f"Rendering frame {frame}")
            
            process = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(work_dir)
            )
            
            if process.returncode != 0:
                logger.error(f"Failed to render frame {frame}: {process.stderr}")
                continue
            
            frames_rendered += 1
        
        render_time = time.time() - start_time
        
        return {
            "frames_rendered": frames_rendered,
            "render_time": render_time,
            "frames_list": frames_to_render
        }
    
    def _create_render_script(self, output_dir: Path, output_format: str,
                             parameters: Dict[str, Any], animation: bool) -> str:
        """Create Blender Python script for rendering."""
        script = f"""
import bpy
import os

# Set render engine
bpy.context.scene.render.engine = '{parameters.get("render_engine", "CYCLES")}'

# Set output settings
bpy.context.scene.render.filepath = '{output_dir}/'
bpy.context.scene.render.image_settings.file_format = '{output_format}'

# Set resolution if specified
if {parameters.get('resolution_x', 0)} > 0:
    bpy.context.scene.render.resolution_x = {parameters.get('resolution_x', 1920)}
if {parameters.get('resolution_y', 0)} > 0:
    bpy.context.scene.render.resolution_y = {parameters.get('resolution_y', 1080)}

# Set samples if specified
if '{parameters.get("render_engine", "CYCLES")}' == 'CYCLES':
    bpy.context.scene.cycles.samples = {parameters.get('samples', 128)}
    bpy.context.scene.cycles.use_denoising = {str(parameters.get('denoising', True))}
    
    # GPU settings
    if {str(self.enable_gpu).lower()}:
        bpy.context.preferences.addons['cycles'].preferences.compute_device_type = 'CUDA'
        bpy.context.scene.cycles.device = 'GPU'
        
        # Enable all GPU devices
        for device in bpy.context.preferences.addons['cycles'].preferences.devices:
            device.use = True

# Set quality settings
bpy.context.scene.render.resolution_percentage = {parameters.get('resolution_percentage', 100)}

# Animation settings
if {str(animation).lower()}:
    bpy.context.scene.frame_start = {parameters.get('frame_start', 1)}
    bpy.context.scene.frame_end = {parameters.get('frame_end', 250)}
    bpy.context.scene.frame_step = {parameters.get('frame_step', 1)}
"""
        
        # Add custom Python code if provided
        if "custom_script" in parameters:
            script += f"\n# Custom user script\n{parameters['custom_script']}\n"
        
        return script
    
    def _upload_results(self, output_dir: Path, output_path: str):
        """Upload rendered images to MinIO."""
        bucket, prefix = output_path.split('/', 1)
        
        # Upload all rendered images
        for file_path in output_dir.glob('*'):
            if file_path.is_file():
                object_name = f"{prefix}/{file_path.name}"
                self.minio_client.fput_object(bucket, object_name, str(file_path))
                logger.info(f"Uploaded {file_path.name}")


if __name__ == "__main__":
    # Create and run worker
    worker = BlenderWorker()
    
    try:
        worker.run()
    except KeyboardInterrupt:
        logger.info("Worker interrupted by user")
    finally:
        worker.cleanup() 