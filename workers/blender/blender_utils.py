"""Blender utility functions."""
import os
import struct
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any
import subprocess
import json

logger = logging.getLogger(__name__)


def validate_blend_file(blend_path: Path) -> bool:
    """Validate that a file is a valid Blender file."""
    try:
        with open(blend_path, 'rb') as f:
            # Check Blender file signature
            header = f.read(12)
            if len(header) < 12:
                return False
            
            # Blender files start with "BLENDER"
            if header[:7] != b'BLENDER':
                return False
            
            # Check pointer size and endianness
            pointer_size = header[7:8]
            if pointer_size not in [b'-', b'_']:  # 32-bit or 64-bit
                return False
            
            endianness = header[8:9]
            if endianness not in [b'v', b'V']:  # little or big endian
                return False
            
            # Check version (3 bytes)
            version = header[9:12]
            try:
                version_num = int(version)
                if version_num < 200 or version_num > 400:  # Reasonable version range
                    return False
            except:
                return False
            
        return True
    except Exception as e:
        logger.error(f"Error validating blend file: {e}")
        return False


def split_frame_range(start: int, end: int, num_workers: int) -> List[List[int]]:
    """Split a frame range into chunks for distributed rendering."""
    total_frames = end - start + 1
    frames_per_worker = total_frames // num_workers
    remainder = total_frames % num_workers
    
    chunks = []
    current_start = start
    
    for i in range(num_workers):
        # Distribute remainder frames among first workers
        chunk_size = frames_per_worker + (1 if i < remainder else 0)
        
        if chunk_size > 0:
            chunk_end = current_start + chunk_size - 1
            chunk_frames = list(range(current_start, chunk_end + 1))
            chunks.append(chunk_frames)
            current_start = chunk_end + 1
    
    return chunks


def setup_render_settings(settings: Dict[str, Any]) -> str:
    """Generate Blender Python code for render settings."""
    script_lines = ["import bpy", ""]
    
    # Render engine
    if "render_engine" in settings:
        script_lines.append(f"bpy.context.scene.render.engine = '{settings['render_engine']}'")
    
    # Resolution
    if "resolution_x" in settings:
        script_lines.append(f"bpy.context.scene.render.resolution_x = {settings['resolution_x']}")
    if "resolution_y" in settings:
        script_lines.append(f"bpy.context.scene.render.resolution_y = {settings['resolution_y']}")
    
    # Output format
    if "file_format" in settings:
        script_lines.append(f"bpy.context.scene.render.image_settings.file_format = '{settings['file_format']}'")
    
    # Cycles specific settings
    if settings.get("render_engine") == "CYCLES":
        if "samples" in settings:
            script_lines.append(f"bpy.context.scene.cycles.samples = {settings['samples']}")
        if "denoising" in settings:
            script_lines.append(f"bpy.context.scene.cycles.use_denoising = {settings['denoising']}")
        if "use_gpu" in settings and settings["use_gpu"]:
            script_lines.extend([
                "bpy.context.preferences.addons['cycles'].preferences.compute_device_type = 'CUDA'",
                "bpy.context.scene.cycles.device = 'GPU'",
                "# Enable all GPU devices",
                "for device in bpy.context.preferences.addons['cycles'].preferences.devices:",
                "    device.use = True"
            ])
    
    # EEVEE specific settings
    elif settings.get("render_engine") == "EEVEE":
        if "samples" in settings:
            script_lines.append(f"bpy.context.scene.eevee.taa_render_samples = {settings['samples']}")
        if "use_bloom" in settings:
            script_lines.append(f"bpy.context.scene.eevee.use_bloom = {settings['use_bloom']}")
        if "use_ssr" in settings:
            script_lines.append(f"bpy.context.scene.eevee.use_ssr = {settings['use_ssr']}")
    
    return "\n".join(script_lines)


def merge_render_outputs(output_dirs: List[Path], final_output: Path, 
                        file_format: str = "PNG") -> Dict[str, Any]:
    """Merge outputs from multiple render workers."""
    final_output.mkdir(parents=True, exist_ok=True)
    
    all_files = []
    total_size = 0
    
    # Collect all rendered files
    for output_dir in output_dirs:
        if output_dir.exists():
            for file_path in sorted(output_dir.glob(f"*.{file_format.lower()}")):
                all_files.append(file_path)
                total_size += file_path.stat().st_size
    
    # Copy files to final output, maintaining frame order
    frame_files = {}
    for file_path in all_files:
        # Extract frame number from filename
        try:
            # Assuming format like "frame_0001.png"
            frame_num = int(file_path.stem.split('_')[-1])
            frame_files[frame_num] = file_path
        except:
            # If can't parse, just copy with original name
            dest = final_output / file_path.name
            file_path.rename(dest)
    
    # Copy in order
    copied_files = []
    for frame_num in sorted(frame_files.keys()):
        src = frame_files[frame_num]
        dest = final_output / f"frame_{frame_num:04d}.{file_format.lower()}"
        src.rename(dest)
        copied_files.append(dest.name)
    
    return {
        "total_frames": len(copied_files),
        "total_size": total_size,
        "file_format": file_format,
        "output_path": str(final_output)
    }


def create_video_from_frames(frames_dir: Path, output_video: Path, 
                           fps: int = 24, codec: str = "h264") -> bool:
    """Create video from rendered frames using ffmpeg."""
    try:
        # Build ffmpeg command
        cmd = [
            "ffmpeg",
            "-framerate", str(fps),
            "-i", str(frames_dir / "frame_%04d.png"),
            "-c:v", codec,
            "-pix_fmt", "yuv420p",
            str(output_video)
        ]
        
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        if process.returncode != 0:
            logger.error(f"FFmpeg failed: {process.stderr}")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating video: {e}")
        return False


def estimate_render_time(blend_file: Path, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate render time based on scene complexity."""
    # This is a simplified estimation
    # In practice, you might want to do a quick test render
    
    estimates = {
        "per_frame_estimate": 60,  # Default 60 seconds per frame
        "total_frames": 1,
        "total_estimate": 60,
        "factors": []
    }
    
    # Adjust based on resolution
    res_x = settings.get("resolution_x", 1920)
    res_y = settings.get("resolution_y", 1080)
    pixel_count = res_x * res_y
    hd_pixels = 1920 * 1080
    
    if pixel_count > hd_pixels:
        factor = pixel_count / hd_pixels
        estimates["per_frame_estimate"] *= factor
        estimates["factors"].append(f"High resolution: {factor:.1f}x")
    
    # Adjust based on samples
    if settings.get("render_engine") == "CYCLES":
        samples = settings.get("samples", 128)
        if samples > 128:
            factor = samples / 128
            estimates["per_frame_estimate"] *= factor
            estimates["factors"].append(f"High samples: {factor:.1f}x")
    
    # Calculate total
    if "frame_start" in settings and "frame_end" in settings:
        estimates["total_frames"] = settings["frame_end"] - settings["frame_start"] + 1
    
    estimates["total_estimate"] = estimates["per_frame_estimate"] * estimates["total_frames"]
    
    return estimates


def get_scene_info(blend_file: Path) -> Dict[str, Any]:
    """Extract scene information from blend file."""
    script = """
import bpy
import json

info = {
    "scenes": [],
    "objects": len(bpy.data.objects),
    "meshes": len(bpy.data.meshes),
    "materials": len(bpy.data.materials),
    "textures": len(bpy.data.textures),
    "images": len(bpy.data.images),
}

for scene in bpy.data.scenes:
    scene_info = {
        "name": scene.name,
        "frame_start": scene.frame_start,
        "frame_end": scene.frame_end,
        "fps": scene.render.fps,
        "resolution": [scene.render.resolution_x, scene.render.resolution_y],
        "render_engine": scene.render.engine
    }
    info["scenes"].append(scene_info)

print(json.dumps(info))
"""
    
    try:
        # Run Blender in background to extract info
        cmd = [
            "blender",
            str(blend_file),
            "--background",
            "--python-expr", script
        ]
        
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Parse JSON from output
        for line in process.stdout.split('\n'):
            if line.startswith('{'):
                return json.loads(line)
        
        return {}
        
    except Exception as e:
        logger.error(f"Error getting scene info: {e}")
        return {} 