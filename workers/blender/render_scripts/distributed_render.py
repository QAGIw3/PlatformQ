"""Blender script for distributed rendering setup."""
import bpy
import sys
import json
import os


def setup_distributed_render(job_config):
    """Configure Blender for distributed rendering."""
    
    # Set render engine
    engine = job_config.get("render_engine", "CYCLES")
    bpy.context.scene.render.engine = engine
    
    # Configure output
    output_path = job_config.get("output_path", "/tmp/render/")
    bpy.context.scene.render.filepath = output_path
    
    # Set file format
    file_format = job_config.get("file_format", "PNG")
    bpy.context.scene.render.image_settings.file_format = file_format
    
    # Resolution settings
    if "resolution_x" in job_config:
        bpy.context.scene.render.resolution_x = job_config["resolution_x"]
    if "resolution_y" in job_config:
        bpy.context.scene.render.resolution_y = job_config["resolution_y"]
    
    # Frame range for this worker
    if "frame_subset" in job_config:
        frames = job_config["frame_subset"]
        if frames:
            bpy.context.scene.frame_start = min(frames)
            bpy.context.scene.frame_end = max(frames)
    
    # Engine-specific settings
    if engine == "CYCLES":
        setup_cycles(job_config)
    elif engine == "EEVEE":
        setup_eevee(job_config)
    
    # GPU settings
    if job_config.get("use_gpu", False):
        enable_gpu_rendering()


def setup_cycles(config):
    """Configure Cycles render engine."""
    cycles = bpy.context.scene.cycles
    
    # Samples
    cycles.samples = config.get("samples", 128)
    
    # Denoising
    cycles.use_denoising = config.get("denoising", True)
    
    # Light paths
    if "max_bounces" in config:
        cycles.max_bounces = config["max_bounces"]
    
    # Performance
    cycles.use_persistent_data = True
    cycles.tile_order = 'HILBERT_SPIRAL'
    
    # Adaptive sampling
    if config.get("adaptive_sampling", True):
        cycles.use_adaptive_sampling = True
        cycles.adaptive_threshold = config.get("adaptive_threshold", 0.01)


def setup_eevee(config):
    """Configure EEVEE render engine."""
    eevee = bpy.context.scene.eevee
    
    # Samples
    eevee.taa_render_samples = config.get("samples", 64)
    
    # Effects
    eevee.use_bloom = config.get("use_bloom", False)
    eevee.use_ssr = config.get("use_ssr", False)
    eevee.use_motion_blur = config.get("use_motion_blur", False)
    
    # Shadows
    eevee.shadow_cube_size = config.get("shadow_cube_size", "1024")
    eevee.shadow_cascade_size = config.get("shadow_cascade_size", "1024")


def enable_gpu_rendering():
    """Enable GPU rendering for Cycles."""
    prefs = bpy.context.preferences
    cycles_prefs = prefs.addons['cycles'].preferences
    
    # Set compute device type
    cycles_prefs.compute_device_type = 'CUDA'  # or 'OPTIX' for RTX cards
    
    # Enable all available GPUs
    cycles_prefs.get_devices()
    for device in cycles_prefs.devices:
        if device.type == 'CUDA':
            device.use = True
            print(f"Enabled GPU: {device.name}")
    
    # Set scene to use GPU
    bpy.context.scene.cycles.device = 'GPU'


def optimize_scene_for_distributed_render():
    """Optimize scene settings for distributed rendering."""
    
    # Disable unnecessary features
    scene = bpy.context.scene
    
    # Simplify modifiers for viewport (not render)
    scene.render.use_simplify = False  # Keep full quality for render
    
    # Memory optimization
    scene.render.use_save_buffers = True
    scene.render.use_persistent_data = True
    
    # Disable auto-save during render
    bpy.context.preferences.filepaths.use_auto_save_temporary_files = False
    
    # Set optimal tile sizes based on device
    if bpy.context.scene.cycles.device == 'GPU':
        # Larger tiles for GPU
        scene.render.tile_x = 256
        scene.render.tile_y = 256
    else:
        # Smaller tiles for CPU
        scene.render.tile_x = 64
        scene.render.tile_y = 64


def render_frame_subset(frames):
    """Render specific frames for distributed processing."""
    scene = bpy.context.scene
    
    for frame in frames:
        scene.frame_set(frame)
        
        # Set output path with frame number
        base_path = scene.render.filepath
        if not base_path.endswith('/'):
            base_path += '_'
        
        scene.render.filepath = f"{base_path}{frame:04d}"
        
        # Render frame
        print(f"Rendering frame {frame}...")
        bpy.ops.render.render(write_still=True)
        
        # Reset filepath
        scene.render.filepath = base_path


if __name__ == "__main__":
    # Read job configuration from command line or environment
    if len(sys.argv) > 1:
        config_path = sys.argv[-1]
        with open(config_path, 'r') as f:
            job_config = json.load(f)
    else:
        # Default configuration
        job_config = {
            "render_engine": "CYCLES",
            "samples": 128,
            "use_gpu": True,
            "file_format": "PNG"
        }
    
    # Setup and optimize
    setup_distributed_render(job_config)
    optimize_scene_for_distributed_render()
    
    # Render if frames specified
    if "frame_subset" in job_config:
        render_frame_subset(job_config["frame_subset"]) 