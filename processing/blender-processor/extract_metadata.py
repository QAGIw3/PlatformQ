import bpy
import json
import sys
import os
from platformq_shared.processor_utils import run_processor

def extract_blender_metadata(filepath):
    """
    Opens a .blend file and extracts rich metadata using Blender's Python API.
    """
        bpy.ops.wm.open_mainfile(filepath=filepath)

    # --- Metadata Extraction ---
    
    scene = bpy.context.scene
    
    # Scene-level information
    metadata = {
        "scene": {
            "name": scene.name,
            "frame_start": scene.frame_start,
            "frame_end": scene.frame_end,
            "render_engine": scene.render.engine,
            "render_resolution_x": scene.render.resolution_x,
            "render_resolution_y": scene.render.resolution_y,
        },
        "counts": {
            "objects": len(bpy.data.objects),
            "meshes": len(bpy.data.meshes),
            "materials": len(bpy.data.materials),
            "textures": len(bpy.data.textures),
            "images": len(bpy.data.images),
            "cameras": len([obj for obj in scene.objects if obj.type == 'CAMERA']),
            "lights": len([obj for obj in scene.objects if obj.type == 'LIGHT']),
        },
        "materials": [mat.name for mat in bpy.data.materials],
        "image_textures": [img.name for img in bpy.data.images if img.source == 'FILE'],
    }

    return metadata

if __name__ == "__main__":
    run_processor(extract_blender_metadata) 