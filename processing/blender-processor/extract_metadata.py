import bpy
import json
import sys
import os

def extract_metadata():
    """
    Opens a .blend file and extracts rich metadata using Blender's Python API.
    """
    filepath = None
    try:
        # Blender's python scripts get passed args after '--'
        args = sys.argv[sys.argv.index("--") + 1:]
        if not args:
            raise ValueError("No .blend file path provided.")
        filepath = args[0]

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File not found: {filepath}")

        bpy.ops.wm.open_mainfile(filepath=filepath)
    except (ValueError, IndexError) as e:
        print(json.dumps({"error": f"Argument parsing error: {e}"}), file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        # Catching potential errors from bpy.ops.wm.open_mainfile
        print(json.dumps({"error": f"Failed to open .blend file '{filepath}': {e}"}), file=sys.stderr)
        sys.exit(1)

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

    # Print the extracted metadata as a JSON string to stdout
    print(json.dumps(metadata, indent=4))

if __name__ == "__main__":
    extract_metadata() 