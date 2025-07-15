import sys
import json
import FreeCAD
import Part

def extract_freecad_metadata(filepath):
    """
    Opens a FreeCAD file and extracts metadata and physical properties.
    """
    try:
        doc = FreeCAD.open(filepath)
    except Exception as e:
        error_message = {"error": f"Failed to open FreeCAD file: {e}"}
        print(json.dumps(error_message))
        sys.exit(1)

    metadata = {
        "document": {
            "name": doc.Name,
            "label": doc.Label,
            "filepath": doc.FileName,
        },
        "objects": [],
    }

    # Iterate through the objects in the document
    for obj in doc.Objects:
        obj_data = {
            "name": obj.Name,
            "label": obj.Label,
            "type": obj.TypeId,
        }
        
        # If the object has a shape, try to compute its properties
        if hasattr(obj, "Shape") and isinstance(obj.Shape, Part.Shape):
            shape = obj.Shape
            obj_data["geometry"] = {
                "volume": shape.Volume,
                "mass": shape.Mass,
                "area": shape.Area,
                "center_of_mass": {
                    "x": shape.CenterOfMass.x,
                    "y": shape.CenterOfMass.y,
                    "z": shape.CenterOfMass.z,
                },
                "bounding_box": {
                    "x_min": shape.BoundBox.XMin, "x_max": shape.BoundBox.XMax,
                    "y_min": shape.BoundBox.YMin, "y_max": shape.BoundBox.YMax,
                    "z_min": shape.BoundBox.ZMin, "z_max": shape.BoundBox.ZMax,
                }
            }
        
        metadata["objects"].append(obj_data)

    print(json.dumps(metadata, indent=4))
    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        error_message = {"error": "No FreeCAD file path provided."}
        print(json.dumps(error_message))
        sys.exit(1)
    else:
        extract_freecad_metadata(sys.argv[1]) 