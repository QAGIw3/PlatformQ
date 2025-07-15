import sys
import json

def extract_openshot_metadata(filepath):
    """
    Parses an OpenShot project file (.osp) and extracts metadata.
    """
    try:
        with open(filepath, 'r') as f:
            project_data = json.load(f)

        metadata = {
            "openshot_project": {
                "name": project_data.get("name"),
                "profile": project_data.get("profile"),
                "fps": project_data.get("fps"),
                "width": project_data.get("width"),
                "height": project_data.get("height"),
                "channel_layout": project_data.get("channel_layout"),
                "sample_rate": project_data.get("sample_rate"),
                "counts": {
                    "tracks": len(project_data.get("tracks", [])),
                    "clips": len(project_data.get("clips", [])),
                    "effects": len(project_data.get("effects", [])),
                    "files": len(project_data.get("files", [])),
                },
                "media_files": [f.get("path") for f in project_data.get("files", [])]
            }
        }
        
        print(json.dumps(metadata, indent=4))

    except Exception as e:
        error_message = {"error": f"Failed to process OpenShot file: {e}"}
        print(json.dumps(error_message))
        sys.exit(1)

    sys.exit(0)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        error_message = {"error": "No .osp file path provided."}
        print(json.dumps(error_message))
        sys.exit(1)
    else:
        extract_openshot_metadata(sys.argv[1]) 