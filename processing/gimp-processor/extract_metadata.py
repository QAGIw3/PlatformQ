#!/usr/bin/env python

import sys
import json
from gimpfu import *

def extract_gimp_metadata(filepath):
    """
    Opens an image file in GIMP and extracts metadata.
    """
    try:
        # The script receives the filepath as an argument from the entrypoint
        img = pdb.gimp_file_load(filepath, filepath)
    except Exception as e:
        # GIMP's error handling can be tricky; we'll return a generic error
        error_message = {"error": "Failed to open image file in GIMP. It may be corrupt or an unsupported format."}
        print(json.dumps(error_message))
        pdb.gimp_quit(1) # Quit GIMP with a non-zero exit code
        return

    metadata = {
        "image": {
            "name": img.name,
            "width": img.width,
            "height": img.height,
            "base_type": img.base_type, # e.g., RGB, Grayscale
        },
        "counts": {
            "layers": len(img.layers),
            "channels": len(img.channels),
            "paths": len(img.vectors),
        },
        "layers": [layer.name for layer in img.layers],
    }

    # Print the extracted metadata as a JSON string
    print(json.dumps(metadata, indent=4))
    
    # Quit GIMP cleanly
    pdb.gimp_quit(0)

if __name__ == '__main__':
    # GIMP passes the script path as the first arg, so the file is the second
    if len(sys.argv) < 2:
        error_message = {"error": "No image file path provided to GIMP script."}
        print(json.dumps(error_message))
        # No need to quit here as GIMP hasn't been fully loaded
    else:
        extract_gimp_metadata(sys.argv[1]) 