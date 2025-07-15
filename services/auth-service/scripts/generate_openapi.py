import json
import os
from app.main import app

# This is a bit of a hack to make sure the script can find the app module.
# In a more complex setup, this might be handled by installing the package.
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def generate_openapi_spec():
    """
    Generates the OpenAPI JSON specification and saves it to a file
    at the root of the project.
    """
    output_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "docs", "auth-service-openapi.json"
    )
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    spec = app.openapi()
    
    with open(output_path, "w") as f:
        json.dump(spec, f, indent=2)
        
    print(f"OpenAPI spec generated successfully at {output_path}")

if __name__ == "__main__":
    generate_openapi_spec() 