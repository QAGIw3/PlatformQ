from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from git import Repo
import os
import tempfile

# ... (app creation using the base factory) ...

# In a real system, this would come from Vault/Consul
TEMPLATE_REPO_URL = "https://github.com/your-company/service-templates.git"

@app.get("/api/v1/templates")
def list_templates():
    """
    Clones the template repository and lists the available service templates.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            Repo.clone_from(TEMPLATE_REPO_URL, tmpdir)
            # The templates are the directories in the root of the repo
            templates = [d for d in os.listdir(tmpdir) if os.path.isdir(os.path.join(tmpdir, d)) and not d.startswith('.')]
            return {"templates": templates}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to clone or list templates: {e}")

@app.get("/api/v1/templates/{template_name}")
def get_template_details(template_name: str):
    """
    Fetches details about a specific template (e.g., its files).
    """
    # ... (Similar logic to clone the repo and list files in the template's dir) ...
    return {"message": "Details for template (conceptual)"} 