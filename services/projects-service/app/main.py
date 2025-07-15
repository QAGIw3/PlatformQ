from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
# Assume these clients exist in our shared library now
# from shared_lib.openproject_client import OpenProjectClient
# from shared_lib.zulip_client import ZulipClient
# from shared_lib.nextcloud_client import NextcloudClient

from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder,
    get_current_tenant_and_user
)

app = create_base_app(
    service_name="projects-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["projects-service"])

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "projects-service is running"}

class ProjectCreateRequest(BaseModel):
    name: str

@app.post("/api/v1/projects", status_code=201)
def create_project(
    project_in: ProjectCreateRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """
    Orchestrates the creation of a new collaborative project across all
    integrated applications.
    """
    tenant_id = context["tenant_id"]
    user = context["user"]
    
    # In a real app, these clients would be initialized on startup
    # and their configs/secrets would come from the ConfigLoader.
    # openproject_client = OpenProjectClient(...)
    # nextcloud_client = NextcloudClient(...)
    # zulip_client = ZulipClient(...)

    # 1. Create the OpenProject project (conceptual)
    # openproject_id = openproject_client.create_project(name=project_in.name)
    # if not openproject_id:
    #     raise HTTPException(status_code=500, detail="Failed to create OpenProject project")

    # 2. Create the Nextcloud folder (conceptual)
    # folder_path = f"Projects/{project_in.name}"
    # nextcloud_client.create_folder(folder_path)

    # 3. Create the Zulip stream (conceptual)
    # zulip_client.create_stream(name=project_in.name, description=f"Channel for {project_in.name}")

    # 4. Store the links in our own database
    # crud_project.create_project(
    #     db=db, tenant_id=tenant_id, user_id=user.id,
    #     project_name=project_in.name,
    #     openproject_id=openproject_id,
    #     nextcloud_folder_path=folder_path,
    #     zulip_stream_name=project_in.name
    # )
    
    # 5. Publish an event
    # publisher.publish(topic_base="project-events", ...)

    return {"message": f"Project '{project_in.name}' created successfully across all platforms."}
