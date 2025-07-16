from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from shared_lib.nextcloud_client import NextcloudClient
from shared_lib.openproject_client import OpenProjectClient
from shared_lib.zulip_client import ZulipClient
from platformq_shared.config import ConfigLoader

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
    
    config_loader = app.state.config_loader
    settings = config_loader.load_settings()

    nextcloud_client = NextcloudClient(
        nextcloud_url=settings["NEXTCLOUD_URL"],
        admin_user=config_loader.get_secret("platformq/nextcloud", "admin_user"),
        admin_pass=config_loader.get_secret("platformq/nextcloud", "admin_pass"),
    )

    # 1. Create the Nextcloud folder
    folder_path = f"Projects/{project_in.name}"
    success = nextcloud_client.create_folder(folder_path)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to create Nextcloud folder")

    openproject_client = OpenProjectClient(
        openproject_url=settings["OPENPROJECT_URL"],
        api_key=config_loader.get_secret("platformq/openproject", "api_key"),
    )

    # 2. Create the OpenProject project
    try:
        # OpenProject requires a unique identifier, often a URL-friendly version of the name
        openproject_identifier = project_in.name.lower().replace(" ", "-")
        openproject_response = openproject_client.create_project(name=project_in.name, identifier=openproject_identifier)
        openproject_id = openproject_response["id"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create OpenProject project: {e}")

    zulip_client = ZulipClient(
        zulip_site=settings["ZULIP_SITE"],
        zulip_email=config_loader.get_secret("platformq/zulip", "email"),
        zulip_api_key=config_loader.get_secret("platformq/zulip", "api_key"),
    )

    # 3. Create the Zulip stream
    try:
        zulip_client.create_stream(name=project_in.name, description=f"Channel for {project_in.name}")
        zulip_stream_name = project_in.name
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create Zulip stream: {e}")

    # 4. Store the links in our own database
    # crud_project.create_project(
    #     db=db, tenant_id=tenant_id, user_id=user.id,
    #     project_name=project_in.name,
    #     openproject_id=openproject_id,
    #     nextcloud_folder_path=folder_path,
    #     zulip_stream_name=zulip_stream_name
    # )
    
    # 5. Publish an event
    # publisher.publish(topic_base="project-events", ...)

    return {"message": f"Project '{project_in.name}' created successfully across all platforms."}
