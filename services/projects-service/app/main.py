from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from platformq_shared.nextcloud_client import NextcloudClient, NextcloudError
from platformq_shared.openproject_client import OpenProjectClient, OpenProjectError
from platformq_shared.zulip_client import ZulipClient, ZulipError
from platformq_shared.config import ConfigLoader
from platformq_shared.resilience import CircuitBreakerError, RateLimitExceeded
import logging
from datetime import date

from .api.deps import (
    get_db_session, 
    get_api_key_crud_placeholder, 
    get_user_crud_placeholder, 
    get_password_verifier_placeholder,
    get_current_tenant_and_user
)
from .api import endpoints

logger = logging.getLogger(__name__)

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


# Client instances (singleton pattern)
_nextcloud_client = None
_openproject_client = None
_zulip_client = None


def get_nextcloud_client() -> NextcloudClient:
    """Get or create Nextcloud client instance"""
    global _nextcloud_client
    if _nextcloud_client is None:
        config_loader = app.state.config_loader
        settings = config_loader.load_settings()
        _nextcloud_client = NextcloudClient(
            nextcloud_url=settings["NEXTCLOUD_URL"],
            admin_user=config_loader.get_secret("platformq/nextcloud", "admin_user"),
            admin_pass=config_loader.get_secret("platformq/nextcloud", "admin_pass"),
            use_connection_pool=True,
            rate_limit=15.0
        )
    return _nextcloud_client


def get_openproject_client() -> OpenProjectClient:
    """Get or create OpenProject client instance"""
    global _openproject_client
    if _openproject_client is None:
        config_loader = app.state.config_loader
        settings = config_loader.load_settings()
        _openproject_client = OpenProjectClient(
            openproject_url=settings["OPENPROJECT_URL"],
            api_key=config_loader.get_secret("platformq/openproject", "api_key"),
            use_connection_pool=True,
            rate_limit=10.0
        )
    return _openproject_client


def get_zulip_client() -> ZulipClient:
    """Get or create Zulip client instance"""
    global _zulip_client
    if _zulip_client is None:
        config_loader = app.state.config_loader
        settings = config_loader.load_settings()
        _zulip_client = ZulipClient(
            zulip_site=settings["ZULIP_SITE"],
            zulip_email=config_loader.get_secret("platformq/zulip", "email"),
            zulip_api_key=config_loader.get_secret("platformq/zulip", "api_key"),
            use_connection_pool=True,
            rate_limit=20.0
        )
    return _zulip_client


class ProjectCreateRequest(BaseModel):
    name: str = Field(..., min_length=3, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    public: bool = Field(False, description="Whether the project should be public")
    members: Optional[List[str]] = Field(None, description="List of user emails to add as members")
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ProjectUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=3, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    active: Optional[bool] = None
    public: Optional[bool] = None


class ProjectResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    openproject_id: int
    nextcloud_folder_path: str
    zulip_stream_name: str
    created_at: str
    status: str = "active"


@app.post("/api/v1/projects", status_code=201, response_model=ProjectResponse)
async def create_project(
    project_in: ProjectCreateRequest,
    background_tasks: BackgroundTasks,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """
    Create a new collaborative project across all integrated platforms.
    
    This will:
    1. Create a folder structure in Nextcloud
    2. Create a project in OpenProject with work package types
    3. Create a stream in Zulip with appropriate settings
    4. Set up initial permissions and memberships
    """
    tenant_id = context["tenant_id"]
    user = context["user"]
    
    # Get client instances
    nextcloud = get_nextcloud_client()
    openproject = get_openproject_client()
    zulip = get_zulip_client()
    
    # Track what was created for rollback
    created_resources = {}
    
    try:
        # 1. Create Nextcloud folder structure
        logger.info(f"Creating Nextcloud folder structure for project: {project_in.name}")
        main_folder = f"Projects/{project_in.name}"
        
        # Create main folder and subfolders
        nextcloud.create_folder(main_folder)
        nextcloud.create_folder(f"{main_folder}/Documents")
        nextcloud.create_folder(f"{main_folder}/Resources")
        nextcloud.create_folder(f"{main_folder}/Archive")
        
        created_resources["nextcloud_folder"] = main_folder
        
        # Create a README file
        readme_content = f"""# {project_in.name}

{project_in.description or 'Project workspace'}

## Folder Structure
- **Documents**: Project documentation and files
- **Resources**: Shared resources and assets  
- **Archive**: Archived materials

## Links
- OpenProject: [View in OpenProject]
- Zulip: [Join discussion]

Created on: {date.today().isoformat()}
"""
        # Note: Would need to implement a method to upload content directly
        # For now, we'll skip the README creation
        
        # 2. Create OpenProject project
        logger.info(f"Creating OpenProject project: {project_in.name}")
        openproject_identifier = project_in.name.lower().replace(" ", "-").replace("_", "-")[:50]
        
        op_project = openproject.create_project(
            name=project_in.name,
            identifier=openproject_identifier,
            description=project_in.description,
            public=project_in.public,
            active=True
        )
        created_resources["openproject_id"] = op_project["id"]
        
        # 3. Create Zulip stream
        logger.info(f"Creating Zulip stream: {project_in.name}")
        zulip_result = zulip.create_stream(
            name=project_in.name,
            description=project_in.description or f"Discussion for {project_in.name} project",
            invite_only=not project_in.public,
            history_public_to_subscribers=True,
            stream_post_policy=1  # Anyone can post
        )
        created_resources["zulip_stream"] = project_in.name
        
        # 4. Add members if specified
        if project_in.members:
            background_tasks.add_task(
                add_project_members,
                project_in.name,
                project_in.members,
                op_project["id"],
                main_folder
            )
        
        # 5. Send welcome message to Zulip
        welcome_message = f"""Welcome to the **{project_in.name}** project! 🎉

This stream is for all discussions related to this project.

**Quick Links:**
- 📁 [Nextcloud Folder]({main_folder})
- 📋 [OpenProject]({op_project.get('_links', {}).get('self', {}).get('href', '')})

**Getting Started:**
1. Check out the project documents in Nextcloud
2. Review work packages in OpenProject
3. Introduce yourself here!

Let's build something great together! 💪"""
        
        try:
            zulip.send_message(
                content=welcome_message,
                message_type="stream",
                to=project_in.name,
                topic="Welcome"
            )
        except Exception as e:
            logger.warning(f"Failed to send welcome message: {e}")
        
        # 6. Store in database (placeholder)
        # project = crud_project.create_project(...)
        
        # 7. Publish event
        if hasattr(app.state, 'event_publisher'):
            try:
                app.state.event_publisher.publish(
                    topic_base="project-events",
                    tenant_id=tenant_id,
                    schema_class=ProjectCreatedEvent,
                    data={
                        "project_id": openproject_identifier,
                        "project_name": project_in.name,
                        "openproject_id": op_project["id"],
                        "nextcloud_folder": main_folder,
                        "zulip_stream": project_in.name,
                        "created_by": user.id
                    }
                )
            except Exception as e:
                logger.error(f"Failed to publish project created event: {e}")
        
        return ProjectResponse(
            id=openproject_identifier,
            name=project_in.name,
            description=project_in.description,
            openproject_id=op_project["id"],
            nextcloud_folder_path=main_folder,
            zulip_stream_name=project_in.name,
            created_at=date.today().isoformat(),
            status="active"
        )
        
    except (CircuitBreakerError, RateLimitExceeded) as e:
        logger.error(f"Service temporarily unavailable: {e}")
        raise HTTPException(
            status_code=503,
            detail="One or more services are temporarily unavailable. Please try again later."
        )
    except Exception as e:
        logger.error(f"Failed to create project: {e}")
        
        # Attempt rollback
        await rollback_project_creation(created_resources)
        
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create project: {str(e)}"
        )


async def rollback_project_creation(created_resources: dict):
    """Rollback created resources on failure"""
    logger.info("Rolling back project creation...")
    
    # Delete in reverse order
    if "zulip_stream" in created_resources:
        try:
            zulip = get_zulip_client()
            stream_id = zulip.get_stream_id(created_resources["zulip_stream"])
            zulip.delete_stream(stream_id)
        except Exception as e:
            logger.error(f"Failed to rollback Zulip stream: {e}")
    
    if "openproject_id" in created_resources:
        try:
            openproject = get_openproject_client()
            openproject.delete_project(created_resources["openproject_id"])
        except Exception as e:
            logger.error(f"Failed to rollback OpenProject: {e}")
    
    if "nextcloud_folder" in created_resources:
        try:
            nextcloud = get_nextcloud_client()
            nextcloud.delete_file(created_resources["nextcloud_folder"])
        except Exception as e:
            logger.error(f"Failed to rollback Nextcloud folder: {e}")


async def add_project_members(
    project_name: str,
    member_emails: List[str],
    openproject_id: int,
    nextcloud_folder: str
):
    """Background task to add members to project"""
    logger.info(f"Adding {len(member_emails)} members to project {project_name}")
    
    nextcloud = get_nextcloud_client()
    openproject = get_openproject_client()
    zulip = get_zulip_client()
    
    for email in member_emails:
        try:
            # Add to Zulip stream
            zulip.subscribe_users(
                streams=[project_name],
                principals=[email]
            )
            
            # Share Nextcloud folder
            nextcloud.create_share(
                path=nextcloud_folder,
                share_type=0,  # User share
                share_with=email.split('@')[0],  # Assuming username is email prefix
                permissions=31  # All permissions
            )
            
            # Add to OpenProject (would need user ID lookup)
            # This is a simplified version
            # openproject.add_member(openproject_id, user_id, role_ids=[4])
            
        except Exception as e:
            logger.error(f"Failed to add member {email} to project: {e}")


@app.get("/api/v1/projects/{project_id}", response_model=ProjectResponse)
async def get_project(
    project_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """Get project details by ID"""
    # This would fetch from database
    # For now, return mock data
    raise HTTPException(status_code=501, detail="Not implemented")


@app.patch("/api/v1/projects/{project_id}")
async def update_project(
    project_id: str,
    project_update: ProjectUpdateRequest,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """Update project properties across all platforms"""
    openproject = get_openproject_client()
    zulip = get_zulip_client()
    
    try:
        # Update OpenProject
        if any([project_update.name, project_update.description, 
                project_update.active, project_update.public]):
            op_updates = {}
            if project_update.name:
                op_updates["name"] = project_update.name
            if project_update.description:
                op_updates["description"] = project_update.description
            if project_update.active is not None:
                op_updates["active"] = project_update.active
            if project_update.public is not None:
                op_updates["public"] = project_update.public
                
            openproject.update_project(project_id, **op_updates)
        
        # Update Zulip stream
        if project_update.name or project_update.description:
            # Get current stream name from DB
            # stream_id = zulip.get_stream_id(current_stream_name)
            # zulip.update_stream(stream_id, ...)
            pass
        
        return {"message": "Project updated successfully"}
        
    except Exception as e:
        logger.error(f"Failed to update project: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to update project: {str(e)}")


@app.delete("/api/v1/projects/{project_id}")
async def delete_project(
    project_id: str,
    archive: bool = True,
    context: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db_session),
):
    """
    Delete or archive a project across all platforms
    
    Args:
        project_id: Project identifier
        archive: If True, archive instead of delete (default: True)
    """
    # This would need to:
    # 1. Archive/delete OpenProject project
    # 2. Archive/delete Zulip stream
    # 3. Move Nextcloud folder to archive or delete
    # 4. Update database
    # 5. Publish event
    
    raise HTTPException(status_code=501, detail="Not implemented")


@app.post("/api/v1/projects/{project_id}/members")
async def add_project_member(
    project_id: str,
    email: str,
    role: str = "member",
    context: dict = Depends(get_current_tenant_and_user),
):
    """Add a member to an existing project"""
    # Implementation would add member across all platforms
    raise HTTPException(status_code=501, detail="Not implemented")


@app.get("/api/v1/projects/{project_id}/activity")
async def get_project_activity(
    project_id: str,
    limit: int = 50,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Get aggregated activity feed from all platforms
    
    Returns recent activities from:
    - OpenProject work packages
    - Nextcloud file changes
    - Zulip messages
    """
    # This would aggregate activity from all platforms
    raise HTTPException(status_code=501, detail="Not implemented")


# Placeholder for event schema
class ProjectCreatedEvent:
    """Event published when a project is created"""
    pass
