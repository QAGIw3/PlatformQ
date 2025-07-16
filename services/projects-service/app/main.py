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
import uuid
from datetime import datetime
import requests # Added for making HTTP requests to VC service

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
    dao_contract_address: Optional[str] = Field(None, description="Optional blockchain address of the associated DAO smart contract")
    dao_did: Optional[str] = Field(None, description="Optional Decentralized Identifier (DID) of the associated DAO")


class ProjectUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=3, max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    active: Optional[bool] = None
    public: Optional[bool] = None
    dao_contract_address: Optional[str] = Field(None, description="Optional blockchain address of the associated DAO smart contract")
    dao_did: Optional[str] = Field(None, description="Optional Decentralized Identifier (DID) of the associated DAO")


class ProjectResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    openproject_id: int
    nextcloud_folder_path: str
    zulip_stream_name: str
    created_at: str
    status: str = "active"
    dao_contract_address: Optional[str] = None
    dao_did: Optional[str] = None


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
                main_folder,
                tenant_id,
                project_in.dao_did, # Pass dao_did to add_project_members
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
        project_data = {
            "id": str(uuid.uuid4()), # Placeholder for actual DB ID generation
            "name": project_in.name,
            "description": project_in.description,
            "openproject_id": op_project["id"],
            "nextcloud_folder_path": main_folder,
            "zulip_stream_name": project_in.name,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "status": "active",
            "dao_contract_address": project_in.dao_contract_address,
            "dao_did": project_in.dao_did,
        }

        # For now, we simulate storing and returning the project data
        # In a real implementation, you would call your CRUD layer here:
        # project = crud_project.create_project(db, tenant_id=tenant_id, project_data=project_data)

        # After project data is conceptualized/stored, issue a DAO creation VC if DAO details are provided
        if project_in.dao_did and app.state.config_loader.load_settings().get("VC_SERVICE_URL"):
            vc_service_url = app.state.config_loader.load_settings()["VC_SERVICE_URL"]
            dao_creation_subject = {
                "id": user.did, # Assuming user has a DID; otherwise, user.id
                "daoId": project_in.dao_did,
                "role": "creator",
                "createdAt": datetime.utcnow().isoformat() + "Z"
            }
            issue_vc_request = {
                "type": "DAOCreationCredential", # A new VC type for DAO creation
                "subject": dao_creation_subject,
                "store_on_ipfs": True
            }
            try:
                response = requests.post(f"{vc_service_url}/api/v1/issue", json=issue_vc_request)
                response.raise_for_status()
                logger.info(f"Issued DAOCreationCredential for DAO {project_in.dao_did}: {response.json().get('id')}")
            except Exception as e:
                logger.error(f"Failed to issue DAOCreationCredential for DAO {project_in.dao_did}: {e}")


        # 7. Publish event
        if hasattr(app.state, 'event_publisher'):
            try:
                app.state.event_publisher.publish(
                    topic_base="project-events",
                    tenant_id=tenant_id,
                    schema_class=ProjectCreatedEvent,
                    data=ProjectCreatedEvent(
                        tenant_id=tenant_id,
                        project_id=project_data["id"],
                        name=project_data["name"],
                        openproject_id=project_data["openproject_id"],
                        nextcloud_folder_path=project_data["nextcloud_folder_path"],
                        zulip_stream_name=project_data["zulip_stream_name"],
                        public=project_in.public,
                        dao_contract_address=project_in.dao_contract_address,
                        dao_did=project_in.dao_did,
                    )
                )
                logger.info(f"Published ProjectCreatedEvent for project {project_data['id']}")
            except Exception as e:
                logger.error(f"Failed to publish ProjectCreatedEvent for project {project_in.name}: {e}")

        return ProjectResponse(**project_data)
        
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
    nextcloud_folder: str,
    tenant_id: str,
    dao_did: Optional[str] = None, # Added dao_did parameter
):
    """Background task to add members to project"""
    logger.info(f"Adding members {member_emails} to project {project_name}")

    nextcloud = get_nextcloud_client()
    openproject = get_openproject_client()
    zulip = get_zulip_client()

    # Placeholder for actual user ID mapping based on email
    # In a real system, you would look up user_id and DID from auth-service
    member_data = []
    for email in member_emails:
        # Simulate user lookup and DID generation
        user_id = f"user-{email.split('@')[0]}" # Example user ID
        user_did = f"did:pqs:{user_id}" # Example DID
        member_data.append({"email": email, "user_id": user_id, "did": user_did})

    for member in member_data:
        try:
            # 1. Add to OpenProject (conceptual, as client needs enhancement)
            logger.info(f"Adding {member['email']} to OpenProject project {openproject_id}")
            # openproject.add_project_member(openproject_id, member["user_id"], "member")

            # 2. Share Nextcloud folder (conceptual)
            logger.info(f"Sharing Nextcloud folder {nextcloud_folder} with {member['email']}")
            # nextcloud.share_folder(nextcloud_folder, member["email"], "editor")
            
            # 3. Add to Zulip stream (conceptual)
            logger.info(f"Adding {member['email']} to Zulip stream {project_name}")
            # zulip.add_users_to_stream(project_name, [member["email"]])

            # 4. Issue DAOMembershipCredential
            if dao_did and app.state.config_loader.load_settings().get("VC_SERVICE_URL"):
                vc_service_url = app.state.config_loader.load_settings()["VC_SERVICE_URL"]
                membership_subject = {
                    "id": member["did"],
                    "daoId": dao_did, # Use the passed dao_did
                    "role": "member",
                    "joinedAt": datetime.utcnow().isoformat() + "Z"
                }
                issue_vc_request = {
                    "type": "DAOMembershipCredential",
                    "subject": membership_subject,
                    "store_on_ipfs": True
                }
                # In a real scenario, you would use an authenticated request
                response = requests.post(f"{vc_service_url}/api/v1/issue", json=issue_vc_request)
                response.raise_for_status() # Raise an exception for bad status codes
                logger.info(f"Issued DAOMembershipCredential for {member['email']}: {response.json().get('id')}")

        except Exception as e:
            logger.error(f"Failed to add {member['email']} to project {project_name}: {e}")


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
    def __init__(self, tenant_id: str, project_id: str, name: str, openproject_id: int, nextcloud_folder_path: str, zulip_stream_name: str, public: bool, dao_contract_address: Optional[str] = None, dao_did: Optional[str] = None):
        self.tenant_id = tenant_id
        self.project_id = project_id
        self.name = name
        self.openproject_id = openproject_id
        self.nextcloud_folder_path = nextcloud_folder_path
        self.zulip_stream_name = zulip_stream_name
        self.public = public
        self.dao_contract_address = dao_contract_address
        self.dao_did = dao_did
