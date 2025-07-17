from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List
from uuid import UUID
from datetime import datetime

from ..schemas.project import Project, ProjectCreate, Milestone
from ..schemas.openproject_webhook import OpenProjectWebhookPayload
from ..repository import ProjectRepository
from ..messaging.pulsar import pulsar_service
from .deps import get_current_tenant_and_user, get_project_repository

router = APIRouter()

@router.post("/", response_model=Project)
def create_project(
    *,
    project_in: ProjectCreate,
    context: dict = Depends(get_current_tenant_and_user),
    repo: ProjectRepository = Depends(get_project_repository)
):
    """
    Create a new project.
    """
    owner_id = str(context["user"].id)
    return repo.add(obj_in=project_in, owner_id=owner_id)

@router.get("/", response_model=List[Project])
def read_projects(
    context: dict = Depends(get_current_tenant_and_user),
    repo: ProjectRepository = Depends(get_project_repository)
):
    """
    Retrieve all projects.
    """
    return repo.list()

@router.get("/{project_id}", response_model=Project)
def read_project(
    *,
    project_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    repo: ProjectRepository = Depends(get_project_repository)
):
    """
    Get a project by ID.
    """
    project = repo.get(id=project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project

@router.post("/{project_id}/milestones/{milestone_id}/complete", response_model=Milestone)
def complete_milestone(
    *,
    project_id: UUID,
    milestone_id: UUID,
    context: dict = Depends(get_current_tenant_and_user),
    repo: ProjectRepository = Depends(get_project_repository)
):
    """
    Mark a milestone as complete.
    """
    user_id = str(context["user"].id)
    milestone = repo.complete_milestone(
        project_id=project_id,
        milestone_id=milestone_id,
        user_id=user_id
    )
    if not milestone:
        raise HTTPException(status_code=404, detail="Milestone or Project not found")
    return milestone

@router.post("/webhooks/openproject", status_code=204)
def openproject_webhook(
    payload: OpenProjectWebhookPayload,
    request: Request
):
    """
    Receive webhooks from OpenProject.
    """
    if payload.action == "work_package:updated":
        work_package = payload.work_package
        # In a real scenario, you'd want more robust parsing and validation
        if work_package.get("type") == "Milestone" and work_package.get("status") == "Closed":
            project_href = work_package["_links"]["project"]["href"]
            project_id = project_href.split("/")[-1]
            milestone_id = work_package["id"]

            # This is a simplification. In a real app, you would need a way
            # to associate the OpenProject user with a platform user.
            user_id = "system" # Placeholder

            event_data = {
                "projectId": str(project_id),
                "milestoneId": str(milestone_id),
                "userId": user_id,
                "completedAt": datetime.utcnow().isoformat()
            }
            pulsar_service.publish_event(event_data)
            print(f"Published milestone completion event: {event_data}")

    return 