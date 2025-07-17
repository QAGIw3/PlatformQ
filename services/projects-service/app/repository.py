import uuid
import time
from typing import List, Optional
from uuid import UUID
from datetime import datetime

from ..schemas.project import Project, ProjectCreate, Milestone
from ..messaging.pulsar import pulsar_service
from ..openproject_client import OpenProjectClient
from platformq.shared.repository import AbstractRepository

class ProjectRepository(AbstractRepository[Project]):
    def __init__(self, openproject_client: OpenProjectClient):
        self.openproject_client = openproject_client

    def add(self, obj_in: ProjectCreate, owner_id: str) -> Project:
        project_data = {"title": obj_in.title, "description": obj_in.description}
        created_project = self.openproject_client.create_project(project_data)
        
        milestones = []
        for ms_in in obj_in.milestones:
            ms_data = {"title": ms_in.title, "description": ms_in.description}
            created_ms = self.openproject_client.create_work_package(created_project["id"], ms_data)
            milestones.append(Milestone(id=UUID(created_ms["id"]), **ms_in.dict(exclude={'id'})))

        return Project(
            id=UUID(created_project["id"]),
            owner_id=owner_id,
            title=created_project["name"],
            description=created_project["description"]["raw"],
            milestones=milestones,
        )

    def get(self, id: UUID) -> Optional[Project]:
        project_data = self.openproject_client.get_project(str(id))
        if project_data:
            return Project(
                id=UUID(project_data["id"]),
                owner_id="mock_owner",
                title=project_data["name"],
                description=project_data["description"]["raw"],
                milestones=[] 
            )
        return None

    def list(self) -> List[Project]:
        projects_data = self.openproject_client.get_projects()
        return [
            Project(
                id=UUID(p["id"]),
                owner_id="mock_owner",
                title=p["name"],
                description=p["description"]["raw"],
                milestones=[]
            ) for p in projects_data
        ]

    def complete_milestone(self, project_id: UUID, milestone_id: UUID, user_id: str) -> Optional[Milestone]:
        updated_package = self.openproject_client.update_work_package_status(str(milestone_id), "Closed")
        if updated_package:
            event_data = {
                "event_id": str(uuid.uuid4()),
                "project_id": str(project_id),
                "milestone_id": str(milestone_id),
                "user_id": user_id,
                "timestamp": int(time.time() * 1000)
            }
            pulsar_service.publish_event(event_data)
            return Milestone(
                id=UUID(updated_package["id"]),
                title=updated_package["subject"],
                description=updated_package["description"]["raw"],
                completed=True,
                completed_at=datetime.utcnow()
            )
        return None 