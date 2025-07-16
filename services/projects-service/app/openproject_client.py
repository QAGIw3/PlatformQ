from typing import Dict, Any, List, Optional
from uuid import uuid4, UUID

# This is a mock client to simulate interactions with an OpenProject API.
# It uses an in-memory store to mimic the behavior of a real API.

class MockOpenProjectClient:
    def __init__(self):
        self._projects: Dict[str, Dict[str, Any]] = {}
        self._work_packages: Dict[str, Dict[str, Any]] = {}

    def create_project(self, project_data: Dict[str, Any]) -> Dict[str, Any]:
        project_id = str(uuid4())
        project = {
            "id": project_id,
            "name": project_data["title"],
            "description": {"raw": project_data.get("description", "")},
        }
        self._projects[project_id] = project
        return project

    def get_project(self, project_id: str) -> Optional[Dict[str, Any]]:
        return self._projects.get(project_id)

    def get_projects(self) -> List[Dict[str, Any]]:
        return list(self._projects.values())

    def update_work_package_status(self, work_package_id: str, status: str) -> Optional[Dict[str, Any]]:
        work_package = self._work_packages.get(work_package_id)
        if work_package and work_package.get("type") == "Milestone":
            work_package["status"] = status
            return work_package
        return None
    
    def create_work_package(self, project_id: str, package_data: Dict[str, Any]) -> Dict[str, Any]:
        package_id = str(uuid4())
        package = {
            "id": package_id,
            "subject": package_data["title"],
            "description": {"raw": package_data.get("description", "")},
            "type": "Milestone", # Assuming all work packages created are milestones for now
            "status": "new",
            "_links": {
                "project": {
                    "href": f"/api/v3/projects/{project_id}"
                }
            }
        }
        self._work_packages[package_id] = package
        return package

openproject_client = MockOpenProjectClient() 