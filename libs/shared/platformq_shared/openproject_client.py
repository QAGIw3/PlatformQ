import requests
import logging

logger = logging.getLogger(__name__)

class OpenProjectClient:
    def __init__(self, openproject_url: str, api_key: str):
        self.base_url = f"{openproject_url}/api/v3"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"ApiKey {api_key}"
        }

    def create_project(self, name: str, identifier: str) -> dict:
        """Creates a new project in OpenProject."""
        project_data = {
            "_links": {
                "type": {
                    "href": "/api/v3/types/1", # Default Project Type
                    "title": "Project"
                }
            },
            "name": name,
            "identifier": identifier,
            "active": True,
            "public": False
        }
        try:
            response = requests.post(f"{self.base_url}/projects", json=project_data, headers=self.headers)
            response.raise_for_status()
            logger.info(f"Created OpenProject project: {name}")
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to create OpenProject project: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while creating OpenProject project: {e}")
            raise 