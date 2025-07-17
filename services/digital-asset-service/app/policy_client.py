import httpx
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class PolicyClient:
    def __init__(self, policy_service_url: str):
        self.policy_service_url = policy_service_url

    async def check_permission(self, subject: Dict[str, Any], action: str, resource: Dict[str, Any]) -> bool:
        """
        Checks if a user has permission to perform an action on a resource.
        """
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.policy_service_url}/evaluate",
                    json={
                        "subject": subject,
                        "action": action,
                        "resource": resource,
                    }
                )
                response.raise_for_status()
                return response.json().get("allow", False)
        except httpx.HTTPError as e:
            logger.error(f"Error checking permission: {e}")
            return False

policy_client = PolicyClient("http://policy-service:8000") 