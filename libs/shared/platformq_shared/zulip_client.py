import requests
import logging

logger = logging.getLogger(__name__)

class ZulipClient:
    def __init__(self, zulip_site: str, zulip_email: str, zulip_api_key: str):
        self.base_url = f"{zulip_site}/api/v1"
        self.auth = (zulip_email, zulip_api_key)
        self.headers = {"Content-Type": "application/json"}

    def create_stream(self, name: str, description: str = "") -> dict:
        """Creates a new stream (channel) in Zulip."""
        stream_data = {
            "subscriptions": [
                {
                    "name": name,
                    "description": description
                }
            ]
        }
        try:
            response = requests.post(
                f"{self.base_url}/users/me/subscriptions",
                json=stream_data,
                auth=self.auth,
                headers=self.headers
            )
            response.raise_for_status()
            logger.info(f"Created Zulip stream: {name}")
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to create Zulip stream: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while creating Zulip stream: {e}")
            raise 