import requests
import logging

logger = logging.getLogger(__name__)

class NextcloudClient:
    def __init__(self, nextcloud_url: str, admin_user: str, admin_pass: str):
        self.base_url = f"{nextcloud_url}/ocs/v1.php/cloud"
        self.auth = (admin_user, admin_pass)
        self.headers = {"OCS-APIRequest": "true"}

    def create_folder(self, path: str):
        """Creates a folder in Nextcloud if it doesn't exist."""
        # Nextcloud's WebDAV endpoint is used for file/folder operations
        dav_url = f"{self.base_url.replace('/ocs/v1.php/cloud', '/remote.php/dav/files')}/{self.auth[0]}/{path}"
        try:
            response = requests.request("MKCOL", dav_url, auth=self.auth)
            if response.status_code not in [201, 405]: # 405 means it already exists
                response.raise_for_status()
            logger.info(f"Ensured folder exists at {path}")
            return True
        except Exception as e:
            logger.error(f"Failed to create Nextcloud folder at {path}: {e}")
            return False

    def upload_file(self, local_path: str, remote_path: str):
        """Uploads a file to a path in Nextcloud."""
        dav_url = f"{self.base_url.replace('/ocs/v1.php/cloud', '/remote.php/dav/files')}/{self.auth[0]}/{remote_path}"
        with open(local_path, 'rb') as f:
            try:
                response = requests.put(dav_url, data=f, auth=self.auth)
                response.raise_for_status()
                logger.info(f"Uploaded file to {remote_path}")
                return True
            except Exception as e:
                logger.error(f"Failed to upload file to Nextcloud: {e}")
                return False 