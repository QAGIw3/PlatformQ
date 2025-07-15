
import logging
import requests
import os
import queue
import threading
import time

logger = logging.getLogger(__name__)

class NextcloudProvisioner:
    def __init__(self, admin_user, admin_pass, nextcloud_url):
        self.auth = (admin_user, admin_pass)
        self.nextcloud_url = nextcloud_url
        self.tasks = queue.Queue()
        self.stop_event = threading.Event()

    def start(self):
        """Starts the background worker thread."""
        logger.info("Starting Nextcloud provisioner worker thread...")
        worker_thread = threading.Thread(target=self._run)
        worker_thread.daemon = True
        worker_thread.start()
        return worker_thread

    def stop(self):
        """Signals the worker thread to stop."""
        logger.info("Stopping Nextcloud provisioner worker thread...")
        self.stop_event.set()

    def add_user_to_queue(self, tenant_id: str, user_data: dict):
        """Adds a new user provisioning task to the queue."""
        self.tasks.put({"tenant_id": tenant_id, "user_data": user_data})

    def _run(self):
        """The main loop for the worker thread."""
        while not self.stop_event.is_set():
            try:
                task = self.tasks.get(timeout=1)
                self._provision_user(task["tenant_id"], task["user_data"])
                self.tasks.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred in the provisioner worker: {e}")

    def _provision_user(self, tenant_id: str, user_data: dict):
        """Calls the Nextcloud User Provisioning API to create a user."""
        email = user_data.email
        full_name = user_data.full_name
        logger.info(f"Provisioning user in Nextcloud for tenant {tenant_id}: {email}")

        url = f"{self.nextcloud_url}/ocs/v1.php/cloud/users"
        headers = {"OCS-APIRequest": "true"}

        payload = {
            "userid": email,
            "password": "a-long-random-password-that-will-not-be-used", # Password is not used due to SSO
            "email": email,
            "displayName": full_name,
        }

        try:
            response = requests.post(url, headers=headers, auth=self.auth, data=payload, timeout=15)
            response.raise_for_status()
            logger.info(f"Successfully provisioned user {email} in Nextcloud for tenant {tenant_id}.")
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 400 and "already exists" in e.response.text:
                logger.warning(f"User {email} already exists in Nextcloud for tenant {tenant_id}.")
            else:
                logger.error(f"Failed to provision user in Nextcloud for tenant {tenant_id}: {e.response.text if e.response else e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"A network error occurred while trying to provision user in Nextcloud: {e}") 