
import logging
import queue
import threading
import time
from typing import Optional, Dict, List
import secrets
import string

from platformq_shared.nextcloud_client import NextcloudClient, NextcloudError, NextcloudAuthError
from platformq_shared.resilience import CircuitBreakerError, RateLimitExceeded, get_metrics_collector

logger = logging.getLogger(__name__)


class NextcloudProvisioner:
    """
    Enhanced Nextcloud user provisioner with comprehensive user setup.
    
    Features:
    - User creation with secure random passwords
    - Personal folder structure creation
    - Group membership management
    - Quota assignment
    - Welcome file creation
    - Resilient error handling
    """
    
    def __init__(self, admin_user: str, admin_pass: str, nextcloud_url: str,
                 default_quota: str = "10GB", default_groups: List[str] = None):
        self.nextcloud_client = NextcloudClient(
            nextcloud_url=nextcloud_url,
            admin_user=admin_user,
            admin_pass=admin_pass,
            use_connection_pool=True,
            rate_limit=10.0
        )
        self.default_quota = default_quota
        self.default_groups = default_groups or ["users"]
        self.tasks = queue.Queue()
        self.stop_event = threading.Event()
        self.metrics = get_metrics_collector()
        
        # Track provisioning stats
        self.stats = {
            "users_created": 0,
            "users_failed": 0,
            "users_skipped": 0
        }

    def start(self) -> threading.Thread:
        """Starts the background worker thread."""
        logger.info("Starting enhanced Nextcloud provisioner worker thread...")
        worker_thread = threading.Thread(target=self._run, name="NextcloudProvisioner")
        worker_thread.daemon = True
        worker_thread.start()
        return worker_thread

    def stop(self):
        """Signals the worker thread to stop."""
        logger.info("Stopping Nextcloud provisioner worker thread...")
        logger.info(f"Provisioning stats: {self.stats}")
        self.stop_event.set()

    def add_user_to_queue(self, tenant_id: str, user_data: dict, priority: int = 5):
        """
        Adds a new user provisioning task to the queue.
        
        Args:
            tenant_id: Tenant identifier
            user_data: User data with email, full_name, etc.
            priority: Task priority (lower = higher priority)
        """
        self.tasks.put((priority, {
            "tenant_id": tenant_id,
            "user_data": user_data,
            "timestamp": time.time()
        }))

    def _generate_secure_password(self, length: int = 24) -> str:
        """Generate a secure random password."""
        alphabet = string.ascii_letters + string.digits + string.punctuation
        # Remove problematic characters
        alphabet = alphabet.replace('"', '').replace("'", '').replace('\\', '')
        return ''.join(secrets.choice(alphabet) for _ in range(length))

    def _run(self):
        """The main loop for the worker thread."""
        while not self.stop_event.is_set():
            try:
                # Get task with priority handling
                priority, task = self.tasks.get(timeout=1)
                
                # Check if task is too old (> 5 minutes)
                if time.time() - task["timestamp"] > 300:
                    logger.warning(f"Skipping stale provisioning task for tenant {task['tenant_id']}")
                    self.stats["users_skipped"] += 1
                    continue
                
                self._provision_user(task["tenant_id"], task["user_data"])
                self.tasks.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Unexpected error in provisioner worker: {e}", exc_info=True)
                self.stats["users_failed"] += 1

    def _provision_user(self, tenant_id: str, user_data: dict):
        """
        Provision a user in Nextcloud with full setup.
        
        This includes:
        1. Creating the user account
        2. Setting up personal folder structure
        3. Adding to appropriate groups
        4. Setting quota
        5. Creating welcome content
        """
        email = user_data.get("email")
        full_name = user_data.get("full_name", email)
        username = user_data.get("username", email.split("@")[0])
        groups = user_data.get("groups", self.default_groups)
        quota = user_data.get("quota", self.default_quota)
        
        if not email:
            logger.error(f"No email provided for user provisioning in tenant {tenant_id}")
            return
            
        logger.info(f"Provisioning user {email} in Nextcloud for tenant {tenant_id}")
        
        start_time = time.time()
        
        try:
            # 1. Create user account
            # Generate secure password (will use SSO, but NC requires a password)
            temp_password = self._generate_secure_password()
            
            try:
                created = self.nextcloud_client.create_user(
                    username=username,
                    password=temp_password,
                    email=email,
                    display_name=full_name,
                    groups=groups,
                    quota=quota
                )
                
                if created:
                    logger.info(f"Successfully created user {username} for tenant {tenant_id}")
                    self.stats["users_created"] += 1
                    
            except NextcloudError as e:
                if "already exists" in str(e).lower():
                    logger.warning(f"User {username} already exists for tenant {tenant_id}")
                    self.stats["users_skipped"] += 1
                    # Continue to ensure user setup is complete
                else:
                    raise
            
            # 2. Create personal folder structure
            self._create_user_folders(username, tenant_id)
            
            # 3. Create welcome file
            self._create_welcome_content(username, full_name, tenant_id)
            
            # 4. Additional setup based on tenant
            if tenant_id:
                # Add tenant-specific group if it exists
                tenant_group = f"tenant_{tenant_id}"
                try:
                    self.nextcloud_client.add_user_to_group(username, tenant_group)
                except Exception as e:
                    logger.debug(f"Could not add user to tenant group {tenant_group}: {e}")
            
            # Record metrics
            duration = time.time() - start_time
            self.metrics.record_request(
                service="nextcloud-provisioner",
                endpoint="provision_user",
                duration=duration,
                status_code=200
            )
            
            logger.info(f"Completed provisioning for {username} in {duration:.2f}s")
            
        except (CircuitBreakerError, RateLimitExceeded) as e:
            logger.error(f"Service temporarily unavailable: {e}")
            # Re-queue the task for later
            self.add_user_to_queue(tenant_id, user_data, priority=1)
            
        except Exception as e:
            logger.error(f"Failed to provision user {email} for tenant {tenant_id}: {e}", exc_info=True)
            self.stats["users_failed"] += 1
            
            # Record failure metrics
            self.metrics.record_request(
                service="nextcloud-provisioner",
                endpoint="provision_user",
                duration=time.time() - start_time,
                status_code=500,
                error=str(e)
            )

    def _create_user_folders(self, username: str, tenant_id: str):
        """Create standard folder structure for new user."""
        folders = [
            f"{username}/Documents",
            f"{username}/Projects",
            f"{username}/Personal",
            f"{username}/Shared",
            f"{username}/Archive"
        ]
        
        for folder in folders:
            try:
                self.nextcloud_client.create_folder(folder)
                logger.debug(f"Created folder: {folder}")
            except NextcloudError as e:
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create folder {folder}: {e}")

    def _create_welcome_content(self, username: str, full_name: str, tenant_id: str):
        """Create welcome content for new user."""
        welcome_content = f"""# Welcome to Nextcloud, {full_name}!

Welcome to your personal cloud storage. Here's a quick guide to get you started:

## Your Folder Structure

We've created the following folders for you:
- **Documents**: For your work documents
- **Projects**: For project-related files  
- **Personal**: For personal files
- **Shared**: For files shared with others
- **Archive**: For archived content

## Getting Started

1. **Desktop Sync**: Download the Nextcloud desktop client to sync files
2. **Mobile Apps**: Get our mobile apps for iOS and Android
3. **Share Files**: Right-click any file to share with colleagues
4. **Collaborate**: Use the built-in document editor for real-time collaboration

## Need Help?

- Check our [User Guide](https://docs.nextcloud.com)
- Contact IT Support at support@example.com

---
*This account was created for tenant: {tenant_id}*
"""
        
        # Note: Would need to implement a method to upload content directly
        # For now, this is a placeholder for future implementation
        logger.debug(f"Would create welcome file for {username}")

    def get_stats(self) -> Dict[str, int]:
        """Get provisioning statistics."""
        return self.stats.copy()

    def bulk_provision_users(self, tenant_id: str, users: List[Dict], 
                           batch_size: int = 10, delay: float = 0.5):
        """
        Bulk provision multiple users with rate limiting.
        
        Args:
            tenant_id: Tenant identifier
            users: List of user data dictionaries
            batch_size: Number of users to process before pausing
            delay: Delay between batches in seconds
        """
        logger.info(f"Starting bulk provisioning of {len(users)} users for tenant {tenant_id}")
        
        for i, user_data in enumerate(users):
            self.add_user_to_queue(tenant_id, user_data, priority=3)
            
            # Rate limiting for bulk operations
            if (i + 1) % batch_size == 0:
                logger.info(f"Queued {i + 1}/{len(users)} users, pausing for {delay}s")
                time.sleep(delay)
                
        logger.info(f"Completed queuing {len(users)} users for provisioning") 