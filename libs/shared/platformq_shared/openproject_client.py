import requests
import logging
from typing import Dict, List, Optional, Union, Any
import json
from datetime import datetime, date
from urllib.parse import quote, urlencode
import time
from functools import wraps
import os

from .resilience import (
    with_resilience, 
    get_connection_pool_manager,
    get_metrics_collector,
    CircuitBreakerError,
    RateLimitExceeded
)

logger = logging.getLogger(__name__)


class OpenProjectError(Exception):
    """Base exception for OpenProject client errors"""
    pass


class OpenProjectAuthError(OpenProjectError):
    """Authentication related errors"""
    pass


class OpenProjectNotFoundError(OpenProjectError):
    """Resource not found errors"""
    pass


class OpenProjectValidationError(OpenProjectError):
    """Validation errors"""
    pass


def retry_on_failure(max_retries: int = 3, backoff_factor: float = 1.0):
    """Decorator for retry logic with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (requests.ConnectionError, requests.Timeout) as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        sleep_time = backoff_factor * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed, retrying in {sleep_time}s: {e}")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"All {max_retries} attempts failed")
            raise last_exception
        return wrapper
    return decorator


class OpenProjectClient:
    """
    Comprehensive OpenProject client with support for:
    - Project management
    - Work packages
    - User management
    - Memberships and roles
    - Time tracking
    - Custom fields
    - Attachments
    - Queries and filtering
    """
    
    def __init__(self, openproject_url: str, api_key: str, 
                 timeout: int = 30, verify_ssl: bool = True,
                 use_connection_pool: bool = True, rate_limit: float = 15.0):
        self.base_url = f"{openproject_url.rstrip('/')}/api/v3"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {api_key}"  # OpenProject uses Basic auth with API key
        }
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        
        # Connection pooling
        self.use_connection_pool = use_connection_pool
        self.rate_limit = rate_limit
        if use_connection_pool:
            self.session = get_connection_pool_manager().get_session(
                self.base_url,
                headers=self.headers,
                timeout=self.timeout
            )
        else:
            self.session = None
        
    def _handle_response(self, response: requests.Response) -> Dict:
        """Handle API response and raise appropriate exceptions"""
        if response.status_code == 401:
            raise OpenProjectAuthError("Authentication failed")
        elif response.status_code == 404:
            raise OpenProjectNotFoundError("Resource not found")
        elif response.status_code == 422:
            try:
                error_data = response.json()
                errors = error_data.get('_embedded', {}).get('errors', [])
                error_messages = [err.get('message', '') for err in errors]
                raise OpenProjectValidationError(f"Validation failed: {', '.join(error_messages)}")
            except:
                raise OpenProjectValidationError(f"Validation failed: {response.text}")
        elif response.status_code >= 400:
            raise OpenProjectError(f"API Error {response.status_code}: {response.text}")
            
        try:
            return response.json()
        except json.JSONDecodeError:
            return {}
    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make HTTP request using session if available"""
        if self.session:
            return self.session.request(method, url, verify=self.verify_ssl, **kwargs)
        else:
            kwargs.setdefault('headers', self.headers)
            kwargs.setdefault('timeout', self.timeout)
            kwargs.setdefault('verify', self.verify_ssl)
            return requests.request(method, url, **kwargs)

    # Project Management
    @with_resilience("openproject", rate_limit=15.0)
    def create_project(self, name: str, identifier: str, description: str = None,
                       parent_id: int = None, public: bool = False, 
                       active: bool = True, custom_fields: Dict = None) -> Dict:
        """Create a new project"""
        url = f"{self.base_url}/projects"
        project_data = {
            "name": name,
            "identifier": identifier,
            "active": active,
            "public": public,
            "_links": {}
        }
        
        if description:
            project_data["description"] = {
                "format": "markdown",
                "raw": description
            }
            
        if parent_id:
            project_data["_links"]["parent"] = {"href": f"/api/v3/projects/{parent_id}"}
            
        if custom_fields:
            for field_name, value in custom_fields.items():
                project_data[field_name] = value
                
        response = self._request("post".upper(), url, json=project_data)
        result = self._handle_response(response)
        logger.info(f"Successfully created project: {name}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def get_project(self, project_id: Union[int, str]) -> Dict:
        """Get project details by ID or identifier"""
        url = f"{self.base_url}/projects/{project_id}"
        response = self._request("get".upper(), url)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def update_project(self, project_id: Union[int, str], name: str = None,
                       description: str = None, active: bool = None,
                       public: bool = None, custom_fields: Dict = None) -> Dict:
        """Update project properties"""
        url = f"{self.base_url}/projects/{project_id}"
        data = {}
        
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = {
                "format": "markdown",
                "raw": description
            }
        if active is not None:
            data["active"] = active
        if public is not None:
            data["public"] = public
        if custom_fields:
            data.update(custom_fields)
            
        response = self._request("patch".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully updated project {project_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def delete_project(self, project_id: Union[int, str]) -> bool:
        """Delete a project"""
        url = f"{self.base_url}/projects/{project_id}"
        response = self._request("delete".upper(), url)
        if response.status_code == 204:
            logger.info(f"Successfully deleted project {project_id}")
            return True
        self._handle_response(response)
        return False
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_projects(self, filters: List[Dict] = None, offset: int = 0,
                      page_size: int = 100, sort_by: str = None) -> Dict:
        """List projects with optional filtering"""
        url = f"{self.base_url}/projects"
        params = {
            "offset": offset,
            "pageSize": page_size
        }
        
        if filters:
            filter_string = json.dumps(filters)
            params["filters"] = filter_string
            
        if sort_by:
            params["sortBy"] = sort_by
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    # Work Package Management
    @with_resilience("openproject", rate_limit=15.0)
    def create_work_package(self, project_id: Union[int, str], subject: str,
                           type_id: int = 1, description: str = None,
                           assignee_id: int = None, status_id: int = None,
                           priority_id: int = None, parent_id: int = None,
                           start_date: date = None, due_date: date = None,
                           estimated_hours: float = None, custom_fields: Dict = None) -> Dict:
        """Create a new work package"""
        url = f"{self.base_url}/work_packages"
        
        wp_data = {
            "subject": subject,
            "_links": {
                "project": {"href": f"/api/v3/projects/{project_id}"},
                "type": {"href": f"/api/v3/types/{type_id}"}
            }
        }
        
        if description:
            wp_data["description"] = {
                "format": "markdown",
                "raw": description
            }
            
        if assignee_id:
            wp_data["_links"]["assignee"] = {"href": f"/api/v3/users/{assignee_id}"}
        if status_id:
            wp_data["_links"]["status"] = {"href": f"/api/v3/statuses/{status_id}"}
        if priority_id:
            wp_data["_links"]["priority"] = {"href": f"/api/v3/priorities/{priority_id}"}
        if parent_id:
            wp_data["_links"]["parent"] = {"href": f"/api/v3/work_packages/{parent_id}"}
            
        if start_date:
            wp_data["startDate"] = start_date.isoformat()
        if due_date:
            wp_data["dueDate"] = due_date.isoformat()
        if estimated_hours is not None:
            wp_data["estimatedTime"] = f"PT{int(estimated_hours)}H"
            
        if custom_fields:
            for field_name, value in custom_fields.items():
                wp_data[field_name] = value
                
        response = self._request("post".upper(), url, json=wp_data)
        result = self._handle_response(response)
        logger.info(f"Successfully created work package: {subject}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def get_work_package(self, work_package_id: int) -> Dict:
        """Get work package details"""
        url = f"{self.base_url}/work_packages/{work_package_id}"
        response = self._request("get".upper(), url)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def update_work_package(self, work_package_id: int, subject: str = None,
                           description: str = None, assignee_id: int = None,
                           status_id: int = None, priority_id: int = None,
                           start_date: date = None, due_date: date = None,
                           estimated_hours: float = None, percentage_done: int = None,
                           custom_fields: Dict = None, lock_version: int = None) -> Dict:
        """Update work package properties"""
        url = f"{self.base_url}/work_packages/{work_package_id}"
        
        data = {}
        if lock_version is not None:
            data["lockVersion"] = lock_version
            
        if subject is not None:
            data["subject"] = subject
        if description is not None:
            data["description"] = {
                "format": "markdown",
                "raw": description
            }
            
        links = {}
        if assignee_id is not None:
            links["assignee"] = {"href": f"/api/v3/users/{assignee_id}"}
        if status_id is not None:
            links["status"] = {"href": f"/api/v3/statuses/{status_id}"}
        if priority_id is not None:
            links["priority"] = {"href": f"/api/v3/priorities/{priority_id}"}
            
        if links:
            data["_links"] = links
            
        if start_date is not None:
            data["startDate"] = start_date.isoformat()
        if due_date is not None:
            data["dueDate"] = due_date.isoformat()
        if estimated_hours is not None:
            data["estimatedTime"] = f"PT{int(estimated_hours)}H"
        if percentage_done is not None:
            data["percentageDone"] = percentage_done
            
        if custom_fields:
            data.update(custom_fields)
            
        response = self._request("patch".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully updated work package {work_package_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def delete_work_package(self, work_package_id: int) -> bool:
        """Delete a work package"""
        url = f"{self.base_url}/work_packages/{work_package_id}"
        response = self._request("delete".upper(), url)
        if response.status_code == 204:
            logger.info(f"Successfully deleted work package {work_package_id}")
            return True
        self._handle_response(response)
        return False
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_work_packages(self, project_id: Union[int, str] = None,
                          filters: List[Dict] = None, offset: int = 0,
                          page_size: int = 100, sort_by: str = None) -> Dict:
        """List work packages with optional filtering"""
        url = f"{self.base_url}/work_packages"
        params = {
            "offset": offset,
            "pageSize": page_size
        }
        
        filter_list = []
        if project_id:
            filter_list.append({
                "project": {
                    "operator": "=",
                    "values": [str(project_id)]
                }
            })
            
        if filters:
            filter_list.extend(filters)
            
        if filter_list:
            params["filters"] = json.dumps(filter_list)
            
        if sort_by:
            params["sortBy"] = sort_by
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def add_work_package_comment(self, work_package_id: int, comment: str) -> Dict:
        """Add a comment to a work package"""
        url = f"{self.base_url}/work_packages/{work_package_id}/activities"
        data = {
            "comment": {
                "format": "markdown",
                "raw": comment
            }
        }
        
        response = self._request("post".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully added comment to work package {work_package_id}")
        return result
    
    # User Management
    @with_resilience("openproject", rate_limit=15.0)
    def create_user(self, login: str, email: str, first_name: str,
                    last_name: str, password: str = None, admin: bool = False,
                    status: str = "active", language: str = "en") -> Dict:
        """Create a new user"""
        url = f"{self.base_url}/users"
        user_data = {
            "login": login,
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "admin": admin,
            "status": status,
            "language": language
        }
        
        if password:
            user_data["password"] = password
            
        response = self._request("post".upper(), url, json=user_data)
        result = self._handle_response(response)
        logger.info(f"Successfully created user: {login}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def get_user(self, user_id: int) -> Dict:
        """Get user details"""
        url = f"{self.base_url}/users/{user_id}"
        response = self._request("get".upper(), url)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def update_user(self, user_id: int, email: str = None, first_name: str = None,
                    last_name: str = None, admin: bool = None, status: str = None,
                    language: str = None) -> Dict:
        """Update user properties"""
        url = f"{self.base_url}/users/{user_id}"
        data = {}
        
        if email is not None:
            data["email"] = email
        if first_name is not None:
            data["firstName"] = first_name
        if last_name is not None:
            data["lastName"] = last_name
        if admin is not None:
            data["admin"] = admin
        if status is not None:
            data["status"] = status
        if language is not None:
            data["language"] = language
            
        response = self._request("patch".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully updated user {user_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def delete_user(self, user_id: int) -> bool:
        """Delete (lock) a user"""
        url = f"{self.base_url}/users/{user_id}/lock"
        response = self._request("post".upper(), url)
        if response.status_code == 204:
            logger.info(f"Successfully locked user {user_id}")
            return True
        self._handle_response(response)
        return False
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_users(self, status: str = None, name: str = None,
                   offset: int = 0, page_size: int = 100) -> Dict:
        """List users with optional filtering"""
        url = f"{self.base_url}/users"
        params = {
            "offset": offset,
            "pageSize": page_size
        }
        
        filters = []
        if status:
            filters.append({
                "status": {
                    "operator": "=",
                    "values": [status]
                }
            })
        if name:
            filters.append({
                "name": {
                    "operator": "~",
                    "values": [name]
                }
            })
            
        if filters:
            params["filters"] = json.dumps(filters)
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    # Memberships
    @with_resilience("openproject", rate_limit=15.0)
    def add_member(self, project_id: Union[int, str], user_id: int,
                   role_ids: List[int]) -> Dict:
        """Add a member to a project"""
        url = f"{self.base_url}/memberships"
        data = {
            "_links": {
                "project": {"href": f"/api/v3/projects/{project_id}"},
                "principal": {"href": f"/api/v3/users/{user_id}"},
                "roles": [{"href": f"/api/v3/roles/{role_id}"} for role_id in role_ids]
            }
        }
        
        response = self._request("post".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully added user {user_id} to project {project_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def update_member_roles(self, membership_id: int, role_ids: List[int]) -> Dict:
        """Update member roles"""
        url = f"{self.base_url}/memberships/{membership_id}"
        data = {
            "_links": {
                "roles": [{"href": f"/api/v3/roles/{role_id}"} for role_id in role_ids]
            }
        }
        
        response = self._request("patch".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully updated membership {membership_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def remove_member(self, membership_id: int) -> bool:
        """Remove a member from a project"""
        url = f"{self.base_url}/memberships/{membership_id}"
        response = self._request("delete".upper(), url)
        if response.status_code == 204:
            logger.info(f"Successfully removed membership {membership_id}")
            return True
        self._handle_response(response)
        return False
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_memberships(self, project_id: Union[int, str] = None,
                         offset: int = 0, page_size: int = 100) -> Dict:
        """List project memberships"""
        url = f"{self.base_url}/memberships"
        params = {
            "offset": offset,
            "pageSize": page_size
        }
        
        if project_id:
            params["filters"] = json.dumps([{
                "project": {
                    "operator": "=",
                    "values": [str(project_id)]
                }
            }])
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    # Types, Statuses, Priorities
    @with_resilience("openproject", rate_limit=15.0)
    def list_types(self, project_id: Union[int, str] = None) -> List[Dict]:
        """List available work package types"""
        if project_id:
            url = f"{self.base_url}/projects/{project_id}/types"
        else:
            url = f"{self.base_url}/types"
            
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_statuses(self) -> List[Dict]:
        """List available work package statuses"""
        url = f"{self.base_url}/statuses"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_priorities(self) -> List[Dict]:
        """List available work package priorities"""
        url = f"{self.base_url}/priorities"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_roles(self) -> List[Dict]:
        """List available roles"""
        url = f"{self.base_url}/roles"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    # Time Tracking
    @with_resilience("openproject", rate_limit=15.0)
    def create_time_entry(self, work_package_id: int, hours: float,
                          spent_on: date, activity_id: int,
                          comment: str = None, user_id: int = None) -> Dict:
        """Create a time entry"""
        url = f"{self.base_url}/time_entries"
        data = {
            "hours": f"PT{int(hours)}H{int((hours % 1) * 60)}M",
            "spentOn": spent_on.isoformat(),
            "_links": {
                "workPackage": {"href": f"/api/v3/work_packages/{work_package_id}"},
                "activity": {"href": f"/api/v3/time_entries/activities/{activity_id}"}
            }
        }
        
        if comment:
            data["comment"] = {
                "format": "plain",
                "raw": comment
            }
            
        if user_id:
            data["_links"]["user"] = {"href": f"/api/v3/users/{user_id}"}
            
        response = self._request("post".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully created time entry for work package {work_package_id}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_time_entries(self, work_package_id: int = None, user_id: int = None,
                          project_id: Union[int, str] = None, offset: int = 0,
                          page_size: int = 100) -> Dict:
        """List time entries with optional filtering"""
        url = f"{self.base_url}/time_entries"
        params = {
            "offset": offset,
            "pageSize": page_size
        }
        
        filters = []
        if work_package_id:
            filters.append({
                "work_package": {
                    "operator": "=",
                    "values": [str(work_package_id)]
                }
            })
        if user_id:
            filters.append({
                "user": {
                    "operator": "=",
                    "values": [str(user_id)]
                }
            })
        if project_id:
            filters.append({
                "project": {
                    "operator": "=",
                    "values": [str(project_id)]
                }
            })
            
        if filters:
            params["filters"] = json.dumps(filters)
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_time_entry_activities(self) -> List[Dict]:
        """List available time entry activities"""
        url = f"{self.base_url}/time_entries/activities"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    # Attachments
    @with_resilience("openproject", rate_limit=15.0)
    def upload_attachment(self, file_path: str, work_package_id: int = None) -> Dict:
        """Upload an attachment"""
        url = f"{self.base_url}/attachments"
        
        # First, prepare the attachment
        filename = os.path.basename(file_path)
        
        with open(file_path, 'rb') as f:
            files = {'file': (filename, f)}
            headers = {
                "Authorization": self.headers["Authorization"]
            }
            
            if work_package_id:
                headers["Content-Type"] = "multipart/form-data"
                data = {
                    "container": f"/api/v3/work_packages/{work_package_id}"
                }
                response = self._request("POST", url, files=files, data=data, headers=headers)
            else:
                response = self._request("POST", url, files=files, headers=headers)
                
        result = self._handle_response(response)
        logger.info(f"Successfully uploaded attachment: {filename}")
        return result
    
    # Queries
    @with_resilience("openproject", rate_limit=15.0)
    def create_query(self, name: str, project_id: Union[int, str] = None,
                     filters: List[Dict] = None, columns: List[str] = None,
                     sort_by: List[List[str]] = None, group_by: str = None,
                     public: bool = False) -> Dict:
        """Create a saved query"""
        url = f"{self.base_url}/queries"
        data = {
            "name": name,
            "public": public
        }
        
        if project_id:
            data["_links"] = {
                "project": {"href": f"/api/v3/projects/{project_id}"}
            }
            
        if filters:
            data["filters"] = filters
        if columns:
            data["columns"] = columns
        if sort_by:
            data["sortBy"] = sort_by
        if group_by:
            data["groupBy"] = group_by
            
        response = self._request("post".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully created query: {name}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def get_query(self, query_id: int) -> Dict:
        """Get a saved query"""
        url = f"{self.base_url}/queries/{query_id}"
        response = self._request("get".upper(), url)
        return self._handle_response(response)
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_queries(self, project_id: Union[int, str] = None) -> Dict:
        """List saved queries"""
        url = f"{self.base_url}/queries"
        params = {}
        
        if project_id:
            params["filters"] = json.dumps([{
                "project": {
                    "operator": "=",
                    "values": [str(project_id)]
                }
            }])
            
        response = self._request("get".upper(), url, params=params)
        return self._handle_response(response)
    
    # Custom Fields
    @with_resilience("openproject", rate_limit=15.0)
    def list_custom_fields(self) -> List[Dict]:
        """List available custom fields"""
        url = f"{self.base_url}/custom_fields"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", [])
    
    # Versions
    @with_resilience("openproject", rate_limit=15.0)
    def create_version(self, project_id: Union[int, str], name: str,
                       description: str = None, start_date: date = None,
                       end_date: date = None, status: str = "open",
                       sharing: str = "none") -> Dict:
        """Create a project version"""
        url = f"{self.base_url}/versions"
        data = {
            "name": name,
            "status": status,
            "sharing": sharing,
            "_links": {
                "definingProject": {"href": f"/api/v3/projects/{project_id}"}
            }
        }
        
        if description:
            data["description"] = {
                "format": "markdown",
                "raw": description
            }
        if start_date:
            data["startDate"] = start_date.isoformat()
        if end_date:
            data["endDate"] = end_date.isoformat()
            
        response = self._request("post".upper(), url, json=data)
        result = self._handle_response(response)
        logger.info(f"Successfully created version: {name}")
        return result
    
    @with_resilience("openproject", rate_limit=15.0)
    def list_versions(self, project_id: Union[int, str]) -> List[Dict]:
        """List project versions"""
        url = f"{self.base_url}/projects/{project_id}/versions"
        response = self._request("get".upper(), url)
        result = self._handle_response(response)
        return result.get("_embedded", {}).get("elements", []) 