import requests
import logging
from typing import Dict, List, Optional, Union, Any
from urllib.parse import urljoin, quote
import xml.etree.ElementTree as ET
from datetime import datetime
import json
from functools import wraps
import time

logger = logging.getLogger(__name__)


class NextcloudError(Exception):
    """Base exception for Nextcloud client errors"""
    pass


class NextcloudAuthError(NextcloudError):
    """Authentication related errors"""
    pass


class NextcloudNotFoundError(NextcloudError):
    """Resource not found errors"""
    pass


class NextcloudPermissionError(NextcloudError):
    """Permission related errors"""
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


class NextcloudClient:
    """
    Comprehensive Nextcloud client with support for:
    - User management
    - File operations
    - Sharing
    - WebDAV
    - Groups
    - Search
    """
    
    def __init__(self, nextcloud_url: str, admin_user: str, admin_pass: str, 
                 timeout: int = 30, verify_ssl: bool = True):
        self.base_url = nextcloud_url.rstrip('/')
        self.auth = (admin_user, admin_pass)
        self.timeout = timeout
        self.verify_ssl = verify_ssl
        self.headers = {
            "OCS-APIRequest": "true",
            "Accept": "application/json"
        }
        
        # API endpoints
        self.ocs_url = f"{self.base_url}/ocs/v2.php"
        self.dav_url = f"{self.base_url}/remote.php/dav"
        self.webdav_url = f"{self.dav_url}/files/{admin_user}"
        
    def _handle_response(self, response: requests.Response, expected_status: List[int] = None) -> Dict:
        """Handle API response and raise appropriate exceptions"""
        if expected_status is None:
            expected_status = [200, 201, 204]
            
        if response.status_code == 401:
            raise NextcloudAuthError("Authentication failed")
        elif response.status_code == 404:
            raise NextcloudNotFoundError("Resource not found")
        elif response.status_code == 403:
            raise NextcloudPermissionError("Permission denied")
        elif response.status_code not in expected_status:
            raise NextcloudError(f"Unexpected status code: {response.status_code}, Response: {response.text}")
            
        # Parse OCS response
        if response.content and 'ocs' in response.headers.get('Content-Type', ''):
            try:
                data = response.json()
                if 'ocs' in data:
                    meta = data['ocs'].get('meta', {})
                    if meta.get('statuscode') != 100:
                        raise NextcloudError(f"OCS Error: {meta.get('message', 'Unknown error')}")
                    return data['ocs'].get('data', {})
                return data
            except json.JSONDecodeError:
                return {}
        return {}
    
    # User Management
    @retry_on_failure()
    def create_user(self, username: str, password: str, email: str = None, 
                    display_name: str = None, groups: List[str] = None, 
                    quota: str = None) -> bool:
        """Create a new user"""
        url = f"{self.ocs_url}/cloud/users"
        data = {
            "userid": username,
            "password": password
        }
        
        if email:
            data["email"] = email
        if display_name:
            data["displayName"] = display_name
        if quota:
            data["quota"] = quota
            
        response = requests.post(url, auth=self.auth, headers=self.headers, 
                                data=data, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        # Add user to groups if specified
        if groups:
            for group in groups:
                self.add_user_to_group(username, group)
                
        logger.info(f"Successfully created user: {username}")
        return True
    
    @retry_on_failure()
    def get_user(self, username: str) -> Dict:
        """Get user information"""
        url = f"{self.ocs_url}/cloud/users/{username}"
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               timeout=self.timeout, verify=self.verify_ssl)
        return self._handle_response(response)
    
    @retry_on_failure()
    def update_user(self, username: str, **kwargs) -> bool:
        """Update user information"""
        url = f"{self.ocs_url}/cloud/users/{username}"
        valid_keys = ['email', 'displayname', 'password', 'quota', 'phone', 
                      'address', 'website', 'twitter']
        
        for key, value in kwargs.items():
            if key in valid_keys:
                data = {'key': key, 'value': value}
                response = requests.put(url, auth=self.auth, headers=self.headers,
                                       data=data, timeout=self.timeout, verify=self.verify_ssl)
                self._handle_response(response)
                
        logger.info(f"Successfully updated user: {username}")
        return True
    
    @retry_on_failure()
    def delete_user(self, username: str) -> bool:
        """Delete a user"""
        url = f"{self.ocs_url}/cloud/users/{username}"
        response = requests.delete(url, auth=self.auth, headers=self.headers,
                                  timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        logger.info(f"Successfully deleted user: {username}")
        return True
    
    @retry_on_failure()
    def list_users(self, search: str = None, limit: int = None, offset: int = None) -> List[str]:
        """List all users"""
        url = f"{self.ocs_url}/cloud/users"
        params = {}
        
        if search:
            params['search'] = search
        if limit:
            params['limit'] = limit
        if offset:
            params['offset'] = offset
            
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               params=params, timeout=self.timeout, verify=self.verify_ssl)
        data = self._handle_response(response)
        return data.get('users', [])
    
    # File Operations
    @retry_on_failure()
    def upload_file(self, local_path: str, remote_path: str) -> bool:
        """Upload a file to Nextcloud"""
        remote_path = remote_path.lstrip('/')
        url = f"{self.webdav_url}/{quote(remote_path, safe='/')}"
        
        with open(local_path, 'rb') as f:
            response = requests.put(url, data=f, auth=self.auth,
                                   timeout=self.timeout, verify=self.verify_ssl)
            self._handle_response(response, [201, 204])
            
        logger.info(f"Successfully uploaded file to {remote_path}")
        return True
    
    @retry_on_failure()
    def download_file(self, remote_path: str, local_path: str) -> bool:
        """Download a file from Nextcloud"""
        remote_path = remote_path.lstrip('/')
        url = f"{self.webdav_url}/{quote(remote_path, safe='/')}"
        
        response = requests.get(url, auth=self.auth, stream=True,
                               timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        logger.info(f"Successfully downloaded file from {remote_path}")
        return True
    
    @retry_on_failure()
    def delete_file(self, remote_path: str) -> bool:
        """Delete a file or folder"""
        remote_path = remote_path.lstrip('/')
        url = f"{self.webdav_url}/{quote(remote_path, safe='/')}"
        
        response = requests.delete(url, auth=self.auth,
                                  timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response, [204])
        
        logger.info(f"Successfully deleted: {remote_path}")
        return True
    
    @retry_on_failure()
    def move_file(self, source_path: str, dest_path: str) -> bool:
        """Move or rename a file/folder"""
        source_path = source_path.lstrip('/')
        dest_path = dest_path.lstrip('/')
        
        source_url = f"{self.webdav_url}/{quote(source_path, safe='/')}"
        dest_url = f"{self.webdav_url}/{quote(dest_path, safe='/')}"
        
        headers = {'Destination': dest_url}
        response = requests.request('MOVE', source_url, auth=self.auth, headers=headers,
                                   timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response, [201, 204])
        
        logger.info(f"Successfully moved {source_path} to {dest_path}")
        return True
    
    @retry_on_failure()
    def copy_file(self, source_path: str, dest_path: str) -> bool:
        """Copy a file/folder"""
        source_path = source_path.lstrip('/')
        dest_path = dest_path.lstrip('/')
        
        source_url = f"{self.webdav_url}/{quote(source_path, safe='/')}"
        dest_url = f"{self.webdav_url}/{quote(dest_path, safe='/')}"
        
        headers = {'Destination': dest_url}
        response = requests.request('COPY', source_url, auth=self.auth, headers=headers,
                                   timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response, [201, 204])
        
        logger.info(f"Successfully copied {source_path} to {dest_path}")
        return True
    
    @retry_on_failure()
    def create_folder(self, path: str) -> bool:
        """Create a folder in Nextcloud"""
        path = path.lstrip('/')
        url = f"{self.webdav_url}/{quote(path, safe='/')}"
        
        response = requests.request("MKCOL", url, auth=self.auth,
                                   timeout=self.timeout, verify=self.verify_ssl)
        if response.status_code == 405:  # Already exists
            logger.warning(f"Folder already exists: {path}")
            return True
            
        self._handle_response(response, [201])
        logger.info(f"Successfully created folder: {path}")
        return True
    
    @retry_on_failure()
    def list_folder_contents(self, path: str = "/") -> List[Dict]:
        """List contents of a folder"""
        path = path.lstrip('/')
        url = f"{self.webdav_url}/{quote(path, safe='/')}"
        
        # PROPFIND request body
        propfind_body = """<?xml version="1.0" encoding="UTF-8"?>
        <d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
            <d:prop>
                <d:getlastmodified/>
                <d:getetag/>
                <d:getcontenttype/>
                <d:getcontentlength/>
                <d:resourcetype/>
                <oc:permissions/>
                <oc:size/>
            </d:prop>
        </d:propfind>"""
        
        headers = {'Depth': '1', 'Content-Type': 'application/xml'}
        response = requests.request('PROPFIND', url, auth=self.auth, headers=headers,
                                   data=propfind_body, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response, [207])  # Multi-Status
        
        # Parse WebDAV response
        root = ET.fromstring(response.content)
        items = []
        
        for response_elem in root.findall('.//{DAV:}response'):
            href = response_elem.find('{DAV:}href').text
            # Skip the parent folder itself
            if href.rstrip('/') == url.rstrip('/'):
                continue
                
            prop = response_elem.find('.//{DAV:}prop')
            item = {
                'path': href,
                'name': href.split('/')[-1] or href.split('/')[-2],
                'type': 'folder' if prop.find('{DAV:}resourcetype/{DAV:}collection') is not None else 'file',
                'size': int(prop.find('{DAV:}getcontentlength').text) if prop.find('{DAV:}getcontentlength') is not None else 0,
                'modified': prop.find('{DAV:}getlastmodified').text if prop.find('{DAV:}getlastmodified') is not None else None
            }
            items.append(item)
            
        return items
    
    # Sharing
    @retry_on_failure()
    def create_share(self, path: str, share_type: int = 0, share_with: str = None,
                     public_upload: bool = False, password: str = None,
                     permissions: int = 31, expire_date: str = None) -> Dict:
        """
        Create a share for a file/folder
        
        share_type: 0=user, 1=group, 3=public link, 6=federated cloud share
        permissions: 1=read, 2=update, 4=create, 8=delete, 16=share, 31=all
        """
        url = f"{self.ocs_url}/apps/files_sharing/api/v1/shares"
        data = {
            'path': path,
            'shareType': share_type
        }
        
        if share_with and share_type != 3:  # Not public link
            data['shareWith'] = share_with
        if public_upload and share_type == 3:
            data['publicUpload'] = 'true'
        if password:
            data['password'] = password
        if permissions != 31:
            data['permissions'] = permissions
        if expire_date:
            data['expireDate'] = expire_date
            
        response = requests.post(url, auth=self.auth, headers=self.headers,
                                data=data, timeout=self.timeout, verify=self.verify_ssl)
        share_data = self._handle_response(response)
        
        logger.info(f"Successfully created share for: {path}")
        return share_data
    
    @retry_on_failure()
    def get_shares(self, path: str = None, reshares: bool = False, 
                   subfiles: bool = False) -> List[Dict]:
        """Get shares for a path or all shares"""
        url = f"{self.ocs_url}/apps/files_sharing/api/v1/shares"
        params = {}
        
        if path:
            params['path'] = path
        if reshares:
            params['reshares'] = 'true'
        if subfiles:
            params['subfiles'] = 'true'
            
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               params=params, timeout=self.timeout, verify=self.verify_ssl)
        return self._handle_response(response)
    
    @retry_on_failure()
    def delete_share(self, share_id: int) -> bool:
        """Delete a share"""
        url = f"{self.ocs_url}/apps/files_sharing/api/v1/shares/{share_id}"
        response = requests.delete(url, auth=self.auth, headers=self.headers,
                                  timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        logger.info(f"Successfully deleted share: {share_id}")
        return True
    
    @retry_on_failure()
    def update_share(self, share_id: int, **kwargs) -> bool:
        """Update share properties"""
        url = f"{self.ocs_url}/apps/files_sharing/api/v1/shares/{share_id}"
        valid_keys = ['permissions', 'password', 'publicUpload', 'expireDate', 'note']
        
        data = {k: v for k, v in kwargs.items() if k in valid_keys}
        if data:
            response = requests.put(url, auth=self.auth, headers=self.headers,
                                   data=data, timeout=self.timeout, verify=self.verify_ssl)
            self._handle_response(response)
            
        logger.info(f"Successfully updated share: {share_id}")
        return True
    
    # Groups
    @retry_on_failure()
    def create_group(self, groupname: str) -> bool:
        """Create a new group"""
        url = f"{self.ocs_url}/cloud/groups"
        data = {'groupid': groupname}
        
        response = requests.post(url, auth=self.auth, headers=self.headers,
                                data=data, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        logger.info(f"Successfully created group: {groupname}")
        return True
    
    @retry_on_failure()
    def delete_group(self, groupname: str) -> bool:
        """Delete a group"""
        url = f"{self.ocs_url}/cloud/groups/{groupname}"
        response = requests.delete(url, auth=self.auth, headers=self.headers,
                                  timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        logger.info(f"Successfully deleted group: {groupname}")
        return True
    
    @retry_on_failure()
    def get_group_members(self, groupname: str) -> List[str]:
        """Get members of a group"""
        url = f"{self.ocs_url}/cloud/groups/{groupname}"
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               timeout=self.timeout, verify=self.verify_ssl)
        data = self._handle_response(response)
        return data.get('users', [])
    
    @retry_on_failure()
    def add_user_to_group(self, username: str, groupname: str) -> bool:
        """Add a user to a group"""
        url = f"{self.ocs_url}/cloud/users/{username}/groups"
        data = {'groupid': groupname}
        
        response = requests.post(url, auth=self.auth, headers=self.headers,
                                data=data, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        logger.info(f"Successfully added {username} to group {groupname}")
        return True
    
    @retry_on_failure()
    def remove_user_from_group(self, username: str, groupname: str) -> bool:
        """Remove a user from a group"""
        url = f"{self.ocs_url}/cloud/users/{username}/groups"
        data = {'groupid': groupname}
        
        response = requests.delete(url, auth=self.auth, headers=self.headers,
                                  data=data, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response)
        
        logger.info(f"Successfully removed {username} from group {groupname}")
        return True
    
    # Search
    @retry_on_failure()
    def search_files(self, query: str, limit: int = 30, offset: int = 0) -> List[Dict]:
        """Search for files and folders"""
        url = f"{self.ocs_url}/search/providers/files/search"
        params = {
            'term': query,
            'limit': limit,
            'cursor': offset
        }
        
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               params=params, timeout=self.timeout, verify=self.verify_ssl)
        return self._handle_response(response).get('entries', [])
    
    # WebDAV operations
    @retry_on_failure()
    def webdav_propfind(self, path: str, properties: List[str] = None, depth: str = "1") -> ET.Element:
        """Perform a PROPFIND request"""
        path = path.lstrip('/')
        url = f"{self.webdav_url}/{quote(path, safe='/')}"
        
        if properties:
            prop_elements = ''.join([f'<d:{prop}/>' for prop in properties])
            propfind_body = f"""<?xml version="1.0" encoding="UTF-8"?>
            <d:propfind xmlns:d="DAV:">
                <d:prop>{prop_elements}</d:prop>
            </d:propfind>"""
        else:
            propfind_body = """<?xml version="1.0" encoding="UTF-8"?>
            <d:propfind xmlns:d="DAV:">
                <d:allprop/>
            </d:propfind>"""
            
        headers = {'Depth': depth, 'Content-Type': 'application/xml'}
        response = requests.request('PROPFIND', url, auth=self.auth, headers=headers,
                                   data=propfind_body, timeout=self.timeout, verify=self.verify_ssl)
        self._handle_response(response, [207])
        
        return ET.fromstring(response.content)
    
    @retry_on_failure()
    def get_file_info(self, path: str) -> Dict:
        """Get detailed information about a file or folder"""
        root = self.webdav_propfind(path, depth="0")
        
        prop = root.find('.//{DAV:}prop')
        if prop is None:
            raise NextcloudError("Unable to get file info")
            
        info = {
            'path': path,
            'type': 'folder' if prop.find('{DAV:}resourcetype/{DAV:}collection') is not None else 'file',
            'size': int(prop.find('{DAV:}getcontentlength').text) if prop.find('{DAV:}getcontentlength') is not None else 0,
            'modified': prop.find('{DAV:}getlastmodified').text if prop.find('{DAV:}getlastmodified') is not None else None,
            'etag': prop.find('{DAV:}getetag').text if prop.find('{DAV:}getetag') is not None else None,
        }
        
        return info
    
    def check_capabilities(self) -> Dict:
        """Get server capabilities"""
        url = f"{self.ocs_url}/cloud/capabilities"
        response = requests.get(url, auth=self.auth, headers=self.headers,
                               timeout=self.timeout, verify=self.verify_ssl)
        return self._handle_response(response).get('capabilities', {}) 