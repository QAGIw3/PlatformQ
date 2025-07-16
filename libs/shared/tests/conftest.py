"""
Pytest configuration for integration tests
"""

import pytest
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import base64
from urllib.parse import urlparse, parse_qs
import socket


def get_free_port():
    """Get a free port for testing"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


class MockNextcloudHandler(BaseHTTPRequestHandler):
    """Mock Nextcloud server for testing"""
    
    def _set_headers(self, status_code=200, content_type='application/json'):
        self.send_response(status_code)
        self.send_header('Content-Type', content_type)
        self.end_headers()
        
    def _send_ocs_response(self, data, status_code=200):
        """Send OCS-formatted response"""
        self._set_headers(status_code, 'application/json; charset=utf-8')
        response = {
            "ocs": {
                "meta": {
                    "status": "ok",
                    "statuscode": 100,
                    "message": "OK"
                },
                "data": data
            }
        }
        self.wfile.write(json.dumps(response).encode())
        
    def do_GET(self):
        """Handle GET requests"""
        path = urlparse(self.path).path
        
        # User endpoints
        if path.startswith('/ocs/v2.php/cloud/users/'):
            username = path.split('/')[-1]
            if username == "testuser":
                self._send_ocs_response({
                    "id": "testuser",
                    "displayname": "Test User",
                    "email": "test@example.com",
                    "quota": {"used": 1024, "total": 10737418240}
                })
            else:
                self._set_headers(404)
                
        # List users
        elif path == '/ocs/v2.php/cloud/users':
            self._send_ocs_response({"users": ["admin", "testuser", "user2"]})
            
        # Capabilities
        elif path == '/ocs/v2.php/cloud/capabilities':
            self._send_ocs_response({
                "capabilities": {
                    "core": {"pollinterval": 60},
                    "files": {"bigfilechunking": True}
                }
            })
            
        else:
            self._set_headers(404)
            
    def do_POST(self):
        """Handle POST requests"""
        path = urlparse(self.path).path
        content_length = int(self.headers.get('Content-Length', 0))
        post_data = self.rfile.read(content_length)
        
        # Create user
        if path == '/ocs/v2.php/cloud/users':
            self._send_ocs_response({"id": "newuser"})
            
        # Create folder (MKCOL)
        elif path.startswith('/remote.php/dav/files/'):
            self._set_headers(201)
            
        else:
            self._set_headers(404)
            
    def do_PUT(self):
        """Handle PUT requests"""
        path = urlparse(self.path).path
        
        # File upload
        if path.startswith('/remote.php/dav/files/'):
            self._set_headers(201)
        else:
            self._set_headers(404)
            
    def do_DELETE(self):
        """Handle DELETE requests"""
        self._set_headers(204)
        
    def do_MKCOL(self):
        """Handle MKCOL (create folder) requests"""
        self._set_headers(201)
        
    def do_PROPFIND(self):
        """Handle PROPFIND requests"""
        self._set_headers(207, 'application/xml')
        # Return minimal WebDAV response
        response = '''<?xml version="1.0"?>
        <d:multistatus xmlns:d="DAV:">
            <d:response>
                <d:href>/files/</d:href>
                <d:propstat>
                    <d:prop>
                        <d:resourcetype><d:collection/></d:resourcetype>
                    </d:prop>
                    <d:status>HTTP/1.1 200 OK</d:status>
                </d:propstat>
            </d:response>
        </d:multistatus>'''
        self.wfile.write(response.encode())
        
    def log_message(self, format, *args):
        """Suppress log messages during testing"""
        pass


class MockZulipHandler(BaseHTTPRequestHandler):
    """Mock Zulip server for testing"""
    
    def _set_headers(self, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
    def _check_auth(self):
        """Check basic auth"""
        auth_header = self.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Basic '):
            self._set_headers(401)
            return False
        return True
        
    def do_GET(self):
        """Handle GET requests"""
        if not self._check_auth():
            return
            
        path = urlparse(self.path).path
        
        # Get messages
        if path == '/api/v1/messages':
            self._set_headers()
            response = {
                "messages": [
                    {
                        "id": 1,
                        "content": "Test message",
                        "timestamp": 1234567890,
                        "sender_id": 1
                    }
                ],
                "found_newest": True,
                "found_oldest": True
            }
            self.wfile.write(json.dumps(response).encode())
            
        # Get streams
        elif path == '/api/v1/streams':
            self._set_headers()
            response = {
                "streams": [
                    {"stream_id": 1, "name": "general", "description": "General discussion"},
                    {"stream_id": 2, "name": "random", "description": "Random stuff"}
                ]
            }
            self.wfile.write(json.dumps(response).encode())
            
        # Get users
        elif path == '/api/v1/users':
            self._set_headers()
            response = {
                "members": [
                    {"user_id": 1, "email": "admin@example.com", "full_name": "Admin User"},
                    {"user_id": 2, "email": "test@example.com", "full_name": "Test User"}
                ]
            }
            self.wfile.write(json.dumps(response).encode())
            
        else:
            self._set_headers(404)
            
    def do_POST(self):
        """Handle POST requests"""
        if not self._check_auth():
            return
            
        path = urlparse(self.path).path
        
        # Send message
        if path == '/api/v1/messages':
            self._set_headers()
            response = {"id": 12345, "msg": "", "result": "success"}
            self.wfile.write(json.dumps(response).encode())
            
        # Create stream
        elif path == '/api/v1/users/me/subscriptions':
            self._set_headers()
            response = {"subscribed": {"test@example.com": ["new-stream"]}, "result": "success"}
            self.wfile.write(json.dumps(response).encode())
            
        # Create user
        elif path == '/api/v1/users':
            self._set_headers()
            response = {"user_id": 3, "result": "success"}
            self.wfile.write(json.dumps(response).encode())
            
        else:
            self._set_headers(404)
            
    def do_DELETE(self):
        """Handle DELETE requests"""
        if not self._check_auth():
            return
        self._set_headers(204)
        
    def log_message(self, format, *args):
        """Suppress log messages during testing"""
        pass


class MockOpenProjectHandler(BaseHTTPRequestHandler):
    """Mock OpenProject server for testing"""
    
    def _set_headers(self, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/hal+json')
        self.end_headers()
        
    def _check_auth(self):
        """Check API key auth"""
        auth_header = self.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Basic '):
            self._set_headers(401)
            return False
        return True
        
    def do_GET(self):
        """Handle GET requests"""
        if not self._check_auth():
            return
            
        path = urlparse(self.path).path
        
        # Get project
        if path.startswith('/api/v3/projects/'):
            project_id = path.split('/')[-1]
            self._set_headers()
            response = {
                "id": int(project_id) if project_id.isdigit() else 1,
                "identifier": "test-project",
                "name": "Test Project",
                "active": True,
                "public": False,
                "_links": {
                    "self": {"href": f"/api/v3/projects/{project_id}"}
                }
            }
            self.wfile.write(json.dumps(response).encode())
            
        # List projects
        elif path == '/api/v3/projects':
            self._set_headers()
            response = {
                "_embedded": {
                    "elements": [
                        {
                            "id": 1,
                            "identifier": "test-project",
                            "name": "Test Project"
                        }
                    ]
                },
                "total": 1,
                "count": 1
            }
            self.wfile.write(json.dumps(response).encode())
            
        # List work packages
        elif path == '/api/v3/work_packages':
            self._set_headers()
            response = {
                "_embedded": {
                    "elements": [
                        {
                            "id": 1,
                            "subject": "Test Work Package",
                            "_links": {
                                "self": {"href": "/api/v3/work_packages/1"},
                                "project": {"href": "/api/v3/projects/1"}
                            }
                        }
                    ]
                },
                "total": 1,
                "count": 1
            }
            self.wfile.write(json.dumps(response).encode())
            
        # List types
        elif path == '/api/v3/types':
            self._set_headers()
            response = {
                "_embedded": {
                    "elements": [
                        {"id": 1, "name": "Task"},
                        {"id": 2, "name": "Bug"}
                    ]
                }
            }
            self.wfile.write(json.dumps(response).encode())
            
        else:
            self._set_headers(404)
            
    def do_POST(self):
        """Handle POST requests"""
        if not self._check_auth():
            return
            
        path = urlparse(self.path).path
        
        # Create project
        if path == '/api/v3/projects':
            self._set_headers(201)
            response = {
                "id": 2,
                "identifier": "new-project",
                "name": "New Project",
                "_links": {
                    "self": {"href": "/api/v3/projects/2"}
                }
            }
            self.wfile.write(json.dumps(response).encode())
            
        # Create work package
        elif path == '/api/v3/work_packages':
            self._set_headers(201)
            response = {
                "id": 2,
                "subject": "New Work Package",
                "_links": {
                    "self": {"href": "/api/v3/work_packages/2"}
                }
            }
            self.wfile.write(json.dumps(response).encode())
            
        else:
            self._set_headers(404)
            
    def do_DELETE(self):
        """Handle DELETE requests"""
        if not self._check_auth():
            return
        self._set_headers(204)
        
    def log_message(self, format, *args):
        """Suppress log messages during testing"""
        pass


def start_mock_server(handler_class, port):
    """Start a mock server in a thread"""
    server = HTTPServer(('localhost', port), handler_class)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    time.sleep(0.5)  # Give server time to start
    return server


@pytest.fixture(scope="session")
def mock_nextcloud_server():
    """Fixture for mock Nextcloud server"""
    port = get_free_port()
    server = start_mock_server(MockNextcloudHandler, port)
    yield f"http://localhost:{port}"
    server.shutdown()


@pytest.fixture(scope="session")
def mock_zulip_server():
    """Fixture for mock Zulip server"""
    port = get_free_port()
    server = start_mock_server(MockZulipHandler, port)
    yield f"http://localhost:{port}"
    server.shutdown()


@pytest.fixture(scope="session")
def mock_openproject_server():
    """Fixture for mock OpenProject server"""
    port = get_free_port()
    server = start_mock_server(MockOpenProjectHandler, port)
    yield f"http://localhost:{port}"
    server.shutdown()


@pytest.fixture
def nextcloud_client(mock_nextcloud_server):
    """Fixture for Nextcloud client with mock server"""
    from platformq_shared.nextcloud_client import NextcloudClient
    return NextcloudClient(
        nextcloud_url=mock_nextcloud_server,
        admin_user="admin",
        admin_pass="password",
        use_connection_pool=False  # Disable for testing
    )


@pytest.fixture
def zulip_client(mock_zulip_server):
    """Fixture for Zulip client with mock server"""
    from platformq_shared.zulip_client import ZulipClient
    return ZulipClient(
        zulip_site=mock_zulip_server,
        zulip_email="test@example.com",
        zulip_api_key="test-api-key",
        use_connection_pool=False  # Disable for testing
    )


@pytest.fixture
def openproject_client(mock_openproject_server):
    """Fixture for OpenProject client with mock server"""
    from platformq_shared.openproject_client import OpenProjectClient
    return OpenProjectClient(
        openproject_url=mock_openproject_server,
        api_key="test-api-key",
        use_connection_pool=False  # Disable for testing
    ) 