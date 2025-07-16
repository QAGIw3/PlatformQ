"""
Integration tests for Nextcloud client
"""

import pytest
import tempfile
import os
from platformq_shared.nextcloud_client import (
    NextcloudClient, NextcloudError, NextcloudAuthError, 
    NextcloudNotFoundError, NextcloudPermissionError
)


class TestNextcloudUserManagement:
    """Test user management functionality"""
    
    def test_create_user(self, nextcloud_client):
        """Test creating a new user"""
        result = nextcloud_client.create_user(
            username="newuser",
            password="password123",
            email="newuser@example.com",
            display_name="New User"
        )
        assert result is True
        
    def test_get_user(self, nextcloud_client):
        """Test getting user information"""
        user_info = nextcloud_client.get_user("testuser")
        assert user_info["id"] == "testuser"
        assert user_info["email"] == "test@example.com"
        assert "quota" in user_info
        
    def test_get_nonexistent_user(self, nextcloud_client):
        """Test getting non-existent user"""
        with pytest.raises(NextcloudError):
            nextcloud_client.get_user("nonexistent")
            
    def test_list_users(self, nextcloud_client):
        """Test listing all users"""
        users = nextcloud_client.list_users()
        assert isinstance(users, list)
        assert "admin" in users
        assert "testuser" in users


class TestNextcloudFileOperations:
    """Test file operations"""
    
    def test_create_folder(self, nextcloud_client):
        """Test creating a folder"""
        result = nextcloud_client.create_folder("test-folder")
        assert result is True
        
    def test_upload_file(self, nextcloud_client):
        """Test uploading a file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("Test content")
            temp_path = f.name
            
        try:
            result = nextcloud_client.upload_file(temp_path, "test-file.txt")
            assert result is True
        finally:
            os.unlink(temp_path)
            
    def test_list_folder_contents(self, nextcloud_client):
        """Test listing folder contents"""
        contents = nextcloud_client.list_folder_contents("/")
        assert isinstance(contents, list)


class TestNextcloudSharing:
    """Test sharing functionality"""
    
    def test_create_share(self, nextcloud_client):
        """Test creating a public share"""
        share_data = nextcloud_client.create_share(
            path="/test-folder",
            share_type=3,  # Public link
            password="sharepass"
        )
        # Mock server returns basic response
        assert isinstance(share_data, dict)


class TestNextcloudCapabilities:
    """Test server capabilities"""
    
    def test_check_capabilities(self, nextcloud_client):
        """Test getting server capabilities"""
        capabilities = nextcloud_client.check_capabilities()
        assert isinstance(capabilities, dict)
        assert "core" in capabilities
        assert "files" in capabilities


class TestNextcloudErrorHandling:
    """Test error handling"""
    
    def test_connection_pooling_disabled(self, mock_nextcloud_server):
        """Test that connection pooling can be disabled"""
        client = NextcloudClient(
            nextcloud_url=mock_nextcloud_server,
            admin_user="admin",
            admin_pass="password",
            use_connection_pool=False
        )
        assert client.session is None
        
    def test_connection_pooling_enabled(self, mock_nextcloud_server):
        """Test that connection pooling can be enabled"""
        client = NextcloudClient(
            nextcloud_url=mock_nextcloud_server,
            admin_user="admin",
            admin_pass="password",
            use_connection_pool=True
        )
        assert client.session is not None


class TestNextcloudResilience:
    """Test resilience patterns"""
    
    def test_rate_limiting(self, nextcloud_client):
        """Test that rate limiting is configured"""
        # The decorator should be applied to methods
        assert hasattr(nextcloud_client.create_user, '__wrapped__')
        
    def test_request_method(self, nextcloud_client):
        """Test the _request method works correctly"""
        # This is tested indirectly through other operations
        users = nextcloud_client.list_users()
        assert isinstance(users, list) 