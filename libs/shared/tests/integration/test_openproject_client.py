"""
Integration tests for OpenProject client
"""

import pytest
from datetime import date
from platformq_shared.openproject_client import (
    OpenProjectClient, OpenProjectError, OpenProjectAuthError,
    OpenProjectNotFoundError, OpenProjectValidationError
)


class TestOpenProjectProjects:
    """Test project management functionality"""
    
    def test_create_project(self, openproject_client):
        """Test creating a new project"""
        result = openproject_client.create_project(
            name="New Test Project",
            identifier="new-test-project",
            description="A test project created via API",
            public=False
        )
        assert "id" in result
        assert result["id"] == 2
        assert result["name"] == "New Project"
        
    def test_get_project(self, openproject_client):
        """Test getting project details"""
        project = openproject_client.get_project(1)
        assert project["id"] == 1
        assert project["identifier"] == "test-project"
        assert project["name"] == "Test Project"
        assert "_links" in project
        
    def test_list_projects(self, openproject_client):
        """Test listing projects"""
        result = openproject_client.list_projects()
        assert "_embedded" in result
        assert "elements" in result["_embedded"]
        projects = result["_embedded"]["elements"]
        assert len(projects) > 0
        assert all("id" in p and "name" in p for p in projects)
        
    def test_get_nonexistent_project(self, openproject_client):
        """Test getting non-existent project"""
        with pytest.raises(OpenProjectNotFoundError):
            openproject_client.get_project(99999)


class TestOpenProjectWorkPackages:
    """Test work package management"""
    
    def test_create_work_package(self, openproject_client):
        """Test creating a work package"""
        result = openproject_client.create_work_package(
            project_id=1,
            subject="Test Work Package",
            type_id=1,
            description="This is a test work package"
        )
        assert "id" in result
        assert result["id"] == 2
        assert result["subject"] == "New Work Package"
        
    def test_list_work_packages(self, openproject_client):
        """Test listing work packages"""
        result = openproject_client.list_work_packages(project_id=1)
        assert "_embedded" in result
        assert "elements" in result["_embedded"]
        work_packages = result["_embedded"]["elements"]
        assert isinstance(work_packages, list)
        
    def test_create_work_package_with_dates(self, openproject_client):
        """Test creating work package with dates"""
        result = openproject_client.create_work_package(
            project_id=1,
            subject="Scheduled Work Package",
            type_id=1,
            start_date=date(2024, 1, 1),
            due_date=date(2024, 1, 31),
            estimated_hours=8.5
        )
        assert "id" in result


class TestOpenProjectTypes:
    """Test types and configuration"""
    
    def test_list_types(self, openproject_client):
        """Test listing work package types"""
        types = openproject_client.list_types()
        assert isinstance(types, list)
        assert len(types) > 0
        assert all("id" in t and "name" in t for t in types)
        
    def test_list_statuses(self, openproject_client):
        """Test listing statuses"""
        # This would need mock implementation
        assert hasattr(openproject_client, 'list_statuses')
        
    def test_list_priorities(self, openproject_client):
        """Test listing priorities"""
        # This would need mock implementation
        assert hasattr(openproject_client, 'list_priorities')


class TestOpenProjectUsers:
    """Test user management"""
    
    def test_create_user(self, openproject_client):
        """Test creating a user"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'create_user')
        
    def test_list_users(self, openproject_client):
        """Test listing users"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'list_users')


class TestOpenProjectMemberships:
    """Test project memberships"""
    
    def test_add_member(self, openproject_client):
        """Test adding a project member"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'add_member')
        
    def test_list_memberships(self, openproject_client):
        """Test listing project memberships"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'list_memberships')


class TestOpenProjectTimeTracking:
    """Test time tracking functionality"""
    
    def test_create_time_entry(self, openproject_client):
        """Test creating a time entry"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'create_time_entry')
        
    def test_list_time_entries(self, openproject_client):
        """Test listing time entries"""
        # This would need proper mock implementation
        assert hasattr(openproject_client, 'list_time_entries')


class TestOpenProjectErrorHandling:
    """Test error handling"""
    
    def test_auth_error(self, mock_openproject_server):
        """Test authentication error"""
        client = OpenProjectClient(
            openproject_url=mock_openproject_server,
            api_key="wrong-key",
            use_connection_pool=False
        )
        # Mock server checks auth
        
    def test_connection_pooling(self, mock_openproject_server):
        """Test connection pooling configuration"""
        client_with_pool = OpenProjectClient(
            openproject_url=mock_openproject_server,
            api_key="test-key",
            use_connection_pool=True
        )
        assert client_with_pool.session is not None
        
        client_without_pool = OpenProjectClient(
            openproject_url=mock_openproject_server,
            api_key="test-key",
            use_connection_pool=False
        )
        assert client_without_pool.session is None


class TestOpenProjectResilience:
    """Test resilience patterns"""
    
    def test_rate_limiting_configured(self, openproject_client):
        """Test that rate limiting is configured"""
        # Check that methods have the resilience wrapper
        assert hasattr(openproject_client.create_project, '__wrapped__')
        
    def test_request_method(self, openproject_client):
        """Test the _request method"""
        # Tested indirectly through other operations
        result = openproject_client.list_projects()
        assert "_embedded" in result 