import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from uuid import uuid4

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../libs/platformq-shared/src'))
from platformq_shared.base_service import create_base_app

from app.main import app 

@pytest.fixture(scope="module")
def client():
    # We can override dependencies for testing purposes
    # For now, we won't override anything, but this is where you would
    # mock database sessions, etc. for more complex tests.
    yield TestClient(app)

def test_create_and_get_user(client):
    """
    Test creating a user via the API and then retrieving it.
    """
    user_email = "test.api@example.com"
    user_full_name = "Test API User"
    user_id = uuid4()
    
    mock_user_data = {
        "id": user_id,
        "email": user_email,
        "full_name": user_full_name,
        "status": "active"
    }

    # Mock the response from the underlying CRUD function
    with patch("app.crud.crud_user.create_user", return_value=mock_user_data) as mock_create:
        with patch("app.crud.crud_user.get_user_by_email", return_value=mock_user_data) as mock_get:
            
            # 1. Create the user
            response = client.post(
                "/api/v1/users/",
                json={"email": user_email, "full_name": user_full_name}
            )
            
            assert response.status_code == 200
            assert response.json()["email"] == user_email
            assert response.json()["full_name"] == user_full_name
            mock_create.assert_called_once()

            # 2. Get the user
            response = client.get(f"/api/v1/users/by-email?email={user_email}")
            
            assert response.status_code == 200
            assert response.json()["email"] == user_email
            mock_get.assert_called_once() 