import pytest
from unittest.mock import MagicMock, patch
from uuid import uuid4
import datetime

from app.crud import crud_user
from app.schemas.user import UserCreate

def test_create_user():
    """
    Test that the create_user function constructs and executes the correct
    Cassandra query and then calls the get_user_by_id function.
    """
    mock_db_session = MagicMock()
    
    # Mock the return value of get_user_by_id to avoid a real db call
    mock_created_user = {
        "id": uuid4(), 
        "email": "test@example.com", 
        "full_name": "Test User",
        "status": "active"
    }
    
    # We patch the 'get_user_by_id' function within the same module
    with patch('app.crud.crud_user.get_user_by_id', return_value=mock_created_user) as mock_get:
        user_to_create = UserCreate(email="test@example.com", full_name="Test User")
        
        created_user = crud_user.create_user(mock_db_session, user=user_to_create)
        
        # 1. Assert that the execute method was called on the mock session
        assert mock_db_session.execute.called
        
        # 2. Assert that the function tried to retrieve the user after creation
        assert mock_get.called
        
        # 3. Assert the result is what we expect
        assert created_user == mock_created_user 