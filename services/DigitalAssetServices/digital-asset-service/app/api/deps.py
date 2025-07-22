from fastapi import Header, Depends, HTTPException, status
from sqlalchemy.orm import Session
from ...db.session import get_db
from ..postgres_db import SessionLocal
from ..repository import DigitalAssetRepository

# Placeholder for user model. In a real app, this might come from a shared library.
class MockUser:
    def __init__(self, id, tenant_id):
        self.id = id
        self.tenant_id = tenant_id
        self.status = "active"

def get_current_tenant_user_and_reputation(
    user_id: str = Header(None, alias="X-Authenticated-Userid"),
    tenant_id: str = Header(None, alias="X-Tid"),
    reputation: int = Header(0, alias="X-User-Reputation"),
    db: Session = Depends(get_db)
):
    """
    This dependency trusts headers injected by an upstream gateway.
    It returns a context dictionary with a mock user, tenant_id, and reputation score.
    """
    if not user_id or not tenant_id:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User or Tenant ID missing from trusted headers."
        )
    
    # This service also does not have a user table, so we use a mock user.
    mock_user = MockUser(id=user_id, tenant_id=tenant_id)

    return {"user": mock_user, "tenant_id": tenant_id, "reputation": reputation}

# Alias for easier use in endpoints
get_current_context = get_current_tenant_user_and_reputation 

def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_digital_asset_repository(db: SessionLocal = Depends(get_db_session)) -> DigitalAssetRepository:
    return DigitalAssetRepository(db) 