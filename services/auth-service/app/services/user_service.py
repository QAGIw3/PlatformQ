from cassandra.cluster import Session
from uuid import UUID
from ..crud import (
    crud_user, crud_api_key, crud_refresh_token, crud_role, crud_subscription
)
from ..schemas.user import UserCreate

class UserService:
    def __init__(self, db: Session):
        self.db = db

    def create_full_user(self, tenant_id: UUID, user_in: UserCreate) -> dict:
        """
        Orchestrates the complete creation of a new user, including
        default roles and subscriptions.
        """
        new_user = crud_user.create_user(self.db, tenant_id=tenant_id, user=user_in)
        
        crud_role.assign_role_to_user(self.db, tenant_id=tenant_id, user_id=new_user.id, role='member')
        
        sub_create = {"user_id": new_user.id, "tier": 'free', "status": "active"}
        crud_subscription.create_subscription_for_user(self.db, tenant_id=tenant_id, subscription=sub_create)
        
        return new_user

    def soft_delete_full_user(self, tenant_id: UUID, user_id: UUID) -> dict:
        """
        Orchestrates the full soft-deletion of a user, including
        revoking all associated keys and tokens.
        """
        # Revoke all API keys
        api_keys = crud_api_key.get_api_keys_for_user(self.db, tenant_id=tenant_id, user_id=user_id)
        for key in api_keys:
            crud_api_key.revoke_api_key(self.db, tenant_id=tenant_id, prefix=key['key_prefix'], user_id=user_id)
            
        # Revoke all refresh tokens
        # (This remains inefficient, as noted before, but the logic now lives here)
        crud_refresh_token.revoke_all_for_user(self.db, tenant_id=tenant_id, user_id=user_id)
        
        # Perform the user soft delete
        return crud_user.soft_delete_user(self.db, tenant_id=tenant_id, user_id=user_id)

# Conceptual: We would need a new crud_refresh_token.revoke_all_for_user function. 