from cassandra.cluster import Session
from uuid import UUID
from ..crud import (
    crud_user, crud_api_key, crud_refresh_token, crud_role, crud_subscription
)
from ..schemas.user import UserCreate, User
import logging
import requests # For interacting with VC service
from fastapi import HTTPException

logger = logging.getLogger(__name__)


class UserService:
    def __init__(self, db: Session):
        self.db = db

    def create_full_user(self, tenant_id: UUID, user_in: UserCreate) -> dict:
        """
        Orchestrates the complete creation of a new user, including
        default roles and subscriptions.
        """
        new_user = crud_user.create_user(self.db, tenant_id=tenant_id, user=user_in, did=user_in.did)
        
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

    async def link_did_to_user(self, user_id: UUID, tenant_id: UUID, did: str, challenge_response: str, vc_service_url: str) -> User:
        """
        Links a Decentralized Identifier (DID) to an existing user account
        after successful verification via a challenge-response mechanism.
        
        Args:
            user_id: The UUID of the user in platformQ.
            tenant_id: The UUID of the tenant.
            did: The DID to link.
            challenge_response: The signed challenge from the user's DID key.
            vc_service_url: The URL of the Verifiable Credential Service.
            
        Returns:
            The updated User object with the linked DID.
            
        Raises:
            HTTPException: If DID verification fails or VC service is unavailable.
        """
        # Step 1: Request a challenge from the DID
        try:
            challenge_request_url = f"{vc_service_url}/api/v1/dids/{did}/challenge"
            challenge_response_obj = requests.post(challenge_request_url, json={"user_id": str(user_id)})
            challenge_response_obj.raise_for_status()
            challenge_data = challenge_response_obj.json()
            challenge = challenge_data.get("challenge")
            if not challenge:
                logger.error(f"VC Service did not return a challenge for DID {did}")
                raise HTTPException(status_code=500, detail="Failed to get DID challenge from VC Service")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to VC Service for DID challenge: {e}")
            raise HTTPException(status_code=503, detail="VC Service unavailable or inaccessible")

        # Step 2: Verify the signed challenge using the VC service
        try:
            verify_request_url = f"{vc_service_url}/api/v1/dids/{did}/verify-signature"
            verify_response = requests.post(verify_request_url, json={
                "challenge": challenge,
                "signature": challenge_response,
            })
            verify_response.raise_for_status()
            verification_result = verify_response.json()
            if not verification_result.get("is_valid"): # Assuming VC service returns {"is_valid": true/false}
                logger.warning(f"DID signature verification failed for DID {did}")
                raise HTTPException(status_code=400, detail="DID signature verification failed")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to VC Service for DID verification: {e}")
            raise HTTPException(status_code=503, detail="VC Service unavailable or inaccessible")

        # Step 3: Link DID to user in auth-service database
        user_update_data = {"did": did}
        updated_user = crud_user.update_user(self.db, tenant_id=tenant_id, user_id=user_id, user_update=user_update_data)
        logger.info(f"Successfully linked DID {did} to user {user_id}")

        return updated_user


# Conceptual: We would need a new crud_refresh_token.revoke_all_for_user function. 