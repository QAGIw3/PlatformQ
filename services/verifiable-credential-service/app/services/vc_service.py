"""
Verifiable Credential service functions
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from platformq_shared.cache import get_cache_client

logger = logging.getLogger(__name__)


async def get_credential_by_id(
    credential_id: str,
    tenant_id: str
) -> Optional[Dict[str, Any]]:
    """
    Retrieve a credential by ID from cache or storage
    
    Args:
        credential_id: The credential ID
        tenant_id: The tenant ID
        
    Returns:
        The credential data or None if not found
    """
    try:
        # Try cache first
        cache_client = await get_cache_client()
        if cache_client:
            cached = await cache_client.get(f"vc:{tenant_id}:{credential_id}")
            if cached:
                return json.loads(cached)
        
        # In production, this would query the database
        # For now, return a mock credential
        logger.warning(f"Credential {credential_id} not found in cache, returning mock")
        
        return {
            "id": credential_id,
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://platformq.com/contexts/v1"
            ],
            "type": ["VerifiableCredential", "PlatformQCredential"],
            "issuer": f"did:platformq:{tenant_id}:issuer",
            "issuanceDate": datetime.utcnow().isoformat() + "Z",
            "credentialSubject": {
                "id": f"did:platformq:{tenant_id}:user123",
                "achievement": "Asset Creation",
                "score": 100
            }
        }
        
    except Exception as e:
        logger.error(f"Error retrieving credential {credential_id}: {e}")
        return None


async def get_credentials_for_user(
    user_id: str,
    tenant_id: str
) -> List[Dict[str, Any]]:
    """
    Get all credentials for a user
    
    Args:
        user_id: The user ID
        tenant_id: The tenant ID
        
    Returns:
        List of credentials
    """
    try:
        # In production, this would query the database
        # For now, return mock credentials
        user_did = f"did:platformq:{tenant_id}:{user_id}"
        
        return [
            {
                "id": "vc:platformq:cred1",
                "@context": [
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.com/contexts/v1"
                ],
                "type": ["VerifiableCredential", "AssetCreationCredential"],
                "issuer": f"did:platformq:{tenant_id}:issuer",
                "issuanceDate": "2024-01-15T10:00:00Z",
                "credentialSubject": {
                    "id": user_did,
                    "assetId": "asset123",
                    "assetType": "3D_MODEL",
                    "createdAt": "2024-01-15T09:30:00Z"
                }
            },
            {
                "id": "vc:platformq:cred2",
                "@context": [
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.com/contexts/v1"
                ],
                "type": ["VerifiableCredential", "TrustScoreCredential"],
                "issuer": f"did:platformq:{tenant_id}:issuer",
                "issuanceDate": "2024-01-10T10:00:00Z",
                "credentialSubject": {
                    "id": user_did,
                    "trustScore": 85,
                    "dimensions": {
                        "technical": 90,
                        "collaboration": 80,
                        "creativity": 85
                    }
                }
            }
        ]
        
    except Exception as e:
        logger.error(f"Error retrieving credentials for user {user_id}: {e}")
        return []


async def store_credential(
    credential: Dict[str, Any],
    tenant_id: str
) -> bool:
    """
    Store a credential in cache and database
    
    Args:
        credential: The credential data
        tenant_id: The tenant ID
        
    Returns:
        True if successful
    """
    try:
        credential_id = credential.get("id")
        if not credential_id:
            logger.error("Credential missing ID")
            return False
        
        # Store in cache
        cache_client = await get_cache_client()
        if cache_client:
            await cache_client.setex(
                f"vc:{tenant_id}:{credential_id}",
                86400,  # 24 hours
                json.dumps(credential)
            )
        
        # In production, also store in database
        logger.info(f"Stored credential {credential_id} for tenant {tenant_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error storing credential: {e}")
        return False 