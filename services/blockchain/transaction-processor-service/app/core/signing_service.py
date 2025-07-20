"""
Signing Service - Handles transaction signing via Key Management Service
"""

import logging
from typing import Dict, Any, Optional
import json

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import Settings

logger = logging.getLogger(__name__)


class SigningService:
    """Handles transaction signing through Key Management Service"""
    
    def __init__(self, key_management_url: str, settings: Settings):
        self.key_management_url = key_management_url
        self.settings = settings
        
        # HTTP client for KMS API
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def sign_transaction(
        self,
        chain: str,
        address: str,
        transaction: Dict[str, Any]
    ) -> str:
        """Sign a transaction using KMS"""
        try:
            # Prepare signing request
            signing_request = {
                'chain': chain,
                'address': address,
                'transaction': transaction,
                'metadata': {
                    'service': self.settings.SERVICE_NAME,
                    'purpose': 'transaction_signing'
                }
            }
            
            # Sign via KMS
            signed_tx = await self._call_kms_sign(signing_request)
            
            # Verify signature if enabled
            if self.settings.TRANSACTION_SIGNATURE_VERIFICATION:
                await self._verify_signature(chain, address, transaction, signed_tx)
                
            return signed_tx
            
        except Exception as e:
            logger.error(f"Error signing transaction for {address} on {chain}: {e}")
            raise
            
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    async def _call_kms_sign(self, signing_request: Dict[str, Any]) -> str:
        """Call KMS to sign transaction"""
        response = await self.http_client.post(
            f"{self.key_management_url}/api/v1/sign/transaction",
            json=signing_request
        )
        response.raise_for_status()
        
        result = response.json()
        return result['signed_transaction']
        
    async def _verify_signature(
        self,
        chain: str,
        address: str,
        transaction: Dict[str, Any],
        signed_tx: str
    ):
        """Verify transaction signature"""
        try:
            response = await self.http_client.post(
                f"{self.key_management_url}/api/v1/verify/transaction",
                json={
                    'chain': chain,
                    'address': address,
                    'transaction': transaction,
                    'signed_transaction': signed_tx
                }
            )
            response.raise_for_status()
            
            result = response.json()
            if not result.get('valid', False):
                raise ValueError(f"Invalid signature for transaction from {address}")
                
        except Exception as e:
            logger.error(f"Signature verification failed: {e}")
            raise
            
    async def get_signing_addresses(self, chain: str) -> list[str]:
        """Get list of addresses available for signing"""
        try:
            response = await self.http_client.get(
                f"{self.key_management_url}/api/v1/addresses/{chain}"
            )
            response.raise_for_status()
            
            return response.json()['addresses']
            
        except Exception as e:
            logger.error(f"Error getting signing addresses for {chain}: {e}")
            return []
            
    async def check_signing_permission(
        self,
        chain: str,
        address: str,
        transaction_value: str
    ) -> bool:
        """Check if transaction signing is permitted"""
        try:
            # Check transaction value limit
            if int(transaction_value) > int(self.settings.MAX_TRANSACTION_VALUE_WEI):
                logger.warning(
                    f"Transaction value {transaction_value} exceeds limit "
                    f"{self.settings.MAX_TRANSACTION_VALUE_WEI}"
                )
                return False
                
            # Check with KMS for additional permissions
            response = await self.http_client.post(
                f"{self.key_management_url}/api/v1/permissions/check",
                json={
                    'chain': chain,
                    'address': address,
                    'action': 'sign_transaction',
                    'value': transaction_value
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get('permitted', False)
                
            return False
            
        except Exception as e:
            logger.error(f"Error checking signing permission: {e}")
            return False
            
    async def close(self):
        """Close HTTP client"""
        await self.http_client.aclose() 