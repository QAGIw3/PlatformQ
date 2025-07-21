"""
Vault Manager - Interfaces with HashiCorp Vault for key operations
"""

import logging
from typing import Dict, Any, List, Optional
import base64
import json

import hvac
from hvac.exceptions import InvalidPath, InvalidRequest
from prometheus_client import Counter, Histogram

from ..config import Settings

logger = logging.getLogger(__name__)

# Metrics
vault_operations = Counter(
    'vault_operations_total',
    'Total Vault operations',
    ['operation', 'status']
)

vault_operation_duration = Histogram(
    'vault_operation_duration_seconds',
    'Vault operation duration',
    ['operation']
)


class VaultManager:
    """Manages interactions with HashiCorp Vault"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        self._initialized = False
        
    async def initialize(self):
        """Initialize Vault client"""
        logger.info("Initializing Vault Manager")
        
        # Create Vault client
        self.client = hvac.Client(
            url=self.settings.VAULT_ADDR,
            token=self.settings.VAULT_TOKEN
        )
        
        # Verify authentication
        if not self.client.is_authenticated():
            raise Exception("Vault authentication failed")
            
        # Enable required secrets engines if not already enabled
        await self._ensure_secrets_engines()
        
        self._initialized = True
        logger.info("Vault Manager initialized")
        
    async def _ensure_secrets_engines(self):
        """Ensure required secrets engines are enabled"""
        try:
            # Check if transit engine is enabled
            enabled_engines = self.client.sys.list_mounted_secrets_engines()
            
            if f"{self.settings.VAULT_TRANSIT_PATH}/" not in enabled_engines['data']:
                # Enable transit engine
                self.client.sys.enable_secrets_engine(
                    backend_type='transit',
                    path=self.settings.VAULT_TRANSIT_PATH
                )
                logger.info(f"Enabled transit engine at {self.settings.VAULT_TRANSIT_PATH}")
                
        except Exception as e:
            logger.error(f"Error ensuring secrets engines: {e}")
            
    async def create_key(
        self,
        key_name: str,
        key_type: str = "ecdsa-p256",
        exportable: bool = False,
        auto_rotate_period: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new cryptographic key"""
        with vault_operation_duration.labels(operation="create_key").time():
            try:
                # Create key in transit engine
                self.client.secrets.transit.create_key(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    key_type=key_type,
                    exportable=exportable,
                    auto_rotate_period=auto_rotate_period
                )
                
                vault_operations.labels(operation="create_key", status="success").inc()
                
                # Get key info
                key_info = await self.get_key_info(key_name)
                
                logger.info(f"Created key: {key_name}")
                return key_info
                
            except Exception as e:
                vault_operations.labels(operation="create_key", status="error").inc()
                logger.error(f"Error creating key {key_name}: {e}")
                raise
                
    async def get_key_info(self, key_name: str) -> Dict[str, Any]:
        """Get information about a key"""
        with vault_operation_duration.labels(operation="get_key_info").time():
            try:
                response = self.client.secrets.transit.read_key(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH
                )
                
                vault_operations.labels(operation="get_key_info", status="success").inc()
                
                return response['data']
                
            except InvalidPath:
                vault_operations.labels(operation="get_key_info", status="not_found").inc()
                return None
            except Exception as e:
                vault_operations.labels(operation="get_key_info", status="error").inc()
                logger.error(f"Error getting key info for {key_name}: {e}")
                raise
                
    async def rotate_key(self, key_name: str) -> Dict[str, Any]:
        """Rotate a key to a new version"""
        with vault_operation_duration.labels(operation="rotate_key").time():
            try:
                self.client.secrets.transit.rotate_key(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH
                )
                
                vault_operations.labels(operation="rotate_key", status="success").inc()
                
                # Get updated key info
                key_info = await self.get_key_info(key_name)
                
                logger.info(f"Rotated key: {key_name} to version {key_info['latest_version']}")
                return key_info
                
            except Exception as e:
                vault_operations.labels(operation="rotate_key", status="error").inc()
                logger.error(f"Error rotating key {key_name}: {e}")
                raise
                
    async def sign_data(
        self,
        key_name: str,
        data: bytes,
        hash_algorithm: str = "sha2-256",
        signature_algorithm: Optional[str] = None,
        key_version: Optional[int] = None
    ) -> str:
        """Sign data using a key"""
        with vault_operation_duration.labels(operation="sign_data").time():
            try:
                # Encode data as base64
                encoded_data = base64.b64encode(data).decode('utf-8')
                
                # Sign data
                response = self.client.secrets.transit.sign_data(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    hash_input=encoded_data,
                    hash_algorithm=hash_algorithm,
                    signature_algorithm=signature_algorithm,
                    key_version=key_version
                )
                
                vault_operations.labels(operation="sign_data", status="success").inc()
                
                return response['data']['signature']
                
            except Exception as e:
                vault_operations.labels(operation="sign_data", status="error").inc()
                logger.error(f"Error signing data with key {key_name}: {e}")
                raise
                
    async def verify_signature(
        self,
        key_name: str,
        data: bytes,
        signature: str,
        hash_algorithm: str = "sha2-256",
        signature_algorithm: Optional[str] = None
    ) -> bool:
        """Verify a signature"""
        with vault_operation_duration.labels(operation="verify_signature").time():
            try:
                # Encode data as base64
                encoded_data = base64.b64encode(data).decode('utf-8')
                
                # Verify signature
                response = self.client.secrets.transit.verify_signed_data(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    hash_input=encoded_data,
                    signature=signature,
                    hash_algorithm=hash_algorithm,
                    signature_algorithm=signature_algorithm
                )
                
                vault_operations.labels(operation="verify_signature", status="success").inc()
                
                return response['data']['valid']
                
            except Exception as e:
                vault_operations.labels(operation="verify_signature", status="error").inc()
                logger.error(f"Error verifying signature with key {key_name}: {e}")
                return False
                
    async def encrypt_data(
        self,
        key_name: str,
        plaintext: bytes,
        key_version: Optional[int] = None
    ) -> str:
        """Encrypt data using a key"""
        with vault_operation_duration.labels(operation="encrypt_data").time():
            try:
                # Encode plaintext as base64
                encoded_plaintext = base64.b64encode(plaintext).decode('utf-8')
                
                # Encrypt data
                response = self.client.secrets.transit.encrypt_data(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    plaintext=encoded_plaintext,
                    key_version=key_version
                )
                
                vault_operations.labels(operation="encrypt_data", status="success").inc()
                
                return response['data']['ciphertext']
                
            except Exception as e:
                vault_operations.labels(operation="encrypt_data", status="error").inc()
                logger.error(f"Error encrypting data with key {key_name}: {e}")
                raise
                
    async def decrypt_data(
        self,
        key_name: str,
        ciphertext: str
    ) -> bytes:
        """Decrypt data using a key"""
        with vault_operation_duration.labels(operation="decrypt_data").time():
            try:
                # Decrypt data
                response = self.client.secrets.transit.decrypt_data(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    ciphertext=ciphertext
                )
                
                vault_operations.labels(operation="decrypt_data", status="success").inc()
                
                # Decode from base64
                plaintext = base64.b64decode(response['data']['plaintext'])
                return plaintext
                
            except Exception as e:
                vault_operations.labels(operation="decrypt_data", status="error").inc()
                logger.error(f"Error decrypting data with key {key_name}: {e}")
                raise
                
    async def export_key(
        self,
        key_name: str,
        key_type: str = "signing-key",
        key_version: Optional[int] = None
    ) -> Dict[str, Any]:
        """Export a key (if exportable)"""
        with vault_operation_duration.labels(operation="export_key").time():
            try:
                response = self.client.secrets.transit.export_key(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    key_type=key_type,
                    version=key_version
                )
                
                vault_operations.labels(operation="export_key", status="success").inc()
                
                return response['data']['keys']
                
            except Exception as e:
                vault_operations.labels(operation="export_key", status="error").inc()
                logger.error(f"Error exporting key {key_name}: {e}")
                raise
                
    async def delete_key(self, key_name: str) -> bool:
        """Delete a key (requires key deletion to be allowed)"""
        with vault_operation_duration.labels(operation="delete_key").time():
            try:
                # First, update key configuration to allow deletion
                self.client.secrets.transit.update_key_configuration(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH,
                    deletion_allowed=True
                )
                
                # Delete the key
                self.client.secrets.transit.delete_key(
                    name=key_name,
                    mount_point=self.settings.VAULT_TRANSIT_PATH
                )
                
                vault_operations.labels(operation="delete_key", status="success").inc()
                
                logger.info(f"Deleted key: {key_name}")
                return True
                
            except Exception as e:
                vault_operations.labels(operation="delete_key", status="error").inc()
                logger.error(f"Error deleting key {key_name}: {e}")
                return False
                
    async def list_keys(self) -> List[str]:
        """List all keys"""
        with vault_operation_duration.labels(operation="list_keys").time():
            try:
                response = self.client.secrets.transit.list_keys(
                    mount_point=self.settings.VAULT_TRANSIT_PATH
                )
                
                vault_operations.labels(operation="list_keys", status="success").inc()
                
                return response.get('data', {}).get('keys', [])
                
            except Exception as e:
                vault_operations.labels(operation="list_keys", status="error").inc()
                logger.error(f"Error listing keys: {e}")
                return []
                
    async def store_secret(
        self,
        path: str,
        data: Dict[str, Any],
        cas: Optional[int] = None
    ) -> Dict[str, Any]:
        """Store a secret in KV store"""
        with vault_operation_duration.labels(operation="store_secret").time():
            try:
                response = self.client.secrets.kv.v2.create_or_update_secret(
                    path=path,
                    secret=data,
                    mount_point=self.settings.VAULT_KV_PATH,
                    cas=cas
                )
                
                vault_operations.labels(operation="store_secret", status="success").inc()
                
                return response['data']
                
            except Exception as e:
                vault_operations.labels(operation="store_secret", status="error").inc()
                logger.error(f"Error storing secret at {path}: {e}")
                raise
                
    async def read_secret(
        self,
        path: str,
        version: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Read a secret from KV store"""
        with vault_operation_duration.labels(operation="read_secret").time():
            try:
                response = self.client.secrets.kv.v2.read_secret_version(
                    path=path,
                    mount_point=self.settings.VAULT_KV_PATH,
                    version=version
                )
                
                vault_operations.labels(operation="read_secret", status="success").inc()
                
                return response['data']['data']
                
            except InvalidPath:
                vault_operations.labels(operation="read_secret", status="not_found").inc()
                return None
            except Exception as e:
                vault_operations.labels(operation="read_secret", status="error").inc()
                logger.error(f"Error reading secret at {path}: {e}")
                raise
                
    async def delete_secret(
        self,
        path: str,
        versions: Optional[List[int]] = None
    ) -> bool:
        """Delete a secret from KV store"""
        with vault_operation_duration.labels(operation="delete_secret").time():
            try:
                if versions:
                    # Delete specific versions
                    self.client.secrets.kv.v2.delete_secret_versions(
                        path=path,
                        mount_point=self.settings.VAULT_KV_PATH,
                        versions=versions
                    )
                else:
                    # Delete latest version
                    self.client.secrets.kv.v2.delete_latest_version_of_secret(
                        path=path,
                        mount_point=self.settings.VAULT_KV_PATH
                    )
                    
                vault_operations.labels(operation="delete_secret", status="success").inc()
                
                return True
                
            except Exception as e:
                vault_operations.labels(operation="delete_secret", status="error").inc()
                logger.error(f"Error deleting secret at {path}: {e}")
                return False 