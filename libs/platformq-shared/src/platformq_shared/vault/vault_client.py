"""
HashiCorp Vault Client

Provides secure secret management, key storage, and cryptographic operations.
Integrates with PlatformQ services for secure credential handling.
"""

import hvac
import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import base64
import json
from dataclasses import dataclass
from enum import Enum
import asyncio
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class VaultEngine(Enum):
    """Vault secret engines"""
    KV_V2 = "kv-v2"
    TRANSIT = "transit"
    PKI = "pki"
    DATABASE = "database"
    AWS = "aws"
    SSH = "ssh"
    TOTP = "totp"
    TRANSFORM = "transform"


@dataclass
class VaultConfig:
    """Vault configuration"""
    url: str = "http://vault:8200"
    token: Optional[str] = None
    app_role_id: Optional[str] = None
    app_role_secret: Optional[str] = None
    namespace: Optional[str] = None
    ssl_verify: bool = True
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


@dataclass
class TransitKey:
    """Transit encryption key info"""
    name: str
    key_type: str
    latest_version: int
    min_decryption_version: int
    min_encryption_version: int
    deletion_allowed: bool
    exportable: bool
    allow_plaintext_backup: bool


class VaultClient:
    """
    HashiCorp Vault client for PlatformQ.
    
    Features:
    - Secret management (KV v2)
    - Transit encryption/decryption
    - PKI certificate management
    - Dynamic database credentials
    - Key generation and management
    - Audit logging
    - High availability support
    """
    
    def __init__(self, config: VaultConfig):
        self.config = config
        self.client: Optional[hvac.Client] = None
        self._lock = asyncio.Lock()
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize Vault client and authenticate"""
        async with self._lock:
            if self._initialized:
                return
                
            try:
                # Create client
                self.client = hvac.Client(
                    url=self.config.url,
                    token=self.config.token,
                    namespace=self.config.namespace,
                    verify=self.config.ssl_verify,
                    timeout=self.config.timeout
                )
                
                # Authenticate
                if self.config.app_role_id and self.config.app_role_secret:
                    await self._authenticate_app_role()
                elif not self.config.token:
                    raise ValueError("No authentication method provided")
                
                # Verify authentication
                if not self.client.is_authenticated():
                    raise Exception("Failed to authenticate with Vault")
                
                # Enable required engines
                await self._ensure_engines()
                
                self._initialized = True
                logger.info("Vault client initialized successfully")
                
            except Exception as e:
                logger.error(f"Failed to initialize Vault client: {e}")
                raise
    
    async def _authenticate_app_role(self) -> None:
        """Authenticate using AppRole"""
        response = await asyncio.to_thread(
            self.client.auth.approle.login,
            role_id=self.config.app_role_id,
            secret_id=self.config.app_role_secret
        )
        self.client.token = response['auth']['client_token']
        
    async def _ensure_engines(self) -> None:
        """Ensure required secret engines are enabled"""
        engines_to_enable = {
            "secret": VaultEngine.KV_V2,
            "transit": VaultEngine.TRANSIT,
            "pki": VaultEngine.PKI,
            "database": VaultEngine.DATABASE
        }
        
        current_engines = await asyncio.to_thread(
            self.client.sys.list_mounted_secrets_engines
        )
        
        for path, engine in engines_to_enable.items():
            full_path = f"{path}/"
            if full_path not in current_engines:
                logger.info(f"Enabling {engine.value} engine at {path}")
                await asyncio.to_thread(
                    self.client.sys.enable_secrets_engine,
                    backend_type=engine.value,
                    path=path
                )
    
    # KV v2 Secret Management
    
    async def write_secret(self, path: str, data: Dict[str, Any], 
                          mount_point: str = "secret") -> None:
        """Write secret to KV v2 engine"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.secrets.kv.v2.create_or_update_secret,
            path=path,
            secret=data,
            mount_point=mount_point
        )
        logger.debug(f"Wrote secret to {mount_point}/{path}")
        
    async def read_secret(self, path: str, version: Optional[int] = None,
                         mount_point: str = "secret") -> Dict[str, Any]:
        """Read secret from KV v2 engine"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.kv.v2.read_secret_version,
            path=path,
            version=version,
            mount_point=mount_point
        )
        
        return response['data']['data']
        
    async def delete_secret(self, path: str, mount_point: str = "secret") -> None:
        """Delete secret from KV v2 engine"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.secrets.kv.v2.delete_metadata_and_all_versions,
            path=path,
            mount_point=mount_point
        )
        logger.debug(f"Deleted secret at {mount_point}/{path}")
        
    async def list_secrets(self, path: str = "", mount_point: str = "secret") -> List[str]:
        """List secrets at path"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.kv.v2.list_secrets,
            path=path,
            mount_point=mount_point
        )
        
        return response.get('data', {}).get('keys', [])
    
    # Transit Encryption
    
    async def create_transit_key(self, key_name: str, key_type: str = "aes256-gcm96",
                               exportable: bool = False) -> TransitKey:
        """Create a new transit encryption key"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.secrets.transit.create_key,
            name=key_name,
            key_type=key_type,
            exportable=exportable,
            mount_point="transit"
        )
        
        # Read key info
        key_info = await self.read_transit_key(key_name)
        logger.info(f"Created transit key: {key_name}")
        return key_info
        
    async def read_transit_key(self, key_name: str) -> TransitKey:
        """Read transit key information"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.transit.read_key,
            name=key_name,
            mount_point="transit"
        )
        
        data = response['data']
        return TransitKey(
            name=key_name,
            key_type=data['type'],
            latest_version=data['latest_version'],
            min_decryption_version=data['min_decryption_version'],
            min_encryption_version=data['min_encryption_version'],
            deletion_allowed=data['deletion_allowed'],
            exportable=data['exportable'],
            allow_plaintext_backup=data['allow_plaintext_backup']
        )
        
    async def encrypt_data(self, key_name: str, plaintext: str,
                          context: Optional[str] = None) -> str:
        """Encrypt data using transit key"""
        await self._ensure_initialized()
        
        # Base64 encode plaintext
        encoded = base64.b64encode(plaintext.encode()).decode()
        
        response = await asyncio.to_thread(
            self.client.secrets.transit.encrypt_data,
            name=key_name,
            plaintext=encoded,
            context=context,
            mount_point="transit"
        )
        
        return response['data']['ciphertext']
        
    async def decrypt_data(self, key_name: str, ciphertext: str,
                          context: Optional[str] = None) -> str:
        """Decrypt data using transit key"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.transit.decrypt_data,
            name=key_name,
            ciphertext=ciphertext,
            context=context,
            mount_point="transit"
        )
        
        # Base64 decode result
        encoded = response['data']['plaintext']
        return base64.b64decode(encoded).decode()
        
    async def sign_data(self, key_name: str, data: str, 
                       algorithm: str = "sha2-256") -> str:
        """Sign data using transit key"""
        await self._ensure_initialized()
        
        # Base64 encode data
        encoded = base64.b64encode(data.encode()).decode()
        
        response = await asyncio.to_thread(
            self.client.secrets.transit.sign_data,
            name=key_name,
            hash_input=encoded,
            algorithm=algorithm,
            mount_point="transit"
        )
        
        return response['data']['signature']
        
    async def verify_signature(self, key_name: str, data: str, signature: str,
                             algorithm: str = "sha2-256") -> bool:
        """Verify data signature"""
        await self._ensure_initialized()
        
        # Base64 encode data
        encoded = base64.b64encode(data.encode()).decode()
        
        response = await asyncio.to_thread(
            self.client.secrets.transit.verify_signed_data,
            name=key_name,
            hash_input=encoded,
            signature=signature,
            algorithm=algorithm,
            mount_point="transit"
        )
        
        return response['data']['valid']
    
    # Blockchain Key Management
    
    async def generate_blockchain_key(self, key_type: str = "secp256k1") -> Dict[str, str]:
        """Generate blockchain key pair"""
        await self._ensure_initialized()
        
        # Use transit engine to generate key
        key_name = f"blockchain_{datetime.utcnow().timestamp()}"
        
        # Create exportable key
        await self.create_transit_key(key_name, key_type=key_type, exportable=True)
        
        # Export key
        response = await asyncio.to_thread(
            self.client.secrets.transit.export_key,
            name=key_name,
            key_type="encryption-key",
            mount_point="transit"
        )
        
        key_data = response['data']['keys']['1']
        
        # Store key metadata
        await self.write_secret(
            f"blockchain/keys/{key_name}",
            {
                "key_name": key_name,
                "key_type": key_type,
                "created_at": datetime.utcnow().isoformat(),
                "public_key": self._derive_public_key(key_data, key_type)
            }
        )
        
        return {
            "key_name": key_name,
            "key_type": key_type,
            "key_data": key_data
        }
        
    async def sign_blockchain_transaction(self, key_name: str, 
                                        transaction_data: Dict[str, Any]) -> str:
        """Sign blockchain transaction"""
        await self._ensure_initialized()
        
        # Serialize transaction
        tx_bytes = json.dumps(transaction_data, sort_keys=True).encode()
        
        # Sign using transit
        signature = await self.sign_data(key_name, tx_bytes.decode())
        
        return signature
        
    # PKI Certificate Management
    
    async def generate_certificate(self, role_name: str, common_name: str,
                                 ttl: str = "24h") -> Dict[str, str]:
        """Generate PKI certificate"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.pki.generate_certificate,
            name=role_name,
            common_name=common_name,
            ttl=ttl,
            mount_point="pki"
        )
        
        return {
            "certificate": response['data']['certificate'],
            "private_key": response['data']['private_key'],
            "ca_chain": response['data']['ca_chain'],
            "serial_number": response['data']['serial_number']
        }
        
    # Database Credential Management
    
    async def get_database_credentials(self, role_name: str) -> Dict[str, str]:
        """Get dynamic database credentials"""
        await self._ensure_initialized()
        
        response = await asyncio.to_thread(
            self.client.secrets.database.generate_credentials,
            name=role_name,
            mount_point="database"
        )
        
        return {
            "username": response['data']['username'],
            "password": response['data']['password'],
            "lease_id": response['lease_id'],
            "lease_duration": response['lease_duration']
        }
        
    async def revoke_lease(self, lease_id: str) -> None:
        """Revoke a lease (e.g., database credentials)"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.sys.revoke_lease,
            lease_id=lease_id
        )
        
    # Utility Methods
    
    async def _ensure_initialized(self) -> None:
        """Ensure client is initialized"""
        if not self._initialized:
            await self.initialize()
            
    def _derive_public_key(self, private_key: str, key_type: str) -> str:
        """Derive public key from private key"""
        # This is a placeholder - actual implementation depends on key type
        # In production, use appropriate cryptographic libraries
        return f"public_key_for_{private_key[:8]}"
        
    @asynccontextmanager
    async def transaction(self):
        """Context manager for transactional operations"""
        # Start transaction
        tx_id = f"tx_{datetime.utcnow().timestamp()}"
        logger.debug(f"Starting Vault transaction: {tx_id}")
        
        try:
            yield tx_id
            logger.debug(f"Vault transaction completed: {tx_id}")
        except Exception as e:
            logger.error(f"Vault transaction failed: {tx_id} - {e}")
            raise
            
    async def health_check(self) -> Dict[str, Any]:
        """Check Vault health"""
        try:
            response = await asyncio.to_thread(
                self.client.sys.read_health_status
            )
            
            return {
                "initialized": response.get('initialized', False),
                "sealed": response.get('sealed', True),
                "standby": response.get('standby', False),
                "version": response.get('version', 'unknown'),
                "cluster_name": response.get('cluster_name'),
                "cluster_id": response.get('cluster_id')
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"error": str(e)}
            
    async def close(self) -> None:
        """Close Vault client"""
        if self.client:
            # Revoke token if using token auth
            if self.config.token and not (self.config.app_role_id):
                try:
                    await asyncio.to_thread(self.client.auth.token.revoke_self)
                except Exception as e:
                    logger.error(f"Failed to revoke token: {e}")
                    
        self._initialized = False
        logger.info("Vault client closed") 