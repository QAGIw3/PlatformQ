"""Vault integration for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Tuple, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
import asyncio
import logging
import json
from cryptography.fernet import Fernet

from platformq_shared.vault.vault_client import VaultClient
from tenacity import retry, stop_after_attempt, wait_exponential
from .base import BaseIntegration, CacheableMixin, LeaseManagerMixin

logger = logging.getLogger(__name__)


@dataclass
class VaultConfig:
    """Vault configuration for DataIntelligenceSuite services."""
    
    # Database engines
    enable_database_engines: bool = True
    database_roles: Dict[str, List[str]] = None
    
    # Encryption
    enable_encryption: bool = True
    encryption_keys: List[str] = None
    
    # Transit engine
    enable_transit: bool = True
    transit_keys: List[str] = None
    
    # PKI
    enable_pki: bool = True
    pki_roles: List[str] = None
    
    # KV paths
    kv_mount: str = "data-intelligence"
    shared_kv_mount: str = "shared"
    
    def __post_init__(self):
        if self.database_roles is None:
            self.database_roles = {
                "postgres": ["readonly", "readwrite", "admin"],
                "cassandra": ["reader", "writer", "admin"],
                "elasticsearch": ["search", "index", "admin"],
                "ignite": ["reader", "writer", "admin"]
            }
        if self.encryption_keys is None:
            self.encryption_keys = ["pii", "financial", "ml-models"]
        if self.transit_keys is None:
            self.transit_keys = ["default", "sensitive", "archive"]
        if self.pki_roles is None:
            self.pki_roles = ["service", "client"]


class VaultIntegration(BaseIntegration, CacheableMixin, LeaseManagerMixin):
    """
    Vault integration for DataIntelligenceSuite services.
    
    Provides:
    - Dynamic database credentials
    - Encryption key management
    - Transit encryption
    - PKI certificate management
    - Secret storage
    """
    
    def __init__(
        self,
        vault_client: VaultClient,
        service_name: str,
        config: Optional[VaultConfig] = None
    ):
        super().__init__(service_name, config)
        self.client = vault_client
        self.config = config or VaultConfig()
        
        # Credential caches
        self._db_credentials: Dict[str, Dict[str, Any]] = {}
        self._encryption_keys: Dict[str, bytes] = {}
        
    async def initialize(self):
        """Initialize Vault integration."""
        try:
            # Set up database engines if enabled
            if self.config.enable_database_engines:
                await self._setup_database_engines()
                
            # Load encryption keys if enabled
            if self.config.enable_encryption:
                await self._load_encryption_keys()
                
            # Set up transit engine if enabled
            if self.config.enable_transit:
                await self._setup_transit_engine()
                
            # Set up PKI if enabled
            if self.config.enable_pki:
                await self._setup_pki()
                
            # Start credential renewal
            await self._start_credential_renewal()
            
            self._initialized = True
            logger.info(f"Vault integration initialized for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault integration: {e}")
            raise
            
    async def _setup_database_engines(self):
        """Set up database secret engines."""
        # This assumes the engines are already configured in Vault
        # In production, you might want to verify or configure them here
        for database, roles in self.config.database_roles.items():
            logger.info(f"Database engine {database} available with roles: {roles}")
            
    async def _load_encryption_keys(self):
        """Load encryption keys from Vault."""
        for key_name in self.config.encryption_keys:
            try:
                # Get or create encryption key
                key_path = f"{self.config.kv_mount}/encryption-keys/{key_name}"
                key_data = await self.client.read_secret(key_path)
                
                if not key_data:
                    # Generate new key
                    key = Fernet.generate_key()
                    await self.client.write_secret(
                        key_path,
                        {"key": key.decode(), "created": datetime.utcnow().isoformat()}
                    )
                    self._encryption_keys[key_name] = key
                else:
                    self._encryption_keys[key_name] = key_data["key"].encode()
                    
                logger.info(f"Loaded encryption key: {key_name}")
                
            except Exception as e:
                logger.error(f"Failed to load encryption key {key_name}: {e}")
                
    async def _setup_transit_engine(self):
        """Set up transit encryption engine."""
        # Ensure transit keys exist
        for key_name in self.config.transit_keys:
            try:
                # Check if key exists
                await self.client.read_secret(f"transit/keys/{key_name}")
                logger.info(f"Transit key {key_name} already exists")
            except:
                # Create key
                await self.client.write_secret(
                    f"transit/keys/{key_name}",
                    {"type": "aes256-gcm96"}
                )
                logger.info(f"Created transit key: {key_name}")
                
    async def _setup_pki(self):
        """Set up PKI certificate management."""
        # This assumes PKI is already configured
        for role in self.config.pki_roles:
            logger.info(f"PKI role available: {role}")
            
    async def _start_credential_renewal(self):
        """Start credential renewal tasks."""
        # Create a renewal task
        self._create_task(self._renewal_loop())
        
    async def _renewal_loop(self):
        """Main renewal loop for all credentials."""
        while True:
            try:
                # Renew active leases
                for lease_id in list(self._active_leases.values()):
                    try:
                        await self.client.renew_lease(lease_id)
                        logger.debug(f"Renewed lease: {lease_id}")
                    except Exception as e:
                        logger.error(f"Failed to renew lease {lease_id}: {e}")
                        
                # Sleep for half the lease duration
                await asyncio.sleep(1800)  # 30 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in renewal loop: {e}")
                await asyncio.sleep(60)
                
    async def _renew_lease(self, lease_id: str):
        """Renew a Vault lease."""
        await self.client.renew_lease(lease_id)
        
    async def _revoke_lease(self, lease_id: str):
        """Revoke a Vault lease."""
        await self.client.revoke_lease(lease_id)
                
    # Database credentials
    @asynccontextmanager
    async def get_database_credentials(
        self,
        database: str,
        role: str = "readonly"
    ) -> AsyncIterator[Tuple[Dict[str, Any], str]]:
        """Get dynamic database credentials."""
        # Validate database and role
        if database not in self.config.database_roles:
            raise ValueError(f"Unknown database: {database}")
        if role not in self.config.database_roles[database]:
            raise ValueError(f"Unknown role {role} for database {database}")
            
        # Get credentials from Vault
        creds = await self.client.get_database_credentials(database, role)
        lease_id = creds["lease_id"]
        
        # Track lease
        self._active_leases[f"{database}-{role}"] = lease_id
        
        try:
            yield (creds["data"], lease_id)
        finally:
            # Revoke lease
            try:
                await self.client.revoke_lease(lease_id)
                if f"{database}-{role}" in self._active_leases:
                    del self._active_leases[f"{database}-{role}"]
                logger.info(f"Revoked credentials for {database}/{role}")
            except Exception as e:
                logger.error(f"Failed to revoke lease: {e}")
                
    # Encryption
    def get_fernet(self, key_name: str = "default") -> Fernet:
        """Get Fernet instance for encryption/decryption."""
        if key_name not in self._encryption_keys:
            # Use default key if specific key not found
            if "default" in self._encryption_keys:
                key_name = "default"
            else:
                raise ValueError(f"Encryption key not found: {key_name}")
                
        return Fernet(self._encryption_keys[key_name])
        
    def encrypt(self, data: bytes, key_name: str = "default") -> bytes:
        """Encrypt data using specified key."""
        fernet = self.get_fernet(key_name)
        return fernet.encrypt(data)
        
    def decrypt(self, encrypted_data: bytes, key_name: str = "default") -> bytes:
        """Decrypt data using specified key."""
        fernet = self.get_fernet(key_name)
        return fernet.decrypt(encrypted_data)
        
    # Transit encryption
    async def transit_encrypt(
        self,
        plaintext: str,
        key_name: str = "default"
    ) -> Dict[str, Any]:
        """Encrypt data using transit engine."""
        return await self.client.transit_encrypt(key_name, plaintext)
        
    async def transit_decrypt(
        self,
        ciphertext: str,
        key_name: str = "default"
    ) -> str:
        """Decrypt data using transit engine."""
        return await self.client.transit_decrypt(key_name, ciphertext)
        
    # PKI certificates
    async def get_certificate(
        self,
        role: str = "service",
        common_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get PKI certificate."""
        if common_name is None:
            common_name = f"{self.service_name}.data-intelligence.local"
            
        return await self.client.issue_certificate(
            role,
            common_name,
            ttl="24h"
        )
        
    # Secret management
    async def get_secret(self, path: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """Get secret from KV store."""
        full_path = f"{self.config.kv_mount}/{self.service_name}/{path}"
        
        # Check cache first
        if use_cache:
            cached = self._get_from_cache(full_path)
            if cached is not None:
                return cached
                
        # Read from Vault
        secret = await self.client.read_secret(full_path)
        
        # Cache the result
        if secret and use_cache:
            self._set_cache(full_path, secret)
            
        return secret
        
    async def put_secret(self, path: str, data: Dict[str, Any]):
        """Write secret to KV store."""
        full_path = f"{self.config.kv_mount}/{self.service_name}/{path}"
        await self.client.write_secret(full_path, data)
        
    async def get_shared_secret(self, path: str) -> Optional[Dict[str, Any]]:
        """Get shared secret from shared KV store."""
        full_path = f"{self.config.shared_kv_mount}/data-intelligence/{path}"
        return await self.client.read_secret(full_path)
        
    # Cleanup
    async def shutdown(self):
        """Shutdown Vault integration."""
        # Cancel all tasks
        await self._cancel_tasks()
        
        # Revoke all active leases
        await self._revoke_all_leases()
        
        self._initialized = False
        logger.info(f"Vault integration shutdown for {self.service_name}") 