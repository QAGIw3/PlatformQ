"""
Digital Asset Service - Vault & Consul Integration
"""

from typing import Dict, Any, Optional, List
import asyncio
from datetime import datetime, timedelta
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from dataclasses import dataclass
from enum import Enum
import hashlib
import logging

logger = logging.getLogger(__name__)

class AssetStorageProvider(Enum):
    MINIO = "minio"
    S3 = "s3" 
    IPFS = "ipfs"
    FILECOIN = "filecoin"

class AssetType(Enum):
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"
    MODEL_3D = "3d-model"
    CODE = "code"
    DATA = "data"
    OTHER = "other"

@dataclass
class StorageConfig:
    """Storage configuration for digital assets"""
    primary_provider: AssetStorageProvider = AssetStorageProvider.MINIO
    replication_enabled: bool = True
    replication_factor: int = 3
    encryption_enabled: bool = True
    compression_enabled: bool = True
    max_file_size_mb: int = 5000
    allowed_mime_types: List[str] = None
    
@dataclass
class ProcessingConfig:
    """Asset processing configuration"""
    auto_thumbnail: bool = True
    auto_metadata_extraction: bool = True
    virus_scanning: bool = True
    content_moderation: bool = False
    max_processing_time_seconds: int = 300
    enable_gpu_processing: bool = True


class DigitalAssetVaultIntegration:
    """Vault integration for digital asset service"""
    
    def __init__(self, vault_client: VaultClient, service_name: str = "digital-asset-service"):
        self.vault = vault_client
        self.service_name = service_name
        self._storage_creds: Dict[str, Any] = {}
        self._encryption_keys: Dict[str, bytes] = {}
        
    async def initialize(self):
        """Initialize Vault integration"""
        # Ensure required secrets exist
        await self._ensure_secrets_exist()
        
        # Load storage credentials
        await self._load_storage_credentials()
        
        # Initialize encryption keys
        await self._init_encryption_keys()
        
        logger.info("Digital asset service Vault integration initialized")
        
    async def _ensure_secrets_exist(self):
        """Ensure all required secrets exist in Vault"""
        required_secrets = [
            "storage/minio/credentials",
            "storage/s3/credentials",
            "storage/ipfs/api-key",
            "encryption/asset-encryption-key",
            "encryption/metadata-encryption-key",
            "api-keys/virus-scanner",
            "api-keys/content-moderation",
            "api-keys/cdn-purge"
        ]
        
        for secret_path in required_secrets:
            full_path = f"{self.service_name}/{secret_path}"
            try:
                await self.vault.get_secret(full_path)
                logger.debug(f"Secret exists: {full_path}")
            except:
                # Generate initial secret if needed
                logger.info(f"Creating initial secret for {full_path}")
                await self._create_initial_secret(secret_path)
                
    async def _create_initial_secret(self, secret_path: str):
        """Create initial secret based on type"""
        import secrets
        
        if "minio" in secret_path:
            secret_data = {
                "endpoint": "http://minio:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
                "bucket": "digital-assets",
                "region": "us-east-1"
            }
        elif "s3" in secret_path:
            secret_data = {
                "access_key": f"AKIA{secrets.token_urlsafe(16)}",
                "secret_key": secrets.token_urlsafe(32),
                "bucket": "platformq-assets",
                "region": "us-east-1"
            }
        elif "ipfs" in secret_path:
            secret_data = {
                "api_key": secrets.token_urlsafe(32),
                "gateway_url": "https://ipfs.io/ipfs/",
                "api_url": "http://ipfs:5001"
            }
        elif "encryption" in secret_path:
            from cryptography.fernet import Fernet
            secret_data = {"value": Fernet.generate_key().decode()}
        else:
            secret_data = {"api_key": secrets.token_urlsafe(32)}
            
        await self.vault.create_or_update_secret(
            f"{self.service_name}/{secret_path}",
            secret_data
        )
        
    async def _load_storage_credentials(self):
        """Load storage provider credentials"""
        providers = ["minio", "s3", "ipfs"]
        
        for provider in providers:
            try:
                creds_path = f"{self.service_name}/storage/{provider}/credentials"
                creds = await self.vault.get_secret(creds_path)
                self._storage_creds[provider] = creds
                logger.info(f"Loaded {provider} credentials")
            except Exception as e:
                logger.error(f"Failed to load {provider} credentials: {e}")
                
    async def _init_encryption_keys(self):
        """Initialize encryption keys for assets"""
        key_types = ["asset-encryption-key", "metadata-encryption-key"]
        
        for key_type in key_types:
            key_path = f"{self.service_name}/encryption/{key_type}"
            try:
                key_data = await self.vault.get_secret(key_path)
                self._encryption_keys[key_type] = key_data["value"].encode()
            except Exception as e:
                logger.error(f"Failed to load encryption key {key_type}: {e}")
                
    async def get_storage_credentials(self, provider: AssetStorageProvider) -> Dict[str, Any]:
        """Get storage provider credentials"""
        provider_name = provider.value
        
        if provider_name in self._storage_creds:
            return self._storage_creds[provider_name].copy()
            
        # Reload if not in cache
        creds_path = f"{self.service_name}/storage/{provider_name}/credentials"
        creds = await self.vault.get_secret(creds_path)
        self._storage_creds[provider_name] = creds
        
        return creds.copy()
        
    async def encrypt_asset_data(self, data: bytes, asset_id: str) -> bytes:
        """Encrypt asset data using Transit engine"""
        # For large files, we'll use envelope encryption
        # Generate a data encryption key (DEK)
        from cryptography.fernet import Fernet
        dek = Fernet.generate_key()
        fernet = Fernet(dek)
        
        # Encrypt the data with DEK
        encrypted_data = fernet.encrypt(data)
        
        # Encrypt the DEK with Transit engine
        encrypted_dek = await self.vault.encrypt_data(
            mount_point="transit",
            key_name=f"{self.service_name}-assets",
            plaintext=dek.decode()
        )
        
        # Combine encrypted DEK and encrypted data
        # Format: [4 bytes: DEK length][encrypted DEK][encrypted data]
        encrypted_dek_bytes = encrypted_dek["ciphertext"].encode()
        dek_length = len(encrypted_dek_bytes).to_bytes(4, 'big')
        
        return dek_length + encrypted_dek_bytes + encrypted_data
        
    async def decrypt_asset_data(self, encrypted_data: bytes, asset_id: str) -> bytes:
        """Decrypt asset data"""
        # Extract DEK length
        dek_length = int.from_bytes(encrypted_data[:4], 'big')
        
        # Extract encrypted DEK
        encrypted_dek = encrypted_data[4:4+dek_length].decode()
        
        # Extract encrypted data
        encrypted_content = encrypted_data[4+dek_length:]
        
        # Decrypt DEK with Transit engine
        decrypted_dek = await self.vault.decrypt_data(
            mount_point="transit",
            key_name=f"{self.service_name}-assets",
            ciphertext=encrypted_dek
        )
        
        # Decrypt data with DEK
        from cryptography.fernet import Fernet
        fernet = Fernet(decrypted_dek["plaintext"].encode())
        
        return fernet.decrypt(encrypted_content)
        
    async def sign_asset_metadata(self, metadata: Dict[str, Any], asset_id: str) -> str:
        """Sign asset metadata for integrity verification"""
        # Serialize metadata
        import json
        metadata_json = json.dumps(metadata, sort_keys=True)
        metadata_hash = hashlib.sha256(metadata_json.encode()).hexdigest()
        
        # Sign with Transit engine
        signature = await self.vault.sign_data(
            mount_point="transit",
            name=f"{self.service_name}-metadata",
            input=metadata_hash,
            hash_algorithm="sha2-256"
        )
        
        return signature["signature"]
        
    async def verify_asset_metadata(self, metadata: Dict[str, Any], signature: str, asset_id: str) -> bool:
        """Verify asset metadata signature"""
        # Serialize metadata
        import json
        metadata_json = json.dumps(metadata, sort_keys=True)
        metadata_hash = hashlib.sha256(metadata_json.encode()).hexdigest()
        
        # Verify with Transit engine
        try:
            result = await self.vault.verify_signature(
                mount_point="transit",
                name=f"{self.service_name}-metadata",
                input=metadata_hash,
                signature=signature,
                hash_algorithm="sha2-256"
            )
            return result["valid"]
        except:
            return False
            
    async def get_cdn_credentials(self) -> Dict[str, str]:
        """Get CDN credentials for asset delivery"""
        creds_path = f"{self.service_name}/api-keys/cdn-purge"
        return await self.vault.get_secret(creds_path)
        
    async def get_virus_scanner_credentials(self) -> Dict[str, str]:
        """Get virus scanner API credentials"""
        creds_path = f"{self.service_name}/api-keys/virus-scanner"
        return await self.vault.get_secret(creds_path)
        
    async def get_content_moderation_credentials(self) -> Dict[str, str]:
        """Get content moderation API credentials"""
        creds_path = f"{self.service_name}/api-keys/content-moderation"
        return await self.vault.get_secret(creds_path)
        
    async def rotate_storage_credentials(self, provider: AssetStorageProvider):
        """Rotate storage provider credentials"""
        provider_name = provider.value
        logger.info(f"Rotating credentials for {provider_name}")
        
        # This would integrate with the actual provider's API
        # For now, generate new credentials
        import secrets
        
        new_creds = {
            "access_key": f"AK{secrets.token_urlsafe(16)}",
            "secret_key": secrets.token_urlsafe(32),
            "rotated_at": datetime.utcnow().isoformat()
        }
        
        # Update in Vault
        creds_path = f"{self.service_name}/storage/{provider_name}/credentials"
        current_creds = await self.vault.get_secret(creds_path)
        
        # Keep old credentials for grace period
        new_creds["previous_access_key"] = current_creds.get("access_key")
        new_creds["previous_secret_key"] = current_creds.get("secret_key")
        new_creds["grace_period_ends"] = (datetime.utcnow() + timedelta(hours=24)).isoformat()
        
        await self.vault.create_or_update_secret(creds_path, new_creds)
        
        # Clear cache
        self._storage_creds.pop(provider_name, None)
        
        logger.info(f"Rotated credentials for {provider_name}")


class DigitalAssetConsulIntegration:
    """Consul integration for digital asset service"""
    
    def __init__(self, consul_client: ConsulClient, service_name: str = "digital-asset-service"):
        self.consul = consul_client
        self.service_name = service_name
        self._storage_config: Optional[StorageConfig] = None
        self._processing_config: Optional[ProcessingConfig] = None
        self._asset_registry: Dict[str, Dict] = {}
        self._watchers: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize Consul integration"""
        # Register service
        await self._register_service()
        
        # Load configurations
        await self.reload_configurations()
        
        # Start configuration watchers
        await self._start_config_watchers()
        
        # Initialize asset registry
        await self._init_asset_registry()
        
        logger.info("Digital asset service Consul integration initialized")
        
    async def _register_service(self):
        """Register digital asset service with Consul"""
        from platformq_shared.consul.consul_client import ServiceDefinition
        
        service = ServiceDefinition(
            name=self.service_name,
            port=8000,
            tags=["storage", "assets", "media", "vault-integrated"],
            meta={
                "version": "2.0.0",
                "capabilities": "storage,encryption,processing,cdn",
                "storage_providers": "minio,s3,ipfs",
                "max_file_size_mb": "5000"
            },
            check={
                "http": "http://localhost:8000/health",
                "interval": "10s",
                "timeout": "5s",
                "deregister_critical_service_after": "60s"
            }
        )
        
        await self.consul.register_service(service)
        
    async def reload_configurations(self):
        """Reload configurations from Consul"""
        base_path = f"services/{self.service_name}/config"
        
        try:
            # Load storage config
            storage_config = await self.consul.kv_get_prefix(f"{base_path}/storage/")
            
            allowed_mime_types = storage_config.get("allowed-mime-types", "").split(",")
            if allowed_mime_types == [""]:
                allowed_mime_types = None
                
            self._storage_config = StorageConfig(
                primary_provider=AssetStorageProvider(storage_config.get("primary-provider", "minio")),
                replication_enabled=storage_config.get("replication-enabled", "true").lower() == "true",
                replication_factor=int(storage_config.get("replication-factor", "3")),
                encryption_enabled=storage_config.get("encryption-enabled", "true").lower() == "true",
                compression_enabled=storage_config.get("compression-enabled", "true").lower() == "true",
                max_file_size_mb=int(storage_config.get("max-file-size-mb", "5000")),
                allowed_mime_types=allowed_mime_types
            )
            
            # Load processing config
            processing_config = await self.consul.kv_get_prefix(f"{base_path}/processing/")
            self._processing_config = ProcessingConfig(
                auto_thumbnail=processing_config.get("auto-thumbnail", "true").lower() == "true",
                auto_metadata_extraction=processing_config.get("auto-metadata-extraction", "true").lower() == "true",
                virus_scanning=processing_config.get("virus-scanning", "true").lower() == "true",
                content_moderation=processing_config.get("content-moderation", "false").lower() == "true",
                max_processing_time_seconds=int(processing_config.get("max-processing-time-seconds", "300")),
                enable_gpu_processing=processing_config.get("enable-gpu-processing", "true").lower() == "true"
            )
            
            logger.info("Reloaded digital asset service configurations")
            
        except Exception as e:
            logger.error(f"Failed to reload configurations: {e}")
            # Use defaults
            self._storage_config = StorageConfig()
            self._processing_config = ProcessingConfig()
            
    async def _start_config_watchers(self):
        """Start configuration watchers"""
        watch_paths = ["config/storage", "config/processing"]
        
        for path in watch_paths:
            full_path = f"services/{self.service_name}/{path}"
            watcher = asyncio.create_task(
                self._watch_config_changes(full_path)
            )
            self._watchers[path] = watcher
            
    async def _watch_config_changes(self, path: str):
        """Watch for configuration changes"""
        try:
            async for event in self.consul.watch_prefix(path):
                logger.info(f"Configuration changed at {path}")
                await self.reload_configurations()
                
        except asyncio.CancelledError:
            logger.info(f"Config watcher cancelled for {path}")
            raise
        except Exception as e:
            logger.error(f"Config watcher error for {path}: {e}")
            
    async def _init_asset_registry(self):
        """Initialize asset registry from Consul"""
        registry_path = f"services/{self.service_name}/assets"
        
        try:
            assets = await self.consul.kv_get_prefix(registry_path)
            
            for asset_id, metadata in assets.items():
                if isinstance(metadata, dict) and "hash" in metadata:
                    self._asset_registry[asset_id] = metadata
                    
            logger.info(f"Loaded {len(self._asset_registry)} assets from registry")
            
        except Exception as e:
            logger.error(f"Failed to initialize asset registry: {e}")
            
    async def get_storage_config(self) -> StorageConfig:
        """Get storage configuration"""
        if not self._storage_config:
            await self.reload_configurations()
        return self._storage_config
        
    async def get_processing_config(self) -> ProcessingConfig:
        """Get processing configuration"""
        if not self._processing_config:
            await self.reload_configurations()
        return self._processing_config
        
    async def register_asset(self, 
                           asset_id: str,
                           metadata: Dict[str, Any]):
        """Register asset in Consul"""
        asset_path = f"services/{self.service_name}/assets/{asset_id}"
        
        asset_info = {
            "id": asset_id,
            "name": metadata.get("name", ""),
            "mime_type": metadata.get("mime_type", ""),
            "size_bytes": metadata.get("size_bytes", 0),
            "hash": metadata.get("hash", ""),
            "storage_provider": metadata.get("storage_provider", "minio"),
            "storage_path": metadata.get("storage_path", ""),
            "encryption_enabled": metadata.get("encryption_enabled", True),
            "created_at": metadata.get("created_at", datetime.utcnow().isoformat()),
            "created_by": metadata.get("created_by", ""),
            "tags": metadata.get("tags", []),
            "processing_status": metadata.get("processing_status", "pending"),
            "cdn_url": metadata.get("cdn_url", ""),
            "thumbnails": metadata.get("thumbnails", {}),
            "metadata_signature": metadata.get("metadata_signature", "")
        }
        
        # Store in Consul
        await self.consul.kv_put(f"{asset_path}/metadata", asset_info)
        
        # Update processing status
        await self.consul.kv_put(
            f"{asset_path}/status",
            metadata.get("processing_status", "pending")
        )
        
        # Cache locally
        self._asset_registry[asset_id] = asset_info
        
        logger.info(f"Registered asset: {asset_id}")
        
    async def get_asset_metadata(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Get asset metadata from registry"""
        if asset_id in self._asset_registry:
            return self._asset_registry[asset_id]
            
        asset_path = f"services/{self.service_name}/assets/{asset_id}/metadata"
        metadata = await self.consul.kv_get(asset_path)
        
        if metadata:
            self._asset_registry[asset_id] = metadata
            
        return metadata
        
    async def update_asset_status(self, 
                                asset_id: str,
                                status: str,
                                details: Optional[Dict] = None):
        """Update asset processing status"""
        status_path = f"services/{self.service_name}/assets/{asset_id}/status"
        
        status_data = {
            "status": status,
            "updated_at": datetime.utcnow().isoformat(),
            "details": details or {}
        }
        
        await self.consul.kv_put(status_path, status_data)
        
        # Update local cache
        if asset_id in self._asset_registry:
            self._asset_registry[asset_id]["processing_status"] = status
            
    async def acquire_processing_slot(self, asset_id: str) -> bool:
        """Acquire processing slot with concurrency control"""
        config = await self.get_processing_config()
        
        # Check current processing count
        processing_count_key = f"services/{self.service_name}/metrics/active-processing"
        
        try:
            current_count = int(await self.consul.kv_get(processing_count_key, default="0"))
            
            # Assume max 10 concurrent processing jobs
            max_concurrent = 10
            
            if current_count >= max_concurrent:
                logger.warning(f"Processing slot unavailable, {current_count} jobs running")
                return False
                
            # Try to increment atomically
            new_count = current_count + 1
            success = await self.consul.kv_put_cas(
                processing_count_key,
                str(new_count),
                cas=current_count
            )
            
            if success:
                # Record processing start
                await self.consul.kv_put(
                    f"services/{self.service_name}/processing/{asset_id}",
                    {
                        "status": "processing",
                        "started": datetime.utcnow().isoformat()
                    },
                    ttl=config.max_processing_time_seconds
                )
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to acquire processing slot: {e}")
            return False
            
    async def release_processing_slot(self, asset_id: str):
        """Release processing slot"""
        try:
            # Decrement processing count
            processing_count_key = f"services/{self.service_name}/metrics/active-processing"
            current_count = int(await self.consul.kv_get(processing_count_key, default="1"))
            await self.consul.kv_put(processing_count_key, str(max(0, current_count - 1)))
            
            # Remove from active processing
            await self.consul.kv_delete(
                f"services/{self.service_name}/processing/{asset_id}"
            )
            
        except Exception as e:
            logger.error(f"Failed to release processing slot: {e}")
            
    async def get_storage_metrics(self) -> Dict[str, Any]:
        """Get storage usage metrics"""
        metrics_path = f"services/{self.service_name}/metrics/storage"
        
        try:
            metrics = await self.consul.kv_get_prefix(metrics_path)
            
            return {
                "total_assets": int(metrics.get("total-assets", "0")),
                "total_size_bytes": int(metrics.get("total-size-bytes", "0")),
                "assets_by_type": metrics.get("assets-by-type", {}),
                "storage_by_provider": metrics.get("storage-by-provider", {})
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage metrics: {e}")
            return {
                "total_assets": 0,
                "total_size_bytes": 0,
                "assets_by_type": {},
                "storage_by_provider": {}
            }
            
    async def update_storage_metrics(self, 
                                   asset_type: str,
                                   size_bytes: int,
                                   provider: str,
                                   operation: str = "add"):
        """Update storage metrics"""
        metrics_path = f"services/{self.service_name}/metrics/storage"
        
        try:
            # Get current metrics
            metrics = await self.get_storage_metrics()
            
            # Update based on operation
            if operation == "add":
                metrics["total_assets"] += 1
                metrics["total_size_bytes"] += size_bytes
                
                # Update by type
                assets_by_type = metrics.get("assets_by_type", {})
                assets_by_type[asset_type] = assets_by_type.get(asset_type, 0) + 1
                metrics["assets_by_type"] = assets_by_type
                
                # Update by provider
                storage_by_provider = metrics.get("storage_by_provider", {})
                storage_by_provider[provider] = storage_by_provider.get(provider, 0) + size_bytes
                metrics["storage_by_provider"] = storage_by_provider
                
            elif operation == "remove":
                metrics["total_assets"] = max(0, metrics["total_assets"] - 1)
                metrics["total_size_bytes"] = max(0, metrics["total_size_bytes"] - size_bytes)
                
            # Store updated metrics
            for key, value in metrics.items():
                await self.consul.kv_put(f"{metrics_path}/{key}", value)
                
        except Exception as e:
            logger.error(f"Failed to update storage metrics: {e}")
            
    async def check_storage_health(self, provider: AssetStorageProvider) -> bool:
        """Check health of storage provider"""
        health_key = f"services/{self.service_name}/health/storage/{provider.value}"
        
        try:
            health_status = await self.consul.kv_get(health_key, default="healthy")
            return health_status == "healthy"
        except:
            return True  # Assume healthy if no data
            
    async def update_storage_health(self, provider: AssetStorageProvider, healthy: bool):
        """Update storage provider health status"""
        health_key = f"services/{self.service_name}/health/storage/{provider.value}"
        
        await self.consul.kv_put(
            health_key,
            "healthy" if healthy else "unhealthy",
            ttl=300  # 5 minute TTL
        ) 