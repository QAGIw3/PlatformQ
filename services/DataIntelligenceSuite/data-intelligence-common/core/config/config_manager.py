"""
Configuration Manager for DataIntelligenceSuite

Provides centralized configuration management using Consul KV.
"""

import logging
import json
import asyncio
from typing import Any, Dict, Optional, List, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import yaml

from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.vault.vault_client import VaultClient

logger = logging.getLogger(__name__)


@dataclass
class ConfigSchema:
    """Schema definition for configuration values"""
    key: str
    type: type
    default: Any = None
    required: bool = False
    description: str = ""
    validator: Optional[Callable] = None
    encrypted: bool = False  # Whether this config should be encrypted in Consul


@dataclass
class ConfigWatcher:
    """Configuration watcher registration"""
    key_pattern: str
    callback: Callable
    include_value: bool = True
    recursive: bool = False


class ConfigManager:
    """
    Centralized configuration management with Consul KV.
    
    Features:
    - Dynamic configuration updates
    - Configuration validation
    - Change notifications
    - Hierarchical configuration
    - Encryption for sensitive values
    - Configuration versioning
    - Rollback support
    """
    
    def __init__(
        self,
        service_name: str,
        consul_client: ConsulClient,
        vault_client: Optional[VaultClient] = None,
        config_prefix: str = "data-intelligence"
    ):
        self.service_name = service_name
        self.consul_client = consul_client
        self.vault_client = vault_client
        self.config_prefix = config_prefix
        
        # Configuration cache
        self._config_cache: Dict[str, Any] = {}
        self._schema_registry: Dict[str, ConfigSchema] = {}
        self._watchers: List[ConfigWatcher] = []
        
        # Background tasks
        self._watch_task: Optional[asyncio.Task] = None
        self._refresh_task: Optional[asyncio.Task] = None
        
        # Configuration metadata
        self._config_version: Dict[str, int] = {}
        self._config_history: Dict[str, List[Dict[str, Any]]] = {}
        
    async def initialize(self):
        """Initialize configuration manager"""
        # Load initial configuration
        await self._load_all_configs()
        
        # Start watchers
        self._watch_task = asyncio.create_task(self._watch_loop())
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        
        logger.info(f"Initialized config manager for {self.service_name}")
        
    async def shutdown(self):
        """Shutdown configuration manager"""
        if self._watch_task:
            self._watch_task.cancel()
            try:
                await self._watch_task
            except asyncio.CancelledError:
                pass
                
        if self._refresh_task:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
                
        logger.info("Shutdown config manager")
        
    def register_schema(self, schema: ConfigSchema):
        """Register configuration schema"""
        self._schema_registry[schema.key] = schema
        
    def register_schemas(self, schemas: List[ConfigSchema]):
        """Register multiple configuration schemas"""
        for schema in schemas:
            self.register_schema(schema)
            
    async def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        # Check cache first
        if key in self._config_cache:
            return self._config_cache[key]
            
        # Build full key
        full_key = self._build_key(key)
        
        # Load from Consul
        value = await self._load_config(full_key)
        
        if value is None:
            # Check schema for default
            schema = self._schema_registry.get(key)
            if schema:
                value = schema.default
            else:
                value = default
                
        return value
        
    async def set(self, key: str, value: Any, user: Optional[str] = None):
        """Set configuration value"""
        # Validate against schema
        schema = self._schema_registry.get(key)
        if schema:
            if not self._validate_value(value, schema):
                raise ValueError(f"Invalid value for {key}")
                
        # Build full key
        full_key = self._build_key(key)
        
        # Prepare value for storage
        store_value = value
        
        # Encrypt if needed
        if schema and schema.encrypted and self.vault_client:
            encrypted = await self.vault_client.transit_encrypt(
                "config-encryption",
                json.dumps(value)
            )
            store_value = {
                "encrypted": encrypted["ciphertext"],
                "encrypted_at": datetime.utcnow().isoformat()
            }
            
        # Add metadata
        config_data = {
            "value": store_value,
            "updated_at": datetime.utcnow().isoformat(),
            "updated_by": user or "system",
            "version": self._config_version.get(key, 0) + 1
        }
        
        # Store in Consul
        await self.consul_client.kv_put(full_key, json.dumps(config_data))
        
        # Update cache
        self._config_cache[key] = value
        self._config_version[key] = config_data["version"]
        
        # Add to history
        if key not in self._config_history:
            self._config_history[key] = []
        self._config_history[key].append(config_data)
        
        # Notify watchers
        await self._notify_watchers(key, value)
        
        logger.info(f"Updated config {key} to version {config_data['version']}")
        
    async def delete(self, key: str):
        """Delete configuration value"""
        full_key = self._build_key(key)
        
        # Delete from Consul
        await self.consul_client.kv_delete(full_key)
        
        # Remove from cache
        if key in self._config_cache:
            del self._config_cache[key]
            
        # Notify watchers
        await self._notify_watchers(key, None)
        
        logger.info(f"Deleted config {key}")
        
    async def get_all(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """Get all configuration values with optional prefix"""
        if prefix:
            full_prefix = self._build_key(prefix)
        else:
            full_prefix = f"{self.config_prefix}/{self.service_name}"
            
        # Get all keys from Consul
        keys = await self.consul_client.kv_list(full_prefix)
        
        configs = {}
        for key in keys:
            # Extract relative key
            relative_key = key.replace(f"{full_prefix}/", "")
            value = await self.get(relative_key)
            configs[relative_key] = value
            
        return configs
        
    def watch(self, key_pattern: str, callback: Callable,
              include_value: bool = True, recursive: bool = False):
        """Watch for configuration changes"""
        watcher = ConfigWatcher(
            key_pattern=key_pattern,
            callback=callback,
            include_value=include_value,
            recursive=recursive
        )
        self._watchers.append(watcher)
        
        logger.info(f"Registered watcher for {key_pattern}")
        
    async def rollback(self, key: str, version: Optional[int] = None):
        """Rollback configuration to previous version"""
        if key not in self._config_history:
            raise ValueError(f"No history for key {key}")
            
        history = self._config_history[key]
        if not history:
            raise ValueError(f"No history entries for key {key}")
            
        if version is None:
            # Rollback to previous version
            if len(history) < 2:
                raise ValueError(f"No previous version for key {key}")
            target = history[-2]
        else:
            # Find specific version
            target = None
            for entry in history:
                if entry["version"] == version:
                    target = entry
                    break
            if not target:
                raise ValueError(f"Version {version} not found for key {key}")
                
        # Restore value
        value = target["value"]
        
        # Decrypt if needed
        if isinstance(value, dict) and "encrypted" in value:
            decrypted = await self.vault_client.transit_decrypt(
                "config-encryption",
                value["encrypted"]
            )
            value = json.loads(decrypted)
            
        await self.set(key, value, user="rollback")
        
        logger.info(f"Rolled back {key} to version {target['version']}")
        
    async def export_config(self, format: str = "json") -> str:
        """Export all configuration"""
        configs = await self.get_all()
        
        if format == "json":
            return json.dumps(configs, indent=2)
        elif format == "yaml":
            return yaml.dump(configs, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    async def import_config(self, data: str, format: str = "json",
                           merge: bool = True, user: Optional[str] = None):
        """Import configuration"""
        if format == "json":
            configs = json.loads(data)
        elif format == "yaml":
            configs = yaml.safe_load(data)
        else:
            raise ValueError(f"Unsupported format: {format}")
            
        if not merge:
            # Clear existing configs
            all_keys = await self.get_all()
            for key in all_keys:
                await self.delete(key)
                
        # Import new configs
        for key, value in configs.items():
            await self.set(key, value, user=user)
            
        logger.info(f"Imported {len(configs)} configuration values")
        
    def _build_key(self, key: str) -> str:
        """Build full Consul key"""
        return f"{self.config_prefix}/{self.service_name}/{key}"
        
    async def _load_config(self, full_key: str) -> Any:
        """Load configuration from Consul"""
        try:
            data = await self.consul_client.kv_get(full_key)
            if not data:
                return None
                
            config_data = json.loads(data)
            value = config_data.get("value")
            
            # Decrypt if needed
            if isinstance(value, dict) and "encrypted" in value and self.vault_client:
                decrypted = await self.vault_client.transit_decrypt(
                    "config-encryption",
                    value["encrypted"]
                )
                value = json.loads(decrypted)
                
            # Extract relative key for caching
            relative_key = full_key.replace(f"{self.config_prefix}/{self.service_name}/", "")
            
            # Update cache
            self._config_cache[relative_key] = value
            self._config_version[relative_key] = config_data.get("version", 0)
            
            return value
            
        except Exception as e:
            logger.error(f"Failed to load config {full_key}: {e}")
            return None
            
    async def _load_all_configs(self):
        """Load all configurations from Consul"""
        try:
            prefix = f"{self.config_prefix}/{self.service_name}"
            keys = await self.consul_client.kv_list(prefix)
            
            for key in keys:
                await self._load_config(key)
                
            logger.info(f"Loaded {len(keys)} configuration values")
            
        except Exception as e:
            logger.error(f"Failed to load configs: {e}")
            
    def _validate_value(self, value: Any, schema: ConfigSchema) -> bool:
        """Validate configuration value against schema"""
        # Type check
        if schema.type and not isinstance(value, schema.type):
            return False
            
        # Required check
        if schema.required and value is None:
            return False
            
        # Custom validator
        if schema.validator:
            try:
                return schema.validator(value)
            except Exception:
                return False
                
        return True
        
    async def _notify_watchers(self, key: str, value: Any):
        """Notify watchers of configuration change"""
        for watcher in self._watchers:
            # Check if key matches pattern
            if self._matches_pattern(key, watcher.key_pattern, watcher.recursive):
                try:
                    if watcher.include_value:
                        if asyncio.iscoroutinefunction(watcher.callback):
                            await watcher.callback(key, value)
                        else:
                            watcher.callback(key, value)
                    else:
                        if asyncio.iscoroutinefunction(watcher.callback):
                            await watcher.callback(key)
                        else:
                            watcher.callback(key)
                except Exception as e:
                    logger.error(f"Error in config watcher: {e}")
                    
    def _matches_pattern(self, key: str, pattern: str, recursive: bool) -> bool:
        """Check if key matches pattern"""
        if pattern == "*":
            return True
            
        if recursive:
            return key.startswith(pattern)
        else:
            return key == pattern or key.startswith(f"{pattern}/")
            
    async def _watch_loop(self):
        """Background task to watch for configuration changes"""
        while True:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                # Get current keys
                prefix = f"{self.config_prefix}/{self.service_name}"
                current_keys = set(await self.consul_client.kv_list(prefix))
                
                # Check for changes
                cached_keys = set(f"{prefix}/{k}" for k in self._config_cache.keys())
                
                # New or updated keys
                for key in current_keys:
                    await self._load_config(key)
                    
                # Deleted keys
                for key in cached_keys - current_keys:
                    relative_key = key.replace(f"{prefix}/", "")
                    if relative_key in self._config_cache:
                        del self._config_cache[relative_key]
                        await self._notify_watchers(relative_key, None)
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in watch loop: {e}")
                
    async def _refresh_loop(self):
        """Periodically refresh all configurations"""
        while True:
            try:
                await asyncio.sleep(300)  # Refresh every 5 minutes
                await self._load_all_configs()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in refresh loop: {e}")
                
    def get_stats(self) -> Dict[str, Any]:
        """Get configuration statistics"""
        return {
            "total_configs": len(self._config_cache),
            "watchers": len(self._watchers),
            "schemas": len(self._schema_registry),
            "versions": self._config_version,
            "history_entries": sum(len(h) for h in self._config_history.values())
        } 