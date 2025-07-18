"""
Configuration Management System with Vault Integration

Provides centralized configuration with hot-reload capability, environment-specific
overrides, feature flags, and secure secrets management through HashiCorp Vault.
"""

import os
import json
import yaml
import asyncio
import logging
from typing import Dict, Any, Optional, Callable, List, Set, Union
from datetime import datetime, timedelta
from pathlib import Path
import threading
from dataclasses import dataclass, field
from enum import Enum
import hvac
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent
from pyignite import Client as IgniteClient
from pyignite.datatypes import String, IntObject
from pydantic import BaseSettings, Field, SecretStr, validator
import httpx

logger = logging.getLogger(__name__)


class ConfigSource(Enum):
    """Configuration source types"""
    FILE = "file"
    VAULT = "vault"
    ENVIRONMENT = "environment"
    IGNITE = "ignite"
    REMOTE = "remote"


@dataclass
class ConfigValue:
    """Wrapper for configuration values with metadata"""
    key: str
    value: Any
    source: ConfigSource
    timestamp: datetime = field(default_factory=datetime.utcnow)
    ttl: Optional[int] = None  # Time to live in seconds
    version: Optional[int] = None
    encrypted: bool = False
    
    def is_expired(self) -> bool:
        """Check if the config value has expired"""
        if self.ttl is None:
            return False
        return datetime.utcnow() > self.timestamp + timedelta(seconds=self.ttl)


class ConfigChangeHandler(FileSystemEventHandler):
    """Handle configuration file changes for hot-reload"""
    
    def __init__(self, config_manager: 'ConfigurationManager'):
        self.config_manager = config_manager
        self._last_reload = datetime.utcnow()
        self._reload_cooldown = 1  # seconds
    
    def on_modified(self, event):
        """Handle file modification events"""
        if isinstance(event, FileModifiedEvent) and not event.is_directory:
            # Debounce rapid changes
            now = datetime.utcnow()
            if (now - self._last_reload).total_seconds() < self._reload_cooldown:
                return
            
            self._last_reload = now
            file_path = Path(event.src_path)
            
            if file_path.suffix in ['.yaml', '.yml', '.json']:
                logger.info(f"Configuration file changed: {file_path}")
                asyncio.create_task(self.config_manager.reload_file_configs())


class VaultClient:
    """HashiCorp Vault client wrapper"""
    
    def __init__(
        self,
        url: str,
        token: Optional[str] = None,
        role_id: Optional[str] = None,
        secret_id: Optional[str] = None,
        mount_point: str = "secret",
        kv_version: int = 2
    ):
        self.url = url
        self.mount_point = mount_point
        self.kv_version = kv_version
        self.client = hvac.Client(url=url)
        
        # Authenticate
        if token:
            self.client.token = token
        elif role_id and secret_id:
            # AppRole authentication
            auth_response = self.client.auth.approle.login(
                role_id=role_id,
                secret_id=secret_id
            )
            self.client.token = auth_response['auth']['client_token']
        else:
            raise ValueError("Either token or role_id/secret_id must be provided")
        
        if not self.client.is_authenticated():
            raise ValueError("Failed to authenticate with Vault")
    
    def read_secret(self, path: str) -> Optional[Dict[str, Any]]:
        """Read secret from Vault"""
        try:
            if self.kv_version == 2:
                response = self.client.secrets.kv.v2.read_secret_version(
                    path=path,
                    mount_point=self.mount_point
                )
                return response['data']['data']
            else:
                response = self.client.secrets.kv.v1.read_secret(
                    path=path,
                    mount_point=self.mount_point
                )
                return response['data']
        except Exception as e:
            logger.error(f"Error reading secret from Vault: {e}")
            return None
    
    def write_secret(self, path: str, secret: Dict[str, Any]) -> bool:
        """Write secret to Vault"""
        try:
            if self.kv_version == 2:
                self.client.secrets.kv.v2.create_or_update_secret(
                    path=path,
                    secret=secret,
                    mount_point=self.mount_point
                )
            else:
                self.client.secrets.kv.v1.create_or_update_secret(
                    path=path,
                    secret=secret,
                    mount_point=self.mount_point
                )
            return True
        except Exception as e:
            logger.error(f"Error writing secret to Vault: {e}")
            return False
    
    def list_secrets(self, path: str = "") -> Optional[List[str]]:
        """List secrets at a given path"""
        try:
            if self.kv_version == 2:
                response = self.client.secrets.kv.v2.list_secrets(
                    path=path,
                    mount_point=self.mount_point
                )
            else:
                response = self.client.secrets.kv.v1.list_secrets(
                    path=path,
                    mount_point=self.mount_point
                )
            return response.get('data', {}).get('keys', [])
        except Exception as e:
            logger.error(f"Error listing secrets from Vault: {e}")
            return None


class ConfigurationManager:
    """
    Centralized configuration management with multiple sources,
    hot-reload capability, and Vault integration.
    """
    
    def __init__(
        self,
        service_name: str,
        config_dir: Optional[str] = None,
        vault_url: Optional[str] = None,
        vault_token: Optional[str] = None,
        ignite_host: str = "ignite",
        ignite_port: int = 10800,
        enable_hot_reload: bool = True,
        cache_ttl: int = 300  # 5 minutes
    ):
        self.service_name = service_name
        self.config_dir = Path(config_dir or os.getenv('CONFIG_DIR', '/etc/platformq'))
        self.enable_hot_reload = enable_hot_reload
        self.cache_ttl = cache_ttl
        self.ignite_host = ignite_host
        self.ignite_port = ignite_port
        
        # Configuration storage
        self._config_cache: Dict[str, ConfigValue] = {}
        self._feature_flags: Dict[str, bool] = {}
        self._callbacks: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()
        
        # Initialize Vault client
        self.vault_client = None
        if vault_url:
            self.vault_client = VaultClient(
                url=vault_url,
                token=vault_token or os.getenv('VAULT_TOKEN'),
                role_id=os.getenv('VAULT_ROLE_ID'),
                secret_id=os.getenv('VAULT_SECRET_ID')
            )
        
        # Initialize Ignite client for distributed config
        self.ignite_client = None
        self.config_cache = None
        self._init_ignite()
        
        # File watcher for hot-reload
        self.observer = None
        if enable_hot_reload and self.config_dir.exists():
            self._setup_file_watcher()
        
        # Load initial configuration
        asyncio.create_task(self.load_all_configs())
    
    def _init_ignite(self):
        """Initialize Ignite client and cache"""
        try:
            self.ignite_client = IgniteClient()
            self.ignite_client.connect(self.ignite_host, self.ignite_port)
            
            # Create or get configuration cache
            cache_name = f"config_{self.service_name}"
            self.config_cache = self.ignite_client.get_or_create_cache(cache_name)
            
            logger.info(f"Connected to Ignite for distributed configuration")
        except Exception as e:
            logger.warning(f"Failed to connect to Ignite: {e}. Distributed config disabled.")
            self.ignite_client = None
            self.config_cache = None
    
    def _setup_file_watcher(self):
        """Setup file system watcher for configuration changes"""
        self.observer = Observer()
        handler = ConfigChangeHandler(self)
        self.observer.schedule(handler, str(self.config_dir), recursive=True)
        self.observer.start()
    
    async def load_all_configs(self):
        """Load configurations from all sources"""
        # 1. Load from environment variables
        self._load_env_configs()
        
        # 2. Load from configuration files
        await self.reload_file_configs()
        
        # 3. Load from Vault
        if self.vault_client:
            await self._load_vault_configs()
        
        # 4. Load from Ignite (distributed config)
        if self.ignite_client:
            await self._load_ignite_configs()
        
        # 5. Load feature flags
        await self._load_feature_flags()
        
        logger.info(f"Loaded {len(self._config_cache)} configuration values")
    
    def _load_env_configs(self):
        """Load configuration from environment variables"""
        prefix = f"{self.service_name.upper()}_"
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                with self._lock:
                    self._config_cache[config_key] = ConfigValue(
                        key=config_key,
                        value=self._parse_value(value),
                        source=ConfigSource.ENVIRONMENT
                    )
    
    async def reload_file_configs(self):
        """Reload configuration from files"""
        if not self.config_dir.exists():
            return
        
        # Load base configuration
        base_config = await self._load_config_file(self.config_dir / "base.yaml")
        
        # Load environment-specific configuration
        env = os.getenv("ENVIRONMENT", "development")
        env_config = await self._load_config_file(self.config_dir / f"{env}.yaml")
        
        # Load service-specific configuration
        service_config = await self._load_config_file(
            self.config_dir / "services" / f"{self.service_name}.yaml"
        )
        
        # Merge configurations (later sources override earlier ones)
        configs = [base_config, env_config, service_config]
        
        for config in configs:
            if config:
                with self._lock:
                    for key, value in self._flatten_dict(config).items():
                        self._config_cache[key] = ConfigValue(
                            key=key,
                            value=value,
                            source=ConfigSource.FILE
                        )
        
        # Notify callbacks
        await self._notify_callbacks()
    
    async def _load_config_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load a single configuration file"""
        if not file_path.exists():
            return None
        
        try:
            with open(file_path, 'r') as f:
                if file_path.suffix == '.json':
                    return json.load(f)
                elif file_path.suffix in ['.yaml', '.yml']:
                    return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading config file {file_path}: {e}")
            return None
    
    async def _load_vault_configs(self):
        """Load configuration from Vault"""
        if not self.vault_client:
            return
        
        # Load service-specific secrets
        service_path = f"{self.service_name}/config"
        secrets = self.vault_client.read_secret(service_path)
        
        if secrets:
            with self._lock:
                for key, value in secrets.items():
                    self._config_cache[key] = ConfigValue(
                        key=key,
                        value=value,
                        source=ConfigSource.VAULT,
                        encrypted=True,
                        ttl=self.cache_ttl
                    )
        
        # Load shared secrets
        shared_path = "shared/config"
        shared_secrets = self.vault_client.read_secret(shared_path)
        
        if shared_secrets:
            with self._lock:
                for key, value in shared_secrets.items():
                    if key not in self._config_cache:  # Don't override service-specific
                        self._config_cache[key] = ConfigValue(
                            key=key,
                            value=value,
                            source=ConfigSource.VAULT,
                            encrypted=True,
                            ttl=self.cache_ttl
                        )
    
    async def _load_ignite_configs(self):
        """Load distributed configuration from Ignite"""
        if not self.config_cache:
            return
        
        try:
            # Get all configuration keys for this service
            # Ignite doesn't support pattern matching like Redis, so we'll scan all keys
            for key in self.config_cache.scan():
                value = self.config_cache.get(key)
                if value:
                    with self._lock:
                        self._config_cache[key] = ConfigValue(
                            key=key,
                            value=self._parse_value(value) if isinstance(value, str) else value,
                            source=ConfigSource.IGNITE,
                            ttl=self.cache_ttl
                        )
        except Exception as e:
            logger.error(f"Error loading Ignite configs: {e}")
    
    async def _load_feature_flags(self):
        """Load feature flags"""
        # Load from file
        flags_file = self.config_dir / "feature_flags.yaml"
        if flags_file.exists():
            flags = await self._load_config_file(flags_file)
            if flags:
                self._feature_flags.update(flags.get(self.service_name, {}))
                self._feature_flags.update(flags.get('global', {}))
        
        # Load from Vault
        if self.vault_client:
            vault_flags = self.vault_client.read_secret("feature_flags/global")
            if vault_flags:
                self._feature_flags.update(vault_flags)
            
            service_flags = self.vault_client.read_secret(f"feature_flags/{self.service_name}")
            if service_flags:
                self._feature_flags.update(service_flags)
        
        # Load from Ignite
        if self.config_cache:
            try:
                flags_key = f"feature_flags_{self.service_name}"
                ignite_flags = self.config_cache.get(flags_key)
                if ignite_flags:
                    self._feature_flags.update(json.loads(ignite_flags) if isinstance(ignite_flags, str) else ignite_flags)
            except Exception as e:
                logger.error(f"Error loading feature flags from Ignite: {e}")
    
    def get(self, key: str, default: Any = None, refresh: bool = False) -> Any:
        """Get configuration value"""
        with self._lock:
            config_value = self._config_cache.get(key)
            
            if config_value and not refresh:
                # Check if expired
                if config_value.is_expired():
                    asyncio.create_task(self._refresh_config(key, config_value))
                else:
                    return config_value.value
            
            # Try environment variable override
            env_key = f"{self.service_name.upper()}_{key.upper()}"
            env_value = os.getenv(env_key)
            if env_value:
                return self._parse_value(env_value)
            
            return default
    
    async def _refresh_config(self, key: str, config_value: ConfigValue):
        """Refresh expired configuration value"""
        if config_value.source == ConfigSource.VAULT and self.vault_client:
            # Refresh from Vault
            path = f"{self.service_name}/config"
            secrets = self.vault_client.read_secret(path)
            if secrets and key in secrets:
                with self._lock:
                    self._config_cache[key] = ConfigValue(
                        key=key,
                        value=secrets[key],
                        source=ConfigSource.VAULT,
                        encrypted=True,
                        ttl=self.cache_ttl
                    )
        elif config_value.source == ConfigSource.IGNITE and self.config_cache:
            # Refresh from Ignite
            value = self.config_cache.get(key)
            if value:
                with self._lock:
                    self._config_cache[key] = ConfigValue(
                        key=key,
                        value=self._parse_value(value) if isinstance(value, str) else value,
                        source=ConfigSource.IGNITE,
                        ttl=self.cache_ttl
                    )
    
    def set(self, key: str, value: Any, persist: bool = True):
        """Set configuration value"""
        with self._lock:
            self._config_cache[key] = ConfigValue(
                key=key,
                value=value,
                source=ConfigSource.IGNITE if persist else ConfigSource.ENVIRONMENT
            )
        
        if persist and self.config_cache:
            asyncio.create_task(self._persist_to_ignite(key, value))
        
        # Notify callbacks
        asyncio.create_task(self._notify_callbacks(key))
    
    async def _persist_to_ignite(self, key: str, value: Any):
        """Persist configuration to Ignite"""
        if not self.config_cache:
            return
        
        try:
            # Convert complex types to JSON string
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            self.config_cache.put(key, value)
            
            # Set expiry if supported (Ignite 2.8+)
            # Note: Ignite's expiry is set at cache level, not per-entry
            # For per-entry TTL, you'd need to use Ignite's ExpiryPolicy
            
        except Exception as e:
            logger.error(f"Error persisting to Ignite: {e}")
    
    def get_feature_flag(self, flag_name: str, default: bool = False) -> bool:
        """Get feature flag value"""
        # Check environment override
        env_key = f"FF_{flag_name.upper()}"
        env_value = os.getenv(env_key)
        if env_value:
            return env_value.lower() in ['true', '1', 'yes', 'on']
        
        return self._feature_flags.get(flag_name, default)
    
    def register_callback(self, key_pattern: str, callback: Callable):
        """Register callback for configuration changes"""
        if key_pattern not in self._callbacks:
            self._callbacks[key_pattern] = []
        self._callbacks[key_pattern].append(callback)
    
    async def _notify_callbacks(self, specific_key: Optional[str] = None):
        """Notify registered callbacks of configuration changes"""
        for pattern, callbacks in self._callbacks.items():
            if specific_key and not self._matches_pattern(specific_key, pattern):
                continue
            
            for callback in callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(specific_key)
                    else:
                        callback(specific_key)
                except Exception as e:
                    logger.error(f"Error in config callback: {e}")
    
    def _matches_pattern(self, key: str, pattern: str) -> bool:
        """Check if key matches pattern (supports wildcards)"""
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            return key.startswith(pattern[:-1])
        return key == pattern
    
    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '') -> Dict[str, Any]:
        """Flatten nested dictionary with dot notation"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key).items())
            else:
                items.append((new_key, v))
        return dict(items)
    
    def _parse_value(self, value: str) -> Any:
        """Parse string value to appropriate type"""
        # Boolean
        if value.lower() in ['true', 'false']:
            return value.lower() == 'true'
        
        # None
        if value.lower() == 'none':
            return None
        
        # Number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass
        
        # JSON
        if value.startswith('{') or value.startswith('['):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        
        # String
        return value
    
    def get_all(self, prefix: Optional[str] = None) -> Dict[str, Any]:
        """Get all configuration values with optional prefix filter"""
        with self._lock:
            if prefix:
                return {
                    k: v.value
                    for k, v in self._config_cache.items()
                    if k.startswith(prefix)
                }
            return {k: v.value for k, v in self._config_cache.items()}
    
    def get_vault_secret(self, path: str) -> Optional[Dict[str, Any]]:
        """Get secret directly from Vault"""
        if not self.vault_client:
            return None
        return self.vault_client.read_secret(path)
    
    def set_vault_secret(self, path: str, secret: Dict[str, Any]) -> bool:
        """Set secret directly in Vault"""
        if not self.vault_client:
            return False
        return self.vault_client.write_secret(path, secret)
    
    async def close(self):
        """Close connections and cleanup"""
        if self.observer:
            self.observer.stop()
            self.observer.join()
        
        if self.ignite_client:
            self.ignite_client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.create_task(self.close())


# Singleton instance
_config_manager: Optional[ConfigurationManager] = None


def init_config_manager(
    service_name: str,
    **kwargs
) -> ConfigurationManager:
    """Initialize the global configuration manager"""
    global _config_manager
    _config_manager = ConfigurationManager(service_name, **kwargs)
    return _config_manager


def get_config_manager() -> ConfigurationManager:
    """Get the global configuration manager"""
    if _config_manager is None:
        raise RuntimeError("Configuration manager not initialized")
    return _config_manager


# Convenience functions
def config(key: str, default: Any = None) -> Any:
    """Get configuration value"""
    return get_config_manager().get(key, default)


def feature_flag(flag_name: str, default: bool = False) -> bool:
    """Get feature flag value"""
    return get_config_manager().get_feature_flag(flag_name, default) 