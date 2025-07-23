"""
Configuration Management for DataIntelligenceSuite

Provides dynamic configuration management using Consul KV.
"""

from .config_manager import ConfigManager, ConfigSchema, ConfigWatcher

__all__ = [
    "ConfigManager",
    "ConfigSchema",
    "ConfigWatcher"
] 