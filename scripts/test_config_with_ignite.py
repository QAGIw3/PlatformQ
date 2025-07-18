#!/usr/bin/env python3
"""
Test Configuration System with Ignite

Demonstrates how the configuration management system uses Ignite
for distributed configuration storage.
"""

import asyncio
import logging
from platformq_shared.config_manager import ConfigurationManager
from platformq_shared.config import init_config, ServiceConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_config_system():
    """Test the configuration system with Ignite backend"""
    
    # Initialize configuration manager directly
    config_manager = ConfigurationManager(
        service_name="test-service",
        config_dir="/etc/platformq",
        vault_url="http://localhost:8200",
        vault_token="test-token",  # In production, use proper auth
        ignite_host="localhost",  # Change to your Ignite host
        ignite_port=10800,
        enable_hot_reload=True,
        cache_ttl=60  # 1 minute for testing
    )
    
    # Load configurations
    await config_manager.load_all_configs()
    
    # Test basic operations
    logger.info("Testing configuration operations...")
    
    # Set a configuration value (persists to Ignite)
    config_manager.set("test.key1", "value1", persist=True)
    config_manager.set("test.key2", {"nested": "object"}, persist=True)
    config_manager.set("test.key3", [1, 2, 3], persist=True)
    
    # Get configuration values
    value1 = config_manager.get("test.key1")
    value2 = config_manager.get("test.key2")
    value3 = config_manager.get("test.key3")
    
    logger.info(f"Retrieved values:")
    logger.info(f"  test.key1: {value1}")
    logger.info(f"  test.key2: {value2}")
    logger.info(f"  test.key3: {value3}")
    
    # Test feature flags
    config_manager.set("feature_flags_test-service", {
        "new_feature": True,
        "experimental_feature": False
    }, persist=True)
    
    flag1 = config_manager.get_feature_flag("new_feature")
    flag2 = config_manager.get_feature_flag("experimental_feature")
    flag3 = config_manager.get_feature_flag("non_existent", default=True)
    
    logger.info(f"Feature flags:")
    logger.info(f"  new_feature: {flag1}")
    logger.info(f"  experimental_feature: {flag2}")
    logger.info(f"  non_existent (default=True): {flag3}")
    
    # Test configuration callbacks
    def config_changed(key):
        logger.info(f"Configuration changed: {key}")
    
    config_manager.register_callback("test.*", config_changed)
    
    # Trigger callback
    config_manager.set("test.dynamic", "updated value")
    
    # Give time for async operations
    await asyncio.sleep(1)
    
    # Test getting all configs with prefix
    all_test_configs = config_manager.get_all(prefix="test.")
    logger.info(f"All test configs: {all_test_configs}")
    
    # Close connections
    await config_manager.close()
    logger.info("Test completed successfully!")


async def test_service_config():
    """Test service configuration with Ignite"""
    
    # Create service config
    service_config = ServiceConfig(
        service_name="test-service",
        environment="development",
        ignite_config_host="localhost",
        ignite_config_port=10800
    )
    
    # Initialize config loader
    config_loader = init_config("test-service", service_config)
    await config_loader.initialize()
    
    # Test configuration access
    logger.info("Testing service configuration...")
    
    # Get various configurations
    db_config = config_loader.get_database_config()
    ignite_config = config_loader.get_ignite_config()
    
    logger.info(f"Database config: {db_config}")
    logger.info(f"Ignite config: {ignite_config}")
    
    # Test feature flags
    feature_enabled = config_loader.get_feature_flag("test_feature", default=False)
    logger.info(f"Test feature enabled: {feature_enabled}")
    
    await config_loader.close()


def main():
    """Main test function"""
    logger.info("Starting configuration system test with Ignite...")
    
    # Run tests
    asyncio.run(test_config_system())
    asyncio.run(test_service_config())
    
    logger.info("All tests completed!")


if __name__ == "__main__":
    main() 