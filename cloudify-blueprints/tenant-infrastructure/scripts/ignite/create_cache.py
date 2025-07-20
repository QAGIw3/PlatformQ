#!/usr/bin/env python3
"""
Cloudify script to create Apache Ignite cache and tables for a tenant.
"""

import os
import sys
import time
import logging
from typing import Dict, Any, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
from pyignite import Client
from pyignite.datatypes import String, IntObject, TimestampObject
from pyignite.exceptions import CacheError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('ignite_provisioner')


class IgniteProvisioner:
    """Handles Ignite cache and table provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.client = None
        self.cache_name = config['cache_name']
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        
    def connect(self):
        """Connect to Ignite cluster."""
        try:
            nodes = [(host['host'], host.get('port', 10800)) 
                     for host in self.config['ignite_hosts']]
            
            self.client = Client()
            self.client.connect(nodes)
            logger.info(f"Connected to Ignite cluster at {nodes}")
            
        except Exception as e:
            raise NonRecoverableError(f"Failed to connect to Ignite: {str(e)}")
            
    def create_cache(self):
        """Create tenant-specific cache with configuration."""
        try:
            # Cache configuration
            cache_config = {
                'name': self.cache_name,
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'TRANSACTIONAL',
                'backups': self.config.get('replication_factor', 2) - 1,
                'write_synchronization_mode': 'FULL_SYNC',
                'statistics_enabled': True,
                'management_enabled': True,
                'read_from_backup': True,
                'copy_on_read': True,
                'data_region_name': 'tenant_data_region',
                'expiry_policy': {
                    'expiry_duration': self.config.get('default_ttl', 86400)  # 1 day default
                }
            }
            
            # Create cache
            cache = self.client.get_or_create_cache(cache_config)
            logger.info(f"Created cache: {self.cache_name}")
            
            # Set cache metadata
            metadata_key = f"_metadata_{self.tenant_id}"
            metadata = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'created_at': int(time.time()),
                'cache_name': self.cache_name,
                'region': self.config.get('region', 'default'),
                'max_memory_mb': self.config.get('max_memory_mb', 1024),
                'eviction_policy': self.config.get('eviction_policy', 'LRU')
            }
            cache.put(metadata_key, metadata)
            
            # Create SQL tables if specified
            if self.config.get('create_tables', True):
                self._create_default_tables(cache)
                
            # Report usage to metering service
            self._report_usage('cache_created', {
                'cache_name': self.cache_name,
                'memory_mb': self.config.get('max_memory_mb', 1024)
            })
            
            return cache
            
        except CacheError as e:
            if 'already exists' in str(e):
                logger.warning(f"Cache {self.cache_name} already exists")
                return self.client.get_cache(self.cache_name)
            raise NonRecoverableError(f"Failed to create cache: {str(e)}")
            
    def _create_default_tables(self, cache):
        """Create default SQL tables for the tenant."""
        try:
            # Session table
            session_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.cache_name}.sessions (
                session_id VARCHAR PRIMARY KEY,
                tenant_id VARCHAR,
                user_id VARCHAR,
                data VARCHAR,
                created_at TIMESTAMP,
                expires_at TIMESTAMP,
                INDEX idx_tenant_user (tenant_id, user_id),
                INDEX idx_expires (expires_at)
            ) WITH "template=partitioned,backups=1,affinityKey=tenant_id"
            """
            cache.query(session_table_sql)
            logger.info(f"Created sessions table in cache {self.cache_name}")
            
            # Key-value store table
            kv_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.cache_name}.key_value_store (
                key VARCHAR PRIMARY KEY,
                tenant_id VARCHAR,
                value VARCHAR,
                ttl_seconds INT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                INDEX idx_tenant (tenant_id)
            ) WITH "template=partitioned,backups=1,affinityKey=tenant_id"
            """
            cache.query(kv_table_sql)
            logger.info(f"Created key_value_store table in cache {self.cache_name}")
            
            # Usage metrics table
            metrics_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.cache_name}.usage_metrics (
                metric_id VARCHAR PRIMARY KEY,
                tenant_id VARCHAR,
                metric_type VARCHAR,
                value DOUBLE,
                timestamp TIMESTAMP,
                metadata VARCHAR,
                INDEX idx_tenant_time (tenant_id, timestamp),
                INDEX idx_type_time (metric_type, timestamp)
            ) WITH "template=partitioned,backups=1,affinityKey=tenant_id"
            """
            cache.query(metrics_table_sql)
            logger.info(f"Created usage_metrics table in cache {self.cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {str(e)}")
            # Non-fatal error - cache is created but tables might need manual creation
            
    def configure_security(self):
        """Configure cache-level security and access control."""
        try:
            # This would integrate with Ignite's security plugin
            # For now, we'll store ACL metadata
            cache = self.client.get_cache(self.cache_name)
            
            acl_key = f"_acl_{self.tenant_id}"
            acl_config = {
                'tenant_id': self.tenant_id,
                'permissions': {
                    'cache_read': True,
                    'cache_write': True,
                    'cache_admin': False,
                    'sql_query': True,
                    'sql_update': True
                },
                'allowed_operations': [
                    'GET', 'PUT', 'REMOVE', 'QUERY', 'GET_ALL', 'PUT_ALL'
                ],
                'ip_whitelist': self.config.get('ip_whitelist', [])
            }
            
            cache.put(acl_key, acl_config)
            logger.info(f"Configured security for cache {self.cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure security: {str(e)}")
            
    def set_resource_limits(self):
        """Set resource limits for the tenant cache."""
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Store resource limits as metadata
            limits_key = f"_limits_{self.tenant_id}"
            limits = {
                'max_memory_mb': self.config.get('max_memory_mb', 1024),
                'max_entries': self.config.get('max_entries', 1000000),
                'max_query_time_ms': self.config.get('max_query_time_ms', 30000),
                'max_concurrent_queries': self.config.get('max_concurrent_queries', 10),
                'eviction_policy': self.config.get('eviction_policy', 'LRU'),
                'eviction_threshold': self.config.get('eviction_threshold', 0.9)
            }
            
            cache.put(limits_key, limits)
            logger.info(f"Set resource limits for cache {self.cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to set resource limits: {str(e)}")
            
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'ignite',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")
            
    def cleanup(self):
        """Cleanup resources."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from Ignite")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'ignite_hosts': inputs.get('ignite_hosts', [{'host': 'localhost'}]),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'cache_name': inputs.get('cache_name', f"tenant_{inputs['tenant_id']}"),
            'replication_factor': inputs.get('replication_factor', 2),
            'max_memory_mb': inputs.get('max_memory_mb', 1024),
            'max_entries': inputs.get('max_entries', 1000000),
            'default_ttl': inputs.get('default_ttl', 86400),
            'eviction_policy': inputs.get('eviction_policy', 'LRU'),
            'create_tables': inputs.get('create_tables', True),
            'region': inputs.get('region', 'default')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['ignite_config'] = config
        
        provisioner = IgniteProvisioner(config)
        
        # Connect to Ignite
        provisioner.connect()
        
        # Create cache
        cache = provisioner.create_cache()
        
        # Configure security
        provisioner.configure_security()
        
        # Set resource limits
        provisioner.set_resource_limits()
        
        # Store cache info in runtime properties
        ctx.instance.runtime_properties['cache_name'] = config['cache_name']
        ctx.instance.runtime_properties['cache_created'] = True
        
        logger.info(f"Successfully provisioned Ignite cache for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Ignite cache: {str(e)}")
        raise NonRecoverableError(str(e))
        
    finally:
        if 'provisioner' in locals():
            provisioner.cleanup()


if __name__ == '__main__':
    main() 