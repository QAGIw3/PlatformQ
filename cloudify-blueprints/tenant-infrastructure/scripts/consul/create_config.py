#!/usr/bin/env python3
"""
Cloudify script to create Consul configuration and service discovery for a tenant.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, List, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
import consul
import base64

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('consul_provisioner')


class ConsulProvisioner:
    """Handles Consul configuration and service discovery provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consul_host = config['consul_host']
        self.consul_port = config.get('consul_port', 8500)
        self.consul_token = config.get('consul_token')
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        self.kv_prefix = config.get('kv_prefix', f"tenants/{self.tenant_id}")
        
        # Initialize Consul client
        self.client = consul.Consul(
            host=self.consul_host,
            port=self.consul_port,
            token=self.consul_token,
            scheme=config.get('consul_scheme', 'http'),
            verify=config.get('verify_ssl', True)
        )
        
    def create_namespace(self):
        """Create namespace for tenant if using Consul Enterprise."""
        if not self.config.get('use_namespaces', False):
            logger.info("Namespaces not enabled, skipping namespace creation")
            return
            
        try:
            namespace_name = f"tenant-{self.tenant_id}"
            
            # Check if namespace exists (Enterprise feature)
            namespaces = self.client.namespaces.list()
            if any(ns['Name'] == namespace_name for ns in namespaces[1]):
                logger.info(f"Namespace {namespace_name} already exists")
                return
                
            # Create namespace
            namespace_data = {
                'Name': namespace_name,
                'Description': f"Namespace for tenant {self.tenant_id}",
                'Meta': {
                    'tenant_id': self.tenant_id,
                    'reseller_id': self.reseller_id or '',
                    'customer_id': self.customer_id or '',
                    'created_at': str(int(time.time()))
                }
            }
            
            self.client.namespaces.create(namespace_data)
            logger.info(f"Created namespace: {namespace_name}")
            
        except Exception as e:
            logger.error(f"Failed to create namespace: {str(e)}")
            # Namespaces might not be available in OSS version
            
    def create_acl_policy(self):
        """Create ACL policy for tenant access."""
        if not self.config.get('use_acls', True):
            logger.info("ACLs not enabled, skipping policy creation")
            return
            
        try:
            policy_name = f"tenant-{self.tenant_id}-policy"
            
            # Check if policy exists
            policies = self.client.acl.policy.list()
            if any(p['Name'] == policy_name for p in policies[1]):
                logger.info(f"Policy {policy_name} already exists")
                return
                
            # Define policy rules
            policy_rules = f"""
# Tenant {self.tenant_id} policy

# KV access - full access to tenant prefix
key_prefix "{self.kv_prefix}/" {{
  policy = "write"
}}

# Service discovery - read all services, write own services
service_prefix "" {{
  policy = "read"
}}

service_prefix "tenant-{self.tenant_id}-" {{
  policy = "write"
}}

# Node access - read only
node_prefix "" {{
  policy = "read"
}}

# Session management for tenant
session_prefix "tenant-{self.tenant_id}-" {{
  policy = "write"
}}

# Prepared queries
query_prefix "tenant-{self.tenant_id}-" {{
  policy = "write"
}}
"""
            
            # Create policy
            policy_data = {
                'Name': policy_name,
                'Description': f'Policy for tenant {self.tenant_id}',
                'Rules': policy_rules
            }
            
            self.client.acl.policy.create(policy_data)
            logger.info(f"Created ACL policy: {policy_name}")
            
        except Exception as e:
            logger.error(f"Failed to create ACL policy: {str(e)}")
            
    def create_acl_token(self):
        """Create ACL token for tenant applications."""
        if not self.config.get('use_acls', True):
            logger.info("ACLs not enabled, skipping token creation")
            return
            
        try:
            token_description = f"Token for tenant {self.tenant_id}"
            policy_name = f"tenant-{self.tenant_id}-policy"
            
            # Create token
            token_data = {
                'Description': token_description,
                'Policies': [
                    {'Name': policy_name}
                ],
                'Local': False,
                'ExpirationTime': None  # No expiration
            }
            
            result = self.client.acl.token.create(token_data)
            
            if result[0]:
                token_id = result[1]['AccessorID']
                secret_id = result[1]['SecretID']
                
                # Store token in runtime properties
                ctx.instance.runtime_properties['consul_token_id'] = token_id
                ctx.instance.runtime_properties['consul_secret_id'] = secret_id
                
                logger.info(f"Created ACL token for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create ACL token: {str(e)}")
            
    def create_kv_structure(self):
        """Create initial KV structure for the tenant."""
        try:
            # Create configuration structure
            kv_data = {
                f"{self.kv_prefix}/config/tenant": {
                    'tenant_id': self.tenant_id,
                    'reseller_id': self.reseller_id,
                    'customer_id': self.customer_id,
                    'created_at': int(time.time()),
                    'environment': self.config.get('environment', 'production'),
                    'region': self.config.get('region', 'default')
                },
                f"{self.kv_prefix}/config/services": {
                    'cassandra': {
                        'hosts': self.config.get('cassandra_hosts', ['cassandra']),
                        'keyspace': f"tenant_{self.tenant_id}",
                        'replication_factor': self.config.get('cassandra_replication', 3)
                    },
                    'ignite': {
                        'hosts': self.config.get('ignite_hosts', ['ignite']),
                        'cache_name': f"tenant_{self.tenant_id}",
                        'port': 10800
                    },
                    'pulsar': {
                        'service_url': self.config.get('pulsar_service_url', 'pulsar://pulsar:6650'),
                        'admin_url': self.config.get('pulsar_admin_url', 'http://pulsar:8080'),
                        'tenant': f"tenant-{self.tenant_id}",
                        'namespace': 'default'
                    },
                    'minio': {
                        'endpoint': self.config.get('minio_endpoint', 'minio:9000'),
                        'bucket': f"tenant-{self.tenant_id}",
                        'secure': self.config.get('minio_secure', False)
                    },
                    'elasticsearch': {
                        'hosts': self.config.get('elasticsearch_hosts', ['elasticsearch:9200']),
                        'index_prefix': f"tenant-{self.tenant_id}",
                        'shards': self.config.get('es_shards', 3),
                        'replicas': self.config.get('es_replicas', 1)
                    },
                    'janusgraph': {
                        'gremlin_server': self.config.get('janusgraph_gremlin', 'janusgraph:8182'),
                        'graph_name': f"tenant_{self.tenant_id}"
                    }
                },
                f"{self.kv_prefix}/config/limits": {
                    'max_storage_gb': self.config.get('max_storage_gb', 100),
                    'max_compute_cores': self.config.get('max_compute_cores', 16),
                    'max_memory_gb': self.config.get('max_memory_gb', 64),
                    'max_network_mbps': self.config.get('max_network_mbps', 1000)
                },
                f"{self.kv_prefix}/config/features": {
                    'enable_ml': self.config.get('enable_ml', True),
                    'enable_streaming': self.config.get('enable_streaming', True),
                    'enable_graph': self.config.get('enable_graph', True),
                    'enable_search': self.config.get('enable_search', True),
                    'enable_object_storage': self.config.get('enable_object_storage', True)
                }
            }
            
            # Write KV data
            for key, value in kv_data.items():
                self.client.kv.put(key, json.dumps(value))
                logger.info(f"Created KV entry: {key}")
                
            # Report usage
            self._report_usage('kv_structure_created', {
                'prefix': self.kv_prefix,
                'entries': len(kv_data)
            })
            
        except Exception as e:
            raise NonRecoverableError(f"Failed to create KV structure: {str(e)}")
            
    def register_services(self):
        """Register tenant services in Consul."""
        try:
            services = [
                {
                    'name': f"tenant-{self.tenant_id}-api",
                    'tags': ['tenant', 'api', f'tenant-{self.tenant_id}'],
                    'port': 8080,
                    'check': {
                        'http': f"http://localhost:8080/health",
                        'interval': '10s',
                        'timeout': '5s'
                    }
                },
                {
                    'name': f"tenant-{self.tenant_id}-web",
                    'tags': ['tenant', 'web', f'tenant-{self.tenant_id}'],
                    'port': 3000,
                    'check': {
                        'http': f"http://localhost:3000/health",
                        'interval': '10s',
                        'timeout': '5s'
                    }
                }
            ]
            
            for service in services:
                service_data = {
                    'ID': f"{service['name']}-{self.config.get('node_id', 'node1')}",
                    'Name': service['name'],
                    'Tags': service['tags'],
                    'Port': service['port'],
                    'Meta': {
                        'tenant_id': self.tenant_id,
                        'reseller_id': self.reseller_id or '',
                        'customer_id': self.customer_id or '',
                        'version': '1.0.0'
                    },
                    'Check': {
                        'HTTP': service['check']['http'],
                        'Interval': service['check']['interval'],
                        'Timeout': service['check']['timeout']
                    }
                }
                
                self.client.agent.service.register(service_data)
                logger.info(f"Registered service: {service['name']}")
                
        except Exception as e:
            logger.error(f"Failed to register services: {str(e)}")
            
    def create_prepared_queries(self):
        """Create prepared queries for service discovery."""
        try:
            queries = [
                {
                    'Name': f"tenant-{self.tenant_id}-services",
                    'Service': {
                        'Service': '',
                        'Tags': [f'tenant-{self.tenant_id}'],
                        'OnlyPassing': True,
                        'Near': '_agent'
                    }
                },
                {
                    'Name': f"tenant-{self.tenant_id}-healthy-api",
                    'Service': {
                        'Service': f"tenant-{self.tenant_id}-api",
                        'OnlyPassing': True,
                        'Near': '_agent'
                    }
                }
            ]
            
            for query in queries:
                result = self.client.query.create(query)
                if result[0]:
                    query_id = result[1]['ID']
                    logger.info(f"Created prepared query: {query['Name']} (ID: {query_id})")
                    
        except Exception as e:
            logger.error(f"Failed to create prepared queries: {str(e)}")
            
    def configure_watches(self):
        """Configure watches for tenant-specific events."""
        try:
            # Create watch configurations
            watches = [
                {
                    'type': 'key',
                    'key': f"{self.kv_prefix}/config/",
                    'handler': f"/opt/consul/handlers/tenant-{self.tenant_id}-config-handler.sh"
                },
                {
                    'type': 'services',
                    'tag': f'tenant-{self.tenant_id}',
                    'handler': f"/opt/consul/handlers/tenant-{self.tenant_id}-service-handler.sh"
                },
                {
                    'type': 'checks',
                    'service': f"tenant-{self.tenant_id}-api",
                    'handler': f"/opt/consul/handlers/tenant-{self.tenant_id}-health-handler.sh"
                }
            ]
            
            # Store watch configurations in KV
            for i, watch in enumerate(watches):
                key = f"{self.kv_prefix}/watches/watch-{i}"
                self.client.kv.put(key, json.dumps(watch))
                logger.info(f"Created watch configuration: {watch['type']}")
                
        except Exception as e:
            logger.error(f"Failed to configure watches: {str(e)}")
            
    def create_service_mesh_config(self):
        """Create service mesh configuration for the tenant."""
        try:
            # Create proxy defaults
            proxy_config = {
                'Kind': 'proxy-defaults',
                'Name': f"tenant-{self.tenant_id}",
                'Config': {
                    'protocol': 'http',
                    'envoy_prometheus_bind_addr': '0.0.0.0:9102',
                    'envoy_stats_bind_addr': '0.0.0.0:9103'
                },
                'MeshGateway': {
                    'Mode': 'local'
                }
            }
            
            key = f"{self.kv_prefix}/mesh/proxy-defaults"
            self.client.kv.put(key, json.dumps(proxy_config))
            
            # Create service defaults
            services = ['api', 'web']
            for service in services:
                service_config = {
                    'Kind': 'service-defaults',
                    'Name': f"tenant-{self.tenant_id}-{service}",
                    'Protocol': 'http',
                    'MeshGateway': {
                        'Mode': 'local'
                    }
                }
                
                key = f"{self.kv_prefix}/mesh/service-{service}"
                self.client.kv.put(key, json.dumps(service_config))
                
            # Create intentions (service-to-service permissions)
            intentions = [
                {
                    'Kind': 'service-intentions',
                    'Name': f"tenant-{self.tenant_id}-api",
                    'Sources': [
                        {
                            'Name': f"tenant-{self.tenant_id}-web",
                            'Action': 'allow'
                        }
                    ]
                }
            ]
            
            for intention in intentions:
                key = f"{self.kv_prefix}/mesh/intentions/{intention['Name']}"
                self.client.kv.put(key, json.dumps(intention))
                
            logger.info(f"Created service mesh configuration for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create service mesh config: {str(e)}")
            
    def create_backup_config(self):
        """Create backup configuration for tenant data."""
        try:
            backup_config = {
                'enabled': self.config.get('enable_backups', True),
                'schedule': self.config.get('backup_schedule', '0 2 * * *'),  # 2 AM daily
                'retention_days': self.config.get('backup_retention_days', 30),
                'destinations': [
                    {
                        'type': 's3',
                        'bucket': f"backups-tenant-{self.tenant_id}",
                        'prefix': 'consul/',
                        'region': self.config.get('backup_region', 'us-east-1')
                    }
                ],
                'include_patterns': [
                    f"{self.kv_prefix}/**"
                ]
            }
            
            key = f"{self.kv_prefix}/backup/config"
            self.client.kv.put(key, json.dumps(backup_config))
            logger.info(f"Created backup configuration for tenant {self.tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to create backup config: {str(e)}")
            
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'consul',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'consul_host': inputs.get('consul_host', 'localhost'),
            'consul_port': inputs.get('consul_port', 8500),
            'consul_token': inputs.get('consul_token'),
            'consul_scheme': inputs.get('consul_scheme', 'http'),
            'verify_ssl': inputs.get('verify_ssl', True),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'kv_prefix': inputs.get('kv_prefix', f"tenants/{inputs['tenant_id']}"),
            
            # Feature flags
            'use_namespaces': inputs.get('use_namespaces', False),  # Enterprise feature
            'use_acls': inputs.get('use_acls', True),
            
            # Service endpoints
            'cassandra_hosts': inputs.get('cassandra_hosts', ['cassandra']),
            'cassandra_replication': inputs.get('cassandra_replication', 3),
            'ignite_hosts': inputs.get('ignite_hosts', ['ignite']),
            'pulsar_service_url': inputs.get('pulsar_service_url', 'pulsar://pulsar:6650'),
            'pulsar_admin_url': inputs.get('pulsar_admin_url', 'http://pulsar:8080'),
            'minio_endpoint': inputs.get('minio_endpoint', 'minio:9000'),
            'minio_secure': inputs.get('minio_secure', False),
            'elasticsearch_hosts': inputs.get('elasticsearch_hosts', ['elasticsearch:9200']),
            'es_shards': inputs.get('es_shards', 3),
            'es_replicas': inputs.get('es_replicas', 1),
            'janusgraph_gremlin': inputs.get('janusgraph_gremlin', 'janusgraph:8182'),
            
            # Resource limits
            'max_storage_gb': inputs.get('max_storage_gb', 100),
            'max_compute_cores': inputs.get('max_compute_cores', 16),
            'max_memory_gb': inputs.get('max_memory_gb', 64),
            'max_network_mbps': inputs.get('max_network_mbps', 1000),
            
            # Feature toggles
            'enable_ml': inputs.get('enable_ml', True),
            'enable_streaming': inputs.get('enable_streaming', True),
            'enable_graph': inputs.get('enable_graph', True),
            'enable_search': inputs.get('enable_search', True),
            'enable_object_storage': inputs.get('enable_object_storage', True),
            
            # Backup configuration
            'enable_backups': inputs.get('enable_backups', True),
            'backup_schedule': inputs.get('backup_schedule', '0 2 * * *'),
            'backup_retention_days': inputs.get('backup_retention_days', 30),
            'backup_region': inputs.get('backup_region', 'us-east-1'),
            
            # Environment
            'environment': inputs.get('environment', 'production'),
            'region': inputs.get('region', 'default'),
            'node_id': inputs.get('node_id', 'node1')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['consul_config'] = config
        
        provisioner = ConsulProvisioner(config)
        
        # Create namespace (Enterprise only)
        provisioner.create_namespace()
        
        # Create ACL policy
        provisioner.create_acl_policy()
        
        # Create ACL token
        provisioner.create_acl_token()
        
        # Create KV structure
        provisioner.create_kv_structure()
        
        # Register services
        provisioner.register_services()
        
        # Create prepared queries
        provisioner.create_prepared_queries()
        
        # Configure watches
        provisioner.configure_watches()
        
        # Create service mesh configuration
        provisioner.create_service_mesh_config()
        
        # Create backup configuration
        provisioner.create_backup_config()
        
        # Store KV prefix in runtime properties
        ctx.instance.runtime_properties['kv_prefix'] = config['kv_prefix']
        ctx.instance.runtime_properties['consul_configured'] = True
        
        logger.info(f"Successfully provisioned Consul for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Consul: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 