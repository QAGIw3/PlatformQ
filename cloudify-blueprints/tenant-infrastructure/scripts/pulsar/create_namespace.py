#!/usr/bin/env python3
"""
Cloudify script to create Apache Pulsar namespace and topics for a tenant.
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
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('pulsar_provisioner')


class PulsarProvisioner:
    """Handles Pulsar namespace and topic provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.admin_url = config['pulsar_admin_url']
        self.service_url = config['pulsar_service_url']
        self.tenant_name = config['pulsar_tenant']
        self.namespace = config['namespace']
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        self.auth_token = config.get('auth_token')
        
        # Setup HTTP session with retries
        self.session = requests.Session()
        retry = Retry(total=3, backoff_factor=0.3)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        if self.auth_token:
            self.session.headers.update({
                'Authorization': f'Bearer {self.auth_token}'
            })
            
    def create_tenant(self):
        """Create Pulsar tenant if it doesn't exist."""
        try:
            # Check if tenant exists
            response = self.session.get(f"{self.admin_url}/admin/v2/tenants/{self.tenant_name}")
            
            if response.status_code == 200:
                logger.info(f"Tenant {self.tenant_name} already exists")
                return
                
            # Create tenant
            tenant_config = {
                'adminRoles': [f'admin-{self.tenant_id}'],
                'allowedClusters': self.config.get('allowed_clusters', ['standalone'])
            }
            
            response = self.session.put(
                f"{self.admin_url}/admin/v2/tenants/{self.tenant_name}",
                json=tenant_config
            )
            
            if response.status_code in [200, 204]:
                logger.info(f"Created tenant: {self.tenant_name}")
            else:
                raise NonRecoverableError(f"Failed to create tenant: {response.text}")
                
        except requests.RequestException as e:
            raise RecoverableError(f"Network error creating tenant: {str(e)}")
            
    def create_namespace(self):
        """Create namespace with policies for the tenant."""
        try:
            namespace_path = f"{self.tenant_name}/{self.namespace}"
            
            # Check if namespace exists
            response = self.session.get(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}"
            )
            
            if response.status_code == 200:
                logger.info(f"Namespace {namespace_path} already exists")
            else:
                # Create namespace
                response = self.session.put(
                    f"{self.admin_url}/admin/v2/namespaces/{namespace_path}"
                )
                
                if response.status_code not in [200, 204]:
                    raise NonRecoverableError(f"Failed to create namespace: {response.text}")
                    
                logger.info(f"Created namespace: {namespace_path}")
                
            # Set namespace policies
            self._configure_namespace_policies(namespace_path)
            
            # Report usage
            self._report_usage('namespace_created', {
                'namespace': namespace_path,
                'retention_gb': self.config.get('retention_size_gb', 10),
                'retention_hours': self.config.get('retention_hours', 72)
            })
            
        except requests.RequestException as e:
            raise RecoverableError(f"Network error creating namespace: {str(e)}")
            
    def _configure_namespace_policies(self, namespace_path: str):
        """Configure policies for the namespace."""
        try:
            # Set retention policy
            retention_policy = {
                'retentionTimeInMinutes': self.config.get('retention_hours', 72) * 60,
                'retentionSizeInMB': self.config.get('retention_size_gb', 10) * 1024
            }
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/retention",
                json=retention_policy
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to set retention policy: {response.text}")
                
            # Set message TTL
            ttl_seconds = self.config.get('message_ttl_seconds', 0)
            if ttl_seconds > 0:
                response = self.session.post(
                    f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/messageTTL",
                    json=ttl_seconds
                )
                
                if response.status_code not in [200, 204]:
                    logger.error(f"Failed to set message TTL: {response.text}")
                    
            # Set deduplication
            if self.config.get('enable_deduplication', False):
                response = self.session.post(
                    f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/deduplication",
                    json=True
                )
                
                if response.status_code not in [200, 204]:
                    logger.error(f"Failed to enable deduplication: {response.text}")
                    
            # Set dispatch rate
            dispatch_rate = {
                'dispatchThrottlingRateInMsg': self.config.get('max_messages_per_second', 10000),
                'dispatchThrottlingRateInByte': self.config.get('max_bytes_per_second', 10485760),
                'ratePeriodInSecond': 1
            }
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/dispatchRate",
                json=dispatch_rate
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to set dispatch rate: {response.text}")
                
            # Set subscription dispatch rate
            subscription_rate = {
                'dispatchThrottlingRateInMsg': self.config.get('max_subscription_messages_per_second', 1000),
                'dispatchThrottlingRateInByte': self.config.get('max_subscription_bytes_per_second', 1048576),
                'ratePeriodInSecond': 1
            }
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/subscriptionDispatchRate",
                json=subscription_rate
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to set subscription dispatch rate: {response.text}")
                
            # Set backlog quota
            backlog_quota = {
                'limit': self.config.get('backlog_quota_gb', 5) * 1073741824,  # Convert GB to bytes
                'policy': 'consumer_backlog_eviction'
            }
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/backlogQuota",
                json=backlog_quota
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to set backlog quota: {response.text}")
                
            logger.info(f"Configured policies for namespace {namespace_path}")
            
        except Exception as e:
            logger.error(f"Failed to configure namespace policies: {str(e)}")
            
    def create_topics(self):
        """Create default topics for the tenant."""
        try:
            namespace_path = f"{self.tenant_name}/{self.namespace}"
            
            # Default topics to create
            default_topics = [
                {
                    'name': 'events',
                    'partitions': self.config.get('default_partitions', 4),
                    'type': 'persistent'
                },
                {
                    'name': 'commands',
                    'partitions': self.config.get('default_partitions', 4),
                    'type': 'persistent'
                },
                {
                    'name': 'metrics',
                    'partitions': self.config.get('metrics_partitions', 8),
                    'type': 'persistent'
                },
                {
                    'name': 'logs',
                    'partitions': self.config.get('logs_partitions', 8),
                    'type': 'persistent'
                },
                {
                    'name': 'dead-letter',
                    'partitions': 2,
                    'type': 'persistent'
                }
            ]
            
            # Add custom topics if specified
            custom_topics = self.config.get('custom_topics', [])
            default_topics.extend(custom_topics)
            
            for topic_config in default_topics:
                topic_name = topic_config['name']
                topic_type = topic_config.get('type', 'persistent')
                partitions = topic_config.get('partitions', 1)
                
                if partitions > 1:
                    # Create partitioned topic
                    topic_path = f"{topic_type}://{namespace_path}/{topic_name}"
                    
                    response = self.session.put(
                        f"{self.admin_url}/admin/v2/{topic_type}/{namespace_path}/{topic_name}/partitions",
                        json=partitions
                    )
                    
                    if response.status_code in [200, 204]:
                        logger.info(f"Created partitioned topic: {topic_path} with {partitions} partitions")
                    elif response.status_code == 409:
                        logger.warning(f"Topic {topic_path} already exists")
                    else:
                        logger.error(f"Failed to create topic {topic_path}: {response.text}")
                else:
                    # Create non-partitioned topic
                    topic_path = f"{topic_type}://{namespace_path}/{topic_name}"
                    
                    response = self.session.put(
                        f"{self.admin_url}/admin/v2/{topic_type}/{namespace_path}/{topic_name}"
                    )
                    
                    if response.status_code in [200, 204]:
                        logger.info(f"Created topic: {topic_path}")
                    elif response.status_code == 409:
                        logger.warning(f"Topic {topic_path} already exists")
                    else:
                        logger.error(f"Failed to create topic {topic_path}: {response.text}")
                        
        except Exception as e:
            logger.error(f"Failed to create topics: {str(e)}")
            
    def configure_security(self):
        """Configure namespace security and access control."""
        try:
            namespace_path = f"{self.tenant_name}/{self.namespace}"
            
            # Grant permissions to tenant admin role
            admin_role = f"admin-{self.tenant_id}"
            permissions = ['produce', 'consume', 'sources', 'sinks', 'functions']
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/permissions/{admin_role}",
                json=permissions
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to grant admin permissions: {response.text}")
                
            # Grant permissions to application role
            app_role = f"app-{self.tenant_id}"
            app_permissions = ['produce', 'consume']
            
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/permissions/{app_role}",
                json=app_permissions
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to grant app permissions: {response.text}")
                
            # Set authentication required
            response = self.session.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace_path}/authenticationEnabled",
                json=True
            )
            
            if response.status_code not in [200, 204]:
                logger.error(f"Failed to enable authentication: {response.text}")
                
            logger.info(f"Configured security for namespace {namespace_path}")
            
        except Exception as e:
            logger.error(f"Failed to configure security: {str(e)}")
            
    def create_subscriptions(self):
        """Create default subscriptions for topics."""
        try:
            namespace_path = f"{self.tenant_name}/{self.namespace}"
            
            # Default subscriptions to create
            subscriptions = [
                {'topic': 'events', 'subscription': 'event-processor'},
                {'topic': 'commands', 'subscription': 'command-handler'},
                {'topic': 'metrics', 'subscription': 'metrics-aggregator'},
                {'topic': 'logs', 'subscription': 'log-processor'}
            ]
            
            for sub_config in subscriptions:
                topic_name = sub_config['topic']
                subscription_name = sub_config['subscription']
                topic_path = f"persistent://{namespace_path}/{topic_name}"
                
                # Create subscription
                response = self.session.put(
                    f"{self.admin_url}/admin/v2/persistent/{namespace_path}/{topic_name}/subscription/{subscription_name}",
                    json={'replicated': False}
                )
                
                if response.status_code in [200, 204]:
                    logger.info(f"Created subscription {subscription_name} on topic {topic_path}")
                elif response.status_code == 409:
                    logger.warning(f"Subscription {subscription_name} already exists on topic {topic_path}")
                else:
                    logger.error(f"Failed to create subscription: {response.text}")
                    
        except Exception as e:
            logger.error(f"Failed to create subscriptions: {str(e)}")
            
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'pulsar',
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
            'pulsar_admin_url': inputs.get('pulsar_admin_url', 'http://localhost:8080'),
            'pulsar_service_url': inputs.get('pulsar_service_url', 'pulsar://localhost:6650'),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'pulsar_tenant': inputs.get('pulsar_tenant', f"tenant-{inputs['tenant_id']}"),
            'namespace': inputs.get('namespace', 'default'),
            'auth_token': inputs.get('auth_token'),
            'allowed_clusters': inputs.get('allowed_clusters', ['standalone']),
            'retention_hours': inputs.get('retention_hours', 72),
            'retention_size_gb': inputs.get('retention_size_gb', 10),
            'message_ttl_seconds': inputs.get('message_ttl_seconds', 0),
            'enable_deduplication': inputs.get('enable_deduplication', False),
            'max_messages_per_second': inputs.get('max_messages_per_second', 10000),
            'max_bytes_per_second': inputs.get('max_bytes_per_second', 10485760),
            'max_subscription_messages_per_second': inputs.get('max_subscription_messages_per_second', 1000),
            'max_subscription_bytes_per_second': inputs.get('max_subscription_bytes_per_second', 1048576),
            'backlog_quota_gb': inputs.get('backlog_quota_gb', 5),
            'default_partitions': inputs.get('default_partitions', 4),
            'metrics_partitions': inputs.get('metrics_partitions', 8),
            'logs_partitions': inputs.get('logs_partitions', 8),
            'custom_topics': inputs.get('custom_topics', []),
            'region': inputs.get('region', 'default')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['pulsar_config'] = config
        
        provisioner = PulsarProvisioner(config)
        
        # Create tenant
        provisioner.create_tenant()
        
        # Create namespace
        provisioner.create_namespace()
        
        # Create topics
        provisioner.create_topics()
        
        # Configure security
        provisioner.configure_security()
        
        # Create subscriptions
        provisioner.create_subscriptions()
        
        # Store namespace info in runtime properties
        ctx.instance.runtime_properties['pulsar_tenant'] = config['pulsar_tenant']
        ctx.instance.runtime_properties['namespace'] = config['namespace']
        ctx.instance.runtime_properties['namespace_created'] = True
        
        logger.info(f"Successfully provisioned Pulsar namespace for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Pulsar namespace: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 