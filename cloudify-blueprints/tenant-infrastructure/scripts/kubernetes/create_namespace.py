#!/usr/bin/env python3
"""
Cloudify script to create Kubernetes namespace and resources for a tenant.
"""

import os
import sys
import time
import json
import base64
import logging
from typing import Dict, Any, List, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
from kubernetes import client, config
from kubernetes.client import V1Namespace, V1ObjectMeta, V1ResourceQuota, V1LimitRange
from kubernetes.client.rest import ApiException

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('kubernetes_provisioner')


class KubernetesProvisioner:
    """Handles Kubernetes namespace and resource provisioning for tenants."""
    
    def __init__(self, k8s_config: Dict[str, Any]):
        self.config = k8s_config
        self.tenant_id = k8s_config['tenant_id']
        self.reseller_id = k8s_config.get('reseller_id')
        self.customer_id = k8s_config.get('customer_id')
        self.namespace = k8s_config.get('namespace', f"tenant-{self.tenant_id}")
        
        # Initialize Kubernetes client
        if k8s_config.get('kubeconfig_path'):
            config.load_kube_config(config_file=k8s_config['kubeconfig_path'])
        elif k8s_config.get('in_cluster', False):
            config.load_incluster_config()
        else:
            # Use provided cluster config
            configuration = client.Configuration()
            configuration.host = k8s_config.get('api_server', 'https://localhost:6443')
            
            if k8s_config.get('api_token'):
                configuration.api_key = {"authorization": f"Bearer {k8s_config['api_token']}"}
            
            if k8s_config.get('ca_cert'):
                configuration.ssl_ca_cert = k8s_config['ca_cert']
                
            configuration.verify_ssl = k8s_config.get('verify_ssl', True)
            client.Configuration.set_default(configuration)
            
        self.core_v1 = client.CoreV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.rbac_v1 = client.RbacAuthorizationV1Api()
        self.networking_v1 = client.NetworkingV1Api()
        
    def create_namespace(self):
        """Create namespace for the tenant."""
        try:
            # Check if namespace exists
            try:
                existing = self.core_v1.read_namespace(name=self.namespace)
                logger.info(f"Namespace {self.namespace} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create namespace
            namespace = V1Namespace(
                metadata=V1ObjectMeta(
                    name=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'reseller-id': self.reseller_id or 'none',
                        'customer-id': self.customer_id or 'none',
                        'managed-by': 'cloudify',
                        'environment': self.config.get('environment', 'production')
                    },
                    annotations={
                        'tenant/id': self.tenant_id,
                        'tenant/created-at': str(int(time.time())),
                        'tenant/contact': self.config.get('contact_email', ''),
                        'tenant/description': f"Namespace for tenant {self.tenant_id}"
                    }
                )
            )
            
            created = self.core_v1.create_namespace(body=namespace)
            logger.info(f"Created namespace: {self.namespace}")
            
            # Report usage
            self._report_usage('namespace_created', {
                'namespace': self.namespace,
                'cpu_quota': self.config.get('cpu_quota', '10'),
                'memory_quota': self.config.get('memory_quota', '10Gi')
            })
            
            return created
            
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create namespace: {str(e)}")
            
    def create_resource_quota(self):
        """Create resource quota for the namespace."""
        try:
            quota_name = f"{self.namespace}-quota"
            
            # Check if quota exists
            try:
                existing = self.core_v1.read_namespaced_resource_quota(
                    name=quota_name,
                    namespace=self.namespace
                )
                logger.info(f"Resource quota {quota_name} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create resource quota
            quota = V1ResourceQuota(
                metadata=V1ObjectMeta(
                    name=quota_name,
                    namespace=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'type': 'tenant-quota'
                    }
                ),
                spec=client.V1ResourceQuotaSpec(
                    hard={
                        'requests.cpu': self.config.get('cpu_quota', '10'),
                        'requests.memory': self.config.get('memory_quota', '10Gi'),
                        'limits.cpu': self.config.get('cpu_limit', '20'),
                        'limits.memory': self.config.get('memory_limit', '20Gi'),
                        'requests.storage': self.config.get('storage_quota', '100Gi'),
                        'persistentvolumeclaims': str(self.config.get('pvc_quota', 10)),
                        'pods': str(self.config.get('pod_quota', 50)),
                        'services': str(self.config.get('service_quota', 10)),
                        'services.loadbalancers': str(self.config.get('lb_quota', 2)),
                        'services.nodeports': str(self.config.get('nodeport_quota', 5)),
                        'configmaps': str(self.config.get('configmap_quota', 50)),
                        'secrets': str(self.config.get('secret_quota', 50))
                    }
                )
            )
            
            created = self.core_v1.create_namespaced_resource_quota(
                namespace=self.namespace,
                body=quota
            )
            logger.info(f"Created resource quota: {quota_name}")
            
            return created
            
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create resource quota: {str(e)}")
            
    def create_limit_range(self):
        """Create limit range for the namespace."""
        try:
            limit_name = f"{self.namespace}-limits"
            
            # Check if limit range exists
            try:
                existing = self.core_v1.read_namespaced_limit_range(
                    name=limit_name,
                    namespace=self.namespace
                )
                logger.info(f"Limit range {limit_name} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create limit range
            limits = V1LimitRange(
                metadata=V1ObjectMeta(
                    name=limit_name,
                    namespace=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'type': 'tenant-limits'
                    }
                ),
                spec=client.V1LimitRangeSpec(
                    limits=[
                        {
                            'type': 'Container',
                            'default': {
                                'cpu': self.config.get('container_default_cpu', '500m'),
                                'memory': self.config.get('container_default_memory', '512Mi')
                            },
                            'defaultRequest': {
                                'cpu': self.config.get('container_request_cpu', '100m'),
                                'memory': self.config.get('container_request_memory', '128Mi')
                            },
                            'min': {
                                'cpu': self.config.get('container_min_cpu', '50m'),
                                'memory': self.config.get('container_min_memory', '64Mi')
                            },
                            'max': {
                                'cpu': self.config.get('container_max_cpu', '4'),
                                'memory': self.config.get('container_max_memory', '8Gi')
                            }
                        },
                        {
                            'type': 'PersistentVolumeClaim',
                            'min': {
                                'storage': self.config.get('pvc_min_storage', '1Gi')
                            },
                            'max': {
                                'storage': self.config.get('pvc_max_storage', '100Gi')
                            }
                        }
                    ]
                )
            )
            
            created = self.core_v1.create_namespaced_limit_range(
                namespace=self.namespace,
                body=limits
            )
            logger.info(f"Created limit range: {limit_name}")
            
            return created
            
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create limit range: {str(e)}")
            
    def create_network_policy(self):
        """Create network policy for tenant isolation."""
        try:
            policy_name = f"{self.namespace}-isolation"
            
            # Check if network policy exists
            try:
                existing = self.networking_v1.read_namespaced_network_policy(
                    name=policy_name,
                    namespace=self.namespace
                )
                logger.info(f"Network policy {policy_name} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create network policy
            policy = client.V1NetworkPolicy(
                metadata=V1ObjectMeta(
                    name=policy_name,
                    namespace=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'type': 'tenant-isolation'
                    }
                ),
                spec=client.V1NetworkPolicySpec(
                    pod_selector=client.V1LabelSelector(match_labels={}),
                    policy_types=['Ingress', 'Egress'],
                    ingress=[
                        {
                            'from': [
                                {
                                    'namespace_selector': {
                                        'match_labels': {
                                            'tenant-id': self.tenant_id
                                        }
                                    }
                                },
                                {
                                    'pod_selector': {}
                                }
                            ]
                        }
                    ],
                    egress=[
                        {
                            'to': [
                                {
                                    'namespace_selector': {
                                        'match_labels': {
                                            'tenant-id': self.tenant_id
                                        }
                                    }
                                },
                                {
                                    'pod_selector': {}
                                },
                                {
                                    # Allow DNS
                                    'namespace_selector': {
                                        'match_labels': {
                                            'name': 'kube-system'
                                        }
                                    },
                                    'pod_selector': {
                                        'match_labels': {
                                            'k8s-app': 'kube-dns'
                                        }
                                    }
                                }
                            ],
                            'ports': [
                                {
                                    'protocol': 'TCP',
                                    'port': 53
                                },
                                {
                                    'protocol': 'UDP',
                                    'port': 53
                                }
                            ]
                        },
                        {
                            # Allow external traffic if enabled
                            'to': [
                                {
                                    'ip_block': {
                                        'cidr': '0.0.0.0/0',
                                        'except': ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16']
                                    }
                                }
                            ]
                        } if self.config.get('allow_external_traffic', True) else {}
                    ]
                )
            )
            
            created = self.networking_v1.create_namespaced_network_policy(
                namespace=self.namespace,
                body=policy
            )
            logger.info(f"Created network policy: {policy_name}")
            
            return created
            
        except ApiException as e:
            logger.error(f"Failed to create network policy: {str(e)}")
            # Network policies might not be supported - non-fatal
            
    def create_service_account(self):
        """Create service account for the tenant."""
        try:
            sa_name = f"{self.namespace}-sa"
            
            # Check if service account exists
            try:
                existing = self.core_v1.read_namespaced_service_account(
                    name=sa_name,
                    namespace=self.namespace
                )
                logger.info(f"Service account {sa_name} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create service account
            sa = client.V1ServiceAccount(
                metadata=V1ObjectMeta(
                    name=sa_name,
                    namespace=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'type': 'tenant-sa'
                    }
                )
            )
            
            created = self.core_v1.create_namespaced_service_account(
                namespace=self.namespace,
                body=sa
            )
            logger.info(f"Created service account: {sa_name}")
            
            return created
            
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create service account: {str(e)}")
            
    def create_rbac_rules(self):
        """Create RBAC rules for tenant access."""
        try:
            role_name = f"{self.namespace}-tenant-role"
            binding_name = f"{self.namespace}-tenant-binding"
            
            # Create role
            try:
                self.rbac_v1.read_namespaced_role(
                    name=role_name,
                    namespace=self.namespace
                )
                logger.info(f"Role {role_name} already exists")
            except ApiException as e:
                if e.status == 404:
                    role = client.V1Role(
                        metadata=V1ObjectMeta(
                            name=role_name,
                            namespace=self.namespace,
                            labels={
                                'tenant-id': self.tenant_id,
                                'type': 'tenant-role'
                            }
                        ),
                        rules=[
                            {
                                'apiGroups': ['', 'apps', 'batch', 'extensions'],
                                'resources': [
                                    'pods', 'services', 'deployments', 'replicasets',
                                    'statefulsets', 'daemonsets', 'jobs', 'cronjobs',
                                    'configmaps', 'secrets', 'persistentvolumeclaims'
                                ],
                                'verbs': ['*']
                            },
                            {
                                'apiGroups': ['networking.k8s.io'],
                                'resources': ['ingresses'],
                                'verbs': ['*']
                            }
                        ]
                    )
                    
                    self.rbac_v1.create_namespaced_role(
                        namespace=self.namespace,
                        body=role
                    )
                    logger.info(f"Created role: {role_name}")
                    
            # Create role binding
            try:
                self.rbac_v1.read_namespaced_role_binding(
                    name=binding_name,
                    namespace=self.namespace
                )
                logger.info(f"Role binding {binding_name} already exists")
            except ApiException as e:
                if e.status == 404:
                    binding = client.V1RoleBinding(
                        metadata=V1ObjectMeta(
                            name=binding_name,
                            namespace=self.namespace,
                            labels={
                                'tenant-id': self.tenant_id,
                                'type': 'tenant-binding'
                            }
                        ),
                        role_ref=client.V1RoleRef(
                            api_group='rbac.authorization.k8s.io',
                            kind='Role',
                            name=role_name
                        ),
                        subjects=[
                            client.V1Subject(
                                kind='ServiceAccount',
                                name=f"{self.namespace}-sa",
                                namespace=self.namespace
                            ),
                            client.V1Subject(
                                kind='Group',
                                name=f"tenant:{self.tenant_id}",
                                api_group='rbac.authorization.k8s.io'
                            )
                        ]
                    )
                    
                    self.rbac_v1.create_namespaced_role_binding(
                        namespace=self.namespace,
                        body=binding
                    )
                    logger.info(f"Created role binding: {binding_name}")
                    
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create RBAC rules: {str(e)}")
            
    def create_default_config(self):
        """Create default ConfigMap for tenant configuration."""
        try:
            config_name = f"{self.namespace}-config"
            
            # Check if ConfigMap exists
            try:
                existing = self.core_v1.read_namespaced_config_map(
                    name=config_name,
                    namespace=self.namespace
                )
                logger.info(f"ConfigMap {config_name} already exists")
                return existing
            except ApiException as e:
                if e.status != 404:
                    raise
                    
            # Create ConfigMap
            config_data = {
                'tenant.id': self.tenant_id,
                'tenant.namespace': self.namespace,
                'reseller.id': self.reseller_id or '',
                'customer.id': self.customer_id or '',
                'region': self.config.get('region', 'default'),
                'environment': self.config.get('environment', 'production'),
                
                # Service endpoints
                'cassandra.hosts': ','.join(self.config.get('cassandra_hosts', ['cassandra'])),
                'ignite.hosts': ','.join(self.config.get('ignite_hosts', ['ignite'])),
                'pulsar.url': self.config.get('pulsar_url', 'pulsar://pulsar:6650'),
                'minio.endpoint': self.config.get('minio_endpoint', 'minio:9000'),
                'elasticsearch.hosts': ','.join(self.config.get('elasticsearch_hosts', ['elasticsearch:9200'])),
                'janusgraph.gremlin': self.config.get('janusgraph_gremlin', 'janusgraph:8182')
            }
            
            config_map = client.V1ConfigMap(
                metadata=V1ObjectMeta(
                    name=config_name,
                    namespace=self.namespace,
                    labels={
                        'tenant-id': self.tenant_id,
                        'type': 'tenant-config'
                    }
                ),
                data=config_data
            )
            
            created = self.core_v1.create_namespaced_config_map(
                namespace=self.namespace,
                body=config_map
            )
            logger.info(f"Created ConfigMap: {config_name}")
            
            return created
            
        except ApiException as e:
            raise NonRecoverableError(f"Failed to create ConfigMap: {str(e)}")
            
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'kubernetes',
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
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'namespace': inputs.get('namespace', f"tenant-{inputs['tenant_id']}"),
            
            # Kubernetes connection
            'kubeconfig_path': inputs.get('kubeconfig_path'),
            'in_cluster': inputs.get('in_cluster', False),
            'api_server': inputs.get('api_server', 'https://localhost:6443'),
            'api_token': inputs.get('api_token'),
            'ca_cert': inputs.get('ca_cert'),
            'verify_ssl': inputs.get('verify_ssl', True),
            
            # Resource quotas
            'cpu_quota': inputs.get('cpu_quota', '10'),
            'cpu_limit': inputs.get('cpu_limit', '20'),
            'memory_quota': inputs.get('memory_quota', '10Gi'),
            'memory_limit': inputs.get('memory_limit', '20Gi'),
            'storage_quota': inputs.get('storage_quota', '100Gi'),
            'pod_quota': inputs.get('pod_quota', 50),
            'pvc_quota': inputs.get('pvc_quota', 10),
            'service_quota': inputs.get('service_quota', 10),
            'lb_quota': inputs.get('lb_quota', 2),
            'nodeport_quota': inputs.get('nodeport_quota', 5),
            'configmap_quota': inputs.get('configmap_quota', 50),
            'secret_quota': inputs.get('secret_quota', 50),
            
            # Container limits
            'container_default_cpu': inputs.get('container_default_cpu', '500m'),
            'container_default_memory': inputs.get('container_default_memory', '512Mi'),
            'container_request_cpu': inputs.get('container_request_cpu', '100m'),
            'container_request_memory': inputs.get('container_request_memory', '128Mi'),
            'container_min_cpu': inputs.get('container_min_cpu', '50m'),
            'container_min_memory': inputs.get('container_min_memory', '64Mi'),
            'container_max_cpu': inputs.get('container_max_cpu', '4'),
            'container_max_memory': inputs.get('container_max_memory', '8Gi'),
            
            # PVC limits
            'pvc_min_storage': inputs.get('pvc_min_storage', '1Gi'),
            'pvc_max_storage': inputs.get('pvc_max_storage', '100Gi'),
            
            # Network policy
            'allow_external_traffic': inputs.get('allow_external_traffic', True),
            
            # Environment
            'environment': inputs.get('environment', 'production'),
            'region': inputs.get('region', 'default'),
            'contact_email': inputs.get('contact_email', ''),
            
            # Service endpoints for ConfigMap
            'cassandra_hosts': inputs.get('cassandra_hosts', ['cassandra']),
            'ignite_hosts': inputs.get('ignite_hosts', ['ignite']),
            'pulsar_url': inputs.get('pulsar_url', 'pulsar://pulsar:6650'),
            'minio_endpoint': inputs.get('minio_endpoint', 'minio:9000'),
            'elasticsearch_hosts': inputs.get('elasticsearch_hosts', ['elasticsearch:9200']),
            'janusgraph_gremlin': inputs.get('janusgraph_gremlin', 'janusgraph:8182')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['kubernetes_config'] = config
        
        provisioner = KubernetesProvisioner(config)
        
        # Create namespace
        provisioner.create_namespace()
        
        # Create resource quota
        provisioner.create_resource_quota()
        
        # Create limit range
        provisioner.create_limit_range()
        
        # Create network policy
        provisioner.create_network_policy()
        
        # Create service account
        provisioner.create_service_account()
        
        # Create RBAC rules
        provisioner.create_rbac_rules()
        
        # Create default configuration
        provisioner.create_default_config()
        
        # Store namespace info in runtime properties
        ctx.instance.runtime_properties['namespace'] = config['namespace']
        ctx.instance.runtime_properties['namespace_created'] = True
        
        logger.info(f"Successfully provisioned Kubernetes namespace for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision Kubernetes namespace: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 