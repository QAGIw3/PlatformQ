"""Crossplane Integration for Platform Service Broker

Provisions resources through Crossplane claims instead of direct cloud APIs.
"""

import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
import asyncio

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class CrossplaneClient:
    """Client for managing resources through Crossplane"""
    
    def __init__(self, broker_config: Dict[str, Any]):
        self.enabled = broker_config.get("crossplane", {}).get("enabled", False)
        self.namespace_prefix = broker_config.get("crossplane", {}).get("namespace_prefix", "tenant-")
        
        if self.enabled:
            # Load Kubernetes config
            try:
                config.load_incluster_config()  # In-cluster config
            except:
                config.load_kube_config()  # Local kubeconfig
            
            # Initialize Kubernetes clients
            self.core_v1 = client.CoreV1Api()
            self.custom_api = client.CustomObjectsApi()
            self.rbac_v1 = client.RbacAuthorizationV1Api()
            
            # Crossplane API details
            self.group = "platformq.io"
            self.version = "v1alpha1"
    
    async def provision_compute(
        self,
        instance_id: str,
        tenant_id: str,
        instance_type: str,
        os_image: str,
        parameters: Dict[str, Any],
        tenant_hierarchy: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Provision compute instance through Crossplane
        
        Returns:
            Tuple of (operation_id, connection_info)
        """
        if not self.enabled:
            return None, None
        
        # Ensure tenant namespace exists
        namespace = await self._ensure_tenant_namespace(tenant_id)
        
        # Create ComputeInstanceClaim
        claim = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "ComputeInstanceClaim",
            "metadata": {
                "name": f"osb-{instance_id}",
                "namespace": namespace,
                "labels": {
                    "platformq.io/instance-id": instance_id,
                    "platformq.io/tenant-id": tenant_id,
                    "platformq.io/service": "compute",
                    "platformq.io/provisioner": "service-broker"
                }
            },
            "spec": {
                "instanceType": instance_type,
                "osImage": os_image,
                "publicIpEnabled": parameters.get("public_ip", False),
                "networkId": parameters.get("network_id", ""),
                "rootDiskSize": parameters.get("disk_size", 20),
                "tenantId": tenant_hierarchy["tenant_id"],
                "customerId": tenant_hierarchy["customer_id"],
                "resellerId": tenant_hierarchy["reseller_id"],
                "metadata": {
                    "managed-by": "platform-service-broker",
                    "instance-id": instance_id
                },
                "tags": parameters.get("tags", {}),
                "writeConnectionSecretToRef": {
                    "name": f"osb-{instance_id}-connection"
                }
            }
        }
        
        # Add additional disks if specified
        if "additional_disks" in parameters:
            claim["spec"]["additionalDisks"] = parameters["additional_disks"]
        
        # Add SSH key if specified
        if "ssh_key_name" in parameters:
            claim["spec"]["sshKeyName"] = parameters["ssh_key_name"]
        
        try:
            # Create the claim
            response = await asyncio.to_thread(
                self.custom_api.create_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural="computeinstanceclaims",
                body=claim
            )
            
            logger.info(f"Created ComputeInstanceClaim {claim['metadata']['name']} in namespace {namespace}")
            
            # Return operation ID (claim name) for status tracking
            return claim["metadata"]["name"], None
            
        except ApiException as e:
            logger.error(f"Failed to create ComputeInstanceClaim: {e}")
            raise
    
    async def provision_platform_service(
        self,
        instance_id: str,
        tenant_id: str,
        service_type: str,
        plan: str,
        parameters: Dict[str, Any],
        tenant_hierarchy: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Provision platform service through Crossplane
        
        Returns:
            Tuple of (operation_id, connection_info)
        """
        if not self.enabled:
            return None, None
        
        # Ensure tenant namespace exists
        namespace = await self._ensure_tenant_namespace(tenant_id)
        
        # Create PlatformServiceClaim
        claim = {
            "apiVersion": f"{self.group}/{self.version}",
            "kind": "PlatformServiceClaim",
            "metadata": {
                "name": f"osb-{instance_id}",
                "namespace": namespace,
                "labels": {
                    "platformq.io/instance-id": instance_id,
                    "platformq.io/tenant-id": tenant_id,
                    "platformq.io/service": service_type,
                    "platformq.io/provisioner": "service-broker"
                }
            },
            "spec": {
                "serviceType": service_type,
                "plan": plan,
                "tenantId": tenant_hierarchy["tenant_id"],
                "customerId": tenant_hierarchy["customer_id"],
                "resellerId": tenant_hierarchy["reseller_id"],
                "writeConnectionSecretToRef": {
                    "name": f"osb-{instance_id}-connection"
                }
            }
        }
        
        # Add service-specific configuration
        if service_type == "cassandra" and "cassandra" in parameters:
            claim["spec"]["cassandra"] = parameters["cassandra"]
        elif service_type == "ignite" and "ignite" in parameters:
            claim["spec"]["ignite"] = parameters["ignite"]
        elif service_type == "pulsar" and "pulsar" in parameters:
            claim["spec"]["pulsar"] = parameters["pulsar"]
        elif service_type == "minio" and "minio" in parameters:
            claim["spec"]["minio"] = parameters["minio"]
        
        # Add resource limits if specified
        if "resources" in parameters:
            claim["spec"]["resources"] = parameters["resources"]
        
        try:
            # Create the claim
            response = await asyncio.to_thread(
                self.custom_api.create_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural="platformserviceclaims",
                body=claim
            )
            
            logger.info(f"Created PlatformServiceClaim {claim['metadata']['name']} in namespace {namespace}")
            
            # Return operation ID (claim name) for status tracking
            return claim["metadata"]["name"], None
            
        except ApiException as e:
            logger.error(f"Failed to create PlatformServiceClaim: {e}")
            raise
    
    async def get_resource_status(
        self,
        claim_name: str,
        claim_type: str,
        tenant_id: str
    ) -> Dict[str, Any]:
        """Get status of a Crossplane claim"""
        
        namespace = self._get_tenant_namespace(tenant_id)
        plural = "computeinstanceclaims" if claim_type == "compute" else "platformserviceclaims"
        
        try:
            claim = await asyncio.to_thread(
                self.custom_api.get_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=plural,
                name=claim_name
            )
            
            status = claim.get("status", {})
            
            # Check if ready
            ready = status.get("ready", False)
            phase = status.get("phase", "Unknown")
            
            # Get connection secret if ready
            connection_info = None
            if ready:
                secret_name = claim["spec"].get("writeConnectionSecretToRef", {}).get("name")
                if secret_name:
                    connection_info = await self._get_connection_secret(namespace, secret_name)
            
            return {
                "ready": ready,
                "phase": phase,
                "conditions": status.get("conditions", []),
                "connection_info": connection_info
            }
            
        except ApiException as e:
            if e.status == 404:
                return {"ready": False, "phase": "NotFound"}
            raise
    
    async def delete_resource(
        self,
        claim_name: str,
        claim_type: str,
        tenant_id: str
    ) -> bool:
        """Delete a Crossplane claim"""
        
        namespace = self._get_tenant_namespace(tenant_id)
        plural = "computeinstanceclaims" if claim_type == "compute" else "platformserviceclaims"
        
        try:
            await asyncio.to_thread(
                self.custom_api.delete_namespaced_custom_object,
                group=self.group,
                version=self.version,
                namespace=namespace,
                plural=plural,
                name=claim_name
            )
            
            logger.info(f"Deleted {claim_type} claim {claim_name} in namespace {namespace}")
            return True
            
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"Claim {claim_name} not found, considering deleted")
                return True
            logger.error(f"Failed to delete claim: {e}")
            return False
    
    async def _ensure_tenant_namespace(self, tenant_id: str) -> str:
        """Ensure tenant namespace exists with proper RBAC"""
        
        namespace_name = self._get_tenant_namespace(tenant_id)
        
        # Check if namespace exists
        try:
            await asyncio.to_thread(
                self.core_v1.read_namespace,
                name=namespace_name
            )
            return namespace_name
        except ApiException as e:
            if e.status != 404:
                raise
        
        # Create namespace
        namespace = client.V1Namespace(
            metadata=client.V1ObjectMeta(
                name=namespace_name,
                labels={
                    "platformq.io/tenant-id": tenant_id,
                    "platformq.io/managed-by": "service-broker"
                }
            )
        )
        
        try:
            await asyncio.to_thread(
                self.core_v1.create_namespace,
                body=namespace
            )
            logger.info(f"Created namespace {namespace_name} for tenant {tenant_id}")
        except ApiException as e:
            if e.status != 409:  # Already exists
                raise
        
        # Create RBAC for tenant
        await self._create_tenant_rbac(namespace_name, tenant_id)
        
        # Create resource quota
        await self._create_resource_quota(namespace_name)
        
        return namespace_name
    
    async def _create_tenant_rbac(self, namespace: str, tenant_id: str):
        """Create RBAC rules for tenant namespace"""
        
        # Create Role
        role = client.V1Role(
            metadata=client.V1ObjectMeta(
                name="tenant-user",
                namespace=namespace
            ),
            rules=[
                client.V1PolicyRule(
                    api_groups=[self.group],
                    resources=["computeinstanceclaims", "platformserviceclaims"],
                    verbs=["get", "list", "watch", "create", "update", "patch", "delete"]
                ),
                client.V1PolicyRule(
                    api_groups=[""],
                    resources=["secrets"],
                    verbs=["get", "list"]
                )
            ]
        )
        
        try:
            await asyncio.to_thread(
                self.rbac_v1.create_namespaced_role,
                namespace=namespace,
                body=role
            )
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create role: {e}")
    
    async def _create_resource_quota(self, namespace: str):
        """Create resource quota for tenant namespace"""
        
        quota = client.V1ResourceQuota(
            metadata=client.V1ObjectMeta(
                name="tenant-quota",
                namespace=namespace
            ),
            spec=client.V1ResourceQuotaSpec(
                hard={
                    "computeinstanceclaims.platformq.io": "20",
                    "platformserviceclaims.platformq.io": "50",
                    "persistentvolumeclaims": "100",
                    "requests.cpu": "100",
                    "requests.memory": "200Gi",
                    "requests.storage": "1Ti"
                }
            )
        )
        
        try:
            await asyncio.to_thread(
                self.core_v1.create_namespaced_resource_quota,
                namespace=namespace,
                body=quota
            )
        except ApiException as e:
            if e.status != 409:
                logger.error(f"Failed to create resource quota: {e}")
    
    async def _get_connection_secret(self, namespace: str, secret_name: str) -> Dict[str, Any]:
        """Get connection details from secret"""
        
        try:
            secret = await asyncio.to_thread(
                self.core_v1.read_namespaced_secret,
                name=secret_name,
                namespace=namespace
            )
            
            # Decode secret data
            connection_info = {}
            for key, value in secret.data.items():
                import base64
                connection_info[key] = base64.b64decode(value).decode('utf-8')
            
            return connection_info
            
        except ApiException as e:
            logger.error(f"Failed to get connection secret: {e}")
            return {}
    
    def _get_tenant_namespace(self, tenant_id: str) -> str:
        """Get namespace name for tenant"""
        return f"{self.namespace_prefix}{tenant_id}" 