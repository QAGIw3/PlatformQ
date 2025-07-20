"""Cloudify Integration Client for Platform Service Broker

Manages deployments through Cloudify for orchestrated provisioning.
"""

import logging
from typing import Dict, Any, Optional, Tuple
import asyncio

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class CloudifyClient:
    """Client for managing resources through Cloudify"""
    
    def __init__(self, broker_config: Dict[str, Any]):
        self.enabled = broker_config.get("cloudify", {}).get("enabled", False)
        self.base_url = broker_config.get("cloudify", {}).get("url", "http://cloudify-manager")
        self.username = broker_config.get("cloudify", {}).get("username", "admin")
        self.password = broker_config.get("cloudify", {}).get("password", "admin")
        self.tenant = broker_config.get("cloudify", {}).get("tenant", "default_tenant")
        
        if self.enabled:
            # Initialize HTTP client with auth
            self.client = httpx.AsyncClient(
                timeout=60.0,
                auth=(self.username, self.password),
                headers={
                    "Tenant": self.tenant,
                    "Content-Type": "application/json"
                }
            )
            
            # Blueprint IDs for OpenStack resources
            self.blueprint_ids = {
                "openstack-compute": "platformq-openstack-compute",
                "openstack-storage": "platformq-openstack-storage",
                "openstack-network": "platformq-openstack-network",
                "tenant-infrastructure": "platformq-tenant-infra"
            }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def provision_compute(
        self,
        instance_id: str,
        instance_type: str,
        image_id: str,
        network_id: str,
        parameters: Dict[str, Any],
        tenant_hierarchy: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Provision compute instance through Cloudify
        
        Returns:
            Tuple of (deployment_id, outputs)
        """
        if not self.enabled:
            return None, None
        
        deployment_id = f"compute-{instance_id}"
        
        # Prepare inputs for Cloudify blueprint
        inputs = {
            "instance_id": instance_id,
            "instance_type": instance_type,
            "image_id": image_id,
            "network_id": network_id,
            "availability_zone": parameters.get("availability_zone", "nova"),
            "key_name": parameters.get("key_name"),
            "security_groups": parameters.get("security_groups", ["default"]),
            "floating_ip_pool": parameters.get("floating_ip_pool"),
            "user_data": parameters.get("user_data", ""),
            
            # Tenant hierarchy for tagging
            "tenant_id": tenant_hierarchy["tenant_id"],
            "customer_id": tenant_hierarchy["customer_id"],
            "reseller_id": tenant_hierarchy["reseller_id"],
            
            # Resource tags
            "tags": {
                "managed-by": "platform-service-broker",
                "instance-id": instance_id,
                "tenant-id": tenant_hierarchy["tenant_id"],
                **parameters.get("tags", {})
            }
        }
        
        try:
            # Create deployment
            await self._create_deployment(
                deployment_id,
                self.blueprint_ids["openstack-compute"],
                inputs
            )
            
            # Execute install workflow
            execution_id = await self._execute_workflow(deployment_id, "install")
            
            if execution_id:
                # Wait for completion
                success = await self._wait_for_execution(execution_id)
                
                if success:
                    # Get outputs
                    outputs = await self._get_deployment_outputs(deployment_id)
                    return deployment_id, outputs
            
            return deployment_id, None
            
        except Exception as e:
            logger.error(f"Failed to provision compute through Cloudify: {e}")
            raise
    
    async def provision_storage(
        self,
        instance_id: str,
        size_gb: int,
        volume_type: str,
        parameters: Dict[str, Any],
        tenant_hierarchy: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Provision storage volume through Cloudify"""
        if not self.enabled:
            return None, None
        
        deployment_id = f"storage-{instance_id}"
        
        inputs = {
            "volume_id": instance_id,
            "size": size_gb,
            "volume_type": volume_type,
            "availability_zone": parameters.get("availability_zone", "nova"),
            "encrypted": parameters.get("encrypted", False),
            "tenant_id": tenant_hierarchy["tenant_id"],
            "customer_id": tenant_hierarchy["customer_id"],
            "reseller_id": tenant_hierarchy["reseller_id"]
        }
        
        try:
            await self._create_deployment(
                deployment_id,
                self.blueprint_ids["openstack-storage"],
                inputs
            )
            
            execution_id = await self._execute_workflow(deployment_id, "install")
            
            if execution_id:
                success = await self._wait_for_execution(execution_id)
                if success:
                    outputs = await self._get_deployment_outputs(deployment_id)
                    return deployment_id, outputs
            
            return deployment_id, None
            
        except Exception as e:
            logger.error(f"Failed to provision storage through Cloudify: {e}")
            raise
    
    async def deprovision_resource(
        self,
        deployment_id: str,
        force: bool = False
    ) -> bool:
        """Deprovision resource through Cloudify"""
        if not self.enabled:
            return False
        
        try:
            # Execute uninstall workflow
            if not force:
                execution_id = await self._execute_workflow(deployment_id, "uninstall")
                if execution_id:
                    success = await self._wait_for_execution(execution_id)
                    if not success and not force:
                        return False
            
            # Delete deployment
            return await self._delete_deployment(deployment_id, force)
            
        except Exception as e:
            logger.error(f"Failed to deprovision resource: {e}")
            return False
    
    async def scale_compute(
        self,
        deployment_id: str,
        delta: int
    ) -> Optional[str]:
        """Scale compute deployment"""
        if not self.enabled:
            return None
        
        return await self._execute_workflow(
            deployment_id,
            "scale",
            {
                "node_id": "compute_instance",
                "delta": delta,
                "scale_compute": True
            }
        )
    
    async def get_deployment_status(
        self,
        deployment_id: str
    ) -> Dict[str, Any]:
        """Get deployment status"""
        if not self.enabled:
            return {"status": "unknown"}
        
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v3.1/deployments/{deployment_id}"
            )
            
            if response.status_code == 200:
                deployment = response.json()
                
                # Get latest execution
                executions_response = await self.client.get(
                    f"{self.base_url}/api/v3.1/executions",
                    params={"deployment_id": deployment_id, "_size": 1}
                )
                
                latest_execution = None
                if executions_response.status_code == 200:
                    executions = executions_response.json().get("items", [])
                    if executions:
                        latest_execution = executions[0]
                
                return {
                    "status": "ready" if latest_execution and latest_execution["status"] == "terminated" else "in_progress",
                    "deployment": deployment,
                    "latest_execution": latest_execution
                }
            elif response.status_code == 404:
                return {"status": "not_found"}
            else:
                return {"status": "error", "message": response.text}
                
        except Exception as e:
            logger.error(f"Failed to get deployment status: {e}")
            return {"status": "error", "message": str(e)}
    
    async def _create_deployment(
        self,
        deployment_id: str,
        blueprint_id: str,
        inputs: Dict[str, Any]
    ) -> bool:
        """Create a deployment"""
        endpoint = f"{self.base_url}/api/v3.1/deployments/{deployment_id}"
        
        payload = {
            "blueprint_id": blueprint_id,
            "inputs": inputs,
            "visibility": "tenant"
        }
        
        response = await self.client.put(endpoint, json=payload)
        
        if response.status_code in (200, 201):
            logger.info(f"Created deployment {deployment_id}")
            return True
        else:
            logger.error(f"Failed to create deployment: {response.text}")
            raise Exception(f"Deployment creation failed: {response.text}")
    
    async def _execute_workflow(
        self,
        deployment_id: str,
        workflow_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Execute a workflow on a deployment"""
        endpoint = f"{self.base_url}/api/v3.1/executions"
        
        payload = {
            "deployment_id": deployment_id,
            "workflow_id": workflow_id,
            "parameters": parameters or {}
        }
        
        response = await self.client.post(endpoint, json=payload)
        
        if response.status_code in (200, 201):
            execution = response.json()
            execution_id = execution.get("id")
            logger.info(f"Started workflow {workflow_id} on {deployment_id}: {execution_id}")
            return execution_id
        else:
            logger.error(f"Failed to execute workflow: {response.text}")
            return None
    
    async def _wait_for_execution(
        self,
        execution_id: str,
        timeout_seconds: int = 600,
        poll_interval: int = 5
    ) -> bool:
        """Wait for an execution to complete"""
        elapsed = 0
        
        while elapsed < timeout_seconds:
            response = await self.client.get(
                f"{self.base_url}/api/v3.1/executions/{execution_id}"
            )
            
            if response.status_code == 200:
                execution = response.json()
                status = execution.get("status")
                
                if status == "terminated":
                    return True
                elif status in ("failed", "cancelled"):
                    logger.error(f"Execution {execution_id} {status}")
                    return False
            
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        
        logger.error(f"Execution {execution_id} timed out")
        return False
    
    async def _get_deployment_outputs(
        self,
        deployment_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get deployment outputs"""
        endpoint = f"{self.base_url}/api/v3.1/deployments/{deployment_id}/outputs"
        
        response = await self.client.get(endpoint)
        
        if response.status_code == 200:
            outputs = response.json()
            # Convert to simple key-value dict
            return {
                output["name"]: output["value"]
                for output in outputs.get("outputs", [])
            }
        else:
            logger.error(f"Failed to get deployment outputs: {response.text}")
            return None
    
    async def _delete_deployment(
        self,
        deployment_id: str,
        force: bool = False
    ) -> bool:
        """Delete a deployment"""
        endpoint = f"{self.base_url}/api/v3.1/deployments/{deployment_id}"
        
        response = await self.client.delete(
            endpoint,
            params={"force": force}
        )
        
        if response.status_code in (200, 202, 204):
            logger.info(f"Deleted deployment {deployment_id}")
            return True
        else:
            logger.error(f"Failed to delete deployment: {response.text}")
            return False
    
    async def close(self):
        """Close the HTTP client"""
        if hasattr(self, 'client'):
            await self.client.aclose() 