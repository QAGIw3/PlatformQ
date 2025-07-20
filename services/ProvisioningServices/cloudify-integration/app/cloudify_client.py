"""Cloudify Client for Platform Q

Manages deployments and blueprints through Cloudify's REST API.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import asyncio
import json

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class CloudifyClient:
    """Client for interacting with Cloudify Manager"""
    
    def __init__(self, config: Dict[str, Any]):
        self.base_url = config.get("cloudify_url", "http://cloudify-manager")
        self.username = config.get("cloudify_username", "admin")
        self.password = config.get("cloudify_password", "admin")
        self.tenant = config.get("cloudify_tenant", "default_tenant")
        
        # Initialize HTTP client with auth
        self.client = httpx.AsyncClient(
            timeout=60.0,
            auth=(self.username, self.password),
            headers={
                "Tenant": self.tenant,
                "Content-Type": "application/json"
            }
        )
        
        # Blueprint IDs for Platform Q services
        self.blueprint_ids = {
            "tenant-infrastructure": "platformq-tenant-infra",
            "cassandra": "platformq-cassandra",
            "ignite": "platformq-ignite",
            "pulsar": "platformq-pulsar",
            "minio": "platformq-minio",
            "elasticsearch": "platformq-elasticsearch",
            "janusgraph": "platformq-janusgraph"
        }
    
    async def upload_blueprint(
        self,
        blueprint_id: str,
        blueprint_path: str,
        blueprint_yaml: str = "blueprint.yaml"
    ) -> bool:
        """Upload a blueprint to Cloudify
        
        Args:
            blueprint_id: Unique identifier for the blueprint
            blueprint_path: Path to blueprint archive or directory
            blueprint_yaml: Main blueprint YAML file name
            
        Returns:
            bool: Success status
        """
        try:
            # For simplicity, assume blueprint is already packaged
            # In production, would package the blueprint directory
            
            endpoint = f"{self.base_url}/api/v3.1/blueprints/{blueprint_id}"
            
            # Upload blueprint
            with open(blueprint_path, 'rb') as f:
                files = {'blueprint_archive': f}
                data = {'blueprint_yaml_file': blueprint_yaml}
                
                response = await self.client.put(
                    endpoint,
                    files=files,
                    data=data
                )
            
            if response.status_code in (200, 201):
                logger.info(f"Successfully uploaded blueprint {blueprint_id}")
                return True
            else:
                logger.error(f"Failed to upload blueprint: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error uploading blueprint {blueprint_id}: {e}")
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def create_deployment(
        self,
        deployment_id: str,
        blueprint_id: str,
        inputs: Dict[str, Any]
    ) -> Optional[str]:
        """Create a deployment from a blueprint
        
        Args:
            deployment_id: Unique identifier for the deployment
            blueprint_id: Blueprint to deploy
            inputs: Input parameters for the deployment
            
        Returns:
            Optional[str]: Execution ID if successful
        """
        try:
            endpoint = f"{self.base_url}/api/v3.1/deployments/{deployment_id}"
            
            payload = {
                "blueprint_id": blueprint_id,
                "inputs": inputs,
                "visibility": "tenant",
                "site_name": inputs.get("site_name", "default")
            }
            
            response = await self.client.put(
                endpoint,
                json=payload
            )
            
            if response.status_code in (200, 201):
                logger.info(f"Successfully created deployment {deployment_id}")
                
                # Start install workflow
                return await self.execute_workflow(
                    deployment_id,
                    "install"
                )
            else:
                logger.error(f"Failed to create deployment: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating deployment {deployment_id}: {e}")
            return None
    
    async def execute_workflow(
        self,
        deployment_id: str,
        workflow_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Execute a workflow on a deployment
        
        Args:
            deployment_id: Deployment identifier
            workflow_id: Workflow to execute (install, uninstall, etc.)
            parameters: Workflow parameters
            
        Returns:
            Optional[str]: Execution ID if successful
        """
        try:
            endpoint = f"{self.base_url}/api/v3.1/executions"
            
            payload = {
                "deployment_id": deployment_id,
                "workflow_id": workflow_id,
                "parameters": parameters or {},
                "force": False,
                "dry_run": False
            }
            
            response = await self.client.post(
                endpoint,
                json=payload
            )
            
            if response.status_code in (200, 201):
                execution = response.json()
                execution_id = execution.get("id")
                logger.info(
                    f"Started workflow {workflow_id} on deployment {deployment_id}: "
                    f"execution {execution_id}"
                )
                return execution_id
            else:
                logger.error(f"Failed to execute workflow: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error executing workflow {workflow_id}: {e}")
            return None
    
    async def get_execution_status(
        self,
        execution_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get the status of an execution
        
        Args:
            execution_id: Execution identifier
            
        Returns:
            Optional[Dict]: Execution details if found
        """
        try:
            endpoint = f"{self.base_url}/api/v3.1/executions/{execution_id}"
            
            response = await self.client.get(endpoint)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get execution status: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting execution status: {e}")
            return None
    
    async def wait_for_execution(
        self,
        execution_id: str,
        timeout_seconds: int = 600,
        poll_interval: int = 5
    ) -> Tuple[bool, Optional[str]]:
        """Wait for an execution to complete
        
        Args:
            execution_id: Execution identifier
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks
            
        Returns:
            Tuple of (success, error_message)
        """
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout_seconds:
            execution = await self.get_execution_status(execution_id)
            
            if not execution:
                return False, "Failed to get execution status"
            
            status = execution.get("status")
            
            if status == "terminated":
                return True, None
            elif status in ("failed", "cancelled"):
                error = execution.get("error", "Unknown error")
                return False, f"Execution {status}: {error}"
            
            # Still running
            await asyncio.sleep(poll_interval)
        
        return False, f"Execution timed out after {timeout_seconds} seconds"
    
    async def get_deployment_outputs(
        self,
        deployment_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get outputs from a deployment
        
        Args:
            deployment_id: Deployment identifier
            
        Returns:
            Optional[Dict]: Deployment outputs if available
        """
        try:
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
                
        except Exception as e:
            logger.error(f"Error getting deployment outputs: {e}")
            return None
    
    async def delete_deployment(
        self,
        deployment_id: str,
        force: bool = False
    ) -> bool:
        """Delete a deployment
        
        Args:
            deployment_id: Deployment identifier
            force: Force deletion even if uninstall fails
            
        Returns:
            bool: Success status
        """
        try:
            # First run uninstall workflow
            if not force:
                execution_id = await self.execute_workflow(
                    deployment_id,
                    "uninstall"
                )
                
                if execution_id:
                    success, error = await self.wait_for_execution(execution_id)
                    if not success:
                        logger.error(f"Uninstall failed: {error}")
                        if not force:
                            return False
            
            # Delete the deployment
            endpoint = f"{self.base_url}/api/v3.1/deployments/{deployment_id}"
            
            response = await self.client.delete(
                endpoint,
                params={"force": force}
            )
            
            if response.status_code in (200, 202, 204):
                logger.info(f"Successfully deleted deployment {deployment_id}")
                return True
            else:
                logger.error(f"Failed to delete deployment: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting deployment {deployment_id}: {e}")
            return False
    
    async def provision_tenant_infrastructure(
        self,
        tenant_id: str,
        tenant_name: str,
        tenant_tier: str,
        metadata: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Provision complete infrastructure for a tenant
        
        Args:
            tenant_id: Unique tenant identifier
            tenant_name: Human-readable tenant name
            tenant_tier: Service tier (starter, standard, premium)
            metadata: Additional tenant metadata
            
        Returns:
            Optional[Dict]: Deployment outputs if successful
        """
        deployment_id = f"tenant-{tenant_id}"
        
        # Prepare inputs based on tier
        inputs = {
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "tier": tenant_tier,
            "reseller_id": metadata.get("reseller_id"),
            "customer_id": metadata.get("customer_id"),
            
            # Resource configurations based on tier
            "cassandra_replication": 3 if tenant_tier == "premium" else 1,
            "cassandra_keyspace": f"tenant_{tenant_id.replace('-', '_')}",
            
            "ignite_backups": 2 if tenant_tier == "premium" else 1,
            "ignite_cache_name": f"tenant_{tenant_id}_cache",
            
            "pulsar_namespace": f"platformq/tenant-{tenant_id}",
            "pulsar_retention_days": 30 if tenant_tier == "premium" else 7,
            
            "minio_bucket": f"tenant-{tenant_id}",
            "minio_quota_gb": 1000 if tenant_tier == "premium" else 100,
            
            "elasticsearch_shards": 3 if tenant_tier == "premium" else 1,
            "elasticsearch_replicas": 2 if tenant_tier == "premium" else 0,
            
            "kubernetes_namespace": f"tenant-{tenant_id}",
            "kubernetes_cpu_limit": "10" if tenant_tier == "premium" else "2",
            "kubernetes_memory_limit": "20Gi" if tenant_tier == "premium" else "4Gi"
        }
        
        # Create deployment
        execution_id = await self.create_deployment(
            deployment_id,
            self.blueprint_ids["tenant-infrastructure"],
            inputs
        )
        
        if not execution_id:
            return None
        
        # Wait for completion
        success, error = await self.wait_for_execution(execution_id)
        
        if not success:
            logger.error(f"Tenant provisioning failed: {error}")
            return None
        
        # Get outputs
        outputs = await self.get_deployment_outputs(deployment_id)
        
        return outputs
    
    async def provision_service(
        self,
        service_type: str,
        tenant_id: str,
        instance_id: str,
        parameters: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Provision a specific service for a tenant
        
        Args:
            service_type: Type of service (cassandra, ignite, etc.)
            tenant_id: Tenant identifier
            instance_id: Unique instance identifier
            parameters: Service-specific parameters
            
        Returns:
            Optional[Dict]: Deployment outputs if successful
        """
        blueprint_id = self.blueprint_ids.get(service_type)
        if not blueprint_id:
            logger.error(f"Unknown service type: {service_type}")
            return None
        
        deployment_id = f"{service_type}-{instance_id}"
        
        # Prepare inputs
        inputs = {
            "tenant_id": tenant_id,
            "instance_id": instance_id,
            **parameters
        }
        
        # Create deployment
        execution_id = await self.create_deployment(
            deployment_id,
            blueprint_id,
            inputs
        )
        
        if not execution_id:
            return None
        
        # Wait for completion
        success, error = await self.wait_for_execution(execution_id)
        
        if not success:
            logger.error(f"Service provisioning failed: {error}")
            return None
        
        # Get outputs
        outputs = await self.get_deployment_outputs(deployment_id)
        
        return outputs
    
    async def scale_deployment(
        self,
        deployment_id: str,
        node_id: str,
        delta: int
    ) -> Optional[str]:
        """Scale a deployment node
        
        Args:
            deployment_id: Deployment identifier
            node_id: Node to scale
            delta: Number of instances to add (positive) or remove (negative)
            
        Returns:
            Optional[str]: Execution ID if successful
        """
        return await self.execute_workflow(
            deployment_id,
            "scale",
            {
                "node_id": node_id,
                "delta": delta,
                "scale_compute": True
            }
        )
    
    async def heal_deployment(
        self,
        deployment_id: str,
        node_instance_id: Optional[str] = None
    ) -> Optional[str]:
        """Heal a deployment or specific node instance
        
        Args:
            deployment_id: Deployment identifier
            node_instance_id: Optional specific node instance to heal
            
        Returns:
            Optional[str]: Execution ID if successful
        """
        parameters = {}
        if node_instance_id:
            parameters["node_instance_id"] = node_instance_id
        
        return await self.execute_workflow(
            deployment_id,
            "heal",
            parameters
        )
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 