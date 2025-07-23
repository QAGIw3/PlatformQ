"""
Kubernetes Manager for unified orchestration
"""

import logging
from typing import Dict, Any, List, Optional
import uuid
from datetime import datetime

from kubernetes import client, config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)


class K8sManager:
    """Manages Kubernetes job and pod creation"""
    
    def __init__(self, in_cluster: bool = True, namespace: str = "default"):
        self.namespace = namespace
        
        # Initialize Kubernetes client
        if in_cluster:
            config.load_incluster_config()
        else:
            config.load_kube_config()
            
        self.batch_api = client.BatchV1Api()
        self.core_api = client.CoreV1Api()
        self.apps_api = client.AppsV1Api()
        
    async def create_job(self, name: str, image: str, command: List[str], 
                        args: List[str] = None, env_vars: Dict[str, str] = None,
                        resources: Dict[str, Any] = None, labels: Dict[str, str] = None) -> str:
        """Create a Kubernetes job"""
        try:
            # Generate unique job name
            job_name = f"{name}-{uuid.uuid4().hex[:8]}"
            
            # Create container spec
            container = client.V1Container(
                name=name,
                image=image,
                command=command,
                args=args or [],
                env=[
                    client.V1EnvVar(name=k, value=v) 
                    for k, v in (env_vars or {}).items()
                ]
            )
            
            # Add resource requirements
            if resources:
                container.resources = client.V1ResourceRequirements(
                    requests=resources.get("requests", {}),
                    limits=resources.get("limits", {})
                )
            
            # Create pod template
            template = client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels=labels or {"app": name}
                ),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[container]
                )
            )
            
            # Create job spec
            spec = client.V1JobSpec(
                template=template,
                backoff_limit=3,
                ttl_seconds_after_finished=3600  # Clean up after 1 hour
            )
            
            # Create job object
            job = client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=client.V1ObjectMeta(
                    name=job_name,
                    labels=labels or {"app": name}
                ),
                spec=spec
            )
            
            # Create job
            api_response = self.batch_api.create_namespaced_job(
                namespace=self.namespace,
                body=job
            )
            
            logger.info(f"Created Kubernetes job: {job_name}")
            return job_name
            
        except ApiException as e:
            logger.error(f"Failed to create Kubernetes job: {e}")
            raise
    
    async def get_job_status(self, job_name: str) -> Dict[str, Any]:
        """Get status of a Kubernetes job"""
        try:
            job = self.batch_api.read_namespaced_job_status(
                name=job_name,
                namespace=self.namespace
            )
            
            status = {
                "name": job_name,
                "active": job.status.active or 0,
                "succeeded": job.status.succeeded or 0,
                "failed": job.status.failed or 0,
                "start_time": job.status.start_time,
                "completion_time": job.status.completion_time
            }
            
            # Determine overall status
            if status["failed"] > 0:
                status["status"] = "failed"
            elif status["succeeded"] > 0:
                status["status"] = "completed"
            elif status["active"] > 0:
                status["status"] = "running"
            else:
                status["status"] = "pending"
                
            return status
            
        except ApiException as e:
            logger.error(f"Failed to get job status: {e}")
            return {"status": "unknown", "error": str(e)}
    
    async def delete_job(self, job_name: str) -> bool:
        """Delete a Kubernetes job"""
        try:
            self.batch_api.delete_namespaced_job(
                name=job_name,
                namespace=self.namespace,
                body=client.V1DeleteOptions(
                    propagation_policy='Background'
                )
            )
            logger.info(f"Deleted Kubernetes job: {job_name}")
            return True
            
        except ApiException as e:
            logger.error(f"Failed to delete job: {e}")
            return False
    
    async def get_job_logs(self, job_name: str) -> str:
        """Get logs from a job's pods"""
        try:
            # Get pods for the job
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=f"job-name={job_name}"
            )
            
            if not pods.items:
                return "No pods found for job"
            
            # Get logs from the first pod
            pod_name = pods.items[0].metadata.name
            logs = self.core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace
            )
            
            return logs
            
        except ApiException as e:
            logger.error(f"Failed to get job logs: {e}")
            return f"Error getting logs: {str(e)}"
    
    async def create_deployment(self, name: str, image: str, replicas: int = 1,
                               port: int = None, env_vars: Dict[str, str] = None,
                               resources: Dict[str, Any] = None) -> str:
        """Create a Kubernetes deployment"""
        try:
            # Create container spec
            container = client.V1Container(
                name=name,
                image=image,
                env=[
                    client.V1EnvVar(name=k, value=v) 
                    for k, v in (env_vars or {}).items()
                ]
            )
            
            # Add port if specified
            if port:
                container.ports = [client.V1ContainerPort(container_port=port)]
            
            # Add resource requirements
            if resources:
                container.resources = client.V1ResourceRequirements(
                    requests=resources.get("requests", {}),
                    limits=resources.get("limits", {})
                )
            
            # Create deployment spec
            spec = client.V1DeploymentSpec(
                replicas=replicas,
                selector=client.V1LabelSelector(
                    match_labels={"app": name}
                ),
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels={"app": name}
                    ),
                    spec=client.V1PodSpec(
                        containers=[container]
                    )
                )
            )
            
            # Create deployment object
            deployment = client.V1Deployment(
                api_version="apps/v1",
                kind="Deployment",
                metadata=client.V1ObjectMeta(
                    name=name
                ),
                spec=spec
            )
            
            # Create deployment
            api_response = self.apps_api.create_namespaced_deployment(
                namespace=self.namespace,
                body=deployment
            )
            
            logger.info(f"Created Kubernetes deployment: {name}")
            return name
            
        except ApiException as e:
            logger.error(f"Failed to create deployment: {e}")
            raise
    
    async def scale_deployment(self, name: str, replicas: int) -> bool:
        """Scale a Kubernetes deployment"""
        try:
            # Get current deployment
            deployment = self.apps_api.read_namespaced_deployment(
                name=name,
                namespace=self.namespace
            )
            
            # Update replica count
            deployment.spec.replicas = replicas
            
            # Patch deployment
            self.apps_api.patch_namespaced_deployment(
                name=name,
                namespace=self.namespace,
                body=deployment
            )
            
            logger.info(f"Scaled deployment {name} to {replicas} replicas")
            return True
            
        except ApiException as e:
            logger.error(f"Failed to scale deployment: {e}")
            return False 