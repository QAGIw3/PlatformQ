"""
Processor Job Operator for Apache Airflow

This operator launches Kubernetes jobs for file processing using
PlatformQ's processor containers (Blender, FreeCAD, GIMP, etc.).
"""

import os
import uuid
import time
from typing import Dict, Any, Optional, List
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from kubernetes import client, config
from kubernetes.client.rest import ApiException


class ProcessorJobOperator(BaseOperator):
    """
    Launches Kubernetes jobs for file processing
    
    :param processor_type: Type of processor (e.g., 'blender', 'freecad', 'gimp')
    :param asset_id: ID of the asset to process
    :param asset_uri: URI to the asset file (S3, MinIO, etc.)
    :param namespace: Kubernetes namespace for the job
    :param image_registry: Container image registry
    :param image_tag: Container image tag
    :param resources: Resource requirements (CPU/memory)
    :param volume_mounts: Additional volume mounts
    :param env_vars: Additional environment variables
    :param ttl_seconds_after_finished: Time to live after job completion
    """
    
    template_fields = ['processor_type', 'asset_id', 'asset_uri']
    ui_color = '#F38181'
    
    @apply_defaults
    def __init__(
        self,
        processor_type: str,
        asset_id: str,
        asset_uri: str,
        namespace: str = 'default',
        image_registry: str = 'platformq',
        image_tag: str = 'latest',
        resources: Optional[Dict[str, Dict[str, str]]] = None,
        volume_mounts: Optional[List[Dict[str, Any]]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        ttl_seconds_after_finished: int = 600,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.processor_type = processor_type
        self.asset_id = asset_id
        self.asset_uri = asset_uri
        self.namespace = namespace
        self.image_registry = image_registry
        self.image_tag = image_tag
        self.resources = resources or {
            'requests': {'cpu': '100m', 'memory': '256Mi'},
            'limits': {'cpu': '2', 'memory': '2Gi'}
        }
        self.volume_mounts = volume_mounts or []
        self.env_vars = env_vars or {}
        self.ttl_seconds_after_finished = ttl_seconds_after_finished
    
    def _get_k8s_client(self):
        """Get Kubernetes client"""
        try:
            # Try in-cluster config first (when running in K8s)
            config.load_incluster_config()
        except:
            # Fall back to kubeconfig file
            config.load_kube_config()
        
        return client.BatchV1Api()
    
    def _create_job_object(self) -> client.V1Job:
        """Create Kubernetes Job object"""
        job_name = f"{self.processor_type}-processor-{self.asset_id[:8]}-{uuid.uuid4().hex[:6]}"
        processor_image = f"{self.image_registry}/{self.processor_type}-processor:{self.image_tag}"
        
        # Container environment variables
        env = [
            client.V1EnvVar(name='ASSET_ID', value=self.asset_id),
            client.V1EnvVar(name='ASSET_URI', value=self.asset_uri),
            client.V1EnvVar(name='PLATFORMQ_PULSAR_URL', value=os.getenv('PLATFORMQ_PULSAR_URL', 'pulsar://pulsar:6650'))
        ]
        
        # Add custom environment variables
        for key, value in self.env_vars.items():
            env.append(client.V1EnvVar(name=key, value=value))
        
        # Container specification
        container = client.V1Container(
            name=f"{self.processor_type}-processor",
            image=processor_image,
            command=[self.asset_uri],  # Most processors expect file path as argument
            env=env,
            resources=client.V1ResourceRequirements(
                requests=self.resources.get('requests', {}),
                limits=self.resources.get('limits', {})
            ),
            volume_mounts=self.volume_mounts
        )
        
        # Pod template
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels={
                    'app': f'{self.processor_type}-processor',
                    'asset_id': self.asset_id[:8],
                    'job_type': 'processor'
                }
            ),
            spec=client.V1PodSpec(
                restart_policy='Never',
                containers=[container]
            )
        )
        
        # Job specification
        job_spec = client.V1JobSpec(
            template=pod_template,
            backoff_limit=2,
            ttl_seconds_after_finished=self.ttl_seconds_after_finished
        )
        
        # Job object
        job = client.V1Job(
            api_version='batch/v1',
            kind='Job',
            metadata=client.V1ObjectMeta(name=job_name),
            spec=job_spec
        )
        
        return job
    
    def execute(self, context) -> Dict[str, Any]:
        """Execute the operator to create and monitor Kubernetes job"""
        k8s_client = self._get_k8s_client()
        job = self._create_job_object()
        
        self.log.info(f"Creating Kubernetes job: {job.metadata.name}")
        
        try:
            # Create the job
            api_response = k8s_client.create_namespaced_job(
                body=job,
                namespace=self.namespace
            )
            
            job_name = api_response.metadata.name
            self.log.info(f"Job created successfully: {job_name}")
            
            # Monitor job status
            start_time = time.time()
            timeout = 3600  # 1 hour timeout
            
            while True:
                # Get job status
                job_status = k8s_client.read_namespaced_job_status(
                    name=job_name,
                    namespace=self.namespace
                )
                
                # Check if job completed
                if job_status.status.succeeded:
                    self.log.info(f"Job {job_name} completed successfully")
                    
                    # Get job output (would need to read from pod logs or output location)
                    result = {
                        'job_name': job_name,
                        'status': 'succeeded',
                        'asset_id': self.asset_id,
                        'processor_type': self.processor_type
                    }
                    
                    # Store result in XCom
                    context['ti'].xcom_push(key='job_result', value=result)
                    
                    return result
                
                # Check if job failed
                if job_status.status.failed:
                    self.log.error(f"Job {job_name} failed")
                    raise Exception(f"Kubernetes job {job_name} failed")
                
                # Check timeout
                if time.time() - start_time > timeout:
                    self.log.error(f"Job {job_name} timed out")
                    # Try to delete the job
                    try:
                        k8s_client.delete_namespaced_job(
                            name=job_name,
                            namespace=self.namespace,
                            propagation_policy='Background'
                        )
                    except:
                        pass
                    raise Exception(f"Kubernetes job {job_name} timed out after {timeout} seconds")
                
                # Wait before checking again
                time.sleep(10)
                
        except ApiException as e:
            self.log.error(f"Exception when calling Kubernetes API: {e}")
            raise
    
    def on_kill(self):
        """Cleanup when task is killed"""
        self.log.info("Task killed, job cleanup should be handled by TTL") 