from .base import BaseConnector
from typing import Optional, Dict, Any
from kubernetes import client, config
import uuid
import os

class BlenderConnector(BaseConnector):
    """
    Orchestrates the metadata extraction for .blend files.
    
    This connector does not process files directly. Instead, when triggered,
    it creates a Kubernetes Job that uses a dedicated 'blender-processor'
    image to perform the actual metadata extraction.
    """

    @property
    def connector_type(self) -> str:
        return "blender"

    def _get_k8s_job_definition(self, asset_id: str, file_uri: str) -> client.V1Job:
        """
        Creates the Kubernetes Job object for the Blender processor.
        
        Args:
            asset_id: The unique ID of the asset being processed.
            file_uri: The URI to the .blend file that needs to be processed.
                      This URI must be resolvable by the pod (e.g., a shared volume).
        """
        job_name = f"blender-processor-{asset_id[:8]}-{uuid.uuid4().hex[:6]}"
        
        # In a real setup, the blender-processor image would be in your enterprise registry.
        processor_image = "platformq/blender-processor:latest"
        
        # This assumes a shared volume (e.g., PVC) is mounted at /data
        # where the .blend file can be accessed.
        file_path_in_pod = f"/data/{file_uri.split('/')[-1]}"

        container = client.V1Container(
            name="blender-processor",
            image=processor_image,
            command=["extract_metadata.py", file_path_in_pod],
            # TODO: Add resource requests and limits
            # TODO: Configure volume mounts for accessing the file
        )

        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(name=job_name, labels={"app": "blender-processor"}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container]
                # TODO: Define volumes
            ),
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=pod_template,
                backoff_limit=2,
                ttl_seconds_after_finished=600 # Clean up finished jobs
            ),
        )
        return job

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic: create and launch the Kubernetes Job.
        """
        if not context or "file_uri" not in context:
            print(f"[{self.connector_type}] Skipping run: no file_uri in context.")
            return

        file_uri = context["file_uri"]
        asset_id = context.get("asset_id", uuid.uuid4().hex)

        print(f"[{self.connector_type}] Orchestrating processing for {file_uri}")

        try:
            # Configure k8s client (in-cluster or from kubeconfig)
            # In a real pod, this would use load_incluster_config()
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                config.load_incluster_config()
            else:
                config.load_kube_config()

            api_client = client.BatchV1Api()
            job_def = self._get_k8s_job_definition(asset_id, file_uri)
            
            namespace = "default" # Should be configurable
            api_client.create_namespaced_job(body=job_def, namespace=namespace)

            print(f"[{self.connector_type}] Successfully created Job: {job_def.metadata.name}")

            # TODO: Add logic to monitor the job, fetch logs on completion,
            # and call _create_digital_asset with the extracted metadata.

        except Exception as e:
            print(f"[{self.connector_type}] Error orchestrating Kubernetes Job: {e}") 