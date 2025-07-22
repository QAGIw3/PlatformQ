from .base import BaseConnector
from .k8s_utils import K8sJobBuilder
from typing import Optional, Dict, Any
from kubernetes import client, config
import uuid
import os

class BlenderConnector(BaseConnector):
    """
    Orchestrates the metadata extraction for .blend files.
    
    This connector demonstrates the "Processor Job" pattern. It does not process
    files directly, which would bloat the service with a large dependency like
    Blender. Instead, when triggered, it creates a dedicated, short-lived
    Kubernetes Job that uses a specialized 'blender-processor' Docker image.
    This keeps the main connector service lightweight and scalable.
    """

    @property
    def connector_type(self) -> str:
        # A unique string identifier for this connector.
        return "blender"

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic for the Blender connector.
        
        This method is typically triggered by an event such as a new .blend file
        being uploaded to the platform's storage. It orchestrates the processing
        of the file by creating a Kubernetes Job.
        """
        if not context or "file_uri" not in context:
            print(f"[{self.connector_type}] Skipping run: no file_uri in context.")
            return

        file_uri = context["file_uri"]
        asset_id = context.get("asset_id", uuid.uuid4().hex)

        print(f"[{self.connector_type}] Orchestrating processing for {file_uri}")

        try:
            # Load Kubernetes configuration. This will use in-cluster config
            # when running inside Kubernetes, or your local kubeconfig when
            # running locally for development.
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                config.load_incluster_config()
            else:
                config.load_kube_config()

            # Use K8sJobBuilder to create job with proper resource limits and volumes
            job_name = f"blender-processor-{asset_id[:8]}-{uuid.uuid4().hex[:6]}"
            job_def = K8sJobBuilder.create_processor_job(
                processor_type="blender",
                job_name=job_name,
                asset_id=asset_id,
                file_uri=file_uri,
                processor_image=self.config.get("processor_image", "platformq/blender-processor:latest"),
                command=None,  # Use default command
                namespace=self.config.get("namespace", "default")
            )
            
            # Create the Job
            api_client = client.BatchV1Api()
            namespace = self.config.get("namespace", "default")
            api_client.create_namespaced_job(body=job_def, namespace=namespace)

            print(f"[{self.connector_type}] Successfully created Job: {job_name}")

            # Optionally, you could monitor the Job's progress here, but
            # typically, the processor itself would update the Digital Asset
            # with extracted metadata upon completion.
            
            # Create initial asset record
            await self._create_digital_asset({
                "asset_name": file_uri.split('/')[-1],
                "asset_type": "BLENDER_MODEL",
                "source_tool": "blender",
                "status": "PROCESSING",
                "metadata": {
                    "processor_job": job_name,
                    "file_uri": file_uri
                }
            })

        except Exception as e:
            print(f"[{self.connector_type}] Error orchestrating Kubernetes Job: {e}")
            raise 