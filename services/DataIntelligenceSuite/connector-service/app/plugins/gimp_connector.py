from .base import BaseConnector
from .k8s_utils import K8sJobBuilder
from typing import Optional, Dict, Any
from kubernetes import client, config
import uuid
import os

class GimpConnector(BaseConnector):
    """
    Orchestrates the metadata extraction for GIMP-compatible image files.
    
    This follows the same "Processor Job" pattern as the Blender connector,
    launching a Kubernetes Job to perform the actual extraction.
    """

    @property
    def connector_type(self) -> str:
        return "gimp"

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """The core logic: create and launch the Kubernetes Job."""
        if not context or "file_uri" not in context:
            print(f"[{self.connector_type}] Skipping run: no file_uri in context.")
            return

        file_uri = context["file_uri"]
        asset_id = context.get("asset_id", uuid.uuid4().hex)

        print(f"[{self.connector_type}] Orchestrating processing for {file_uri}")

        try:
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                config.load_incluster_config()
            else:
                config.load_kube_config()

            # Use K8sJobBuilder to create job with proper resource limits and volumes
            job_name = f"gimp-processor-{asset_id[:8]}-{uuid.uuid4().hex[:6]}"
            
            # GIMP requires special command format
            filename = file_uri.split('/')[-1]
            file_path_in_pod = f"/data/input/{filename}"
            
            job_def = K8sJobBuilder.create_processor_job(
                processor_type="gimp",
                job_name=job_name,
                asset_id=asset_id,
                file_uri=file_uri,
                processor_image=self.config.get("processor_image", "platformq/gimp-processor:latest"),
                command=["python", "/app/extract_metadata.py", file_path_in_pod],
                namespace=self.config.get("namespace", "default")
            )
            
            api_client = client.BatchV1Api()
            namespace = self.config.get("namespace", "default")
            api_client.create_namespaced_job(body=job_def, namespace=namespace)

            print(f"[{self.connector_type}] Successfully created Job: {job_name}")
            
            # Create initial asset record
            await self._create_digital_asset({
                "asset_name": file_uri.split('/')[-1],
                "asset_type": "IMAGE",
                "source_tool": "gimp",
                "status": "PROCESSING",
                "metadata": {
                    "processor_job": job_name,
                    "file_uri": file_uri,
                    "media_type": "image"
                }
            })
            
        except Exception as e:
            print(f"[{self.connector_type}] Error orchestrating Kubernetes Job: {e}")
            raise 