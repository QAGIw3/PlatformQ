from .base import BaseConnector
from typing import Optional, Dict, Any
from kubernetes import client, config
import uuid
import os

class FlightgearConnector(BaseConnector):
    """
    Orchestrates metadata extraction for FlightGear log files.
    """

    @property
    def connector_type(self) -> str:
        return "flightgear"

    def _get_k8s_job_definition(self, asset_id: str, file_uri: str) -> client.V1Job:
        """Creates the Kubernetes Job object for the FlightGear processor."""
        job_name = f"flightgear-processor-{asset_id[:8]}-{uuid.uuid4().hex[:6]}"
        processor_image = "platformq/flightgear-processor:latest"
        file_path_in_pod = f"/data/{file_uri.split('/')[-1]}"

        container = client.V1Container(
            name="flightgear-processor",
            image=processor_image,
            command=[file_path_in_pod],
            # TODO: Add resource requests/limits and volume mounts
        )

        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(name=job_name, labels={"app": "flightgear-processor"}),
            spec=client.V1PodSpec(restart_policy="Never", containers=[container]),
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(template=pod_template, backoff_limit=2, ttl_seconds_after_finished=600),
        )
        return job

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

            api_client = client.BatchV1Api()
            job_def = self._get_k8s_job_definition(asset_id, file_uri)
            
            namespace = "default"
            api_client.create_namespaced_job(body=job_def, namespace=namespace)

            print(f"[{self.connector_type}] Successfully created Job: {job_def.metadata.name}")
        except Exception as e:
            print(f"[{self.connector_type}] Error orchestrating Kubernetes Job: {e}") 