from .base import BaseConnector
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

    def _get_k8s_job_definition(self, asset_id: str, file_uri: str) -> client.V1Job:
        """
        Dynamically creates the Kubernetes Job object for the Blender processor.
        
        Args:
            asset_id: The unique ID of the asset being processed. This is used
                      to name and label the Job for traceability.
            file_uri: The URI to the .blend file that needs to be processed.
                      This URI must be resolvable by the processor pod, which
                      typically means it points to a location on a shared
                      PersistentVolumeClaim (PVC) that will be mounted into the pod.
        """
        # We create a unique name for the Job to avoid collisions.
        job_name = f"blender-processor-{asset_id[:8]}-{uuid.uuid4().hex[:6]}"
        
        # In a production setup, this image would be hosted in a private container
        # registry (e.g., GCR, ECR, Harbor).
        processor_image = "platformq/blender-processor:latest"
        
        # This is the path where the shared volume will be mounted inside the
        # processor pod. The file_uri is appended to find the specific file.
        file_path_in_pod = f"/data/{file_uri.split('/')[-1]}"

        # This defines the container that will run inside our Job's Pod.
        container = client.V1Container(
            name="blender-processor",
            image=processor_image,
            # The command and arguments passed to the container's entrypoint.
            # The entrypoint for our image is 'blender --background --python'.
            # This command appends our script and the file path.
            command=["/app/extract_metadata.py", file_path_in_pod],
            # In production, it's critical to set resource requests and limits
            # to ensure stable cluster performance.
            # resources=client.V1ResourceRequirements(
            #     requests={"cpu": "500m", "memory": "1Gi"},
            #     limits={"cpu": "1", "memory": "2Gi"},
            # ),
            # This is where we would define the volume and volume mount for the
            # shared data PVC.
            # volume_mounts=[client.V1VolumeMount(name="shared-data", mount_path="/data")]
        )

        # The Pod template defines the Pod that will be created by the Job.
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(name=job_name, labels={"app": "blender-processor"}),
            spec=client.V1PodSpec(
                # 'Never' ensures that the pod does not restart on failure, allowing
                # us to inspect the logs for the root cause.
                restart_policy="Never",
                containers=[container]
                # volumes=[client.V1Volume(name="shared-data", persistent_volume_claim=...)]
            ),
        )

        # The Job specification itself.
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=pod_template,
                # 'backoff_limit' controls how many times the Job will be retried on failure.
                backoff_limit=2,
                # 'ttl_seconds_after_finished' automatically cleans up the Job and its
                # Pod after it completes, preventing clutter in the cluster.
                ttl_seconds_after_finished=600
            ),
        )
        return job

    async def run(self, context: Optional[Dict[str, Any]] = None):
        """
        The core logic: create and launch the Kubernetes Job.
        This method is called by the service scheduler or a manual trigger.
        """
        if not context or "file_uri" not in context:
            print(f"[{self.connector_type}] Skipping run: no file_uri in context.")
            return

        file_uri = context["file_uri"]
        asset_id = context.get("asset_id", uuid.uuid4().hex)

        print(f"[{self.connector_type}] Orchestrating processing for {file_uri}")

        try:
            # The kubernetes client library can be configured in two ways:
            # 1. load_incluster_config(): When running inside a pod in the cluster.
            #    It automatically uses the pod's service account. This is the
            #    production method.
            # 2. load_kube_config(): When running locally for development. It uses
            #    the standard ~/.kube/config file.
            if os.getenv("KUBERNETES_SERVICE_HOST"):
                config.load_incluster_config()
            else:
                config.load_kube_config()

            api_client = client.BatchV1Api()
            job_def = self._get_k8s_job_definition(asset_id, file_uri)
            
            # The namespace where the job will be created. This should be
            # configurable in a real system, likely per-tenant.
            namespace = "default"
            api_client.create_namespaced_job(body=job_def, namespace=namespace)

            print(f"[{self.connector_type}] Successfully created Job: {job_def.metadata.name}")

            # In a full implementation, the logic to monitor the job's status
            # would go here. This could involve:
            # 1. Periodically polling the Job's status.
            # 2. Using a Kubernetes watch to get real-time status updates.
            # 3. On success, fetching the logs from the completed pod.
            # 4. Parsing the JSON metadata from the logs.
            # 5. Calling self._create_digital_asset() with the result.

        except Exception as e:
            print(f"[{self.connector_type}] Error orchestrating Kubernetes Job: {e}") 