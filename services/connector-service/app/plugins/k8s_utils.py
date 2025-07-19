"""
Kubernetes utilities for connector plugins.

Provides common configurations for processor jobs including resource limits
and volume mounts.
"""

from kubernetes import client
from typing import Dict, Any, List, Optional


class K8sJobBuilder:
    """Builder for Kubernetes Job specifications with common configurations"""
    
    # Default resource configurations per processor type
    PROCESSOR_RESOURCES = {
        "blender": {
            "requests": {"cpu": "1", "memory": "4Gi"},
            "limits": {"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": "1"}
        },
        "freecad": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"}
        },
        "openfoam": {
            "requests": {"cpu": "2", "memory": "4Gi"},
            "limits": {"cpu": "8", "memory": "16Gi"}
        },
        "gimp": {
            "requests": {"cpu": "0.5", "memory": "1Gi"},
            "limits": {"cpu": "2", "memory": "2Gi"}
        },
        "audacity": {
            "requests": {"cpu": "0.5", "memory": "1Gi"},
            "limits": {"cpu": "1", "memory": "2Gi"}
        },
        "openshot": {
            "requests": {"cpu": "1", "memory": "2Gi"},
            "limits": {"cpu": "2", "memory": "4Gi"}
        },
        "flightgear": {
            "requests": {"cpu": "0.5", "memory": "512Mi"},
            "limits": {"cpu": "1", "memory": "1Gi"}
        }
    }
    
    @staticmethod
    def create_processor_job(
        processor_type: str,
        job_name: str,
        asset_id: str,
        file_uri: str,
        processor_image: str,
        command: Optional[List[str]] = None,
        args: Optional[List[str]] = None,
        namespace: str = "default",
        ttl_seconds: int = 600
    ) -> client.V1Job:
        """
        Create a Kubernetes Job for a processor with proper resource limits and volume mounts.
        
        Args:
            processor_type: Type of processor (e.g., 'blender', 'freecad')
            job_name: Name for the Kubernetes job
            asset_id: ID of the asset being processed
            file_uri: URI of the file to process
            processor_image: Docker image for the processor
            command: Optional command override
            args: Optional arguments
            namespace: Kubernetes namespace
            ttl_seconds: Time to live after job completion
            
        Returns:
            Configured Kubernetes Job object
        """
        # Get resource configuration for processor type
        resources = K8sJobBuilder.PROCESSOR_RESOURCES.get(
            processor_type,
            {
                "requests": {"cpu": "0.5", "memory": "512Mi"},
                "limits": {"cpu": "1", "memory": "1Gi"}
            }
        )
        
        # Extract filename from URI
        filename = file_uri.split('/')[-1]
        file_path_in_pod = f"/data/input/{filename}"
        
        # Default command if not provided
        if command is None:
            command = ["/app/processor.sh", file_path_in_pod]
        
        # Configure container
        container = client.V1Container(
            name=f"{processor_type}-processor",
            image=processor_image,
            command=command,
            args=args,
            resources=client.V1ResourceRequirements(
                requests=resources["requests"],
                limits=resources["limits"]
            ),
            volume_mounts=[
                # Input data volume
                client.V1VolumeMount(
                    name="input-data",
                    mount_path="/data/input",
                    read_only=True
                ),
                # Output/working directory
                client.V1VolumeMount(
                    name="output-data",
                    mount_path="/data/output"
                ),
                # Shared cache for libraries/models
                client.V1VolumeMount(
                    name="cache",
                    mount_path="/cache"
                )
            ],
            env=[
                client.V1EnvVar(name="ASSET_ID", value=asset_id),
                client.V1EnvVar(name="INPUT_FILE", value=file_path_in_pod),
                client.V1EnvVar(name="OUTPUT_DIR", value="/data/output"),
                client.V1EnvVar(name="PROCESSOR_TYPE", value=processor_type),
                client.V1EnvVar(name="DIGITAL_ASSET_SERVICE_URL", value="http://digital-asset-service:8000"),
                client.V1EnvVar(name="STORAGE_SERVICE_URL", value="http://storage-service:8000")
            ],
            # Security context
            security_context=client.V1SecurityContext(
                run_as_non_root=True,
                run_as_user=1000,
                read_only_root_filesystem=False,
                allow_privilege_escalation=False
            )
        )
        
        # Configure volumes
        volumes = [
            # Input data from PVC
            client.V1Volume(
                name="input-data",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name="processor-input-pvc"
                )
            ),
            # Output data to PVC
            client.V1Volume(
                name="output-data",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name="processor-output-pvc"
                )
            ),
            # Shared cache
            client.V1Volume(
                name="cache",
                empty_dir=client.V1EmptyDirVolumeSource(
                    medium="Memory",
                    size_limit="1Gi"
                )
            )
        ]
        
        # Pod template
        pod_template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                name=job_name,
                labels={
                    "app": f"{processor_type}-processor",
                    "asset-id": asset_id[:8],
                    "processor-type": processor_type
                },
                annotations={
                    "prometheus.io/scrape": "true",
                    "prometheus.io/port": "9090"
                }
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                volumes=volumes,
                # Node selector for GPU if needed
                node_selector={
                    "node-type": "gpu-enabled"
                } if "gpu" in resources.get("limits", {}) else None,
                # Tolerations for spot instances
                tolerations=[
                    client.V1Toleration(
                        key="spot-instance",
                        operator="Equal",
                        value="true",
                        effect="NoSchedule"
                    )
                ],
                # Service account for accessing APIs
                service_account_name="processor-service-account"
            )
        )
        
        # Job specification
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(
                name=job_name,
                labels={
                    "app": f"{processor_type}-processor",
                    "managed-by": "connector-service"
                }
            ),
            spec=client.V1JobSpec(
                template=pod_template,
                backoff_limit=2,
                ttl_seconds_after_finished=ttl_seconds,
                active_deadline_seconds=3600  # 1 hour timeout
            )
        )
        
        return job
    
    @staticmethod
    def create_pvcs_if_not_exist(namespace: str = "default"):
        """
        Create the required PVCs if they don't exist.
        Should be called during service initialization.
        """
        from kubernetes import config as k8s_config
        import os
        
        # Load config
        if os.getenv("KUBERNETES_SERVICE_HOST"):
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()
            
        v1 = client.CoreV1Api()
        
        pvcs = [
            {
                "name": "processor-input-pvc",
                "size": "100Gi",
                "access_mode": "ReadOnlyMany"
            },
            {
                "name": "processor-output-pvc", 
                "size": "100Gi",
                "access_mode": "ReadWriteMany"
            }
        ]
        
        for pvc_config in pvcs:
            try:
                # Check if PVC exists
                v1.read_namespaced_persistent_volume_claim(
                    name=pvc_config["name"],
                    namespace=namespace
                )
            except client.ApiException as e:
                if e.status == 404:
                    # Create PVC
                    pvc = client.V1PersistentVolumeClaim(
                        metadata=client.V1ObjectMeta(name=pvc_config["name"]),
                        spec=client.V1PersistentVolumeClaimSpec(
                            access_modes=[pvc_config["access_mode"]],
                            resources=client.V1ResourceRequirements(
                                requests={"storage": pvc_config["size"]}
                            ),
                            storage_class_name="fast-ssd"
                        )
                    )
                    v1.create_namespaced_persistent_volume_claim(
                        namespace=namespace,
                        body=pvc
                    )
                    print(f"Created PVC: {pvc_config['name']}")
                else:
                    raise 