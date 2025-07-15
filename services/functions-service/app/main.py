from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel
from kubernetes import client, config
import yaml

app = create_base_app(
    service_name="functions-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["functions-service"])

# Load the in-cluster Kubernetes configuration
config.load_incluster_config()
api = client.CustomObjectsApi()

class FunctionDeployRequest(BaseModel):
    function_name: str
    code: str # The user's Python code as a string
    # In a real app, this would be a git URL or a zip file

@app.post("/api/v1/functions", status_code=201)
def deploy_function(
    deploy_request: FunctionDeployRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Deploys user code as a Knative Service.
    This is a simplified example. A real implementation would:
    1. Use a tool like Kaniko to build a container image from the code.
    2. Push the image to a container registry.
    3. Apply the Knative manifest with the new image tag.
    """
    tenant_id = context["tenant_id"]
    
    # For this example, we'll use a pre-built "python-runtime" image
    # and inject the user's code via a ConfigMap.
    
    # 1. Create a ConfigMap with the user's code
    code_config_map = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": f"{deploy_request.function_name}-code"},
        "data": {"user_function.py": deploy_request.code},
    }
    
    # 2. Define the Knative Service manifest
    knative_service = {
        "apiVersion": "serving.knative.dev/v1",
        "kind": "Service",
        "metadata": {"name": deploy_request.function_name, "namespace": tenant_id},
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "image": "gcr.io/your-repo/python-runtime:latest", # A generic Python runtime
                            "volumeMounts": [{"name": "code", "mountPath": "/app/src"}],
                        }
                    ],
                    "volumes": [{"name": "code", "configMap": {"name": f"{deploy_request.function_name}-code"}}],
                }
            }
        },
    }

    try:
        # Apply the ConfigMap
        api.create_namespaced_custom_object(
            group="v1", version="", namespace=tenant_id, plural="configmaps", body=code_config_map
        )
        # Apply the Knative Service
        api.create_namespaced_custom_object(
            group="serving.knative.dev", version="v1", namespace=tenant_id, plural="services", body=knative_service
        )
    except client.ApiException as e:
        raise HTTPException(status_code=e.status, detail=e.body)

    return {"message": "Function deployment initiated."}

class WasmFunctionDeployRequest(BaseModel):
    function_name: str
    image_url: str # URL to the OCI registry containing the .wasm module

@app.post("/api/v1/wasm-functions", status_code=201)
def deploy_wasm_function(
    deploy_request: WasmFunctionDeployRequest,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Deploys a pre-compiled WASM module as a Knative Service.
    """
    tenant_id = context["tenant_id"]
    
    knative_service = {
        "apiVersion": "serving.knative.dev/v1",
        "kind": "Service",
        "metadata": {
            "name": deploy_request.function_name,
            "namespace": tenant_id,
        },
        "spec": {
            "template": {
                "metadata": {
                    # This annotation tells containerd to use the WasmEdge runtime
                    "annotations": {"module.wasm.image/variant": "compat-smart"}
                },
                "spec": {
                    "runtimeClassName": "wasmedge",
                    "tolerations": [
                        {"key": "workload.gke.io/type", "operator": "Equal", "value": "wasm", "effect": "NoSchedule"}
                    ],
                    "containers": [
                        {
                            "image": deploy_request.image_url,
                            "command": ["/main.wasm"], # Entrypoint inside the WASM module
                        }
                    ],
                },
            }
        },
    }

    try:
        api.create_namespaced_custom_object(
            group="serving.knative.dev", version="v1", namespace=tenant_id, plural="services", body=knative_service
        )
    except client.ApiException as e:
        raise HTTPException(status_code=e.status, detail=e.body)

    return {"message": "WASM function deployment initiated."}

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "functions-service is running"}
