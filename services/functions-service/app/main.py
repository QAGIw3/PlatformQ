from shared_lib.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
from kubernetes import client, config
import yaml
import base64
from wasmtime import Engine, Store, Module, Instance, Func, FuncType
import wasmtime
import asyncio
import pulsar
import json
import logging
import httpx
from .db import create_db_and_tables, get_db
from .api.endpoints import wasm_modules
from . import wasm_module_crud
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
from platformq_shared.events import ExecuteWasmFunction, FunctionExecutionCompleted
from pulsar.schema import AvroSchema
from sqlalchemy.orm import Session

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import connector_pb2, connector_pb2_grpc
import grpc
import os

# --- Configuration ---
# In a real app, these would come from environment variables or a config service
PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar:6650")
CONNECTOR_SERVICE_GRPC_TARGET = os.environ.get("CONNECTOR_SERVICE_GRPC_TARGET", "connector-service:50051")
# ---

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="functions-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud_placeholder,
    user_crud_dependency=get_user_crud_placeholder,
    password_verifier_dependency=get_password_verifier_placeholder,
)

# --- Wasmtime Engine Setup ---
# These are created once and shared across all requests for performance.
wasm_engine = Engine()
wasm_store = Store(wasm_engine)
# ---

# --- Pulsar Consumer for WASM Execution ---
async def consume_execution_requests(app):
    logger.info("Starting Pulsar consumer for WASM execution requests...")
    publisher = app.state.event_publisher
    
    # We create a new DB session for the consumer thread
    db_session = next(get_db())

    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern="persistent://platformq/.*/wasm-function-execution-requests",
        subscription_name="functions-service-execution-sub",
        schema=AvroSchema(ExecuteWasmFunction)
    )

    while True: # In a real app, we'd need a graceful shutdown mechanism
        try:
            msg = await asyncio.to_thread(consumer.receive)
            event = msg.value()
            logger.info(f"Received execution request for module '{event.wasm_module_id}' on asset '{event.asset_id}'")

            # 1. Get module metadata from the database
            module_meta = wasm_module_crud.get_wasm_module(db_session, event.wasm_module_id)
            if not module_meta:
                raise Exception(f"WASM module '{event.wasm_module_id}' not found in registry.")

            # 2. Load the WASM module from disk
            with open(module_meta.filepath, "rb") as f:
                wasm_bytes = f.read()
            wasm_module = Module(wasm_engine, wasm_bytes)

            # 3. Fetch the asset data from the URI
            async with httpx.AsyncClient() as http_client:
                response = await http_client.get(event.asset_uri, timeout=60.0)
                response.raise_for_status()
                asset_bytes = response.content

            # 4. Execute the WASM module
            # This follows the same ABI as the old `run_embedded_wasm` endpoint
            linker = wasmtime.Linker(wasm_engine)
            instance = linker.instantiate(wasm_store, wasm_module)
            memory = instance.exports(wasm_store).get("memory")
            alloc_func = instance.exports(wasm_store).get("allocate")
            run_func = instance.exports(wasm_store).get("run")

            input_ptr = alloc_func(wasm_store, len(asset_bytes))
            memory.write(wasm_store, asset_bytes, input_ptr)
            
            output_ptr = run_func(wasm_store, input_ptr, len(asset_bytes))
            
            result_bytes = []
            i = output_ptr
            while True:
                byte = memory.read(wasm_store, i, 1)
                if byte == b'\0': break
                result_bytes.append(byte)
                i += 1
            result_json = b"".join(result_bytes).decode('utf-8')
            results_map = json.loads(result_json)
            
            # 5. Publish completion event
            completion_event = FunctionExecutionCompleted(
                tenant_id=event.tenant_id,
                asset_id=event.asset_id,
                wasm_module_id=event.wasm_module_id,
                status="SUCCESS",
                results=results_map
            )
            publisher.publish(
                topic_base='function-execution-completed-events',
                tenant_id=event.tenant_id,
                schema_class=FunctionExecutionCompleted,
                data=completion_event
            )
            logger.info(f"Successfully executed module '{event.wasm_module_id}' and published results.")
            consumer.acknowledge(msg)

        except Exception as e:
            logger.error(f"Error processing execution request: {e}")
            # Also publish a failure event
            if 'event' in locals():
                failure_event = FunctionExecutionCompleted(
                    tenant_id=event.tenant_id,
                    asset_id=event.asset_id,
                    wasm_module_id=event.wasm_module_id,
                    status="FAILURE",
                    error_message=str(e)
                )
                publisher.publish(
                    topic_base='function-execution-completed-events',
                    tenant_id=event.tenant_id,
                    schema_class=FunctionExecutionCompleted,
                    data=failure_event
                )
            if 'msg' in locals():
                consumer.negative_acknowledge(msg)
    
    db_session.close()


# ---

# Include service-specific routers
app.include_router(endpoints.router, prefix="/api/v1", tags=["functions-service"])
app.include_router(wasm_modules.router, prefix="/api/v1", tags=["wasm-modules"])

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

@app.on_event("startup")
async def startup_event():
    # Create the SQLite database and tables
    create_db_and_tables()

    # Setup the event publisher
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    pulsar_url = settings.get("PULSAR_URL", "pulsar://pulsar:6650")
    publisher = EventPublisher(pulsar_url=pulsar_url)
    publisher.connect()
    app.state.event_publisher = publisher

    # Start the background event consumers
    asyncio.create_task(consume_execution_requests(app))

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()

class WasmRunRequest(BaseModel):
    wasm_module_b64: str = Field(..., description="The WASM module, encoded in Base64.")
    handler_function: str = "run"
    input_data: str

@app.post("/api/v1/functions/wasm/run-embedded")
async def run_embedded_wasm(
    run_request: WasmRunRequest,
):
    """
    Executes a WASM function within the service's embedded Wasmtime runtime.
    
    This is designed for short-lived, trusted computations. It expects the WASM
    module to export a function that takes two integers (a pointer to the input
    string and its length) and returns an integer (a pointer to the output string).
    """
    try:
        wasm_bytes = base64.b64decode(run_request.wasm_module_b64)
        module = Module(wasm_engine, wasm_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to decode or compile WASM module: {e}")

    # For the PoC, we'll implement a basic string-in, string-out ABI.
    # The WASM module needs to export memory and an 'allocate' function.
    linker = wasmtime.Linker(wasm_engine)
    instance = linker.instantiate(wasm_store, module) # Note: this will fail if memory is not exported

    memory = instance.exports(wasm_store).get("memory")
    if not memory:
        raise HTTPException(status_code=400, detail="WASM module must export 'memory'")

    alloc_func = instance.exports(wasm_store).get("allocate")
    if not alloc_func:
        raise HTTPException(status_code=400, detail="WASM module must export an 'allocate' function")

    # 1. Allocate memory in the WASM module and write the input string to it.
    input_bytes = run_request.input_data.encode('utf-8')
    input_ptr = alloc_func(wasm_store, len(input_bytes))
    memory.write(wasm_store, input_bytes, input_ptr)

    # 2. Get the handler function and call it.
    run_func = instance.exports(wasm_store).get(run_request.handler_function)
    if not run_func:
        raise HTTPException(status_code=400, detail=f"WASM module must export handler '{run_request.handler_function}'")
    
    output_ptr = run_func(wasm_store, input_ptr, len(input_bytes))

    # 3. Read the null-terminated result string back from memory.
    result_bytes = []
    i = output_ptr
    while True:
        byte = memory.read(wasm_store, i, 1)
        if byte == b'\0':
            break
        result_bytes.append(byte)
        i += 1
    
    result = b"".join(result_bytes).decode('utf-8')
    return {"result": result}


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
