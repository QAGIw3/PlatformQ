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

# --- Pulsar Consumer Setup ---
async def consume_asset_events():
    """
    A background task that listens for asset creation events and
    triggers the asset-linker WASM module.
    """
    print("Starting Pulsar consumer for asset events...")
    client = pulsar.Client('pulsar://localhost:6650') # Should be from config
    consumer = client.subscribe('non-persistent://public/default/digital_asset_created', 'functions-service-subscriber')
    
    # Pre-load the WASM module for efficiency
    try:
        # In a real app, this would be fetched from a registry or secure storage
        with open("examples/asset-linker-wasm/target/wasm32-unknown-unknown/release/asset_linker_wasm.wasm", "rb") as f:
            wasm_bytes = f.read()
        wasm_module = Module(wasm_engine, wasm_bytes)
    except FileNotFoundError:
        print("WARNING: asset_linker_wasm.wasm not found. The event consumer will not work.")
        print("Build it with: cd examples/asset-linker-wasm && cargo build --target wasm32-unknown-unknown --release")
        client.close()
        return

    while True:
        try:
            msg = await asyncio.to_thread(consumer.receive)
            print("Received asset creation event")
            
            # Here we would use an Avro schema to decode, but for PoC we'll assume JSON
            event_data = json.loads(msg.data().decode('utf-8'))
            
            # --- Execute WASM ---
            # This logic is very similar to the run_embedded_wasm endpoint
            linker = wasmtime.Linker(wasm_engine)
            instance = linker.instantiate(wasm_store, wasm_module)
            memory = instance.exports(wasm_store).get("memory")
            alloc_func = instance.exports(wasm_store).get("allocate")
            run_func = instance.exports(wasm_store).get("run")

            input_bytes = json.dumps(event_data).encode('utf-8')
            input_ptr = alloc_func(wasm_store, len(input_bytes))
            memory.write(wasm_store, input_bytes, input_ptr)
            
            output_ptr = run_func(wasm_store, input_ptr, len(input_bytes))
            
            # Read back the result
            result_bytes = []
            i = output_ptr
            while True:
                byte = memory.read(wasm_store, i, 1)
                if byte == b'\0': break
                result_bytes.append(byte)
                i += 1
            result_json = b"".join(result_bytes).decode('utf-8')
            api_call = json.loads(result_json)
            # --- End WASM Execution ---

            print("--- MOCKING OpenProject API Call ---")
            print(f"  METHOD: {api_call['method']}")
            print(f"  URL:    {api_call['url']}")
            print(f"  BODY:   {api_call['body']}")
            print("------------------------------------")

            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Error processing event: {e}")
            # In a real app, you would handle message redelivery (nack)

async def consume_simulation_events():
    """
    A background task that listens for simulation completion events,
    analyzes the logs, and triggers follow-up actions.
    """
    print("Starting Pulsar consumer for simulation events...")
    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('non-persistent://public/default/simulation-lifecycle-events', 'functions-service-sim-subscriber')

    try:
        with open("examples/log-analyzer-wasm/target/wasm32-unknown-unknown/release/log_analyzer_wasm.wasm", "rb") as f:
            log_analyzer_wasm_module = Module(wasm_engine, f.read())
    except FileNotFoundError:
        print("WARNING: log_analyzer_wasm.wasm not found. The simulation event consumer will not work.")
        client.close()
        return

    while True:
        try:
            msg = await asyncio.to_thread(consumer.receive)
            event_data = json.loads(msg.data().decode('utf-8'))
            print(f"Received simulation completion event for run: {event_data['run_id']}")

            # 1. MOCK: Fetch log content from the URI
            log_content = "This is a log file. It contains some INFO and some WARNINGS. Oh no, a FAILURE occurred."
            
            # 2. Execute WASM analyzer
            linker = wasmtime.Linker(wasm_engine)
            instance = linker.instantiate(wasm_store, log_analyzer_wasm_module)
            memory = instance.exports(wasm_store).get("memory")
            alloc_func = instance.exports(wasm_store).get("allocate")
            run_func = instance.exports(wasm_store).get("run")

            input_bytes = log_content.encode('utf-8')
            input_ptr = alloc_func(wasm_store, len(input_bytes))
            memory.write(wasm_store, input_bytes, input_ptr)
            
            output_ptr = run_func(wasm_store, input_ptr, len(input_bytes))
            result_bytes = []
            i = output_ptr
            while True:
                byte = memory.read(wasm_store, i, 1)
                if byte == b'\0': break
                result_bytes.append(byte)
                i += 1
            analysis_result = json.loads(b"".join(result_bytes).decode('utf-8'))
            
            print(f"  -> Analysis result: {analysis_result['status']}")

            # 3. If failure, orchestrate follow-up actions
            if analysis_result['status'] == 'FAILURE':
                print("  -> Failure detected. Orchestrating response...")
                # MOCK: Call connector-service to create a Digital Asset for the log
                print("     - MOCK: Triggering connector-service to create asset for log_uri:", event_data['log_uri'])
                # MOCK: Call OpenProject API to create an issue
                print("     - MOCK: Creating issue in OpenProject with summary:", analysis_result['summary'])
                # MOCK: Link asset to issue
                print("     - MOCK: Linking new asset to OpenProject issue.")

            consumer.acknowledge(msg)
        except Exception as e:
            print(f"Error processing simulation event: {e}")

# ---

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

@app.on_event("startup")
async def startup_event():
    # Start the background event consumers
    asyncio.create_task(consume_asset_events())
    asyncio.create_task(consume_simulation_events())

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
