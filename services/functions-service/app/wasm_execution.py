from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import base64
from wasmtime import Module
import wasmtime

from .wasm_runtime import wasm_engine, wasm_store

router = APIRouter()


class WasmRunRequest(BaseModel):
    wasm_module_b64: str = Field(..., description="The WASM module, encoded in Base64.")
    handler_function: str = "run"
    input_data: str


@router.post("/functions/wasm/run-embedded")
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