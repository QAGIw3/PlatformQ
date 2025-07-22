import asyncio
import json
import logging
import os
import httpx
import pulsar
from pulsar.schema import AvroSchema
import wasmtime
from wasmtime import Module

from platformq_shared.events import ExecuteWasmFunction, FunctionExecutionCompleted
from .crud import wasm_module_crud
from .db import get_db
from .wasm_runtime import wasm_engine, wasm_store

PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar:6650")

logger = logging.getLogger(__name__)

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