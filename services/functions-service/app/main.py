from platformq.shared.base_service import create_base_app
from fastapi import Depends, HTTPException
from pydantic import BaseModel, Field
import yaml
import base64
import asyncio
import logging
import httpx
from .db import create_db_and_tables, get_db
from .api.endpoints import functions
from .model_serving import router as model_serving_router
from platformq_shared.event_publisher import EventPublisher
from .core.config import settings
from platformq_shared.events import FunctionExecutionCompleted
from pulsar.schema import AvroSchema
from sqlalchemy.orm import Session
from .pulsar_knative_bridge import run_pulsar_knative_bridge

# Assuming the generate_grpc.sh script has been run
from .grpc_generated import connector_pb2, connector_pb2_grpc
import grpc
import os
from wasmtime import Store, Module, Instance
from typing import Dict
import json

# ---

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = create_base_app(
    service_name="functions-service",
    db_session_dependency=get_db,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None,
)

# Include service-specific routers
app.include_router(functions.router, prefix="/api/v1", tags=["functions"])
app.include_router(model_serving_router, prefix="/api/v1/models", tags=["model-serving"])


@app.on_event("startup")
async def startup_event():
    # Create the SQLite database and tables
    create_db_and_tables()

    # Setup the event publisher
    pulsar_url = settings.pulsar_url
    publisher = EventPublisher(pulsar_url=pulsar_url)
    publisher.connect()
    app.state.event_publisher = publisher

    # Start the background event consumers
    asyncio.create_task(run_pulsar_knative_bridge())

@app.on_event("shutdown")
def shutdown_event():
    if app.state.event_publisher:
        app.state.event_publisher.close()

# Service-specific root endpoint
@app.get("/")
def read_root():
    return {"message": "functions-service is running"}

@app.post('/api/v1/simulate-preview')
async def simulate_preview(wasm_module: bytes, input_data: Dict):
    store = Store()
    module = Module(store.engine, wasm_module)
    instance = Instance(store, module, [])
    input_ptr = instance.exports['alloc'](len(json.dumps(input_data)))
    # Write input to memory
    result_ptr = instance.exports['preview'](input_ptr)
    # Read result
    return {'preview': result}

@app.post('/api/v1/transform-asset')
async def transform_asset(asset_id: str, transform_type: str):
    # WASM transform
    return {'transformed': True}
