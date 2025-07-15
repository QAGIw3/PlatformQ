import importlib
import pkgutil
from fastapi import FastAPI, Request, HTTPException
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Dict
import asyncio

from . import plugins

app = FastAPI(
    title="Connector Service",
    description="Hosts and manages data connectors for ingesting data into platformQ.",
    version="0.1.0",
)

scheduler = AsyncIOScheduler()
# This dictionary will hold our instantiated connector plugins
connector_plugins: Dict[str, plugins.base.BaseConnector] = {}

def discover_and_schedule_plugins():
    """
    Dynamically discovers all connector plugins, instantiates them,
    and schedules their `run` method if they have a schedule defined.
    """
    print("Discovering plugins...")
    # Dynamically import all modules in the 'plugins' package
    for (_, name, _) in pkgutil.iter_modules(plugins.__path__):
        if name != "base":
            plugin_module = importlib.import_module(f".{name}", plugins.__name__)
            
            # Find the connector class within the module
            for attribute_name in dir(plugin_module):
                attribute = getattr(plugin_module, attribute_name)
                if isinstance(attribute, type) and issubclass(attribute, plugins.base.BaseConnector) and attribute is not plugins.base.BaseConnector:
                    # Instantiate the connector (with dummy config for now)
                    connector_instance = attribute(config={})
                    connector_plugins[connector_instance.connector_type] = connector_instance
                    
                    # Schedule it if a schedule is provided
                    if connector_instance.schedule:
                        print(f"Scheduling '{connector_instance.connector_type}' with cron: {connector_instance.schedule}")
                        scheduler.add_job(
                            connector_instance.run,
                            'cron',
                            **{field: val for field, val in zip(['minute', 'hour', 'day', 'month', 'day_of_week'], connector_instance.schedule.split())}
                        )


@app.on_event("startup")
async def startup_event():
    """
    On startup, discover plugins and start the scheduler.
    """
    discover_and_schedule_plugins()
    scheduler.start()
    print("Connector Service started. Scheduler is running.")


@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()

@app.get("/")
def read_root():
    return {"message": "connector-service is running"}

@app.post("/webhooks/{connector_name}")
async def receive_webhook(connector_name: str, request: Request):
    """
    A generic endpoint to receive webhooks and trigger the corresponding connector.
    """
    if connector_name not in connector_plugins:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    
    connector = connector_plugins[connector_name]
    
    try:
        payload = await request.json()
        # Run the connector's logic in the background
        asyncio.create_task(connector.run(context={"payload": payload}))
        return {"message": "Webhook received and is being processed."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")

# TODO: Add endpoints for managing connectors (e.g., list, trigger, check status).
