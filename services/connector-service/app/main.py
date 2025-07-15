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

# The scheduler is used to run connectors that have a cron schedule defined.
scheduler = AsyncIOScheduler()

# This dictionary acts as a plugin registry, holding instantiated connector plugins,
# mapping their unique 'connector_type' string to the class instance.
connector_plugins: Dict[str, plugins.base.BaseConnector] = {}

def discover_and_schedule_plugins():
    """
    Dynamically discovers, imports, and instantiates all connector plugins
    from the 'plugins' directory. This is the core of the plugin architecture.
    
    If a connector has a 'schedule' property, it is automatically added to
    the APScheduler job queue.
    """
    print("Discovering plugins...")
    # pkgutil.iter_modules is used to find all modules in the plugins package.
    # This is more robust than walking the filesystem as it works with different
    # packaging formats.
    for (_, name, _) in pkgutil.iter_modules(plugins.__path__):
        # We skip the 'base' module as it only contains the abstract class.
        if name != "base":
            plugin_module = importlib.import_module(f".{name}", plugins.__name__)
            
            # We iterate through the attributes of the loaded module to find
            # the class that inherits from our BaseConnector.
            for attribute_name in dir(plugin_module):
                attribute = getattr(plugin_module, attribute_name)
                if isinstance(attribute, type) and issubclass(attribute, plugins.base.BaseConnector) and attribute is not plugins.base.BaseConnector:
                    # Instantiate the connector (with dummy config for now)
                    # and add it to our registry.
                    # In a production system, a config object would be passed here.
                    connector_instance = attribute(config={})
                    connector_plugins[connector_instance.connector_type] = connector_instance
                    
                    # If a schedule is provided (e.g., '0 * * * *'), parse the cron
                    # string and add the connector's 'run' method to the scheduler.
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
    On startup, run the plugin discovery and start the global scheduler.
    """
    discover_and_schedule_plugins()
    scheduler.start()
    print("Connector Service started. Scheduler is running.")


@app.on_event("shutdown")
async def shutdown_event():
    """
    Ensure the scheduler is shut down cleanly when the service stops.
    """
    scheduler.shutdown()

@app.get("/")
def read_root():
    return {"message": "connector-service is running"}

@app.get("/connectors")
async def list_connectors():
    """
    An administrative endpoint to list all discovered and loaded connectors.
    """
    return {"connectors": list(connector_plugins.keys())}

@app.post("/connectors/{connector_name}/run")
async def trigger_connector_run(connector_name: str, request: Request):
    """
    An administrative endpoint to manually trigger a connector's run method.
    This is useful for testing, debugging, or reprocessing data.
    The request body is passed as the 'context' to the connector.
    """
    if connector_name not in connector_plugins:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    
    connector = connector_plugins[connector_name]
    
    try:
        context_payload = await request.json()
        # The connector's run method is executed in the background to avoid
        # blocking the HTTP request.
        asyncio.create_task(connector.run(context=context_payload))
        return {
            "message": f"Manual run for connector '{connector_name}' triggered successfully.",
            "context": context_payload
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload for context: {e}")


@app.post("/webhooks/{connector_name}")
async def receive_webhook(connector_name: str, request: Request):
    """
    A generic endpoint to receive real-time events via webhooks and trigger
    the corresponding connector. The entire request body is passed as the
    'payload' in the context dictionary.
    """
    if connector_name not in connector_plugins:
        raise HTTPException(status_code=404, detail=f"Connector '{connector_name}' not found.")
    
    connector = connector_plugins[connector_name]
    
    try:
        payload = await request.json()
        # The connector's logic is run in the background to immediately
        # return a 200 OK to the webhook source.
        asyncio.create_task(connector.run(context={"payload": payload}))
        return {"message": "Webhook received and is being processed."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON payload: {e}")

# TODO: Add endpoints for managing connectors (e.g., list, trigger, check status).
