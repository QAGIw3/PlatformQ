"""
Connector API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, Body, Response
from fastapi.responses import JSONResponse

from app.core.auth import verify_token
from app.core.connector_manager import ConnectorManager
from app.core.config import settings

router = APIRouter(prefix="/api/v1/connectors", tags=["connectors"])


# Global connector manager instance (injected by main.py)
connector_manager = None

def get_connector_manager() -> ConnectorManager:
    """Dependency to get connector manager instance"""
    if connector_manager is None:
        raise RuntimeError("Connector manager not initialized")
    return connector_manager


@router.get("/", response_model=List[Dict[str, Any]])
async def list_connectors(
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    token_data: dict = Depends(verify_token)
):
    """List all configured connectors"""
    try:
        connectors = await connector_manager.list_connectors()
        return connectors
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=Dict[str, Any])
async def create_connector(
    connector_id: str,
    config: Dict[str, Any] = Body(...),
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    token_data: dict = Depends(verify_token)
):
    """Create a new connector"""
    try:
        # Add tenant_id to config
        config["tenant_id"] = token_data.get("tenant_id", "default")
        
        result = await connector_manager.add_connector(connector_id, config)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{connector_id}")
async def delete_connector(
    connector_id: str,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    token_data: dict = Depends(verify_token)
):
    """Delete a connector"""
    try:
        success = await connector_manager.remove_connector(connector_id)
        if not success:
            raise HTTPException(status_code=404, detail="Connector not found")
        return {"status": "success", "message": f"Connector {connector_id} deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{connector_id}/trigger")
async def trigger_connector(
    connector_id: str,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    token_data: dict = Depends(verify_token)
):
    """Manually trigger a connector sync"""
    try:
        job_id = await connector_manager.trigger_connector(connector_id)
        return {
            "status": "success",
            "connector_id": connector_id,
            "job_id": job_id
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/webhook/{webhook_type}")
async def receive_webhook(
    webhook_type: str,
    payload: Dict[str, Any] = Body(...),
    connector_manager: ConnectorManager = Depends(get_connector_manager)
):
    """Receive webhook data"""
    try:
        # Process webhook through connector manager
        result = await connector_manager.process_webhook(webhook_type, payload)
        
        return {
            "status": "success",
            "webhook_type": webhook_type,
            "data": result
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{connector_id}/status")
async def get_connector_status(
    connector_id: str,
    connector_manager: ConnectorManager = Depends(get_connector_manager),
    token_data: dict = Depends(verify_token)
):
    """Get connector status and last sync info"""
    try:
        connectors = await connector_manager.list_connectors()
        connector = next((c for c in connectors if c["connector_id"] == connector_id), None)
        
        if not connector:
            raise HTTPException(status_code=404, detail="Connector not found")
            
        # Add additional status info
        connector_obj = connector_manager.connectors.get(connector_id)
        if connector_obj:
            connector["last_sync_time"] = connector_obj.last_sync_time
            
        return connector
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 