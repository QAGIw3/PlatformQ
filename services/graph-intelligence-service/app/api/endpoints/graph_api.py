from fastapi import APIRouter, Depends, HTTPException
from ....platformq_shared.api.deps import get_current_tenant_and_user
from ...services.graph_service import graph_service
from typing import Dict, Any, List

router = APIRouter()

@router.get("/assets/{asset_id}/lineage", response_model=Dict[str, Any])
async def get_asset_lineage(
    asset_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Get the complete downstream lineage for an asset. This finds all assets
    that are derived from the given asset, traversing the graph.
    """
    try:
        lineage = await graph_service.get_downstream_lineage(asset_id)
        if not lineage:
            raise HTTPException(status_code=404, detail="Asset not found or has no lineage.")
        return lineage
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/{user_id}/assets", response_model=List[Dict[str, Any]])
async def get_user_assets(
    user_id: str,
    context: dict = Depends(get_current_tenant_and_user),
):
    """
    Get all assets owned by a specific user.
    """
    # Simple authorization: ensure the request is for the currently authenticated user
    # In a real app, you might have admin roles that can view other users' assets.
    if user_id != str(context["user"].id):
        raise HTTPException(status_code=403, detail="Not authorized to view assets for this user.")

    try:
        assets = await graph_service.get_user_assets(user_id)
        return assets
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/internal/graph/lineage")
async def update_lineage_endpoint(
    lineage_data: Dict[str, Any],
):
    """
    Internal endpoint to update the graph with new lineage information.
    """
    try:
        await graph_service.update_lineage(lineage_data)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 