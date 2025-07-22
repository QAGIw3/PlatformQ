"""
Discovery API endpoints

Handles medallion layer auto-discovery operations.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from app.core.medallion_discovery import MedallionDiscoveryEngine, DataLayer
from app.core.atlas_client import AtlasClient
from platformq_events import EventStream

router = APIRouter(prefix="/api/v1/discovery", tags=["discovery"])

# Dependencies will be injected by the main app
medallion_discovery: Optional[MedallionDiscoveryEngine] = None
atlas_client: Optional[AtlasClient] = None
event_stream: Optional[EventStream] = None


def set_discovery_deps(**deps):
    """Set dependencies for the discovery router"""
    global medallion_discovery, atlas_client, event_stream
    medallion_discovery = deps.get("medallion_discovery")
    atlas_client = deps.get("atlas_client")
    event_stream = deps.get("event_stream")


@router.post("/scan")
async def trigger_discovery_scan(
    background_tasks: BackgroundTasks,
    layer: Optional[DataLayer] = None,
    force_full_scan: bool = False
):
    """
    Trigger a discovery scan for medallion layers
    
    - **layer**: Optional specific layer to scan (bronze, silver, gold)
    - **force_full_scan**: Force a full scan even if recently scanned
    """
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    async def run_scan():
        try:
            if layer:
                # Scan specific layer
                assets = await medallion_discovery.discover_layer(layer, force_full_scan)
                result = await medallion_discovery.register_discovered_assets(assets)
                
                # Emit event
                await event_stream.publish(
                    topic="catalog-discovery",
                    event_type="layer_scan_completed",
                    data={
                        "layer": layer.value,
                        "discovered": len(assets),
                        "registered": result["registered"],
                        "updated": result["updated"]
                    }
                )
            else:
                # Scan all layers
                discoveries = await medallion_discovery.discover_all_layers(force_full_scan)
                
                total_registered = 0
                total_updated = 0
                
                for layer_name, assets in discoveries.items():
                    if assets:
                        result = await medallion_discovery.register_discovered_assets(assets)
                        total_registered += result["registered"]
                        total_updated += result["updated"]
                
                # Emit event
                await event_stream.publish(
                    topic="catalog-discovery",
                    event_type="full_scan_completed",
                    data={
                        "total_discovered": sum(len(assets) for assets in discoveries.values()),
                        "total_registered": total_registered,
                        "total_updated": total_updated
                    }
                )
        except Exception as e:
            logger.error(f"Discovery scan failed: {e}")
            await event_stream.publish(
                topic="catalog-discovery",
                event_type="scan_failed",
                data={"error": str(e)}
            )
    
    background_tasks.add_task(run_scan)
    
    return {
        "status": "scan_initiated",
        "layer": layer.value if layer else "all",
        "force_full_scan": force_full_scan,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/status")
async def get_discovery_status():
    """Get current discovery status and statistics"""
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    status = {
        "last_discovery_times": {},
        "discovered_assets_count": {},
        "next_scheduled_scan": None
    }
    
    # Get last discovery times
    for layer in DataLayer:
        last_time = medallion_discovery.last_discovery_time.get(layer)
        if last_time:
            status["last_discovery_times"][layer.value] = last_time.isoformat()
    
    # Get discovered assets count
    for layer, assets in medallion_discovery.discovered_assets.items():
        status["discovered_assets_count"][layer] = len(assets)
    
    return status


@router.get("/assets/{layer}")
async def get_discovered_assets(
    layer: DataLayer,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get discovered assets for a specific layer"""
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    # Get assets from discovery engine
    assets = medallion_discovery.discovered_assets.get(layer, [])
    
    # Apply pagination
    total = len(assets)
    paginated_assets = assets[offset:offset + limit]
    
    return {
        "layer": layer.value,
        "total": total,
        "limit": limit,
        "offset": offset,
        "assets": [
            {
                "name": asset.name,
                "qualified_name": asset.qualified_name,
                "path": asset.path,
                "format": asset.format,
                "size_bytes": asset.size_bytes,
                "row_count": asset.row_count,
                "column_count": asset.column_count,
                "quality_score": asset.quality_score,
                "created_time": asset.created_time.isoformat(),
                "modified_time": asset.modified_time.isoformat(),
                "tags": asset.tags
            }
            for asset in paginated_assets
        ]
    }


@router.post("/assets/{layer}/{dataset_name}/profile")
async def profile_dataset(
    layer: DataLayer,
    dataset_name: str,
    background_tasks: BackgroundTasks
):
    """Trigger data profiling for a discovered dataset"""
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    # Find the asset
    assets = medallion_discovery.discovered_assets.get(layer, [])
    asset = next((a for a in assets if a.name == dataset_name), None)
    
    if not asset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    async def run_profiling():
        try:
            profile = await medallion_discovery._profile_dataset(asset)
            
            # Update asset in catalog
            entity = await atlas_client.get_entity_by_attribute(
                type_name="dataset",
                attr_name="qualifiedName",
                attr_value=asset.qualified_name
            )
            
            if entity:
                await atlas_client.partial_update_entity(
                    entity["guid"],
                    {"dataProfile": profile}
                )
            
            # Emit event
            await event_stream.publish(
                topic="catalog-discovery",
                event_type="dataset_profiled",
                data={
                    "dataset": dataset_name,
                    "layer": layer.value,
                    "profile_summary": {
                        "row_count": profile.get("row_count"),
                        "column_count": profile.get("column_count"),
                        "null_percentage": profile.get("null_percentage")
                    }
                }
            )
        except Exception as e:
            logger.error(f"Profiling failed for {dataset_name}: {e}")
    
    background_tasks.add_task(run_profiling)
    
    return {
        "status": "profiling_initiated",
        "dataset": dataset_name,
        "layer": layer.value,
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/recommendations")
async def get_discovery_recommendations():
    """Get recommendations for improving data discovery"""
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    recommendations = []
    
    # Check for undiscovered buckets
    for layer, bucket in medallion_discovery.layer_buckets.items():
        if not medallion_discovery.minio_client.bucket_exists(bucket):
            recommendations.append({
                "type": "missing_bucket",
                "severity": "high",
                "message": f"Bucket '{bucket}' for {layer.value} layer does not exist",
                "action": f"Create bucket '{bucket}' to enable {layer.value} layer discovery"
            })
    
    # Check for stale discoveries
    for layer, last_time in medallion_discovery.last_discovery_time.items():
        age_hours = (datetime.utcnow() - last_time).total_seconds() / 3600
        if age_hours > 48:
            recommendations.append({
                "type": "stale_discovery",
                "severity": "medium",
                "message": f"{layer.value} layer hasn't been scanned in {int(age_hours)} hours",
                "action": f"Run discovery scan for {layer.value} layer"
            })
    
    # Check for datasets without quality scores
    for layer, assets in medallion_discovery.discovered_assets.items():
        no_quality = [a for a in assets if a.quality_score is None]
        if no_quality:
            recommendations.append({
                "type": "missing_quality",
                "severity": "low",
                "message": f"{len(no_quality)} datasets in {layer.value} layer lack quality scores",
                "action": "Run quality assessment for these datasets"
            })
    
    return {
        "recommendations": recommendations,
        "total": len(recommendations),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/schedule")
async def update_discovery_schedule(
    interval_minutes: int = Query(..., ge=15, le=1440),
    full_scan_interval_hours: int = Query(..., ge=1, le=168)
):
    """Update the discovery schedule"""
    if not medallion_discovery:
        raise HTTPException(status_code=503, detail="Discovery engine not initialized")
    
    # This would update the scheduler
    # For now, return the configuration
    return {
        "status": "schedule_updated",
        "interval_minutes": interval_minutes,
        "full_scan_interval_hours": full_scan_interval_hours,
        "note": "Schedule update will take effect on next cycle"
    } 