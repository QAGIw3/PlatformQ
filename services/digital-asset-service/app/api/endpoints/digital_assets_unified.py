"""Example digital assets endpoint using unified data access

This demonstrates how to refactor endpoints to use the unified data layer.
"""

from fastapi import APIRouter, Depends, HTTPException, Query as QueryParam
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from platformq_unified_data import UnifiedDataManager
from ...unified_data_models import (
    UnifiedDigitalAsset,
    UnifiedAssetLineage,
    UnifiedAssetMetrics
)
from ...auth import get_current_user
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize unified data manager (in production, this would be in app startup)
data_manager = UnifiedDataManager()

# Initialize event publisher
event_publisher = EventPublisher('pulsar://pulsar:6650')


async def get_data_manager() -> UnifiedDataManager:
    """Dependency to get initialized data manager"""
    await data_manager.initialize()
    return data_manager


@router.post("/assets", response_model=Dict[str, Any])
async def create_asset(
    asset_data: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Create a new digital asset using unified data access"""
    try:
        # Get repository for the user's tenant
        asset_repo = dm.get_repository(UnifiedDigitalAsset, tenant_id=current_user['tenant_id'])
        
        # Create asset instance
        asset = UnifiedDigitalAsset(
            name=asset_data['name'],
            description=asset_data.get('description'),
            asset_type=asset_data['asset_type'],
            file_format=asset_data.get('file_format'),
            file_size=asset_data.get('file_size', 0),
            owner_id=current_user['user_id'],
            tenant_id=current_user['tenant_id'],
            metadata=asset_data.get('metadata', {}),
            tags=asset_data.get('tags', []),
            categories=asset_data.get('categories', [])
        )
        
        # Create in data stores
        created_asset = await asset_repo.create(asset)
        
        # Initialize metrics in Ignite
        metrics_repo = dm.get_repository(UnifiedAssetMetrics, tenant_id=current_user['tenant_id'])
        metrics = UnifiedAssetMetrics(
            asset_id=created_asset.id,
            quality_score=0.0
        )
        await metrics_repo.create(metrics)
        
        # Publish event
        await event_publisher.publish_event('asset.created', {
            'asset_id': str(created_asset.id),
            'owner_id': created_asset.owner_id,
            'tenant_id': created_asset.tenant_id,
            'asset_type': created_asset.asset_type
        })
        
        return created_asset.to_dict()
        
    except Exception as e:
        logger.error(f"Failed to create asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/{asset_id}", response_model=Dict[str, Any])
async def get_asset(
    asset_id: str,
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Get asset by ID with caching"""
    try:
        asset_repo = dm.get_repository(UnifiedDigitalAsset, tenant_id=current_user['tenant_id'])
        
        # This will use multi-level caching automatically
        asset = await asset_repo.get_by_id(asset_id)
        
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        # Check access permissions
        if asset.owner_id != current_user['user_id'] and not asset.is_public:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Update metrics asynchronously
        metrics_repo = dm.get_repository(UnifiedAssetMetrics, tenant_id=current_user['tenant_id'])
        metrics = await metrics_repo.find_one(asset_id=asset_id)
        if metrics:
            metrics.views_last_hour += 1
            await metrics_repo.update(metrics)
        
        return asset.to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets", response_model=List[Dict[str, Any]])
async def search_assets(
    q: Optional[str] = QueryParam(None, description="Search query"),
    asset_type: Optional[str] = QueryParam(None),
    tags: Optional[List[str]] = QueryParam(None),
    owner_id: Optional[str] = QueryParam(None),
    is_public: Optional[bool] = QueryParam(None),
    created_after: Optional[datetime] = QueryParam(None),
    limit: int = QueryParam(20, ge=1, le=100),
    offset: int = QueryParam(0, ge=0),
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Search assets using unified query builder"""
    try:
        asset_repo = dm.get_repository(UnifiedDigitalAsset, tenant_id=current_user['tenant_id'])
        
        # Build query using fluent API
        query = asset_repo.query()
        
        # Add filters
        if asset_type:
            query = query.filter(asset_type=asset_type)
        
        if owner_id:
            query = query.filter(owner_id=owner_id)
        
        if is_public is not None:
            query = query.filter(is_public=is_public)
        
        if created_after:
            query = query.filter(created_at__gte=created_after)
        
        if tags:
            # This would need custom handling for array contains
            for tag in tags:
                query = query.filter(tags__contains=tag)
        
        # Add text search if provided (would search in Elasticsearch)
        if q:
            # This would trigger Elasticsearch search
            query = query.filter(name__ilike=f"%{q}%")
        
        # Add pagination and ordering
        query = query.order_by("-created_at").limit(limit).offset(offset)
        
        # Execute query
        assets = await asset_repo.find(query)
        
        return [asset.to_dict() for asset in assets]
        
    except Exception as e:
        logger.error(f"Failed to search assets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/assets/{asset_id}", response_model=Dict[str, Any])
async def update_asset(
    asset_id: str,
    update_data: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Update asset with automatic cache invalidation"""
    try:
        asset_repo = dm.get_repository(UnifiedDigitalAsset, tenant_id=current_user['tenant_id'])
        
        # Get existing asset
        asset = await asset_repo.get_by_id(asset_id)
        
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        # Check ownership
        if asset.owner_id != current_user['user_id']:
            raise HTTPException(status_code=403, detail="Only owner can update asset")
        
        # Update fields
        for field, value in update_data.items():
            if hasattr(asset, field) and field not in ['id', 'created_at', 'owner_id', 'tenant_id']:
                setattr(asset, field, value)
        
        # Update in stores (cache invalidation happens automatically)
        updated_asset = await asset_repo.update(asset)
        
        # Publish event
        await event_publisher.publish_event('asset.updated', {
            'asset_id': str(updated_asset.id),
            'updated_fields': list(update_data.keys()),
            'updated_by': current_user['user_id']
        })
        
        return updated_asset.to_dict()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/{asset_id}/lineage", response_model=Dict[str, Any])
async def get_asset_lineage(
    asset_id: str,
    depth: int = QueryParam(3, ge=1, le=10),
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Get asset lineage using graph traversal"""
    try:
        # First check if user has access to the asset
        asset_repo = dm.get_repository(UnifiedDigitalAsset, tenant_id=current_user['tenant_id'])
        asset = await asset_repo.get_by_id(asset_id)
        
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        if asset.owner_id != current_user['user_id'] and not asset.is_public:
            raise HTTPException(status_code=403, detail="Access denied")
        
        # Get lineage from graph store
        lineage_repo = dm.get_repository(UnifiedAssetLineage, tenant_id=current_user['tenant_id'])
        
        # Get parent lineage
        parent_query = lineage_repo.query().filter(child_asset_id=asset_id)
        parents = await lineage_repo.find(parent_query)
        
        # Get child lineage
        child_query = lineage_repo.query().filter(parent_asset_id=asset_id)
        children = await lineage_repo.find(child_query)
        
        # Build lineage tree (simplified - real implementation would do recursive traversal)
        lineage = {
            'asset_id': asset_id,
            'parents': [p.to_dict() for p in parents],
            'children': [c.to_dict() for c in children]
        }
        
        return lineage
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get asset lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assets/federated-search", response_model=List[Dict[str, Any]])
async def federated_search(
    search_query: Dict[str, Any],
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Execute federated search across multiple data stores"""
    try:
        # Build SQL for federated query
        sql = """
        SELECT 
            c.id,
            c.name,
            c.asset_type,
            c.owner_id,
            c.created_at,
            e.search_score,
            e.highlighted_text,
            m.file_size,
            m.last_modified
        FROM cassandra.platformq.digital_assets c
        LEFT JOIN elasticsearch.default.assets e ON c.id = e.asset_id
        LEFT JOIN minio.bronze.asset_metadata m ON c.id = m.asset_id
        WHERE c.tenant_id = :tenant_id
        """
        
        # Add conditions based on search query
        conditions = []
        params = {'tenant_id': current_user['tenant_id']}
        
        if search_query.get('text'):
            conditions.append("e.content MATCH :search_text")
            params['search_text'] = search_query['text']
        
        if search_query.get('asset_type'):
            conditions.append("c.asset_type = :asset_type")
            params['asset_type'] = search_query['asset_type']
        
        if search_query.get('date_range'):
            conditions.append("c.created_at BETWEEN :start_date AND :end_date")
            params['start_date'] = search_query['date_range']['start']
            params['end_date'] = search_query['date_range']['end']
        
        if conditions:
            sql += " AND " + " AND ".join(conditions)
        
        sql += " ORDER BY e.search_score DESC LIMIT 50"
        
        # Execute federated query
        results = await dm.federated_query(sql, params)
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to execute federated search: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/assets/analytics/dashboard", response_model=Dict[str, Any])
async def get_analytics_dashboard(
    time_range: str = QueryParam("1h", regex="^(1h|24h|7d|30d)$"),
    current_user: Dict = Depends(get_current_user),
    dm: UnifiedDataManager = Depends(get_data_manager)
):
    """Get real-time analytics dashboard using Ignite cache"""
    try:
        metrics_repo = dm.get_repository(UnifiedAssetMetrics, tenant_id=current_user['tenant_id'])
        
        # Get all metrics for tenant (from Ignite - very fast)
        all_metrics = await metrics_repo.find(metrics_repo.query())
        
        # Aggregate metrics
        total_views = sum(m.views_last_hour for m in all_metrics)
        total_downloads = sum(m.downloads_last_hour for m in all_metrics)
        active_viewers = sum(m.current_viewers for m in all_metrics)
        processing_jobs = sum(m.processing_jobs for m in all_metrics)
        
        # Calculate averages
        avg_quality = sum(m.quality_score for m in all_metrics if m.quality_score) / len(all_metrics) if all_metrics else 0
        avg_load_time = sum(m.avg_load_time_ms for m in all_metrics if m.avg_load_time_ms) / len(all_metrics) if all_metrics else 0
        
        dashboard = {
            'summary': {
                'total_assets': len(all_metrics),
                'total_views': total_views,
                'total_downloads': total_downloads,
                'active_viewers': active_viewers,
                'processing_jobs': processing_jobs
            },
            'performance': {
                'avg_quality_score': avg_quality,
                'avg_load_time_ms': avg_load_time
            },
            'time_range': time_range,
            'generated_at': datetime.utcnow().isoformat()
        }
        
        return dashboard
        
    except Exception as e:
        logger.error(f"Failed to get analytics dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 