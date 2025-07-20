"""
Digital Asset Events API Router

Handles routing and processing of digital asset lifecycle events including:
- Asset creation and updates
- Peer review events
- Marketplace transactions
- License management
- Asset lineage tracking
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from enum import Enum
import asyncio
import logging

from ..core.event_router import EventRouter
from ..core.schemas import Event, RoutingRule
from ..monitoring.dlq_monitor import DLQMonitor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/asset-events", tags=["Asset Events"])


class AssetEventType(Enum):
    """Types of digital asset events"""
    ASSET_CREATED = "asset_created"
    ASSET_UPDATED = "asset_updated"
    ASSET_DELETED = "asset_deleted"
    ASSET_PUBLISHED = "asset_published"
    REVIEW_SUBMITTED = "review_submitted"
    REVIEW_COMPLETED = "review_completed"
    ASSET_PURCHASED = "asset_purchased"
    ASSET_LICENSED = "asset_licensed"
    LICENSE_EXPIRED = "license_expired"
    ROYALTY_DISTRIBUTED = "royalty_distributed"
    METADATA_UPDATED = "metadata_updated"
    ASSET_VERIFIED = "asset_verified"


class AssetType(Enum):
    """Types of digital assets"""
    MODEL_3D = "model_3d"
    DATASET = "dataset"
    DOCUMENT = "document"
    IMAGE = "image"
    CODE = "code"
    SIMULATION = "simulation"
    ALGORITHM = "algorithm"


class AssetMetadata(BaseModel):
    """Digital asset metadata"""
    asset_id: str
    name: str
    type: AssetType
    owner_id: str
    cid: str  # Content ID (IPFS)
    size_bytes: int
    format: str
    version: str
    tags: List[str] = Field(default_factory=list)
    license_type: Optional[str] = None
    price: Optional[Decimal] = None


class AssetCreatedEvent(BaseModel):
    """Asset creation event"""
    event_type: AssetEventType = AssetEventType.ASSET_CREATED
    asset_metadata: AssetMetadata
    timestamp: datetime
    source_service: str
    parent_asset_id: Optional[str] = None
    creation_metadata: Dict[str, Any] = Field(default_factory=dict)


class ReviewEvent(BaseModel):
    """Peer review event"""
    event_type: AssetEventType
    asset_id: str
    review_id: str
    reviewer_id: str
    timestamp: datetime
    rating: int = Field(ge=1, le=5)
    comments: Optional[str] = None
    review_type: str  # "quality", "accuracy", "completeness"
    metadata: Dict[str, Any] = Field(default_factory=dict)


class MarketplaceEvent(BaseModel):
    """Marketplace transaction event"""
    event_type: AssetEventType
    asset_id: str
    transaction_id: str
    buyer_id: str
    seller_id: str
    timestamp: datetime
    price: Decimal
    currency: str
    transaction_type: str  # "purchase", "license"
    license_duration_days: Optional[int] = None
    blockchain_tx_hash: Optional[str] = None


class LicenseEvent(BaseModel):
    """License management event"""
    event_type: AssetEventType
    asset_id: str
    license_id: str
    licensee_id: str
    timestamp: datetime
    license_type: str
    expiration_date: Optional[datetime] = None
    usage_limits: Optional[Dict[str, int]] = None
    status: str  # "active", "expired", "revoked"


class RoyaltyEvent(BaseModel):
    """Royalty distribution event"""
    event_type: AssetEventType = AssetEventType.ROYALTY_DISTRIBUTED
    asset_id: str
    transaction_id: str
    timestamp: datetime
    total_amount: Decimal
    currency: str
    distributions: List[Dict[str, Any]]  # recipient_id, amount, percentage


class AssetEventRouter:
    """Routes digital asset events to appropriate destinations"""
    
    def __init__(self, event_router: EventRouter, dlq_monitor: Optional[DLQMonitor] = None):
        self.event_router = event_router
        self.dlq_monitor = dlq_monitor
        
        # Asset-specific routing rules
        self.asset_routing_rules = {
            AssetEventType.ASSET_CREATED: ["asset-registry", "asset-lineage", "asset-search"],
            AssetEventType.ASSET_UPDATED: ["asset-registry", "asset-lineage", "asset-search"],
            AssetEventType.ASSET_DELETED: ["asset-registry", "asset-lineage", "asset-cleanup"],
            AssetEventType.ASSET_PUBLISHED: ["asset-marketplace", "asset-search", "asset-notifications"],
            AssetEventType.REVIEW_SUBMITTED: ["review-queue", "asset-reputation"],
            AssetEventType.REVIEW_COMPLETED: ["asset-registry", "asset-reputation", "asset-search"],
            AssetEventType.ASSET_PURCHASED: ["asset-marketplace", "royalty-processor", "asset-analytics"],
            AssetEventType.ASSET_LICENSED: ["license-manager", "asset-usage-tracker"],
            AssetEventType.LICENSE_EXPIRED: ["license-manager", "asset-notifications"],
            AssetEventType.ROYALTY_DISTRIBUTED: ["royalty-ledger", "asset-analytics"],
            AssetEventType.METADATA_UPDATED: ["asset-registry", "asset-search"],
            AssetEventType.ASSET_VERIFIED: ["asset-registry", "asset-trust-score"]
        }
        
        # Enrichment functions
        self.enrichment_functions = {
            AssetEventType.ASSET_CREATED: self._enrich_asset_created,
            AssetEventType.ASSET_PURCHASED: self._enrich_asset_purchased,
            AssetEventType.REVIEW_COMPLETED: self._enrich_review_completed
        }
        
    async def _enrich_asset_created(self, event: AssetCreatedEvent) -> Dict[str, Any]:
        """Enrich asset creation event"""
        enriched = event.dict()
        
        # Add provenance information
        if event.parent_asset_id:
            enriched["provenance"] = {
                "derived_from": event.parent_asset_id,
                "derivation_type": "modified",
                "lineage_depth": 1  # Would be calculated from lineage graph
            }
            
        # Add estimated processing requirements
        asset_type = event.asset_metadata.type
        if asset_type == AssetType.MODEL_3D:
            enriched["processing_requirements"] = {
                "gpu_required": True,
                "estimated_time_seconds": event.asset_metadata.size_bytes / 1_000_000 * 2
            }
        elif asset_type == AssetType.DATASET:
            enriched["processing_requirements"] = {
                "memory_gb": event.asset_metadata.size_bytes / 1_000_000_000 * 2,
                "estimated_time_seconds": event.asset_metadata.size_bytes / 10_000_000
            }
            
        # Add content hash for integrity
        enriched["content_hash"] = event.asset_metadata.cid
        
        return enriched
        
    async def _enrich_asset_purchased(self, event: MarketplaceEvent) -> Dict[str, Any]:
        """Enrich asset purchase event"""
        enriched = event.dict()
        
        # Calculate platform fees
        platform_fee_percent = 0.025  # 2.5% platform fee
        platform_fee = float(event.price) * platform_fee_percent
        
        enriched["fees"] = {
            "platform_fee": platform_fee,
            "gas_fee": 0.01 if event.blockchain_tx_hash else 0,
            "total_cost": float(event.price) + platform_fee
        }
        
        # Add transaction verification status
        if event.blockchain_tx_hash:
            enriched["verification"] = {
                "blockchain_verified": True,
                "confirmation_blocks": 3,  # Would be fetched from blockchain
                "finality": "confirmed"
            }
            
        return enriched
        
    async def _enrich_review_completed(self, event: ReviewEvent) -> Dict[str, Any]:
        """Enrich review completion event"""
        enriched = event.dict()
        
        # Calculate review impact
        enriched["review_impact"] = {
            "reputation_change": (event.rating - 3) * 0.1,  # Simplified calculation
            "quality_score_impact": event.rating / 5.0,
            "review_weight": 1.0  # Would be based on reviewer reputation
        }
        
        # Add review analytics
        enriched["analytics"] = {
            "sentiment": "positive" if event.rating >= 4 else "negative" if event.rating <= 2 else "neutral",
            "aspects_covered": ["quality"] if event.review_type == "quality" else ["accuracy"],
            "review_depth": "detailed" if event.comments and len(event.comments) > 100 else "brief"
        }
        
        return enriched
        
    async def route_asset_event(self, event: Union[AssetCreatedEvent, ReviewEvent, 
                                                  MarketplaceEvent, LicenseEvent, RoyaltyEvent]) -> Dict[str, Any]:
        """Route asset event to appropriate destinations"""
        event_type = event.event_type
        destinations = self.asset_routing_rules.get(event_type, ["asset-default"])
        
        # Apply enrichment if available
        enrichment_func = self.enrichment_functions.get(event_type)
        if enrichment_func:
            event_data = await enrichment_func(event)
        else:
            event_data = event.dict()
            
        # Create routing event
        routing_event = Event(
            event_id=f"asset-{event.timestamp.timestamp()}-{event_type.value}",
            event_type=f"asset.{event_type.value}",
            source="digital-asset-service",
            timestamp=event.timestamp,
            data=event_data,
            metadata={
                "asset_event_type": event_type.value,
                "destinations": destinations,
                "asset_id": getattr(event, "asset_id", event.asset_metadata.asset_id if hasattr(event, "asset_metadata") else None)
            }
        )
        
        # Route to each destination
        results = {}
        for destination in destinations:
            try:
                result = await self.event_router.route_event(routing_event, destination)
                results[destination] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Failed to route asset event to {destination}: {e}")
                results[destination] = {"status": "failed", "error": str(e)}
                
                # Send to DLQ if available
                if self.dlq_monitor:
                    await self.dlq_monitor.send_to_dlq(routing_event, str(e))
                    
        return {
            "event_id": routing_event.event_id,
            "routed_to": results,
            "timestamp": datetime.utcnow()
        }


# Initialize router instance
asset_router_instance = None


def get_asset_router(event_router: EventRouter = Depends(lambda: router.app.state.event_router),
                    dlq_monitor: Optional[DLQMonitor] = Depends(lambda: getattr(router.app.state, 'dlq_monitor', None))) -> AssetEventRouter:
    """Get asset router instance"""
    global asset_router_instance
    if not asset_router_instance:
        asset_router_instance = AssetEventRouter(event_router, dlq_monitor)
    return asset_router_instance


@router.post("/asset-created")
async def submit_asset_created_event(event: AssetCreatedEvent,
                                   asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Submit asset creation event"""
    return await asset_router.route_asset_event(event)


@router.post("/review-events")
async def submit_review_event(event: ReviewEvent,
                            asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Submit review event"""
    return await asset_router.route_asset_event(event)


@router.post("/marketplace-events")
async def submit_marketplace_event(event: MarketplaceEvent,
                                 asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Submit marketplace event"""
    return await asset_router.route_asset_event(event)


@router.post("/license-events")
async def submit_license_event(event: LicenseEvent,
                             asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Submit license event"""
    return await asset_router.route_asset_event(event)


@router.post("/royalty-events")
async def submit_royalty_event(event: RoyaltyEvent,
                             asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Submit royalty distribution event"""
    return await asset_router.route_asset_event(event)


@router.post("/batch-events")
async def submit_batch_asset_events(events: List[Union[AssetCreatedEvent, ReviewEvent,
                                                      MarketplaceEvent, LicenseEvent, RoyaltyEvent]],
                                   asset_router: AssetEventRouter = Depends(get_asset_router),
                                   background_tasks: BackgroundTasks = BackgroundTasks()) -> Dict[str, Any]:
    """Submit multiple asset events in batch"""
    # Process in background
    background_tasks.add_task(process_batch_asset_events, events, asset_router)
    
    return {
        "status": "accepted",
        "event_count": len(events),
        "message": "Events queued for processing"
    }


async def process_batch_asset_events(events: List[Any], asset_router: AssetEventRouter):
    """Process batch asset events asynchronously"""
    for event in events:
        try:
            await asset_router.route_asset_event(event)
        except Exception as e:
            logger.error(f"Failed to process asset event: {e}")


@router.get("/routing-rules")
async def get_asset_routing_rules(asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, List[str]]:
    """Get current asset event routing rules"""
    return {
        event_type.value: destinations
        for event_type, destinations in asset_router.asset_routing_rules.items()
    }


@router.put("/routing-rules/{event_type}")
async def update_asset_routing_rule(event_type: AssetEventType,
                                  destinations: List[str],
                                  asset_router: AssetEventRouter = Depends(get_asset_router)) -> Dict[str, Any]:
    """Update routing rule for specific asset event type"""
    asset_router.asset_routing_rules[event_type] = destinations
    return {
        "event_type": event_type.value,
        "destinations": destinations,
        "updated_at": datetime.utcnow()
    } 