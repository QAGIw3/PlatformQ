"""
Event-Driven Digital Asset Integration

Integrates digital asset service with event-driven architecture for:
- Asset lifecycle event publishing
- Lineage tracking in graph database
- Review and reputation management
- Marketplace transaction tracking
"""

import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from decimal import Decimal
from enum import Enum
import asyncio
import json

from platformq_shared import ServiceClient
import httpx

logger = logging.getLogger(__name__)


class AssetEventType(Enum):
    """Asset event types matching Event Router Service"""
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


class EventDrivenAssetIntegration:
    """Integrates digital asset service with event-driven architecture"""
    
    def __init__(self, vault_consul_integration=None):
        self.vault_consul = vault_consul_integration
        
        # Service clients
        self.event_router_client = ServiceClient(
            service_name="event-router-service",
            circuit_breaker_threshold=5,
            rate_limit=1000.0
        )
        
        self.graph_intelligence_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=200.0
        )
        
        self.search_service_client = ServiceClient(
            service_name="search-service",
            circuit_breaker_threshold=5,
            rate_limit=500.0
        )
        
        # Event handlers
        self.event_handlers: Dict[AssetEventType, List[Callable]] = {
            event_type: [] for event_type in AssetEventType
        }
        
        # Metrics
        self.events_published = 0
        self.events_failed = 0
        self.lineage_updates = 0
        self.search_updates = 0
        
    async def initialize(self):
        """Initialize integration"""
        logger.info("Initializing event-driven asset integration")
        
        # Register default handlers
        self._register_default_handlers()
        
        # Start background tasks
        asyncio.create_task(self._monitor_integration_health())
        
    def _register_default_handlers(self):
        """Register default event handlers"""
        # Asset lifecycle handlers
        self.register_event_handler(
            AssetEventType.ASSET_CREATED,
            self._handle_asset_created
        )
        self.register_event_handler(
            AssetEventType.REVIEW_COMPLETED,
            self._handle_review_completed
        )
        self.register_event_handler(
            AssetEventType.ASSET_PURCHASED,
            self._handle_asset_purchased
        )
        
    def register_event_handler(self, event_type: AssetEventType, handler: Callable):
        """Register handler for specific asset event type"""
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered asset handler for {event_type.value}")
        
    async def publish_asset_event(self, event_type: AssetEventType, event_data: Dict[str, Any]) -> bool:
        """Publish asset event to event router"""
        try:
            # Prepare event
            event = {
                "event_type": event_type.value,
                "timestamp": datetime.utcnow().isoformat(),
                **event_data
            }
            
            # Route to appropriate endpoint based on event type
            endpoint_map = {
                AssetEventType.ASSET_CREATED: "/api/v1/asset-events/asset-created",
                AssetEventType.ASSET_UPDATED: "/api/v1/asset-events/asset-created",
                AssetEventType.ASSET_DELETED: "/api/v1/asset-events/asset-created",
                AssetEventType.ASSET_PUBLISHED: "/api/v1/asset-events/asset-created",
                AssetEventType.REVIEW_SUBMITTED: "/api/v1/asset-events/review-events",
                AssetEventType.REVIEW_COMPLETED: "/api/v1/asset-events/review-events",
                AssetEventType.ASSET_PURCHASED: "/api/v1/asset-events/marketplace-events",
                AssetEventType.ASSET_LICENSED: "/api/v1/asset-events/license-events",
                AssetEventType.LICENSE_EXPIRED: "/api/v1/asset-events/license-events",
                AssetEventType.ROYALTY_DISTRIBUTED: "/api/v1/asset-events/royalty-events",
                AssetEventType.METADATA_UPDATED: "/api/v1/asset-events/asset-created",
                AssetEventType.ASSET_VERIFIED: "/api/v1/asset-events/asset-created"
            }
            
            endpoint = endpoint_map.get(event_type)
            if not endpoint:
                logger.error(f"No endpoint mapped for event type: {event_type}")
                return False
                
            # Publish to event router
            response = await self.event_router_client.post(endpoint, json=event)
            
            if response.status_code == 200:
                self.events_published += 1
                logger.info(f"Published asset event: {event_type.value}")
                
                # Execute local handlers
                for handler in self.event_handlers[event_type]:
                    asyncio.create_task(handler(event))
                    
                return True
            else:
                self.events_failed += 1
                logger.error(f"Failed to publish asset event: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing asset event: {e}")
            self.events_failed += 1
            return False
            
    async def _handle_asset_created(self, event: Dict[str, Any]):
        """Handle asset created event"""
        try:
            # Update asset lineage
            await self.create_asset_lineage_node(event)
            
            # Index in search service
            await self.index_asset_for_search(event)
            
            # Check for parent asset relationships
            if event.get("parent_asset_id"):
                await self.create_derivation_relationship(event)
                
        except Exception as e:
            logger.error(f"Error handling asset created event: {e}")
            
    async def _handle_review_completed(self, event: Dict[str, Any]):
        """Handle review completed event"""
        try:
            # Update lineage with review
            await self.add_review_to_lineage(event)
            
            # Update search index with new quality score
            await self.update_asset_search_index(event["asset_id"])
            
        except Exception as e:
            logger.error(f"Error handling review completed event: {e}")
            
    async def _handle_asset_purchased(self, event: Dict[str, Any]):
        """Handle asset purchased event"""
        try:
            # Record transaction in lineage
            await self.add_transaction_to_lineage(event)
            
            # Process royalty distribution
            if event.get("royalty_distributions"):
                await self.process_royalty_distribution(event)
                
        except Exception as e:
            logger.error(f"Error handling asset purchased event: {e}")
            
    async def create_asset_lineage_node(self, event: Dict[str, Any]) -> bool:
        """Create asset node in lineage graph"""
        try:
            asset_metadata = event.get("asset_metadata", {})
            
            # Create asset node
            asset_node = {
                "asset_id": asset_metadata.get("asset_id"),
                "cid": asset_metadata.get("cid"),
                "name": asset_metadata.get("name"),
                "asset_type": asset_metadata.get("type", {}).get("value", "unknown"),
                "owner_id": asset_metadata.get("owner_id"),
                "size_bytes": asset_metadata.get("size_bytes", 0),
                "format": asset_metadata.get("format", ""),
                "version": asset_metadata.get("version", "1.0"),
                "tags": asset_metadata.get("tags", []),
                "metadata": {
                    "license_type": asset_metadata.get("license_type"),
                    "price": float(asset_metadata.get("price", 0)) if asset_metadata.get("price") else None
                }
            }
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/asset-lineage/assets",
                json=asset_node
            )
            
            if response.status_code == 200:
                self.lineage_updates += 1
                logger.info(f"Created asset lineage node: {asset_node['asset_id']}")
                return True
            else:
                logger.error(f"Failed to create asset lineage node: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating asset lineage node: {e}")
            return False
            
    async def create_derivation_relationship(self, event: Dict[str, Any]) -> bool:
        """Create derivation relationship between assets"""
        try:
            asset_metadata = event.get("asset_metadata", {})
            parent_id = event.get("parent_asset_id")
            
            if not parent_id:
                return False
                
            derivation = {
                "child_id": asset_metadata.get("asset_id"),
                "parent_id": parent_id,
                "derivation_type": event.get("creation_metadata", {}).get("derivation_type", "derived")
            }
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/asset-lineage/derivations",
                json=derivation
            )
            
            if response.status_code == 200:
                self.lineage_updates += 1
                logger.info(f"Created derivation: {derivation['child_id']} <- {derivation['parent_id']}")
                return True
            else:
                logger.error(f"Failed to create derivation: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error creating derivation relationship: {e}")
            return False
            
    async def add_review_to_lineage(self, event: Dict[str, Any]) -> bool:
        """Add review to asset lineage"""
        try:
            review_node = {
                "review_id": event.get("review_id"),
                "asset_id": event.get("asset_id"),
                "reviewer_id": event.get("reviewer_id"),
                "rating": event.get("rating"),
                "review_type": event.get("review_type"),
                "comments": event.get("comments"),
                "verified": event.get("metadata", {}).get("verified", False)
            }
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/asset-lineage/reviews",
                json=review_node
            )
            
            if response.status_code == 200:
                self.lineage_updates += 1
                logger.info(f"Added review to lineage: {review_node['review_id']}")
                return True
            else:
                logger.error(f"Failed to add review to lineage: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding review to lineage: {e}")
            return False
            
    async def add_transaction_to_lineage(self, event: Dict[str, Any]) -> bool:
        """Add marketplace transaction to lineage"""
        try:
            transaction_node = {
                "transaction_id": event.get("transaction_id"),
                "asset_id": event.get("asset_id"),
                "buyer_id": event.get("buyer_id"),
                "seller_id": event.get("seller_id"),
                "price": float(event.get("price", 0)),
                "currency": event.get("currency", "USD"),
                "transaction_type": event.get("transaction_type", "purchase"),
                "blockchain_tx_hash": event.get("blockchain_tx_hash")
            }
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/asset-lineage/transactions",
                json=transaction_node
            )
            
            if response.status_code == 200:
                self.lineage_updates += 1
                logger.info(f"Added transaction to lineage: {transaction_node['transaction_id']}")
                return True
            else:
                logger.error(f"Failed to add transaction to lineage: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding transaction to lineage: {e}")
            return False
            
    async def index_asset_for_search(self, event: Dict[str, Any]) -> bool:
        """Index asset in search service"""
        try:
            asset_metadata = event.get("asset_metadata", {})
            
            # Prepare search document
            search_doc = {
                "id": asset_metadata.get("asset_id"),
                "type": "digital_asset",
                "content": {
                    "name": asset_metadata.get("name"),
                    "description": event.get("creation_metadata", {}).get("description", ""),
                    "tags": asset_metadata.get("tags", []),
                    "asset_type": asset_metadata.get("type", {}).get("value", "unknown"),
                    "owner_id": asset_metadata.get("owner_id"),
                    "cid": asset_metadata.get("cid"),
                    "format": asset_metadata.get("format"),
                    "created_at": event.get("timestamp")
                }
            }
            
            response = await self.search_service_client.post(
                "/api/v1/search/index",
                json=search_doc
            )
            
            if response.status_code in [200, 201]:
                self.search_updates += 1
                logger.info(f"Indexed asset for search: {asset_metadata.get('asset_id')}")
                return True
            else:
                logger.error(f"Failed to index asset: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error indexing asset for search: {e}")
            return False
            
    async def update_asset_search_index(self, asset_id: str) -> bool:
        """Update asset in search index"""
        try:
            # Get latest asset data including quality score
            response = await self.graph_intelligence_client.get(
                f"/api/v1/asset-lineage/assets/{asset_id}/lineage",
                params={"depth": 1}
            )
            
            if response.status_code == 200:
                asset_data = response.json().get("asset", {})
                
                # Update search index
                update_doc = {
                    "id": asset_id,
                    "type": "digital_asset",
                    "updates": {
                        "quality_score": asset_data.get("quality_score", 0),
                        "trust_score": asset_data.get("trust_score", 0),
                        "review_count": len(response.json().get("reviews", [])),
                        "transaction_count": len(response.json().get("transactions", []))
                    }
                }
                
                search_response = await self.search_service_client.put(
                    f"/api/v1/search/update/{asset_id}",
                    json=update_doc
                )
                
                if search_response.status_code == 200:
                    self.search_updates += 1
                    return True
                    
            return False
            
        except Exception as e:
            logger.error(f"Error updating asset search index: {e}")
            return False
            
    async def process_royalty_distribution(self, event: Dict[str, Any]) -> bool:
        """Process royalty distribution event"""
        try:
            # Create royalty event
            royalty_event = {
                "event_type": AssetEventType.ROYALTY_DISTRIBUTED.value,
                "asset_id": event.get("asset_id"),
                "transaction_id": event.get("transaction_id"),
                "timestamp": datetime.utcnow().isoformat(),
                "total_amount": event.get("price", 0) * 0.1,  # 10% royalty example
                "currency": event.get("currency", "USD"),
                "distributions": event.get("royalty_distributions", [])
            }
            
            # Publish royalty event
            return await self.publish_asset_event(
                AssetEventType.ROYALTY_DISTRIBUTED,
                royalty_event
            )
            
        except Exception as e:
            logger.error(f"Error processing royalty distribution: {e}")
            return False
            
    async def get_asset_lineage(self, asset_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get asset lineage from graph database"""
        try:
            response = await self.graph_intelligence_client.get(
                f"/api/v1/asset-lineage/assets/{asset_id}/lineage",
                params={"depth": depth}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get asset lineage: {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting asset lineage: {e}")
            return {}
            
    async def analyze_asset_impact(self, asset_id: str, change_type: str = "update") -> Dict[str, Any]:
        """Analyze impact of asset changes"""
        try:
            response = await self.graph_intelligence_client.post(
                "/api/v1/asset-lineage/impact-analysis",
                json={
                    "asset_id": asset_id,
                    "change_type": change_type
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to analyze asset impact: {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing asset impact: {e}")
            return {}
            
    async def find_duplicate_assets(self, cid: str) -> List[Dict[str, Any]]:
        """Find duplicate assets by content ID"""
        try:
            response = await self.graph_intelligence_client.get(
                f"/api/v1/asset-lineage/assets/duplicates/{cid}"
            )
            
            if response.status_code == 200:
                return response.json().get("duplicates", [])
            else:
                logger.error(f"Failed to find duplicate assets: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error finding duplicate assets: {e}")
            return []
            
    async def get_user_reputation(self, user_id: str) -> Dict[str, Any]:
        """Get user reputation from graph database"""
        try:
            response = await self.graph_intelligence_client.get(
                f"/api/v1/asset-lineage/users/{user_id}/reputation"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get user reputation: {response.text}")
                return {"user_id": user_id, "reputation_score": 0.0}
                
        except Exception as e:
            logger.error(f"Error getting user reputation: {e}")
            return {"user_id": user_id, "reputation_score": 0.0}
            
    async def _monitor_integration_health(self):
        """Monitor integration health and metrics"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Log metrics
                logger.info(f"Asset Integration metrics - "
                          f"Events published: {self.events_published}, "
                          f"Events failed: {self.events_failed}, "
                          f"Lineage updates: {self.lineage_updates}, "
                          f"Search updates: {self.search_updates}")
                
                # Check service health
                services = [
                    ("event-router", self.event_router_client),
                    ("graph-intelligence", self.graph_intelligence_client),
                    ("search-service", self.search_service_client)
                ]
                
                for service_name, client in services:
                    try:
                        # Simple health check
                        response = await client.get("/health", timeout=5.0)
                        if response.status_code != 200:
                            logger.warning(f"{service_name} health check failed")
                    except Exception as e:
                        logger.error(f"{service_name} is unreachable: {e}")
                        
            except Exception as e:
                logger.error(f"Error in integration health monitor: {e}")
                
    def get_metrics(self) -> Dict[str, int]:
        """Get integration metrics"""
        return {
            "events_published": self.events_published,
            "events_failed": self.events_failed,
            "lineage_updates": self.lineage_updates,
            "search_updates": self.search_updates
        } 