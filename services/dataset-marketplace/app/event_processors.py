"""
Dataset Marketplace Event Processors

Event processing for dataset marketplace operations with trust-weighted features.
"""

import logging
import json
from typing import Dict, Any, Optional
from datetime import datetime

from platformq_shared import EventProcessor
from platformq_shared.schemas import DatasetEvent, TransactionEvent

from .models import DatasetStatus, PurchaseStatus
from .repository import (
    DatasetListingRepository,
    DatasetPurchaseRepository,
    DatasetQualityCheckRepository
)
from .engines.trust_weighted_data_engine import TrustWeightedDataEngine

logger = logging.getLogger(__name__)


class DatasetMarketplaceEventProcessor(EventProcessor):
    """Process events for dataset marketplace with trust-weighted features"""
    
    def __init__(self, pulsar_client, service_name: str, trust_engine: Optional[TrustWeightedDataEngine] = None):
        super().__init__(pulsar_client, service_name)
        self.trust_engine = trust_engine
        
        # Subscribe to relevant topics
        self.topics = [
            "dataset-events",
            "transaction-events",
            "quality-assessment-events",
            "trust-score-updates",
            "data-access-events",
            "platformq.data.marketplace.quality-assessments",
            "platformq.data.marketplace.access-requests",
            "platformq.knowledge.graph.updates"
        ]
    
    async def process_event(self, topic: str, event_data: Dict[str, Any]):
        """Process incoming events"""
        try:
            event_type = event_data.get("event_type", "")
            
            # Dataset events
            if topic == "dataset-events":
                await self._process_dataset_event(event_type, event_data)
            
            # Transaction events
            elif topic == "transaction-events":
                await self._process_transaction_event(event_type, event_data)
            
            # Quality assessment events
            elif topic in ["quality-assessment-events", "platformq.data.marketplace.quality-assessments"]:
                await self._process_quality_assessment(event_data)
            
            # Trust score updates
            elif topic == "trust-score-updates":
                await self._process_trust_update(event_data)
            
            # Data access events
            elif topic in ["data-access-events", "platformq.data.marketplace.access-requests"]:
                await self._process_access_request(event_data)
            
            # Knowledge graph updates
            elif topic == "platformq.knowledge.graph.updates":
                await self._process_graph_update(event_data)
            
        except Exception as e:
            logger.error(f"Error processing event from {topic}: {e}", exc_info=True)
    
    async def _process_dataset_event(self, event_type: str, event_data: Dict[str, Any]):
        """Process dataset-related events"""
        dataset_id = event_data.get("dataset_id")
        
        if event_type == "dataset_created":
            # Register in knowledge graph
            if self.trust_engine:
                await self.trust_engine.register_dataset_in_graph(
                    dataset_id=dataset_id,
                    dataset_info=event_data,
                    creator_id=event_data.get("seller_id")
                )
            
            # Trigger initial quality assessment
            await self._trigger_quality_assessment(dataset_id, event_data)
            
        elif event_type == "dataset_updated":
            # Update cache
            await self._update_dataset_cache(dataset_id, event_data)
            
        elif event_type == "dataset_deleted":
            # Clean up related data
            await self._cleanup_dataset(dataset_id)
    
    async def _process_transaction_event(self, event_type: str, event_data: Dict[str, Any]):
        """Process transaction events"""
        if event_type == "purchase_completed":
            purchase_id = event_data.get("purchase_id")
            dataset_id = event_data.get("dataset_id")
            buyer_id = event_data.get("buyer_id")
            
            # Create access grant
            if self.trust_engine:
                await self.trust_engine.grant_data_access(
                    dataset_id=dataset_id,
                    user_id=buyer_id,
                    access_level=event_data.get("access_level", "FULL"),
                    duration_days=event_data.get("duration_days", 365)
                )
            
            # Update analytics
            await self._update_purchase_analytics(dataset_id, event_data)
            
            # Create graph edge for purchase relationship
            await self._create_purchase_edge(buyer_id, dataset_id, purchase_id)
    
    async def _process_quality_assessment(self, event_data: Dict[str, Any]):
        """Process quality assessment events"""
        assessment_id = event_data.get("assessment_id")
        dataset_id = event_data.get("dataset_id")
        
        if not self.trust_engine:
            logger.warning("Trust engine not available for quality assessment")
            return
        
        # Register assessment in knowledge graph
        await self.trust_engine.graph_intelligence.register_quality_assessment(
            assessment_id=assessment_id,
            dataset_id=dataset_id,
            assessor_id=event_data.get("assessor_id"),
            quality_score=event_data.get("overall_quality_score"),
            dimensions=event_data.get("quality_dimensions", {})
        )
        
        # Update dataset quality metrics
        await self._update_quality_metrics(dataset_id, event_data)
        
        # Trigger pricing update based on new quality score
        await self._update_dynamic_pricing(dataset_id, event_data.get("trust_adjusted_score"))
    
    async def _process_trust_update(self, event_data: Dict[str, Any]):
        """Process trust score updates"""
        entity_id = event_data.get("entity_id")
        entity_type = event_data.get("entity_type")
        new_trust_scores = event_data.get("trust_scores", {})
        
        # Update cached trust scores
        if self.trust_engine:
            await self.trust_engine.update_cached_trust_scores(
                entity_id=entity_id,
                trust_scores=new_trust_scores
            )
        
        # If it's a dataset owner, update their datasets' trust-weighted scores
        if entity_type == "user":
            await self._update_user_datasets_trust(entity_id, new_trust_scores)
    
    async def _process_access_request(self, event_data: Dict[str, Any]):
        """Process data access requests"""
        request_id = event_data.get("request_id")
        dataset_id = event_data.get("dataset_id")
        requester_id = event_data.get("requester_id")
        
        if not self.trust_engine:
            logger.warning("Trust engine not available for access request")
            return
        
        # Evaluate access based on trust scores
        access_decision = await self.trust_engine.evaluate_access_request(
            dataset_id=dataset_id,
            user_id=requester_id,
            requested_level=event_data.get("requested_access_level"),
            purpose=event_data.get("access_purpose")
        )
        
        # Publish access decision
        await self.publish_event("data-access-decisions", {
            "request_id": request_id,
            "dataset_id": dataset_id,
            "requester_id": requester_id,
            "decision": access_decision.get("granted"),
            "reason": access_decision.get("reason"),
            "granted_level": access_decision.get("granted_level"),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def _process_graph_update(self, event_data: Dict[str, Any]):
        """Process knowledge graph updates"""
        update_type = event_data.get("update_type")
        
        if update_type == "node_created":
            # Cache new node if it's a dataset
            if event_data.get("node_type") == "dataset":
                await self._cache_dataset_node(event_data)
        
        elif update_type == "edge_created":
            # Update relationship caches
            await self._update_relationship_cache(event_data)
        
        elif update_type == "inference_generated":
            # Process new inferences about datasets
            await self._process_inference(event_data)
    
    async def _trigger_quality_assessment(self, dataset_id: str, dataset_info: Dict[str, Any]):
        """Trigger automated quality assessment for a dataset"""
        if self.trust_engine:
            # Publish event to trigger quality assessment
            await self.publish_event("dataset-uploads", {
                "dataset_id": dataset_id,
                "data_uri": dataset_info.get("download_url"),
                "format": dataset_info.get("format"),
                "schema_info": dataset_info.get("schema_info"),
                "timestamp": datetime.utcnow().isoformat()
            })
    
    async def _update_dataset_cache(self, dataset_id: str, updates: Dict[str, Any]):
        """Update dataset information in cache"""
        if self.trust_engine:
            await self.trust_engine.ignite.put(
                "datasets",
                dataset_id,
                updates,
                ttl=3600  # 1 hour cache
            )
    
    async def _cleanup_dataset(self, dataset_id: str):
        """Clean up dataset-related data"""
        if self.trust_engine:
            # Remove from caches
            await self.trust_engine.ignite.remove("datasets", dataset_id)
            await self.trust_engine.ignite.remove("quality_scores", dataset_id)
            await self.trust_engine.ignite.remove("trust_chains", dataset_id)
    
    async def _update_purchase_analytics(self, dataset_id: str, purchase_data: Dict[str, Any]):
        """Update purchase analytics"""
        # Implementation for updating analytics
        pass
    
    async def _create_purchase_edge(self, buyer_id: str, dataset_id: str, purchase_id: str):
        """Create purchase relationship in knowledge graph"""
        if self.trust_engine:
            await self.trust_engine.graph_intelligence.graph_client.create_knowledge_edge(
                source_node_id=buyer_id,
                target_node_id=dataset_id,
                edge_type="purchased",
                properties={
                    "purchase_id": purchase_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    async def _update_quality_metrics(self, dataset_id: str, assessment_data: Dict[str, Any]):
        """Update quality metrics for a dataset"""
        # Implementation for updating quality metrics
        pass
    
    async def _update_dynamic_pricing(self, dataset_id: str, quality_score: float):
        """Update dynamic pricing based on quality score"""
        if self.trust_engine:
            await self.trust_engine.update_dataset_pricing(dataset_id, quality_score)
    
    async def _update_user_datasets_trust(self, user_id: str, trust_scores: Dict[str, float]):
        """Update trust-weighted scores for all datasets owned by a user"""
        # Implementation for updating user's datasets
        pass
    
    async def _cache_dataset_node(self, node_data: Dict[str, Any]):
        """Cache dataset node from knowledge graph"""
        if self.trust_engine:
            dataset_id = node_data.get("node_id")
            await self.trust_engine.ignite.put(
                "knowledge_nodes",
                f"dataset:{dataset_id}",
                node_data.get("properties", {}),
                ttl=3600
            )
    
    async def _update_relationship_cache(self, edge_data: Dict[str, Any]):
        """Update relationship caches"""
        # Implementation for caching relationships
        pass
    
    async def _process_inference(self, inference_data: Dict[str, Any]):
        """Process inferences from knowledge graph"""
        # Implementation for processing inferences
        pass 