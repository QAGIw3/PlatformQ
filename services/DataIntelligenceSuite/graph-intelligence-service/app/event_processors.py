"""
Event Processors for Graph Intelligence Service

Handles graph updates based on platform events.
"""

import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid

from platformq_shared import (
    EventProcessor,
    event_handler,
    batch_event_handler,
    ProcessingResult,
    ProcessingStatus,
    ServiceClients
)
from platformq_events import (
    AssetCreatedEvent,
    AssetUpdatedEvent,
    AssetDeletedEvent,
    UserCreatedEvent,
    UserActivityEvent,
    ProjectCreatedEvent,
    CollaborationAddedEvent,
    SimulationCompletedEvent,
    TrustScoreUpdatedEvent,
    DataLineageEvent,
    GraphQueryRequestEvent
)

from .repository import (
    GraphNodeRepository,
    GraphEdgeRepository,
    GraphAnalyticsRepository
)
from .services.graph_processor import GraphProcessor
from .services.lineage_tracker import LineageTracker
from .services.trust_network import TrustNetworkManager

logger = logging.getLogger(__name__)


class GraphUpdateProcessor(EventProcessor):
    """Process events that update the graph"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 node_repo: GraphNodeRepository,
                 edge_repo: GraphEdgeRepository,
                 analytics_repo: GraphAnalyticsRepository,
                 graph_processor: GraphProcessor):
        super().__init__(service_name, pulsar_url)
        self.node_repo = node_repo
        self.edge_repo = edge_repo
        self.analytics_repo = analytics_repo
        self.graph_processor = graph_processor
        
    async def on_start(self):
        """Initialize processor resources"""
        logger.info("Starting graph update processor")
        
    async def on_stop(self):
        """Cleanup processor resources"""
        logger.info("Stopping graph update processor")
        
    @event_handler("persistent://platformq/*/asset-created-events", AssetCreatedEvent)
    async def handle_asset_created(self, event: AssetCreatedEvent, msg):
        """Create asset node in graph"""
        try:
            # Create asset node
            node = self.node_repo.create_node(
                node_type="Asset",
                properties={
                    "asset_id": event.asset_id,
                    "name": event.name,
                    "file_type": event.file_type,
                    "creator_id": event.creator_id,
                    "tags": event.tags,
                    "storage_path": event.storage_path
                },
                tenant_id=event.tenant_id
            )
            
            # Create edge to creator
            self.edge_repo.create_edge(
                from_node_id=event.creator_id,
                to_node_id=event.asset_id,
                edge_type="created",
                properties={
                    "created_at": event.created_at
                },
                tenant_id=event.tenant_id
            )
            
            # Process tags as nodes
            for tag in event.tags:
                tag_node_id = f"tag_{tag}"
                
                # Create or update tag node
                existing_tag = self.node_repo.get_node(tag_node_id, event.tenant_id)
                if not existing_tag:
                    self.node_repo.create_node(
                        node_type="Tag",
                        properties={
                            "tag_id": tag_node_id,
                            "name": tag
                        },
                        tenant_id=event.tenant_id
                    )
                    
                # Link asset to tag
                self.edge_repo.create_edge(
                    from_node_id=event.asset_id,
                    to_node_id=tag_node_id,
                    edge_type="tagged_with",
                    properties={},
                    tenant_id=event.tenant_id
                )
                
            logger.info(f"Created asset node: {event.asset_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing asset created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
    async def handle_user_created(self, event: UserCreatedEvent, msg):
        """Create user node in graph"""
        try:
            # Create user node
            node = self.node_repo.create_node(
                node_type="User",
                properties={
                    "user_id": event.user_id,
                    "username": event.username,
                    "full_name": event.full_name,
                    "email": event.email,
                    "roles": event.roles
                },
                tenant_id=event.tenant_id
            )
            
            # Initialize trust score
            self.edge_repo.create_edge(
                from_node_id=event.user_id,
                to_node_id="trust_network",
                edge_type="has_trust_score",
                properties={
                    "score": 0.5,  # Initial trust score
                    "updated_at": event.created_at
                },
                tenant_id=event.tenant_id
            )
            
            logger.info(f"Created user node: {event.user_id}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing user created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/project-created-events", ProjectCreatedEvent)
    async def handle_project_created(self, event: ProjectCreatedEvent, msg):
        """Create project node and relationships"""
        try:
            # Create project node
            node = self.node_repo.create_node(
                node_type="Project",
                properties={
                    "project_id": event.project_id,
                    "name": event.project_name,
                    "description": event.description,
                    "project_type": event.project_type,
                    "creator_id": event.creator_id
                },
                tenant_id=event.tenant_id
            )
            
            # Link creator
            self.edge_repo.create_edge(
                from_node_id=event.creator_id,
                to_node_id=event.project_id,
                edge_type="created_project",
                properties={
                    "created_at": event.created_at
                },
                tenant_id=event.tenant_id
            )
            
            # Link collaborators
            for collaborator_id in event.collaborators:
                self.edge_repo.create_edge(
                    from_node_id=collaborator_id,
                    to_node_id=event.project_id,
                    edge_type="collaborates_on",
                    properties={
                        "role": "member",
                        "joined_at": event.created_at
                    },
                    tenant_id=event.tenant_id
                )
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing project created event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @batch_event_handler(
        "persistent://platformq/*/user-activity-events",
        UserActivityEvent,
        max_batch_size=100,
        max_wait_time_ms=5000
    )
    async def handle_user_activities(self, events: List[UserActivityEvent], msgs):
        """Process batch of user activities"""
        try:
            # Group by activity type for efficient processing
            by_type = {}
            for event in events:
                activity_type = event.activity_type
                if activity_type not in by_type:
                    by_type[activity_type] = []
                by_type[activity_type].append(event)
                
            # Process each type
            for activity_type, type_events in by_type.items():
                if activity_type == "viewed":
                    await self._process_view_activities(type_events)
                elif activity_type == "downloaded":
                    await self._process_download_activities(type_events)
                elif activity_type == "shared":
                    await self._process_share_activities(type_events)
                    
            logger.info(f"Processed {len(events)} user activities")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing user activities: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    @event_handler("persistent://platformq/*/simulation-completed-events", SimulationCompletedEvent)
    async def handle_simulation_completed(self, event: SimulationCompletedEvent, msg):
        """Create simulation result nodes and relationships"""
        try:
            # Create simulation result node
            result_node_id = f"sim_result_{event.simulation_id}"
            
            node = self.node_repo.create_node(
                node_type="SimulationResult",
                properties={
                    "simulation_id": event.simulation_id,
                    "simulation_name": event.simulation_name,
                    "status": event.status,
                    "duration_seconds": event.duration_seconds,
                    "result_path": event.result_path,
                    "metrics": event.metrics
                },
                tenant_id=event.tenant_id
            )
            
            # Link to simulation
            self.edge_repo.create_edge(
                from_node_id=event.simulation_id,
                to_node_id=result_node_id,
                edge_type="produced_result",
                properties={
                    "completed_at": event.completed_at
                },
                tenant_id=event.tenant_id
            )
            
            # Link to owner
            self.edge_repo.create_edge(
                from_node_id=event.owner_id,
                to_node_id=result_node_id,
                edge_type="owns_result",
                properties={},
                tenant_id=event.tenant_id
            )
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing simulation completed event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _process_view_activities(self, events: List[UserActivityEvent]):
        """Process view activities"""
        for event in events:
            # Create lightweight "viewed" edges
            self.edge_repo.create_edge(
                from_node_id=event.user_id,
                to_node_id=event.target_id,
                edge_type="viewed",
                properties={
                    "timestamp": event.timestamp,
                    "duration": event.metadata.get("duration", 0)
                },
                tenant_id=event.tenant_id
            )
            
    async def _process_download_activities(self, events: List[UserActivityEvent]):
        """Process download activities"""
        for event in events:
            self.edge_repo.create_edge(
                from_node_id=event.user_id,
                to_node_id=event.target_id,
                edge_type="downloaded",
                properties={
                    "timestamp": event.timestamp,
                    "version": event.metadata.get("version", "latest")
                },
                tenant_id=event.tenant_id
            )
            
    async def _process_share_activities(self, events: List[UserActivityEvent]):
        """Process share activities"""
        for event in events:
            # Create share relationship
            self.edge_repo.create_edge(
                from_node_id=event.user_id,
                to_node_id=event.target_id,
                edge_type="shared",
                properties={
                    "timestamp": event.timestamp,
                    "shared_with": event.metadata.get("recipients", [])
                },
                tenant_id=event.tenant_id
            )


class LineageProcessor(EventProcessor):
    """Process data lineage events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 node_repo: GraphNodeRepository,
                 edge_repo: GraphEdgeRepository,
                 lineage_tracker: LineageTracker):
        super().__init__(service_name, pulsar_url)
        self.node_repo = node_repo
        self.edge_repo = edge_repo
        self.lineage_tracker = lineage_tracker
        
    @event_handler("persistent://platformq/*/data-lineage-events", DataLineageEvent)
    async def handle_lineage_event(self, event: DataLineageEvent, msg):
        """Track data lineage in graph"""
        try:
            # Create lineage edge
            self.edge_repo.create_edge(
                from_node_id=event.source_id,
                to_node_id=event.target_id,
                edge_type="derived_from",
                properties={
                    "transformation": event.transformation_type,
                    "timestamp": event.timestamp,
                    "metadata": event.metadata
                },
                tenant_id=event.tenant_id
            )
            
            # Update lineage tracker
            await self.lineage_tracker.track_transformation(
                source_id=event.source_id,
                target_id=event.target_id,
                transformation_type=event.transformation_type,
                metadata=event.metadata
            )
            
            # Check for lineage cycles or issues
            lineage_path = await self.lineage_tracker.get_full_lineage(
                event.target_id,
                event.tenant_id
            )
            
            if self._has_cycle(lineage_path):
                logger.warning(f"Lineage cycle detected for {event.target_id}")
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing lineage event: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    def _has_cycle(self, lineage_path: List[str]) -> bool:
        """Check if lineage path has a cycle"""
        return len(lineage_path) != len(set(lineage_path))


class TrustNetworkProcessor(EventProcessor):
    """Process trust network events"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 edge_repo: GraphEdgeRepository,
                 trust_manager: TrustNetworkManager,
                 service_clients: ServiceClients):
        super().__init__(service_name, pulsar_url)
        self.edge_repo = edge_repo
        self.trust_manager = trust_manager
        self.service_clients = service_clients
        
    @event_handler("persistent://platformq/*/trust-score-updated-events", TrustScoreUpdatedEvent)
    async def handle_trust_update(self, event: TrustScoreUpdatedEvent, msg):
        """Update trust scores in graph"""
        try:
            # Update trust edge
            existing_edges = self.edge_repo.get_edges(
                event.entity_id,
                direction="out",
                edge_type="has_trust_score",
                tenant_id=event.tenant_id
            )
            
            if existing_edges:
                # Update existing edge
                edge_id = existing_edges[0]['edge_id']
                self.edge_repo.delete_edge(edge_id, event.tenant_id)
                
            # Create new trust edge
            self.edge_repo.create_edge(
                from_node_id=event.entity_id,
                to_node_id="trust_network",
                edge_type="has_trust_score",
                properties={
                    "score": event.new_score,
                    "previous_score": event.previous_score,
                    "reason": event.reason,
                    "updated_at": event.updated_at
                },
                tenant_id=event.tenant_id
            )
            
            # Update trust network
            await self.trust_manager.update_trust_score(
                entity_id=event.entity_id,
                new_score=event.new_score,
                reason=event.reason
            )
            
            # Check for trust-based insights
            if event.new_score > 0.9:
                # High trust entity
                await self._notify_high_trust_entity(event)
            elif event.new_score < 0.2:
                # Low trust entity
                await self._notify_low_trust_entity(event)
                
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing trust update: {e}")
            return ProcessingResult(
                status=ProcessingStatus.RETRY,
                message=str(e)
            )
            
    async def _notify_high_trust_entity(self, event: TrustScoreUpdatedEvent):
        """Handle high trust entity"""
        # Could trigger special privileges or badges
        logger.info(f"Entity {event.entity_id} achieved high trust score: {event.new_score}")
        
    async def _notify_low_trust_entity(self, event: TrustScoreUpdatedEvent):
        """Handle low trust entity"""
        # Could trigger restrictions or reviews
        logger.warning(f"Entity {event.entity_id} has low trust score: {event.new_score}")


class GraphQueryProcessor(EventProcessor):
    """Process graph query requests"""
    
    def __init__(self, service_name: str, pulsar_url: str,
                 analytics_repo: GraphAnalyticsRepository,
                 graph_processor: GraphProcessor):
        super().__init__(service_name, pulsar_url)
        self.analytics_repo = analytics_repo
        self.graph_processor = graph_processor
        
    @event_handler("persistent://platformq/*/graph-query-request-events", GraphQueryRequestEvent)
    async def handle_query_request(self, event: GraphQueryRequestEvent, msg):
        """Process graph query requests"""
        try:
            result = None
            
            if event.query_type == "shortest_path":
                result = self.analytics_repo.find_paths(
                    event.parameters["from_node_id"],
                    event.parameters["to_node_id"],
                    event.tenant_id,
                    max_depth=event.parameters.get("max_depth", 5)
                )
                
            elif event.query_type == "neighborhood":
                result = self.analytics_repo.get_node_neighborhood(
                    event.parameters["node_id"],
                    event.tenant_id,
                    hops=event.parameters.get("hops", 2)
                )
                
            elif event.query_type == "community_detection":
                result = self.analytics_repo.find_communities(
                    event.tenant_id,
                    algorithm=event.parameters.get("algorithm", "label_propagation")
                )
                
            elif event.query_type == "centrality":
                result = self.analytics_repo.calculate_centrality(
                    event.tenant_id,
                    centrality_type=event.parameters.get("type", "degree")
                )
                
            # Store result if requested
            if event.store_result and result:
                insight = self.analytics_repo.generate_insight(
                    insight_type=f"query_{event.query_type}",
                    data={
                        "query_id": event.query_id,
                        "result": result,
                        "parameters": event.parameters
                    },
                    tenant_id=event.tenant_id
                )
                
            logger.info(f"Processed graph query: {event.query_type}")
            
            return ProcessingResult(status=ProcessingStatus.SUCCESS)
            
        except Exception as e:
            logger.error(f"Error processing graph query: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                message=str(e)
            ) 