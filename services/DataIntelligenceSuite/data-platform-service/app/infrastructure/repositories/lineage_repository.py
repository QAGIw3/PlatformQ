"""
Lineage Repository Implementation

Handles lineage data persistence and retrieval.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
import logging
import uuid

from app.core.lineage_processor import LineageProcessor, LineageDirection, ProcessType
from app.core.atlas_client import AtlasClient
from app.services.storage import IgniteCacheAdapter

logger = logging.getLogger(__name__)


class LineageRepository:
    """
    Repository for lineage management.
    
    Handles lineage creation, traversal, and analysis.
    """
    
    def __init__(
        self,
        lineage_processor: LineageProcessor,
        atlas_client: AtlasClient,
        cache_manager: IgniteCacheAdapter
    ):
        self.lineage_processor = lineage_processor
        self.atlas_client = atlas_client
        self.cache_manager = cache_manager
        self.cache_prefix = "lineage"
        
    async def create_lineage(
        self,
        process_name: str,
        process_type: ProcessType,
        inputs: List[str],
        outputs: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create lineage relationship"""
        try:
            # Create lineage through processor
            lineage = await self.lineage_processor.create_lineage(
                process_name=process_name,
                process_type=process_type,
                inputs=inputs,
                outputs=outputs,
                metadata=metadata
            )
            
            # Cache the lineage
            for entity_guid in inputs + outputs:
                await self._invalidate_lineage_cache(entity_guid)
                
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to create lineage: {e}")
            raise
            
    async def get_lineage(
        self,
        entity_guid: str,
        direction: LineageDirection = LineageDirection.BOTH,
        depth: int = 3
    ) -> Dict[str, Any]:
        """Get lineage graph for entity"""
        try:
            # Check cache first
            cache_key = f"{self.cache_prefix}:{entity_guid}:{direction.value}:{depth}"
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
                
            # Get from processor
            lineage = await self.lineage_processor.get_lineage(
                entity_guid=entity_guid,
                direction=direction,
                depth=depth
            )
            
            # Process lineage into graph format
            graph = self._process_lineage_to_graph(lineage)
            
            # Cache it
            await self.cache_manager.set(cache_key, graph, ttl=300)  # 5 min cache
            
            return graph
            
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            raise
            
    async def get_impact_analysis(
        self,
        entity_guid: str,
        change_type: str,
        max_depth: int = 5
    ) -> Dict[str, Any]:
        """Analyze impact of changes"""
        try:
            # Get downstream lineage
            lineage = await self.get_lineage(
                entity_guid=entity_guid,
                direction=LineageDirection.DOWNSTREAM,
                depth=max_depth
            )
            
            # Analyze impact
            impact = await self.lineage_processor.analyze_impact(
                entity_guid=entity_guid,
                change_type=change_type,
                max_depth=max_depth
            )
            
            return impact
            
        except Exception as e:
            logger.error(f"Failed to analyze impact: {e}")
            raise
            
    async def get_audit_trail(
        self,
        entity_guid: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get audit trail for entity"""
        try:
            # Get audit events from Atlas
            audit_events = await self.atlas_client.get_audit_events(
                start_time=start_date,
                end_time=end_date
            )
            
            # Filter for entity
            entity_events = [
                event for event in audit_events
                if event.get('entity_guid') == entity_guid
            ]
            
            # Get lineage events
            lineage_events = await self._get_lineage_events(
                entity_guid,
                start_date,
                end_date
            )
            
            return {
                "entity_guid": entity_guid,
                "audit_events": entity_events,
                "lineage_events": lineage_events,
                "event_count": len(entity_events) + len(lineage_events)
            }
            
        except Exception as e:
            logger.error(f"Failed to get audit trail: {e}")
            raise
            
    async def find_process_by_name(
        self,
        process_name: str
    ) -> Optional[Dict[str, Any]]:
        """Find process entity by name"""
        try:
            # Search for process entity
            result = await self.atlas_client.search_entities(
                query=process_name,
                type_name="Process"
            )
            
            if result.get('entities'):
                return result['entities'][0]
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to find process: {e}")
            raise
            
    async def get_processes_for_entity(
        self,
        entity_guid: str,
        process_type: Optional[ProcessType] = None
    ) -> List[Dict[str, Any]]:
        """Get all processes related to an entity"""
        try:
            # Get lineage
            lineage = await self.get_lineage(
                entity_guid=entity_guid,
                direction=LineageDirection.BOTH,
                depth=1
            )
            
            # Extract process nodes
            processes = []
            for node in lineage.get('nodes', []):
                if node.get('type_name') == 'Process':
                    if process_type is None or node.get('process_type') == process_type.value:
                        processes.append(node)
                        
            return processes
            
        except Exception as e:
            logger.error(f"Failed to get processes: {e}")
            raise
            
    async def get_data_flow_paths(
        self,
        source_guid: str,
        target_guid: str,
        max_depth: int = 10
    ) -> List[List[str]]:
        """Find all paths between two entities"""
        try:
            # Use lineage processor to find paths
            paths = []
            
            # Get downstream lineage from source
            lineage = await self.get_lineage(
                entity_guid=source_guid,
                direction=LineageDirection.DOWNSTREAM,
                depth=max_depth
            )
            
            # Find paths to target
            paths = self._find_paths_in_graph(
                lineage,
                source_guid,
                target_guid
            )
            
            return paths
            
        except Exception as e:
            logger.error(f"Failed to get data flow paths: {e}")
            raise
            
    async def get_lineage_metrics(self) -> Dict[str, Any]:
        """Get lineage metrics"""
        try:
            # Get metrics from processor
            metrics = await self.lineage_processor.get_metrics()
            
            # Add repository-specific metrics
            cache_stats = await self.cache_manager.get_stats()
            metrics['cache_stats'] = cache_stats
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get lineage metrics: {e}")
            raise
            
    async def track_transformation(
        self,
        transformation_id: str,
        source_entities: List[str],
        target_entities: List[str],
        transformation_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Track data transformation"""
        try:
            # Create transformation record
            transformation = {
                "id": transformation_id,
                "timestamp": datetime.utcnow().isoformat(),
                "sources": source_entities,
                "targets": target_entities,
                "details": transformation_details
            }
            
            # Store in cache (in real implementation, would persist)
            cache_key = f"{self.cache_prefix}:transformation:{transformation_id}"
            await self.cache_manager.set(cache_key, transformation, ttl=None)
            
            # Create lineage
            process_name = transformation_details.get('name', f'Transformation_{transformation_id}')
            await self.create_lineage(
                process_name=process_name,
                process_type=ProcessType.ETL,
                inputs=source_entities,
                outputs=target_entities,
                metadata=transformation_details
            )
            
            return transformation
            
        except Exception as e:
            logger.error(f"Failed to track transformation: {e}")
            raise
            
    def _process_lineage_to_graph(
        self,
        lineage: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Convert lineage to graph format"""
        graph = {
            "nodes": [],
            "edges": [],
            "metadata": {}
        }
        
        # Process entities and relationships
        entities = lineage.get('entities', [])
        relations = lineage.get('relations', [])
        
        # Create nodes
        for entity in entities:
            node = {
                "guid": entity.get('guid'),
                "type_name": entity.get('typeName'),
                "qualified_name": entity.get('attributes', {}).get('qualifiedName'),
                "name": entity.get('attributes', {}).get('name'),
                "attributes": entity.get('attributes', {}),
                "classifications": entity.get('classifications', []),
                "status": entity.get('status')
            }
            graph['nodes'].append(node)
            
        # Create edges
        for relation in relations:
            edge = {
                "from_guid": relation.get('fromEntityId'),
                "to_guid": relation.get('toEntityId'),
                "relationship_type": relation.get('relationshipType'),
                "attributes": relation.get('attributes', {})
            }
            graph['edges'].append(edge)
            
        # Add metadata
        graph['metadata'] = {
            "query_time": datetime.utcnow().isoformat(),
            "node_count": len(graph['nodes']),
            "edge_count": len(graph['edges'])
        }
        
        return graph
        
    def _find_paths_in_graph(
        self,
        graph: Dict[str, Any],
        source: str,
        target: str
    ) -> List[List[str]]:
        """Find all paths between nodes in graph"""
        # Build adjacency list
        adjacency = {}
        for edge in graph.get('edges', []):
            from_node = edge['from_guid']
            to_node = edge['to_guid']
            
            if from_node not in adjacency:
                adjacency[from_node] = []
            adjacency[from_node].append(to_node)
            
        # DFS to find paths
        paths = []
        
        def dfs(current: str, path: List[str]):
            if current == target:
                paths.append(path.copy())
                return
                
            if current in adjacency:
                for neighbor in adjacency[current]:
                    if neighbor not in path:  # Avoid cycles
                        path.append(neighbor)
                        dfs(neighbor, path)
                        path.pop()
                        
        dfs(source, [source])
        return paths
        
    async def _invalidate_lineage_cache(self, entity_guid: str):
        """Invalidate lineage cache for entity"""
        try:
            # Delete all cached lineage for this entity
            pattern = f"{self.cache_prefix}:{entity_guid}:*"
            await self.cache_manager.delete_pattern(pattern)
            
        except Exception as e:
            logger.error(f"Failed to invalidate cache: {e}")
            
    async def _get_lineage_events(
        self,
        entity_guid: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> List[Dict[str, Any]]:
        """Get lineage-specific events"""
        # In a real implementation, this would query lineage event store
        # For now, return empty list
        return [] 