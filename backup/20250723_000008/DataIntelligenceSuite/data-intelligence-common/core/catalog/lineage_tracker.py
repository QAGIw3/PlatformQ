"""
Data lineage tracking for catalog entities.

Provides comprehensive lineage tracking and impact analysis.
"""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import networkx as nx

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class LineageType(str, Enum):
    """Types of lineage relationships"""
    DATA_FLOW = "data_flow"
    DERIVES_FROM = "derives_from"
    COPIES_FROM = "copies_from"
    TRANSFORMS_TO = "transforms_to"
    AGGREGATES_FROM = "aggregates_from"
    JOINS_WITH = "joins_with"
    FILTERS_FROM = "filters_from"
    SAMPLES_FROM = "samples_from"
    VERSION_OF = "version_of"
    REPLACES = "replaces"
    CUSTOM = "custom"


class LineageDirection(str, Enum):
    """Direction of lineage traversal"""
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"
    BOTH = "both"


@dataclass
class LineageNode:
    """Represents a node in the lineage graph"""
    entity_id: str
    entity_type: str
    entity_name: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "entity_id": self.entity_id,
            "entity_type": self.entity_type,
            "entity_name": self.entity_name,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class LineageEdge:
    """Represents an edge in the lineage graph"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_id: str = ""
    target_id: str = ""
    lineage_type: LineageType = LineageType.DATA_FLOW
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    confidence: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "lineage_type": self.lineage_type.value,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "confidence": self.confidence
        }


@dataclass
class LineageGraph:
    """Lineage graph representation"""
    nodes: List[LineageNode] = field(default_factory=list)
    edges: List[LineageEdge] = field(default_factory=list)
    root_id: Optional[str] = None
    depth: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "root_id": self.root_id,
            "depth": self.depth,
            "metadata": self.metadata
        }


class LineageTracker:
    """
    Tracks data lineage for catalog entities.
    
    Features:
    - Lineage graph management
    - Impact analysis
    - Lineage traversal
    - Cycle detection
    - Event publishing
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # NetworkX graph for efficient operations
        self.graph = nx.DiGraph()
        
        # Storage
        self._nodes: Dict[str, LineageNode] = {}
        self._edges: Dict[str, LineageEdge] = {}
        self._edge_index: Dict[Tuple[str, str], str] = {}  # (source, target) -> edge_id
        
    def add_node(
        self,
        entity_id: str,
        entity_type: str,
        entity_name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> LineageNode:
        """Add node to lineage graph"""
        node = LineageNode(
            entity_id=entity_id,
            entity_type=entity_type,
            entity_name=entity_name,
            metadata=metadata or {}
        )
        
        self._nodes[entity_id] = node
        self.graph.add_node(entity_id, **node.to_dict())
        
        # Cache node
        if self.cache:
            cache_key = f"lineage:node:{entity_id}"
            self.cache.set(cache_key, node.to_dict(), ttl=3600)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="lineage.node.added",
                source="lineage_tracker",
                data={"entity_id": entity_id}
            ))
            
        logger.info(f"Added lineage node: {entity_id}")
        return node
        
    def add_edge(
        self,
        source_id: str,
        target_id: str,
        lineage_type: LineageType = LineageType.DATA_FLOW,
        metadata: Optional[Dict[str, Any]] = None,
        confidence: float = 1.0,
        user: Optional[str] = None
    ) -> LineageEdge:
        """Add edge to lineage graph"""
        # Ensure nodes exist
        if source_id not in self._nodes or target_id not in self._nodes:
            raise ValueError("Source or target node not found")
            
        # Check for existing edge
        edge_key = (source_id, target_id)
        if edge_key in self._edge_index:
            edge_id = self._edge_index[edge_key]
            edge = self._edges[edge_id]
            edge.updated_at = datetime.utcnow()
            edge.metadata.update(metadata or {})
            return edge
            
        # Create new edge
        edge = LineageEdge(
            source_id=source_id,
            target_id=target_id,
            lineage_type=lineage_type,
            metadata=metadata or {},
            confidence=confidence,
            created_by=user
        )
        
        self._edges[edge.id] = edge
        self._edge_index[edge_key] = edge.id
        self.graph.add_edge(source_id, target_id, **edge.to_dict())
        
        # Check for cycles
        if self._creates_cycle(source_id, target_id):
            logger.warning(f"Edge creates cycle: {source_id} -> {target_id}")
            
        # Cache edge
        if self.cache:
            cache_key = f"lineage:edge:{edge.id}"
            self.cache.set(cache_key, edge.to_dict(), ttl=3600)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="lineage.edge.added",
                source="lineage_tracker",
                data={
                    "edge_id": edge.id,
                    "source_id": source_id,
                    "target_id": target_id
                }
            ))
            
        logger.info(f"Added lineage edge: {source_id} -> {target_id}")
        return edge
        
    def remove_node(self, entity_id: str) -> bool:
        """Remove node and its edges from lineage graph"""
        if entity_id not in self._nodes:
            return False
            
        # Remove edges
        edges_to_remove = []
        for edge_key, edge_id in self._edge_index.items():
            if edge_key[0] == entity_id or edge_key[1] == entity_id:
                edges_to_remove.append((edge_key, edge_id))
                
        for edge_key, edge_id in edges_to_remove:
            del self._edge_index[edge_key]
            del self._edges[edge_id]
            
        # Remove node
        del self._nodes[entity_id]
        self.graph.remove_node(entity_id)
        
        # Clear cache
        if self.cache:
            cache_key = f"lineage:node:{entity_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="lineage.node.removed",
                source="lineage_tracker",
                data={"entity_id": entity_id}
            ))
            
        logger.info(f"Removed lineage node: {entity_id}")
        return True
        
    def remove_edge(self, source_id: str, target_id: str) -> bool:
        """Remove edge from lineage graph"""
        edge_key = (source_id, target_id)
        if edge_key not in self._edge_index:
            return False
            
        edge_id = self._edge_index[edge_key]
        del self._edge_index[edge_key]
        del self._edges[edge_id]
        
        self.graph.remove_edge(source_id, target_id)
        
        # Clear cache
        if self.cache:
            cache_key = f"lineage:edge:{edge_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="lineage.edge.removed",
                source="lineage_tracker",
                data={
                    "edge_id": edge_id,
                    "source_id": source_id,
                    "target_id": target_id
                }
            ))
            
        logger.info(f"Removed lineage edge: {source_id} -> {target_id}")
        return True
        
    def get_lineage(
        self,
        entity_id: str,
        direction: LineageDirection = LineageDirection.BOTH,
        depth: int = -1,
        lineage_types: Optional[List[LineageType]] = None
    ) -> LineageGraph:
        """Get lineage graph for entity"""
        if entity_id not in self._nodes:
            raise ValueError(f"Entity not found: {entity_id}")
            
        # Check cache
        if self.cache and depth != -1:
            cache_key = f"lineage:graph:{entity_id}:{direction.value}:{depth}"
            cached = self.cache.get(cache_key)
            if cached:
                return self._dict_to_graph(cached)
                
        # Build lineage graph
        nodes = set()
        edges = set()
        
        if direction in [LineageDirection.UPSTREAM, LineageDirection.BOTH]:
            upstream_nodes = self._traverse_upstream(entity_id, depth, lineage_types)
            nodes.update(upstream_nodes)
            
        if direction in [LineageDirection.DOWNSTREAM, LineageDirection.BOTH]:
            downstream_nodes = self._traverse_downstream(entity_id, depth, lineage_types)
            nodes.update(downstream_nodes)
            
        # Always include the root node
        nodes.add(entity_id)
        
        # Get edges between nodes
        for node_id in nodes:
            for edge_key, edge_id in self._edge_index.items():
                if edge_key[0] == node_id and edge_key[1] in nodes:
                    edge = self._edges[edge_id]
                    if not lineage_types or edge.lineage_type in lineage_types:
                        edges.add(edge_id)
                        
        # Build graph
        graph = LineageGraph(
            nodes=[self._nodes[n] for n in nodes],
            edges=[self._edges[e] for e in edges],
            root_id=entity_id,
            depth=depth,
            metadata={
                "direction": direction.value,
                "lineage_types": [t.value for t in lineage_types] if lineage_types else None
            }
        )
        
        # Cache result
        if self.cache and depth != -1:
            cache_key = f"lineage:graph:{entity_id}:{direction.value}:{depth}"
            self.cache.set(cache_key, graph.to_dict(), ttl=300)
            
        return graph
        
    def _traverse_upstream(
        self,
        entity_id: str,
        depth: int,
        lineage_types: Optional[List[LineageType]] = None,
        visited: Optional[Set[str]] = None
    ) -> Set[str]:
        """Traverse upstream lineage"""
        if visited is None:
            visited = set()
            
        if entity_id in visited or (depth == 0):
            return visited
            
        visited.add(entity_id)
        
        # Get predecessors
        for predecessor in self.graph.predecessors(entity_id):
            edge_key = (predecessor, entity_id)
            if edge_key in self._edge_index:
                edge = self._edges[self._edge_index[edge_key]]
                if not lineage_types or edge.lineage_type in lineage_types:
                    self._traverse_upstream(
                        predecessor,
                        depth - 1 if depth > 0 else -1,
                        lineage_types,
                        visited
                    )
                    
        return visited
        
    def _traverse_downstream(
        self,
        entity_id: str,
        depth: int,
        lineage_types: Optional[List[LineageType]] = None,
        visited: Optional[Set[str]] = None
    ) -> Set[str]:
        """Traverse downstream lineage"""
        if visited is None:
            visited = set()
            
        if entity_id in visited or (depth == 0):
            return visited
            
        visited.add(entity_id)
        
        # Get successors
        for successor in self.graph.successors(entity_id):
            edge_key = (entity_id, successor)
            if edge_key in self._edge_index:
                edge = self._edges[self._edge_index[edge_key]]
                if not lineage_types or edge.lineage_type in lineage_types:
                    self._traverse_downstream(
                        successor,
                        depth - 1 if depth > 0 else -1,
                        lineage_types,
                        visited
                    )
                    
        return visited
        
    def get_impact_analysis(
        self,
        entity_id: str,
        change_type: str = "schema_change"
    ) -> Dict[str, Any]:
        """Analyze impact of changes to entity"""
        # Get downstream entities
        downstream_graph = self.get_lineage(
            entity_id,
            direction=LineageDirection.DOWNSTREAM,
            depth=-1
        )
        
        impact = {
            "entity_id": entity_id,
            "change_type": change_type,
            "directly_impacted": [],
            "indirectly_impacted": [],
            "impact_summary": {},
            "risk_level": "low"
        }
        
        # Analyze direct impacts
        for edge in downstream_graph.edges:
            if edge.source_id == entity_id:
                target_node = next(
                    (n for n in downstream_graph.nodes if n.entity_id == edge.target_id),
                    None
                )
                if target_node:
                    impact["directly_impacted"].append({
                        "entity_id": target_node.entity_id,
                        "entity_name": target_node.entity_name,
                        "entity_type": target_node.entity_type,
                        "lineage_type": edge.lineage_type.value,
                        "confidence": edge.confidence
                    })
                    
        # Analyze indirect impacts
        all_downstream = set(n.entity_id for n in downstream_graph.nodes)
        all_downstream.discard(entity_id)
        direct_ids = set(i["entity_id"] for i in impact["directly_impacted"])
        indirect_ids = all_downstream - direct_ids
        
        for node_id in indirect_ids:
            node = next(
                (n for n in downstream_graph.nodes if n.entity_id == node_id),
                None
            )
            if node:
                impact["indirectly_impacted"].append({
                    "entity_id": node.entity_id,
                    "entity_name": node.entity_name,
                    "entity_type": node.entity_type
                })
                
        # Calculate impact summary
        impact["impact_summary"] = {
            "total_impacted": len(all_downstream),
            "directly_impacted": len(impact["directly_impacted"]),
            "indirectly_impacted": len(impact["indirectly_impacted"]),
            "by_type": {}
        }
        
        # Count by entity type
        for node in downstream_graph.nodes:
            if node.entity_id != entity_id:
                entity_type = node.entity_type
                impact["impact_summary"]["by_type"][entity_type] = \
                    impact["impact_summary"]["by_type"].get(entity_type, 0) + 1
                    
        # Determine risk level
        total_impacted = impact["impact_summary"]["total_impacted"]
        if total_impacted > 50:
            impact["risk_level"] = "critical"
        elif total_impacted > 20:
            impact["risk_level"] = "high"
        elif total_impacted > 5:
            impact["risk_level"] = "medium"
            
        return impact
        
    def find_common_ancestors(
        self,
        entity_ids: List[str]
    ) -> List[str]:
        """Find common ancestors of multiple entities"""
        if not entity_ids:
            return []
            
        # Get ancestors for each entity
        ancestor_sets = []
        for entity_id in entity_ids:
            ancestors = self._traverse_upstream(entity_id, -1)
            ancestor_sets.append(ancestors)
            
        # Find intersection
        common_ancestors = set.intersection(*ancestor_sets) if ancestor_sets else set()
        
        # Remove the input entities themselves
        for entity_id in entity_ids:
            common_ancestors.discard(entity_id)
            
        return list(common_ancestors)
        
    def find_paths(
        self,
        source_id: str,
        target_id: str,
        max_paths: int = 10
    ) -> List[List[str]]:
        """Find paths between two entities"""
        if source_id not in self._nodes or target_id not in self._nodes:
            return []
            
        try:
            # Find all simple paths
            paths = list(nx.all_simple_paths(
                self.graph,
                source_id,
                target_id,
                cutoff=10  # Maximum path length
            ))
            
            # Sort by length and return top paths
            paths.sort(key=len)
            return paths[:max_paths]
        except nx.NetworkXNoPath:
            return []
            
    def detect_cycles(self) -> List[List[str]]:
        """Detect cycles in lineage graph"""
        try:
            cycles = list(nx.simple_cycles(self.graph))
            return cycles
        except:
            return []
            
    def _creates_cycle(self, source_id: str, target_id: str) -> bool:
        """Check if adding edge would create cycle"""
        # Temporarily add edge
        self.graph.add_edge(source_id, target_id)
        
        # Check for cycle
        has_cycle = not nx.is_directed_acyclic_graph(self.graph)
        
        # Remove temporary edge
        self.graph.remove_edge(source_id, target_id)
        
        return has_cycle
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get lineage graph statistics"""
        return {
            "total_nodes": len(self._nodes),
            "total_edges": len(self._edges),
            "avg_degree": sum(dict(self.graph.degree()).values()) / len(self._nodes) if self._nodes else 0,
            "max_in_degree": max(dict(self.graph.in_degree()).values()) if self._nodes else 0,
            "max_out_degree": max(dict(self.graph.out_degree()).values()) if self._nodes else 0,
            "has_cycles": not nx.is_directed_acyclic_graph(self.graph),
            "connected_components": nx.number_weakly_connected_components(self.graph),
            "lineage_types": {
                lt.value: sum(1 for e in self._edges.values() if e.lineage_type == lt)
                for lt in LineageType
            }
        }
        
    def export_graph(self, format: str = "json") -> Union[Dict[str, Any], str]:
        """Export lineage graph"""
        if format == "json":
            return {
                "nodes": {k: v.to_dict() for k, v in self._nodes.items()},
                "edges": {k: v.to_dict() for k, v in self._edges.items()},
                "statistics": self.get_statistics()
            }
        elif format == "dot":
            # Export as Graphviz DOT format
            return nx.drawing.nx_pydot.to_pydot(self.graph).to_string()
        else:
            raise ValueError(f"Unsupported format: {format}")
            
    def import_graph(self, data: Dict[str, Any]):
        """Import lineage graph"""
        # Clear existing data
        self._nodes.clear()
        self._edges.clear()
        self._edge_index.clear()
        self.graph.clear()
        
        # Import nodes
        for node_id, node_data in data.get("nodes", {}).items():
            node = LineageNode(
                entity_id=node_data["entity_id"],
                entity_type=node_data["entity_type"],
                entity_name=node_data["entity_name"],
                metadata=node_data.get("metadata", {}),
                created_at=datetime.fromisoformat(node_data["created_at"]),
                updated_at=datetime.fromisoformat(node_data["updated_at"])
            )
            self._nodes[node_id] = node
            self.graph.add_node(node_id, **node.to_dict())
            
        # Import edges
        for edge_id, edge_data in data.get("edges", {}).items():
            edge = LineageEdge(
                id=edge_id,
                source_id=edge_data["source_id"],
                target_id=edge_data["target_id"],
                lineage_type=LineageType(edge_data["lineage_type"]),
                metadata=edge_data.get("metadata", {}),
                created_at=datetime.fromisoformat(edge_data["created_at"]),
                created_by=edge_data.get("created_by"),
                confidence=edge_data.get("confidence", 1.0)
            )
            self._edges[edge_id] = edge
            self._edge_index[(edge.source_id, edge.target_id)] = edge_id
            self.graph.add_edge(edge.source_id, edge.target_id, **edge.to_dict())
            
    def _dict_to_graph(self, data: Dict[str, Any]) -> LineageGraph:
        """Convert dictionary to LineageGraph"""
        nodes = [
            LineageNode(**node_data)
            for node_data in data.get("nodes", [])
        ]
        
        edges = [
            LineageEdge(**edge_data)
            for edge_data in data.get("edges", [])
        ]
        
        return LineageGraph(
            nodes=nodes,
            edges=edges,
            root_id=data.get("root_id"),
            depth=data.get("depth", 0),
            metadata=data.get("metadata", {})
        ) 