"""
Graph-based Data Lineage Tracker using JanusGraph
"""
import asyncio
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from enum import Enum
import json
import networkx as nx

from gremlin_python.driver import client as gremlin_client
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Cardinality
from elasticsearch import AsyncElasticsearch

from platformq_shared.utils.logger import get_logger
from platformq_shared.errors import ValidationError, NotFoundError
from ..core.cache_manager import DataCacheManager

logger = get_logger(__name__)


class LineageNodeType(str, Enum):
    """Types of lineage nodes"""
    DATASET = "dataset"
    TABLE = "table"
    COLUMN = "column"
    PROCESS = "process"
    JOB = "job"
    API = "api"
    MODEL = "model"
    REPORT = "report"
    USER = "user"


class LineageEdgeType(str, Enum):
    """Types of lineage relationships"""
    DERIVES_FROM = "derives_from"
    TRANSFORMS = "transforms"
    READS = "reads"
    WRITES = "writes"
    USES = "uses"
    OWNS = "owns"
    DEPENDS_ON = "depends_on"
    IMPACTS = "impacts"


class DataLineageTracker:
    """
    Graph-based data lineage tracking system.
    
    Features:
    - End-to-end data flow tracking
    - Column-level lineage
    - Impact analysis
    - Data provenance
    - Lineage visualization
    - Compliance tracking
    - Real-time updates
    """
    
    def __init__(self,
                 janusgraph_host: str = "janusgraph",
                 janusgraph_port: int = 8182,
                 elasticsearch_client: Optional[AsyncElasticsearch] = None,
                 cache_manager: Optional[DataCacheManager] = None):
        self.janusgraph_host = janusgraph_host
        self.janusgraph_port = janusgraph_port
        self.es_client = elasticsearch_client
        self.cache = cache_manager
        
        # Gremlin client
        self.gremlin_client = None
        
        # NetworkX for local graph operations
        self.local_graph = nx.DiGraph()
        
        # Lineage index in Elasticsearch
        self.lineage_index = "platformq_lineage_events"
        
        # Statistics
        self.stats = {
            "nodes_created": 0,
            "edges_created": 0,
            "lineage_queries": 0,
            "impact_analyses": 0
        }
    
    async def initialize(self) -> None:
        """Initialize the lineage tracker"""
        try:
            # Connect to JanusGraph via Gremlin
            self.gremlin_client = gremlin_client.Client(
                f'ws://{self.janusgraph_host}:{self.janusgraph_port}/gremlin',
                'g'
            )
            
            # Create graph schema
            await self._create_graph_schema()
            
            # Create Elasticsearch index for lineage events
            if self.es_client:
                await self._create_lineage_index()
            
            logger.info("Data lineage tracker initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize lineage tracker: {e}")
            raise
    
    async def _create_graph_schema(self) -> None:
        """Create JanusGraph schema"""
        try:
            # Create vertex labels
            for node_type in LineageNodeType:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.getVertexLabel('{node_type.value}')) {{ " \
                       f"mgmt.makeVertexLabel('{node_type.value}').make() }}; " \
                       f"mgmt.commit()"
                self.gremlin_client.submit(query).all().result()
            
            # Create edge labels
            for edge_type in LineageEdgeType:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.getEdgeLabel('{edge_type.value}')) {{ " \
                       f"mgmt.makeEdgeLabel('{edge_type.value}').make() }}; " \
                       f"mgmt.commit()"
                self.gremlin_client.submit(query).all().result()
            
            # Create property keys
            properties = [
                ("name", "String"),
                ("type", "String"),
                ("location", "String"),
                ("tenant_id", "String"),
                ("created_at", "Date"),
                ("updated_at", "Date"),
                ("metadata", "String")  # JSON string
            ]
            
            for prop_name, prop_type in properties:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.getPropertyKey('{prop_name}')) {{ " \
                       f"mgmt.makePropertyKey('{prop_name}').dataType({prop_type}.class).make() }}; " \
                       f"mgmt.commit()"
                self.gremlin_client.submit(query).all().result()
            
            # Create indices
            indices = [
                ("byName", "name"),
                ("byTenantId", "tenant_id"),
                ("byType", "type")
            ]
            
            for index_name, prop_name in indices:
                query = f"mgmt = graph.openManagement(); " \
                       f"if (!mgmt.getGraphIndex('{index_name}')) {{ " \
                       f"mgmt.buildIndex('{index_name}', Vertex.class)" \
                       f".addKey(mgmt.getPropertyKey('{prop_name}')).buildCompositeIndex() }}; " \
                       f"mgmt.commit()"
                self.gremlin_client.submit(query).all().result()
            
            logger.info("JanusGraph schema created")
            
        except Exception as e:
            logger.error(f"Failed to create graph schema: {e}")
            raise
    
    async def _create_lineage_index(self) -> None:
        """Create Elasticsearch index for lineage events"""
        if not self.es_client:
            return
        
        mapping = {
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "source_id": {"type": "keyword"},
                    "target_id": {"type": "keyword"},
                    "process_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "metadata": {"type": "object", "enabled": False}
                }
            }
        }
        
        if not await self.es_client.indices.exists(index=self.lineage_index):
            await self.es_client.indices.create(index=self.lineage_index, body=mapping)
            logger.info(f"Created lineage index: {self.lineage_index}")
    
    async def add_node(self,
                      node_id: str,
                      node_type: LineageNodeType,
                      name: str,
                      tenant_id: str,
                      metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add a node to the lineage graph"""
        try:
            # Create vertex in JanusGraph
            properties = {
                "id": node_id,
                "name": name,
                "type": node_type.value,
                "tenant_id": tenant_id,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            if metadata:
                properties["metadata"] = json.dumps(metadata)
            
            # Build Gremlin query
            query = "g.addV(label)"
            params = {"label": node_type.value}
            
            for key, value in properties.items():
                query += f".property('{key}', {key}_value)"
                params[f"{key}_value"] = value
            
            result = self.gremlin_client.submit(query, params).all().result()
            
            # Add to local graph
            self.local_graph.add_node(node_id, **properties)
            
            # Update statistics
            self.stats["nodes_created"] += 1
            
            # Cache node data
            if self.cache:
                await self.cache.set_metadata("lineage_node", node_id, properties)
            
            logger.info(f"Added lineage node: {node_id} ({node_type.value})")
            
            return properties
            
        except Exception as e:
            logger.error(f"Failed to add lineage node: {e}")
            raise
    
    async def add_edge(self,
                      source_id: str,
                      target_id: str,
                      edge_type: LineageEdgeType,
                      metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add an edge between nodes"""
        try:
            # Create edge in JanusGraph
            edge_properties = {
                "type": edge_type.value,
                "created_at": datetime.utcnow().isoformat()
            }
            
            if metadata:
                edge_properties["metadata"] = json.dumps(metadata)
            
            # Build Gremlin query
            query = "g.V().has('id', source_id).as('source')" \
                   ".V().has('id', target_id).as('target')" \
                   ".addE(edge_label).from('source').to('target')"
            
            params = {
                "source_id": source_id,
                "target_id": target_id,
                "edge_label": edge_type.value
            }
            
            for key, value in edge_properties.items():
                query += f".property('{key}', {key}_value)"
                params[f"{key}_value"] = value
            
            result = self.gremlin_client.submit(query, params).all().result()
            
            # Add to local graph
            self.local_graph.add_edge(source_id, target_id, **edge_properties)
            
            # Log lineage event
            if self.es_client:
                await self._log_lineage_event(source_id, target_id, edge_type, metadata)
            
            # Update statistics
            self.stats["edges_created"] += 1
            
            logger.info(f"Added lineage edge: {source_id} -> {target_id} ({edge_type.value})")
            
            return edge_properties
            
        except Exception as e:
            logger.error(f"Failed to add lineage edge: {e}")
            raise
    
    async def track_transformation(self,
                                 source_datasets: List[str],
                                 target_dataset: str,
                                 process_id: str,
                                 process_type: str,
                                 tenant_id: str,
                                 column_mappings: Optional[Dict[str, List[str]]] = None) -> None:
        """Track a data transformation"""
        try:
            # Create process node
            await self.add_node(
                node_id=process_id,
                node_type=LineageNodeType.PROCESS,
                name=f"{process_type}_process",
                tenant_id=tenant_id,
                metadata={
                    "process_type": process_type,
                    "start_time": datetime.utcnow().isoformat()
                }
            )
            
            # Link sources to process
            for source in source_datasets:
                await self.add_edge(
                    source_id=source,
                    target_id=process_id,
                    edge_type=LineageEdgeType.READS
                )
            
            # Link process to target
            await self.add_edge(
                source_id=process_id,
                target_id=target_dataset,
                edge_type=LineageEdgeType.WRITES
            )
            
            # Track column-level lineage if provided
            if column_mappings:
                await self._track_column_lineage(
                    source_datasets,
                    target_dataset,
                    column_mappings,
                    process_id,
                    tenant_id
                )
            
            logger.info(f"Tracked transformation: {source_datasets} -> {target_dataset}")
            
        except Exception as e:
            logger.error(f"Failed to track transformation: {e}")
            raise
    
    async def _track_column_lineage(self,
                                  source_datasets: List[str],
                                  target_dataset: str,
                                  column_mappings: Dict[str, List[str]],
                                  process_id: str,
                                  tenant_id: str) -> None:
        """Track column-level lineage"""
        for target_col, source_info in column_mappings.items():
            # Create target column node
            target_col_id = f"{target_dataset}.{target_col}"
            await self.add_node(
                node_id=target_col_id,
                node_type=LineageNodeType.COLUMN,
                name=target_col,
                tenant_id=tenant_id,
                metadata={"dataset": target_dataset}
            )
            
            # Link target column to dataset
            await self.add_edge(
                source_id=target_dataset,
                target_id=target_col_id,
                edge_type=LineageEdgeType.USES
            )
            
            # Link source columns
            for source_col_ref in source_info:
                if "." in source_col_ref:
                    source_dataset, source_col = source_col_ref.split(".", 1)
                    source_col_id = f"{source_dataset}.{source_col}"
                    
                    # Create source column node
                    await self.add_node(
                        node_id=source_col_id,
                        node_type=LineageNodeType.COLUMN,
                        name=source_col,
                        tenant_id=tenant_id,
                        metadata={"dataset": source_dataset}
                    )
                    
                    # Link source column to its dataset
                    await self.add_edge(
                        source_id=source_dataset,
                        target_id=source_col_id,
                        edge_type=LineageEdgeType.USES
                    )
                    
                    # Link source to target through process
                    await self.add_edge(
                        source_id=source_col_id,
                        target_id=target_col_id,
                        edge_type=LineageEdgeType.DERIVES_FROM,
                        metadata={"process_id": process_id}
                    )
    
    async def get_upstream_lineage(self,
                                 node_id: str,
                                 max_depth: int = 5) -> Dict[str, Any]:
        """Get upstream lineage for a node"""
        try:
            self.stats["lineage_queries"] += 1
            
            # Query upstream nodes
            query = f"g.V().has('id', '{node_id}').repeat(__.in()).times({max_depth})" \
                   f".path().by(valueMap('id', 'name', 'type'))"
            
            paths = self.gremlin_client.submit(query).all().result()
            
            # Build lineage tree
            lineage = {
                "node_id": node_id,
                "upstream_nodes": [],
                "paths": []
            }
            
            unique_nodes = set()
            
            for path in paths:
                path_nodes = []
                for node in path:
                    node_data = {
                        "id": node["id"][0],
                        "name": node["name"][0],
                        "type": node["type"][0]
                    }
                    path_nodes.append(node_data)
                    unique_nodes.add(node["id"][0])
                
                lineage["paths"].append(path_nodes)
            
            lineage["upstream_nodes"] = list(unique_nodes)
            
            # Cache result
            if self.cache:
                await self.cache.set_metadata(
                    "upstream_lineage",
                    node_id,
                    lineage,
                    ttl=300  # 5 minutes
                )
            
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to get upstream lineage: {e}")
            raise
    
    async def get_downstream_lineage(self,
                                   node_id: str,
                                   max_depth: int = 5) -> Dict[str, Any]:
        """Get downstream lineage for a node"""
        try:
            self.stats["lineage_queries"] += 1
            
            # Query downstream nodes
            query = f"g.V().has('id', '{node_id}').repeat(__.out()).times({max_depth})" \
                   f".path().by(valueMap('id', 'name', 'type'))"
            
            paths = self.gremlin_client.submit(query).all().result()
            
            # Build lineage tree
            lineage = {
                "node_id": node_id,
                "downstream_nodes": [],
                "paths": []
            }
            
            unique_nodes = set()
            
            for path in paths:
                path_nodes = []
                for node in path:
                    node_data = {
                        "id": node["id"][0],
                        "name": node["name"][0],
                        "type": node["type"][0]
                    }
                    path_nodes.append(node_data)
                    unique_nodes.add(node["id"][0])
                
                lineage["paths"].append(path_nodes)
            
            lineage["downstream_nodes"] = list(unique_nodes)
            
            # Cache result
            if self.cache:
                await self.cache.set_metadata(
                    "downstream_lineage",
                    node_id,
                    lineage,
                    ttl=300  # 5 minutes
                )
            
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to get downstream lineage: {e}")
            raise
    
    async def impact_analysis(self,
                            node_id: str,
                            change_type: str = "schema_change") -> Dict[str, Any]:
        """Analyze impact of changes to a node"""
        try:
            self.stats["impact_analyses"] += 1
            
            # Get downstream lineage
            downstream = await self.get_downstream_lineage(node_id, max_depth=10)
            
            # Categorize impacted nodes
            impact = {
                "source_node": node_id,
                "change_type": change_type,
                "directly_impacted": [],
                "indirectly_impacted": [],
                "impact_summary": {}
            }
            
            # Query direct dependencies
            query = f"g.V().has('id', '{node_id}').out().valueMap('id', 'name', 'type')"
            direct_nodes = self.gremlin_client.submit(query).all().result()
            
            for node in direct_nodes:
                impact["directly_impacted"].append({
                    "id": node["id"][0],
                    "name": node["name"][0],
                    "type": node["type"][0],
                    "impact_level": "high"
                })
            
            # All downstream nodes not directly connected
            all_downstream = set(downstream["downstream_nodes"])
            direct_ids = {n["id"] for n in impact["directly_impacted"]}
            indirect_ids = all_downstream - direct_ids - {node_id}
            
            # Query indirect nodes
            if indirect_ids:
                query = f"g.V().has('id', within({list(indirect_ids)})).valueMap('id', 'name', 'type')"
                indirect_nodes = self.gremlin_client.submit(query).all().result()
                
                for node in indirect_nodes:
                    impact["indirectly_impacted"].append({
                        "id": node["id"][0],
                        "name": node["name"][0],
                        "type": node["type"][0],
                        "impact_level": "medium"
                    })
            
            # Summarize by type
            for node_list in [impact["directly_impacted"], impact["indirectly_impacted"]]:
                for node in node_list:
                    node_type = node["type"]
                    if node_type not in impact["impact_summary"]:
                        impact["impact_summary"][node_type] = 0
                    impact["impact_summary"][node_type] += 1
            
            return impact
            
        except Exception as e:
            logger.error(f"Failed to perform impact analysis: {e}")
            raise
    
    async def find_data_provenance(self,
                                 node_id: str) -> Dict[str, Any]:
        """Find complete data provenance"""
        try:
            # Get all upstream paths to root nodes
            query = f"g.V().has('id', '{node_id}').repeat(__.in()).until(__.not(__.in()))" \
                   f".path().by(valueMap('id', 'name', 'type', 'created_at'))"
            
            paths = self.gremlin_client.submit(query).all().result()
            
            provenance = {
                "target_node": node_id,
                "sources": [],
                "transformation_chain": [],
                "created_at": None
            }
            
            # Analyze paths
            for path in paths:
                if path:
                    # First node is the source
                    source = path[0]
                    provenance["sources"].append({
                        "id": source["id"][0],
                        "name": source["name"][0],
                        "type": source["type"][0],
                        "created_at": source.get("created_at", [None])[0]
                    })
                    
                    # Build transformation chain
                    chain = []
                    for i in range(len(path) - 1):
                        chain.append({
                            "from": path[i]["name"][0],
                            "to": path[i+1]["name"][0],
                            "step": i + 1
                        })
                    
                    provenance["transformation_chain"].extend(chain)
            
            # Remove duplicates from sources
            seen = set()
            unique_sources = []
            for source in provenance["sources"]:
                if source["id"] not in seen:
                    seen.add(source["id"])
                    unique_sources.append(source)
            
            provenance["sources"] = unique_sources
            
            return provenance
            
        except Exception as e:
            logger.error(f"Failed to find data provenance: {e}")
            raise
    
    async def visualize_lineage(self,
                              node_id: str,
                              direction: str = "both",
                              max_depth: int = 3) -> Dict[str, Any]:
        """Generate lineage visualization data"""
        try:
            nodes = []
            edges = []
            processed = set()
            
            # Get lineage based on direction
            if direction in ["upstream", "both"]:
                upstream = await self.get_upstream_lineage(node_id, max_depth)
                await self._build_viz_data(upstream["paths"], nodes, edges, processed, "upstream")
            
            if direction in ["downstream", "both"]:
                downstream = await self.get_downstream_lineage(node_id, max_depth)
                await self._build_viz_data(downstream["paths"], nodes, edges, processed, "downstream")
            
            # Add the center node if not already added
            if node_id not in processed:
                query = f"g.V().has('id', '{node_id}').valueMap('id', 'name', 'type')"
                result = self.gremlin_client.submit(query).all().result()
                if result:
                    node_data = result[0]
                    nodes.append({
                        "id": node_data["id"][0],
                        "label": node_data["name"][0],
                        "type": node_data["type"][0],
                        "group": "center"
                    })
            
            return {
                "nodes": nodes,
                "edges": edges,
                "layout": "hierarchical",
                "center_node": node_id
            }
            
        except Exception as e:
            logger.error(f"Failed to visualize lineage: {e}")
            raise
    
    async def _build_viz_data(self,
                            paths: List[List[Dict]],
                            nodes: List[Dict],
                            edges: List[Dict],
                            processed: Set[str],
                            direction: str) -> None:
        """Build visualization data from paths"""
        for path in paths:
            for i, node in enumerate(path):
                node_id = node["id"]
                
                # Add node if not processed
                if node_id not in processed:
                    nodes.append({
                        "id": node_id,
                        "label": node["name"],
                        "type": node["type"],
                        "group": direction
                    })
                    processed.add(node_id)
                
                # Add edge
                if i > 0:
                    prev_node = path[i-1]
                    edge_key = f"{prev_node['id']}->{node_id}"
                    
                    if direction == "upstream":
                        # Reverse for upstream
                        edges.append({
                            "source": node_id,
                            "target": prev_node["id"],
                            "label": "derives_from"
                        })
                    else:
                        edges.append({
                            "source": prev_node["id"],
                            "target": node_id,
                            "label": "flows_to"
                        })
    
    async def _log_lineage_event(self,
                               source_id: str,
                               target_id: str,
                               edge_type: LineageEdgeType,
                               metadata: Optional[Dict[str, Any]] = None) -> None:
        """Log lineage event to Elasticsearch"""
        if not self.es_client:
            return
        
        event = {
            "event_id": f"{source_id}_{target_id}_{datetime.utcnow().timestamp()}",
            "event_type": edge_type.value,
            "source_id": source_id,
            "target_id": target_id,
            "timestamp": datetime.utcnow(),
            "metadata": metadata or {}
        }
        
        # Extract tenant_id from nodes if available
        if self.cache:
            source_data = await self.cache.get_metadata("lineage_node", source_id)
            if source_data and "tenant_id" in source_data:
                event["tenant_id"] = source_data["tenant_id"]
        
        await self.es_client.index(
            index=self.lineage_index,
            body=event
        )
    
    async def search_lineage_events(self,
                                  filters: Dict[str, Any],
                                  limit: int = 100) -> List[Dict[str, Any]]:
        """Search lineage events"""
        if not self.es_client:
            return []
        
        must_clauses = []
        
        for field, value in filters.items():
            if isinstance(value, list):
                must_clauses.append({"terms": {field: value}})
            else:
                must_clauses.append({"term": {field: value}})
        
        query = {
            "query": {"bool": {"must": must_clauses}},
            "sort": [{"timestamp": {"order": "desc"}}],
            "size": limit
        }
        
        result = await self.es_client.search(
            index=self.lineage_index,
            body=query
        )
        
        events = [hit["_source"] for hit in result["hits"]["hits"]]
        
        return events
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get lineage tracker statistics"""
        # Count nodes and edges in graph
        node_count_query = "g.V().count()"
        edge_count_query = "g.E().count()"
        
        node_count = self.gremlin_client.submit(node_count_query).all().result()[0]
        edge_count = self.gremlin_client.submit(edge_count_query).all().result()[0]
        
        return {
            "total_nodes": node_count,
            "total_edges": edge_count,
            **self.stats
        }
    
    async def cleanup_old_lineage(self, days: int = 90) -> int:
        """Clean up old lineage data"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            # Delete old nodes
            query = f"g.V().has('created_at', lt('{cutoff_date.isoformat()}')).drop()"
            
            result = self.gremlin_client.submit(query).all().result()
            
            logger.info(f"Cleaned up lineage data older than {days} days")
            
            return len(result)
            
        except Exception as e:
            logger.error(f"Failed to cleanup old lineage: {e}")
            raise
    
    async def export_lineage_graph(self,
                                 tenant_id: str,
                                 format: str = "graphml") -> str:
        """Export lineage graph"""
        try:
            # Query all nodes and edges for tenant
            nodes_query = f"g.V().has('tenant_id', '{tenant_id}').valueMap()"
            edges_query = f"g.V().has('tenant_id', '{tenant_id}').outE().as('e')" \
                         f".inV().has('tenant_id', '{tenant_id}').as('v')" \
                         f".select('e').valueMap()"
            
            nodes = self.gremlin_client.submit(nodes_query).all().result()
            edges = self.gremlin_client.submit(edges_query).all().result()
            
            # Build NetworkX graph
            export_graph = nx.DiGraph()
            
            # Add nodes
            for node in nodes:
                node_id = node["id"][0]
                attrs = {k: v[0] if isinstance(v, list) else v for k, v in node.items()}
                export_graph.add_node(node_id, **attrs)
            
            # Add edges
            for edge in edges:
                # Extract source and target from edge
                # This is simplified - actual implementation would parse edge data
                pass
            
            # Export based on format
            if format == "graphml":
                return nx.write_graphml(export_graph)
            elif format == "json":
                return json.dumps(nx.node_link_data(export_graph))
            else:
                raise ValueError(f"Unsupported export format: {format}")
                
        except Exception as e:
            logger.error(f"Failed to export lineage graph: {e}")
            raise
    
    async def shutdown(self) -> None:
        """Shutdown lineage tracker"""
        if self.gremlin_client:
            self.gremlin_client.close()
        logger.info("Data lineage tracker shut down") 