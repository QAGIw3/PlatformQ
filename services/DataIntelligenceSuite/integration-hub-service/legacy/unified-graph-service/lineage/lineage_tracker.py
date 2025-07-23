"""Lineage tracker for data lineage and provenance"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
import asyncio
import json
from enum import Enum

from app.core.config import Settings
from app.graph.janusgraph_client import JanusGraphClient


logger = logging.getLogger(__name__)


class LineageType(Enum):
    """Types of lineage relationships"""
    DATA_FLOW = "data_flow"
    DERIVED_FROM = "derived_from"
    TRANSFORMED_BY = "transformed_by"
    COPIED_FROM = "copied_from"
    AGGREGATED_FROM = "aggregated_from"
    VERSION_OF = "version_of"
    VALIDATED_BY = "validated_by"


class EntityType(Enum):
    """Types of entities in lineage"""
    DATASET = "dataset"
    MODEL = "model"
    PIPELINE = "pipeline"
    TRANSFORMATION = "transformation"
    REPORT = "report"
    API_ENDPOINT = "api_endpoint"
    DATABASE_TABLE = "database_table"
    FILE = "file"
    STREAM = "stream"


class LineageTracker:
    """Tracker for data lineage and provenance"""
    
    def __init__(self, settings: Settings, graph_client: JanusGraphClient):
        self.settings = settings
        self.graph_client = graph_client
        self.lineage_cache: Dict[str, Any] = {}
        
    async def track_lineage(self, entity_id: str, entity_type: str,
                           operation: str, metadata: Dict[str, Any],
                           parent_ids: Optional[List[str]] = None,
                           child_ids: Optional[List[str]] = None) -> str:
        """Track lineage for an entity"""
        try:
            # Create or update entity
            entity_props = {
                'entity_type': entity_type,
                'last_operation': operation,
                'last_updated': datetime.utcnow().isoformat(),
                **metadata
            }
            
            # Check if entity exists
            existing = await self.graph_client.get_node(entity_id)
            if existing:
                # Update existing entity
                await self.graph_client.update_node(entity_id, entity_props)
            else:
                # Create new entity
                await self.graph_client.create_node(
                    entity_type,
                    entity_props,
                    entity_id
                )
                
            # Create lineage relationships
            if parent_ids:
                for parent_id in parent_ids:
                    await self._create_lineage_edge(
                        parent_id,
                        entity_id,
                        operation,
                        metadata
                    )
                    
            if child_ids:
                for child_id in child_ids:
                    await self._create_lineage_edge(
                        entity_id,
                        child_id,
                        operation,
                        metadata
                    )
                    
            # Clear cache
            self._clear_lineage_cache(entity_id)
            
            logger.info(f"Tracked lineage for {entity_type} {entity_id}: {operation}")
            return entity_id
            
        except Exception as e:
            logger.error(f"Failed to track lineage for {entity_id}: {e}")
            raise
            
    async def get_lineage(self, entity_id: str, direction: str = "both",
                         max_depth: int = 5,
                         lineage_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Get lineage graph for an entity"""
        try:
            # Check cache
            cache_key = f"{entity_id}:{direction}:{max_depth}"
            if cache_key in self.lineage_cache:
                return self.lineage_cache[cache_key]
                
            # Build lineage graph
            nodes = {}
            edges = []
            visited = set()
            
            # BFS traversal
            queue = [(entity_id, 0)]
            visited.add(entity_id)
            
            while queue:
                current_id, depth = queue.pop(0)
                
                if depth > max_depth:
                    continue
                    
                # Get entity details
                entity = await self.graph_client.get_node(current_id)
                if entity:
                    nodes[current_id] = {
                        'id': current_id,
                        'type': entity.get('entity_type', entity.get('label')),
                        'properties': entity,
                        'depth': depth
                    }
                    
                # Get lineage relationships
                if direction in ["upstream", "both"]:
                    upstream = await self._get_upstream_lineage(
                        current_id,
                        lineage_types
                    )
                    for parent in upstream:
                        edges.append({
                            'from': parent['source_id'],
                            'to': current_id,
                            'type': parent['lineage_type'],
                            'operation': parent.get('operation'),
                            'timestamp': parent.get('created_at')
                        })
                        
                        parent_id = parent['source_id']
                        if parent_id not in visited and depth < max_depth:
                            queue.append((parent_id, depth + 1))
                            visited.add(parent_id)
                            
                if direction in ["downstream", "both"]:
                    downstream = await self._get_downstream_lineage(
                        current_id,
                        lineage_types
                    )
                    for child in downstream:
                        edges.append({
                            'from': current_id,
                            'to': child['target_id'],
                            'type': child['lineage_type'],
                            'operation': child.get('operation'),
                            'timestamp': child.get('created_at')
                        })
                        
                        child_id = child['target_id']
                        if child_id not in visited and depth < max_depth:
                            queue.append((child_id, depth + 1))
                            visited.add(child_id)
                            
            lineage_graph = {
                'root': entity_id,
                'direction': direction,
                'nodes': list(nodes.values()),
                'edges': edges,
                'node_count': len(nodes),
                'edge_count': len(edges),
                'max_depth_reached': max(n['depth'] for n in nodes.values()) if nodes else 0
            }
            
            # Cache result
            self.lineage_cache[cache_key] = lineage_graph
            
            return lineage_graph
            
        except Exception as e:
            logger.error(f"Failed to get lineage for {entity_id}: {e}")
            raise
            
    async def analyze_impact(self, entity_id: str,
                           change_type: str = "schema_change",
                           max_depth: int = 10) -> Dict[str, Any]:
        """Analyze impact of changes to an entity"""
        try:
            # Get downstream lineage
            lineage = await self.get_lineage(
                entity_id,
                direction="downstream",
                max_depth=max_depth
            )
            
            impacted_entities = []
            impact_scores = {}
            
            # Analyze each downstream entity
            for node in lineage['nodes']:
                if node['id'] != entity_id:
                    # Calculate impact score based on distance and entity type
                    impact_score = self._calculate_impact_score(
                        node,
                        change_type,
                        lineage['edges']
                    )
                    
                    impact_scores[node['id']] = impact_score
                    
                    if impact_score > 0.3:  # Significant impact threshold
                        impacted_entities.append({
                            'id': node['id'],
                            'type': node['type'],
                            'impact_score': impact_score,
                            'distance': node['depth'],
                            'recommended_action': self._recommend_action(
                                node['type'],
                                change_type,
                                impact_score
                            )
                        })
                        
            # Sort by impact score
            impacted_entities.sort(key=lambda x: x['impact_score'], reverse=True)
            
            return {
                'source_entity': entity_id,
                'change_type': change_type,
                'total_impacted': len(impacted_entities),
                'critical_impacts': len([e for e in impacted_entities if e['impact_score'] > 0.7]),
                'impacted_entities': impacted_entities[:50],  # Top 50
                'impact_summary': self._summarize_impact(impacted_entities),
                'analyzed_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze impact for {entity_id}: {e}")
            raise
            
    async def get_provenance(self, entity_id: str) -> Dict[str, Any]:
        """Get complete provenance chain for an entity"""
        try:
            # Get full upstream lineage
            lineage = await self.get_lineage(
                entity_id,
                direction="upstream",
                max_depth=20
            )
            
            # Build provenance chain
            provenance_chain = []
            
            # Find all root nodes (no incoming edges)
            root_nodes = set(lineage['nodes']) - {e['to'] for e in lineage['edges']}
            
            for root_id in root_nodes:
                # Trace path from root to entity
                paths = await self._find_lineage_paths(root_id, entity_id, lineage)
                
                for path in paths:
                    provenance_chain.append({
                        'origin': path[0],
                        'path': path,
                        'transformations': await self._get_path_transformations(path, lineage)
                    })
                    
            return {
                'entity_id': entity_id,
                'provenance_chains': provenance_chain,
                'total_origins': len(root_nodes),
                'data_sources': [n for n in lineage['nodes'] if n['type'] == EntityType.DATASET.value],
                'transformations_applied': self._count_transformations(lineage),
                'quality_validations': await self._get_quality_validations(entity_id),
                'created_at': await self._get_creation_time(entity_id)
            }
            
        except Exception as e:
            logger.error(f"Failed to get provenance for {entity_id}: {e}")
            raise
            
    async def compare_lineages(self, entity_id1: str, entity_id2: str) -> Dict[str, Any]:
        """Compare lineages of two entities"""
        try:
            # Get lineages for both entities
            lineage1 = await self.get_lineage(entity_id1, max_depth=10)
            lineage2 = await self.get_lineage(entity_id2, max_depth=10)
            
            # Extract node sets
            nodes1 = {n['id'] for n in lineage1['nodes']}
            nodes2 = {n['id'] for n in lineage2['nodes']}
            
            # Find common ancestors
            common_ancestors = nodes1.intersection(nodes2)
            
            # Find unique dependencies
            unique_to_1 = nodes1 - nodes2
            unique_to_2 = nodes2 - nodes1
            
            # Calculate similarity score
            if nodes1 or nodes2:
                similarity = len(common_ancestors) / len(nodes1.union(nodes2))
            else:
                similarity = 0
                
            return {
                'entity1': entity_id1,
                'entity2': entity_id2,
                'similarity_score': similarity,
                'common_ancestors': list(common_ancestors),
                'unique_dependencies_1': list(unique_to_1),
                'unique_dependencies_2': list(unique_to_2),
                'divergence_point': await self._find_divergence_point(
                    entity_id1,
                    entity_id2,
                    common_ancestors
                )
            }
            
        except Exception as e:
            logger.error(f"Failed to compare lineages: {e}")
            raise
            
    async def validate_lineage_integrity(self, entity_id: str) -> Dict[str, Any]:
        """Validate integrity of lineage graph"""
        try:
            issues = []
            warnings = []
            
            # Get full lineage
            lineage = await self.get_lineage(entity_id, max_depth=20)
            
            # Check for cycles
            cycles = self._detect_cycles(lineage)
            if cycles:
                issues.append({
                    'type': 'cycle_detected',
                    'severity': 'high',
                    'details': f"Found {len(cycles)} cycles in lineage",
                    'cycles': cycles[:5]  # First 5 cycles
                })
                
            # Check for missing entities
            for edge in lineage['edges']:
                if edge['from'] not in [n['id'] for n in lineage['nodes']]:
                    warnings.append({
                        'type': 'missing_source',
                        'severity': 'medium',
                        'edge': edge
                    })
                if edge['to'] not in [n['id'] for n in lineage['nodes']]:
                    warnings.append({
                        'type': 'missing_target',
                        'severity': 'medium',
                        'edge': edge
                    })
                    
            # Check for orphaned nodes
            connected_nodes = set()
            for edge in lineage['edges']:
                connected_nodes.add(edge['from'])
                connected_nodes.add(edge['to'])
                
            orphaned = []
            for node in lineage['nodes']:
                if node['id'] != entity_id and node['id'] not in connected_nodes:
                    orphaned.append(node['id'])
                    
            if orphaned:
                warnings.append({
                    'type': 'orphaned_nodes',
                    'severity': 'low',
                    'count': len(orphaned),
                    'nodes': orphaned[:10]
                })
                
            # Check timestamps consistency
            time_issues = self._check_temporal_consistency(lineage)
            issues.extend(time_issues)
            
            return {
                'entity_id': entity_id,
                'is_valid': len(issues) == 0,
                'issues': issues,
                'warnings': warnings,
                'stats': {
                    'total_nodes': len(lineage['nodes']),
                    'total_edges': len(lineage['edges']),
                    'max_depth': lineage['max_depth_reached']
                },
                'validated_at': datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to validate lineage integrity: {e}")
            raise
            
    async def _create_lineage_edge(self, from_id: str, to_id: str,
                                  operation: str, metadata: Dict[str, Any]):
        """Create lineage edge between entities"""
        edge_props = {
            'lineage_type': self._determine_lineage_type(operation),
            'operation': operation,
            'created_at': datetime.utcnow().isoformat(),
            **{k: v for k, v in metadata.items() if k != 'entity_type'}
        }
        
        await self.graph_client.create_edge(
            'LINEAGE',
            from_id,
            to_id,
            edge_props
        )
        
    async def _get_upstream_lineage(self, entity_id: str,
                                   lineage_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get upstream lineage relationships"""
        query = "g.V(entity_id).inE('LINEAGE')"
        
        if lineage_types:
            type_filter = " or ".join([f"lineage_type == '{t}'" for t in lineage_types])
            query += f".filter{{{type_filter}}}"
            
        query += ".as('e').outV().as('v').select('e', 'v').by(valueMap(true))"
        
        results = await self.graph_client.execute_query(query, {'entity_id': entity_id})
        
        lineage = []
        for result in results:
            edge = result['e']
            vertex = result['v']
            lineage.append({
                'source_id': str(vertex['id']),
                'lineage_type': edge.get('lineage_type', ['unknown'])[0],
                'operation': edge.get('operation', ['unknown'])[0],
                'created_at': edge.get('created_at', [None])[0]
            })
            
        return lineage
        
    async def _get_downstream_lineage(self, entity_id: str,
                                     lineage_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get downstream lineage relationships"""
        query = "g.V(entity_id).outE('LINEAGE')"
        
        if lineage_types:
            type_filter = " or ".join([f"lineage_type == '{t}'" for t in lineage_types])
            query += f".filter{{{type_filter}}}"
            
        query += ".as('e').inV().as('v').select('e', 'v').by(valueMap(true))"
        
        results = await self.graph_client.execute_query(query, {'entity_id': entity_id})
        
        lineage = []
        for result in results:
            edge = result['e']
            vertex = result['v']
            lineage.append({
                'target_id': str(vertex['id']),
                'lineage_type': edge.get('lineage_type', ['unknown'])[0],
                'operation': edge.get('operation', ['unknown'])[0],
                'created_at': edge.get('created_at', [None])[0]
            })
            
        return lineage
        
    def _determine_lineage_type(self, operation: str) -> str:
        """Determine lineage type from operation"""
        operation_lower = operation.lower()
        
        if 'transform' in operation_lower:
            return LineageType.TRANSFORMED_BY.value
        elif 'derive' in operation_lower:
            return LineageType.DERIVED_FROM.value
        elif 'copy' in operation_lower or 'replicate' in operation_lower:
            return LineageType.COPIED_FROM.value
        elif 'aggregate' in operation_lower or 'sum' in operation_lower:
            return LineageType.AGGREGATED_FROM.value
        elif 'version' in operation_lower:
            return LineageType.VERSION_OF.value
        elif 'validate' in operation_lower or 'quality' in operation_lower:
            return LineageType.VALIDATED_BY.value
        else:
            return LineageType.DATA_FLOW.value
            
    def _calculate_impact_score(self, node: Dict[str, Any],
                               change_type: str,
                               edges: List[Dict[str, Any]]) -> float:
        """Calculate impact score for a node"""
        # Base score based on distance
        distance_factor = 1.0 / (node['depth'] + 1)
        
        # Entity type factor
        critical_types = [EntityType.MODEL.value, EntityType.REPORT.value, EntityType.API_ENDPOINT.value]
        type_factor = 2.0 if node['type'] in critical_types else 1.0
        
        # Change type factor
        change_factors = {
            'schema_change': 0.8,
            'data_deletion': 1.0,
            'transformation_update': 0.6,
            'quality_rule_change': 0.4
        }
        change_factor = change_factors.get(change_type, 0.5)
        
        # Calculate final score
        impact_score = distance_factor * type_factor * change_factor
        
        return min(impact_score, 1.0)
        
    def _recommend_action(self, entity_type: str, change_type: str,
                         impact_score: float) -> str:
        """Recommend action based on impact"""
        if impact_score > 0.7:
            if entity_type == EntityType.MODEL.value:
                return "Retrain model with updated data"
            elif entity_type == EntityType.REPORT.value:
                return "Regenerate report and notify stakeholders"
            elif entity_type == EntityType.API_ENDPOINT.value:
                return "Update API documentation and notify consumers"
            else:
                return "Review and update affected entity"
        elif impact_score > 0.3:
            return "Monitor for issues and prepare update plan"
        else:
            return "Low impact - monitor only"
            
    def _summarize_impact(self, impacted_entities: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize impact analysis"""
        if not impacted_entities:
            return {'severity': 'none', 'action': 'No action required'}
            
        max_score = max(e['impact_score'] for e in impacted_entities)
        critical_count = len([e for e in impacted_entities if e['impact_score'] > 0.7])
        
        if max_score > 0.7:
            severity = 'high'
            action = f"Immediate action required for {critical_count} critical entities"
        elif max_score > 0.3:
            severity = 'medium'
            action = "Review and plan updates for affected entities"
        else:
            severity = 'low'
            action = "Monitor affected entities"
            
        return {
            'severity': severity,
            'recommended_action': action,
            'critical_entities': critical_count,
            'total_affected': len(impacted_entities)
        }
        
    async def _find_lineage_paths(self, start_id: str, end_id: str,
                                 lineage: Dict[str, Any]) -> List[List[str]]:
        """Find all paths from start to end in lineage graph"""
        # Build adjacency list
        adjacency = {}
        for edge in lineage['edges']:
            if edge['from'] not in adjacency:
                adjacency[edge['from']] = []
            adjacency[edge['from']].append(edge['to'])
            
        # DFS to find all paths
        paths = []
        
        def dfs(current: str, target: str, path: List[str], visited: Set[str]):
            if current == target:
                paths.append(path.copy())
                return
                
            if current not in adjacency:
                return
                
            for neighbor in adjacency[current]:
                if neighbor not in visited:
                    visited.add(neighbor)
                    path.append(neighbor)
                    dfs(neighbor, target, path, visited)
                    path.pop()
                    visited.remove(neighbor)
                    
        dfs(start_id, end_id, [start_id], {start_id})
        
        return paths
        
    def _detect_cycles(self, lineage: Dict[str, Any]) -> List[List[str]]:
        """Detect cycles in lineage graph"""
        # Build adjacency list
        adjacency = {}
        for edge in lineage['edges']:
            if edge['from'] not in adjacency:
                adjacency[edge['from']] = []
            adjacency[edge['from']].append(edge['to'])
            
        cycles = []
        visited = set()
        rec_stack = set()
        
        def dfs(node: str, path: List[str]) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            if node in adjacency:
                for neighbor in adjacency[node]:
                    if neighbor not in visited:
                        if dfs(neighbor, path):
                            return True
                    elif neighbor in rec_stack:
                        # Found cycle
                        cycle_start = path.index(neighbor)
                        cycles.append(path[cycle_start:] + [neighbor])
                        
            path.pop()
            rec_stack.remove(node)
            return False
            
        for node in adjacency:
            if node not in visited:
                dfs(node, [])
                
        return cycles
        
    def _clear_lineage_cache(self, entity_id: str):
        """Clear lineage cache for an entity"""
        keys_to_remove = [k for k in self.lineage_cache.keys() if entity_id in k]
        for key in keys_to_remove:
            del self.lineage_cache[key] 