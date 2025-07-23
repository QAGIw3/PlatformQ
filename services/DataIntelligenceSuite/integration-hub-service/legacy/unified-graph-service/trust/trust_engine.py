"""Trust engine for trust and reputation management"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import numpy as np
from enum import Enum

from app.core.config import Settings
from app.graph.janusgraph_client import JanusGraphClient


logger = logging.getLogger(__name__)


class TrustDimension(Enum):
    """Trust dimensions"""
    RELIABILITY = "reliability"
    COMPETENCE = "competence"
    HONESTY = "honesty"
    BENEVOLENCE = "benevolence"
    PREDICTABILITY = "predictability"


class TrustEngine:
    """Engine for trust and reputation calculations"""
    
    def __init__(self, settings: Settings, graph_client: JanusGraphClient):
        self.settings = settings
        self.graph_client = graph_client
        self.trust_cache: Dict[str, Any] = {}
        self._update_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the trust engine"""
        logger.info("Starting trust engine")
        
        # Start periodic trust update task
        self._update_task = asyncio.create_task(self._periodic_trust_update())
        
        logger.info("Trust engine started")
        
    async def stop(self):
        """Stop the trust engine"""
        if self._update_task:
            self._update_task.cancel()
            
        logger.info("Trust engine stopped")
        
    async def calculate_trust_score(self, entity_id: str, 
                                   context: Optional[str] = None,
                                   dimensions: Optional[List[str]] = None) -> Dict[str, Any]:
        """Calculate trust score for an entity"""
        try:
            # Check cache first
            cache_key = f"{entity_id}:{context or 'global'}"
            if cache_key in self.trust_cache:
                cached = self.trust_cache[cache_key]
                if (datetime.utcnow() - cached['timestamp']).seconds < self.settings.cache_ttl:
                    return cached['score']
                    
            # Get entity data
            entity = await self.graph_client.get_node(entity_id)
            if not entity:
                raise ValueError(f"Entity {entity_id} not found")
                
            # Calculate trust score based on algorithm
            if self.settings.trust_algorithm == "eigentrust":
                score = await self._calculate_eigentrust(entity_id, context, dimensions)
            elif self.settings.trust_algorithm == "page_rank":
                score = await self._calculate_pagerank_trust(entity_id, context)
            elif self.settings.trust_algorithm == "multi_dimensional":
                score = await self._calculate_multi_dimensional_trust(entity_id, dimensions)
            else:
                score = await self._calculate_basic_trust(entity_id, context)
                
            # Cache the result
            self.trust_cache[cache_key] = {
                'score': score,
                'timestamp': datetime.utcnow()
            }
            
            return score
            
        except Exception as e:
            logger.error(f"Failed to calculate trust score for {entity_id}: {e}")
            raise
            
    async def propagate_trust(self, source_id: str, max_depth: Optional[int] = None) -> Dict[str, float]:
        """Propagate trust from a source entity"""
        try:
            depth = max_depth or self.settings.trust_propagation_depth
            propagated_trust = {}
            
            # Initialize with source trust
            source_trust = await self.calculate_trust_score(source_id)
            propagated_trust[source_id] = source_trust['overall_score']
            
            # BFS propagation
            current_level = {source_id: source_trust['overall_score']}
            visited = {source_id}
            
            for level in range(1, depth + 1):
                next_level = {}
                decay = self.settings.trust_decay_factor ** level
                
                for entity_id, trust_value in current_level.items():
                    # Get trusted neighbors
                    query = """
                        g.V(entity_id).out('TRUSTS').
                        has('trust_level', P.gte(0.5)).
                        valueMap(true)
                    """
                    neighbors = await self.graph_client.execute_query(
                        query, {'entity_id': entity_id}
                    )
                    
                    for neighbor in neighbors:
                        neighbor_id = str(neighbor['id'])
                        if neighbor_id not in visited:
                            # Calculate propagated trust
                            edge_trust = neighbor.get('trust_level', [0.5])[0]
                            propagated = trust_value * edge_trust * decay
                            
                            if neighbor_id in next_level:
                                # Take maximum trust path
                                next_level[neighbor_id] = max(next_level[neighbor_id], propagated)
                            else:
                                next_level[neighbor_id] = propagated
                                
                            visited.add(neighbor_id)
                            
                # Update propagated trust
                propagated_trust.update(next_level)
                current_level = next_level
                
                if not current_level:
                    break
                    
            return propagated_trust
            
        except Exception as e:
            logger.error(f"Failed to propagate trust from {source_id}: {e}")
            raise
            
    async def get_trust_network(self, entity_id: str, radius: int = 2) -> Dict[str, Any]:
        """Get trust network around an entity"""
        try:
            nodes = []
            edges = []
            visited = set()
            
            # BFS to build network
            queue = [(entity_id, 0)]
            visited.add(entity_id)
            
            while queue:
                current_id, depth = queue.pop(0)
                
                if depth > radius:
                    continue
                    
                # Get entity info
                entity = await self.graph_client.get_node(current_id)
                if entity:
                    trust_score = await self.calculate_trust_score(current_id)
                    nodes.append({
                        'id': current_id,
                        'label': entity.get('label'),
                        'trust_score': trust_score['overall_score'],
                        'depth': depth
                    })
                    
                # Get trust relationships
                query = """
                    g.V(entity_id).bothE('TRUSTS').as('e').
                    otherV().as('v').
                    select('e', 'v').by(valueMap(true))
                """
                relationships = await self.graph_client.execute_query(
                    query, {'entity_id': current_id}
                )
                
                for rel in relationships:
                    edge_data = rel['e']
                    other_vertex = rel['v']
                    other_id = str(other_vertex['id'])
                    
                    # Add edge
                    edges.append({
                        'source': current_id if edge_data['label'] == 'TRUSTS' else other_id,
                        'target': other_id if edge_data['label'] == 'TRUSTS' else current_id,
                        'trust_level': edge_data.get('trust_level', [0.5])[0],
                        'context': edge_data.get('context', ['general'])[0]
                    })
                    
                    # Add to queue if not visited and within radius
                    if other_id not in visited and depth < radius:
                        queue.append((other_id, depth + 1))
                        visited.add(other_id)
                        
            return {
                'center': entity_id,
                'nodes': nodes,
                'edges': edges,
                'network_size': len(nodes),
                'average_trust': np.mean([n['trust_score'] for n in nodes]) if nodes else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to get trust network for {entity_id}: {e}")
            raise
            
    async def update_trust_relationship(self, from_id: str, to_id: str,
                                      trust_level: float, context: Optional[str] = None,
                                      dimensions: Optional[Dict[str, float]] = None) -> bool:
        """Update trust relationship between entities"""
        try:
            # Validate trust level
            if not 0 <= trust_level <= 1:
                raise ValueError("Trust level must be between 0 and 1")
                
            # Check if relationship exists
            query = """
                g.V(from_id).outE('TRUSTS').where(inV().hasId(to_id)).
                valueMap(true)
            """
            existing = await self.graph_client.execute_query(
                query, {'from_id': from_id, 'to_id': to_id}
            )
            
            properties = {
                'trust_level': trust_level,
                'updated_at': datetime.utcnow().isoformat()
            }
            
            if context:
                properties['context'] = context
                
            if dimensions:
                for dim, value in dimensions.items():
                    properties[f'trust_{dim}'] = value
                    
            if existing:
                # Update existing edge
                edge_id = str(existing[0]['id'])
                query = """
                    g.E(edge_id).property('trust_level', trust_level).
                    property('updated_at', updated_at)
                """
                bindings = {
                    'edge_id': edge_id,
                    'trust_level': trust_level,
                    'updated_at': properties['updated_at']
                }
                
                # Add other properties
                for key, value in properties.items():
                    if key not in ['trust_level', 'updated_at']:
                        query += f".property('{key}', {key}_val)"
                        bindings[f'{key}_val'] = value
                        
                await self.graph_client.execute_query(query, bindings)
                
            else:
                # Create new edge
                await self.graph_client.create_edge('TRUSTS', from_id, to_id, properties)
                
            # Clear cache
            self._clear_trust_cache(from_id)
            self._clear_trust_cache(to_id)
            
            logger.info(f"Updated trust relationship from {from_id} to {to_id}: {trust_level}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update trust relationship: {e}")
            raise
            
    async def get_trust_recommendations(self, entity_id: str, 
                                       min_trust: float = 0.6,
                                       limit: int = 10) -> List[Dict[str, Any]]:
        """Get trust-based recommendations for connections"""
        try:
            # Get propagated trust scores
            propagated = await self.propagate_trust(entity_id)
            
            # Get current connections
            query = """
                g.V(entity_id).out('TRUSTS').id()
            """
            current_connections = await self.graph_client.execute_query(
                query, {'entity_id': entity_id}
            )
            current_ids = {str(conn) for conn in current_connections}
            
            # Filter recommendations
            recommendations = []
            for recommended_id, trust_score in propagated.items():
                if (recommended_id != entity_id and 
                    recommended_id not in current_ids and 
                    trust_score >= min_trust):
                    
                    # Get entity info
                    entity = await self.graph_client.get_node(recommended_id)
                    if entity:
                        recommendations.append({
                            'id': recommended_id,
                            'label': entity.get('label'),
                            'propagated_trust': trust_score,
                            'reason': await self._get_recommendation_reason(
                                entity_id, recommended_id, propagated
                            )
                        })
                        
            # Sort by trust score and limit
            recommendations.sort(key=lambda x: x['propagated_trust'], reverse=True)
            return recommendations[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get trust recommendations for {entity_id}: {e}")
            raise
            
    async def _calculate_eigentrust(self, entity_id: str, context: Optional[str],
                                   dimensions: Optional[List[str]]) -> Dict[str, Any]:
        """Calculate trust using EigenTrust algorithm"""
        # Simplified EigenTrust implementation
        # In production, this would use matrix operations on the entire graph
        
        # Get local trust values
        query = """
            g.V(entity_id).
            inE('TRUSTS').as('e').
            outV().as('v').
            select('e', 'v').by(valueMap(true))
        """
        trust_relationships = await self.graph_client.execute_query(
            query, {'entity_id': entity_id}
        )
        
        if not trust_relationships:
            return {
                'overall_score': 0.5,  # Default neutral trust
                'confidence': 0.0,
                'dimensions': {},
                'algorithm': 'eigentrust'
            }
            
        # Calculate normalized trust
        trust_sum = 0
        trust_count = 0
        dimension_scores = {}
        
        for rel in trust_relationships:
            edge = rel['e']
            trust_level = edge.get('trust_level', [0.5])[0]
            
            # Apply context filter if specified
            if context and edge.get('context', ['general'])[0] != context:
                continue
                
            trust_sum += trust_level
            trust_count += 1
            
            # Track dimension scores
            if dimensions:
                for dim in dimensions:
                    dim_key = f'trust_{dim}'
                    if dim_key in edge:
                        if dim not in dimension_scores:
                            dimension_scores[dim] = []
                        dimension_scores[dim].append(edge[dim_key][0])
                        
        overall_score = trust_sum / trust_count if trust_count > 0 else 0.5
        
        # Calculate dimension averages
        for dim, scores in dimension_scores.items():
            dimension_scores[dim] = np.mean(scores)
            
        return {
            'overall_score': overall_score,
            'confidence': min(trust_count / 10, 1.0),  # Confidence based on number of ratings
            'dimensions': dimension_scores,
            'algorithm': 'eigentrust',
            'trust_count': trust_count
        }
        
    async def _calculate_pagerank_trust(self, entity_id: str, context: Optional[str]) -> Dict[str, Any]:
        """Calculate trust using PageRank-based algorithm"""
        # This would integrate with GraphX PageRank results
        # For now, using simplified calculation
        
        base_trust = await self._calculate_basic_trust(entity_id, context)
        
        # Get entity's influence (approximated by number of incoming trust edges)
        query = "g.V(entity_id).inE('TRUSTS').count()"
        in_degree = await self.graph_client.execute_query(query, {'entity_id': entity_id})
        
        influence_factor = 1 + (in_degree[0] if in_degree else 0) * 0.1
        adjusted_score = min(base_trust['overall_score'] * influence_factor, 1.0)
        
        return {
            **base_trust,
            'overall_score': adjusted_score,
            'algorithm': 'pagerank',
            'influence_factor': influence_factor
        }
        
    async def _calculate_multi_dimensional_trust(self, entity_id: str,
                                               dimensions: Optional[List[str]]) -> Dict[str, Any]:
        """Calculate multi-dimensional trust score"""
        dims = dimensions or [d.value for d in TrustDimension]
        
        # Get all trust relationships
        query = """
            g.V(entity_id).inE('TRUSTS').valueMap(true)
        """
        relationships = await self.graph_client.execute_query(
            query, {'entity_id': entity_id}
        )
        
        dimension_scores = {dim: [] for dim in dims}
        
        for rel in relationships:
            for dim in dims:
                dim_key = f'trust_{dim}'
                if dim_key in rel:
                    dimension_scores[dim].append(rel[dim_key][0])
                    
        # Calculate weighted average
        weights = {
            TrustDimension.RELIABILITY.value: 0.3,
            TrustDimension.COMPETENCE.value: 0.25,
            TrustDimension.HONESTY.value: 0.2,
            TrustDimension.BENEVOLENCE.value: 0.15,
            TrustDimension.PREDICTABILITY.value: 0.1
        }
        
        overall_score = 0
        calculated_dimensions = {}
        
        for dim, scores in dimension_scores.items():
            if scores:
                dim_score = np.mean(scores)
                calculated_dimensions[dim] = dim_score
                overall_score += dim_score * weights.get(dim, 0.2)
            else:
                calculated_dimensions[dim] = 0.5  # Neutral default
                overall_score += 0.5 * weights.get(dim, 0.2)
                
        return {
            'overall_score': overall_score,
            'confidence': len(relationships) / 10 if relationships else 0,
            'dimensions': calculated_dimensions,
            'algorithm': 'multi_dimensional'
        }
        
    async def _calculate_basic_trust(self, entity_id: str, context: Optional[str]) -> Dict[str, Any]:
        """Calculate basic average trust score"""
        query = "g.V(entity_id).inE('TRUSTS')"
        
        if context:
            query += f".has('context', '{context}')"
            
        query += ".values('trust_level')"
        
        trust_values = await self.graph_client.execute_query(
            query, {'entity_id': entity_id}
        )
        
        if not trust_values:
            return {
                'overall_score': 0.5,
                'confidence': 0.0,
                'dimensions': {},
                'algorithm': 'basic'
            }
            
        return {
            'overall_score': np.mean(trust_values),
            'confidence': min(len(trust_values) / 10, 1.0),
            'dimensions': {},
            'algorithm': 'basic',
            'trust_count': len(trust_values)
        }
        
    async def _get_recommendation_reason(self, source_id: str, target_id: str,
                                       propagated: Dict[str, float]) -> str:
        """Get recommendation reason"""
        # Find the path that led to this recommendation
        query = """
            g.V(source_id).
            repeat(out('TRUSTS').simplePath()).
            until(hasId(target_id).or().loops().is(3)).
            path().by(id).limit(1)
        """
        
        paths = await self.graph_client.execute_query(
            query, {'source_id': source_id, 'target_id': target_id}
        )
        
        if paths:
            path = paths[0]
            if len(path) == 2:
                return "Direct trust relationship"
            elif len(path) == 3:
                intermediary = path[1]
                return f"Trusted through mutual connection {intermediary}"
            else:
                return f"Connected through {len(path)-2} degrees of trust"
        else:
            return "Recommended based on network analysis"
            
    def _clear_trust_cache(self, entity_id: str):
        """Clear trust cache for an entity"""
        keys_to_remove = [k for k in self.trust_cache.keys() if k.startswith(f"{entity_id}:")]
        for key in keys_to_remove:
            del self.trust_cache[key]
            
    async def _periodic_trust_update(self):
        """Periodically update trust scores"""
        while True:
            try:
                await asyncio.sleep(self.settings.trust_update_interval)
                
                # Clear old cache entries
                now = datetime.utcnow()
                expired_keys = []
                
                for key, value in self.trust_cache.items():
                    if (now - value['timestamp']).seconds > self.settings.cache_ttl:
                        expired_keys.append(key)
                        
                for key in expired_keys:
                    del self.trust_cache[key]
                    
                logger.info(f"Cleared {len(expired_keys)} expired trust cache entries")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic trust update: {e}") 