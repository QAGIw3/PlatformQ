"""
Graph-Enriched Search Integration

Combines Elasticsearch full-text search with JanusGraph relationship queries
for comprehensive, context-aware search results.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
import httpx
from elasticsearch import AsyncElasticsearch
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Order
import networkx as nx
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class GraphSearchConfig(BaseModel):
    """Configuration for graph-enriched search"""
    max_graph_depth: int = Field(default=3, description="Maximum traversal depth")
    relationship_boost: float = Field(default=1.5, description="Boost factor for related entities")
    include_lineage: bool = Field(default=True, description="Include asset lineage in results")
    include_collaborators: bool = Field(default=True, description="Include collaborator network")
    semantic_expansion: bool = Field(default=True, description="Expand search with semantic relationships")


class GraphEnrichedSearchEngine:
    """Combines Elasticsearch and JanusGraph for unified search"""
    
    def __init__(self,
                 es_client: AsyncElasticsearch,
                 janusgraph_url: str,
                 graph_intelligence_url: str):
        self.es = es_client
        self.janusgraph_url = janusgraph_url
        self.graph_intel_url = graph_intelligence_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.gremlin_client = None
        
    async def connect(self):
        """Initialize connections"""
        # Connect to JanusGraph via Gremlin
        self.gremlin_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
    async def unified_search(self,
                           query: str,
                           filters: Dict[str, Any],
                           config: GraphSearchConfig,
                           user_context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform unified search across text and graph"""
        try:
            # Phase 1: Elasticsearch full-text search
            text_results = await self._elasticsearch_search(query, filters)
            
            if not text_results["hits"]:
                return {
                    "results": [],
                    "total": 0,
                    "graph_context": {},
                    "facets": {}
                }
                
            # Phase 2: Graph enrichment
            enriched_results = await self._enrich_with_graph(
                text_results["hits"],
                config,
                user_context
            )
            
            # Phase 3: Semantic expansion
            if config.semantic_expansion:
                expanded_results = await self._semantic_expansion(
                    enriched_results,
                    query,
                    config
                )
            else:
                expanded_results = enriched_results
                
            # Phase 4: Ranking and aggregation
            final_results = self._rank_and_aggregate(
                expanded_results,
                query,
                config
            )
            
            # Phase 5: Generate facets and insights
            facets = await self._generate_facets(final_results)
            insights = await self._generate_insights(final_results, query)
            
            return {
                "results": final_results[:50],  # Top 50 results
                "total": len(final_results),
                "graph_context": self._extract_graph_context(final_results),
                "facets": facets,
                "insights": insights,
                "search_metadata": {
                    "query": query,
                    "timestamp": datetime.utcnow().isoformat(),
                    "enrichment_depth": config.max_graph_depth
                }
            }
            
        except Exception as e:
            logger.error(f"Error in unified search: {e}")
            raise
            
    async def _elasticsearch_search(self,
                                  query: str,
                                  filters: Dict[str, Any]) -> Dict[str, Any]:
        """Perform Elasticsearch search"""
        try:
            # Build query
            es_query = {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "title^3",
                                    "description^2",
                                    "content",
                                    "tags^2",
                                    "metadata.*"
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    ],
                    "filter": []
                }
            }
            
            # Add filters
            for field, value in filters.items():
                if isinstance(value, list):
                    es_query["bool"]["filter"].append({
                        "terms": {field: value}
                    })
                else:
                    es_query["bool"]["filter"].append({
                        "term": {field: value}
                    })
                    
            # Search across multiple indices
            response = await self.es.search(
                index="assets,simulations,projects,workflows",
                body={
                    "query": es_query,
                    "size": 100,
                    "highlight": {
                        "fields": {
                            "*": {}
                        }
                    },
                    "_source": True
                }
            )
            
            return {
                "hits": [hit for hit in response["hits"]["hits"]],
                "total": response["hits"]["total"]["value"]
            }
            
        except Exception as e:
            logger.error(f"Elasticsearch search error: {e}")
            return {"hits": [], "total": 0}
            
    async def _enrich_with_graph(self,
                               hits: List[Dict[str, Any]],
                               config: GraphSearchConfig,
                               user_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enrich search results with graph relationships"""
        enriched = []
        
        for hit in hits:
            try:
                entity_id = hit["_id"]
                entity_type = hit["_index"]
                
                # Get graph data for entity
                graph_data = await self._get_graph_context(
                    entity_id,
                    entity_type,
                    config
                )
                
                # Merge with search hit
                enriched_hit = {
                    **hit,
                    "graph_enrichment": graph_data,
                    "relevance_score": hit["_score"]
                }
                
                # Add lineage if requested
                if config.include_lineage and entity_type == "assets":
                    lineage = await self._get_asset_lineage(entity_id)
                    enriched_hit["lineage"] = lineage
                    
                # Add collaborators if requested
                if config.include_collaborators:
                    collaborators = await self._get_collaborators(
                        entity_id,
                        entity_type
                    )
                    enriched_hit["collaborators"] = collaborators
                    
                enriched.append(enriched_hit)
                
            except Exception as e:
                logger.error(f"Error enriching hit {hit['_id']}: {e}")
                enriched.append(hit)
                
        return enriched
        
    async def _get_graph_context(self,
                               entity_id: str,
                               entity_type: str,
                               config: GraphSearchConfig) -> Dict[str, Any]:
        """Get graph context for an entity"""
        try:
            # Query graph intelligence service
            response = await self.http_client.post(
                f"{self.graph_intel_url}/api/v1/graph/context",
                json={
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "max_depth": config.max_graph_depth,
                    "include_properties": True
                }
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting graph context: {e}")
            return {}
            
    async def _get_asset_lineage(self, asset_id: str) -> Dict[str, Any]:
        """Get asset lineage from graph"""
        try:
            # Gremlin query for lineage
            query = """
            g.V().has('asset_id', asset_id)
                .repeat(__.in('derived_from', 'transformed_to'))
                .times(10)
                .path()
                .by(valueMap('asset_id', 'type', 'created_at'))
            """
            
            result = self.gremlin_client.submit(
                query,
                {'asset_id': asset_id}
            ).all().result()
            
            # Process lineage path
            lineage = {
                "ancestors": [],
                "descendants": [],
                "transformations": []
            }
            
            for path in result:
                for i, node in enumerate(path):
                    if i > 0:
                        lineage["ancestors"].append({
                            "id": node.get("asset_id", [None])[0],
                            "type": node.get("type", [None])[0],
                            "created_at": node.get("created_at", [None])[0]
                        })
                        
            return lineage
            
        except Exception as e:
            logger.error(f"Error getting asset lineage: {e}")
            return {}
            
    async def _get_collaborators(self,
                               entity_id: str,
                               entity_type: str) -> List[Dict[str, Any]]:
        """Get collaborators from graph"""
        try:
            # Query for users connected to entity
            query = """
            g.V().has(label, entity_id)
                .in('created', 'modified', 'collaborated_on')
                .hasLabel('user')
                .dedup()
                .valueMap('user_id', 'name', 'role')
                .limit(10)
            """
            
            label_map = {
                "assets": "asset_id",
                "simulations": "simulation_id",
                "projects": "project_id"
            }
            
            label = label_map.get(entity_type, "id")
            
            result = self.gremlin_client.submit(
                query,
                {'label': label, 'entity_id': entity_id}
            ).all().result()
            
            collaborators = []
            for user in result:
                collaborators.append({
                    "user_id": user.get("user_id", [None])[0],
                    "name": user.get("name", [None])[0],
                    "role": user.get("role", [None])[0]
                })
                
            return collaborators
            
        except Exception as e:
            logger.error(f"Error getting collaborators: {e}")
            return []
            
    async def _semantic_expansion(self,
                                results: List[Dict[str, Any]],
                                query: str,
                                config: GraphSearchConfig) -> List[Dict[str, Any]]:
        """Expand results with semantically related entities"""
        try:
            # Get semantic relationships from graph
            expanded = results.copy()
            seen_ids = {r["_id"] for r in results}
            
            for result in results[:10]:  # Expand top 10 results
                # Query for semantically related entities
                related = await self._get_semantic_relations(
                    result["_id"],
                    result["_index"]
                )
                
                for rel in related:
                    if rel["id"] not in seen_ids:
                        # Fetch entity details
                        entity = await self._fetch_entity(
                            rel["id"],
                            rel["type"]
                        )
                        
                        if entity:
                            entity["relevance_score"] = (
                                result["relevance_score"] * 
                                config.relationship_boost * 
                                rel["similarity"]
                            )
                            entity["expansion_source"] = result["_id"]
                            entity["relationship_type"] = rel["relationship"]
                            
                            expanded.append(entity)
                            seen_ids.add(rel["id"])
                            
            return expanded
            
        except Exception as e:
            logger.error(f"Error in semantic expansion: {e}")
            return results
            
    async def _get_semantic_relations(self,
                                    entity_id: str,
                                    entity_type: str) -> List[Dict[str, Any]]:
        """Get semantically related entities"""
        try:
            response = await self.http_client.post(
                f"{self.graph_intel_url}/api/v1/graph/semantic-relations",
                json={
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "limit": 5
                }
            )
            
            if response.status_code == 200:
                return response.json()["relations"]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error getting semantic relations: {e}")
            return []
            
    async def _fetch_entity(self,
                          entity_id: str,
                          entity_type: str) -> Optional[Dict[str, Any]]:
        """Fetch entity details from Elasticsearch"""
        try:
            response = await self.es.get(
                index=entity_type,
                id=entity_id
            )
            
            return {
                "_id": response["_id"],
                "_index": response["_index"],
                "_source": response["_source"],
                "_score": 0.0  # Will be updated by caller
            }
            
        except Exception as e:
            logger.error(f"Error fetching entity: {e}")
            return None
            
    def _rank_and_aggregate(self,
                          results: List[Dict[str, Any]],
                          query: str,
                          config: GraphSearchConfig) -> List[Dict[str, Any]]:
        """Rank and aggregate results"""
        # Calculate composite scores
        for result in results:
            text_score = result.get("_score", 0)
            graph_boost = 1.0
            
            # Boost based on graph features
            if "graph_enrichment" in result:
                connections = result["graph_enrichment"].get("connection_count", 0)
                graph_boost += min(connections * 0.1, 2.0)
                
            if "collaborators" in result:
                collab_count = len(result["collaborators"])
                graph_boost += min(collab_count * 0.05, 0.5)
                
            if "lineage" in result:
                lineage_depth = len(result["lineage"].get("ancestors", []))
                graph_boost += min(lineage_depth * 0.02, 0.2)
                
            # Calculate final score
            result["composite_score"] = text_score * graph_boost
            
        # Sort by composite score
        results.sort(key=lambda x: x["composite_score"], reverse=True)
        
        # Remove duplicates
        seen = set()
        unique_results = []
        
        for result in results:
            key = f"{result['_index']}:{result['_id']}"
            if key not in seen:
                seen.add(key)
                unique_results.append(result)
                
        return unique_results
        
    def _extract_graph_context(self,
                             results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract overall graph context from results"""
        context = {
            "entity_types": {},
            "relationship_types": {},
            "clusters": [],
            "key_nodes": []
        }
        
        # Count entity types
        for result in results:
            entity_type = result["_index"]
            context["entity_types"][entity_type] = \
                context["entity_types"].get(entity_type, 0) + 1
                
        # Extract relationship types
        for result in results:
            if "graph_enrichment" in result:
                rels = result["graph_enrichment"].get("relationships", [])
                for rel in rels:
                    rel_type = rel.get("type")
                    if rel_type:
                        context["relationship_types"][rel_type] = \
                            context["relationship_types"].get(rel_type, 0) + 1
                            
        # Identify key nodes (high connectivity)
        for result in results[:20]:
            if "graph_enrichment" in result:
                conn_count = result["graph_enrichment"].get("connection_count", 0)
                if conn_count > 10:
                    context["key_nodes"].append({
                        "id": result["_id"],
                        "type": result["_index"],
                        "connections": conn_count
                    })
                    
        return context
        
    async def _generate_facets(self,
                             results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate search facets"""
        facets = {
            "entity_type": {},
            "tags": {},
            "date_ranges": {},
            "collaborators": {},
            "relationships": {}
        }
        
        for result in results:
            # Entity type facet
            entity_type = result["_index"]
            facets["entity_type"][entity_type] = \
                facets["entity_type"].get(entity_type, 0) + 1
                
            # Tags facet
            source = result.get("_source", {})
            tags = source.get("tags", [])
            for tag in tags:
                facets["tags"][tag] = facets["tags"].get(tag, 0) + 1
                
            # Collaborators facet
            if "collaborators" in result:
                for collab in result["collaborators"]:
                    name = collab.get("name", "Unknown")
                    facets["collaborators"][name] = \
                        facets["collaborators"].get(name, 0) + 1
                        
        return facets
        
    async def _generate_insights(self,
                               results: List[Dict[str, Any]],
                               query: str) -> Dict[str, Any]:
        """Generate search insights"""
        insights = {
            "query_interpretation": await self._interpret_query(query),
            "result_summary": self._summarize_results(results),
            "recommendations": await self._generate_recommendations(results, query),
            "trending_topics": await self._get_trending_topics(results)
        }
        
        return insights
        
    async def _interpret_query(self, query: str) -> Dict[str, Any]:
        """Interpret user query intent"""
        # Simplified query interpretation
        interpretation = {
            "intent": "search",
            "entities": [],
            "filters": []
        }
        
        # Check for specific patterns
        if "simulation" in query.lower():
            interpretation["entities"].append("simulation")
        if "asset" in query.lower():
            interpretation["entities"].append("asset")
        if "by" in query.lower():
            interpretation["filters"].append("author")
            
        return interpretation
        
    def _summarize_results(self,
                         results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize search results"""
        summary = {
            "total_results": len(results),
            "entity_distribution": {},
            "avg_relevance_score": 0,
            "has_graph_enrichment": 0
        }
        
        if results:
            scores = [r.get("composite_score", 0) for r in results]
            summary["avg_relevance_score"] = sum(scores) / len(scores)
            
            for result in results:
                entity_type = result["_index"]
                summary["entity_distribution"][entity_type] = \
                    summary["entity_distribution"].get(entity_type, 0) + 1
                    
                if "graph_enrichment" in result:
                    summary["has_graph_enrichment"] += 1
                    
        return summary
        
    async def _generate_recommendations(self,
                                      results: List[Dict[str, Any]],
                                      query: str) -> List[str]:
        """Generate search recommendations"""
        recommendations = []
        
        # Check result quality
        if len(results) < 5:
            recommendations.append("Try broadening your search terms")
        elif len(results) > 50:
            recommendations.append("Consider adding filters to narrow results")
            
        # Check for graph enrichment
        enriched_count = sum(1 for r in results if "graph_enrichment" in r)
        if enriched_count < len(results) * 0.5:
            recommendations.append("Enable graph enrichment for better context")
            
        return recommendations
        
    async def _get_trending_topics(self,
                                 results: List[Dict[str, Any]]) -> List[str]:
        """Get trending topics from results"""
        # Extract and count all tags
        tag_counts = {}
        
        for result in results:
            tags = result.get("_source", {}).get("tags", [])
            for tag in tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
                
        # Sort by count and return top 5
        sorted_tags = sorted(
            tag_counts.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        return [tag for tag, _ in sorted_tags[:5]]
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose()
        if self.gremlin_client:
            self.gremlin_client.close()


class GraphSearchOrchestrator:
    """Orchestrates graph-enriched search operations"""
    
    def __init__(self,
                 search_engine: GraphEnrichedSearchEngine,
                 cache_ttl: int = 300):
        self.search_engine = search_engine
        self.cache_ttl = cache_ttl
        self.cache = {}  # Simple in-memory cache
        
    async def search(self,
                   query: str,
                   filters: Optional[Dict[str, Any]] = None,
                   config: Optional[GraphSearchConfig] = None,
                   user_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute graph-enriched search with caching"""
        # Generate cache key
        cache_key = self._generate_cache_key(query, filters, config)
        
        # Check cache
        if cache_key in self.cache:
            cached_result, timestamp = self.cache[cache_key]
            if (datetime.utcnow() - timestamp).seconds < self.cache_ttl:
                return cached_result
                
        # Execute search
        result = await self.search_engine.unified_search(
            query,
            filters or {},
            config or GraphSearchConfig(),
            user_context or {}
        )
        
        # Cache result
        self.cache[cache_key] = (result, datetime.utcnow())
        
        # Clean old cache entries
        self._clean_cache()
        
        return result
        
    def _generate_cache_key(self,
                          query: str,
                          filters: Optional[Dict[str, Any]],
                          config: Optional[GraphSearchConfig]) -> str:
        """Generate cache key for search"""
        key_parts = [
            query,
            json.dumps(filters or {}, sort_keys=True),
            json.dumps(config.dict() if config else {}, sort_keys=True)
        ]
        
        return ":".join(key_parts)
        
    def _clean_cache(self):
        """Remove expired cache entries"""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for key, (_, timestamp) in self.cache.items():
            if (current_time - timestamp).seconds > self.cache_ttl:
                expired_keys.append(key)
                
        for key in expired_keys:
            del self.cache[key] 