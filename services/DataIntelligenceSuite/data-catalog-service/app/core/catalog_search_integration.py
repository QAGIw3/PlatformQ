"""
Catalog Search Integration

Integrates with the enhanced AI-powered Search Service for
intelligent catalog discovery and exploration.
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
import asyncio
from dataclasses import dataclass
from enum import Enum
import json
import httpx

from app.core.atlas_client import AtlasClient
from app.core.config import settings

logger = logging.getLogger(__name__)


class SearchIntent(str, Enum):
    """Search intent types"""
    FIND_DATASET = "find_dataset"
    EXPLORE_SCHEMA = "explore_schema"
    TRACK_LINEAGE = "track_lineage"
    FIND_QUALITY = "find_quality"
    DISCOVER_TERMS = "discover_terms"
    EXPLORE_RELATIONSHIPS = "explore_relationships"


@dataclass
class CatalogSearchResult:
    """Enhanced search result for catalog"""
    entity_id: str
    entity_type: str
    name: str
    qualified_name: str
    description: str
    score: float
    highlights: Dict[str, List[str]]
    quality_score: Optional[float]
    trust_level: Optional[str]
    business_terms: List[str]
    tags: List[str]
    layer: Optional[str]
    owner: str
    last_modified: datetime
    lineage_depth: int
    related_entities: List[Dict[str, Any]]
    suggested_actions: List[str]


class CatalogSearchIntegration:
    """
    Integrates catalog with AI-powered search service
    """
    
    def __init__(
        self,
        atlas_client: AtlasClient,
        search_service_url: Optional[str] = None
    ):
        self.atlas_client = atlas_client
        self.search_service_url = search_service_url or settings.search_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Search configuration
        self.default_search_config = {
            "enable_ai": True,
            "include_lineage": True,
            "include_quality": True,
            "include_recommendations": True,
            "max_results": 20
        }
        
    async def intelligent_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        user_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Perform intelligent catalog search with AI enhancements
        """
        try:
            # Prepare search request
            search_request = {
                "query": query,
                "filters": self._prepare_filters(filters),
                "size": self.default_search_config["max_results"],
                "include_ai": self.default_search_config["enable_ai"]
            }
            
            # Add user context for personalization
            if user_context:
                search_request["user_context"] = user_context
            
            # Call enhanced search service
            response = await self.http_client.post(
                f"{self.search_service_url}/api/v1/unified/search",
                json=search_request,
                headers={"Authorization": f"Bearer {settings.search_api_key}"}
            )
            
            if response.status_code != 200:
                logger.error(f"Search service returned {response.status_code}")
                return self._fallback_search(query, filters)
            
            search_data = response.json()
            
            # Enhance results with catalog-specific information
            enhanced_results = await self._enhance_search_results(
                search_data.get("results", []),
                search_data.get("query_analysis", {})
            )
            
            # Generate catalog-specific insights
            insights = await self._generate_catalog_insights(
                enhanced_results,
                search_data.get("query_analysis", {})
            )
            
            return {
                "query": query,
                "intent": self._determine_search_intent(query, search_data.get("query_analysis", {})),
                "results": enhanced_results,
                "total": len(enhanced_results),
                "facets": search_data.get("aggregations", {}),
                "insights": insights,
                "suggestions": search_data.get("suggestions", []),
                "query_analysis": search_data.get("query_analysis", {})
            }
            
        except Exception as e:
            logger.error(f"Error in intelligent search: {e}")
            return self._fallback_search(query, filters)
    
    def _prepare_filters(self, filters: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Prepare filters for search service
        """
        prepared = {
            "service_name": "data-catalog-service",
            "entity_type": ["dataset", "table", "schema", "column"]
        }
        
        if filters:
            # Map catalog filters to search filters
            if "layer" in filters:
                prepared["layer"] = filters["layer"]
            if "quality_min" in filters:
                prepared["quality_score_gte"] = filters["quality_min"]
            if "owner" in filters:
                prepared["owner"] = filters["owner"]
            if "tags" in filters:
                prepared["tags"] = filters["tags"]
            if "classification" in filters:
                prepared["classifications"] = filters["classification"]
        
        return prepared
    
    def _determine_search_intent(
        self,
        query: str,
        query_analysis: Dict[str, Any]
    ) -> SearchIntent:
        """
        Determine the search intent
        """
        # Use AI analysis if available
        if query_analysis.get("primary_intent"):
            intent_map = {
                "find_specific_item": SearchIntent.FIND_DATASET,
                "explore_category": SearchIntent.EXPLORE_SCHEMA,
                "get_recommendations": SearchIntent.DISCOVER_TERMS
            }
            ai_intent = query_analysis.get("primary_intent")
            if ai_intent in intent_map:
                return intent_map[ai_intent]
        
        # Fallback to keyword matching
        query_lower = query.lower()
        
        if any(word in query_lower for word in ["lineage", "source", "derived", "upstream", "downstream"]):
            return SearchIntent.TRACK_LINEAGE
        elif any(word in query_lower for word in ["quality", "trust", "accuracy", "completeness"]):
            return SearchIntent.FIND_QUALITY
        elif any(word in query_lower for word in ["term", "glossary", "business", "definition"]):
            return SearchIntent.DISCOVER_TERMS
        elif any(word in query_lower for word in ["schema", "structure", "columns", "fields"]):
            return SearchIntent.EXPLORE_SCHEMA
        elif any(word in query_lower for word in ["related", "connected", "relationship"]):
            return SearchIntent.EXPLORE_RELATIONSHIPS
        else:
            return SearchIntent.FIND_DATASET
    
    async def _enhance_search_results(
        self,
        results: List[Dict[str, Any]],
        query_analysis: Dict[str, Any]
    ) -> List[CatalogSearchResult]:
        """
        Enhance search results with catalog-specific information
        """
        enhanced = []
        
        for result in results:
            try:
                # Get full entity details from Atlas
                entity = await self.atlas_client.get_entity(result["entity_id"])
                if not entity:
                    continue
                
                # Get additional information based on intent
                quality_info = await self._get_quality_info(result["entity_id"])
                lineage_info = await self._get_lineage_summary(result["entity_id"])
                business_terms = await self._get_business_terms(result["entity_id"])
                
                # Create enhanced result
                enhanced_result = CatalogSearchResult(
                    entity_id=result["entity_id"],
                    entity_type=result["entity_type"],
                    name=entity.get("attributes", {}).get("name", ""),
                    qualified_name=entity.get("attributes", {}).get("qualifiedName", ""),
                    description=entity.get("attributes", {}).get("description", ""),
                    score=result.get("score", 0.0),
                    highlights=result.get("highlights", {}),
                    quality_score=quality_info.get("score"),
                    trust_level=quality_info.get("trust_level"),
                    business_terms=business_terms,
                    tags=entity.get("labels", []),
                    layer=entity.get("attributes", {}).get("layer"),
                    owner=entity.get("attributes", {}).get("owner", "unknown"),
                    last_modified=self._parse_datetime(
                        entity.get("attributes", {}).get("modifiedTime")
                    ),
                    lineage_depth=lineage_info.get("depth", 0),
                    related_entities=await self._get_related_entities(result["entity_id"]),
                    suggested_actions=self._generate_suggested_actions(
                        entity,
                        quality_info,
                        query_analysis
                    )
                )
                
                enhanced.append(enhanced_result)
                
            except Exception as e:
                logger.debug(f"Error enhancing result {result.get('entity_id')}: {e}")
        
        return enhanced
    
    async def _get_quality_info(self, entity_id: str) -> Dict[str, Any]:
        """
        Get quality information for entity
        """
        try:
            entity = await self.atlas_client.get_entity(entity_id)
            return {
                "score": entity.get("attributes", {}).get("dataQualityScore"),
                "trust_level": entity.get("attributes", {}).get("dataTrustLevel")
            }
        except:
            return {}
    
    async def _get_lineage_summary(self, entity_id: str) -> Dict[str, Any]:
        """
        Get lineage summary for entity
        """
        try:
            # This would call Atlas lineage API
            # Simplified for illustration
            return {
                "depth": 3,
                "upstream_count": 5,
                "downstream_count": 2
            }
        except:
            return {"depth": 0}
    
    async def _get_business_terms(self, entity_id: str) -> List[str]:
        """
        Get mapped business terms
        """
        try:
            # This would query Atlas relationships
            # Simplified for illustration
            return ["Customer Data", "Revenue Metrics"]
        except:
            return []
    
    async def _get_related_entities(self, entity_id: str) -> List[Dict[str, Any]]:
        """
        Get related entities
        """
        try:
            # This would query Atlas relationships
            # Simplified for illustration
            return [
                {
                    "entity_id": "related-1",
                    "name": "customer_orders",
                    "type": "table",
                    "relationship": "derived_from"
                }
            ]
        except:
            return []
    
    def _parse_datetime(self, datetime_str: Optional[str]) -> datetime:
        """
        Parse datetime string
        """
        if not datetime_str:
            return datetime.utcnow()
        try:
            return datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
        except:
            return datetime.utcnow()
    
    def _generate_suggested_actions(
        self,
        entity: Dict[str, Any],
        quality_info: Dict[str, Any],
        query_analysis: Dict[str, Any]
    ) -> List[str]:
        """
        Generate suggested actions based on entity state and search intent
        """
        actions = []
        
        # Quality-based actions
        quality_score = quality_info.get("score", 0)
        if quality_score and quality_score < 0.7:
            actions.append("Improve data quality")
        
        # Schema-based actions
        if not entity.get("attributes", {}).get("schema"):
            actions.append("Define schema")
        
        # Business term actions
        if not entity.get("meanings"):  # No business terms mapped
            actions.append("Map business terms")
        
        # Intent-based actions
        intent = query_analysis.get("primary_intent")
        if intent == "track_lineage" and not entity.get("lineage"):
            actions.append("Configure lineage tracking")
        
        # Classification actions
        if not entity.get("classifications"):
            actions.append("Add classifications")
        
        return actions
    
    async def _generate_catalog_insights(
        self,
        results: List[CatalogSearchResult],
        query_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate insights specific to catalog search
        """
        insights = {
            "summary": "",
            "patterns": [],
            "recommendations": [],
            "quality_summary": {},
            "lineage_summary": {}
        }
        
        if not results:
            insights["summary"] = "No catalog entries found matching your search."
            insights["recommendations"].append("Try broadening your search terms")
            return insights
        
        # Analyze results
        quality_scores = [r.quality_score for r in results if r.quality_score]
        trust_levels = [r.trust_level for r in results if r.trust_level]
        layers = [r.layer for r in results if r.layer]
        
        # Generate summary
        insights["summary"] = f"Found {len(results)} catalog entries"
        
        # Quality insights
        if quality_scores:
            avg_quality = sum(quality_scores) / len(quality_scores)
            insights["quality_summary"] = {
                "average_score": round(avg_quality, 2),
                "high_quality_count": len([s for s in quality_scores if s >= 0.8]),
                "low_quality_count": len([s for s in quality_scores if s < 0.5])
            }
            
            if avg_quality < 0.7:
                insights["recommendations"].append(
                    "Consider data quality improvement initiatives"
                )
        
        # Layer distribution
        if layers:
            layer_dist = {}
            for layer in layers:
                layer_dist[layer] = layer_dist.get(layer, 0) + 1
            insights["patterns"].append({
                "type": "layer_distribution",
                "data": layer_dist
            })
        
        # Intent-based recommendations
        intent = self._determine_search_intent(
            query_analysis.get("original_query", ""),
            query_analysis
        )
        
        if intent == SearchIntent.FIND_QUALITY:
            insights["recommendations"].append(
                "Use quality filters to find high-trust datasets"
            )
        elif intent == SearchIntent.TRACK_LINEAGE:
            insights["recommendations"].append(
                "Enable lineage view to explore data dependencies"
            )
        
        return insights
    
    async def _fallback_search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Fallback to direct Atlas search
        """
        try:
            results = await self.atlas_client.search_entities(
                query=query,
                type_name="dataset",
                limit=20
            )
            
            return {
                "query": query,
                "intent": SearchIntent.FIND_DATASET,
                "results": self._convert_atlas_results(results.get("entities", [])),
                "total": results.get("count", 0),
                "facets": {},
                "insights": {
                    "summary": "Search performed using catalog backend",
                    "recommendations": ["Enhanced search unavailable"]
                }
            }
        except Exception as e:
            logger.error(f"Fallback search failed: {e}")
            return {
                "query": query,
                "results": [],
                "total": 0,
                "error": "Search unavailable"
            }
    
    def _convert_atlas_results(
        self,
        atlas_entities: List[Dict[str, Any]]
    ) -> List[CatalogSearchResult]:
        """
        Convert Atlas results to catalog search results
        """
        results = []
        
        for entity in atlas_entities:
            try:
                result = CatalogSearchResult(
                    entity_id=entity.get("guid", ""),
                    entity_type=entity.get("typeName", ""),
                    name=entity.get("attributes", {}).get("name", ""),
                    qualified_name=entity.get("attributes", {}).get("qualifiedName", ""),
                    description=entity.get("attributes", {}).get("description", ""),
                    score=1.0,  # No relevance score from Atlas
                    highlights={},
                    quality_score=entity.get("attributes", {}).get("dataQualityScore"),
                    trust_level=entity.get("attributes", {}).get("dataTrustLevel"),
                    business_terms=[],
                    tags=entity.get("labels", []),
                    layer=entity.get("attributes", {}).get("layer"),
                    owner=entity.get("attributes", {}).get("owner", "unknown"),
                    last_modified=self._parse_datetime(
                        entity.get("attributes", {}).get("modifiedTime")
                    ),
                    lineage_depth=0,
                    related_entities=[],
                    suggested_actions=[]
                )
                results.append(result)
            except Exception as e:
                logger.debug(f"Error converting result: {e}")
        
        return results
    
    async def index_catalog_updates(self, entity_updates: List[Dict[str, Any]]):
        """
        Index catalog updates to search service
        """
        try:
            # Prepare updates for search indexing
            search_updates = []
            
            for update in entity_updates:
                entity = update.get("entity", {})
                
                # Extract searchable content
                search_doc = {
                    "entity_id": entity.get("guid"),
                    "entity_type": entity.get("typeName"),
                    "service_name": "data-catalog-service",
                    "title": entity.get("attributes", {}).get("name"),
                    "description": entity.get("attributes", {}).get("description", ""),
                    "content": self._extract_searchable_content(entity),
                    "tags": entity.get("labels", []),
                    "metadata": {
                        "qualified_name": entity.get("attributes", {}).get("qualifiedName"),
                        "owner": entity.get("attributes", {}).get("owner"),
                        "layer": entity.get("attributes", {}).get("layer"),
                        "quality_score": entity.get("attributes", {}).get("dataQualityScore"),
                        "trust_level": entity.get("attributes", {}).get("dataTrustLevel")
                    },
                    "updated_at": datetime.utcnow().isoformat()
                }
                
                search_updates.append(search_doc)
            
            # Send to search service for indexing
            if search_updates:
                response = await self.http_client.post(
                    f"{self.search_service_url}/api/v1/unified/index/bulk",
                    json={"documents": search_updates},
                    headers={"Authorization": f"Bearer {settings.search_api_key}"}
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to index updates: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Error indexing catalog updates: {e}")
    
    def _extract_searchable_content(self, entity: Dict[str, Any]) -> str:
        """
        Extract all searchable content from entity
        """
        content_parts = []
        
        attrs = entity.get("attributes", {})
        
        # Add name and description
        if attrs.get("name"):
            content_parts.append(attrs["name"])
        if attrs.get("description"):
            content_parts.append(attrs["description"])
        
        # Add column names from schema
        schema_str = attrs.get("schema")
        if schema_str:
            try:
                schema = json.loads(schema_str)
                for field in schema.get("fields", []):
                    content_parts.append(field.get("name", ""))
            except:
                pass
        
        # Add classifications
        for classification in entity.get("classifications", []):
            content_parts.append(classification.get("typeName", ""))
        
        # Add business terms
        for meaning in entity.get("meanings", []):
            content_parts.append(meaning.get("displayText", ""))
        
        return " ".join(content_parts)
    
    async def get_search_suggestions(
        self,
        prefix: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[str]:
        """
        Get search suggestions from enhanced search service
        """
        try:
            response = await self.http_client.get(
                f"{self.search_service_url}/api/v1/unified/suggestions",
                params={
                    "prefix": prefix,
                    "size": 10,
                    "context": json.dumps(context) if context else None
                },
                headers={"Authorization": f"Bearer {settings.search_api_key}"}
            )
            
            if response.status_code == 200:
                data = response.json()
                return [s["text"] for s in data.get("suggestions", [])]
                
        except Exception as e:
            logger.error(f"Error getting suggestions: {e}")
        
        return []
    
    async def track_search_analytics(
        self,
        query: str,
        results_count: int,
        clicked_result: Optional[str] = None,
        user_id: Optional[str] = None
    ):
        """
        Track search analytics
        """
        try:
            # Track search event
            await self.http_client.post(
                f"{self.search_service_url}/api/v1/unified/analytics/track",
                json={
                    "event_type": "search",
                    "query": query,
                    "results_count": results_count,
                    "service": "data-catalog",
                    "user_id": user_id,
                    "timestamp": datetime.utcnow().isoformat()
                },
                headers={"Authorization": f"Bearer {settings.search_api_key}"}
            )
            
            # Track click if provided
            if clicked_result:
                await self.http_client.post(
                    f"{self.search_service_url}/api/v1/unified/click",
                    json={
                        "query": query,
                        "result_id": clicked_result,
                        "result_type": "catalog_entity",
                        "user_id": user_id
                    },
                    headers={"Authorization": f"Bearer {settings.search_api_key}"}
                )
                
        except Exception as e:
            logger.debug(f"Error tracking analytics: {e}")
    
    async def cleanup(self):
        """
        Cleanup resources
        """
        await self.http_client.aclose() 