"""
Hybrid Search Service

Combines traditional text search with semantic vector search for optimal results.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import asyncio
from collections import defaultdict
import numpy as np

from elasticsearch import AsyncElasticsearch
from .vector_search import VectorSearchService

logger = logging.getLogger(__name__)


class HybridSearchService:
    """Combines text and vector search for enhanced results"""
    
    def __init__(self, es_client: AsyncElasticsearch, vector_service: VectorSearchService):
        self.es_client = es_client
        self.vector_service = vector_service
        
    async def hybrid_search(self, query: str, tenant_id: Optional[str] = None,
                            size: int = 10, filters: Optional[Dict[str, Any]] = None,
                            text_weight: float = 0.6, vector_weight: float = 0.4,
                            rerank: bool = True) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining text and vector results
        
        :param query: Search query
        :param tenant_id: Tenant ID for filtering
        :param size: Number of results to return
        :param filters: Additional filters
        :param text_weight: Weight for text search results (0-1)
        :param vector_weight: Weight for vector search results (0-1)
        :param rerank: Whether to apply re-ranking
        :return: Combined and ranked results
        """
        # Normalize weights
        total_weight = text_weight + vector_weight
        text_weight = text_weight / total_weight
        vector_weight = vector_weight / total_weight
        
        # Perform searches in parallel
        text_task = asyncio.create_task(
            self._text_search(query, tenant_id, size * 2, filters)
        )
        vector_task = asyncio.create_task(
            self._vector_search(query, tenant_id, size * 2, filters)
        )
        
        # Wait for both searches
        text_results, vector_results = await asyncio.gather(text_task, vector_task)
        
        # Combine results
        combined_results = self._combine_results(
            text_results, vector_results, 
            text_weight, vector_weight
        )
        
        # Apply re-ranking if requested
        if rerank:
            combined_results = await self._rerank_results(combined_results, query)
        
        # Return top results
        return combined_results[:size]
    
    async def _text_search(self, query: str, tenant_id: Optional[str],
                           size: int, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform text search using Elasticsearch"""
        try:
            # Build query
            es_query = {
                "size": size,
                "query": {
                    "bool": {
                        "must": {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "name^3",
                                    "title^3", 
                                    "description^2",
                                    "content",
                                    "tags^2",
                                    "metadata.keywords^2"
                                ],
                                "type": "best_fields",
                                "fuzziness": "AUTO",
                                "prefix_length": 2
                            }
                        }
                    }
                },
                "highlight": {
                    "fields": {
                        "name": {},
                        "title": {},
                        "description": {},
                        "content": {"fragment_size": 150}
                    }
                }
            }
            
            # Add filters
            if filters or tenant_id:
                es_query["query"]["bool"]["filter"] = []
                
                if tenant_id:
                    es_query["query"]["bool"]["filter"].append({
                        "term": {"tenant_id": tenant_id}
                    })
                
                if filters:
                    for field, value in filters.items():
                        if isinstance(value, list):
                            es_query["query"]["bool"]["filter"].append({
                                "terms": {field: value}
                            })
                        else:
                            es_query["query"]["bool"]["filter"].append({
                                "term": {field: value}
                            })
            
            # Execute search
            response = await self.es_client.search(
                index="platformq_search",
                body=es_query
            )
            
            # Process results
            results = []
            max_score = response["hits"]["max_score"] or 1.0
            
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"] / max_score,  # Normalize scores
                    "source": hit["_source"],
                    "search_type": "text",
                    "highlights": hit.get("highlight", {})
                }
                results.append(result)
            
            return results
            
        except Exception as e:
            logger.error(f"Text search failed: {e}")
            return []
    
    async def _vector_search(self, query: str, tenant_id: Optional[str],
                             size: int, filters: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Perform vector search using Milvus"""
        try:
            # Perform vector search
            results = await self.vector_service.search(
                query=query,
                collection_name="text_embeddings",
                tenant_id=tenant_id,
                top_k=size,
                filters=filters
            )
            
            # Add search type to results
            for result in results:
                result["search_type"] = "vector"
            
            return results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            return []
    
    def _combine_results(self, text_results: List[Dict[str, Any]], 
                         vector_results: List[Dict[str, Any]],
                         text_weight: float, vector_weight: float) -> List[Dict[str, Any]]:
        """Combine and score results from both searches"""
        # Create a map to track combined scores
        combined_scores = defaultdict(lambda: {
            "text_score": 0.0,
            "vector_score": 0.0,
            "combined_score": 0.0,
            "result": None
        })
        
        # Process text results
        for result in text_results:
            doc_id = result["id"]
            combined_scores[doc_id]["text_score"] = result["score"]
            combined_scores[doc_id]["result"] = result
        
        # Process vector results
        for result in vector_results:
            doc_id = result["id"]
            combined_scores[doc_id]["vector_score"] = result["score"]
            if combined_scores[doc_id]["result"] is None:
                combined_scores[doc_id]["result"] = result
            else:
                # Merge metadata from vector search
                combined_scores[doc_id]["result"]["vector_metadata"] = result.get("metadata", {})
        
        # Calculate combined scores
        final_results = []
        for doc_id, scores in combined_scores.items():
            # Calculate weighted combined score
            combined_score = (
                scores["text_score"] * text_weight +
                scores["vector_score"] * vector_weight
            )
            
            # Apply boost for results that appear in both searches
            if scores["text_score"] > 0 and scores["vector_score"] > 0:
                combined_score *= 1.2  # 20% boost
            
            # Create final result
            result = scores["result"].copy()
            result.update({
                "combined_score": combined_score,
                "text_score": scores["text_score"],
                "vector_score": scores["vector_score"],
                "search_types": self._get_search_types(scores)
            })
            
            final_results.append(result)
        
        # Sort by combined score
        final_results.sort(key=lambda x: x["combined_score"], reverse=True)
        
        return final_results
    
    def _get_search_types(self, scores: Dict[str, float]) -> List[str]:
        """Determine which search types found this result"""
        types = []
        if scores["text_score"] > 0:
            types.append("text")
        if scores["vector_score"] > 0:
            types.append("vector")
        return types
    
    async def _rerank_results(self, results: List[Dict[str, Any]], 
                              query: str) -> List[Dict[str, Any]]:
        """Apply advanced re-ranking to results"""
        # Simple re-ranking based on result diversity and relevance
        # In production, this could use a learned ranking model
        
        seen_sources = set()
        diverse_results = []
        other_results = []
        
        for result in results:
            source = result.get("source", {}).get("source_service", "unknown")
            
            # Promote diversity by limiting results from same source
            if source not in seen_sources or len(seen_sources) < 3:
                diverse_results.append(result)
                seen_sources.add(source)
            else:
                other_results.append(result)
        
        # Combine diverse results first, then others
        reranked = diverse_results + other_results
        
        # Apply query-specific boosts
        for result in reranked:
            # Boost exact matches
            name = result.get("source", {}).get("name", "").lower()
            if query.lower() in name:
                result["combined_score"] *= 1.5
            
            # Boost recent results
            if "created_at" in result.get("source", {}):
                # Implementation would check recency
                pass
        
        # Re-sort after boosts
        reranked.sort(key=lambda x: x["combined_score"], reverse=True)
        
        return reranked
    
    async def explain_search(self, query: str, doc_id: str, 
                             tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Explain why a document matched a query"""
        explanation = {
            "query": query,
            "document_id": doc_id,
            "explanations": {}
        }
        
        # Get text search explanation
        try:
            es_explain = await self.es_client.explain(
                index="platformq_search",
                id=doc_id,
                body={
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["name^3", "description^2", "content"]
                        }
                    }
                }
            )
            
            if es_explain["matched"]:
                explanation["explanations"]["text_search"] = {
                    "matched": True,
                    "score": es_explain["explanation"]["value"],
                    "description": es_explain["explanation"]["description"]
                }
        except:
            pass
        
        # Get vector similarity
        try:
            # Get document embedding
            similar_docs = await self.vector_service.find_similar(
                doc_id=doc_id,
                tenant_id=tenant_id,
                top_k=1
            )
            
            if similar_docs:
                explanation["explanations"]["vector_search"] = {
                    "matched": True,
                    "similarity_score": similar_docs[0]["score"]
                }
        except:
            pass
        
        return explanation


class SearchOptimizer:
    """Optimize search queries for better results"""
    
    def __init__(self):
        self.common_synonyms = {
            "ml": ["machine learning", "ML", "artificial intelligence"],
            "ai": ["artificial intelligence", "AI", "machine learning"],
            "3d": ["three dimensional", "3D", "threed"],
            "cad": ["computer aided design", "CAD", "computer-aided design"],
            "api": ["application programming interface", "API"],
            "ui": ["user interface", "UI", "interface"],
            "ux": ["user experience", "UX", "experience"]
        }
        
    def expand_query(self, query: str) -> str:
        """Expand query with synonyms and variations"""
        expanded_terms = [query]
        
        # Check for acronyms and expand
        query_lower = query.lower()
        for term, synonyms in self.common_synonyms.items():
            if term in query_lower:
                expanded_terms.extend(synonyms)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_terms = []
        for term in expanded_terms:
            if term.lower() not in seen:
                seen.add(term.lower())
                unique_terms.append(term)
        
        return " OR ".join(f'"{term}"' if " " in term else term for term in unique_terms)
    
    def extract_filters(self, query: str) -> Tuple[str, Dict[str, Any]]:
        """Extract filters from natural language query"""
        filters = {}
        cleaned_query = query
        
        # Date filters
        import re
        date_pattern = r'(created|updated|modified)\s+(after|before|since)\s+(\d{4}-\d{2}-\d{2})'
        date_matches = re.findall(date_pattern, query, re.IGNORECASE)
        
        for field, operator, date in date_matches:
            filter_field = f"{field}_at"
            if operator in ["after", "since"]:
                filters[filter_field] = {"gte": date}
            else:
                filters[filter_field] = {"lte": date}
            
            # Remove from query
            cleaned_query = re.sub(f'{field}\s+{operator}\s+{date}', '', cleaned_query, flags=re.IGNORECASE)
        
        # File type filters
        type_pattern = r'type:\s*(\w+)'
        type_matches = re.findall(type_pattern, query, re.IGNORECASE)
        
        if type_matches:
            filters["file_type"] = type_matches[0].upper()
            cleaned_query = re.sub(type_pattern, '', cleaned_query, flags=re.IGNORECASE)
        
        # Clean up query
        cleaned_query = ' '.join(cleaned_query.split())
        
        return cleaned_query, filters 