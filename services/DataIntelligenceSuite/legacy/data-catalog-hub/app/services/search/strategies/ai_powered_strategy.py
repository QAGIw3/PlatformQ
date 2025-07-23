"""
AI-Powered Search Strategy

Natural language search with AI query understanding and RAG support.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
import asyncio
import json

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions
from app.services.ai import UnifiedQueryAnalyzer, EmbeddingManager
from .base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class AIPoweredSearchStrategy(BaseSearchStrategy):
    """
    AI-powered search strategy using natural language understanding.
    
    Features:
    - Natural language query processing
    - Intent detection and query expansion
    - Multi-hop graph traversal
    - RAG (Retrieval Augmented Generation) support
    - Contextual understanding
    - Query rewriting and optimization
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        query_analyzer: UnifiedQueryAnalyzer,
        embedding_manager: EmbeddingManager
    ):
        super().__init__(es_client)
        self.query_analyzer = query_analyzer
        self.embedding_manager = embedding_manager
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute AI-powered natural language search."""
        try:
            # Analyze query with AI
            query_analysis = await self._analyze_query(query, options)
            
            # Extract search parameters from analysis
            search_params = self._extract_search_parameters(query_analysis)
            
            # Build multi-stage search pipeline
            search_pipeline = await self._build_search_pipeline(
                query,
                query_analysis,
                search_params,
                options
            )
            
            # Execute search pipeline
            results = await self._execute_search_pipeline(
                search_pipeline,
                options
            )
            
            # Apply AI-based re-ranking
            if getattr(options, 'include_reasoning', True):
                results = await self._ai_rerank_results(
                    results,
                    query,
                    query_analysis
                )
                
            # Add AI explanations
            if getattr(options, 'include_explanations', True):
                results = await self._add_ai_explanations(
                    results,
                    query,
                    query_analysis
                )
                
            return results
            
        except Exception as e:
            logger.error(f"AI-powered search failed: {e}")
            raise
            
    async def _analyze_query(
        self,
        query: str,
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Analyze query using AI to understand intent and context."""
        # Get user context
        context = {
            'tenant_id': options.tenant_id,
            'user_history': getattr(options, 'user_history', []),
            'session_context': getattr(options, 'session_context', {}),
            'domain_context': getattr(options, 'domain_context', {})
        }
        
        # Analyze query
        analysis = await self.query_analyzer.analyze(query, context)
        
        # Enhance with additional analysis
        analysis['query_type'] = self._detect_query_type(query)
        analysis['temporal_context'] = self._extract_temporal_context(query)
        analysis['comparison_context'] = self._extract_comparison_context(query)
        
        return analysis
        
    def _detect_query_type(self, query: str) -> str:
        """Detect the type of query."""
        query_lower = query.lower()
        
        # Question patterns
        if any(query_lower.startswith(q) for q in ['what', 'where', 'when', 'who', 'how', 'why']):
            return 'question'
            
        # Navigation patterns
        if any(phrase in query_lower for phrase in ['show me', 'find', 'list', 'get']):
            return 'navigation'
            
        # Analysis patterns
        if any(phrase in query_lower for phrase in ['analyze', 'compare', 'difference between']):
            return 'analysis'
            
        # Definition patterns
        if any(phrase in query_lower for phrase in ['what is', 'define', 'meaning of']):
            return 'definition'
            
        return 'general'
        
    def _extract_temporal_context(self, query: str) -> Dict[str, Any]:
        """Extract temporal context from query."""
        temporal_context = {}
        
        query_lower = query.lower()
        
        # Recent patterns
        if any(phrase in query_lower for phrase in ['recent', 'latest', 'newest', 'today']):
            temporal_context['recency'] = 'very_recent'
            temporal_context['boost_recent'] = True
            
        # Historical patterns
        elif any(phrase in query_lower for phrase in ['old', 'historical', 'archive']):
            temporal_context['recency'] = 'historical'
            temporal_context['boost_recent'] = False
            
        # Specific time periods
        if 'last week' in query_lower:
            temporal_context['time_range'] = 'last_week'
        elif 'last month' in query_lower:
            temporal_context['time_range'] = 'last_month'
        elif 'last year' in query_lower:
            temporal_context['time_range'] = 'last_year'
            
        return temporal_context
        
    def _extract_comparison_context(self, query: str) -> Dict[str, Any]:
        """Extract comparison context from query."""
        comparison_context = {}
        
        query_lower = query.lower()
        
        # Comparison patterns
        if 'difference between' in query_lower or 'compare' in query_lower:
            comparison_context['type'] = 'comparison'
            # Extract entities being compared
            # This would be enhanced with NER
            
        # Similarity patterns
        elif 'similar to' in query_lower or 'like' in query_lower:
            comparison_context['type'] = 'similarity'
            
        return comparison_context
        
    def _extract_search_parameters(
        self,
        query_analysis: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract search parameters from query analysis."""
        params = {
            'primary_intent': query_analysis.get('intent', 'search'),
            'entities': query_analysis.get('entities', []),
            'keywords': query_analysis.get('keywords', []),
            'filters': {},
            'boosters': []
        }
        
        # Extract entity types
        if query_analysis.get('entity_types'):
            params['filters']['entity_types'] = query_analysis['entity_types']
            
        # Extract classifications
        if query_analysis.get('classifications'):
            params['filters']['classifications'] = query_analysis['classifications']
            
        # Extract quality requirements
        if query_analysis.get('quality_requirements'):
            params['filters']['min_quality_score'] = query_analysis['quality_requirements'].get('min_score', 0.7)
            
        # Set boosters based on analysis
        if query_analysis.get('temporal_context', {}).get('boost_recent'):
            params['boosters'].append('recency')
            
        if query_analysis.get('requires_high_quality'):
            params['boosters'].append('quality')
            
        return params
        
    async def _build_search_pipeline(
        self,
        query: str,
        query_analysis: Dict[str, Any],
        search_params: Dict[str, Any],
        options: SearchOptions
    ) -> List[Dict[str, Any]]:
        """Build multi-stage search pipeline."""
        pipeline = []
        
        # Stage 1: Semantic search for relevant entities
        semantic_stage = {
            'stage': 'semantic_search',
            'query': await self._build_semantic_query(
                query,
                query_analysis,
                options
            )
        }
        pipeline.append(semantic_stage)
        
        # Stage 2: Expansion search for related entities
        if query_analysis.get('query_type') in ['question', 'analysis']:
            expansion_stage = {
                'stage': 'expansion_search',
                'query': await self._build_expansion_query(
                    query_analysis,
                    search_params,
                    options
                )
            }
            pipeline.append(expansion_stage)
            
        # Stage 3: Graph traversal for multi-hop relationships
        if getattr(options, 'max_hops', 0) > 0:
            graph_stage = {
                'stage': 'graph_traversal',
                'max_hops': options.max_hops,
                'relationship_types': query_analysis.get('relationship_types', [])
            }
            pipeline.append(graph_stage)
            
        return pipeline
        
    async def _build_semantic_query(
        self,
        query: str,
        query_analysis: Dict[str, Any],
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build semantic search query."""
        # Generate embedding
        query_embedding = await self.embedding_manager.embed_text(
            query_analysis.get('enhanced_query', query)
        )
        
        # Build combined text + vector query
        es_query = {
            "size": options.size * 2,  # Get more for re-ranking
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": [
                                    "name^5",
                                    "title^5",
                                    "description^3",
                                    "content^2"
                                ],
                                "type": "best_fields",
                                "boost": 0.3
                            }
                        }
                    ]
                }
            },
            "knn": {
                "field": "text_embedding",
                "query_vector": query_embedding.tolist(),
                "k": options.size * 2,
                "num_candidates": options.size * 10,
                "boost": 0.7
            }
        }
        
        # Apply filters
        filters = self._build_filters(options)
        if filters:
            es_query["query"]["bool"]["filter"] = filters
            es_query["knn"]["filter"] = {"bool": {"must": filters}}
            
        return es_query
        
    async def _build_expansion_query(
        self,
        query_analysis: Dict[str, Any],
        search_params: Dict[str, Any],
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build query for expansion search."""
        # Extract key terms for expansion
        expansion_terms = []
        
        # Add entities
        for entity in search_params.get('entities', []):
            expansion_terms.append(entity['text'])
            
        # Add keywords
        expansion_terms.extend(search_params.get('keywords', []))
        
        # Add synonyms and related terms
        for term in expansion_terms[:]:
            synonyms = query_analysis.get('synonyms', {}).get(term, [])
            expansion_terms.extend(synonyms)
            
        # Build expansion query
        should_clauses = []
        for term in expansion_terms:
            should_clauses.append({
                "match": {
                    "content": {
                        "query": term,
                        "boost": 0.5
                    }
                }
            })
            
        es_query = {
            "size": options.size,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            }
        }
        
        return es_query
        
    async def _execute_search_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute the search pipeline stages."""
        all_results = []
        seen_ids = set()
        
        # Get index
        index = self._get_index_name()
        
        for stage in pipeline:
            if stage['stage'] == 'semantic_search':
                # Execute semantic search
                response = await self.es_client.search(
                    index=index,
                    body=stage['query']
                )
                
                # Convert results
                results = self._convert_hits_to_results(
                    response["hits"]["hits"],
                    search_type="ai_semantic"
                )
                
                # Add unique results
                for result in results:
                    if result.id not in seen_ids:
                        seen_ids.add(result.id)
                        all_results.append(result)
                        
            elif stage['stage'] == 'expansion_search':
                # Execute expansion search
                response = await self.es_client.search(
                    index=index,
                    body=stage['query']
                )
                
                # Convert results
                results = self._convert_hits_to_results(
                    response["hits"]["hits"],
                    search_type="ai_expansion"
                )
                
                # Add unique results with lower scores
                for result in results:
                    if result.id not in seen_ids:
                        seen_ids.add(result.id)
                        result.score *= 0.7  # Lower score for expansion results
                        all_results.append(result)
                        
            elif stage['stage'] == 'graph_traversal':
                # This would integrate with graph database
                # For now, we'll skip this stage
                pass
                
        # Sort by score
        all_results.sort(key=lambda x: x.score, reverse=True)
        
        # Return top results
        return all_results[:options.size]
        
    async def _ai_rerank_results(
        self,
        results: List[SearchResult],
        query: str,
        query_analysis: Dict[str, Any]
    ) -> List[SearchResult]:
        """Re-rank results using AI understanding."""
        # Simple re-ranking based on query analysis
        # In production, this could use a cross-encoder model
        
        for result in results:
            # Boost based on intent match
            if query_analysis.get('primary_intent') == 'definition':
                if result.description and 'definition' in result.description.lower():
                    result.score *= 1.5
                    
            # Boost based on entity type match
            expected_types = query_analysis.get('entity_types', [])
            if expected_types and result.entity_type in expected_types:
                result.score *= 1.3
                
            # Boost based on query type
            if query_analysis.get('query_type') == 'question':
                # Boost results that look like answers
                if result.description and len(result.description) > 100:
                    result.score *= 1.2
                    
        # Re-sort
        results.sort(key=lambda x: x.score, reverse=True)
        
        return results
        
    async def _add_ai_explanations(
        self,
        results: List[SearchResult],
        query: str,
        query_analysis: Dict[str, Any]
    ) -> List[SearchResult]:
        """Add AI-generated explanations to results."""
        for result in results:
            explanation = {
                'relevance_reason': self._generate_relevance_explanation(
                    result,
                    query,
                    query_analysis
                ),
                'confidence': self._calculate_confidence(result, query_analysis),
                'query_understanding': query_analysis.get('intent_explanation', '')
            }
            
            result.ai_explanation = explanation
            
        return results
        
    def _generate_relevance_explanation(
        self,
        result: SearchResult,
        query: str,
        query_analysis: Dict[str, Any]
    ) -> str:
        """Generate explanation for why result is relevant."""
        reasons = []
        
        # Name match
        if result.name and any(
            keyword.lower() in result.name.lower()
            for keyword in query_analysis.get('keywords', [])
        ):
            reasons.append(f"Name matches query terms")
            
        # Type match
        if query_analysis.get('entity_types') and result.entity_type in query_analysis['entity_types']:
            reasons.append(f"Entity type matches expected type")
            
        # Semantic match
        if result.search_type == 'ai_semantic':
            reasons.append("Strong semantic similarity to query")
            
        # Quality
        if result.quality_score and result.quality_score > 0.8:
            reasons.append("High quality data asset")
            
        return "; ".join(reasons) if reasons else "General relevance to query"
        
    def _calculate_confidence(
        self,
        result: SearchResult,
        query_analysis: Dict[str, Any]
    ) -> float:
        """Calculate confidence score for result relevance."""
        confidence = result.score
        
        # Adjust based on query clarity
        query_clarity = query_analysis.get('clarity_score', 0.5)
        confidence *= (0.5 + query_clarity * 0.5)
        
        # Ensure in range [0, 1]
        return min(max(confidence, 0.0), 1.0) 