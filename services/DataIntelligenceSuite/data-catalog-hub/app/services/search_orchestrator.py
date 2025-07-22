"""
Search Orchestrator

Main entry point for all search operations in the Data Catalog Hub.
Coordinates between different search strategies and AI components.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
import asyncio

from elasticsearch import AsyncElasticsearch

from .interfaces import SearchResult, SearchOptions, SearchStrategy
from .ai.query_analyzer import UnifiedQueryAnalyzer
from .ai.embedding_manager import EmbeddingManager
from .storage.ignite_cache_adapter import IgniteCacheAdapter
from .strategies.base_strategy import TextSearchStrategy, ExactMatchStrategy

logger = logging.getLogger(__name__)


class SearchOrchestrator:
    """
    Orchestrates all search operations, coordinating between
    query analysis, search strategies, and result processing.
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        query_analyzer: UnifiedQueryAnalyzer,
        embedding_manager: EmbeddingManager,
        cache_adapter: Optional[IgniteCacheAdapter] = None,
        strategies: Optional[Dict[str, SearchStrategy]] = None
    ):
        self.es_client = es_client
        self.query_analyzer = query_analyzer
        self.embedding_manager = embedding_manager
        self.cache_adapter = cache_adapter
        
        # Initialize default strategies if not provided
        if strategies:
            self.strategies = strategies
        else:
            self.strategies = self._create_default_strategies()
            
        self._initialized = False
        
    def _create_default_strategies(self) -> Dict[str, SearchStrategy]:
        """Create default search strategies"""
        return {
            "text": TextSearchStrategy(self.es_client),
            "exact": ExactMatchStrategy(self.es_client),
            # Additional strategies would be added here:
            # "vector": VectorSearchStrategy(self.es_client, self.embedding_manager),
            # "hybrid": HybridSearchStrategy(self.es_client, self.embedding_manager),
            # "graph": GraphSearchStrategy(self.es_client, self.graph_client)
        }
        
    async def initialize(self):
        """Initialize all components"""
        if self._initialized:
            return
            
        try:
            # Initialize components
            init_tasks = [
                self.query_analyzer.initialize(),
                self.embedding_manager.initialize()
            ]
            
            if self.cache_adapter:
                init_tasks.append(self.cache_adapter.initialize())
                
            await asyncio.gather(*init_tasks)
            
            self._initialized = True
            logger.info("Search orchestrator initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize search orchestrator: {e}")
            raise
            
    async def search(
        self,
        query: str,
        strategy: Optional[str] = None,
        options: Optional[SearchOptions] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main search entry point
        
        Args:
            query: Search query
            strategy: Specific strategy to use (None for auto-selection)
            options: Search options
            user_id: User ID for personalization
            session_id: Session ID for context
            
        Returns:
            Search results with metadata
        """
        if not self._initialized:
            await self.initialize()
            
        start_time = datetime.utcnow()
        
        # Default options
        if options is None:
            options = SearchOptions()
            
        # Build context
        context = await self._build_context(user_id, session_id)
        
        try:
            # 1. Analyze query
            analysis = await self.query_analyzer.analyze(query, context)
            
            # 2. Determine search strategy
            if not strategy:
                strategy = self._select_strategy(analysis)
                
            if strategy not in self.strategies:
                raise ValueError(f"Unknown search strategy: {strategy}")
                
            # 3. Update options based on analysis
            options = self._update_options(options, analysis)
            
            # 4. Execute search
            search_strategy = self.strategies[strategy]
            results = await search_strategy.search(
                analysis["enhanced_query"],
                options
            )
            
            # 5. Post-process results
            results = await self._post_process_results(
                results,
                analysis,
                user_id
            )
            
            # 6. Track analytics
            elapsed_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            await self._track_search(
                query,
                strategy,
                len(results),
                elapsed_ms,
                user_id,
                session_id
            )
            
            # Return comprehensive response
            return {
                "query": query,
                "results": results,
                "metadata": {
                    "strategy_used": strategy,
                    "total_results": len(results),
                    "response_time_ms": elapsed_ms,
                    "analysis": analysis,
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
        except Exception as e:
            logger.error(f"Search failed for query '{query}': {e}")
            
            # Return error response
            return {
                "query": query,
                "results": [],
                "metadata": {
                    "error": str(e),
                    "strategy_attempted": strategy,
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
            
    def _select_strategy(self, analysis: Dict[str, Any]) -> str:
        """Select best search strategy based on query analysis"""
        intent = analysis.get("intent", {})
        primary_intent = intent.get("primary_intent", "general_search")
        
        # Map intents to strategies
        strategy_map = {
            "find_specific_item": "exact",
            "navigational": "exact",
            "technical_search": "text",  # Would be "hybrid" when available
            "code_search": "text",  # Would be "code" when available
            "find_similar": "text",  # Would be "vector" when available
            "image_search": "text",  # Would be "image" when available
        }
        
        return strategy_map.get(primary_intent, "text")
        
    def _update_options(
        self,
        options: SearchOptions,
        analysis: Dict[str, Any]
    ) -> SearchOptions:
        """Update search options based on query analysis"""
        # Add extracted filters
        if analysis.get("filters"):
            if options.filters is None:
                options.filters = {}
            options.filters.update(analysis["filters"])
            
        # Update language if detected
        entities = analysis.get("entities", {})
        if entities.get("languages"):
            options.language = entities["languages"][0]
            
        # Set boost_recent for time-sensitive queries
        if analysis.get("context", {}).get("temporal_reference"):
            options.boost_recent = True
            
        return options
        
    async def _build_context(
        self,
        user_id: Optional[str],
        session_id: Optional[str]
    ) -> Dict[str, Any]:
        """Build context for query analysis"""
        context = {}
        
        if not self.cache_adapter:
            return context
            
        # Get session history
        if session_id:
            session_key = f"session:{session_id}:queries"
            session_history = await self.cache_adapter.get(session_key)
            if session_history:
                context["session_history"] = session_history
                
        # Get user preferences
        if user_id:
            pref_key = f"user:{user_id}:preferences"
            preferences = await self.cache_adapter.get(pref_key)
            if preferences:
                context["user_preferences"] = preferences
                
        return context
        
    async def _post_process_results(
        self,
        results: List[SearchResult],
        analysis: Dict[str, Any],
        user_id: Optional[str]
    ) -> List[SearchResult]:
        """Post-process search results"""
        # Add relevance explanations if needed
        for result in results:
            if not result.explanation and analysis.get("intent"):
                result.explanation = self._generate_relevance_explanation(
                    result,
                    analysis
                )
                
        # Sort by score
        results.sort(key=lambda x: x.score, reverse=True)
        
        return results
        
    def _generate_relevance_explanation(
        self,
        result: SearchResult,
        analysis: Dict[str, Any]
    ) -> str:
        """Generate simple relevance explanation"""
        explanations = []
        
        # Check for keyword matches
        keywords = [kw["text"] for kw in analysis.get("entities", {}).get("keywords", [])]
        matched_keywords = []
        
        result_text = (
            f"{result.source.get('name', '')} "
            f"{result.source.get('title', '')} "
            f"{result.source.get('description', '')}"
        ).lower()
        
        for keyword in keywords:
            if keyword.lower() in result_text:
                matched_keywords.append(keyword)
                
        if matched_keywords:
            explanations.append(f"Matches keywords: {', '.join(matched_keywords)}")
            
        # Check search type
        if result.search_type:
            explanations.append(f"Found via {result.search_type} search")
            
        return "; ".join(explanations) if explanations else "Relevant to your query"
        
    async def _track_search(
        self,
        query: str,
        strategy: str,
        result_count: int,
        elapsed_ms: int,
        user_id: Optional[str],
        session_id: Optional[str]
    ):
        """Track search analytics"""
        if not self.cache_adapter:
            return
            
        try:
            # Track in session
            if session_id:
                session_key = f"session:{session_id}:queries"
                session_queries = await self.cache_adapter.get(session_key) or []
                session_queries.append({
                    "query": query,
                    "strategy": strategy,
                    "timestamp": datetime.utcnow().isoformat(),
                    "result_count": result_count
                })
                # Keep last 20 queries
                session_queries = session_queries[-20:]
                await self.cache_adapter.set(session_key, session_queries, ttl=3600)
                
            # Track global search metrics
            metric_key = f"search_metrics:{datetime.utcnow().strftime('%Y-%m-%d')}"
            metrics = await self.cache_adapter.get(metric_key) or {
                "total_searches": 0,
                "total_response_time": 0,
                "strategies": {}
            }
            
            metrics["total_searches"] += 1
            metrics["total_response_time"] += elapsed_ms
            
            if strategy not in metrics["strategies"]:
                metrics["strategies"][strategy] = 0
            metrics["strategies"][strategy] += 1
            
            await self.cache_adapter.set(metric_key, metrics, ttl=86400 * 7)  # 7 days
            
        except Exception as e:
            logger.warning(f"Failed to track search analytics: {e}")
            
    async def get_suggestions(
        self,
        query: str,
        user_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get search suggestions for a query"""
        if not self._initialized:
            await self.initialize()
            
        context = await self._build_context(user_id, None)
        analysis = await self.query_analyzer.analyze(query, context)
        
        return analysis.get("suggestions", []) 