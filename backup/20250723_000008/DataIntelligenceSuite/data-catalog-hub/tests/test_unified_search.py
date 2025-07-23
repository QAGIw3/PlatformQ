"""
Test Unified Search Service

Tests for the consolidated search functionality.
"""

import pytest
from unittest.mock import AsyncMock, Mock

from app.services.interfaces import SearchOptions, SearchResult, ServiceResult


class TestUnifiedSearchService:
    """Test unified search service."""
    
    @pytest.mark.asyncio
    async def test_text_search(self, search_service, mock_elasticsearch):
        """Test text-based search."""
        # Configure mock response
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "doc1",
                        "_score": 0.95,
                        "_source": {
                            "title": "Test Document",
                            "content": "This is a test",
                            "entity_type": "dataset"
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Perform search
        result = await search_service.text_search(
            query="test",
            fields=["title", "content"],
            limit=10
        )
        
        assert result.success
        assert len(result.data["results"]) == 1
        assert result.data["results"][0]["id"] == "doc1"
        assert result.data["strategy"] == "text"
    
    @pytest.mark.asyncio
    async def test_vector_search(self, search_service, mock_elasticsearch):
        """Test vector similarity search."""
        # Configure mock for vector search
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "vec1",
                        "_score": 0.89,
                        "_source": {
                            "title": "Similar Document",
                            "entity_type": "dataset",
                            "text_embedding": [0.1] * 768
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Mock embedding generation
        search_service.embedding_manager.embed = AsyncMock(
            return_value=[0.2] * 768
        )
        
        # Perform vector search
        result = await search_service.vector_search(
            query="find similar datasets",
            entity_types=["dataset"],
            threshold=0.7,
            limit=5
        )
        
        assert result.success
        assert len(result.data["results"]) == 1
        assert result.data["threshold"] == 0.7
        assert "metrics" in result.data
    
    @pytest.mark.asyncio
    async def test_hybrid_search(self, search_service, mock_elasticsearch):
        """Test hybrid text + vector search."""
        # Configure mock for hybrid results
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "hybrid1",
                        "_score": 0.92,
                        "_source": {
                            "title": "Hybrid Result",
                            "content": "Matches both text and vector",
                            "entity_type": "table"
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Perform hybrid search
        result = await search_service.hybrid_search(
            query="hybrid test query",
            text_weight=0.6,
            vector_weight=0.4,
            limit=10
        )
        
        assert result.success
        assert result.data["strategy"] == "hybrid"
        assert result.data["weights"]["text"] == 0.6
        assert result.data["weights"]["vector"] == 0.4
    
    @pytest.mark.asyncio
    async def test_ai_powered_search(self, search_service, mock_elasticsearch):
        """Test AI-enhanced search with query understanding."""
        # Configure mocks
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "ai1",
                        "_score": 0.95,
                        "_source": {
                            "title": "Customer Data",
                            "content": "Customer purchase history",
                            "entity_type": "table"
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Mock query analyzer
        search_service.query_analyzer.analyze = AsyncMock(
            return_value={
                "enhanced_query": "customer purchase data",
                "intent": {"type": "data_discovery"},
                "entities": {"business_term": ["customer", "purchase"]}
            }
        )
        
        # Perform AI-powered search
        result = await search_service.ai_powered_search(
            query="show me customer buying patterns",
            use_rag=True,
            include_explanations=True
        )
        
        assert result.success
        assert "enhanced_query" in result.data
        assert "query_analysis" in result.data
        assert result.data["results"][0].get("explanation") is not None
    
    @pytest.mark.asyncio
    async def test_search_with_filters(self, search_service, mock_elasticsearch):
        """Test search with filters."""
        # Configure mock
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "filtered1",
                        "_score": 0.88,
                        "_source": {
                            "title": "Filtered Result",
                            "entity_type": "table",
                            "classification": "PII"
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Search with filters
        options = SearchOptions(
            limit=10,
            filters={
                "entity_type": "table",
                "classification": "PII"
            }
        )
        
        result = await search_service.search("sensitive data", options)
        
        assert result.success
        assert len(result.data["results"]) == 1
        assert result.data["results"][0]["metadata"]["classification"] == "PII"
    
    @pytest.mark.asyncio
    async def test_find_similar(self, search_service, mock_elasticsearch):
        """Test finding similar entities."""
        # Configure mock to return entity with embedding
        mock_elasticsearch.get.return_value = {
            "found": True,
            "_source": {
                "text_embedding": [0.1] * 768,
                "entity_type": "table"
            }
        }
        
        mock_elasticsearch.search.return_value = {
            "hits": {
                "hits": [
                    {
                        "_id": "similar1",
                        "_score": 0.93,
                        "_source": {
                            "title": "Similar Table",
                            "entity_type": "table"
                        }
                    }
                ],
                "total": {"value": 1}
            }
        }
        
        # Find similar entities
        result = await search_service.find_similar(
            entity_id="source-entity",
            limit=5
        )
        
        assert result.success
        assert "similar_entities" in result.data
        assert len(result.data["similar_entities"]) == 1
    
    @pytest.mark.asyncio
    async def test_get_facets(self, search_service, mock_elasticsearch):
        """Test getting search facets."""
        # Configure mock for aggregations
        mock_elasticsearch.search.return_value = {
            "aggregations": {
                "entity_type": {
                    "buckets": [
                        {"key": "table", "doc_count": 50},
                        {"key": "dataset", "doc_count": 30}
                    ]
                },
                "classification": {
                    "buckets": [
                        {"key": "PII", "doc_count": 15},
                        {"key": "Financial", "doc_count": 10}
                    ]
                }
            }
        }
        
        # Get facets
        result = await search_service.get_facets(
            query="data",
            fields=["entity_type", "classification"]
        )
        
        assert result.success
        assert "facets" in result.data
        assert len(result.data["facets"]["entity_type"]) == 2
        assert result.data["facets"]["entity_type"][0]["value"] == "table"
        assert result.data["facets"]["entity_type"][0]["count"] == 50
    
    @pytest.mark.asyncio
    async def test_search_caching(self, search_service, mock_ignite_cache):
        """Test search result caching."""
        # Configure cache to return cached result
        cached_result = {
            "results": [{"id": "cached1", "title": "Cached Result"}],
            "total": 1,
            "cached": True
        }
        mock_ignite_cache.get.return_value = cached_result
        
        # Perform search (should hit cache)
        result = await search_service.search("cached query")
        
        assert result.success
        assert result.data["cached"] is True
        assert len(result.data["results"]) == 1
        
        # Verify cache was checked
        mock_ignite_cache.get.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_search_error_handling(self, search_service, mock_elasticsearch):
        """Test error handling in search."""
        # Configure mock to raise exception
        mock_elasticsearch.search.side_effect = Exception("Elasticsearch error")
        
        # Perform search
        result = await search_service.search("error test")
        
        assert not result.success
        assert "error" in result.error.lower()
        assert result.details is not None 