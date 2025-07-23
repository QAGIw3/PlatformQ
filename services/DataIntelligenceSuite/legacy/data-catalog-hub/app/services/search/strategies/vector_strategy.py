"""
Vector Search Strategy

Semantic search using Elasticsearch 8's native k-NN capabilities.
"""

import logging
from typing import List, Dict, Any, Optional, Union
import numpy as np

from elasticsearch import AsyncElasticsearch

from app.services.interfaces import SearchResult, SearchOptions
from app.services.ai import EmbeddingManager
from .base import BaseSearchStrategy

logger = logging.getLogger(__name__)


class VectorSearchStrategy(BaseSearchStrategy):
    """
    Vector-based semantic search using Elasticsearch k-NN.
    
    This strategy consolidates all vector search capabilities:
    - Native ES8 k-NN search with HNSW algorithm
    - Multiple embedding models (text, code, multilingual)
    - Semantic similarity search
    - Multi-modal support
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        embedding_manager: EmbeddingManager
    ):
        super().__init__(es_client)
        self.embedding_manager = embedding_manager
        
        # Vector field configurations
        self.vector_fields = {
            "text": {
                "field": "text_embedding",
                "dims": 768,
                "similarity": "cosine"
            },
            "title": {
                "field": "title_embedding", 
                "dims": 768,
                "similarity": "cosine"
            },
            "description": {
                "field": "description_embedding",
                "dims": 768,
                "similarity": "cosine"
            },
            "code": {
                "field": "code_embedding",
                "dims": 768,
                "similarity": "dot_product"
            },
            "multilingual": {
                "field": "multilingual_embedding",
                "dims": 768,
                "similarity": "cosine"
            }
        }
        
    async def search(
        self,
        query: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Execute vector-based semantic search."""
        try:
            # Generate query embedding
            embedding_type = getattr(options, 'embedding_type', 'text')
            query_embedding = await self._generate_embedding(query, embedding_type)
            
            # Select vector fields to search
            vector_fields = self._select_vector_fields(options)
            
            # Build k-NN query
            es_query = await self._build_knn_query(
                query_embedding,
                vector_fields,
                options
            )
            
            # Get appropriate index
            index = self._get_index_name(
                options.filters.get('entity_type') if options.filters else None
            )
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                body=es_query
            )
            
            # Convert results
            results = self._convert_hits_to_results(
                response["hits"]["hits"],
                search_type="vector"
            )
            
            # Add semantic metadata
            for result in results:
                result.metadata = {
                    "total_hits": response["hits"]["total"]["value"],
                    "embedding_type": embedding_type,
                    "vector_fields": vector_fields,
                    "took_ms": response.get("took", 0)
                }
                
            return results
            
        except Exception as e:
            logger.error(f"Vector search failed: {e}")
            raise
            
    async def _generate_embedding(
        self,
        text: str,
        embedding_type: str = 'text'
    ) -> np.ndarray:
        """Generate embedding for the query."""
        if embedding_type == 'code':
            return await self.embedding_manager.embed_code(text)
        elif embedding_type == 'multilingual':
            return await self.embedding_manager.embed_multilingual(text)
        else:
            return await self.embedding_manager.embed_text(text)
            
    def _select_vector_fields(
        self,
        options: SearchOptions
    ) -> List[Dict[str, Any]]:
        """Select which vector fields to search based on options."""
        # Get requested fields from options attributes or metadata
        requested_fields = getattr(options, 'embedding_fields', None)
        if not requested_fields and hasattr(options, 'metadata') and options.metadata:
            requested_fields = options.metadata.get('embedding_fields')
        
        if requested_fields:
            # Use only requested fields
            fields = []
            for field_name in requested_fields:
                if field_name in self.vector_fields:
                    fields.append(self.vector_fields[field_name])
        else:
            # Use default fields based on context
            if options.filters and 'entity_type' in options.filters:
                entity_type = options.filters['entity_type']
                if isinstance(entity_type, list):
                    entity_type = entity_type[0] if entity_type else None
                    
                if entity_type in ['code', 'script', 'function']:
                    fields = [self.vector_fields['code']]
                else:
                    fields = [
                        self.vector_fields['text'],
                        self.vector_fields['title'],
                        self.vector_fields['description']
                    ]
            else:
                # Default to text-based fields
                fields = [
                    self.vector_fields['text'],
                    self.vector_fields['title'],
                    self.vector_fields['description']
                ]
                
        return fields
        
    async def _build_knn_query(
        self,
        query_vector: np.ndarray,
        vector_fields: List[Dict[str, Any]],
        options: SearchOptions
    ) -> Dict[str, Any]:
        """Build Elasticsearch k-NN query."""
        # Base query structure
        es_query = {
            "size": options.size,
            "from": options.from_offset
        }
        
        # k-NN parameters
        k = getattr(options, 'k', options.size)
        num_candidates = getattr(options, 'num_candidates', k * 10)
        threshold = getattr(options, 'threshold', 0.7)
        
        # Build k-NN queries for each field
        if len(vector_fields) == 1:
            # Single field k-NN
            field = vector_fields[0]
            knn_query = {
                "field": field["field"],
                "query_vector": query_vector.tolist(),
                "k": k,
                "num_candidates": num_candidates
            }
            
            # Add filters
            filters = self._build_filters(options)
            if filters:
                knn_query["filter"] = {"bool": {"must": filters}}
                
            es_query["knn"] = knn_query
            
        else:
            # Multiple field k-NN (ES8.4+)
            es_query["knn"] = []
            
            for field in vector_fields:
                knn_query = {
                    "field": field["field"],
                    "query_vector": query_vector.tolist(),
                    "k": k,
                    "num_candidates": num_candidates,
                    "boost": field.get("boost", 1.0)
                }
                
                # Add filters
                filters = self._build_filters(options)
                if filters:
                    knn_query["filter"] = {"bool": {"must": filters}}
                    
                es_query["knn"].append(knn_query)
                
        # Add minimum score threshold
        es_query["min_score"] = threshold
        
        # Apply boosting
        if options.boost_recent:
            # Add query for recency boost
            if "query" not in es_query:
                es_query["query"] = {"match_all": {}}
                
            es_query["query"] = await self._boost_by_recency(
                es_query["query"],
                decay_days=30
            )
            
        if options.boost_quality:
            if "query" not in es_query:
                es_query["query"] = {"match_all": {}}
                
            es_query["query"] = await self._boost_by_quality(
                es_query["query"],
                boost_factor=1.3
            )
            
        # Add source filtering
        es_query["_source"] = {
            "excludes": ["*_embedding"]  # Exclude embedding fields from results
        }
        
        # Add explanation
        if options.include_explanations:
            es_query["explain"] = True
            
        return es_query
        
    async def find_similar(
        self,
        entity_id: str,
        options: SearchOptions
    ) -> List[SearchResult]:
        """Find entities similar to a given entity."""
        try:
            # Get the entity's embeddings
            index = self._get_index_name()
            
            entity = await self.es_client.get(
                index=index,
                id=entity_id,
                _source_includes=["text_embedding", "title_embedding", "entity_type"]
            )
            
            if not entity["found"]:
                return []
                
            # Use the entity's embedding for search
            source = entity["_source"]
            embedding = source.get("text_embedding") or source.get("title_embedding")
            
            if not embedding:
                logger.warning(f"No embedding found for entity {entity_id}")
                return []
                
            # Convert to numpy array
            query_vector = np.array(embedding)
            
            # Build k-NN query
            es_query = await self._build_knn_query(
                query_vector,
                [self.vector_fields["text"]],
                options
            )
            
            # Exclude the source entity
            if "query" not in es_query:
                es_query["query"] = {"match_all": {}}
                
            es_query["query"] = {
                "bool": {
                    "must": es_query.get("query", {"match_all": {}}),
                    "must_not": [
                        {"term": {"_id": entity_id}}
                    ]
                }
            }
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                body=es_query
            )
            
            # Convert results
            results = self._convert_hits_to_results(
                response["hits"]["hits"],
                search_type="similarity"
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Find similar failed for entity {entity_id}: {e}")
            raise 