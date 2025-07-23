"""
Search Migration Adapters

Provides compatibility layers for migrating from old search services
to the new UnifiedSearchService.
"""

import logging
from typing import List, Dict, Any, Optional, Union
import numpy as np

from app.services.search.unified_search_service import UnifiedSearchService
from app.services.interfaces import SearchOptions, ServiceResult

logger = logging.getLogger(__name__)


class VectorSearchServiceAdapter:
    """
    Adapter that provides the old VectorSearchService interface
    using the new UnifiedSearchService.
    """
    
    def __init__(self, unified_search: UnifiedSearchService):
        self.unified_search = unified_search
        
    async def initialize(self):
        """Initialize the service"""
        await self.unified_search.initialize()
        
    async def search(
        self,
        query: Union[str, np.ndarray],
        collection_name: str = "text_embeddings",
        tenant_id: Optional[str] = None,
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Perform vector similarity search - old interface"""
        try:
            # Map collection name to entity type
            entity_type = None
            if "code" in collection_name:
                entity_type = ["code", "script", "function"]
            elif "image" in collection_name:
                entity_type = ["image", "diagram"]
                
            # Convert numpy array to string if needed
            if isinstance(query, np.ndarray):
                # This is a pre-computed embedding, not supported directly
                # Would need to implement a direct embedding search
                raise NotImplementedError("Direct embedding search not yet supported")
                
            # Use unified search
            result = await self.unified_search.vector_search(
                query=query,
                entity_types=entity_type,
                filters=filters,
                limit=top_k
            )
            
            if result.success:
                # Convert to old format
                return [
                    {
                        "id": r.get("id"),
                        "score": r.get("score", 0),
                        "metadata": r.get("metadata", {}),
                        "tenant_id": tenant_id
                    }
                    for r in result.data.get("results", [])
                ]
            else:
                logger.error(f"Vector search failed: {result.error}")
                return []
                
        except Exception as e:
            logger.error(f"Vector search adapter failed: {e}")
            raise
            
    async def embed_text(
        self,
        text: Union[str, List[str]],
        model_type: str = "text"
    ) -> np.ndarray:
        """Generate text embeddings"""
        return await self.unified_search.embedding_manager.embed(
            text,
            content_type=model_type
        )


class ESVectorSearchServiceAdapter:
    """
    Adapter that provides the old ESVectorSearchService interface
    using the new UnifiedSearchService.
    """
    
    def __init__(self, unified_search: UnifiedSearchService):
        self.unified_search = unified_search
        self.es_client = unified_search.es_client
        
    async def knn_search(
        self,
        query_vector: Union[str, np.ndarray],
        index: str = "unified",
        field: str = "text_embedding",
        k: int = 10,
        num_candidates: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        boost: float = 1.0
    ) -> List[Dict[str, Any]]:
        """k-NN search - old interface"""
        try:
            # If numpy array provided, we need to handle differently
            if isinstance(query_vector, np.ndarray):
                # Direct embedding search not yet supported
                raise NotImplementedError("Direct embedding search not yet supported")
                
            # Map field to embedding fields
            embedding_fields = [field]
            
            # Use unified search
            result = await self.unified_search.vector_search(
                query=query_vector,
                embedding_fields=embedding_fields,
                filters=filters,
                limit=k
            )
            
            if result.success:
                # Convert to old format
                return [
                    {
                        "id": r.get("id"),
                        "score": r.get("score", 0) * boost,
                        "source": r.get("metadata", {}),
                        "index": index
                    }
                    for r in result.data.get("results", [])
                ]
            else:
                logger.error(f"k-NN search failed: {result.error}")
                return []
                
        except Exception as e:
            logger.error(f"k-NN search adapter failed: {e}")
            raise
            
    async def hybrid_search(
        self,
        query: str,
        index: str = "unified",
        text_fields: Optional[List[str]] = None,
        vector_field: str = "text_embedding",
        k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        text_boost: float = 1.0,
        vector_boost: float = 1.0,
        minimum_should_match: int = 1
    ) -> List[Dict[str, Any]]:
        """Hybrid search - old interface"""
        try:
            # Normalize weights
            total = text_boost + vector_boost
            text_weight = text_boost / total
            vector_weight = vector_boost / total
            
            # Use unified search
            result = await self.unified_search.hybrid_search(
                query=query,
                text_weight=text_weight,
                vector_weight=vector_weight,
                filters=filters,
                limit=k
            )
            
            if result.success:
                # Convert to old format
                return [
                    {
                        "id": r.get("id"),
                        "score": r.get("score", 0),
                        "source": r.get("metadata", {}),
                        "index": index
                    }
                    for r in result.data.get("results", [])
                ]
            else:
                logger.error(f"Hybrid search failed: {result.error}")
                return []
                
        except Exception as e:
            logger.error(f"Hybrid search adapter failed: {e}")
            raise
            
    async def embed_text(
        self,
        text: Union[str, List[str]],
        model_type: str = "text"
    ) -> np.ndarray:
        """Generate text embeddings"""
        return await self.unified_search.embedding_manager.embed(
            text,
            content_type=model_type
        )


class HybridSearchServiceAdapter:
    """
    Adapter that provides the old HybridSearchService interface
    using the new UnifiedSearchService.
    """
    
    def __init__(self, unified_search: UnifiedSearchService):
        self.unified_search = unified_search
        self.es_client = unified_search.es_client
        
    async def hybrid_search(
        self,
        query: str,
        tenant_id: Optional[str] = None,
        size: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        text_weight: float = 0.6,
        vector_weight: float = 0.4,
        rerank: bool = True
    ) -> List[Dict[str, Any]]:
        """Perform hybrid search - old interface"""
        try:
            result = await self.unified_search.hybrid_search(
                query=query,
                text_weight=text_weight,
                vector_weight=vector_weight,
                filters=filters,
                limit=size
            )
            
            if result.success:
                # Convert to old format
                return result.data.get("results", [])
            else:
                logger.error(f"Hybrid search failed: {result.error}")
                return []
                
        except Exception as e:
            logger.error(f"Hybrid search adapter failed: {e}")
            return [] 