"""
Vector Search Service with Milvus Integration

This module provides vector search capabilities using Milvus for 
semantic search across text, images, and other embeddings.
"""

import logging
import asyncio
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import numpy as np
import json
from functools import lru_cache

from sentence_transformers import SentenceTransformer
from pymilvus import (
    connections, Collection, CollectionSchema, FieldSchema, DataType,
    utility, MilvusException
)
import torch
from PIL import Image
import clip
from transformers import AutoTokenizer, AutoModel
import redis.asyncio as redis
from sklearn.preprocessing import normalize

from ..core.config import settings

logger = logging.getLogger(__name__)


class VectorSearchService:
    """Advanced vector search with multi-modal support"""
    
    def __init__(self):
        self.milvus_host = settings.MILVUS_HOST or "localhost"
        self.milvus_port = settings.MILVUS_PORT or 19530
        self.redis_client = None
        self.text_model = None
        self.image_model = None
        self.clip_model = None
        self.collections = {}
        
    async def initialize(self):
        """Initialize connections and models"""
        try:
            # Connect to Milvus
            connections.connect(
                alias="default",
                host=self.milvus_host,
                port=self.milvus_port
            )
            logger.info(f"Connected to Milvus at {self.milvus_host}:{self.milvus_port}")
            
            # Initialize Redis for caching
            self.redis_client = await redis.from_url(
                f"redis://{settings.REDIS_HOST}:6379",
                decode_responses=True
            )
            
            # Load embedding models
            await self._load_models()
            
            # Create collections if they don't exist
            await self._create_collections()
            
        except Exception as e:
            logger.error(f"Failed to initialize vector search: {e}")
            raise
    
    async def _load_models(self):
        """Load embedding models"""
        # Text embeddings - using sentence-transformers
        self.text_model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')
        
        # Multi-lingual support
        self.multilingual_model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')
        
        # Code embeddings
        self.code_model = SentenceTransformer('microsoft/codebert-base')
        
        # Image embeddings - using CLIP
        self.clip_model, self.clip_preprocess = clip.load("ViT-B/32", device="cpu")
        
        logger.info("Loaded all embedding models")
    
    async def _create_collections(self):
        """Create Milvus collections for different data types"""
        
        # Text collection
        await self._create_collection(
            "text_embeddings",
            dim=768,  # all-mpnet-base-v2 dimension
            index_type="IVF_FLAT",
            metric_type="IP"  # Inner product for normalized vectors
        )
        
        # Image collection
        await self._create_collection(
            "image_embeddings",
            dim=512,  # CLIP dimension
            index_type="IVF_FLAT",
            metric_type="L2"
        )
        
        # Code collection
        await self._create_collection(
            "code_embeddings",
            dim=768,  # CodeBERT dimension
            index_type="IVF_FLAT",
            metric_type="IP"
        )
        
        # Multi-modal collection (combined embeddings)
        await self._create_collection(
            "multimodal_embeddings",
            dim=1024,  # Combined dimension
            index_type="HNSW",
            metric_type="L2"
        )
    
    async def _create_collection(self, name: str, dim: int, 
                                 index_type: str, metric_type: str):
        """Create a Milvus collection if it doesn't exist"""
        try:
            if not utility.has_collection(name):
                # Define schema
                fields = [
                    FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=128),
                    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
                    FieldSchema(name="metadata", dtype=DataType.JSON),
                    FieldSchema(name="tenant_id", dtype=DataType.VARCHAR, max_length=64),
                    FieldSchema(name="created_at", dtype=DataType.INT64)
                ]
                
                schema = CollectionSchema(
                    fields=fields,
                    description=f"Collection for {name}"
                )
                
                collection = Collection(
                    name=name,
                    schema=schema,
                    using="default"
                )
                
                # Create index
                index_params = {
                    "metric_type": metric_type,
                    "index_type": index_type,
                    "params": {"nlist": 1024} if index_type == "IVF_FLAT" else {"M": 16, "efConstruction": 200}
                }
                
                collection.create_index(
                    field_name="embedding",
                    index_params=index_params
                )
                
                # Load collection to memory
                collection.load()
                
                logger.info(f"Created collection: {name}")
            else:
                collection = Collection(name)
                collection.load()
            
            self.collections[name] = collection
            
        except Exception as e:
            logger.error(f"Failed to create collection {name}: {e}")
            raise
    
    async def embed_text(self, text: Union[str, List[str]], 
                         model_type: str = "default") -> np.ndarray:
        """Generate text embeddings"""
        # Check cache first
        cache_key = f"text_embedding:{model_type}:{hash(str(text))}"
        cached = await self.redis_client.get(cache_key)
        if cached:
            return np.array(json.loads(cached))
        
        # Generate embeddings
        if model_type == "multilingual":
            embeddings = self.multilingual_model.encode(text, convert_to_numpy=True)
        elif model_type == "code":
            embeddings = self.code_model.encode(text, convert_to_numpy=True)
        else:
            embeddings = self.text_model.encode(text, convert_to_numpy=True)
        
        # Normalize embeddings
        embeddings = normalize(embeddings.reshape(-1, embeddings.shape[-1]))[0]
        
        # Cache result
        await self.redis_client.setex(
            cache_key, 
            3600,  # 1 hour TTL
            json.dumps(embeddings.tolist())
        )
        
        return embeddings
    
    async def embed_image(self, image_path: str) -> np.ndarray:
        """Generate image embeddings using CLIP"""
        # Check cache
        cache_key = f"image_embedding:{image_path}"
        cached = await self.redis_client.get(cache_key)
        if cached:
            return np.array(json.loads(cached))
        
        # Load and preprocess image
        image = Image.open(image_path)
        image_input = self.clip_preprocess(image).unsqueeze(0)
        
        # Generate embedding
        with torch.no_grad():
            image_features = self.clip_model.encode_image(image_input)
            embedding = image_features.cpu().numpy().flatten()
        
        # Normalize
        embedding = normalize(embedding.reshape(1, -1))[0]
        
        # Cache result
        await self.redis_client.setex(
            cache_key,
            3600,
            json.dumps(embedding.tolist())
        )
        
        return embedding
    
    async def create_multimodal_embedding(self, text: str = None, 
                                          image_path: str = None) -> np.ndarray:
        """Create combined text-image embedding"""
        embeddings = []
        
        if text:
            text_emb = await self.embed_text(text)
            # Project to common space
            text_emb_projected = self._project_to_multimodal_space(text_emb, "text")
            embeddings.append(text_emb_projected)
        
        if image_path:
            image_emb = await self.embed_image(image_path)
            # Project to common space
            image_emb_projected = self._project_to_multimodal_space(image_emb, "image")
            embeddings.append(image_emb_projected)
        
        if not embeddings:
            raise ValueError("Either text or image must be provided")
        
        # Combine embeddings
        if len(embeddings) > 1:
            combined = np.concatenate(embeddings)
        else:
            combined = embeddings[0]
        
        # Normalize final embedding
        return normalize(combined.reshape(1, -1))[0]
    
    def _project_to_multimodal_space(self, embedding: np.ndarray, 
                                     modality: str) -> np.ndarray:
        """Project embeddings to common multimodal space"""
        # Simple projection - in production, use learned projection matrices
        if modality == "text":
            # Pad text embedding to match multimodal dimension
            padding = np.zeros(256)
            return np.concatenate([embedding, padding])
        elif modality == "image":
            # Pad image embedding
            padding = np.zeros(512)
            return np.concatenate([embedding, padding])
        return embedding
    
    async def index_document(self, doc_id: str, content: Dict[str, Any], 
                             tenant_id: str, collection_name: str = "text_embeddings"):
        """Index a document with its embedding"""
        try:
            # Extract text content
            text = self._extract_text_content(content)
            
            # Generate embedding
            embedding = await self.embed_text(text)
            
            # Prepare data for insertion
            data = {
                "id": doc_id,
                "embedding": embedding.tolist(),
                "metadata": json.dumps(content),
                "tenant_id": tenant_id,
                "created_at": int(datetime.utcnow().timestamp())
            }
            
            # Insert into Milvus
            collection = self.collections[collection_name]
            collection.insert([data])
            
            # Flush to ensure persistence
            collection.flush()
            
            logger.info(f"Indexed document {doc_id} in {collection_name}")
            
        except Exception as e:
            logger.error(f"Failed to index document {doc_id}: {e}")
            raise
    
    async def search(self, query: Union[str, np.ndarray], 
                     collection_name: str = "text_embeddings",
                     tenant_id: str = None,
                     top_k: int = 10,
                     filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Perform vector similarity search"""
        try:
            # Generate query embedding if string provided
            if isinstance(query, str):
                if collection_name == "text_embeddings":
                    query_embedding = await self.embed_text(query)
                elif collection_name == "code_embeddings":
                    query_embedding = await self.embed_text(query, model_type="code")
                else:
                    query_embedding = query
            else:
                query_embedding = query
            
            # Build search parameters
            search_params = {
                "metric_type": "IP" if "text" in collection_name or "code" in collection_name else "L2",
                "params": {"nprobe": 10}
            }
            
            # Build filter expression
            expr_parts = []
            if tenant_id:
                expr_parts.append(f'tenant_id == "{tenant_id}"')
            
            if filters:
                for key, value in filters.items():
                    if isinstance(value, str):
                        expr_parts.append(f'{key} == "{value}"')
                    else:
                        expr_parts.append(f'{key} == {value}')
            
            expr = " && ".join(expr_parts) if expr_parts else None
            
            # Perform search
            collection = self.collections[collection_name]
            results = collection.search(
                data=[query_embedding.tolist()],
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                expr=expr,
                output_fields=["id", "metadata", "tenant_id"]
            )
            
            # Format results
            formatted_results = []
            for hits in results:
                for hit in hits:
                    result = {
                        "id": hit.entity.get("id"),
                        "score": hit.score,
                        "metadata": json.loads(hit.entity.get("metadata", "{}")),
                        "tenant_id": hit.entity.get("tenant_id")
                    }
                    formatted_results.append(result)
            
            return formatted_results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            raise
    
    async def hybrid_search(self, query: str, tenant_id: str,
                            text_weight: float = 0.7,
                            vector_weight: float = 0.3,
                            top_k: int = 10) -> List[Dict[str, Any]]:
        """Perform hybrid text + vector search"""
        # This would combine Elasticsearch text search with Milvus vector search
        # Implementation depends on specific requirements
        pass
    
    async def find_similar(self, doc_id: str, collection_name: str = "text_embeddings",
                           tenant_id: str = None, top_k: int = 10) -> List[Dict[str, Any]]:
        """Find similar documents to a given document"""
        try:
            # Get the document's embedding
            collection = self.collections[collection_name]
            
            # Query by ID
            doc_result = collection.query(
                expr=f'id == "{doc_id}"',
                output_fields=["embedding"]
            )
            
            if not doc_result:
                raise ValueError(f"Document {doc_id} not found")
            
            # Use the embedding to search for similar documents
            embedding = doc_result[0]["embedding"]
            
            # Search excluding the original document
            results = await self.search(
                query=np.array(embedding),
                collection_name=collection_name,
                tenant_id=tenant_id,
                top_k=top_k + 1  # Get extra to exclude self
            )
            
            # Filter out the original document
            return [r for r in results if r["id"] != doc_id][:top_k]
            
        except Exception as e:
            logger.error(f"Find similar failed for {doc_id}: {e}")
            raise
    
    def _extract_text_content(self, content: Dict[str, Any]) -> str:
        """Extract text from various content types"""
        text_parts = []
        
        # Extract from common fields
        for field in ["name", "title", "description", "content", "summary"]:
            if field in content and content[field]:
                text_parts.append(str(content[field]))
        
        # Extract from nested metadata
        if "metadata" in content and isinstance(content["metadata"], dict):
            for key, value in content["metadata"].items():
                if isinstance(value, str) and len(value) < 1000:
                    text_parts.append(f"{key}: {value}")
        
        # Extract from tags
        if "tags" in content and isinstance(content["tags"], list):
            text_parts.append(" ".join(content["tags"]))
        
        return " ".join(text_parts)
    
    async def delete_document(self, doc_id: str, collection_name: str = "text_embeddings"):
        """Delete a document from the vector index"""
        try:
            collection = self.collections[collection_name]
            expr = f'id == "{doc_id}"'
            collection.delete(expr)
            logger.info(f"Deleted document {doc_id} from {collection_name}")
        except Exception as e:
            logger.error(f"Failed to delete document {doc_id}: {e}")
            raise
    
    async def update_document(self, doc_id: str, content: Dict[str, Any],
                              tenant_id: str, collection_name: str = "text_embeddings"):
        """Update a document by deleting and re-indexing"""
        await self.delete_document(doc_id, collection_name)
        await self.index_document(doc_id, content, tenant_id, collection_name)
    
    async def get_collection_stats(self, collection_name: str) -> Dict[str, Any]:
        """Get statistics about a collection"""
        try:
            collection = self.collections[collection_name]
            stats = {
                "name": collection_name,
                "num_entities": collection.num_entities,
                "schema": str(collection.schema),
                "index": str(collection.indexes)
            }
            return stats
        except Exception as e:
            logger.error(f"Failed to get stats for {collection_name}: {e}")
            raise
    
    async def close(self):
        """Clean up connections"""
        try:
            # Release collections
            for collection in self.collections.values():
                collection.release()
            
            # Disconnect from Milvus
            connections.disconnect("default")
            
            # Close Redis
            if self.redis_client:
                await self.redis_client.close()
                
            logger.info("Vector search service closed")
            
        except Exception as e:
            logger.error(f"Error closing vector search service: {e}")


# Singleton instance
_vector_search_service = None

async def get_vector_search_service() -> VectorSearchService:
    """Get or create the vector search service instance"""
    global _vector_search_service
    if _vector_search_service is None:
        _vector_search_service = VectorSearchService()
        await _vector_search_service.initialize()
    return _vector_search_service 