"""
Unified Embedding Manager

Consolidates all embedding generation logic from multiple services
into a single, configurable manager with caching support.
"""

import logging
from typing import Dict, List, Union, Optional, Any
import numpy as np
import hashlib
from datetime import datetime, timedelta
from functools import lru_cache
import asyncio

import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
import clip
from PIL import Image
from pyignite import AsyncClient
from pyignite.datatypes import String, ByteArray

from ...core.config import settings
from ..interfaces import EmbeddingProvider

logger = logging.getLogger(__name__)


class EmbeddingModel:
    """Base class for embedding models"""
    
    def __init__(self, model_name: str, dimension: int):
        self.model_name = model_name
        self.dimension = dimension
        self.model = None
        
    async def initialize(self):
        """Initialize the model"""
        raise NotImplementedError
        
    async def embed(self, inputs: Union[str, List[str]]) -> np.ndarray:
        """Generate embeddings"""
        raise NotImplementedError


class TextEmbeddingModel(EmbeddingModel):
    """Text embedding model using SentenceTransformers"""
    
    async def initialize(self):
        self.model = SentenceTransformer(self.model_name)
        logger.info(f"Initialized text embedding model: {self.model_name}")
        
    async def embed(self, inputs: Union[str, List[str]]) -> np.ndarray:
        if isinstance(inputs, str):
            inputs = [inputs]
        
        # Run in thread pool to avoid blocking
        embeddings = await asyncio.to_thread(
            self.model.encode,
            inputs,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        
        return embeddings[0] if len(inputs) == 1 else embeddings


class CodeEmbeddingModel(EmbeddingModel):
    """Code embedding model using CodeBERT or similar"""
    
    async def initialize(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModel.from_pretrained(self.model_name)
        logger.info(f"Initialized code embedding model: {self.model_name}")
        
    async def embed(self, inputs: Union[str, List[str]]) -> np.ndarray:
        if isinstance(inputs, str):
            inputs = [inputs]
            
        embeddings = []
        for code in inputs:
            tokenized = self.tokenizer(
                code,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True
            )
            
            with torch.no_grad():
                outputs = self.model(**tokenized)
                # Use CLS token embedding
                embedding = outputs.last_hidden_state[:, 0, :].numpy()
                embeddings.append(embedding[0])
                
        embeddings = np.array(embeddings)
        # Normalize
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        
        return embeddings[0] if len(inputs) == 1 else embeddings


class ImageEmbeddingModel(EmbeddingModel):
    """Image embedding model using CLIP"""
    
    async def initialize(self):
        self.model, self.preprocess = clip.load(self.model_name, device="cpu")
        logger.info(f"Initialized image embedding model: {self.model_name}")
        
    async def embed(self, image_paths: Union[str, List[str]]) -> np.ndarray:
        if isinstance(image_paths, str):
            image_paths = [image_paths]
            
        embeddings = []
        for path in image_paths:
            image = Image.open(path)
            image_input = self.preprocess(image).unsqueeze(0)
            
            with torch.no_grad():
                embedding = self.model.encode_image(image_input)
                embeddings.append(embedding.numpy().flatten())
                
        embeddings = np.array(embeddings)
        # Normalize
        embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        
        return embeddings[0] if len(image_paths) == 1 else embeddings


class EmbeddingManager(EmbeddingProvider):
    """
    Unified embedding manager that handles all embedding types
    with caching and model management using Apache Ignite
    """
    
    def __init__(self, ignite_client: Optional[AsyncClient] = None):
        self.ignite_client = ignite_client
        self.cache_name = "embeddings_cache"
        self.models: Dict[str, EmbeddingModel] = {}
        self.cache_ttl = settings.EMBEDDING_CACHE_TTL or 3600
        self._local_cache = {}
        
        # Model configurations
        self.model_configs = {
            "text": {
                "class": TextEmbeddingModel,
                "default": "sentence-transformers/all-mpnet-base-v2",
                "dimension": 768
            },
            "text_multilingual": {
                "class": TextEmbeddingModel,
                "default": "sentence-transformers/paraphrase-multilingual-mpnet-base-v2",
                "dimension": 768
            },
            "code": {
                "class": CodeEmbeddingModel,
                "default": "microsoft/codebert-base",
                "dimension": 768
            },
            "image": {
                "class": ImageEmbeddingModel,
                "default": "ViT-B/32",
                "dimension": 512
            }
        }
        
    async def initialize(self):
        """Initialize all configured embedding models"""
        for model_type, config in self.model_configs.items():
            if self._should_load_model(model_type):
                await self._load_model(model_type, config)
                
    def _should_load_model(self, model_type: str) -> bool:
        """Check if model should be loaded based on configuration"""
        if model_type == "code" and not settings.ENABLE_CODE_SEARCH:
            return False
        if model_type == "text_multilingual" and not settings.ENABLE_MULTILINGUAL:
            return False
        if model_type == "image" and not settings.ENABLE_IMAGE_SEARCH:
            return False
        return True
        
    async def _load_model(self, model_type: str, config: Dict[str, Any]):
        """Load a specific embedding model"""
        model_class = config["class"]
        model_name = getattr(settings, f"{model_type.upper()}_MODEL_NAME", config["default"])
        
        model = model_class(model_name, config["dimension"])
        await model.initialize()
        
        self.models[model_type] = model
        
    async def embed(
        self,
        content: Union[str, List[str]],
        model_type: str = "text",
        language: Optional[str] = None,
        use_cache: bool = True
    ) -> np.ndarray:
        """
        Generate embeddings for content
        
        Args:
            content: Text, code, or image path(s) to embed
            model_type: Type of model to use
            language: Language code for multilingual text
            use_cache: Whether to use caching
            
        Returns:
            Embedding vector(s)
        """
        # Adjust model type based on language
        if model_type == "text" and language and language != "en":
            model_type = "text_multilingual"
            
        # Check if model is loaded
        if model_type not in self.models:
            raise ValueError(f"Model type '{model_type}' not available")
            
        # Handle caching for single inputs
        if use_cache and isinstance(content, str):
            cached = await self._get_cached_embedding(content, model_type)
            if cached is not None:
                return cached
                
        # Generate embeddings
        embeddings = await self.models[model_type].embed(content)
        
        # Cache single embeddings
        if use_cache and isinstance(content, str):
            await self._cache_embedding(content, model_type, embeddings)
            
        return embeddings
        
    async def embed_batch(
        self,
        contents: List[str],
        model_type: str = "text",
        batch_size: int = 32
    ) -> List[np.ndarray]:
        """Embed multiple contents in batches"""
        all_embeddings = []
        
        for i in range(0, len(contents), batch_size):
            batch = contents[i:i + batch_size]
            embeddings = await self.embed(batch, model_type, use_cache=False)
            all_embeddings.extend(embeddings)
            
        return all_embeddings
        
    async def create_multimodal_embedding(
        self,
        text: Optional[str] = None,
        image_path: Optional[str] = None,
        code: Optional[str] = None,
        weights: Optional[Dict[str, float]] = None
    ) -> np.ndarray:
        """Create a combined embedding from multiple modalities"""
        embeddings = []
        used_weights = []
        
        if weights is None:
            weights = {"text": 1.0, "image": 1.0, "code": 1.0}
            
        if text:
            text_emb = await self.embed(text, "text")
            embeddings.append(text_emb)
            used_weights.append(weights.get("text", 1.0))
            
        if image_path:
            image_emb = await self.embed(image_path, "image")
            # Project to same dimension as text
            image_emb = self._project_embedding(image_emb, self.models["text"].dimension)
            embeddings.append(image_emb)
            used_weights.append(weights.get("image", 1.0))
            
        if code:
            code_emb = await self.embed(code, "code")
            embeddings.append(code_emb)
            used_weights.append(weights.get("code", 1.0))
            
        if not embeddings:
            raise ValueError("At least one modality must be provided")
            
        # Weighted combination
        combined = np.zeros_like(embeddings[0])
        total_weight = sum(used_weights)
        
        for emb, weight in zip(embeddings, used_weights):
            combined += (weight / total_weight) * emb
            
        # Normalize
        combined = combined / np.linalg.norm(combined)
        
        return combined
        
    def _project_embedding(self, embedding: np.ndarray, target_dim: int) -> np.ndarray:
        """Project embedding to target dimension"""
        current_dim = embedding.shape[0]
        
        if current_dim == target_dim:
            return embedding
        elif current_dim > target_dim:
            # Truncate
            return embedding[:target_dim]
        else:
            # Pad with zeros
            padded = np.zeros(target_dim)
            padded[:current_dim] = embedding
            return padded
            
    def _cache_key(self, content: str, model_type: str) -> str:
        """Generate cache key for content"""
        content_hash = hashlib.md5(f"{model_type}:{content}".encode()).hexdigest()
        return f"embedding:{model_type}:{content_hash}"
        
    async def _get_cached_embedding(
        self,
        content: str,
        model_type: str
    ) -> Optional[np.ndarray]:
        """Get embedding from cache"""
        key = self._cache_key(content, model_type)
        
        # Check local cache first
        if key in self._local_cache:
            cached = self._local_cache[key]
            if (datetime.utcnow() - cached["timestamp"]).seconds < self.cache_ttl:
                return cached["embedding"]
                
        # Check Ignite cache
        if self.ignite_client:
            try:
                cache = await self.ignite_client.get_or_create_cache(self.cache_name)
                data = await cache.get(key)
                if data:
                    # Ignite stores as bytes, convert back to numpy array
                    embedding = np.frombuffer(data, dtype=np.float32)
                    # Update local cache
                    self._local_cache[key] = {
                        "embedding": embedding,
                        "timestamp": datetime.utcnow()
                    }
                    return embedding
            except Exception as e:
                logger.warning(f"Ignite cache retrieval failed: {e}")
                
        return None
        
    async def _cache_embedding(
        self,
        content: str,
        model_type: str,
        embedding: np.ndarray
    ):
        """Cache embedding in Ignite"""
        key = self._cache_key(content, model_type)
        
        # Update local cache
        self._local_cache[key] = {
            "embedding": embedding,
            "timestamp": datetime.utcnow()
        }
        
        # Update Ignite cache
        if self.ignite_client:
            try:
                cache = await self.ignite_client.get_or_create_cache(self.cache_name)
                # Store embedding as bytes with TTL metadata
                await cache.put(key, embedding.tobytes())
                # Note: Ignite handles TTL differently - would need to configure expiry policy
                # on cache creation for automatic expiration
            except Exception as e:
                logger.warning(f"Ignite cache storage failed: {e}")
                
    async def get_dimension(self, content_type: str = "text") -> int:
        """Get embedding dimension for content type"""
        if content_type in self.models:
            return self.models[content_type].dimension
        elif content_type in self.model_configs:
            return self.model_configs[content_type]["dimension"]
        else:
            raise ValueError(f"Unknown content type: {content_type}")
            
    def get_model_info(self) -> Dict[str, Any]:
        """Get information about loaded models"""
        return {
            model_type: {
                "name": model.model_name,
                "dimension": model.dimension,
                "loaded": True
            }
            for model_type, model in self.models.items()
        } 