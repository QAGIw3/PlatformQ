"""
Enhanced Vector Search with Elasticsearch 8

Comprehensive vector search implementation featuring:
- Native ES8 k-NN with HNSW algorithm
- JanusGraph integration for relationship-aware search
- RAG (Retrieval Augmented Generation) for AI assistants
- Multi-modal embeddings (text, image, code, graph)
- Hybrid scoring with BM25 + vectors
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Union, Set
from datetime import datetime, timedelta
import asyncio
import numpy as np
from collections import defaultdict
import hashlib
import uuid
import json

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk, async_scan
import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel, pipeline
from PIL import Image
import clip
import openai
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from langchain.vectorstores import ElasticsearchStore
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI

# Graph integration
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

# Cache
import redis
from functools import lru_cache

logger = logging.getLogger(__name__)


class MultiModalEmbedder:
    """Multi-modal embedding generation for different content types"""
    
    def __init__(self):
        # Text embeddings
        self.text_model = SentenceTransformer('all-mpnet-base-v2')
        self.multilingual_model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
        
        # Code embeddings
        self.code_tokenizer = AutoTokenizer.from_pretrained("microsoft/codebert-base")
        self.code_model = AutoModel.from_pretrained("microsoft/codebert-base")
        
        # Image embeddings
        self.clip_model, self.clip_preprocess = clip.load("ViT-B/32", device="cpu")
        
        # Graph embeddings (placeholder for Node2Vec/GraphSAGE)
        self.graph_embedder = None
        
    async def embed_text(self, text: str, language: str = "en") -> np.ndarray:
        """Generate text embeddings"""
        if language != "en":
            return self.multilingual_model.encode(text, convert_to_numpy=True)
        return self.text_model.encode(text, convert_to_numpy=True)
        
    async def embed_code(self, code: str, language: str = "python") -> np.ndarray:
        """Generate code embeddings"""
        inputs = self.code_tokenizer(code, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            outputs = self.code_model(**inputs)
        # Use CLS token embedding
        return outputs.last_hidden_state[:, 0, :].numpy().flatten()
        
    async def embed_image(self, image_path: str) -> np.ndarray:
        """Generate image embeddings using CLIP"""
        image = Image.open(image_path)
        image_input = self.clip_preprocess(image).unsqueeze(0)
        with torch.no_grad():
            image_features = self.clip_model.encode_image(image_input)
        return image_features.numpy().flatten()
        
    async def embed_graph_node(self, node_id: str, graph_client) -> np.ndarray:
        """Generate graph node embeddings (simplified)"""
        # This would use Node2Vec or GraphSAGE in production
        # For now, return a placeholder
        return np.random.rand(128)


class EnhancedVectorSearchService:
    """
    Enhanced vector search with ES8, JanusGraph, and RAG
    """
    
    def __init__(self,
                 es_client: AsyncElasticsearch,
                 janusgraph_url: str = "ws://janusgraph:8182/gremlin",
                 redis_client: Optional[redis.Redis] = None,
                 openai_api_key: Optional[str] = None):
        self.es_client = es_client
        self.janusgraph_url = janusgraph_url
        self.redis_client = redis_client
        self.openai_api_key = openai_api_key
        
        # Multi-modal embedder
        self.embedder = MultiModalEmbedder()
        
        # Graph client
        self.graph_client = None
        
        # RAG components
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200
        )
        
        # Cache for embeddings
        self.embedding_cache = {}
        
        # Index configurations
        self.index_configs = {
            "unified": {
                "dims": 768,
                "similarity": "cosine"
            },
            "code": {
                "dims": 768,
                "similarity": "dot_product"
            },
            "image": {
                "dims": 512,
                "similarity": "l2_norm"
            },
            "graph": {
                "dims": 128,
                "similarity": "cosine"
            }
        }
        
    async def initialize(self):
        """Initialize services and create indices"""
        # Connect to JanusGraph
        self.graph_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        # Create ES8 indices with vector fields
        await self._create_vector_indices()
        
        # Initialize OpenAI if API key provided
        if self.openai_api_key:
            openai.api_key = self.openai_api_key
            
        logger.info("Enhanced vector search service initialized")
        
    async def _create_vector_indices(self):
        """Create Elasticsearch 8 indices with dense_vector fields"""
        for index_name, config in self.index_configs.items():
            index_mapping = {
                "mappings": {
                    "properties": {
                        # Standard fields
                        "id": {"type": "keyword"},
                        "content": {"type": "text", "analyzer": "standard"},
                        "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                        "type": {"type": "keyword"},
                        "tenant_id": {"type": "keyword"},
                        "created_at": {"type": "date"},
                        
                        # Vector field with native k-NN
                        "embedding": {
                            "type": "dense_vector",
                            "dims": config["dims"],
                            "index": True,
                            "similarity": config["similarity"]
                        },
                        
                        # Additional vector fields for multi-vector
                        "title_embedding": {
                            "type": "dense_vector",
                            "dims": config["dims"],
                            "index": True,
                            "similarity": config["similarity"]
                        },
                        
                        # Nested chunks for RAG
                        "chunks": {
                            "type": "nested",
                            "properties": {
                                "chunk_id": {"type": "keyword"},
                                "content": {"type": "text"},
                                "chunk_embedding": {
                                    "type": "dense_vector",
                                    "dims": config["dims"],
                                    "index": True,
                                    "similarity": config["similarity"]
                                },
                                "page_number": {"type": "integer"},
                                "metadata": {"type": "object", "dynamic": True}
                            }
                        },
                        
                        # Graph relationships
                        "graph_relationships": {
                            "type": "nested",
                            "properties": {
                                "type": {"type": "keyword"},
                                "target_id": {"type": "keyword"},
                                "weight": {"type": "float"},
                                "properties": {"type": "object", "dynamic": True}
                            }
                        },
                        
                        # Metadata
                        "metadata": {"type": "object", "dynamic": True}
                    }
                },
                "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 1,
                    "index.knn": True,
                    "index.knn.algo_param.ef_search": 100
                }
            }
            
            # Create index if not exists
            if not await self.es_client.indices.exists(index=f"vector_{index_name}"):
                await self.es_client.indices.create(
                    index=f"vector_{index_name}",
                    body=index_mapping
                )
                logger.info(f"Created vector index: vector_{index_name}")
                
    async def index_document(self,
                           doc_id: str,
                           content: str,
                           doc_type: str = "text",
                           title: Optional[str] = None,
                           metadata: Optional[Dict] = None,
                           tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """Index document with embeddings and chunks for RAG"""
        try:
            # Generate embeddings based on type
            if doc_type == "text":
                content_embedding = await self.embedder.embed_text(content)
                title_embedding = await self.embedder.embed_text(title) if title else None
            elif doc_type == "code":
                content_embedding = await self.embedder.embed_code(content)
                title_embedding = await self.embedder.embed_code(title) if title else None
            elif doc_type == "image":
                content_embedding = await self.embedder.embed_image(content)  # content is path
                title_embedding = None
            else:
                raise ValueError(f"Unknown document type: {doc_type}")
                
            # Split into chunks for RAG
            chunks = []
            if doc_type in ["text", "code"]:
                text_chunks = self.text_splitter.split_text(content)
                for i, chunk_text in enumerate(text_chunks):
                    chunk_embedding = await self.embedder.embed_text(chunk_text)
                    chunks.append({
                        "chunk_id": f"{doc_id}_chunk_{i}",
                        "content": chunk_text,
                        "chunk_embedding": chunk_embedding.tolist(),
                        "page_number": i,
                        "metadata": {"position": i, "total_chunks": len(text_chunks)}
                    })
                    
            # Get graph relationships if available
            graph_relationships = await self._get_graph_relationships(doc_id)
            
            # Prepare document
            document = {
                "id": doc_id,
                "content": content if doc_type != "image" else None,
                "title": title,
                "type": doc_type,
                "tenant_id": tenant_id,
                "created_at": datetime.utcnow(),
                "embedding": content_embedding.tolist(),
                "chunks": chunks,
                "graph_relationships": graph_relationships,
                "metadata": metadata or {}
            }
            
            if title_embedding is not None:
                document["title_embedding"] = title_embedding.tolist()
                
            # Index to Elasticsearch
            index_name = f"vector_{self._get_index_for_type(doc_type)}"
            response = await self.es_client.index(
                index=index_name,
                id=doc_id,
                body=document
            )
            
            # Cache embeddings
            self._cache_embedding(doc_id, content_embedding)
            
            return {
                "doc_id": doc_id,
                "index": index_name,
                "chunks": len(chunks),
                "status": "indexed"
            }
            
        except Exception as e:
            logger.error(f"Error indexing document {doc_id}: {e}")
            raise
            
    async def hybrid_search(self,
                          query: str,
                          index: str = "unified",
                          k: int = 10,
                          tenant_id: Optional[str] = None,
                          filters: Optional[Dict] = None,
                          include_graph: bool = True,
                          text_weight: float = 0.4,
                          vector_weight: float = 0.6) -> List[Dict[str, Any]]:
        """
        Hybrid search combining BM25 text search with k-NN vector search
        """
        try:
            # Generate query embedding
            query_embedding = await self.embedder.embed_text(query)
            
            # Build hybrid query for ES8
            query_body = {
                "size": k * 2,  # Get more candidates for re-ranking
                "query": {
                    "bool": {
                        "should": [
                            # BM25 text search
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["content^2", "title^3"],
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                    "boost": text_weight
                                }
                            }
                        ]
                    }
                },
                # k-NN vector search
                "knn": {
                    "field": "embedding",
                    "query_vector": query_embedding.tolist(),
                    "k": k,
                    "num_candidates": k * 10,
                    "boost": vector_weight
                }
            }
            
            # Add filters
            if tenant_id or filters:
                query_body["query"]["bool"]["filter"] = []
                if tenant_id:
                    query_body["query"]["bool"]["filter"].append(
                        {"term": {"tenant_id": tenant_id}}
                    )
                if filters:
                    for field, value in filters.items():
                        query_body["query"]["bool"]["filter"].append(
                            {"term": {field: value}}
                        )
                        
            # Execute search
            response = await self.es_client.search(
                index=f"vector_{index}",
                body=query_body
            )
            
            # Process results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    "content": hit["_source"].get("content"),
                    "title": hit["_source"].get("title"),
                    "type": hit["_source"].get("type"),
                    "metadata": hit["_source"].get("metadata", {})
                }
                
                # Enrich with graph data if requested
                if include_graph and hit["_source"].get("graph_relationships"):
                    result["graph_context"] = await self._enrich_with_graph(
                        hit["_id"],
                        hit["_source"]["graph_relationships"]
                    )
                    
                results.append(result)
                
            # Re-rank based on combined scores
            results.sort(key=lambda x: x["score"], reverse=True)
            
            return results[:k]
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            raise
            
    async def graph_enhanced_search(self,
                                  query: str,
                                  start_nodes: List[str],
                                  max_depth: int = 2,
                                  relationship_types: Optional[List[str]] = None,
                                  k: int = 10) -> List[Dict[str, Any]]:
        """
        Graph-enhanced search starting from specific nodes
        """
        try:
            # Get query embedding
            query_embedding = await self.embedder.embed_text(query)
            
            # First, find related nodes in graph
            graph_nodes = await self._traverse_graph(
                start_nodes,
                max_depth,
                relationship_types
            )
            
            # Search for documents related to these nodes
            node_ids = [node["id"] for node in graph_nodes]
            
            # Build query
            query_body = {
                "size": k,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "terms": {
                                    "graph_relationships.target_id": node_ids
                                }
                            }
                        ]
                    }
                },
                "knn": {
                    "field": "embedding",
                    "query_vector": query_embedding.tolist(),
                    "k": k,
                    "num_candidates": k * 5
                }
            }
            
            # Execute search
            response = await self.es_client.search(
                index="vector_unified",
                body=query_body
            )
            
            # Process results with graph context
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    "content": hit["_source"].get("content"),
                    "title": hit["_source"].get("title"),
                    "graph_path": self._find_path(start_nodes[0], hit["_id"], graph_nodes),
                    "graph_score": self._calculate_graph_score(hit["_id"], graph_nodes)
                }
                results.append(result)
                
            # Combine scores (vector similarity + graph proximity)
            for result in results:
                result["combined_score"] = (
                    result["score"] * 0.7 +
                    result["graph_score"] * 0.3
                )
                
            # Sort by combined score
            results.sort(key=lambda x: x["combined_score"], reverse=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Graph-enhanced search failed: {e}")
            raise
            
    async def rag_search(self,
                        question: str,
                        index: str = "unified",
                        k: int = 5,
                        tenant_id: Optional[str] = None,
                        model: str = "gpt-3.5-turbo") -> Dict[str, Any]:
        """
        RAG (Retrieval Augmented Generation) search
        """
        try:
            # Search for relevant chunks
            chunks = await self._search_chunks(question, index, k * 2, tenant_id)
            
            # Prepare context
            context = "\n\n".join([
                f"[{i+1}] {chunk['content']}"
                for i, chunk in enumerate(chunks[:k])
            ])
            
            # Generate answer using LLM
            if self.openai_api_key:
                prompt = f"""Answer the following question based on the provided context.
                If the answer cannot be found in the context, say "I cannot find this information in the provided context."
                
                Context:
                {context}
                
                Question: {question}
                
                Answer:"""
                
                response = await self._generate_with_openai(prompt, model)
                answer = response.strip()
                
                # Extract sources
                sources = [
                    {
                        "doc_id": chunk["doc_id"],
                        "chunk_id": chunk["chunk_id"],
                        "relevance_score": chunk["score"],
                        "content_preview": chunk["content"][:200] + "..."
                    }
                    for chunk in chunks[:k]
                ]
                
            else:
                # Fallback to simple extraction
                answer = self._extract_answer(question, chunks)
                sources = []
                
            return {
                "question": question,
                "answer": answer,
                "sources": sources,
                "confidence": self._calculate_confidence(chunks),
                "model_used": model if self.openai_api_key else "extraction"
            }
            
        except Exception as e:
            logger.error(f"RAG search failed: {e}")
            raise
            
    async def _search_chunks(self,
                           query: str,
                           index: str,
                           k: int,
                           tenant_id: Optional[str]) -> List[Dict[str, Any]]:
        """Search for relevant document chunks"""
        query_embedding = await self.embedder.embed_text(query)
        
        query_body = {
            "size": k,
            "query": {
                "nested": {
                    "path": "chunks",
                    "query": {
                        "knn": {
                            "field": "chunks.chunk_embedding",
                            "query_vector": query_embedding.tolist(),
                            "k": k,
                            "num_candidates": k * 5
                        }
                    },
                    "inner_hits": {
                        "size": 3,
                        "_source": ["chunks.content", "chunks.chunk_id", "chunks.metadata"]
                    }
                }
            }
        }
        
        if tenant_id:
            query_body["query"] = {
                "bool": {
                    "must": [query_body["query"]],
                    "filter": [{"term": {"tenant_id": tenant_id}}]
                }
            }
            
        response = await self.es_client.search(
            index=f"vector_{index}",
            body=query_body
        )
        
        # Extract chunks
        chunks = []
        for hit in response["hits"]["hits"]:
            for inner_hit in hit["inner_hits"]["chunks"]["hits"]["hits"]:
                chunks.append({
                    "doc_id": hit["_id"],
                    "chunk_id": inner_hit["_source"]["chunk_id"],
                    "content": inner_hit["_source"]["content"],
                    "score": inner_hit["_score"],
                    "metadata": inner_hit["_source"].get("metadata", {})
                })
                
        return chunks
        
    async def _generate_with_openai(self, prompt: str, model: str) -> str:
        """Generate response using OpenAI"""
        response = await openai.ChatCompletion.acreate(
            model=model,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that answers questions based on provided context."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=500
        )
        return response.choices[0].message.content
        
    async def _get_graph_relationships(self, doc_id: str) -> List[Dict[str, Any]]:
        """Get graph relationships for a document"""
        if not self.graph_client:
            return []
            
        try:
            # Query JanusGraph for relationships
            query = f"""
                g.V().has('doc_id', '{doc_id}')
                 .outE()
                 .project('type', 'target', 'properties')
                 .by(label)
                 .by(inV().values('doc_id'))
                 .by(valueMap())
                 .limit(50)
            """
            
            result = await self.graph_client.submit(query)
            relationships = []
            
            for edge in result:
                relationships.append({
                    "type": edge["type"],
                    "target_id": edge["target"],
                    "weight": edge["properties"].get("weight", [1.0])[0],
                    "properties": dict(edge["properties"])
                })
                
            return relationships
            
        except Exception as e:
            logger.error(f"Error getting graph relationships: {e}")
            return []
            
    async def _traverse_graph(self,
                            start_nodes: List[str],
                            max_depth: int,
                            relationship_types: Optional[List[str]]) -> List[Dict[str, Any]]:
        """Traverse graph from start nodes"""
        if not self.graph_client:
            return []
            
        try:
            # Build traversal query
            base_query = f"g.V().has('doc_id', within({start_nodes}))"
            
            for depth in range(max_depth):
                if relationship_types:
                    base_query += f".out({','.join(relationship_types)})"
                else:
                    base_query += ".out()"
                    
            query = base_query + ".dedup().valueMap(true).limit(100)"
            
            result = await self.graph_client.submit(query)
            nodes = []
            
            for node_data in result:
                nodes.append({
                    "id": node_data.get("doc_id", [None])[0],
                    "type": node_data.get("type", [None])[0],
                    "properties": {k: v[0] if isinstance(v, list) else v 
                                 for k, v in node_data.items()}
                })
                
            return nodes
            
        except Exception as e:
            logger.error(f"Error traversing graph: {e}")
            return []
            
    async def _enrich_with_graph(self,
                               doc_id: str,
                               relationships: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Enrich search result with graph context"""
        # Get related document titles and types
        related_docs = []
        
        for rel in relationships[:5]:  # Top 5 relationships
            try:
                # Get related document info
                related = await self.es_client.get(
                    index="vector_unified",
                    id=rel["target_id"]
                )
                related_docs.append({
                    "id": rel["target_id"],
                    "title": related["_source"].get("title", "Untitled"),
                    "type": related["_source"].get("type"),
                    "relationship": rel["type"],
                    "weight": rel["weight"]
                })
            except:
                pass
                
        return {
            "related_documents": related_docs,
            "relationship_count": len(relationships),
            "primary_relationships": list(set(r["type"] for r in relationships[:10]))
        }
        
    def _find_path(self, start: str, end: str, nodes: List[Dict[str, Any]]) -> List[str]:
        """Find path between nodes (simplified)"""
        # This would use actual graph traversal in production
        return [start, "->", end]
        
    def _calculate_graph_score(self, doc_id: str, graph_nodes: List[Dict[str, Any]]) -> float:
        """Calculate graph-based relevance score"""
        # Simplified scoring based on presence in graph results
        for i, node in enumerate(graph_nodes):
            if node["id"] == doc_id:
                # Higher score for nodes found earlier in traversal
                return 1.0 - (i / len(graph_nodes))
        return 0.0
        
    def _calculate_confidence(self, chunks: List[Dict[str, Any]]) -> float:
        """Calculate confidence score for RAG answer"""
        if not chunks:
            return 0.0
            
        # Average of top chunk scores
        top_scores = [chunk["score"] for chunk in chunks[:3]]
        return sum(top_scores) / len(top_scores) if top_scores else 0.0
        
    def _extract_answer(self, question: str, chunks: List[Dict[str, Any]]) -> str:
        """Simple answer extraction without LLM"""
        if not chunks:
            return "No relevant information found."
            
        # Return top chunk as answer
        return chunks[0]["content"]
        
    def _cache_embedding(self, doc_id: str, embedding: np.ndarray):
        """Cache embedding in Redis"""
        if self.redis_client:
            try:
                self.redis_client.setex(
                    f"embedding:{doc_id}",
                    3600,  # 1 hour TTL
                    embedding.tobytes()
                )
            except:
                pass
                
    def _get_index_for_type(self, doc_type: str) -> str:
        """Get index name for document type"""
        type_to_index = {
            "text": "unified",
            "code": "code",
            "image": "image",
            "graph": "graph"
        }
        return type_to_index.get(doc_type, "unified") 