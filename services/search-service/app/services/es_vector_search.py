"""
Elasticsearch v8 Native Vector Search Service

Provides semantic search using native Elasticsearch dense_vector fields:
- k-NN search with HNSW algorithm
- Hybrid text + vector search
- RAG (Retrieval Augmented Generation) support
- Multi-modal embeddings (text, image, code)
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Union
import numpy as np
from datetime import datetime
import asyncio
from collections import defaultdict

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import torch
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModel
import openai

from ..core.config import settings

logger = logging.getLogger(__name__)


class ESVectorSearchService:
    """
    Native Elasticsearch v8 vector search with k-NN and RAG support
    """
    
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client
        
        # Embedding models
        self.text_model = None
        self.code_model = None
        self.multilingual_model = None
        
        # Model cache
        self.embedding_cache = {}
        
        # RAG configuration
        self.rag_enabled = settings.ENABLE_RAG
        self.openai_client = None
        if self.rag_enabled and settings.OPENAI_API_KEY:
            openai.api_key = settings.OPENAI_API_KEY
            
    async def initialize(self):
        """Initialize embedding models"""
        try:
            # Load text embedding model
            self.text_model = SentenceTransformer(settings.EMBEDDING_MODEL_NAME)
            logger.info(f"Loaded text embedding model: {settings.EMBEDDING_MODEL_NAME}")
            
            # Load code embedding model if enabled
            if settings.ENABLE_CODE_SEARCH:
                self.code_tokenizer = AutoTokenizer.from_pretrained(settings.CODE_MODEL_NAME)
                self.code_model = AutoModel.from_pretrained(settings.CODE_MODEL_NAME)
                logger.info(f"Loaded code embedding model: {settings.CODE_MODEL_NAME}")
            
            # Load multilingual model if enabled
            if settings.ENABLE_MULTILINGUAL:
                self.multilingual_model = SentenceTransformer(settings.MULTILINGUAL_MODEL_NAME)
                logger.info(f"Loaded multilingual model: {settings.MULTILINGUAL_MODEL_NAME}")
                
        except Exception as e:
            logger.error(f"Failed to initialize embedding models: {e}")
            raise
    
    async def embed_text(self, text: Union[str, List[str]], 
                        model_type: str = "text") -> np.ndarray:
        """Generate embeddings for text"""
        try:
            if isinstance(text, str):
                texts = [text]
            else:
                texts = text
                
            # Use appropriate model
            if model_type == "text":
                embeddings = self.text_model.encode(texts, convert_to_numpy=True)
            elif model_type == "multilingual" and self.multilingual_model:
                embeddings = self.multilingual_model.encode(texts, convert_to_numpy=True)
            elif model_type == "code" and self.code_model:
                embeddings = await self._embed_code(texts)
            else:
                embeddings = self.text_model.encode(texts, convert_to_numpy=True)
                
            return embeddings[0] if isinstance(text, str) else embeddings
            
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            raise
    
    async def _embed_code(self, code_snippets: List[str]) -> np.ndarray:
        """Generate embeddings for code using CodeBERT or similar"""
        embeddings = []
        
        for code in code_snippets:
            inputs = self.code_tokenizer(code, return_tensors="pt", 
                                        truncation=True, max_length=512)
            with torch.no_grad():
                outputs = self.code_model(**inputs)
                # Use CLS token embedding
                embedding = outputs.last_hidden_state[:, 0, :].numpy()
                embeddings.append(embedding[0])
                
        return np.array(embeddings)
    
    async def knn_search(self, 
                        query_vector: Union[str, np.ndarray],
                        index: str = "unified",
                        field: str = "text_embedding",
                        k: int = 10,
                        num_candidates: int = 100,
                        filters: Optional[Dict[str, Any]] = None,
                        tenant_id: Optional[str] = None,
                        boost: float = 1.0) -> List[Dict[str, Any]]:
        """
        Perform k-NN search using native Elasticsearch dense_vector
        
        Args:
            query_vector: Query text or embedding vector
            index: Index to search
            field: Vector field name
            k: Number of nearest neighbors
            num_candidates: Number of candidates for HNSW algorithm
            filters: Additional filters
            tenant_id: Tenant ID for multi-tenancy
            boost: Boost factor for scoring
            
        Returns:
            List of search results with scores
        """
        try:
            # Generate embedding if text provided
            if isinstance(query_vector, str):
                embedding = await self.embed_text(query_vector)
            else:
                embedding = query_vector
                
            # Build k-NN query
            knn_query = {
                "field": field,
                "query_vector": embedding.tolist(),
                "k": k,
                "num_candidates": num_candidates
            }
            
            # Add filters
            if filters or tenant_id:
                filter_clauses = []
                
                if tenant_id:
                    filter_clauses.append({"term": {"tenant_id": tenant_id}})
                    
                if filters:
                    for field_name, value in filters.items():
                        if isinstance(value, list):
                            filter_clauses.append({"terms": {field_name: value}})
                        else:
                            filter_clauses.append({"term": {field_name: value}})
                            
                knn_query["filter"] = {"bool": {"must": filter_clauses}}
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                knn=knn_query,
                size=k,
                _source=True
            )
            
            # Process results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"] * boost,
                    "source": hit["_source"],
                    "index": hit["_index"]
                }
                results.append(result)
                
            return results
            
        except Exception as e:
            logger.error(f"k-NN search failed: {e}")
            raise
    
    async def hybrid_search(self,
                           query: str,
                           index: str = "unified",
                           text_fields: List[str] = None,
                           vector_field: str = "text_embedding",
                           k: int = 10,
                           filters: Optional[Dict[str, Any]] = None,
                           tenant_id: Optional[str] = None,
                           text_boost: float = 1.0,
                           vector_boost: float = 1.0,
                           minimum_should_match: int = 1) -> List[Dict[str, Any]]:
        """
        Perform hybrid search combining text and vector search
        
        Uses Elasticsearch v8's ability to combine traditional text search
        with k-NN vector search in a single query
        """
        try:
            # Default text fields
            if text_fields is None:
                text_fields = ["name^3", "title^3", "description^2", "content"]
            
            # Generate embedding for vector search
            embedding = await self.embed_text(query)
            
            # Build hybrid query
            query_body = {
                "size": k,
                "query": {
                    "bool": {
                        "should": [
                            # Text search component
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": text_fields,
                                    "type": "best_fields",
                                    "fuzziness": "AUTO",
                                    "boost": text_boost
                                }
                            }
                        ],
                        "minimum_should_match": minimum_should_match
                    }
                },
                # k-NN search component
                "knn": {
                    "field": vector_field,
                    "query_vector": embedding.tolist(),
                    "k": k,
                    "num_candidates": k * 10,
                    "boost": vector_boost
                }
            }
            
            # Add filters
            if filters or tenant_id:
                filter_clauses = []
                
                if tenant_id:
                    filter_clauses.append({"term": {"tenant_id": tenant_id}})
                    
                if filters:
                    for field_name, value in filters.items():
                        if isinstance(value, list):
                            filter_clauses.append({"terms": {field_name: value}})
                        else:
                            filter_clauses.append({"term": {field_name: value}})
                            
                query_body["query"]["bool"]["filter"] = filter_clauses
                query_body["knn"]["filter"] = {"bool": {"must": filter_clauses}}
            
            # Add highlighting
            query_body["highlight"] = {
                "fields": {
                    field.split("^")[0]: {} for field in text_fields
                }
            }
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                body=query_body
            )
            
            # Process results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    "source": hit["_source"],
                    "index": hit["_index"],
                    "highlights": hit.get("highlight", {})
                }
                results.append(result)
                
            return results
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            raise
    
    async def semantic_search(self,
                            query: str,
                            index: str = "unified",
                            semantic_fields: List[str] = None,
                            k: int = 10,
                            filters: Optional[Dict[str, Any]] = None,
                            tenant_id: Optional[str] = None,
                            include_explanations: bool = False) -> List[Dict[str, Any]]:
        """
        Perform semantic search across multiple vector fields
        
        Searches across different embedding types (text, title, image, etc.)
        and combines results intelligently
        """
        try:
            # Default semantic fields
            if semantic_fields is None:
                semantic_fields = ["text_embedding", "title_embedding"]
            
            # Generate query embedding
            query_embedding = await self.embed_text(query)
            
            # Perform k-NN search on each field
            search_tasks = []
            for field in semantic_fields:
                task = self.knn_search(
                    query_vector=query_embedding,
                    index=index,
                    field=field,
                    k=k * 2,  # Get more candidates for merging
                    filters=filters,
                    tenant_id=tenant_id
                )
                search_tasks.append(task)
            
            # Execute searches in parallel
            all_results = await asyncio.gather(*search_tasks)
            
            # Merge and deduplicate results
            merged_results = self._merge_semantic_results(all_results, k)
            
            # Add semantic explanations if requested
            if include_explanations:
                for result in merged_results:
                    result["semantic_explanation"] = await self._generate_semantic_explanation(
                        query, result["source"]
                    )
            
            return merged_results
            
        except Exception as e:
            logger.error(f"Semantic search failed: {e}")
            raise
    
    async def rag_search(self,
                        question: str,
                        index: str = "documents",
                        k: int = 5,
                        chunk_size: int = 3,
                        filters: Optional[Dict[str, Any]] = None,
                        tenant_id: Optional[str] = None,
                        include_sources: bool = True) -> Dict[str, Any]:
        """
        Perform RAG (Retrieval Augmented Generation) search
        
        Retrieves relevant document chunks and generates an answer
        using a language model
        """
        if not self.rag_enabled:
            raise ValueError("RAG is not enabled in configuration")
            
        try:
            # Search for relevant document chunks
            chunk_results = await self._search_document_chunks(
                question=question,
                index=index,
                k=k * chunk_size,
                filters=filters,
                tenant_id=tenant_id
            )
            
            if not chunk_results:
                return {
                    "answer": "I couldn't find relevant information to answer your question.",
                    "sources": [],
                    "confidence": 0.0
                }
            
            # Group chunks by document
            chunks_by_doc = defaultdict(list)
            for chunk in chunk_results:
                doc_id = chunk["parent_id"]
                chunks_by_doc[doc_id].append(chunk)
            
            # Select best chunks from each document
            selected_chunks = []
            for doc_id, chunks in chunks_by_doc.items():
                # Sort by score and take top chunks
                chunks.sort(key=lambda x: x["score"], reverse=True)
                selected_chunks.extend(chunks[:chunk_size])
            
            # Sort all selected chunks by score
            selected_chunks.sort(key=lambda x: x["score"], reverse=True)
            context_chunks = selected_chunks[:k]
            
            # Build context for LLM
            context = self._build_rag_context(context_chunks)
            
            # Generate answer using LLM
            answer = await self._generate_rag_answer(question, context)
            
            # Prepare response
            response = {
                "answer": answer,
                "confidence": self._calculate_confidence(context_chunks),
                "search_results": len(chunk_results)
            }
            
            if include_sources:
                response["sources"] = [
                    {
                        "document_id": chunk["parent_id"],
                        "document_title": chunk.get("parent_title", "Unknown"),
                        "chunk_id": chunk["chunk_id"],
                        "content": chunk["content"][:200] + "...",
                        "score": chunk["score"],
                        "page": chunk.get("page_number")
                    }
                    for chunk in context_chunks
                ]
            
            return response
            
        except Exception as e:
            logger.error(f"RAG search failed: {e}")
            raise
    
    async def _search_document_chunks(self,
                                    question: str,
                                    index: str,
                                    k: int,
                                    filters: Optional[Dict[str, Any]],
                                    tenant_id: Optional[str]) -> List[Dict[str, Any]]:
        """Search for relevant document chunks using nested queries"""
        try:
            # Generate question embedding
            question_embedding = await self.embed_text(question)
            
            # Build nested k-NN query for chunks
            query_body = {
                "size": k,
                "query": {
                    "nested": {
                        "path": "chunks",
                        "query": {
                            "knn": {
                                "chunks.chunk_embedding": {
                                    "vector": question_embedding.tolist(),
                                    "k": k
                                }
                            }
                        },
                        "inner_hits": {
                            "size": 3,
                            "_source": ["chunks.content", "chunks.chunk_id", "chunks.page_number"]
                        }
                    }
                }
            }
            
            # Add filters
            if filters or tenant_id:
                filter_clauses = []
                
                if tenant_id:
                    filter_clauses.append({"term": {"tenant_id": tenant_id}})
                    
                if filters:
                    for field_name, value in filters.items():
                        if isinstance(value, list):
                            filter_clauses.append({"terms": {field_name: value}})
                        else:
                            filter_clauses.append({"term": {field_name: value}})
                            
                query_body["query"] = {
                    "bool": {
                        "must": [query_body["query"]],
                        "filter": filter_clauses
                    }
                }
            
            # Execute search
            response = await self.es_client.search(
                index=index,
                body=query_body
            )
            
            # Extract chunks from nested hits
            chunks = []
            for hit in response["hits"]["hits"]:
                parent_doc = hit["_source"]
                for inner_hit in hit["inner_hits"]["chunks"]["hits"]["hits"]:
                    chunk_data = inner_hit["_source"]
                    chunks.append({
                        "parent_id": hit["_id"],
                        "parent_title": parent_doc.get("title", "Unknown"),
                        "chunk_id": chunk_data["chunk_id"],
                        "content": chunk_data["content"],
                        "page_number": chunk_data.get("page_number"),
                        "score": inner_hit["_score"]
                    })
            
            return chunks
            
        except Exception as e:
            logger.error(f"Document chunk search failed: {e}")
            return []
    
    def _build_rag_context(self, chunks: List[Dict[str, Any]]) -> str:
        """Build context string from chunks for RAG"""
        context_parts = []
        
        for i, chunk in enumerate(chunks):
            source_info = f"[Source {i+1}: {chunk['parent_title']}"
            if chunk.get("page_number"):
                source_info += f", Page {chunk['page_number']}"
            source_info += "]"
            
            context_parts.append(f"{source_info}\n{chunk['content']}\n")
        
        return "\n---\n".join(context_parts)
    
    async def _generate_rag_answer(self, question: str, context: str) -> str:
        """Generate answer using OpenAI or similar LLM"""
        try:
            prompt = f"""Based on the following context, please answer the question. 
If the answer is not in the context, say "I don't have enough information to answer that."

Context:
{context}

Question: {question}

Answer:"""

            # Use OpenAI API
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant that answers questions based on provided context."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=500
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            logger.error(f"Failed to generate RAG answer: {e}")
            return "I encountered an error while generating the answer."
    
    def _calculate_confidence(self, chunks: List[Dict[str, Any]]) -> float:
        """Calculate confidence score based on chunk relevance"""
        if not chunks:
            return 0.0
            
        # Use average of top scores, normalized
        scores = [chunk["score"] for chunk in chunks[:3]]
        avg_score = sum(scores) / len(scores)
        
        # Normalize to 0-1 range (assuming max score around 2.0)
        confidence = min(avg_score / 2.0, 1.0)
        
        return round(confidence, 2)
    
    def _merge_semantic_results(self, 
                               all_results: List[List[Dict[str, Any]]], 
                               k: int) -> List[Dict[str, Any]]:
        """Merge results from multiple semantic fields"""
        # Combine all results
        seen_ids = set()
        merged = []
        
        # First pass: collect unique results
        for results in all_results:
            for result in results:
                if result["id"] not in seen_ids:
                    seen_ids.add(result["id"])
                    merged.append(result)
                else:
                    # Update score if higher
                    for m in merged:
                        if m["id"] == result["id"] and result["score"] > m["score"]:
                            m["score"] = result["score"]
                            break
        
        # Sort by score and return top k
        merged.sort(key=lambda x: x["score"], reverse=True)
        return merged[:k]
    
    async def _generate_semantic_explanation(self, 
                                           query: str, 
                                           document: Dict[str, Any]) -> str:
        """Generate explanation for why document is semantically relevant"""
        # Simple explanation based on matched fields
        explanation_parts = []
        
        if "name" in document and query.lower() in document["name"].lower():
            explanation_parts.append("Title contains search terms")
        
        if "description" in document:
            desc_lower = document["description"].lower()
            query_words = query.lower().split()
            matched_words = [w for w in query_words if w in desc_lower]
            if matched_words:
                explanation_parts.append(f"Description matches: {', '.join(matched_words)}")
        
        if not explanation_parts:
            explanation_parts.append("Semantic similarity to query")
            
        return "; ".join(explanation_parts)
    
    async def index_with_embeddings(self,
                                  documents: List[Dict[str, Any]],
                                  index: str = "unified",
                                  batch_size: int = 100) -> Dict[str, Any]:
        """Index documents with generated embeddings"""
        try:
            indexed = 0
            errors = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                
                # Generate embeddings for batch
                texts = []
                titles = []
                
                for doc in batch:
                    texts.append(doc.get("description", "") or doc.get("content", ""))
                    titles.append(doc.get("title", "") or doc.get("name", ""))
                
                # Generate embeddings
                if texts:
                    text_embeddings = await self.embed_text(texts)
                    title_embeddings = await self.embed_text(titles)
                    
                    # Add embeddings to documents
                    for j, doc in enumerate(batch):
                        if texts[j]:
                            doc["text_embedding"] = text_embeddings[j].tolist()
                        if titles[j]:
                            doc["title_embedding"] = title_embeddings[j].tolist()
                
                # Bulk index
                actions = [
                    {
                        "_index": index,
                        "_id": doc.get("id") or doc.get("entity_id"),
                        "_source": doc
                    }
                    for doc in batch
                ]
                
                success, failed = await async_bulk(
                    self.es_client,
                    actions,
                    raise_on_error=False
                )
                
                indexed += success
                errors += len(failed)
                
                if failed:
                    logger.error(f"Failed to index {len(failed)} documents")
            
            return {
                "indexed": indexed,
                "errors": errors,
                "total": len(documents)
            }
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            raise 