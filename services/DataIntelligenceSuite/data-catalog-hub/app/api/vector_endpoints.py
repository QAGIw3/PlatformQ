"""
Vector Search API Endpoints

Comprehensive endpoints for ES8 vector search, hybrid search, 
graph-enhanced search, and RAG capabilities.
"""

from fastapi import APIRouter, Depends, HTTPException, Query, File, UploadFile, Body
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field
import tempfile
import os

from elasticsearch import AsyncElasticsearch
from ..core.dependencies import get_es_client, get_current_user
from ..services.enhanced_vector_search import EnhancedVectorSearchService
from ..core.config import settings

router = APIRouter(prefix="/vector", tags=["vector_search"])

# Global service instance
vector_service: Optional[EnhancedVectorSearchService] = None


def get_vector_service() -> EnhancedVectorSearchService:
    """Get vector search service instance"""
    if not vector_service:
        raise HTTPException(status_code=500, detail="Vector service not initialized")
    return vector_service


# Request/Response Models
class IndexDocumentRequest(BaseModel):
    doc_id: str = Field(..., description="Document ID")
    content: str = Field(..., description="Document content")
    doc_type: str = Field(default="text", description="Document type: text, code, image")
    title: Optional[str] = Field(None, description="Document title")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")
    tenant_id: Optional[str] = Field(None, description="Tenant ID for multi-tenancy")


class HybridSearchRequest(BaseModel):
    query: str = Field(..., description="Search query")
    index: str = Field(default="unified", description="Index to search")
    k: int = Field(default=10, description="Number of results")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    filters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional filters")
    include_graph: bool = Field(default=True, description="Include graph relationships")
    text_weight: float = Field(default=0.4, description="Weight for text search")
    vector_weight: float = Field(default=0.6, description="Weight for vector search")


class GraphSearchRequest(BaseModel):
    query: str = Field(..., description="Search query")
    start_nodes: List[str] = Field(..., description="Starting node IDs")
    max_depth: int = Field(default=2, description="Maximum traversal depth")
    relationship_types: Optional[List[str]] = Field(None, description="Filter by relationship types")
    k: int = Field(default=10, description="Number of results")


class RAGSearchRequest(BaseModel):
    question: str = Field(..., description="Question to answer")
    index: str = Field(default="unified", description="Index to search")
    k: int = Field(default=5, description="Number of context chunks")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    model: str = Field(default="gpt-3.5-turbo", description="LLM model to use")


class BatchIndexRequest(BaseModel):
    documents: List[IndexDocumentRequest] = Field(..., description="Documents to index")
    parallel: bool = Field(default=True, description="Process in parallel")


class SimilaritySearchRequest(BaseModel):
    doc_id: Optional[str] = Field(None, description="Find similar to this document")
    query_text: Optional[str] = Field(None, description="Find similar to this text")
    index: str = Field(default="unified", description="Index to search")
    k: int = Field(default=10, description="Number of results")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")


# Endpoints
@router.post("/index")
async def index_document(
    request: IndexDocumentRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Index a document with vector embeddings"""
    try:
        # Use user's tenant_id if not specified
        tenant_id = request.tenant_id or current_user.tenant_id
        
        result = await service.index_document(
            doc_id=request.doc_id,
            content=request.content,
            doc_type=request.doc_type,
            title=request.title,
            metadata=request.metadata,
            tenant_id=tenant_id
        )
        
        return {
            "status": "success",
            "result": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/index/batch")
async def batch_index_documents(
    request: BatchIndexRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Batch index multiple documents"""
    try:
        results = []
        errors = []
        
        # Process documents
        for doc in request.documents:
            try:
                tenant_id = doc.tenant_id or current_user.tenant_id
                result = await service.index_document(
                    doc_id=doc.doc_id,
                    content=doc.content,
                    doc_type=doc.doc_type,
                    title=doc.title,
                    metadata=doc.metadata,
                    tenant_id=tenant_id
                )
                results.append(result)
            except Exception as e:
                errors.append({
                    "doc_id": doc.doc_id,
                    "error": str(e)
                })
                
        return {
            "status": "completed",
            "indexed": len(results),
            "errors": len(errors),
            "results": results,
            "error_details": errors
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/index/image")
async def index_image_document(
    doc_id: str = Query(..., description="Document ID"),
    title: Optional[str] = Query(None, description="Image title"),
    file: UploadFile = File(..., description="Image file"),
    metadata: str = Query(default="{}", description="JSON metadata"),
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Index an image with CLIP embeddings"""
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix=file.filename) as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_path = tmp_file.name
            
        try:
            # Parse metadata
            import json
            metadata_dict = json.loads(metadata)
            
            # Index image
            result = await service.index_document(
                doc_id=doc_id,
                content=tmp_path,  # Path to image
                doc_type="image",
                title=title,
                metadata=metadata_dict,
                tenant_id=current_user.tenant_id
            )
            
            return {
                "status": "success",
                "result": result
            }
            
        finally:
            # Clean up temp file
            os.unlink(tmp_path)
            
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/search/hybrid")
async def hybrid_search(
    request: HybridSearchRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Perform hybrid text + vector search"""
    try:
        # Use user's tenant_id if not specified
        tenant_id = request.tenant_id or current_user.tenant_id
        
        results = await service.hybrid_search(
            query=request.query,
            index=request.index,
            k=request.k,
            tenant_id=tenant_id,
            filters=request.filters,
            include_graph=request.include_graph,
            text_weight=request.text_weight,
            vector_weight=request.vector_weight
        )
        
        return {
            "query": request.query,
            "total_results": len(results),
            "results": results,
            "search_type": "hybrid",
            "weights": {
                "text": request.text_weight,
                "vector": request.vector_weight
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/search/graph")
async def graph_enhanced_search(
    request: GraphSearchRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Perform graph-enhanced vector search"""
    try:
        results = await service.graph_enhanced_search(
            query=request.query,
            start_nodes=request.start_nodes,
            max_depth=request.max_depth,
            relationship_types=request.relationship_types,
            k=request.k
        )
        
        return {
            "query": request.query,
            "start_nodes": request.start_nodes,
            "total_results": len(results),
            "results": results,
            "search_type": "graph_enhanced",
            "traversal_depth": request.max_depth
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/search/rag")
async def rag_search(
    request: RAGSearchRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Perform RAG (Retrieval Augmented Generation) search"""
    try:
        # Use user's tenant_id if not specified
        tenant_id = request.tenant_id or current_user.tenant_id
        
        result = await service.rag_search(
            question=request.question,
            index=request.index,
            k=request.k,
            tenant_id=tenant_id,
            model=request.model
        )
        
        return {
            "status": "success",
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/search/similar")
async def similarity_search(
    request: SimilaritySearchRequest,
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service)
) -> Dict[str, Any]:
    """Find similar documents using vector similarity"""
    try:
        if not request.doc_id and not request.query_text:
            raise ValueError("Either doc_id or query_text must be provided")
            
        # Use user's tenant_id
        tenant_id = request.tenant_id or current_user.tenant_id
        
        if request.doc_id:
            # Find similar to existing document
            # First get the document's embedding
            doc = await service.es_client.get(
                index=f"vector_{request.index}",
                id=request.doc_id
            )
            query_vector = doc["_source"]["embedding"]
            
        else:
            # Find similar to text
            query_vector = await service.embedder.embed_text(request.query_text)
            query_vector = query_vector.tolist()
            
        # Perform k-NN search
        query_body = {
            "size": request.k,
            "knn": {
                "field": "embedding",
                "query_vector": query_vector,
                "k": request.k,
                "num_candidates": request.k * 10
            }
        }
        
        if tenant_id:
            query_body["query"] = {
                "bool": {
                    "filter": [{"term": {"tenant_id": tenant_id}}]
                }
            }
            
        response = await service.es_client.search(
            index=f"vector_{request.index}",
            body=query_body
        )
        
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "score": hit["_score"],
                "title": hit["_source"].get("title"),
                "type": hit["_source"].get("type"),
                "metadata": hit["_source"].get("metadata", {})
            })
            
        return {
            "query_type": "document" if request.doc_id else "text",
            "query_id": request.doc_id,
            "query_text": request.query_text,
            "total_results": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/stats")
async def get_vector_stats(
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service),
    es_client: AsyncElasticsearch = Depends(get_es_client)
) -> Dict[str, Any]:
    """Get vector search statistics"""
    try:
        stats = {}
        
        # Get index stats for each vector index
        for index_name in ["unified", "code", "image", "graph"]:
            full_index_name = f"vector_{index_name}"
            
            if await es_client.indices.exists(index=full_index_name):
                index_stats = await es_client.indices.stats(index=full_index_name)
                index_info = index_stats["indices"][full_index_name]
                
                stats[index_name] = {
                    "document_count": index_info["primaries"]["docs"]["count"],
                    "size_in_bytes": index_info["primaries"]["store"]["size_in_bytes"],
                    "segments": index_info["primaries"]["segments"]["count"]
                }
                
        # Get service configuration
        stats["configuration"] = {
            "models": {
                "text": "all-mpnet-base-v2",
                "multilingual": "paraphrase-multilingual-mpnet-base-v2",
                "code": "microsoft/codebert-base",
                "image": "CLIP ViT-B/32"
            },
            "dimensions": service.index_configs,
            "rag_enabled": bool(service.openai_api_key)
        }
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reindex/{index_name}")
async def reindex_documents(
    index_name: str,
    batch_size: int = Query(default=100, description="Batch size for reindexing"),
    current_user=Depends(get_current_user),
    service: EnhancedVectorSearchService = Depends(get_vector_service),
    es_client: AsyncElasticsearch = Depends(get_es_client)
) -> Dict[str, Any]:
    """Reindex documents with updated embeddings"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    try:
        source_index = f"vector_{index_name}"
        temp_index = f"vector_{index_name}_temp"
        
        # Create temporary index
        await service._create_vector_indices()
        
        # Reindex with updated embeddings
        processed = 0
        errors = 0
        
        # Scroll through source index
        async for doc in es_client.helpers.async_scan(
            client=es_client,
            index=source_index,
            size=batch_size
        ):
            try:
                # Re-generate embeddings
                await service.index_document(
                    doc_id=doc["_id"],
                    content=doc["_source"].get("content", ""),
                    doc_type=doc["_source"].get("type", "text"),
                    title=doc["_source"].get("title"),
                    metadata=doc["_source"].get("metadata", {}),
                    tenant_id=doc["_source"].get("tenant_id")
                )
                processed += 1
                
            except Exception as e:
                errors += 1
                
        return {
            "status": "completed",
            "processed": processed,
            "errors": errors,
            "index": index_name
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Initialize service on startup
def init_vector_service(es_client: AsyncElasticsearch):
    """Initialize the enhanced vector service"""
    global vector_service
    
    vector_service = EnhancedVectorSearchService(
        es_client=es_client,
        janusgraph_url=settings.JANUSGRAPH_URL,
        redis_client=None,  # Would initialize Redis here
        openai_api_key=settings.OPENAI_API_KEY
    )
    
    # Note: Actual initialization happens in lifespan 