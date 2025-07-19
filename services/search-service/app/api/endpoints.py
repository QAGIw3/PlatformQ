from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, BackgroundTasks, Body
from typing import List, Optional, Dict, Any
from elasticsearch import AsyncElasticsearch
from pydantic import BaseModel, Field
import tempfile
import os

from ..dependencies import get_es_client
from ..services.vector_search import get_vector_search_service, VectorSearchService
from ..services.es_vector_search import ESVectorSearchService
from ..services.hybrid_search import HybridSearchService
from ..services.query_understanding import QueryUnderstandingService
from ..core.config import settings

router = APIRouter()


class SearchRequest(BaseModel):
    """Unified search request model"""
    query: str = Field(..., description="Search query")
    search_type: str = Field("hybrid", description="Type of search: text, vector, hybrid, multi_modal")
    tenant_id: Optional[str] = Field(None, description="Tenant ID for multi-tenant search")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    size: int = Field(10, description="Number of results")
    from_: int = Field(0, description="Offset for pagination")
    include_metadata: bool = Field(True, description="Include full metadata in results")
    
    
class VectorSearchRequest(BaseModel):
    """Vector search request model"""
    query: str = Field(..., description="Text query for vector search")
    collection: str = Field("text_embeddings", description="Collection to search")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    top_k: int = Field(10, description="Number of results")
    filters: Optional[Dict[str, Any]] = Field(None, description="Metadata filters")
    

class SimilaritySearchRequest(BaseModel):
    """Find similar items request"""
    document_id: str = Field(..., description="Document ID to find similar items for")
    collection: str = Field("text_embeddings", description="Collection to search")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    top_k: int = Field(10, description="Number of similar items")


class MultiModalSearchRequest(BaseModel):
    """Multi-modal search request"""
    text_query: Optional[str] = Field(None, description="Text query")
    image_url: Optional[str] = Field(None, description="Image URL for search")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    top_k: int = Field(10, description="Number of results")
    search_mode: str = Field("combined", description="Search mode: text_only, image_only, combined")


@router.get("/search")
async def search(
    q: str,
    es_client: AsyncElasticsearch = Depends(get_es_client),
    index_name: str = "platformq_search",
    size: int = 10,
    from_: int = 0
):
    """Perform a search across all indexed entities"""
    try:
        query = {
            "from": from_,
            "size": size,
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["name", "description", "tags"]
                }
            }
        }
        
        response = await es_client.search(
            index=index_name,
            body=query
        )
        
        return {
            "total": response["hits"]["total"]["value"],
            "results": [hit["_source"] for hit in response["hits"]["hits"]]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/unified")
async def unified_search(
    request: SearchRequest,
    es_client: AsyncElasticsearch = Depends(get_es_client),
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Unified search endpoint supporting multiple search types"""
    try:
        if request.search_type == "text":
            # Traditional text search with Elasticsearch
            query = {
                "from": request.from_,
                "size": request.size,
                "query": {
                    "bool": {
                        "must": {
                            "multi_match": {
                                "query": request.query,
                                "fields": ["name^3", "title^3", "description^2", "content", "tags^2"],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    }
                }
            }
            
            # Add filters if provided
            if request.filters:
                query["query"]["bool"]["filter"] = []
                for field, value in request.filters.items():
                    query["query"]["bool"]["filter"].append({
                        "term": {field: value}
                    })
            
            # Add tenant filter
            if request.tenant_id:
                if "filter" not in query["query"]["bool"]:
                    query["query"]["bool"]["filter"] = []
                query["query"]["bool"]["filter"].append({
                    "term": {"tenant_id": request.tenant_id}
                })
            
            response = await es_client.search(
                index=settings.ES_INDEX_NAME,
                body=query
            )
            
            return {
                "search_type": "text",
                "total": response["hits"]["total"]["value"],
                "results": [
                    {
                        "id": hit["_id"],
                        "score": hit["_score"],
                        "source": hit["_source"] if request.include_metadata else {
                            "name": hit["_source"].get("name"),
                            "description": hit["_source"].get("description")
                        }
                    }
                    for hit in response["hits"]["hits"]
                ]
            }
            
        elif request.search_type == "vector":
            # Pure vector search
            results = await vector_service.search(
                query=request.query,
                tenant_id=request.tenant_id,
                top_k=request.size,
                filters=request.filters
            )
            
            return {
                "search_type": "vector",
                "total": len(results),
                "results": results
            }
            
        elif request.search_type == "hybrid":
            # Hybrid search combining text and vector
            hybrid_service = HybridSearchService(es_client, vector_service)
            results = await hybrid_service.hybrid_search(
                query=request.query,
                tenant_id=request.tenant_id,
                size=request.size,
                filters=request.filters,
                text_weight=0.6,
                vector_weight=0.4
            )
            
            return {
                "search_type": "hybrid",
                "total": len(results),
                "results": results
            }
            
        else:
            raise HTTPException(status_code=400, detail=f"Unknown search type: {request.search_type}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/vector")
async def vector_search(
    request: VectorSearchRequest,
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Perform semantic vector search"""
    try:
        results = await vector_service.search(
            query=request.query,
            collection_name=request.collection,
            tenant_id=request.tenant_id,
            top_k=request.top_k,
            filters=request.filters
        )
        
        return {
            "query": request.query,
            "collection": request.collection,
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/similar")
async def find_similar(
    request: SimilaritySearchRequest,
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Find similar documents based on vector similarity"""
    try:
        results = await vector_service.find_similar(
            doc_id=request.document_id,
            collection_name=request.collection,
            tenant_id=request.tenant_id,
            top_k=request.top_k
        )
        
        return {
            "source_document": request.document_id,
            "collection": request.collection,
            "total": len(results),
            "similar_documents": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/multi_modal")
async def multi_modal_search(
    request: MultiModalSearchRequest,
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Perform multi-modal search combining text and image"""
    try:
        # Create multi-modal embedding
        if request.search_mode == "text_only" and request.text_query:
            embedding = await vector_service.embed_text(request.text_query)
            collection = "text_embeddings"
        elif request.search_mode == "image_only" and request.image_url:
            # Download image temporarily
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(request.image_url)
                with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                    tmp.write(response.content)
                    tmp_path = tmp.name
            
            try:
                embedding = await vector_service.embed_image(tmp_path)
                collection = "image_embeddings"
            finally:
                os.unlink(tmp_path)
                
        elif request.search_mode == "combined":
            embedding = await vector_service.create_multimodal_embedding(
                text=request.text_query,
                image_path=request.image_url  # Would need to download first
            )
            collection = "multimodal_embeddings"
        else:
            raise ValueError("Invalid search mode or missing query")
        
        # Search with embedding
        results = await vector_service.search(
            query=embedding,
            collection_name=collection,
            tenant_id=request.tenant_id,
            top_k=request.top_k
        )
        
        return {
            "search_mode": request.search_mode,
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/image")
async def search_by_image(
    file: UploadFile = File(...),
    tenant_id: Optional[str] = Query(None),
    top_k: int = Query(10),
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Search by uploading an image"""
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(suffix=f".{file.filename.split('.')[-1]}", delete=False) as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            # Generate image embedding
            embedding = await vector_service.embed_image(tmp_path)
            
            # Search in image collection
            results = await vector_service.search(
                query=embedding,
                collection_name="image_embeddings",
                tenant_id=tenant_id,
                top_k=top_k
            )
            
            return {
                "filename": file.filename,
                "total": len(results),
                "results": results
            }
            
        finally:
            os.unlink(tmp_path)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/index/vector")
async def index_vector_document(
    document: Dict[str, Any],
    tenant_id: str,
    collection: str = "text_embeddings",
    background_tasks: BackgroundTasks = BackgroundTasks(),
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Index a document in the vector store"""
    try:
        # Index asynchronously
        background_tasks.add_task(
            vector_service.index_document,
            doc_id=document.get("id"),
            content=document,
            tenant_id=tenant_id,
            collection_name=collection
        )
        
        return {
            "status": "indexing",
            "document_id": document.get("id"),
            "collection": collection
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggest")
async def autocomplete(
    prefix: str,
    tenant_id: Optional[str] = None,
    size: int = 10,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Get autocomplete suggestions"""
    try:
        query = {
            "size": 0,
            "suggest": {
                "text": prefix,
                "autocomplete": {
                    "prefix": prefix,
                    "completion": {
                        "field": "suggest",
                        "size": size,
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        }
        
        # Add tenant filter if provided
        if tenant_id:
            query["query"] = {
                "term": {"tenant_id": tenant_id}
            }
        
        response = await es_client.search(
            index=settings.ES_INDEX_NAME,
            body=query
        )
        
        suggestions = []
        if "suggest" in response and "autocomplete" in response["suggest"]:
            for option in response["suggest"]["autocomplete"][0]["options"]:
                suggestions.append({
                    "text": option["text"],
                    "score": option["score"]
                })
        
        return {
            "prefix": prefix,
            "suggestions": suggestions
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/query")
async def analyze_query(
    query: str,
    query_service: QueryUnderstandingService = Depends()
):
    """Analyze and understand user query intent"""
    try:
        analysis = await query_service.analyze_query(query)
        
        return {
            "original_query": query,
            "intent": analysis["intent"],
            "entities": analysis["entities"],
            "enhanced_query": analysis["enhanced_query"],
            "suggested_filters": analysis["filters"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/collections")
async def get_collection_stats(
    vector_service: VectorSearchService = Depends(get_vector_search_service)
):
    """Get statistics about vector collections"""
    try:
        stats = {}
        for collection_name in ["text_embeddings", "image_embeddings", "code_embeddings", "multimodal_embeddings"]:
            try:
                collection_stats = await vector_service.get_collection_stats(collection_name)
                stats[collection_name] = collection_stats
            except:
                stats[collection_name] = {"status": "not_available"}
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/knn")
async def knn_search(
    request: SearchRequest,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Native Elasticsearch k-NN vector search"""
    try:
        es_vector_service = ESVectorSearchService(es_client)
        
        results = await es_vector_service.knn_search(
            query_vector=request.query,
            index=request.filters.get("index", "unified") if request.filters else "unified",
            field=request.filters.get("vector_field", "text_embedding") if request.filters else "text_embedding",
            k=request.size,
            filters=request.filters,
            tenant_id=request.tenant_id
        )
        
        return {
            "search_type": "knn",
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/semantic")
async def semantic_search(
    request: SearchRequest,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Semantic search across multiple vector fields"""
    try:
        es_vector_service = ESVectorSearchService(es_client)
        
        results = await es_vector_service.semantic_search(
            query=request.query,
            index=request.filters.get("index", "unified") if request.filters else "unified",
            k=request.size,
            filters=request.filters,
            tenant_id=request.tenant_id,
            include_explanations=request.include_metadata
        )
        
        return {
            "search_type": "semantic",
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/search/hybrid-v8")
async def hybrid_search_v8(
    request: SearchRequest,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Hybrid search using native Elasticsearch v8 text + vector"""
    try:
        es_vector_service = ESVectorSearchService(es_client)
        
        # Extract weights from filters
        text_boost = float(request.filters.get("text_boost", 1.0)) if request.filters else 1.0
        vector_boost = float(request.filters.get("vector_boost", 1.0)) if request.filters else 1.0
        
        results = await es_vector_service.hybrid_search(
            query=request.query,
            index=request.filters.get("index", "unified") if request.filters else "unified",
            k=request.size,
            filters=request.filters,
            tenant_id=request.tenant_id,
            text_boost=text_boost,
            vector_boost=vector_boost
        )
        
        return {
            "search_type": "hybrid-v8",
            "total": len(results),
            "results": results
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class RAGRequest(BaseModel):
    """RAG search request model"""
    question: str = Field(..., description="Question to answer")
    index: str = Field("documents", description="Index to search")
    k: int = Field(5, description="Number of documents to retrieve")
    chunk_size: int = Field(3, description="Number of chunks per document")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    include_sources: bool = Field(True, description="Include source documents")


@router.post("/search/rag")
async def rag_search(
    request: RAGRequest,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Retrieval Augmented Generation search for Q&A"""
    try:
        if not settings.ENABLE_RAG:
            raise HTTPException(
                status_code=400, 
                detail="RAG is not enabled. Set ENABLE_RAG=true and provide OPENAI_API_KEY"
            )
        
        es_vector_service = ESVectorSearchService(es_client)
        
        result = await es_vector_service.rag_search(
            question=request.question,
            index=request.index,
            k=request.k,
            chunk_size=request.chunk_size,
            filters=request.filters,
            tenant_id=request.tenant_id,
            include_sources=request.include_sources
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class IndexDocumentsRequest(BaseModel):
    """Request for indexing documents with embeddings"""
    documents: List[Dict[str, Any]] = Field(..., description="Documents to index")
    index: str = Field("unified", description="Target index")
    generate_embeddings: bool = Field(True, description="Generate embeddings for documents")


@router.post("/index/documents")
async def index_documents_with_embeddings(
    request: IndexDocumentsRequest,
    background_tasks: BackgroundTasks,
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Index documents with generated embeddings"""
    try:
        if not request.generate_embeddings:
            # Direct indexing without embeddings
            raise HTTPException(
                status_code=400,
                detail="Use standard indexing endpoint for documents without embeddings"
            )
        
        es_vector_service = ESVectorSearchService(es_client)
        
        # Start indexing in background
        background_tasks.add_task(
            es_vector_service.index_with_embeddings,
            documents=request.documents,
            index=request.index
        )
        
        return {
            "status": "accepted",
            "message": f"Indexing {len(request.documents)} documents with embeddings",
            "index": request.index
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/indices/stats")
async def get_index_statistics(
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Get statistics for all search indices"""
    try:
        # Get all indices
        indices = await es_client.cat.indices(format="json")
        
        # Filter platform indices
        platform_indices = [
            idx for idx in indices 
            if idx["index"].startswith(settings.ES_INDEX_PREFIX)
        ]
        
        # Get detailed stats
        stats = {}
        for idx in platform_indices:
            index_name = idx["index"]
            
            # Get mapping to check for vector fields
            mapping = await es_client.indices.get_mapping(index=index_name)
            properties = mapping[index_name]["mappings"]["properties"]
            
            vector_fields = []
            for field, config in properties.items():
                if config.get("type") == "dense_vector":
                    vector_fields.append({
                        "field": field,
                        "dims": config.get("dims"),
                        "similarity": config.get("similarity", "l2")
                    })
            
            stats[index_name] = {
                "health": idx["health"],
                "status": idx["status"],
                "docs_count": int(idx["docs.count"] or 0),
                "store_size": idx["store.size"],
                "vector_fields": vector_fields,
                "vector_enabled": len(vector_fields) > 0
            }
        
        return {
            "total_indices": len(platform_indices),
            "indices": stats,
            "elasticsearch_version": (await es_client.info())["version"]["number"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/embeddings/generate")
async def generate_embeddings(
    texts: List[str] = Body(..., description="Texts to generate embeddings for"),
    model_type: str = Body("text", description="Model type: text, code, multilingual"),
    es_client: AsyncElasticsearch = Depends(get_es_client)
):
    """Generate embeddings for given texts"""
    try:
        es_vector_service = ESVectorSearchService(es_client)
        
        embeddings = await es_vector_service.embed_text(texts, model_type=model_type)
        
        return {
            "embeddings": embeddings.tolist(),
            "model_type": model_type,
            "dimensions": embeddings.shape[1] if len(embeddings.shape) > 1 else len(embeddings),
            "count": len(texts)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
