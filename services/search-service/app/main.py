from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Union
from enum import Enum
import logging
import os
from datetime import datetime
from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch.exceptions import NotFoundError, RequestError
import json
import asyncio
from platformq_shared.security import get_current_tenant_and_user
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.config import ConfigLoader
import pulsar
from pulsar.schema import AvroSchema
import threading

logger = logging.getLogger(__name__)

# Configuration
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME", None)
ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD", None)
PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://localhost:6650")

config_loader = ConfigLoader()
settings = config_loader.load_settings()

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = int(os.getenv("ELASTICSEARCH_PORT", 9200))
ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME", "elastic")
ELASTICSEARCH_PASSWORD = settings.get("ELASTICSEARCH_PASSWORD")

# Search result types
class SearchType(str, Enum):
    ALL = "all"
    DIGITAL_ASSET = "digital_asset"
    DOCUMENT = "document"
    USER = "user"
    PROJECT = "project"
    WORKFLOW = "workflow"
    VERIFIABLE_CREDENTIAL = "verifiable_credential"
    GRAPH_ENTITY = "graph_entity"
    ACTIVITY = "activity"

# Pydantic models
class SearchQuery(BaseModel):
    query: str = Field(..., description="Search query string")
    search_type: SearchType = Field(SearchType.ALL, description="Type of entities to search")
    filters: Optional[Dict[str, Any]] = Field(None, description="Additional filters")
    from_: Optional[int] = Field(0, alias="from", description="Pagination offset")
    size: Optional[int] = Field(20, description="Number of results")
    sort_by: Optional[str] = Field("_score", description="Sort field")
    sort_order: Optional[str] = Field("desc", description="Sort order")
    highlight: Optional[bool] = Field(True, description="Enable result highlighting")
    aggregations: Optional[List[str]] = Field(None, description="Fields to aggregate")

class SearchHit(BaseModel):
    id: str
    index: str
    type: str
    score: float
    source: Dict[str, Any]
    highlights: Optional[Dict[str, List[str]]] = None

class AggregationBucket(BaseModel):
    key: str
    doc_count: int

class SearchAggregation(BaseModel):
    name: str
    buckets: List[AggregationBucket]

class SearchResponse(BaseModel):
    total: int
    hits: List[SearchHit]
    aggregations: Optional[Dict[str, SearchAggregation]] = None
    took_ms: int
    max_score: Optional[float] = None

class IndexMapping(BaseModel):
    properties: Dict[str, Any]
    settings: Optional[Dict[str, Any]] = None

# Elasticsearch client manager
class ElasticsearchManager:
    def __init__(self):
        self.client = None
        self.index_mappings = self._get_default_mappings()
        
    async def initialize(self):
        """Initialize Elasticsearch client and create indices"""
        auth = None
        if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD:
            auth = (ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
            
        self.client = AsyncElasticsearch(
            [ELASTICSEARCH_URL],
            basic_auth=auth,
            verify_certs=False
        )
        
        # Create default indices
        await self._create_indices()
        
    async def close(self):
        """Close Elasticsearch client"""
        if self.client:
            await self.client.close()
            
    def _get_default_mappings(self) -> Dict[str, IndexMapping]:
        """Get default index mappings for each entity type"""
        return {
            "digital_assets": IndexMapping(
                properties={
                    "asset_id": {"type": "keyword"},
                    "asset_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "asset_type": {"type": "keyword"},
                    "description": {"type": "text"},
                    "source_tool": {"type": "keyword"},
                    "metadata": {"type": "object", "dynamic": True},
                    "tags": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "tenant_id": {"type": "keyword"},
                    "created_by": {"type": "keyword"},
                    "file_size": {"type": "long"},
                    "file_path": {"type": "keyword"},
                    "content": {"type": "text"},  # For full-text search of file content
                    "suggest": {"type": "completion"}  # For autocomplete
                },
                settings={
                    "analysis": {
                        "analyzer": {
                            "asset_analyzer": {
                                "type": "custom",
                                "tokenizer": "standard",
                                "filter": ["lowercase", "asciifolding", "synonym_filter"]
                            }
                        },
                        "filter": {
                            "synonym_filter": {
                                "type": "synonym",
                                "synonyms": [
                                    "3d,three dimensional,3 dimensional",
                                    "cad,computer aided design",
                                    "bom,bill of materials"
                                ]
                            }
                        }
                    }
                }
            ),
            "documents": IndexMapping(
                properties={
                    "document_id": {"type": "keyword"},
                    "title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "content": {"type": "text"},
                    "path": {"type": "keyword"},
                    "project_id": {"type": "keyword"},
                    "author": {"type": "keyword"},
                    "last_modified": {"type": "date"},
                    "tags": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "file_type": {"type": "keyword"},
                    "language": {"type": "keyword"},
                    "suggest": {"type": "completion"}
                }
            ),
            "activities": IndexMapping(
                properties={
                    "event_id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "event_source": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "user_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "description": {"type": "text"},
                    "entity_type": {"type": "keyword"},
                    "entity_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "metadata": {"type": "object", "dynamic": True},
                    "ip_address": {"type": "ip"},
                    "user_agent": {"type": "text"}
                }
            ),
            "graph_entities": IndexMapping(
                properties={
                    "entity_id": {"type": "keyword"},
                    "entity_type": {"type": "keyword"},
                    "label": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "properties": {"type": "object", "dynamic": True},
                    "in_edges": {"type": "nested"},
                    "out_edges": {"type": "nested"},
                    "centrality_score": {"type": "float"},
                    "community_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"}
                }
            ),
            "verifiable_credentials": IndexMapping(
                properties={
                    "credential_id": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "issuer": {"type": "keyword"},
                    "subject": {"type": "keyword"},
                    "issuance_date": {"type": "date"},
                    "expiration_date": {"type": "date"},
                    "status": {"type": "keyword"},
                    "claims": {"type": "object", "dynamic": True},
                    "proof_type": {"type": "keyword"},
                    "blockchain_anchor": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "revoked": {"type": "boolean"},
                    "credential_subject": {"type": "text"}
                }
            ),
            "projects": IndexMapping(
                properties={
                    "project_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "description": {"type": "text"},
                    "status": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "owner_id": {"type": "keyword"},
                    "members": {"type": "keyword"},
                    "tags": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "openproject_id": {"type": "keyword"},
                    "nextcloud_folder": {"type": "keyword"},
                    "zulip_stream": {"type": "keyword"},
                    "suggest": {"type": "completion"}
                }
            ),
            "workflows": IndexMapping(
                properties={
                    "workflow_id": {"type": "keyword"},
                    "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "description": {"type": "text"},
                    "type": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "started_at": {"type": "date"},
                    "completed_at": {"type": "date"},
                    "error_message": {"type": "text"},
                    "tenant_id": {"type": "keyword"},
                    "created_by": {"type": "keyword"},
                    "dag_id": {"type": "keyword"},
                    "tags": {"type": "keyword"}
                }
            )
        }
        
    async def _create_indices(self):
        """Create Elasticsearch indices with proper mappings"""
        for index_pattern, mapping in self.index_mappings.items():
            # Create index pattern for multi-tenancy: {tenant_id}_{index_pattern}
            # For now, create a template that will apply to all tenant indices
            template_name = f"platformq_{index_pattern}_template"
            
            try:
                await self.client.indices.put_index_template(
                    name=template_name,
                    body={
                        "index_patterns": [f"*_{index_pattern}"],
                        "template": {
                            "settings": mapping.settings or {},
                            "mappings": {
                                "properties": mapping.properties
                            }
                        }
                    }
                )
                logger.info(f"Created index template: {template_name}")
            except RequestError as e:
                if "resource_already_exists_exception" not in str(e):
                    logger.error(f"Failed to create index template {template_name}: {e}")
                    
    async def search(self, tenant_id: str, query: SearchQuery) -> SearchResponse:
        """Execute search query"""
        # Build index pattern based on search type
        if query.search_type == SearchType.ALL:
            index_pattern = f"{tenant_id}_*"
        else:
            index_mapping = {
                SearchType.DIGITAL_ASSET: "digital_assets",
                SearchType.DOCUMENT: "documents",
                SearchType.USER: "users",
                SearchType.PROJECT: "projects",
                SearchType.WORKFLOW: "workflows",
                SearchType.VERIFIABLE_CREDENTIAL: "verifiable_credentials",
                SearchType.GRAPH_ENTITY: "graph_entities",
                SearchType.ACTIVITY: "activities"
            }
            index_pattern = f"{tenant_id}_{index_mapping[query.search_type]}"
            
        # Build Elasticsearch query
        es_query = self._build_elasticsearch_query(query)
        
        # Execute search
        start_time = datetime.utcnow()
        
        try:
            response = await self.client.search(
                index=index_pattern,
                body=es_query,
                from_=query.from_,
                size=query.size
            )
            
            # Process response
            hits = []
            for hit in response["hits"]["hits"]:
                search_hit = SearchHit(
                    id=hit["_id"],
                    index=hit["_index"],
                    type=hit["_index"].split("_")[-1],  # Extract type from index name
                    score=hit["_score"],
                    source=hit["_source"],
                    highlights=hit.get("highlight")
                )
                hits.append(search_hit)
                
            # Process aggregations
            aggregations = None
            if "aggregations" in response:
                aggregations = {}
                for agg_name, agg_data in response["aggregations"].items():
                    if "buckets" in agg_data:
                        aggregations[agg_name] = SearchAggregation(
                            name=agg_name,
                            buckets=[
                                AggregationBucket(key=bucket["key"], doc_count=bucket["doc_count"])
                                for bucket in agg_data["buckets"]
                            ]
                        )
                        
            took_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
            
            return SearchResponse(
                total=response["hits"]["total"]["value"],
                hits=hits,
                aggregations=aggregations,
                took_ms=took_ms,
                max_score=response["hits"].get("max_score")
            )
            
        except NotFoundError:
            # Index doesn't exist yet, return empty results
            return SearchResponse(
                total=0,
                hits=[],
                took_ms=0
            )
            
    def _build_elasticsearch_query(self, query: SearchQuery) -> Dict[str, Any]:
        """Build Elasticsearch query from search query"""
        es_query = {
            "query": {
                "bool": {
                    "must": []
                }
            }
        }
        
        # Add main query
        if query.query:
            if query.query.startswith('"') and query.query.endswith('"'):
                # Exact phrase search
                es_query["query"]["bool"]["must"].append({
                    "match_phrase": {
                        "_all": query.query.strip('"')
                    }
                })
            else:
                # Full-text search with boosting
                es_query["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query.query,
                        "fields": [
                            "name^3",
                            "title^3",
                            "asset_name^3",
                            "description^2",
                            "content",
                            "tags^2",
                            "*"
                        ],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                })
        else:
            # Match all if no query
            es_query["query"]["bool"]["must"].append({"match_all": {}})
            
        # Add filters
        if query.filters:
            for field, value in query.filters.items():
                if isinstance(value, list):
                    es_query["query"]["bool"]["must"].append({
                        "terms": {field: value}
                    })
                elif isinstance(value, dict):
                    # Range query
                    if "gte" in value or "lte" in value:
                        es_query["query"]["bool"]["must"].append({
                            "range": {field: value}
                        })
                else:
                    es_query["query"]["bool"]["must"].append({
                        "term": {field: value}
                    })
                    
        # Add highlighting
        if query.highlight:
            es_query["highlight"] = {
                "fields": {
                    "*": {
                        "fragment_size": 150,
                        "number_of_fragments": 3,
                        "pre_tags": ["<mark>"],
                        "post_tags": ["</mark>"]
                    }
                }
            }
            
        # Add aggregations
        if query.aggregations:
            es_query["aggs"] = {}
            for field in query.aggregations:
                es_query["aggs"][f"{field}_agg"] = {
                    "terms": {
                        "field": field,
                        "size": 20
                    }
                }
                
        # Add sorting
        if query.sort_by != "_score":
            es_query["sort"] = [{
                query.sort_by: {"order": query.sort_order}
            }]
            
        return es_query
        
    async def index_document(self, tenant_id: str, index_type: str, 
                           doc_id: str, document: Dict[str, Any]):
        """Index a single document"""
        index_name = f"{tenant_id}_{index_type}"
        
        # Add tenant_id to document
        document["tenant_id"] = tenant_id
        document["indexed_at"] = datetime.utcnow().isoformat()
        
        await self.client.index(
            index=index_name,
            id=doc_id,
            body=document
        )
        
    async def bulk_index(self, tenant_id: str, index_type: str, 
                        documents: List[Dict[str, Any]]):
        """Bulk index documents"""
        index_name = f"{tenant_id}_{index_type}"
        
        # Prepare bulk actions
        actions = []
        for doc in documents:
            doc["tenant_id"] = tenant_id
            doc["indexed_at"] = datetime.utcnow().isoformat()
            
            action = {
                "_index": index_name,
                "_id": doc.get("id", doc.get(f"{index_type[:-1]}_id")),
                "_source": doc
            }
            actions.append(action)
            
        # Execute bulk indexing
        await helpers.async_bulk(self.client, actions)
        
    async def delete_document(self, tenant_id: str, index_type: str, doc_id: str):
        """Delete a document from index"""
        index_name = f"{tenant_id}_{index_type}"
        
        try:
            await self.client.delete(
                index=index_name,
                id=doc_id
            )
        except NotFoundError:
            pass  # Document already deleted
            
    async def suggest(self, tenant_id: str, prefix: str, 
                     search_type: SearchType = SearchType.ALL) -> List[str]:
        """Get autocomplete suggestions"""
        # Build index pattern
        if search_type == SearchType.ALL:
            index_pattern = f"{tenant_id}_*"
        else:
            index_mapping = {
                SearchType.DIGITAL_ASSET: "digital_assets",
                SearchType.DOCUMENT: "documents",
                SearchType.PROJECT: "projects"
            }
            if search_type in index_mapping:
                index_pattern = f"{tenant_id}_{index_mapping[search_type]}"
            else:
                return []  # Suggestions not supported for this type
                
        # Build suggest query
        suggest_query = {
            "suggest": {
                "text": prefix,
                "completion": {
                    "field": "suggest",
                    "size": 10,
                    "skip_duplicates": True
                }
            }
        }
        
        try:
            response = await self.client.search(
                index=index_pattern,
                body=suggest_query
            )
            
            # Extract suggestions
            suggestions = []
            if "suggest" in response and "completion" in response["suggest"]:
                for option in response["suggest"]["completion"][0]["options"]:
                    suggestions.append(option["text"])
                    
            return suggestions
            
        except NotFoundError:
            return []

# Create the Elasticsearch manager
es_manager = ElasticsearchManager()

# Create the FastAPI app
app = create_base_app(
    service_name="search-service",
    db_session_dependency=lambda: None,  # No database needed
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# API Endpoints
@app.post("/api/v1/search", response_model=SearchResponse)
async def search(
    query: SearchQuery,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Execute a search query across indexed data"""
    tenant_id = context["tenant_id"]
    
    # Log search for analytics
    logger.info(f"Search query from tenant {tenant_id}: {query.query}")
    
    # Execute search
    results = await es_manager.search(tenant_id, query)
    
    # Publish search event for analytics
    try:
        event_data = {
            "query": query.query,
            "search_type": query.search_type.value,
            "result_count": results.total,
            "user_id": context.get("user", {}).get("id"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        publisher = app.state.event_publisher
        publisher.publish(
            topic_base="search-queries",
            tenant_id=tenant_id,
            schema_class=None,
            data=event_data
        )
    except Exception as e:
        logger.error(f"Failed to publish search event: {e}")
        
    return results

@app.get("/api/v1/suggest")
async def autocomplete(
    prefix: str = Query(..., description="Search prefix"),
    search_type: SearchType = Query(SearchType.ALL, description="Type to search"),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get autocomplete suggestions"""
    tenant_id = context["tenant_id"]
    
    suggestions = await es_manager.suggest(tenant_id, prefix, search_type)
    
    return {"suggestions": suggestions}

@app.post("/api/v1/index/{index_type}/{doc_id}")
async def index_document(
    index_type: str,
    doc_id: str,
    document: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user)
):
    """Index a single document (for testing/manual indexing)"""
    tenant_id = context["tenant_id"]
    
    # Validate index type
    valid_types = ["digital_assets", "documents", "activities", "graph_entities", 
                   "verifiable_credentials", "projects", "workflows"]
    if index_type not in valid_types:
        raise HTTPException(status_code=400, detail=f"Invalid index type. Must be one of: {valid_types}")
        
    await es_manager.index_document(tenant_id, index_type, doc_id, document)
    
    return {"message": "Document indexed successfully", "id": doc_id}

@app.delete("/api/v1/index/{index_type}/{doc_id}")
async def delete_document(
    index_type: str,
    doc_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Delete a document from index"""
    tenant_id = context["tenant_id"]
    
    await es_manager.delete_document(tenant_id, index_type, doc_id)
    
    return {"message": "Document deleted successfully", "id": doc_id}

@app.post("/api/v1/reindex/{index_type}")
async def trigger_reindex(
    index_type: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Trigger reindexing of a specific type (admin operation)"""
    tenant_id = context["tenant_id"]
    
    # This would typically trigger a background job to reindex all data
    # For now, return a placeholder response
    return {
        "message": "Reindex triggered",
        "index_type": index_type,
        "tenant_id": tenant_id,
        "job_id": "reindex-" + datetime.utcnow().strftime("%Y%m%d%H%M%S")
    }

# Event consumers for automatic indexing
class IndexingConsumer(threading.Thread):
    """Background thread that consumes events and indexes them"""
    
    def __init__(self, app_state):
        super().__init__(daemon=True)
        self.app_state = app_state
        self.running = True
        
    def run(self):
        """Consume events and index them"""
        client = pulsar.Client(PULSAR_URL)
        
        # Subscribe to various event topics
        consumers = {
            "digital_assets": client.subscribe(
                "persistent://platformq/.*/digital-asset-created-events",
                "search-indexer-assets",
                consumer_type=pulsar.ConsumerType.Shared
            ),
            "documents": client.subscribe(
                "persistent://platformq/.*/document-updated-events",
                "search-indexer-documents",
                consumer_type=pulsar.ConsumerType.Shared
            ),
            "activities": client.subscribe(
                "persistent://platformq/.*/activity-events",
                "search-indexer-activities",
                consumer_type=pulsar.ConsumerType.Shared
            ),
            "verifiable_credentials": client.subscribe(
                "persistent://platformq/.*/verifiable-credential-issued-events",
                "search-indexer-vcs",
                consumer_type=pulsar.ConsumerType.Shared
            )
        }
        
        while self.running:
            for event_type, consumer in consumers.items():
                try:
                    msg = consumer.receive(timeout_millis=100)
                    if msg:
                        self._process_event(event_type, msg)
                        consumer.acknowledge(msg)
                except Exception as e:
                    if "timeout" not in str(e).lower():
                        logger.error(f"Error processing {event_type} event: {e}")
                        
        # Cleanup
        for consumer in consumers.values():
            consumer.close()
        client.close()
        
    def _process_event(self, event_type: str, msg):
        """Process an event and index it"""
        try:
            # Extract tenant_id from topic
            topic = msg.topic_name()
            tenant_id = topic.split("/")[3]  # persistent://platformq/{tenant_id}/...
            
            # Parse event data
            event_data = json.loads(msg.data())
            
            # Index based on event type
            asyncio.run(es_manager.index_document(
                tenant_id,
                event_type,
                event_data.get("credential_id", event_data.get("id", event_data.get(f"{event_type[:-1]}_id"))),
                event_data
            ))
            
            logger.info(f"Indexed {event_type} for tenant {tenant_id}")
            
        except Exception as e:
            logger.error(f"Failed to index event: {e}")

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Initialize the service"""
    # Initialize Elasticsearch
    await es_manager.initialize()
    
    # Start indexing consumer
    consumer = IndexingConsumer(app.state)
    consumer.start()
    app.state.indexing_consumer = consumer
    
    logger.info("Search service started")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    # Stop indexing consumer
    if hasattr(app.state, "indexing_consumer"):
        app.state.indexing_consumer.running = False
        app.state.indexing_consumer.join(timeout=5)
        
    # Close Elasticsearch connection
    await es_manager.close()
    
    logger.info("Search service stopped")

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # Check Elasticsearch connection
    es_health = "unhealthy"
    try:
        info = await es_manager.client.info()
        es_health = "healthy"
    except:
        pass
        
    return {
        "status": "healthy" if es_health == "healthy" else "degraded",
        "service": "search-service",
        "elasticsearch": es_health
    } 