"""
Content Indexer for searchable storage.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from collections import defaultdict

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class IndexingStatus(str, Enum):
    """Status of indexing operation."""
    PENDING = "pending"
    INDEXING = "indexing"
    INDEXED = "indexed"
    FAILED = "failed"
    UPDATING = "updating"


@dataclass
class IndexableContent:
    """Content to be indexed."""
    identifier: str
    tenant_id: str
    content_type: str
    title: Optional[str] = None
    content: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    file_size: Optional[int] = None
    checksum: Optional[str] = None
    language: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for indexing."""
        return {
            "identifier": self.identifier,
            "tenant_id": self.tenant_id,
            "content_type": self.content_type,
            "title": self.title,
            "content": self.content,
            "metadata": self.metadata,
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "file_size": self.file_size,
            "checksum": self.checksum,
            "language": self.language
        }


@dataclass
class SearchQuery:
    """Search query parameters."""
    query: str
    tenant_id: str
    filters: Dict[str, Any] = field(default_factory=dict)
    fields: List[str] = field(default_factory=list)
    from_: int = 0
    size: int = 10
    sort: List[Dict[str, str]] = field(default_factory=list)
    highlight: bool = True
    fuzzy: bool = True
    aggregations: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SearchResult:
    """Search result."""
    total: int
    hits: List[Dict[str, Any]]
    aggregations: Dict[str, Any] = field(default_factory=dict)
    took: int = 0
    max_score: Optional[float] = None


class ContentIndexer:
    """
    Indexes and searches content using Elasticsearch.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        elasticsearch_url: str = "http://elasticsearch:9200"
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.elasticsearch_url = elasticsearch_url
        
        # Elasticsearch client
        self.es_client: Optional[AsyncElasticsearch] = None
        
        # Index configuration
        self.index_prefix = "platformq_content"
        self.index_settings = {
            "number_of_shards": 3,
            "number_of_replicas": 1,
            "analysis": {
                "analyzer": {
                    "content_analyzer": {
                        "type": "custom",
                        "tokenizer": "standard",
                        "filter": ["lowercase", "stop", "snowball"]
                    }
                }
            }
        }
        
        # Indexing queue
        self.indexing_queue: asyncio.Queue = asyncio.Queue()
        self.indexing_workers: List[asyncio.Task] = []
        self.num_workers = 2
        
        # Statistics
        self.indexing_stats = {
            "total_indexed": 0,
            "total_failed": 0,
            "last_indexed": None
        }
        
        logger.info("Content Indexer initialized")
        
    async def initialize(self):
        """Initialize content indexer."""
        # Create Elasticsearch client
        self.es_client = AsyncElasticsearch([self.elasticsearch_url])
        
        # Create index template
        await self._create_index_template()
        
        # Subscribe to events
        await self.event_bus.subscribe("storage.uploaded", self._handle_storage_uploaded)
        await self.event_bus.subscribe("storage.deleted", self._handle_storage_deleted)
        await self.event_bus.subscribe("preview.generated", self._handle_preview_generated)
        
        # Start indexing workers
        for i in range(self.num_workers):
            worker = asyncio.create_task(self._indexing_worker(i))
            self.indexing_workers.append(worker)
        
        logger.info("Content Indexer ready")
        
    async def cleanup(self):
        """Cleanup indexer resources."""
        # Stop workers
        for worker in self.indexing_workers:
            worker.cancel()
        
        # Wait for workers
        await asyncio.gather(*self.indexing_workers, return_exceptions=True)
        
        # Close Elasticsearch client
        if self.es_client:
            await self.es_client.close()
        
        logger.info("Content Indexer cleaned up")
        
    async def index_content(
        self,
        content: IndexableContent,
        refresh: bool = False
    ) -> bool:
        """Index content synchronously."""
        try:
            # Get index name
            index_name = self._get_index_name(content.tenant_id)
            
            # Ensure index exists
            await self._ensure_index_exists(index_name)
            
            # Index document
            response = await self.es_client.index(
                index=index_name,
                id=content.identifier,
                body=content.to_dict(),
                refresh=refresh
            )
            
            # Update statistics
            self.indexing_stats["total_indexed"] += 1
            self.indexing_stats["last_indexed"] = datetime.utcnow().isoformat()
            
            # Cache indexing status
            await self.cache_manager.set(
                f"indexing:status:{content.tenant_id}:{content.identifier}",
                IndexingStatus.INDEXED.value,
                ttl=3600
            )
            
            # Publish event
            await self.event_bus.publish("content.indexed", {
                "identifier": content.identifier,
                "tenant_id": content.tenant_id,
                "index": index_name
            })
            
            logger.info(f"Indexed content: {content.identifier}")
            
            return response.get("result") in ["created", "updated"]
            
        except Exception as e:
            logger.error(f"Error indexing content: {e}")
            self.indexing_stats["total_failed"] += 1
            
            # Cache failure status
            await self.cache_manager.set(
                f"indexing:status:{content.tenant_id}:{content.identifier}",
                IndexingStatus.FAILED.value,
                ttl=3600
            )
            
            return False
            
    async def index_content_async(
        self,
        content: IndexableContent
    ) -> str:
        """Queue content for asynchronous indexing."""
        # Add to queue
        await self.indexing_queue.put(content)
        
        # Cache pending status
        await self.cache_manager.set(
            f"indexing:status:{content.tenant_id}:{content.identifier}",
            IndexingStatus.PENDING.value,
            ttl=3600
        )
        
        logger.info(f"Queued content for indexing: {content.identifier}")
        
        return content.identifier
        
    async def search(
        self,
        query: SearchQuery
    ) -> SearchResult:
        """Search indexed content."""
        try:
            # Get index name
            index_name = self._get_index_name(query.tenant_id)
            
            # Build query
            es_query = self._build_search_query(query)
            
            # Execute search
            response = await self.es_client.search(
                index=index_name,
                body=es_query,
                from_=query.from_,
                size=query.size
            )
            
            # Parse results
            hits = []
            for hit in response["hits"]["hits"]:
                result = {
                    "_id": hit["_id"],
                    "_score": hit["_score"],
                    **hit["_source"]
                }
                
                # Add highlights if available
                if "highlight" in hit:
                    result["_highlight"] = hit["highlight"]
                
                hits.append(result)
            
            # Create search result
            result = SearchResult(
                total=response["hits"]["total"]["value"],
                hits=hits,
                took=response["took"],
                max_score=response["hits"]["max_score"]
            )
            
            # Add aggregations if present
            if "aggregations" in response:
                result.aggregations = response["aggregations"]
            
            # Cache search result
            cache_key = f"search:{query.tenant_id}:{hash(json.dumps(es_query))}"
            await self.cache_manager.set(
                cache_key,
                result.__dict__,
                ttl=300  # 5 minutes
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error searching content: {e}")
            return SearchResult(total=0, hits=[])
            
    async def update_content(
        self,
        identifier: str,
        tenant_id: str,
        updates: Dict[str, Any],
        refresh: bool = False
    ) -> bool:
        """Update indexed content."""
        try:
            # Get index name
            index_name = self._get_index_name(tenant_id)
            
            # Update document
            response = await self.es_client.update(
                index=index_name,
                id=identifier,
                body={
                    "doc": updates,
                    "doc_as_upsert": True
                },
                refresh=refresh
            )
            
            # Update cache status
            await self.cache_manager.set(
                f"indexing:status:{tenant_id}:{identifier}",
                IndexingStatus.INDEXED.value,
                ttl=3600
            )
            
            return response.get("result") in ["updated", "created"]
            
        except Exception as e:
            logger.error(f"Error updating content: {e}")
            return False
            
    async def delete_content(
        self,
        identifier: str,
        tenant_id: str,
        refresh: bool = False
    ) -> bool:
        """Delete indexed content."""
        try:
            # Get index name
            index_name = self._get_index_name(tenant_id)
            
            # Delete document
            response = await self.es_client.delete(
                index=index_name,
                id=identifier,
                refresh=refresh,
                ignore=[404]  # Ignore if not found
            )
            
            # Remove from cache
            await self.cache_manager.delete(
                f"indexing:status:{tenant_id}:{identifier}"
            )
            
            # Publish event
            await self.event_bus.publish("content.deleted_from_index", {
                "identifier": identifier,
                "tenant_id": tenant_id
            })
            
            return response.get("result") == "deleted"
            
        except Exception as e:
            logger.error(f"Error deleting content: {e}")
            return False
            
    async def bulk_index(
        self,
        contents: List[IndexableContent],
        refresh: bool = False
    ) -> Dict[str, int]:
        """Bulk index multiple contents."""
        if not contents:
            return {"indexed": 0, "failed": 0}
        
        # Group by tenant
        by_tenant = defaultdict(list)
        for content in contents:
            by_tenant[content.tenant_id].append(content)
        
        total_indexed = 0
        total_failed = 0
        
        for tenant_id, tenant_contents in by_tenant.items():
            # Get index name
            index_name = self._get_index_name(tenant_id)
            
            # Ensure index exists
            await self._ensure_index_exists(index_name)
            
            # Prepare bulk actions
            actions = []
            for content in tenant_contents:
                actions.append({
                    "_index": index_name,
                    "_id": content.identifier,
                    "_source": content.to_dict()
                })
            
            try:
                # Perform bulk indexing
                success, failed = await async_bulk(
                    self.es_client,
                    actions,
                    refresh=refresh
                )
                
                total_indexed += success
                total_failed += len(failed)
                
                # Update statistics
                self.indexing_stats["total_indexed"] += success
                self.indexing_stats["total_failed"] += len(failed)
                
                logger.info(f"Bulk indexed {success} documents for tenant {tenant_id}")
                
            except Exception as e:
                logger.error(f"Error in bulk indexing: {e}")
                total_failed += len(tenant_contents)
        
        return {"indexed": total_indexed, "failed": total_failed}
        
    async def get_indexing_status(
        self,
        identifier: str,
        tenant_id: str
    ) -> Optional[IndexingStatus]:
        """Get indexing status for content."""
        # Check cache
        status = await self.cache_manager.get(
            f"indexing:status:{tenant_id}:{identifier}"
        )
        
        if status:
            return IndexingStatus(status)
        
        # Check if exists in index
        index_name = self._get_index_name(tenant_id)
        
        try:
            exists = await self.es_client.exists(
                index=index_name,
                id=identifier
            )
            
            if exists:
                return IndexingStatus.INDEXED
            
        except Exception:
            pass
        
        return None
        
    async def analyze_text(
        self,
        text: str,
        analyzer: str = "content_analyzer"
    ) -> List[str]:
        """Analyze text using Elasticsearch analyzer."""
        try:
            response = await self.es_client.indices.analyze(
                body={
                    "analyzer": analyzer,
                    "text": text
                }
            )
            
            return [token["token"] for token in response["tokens"]]
            
        except Exception as e:
            logger.error(f"Error analyzing text: {e}")
            return []
            
    async def suggest(
        self,
        prefix: str,
        tenant_id: str,
        field: str = "content",
        size: int = 5
    ) -> List[str]:
        """Get search suggestions."""
        try:
            index_name = self._get_index_name(tenant_id)
            
            response = await self.es_client.search(
                index=index_name,
                body={
                    "suggest": {
                        "text": prefix,
                        "completion": {
                            "field": f"{field}.suggest",
                            "size": size
                        }
                    }
                }
            )
            
            suggestions = []
            if "suggest" in response and "completion" in response["suggest"]:
                for option in response["suggest"]["completion"][0]["options"]:
                    suggestions.append(option["text"])
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error getting suggestions: {e}")
            return []
            
    async def _create_index_template(self):
        """Create index template for content indices."""
        try:
            template_body = {
                "index_patterns": [f"{self.index_prefix}_*"],
                "settings": self.index_settings,
                "mappings": {
                    "properties": {
                        "identifier": {"type": "keyword"},
                        "tenant_id": {"type": "keyword"},
                        "content_type": {"type": "keyword"},
                        "title": {
                            "type": "text",
                            "analyzer": "content_analyzer",
                            "fields": {
                                "keyword": {"type": "keyword"}
                            }
                        },
                        "content": {
                            "type": "text",
                            "analyzer": "content_analyzer",
                            "fields": {
                                "suggest": {
                                    "type": "completion"
                                }
                            }
                        },
                        "metadata": {"type": "object", "enabled": True},
                        "tags": {"type": "keyword"},
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"},
                        "file_size": {"type": "long"},
                        "checksum": {"type": "keyword"},
                        "language": {"type": "keyword"}
                    }
                }
            }
            
            await self.es_client.indices.put_template(
                name=f"{self.index_prefix}_template",
                body=template_body
            )
            
            logger.info("Created index template")
            
        except Exception as e:
            logger.error(f"Error creating index template: {e}")
            
    async def _ensure_index_exists(self, index_name: str):
        """Ensure index exists."""
        try:
            exists = await self.es_client.indices.exists(index=index_name)
            
            if not exists:
                await self.es_client.indices.create(
                    index=index_name,
                    body={
                        "settings": self.index_settings
                    }
                )
                
                logger.info(f"Created index: {index_name}")
                
        except Exception as e:
            logger.error(f"Error ensuring index exists: {e}")
            
    def _get_index_name(self, tenant_id: str) -> str:
        """Get index name for tenant."""
        return f"{self.index_prefix}_{tenant_id}"
        
    def _build_search_query(self, query: SearchQuery) -> Dict[str, Any]:
        """Build Elasticsearch query from search query."""
        # Base query
        es_query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": []
                }
            }
        }
        
        # Add main query
        if query.query:
            if query.fuzzy:
                es_query["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query.query,
                        "fields": query.fields or ["title^2", "content", "tags"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                })
            else:
                es_query["query"]["bool"]["must"].append({
                    "multi_match": {
                        "query": query.query,
                        "fields": query.fields or ["title^2", "content", "tags"]
                    }
                })
        
        # Add filters
        for field, value in query.filters.items():
            if isinstance(value, list):
                es_query["query"]["bool"]["filter"].append({
                    "terms": {field: value}
                })
            else:
                es_query["query"]["bool"]["filter"].append({
                    "term": {field: value}
                })
        
        # Add highlighting
        if query.highlight:
            es_query["highlight"] = {
                "fields": {
                    "title": {},
                    "content": {
                        "fragment_size": 150,
                        "number_of_fragments": 3
                    }
                }
            }
        
        # Add sorting
        if query.sort:
            es_query["sort"] = query.sort
        else:
            es_query["sort"] = ["_score", {"created_at": "desc"}]
        
        # Add aggregations
        if query.aggregations:
            es_query["aggs"] = query.aggregations
        
        return es_query
        
    async def _indexing_worker(self, worker_id: int):
        """Worker to process indexing queue."""
        logger.info(f"Indexing worker {worker_id} started")
        
        while True:
            try:
                # Get content from queue
                content = await self.indexing_queue.get()
                
                # Update status
                await self.cache_manager.set(
                    f"indexing:status:{content.tenant_id}:{content.identifier}",
                    IndexingStatus.INDEXING.value,
                    ttl=3600
                )
                
                # Index content
                success = await self.index_content(content)
                
                if not success:
                    logger.error(f"Failed to index content: {content.identifier}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(1)
        
        logger.info(f"Indexing worker {worker_id} stopped")
        
    async def _handle_storage_uploaded(self, event_data: Dict[str, Any]):
        """Handle storage upload event."""
        try:
            # Create indexable content
            content = IndexableContent(
                identifier=event_data.get("identifier"),
                tenant_id=event_data.get("tenant_id"),
                content_type=event_data.get("content_type", "unknown"),
                file_size=event_data.get("size"),
                metadata=event_data.get("metadata", {})
            )
            
            # Queue for indexing
            await self.index_content_async(content)
            
        except Exception as e:
            logger.error(f"Error handling storage upload: {e}")
            
    async def _handle_storage_deleted(self, event_data: Dict[str, Any]):
        """Handle storage delete event."""
        try:
            await self.delete_content(
                identifier=event_data.get("identifier"),
                tenant_id=event_data.get("tenant_id")
            )
            
        except Exception as e:
            logger.error(f"Error handling storage delete: {e}")
            
    async def _handle_preview_generated(self, event_data: Dict[str, Any]):
        """Handle preview generation event."""
        try:
            # Extract text content from preview
            preview_type = event_data.get("preview_type")
            
            if preview_type in ["text_extract", "full_text"]:
                # Get preview result
                preview_id = event_data.get("preview_id")
                # This would fetch the preview and update the index
                
        except Exception as e:
            logger.error(f"Error handling preview generation: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get indexer statistics."""
        return {
            "indexing_stats": self.indexing_stats,
            "queue_size": self.indexing_queue.qsize(),
            "num_workers": self.num_workers,
            "index_prefix": self.index_prefix
        } 