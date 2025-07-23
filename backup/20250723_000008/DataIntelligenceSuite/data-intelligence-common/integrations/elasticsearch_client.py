"""
Elasticsearch Client Integration

Provides high-level client for Elasticsearch operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import asyncio

from elasticsearch import AsyncElasticsearch, helpers
from elasticsearch.exceptions import (
    NotFoundError,
    RequestError,
    ConflictError,
    ConnectionError as ESConnectionError
)

logger = logging.getLogger(__name__)


@dataclass
class ElasticsearchConfig:
    """Configuration for Elasticsearch client"""
    hosts: List[str] = field(default_factory=lambda: ["localhost:9200"])
    
    # Authentication
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    
    # Connection settings
    timeout: int = 30
    max_retries: int = 3
    retry_on_timeout: bool = True
    
    # SSL
    use_ssl: bool = False
    verify_certs: bool = True
    ca_certs: Optional[str] = None
    
    # Performance
    max_chunk_bytes: int = 100 * 1024 * 1024  # 100MB
    chunk_size: int = 500
    max_concurrent_searches: int = 10


class ElasticsearchClient:
    """
    High-level client for Elasticsearch operations.
    
    Features:
    - Async operations
    - Bulk operations
    - Index management
    - Search with aggregations
    - Document CRUD
    - Mapping management
    """
    
    def __init__(self, config: ElasticsearchConfig):
        self.config = config
        self._client: Optional[AsyncElasticsearch] = None
        
    async def connect(self):
        """Connect to Elasticsearch cluster"""
        try:
            # Build connection parameters
            kwargs = {
                "hosts": self.config.hosts,
                "timeout": self.config.timeout,
                "max_retries": self.config.max_retries,
                "retry_on_timeout": self.config.retry_on_timeout,
            }
            
            # Add authentication
            if self.config.username and self.config.password:
                kwargs["basic_auth"] = (self.config.username, self.config.password)
            elif self.config.api_key:
                kwargs["api_key"] = self.config.api_key
                
            # Add SSL settings
            if self.config.use_ssl:
                kwargs["use_ssl"] = True
                kwargs["verify_certs"] = self.config.verify_certs
                if self.config.ca_certs:
                    kwargs["ca_certs"] = self.config.ca_certs
                    
            # Create client
            self._client = AsyncElasticsearch(**kwargs)
            
            # Test connection
            info = await self._client.info()
            logger.info(
                f"Connected to Elasticsearch cluster: {info['cluster_name']} "
                f"version {info['version']['number']}"
            )
            
        except Exception as e:
            logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Elasticsearch"""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Disconnected from Elasticsearch")
            
    async def create_index(
        self,
        index: str,
        mappings: Optional[Dict[str, Any]] = None,
        settings: Optional[Dict[str, Any]] = None,
        aliases: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Create an index with optional mappings and settings"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings
        if aliases:
            body["aliases"] = aliases
            
        try:
            response = await self._client.indices.create(
                index=index,
                body=body if body else None
            )
            logger.info(f"Created index: {index}")
            return response.get("acknowledged", False)
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                logger.warning(f"Index already exists: {index}")
                return False
            raise
            
    async def delete_index(self, index: str) -> bool:
        """Delete an index"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        try:
            response = await self._client.indices.delete(index=index)
            logger.info(f"Deleted index: {index}")
            return response.get("acknowledged", False)
        except NotFoundError:
            logger.warning(f"Index not found: {index}")
            return False
            
    async def index_document(
        self,
        index: str,
        document: Dict[str, Any],
        doc_id: Optional[str] = None,
        refresh: Union[bool, str] = False
    ) -> str:
        """Index a single document"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        response = await self._client.index(
            index=index,
            body=document,
            id=doc_id,
            refresh=refresh
        )
        
        return response["_id"]
        
    async def bulk_index(
        self,
        index: str,
        documents: List[Dict[str, Any]],
        doc_id_field: Optional[str] = None,
        refresh: Union[bool, str] = False
    ) -> Dict[str, Any]:
        """Bulk index documents"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        # Prepare bulk actions
        actions = []
        for doc in documents:
            action = {
                "_index": index,
                "_source": doc
            }
            if doc_id_field and doc_id_field in doc:
                action["_id"] = doc[doc_id_field]
            actions.append(action)
            
        # Execute bulk operation
        success, failed = await helpers.async_bulk(
            self._client,
            actions,
            chunk_size=self.config.chunk_size,
            max_chunk_bytes=self.config.max_chunk_bytes,
            refresh=refresh,
            raise_on_error=False
        )
        
        return {
            "success": success,
            "failed": failed,
            "total": len(documents)
        }
        
    async def get_document(
        self,
        index: str,
        doc_id: str,
        source_includes: Optional[List[str]] = None,
        source_excludes: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a document by ID"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        try:
            response = await self._client.get(
                index=index,
                id=doc_id,
                _source_includes=source_includes,
                _source_excludes=source_excludes
            )
            return response["_source"]
        except NotFoundError:
            return None
            
    async def update_document(
        self,
        index: str,
        doc_id: str,
        doc: Optional[Dict[str, Any]] = None,
        script: Optional[Dict[str, Any]] = None,
        upsert: Optional[Dict[str, Any]] = None,
        refresh: Union[bool, str] = False
    ) -> bool:
        """Update a document"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {}
        if doc:
            body["doc"] = doc
        if script:
            body["script"] = script
        if upsert:
            body["upsert"] = upsert
            
        try:
            response = await self._client.update(
                index=index,
                id=doc_id,
                body=body,
                refresh=refresh
            )
            return response["result"] in ["updated", "created"]
        except NotFoundError:
            return False
            
    async def delete_document(
        self,
        index: str,
        doc_id: str,
        refresh: Union[bool, str] = False
    ) -> bool:
        """Delete a document"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        try:
            response = await self._client.delete(
                index=index,
                id=doc_id,
                refresh=refresh
            )
            return response["result"] == "deleted"
        except NotFoundError:
            return False
            
    async def search(
        self,
        index: Union[str, List[str]],
        query: Optional[Dict[str, Any]] = None,
        size: int = 10,
        from_: int = 0,
        sort: Optional[List[Dict[str, Any]]] = None,
        source: Union[bool, List[str], Dict[str, Any]] = True,
        aggs: Optional[Dict[str, Any]] = None,
        highlight: Optional[Dict[str, Any]] = None,
        track_total_hits: Union[bool, int] = True
    ) -> Dict[str, Any]:
        """Search documents"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {}
        if query:
            body["query"] = query
        if aggs:
            body["aggs"] = aggs
        if highlight:
            body["highlight"] = highlight
            
        response = await self._client.search(
            index=index,
            body=body,
            size=size,
            from_=from_,
            sort=sort,
            _source=source,
            track_total_hits=track_total_hits
        )
        
        return {
            "total": response["hits"]["total"]["value"],
            "hits": [
                {
                    "id": hit["_id"],
                    "score": hit.get("_score"),
                    "source": hit.get("_source", {}),
                    "highlight": hit.get("highlight", {})
                }
                for hit in response["hits"]["hits"]
            ],
            "aggregations": response.get("aggregations", {}),
            "took": response["took"]
        }
        
    async def count(
        self,
        index: Union[str, List[str]],
        query: Optional[Dict[str, Any]] = None
    ) -> int:
        """Count documents matching query"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {"query": query} if query else None
        
        response = await self._client.count(
            index=index,
            body=body
        )
        
        return response["count"]
        
    async def aggregate(
        self,
        index: Union[str, List[str]],
        aggs: Dict[str, Any],
        query: Optional[Dict[str, Any]] = None,
        size: int = 0
    ) -> Dict[str, Any]:
        """Run aggregations"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {"aggs": aggs}
        if query:
            body["query"] = query
            
        response = await self._client.search(
            index=index,
            body=body,
            size=size
        )
        
        return response.get("aggregations", {})
        
    async def scroll(
        self,
        index: Union[str, List[str]],
        query: Optional[Dict[str, Any]] = None,
        size: int = 1000,
        scroll: str = "5m"
    ):
        """Scroll through large result sets"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {"query": query} if query else {}
        
        # Initial search
        response = await self._client.search(
            index=index,
            body=body,
            size=size,
            scroll=scroll
        )
        
        scroll_id = response["_scroll_id"]
        hits = response["hits"]["hits"]
        
        try:
            while hits:
                for hit in hits:
                    yield {
                        "id": hit["_id"],
                        "source": hit.get("_source", {}),
                        "score": hit.get("_score")
                    }
                    
                # Get next batch
                response = await self._client.scroll(
                    scroll_id=scroll_id,
                    scroll=scroll
                )
                
                scroll_id = response["_scroll_id"]
                hits = response["hits"]["hits"]
                
        finally:
            # Clear scroll
            if scroll_id:
                await self._client.clear_scroll(scroll_id=scroll_id)
                
    async def reindex(
        self,
        source_index: str,
        dest_index: str,
        query: Optional[Dict[str, Any]] = None,
        wait_for_completion: bool = True
    ) -> Dict[str, Any]:
        """Reindex documents from source to destination"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {
            "source": {"index": source_index},
            "dest": {"index": dest_index}
        }
        
        if query:
            body["source"]["query"] = query
            
        response = await self._client.reindex(
            body=body,
            wait_for_completion=wait_for_completion
        )
        
        return response
        
    async def update_mapping(
        self,
        index: str,
        properties: Dict[str, Any]
    ) -> bool:
        """Update index mapping"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        response = await self._client.indices.put_mapping(
            index=index,
            body={"properties": properties}
        )
        
        return response.get("acknowledged", False)
        
    async def analyze(
        self,
        text: str,
        analyzer: Optional[str] = None,
        index: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Analyze text using Elasticsearch analyzer"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        body = {"text": text}
        if analyzer:
            body["analyzer"] = analyzer
            
        response = await self._client.indices.analyze(
            index=index,
            body=body
        )
        
        return response["tokens"]
        
    async def get_cluster_health(self) -> Dict[str, Any]:
        """Get cluster health status"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        return await self._client.cluster.health()
        
    async def get_indices_stats(self, index: Optional[str] = None) -> Dict[str, Any]:
        """Get indices statistics"""
        if not self._client:
            raise RuntimeError("Not connected to Elasticsearch")
            
        return await self._client.indices.stats(index=index) 