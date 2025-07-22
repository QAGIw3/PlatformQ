"""Elasticsearch Client"""

from typing import Dict, Any, List
import logging
from elasticsearch import AsyncElasticsearch

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """Client for Elasticsearch operations"""
    
    def __init__(self, hosts: List[str] = None):
        self.hosts = hosts or ["elasticsearch:9200"]
        self.client = AsyncElasticsearch(hosts=self.hosts)
    
    async def index_document(self, index: str, doc_id: str, document: Dict[str, Any]):
        """Index a document"""
        await self.client.index(index=index, id=doc_id, body=document)
    
    async def search(self, index: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Search documents"""
        result = await self.client.search(index=index, body=query)
        return [hit["_source"] for hit in result["hits"]["hits"]]
    
    async def close(self):
        await self.client.close() 