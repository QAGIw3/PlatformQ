from elasticsearch import AsyncElasticsearch
from functools import lru_cache

from ..core.config import settings
from ..services.indexer import Indexer
from ..messaging.search_consumer import SearchConsumer

@lru_cache()
def get_es_client() -> AsyncElasticsearch:
    return AsyncElasticsearch(hosts=[settings.ES_HOST])

@lru_cache()
def get_indexer() -> Indexer:
    return Indexer(es_client=get_es_client())

@lru_cache()
def get_search_consumer() -> SearchConsumer:
    return SearchConsumer(indexer=get_indexer())
