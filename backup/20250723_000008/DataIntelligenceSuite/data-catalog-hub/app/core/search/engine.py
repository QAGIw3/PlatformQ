"""
Search Engine for data discovery
"""

import json
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from collections import defaultdict

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from platformq_shared.logging import get_logger
from ..config import Settings
from ..atlas_client import AtlasClient
from ..cache_manager import CacheManager

logger = get_logger(__name__)


class SearchEngine:
    """Elasticsearch-powered search and discovery engine"""
    
    def __init__(self, settings: Settings, atlas_client: AtlasClient, cache_manager: CacheManager):
        self.settings = settings
        self.atlas = atlas_client
        self.cache = cache_manager
        self.es_client: Optional[AsyncElasticsearch] = None
        self.index_prefix = settings.search_index_prefix
        
    async def initialize(self):
        """Initialize the search engine"""
        logger.info("Initializing Search Engine")
        
        # Create Elasticsearch client
        self.es_client = AsyncElasticsearch(
            hosts=self.settings.elasticsearch_hosts,
            timeout=self.settings.search_timeout
        )
        
        # Verify connectivity
        if not await self.es_client.ping():
            raise RuntimeError("Failed to connect to Elasticsearch")
            
        # Create indexes if needed
        await self._ensure_indexes()
        
        # Start background indexing
        asyncio.create_task(self._sync_with_atlas())
        
        logger.info("Search Engine initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.es_client:
            await self.es_client.close()
            
    async def _ensure_indexes(self):
        """Ensure search indexes exist with proper mappings"""
        indexes = {
            f"{self.index_prefix}entities": {
                "mappings": {
                    "properties": {
                        "guid": {"type": "keyword"},
                        "typeName": {"type": "keyword"},
                        "qualifiedName": {"type": "keyword"},
                        "name": {"type": "text", "analyzer": "standard"},
                        "description": {"type": "text", "analyzer": "standard"},
                        "owner": {"type": "keyword"},
                        "classifications": {"type": "keyword"},
                        "tags": {"type": "keyword"},
                        "attributes": {"type": "object", "enabled": True},
                        "createdTime": {"type": "date"},
                        "modifiedTime": {"type": "date"},
                        "searchText": {"type": "text", "analyzer": "standard"},
                        "suggest": {
                            "type": "completion",
                            "analyzer": "simple",
                            "preserve_separators": True,
                            "preserve_position_increments": True,
                            "max_input_length": 50
                        }
                    }
                }
            },
            f"{self.index_prefix}glossary": {
                "mappings": {
                    "properties": {
                        "guid": {"type": "keyword"},
                        "termName": {"type": "text", "analyzer": "standard"},
                        "definition": {"type": "text", "analyzer": "standard"},
                        "abbreviation": {"type": "keyword"},
                        "usage": {"type": "text"},
                        "categories": {"type": "keyword"},
                        "relatedTerms": {"type": "keyword"},
                        "suggest": {"type": "completion"}
                    }
                }
            }
        }
        
        for index_name, index_config in indexes.items():
            exists = await self.es_client.indices.exists(index=index_name)
            if not exists:
                await self.es_client.indices.create(
                    index=index_name,
                    body=index_config
                )
                logger.info(f"Created index: {index_name}")
                
    async def index_entity(self, entity: Dict[str, Any]):
        """Index a single entity"""
        try:
            # Prepare document for indexing
            doc = {
                "guid": entity['guid'],
                "typeName": entity['typeName'],
                "qualifiedName": entity['attributes'].get('qualifiedName', ''),
                "name": entity['attributes'].get('name', ''),
                "description": entity['attributes'].get('description', ''),
                "owner": entity['attributes'].get('owner', ''),
                "classifications": [c['typeName'] for c in entity.get('classifications', [])],
                "tags": entity.get('tags', []),
                "attributes": entity['attributes'],
                "createdTime": entity.get('createTime'),
                "modifiedTime": entity.get('updateTime'),
                "searchText": self._create_search_text(entity),
                "suggest": {
                    "input": [
                        entity['attributes'].get('name', ''),
                        entity['attributes'].get('qualifiedName', '').split('.')[-1]
                    ],
                    "weight": self._calculate_weight(entity)
                }
            }
            
            # Index document
            await self.es_client.index(
                index=f"{self.index_prefix}entities",
                id=entity['guid'],
                body=doc
            )
            
        except Exception as e:
            logger.error(f"Failed to index entity {entity.get('guid')}: {e}")
            
    def _create_search_text(self, entity: Dict[str, Any]) -> str:
        """Create searchable text from entity"""
        parts = [
            entity['attributes'].get('name', ''),
            entity['attributes'].get('qualifiedName', ''),
            entity['attributes'].get('description', ''),
            entity['attributes'].get('owner', ''),
            ' '.join(entity.get('tags', [])),
            ' '.join([c['typeName'] for c in entity.get('classifications', [])])
        ]
        
        return ' '.join(filter(None, parts))
        
    def _calculate_weight(self, entity: Dict[str, Any]) -> int:
        """Calculate suggestion weight based on entity importance"""
        weight = 1
        
        # Boost by type
        if entity['typeName'] in ['dataset', 'table', 'database']:
            weight += 2
            
        # Boost by classifications
        if entity.get('classifications'):
            weight += len(entity['classifications'])
            
        # Boost by usage (would need usage metrics)
        # weight += entity.get('usage_count', 0) // 10
        
        return min(weight, 10)  # Cap at 10
        
    async def search(self,
                    query: str,
                    filters: Optional[Dict[str, Any]] = None,
                    limit: int = None,
                    offset: int = 0,
                    sort_by: Optional[str] = None) -> Dict[str, Any]:
        """Perform full-text search"""
        # Check cache
        cache_key = f"search:{query}:{json.dumps(filters or {})}:{limit}:{offset}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
            
        # Build query
        must_clauses = []
        
        # Main query
        if query and query != "*":
            must_clauses.append({
                "multi_match": {
                    "query": query,
                    "fields": ["name^3", "description^2", "searchText"],
                    "type": "best_fields",
                    "fuzziness": "AUTO"
                }
            })
        else:
            must_clauses.append({"match_all": {}})
            
        # Apply filters
        filter_clauses = []
        if filters:
            if 'typeName' in filters:
                if isinstance(filters['typeName'], list):
                    filter_clauses.append({
                        "terms": {"typeName": filters['typeName']}
                    })
                else:
                    filter_clauses.append({
                        "term": {"typeName": filters['typeName']}
                    })
                    
            if 'owner' in filters:
                filter_clauses.append({
                    "term": {"owner": filters['owner']}
                })
                
            if 'classifications' in filters:
                for classification in filters['classifications']:
                    filter_clauses.append({
                        "term": {"classifications": classification}
                    })
                    
            if 'tags' in filters:
                for tag in filters['tags']:
                    filter_clauses.append({
                        "term": {"tags": tag}
                    })
                    
        # Build final query
        es_query = {
            "bool": {
                "must": must_clauses,
                "filter": filter_clauses
            }
        }
        
        # Prepare search body
        body = {
            "query": es_query,
            "from": offset,
            "size": limit or self.settings.search_result_limit,
            "highlight": {
                "fields": {
                    "name": {},
                    "description": {},
                    "searchText": {}
                }
            },
            "aggs": {
                "types": {
                    "terms": {"field": "typeName", "size": 20}
                },
                "owners": {
                    "terms": {"field": "owner", "size": 20}
                },
                "classifications": {
                    "terms": {"field": "classifications", "size": 20}
                }
            }
        }
        
        # Add sorting
        if sort_by:
            body["sort"] = [{sort_by: {"order": "desc"}}]
        else:
            body["sort"] = ["_score", {"modifiedTime": {"order": "desc"}}]
            
        # Execute search
        result = await self.es_client.search(
            index=f"{self.index_prefix}entities",
            body=body
        )
        
        # Format response
        response = {
            "total": result['hits']['total']['value'],
            "results": [],
            "facets": {
                "types": [
                    {"value": b['key'], "count": b['doc_count']}
                    for b in result['aggregations']['types']['buckets']
                ],
                "owners": [
                    {"value": b['key'], "count": b['doc_count']}
                    for b in result['aggregations']['owners']['buckets']
                ],
                "classifications": [
                    {"value": b['key'], "count": b['doc_count']}
                    for b in result['aggregations']['classifications']['buckets']
                ]
            }
        }
        
        # Process hits
        for hit in result['hits']['hits']:
            doc = hit['_source']
            doc['_score'] = hit['_score']
            
            # Add highlights
            if 'highlight' in hit:
                doc['_highlights'] = hit['highlight']
                
            response['results'].append(doc)
            
        # Cache results
        await self.cache.set(cache_key, response, ttl=self.settings.cache_ttl)
        
        return response
        
    async def suggest(self, prefix: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get search suggestions"""
        body = {
            "suggest": {
                "entity-suggest": {
                    "prefix": prefix,
                    "completion": {
                        "field": "suggest",
                        "size": limit,
                        "fuzzy": {
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        }
        
        result = await self.es_client.search(
            index=f"{self.index_prefix}entities",
            body=body
        )
        
        suggestions = []
        for option in result['suggest']['entity-suggest'][0]['options']:
            suggestions.append({
                "text": option['text'],
                "score": option['_score'],
                "guid": option['_source']['guid'],
                "typeName": option['_source']['typeName']
            })
            
        return suggestions
        
    async def find_related(self, guid: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Find related entities using various strategies"""
        # Get the entity
        entity = await self.atlas.get_entity_by_guid(guid)
        if not entity:
            return []
            
        # Extract features for similarity
        owner = entity['attributes'].get('owner')
        classifications = [c['typeName'] for c in entity.get('classifications', [])]
        tags = entity.get('tags', [])
        
        # Build query to find similar entities
        should_clauses = []
        
        # Same owner
        if owner:
            should_clauses.append({
                "term": {"owner": {"value": owner, "boost": 2}}
            })
            
        # Same classifications
        for classification in classifications:
            should_clauses.append({
                "term": {"classifications": {"value": classification, "boost": 3}}
            })
            
        # Same tags
        for tag in tags:
            should_clauses.append({
                "term": {"tags": {"value": tag, "boost": 1.5}}
            })
            
        # More like this on description
        if entity['attributes'].get('description'):
            should_clauses.append({
                "more_like_this": {
                    "fields": ["description"],
                    "like": entity['attributes']['description'],
                    "min_term_freq": 1,
                    "min_doc_freq": 1
                }
            })
            
        body = {
            "query": {
                "bool": {
                    "should": should_clauses,
                    "must_not": [
                        {"term": {"guid": guid}}  # Exclude self
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": limit
        }
        
        result = await self.es_client.search(
            index=f"{self.index_prefix}entities",
            body=body
        )
        
        related = []
        for hit in result['hits']['hits']:
            doc = hit['_source']
            doc['_score'] = hit['_score']
            doc['_relationship'] = self._determine_relationship(entity, doc)
            related.append(doc)
            
        return related
        
    def _determine_relationship(self, entity1: Dict[str, Any], entity2: Dict[str, Any]) -> str:
        """Determine relationship type between entities"""
        # Check various relationship types
        if entity1.get('owner') == entity2.get('owner'):
            return "same_owner"
            
        common_classifications = set(entity1.get('classifications', [])) & set(entity2.get('classifications', []))
        if common_classifications:
            return f"shared_classification:{','.join(common_classifications)}"
            
        common_tags = set(entity1.get('tags', [])) & set(entity2.get('tags', []))
        if common_tags:
            return f"shared_tags:{','.join(common_tags)}"
            
        return "similar"
        
    async def get_recommendations(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get personalized recommendations based on user activity"""
        # This would integrate with user activity tracking
        # For now, return popular/recent items
        
        body = {
            "query": {
                "function_score": {
                    "query": {"match_all": {}},
                    "functions": [
                        {
                            "field_value_factor": {
                                "field": "modifiedTime",
                                "modifier": "log1p",
                                "factor": 1.2
                            }
                        }
                    ],
                    "boost_mode": "multiply"
                }
            },
            "size": limit
        }
        
        result = await self.es_client.search(
            index=f"{self.index_prefix}entities",
            body=body
        )
        
        recommendations = []
        for hit in result['hits']['hits']:
            doc = hit['_source']
            doc['_score'] = hit['_score']
            doc['_reason'] = "trending"
            recommendations.append(doc)
            
        return recommendations
        
    async def _sync_with_atlas(self):
        """Background task to sync with Atlas"""
        while True:
            try:
                # Get entities from Atlas
                offset = 0
                batch_size = 100
                
                while True:
                    result = await self.atlas.search_entities(
                        query="*",
                        limit=batch_size,
                        offset=offset
                    )
                    
                    entities = result.get('entities', [])
                    if not entities:
                        break
                        
                    # Bulk index
                    actions = []
                    for entity in entities:
                        action = {
                            "_index": f"{self.index_prefix}entities",
                            "_id": entity['guid'],
                            "_source": {
                                "guid": entity['guid'],
                                "typeName": entity['typeName'],
                                "qualifiedName": entity['attributes'].get('qualifiedName', ''),
                                "name": entity['attributes'].get('name', ''),
                                "description": entity['attributes'].get('description', ''),
                                "owner": entity['attributes'].get('owner', ''),
                                "classifications": [c['typeName'] for c in entity.get('classifications', [])],
                                "tags": entity.get('tags', []),
                                "attributes": entity['attributes'],
                                "createdTime": entity.get('createTime'),
                                "modifiedTime": entity.get('updateTime'),
                                "searchText": self._create_search_text(entity),
                                "suggest": {
                                    "input": [
                                        entity['attributes'].get('name', ''),
                                        entity['attributes'].get('qualifiedName', '').split('.')[-1]
                                    ],
                                    "weight": self._calculate_weight(entity)
                                }
                            }
                        }
                        actions.append(action)
                        
                    if actions:
                        await async_bulk(self.es_client, actions)
                        
                    offset += batch_size
                    
                    # Avoid overwhelming the system
                    await asyncio.sleep(0.1)
                    
                logger.info(f"Synced {offset} entities with Elasticsearch")
                
            except Exception as e:
                logger.error(f"Search sync error: {e}")
                
            # Run sync periodically
            await asyncio.sleep(300)  # Every 5 minutes 