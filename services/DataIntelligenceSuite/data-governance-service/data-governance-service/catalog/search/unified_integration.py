"""
Unified Search Integration Service

Connects to all platform services to provide comprehensive search
across the entire ecosystem with real-time updates and AI enhancements.
"""

import logging
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
import asyncio
import httpx
from collections import defaultdict
import json

from elasticsearch import AsyncElasticsearch
from ..config import settings
from platformq_consul import VaultConsulIntegration

logger = logging.getLogger(__name__)


class ServiceRegistry:
    """Registry of all searchable services in the platform"""
    
    SEARCHABLE_SERVICES = {
        "auth-service": {
            "url": "http://auth-service:8001",
            "endpoints": {
                "users": "/api/v1/users",
                "roles": "/api/v1/roles",
                "permissions": "/api/v1/permissions"
            },
            "entity_types": ["user", "role", "permission"]
        },
        "digital-asset-service": {
            "url": "http://digital-asset-service:8002",
            "endpoints": {
                "assets": "/api/v1/assets",
                "metadata": "/api/v1/metadata"
            },
            "entity_types": ["digital_asset", "3d_model", "image", "document"]
        },
        "verifiable-credential-service": {
            "url": "http://verifiable-credential-service:8003",
            "endpoints": {
                "credentials": "/api/v1/credentials",
                "schemas": "/api/v1/schemas"
            },
            "entity_types": ["credential", "schema", "issuer"]
        },
        "governance-service": {
            "url": "http://governance-service:8004",
            "endpoints": {
                "proposals": "/api/v1/proposals",
                "votes": "/api/v1/votes"
            },
            "entity_types": ["proposal", "vote", "governance_action"]
        },
        "compliance-service": {
            "url": "http://compliance-service:8005",
            "endpoints": {
                "policies": "/api/v1/policies",
                "audits": "/api/v1/audits"
            },
            "entity_types": ["policy", "audit", "compliance_check"]
        },
        "data-ingestion-service": {
            "url": "http://data-ingestion-service:8010",
            "endpoints": {
                "datasets": "/api/v1/lake/layers",
                "schemas": "/api/v1/schemas"
            },
            "entity_types": ["dataset", "data_source", "ingestion_job"]
        },
        "analytics-service": {
            "url": "http://analytics-service:8011",
            "endpoints": {
                "reports": "/api/v1/reports",
                "dashboards": "/api/v1/dashboards"
            },
            "entity_types": ["report", "dashboard", "metric"]
        },
        "blockchain-connector-service": {
            "url": "http://blockchain-connector-service:8020",
            "endpoints": {
                "transactions": "/api/v1/transactions",
                "contracts": "/api/v1/contracts"
            },
            "entity_types": ["transaction", "smart_contract", "blockchain_event"]
        },
        "marketplace-service": {
            "url": "http://marketplace-service:8030",
            "endpoints": {
                "listings": "/api/v1/listings",
                "orders": "/api/v1/orders"
            },
            "entity_types": ["listing", "order", "marketplace_item"]
        }
    }


class UnifiedSearchIntegration:
    """
    Integrates search across all platform services
    """
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        vault_consul: VaultConsulIntegration,
        service_registry: Optional[Dict[str, Any]] = None
    ):
        self.es_client = es_client
        self.vault_consul = vault_consul
        self.service_registry = service_registry or ServiceRegistry.SEARCHABLE_SERVICES
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Cache for service health status
        self.service_health_cache = {}
        self.health_cache_ttl = 60  # seconds
        
        # Index prefix for unified search
        self.index_prefix = "unified_search"
        
    async def initialize(self):
        """Initialize unified search integration"""
        try:
            # Create unified search indices
            await self._create_unified_indices()
            
            # Start background tasks
            asyncio.create_task(self._periodic_sync())
            asyncio.create_task(self._health_monitor())
            
            logger.info("Unified search integration initialized")
            
        except Exception as e:
            logger.error(f"Error initializing unified search: {e}")
            raise
    
    async def _create_unified_indices(self):
        """Create indices for unified search"""
        # Unified index mapping
        mapping = {
            "mappings": {
                "properties": {
                    # Common fields
                    "entity_id": {"type": "keyword"},
                    "entity_type": {"type": "keyword"},
                    "service_name": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    
                    # Searchable fields
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "suggest": {"type": "completion"}
                        }
                    },
                    "description": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "content": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "tags": {"type": "keyword"},
                    "categories": {"type": "keyword"},
                    
                    # Metadata
                    "metadata": {"type": "object", "enabled": False},
                    
                    # Relationships
                    "related_entities": {
                        "type": "nested",
                        "properties": {
                            "entity_id": {"type": "keyword"},
                            "entity_type": {"type": "keyword"},
                            "relationship": {"type": "keyword"}
                        }
                    },
                    
                    # Vector embedding
                    "embedding": {
                        "type": "dense_vector",
                        "dims": 768,
                        "index": True,
                        "similarity": "cosine"
                    },
                    
                    # Access control
                    "visibility": {"type": "keyword"},
                    "allowed_roles": {"type": "keyword"},
                    "owner": {"type": "keyword"}
                }
            }
        }
        
        index_name = f"{self.index_prefix}_entities"
        if not await self.es_client.indices.exists(index=index_name):
            await self.es_client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created unified search index: {index_name}")
    
    async def search_all_services(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        size: int = 10,
        from_: int = 0,
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Search across all services"""
        try:
            # Build ES query
            es_query = {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["title^3", "description^2", "content", "tags^2"],
                                "type": "best_fields",
                                "fuzziness": "AUTO"
                            }
                        }
                    ],
                    "filter": []
                }
            }
            
            # Add tenant filter
            if tenant_id:
                es_query["bool"]["filter"].append({"term": {"tenant_id": tenant_id}})
            
            # Add custom filters
            if filters:
                for field, value in filters.items():
                    if isinstance(value, list):
                        es_query["bool"]["filter"].append({"terms": {field: value}})
                    else:
                        es_query["bool"]["filter"].append({"term": {field: value}})
            
            # Search
            response = await self.es_client.search(
                index=f"{self.index_prefix}_*",
                body={
                    "query": es_query,
                    "from": from_,
                    "size": size,
                    "aggs": {
                        "entity_types": {"terms": {"field": "entity_type"}},
                        "services": {"terms": {"field": "service_name"}},
                        "tags": {"terms": {"field": "tags", "size": 20}}
                    },
                    "highlight": {
                        "fields": {
                            "title": {},
                            "description": {},
                            "content": {"fragment_size": 150}
                        }
                    }
                }
            )
            
            # Format results
            results = []
            for hit in response["hits"]["hits"]:
                result = {
                    "id": hit["_id"],
                    "score": hit["_score"],
                    "entity_id": hit["_source"]["entity_id"],
                    "entity_type": hit["_source"]["entity_type"],
                    "service": hit["_source"]["service_name"],
                    "title": hit["_source"].get("title"),
                    "description": hit["_source"].get("description"),
                    "created_at": hit["_source"].get("created_at"),
                    "highlights": hit.get("highlight", {})
                }
                
                # Add service-specific URL
                service_info = self.service_registry.get(hit["_source"]["service_name"], {})
                if service_info:
                    result["url"] = self._build_entity_url(
                        service_info["url"],
                        hit["_source"]["entity_type"],
                        hit["_source"]["entity_id"]
                    )
                
                results.append(result)
            
            return {
                "results": results,
                "total": response["hits"]["total"]["value"],
                "aggregations": {
                    "entity_types": self._format_aggregation(response["aggregations"]["entity_types"]),
                    "services": self._format_aggregation(response["aggregations"]["services"]),
                    "tags": self._format_aggregation(response["aggregations"]["tags"])
                }
            }
            
        except Exception as e:
            logger.error(f"Error searching all services: {e}")
            raise
    
    async def index_from_service(
        self,
        service_name: str,
        entity_type: str,
        force_full_sync: bool = False
    ) -> Dict[str, Any]:
        """Index data from a specific service"""
        try:
            service_info = self.service_registry.get(service_name)
            if not service_info:
                raise ValueError(f"Unknown service: {service_name}")
            
            # Check service health
            if not await self._is_service_healthy(service_name):
                logger.warning(f"Service {service_name} is not healthy, skipping indexing")
                return {"status": "skipped", "reason": "service_unhealthy"}
            
            # Get endpoint for entity type
            endpoint = None
            for ep_name, ep_path in service_info["endpoints"].items():
                if entity_type in ep_name or entity_type in service_info["entity_types"]:
                    endpoint = ep_path
                    break
            
            if not endpoint:
                raise ValueError(f"No endpoint found for entity type {entity_type}")
            
            # Fetch data from service
            url = f"{service_info['url']}{endpoint}"
            
            # Get service API key from Vault
            api_key = await self.vault_consul.get_service_api_key(service_name)
            headers = {"X-API-Key": api_key} if api_key else {}
            
            # Paginate through results
            indexed_count = 0
            page = 0
            page_size = 100
            
            while True:
                response = await self.http_client.get(
                    url,
                    headers=headers,
                    params={"page": page, "size": page_size}
                )
                
                if response.status_code != 200:
                    logger.error(f"Error fetching from {service_name}: {response.status_code}")
                    break
                
                data = response.json()
                items = data.get("items", data.get("results", data.get("data", [])))
                
                if not items:
                    break
                
                # Index items
                bulk_actions = []
                for item in items:
                    doc = self._transform_to_unified_format(
                        item,
                        entity_type,
                        service_name
                    )
                    
                    bulk_actions.append({
                        "index": {
                            "_index": f"{self.index_prefix}_entities",
                            "_id": f"{service_name}_{entity_type}_{doc['entity_id']}"
                        }
                    })
                    bulk_actions.append(doc)
                
                if bulk_actions:
                    await self.es_client.bulk(body=bulk_actions)
                    indexed_count += len(items)
                
                # Check if more pages
                total = data.get("total", data.get("count", 0))
                if page * page_size >= total:
                    break
                
                page += 1
            
            logger.info(f"Indexed {indexed_count} {entity_type} from {service_name}")
            
            return {
                "status": "success",
                "service": service_name,
                "entity_type": entity_type,
                "indexed_count": indexed_count
            }
            
        except Exception as e:
            logger.error(f"Error indexing from service {service_name}: {e}")
            return {
                "status": "error",
                "service": service_name,
                "entity_type": entity_type,
                "error": str(e)
            }
    
    def _transform_to_unified_format(
        self,
        item: Dict[str, Any],
        entity_type: str,
        service_name: str
    ) -> Dict[str, Any]:
        """Transform service-specific data to unified format"""
        # Common mappings
        doc = {
            "entity_id": item.get("id", item.get("_id", item.get("uuid"))),
            "entity_type": entity_type,
            "service_name": service_name,
            "tenant_id": item.get("tenant_id", "default"),
            "created_at": item.get("created_at", item.get("created", datetime.utcnow().isoformat())),
            "updated_at": item.get("updated_at", item.get("modified", datetime.utcnow().isoformat())),
            "visibility": item.get("visibility", "private"),
            "owner": item.get("owner", item.get("created_by"))
        }
        
        # Service-specific mappings
        if service_name == "digital-asset-service":
            doc.update({
                "title": item.get("name", item.get("filename")),
                "description": item.get("description", ""),
                "content": item.get("extracted_text", ""),
                "tags": item.get("tags", []),
                "categories": [item.get("asset_type", "unknown")],
                "metadata": {
                    "file_type": item.get("file_type"),
                    "file_size": item.get("file_size"),
                    "mime_type": item.get("mime_type")
                }
            })
        
        elif service_name == "auth-service":
            if entity_type == "user":
                doc.update({
                    "title": item.get("username"),
                    "description": f"{item.get('first_name', '')} {item.get('last_name', '')}".strip(),
                    "content": item.get("bio", ""),
                    "tags": item.get("roles", []),
                    "categories": ["user"]
                })
            elif entity_type == "role":
                doc.update({
                    "title": item.get("name"),
                    "description": item.get("description", ""),
                    "tags": item.get("permissions", []),
                    "categories": ["role"]
                })
        
        elif service_name == "marketplace-service":
            doc.update({
                "title": item.get("title", item.get("name")),
                "description": item.get("description", ""),
                "content": item.get("detailed_description", ""),
                "tags": item.get("tags", []),
                "categories": [item.get("category", "uncategorized")],
                "metadata": {
                    "price": item.get("price"),
                    "currency": item.get("currency"),
                    "status": item.get("status")
                }
            })
        
        # Extract relationships
        doc["related_entities"] = self._extract_relationships(item, entity_type)
        
        return doc
    
    def _extract_relationships(
        self,
        item: Dict[str, Any],
        entity_type: str
    ) -> List[Dict[str, str]]:
        """Extract entity relationships"""
        relationships = []
        
        # Common relationship patterns
        if "created_by" in item:
            relationships.append({
                "entity_id": item["created_by"],
                "entity_type": "user",
                "relationship": "created_by"
            })
        
        if "owner" in item and item["owner"] != item.get("created_by"):
            relationships.append({
                "entity_id": item["owner"],
                "entity_type": "user",
                "relationship": "owned_by"
            })
        
        # Entity-specific relationships
        if entity_type == "digital_asset" and "project_id" in item:
            relationships.append({
                "entity_id": item["project_id"],
                "entity_type": "project",
                "relationship": "belongs_to"
            })
        
        if entity_type == "order" and "listing_id" in item:
            relationships.append({
                "entity_id": item["listing_id"],
                "entity_type": "listing",
                "relationship": "order_for"
            })
        
        return relationships
    
    async def _periodic_sync(self):
        """Periodically sync data from all services"""
        while True:
            try:
                await asyncio.sleep(300)  # 5 minutes
                
                # Sync each service
                for service_name, service_info in self.service_registry.items():
                    for entity_type in service_info["entity_types"]:
                        await self.index_from_service(service_name, entity_type)
                
            except Exception as e:
                logger.error(f"Error in periodic sync: {e}")
    
    async def _health_monitor(self):
        """Monitor health of all services"""
        while True:
            try:
                await asyncio.sleep(30)  # 30 seconds
                
                for service_name in self.service_registry:
                    await self._is_service_healthy(service_name)
                
            except Exception as e:
                logger.error(f"Error in health monitor: {e}")
    
    async def _is_service_healthy(self, service_name: str) -> bool:
        """Check if a service is healthy"""
        # Check cache
        cached = self.service_health_cache.get(service_name)
        if cached and (datetime.utcnow() - cached["timestamp"]).seconds < self.health_cache_ttl:
            return cached["healthy"]
        
        try:
            service_info = self.service_registry[service_name]
            response = await self.http_client.get(
                f"{service_info['url']}/health",
                timeout=5.0
            )
            
            healthy = response.status_code == 200
            
            # Update cache
            self.service_health_cache[service_name] = {
                "healthy": healthy,
                "timestamp": datetime.utcnow()
            }
            
            return healthy
            
        except Exception:
            # Update cache
            self.service_health_cache[service_name] = {
                "healthy": False,
                "timestamp": datetime.utcnow()
            }
            return False
    
    def _build_entity_url(self, service_url: str, entity_type: str, entity_id: str) -> str:
        """Build URL to access entity in its service"""
        # Common patterns
        if entity_type in ["user", "role", "permission"]:
            return f"{service_url}/api/v1/{entity_type}s/{entity_id}"
        elif entity_type == "digital_asset":
            return f"{service_url}/api/v1/assets/{entity_id}"
        elif entity_type == "listing":
            return f"{service_url}/api/v1/listings/{entity_id}"
        else:
            return f"{service_url}/api/v1/{entity_type}/{entity_id}"
    
    def _format_aggregation(self, agg: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Format ES aggregation buckets"""
        return [
            {"key": bucket["key"], "count": bucket["doc_count"]}
            for bucket in agg.get("buckets", [])
        ]
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.http_client.aclose() 