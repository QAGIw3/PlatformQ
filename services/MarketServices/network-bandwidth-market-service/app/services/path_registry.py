"""
Network Path Registry Service
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import uuid
from pyignite import Client
from elasticsearch import Elasticsearch
import asyncio

from ..models import (
    NetworkPath, NetworkNode, PathStatus, PathRegistrationRequest,
    PathSearchRequest, CongestionLevel
)
from ..config import settings


logger = logging.getLogger(__name__)


class PathRegistryService:
    """Service for managing network path registration and discovery"""
    
    def __init__(self):
        self.ignite_client = None
        self.es_client = None
        self.path_cache = None
        
    async def initialize(self):
        """Initialize connections"""
        try:
            # Connect to Ignite
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.path_cache = self.ignite_client.get_or_create_cache(
                settings.IGNITE_CACHE_PATH_STATE
            )
            
            # Connect to Elasticsearch
            self.es_client = Elasticsearch([settings.ELASTICSEARCH_URL])
            
            # Create Elasticsearch index if not exists
            if not self.es_client.indices.exists(index=settings.ELASTICSEARCH_INDEX_PATHS):
                await self._create_elasticsearch_index()
                
            logger.info("Path Registry Service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Path Registry Service: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.es_client:
            self.es_client.close()
    
    async def _create_elasticsearch_index(self):
        """Create Elasticsearch index for paths"""
        mapping = {
            "mappings": {
                "properties": {
                    "path_id": {"type": "keyword"},
                    "source.node_id": {"type": "keyword"},
                    "source.name": {"type": "text"},
                    "source.location": {"type": "text"},
                    "source.provider": {"type": "keyword"},
                    "destination.node_id": {"type": "keyword"},
                    "destination.name": {"type": "text"},
                    "destination.location": {"type": "text"},
                    "destination.provider": {"type": "keyword"},
                    "hops": {"type": "nested"},
                    "total_distance_km": {"type": "float"},
                    "latency_ms": {"type": "float"},
                    "max_bandwidth_mbps": {"type": "integer"},
                    "reliability_score": {"type": "float"},
                    "status": {"type": "keyword"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "geo_source": {"type": "geo_point"},
                    "geo_destination": {"type": "geo_point"}
                }
            }
        }
        self.es_client.indices.create(
            index=settings.ELASTICSEARCH_INDEX_PATHS,
            body=mapping
        )
    
    async def register_path(
        self,
        request: PathRegistrationRequest
    ) -> NetworkPath:
        """Register a new network path"""
        try:
            path_id = f"path_{uuid.uuid4().hex[:8]}"
            
            # Calculate total distance (simplified)
            total_distance = self._calculate_path_distance(
                request.source,
                request.destination,
                request.hops
            )
            
            # Create path object
            path = NetworkPath(
                path_id=path_id,
                source=request.source,
                destination=request.destination,
                hops=request.hops,
                total_distance_km=total_distance,
                latency_ms=request.base_latency_ms,
                available_bandwidth_mbps=request.max_bandwidth_mbps,
                max_bandwidth_mbps=request.max_bandwidth_mbps,
                reliability_score=0.99,  # Initial high score
                status=PathStatus.ACTIVE,
                provider_path_id=request.provider_path_id,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            # Store in Ignite cache
            self.path_cache.put(path_id, path.dict())
            
            # Index in Elasticsearch
            es_doc = path.dict()
            if request.source.latitude and request.source.longitude:
                es_doc["geo_source"] = {
                    "lat": request.source.latitude,
                    "lon": request.source.longitude
                }
            if request.destination.latitude and request.destination.longitude:
                es_doc["geo_destination"] = {
                    "lat": request.destination.latitude,
                    "lon": request.destination.longitude
                }
            
            self.es_client.index(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                id=path_id,
                body=es_doc
            )
            
            logger.info(f"Registered network path: {path_id}")
            return path
            
        except Exception as e:
            logger.error(f"Failed to register path: {e}")
            raise
    
    async def get_path(self, path_id: str) -> Optional[NetworkPath]:
        """Get path by ID"""
        try:
            # Check cache first
            path_data = self.path_cache.get(path_id)
            if path_data:
                return NetworkPath(**path_data)
            
            # Fallback to Elasticsearch
            result = self.es_client.get(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                id=path_id,
                ignore=404
            )
            
            if result.get("found"):
                path_data = result["_source"]
                # Update cache
                self.path_cache.put(path_id, path_data)
                return NetworkPath(**path_data)
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get path {path_id}: {e}")
            return None
    
    async def update_path_status(
        self,
        path_id: str,
        status: PathStatus,
        available_bandwidth: Optional[int] = None
    ) -> bool:
        """Update path status and availability"""
        try:
            path = await self.get_path(path_id)
            if not path:
                return False
            
            path.status = status
            path.updated_at = datetime.utcnow()
            
            if available_bandwidth is not None:
                path.available_bandwidth_mbps = available_bandwidth
            
            # Update in cache
            self.path_cache.put(path_id, path.dict())
            
            # Update in Elasticsearch
            self.es_client.update(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                id=path_id,
                body={
                    "doc": {
                        "status": status.value,
                        "available_bandwidth_mbps": path.available_bandwidth_mbps,
                        "updated_at": path.updated_at.isoformat()
                    }
                }
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update path status: {e}")
            return False
    
    async def search_paths(
        self,
        request: PathSearchRequest
    ) -> List[NetworkPath]:
        """Search for network paths based on criteria"""
        try:
            # Build Elasticsearch query
            must_clauses = []
            
            if request.source:
                must_clauses.append({
                    "term": {"source.node_id": request.source}
                })
            
            if request.destination:
                must_clauses.append({
                    "term": {"destination.node_id": request.destination}
                })
            
            if request.min_bandwidth_mbps:
                must_clauses.append({
                    "range": {
                        "available_bandwidth_mbps": {
                            "gte": request.min_bandwidth_mbps
                        }
                    }
                })
            
            if request.max_latency_ms:
                must_clauses.append({
                    "range": {
                        "latency_ms": {
                            "lte": request.max_latency_ms
                        }
                    }
                })
            
            if request.max_hops:
                must_clauses.append({
                    "script": {
                        "script": {
                            "source": "doc['hops'].size() <= params.max_hops",
                            "params": {"max_hops": request.max_hops}
                        }
                    }
                })
            
            if request.providers:
                must_clauses.append({
                    "terms": {"source.provider": request.providers}
                })
            
            if request.status:
                must_clauses.append({
                    "terms": {"status": [s.value for s in request.status]}
                })
            else:
                # Default to active paths only
                must_clauses.append({
                    "term": {"status": PathStatus.ACTIVE.value}
                })
            
            query = {
                "bool": {
                    "must": must_clauses
                }
            }
            
            # Execute search
            results = self.es_client.search(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                body={
                    "query": query,
                    "size": settings.MAX_PATHS_PER_REQUEST,
                    "sort": [
                        {"reliability_score": {"order": "desc"}},
                        {"latency_ms": {"order": "asc"}}
                    ]
                }
            )
            
            paths = []
            for hit in results["hits"]["hits"]:
                path_data = hit["_source"]
                paths.append(NetworkPath(**path_data))
            
            return paths
            
        except Exception as e:
            logger.error(f"Failed to search paths: {e}")
            return []
    
    async def find_alternative_paths(
        self,
        source: str,
        destination: str,
        exclude_path_ids: List[str]
    ) -> List[NetworkPath]:
        """Find alternative paths excluding specific paths"""
        try:
            query = {
                "bool": {
                    "must": [
                        {"term": {"source.node_id": source}},
                        {"term": {"destination.node_id": destination}},
                        {"term": {"status": PathStatus.ACTIVE.value}}
                    ],
                    "must_not": [
                        {"terms": {"path_id": exclude_path_ids}}
                    ]
                }
            }
            
            results = self.es_client.search(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                body={
                    "query": query,
                    "size": 10,
                    "sort": [
                        {"available_bandwidth_mbps": {"order": "desc"}},
                        {"latency_ms": {"order": "asc"}}
                    ]
                }
            )
            
            paths = []
            for hit in results["hits"]["hits"]:
                path_data = hit["_source"]
                paths.append(NetworkPath(**path_data))
            
            return paths
            
        except Exception as e:
            logger.error(f"Failed to find alternative paths: {e}")
            return []
    
    async def update_path_reliability(
        self,
        path_id: str,
        success: bool
    ):
        """Update path reliability score based on usage"""
        try:
            path = await self.get_path(path_id)
            if not path:
                return
            
            # Simple exponential moving average
            alpha = 0.1
            if success:
                path.reliability_score = min(
                    1.0,
                    path.reliability_score * (1 - alpha) + 1.0 * alpha
                )
            else:
                path.reliability_score = max(
                    0.0,
                    path.reliability_score * (1 - alpha) + 0.0 * alpha
                )
            
            # Update status based on reliability
            if path.reliability_score < 0.7:
                path.status = PathStatus.DEGRADED
            elif path.reliability_score < 0.5:
                path.status = PathStatus.OFFLINE
            
            # Update in cache
            self.path_cache.put(path_id, path.dict())
            
            # Update in Elasticsearch
            self.es_client.update(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                id=path_id,
                body={
                    "doc": {
                        "reliability_score": path.reliability_score,
                        "status": path.status.value,
                        "updated_at": datetime.utcnow().isoformat()
                    }
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to update path reliability: {e}")
    
    def _calculate_path_distance(
        self,
        source: NetworkNode,
        destination: NetworkNode,
        hops: List[NetworkNode]
    ) -> float:
        """Calculate approximate path distance"""
        # Simplified calculation - in production would use actual geo calculations
        total_distance = 0.0
        
        nodes = [source] + hops + [destination]
        for i in range(len(nodes) - 1):
            # Placeholder - would calculate actual distance
            total_distance += 100.0  # km
        
        return total_distance
    
    async def get_paths_by_status(
        self,
        status: PathStatus,
        limit: int = 100
    ) -> List[NetworkPath]:
        """Get paths by status"""
        try:
            results = self.es_client.search(
                index=settings.ELASTICSEARCH_INDEX_PATHS,
                body={
                    "query": {
                        "term": {"status": status.value}
                    },
                    "size": limit
                }
            )
            
            paths = []
            for hit in results["hits"]["hits"]:
                path_data = hit["_source"]
                paths.append(NetworkPath(**path_data))
            
            return paths
            
        except Exception as e:
            logger.error(f"Failed to get paths by status: {e}")
            return [] 