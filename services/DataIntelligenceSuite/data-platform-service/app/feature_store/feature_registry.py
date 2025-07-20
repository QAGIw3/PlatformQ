"""
Feature Registry

Manages feature metadata and discovery
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import asyncio

from elasticsearch import AsyncElasticsearch
from platformq_shared.cache.ignite_client import IgniteClient

from .models import FeatureGroup, Feature, FeatureView, FeatureSet

logger = logging.getLogger(__name__)


class FeatureRegistry:
    """Registry for feature metadata"""
    
    def __init__(self,
                 elasticsearch_host: str = "elasticsearch:9200",
                 cache_ttl: int = 3600):
        self.es_client = AsyncElasticsearch([elasticsearch_host])
        self.cache = IgniteClient()
        self.cache_ttl = cache_ttl
        
        # Elasticsearch indices
        self.feature_group_index = "feature_registry_groups"
        self.feature_index = "feature_registry_features"
        self.feature_view_index = "feature_registry_views"
        
    async def initialize(self):
        """Initialize registry indices"""
        # Create indices if they don't exist
        indices = [
            (self.feature_group_index, self._get_feature_group_mapping()),
            (self.feature_index, self._get_feature_mapping()),
            (self.feature_view_index, self._get_feature_view_mapping())
        ]
        
        for index_name, mapping in indices:
            if not await self.es_client.indices.exists(index=index_name):
                await self.es_client.indices.create(
                    index=index_name,
                    body={"mappings": mapping}
                )
                
    async def register_feature_group(self, feature_group: FeatureGroup) -> str:
        """Register a new feature group"""
        try:
            # Generate ID
            group_id = f"fg_{feature_group.name}_{datetime.utcnow().timestamp()}"
            
            # Index feature group
            await self.es_client.index(
                index=self.feature_group_index,
                id=group_id,
                body=feature_group.dict()
            )
            
            # Index individual features
            for feature in feature_group.features:
                await self._register_feature(feature, feature_group.name, group_id)
                
            # Cache for quick access
            cache_key = f"feature_group:{feature_group.name}"
            await self.cache.put(cache_key, feature_group.dict(), ttl=self.cache_ttl)
            
            logger.info(f"Registered feature group: {feature_group.name}")
            
            return group_id
            
        except Exception as e:
            logger.error(f"Failed to register feature group: {e}")
            raise
            
    async def get_feature_group(self, name: str) -> Optional[FeatureGroup]:
        """Get feature group by name"""
        try:
            # Check cache first
            cache_key = f"feature_group:{name}"
            cached = await self.cache.get(cache_key)
            
            if cached:
                return FeatureGroup(**cached)
                
            # Query Elasticsearch
            result = await self.es_client.search(
                index=self.feature_group_index,
                body={
                    "query": {
                        "term": {"name.keyword": name}
                    },
                    "size": 1
                }
            )
            
            if result["hits"]["total"]["value"] > 0:
                group_data = result["hits"]["hits"][0]["_source"]
                feature_group = FeatureGroup(**group_data)
                
                # Update cache
                await self.cache.put(cache_key, group_data, ttl=self.cache_ttl)
                
                return feature_group
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to get feature group: {e}")
            raise
            
    async def search_features(self,
                            query: str,
                            tags: Optional[List[str]] = None,
                            owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search for features"""
        try:
            # Build query
            must_clauses = []
            
            if query:
                must_clauses.append({
                    "multi_match": {
                        "query": query,
                        "fields": ["name^2", "description", "tags"]
                    }
                })
                
            if tags:
                must_clauses.append({
                    "terms": {"tags.keyword": tags}
                })
                
            if owner:
                must_clauses.append({
                    "term": {"owner.keyword": owner}
                })
                
            # Execute search
            result = await self.es_client.search(
                index=self.feature_index,
                body={
                    "query": {
                        "bool": {
                            "must": must_clauses
                        }
                    },
                    "size": 100
                }
            )
            
            features = []
            for hit in result["hits"]["hits"]:
                feature_data = hit["_source"]
                feature_data["_score"] = hit["_score"]
                features.append(feature_data)
                
            return features
            
        except Exception as e:
            logger.error(f"Failed to search features: {e}")
            raise
            
    async def register_feature_view(self, feature_view: FeatureView) -> str:
        """Register a feature view"""
        try:
            # Generate ID
            view_id = f"fv_{feature_view.name}_{datetime.utcnow().timestamp()}"
            
            # Index view
            await self.es_client.index(
                index=self.feature_view_index,
                id=view_id,
                body=feature_view.dict()
            )
            
            # Cache
            cache_key = f"feature_view:{feature_view.name}"
            await self.cache.put(cache_key, feature_view.dict(), ttl=self.cache_ttl)
            
            logger.info(f"Registered feature view: {feature_view.name}")
            
            return view_id
            
        except Exception as e:
            logger.error(f"Failed to register feature view: {e}")
            raise
            
    async def get_feature_lineage(self, feature_name: str, feature_group: str) -> Dict[str, Any]:
        """Get feature lineage information"""
        try:
            # Query for feature
            result = await self.es_client.search(
                index=self.feature_index,
                body={
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"name.keyword": feature_name}},
                                {"term": {"feature_group.keyword": feature_group}}
                            ]
                        }
                    },
                    "size": 1
                }
            )
            
            if result["hits"]["total"]["value"] == 0:
                return {}
                
            feature_data = result["hits"]["hits"][0]["_source"]
            
            lineage = {
                "feature": f"{feature_group}.{feature_name}",
                "source_columns": feature_data.get("source_columns", []),
                "derivation": feature_data.get("derivation"),
                "transform": feature_data.get("transform"),
                "dependencies": []
            }
            
            # Get dependencies
            if feature_data.get("source_columns"):
                for col in feature_data["source_columns"]:
                    # Query for features derived from this column
                    dep_result = await self.es_client.search(
                        index=self.feature_index,
                        body={
                            "query": {
                                "term": {"source_columns.keyword": col}
                            }
                        }
                    )
                    
                    for hit in dep_result["hits"]["hits"]:
                        dep = hit["_source"]
                        lineage["dependencies"].append({
                            "feature": f"{dep['feature_group']}.{dep['name']}",
                            "type": "derived_from"
                        })
                        
            return lineage
            
        except Exception as e:
            logger.error(f"Failed to get feature lineage: {e}")
            raise
            
    async def list_feature_groups(self,
                                tags: Optional[List[str]] = None,
                                owner: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all feature groups"""
        try:
            # Build query
            must_clauses = []
            
            if tags:
                must_clauses.append({
                    "terms": {"tags.keyword": tags}
                })
                
            if owner:
                must_clauses.append({
                    "term": {"owner.keyword": owner}
                })
                
            # Execute search
            query = {"match_all": {}} if not must_clauses else {"bool": {"must": must_clauses}}
            
            result = await self.es_client.search(
                index=self.feature_group_index,
                body={
                    "query": query,
                    "size": 1000,
                    "sort": [{"created_at": {"order": "desc"}}]
                }
            )
            
            groups = []
            for hit in result["hits"]["hits"]:
                group_data = hit["_source"]
                group_data["_id"] = hit["_id"]
                groups.append(group_data)
                
            return groups
            
        except Exception as e:
            logger.error(f"Failed to list feature groups: {e}")
            raise
            
    async def get_feature_statistics(self, feature_group: str) -> Dict[str, Any]:
        """Get statistics for a feature group"""
        try:
            # Get feature group
            group = await self.get_feature_group(feature_group)
            if not group:
                return {}
                
            # Query for all features in group
            result = await self.es_client.search(
                index=self.feature_index,
                body={
                    "query": {
                        "term": {"feature_group.keyword": feature_group}
                    },
                    "aggs": {
                        "by_type": {
                            "terms": {"field": "type.keyword"}
                        },
                        "by_transform": {
                            "terms": {"field": "transform.keyword"}
                        }
                    }
                }
            )
            
            stats = {
                "feature_count": result["hits"]["total"]["value"],
                "types": {},
                "transforms": {}
            }
            
            # Process aggregations
            for bucket in result["aggregations"]["by_type"]["buckets"]:
                stats["types"][bucket["key"]] = bucket["doc_count"]
                
            for bucket in result["aggregations"]["by_transform"]["buckets"]:
                stats["transforms"][bucket["key"]] = bucket["doc_count"]
                
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get feature statistics: {e}")
            raise
            
    # Helper methods
    
    async def _register_feature(self, feature: Feature, feature_group: str, group_id: str):
        """Register individual feature"""
        feature_data = feature.dict()
        feature_data["feature_group"] = feature_group
        feature_data["feature_group_id"] = group_id
        
        feature_id = f"f_{feature_group}_{feature.name}"
        
        await self.es_client.index(
            index=self.feature_index,
            id=feature_id,
            body=feature_data
        )
        
    def _get_feature_group_mapping(self) -> Dict[str, Any]:
        """Get Elasticsearch mapping for feature groups"""
        return {
            "properties": {
                "name": {"type": "keyword"},
                "description": {"type": "text"},
                "features": {"type": "nested"},
                "primary_keys": {"type": "keyword"},
                "event_time_column": {"type": "keyword"},
                "source_type": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "owner": {"type": "keyword"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"}
            }
        }
        
    def _get_feature_mapping(self) -> Dict[str, Any]:
        """Get Elasticsearch mapping for features"""
        return {
            "properties": {
                "name": {"type": "keyword"},
                "description": {"type": "text"},
                "type": {"type": "keyword"},
                "transform": {"type": "keyword"},
                "tags": {"type": "keyword"},
                "owner": {"type": "keyword"},
                "feature_group": {"type": "keyword"},
                "feature_group_id": {"type": "keyword"},
                "source_columns": {"type": "keyword"},
                "derivation": {"type": "text"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"}
            }
        }
        
    def _get_feature_view_mapping(self) -> Dict[str, Any]:
        """Get Elasticsearch mapping for feature views"""
        return {
            "properties": {
                "name": {"type": "keyword"},
                "description": {"type": "text"},
                "feature_set": {"type": "keyword"},
                "query": {"type": "text"},
                "materialization_enabled": {"type": "boolean"},
                "online_enabled": {"type": "boolean"},
                "created_at": {"type": "date"},
                "updated_at": {"type": "date"},
                "last_refreshed": {"type": "date"}
            }
        } 