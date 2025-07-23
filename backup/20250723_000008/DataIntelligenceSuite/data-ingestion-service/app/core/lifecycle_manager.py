"""
Data Lifecycle Manager

Manages data tiering, retention policies, and cost optimization
across hot, warm, and cold storage tiers.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import json

from minio import Minio
from minio.error import S3Error
from pyignite import Client as IgniteClient
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from elasticsearch import AsyncElasticsearch

from app.core.config import Settings

logger = logging.getLogger(__name__)


class StorageTier(str, Enum):
    """Storage tier types"""
    HOT = "hot"      # Frequently accessed data (Ignite cache)
    WARM = "warm"    # Occasionally accessed data (Cassandra)
    COLD = "cold"    # Archived data (MinIO/S3)


class TieringPolicy:
    """Data tiering policy"""
    def __init__(
        self,
        hot_duration_days: int = 7,
        warm_duration_days: int = 30,
        cold_duration_days: int = 365,
        delete_after_days: Optional[int] = None
    ):
        self.hot_duration_days = hot_duration_days
        self.warm_duration_days = warm_duration_days
        self.cold_duration_days = cold_duration_days
        self.delete_after_days = delete_after_days


class DataLifecycleManager:
    """Manages data lifecycle across storage tiers"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        
        # Storage clients
        self.minio_client = None
        self.ignite_client = None
        self.cassandra_cluster = None
        self.cassandra_session = None
        self.es_client = None
        
        # Default policies by data type
        self.default_policies = {
            "events": TieringPolicy(7, 30, 365),
            "metrics": TieringPolicy(1, 7, 90),
            "logs": TieringPolicy(3, 14, 180),
            "assets": TieringPolicy(30, 90, None),  # Never delete assets
            "analytics": TieringPolicy(14, 60, 730)
        }
        
        # Cost per GB per month for each tier
        self.storage_costs = {
            StorageTier.HOT: 0.50,   # Memory storage
            StorageTier.WARM: 0.10,  # SSD storage
            StorageTier.COLD: 0.01   # Object storage
        }
        
    async def initialize(self):
        """Initialize storage clients"""
        try:
            # Initialize MinIO
            self.minio_client = Minio(
                self.settings.minio_endpoint,
                access_key=self.settings.minio_access_key,
                secret_key=self.settings.minio_secret_key,
                secure=self.settings.minio_secure
            )
            
            # Initialize Ignite
            self.ignite_client = IgniteClient()
            self.ignite_client.connect(self.settings.ignite_host, self.settings.ignite_port)
            
            # Initialize Cassandra
            auth_provider = PlainTextAuthProvider(
                username=self.settings.cassandra_username,
                password=self.settings.cassandra_password
            ) if self.settings.cassandra_username else None
            
            self.cassandra_cluster = Cluster(
                self.settings.cassandra_hosts,
                port=self.settings.cassandra_port,
                auth_provider=auth_provider
            )
            self.cassandra_session = self.cassandra_cluster.connect()
            
            # Initialize Elasticsearch
            self.es_client = AsyncElasticsearch(
                hosts=[f"http://{self.settings.elasticsearch_hosts[0]}"],
                basic_auth=(self.settings.elasticsearch_username, self.settings.elasticsearch_password)
                if self.settings.elasticsearch_username else None
            )
            
            logger.info("Data lifecycle manager initialized")
            
        except Exception as e:
            logger.error(f"Error initializing lifecycle manager: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup storage connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.cassandra_cluster:
            self.cassandra_cluster.shutdown()
        if self.es_client:
            await self.es_client.close()
    
    async def apply_tiering_policy(
        self,
        data_type: str,
        dataset_name: str,
        custom_policy: Optional[TieringPolicy] = None
    ) -> Dict[str, Any]:
        """Apply tiering policy to a dataset"""
        try:
            policy = custom_policy or self.default_policies.get(data_type, TieringPolicy())
            
            # Get current data distribution
            distribution = await self._get_data_distribution(dataset_name)
            
            # Calculate transitions needed
            transitions = []
            current_time = datetime.utcnow()
            
            # Check hot tier data
            for item in distribution.get(StorageTier.HOT, []):
                age_days = (current_time - item["timestamp"]).days
                
                if age_days > policy.hot_duration_days:
                    transitions.append({
                        "item": item,
                        "from": StorageTier.HOT,
                        "to": StorageTier.WARM
                    })
            
            # Check warm tier data
            for item in distribution.get(StorageTier.WARM, []):
                age_days = (current_time - item["timestamp"]).days
                
                if age_days > policy.warm_duration_days:
                    transitions.append({
                        "item": item,
                        "from": StorageTier.WARM,
                        "to": StorageTier.COLD
                    })
            
            # Check cold tier data for deletion
            if policy.delete_after_days:
                for item in distribution.get(StorageTier.COLD, []):
                    age_days = (current_time - item["timestamp"]).days
                    
                    if age_days > policy.delete_after_days:
                        transitions.append({
                            "item": item,
                            "from": StorageTier.COLD,
                            "to": None  # Delete
                        })
            
            # Execute transitions
            results = await self._execute_transitions(transitions)
            
            # Calculate cost savings
            cost_savings = self._calculate_cost_savings(transitions)
            
            return {
                "dataset": dataset_name,
                "transitions_count": len(transitions),
                "transitions": results,
                "cost_savings_monthly": cost_savings,
                "policy_applied": {
                    "hot_days": policy.hot_duration_days,
                    "warm_days": policy.warm_duration_days,
                    "cold_days": policy.cold_duration_days,
                    "delete_after": policy.delete_after_days
                }
            }
            
        except Exception as e:
            logger.error(f"Error applying tiering policy: {e}")
            raise
    
    async def _get_data_distribution(self, dataset_name: str) -> Dict[StorageTier, List[Dict[str, Any]]]:
        """Get current data distribution across tiers"""
        distribution = {
            StorageTier.HOT: [],
            StorageTier.WARM: [],
            StorageTier.COLD: []
        }
        
        try:
            # Check hot tier (Ignite)
            cache = self.ignite_client.get_or_create_cache(f"hot_{dataset_name}")
            # Scan cache entries
            with cache.scan() as cursor:
                for key, value in cursor:
                    if isinstance(value, dict) and "timestamp" in value:
                        distribution[StorageTier.HOT].append({
                            "key": key,
                            "timestamp": value["timestamp"],
                            "size_bytes": value.get("size_bytes", 0)
                        })
            
            # Check warm tier (Cassandra)
            query = f"SELECT key, timestamp, size_bytes FROM {self.settings.cassandra_keyspace}.{dataset_name}_warm"
            rows = self.cassandra_session.execute(query)
            for row in rows:
                distribution[StorageTier.WARM].append({
                    "key": row.key,
                    "timestamp": row.timestamp,
                    "size_bytes": row.size_bytes
                })
            
            # Check cold tier (MinIO)
            objects = self.minio_client.list_objects(
                f"cold-{dataset_name}",
                recursive=True
            )
            for obj in objects:
                distribution[StorageTier.COLD].append({
                    "key": obj.object_name,
                    "timestamp": obj.last_modified.replace(tzinfo=None),
                    "size_bytes": obj.size
                })
            
        except Exception as e:
            logger.error(f"Error getting data distribution: {e}")
            
        return distribution
    
    async def _execute_transitions(self, transitions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Execute data transitions between tiers"""
        results = []
        
        for transition in transitions:
            try:
                item = transition["item"]
                from_tier = transition["from"]
                to_tier = transition["to"]
                
                if to_tier is None:
                    # Delete data
                    await self._delete_from_tier(item, from_tier)
                    results.append({
                        "key": item["key"],
                        "action": "deleted",
                        "from": from_tier,
                        "status": "success"
                    })
                else:
                    # Move data
                    data = await self._read_from_tier(item, from_tier)
                    await self._write_to_tier(item, data, to_tier)
                    await self._delete_from_tier(item, from_tier)
                    
                    results.append({
                        "key": item["key"],
                        "action": "moved",
                        "from": from_tier,
                        "to": to_tier,
                        "status": "success"
                    })
                    
            except Exception as e:
                logger.error(f"Error transitioning {item['key']}: {e}")
                results.append({
                    "key": item["key"],
                    "action": "failed",
                    "error": str(e)
                })
        
        return results
    
    async def _read_from_tier(self, item: Dict[str, Any], tier: StorageTier) -> Any:
        """Read data from a specific tier"""
        if tier == StorageTier.HOT:
            cache = self.ignite_client.get_cache(f"hot_{item.get('dataset', 'default')}")
            return cache.get(item["key"])
            
        elif tier == StorageTier.WARM:
            query = f"SELECT data FROM {self.settings.cassandra_keyspace}.warm_storage WHERE key = ?"
            row = self.cassandra_session.execute(query, [item["key"]]).one()
            return row.data if row else None
            
        elif tier == StorageTier.COLD:
            response = self.minio_client.get_object(
                f"cold-{item.get('dataset', 'default')}",
                item["key"]
            )
            data = response.read()
            response.close()
            response.release_conn()
            return json.loads(data.decode('utf-8'))
    
    async def _write_to_tier(self, item: Dict[str, Any], data: Any, tier: StorageTier):
        """Write data to a specific tier"""
        if tier == StorageTier.HOT:
            cache = self.ignite_client.get_or_create_cache(f"hot_{item.get('dataset', 'default')}")
            cache.put(item["key"], data)
            
        elif tier == StorageTier.WARM:
            query = """
                INSERT INTO {}.warm_storage (key, data, timestamp, size_bytes)
                VALUES (?, ?, ?, ?)
            """.format(self.settings.cassandra_keyspace)
            
            self.cassandra_session.execute(query, [
                item["key"],
                json.dumps(data),
                datetime.utcnow(),
                item.get("size_bytes", 0)
            ])
            
        elif tier == StorageTier.COLD:
            import io
            data_bytes = json.dumps(data).encode('utf-8')
            self.minio_client.put_object(
                f"cold-{item.get('dataset', 'default')}",
                item["key"],
                io.BytesIO(data_bytes),
                length=len(data_bytes)
            )
    
    async def _delete_from_tier(self, item: Dict[str, Any], tier: StorageTier):
        """Delete data from a specific tier"""
        if tier == StorageTier.HOT:
            cache = self.ignite_client.get_cache(f"hot_{item.get('dataset', 'default')}")
            cache.remove(item["key"])
            
        elif tier == StorageTier.WARM:
            query = f"DELETE FROM {self.settings.cassandra_keyspace}.warm_storage WHERE key = ?"
            self.cassandra_session.execute(query, [item["key"]])
            
        elif tier == StorageTier.COLD:
            self.minio_client.remove_object(
                f"cold-{item.get('dataset', 'default')}",
                item["key"]
            )
    
    def _calculate_cost_savings(self, transitions: List[Dict[str, Any]]) -> float:
        """Calculate monthly cost savings from transitions"""
        savings = 0.0
        
        for transition in transitions:
            item = transition["item"]
            size_gb = item.get("size_bytes", 0) / (1024 ** 3)
            
            from_tier = transition["from"]
            to_tier = transition["to"]
            
            if to_tier is None:
                # Deletion saves the cost of current tier
                savings += size_gb * self.storage_costs[from_tier]
            else:
                # Moving saves the difference in cost
                from_cost = self.storage_costs[from_tier]
                to_cost = self.storage_costs[to_tier]
                savings += size_gb * (from_cost - to_cost)
        
        return round(savings, 2)
    
    async def generate_cost_report(self, dataset_name: Optional[str] = None) -> Dict[str, Any]:
        """Generate storage cost report"""
        try:
            distribution = await self._get_data_distribution(dataset_name or "all")
            
            costs = {}
            total_size_gb = 0
            total_cost = 0
            
            for tier, items in distribution.items():
                tier_size_gb = sum(item.get("size_bytes", 0) for item in items) / (1024 ** 3)
                tier_cost = tier_size_gb * self.storage_costs[tier]
                
                costs[tier] = {
                    "size_gb": round(tier_size_gb, 2),
                    "items_count": len(items),
                    "cost_per_gb": self.storage_costs[tier],
                    "total_cost": round(tier_cost, 2)
                }
                
                total_size_gb += tier_size_gb
                total_cost += tier_cost
            
            # Calculate optimal distribution
            optimal_distribution = self._calculate_optimal_distribution(distribution)
            potential_savings = total_cost - optimal_distribution["total_cost"]
            
            return {
                "dataset": dataset_name or "all",
                "current_costs": costs,
                "total_size_gb": round(total_size_gb, 2),
                "total_monthly_cost": round(total_cost, 2),
                "optimal_distribution": optimal_distribution,
                "potential_monthly_savings": round(potential_savings, 2),
                "recommendations": self._generate_recommendations(distribution, optimal_distribution)
            }
            
        except Exception as e:
            logger.error(f"Error generating cost report: {e}")
            raise
    
    def _calculate_optimal_distribution(self, current_distribution: Dict[StorageTier, List]) -> Dict[str, Any]:
        """Calculate optimal data distribution for cost"""
        # Simple optimization: move data based on access patterns
        # In real implementation, this would analyze access logs
        
        optimal_costs = {}
        total_cost = 0
        
        for tier, items in current_distribution.items():
            # Simulate optimal distribution
            if tier == StorageTier.HOT:
                # Keep only very recent data hot
                recent_items = [i for i in items if (datetime.utcnow() - i["timestamp"]).days < 1]
                size_gb = sum(i.get("size_bytes", 0) for i in recent_items) / (1024 ** 3)
            elif tier == StorageTier.WARM:
                # Keep moderately accessed data warm
                size_gb = sum(i.get("size_bytes", 0) for i in items) / (1024 ** 3) * 0.3
            else:
                # Most data should be cold
                size_gb = sum(i.get("size_bytes", 0) for i in items) / (1024 ** 3) * 1.5
            
            cost = size_gb * self.storage_costs[tier]
            optimal_costs[tier] = {
                "size_gb": round(size_gb, 2),
                "cost": round(cost, 2)
            }
            total_cost += cost
        
        return {
            "distribution": optimal_costs,
            "total_cost": round(total_cost, 2)
        }
    
    def _generate_recommendations(self, current: Dict, optimal: Dict) -> List[str]:
        """Generate cost optimization recommendations"""
        recommendations = []
        
        # Analyze hot tier
        hot_items = current.get(StorageTier.HOT, [])
        if hot_items:
            old_hot_data = [i for i in hot_items if (datetime.utcnow() - i["timestamp"]).days > 7]
            if old_hot_data:
                recommendations.append(
                    f"Move {len(old_hot_data)} items from hot to warm tier (>7 days old)"
                )
        
        # Analyze warm tier
        warm_items = current.get(StorageTier.WARM, [])
        if warm_items:
            old_warm_data = [i for i in warm_items if (datetime.utcnow() - i["timestamp"]).days > 30]
            if old_warm_data:
                recommendations.append(
                    f"Move {len(old_warm_data)} items from warm to cold tier (>30 days old)"
                )
        
        # General recommendations
        recommendations.extend([
            "Enable automatic tiering policies for consistent cost optimization",
            "Consider compressing data before moving to cold storage",
            "Review and adjust retention policies based on compliance requirements"
        ])
        
        return recommendations 