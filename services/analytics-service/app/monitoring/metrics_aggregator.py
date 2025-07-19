"""
Metrics Aggregator Module

Aggregates metrics from multiple sources including Druid, Ignite, and streaming data
for unified analytics and monitoring.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np

from pyignite import Client as IgniteClient
import httpx

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates metrics from multiple data sources"""
    
    def __init__(self, ignite_config: Dict[str, Any], druid_config: Dict[str, str]):
        self.ignite_config = ignite_config
        self.druid_config = druid_config
        self.ignite_client = None
        self.druid_client = httpx.AsyncClient(base_url=druid_config['broker_url'])
        self.aggregation_cache = {}
        
    async def initialize(self):
        """Initialize connections"""
        # Connect to Ignite
        self.ignite_client = IgniteClient()
        self.ignite_client.connect(self.ignite_config['host'], self.ignite_config['port'])
        
        # Create aggregation caches
        self._create_caches()
        
    def _create_caches(self):
        """Create Ignite caches for aggregations"""
        # Metrics aggregation cache
        self.ignite_client.get_or_create_cache({
            'name': 'metrics_aggregations',
            'cache_mode': 'PARTITIONED',
            'atomicity_mode': 'ATOMIC',
            'backups': 1
        })
        
        # Rolling window cache
        self.ignite_client.get_or_create_cache({
            'name': 'rolling_windows',
            'cache_mode': 'REPLICATED',
            'atomicity_mode': 'ATOMIC'
        })
        
    async def aggregate_metrics(self, 
                               source: str,
                               metrics: List[str],
                               time_range: str = "1h",
                               group_by: Optional[List[str]] = None,
                               filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Aggregate metrics from specified source"""
        
        # Parse time range
        end_time = datetime.utcnow()
        start_time = self._parse_time_range(time_range, end_time)
        
        # Check cache first
        cache_key = self._generate_cache_key(source, metrics, time_range, group_by, filters)
        cached_result = self._get_cached_result(cache_key)
        if cached_result:
            return cached_result
            
        # Fetch and aggregate based on source
        if source == "druid":
            result = await self._aggregate_from_druid(
                metrics, start_time, end_time, group_by, filters
            )
        elif source == "ignite":
            result = await self._aggregate_from_ignite(
                metrics, start_time, end_time, group_by, filters
            )
        else:
            result = await self._aggregate_from_mixed(
                metrics, start_time, end_time, group_by, filters
            )
            
        # Cache result
        self._cache_result(cache_key, result)
        
        return result
        
    async def _aggregate_from_druid(self, 
                                   metrics: List[str],
                                   start_time: datetime,
                                   end_time: datetime,
                                   group_by: Optional[List[str]] = None,
                                   filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Aggregate metrics from Druid"""
        
        # Build Druid query
        query = {
            "queryType": "groupBy",
            "dataSource": "platform_metrics",
            "granularity": "minute",
            "dimensions": group_by or [],
            "aggregations": [
                {
                    "type": "doubleSum",
                    "name": f"{metric}_sum",
                    "fieldName": metric
                } for metric in metrics
            ] + [
                {
                    "type": "count",
                    "name": "count"
                }
            ],
            "intervals": [f"{start_time.isoformat()}/{end_time.isoformat()}"]
        }
        
        # Add filters if provided
        if filters:
            query["filter"] = self._build_druid_filter(filters)
            
        # Execute query
        response = await self.druid_client.post("/druid/v2", json=query)
        data = response.json()
        
        # Process results
        return self._process_druid_results(data, metrics)
        
    async def _aggregate_from_ignite(self,
                                    metrics: List[str],
                                    start_time: datetime,
                                    end_time: datetime,
                                    group_by: Optional[List[str]] = None,
                                    filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Aggregate metrics from Ignite cache"""
        
        cache = self.ignite_client.get_cache('realtime_metrics')
        
        # Build SQL query
        group_clause = f"GROUP BY {', '.join(group_by)}" if group_by else ""
        where_clauses = [f"timestamp >= '{start_time.isoformat()}'",
                        f"timestamp <= '{end_time.isoformat()}'"]
        
        if filters:
            for key, value in filters.items():
                where_clauses.append(f"{key} = '{value}'")
                
        where_clause = " AND ".join(where_clauses)
        
        select_metrics = []
        for metric in metrics:
            select_metrics.extend([
                f"SUM({metric}) as {metric}_sum",
                f"AVG({metric}) as {metric}_avg",
                f"MAX({metric}) as {metric}_max",
                f"MIN({metric}) as {metric}_min"
            ])
            
        query = f"""
        SELECT {', '.join(select_metrics)}, COUNT(*) as count
        FROM realtime_metrics
        WHERE {where_clause}
        {group_clause}
        """
        
        # Execute query
        results = []
        with cache.query_scan(query) as cursor:
            for row in cursor:
                results.append(row)
                
        return self._process_ignite_results(results, metrics)
        
    async def _aggregate_from_mixed(self,
                                   metrics: List[str],
                                   start_time: datetime,
                                   end_time: datetime,
                                   group_by: Optional[List[str]] = None,
                                   filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Aggregate metrics from both Druid and Ignite"""
        
        # Get recent data from Ignite (last hour)
        recent_cutoff = datetime.utcnow() - timedelta(hours=1)
        
        results = {}
        
        # Historical data from Druid
        if start_time < recent_cutoff:
            druid_results = await self._aggregate_from_druid(
                metrics, start_time, min(end_time, recent_cutoff), group_by, filters
            )
            results['historical'] = druid_results
            
        # Recent data from Ignite
        if end_time > recent_cutoff:
            ignite_results = await self._aggregate_from_ignite(
                metrics, max(start_time, recent_cutoff), end_time, group_by, filters
            )
            results['realtime'] = ignite_results
            
        # Merge results
        return self._merge_results(results)
        
    async def calculate_rolling_windows(self,
                                       metric: str,
                                       windows: List[int],
                                       source: str = "mixed") -> Dict[str, Any]:
        """Calculate rolling window statistics"""
        
        results = {}
        
        for window in windows:
            # Get data for window
            data = await self.aggregate_metrics(
                source=source,
                metrics=[metric],
                time_range=f"{window}m"
            )
            
            # Calculate rolling statistics
            if data and 'data' in data:
                values = [d.get(f"{metric}_sum", 0) for d in data['data']]
                
                results[f"window_{window}m"] = {
                    'mean': np.mean(values),
                    'std': np.std(values),
                    'min': np.min(values),
                    'max': np.max(values),
                    'median': np.median(values)
                }
                
        return results
        
    async def get_metric_trends(self,
                               metrics: List[str],
                               time_range: str = "24h",
                               interval: str = "1h") -> Dict[str, Any]:
        """Get metric trends over time"""
        
        # Parse time range and interval
        end_time = datetime.utcnow()
        start_time = self._parse_time_range(time_range, end_time)
        interval_minutes = self._parse_interval(interval)
        
        trends = {}
        
        for metric in metrics:
            # Get time series data
            data = await self.aggregate_metrics(
                source="mixed",
                metrics=[metric],
                time_range=time_range
            )
            
            if data and 'data' in data:
                # Calculate trend
                values = [d.get(f"{metric}_sum", 0) for d in data['data']]
                timestamps = [d.get('timestamp') for d in data['data']]
                
                # Simple linear regression for trend
                if len(values) > 1:
                    x = np.arange(len(values))
                    slope, intercept = np.polyfit(x, values, 1)
                    
                    trends[metric] = {
                        'direction': 'increasing' if slope > 0 else 'decreasing',
                        'slope': float(slope),
                        'current_value': values[-1] if values else 0,
                        'change_rate': float(slope / np.mean(values)) if np.mean(values) != 0 else 0
                    }
                    
        return trends
        
    def _parse_time_range(self, time_range: str, end_time: datetime) -> datetime:
        """Parse time range string to datetime"""
        unit_map = {
            'm': 'minutes',
            'h': 'hours', 
            'd': 'days',
            'w': 'weeks'
        }
        
        # Extract number and unit
        import re
        match = re.match(r'(\d+)([mhdw])', time_range)
        if match:
            num, unit = match.groups()
            kwargs = {unit_map[unit]: int(num)}
            return end_time - timedelta(**kwargs)
        else:
            return end_time - timedelta(hours=1)  # Default 1 hour
            
    def _parse_interval(self, interval: str) -> int:
        """Parse interval string to minutes"""
        unit_multipliers = {
            'm': 1,
            'h': 60,
            'd': 1440
        }
        
        import re
        match = re.match(r'(\d+)([mhd])', interval)
        if match:
            num, unit = match.groups()
            return int(num) * unit_multipliers[unit]
        else:
            return 60  # Default 1 hour
            
    def _generate_cache_key(self, source: str, metrics: List[str], 
                           time_range: str, group_by: Optional[List[str]], 
                           filters: Optional[Dict[str, Any]]) -> str:
        """Generate cache key for aggregation"""
        import hashlib
        key_parts = [
            source,
            ','.join(sorted(metrics)),
            time_range,
            ','.join(sorted(group_by or [])),
            str(sorted(filters.items()) if filters else '')
        ]
        key_string = '|'.join(key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()
        
    def _get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get cached aggregation result"""
        cache = self.ignite_client.get_cache('metrics_aggregations')
        return cache.get(cache_key)
        
    def _cache_result(self, cache_key: str, result: Dict[str, Any], ttl: int = 300):
        """Cache aggregation result with TTL"""
        cache = self.ignite_client.get_cache('metrics_aggregations')
        cache.put(cache_key, result, ttl)
        
    def _build_druid_filter(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Build Druid filter from dictionary"""
        if len(filters) == 1:
            key, value = list(filters.items())[0]
            return {"type": "selector", "dimension": key, "value": value}
        else:
            return {
                "type": "and",
                "fields": [
                    {"type": "selector", "dimension": k, "value": v}
                    for k, v in filters.items()
                ]
            }
            
    def _process_druid_results(self, data: List[Dict], metrics: List[str]) -> Dict[str, Any]:
        """Process Druid query results"""
        processed = {
            'data': data,
            'summary': {
                'total_count': sum(d.get('count', 0) for d in data)
            }
        }
        
        for metric in metrics:
            metric_sum = sum(d.get(f"{metric}_sum", 0) for d in data)
            processed['summary'][f"{metric}_total"] = metric_sum
            
        return processed
        
    def _process_ignite_results(self, results: List[Any], metrics: List[str]) -> Dict[str, Any]:
        """Process Ignite query results"""
        if not results:
            return {'data': [], 'summary': {}}
            
        # Convert to list of dicts
        data = []
        for row in results:
            if hasattr(row, '_asdict'):
                data.append(row._asdict())
            else:
                data.append(dict(row))
                
        return {
            'data': data,
            'summary': {
                'total_count': len(data)
            }
        }
        
    def _merge_results(self, results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Merge results from multiple sources"""
        merged_data = []
        
        # Add historical data
        if 'historical' in results and 'data' in results['historical']:
            merged_data.extend(results['historical']['data'])
            
        # Add realtime data
        if 'realtime' in results and 'data' in results['realtime']:
            merged_data.extend(results['realtime']['data'])
            
        # Sort by timestamp
        merged_data.sort(key=lambda x: x.get('timestamp', ''))
        
        return {
            'data': merged_data,
            'summary': {
                'total_count': len(merged_data),
                'sources': list(results.keys())
            }
        }
        
    async def close(self):
        """Close connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.druid_client:
            await self.druid_client.aclose() 