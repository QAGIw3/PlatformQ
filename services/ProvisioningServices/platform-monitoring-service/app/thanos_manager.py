"""Thanos Manager for long-term storage and global queries"""

import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import aiohttp
from minio import Minio
from prometheus_client import Counter, Histogram

from config import settings
from models import (
    RegionConfig,
    MetricsQuery,
    QueryResult,
    MetricValue,
    MetricSeries,
    TimeRange,
    CompactionGroup,
    ThanosConfig
)

logger = logging.getLogger(__name__)

# Metrics
thanos_query_counter = Counter(
    'thanos_queries_total',
    'Total number of Thanos queries',
    ['query_type', 'status']
)
thanos_query_duration = Histogram(
    'thanos_query_duration_seconds',
    'Duration of Thanos queries',
    ['query_type']
)
thanos_compaction_counter = Counter(
    'thanos_compaction_runs_total',
    'Total number of compaction runs',
    ['status']
)


class ThanosManager:
    """Manages Thanos components for long-term storage and querying"""
    
    def __init__(self):
        self.session = None
        self.minio_client = None
        self.running = False
        self.compaction_task = None
        self.region_configs: Dict[str, RegionConfig] = {}
        
        # Thanos configuration
        self.thanos_config = ThanosConfig(
            object_store_config={
                'type': 's3',
                'config': {
                    'bucket': settings.MINIO_BUCKET,
                    'endpoint': settings.MINIO_ENDPOINT,
                    'access_key': settings.MINIO_ACCESS_KEY,
                    'secret_key': settings.MINIO_SECRET_KEY,
                    'insecure': not settings.MINIO_SECURE,
                    'signature_version2': False,
                    'put_user_metadata': {}
                }
            },
            compaction_groups=[
                CompactionGroup(
                    resolution='0',
                    retention=settings.COMPACTION_RETENTION_1H
                ),
                CompactionGroup(
                    resolution='5m',
                    retention=settings.COMPACTION_RETENTION_5M
                ),
                CompactionGroup(
                    resolution='1h',
                    retention=settings.COMPACTION_RETENTION_1D
                )
            ]
        )
        
    async def start(self):
        """Start the Thanos manager"""
        logger.info("Starting Thanos Manager")
        self.running = True
        self.session = aiohttp.ClientSession()
        
        # Initialize MinIO client
        self.minio_client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE
        )
        
        # Ensure bucket exists
        await self._ensure_bucket()
        
        # Start compaction monitoring
        self.compaction_task = asyncio.create_task(self._monitor_compaction())
        
    async def stop(self):
        """Stop the Thanos manager"""
        logger.info("Stopping Thanos Manager")
        self.running = False
        
        if self.compaction_task:
            self.compaction_task.cancel()
            try:
                await self.compaction_task
            except asyncio.CancelledError:
                pass
                
        if self.session:
            await self.session.close()
            
    async def is_ready(self) -> bool:
        """Check if Thanos is ready"""
        try:
            async with self.session.get(
                f"{settings.THANOS_QUERY_URL}/-/healthy",
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return resp.status == 200
        except:
            return False
            
    async def configure_region(self, region_id: str, config: RegionConfig):
        """Configure Thanos for a new region"""
        logger.info(f"Configuring Thanos for region: {region_id}")
        
        self.region_configs[region_id] = config
        
        # The actual Thanos sidecar configuration would be handled
        # by the deployment automation (Kubernetes, etc.)
        # Here we just track the configuration
        
        logger.info(f"Thanos configured for region: {region_id}")
        
    async def remove_region(self, region_id: str):
        """Remove Thanos configuration for a region"""
        logger.info(f"Removing Thanos configuration for region: {region_id}")
        
        if region_id in self.region_configs:
            del self.region_configs[region_id]
            
    async def query(
        self,
        promql: str,
        time_range: Optional[TimeRange] = None,
        regions: Optional[List[str]] = None,
        tenant_id: Optional[str] = None
    ) -> QueryResult:
        """Execute an instant query via Thanos Query"""
        with thanos_query_duration.labels(query_type='instant').time():
            try:
                # Build query parameters
                params = {'query': promql}
                
                if time_range and time_range.end:
                    params['time'] = int(time_range.end.timestamp())
                    
                # Add tenant filter if specified
                if tenant_id:
                    if '{' in promql:
                        # Insert tenant filter into existing selector
                        promql = promql.replace('{', f'{{tenant_id="{tenant_id}",')
                    else:
                        # Add tenant filter to metric name
                        promql = f'{promql}{{tenant_id="{tenant_id}"}}'
                    params['query'] = promql
                    
                # Add region filter if specified
                if regions:
                    region_filter = '|'.join(regions)
                    if '{' in promql:
                        promql = promql.replace('{', f'{{region=~"{region_filter}",')
                    else:
                        promql = f'{promql}{{region=~"{region_filter}"}}'
                    params['query'] = promql
                    
                # Execute query
                async with self.session.get(
                    f"{settings.THANOS_QUERY_URL}/api/v1/query",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=settings.QUERY_TIMEOUT)
                ) as resp:
                    data = await resp.json()
                    
                    if resp.status != 200 or data['status'] != 'success':
                        thanos_query_counter.labels(
                            query_type='instant',
                            status='error'
                        ).inc()
                        raise Exception(f"Query failed: {data.get('error', 'Unknown error')}")
                        
                    thanos_query_counter.labels(
                        query_type='instant',
                        status='success'
                    ).inc()
                    
                    # Convert to our model
                    return self._convert_query_result(data)
                    
            except Exception as e:
                logger.error(f"Thanos query failed: {e}")
                thanos_query_counter.labels(
                    query_type='instant',
                    status='error'
                ).inc()
                raise
                
    async def query_range(
        self,
        promql: str,
        time_range: TimeRange,
        step: str = "1m",
        regions: Optional[List[str]] = None,
        tenant_id: Optional[str] = None
    ) -> QueryResult:
        """Execute a range query via Thanos Query"""
        with thanos_query_duration.labels(query_type='range').time():
            try:
                # Build query parameters
                params = {
                    'query': promql,
                    'start': int(time_range.start.timestamp()),
                    'end': int(time_range.end.timestamp()),
                    'step': step
                }
                
                # Add tenant filter if specified
                if tenant_id:
                    if '{' in promql:
                        promql = promql.replace('{', f'{{tenant_id="{tenant_id}",')
                    else:
                        promql = f'{promql}{{tenant_id="{tenant_id}"}}'
                    params['query'] = promql
                    
                # Add region filter if specified
                if regions:
                    region_filter = '|'.join(regions)
                    if '{' in promql:
                        promql = promql.replace('{', f'{{region=~"{region_filter}",')
                    else:
                        promql = f'{promql}{{region=~"{region_filter}"}}'
                    params['query'] = promql
                    
                # Execute query
                async with self.session.get(
                    f"{settings.THANOS_QUERY_URL}/api/v1/query_range",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=settings.QUERY_TIMEOUT)
                ) as resp:
                    data = await resp.json()
                    
                    if resp.status != 200 or data['status'] != 'success':
                        thanos_query_counter.labels(
                            query_type='range',
                            status='error'
                        ).inc()
                        raise Exception(f"Range query failed: {data.get('error', 'Unknown error')}")
                        
                    thanos_query_counter.labels(
                        query_type='range',
                        status='success'
                    ).inc()
                    
                    # Convert to our model
                    return self._convert_query_result(data)
                    
            except Exception as e:
                logger.error(f"Thanos range query failed: {e}")
                thanos_query_counter.labels(
                    query_type='range',
                    status='error'
                ).inc()
                raise
                
    async def get_metadata(
        self,
        metric: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """Get metric metadata from Thanos"""
        try:
            params = {'limit': limit}
            if metric:
                params['metric'] = metric
                
            async with self.session.get(
                f"{settings.THANOS_QUERY_URL}/api/v1/metadata",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                return await resp.json()
                
        except Exception as e:
            logger.error(f"Failed to get metadata: {e}")
            raise
            
    async def get_label_names(
        self,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> List[str]:
        """Get all label names"""
        try:
            params = {}
            if start:
                params['start'] = int(start.timestamp())
            if end:
                params['end'] = int(end.timestamp())
                
            async with self.session.get(
                f"{settings.THANOS_QUERY_URL}/api/v1/labels",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                return data['data'] if data['status'] == 'success' else []
                
        except Exception as e:
            logger.error(f"Failed to get label names: {e}")
            return []
            
    async def get_label_values(
        self,
        label_name: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> List[str]:
        """Get all values for a label"""
        try:
            params = {}
            if start:
                params['start'] = int(start.timestamp())
            if end:
                params['end'] = int(end.timestamp())
                
            async with self.session.get(
                f"{settings.THANOS_QUERY_URL}/api/v1/label/{label_name}/values",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                return data['data'] if data['status'] == 'success' else []
                
        except Exception as e:
            logger.error(f"Failed to get label values: {e}")
            return []
            
    async def get_series(
        self,
        match: List[str],
        start: Optional[datetime] = None,
        end: Optional[datetime] = None
    ) -> List[Dict[str, str]]:
        """Get series matching the given label matchers"""
        try:
            params = {'match[]': match}
            if start:
                params['start'] = int(start.timestamp())
            if end:
                params['end'] = int(end.timestamp())
                
            async with self.session.get(
                f"{settings.THANOS_QUERY_URL}/api/v1/series",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                data = await resp.json()
                return data['data'] if data['status'] == 'success' else []
                
        except Exception as e:
            logger.error(f"Failed to get series: {e}")
            return []
            
    def _convert_query_result(self, data: Dict[str, Any]) -> QueryResult:
        """Convert Prometheus/Thanos query result to our model"""
        series_list = []
        
        for result in data.get('data', {}).get('result', []):
            values = []
            
            # Handle both instant and range query results
            if 'value' in result:
                # Instant query
                timestamp, value = result['value']
                values.append(MetricValue(
                    timestamp=float(timestamp),
                    value=float(value)
                ))
            elif 'values' in result:
                # Range query
                for timestamp, value in result['values']:
                    values.append(MetricValue(
                        timestamp=float(timestamp),
                        value=float(value)
                    ))
                    
            series = MetricSeries(
                labels=result['metric'],
                values=values
            )
            series_list.append(series)
            
        return QueryResult(
            status='success',
            data=series_list,
            warnings=data.get('warnings', []),
            execution_time=0.0  # Would need timing info from query
        )
        
    async def _ensure_bucket(self):
        """Ensure MinIO bucket exists for Thanos"""
        try:
            if not self.minio_client.bucket_exists(settings.MINIO_BUCKET):
                self.minio_client.make_bucket(settings.MINIO_BUCKET)
                logger.info(f"Created MinIO bucket: {settings.MINIO_BUCKET}")
                
                # Set bucket lifecycle for automatic cleanup
                lifecycle_config = {
                    "Rules": [
                        {
                            "ID": "cleanup-incomplete-uploads",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "AbortIncompleteMultipartUpload": {
                                "DaysAfterInitiation": 1
                            }
                        },
                        {
                            "ID": "cleanup-debug-data",
                            "Status": "Enabled",
                            "Filter": {"Prefix": "debug/"},
                            "Expiration": {"Days": 7}
                        }
                    ]
                }
                
                self.minio_client.set_bucket_lifecycle(
                    settings.MINIO_BUCKET,
                    lifecycle_config
                )
                
        except Exception as e:
            logger.error(f"Failed to ensure bucket: {e}")
            
    async def _monitor_compaction(self):
        """Monitor Thanos compaction status"""
        while self.running:
            try:
                # Check compaction status
                async with self.session.get(
                    f"{settings.THANOS_COMPACT_URL}/api/v1/metrics",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        metrics = await resp.text()
                        # Parse relevant metrics
                        if 'thanos_compact_group_compactions_total' in metrics:
                            thanos_compaction_counter.labels(status='success').inc()
                        
                # Check for compaction errors
                async with self.session.get(
                    f"{settings.THANOS_COMPACT_URL}/api/v1/blocks",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        # Log any blocks with issues
                        for block in data.get('blocks', []):
                            if block.get('compaction', {}).get('failed'):
                                logger.error(f"Compaction failed for block: {block['ulid']}")
                                thanos_compaction_counter.labels(status='failure').inc()
                                
            except Exception as e:
                logger.error(f"Compaction monitoring error: {e}")
                
            await asyncio.sleep(300)  # Check every 5 minutes
            
    async def run_compaction(self):
        """Manually trigger compaction (if needed)"""
        try:
            # Thanos Compact runs continuously, but we can check its status
            async with self.session.get(
                f"{settings.THANOS_COMPACT_URL}/api/v1/status",
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(f"Compaction status: {data}")
                    return data
                else:
                    raise Exception(f"Compaction status check failed: {resp.status}")
                    
        except Exception as e:
            logger.error(f"Failed to check compaction: {e}")
            raise 