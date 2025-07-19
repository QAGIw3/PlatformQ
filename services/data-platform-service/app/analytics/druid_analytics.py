"""
Apache Druid Analytics Engine for Data Platform Service

Provides time-series analytics, OLAP queries, and data ingestion capabilities.
"""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import asyncio
import httpx
from pydruid.client import PyDruid
from pydruid.utils.aggregators import longsum, doublesum, doublemin, doublemax, count
from pydruid.utils.filters import Dimension, Filter, Bound
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class DruidAnalyticsEngine:
    """Apache Druid analytics engine for time-series OLAP in data platform"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize Druid analytics engine"""
        config = config or {}
        self.coordinator_url = config.get('coordinator_url', 'http://druid-coordinator:8081')
        self.broker_url = config.get('broker_url', 'http://druid-broker:8082')
        self.overlord_url = config.get('overlord_url', 'http://druid-overlord:8090')
        
        # Initialize PyDruid client
        self.client = PyDruid(self.broker_url, 'druid/v2')
        
        # HTTP client for management APIs
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Cache for datasource metadata
        self.datasource_cache = {}
        
        logger.info(f"Initialized Druid Analytics Engine with broker at {self.broker_url}")
    
    async def query_timeseries(self,
                             datasource: str,
                             metrics: Union[str, List[str]],
                             granularity: str = "hour",
                             filter: Optional[Dict[str, Any]] = None,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             aggregation: str = "sum") -> List[Dict[str, Any]]:
        """Execute time series query on Druid"""
        try:
            # Normalize metrics to list
            if isinstance(metrics, str):
                metrics = [metrics]
            
            # Default time range if not specified
            if not end_time:
                end_time = datetime.utcnow()
            if not start_time:
                start_time = end_time - timedelta(days=1)
            
            # Build aggregators
            aggregators = []
            for metric in metrics:
                if aggregation == "sum":
                    aggregators.append(doublesum(metric))
                elif aggregation == "min":
                    aggregators.append(doublemin(metric))
                elif aggregation == "max":
                    aggregators.append(doublemax(metric))
                elif aggregation == "count":
                    aggregators.append(count(metric))
                else:
                    aggregators.append(doublesum(metric))
            
            # Build filter
            druid_filter = None
            if filter:
                filter_conditions = []
                for key, value in filter.items():
                    if isinstance(value, list):
                        filter_conditions.append(
                            Dimension(key) == value[0] if len(value) == 1 
                            else Filter(type="in", dimension=key, values=value)
                        )
                    else:
                        filter_conditions.append(Dimension(key) == value)
                
                if len(filter_conditions) > 1:
                    druid_filter = Filter(type="and", fields=filter_conditions)
                elif filter_conditions:
                    druid_filter = filter_conditions[0]
            
            # Execute query
            result = self.client.timeseries(
                datasource=datasource,
                granularity=granularity,
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                aggregations=aggregators,
                filter=druid_filter
            )
            
            return result.export()
            
        except Exception as e:
            logger.error(f"Druid timeseries query failed: {e}")
            raise
    
    async def query_groupby(self,
                          datasource: str,
                          dimensions: List[str],
                          metrics: List[str],
                          filter: Optional[Dict[str, Any]] = None,
                          start_time: Optional[datetime] = None,
                          end_time: Optional[datetime] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Execute group by query on Druid"""
        try:
            # Default time range
            if not end_time:
                end_time = datetime.utcnow()
            if not start_time:
                start_time = end_time - timedelta(days=1)
            
            # Build aggregators
            aggregators = [doublesum(metric) for metric in metrics]
            
            # Build filter
            druid_filter = None
            if filter:
                filter_conditions = []
                for key, value in filter.items():
                    filter_conditions.append(Dimension(key) == value)
                
                if len(filter_conditions) > 1:
                    druid_filter = Filter(type="and", fields=filter_conditions)
                elif filter_conditions:
                    druid_filter = filter_conditions[0]
            
            # Execute query
            result = self.client.groupby(
                datasource=datasource,
                dimensions=dimensions,
                granularity="all",
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                aggregations=aggregators,
                filter=druid_filter,
                limit_spec={
                    "type": "default",
                    "limit": limit,
                    "columns": [{"dimension": metrics[0], "direction": "descending"}]
                }
            )
            
            return result.export()
            
        except Exception as e:
            logger.error(f"Druid groupby query failed: {e}")
            raise
    
    async def ingest_batch(self, 
                         datasource: str,
                         data: List[Dict[str, Any]],
                         timestamp_column: str = "timestamp",
                         timestamp_format: str = "iso") -> Dict[str, Any]:
        """Ingest batch data into Druid"""
        try:
            # Create ingestion spec
            ingestion_spec = {
                "type": "index_parallel",
                "spec": {
                    "dataSchema": {
                        "dataSource": datasource,
                        "timestampSpec": {
                            "column": timestamp_column,
                            "format": timestamp_format
                        },
                        "dimensionsSpec": {
                            "dimensions": self._infer_dimensions(data)
                        },
                        "metricsSpec": self._infer_metrics(data),
                        "granularitySpec": {
                            "type": "uniform",
                            "segmentGranularity": "hour",
                            "queryGranularity": "minute",
                            "rollup": False
                        }
                    },
                    "ioConfig": {
                        "type": "index_parallel",
                        "inputSource": {
                            "type": "inline",
                            "data": "\n".join([str(row) for row in data])
                        },
                        "inputFormat": {
                            "type": "json"
                        }
                    },
                    "tuningConfig": {
                        "type": "index_parallel",
                        "maxRowsPerSegment": 5000000,
                        "maxRowsInMemory": 1000000
                    }
                }
            }
            
            # Submit ingestion task
            response = await self.http_client.post(
                f"{self.overlord_url}/druid/indexer/v1/task",
                json=ingestion_spec
            )
            response.raise_for_status()
            
            task_id = response.json()["task"]
            
            return {
                "status": "submitted",
                "task_id": task_id,
                "datasource": datasource,
                "row_count": len(data)
            }
            
        except Exception as e:
            logger.error(f"Druid batch ingestion failed: {e}")
            raise
    
    async def get_datasources(self) -> List[str]:
        """Get list of available datasources"""
        try:
            response = await self.http_client.get(
                f"{self.coordinator_url}/druid/coordinator/v1/datasources"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get datasources: {e}")
            raise
    
    async def get_datasource_metadata(self, datasource: str) -> Dict[str, Any]:
        """Get metadata for a datasource"""
        try:
            # Check cache first
            if datasource in self.datasource_cache:
                cached = self.datasource_cache[datasource]
                if cached["timestamp"] > datetime.utcnow() - timedelta(minutes=5):
                    return cached["metadata"]
            
            # Fetch from Druid
            response = await self.http_client.get(
                f"{self.coordinator_url}/druid/coordinator/v1/datasources/{datasource}"
            )
            response.raise_for_status()
            
            metadata = response.json()
            
            # Cache the result
            self.datasource_cache[datasource] = {
                "metadata": metadata,
                "timestamp": datetime.utcnow()
            }
            
            return metadata
            
        except Exception as e:
            logger.error(f"Failed to get datasource metadata: {e}")
            raise
    
    async def check_health(self) -> Dict[str, Any]:
        """Check Druid cluster health"""
        try:
            health_status = {
                "coordinator": False,
                "broker": False,
                "overlord": False,
                "datasources": []
            }
            
            # Check coordinator
            try:
                response = await self.http_client.get(
                    f"{self.coordinator_url}/status/health"
                )
                health_status["coordinator"] = response.status_code == 200
            except:
                pass
            
            # Check broker
            try:
                response = await self.http_client.get(
                    f"{self.broker_url}/status/health"
                )
                health_status["broker"] = response.status_code == 200
            except:
                pass
            
            # Check overlord
            try:
                response = await self.http_client.get(
                    f"{self.overlord_url}/status/health"
                )
                health_status["overlord"] = response.status_code == 200
            except:
                pass
            
            # Get datasources if coordinator is healthy
            if health_status["coordinator"]:
                try:
                    health_status["datasources"] = await self.get_datasources()
                except:
                    pass
            
            health_status["healthy"] = all([
                health_status["coordinator"],
                health_status["broker"],
                health_status["overlord"]
            ])
            
            return health_status
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"healthy": False, "error": str(e)}
    
    def _infer_dimensions(self, data: List[Dict[str, Any]]) -> List[str]:
        """Infer dimensions from data"""
        if not data:
            return []
        
        sample = data[0]
        dimensions = []
        
        for key, value in sample.items():
            if key == "timestamp" or key.endswith("_time"):
                continue
            if isinstance(value, (str, bool)):
                dimensions.append(key)
            elif isinstance(value, (int, float)) and key.endswith("_id"):
                dimensions.append(key)
        
        return dimensions
    
    def _infer_metrics(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Infer metrics from data"""
        if not data:
            return []
        
        sample = data[0]
        metrics = []
        
        for key, value in sample.items():
            if isinstance(value, (int, float)) and not key.endswith("_id"):
                metrics.append({
                    "type": "doubleSum",
                    "name": key,
                    "fieldName": key
                })
        
        return metrics
    
    async def close(self):
        """Close HTTP client"""
        await self.http_client.aclose() 