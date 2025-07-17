"""
Apache Druid Analytics Engine

Provides advanced time-series analytics and OLAP queries using Apache Druid.
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
    """Apache Druid analytics engine for time-series OLAP"""
    
    def __init__(self, config: Dict[str, Any]):
        self.coordinator_url = config.get('coordinator_url', 'http://druid-coordinator:8081')
        self.broker_url = config.get('broker_url', 'http://druid-broker:8082')
        self.overlord_url = config.get('overlord_url', 'http://druid-overlord:8090')
        
        # Initialize PyDruid client
        self.client = PyDruid(self.broker_url, 'druid/v2')
        
        # HTTP client for management APIs
        self.http_client = httpx.AsyncClient()
        
        # Cache for datasource metadata
        self.datasource_cache = {}
        
    async def query_timeseries(self,
                             datasource: str,
                             metric: str,
                             aggregation: str = "sum",
                             granularity: str = "minute",
                             filter: Optional[Dict[str, Any]] = None,
                             group_by: Optional[List[str]] = None,
                             start_time: datetime = None,
                             end_time: datetime = None) -> List[Dict[str, Any]]:
        """Execute time series query"""
        try:
            # Build aggregator based on type
            if aggregation == "sum":
                aggregator = doublesum(metric)
            elif aggregation == "avg":
                aggregator = {"type": "doubleAvg", "name": metric, "fieldName": metric}
            elif aggregation == "min":
                aggregator = doublemin(metric)
            elif aggregation == "max":
                aggregator = doublemax(metric)
            elif aggregation == "count":
                aggregator = count(metric)
            else:
                aggregator = doublesum(metric)
            
            # Build filter if provided
            druid_filter = None
            if filter:
                filters = []
                for key, value in filter.items():
                    if isinstance(value, list):
                        filters.append(Dimension(key) in value)
                    else:
                        filters.append(Dimension(key) == value)
                
                if len(filters) > 1:
                    druid_filter = Filter(type="and", fields=filters)
                else:
                    druid_filter = filters[0] if filters else None
            
            # Build time bounds
            time_filter = Bound(
                dimension="__time",
                lower=start_time.isoformat() if start_time else "2000-01-01T00:00:00",
                upper=end_time.isoformat() if end_time else datetime.utcnow().isoformat()
            )
            
            # Combine filters
            if druid_filter:
                final_filter = Filter(type="and", fields=[time_filter, druid_filter])
            else:
                final_filter = time_filter
            
            # Execute query
            if group_by:
                # Use groupBy query
                query = self.client.groupby(
                    datasource=datasource,
                    granularity=granularity,
                    intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                    dimensions=group_by,
                    aggregations=[aggregator],
                    filter=final_filter
                )
            else:
                # Use timeseries query
                query = self.client.timeseries(
                    datasource=datasource,
                    granularity=granularity,
                    intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                    aggregations=[aggregator],
                    filter=final_filter
                )
            
            # Execute and return results
            result = query.export_pandas()
            return result.to_dict('records') if not result.empty else []
            
        except Exception as e:
            logger.error(f"Error executing timeseries query: {e}")
            raise
    
    async def query_olap(self,
                        datasource: str,
                        metrics: List[str],
                        dimensions: List[str],
                        filters: Optional[Dict[str, Any]] = None,
                        time_range: Optional[Dict[str, datetime]] = None,
                        rollup: Optional[str] = None) -> Dict[str, Any]:
        """Execute OLAP cube query"""
        try:
            # Build aggregations
            aggregations = []
            for metric in metrics:
                aggregations.append(doublesum(metric))
            
            # Build time interval
            if time_range:
                start = time_range.get('start', datetime.utcnow() - timedelta(days=1))
                end = time_range.get('end', datetime.utcnow())
            else:
                start = datetime.utcnow() - timedelta(days=1)
                end = datetime.utcnow()
            
            intervals = [f"{start.isoformat()}/{end.isoformat()}"]
            
            # Build filter
            druid_filter = None
            if filters:
                filter_list = []
                for key, value in filters.items():
                    if isinstance(value, list):
                        filter_list.append(Dimension(key) in value)
                    else:
                        filter_list.append(Dimension(key) == value)
                
                if len(filter_list) > 1:
                    druid_filter = Filter(type="and", fields=filter_list)
                else:
                    druid_filter = filter_list[0] if filter_list else None
            
            # Execute groupBy query
            query = self.client.groupby(
                datasource=datasource,
                granularity=rollup or "all",
                intervals=intervals,
                dimensions=dimensions,
                aggregations=aggregations,
                filter=druid_filter
            )
            
            result_df = query.export_pandas()
            
            # Process results into OLAP cube format
            if not result_df.empty:
                # Pivot table for multi-dimensional analysis
                if len(dimensions) > 1:
                    pivot_result = {}
                    for metric in metrics:
                        if metric in result_df.columns:
                            pivot = result_df.pivot_table(
                                values=metric,
                                index=dimensions[0],
                                columns=dimensions[1:] if len(dimensions) > 1 else None,
                                aggfunc='sum'
                            )
                            pivot_result[metric] = pivot.to_dict()
                    
                    return {
                        "cube": pivot_result,
                        "dimensions": dimensions,
                        "metrics": metrics,
                        "row_count": len(result_df)
                    }
                else:
                    return {
                        "data": result_df.to_dict('records'),
                        "dimensions": dimensions,
                        "metrics": metrics,
                        "row_count": len(result_df)
                    }
            else:
                return {
                    "data": [],
                    "dimensions": dimensions,
                    "metrics": metrics,
                    "row_count": 0
                }
                
        except Exception as e:
            logger.error(f"Error executing OLAP query: {e}")
            raise
    
    async def query_topn(self,
                        datasource: str,
                        metric: str,
                        dimension: str,
                        threshold: int = 10,
                        start_time: datetime = None,
                        end_time: datetime = None) -> List[Dict[str, Any]]:
        """Execute TopN query"""
        try:
            if not start_time:
                start_time = datetime.utcnow() - timedelta(hours=1)
            if not end_time:
                end_time = datetime.utcnow()
            
            query = self.client.topn(
                datasource=datasource,
                dimension=dimension,
                threshold=threshold,
                metric=metric,
                intervals=[f"{start_time.isoformat()}/{end_time.isoformat()}"],
                aggregations=[doublesum(metric)]
            )
            
            result = query.export_pandas()
            return result.to_dict('records') if not result.empty else []
            
        except Exception as e:
            logger.error(f"Error executing TopN query: {e}")
            raise
    
    async def ingest_metric(self, datasource: str, metric: Dict[str, Any]):
        """Ingest a single metric into Druid"""
        try:
            # Ensure timestamp
            if 'timestamp' not in metric:
                metric['timestamp'] = datetime.utcnow().isoformat()
            
            # Send to Druid ingestion endpoint
            # In production, this would use Kafka/Kinesis ingestion
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.overlord_url}/druid/indexer/v1/task",
                    json={
                        "type": "index_parallel",
                        "spec": {
                            "dataSchema": {
                                "dataSource": datasource,
                                "timestampSpec": {
                                    "column": "timestamp",
                                    "format": "iso"
                                },
                                "dimensionsSpec": {
                                    "dimensions": list(metric.keys() - {'timestamp', 'value'})
                                },
                                "metricsSpec": [
                                    {"type": "doubleSum", "name": "value", "fieldName": "value"}
                                ],
                                "granularitySpec": {
                                    "type": "uniform",
                                    "segmentGranularity": "hour",
                                    "queryGranularity": "minute"
                                }
                            },
                            "ioConfig": {
                                "type": "index_parallel",
                                "inputSource": {
                                    "type": "inline",
                                    "data": [metric]
                                },
                                "inputFormat": {
                                    "type": "json"
                                }
                            }
                        }
                    }
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to ingest metric: {response.text}")
                    
        except Exception as e:
            logger.error(f"Error ingesting metric: {e}")
            raise
    
    async def ensure_datasource(self, datasource: str):
        """Ensure datasource exists in Druid"""
        try:
            # Check if datasource exists
            response = await self.http_client.get(
                f"{self.coordinator_url}/druid/coordinator/v1/datasources/{datasource}"
            )
            
            if response.status_code == 404:
                # Create datasource spec
                logger.info(f"Creating datasource: {datasource}")
                # In production, this would create the ingestion spec
                
        except Exception as e:
            logger.error(f"Error ensuring datasource: {e}")
    
    async def get_latest_metrics(self, datasource: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get latest metric values"""
        try:
            # Query last 5 minutes
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=5)
            
            results = await self.query_timeseries(
                datasource=datasource,
                metric="value",
                aggregation="avg",
                granularity="minute",
                filter=filters,
                start_time=start_time,
                end_time=end_time
            )
            
            if results:
                return results[-1]  # Return most recent
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting latest metrics: {e}")
            return {}
    
    async def analyze_trends(self, datasource: str, filters: Optional[Dict[str, Any]] = None,
                           window_hours: int = 24) -> Dict[str, Any]:
        """Analyze trends in metrics"""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=window_hours)
            
            # Get time series data
            data = await self.query_timeseries(
                datasource=datasource,
                metric="value",
                aggregation="avg",
                granularity="hour" if window_hours > 24 else "minute",
                filter=filters,
                start_time=start_time,
                end_time=end_time
            )
            
            if not data:
                return {"trend": "no_data"}
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(data)
            
            # Calculate trend
            values = df['value'].values
            times = pd.to_datetime(df['timestamp']).values
            
            # Linear regression for trend
            from scipy import stats
            x = np.arange(len(values))
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, values)
            
            # Determine trend direction
            if abs(slope) < 0.01:
                trend_direction = "stable"
            elif slope > 0:
                trend_direction = "increasing"
            else:
                trend_direction = "decreasing"
            
            # Calculate statistics
            return {
                "trend_direction": trend_direction,
                "slope": float(slope),
                "r_squared": float(r_value ** 2),
                "mean": float(np.mean(values)),
                "std": float(np.std(values)),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "data_points": len(values)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing trends: {e}")
            return {"trend": "error", "error": str(e)}
    
    async def assess_data_quality(self, datasource: str) -> Dict[str, Any]:
        """Assess data quality for a datasource"""
        try:
            # Get recent data
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            
            # Query for completeness
            data = await self.query_timeseries(
                datasource=datasource,
                metric="count",
                aggregation="count",
                granularity="hour",
                start_time=start_time,
                end_time=end_time
            )
            
            # Calculate quality metrics
            expected_hours = 24
            actual_hours = len(data)
            completeness = actual_hours / expected_hours
            
            # Check for gaps
            gaps = []
            if data:
                df = pd.DataFrame(data)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp').sort_index()
                
                # Find gaps larger than expected granularity
                time_diffs = df.index.to_series().diff()
                gap_threshold = pd.Timedelta(hours=2)
                gaps = time_diffs[time_diffs > gap_threshold].tolist()
            
            # Calculate consistency (variance in data rate)
            if data and 'count' in data[0]:
                counts = [d['count'] for d in data]
                consistency = 1 - (np.std(counts) / (np.mean(counts) + 1e-9))
            else:
                consistency = 0
            
            # Timeliness - how recent is the data
            if data:
                last_timestamp = pd.to_datetime(data[-1]['timestamp'])
                delay = (datetime.utcnow() - last_timestamp).total_seconds() / 60
                timeliness = max(0, 1 - (delay / 60))  # Penalize if > 1 hour old
            else:
                timeliness = 0
            
            # Overall score
            overall_score = (completeness * 0.4 + consistency * 0.3 + timeliness * 0.3)
            
            return {
                "overall_score": float(overall_score),
                "completeness": float(completeness),
                "consistency": float(consistency),
                "timeliness": float(timeliness),
                "issues": {
                    "gaps": len(gaps),
                    "missing_hours": expected_hours - actual_hours,
                    "last_update_minutes_ago": delay if data else None
                }
            }
            
        except Exception as e:
            logger.error(f"Error assessing data quality: {e}")
            return {
                "overall_score": 0,
                "error": str(e)
            }
    
    async def get_simulation_insights(self, simulation_id: str) -> Dict[str, Any]:
        """Get Druid-based insights for a simulation"""
        try:
            # Query multiple metrics
            metrics = ["cpu_usage", "memory_usage", "convergence_rate", "error_rate"]
            
            insights = {}
            for metric in metrics:
                trend = await self.analyze_trends(
                    datasource="simulation_metrics",
                    filters={"simulation_id": simulation_id, "metric_name": metric},
                    window_hours=6
                )
                insights[metric] = trend
            
            return insights
            
        except Exception as e:
            logger.error(f"Error getting simulation insights: {e}")
            return {}
    
    async def generate_batch_report(self,
                                  datasources: List[str],
                                  start_time: datetime,
                                  end_time: datetime) -> Dict[str, Any]:
        """Generate comprehensive batch report"""
        try:
            report = {
                "period": f"{start_time.isoformat()} to {end_time.isoformat()}",
                "datasources": {}
            }
            
            for datasource in datasources:
                # Get aggregated metrics
                metrics = await self.query_timeseries(
                    datasource=datasource,
                    metric="value",
                    aggregation="sum",
                    granularity="day",
                    start_time=start_time,
                    end_time=end_time
                )
                
                # Get quality assessment
                quality = await self.assess_data_quality(datasource)
                
                report["datasources"][datasource] = {
                    "metrics": metrics,
                    "quality": quality,
                    "row_count": len(metrics)
                }
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating batch report: {e}")
            raise
    
    async def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics for all datasources"""
        try:
            # In production, query metadata to get all datasources
            datasources = ["metrics", "simulation_metrics", "component_metrics"]
            
            current_metrics = {}
            for ds in datasources:
                latest = await self.get_latest_metrics(ds)
                if latest:
                    current_metrics[ds] = latest
            
            return current_metrics
            
        except Exception as e:
            logger.error(f"Error getting current metrics: {e}")
            return {}
    
    async def check_health(self) -> bool:
        """Check Druid cluster health"""
        try:
            response = await self.http_client.get(
                f"{self.coordinator_url}/status/health"
            )
            return response.status_code == 200
        except:
            return False
    
    def close(self):
        """Close connections"""
        asyncio.create_task(self.http_client.aclose()) 