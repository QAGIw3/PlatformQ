"""
Analytics Service Integration

Provides integration with the Analytics Service for:
- Federated analytics queries across data sources
- Advanced time-series analysis
- ML-powered anomaly detection
- Real-time dashboard data streaming
"""

import logging
from typing import Dict, Any, List, Optional, AsyncIterator
from datetime import datetime, timedelta
import asyncio
import httpx
import pandas as pd

from ..federation.federated_query_engine import FederatedQueryEngine
from ..lake.medallion_architecture import MedallionLakeManager
from ..quality.profiler import DataQualityProfiler
from ..core.cache_manager import DataCacheManager

logger = logging.getLogger(__name__)


class AnalyticsIntegration:
    """Integration with Analytics Service"""
    
    def __init__(self,
                 federated_engine: FederatedQueryEngine,
                 lake_manager: MedallionLakeManager,
                 quality_profiler: DataQualityProfiler,
                 cache_manager: DataCacheManager,
                 analytics_service_url: str = "http://analytics-service:8000"):
        self.federated_engine = federated_engine
        self.lake_manager = lake_manager
        self.quality_profiler = quality_profiler
        self.cache_manager = cache_manager
        self.analytics_service_url = analytics_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # WebSocket connections for streaming
        self.ws_connections = {}
        
    async def execute_unified_query(self,
                                   query: str,
                                   query_type: Optional[str] = None,
                                   time_range: str = "7d",
                                   cache_results: bool = True) -> Dict[str, Any]:
        """
        Execute unified analytics query across federated sources
        
        Args:
            query: SQL query or query template
            query_type: Type of query (batch, realtime, etc.)
            time_range: Time range for the query
            cache_results: Whether to cache results
            
        Returns:
            Query results with execution metadata
        """
        try:
            # First try to get from cache
            cache_key = f"analytics_query_{hash(query)}_{time_range}"
            if cache_results:
                cached = await self.cache_manager.get(cache_key)
                if cached:
                    return cached
                    
            # Prepare query with time filters
            enriched_query = self._enrich_query_with_time_filter(query, time_range)
            
            # Execute through federated engine
            fed_result = await self.federated_engine.execute_query(
                query=enriched_query,
                cache_results=False  # We handle caching ourselves
            )
            
            # Submit to analytics service for advanced processing
            response = await self.http_client.post(
                f"{self.analytics_service_url}/api/v1/query",
                json={
                    "query": query,
                    "query_type": query_type,
                    "mode": "auto",
                    "time_range": time_range,
                    "federated_data": fed_result
                }
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Cache if requested
            if cache_results:
                ttl = self._get_cache_ttl(time_range)
                await self.cache_manager.set(cache_key, result, ttl=ttl)
                
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute unified query: {e}")
            raise
            
    async def create_cross_service_dashboard(self,
                                           dashboard_config: Dict[str, Any],
                                           refresh_interval: int = 30) -> str:
        """
        Create cross-service analytics dashboard
        
        Args:
            dashboard_config: Dashboard configuration
            refresh_interval: Refresh interval in seconds
            
        Returns:
            Dashboard ID
        """
        try:
            # Prepare data sources
            data_sources = []
            
            for source in dashboard_config.get("data_sources", []):
                if source["type"] == "federated_query":
                    # Register federated query as data source
                    query_result = await self.federated_engine.execute_query(
                        query=source["query"],
                        explain_only=True
                    )
                    
                    data_sources.append({
                        "type": "federated",
                        "query": source["query"],
                        "schema": query_result.get("schema", [])
                    })
                    
                elif source["type"] == "data_lake":
                    # Register data lake path
                    data_sources.append({
                        "type": "lake",
                        "path": source["path"],
                        "zone": source.get("zone", "gold")
                    })
                    
            # Create dashboard in analytics service
            response = await self.http_client.post(
                f"{self.analytics_service_url}/api/v1/dashboards",
                json={
                    "name": dashboard_config["name"],
                    "type": dashboard_config.get("type", "custom"),
                    "config": {
                        "data_sources": data_sources,
                        "widgets": dashboard_config.get("widgets", []),
                        "layout": dashboard_config.get("layout", {})
                    },
                    "refresh_interval": refresh_interval
                }
            )
            response.raise_for_status()
            
            dashboard_data = response.json()
            dashboard_id = dashboard_data["dashboard_id"]
            
            # Set up data refresh job
            await self._schedule_dashboard_refresh(dashboard_id, refresh_interval)
            
            return dashboard_id
            
        except Exception as e:
            logger.error(f"Failed to create dashboard: {e}")
            raise
            
    async def detect_anomalies(self,
                             data_source: str,
                             metrics: List[str],
                             time_window: str = "1h",
                             sensitivity: float = 0.95) -> List[Dict[str, Any]]:
        """
        Detect anomalies in data using ML
        
        Args:
            data_source: Data source path or query
            metrics: Metrics to analyze
            time_window: Time window for analysis
            sensitivity: Anomaly detection sensitivity
            
        Returns:
            List of detected anomalies
        """
        try:
            # Get data from source
            if data_source.startswith("SELECT"):
                # It's a query
                result = await self.federated_engine.execute_query(data_source)
                df = pd.DataFrame(result["rows"], columns=[col["name"] for col in result["columns"]])
            else:
                # It's a lake path
                df = self.lake_manager.spark.read.parquet(
                    f"{self.lake_manager.base_path}/{data_source}"
                ).toPandas()
                
            # Prepare metrics data
            metrics_data = {}
            for metric in metrics:
                if metric in df.columns:
                    metrics_data[metric] = df[metric].tolist()
                    
            # Call analytics service for anomaly detection
            response = await self.http_client.post(
                f"{self.analytics_service_url}/api/v1/ml/detect-anomalies",
                json={
                    "data_source": data_source,
                    "metrics": metrics_data,
                    "config": {
                        "method": "isolation_forest",
                        "sensitivity": sensitivity,
                        "time_window": time_window
                    }
                }
            )
            response.raise_for_status()
            
            anomalies = response.json()["anomalies"]
            
            # Enrich with data quality insights
            for anomaly in anomalies:
                quality_issues = await self.quality_profiler.check_quality(
                    data_source=data_source,
                    column=anomaly["metric"],
                    timestamp=anomaly["timestamp"]
                )
                anomaly["quality_context"] = quality_issues
                
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            raise
            
    async def create_time_series_forecast(self,
                                        query: str,
                                        target_column: str,
                                        horizon_days: int = 7,
                                        confidence_interval: float = 0.95) -> Dict[str, Any]:
        """
        Create time series forecast
        
        Args:
            query: Query to get historical data
            target_column: Column to forecast
            horizon_days: Forecast horizon in days
            confidence_interval: Confidence interval for predictions
            
        Returns:
            Forecast results
        """
        try:
            # Get historical data
            result = await self.federated_engine.execute_query(query)
            df = pd.DataFrame(result["rows"], columns=[col["name"] for col in result["columns"]])
            
            # Prepare time series data
            ts_data = {
                "values": df[target_column].tolist(),
                "timestamps": df["timestamp"].tolist() if "timestamp" in df else None
            }
            
            # Call analytics service for forecasting
            response = await self.http_client.post(
                f"{self.analytics_service_url}/api/v1/ml/forecast",
                json={
                    "time_series": ts_data,
                    "target_column": target_column,
                    "horizon_days": horizon_days,
                    "confidence_interval": confidence_interval
                }
            )
            response.raise_for_status()
            
            forecast = response.json()
            
            # Store forecast results in lake for future analysis
            forecast_path = f"forecasts/{target_column}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            await self._save_forecast_to_lake(forecast, forecast_path)
            
            return {
                "forecast": forecast,
                "storage_path": forecast_path,
                "metadata": {
                    "created_at": datetime.utcnow().isoformat(),
                    "horizon_days": horizon_days,
                    "confidence_interval": confidence_interval
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to create forecast: {e}")
            raise
            
    async def stream_analytics_data(self,
                                   stream_config: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream real-time analytics data
        
        Args:
            stream_config: Streaming configuration
            
        Yields:
            Analytics data updates
        """
        stream_id = f"stream_{datetime.utcnow().timestamp()}"
        
        try:
            # Set up WebSocket connection
            ws_url = f"ws://analytics-service:8000/api/v1/ws/analytics/{stream_id}"
            
            async with httpx.AsyncClient() as client:
                async with client.websocket(ws_url) as ws:
                    # Send configuration
                    await ws.send_json(stream_config)
                    
                    # Stream data
                    while True:
                        message = await ws.receive_json()
                        
                        # Enrich with federated data if needed
                        if stream_config.get("enrich_with_federated"):
                            enriched = await self._enrich_stream_data(message)
                            yield enriched
                        else:
                            yield message
                            
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            raise
            
    async def get_platform_metrics(self,
                                  scope: str = "platform",
                                  time_range: str = "1h",
                                  service_filter: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Get platform-wide analytics metrics
        
        Args:
            scope: Metrics scope (platform, service, etc.)
            time_range: Time range for metrics
            service_filter: Optional service filter
            
        Returns:
            Platform metrics
        """
        try:
            # Get metrics from analytics service
            params = {
                "time_range": time_range
            }
            
            if service_filter:
                params["service_id"] = ",".join(service_filter)
                
            response = await self.http_client.get(
                f"{self.analytics_service_url}/api/v1/monitor/{scope}",
                params=params
            )
            response.raise_for_status()
            
            metrics = response.json()
            
            # Enrich with data platform specific metrics
            platform_metrics = await self._get_data_platform_metrics(time_range)
            metrics["data_platform"] = platform_metrics
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get platform metrics: {e}")
            raise
            
    async def export_analytics_results(self,
                                     query_id: str,
                                     export_format: str = "parquet",
                                     destination: Optional[str] = None) -> str:
        """
        Export analytics results to data lake
        
        Args:
            query_id: Query result ID
            export_format: Export format
            destination: Optional destination path
            
        Returns:
            Export path
        """
        try:
            # Get query results from cache
            cache_key = f"analytics_query_{query_id}"
            result = await self.cache_manager.get(cache_key)
            
            if not result:
                raise ValueError(f"Query results not found: {query_id}")
                
            # Convert to DataFrame
            df = pd.DataFrame(result["data"]["rows"], 
                            columns=[col["name"] for col in result["data"]["columns"]])
            
            # Generate export path
            if not destination:
                destination = f"analytics_exports/{query_id}/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                
            # Save to lake
            spark_df = self.lake_manager.spark.createDataFrame(df)
            
            if export_format == "parquet":
                spark_df.write.mode("overwrite").parquet(
                    f"{self.lake_manager.base_path}/{destination}"
                )
            elif export_format == "csv":
                spark_df.write.mode("overwrite").csv(
                    f"{self.lake_manager.base_path}/{destination}",
                    header=True
                )
            else:
                raise ValueError(f"Unsupported format: {export_format}")
                
            # Track lineage
            await self._track_export_lineage(query_id, destination)
            
            return destination
            
        except Exception as e:
            logger.error(f"Failed to export analytics results: {e}")
            raise
            
    # Helper methods
    
    def _enrich_query_with_time_filter(self, query: str, time_range: str) -> str:
        """Add time filter to query"""
        # Parse time range
        now = datetime.utcnow()
        time_map = {
            "1h": timedelta(hours=1),
            "1d": timedelta(days=1),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30),
            "90d": timedelta(days=90)
        }
        
        delta = time_map.get(time_range, timedelta(days=7))
        start_time = now - delta
        
        # Add time filter if not present
        if "WHERE" in query.upper():
            return f"{query} AND timestamp >= '{start_time.isoformat()}'"
        else:
            return f"{query} WHERE timestamp >= '{start_time.isoformat()}'"
            
    def _get_cache_ttl(self, time_range: str) -> int:
        """Get cache TTL based on time range"""
        ttl_map = {
            "1h": 300,      # 5 minutes
            "1d": 3600,     # 1 hour
            "7d": 14400,    # 4 hours
            "30d": 86400,   # 1 day
            "90d": 172800   # 2 days
        }
        return ttl_map.get(time_range, 3600)
        
    async def _schedule_dashboard_refresh(self, dashboard_id: str, interval: int):
        """Schedule dashboard data refresh"""
        async def refresh_task():
            while dashboard_id in self.ws_connections:
                try:
                    # Refresh dashboard data
                    await self._refresh_dashboard_data(dashboard_id)
                    await asyncio.sleep(interval)
                except Exception as e:
                    logger.error(f"Dashboard refresh error: {e}")
                    await asyncio.sleep(interval)
                    
        asyncio.create_task(refresh_task())
        
    async def _refresh_dashboard_data(self, dashboard_id: str):
        """Refresh dashboard data"""
        # Get dashboard config
        response = await self.http_client.get(
            f"{self.analytics_service_url}/api/v1/dashboards/{dashboard_id}"
        )
        dashboard = response.json()
        
        # Refresh each data source
        for source in dashboard["config"]["data_sources"]:
            if source["type"] == "federated":
                result = await self.federated_engine.execute_query(source["query"])
                # Update dashboard data
                await self.http_client.post(
                    f"{self.analytics_service_url}/api/v1/dashboards/{dashboard_id}/data",
                    json={"source_id": source["id"], "data": result}
                )
                
    async def _enrich_stream_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich streaming data with federated context"""
        # Example: Add related data from federated sources
        if "entity_id" in data:
            query = f"SELECT * FROM entities WHERE id = '{data['entity_id']}'"
            result = await self.federated_engine.execute_query(query)
            if result["rows"]:
                data["entity_context"] = result["rows"][0]
                
        return data
        
    async def _get_data_platform_metrics(self, time_range: str) -> Dict[str, Any]:
        """Get data platform specific metrics"""
        return {
            "lake_size_gb": await self.lake_manager.get_total_size() / (1024**3),
            "active_pipelines": await self._count_active_pipelines(),
            "data_quality_score": await self.quality_profiler.get_overall_score(),
            "federated_queries_per_min": await self._get_query_rate(),
            "ingestion_rate_mbps": await self._get_ingestion_rate()
        }
        
    async def _count_active_pipelines(self) -> int:
        """Count active pipelines"""
        # Implementation would query pipeline coordinator
        return 0
        
    async def _get_query_rate(self) -> float:
        """Get federated query rate"""
        # Implementation would get from metrics
        return 0.0
        
    async def _get_ingestion_rate(self) -> float:
        """Get data ingestion rate"""
        # Implementation would get from metrics
        return 0.0
        
    async def _save_forecast_to_lake(self, forecast: Dict[str, Any], path: str):
        """Save forecast results to data lake"""
        df = pd.DataFrame(forecast["predictions"])
        spark_df = self.lake_manager.spark.createDataFrame(df)
        spark_df.write.mode("overwrite").parquet(f"{self.lake_manager.base_path}/{path}")
        
    async def _track_export_lineage(self, query_id: str, destination: str):
        """Track lineage for exported data"""
        # Implementation would use lineage tracker
        pass
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 