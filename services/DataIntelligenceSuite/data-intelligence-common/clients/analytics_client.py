"""
Analytics Engine Service Client

Enhanced client for analytics service with built-in patterns.
"""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass

from .base import RESTClient, ClientConfig, cached, monitored, retry, RetryConfig
from ..models.data_models import Dataset, DataSchema
from ..models.processing_models import JobStatus

logger = __import__('logging').getLogger(__name__)


@dataclass
class AnalyticsClientConfig(ClientConfig):
    """Analytics client specific configuration"""
    name: str = "analytics-engine"
    base_url: str = "http://analytics-engine-service:8000"
    
    # Analytics specific
    default_engine: str = "spark"
    max_query_results: int = 10000
    query_timeout: timedelta = timedelta(minutes=5)
    
    # Caching
    cache_query_results: bool = True
    cache_ttl: timedelta = timedelta(minutes=15)


class AnalyticsClient(RESTClient):
    """
    Enhanced client for Analytics Engine Service.
    
    Features:
    - Automatic retry with exponential backoff
    - Query result caching
    - Circuit breaker for external queries
    - Metrics collection
    - Authentication handling
    """
    
    def __init__(self, config: Optional[AnalyticsClientConfig] = None, **kwargs):
        super().__init__(config or AnalyticsClientConfig(), **kwargs)
        
    # Query Operations
    
    @monitored("analytics.query")
    @cached(ttl=timedelta(minutes=15))
    @retry(RetryConfig(max_attempts=3))
    async def execute_query(
        self,
        query: str,
        engine: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """
        Execute analytics query with caching and retry.
        
        Args:
            query: SQL or analytics query
            engine: Query engine (spark, trino, etc.)
            parameters: Query parameters
            timeout: Query timeout
            
        Returns:
            Query results
        """
        data = {
            "query": query,
            "engine": engine or self.config.default_engine,
            "parameters": parameters or {},
            "timeout_seconds": int((timeout or self.config.query_timeout).total_seconds())
        }
        
        return await self.post("/api/v1/query/execute", data=data)
        
    @monitored("analytics.query_async")
    async def submit_query(
        self,
        query: str,
        engine: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        callback_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """Submit query for async execution"""
        data = {
            "query": query,
            "engine": engine or self.config.default_engine,
            "parameters": parameters or {},
            "callback_url": callback_url
        }
        
        return await self.post("/api/v1/query/submit", data=data)
        
    @monitored("analytics.query_status")
    @cached(ttl=timedelta(seconds=30))
    async def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """Get query execution status"""
        return await self.get(f"/api/v1/query/{query_id}/status")
        
    @monitored("analytics.query_results")
    @cached(ttl=timedelta(minutes=10))
    async def get_query_results(
        self,
        query_id: str,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get query results with pagination"""
        params = {
            "offset": offset,
            "limit": limit or self.config.max_query_results
        }
        
        return await self.get(f"/api/v1/query/{query_id}/results", params=params)
        
    @monitored("analytics.query_cancel")
    async def cancel_query(self, query_id: str) -> Dict[str, Any]:
        """Cancel running query"""
        return await self.post(f"/api/v1/query/{query_id}/cancel")
        
    # Dataset Operations
    
    @monitored("analytics.dataset_analyze")
    @cached(ttl=timedelta(hours=1))
    async def analyze_dataset(
        self,
        dataset_id: str,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Analyze dataset statistics.
        
        Args:
            dataset_id: Dataset identifier
            columns: Columns to analyze (None for all)
            sample_size: Sample size for analysis
            
        Returns:
            Dataset statistics and profiling results
        """
        data = {
            "dataset_id": dataset_id,
            "columns": columns,
            "sample_size": sample_size
        }
        
        return await self.post("/api/v1/dataset/analyze", data=data)
        
    @monitored("analytics.dataset_profile")
    @cached(ttl=timedelta(hours=2))
    async def profile_dataset(
        self,
        dataset_id: str,
        profiling_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Deep dataset profiling"""
        data = {
            "dataset_id": dataset_id,
            "config": profiling_config or {}
        }
        
        return await self.post("/api/v1/dataset/profile", data=data)
        
    @monitored("analytics.dataset_preview")
    @cached(ttl=timedelta(minutes=30))
    async def preview_dataset(
        self,
        dataset_id: str,
        limit: int = 100,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Preview dataset records"""
        params = {
            "limit": limit,
            "columns": ",".join(columns) if columns else None
        }
        
        return await self.get(f"/api/v1/dataset/{dataset_id}/preview", params=params)
        
    # Aggregation Operations
    
    @monitored("analytics.aggregate")
    @cached(ttl=timedelta(minutes=30))
    async def aggregate(
        self,
        dataset_id: str,
        group_by: List[str],
        aggregations: Dict[str, str],
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Perform aggregations on dataset.
        
        Args:
            dataset_id: Dataset to aggregate
            group_by: Grouping columns
            aggregations: Column -> aggregation function mapping
            filters: Optional filters
            
        Returns:
            Aggregation results
        """
        data = {
            "dataset_id": dataset_id,
            "group_by": group_by,
            "aggregations": aggregations,
            "filters": filters or {}
        }
        
        return await self.post("/api/v1/aggregate", data=data)
        
    @monitored("analytics.timeseries")
    @cached(ttl=timedelta(minutes=15))
    async def timeseries_analysis(
        self,
        dataset_id: str,
        time_column: str,
        value_columns: List[str],
        granularity: str = "day",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Time series analysis"""
        data = {
            "dataset_id": dataset_id,
            "time_column": time_column,
            "value_columns": value_columns,
            "granularity": granularity,
            "start_time": start_time.isoformat() if start_time else None,
            "end_time": end_time.isoformat() if end_time else None
        }
        
        return await self.post("/api/v1/timeseries", data=data)
        
    # Join Operations
    
    @monitored("analytics.join")
    async def join_datasets(
        self,
        left_dataset: str,
        right_dataset: str,
        join_keys: Union[str, List[str]],
        join_type: str = "inner",
        output_dataset: Optional[str] = None
    ) -> Dict[str, Any]:
        """Join two datasets"""
        if isinstance(join_keys, str):
            join_keys = [join_keys]
            
        data = {
            "left_dataset": left_dataset,
            "right_dataset": right_dataset,
            "join_keys": join_keys,
            "join_type": join_type,
            "output_dataset": output_dataset
        }
        
        return await self.post("/api/v1/join", data=data)
        
    # Transform Operations
    
    @monitored("analytics.transform")
    async def transform_dataset(
        self,
        dataset_id: str,
        transformations: List[Dict[str, Any]],
        output_dataset: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Apply transformations to dataset.
        
        Args:
            dataset_id: Source dataset
            transformations: List of transformation specs
            output_dataset: Output dataset name
            
        Returns:
            Transformation job details
        """
        data = {
            "dataset_id": dataset_id,
            "transformations": transformations,
            "output_dataset": output_dataset
        }
        
        return await self.post("/api/v1/transform", data=data)
        
    @monitored("analytics.filter")
    async def filter_dataset(
        self,
        dataset_id: str,
        filters: Dict[str, Any],
        output_dataset: Optional[str] = None
    ) -> Dict[str, Any]:
        """Filter dataset records"""
        data = {
            "dataset_id": dataset_id,
            "filters": filters,
            "output_dataset": output_dataset
        }
        
        return await self.post("/api/v1/filter", data=data)
        
    # Export Operations
    
    @monitored("analytics.export")
    async def export_results(
        self,
        query_id: str,
        format: str = "parquet",
        destination: Optional[str] = None,
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Export query results"""
        data = {
            "query_id": query_id,
            "format": format,
            "destination": destination,
            "options": options or {}
        }
        
        return await self.post("/api/v1/export", data=data)
        
    # Visualization Support
    
    @monitored("analytics.chart_data")
    @cached(ttl=timedelta(minutes=10))
    async def get_chart_data(
        self,
        dataset_id: str,
        chart_type: str,
        config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get data formatted for visualization"""
        data = {
            "dataset_id": dataset_id,
            "chart_type": chart_type,
            "config": config
        }
        
        return await self.post("/api/v1/visualization/chart-data", data=data)
        
    # Job Management
    
    @monitored("analytics.job_list")
    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Dict[str, Any]:
        """List analytics jobs"""
        params = {
            "status": status.value if status else None,
            "limit": limit,
            "offset": offset
        }
        
        return await self.get("/api/v1/jobs", params=params)
        
    @monitored("analytics.job_details")
    @cached(ttl=timedelta(minutes=1))
    async def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """Get job details"""
        return await self.get(f"/api/v1/jobs/{job_id}")
        
    @monitored("analytics.job_logs")
    async def get_job_logs(
        self,
        job_id: str,
        tail: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get job execution logs"""
        params = {"tail": tail} if tail else {}
        return await self.get(f"/api/v1/jobs/{job_id}/logs", params=params)
        
    # Health and Metrics
    
    @monitored("analytics.health")
    async def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        return await self.get("/health")
        
    @monitored("analytics.metrics")
    @cached(ttl=timedelta(seconds=30))
    async def get_metrics(self) -> Dict[str, Any]:
        """Get service metrics"""
        return await self.get("/metrics") 