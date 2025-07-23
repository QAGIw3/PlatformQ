"""
Apache Druid Client Integration

Provides high-level client for Apache Druid real-time analytics operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import requests
import json

logger = logging.getLogger(__name__)


@dataclass
class DruidConfig:
    """Configuration for Druid client"""
    broker_endpoint: str = "http://localhost:8082"
    coordinator_endpoint: str = "http://localhost:8081"
    overlord_endpoint: str = "http://localhost:8090"
    
    # Authentication
    auth_token: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    
    # Timeouts
    request_timeout: int = 30
    query_timeout: int = 300
    
    # Query defaults
    default_granularity: str = "day"
    default_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataSource:
    """Druid datasource information"""
    name: str
    properties: Dict[str, Any] = field(default_factory=dict)
    segments: List[Dict[str, Any]] = field(default_factory=list)
    num_segments: int = 0
    num_rows: int = 0
    size_bytes: int = 0
    min_time: Optional[datetime] = None
    max_time: Optional[datetime] = None


@dataclass
class QueryResult:
    """Druid query result"""
    query_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    result: List[Dict[str, Any]] = field(default_factory=list)
    duration_ms: Optional[int] = None


@dataclass
class IngestionSpec:
    """Druid ingestion specification"""
    type: str  # "index_parallel" or "kafka" or "kinesis"
    spec: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskStatus:
    """Druid task status"""
    task_id: str
    status: str
    type: str
    datasource: str
    created_time: datetime
    queue_insertion_time: Optional[datetime] = None
    status_code: Optional[str] = None
    duration: Optional[int] = None
    location: Optional[Dict[str, Any]] = None
    error_msg: Optional[str] = None


class DruidClient:
    """
    High-level client for Apache Druid operations.
    
    Features:
    - Native and SQL queries
    - Datasource management
    - Ingestion task management
    - Segment management
    - Real-time and batch ingestion
    """
    
    def __init__(self, config: DruidConfig):
        self.config = config
        self._session = requests.Session()
        
        # Set up authentication
        if config.auth_token:
            self._session.headers.update({
                "Authorization": f"Bearer {config.auth_token}"
            })
        elif config.username and config.password:
            self._session.auth = (config.username, config.password)
            
    def _request(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request"""
        kwargs.setdefault("timeout", self.config.request_timeout)
        
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        
        return response
        
    # Query operations
    
    def sql_query(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
        result_format: str = "object"
    ) -> QueryResult:
        """Execute SQL query"""
        url = f"{self.config.broker_endpoint}/druid/v2/sql"
        
        payload = {
            "query": query,
            "resultFormat": result_format,
            "context": context or self.config.default_context
        }
        
        if parameters:
            payload["parameters"] = parameters
            
        response = self._request(
            "POST",
            url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        result = response.json()
        
        return QueryResult(
            query_id=response.headers.get("X-Druid-Query-Id"),
            result=result if isinstance(result, list) else [result]
        )
        
    def native_query(
        self,
        query_type: str,
        datasource: str,
        intervals: List[str],
        granularity: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        post_aggregations: Optional[List[Dict[str, Any]]] = None,
        dimensions: Optional[List[Union[str, Dict[str, Any]]]] = None,
        metric: Optional[Union[str, Dict[str, Any]]] = None,
        threshold: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> QueryResult:
        """Execute native query"""
        url = f"{self.config.broker_endpoint}/druid/v2"
        
        # Build query
        query = {
            "queryType": query_type,
            "dataSource": datasource,
            "intervals": intervals,
            "granularity": granularity or self.config.default_granularity,
            "context": context or self.config.default_context
        }
        
        if filter:
            query["filter"] = filter
        if aggregations:
            query["aggregations"] = aggregations
        if post_aggregations:
            query["postAggregations"] = post_aggregations
        if dimensions:
            query["dimensions"] = dimensions
        if metric:
            query["metric"] = metric
        if threshold:
            query["threshold"] = threshold
            
        # Add any additional parameters
        query.update(kwargs)
        
        response = self._request(
            "POST",
            url,
            json=query,
            headers={"Content-Type": "application/json"}
        )
        
        result = response.json()
        
        return QueryResult(
            query_id=response.headers.get("X-Druid-Query-Id"),
            result=result if isinstance(result, list) else [result]
        )
        
    def timeseries_query(
        self,
        datasource: str,
        intervals: List[str],
        aggregations: List[Dict[str, Any]],
        granularity: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        post_aggregations: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute timeseries query"""
        return self.native_query(
            query_type="timeseries",
            datasource=datasource,
            intervals=intervals,
            aggregations=aggregations,
            granularity=granularity,
            filter=filter,
            post_aggregations=post_aggregations,
            context=context
        )
        
    def topn_query(
        self,
        datasource: str,
        intervals: List[str],
        dimension: Union[str, Dict[str, Any]],
        metric: Union[str, Dict[str, Any]],
        threshold: int,
        aggregations: List[Dict[str, Any]],
        granularity: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        post_aggregations: Optional[List[Dict[str, Any]]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute TopN query"""
        return self.native_query(
            query_type="topN",
            datasource=datasource,
            intervals=intervals,
            dimension=dimension,
            metric=metric,
            threshold=threshold,
            aggregations=aggregations,
            granularity=granularity,
            filter=filter,
            post_aggregations=post_aggregations,
            context=context
        )
        
    def groupby_query(
        self,
        datasource: str,
        intervals: List[str],
        dimensions: List[Union[str, Dict[str, Any]]],
        aggregations: List[Dict[str, Any]],
        granularity: Optional[str] = None,
        filter: Optional[Dict[str, Any]] = None,
        post_aggregations: Optional[List[Dict[str, Any]]] = None,
        having: Optional[Dict[str, Any]] = None,
        limit_spec: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> QueryResult:
        """Execute GroupBy query"""
        return self.native_query(
            query_type="groupBy",
            datasource=datasource,
            intervals=intervals,
            dimensions=dimensions,
            aggregations=aggregations,
            granularity=granularity,
            filter=filter,
            post_aggregations=post_aggregations,
            having=having,
            limitSpec=limit_spec,
            context=context
        )
        
    # Datasource operations
    
    def list_datasources(
        self,
        include_disabled: bool = False,
        full: bool = False
    ) -> List[DataSource]:
        """List all datasources"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources"
        
        params = {"includeDisabled": include_disabled, "full": full}
        
        response = self._request("GET", url, params=params)
        
        if full:
            # Full response includes detailed information
            datasources = []
            for ds_data in response.json():
                datasources.append(DataSource(
                    name=ds_data["name"],
                    properties=ds_data.get("properties", {}),
                    segments=ds_data.get("segments", []),
                    num_segments=len(ds_data.get("segments", [])),
                    # Additional processing would be needed for full stats
                ))
            return datasources
        else:
            # Simple response is just a list of names
            return [DataSource(name=name) for name in response.json()]
            
    def get_datasource(self, datasource: str) -> DataSource:
        """Get datasource details"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources/{datasource}"
        
        response = self._request("GET", url)
        ds_data = response.json()
        
        # Get segments for additional info
        segments_url = f"{url}/segments"
        segments_response = self._request("GET", segments_url)
        segments = segments_response.json()
        
        # Calculate stats
        num_rows = 0
        size_bytes = 0
        min_time = None
        max_time = None
        
        for segment in segments:
            if "num_rows" in segment:
                num_rows += segment["num_rows"]
            if "size" in segment:
                size_bytes += segment["size"]
                
            # Parse interval
            if "interval" in segment:
                start, end = segment["interval"].split("/")
                start_time = datetime.fromisoformat(start.replace("Z", "+00:00"))
                end_time = datetime.fromisoformat(end.replace("Z", "+00:00"))
                
                if not min_time or start_time < min_time:
                    min_time = start_time
                if not max_time or end_time > max_time:
                    max_time = end_time
                    
        return DataSource(
            name=datasource,
            properties=ds_data.get("properties", {}),
            segments=segments,
            num_segments=len(segments),
            num_rows=num_rows,
            size_bytes=size_bytes,
            min_time=min_time,
            max_time=max_time
        )
        
    def delete_datasource(self, datasource: str) -> bool:
        """Delete a datasource"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources/{datasource}"
        
        try:
            self._request("DELETE", url)
            return True
        except Exception:
            return False
            
    # Ingestion operations
    
    def submit_task(
        self,
        spec: Union[Dict[str, Any], IngestionSpec]
    ) -> str:
        """Submit an ingestion task"""
        url = f"{self.config.overlord_endpoint}/druid/indexer/v1/task"
        
        if isinstance(spec, IngestionSpec):
            payload = {"type": spec.type, "spec": spec.spec}
        else:
            payload = spec
            
        response = self._request(
            "POST",
            url,
            json=payload,
            headers={"Content-Type": "application/json"}
        )
        
        result = response.json()
        return result["task"]
        
    def get_task_status(self, task_id: str) -> TaskStatus:
        """Get task status"""
        url = f"{self.config.overlord_endpoint}/druid/indexer/v1/task/{task_id}/status"
        
        response = self._request("GET", url)
        status_data = response.json()
        
        return TaskStatus(
            task_id=task_id,
            status=status_data["status"]["status"],
            type=status_data["status"].get("type", "unknown"),
            datasource=status_data["status"].get("dataSource", "unknown"),
            created_time=datetime.fromisoformat(
                status_data["status"]["createdTime"].replace("Z", "+00:00")
            ),
            queue_insertion_time=datetime.fromisoformat(
                status_data["status"]["queueInsertionTime"].replace("Z", "+00:00")
            ) if status_data["status"].get("queueInsertionTime") else None,
            status_code=status_data["status"].get("statusCode"),
            duration=status_data["status"].get("duration"),
            location=status_data["status"].get("location"),
            error_msg=status_data["status"].get("errorMsg")
        )
        
    def list_tasks(
        self,
        state: Optional[str] = None,
        datasource: Optional[str] = None,
        created_time_interval: Optional[str] = None,
        max_tasks: Optional[int] = None,
        type: Optional[str] = None
    ) -> List[TaskStatus]:
        """List tasks"""
        url = f"{self.config.overlord_endpoint}/druid/indexer/v1/tasks"
        
        params = {}
        if state:
            params["state"] = state
        if datasource:
            params["datasource"] = datasource
        if created_time_interval:
            params["createdTimeInterval"] = created_time_interval
        if max_tasks:
            params["max"] = max_tasks
        if type:
            params["type"] = type
            
        response = self._request("GET", url, params=params)
        
        tasks = []
        for task_data in response.json():
            tasks.append(TaskStatus(
                task_id=task_data["id"],
                status=task_data.get("statusCode", "UNKNOWN"),
                type=task_data.get("type", "unknown"),
                datasource=task_data.get("dataSource", "unknown"),
                created_time=datetime.fromisoformat(
                    task_data["createdTime"].replace("Z", "+00:00")
                ),
                queue_insertion_time=datetime.fromisoformat(
                    task_data["queueInsertionTime"].replace("Z", "+00:00")
                ) if task_data.get("queueInsertionTime") else None,
                duration=task_data.get("duration"),
                location=task_data.get("location"),
                error_msg=task_data.get("errorMsg")
            ))
            
        return tasks
        
    def shutdown_task(self, task_id: str) -> Dict[str, Any]:
        """Shutdown a task"""
        url = f"{self.config.overlord_endpoint}/druid/indexer/v1/task/{task_id}/shutdown"
        
        response = self._request("POST", url)
        return response.json()
        
    # Segment operations
    
    def list_segments(
        self,
        datasource: str,
        interval: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List segments for a datasource"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources/{datasource}/segments"
        
        params = {}
        if interval:
            params["interval"] = interval
            
        response = self._request("GET", url, params=params)
        return response.json()
        
    def get_segment_metadata(
        self,
        datasource: str,
        segment_id: str
    ) -> Dict[str, Any]:
        """Get segment metadata"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources/{datasource}/segments/{segment_id}"
        
        response = self._request("GET", url)
        return response.json()
        
    def mark_segments_unused(
        self,
        datasource: str,
        interval: str
    ) -> int:
        """Mark segments as unused"""
        url = f"{self.config.coordinator_endpoint}/druid/coordinator/v1/datasources/{datasource}/markUnused"
        
        response = self._request(
            "POST",
            url,
            json={"interval": interval},
            headers={"Content-Type": "application/json"}
        )
        
        result = response.json()
        return result.get("numChangedSegments", 0)
        
    # Utility methods
    
    def create_batch_ingestion_spec(
        self,
        datasource: str,
        input_source: Dict[str, Any],
        input_format: Dict[str, Any],
        timestamp_spec: Dict[str, Any],
        dimensions_spec: Dict[str, Any],
        metrics_spec: Optional[List[Dict[str, Any]]] = None,
        granularity_spec: Optional[Dict[str, Any]] = None,
        transform_spec: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> IngestionSpec:
        """Create batch ingestion specification"""
        spec = {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": timestamp_spec,
                "dimensionsSpec": dimensions_spec,
                "metricsSpec": metrics_spec or [],
                "granularitySpec": granularity_spec or {
                    "type": "uniform",
                    "segmentGranularity": "day",
                    "queryGranularity": "none",
                    "rollup": False
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": input_source,
                "inputFormat": input_format
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 5000000,
                "maxRowsInMemory": 25000
            }
        }
        
        if transform_spec:
            spec["dataSchema"]["transformSpec"] = transform_spec
            
        # Add any additional configurations
        spec.update(kwargs)
        
        return IngestionSpec(type="index_parallel", spec=spec)
        
    def wait_for_task(
        self,
        task_id: str,
        timeout: Optional[int] = None,
        poll_interval: int = 5
    ) -> TaskStatus:
        """Wait for task completion"""
        import time
        
        start_time = time.time()
        timeout = timeout or self.config.query_timeout
        
        while True:
            status = self.get_task_status(task_id)
            
            if status.status in ["SUCCESS", "FAILED"]:
                return status
                
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Task {task_id} did not complete within {timeout}s"
                )
                
            time.sleep(poll_interval)
            
    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        urls = {
            "coordinator": f"{self.config.coordinator_endpoint}/status",
            "broker": f"{self.config.broker_endpoint}/status",
            "overlord": f"{self.config.overlord_endpoint}/status"
        }
        
        info = {}
        for component, url in urls.items():
            try:
                response = self._request("GET", url)
                info[component] = response.json()
            except Exception as e:
                info[component] = {"error": str(e)}
                
        return info 