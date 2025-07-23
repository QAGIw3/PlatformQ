"""
Analytics Service Client

Client for analytics operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

from .base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Analytics query types"""
    SQL = "sql"
    DATAFRAME = "dataframe"
    AGGREGATION = "aggregation"
    TIME_SERIES = "time_series"
    STATISTICAL = "statistical"


class AggregationType(Enum):
    """Aggregation types"""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    STDDEV = "stddev"
    VARIANCE = "variance"
    PERCENTILE = "percentile"


@dataclass
class QueryResult:
    """Query result model"""
    query_id: str
    status: str
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    execution_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class Dashboard:
    """Dashboard model"""
    id: str
    name: str
    description: Optional[str] = None
    owner: str
    widgets: List[Dict[str, Any]] = field(default_factory=list)
    layout: Dict[str, Any] = field(default_factory=dict)
    filters: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class Report:
    """Report model"""
    id: str
    name: str
    description: Optional[str] = None
    owner: str
    template: str
    parameters: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None
    format: str = "pdf"
    recipients: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Metric:
    """Metric definition"""
    id: str
    name: str
    formula: str
    description: Optional[str] = None
    dimensions: List[str] = field(default_factory=list)
    filters: Dict[str, Any] = field(default_factory=dict)
    aggregation: AggregationType = AggregationType.SUM
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnalyticsServiceClient(BaseServiceClient):
    """
    Client for analytics service operations.
    
    Features:
    - Query execution
    - Dashboard management
    - Report generation
    - Metric computation
    - Time series analysis
    """
    
    def __init__(self, config: Optional[ClientConfig] = None, **kwargs):
        if not config:
            config = ClientConfig(service_name="analytics-service")
        super().__init__(config, **kwargs)
        
    # Query Operations
    
    async def execute_query(
        self,
        query: str,
        query_type: QueryType = QueryType.SQL,
        parameters: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
        limit: Optional[int] = None
    ) -> QueryResult:
        """
        Execute analytics query.
        
        Args:
            query: Query string
            query_type: Type of query
            parameters: Query parameters
            timeout: Query timeout in seconds
            limit: Result limit
            
        Returns:
            Query result
        """
        data = {
            "query": query,
            "type": query_type.value,
            "parameters": parameters or {},
            "limit": limit
        }
        
        if timeout:
            data["timeout"] = timeout
            
        response = await self.post("/query", json_data=data)
        
        return QueryResult(
            query_id=response["query_id"],
            status=response["status"],
            rows=response.get("rows", []),
            columns=response.get("columns", []),
            row_count=response.get("row_count", 0),
            execution_time=response.get("execution_time", 0.0),
            metadata=response.get("metadata", {}),
            error=response.get("error")
        )
        
    async def get_query_status(self, query_id: str) -> Dict[str, Any]:
        """
        Get query execution status.
        
        Args:
            query_id: Query ID
            
        Returns:
            Query status information
        """
        return await self.get(f"/query/{query_id}/status")
        
    async def cancel_query(self, query_id: str) -> bool:
        """
        Cancel running query.
        
        Args:
            query_id: Query ID
            
        Returns:
            Success status
        """
        response = await self.post(f"/query/{query_id}/cancel")
        return response.get("success", False)
        
    async def get_query_history(
        self,
        user: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get query execution history.
        
        Args:
            user: Filter by user
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of query history entries
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if user:
            params["user"] = user
            
        response = await self.get("/query/history", params=params)
        return response.get("queries", [])
        
    # Dashboard Operations
    
    async def create_dashboard(
        self,
        name: str,
        description: Optional[str] = None,
        widgets: Optional[List[Dict[str, Any]]] = None,
        layout: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None
    ) -> Dashboard:
        """
        Create a new dashboard.
        
        Args:
            name: Dashboard name
            description: Dashboard description
            widgets: Dashboard widgets
            layout: Widget layout
            tags: Dashboard tags
            
        Returns:
            Created dashboard
        """
        data = {
            "name": name,
            "description": description,
            "widgets": widgets or [],
            "layout": layout or {},
            "tags": tags or []
        }
        
        response = await self.post("/dashboards", json_data=data)
        
        return Dashboard(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            owner=response["owner"],
            widgets=response.get("widgets", []),
            layout=response.get("layout", {}),
            filters=response.get("filters", []),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            tags=response.get("tags", [])
        )
        
    async def get_dashboard(self, dashboard_id: str) -> Optional[Dashboard]:
        """
        Get dashboard by ID.
        
        Args:
            dashboard_id: Dashboard ID
            
        Returns:
            Dashboard if found
        """
        try:
            response = await self.get(f"/dashboards/{dashboard_id}")
            
            return Dashboard(
                id=response["id"],
                name=response["name"],
                description=response.get("description"),
                owner=response["owner"],
                widgets=response.get("widgets", []),
                layout=response.get("layout", {}),
                filters=response.get("filters", []),
                created_at=response.get("created_at"),
                updated_at=response.get("updated_at"),
                tags=response.get("tags", [])
            )
        except Exception as e:
            logger.error(f"Failed to get dashboard {dashboard_id}: {e}")
            return None
            
    async def update_dashboard(
        self,
        dashboard_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        widgets: Optional[List[Dict[str, Any]]] = None,
        layout: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None
    ) -> Dashboard:
        """
        Update dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            name: New name
            description: New description
            widgets: New widgets
            layout: New layout
            tags: New tags
            
        Returns:
            Updated dashboard
        """
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if widgets is not None:
            data["widgets"] = widgets
        if layout is not None:
            data["layout"] = layout
        if tags is not None:
            data["tags"] = tags
            
        response = await self.patch(f"/dashboards/{dashboard_id}", json_data=data)
        
        return Dashboard(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            owner=response["owner"],
            widgets=response.get("widgets", []),
            layout=response.get("layout", {}),
            filters=response.get("filters", []),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at"),
            tags=response.get("tags", [])
        )
        
    async def delete_dashboard(self, dashboard_id: str) -> bool:
        """
        Delete dashboard.
        
        Args:
            dashboard_id: Dashboard ID
            
        Returns:
            Success status
        """
        response = await self.delete(f"/dashboards/{dashboard_id}")
        return response.get("success", False)
        
    async def list_dashboards(
        self,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dashboard]:
        """
        List dashboards.
        
        Args:
            owner: Filter by owner
            tags: Filter by tags
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of dashboards
        """
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if owner:
            params["owner"] = owner
        if tags:
            params["tags"] = ",".join(tags)
            
        response = await self.get("/dashboards", params=params)
        
        return [
            Dashboard(
                id=d["id"],
                name=d["name"],
                description=d.get("description"),
                owner=d["owner"],
                widgets=d.get("widgets", []),
                layout=d.get("layout", {}),
                filters=d.get("filters", []),
                created_at=d.get("created_at"),
                updated_at=d.get("updated_at"),
                tags=d.get("tags", [])
            )
            for d in response.get("dashboards", [])
        ]
        
    # Report Operations
    
    async def create_report(
        self,
        name: str,
        template: str,
        description: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        schedule: Optional[Dict[str, Any]] = None,
        format: str = "pdf",
        recipients: Optional[List[str]] = None
    ) -> Report:
        """
        Create a new report.
        
        Args:
            name: Report name
            template: Report template
            description: Report description
            parameters: Report parameters
            schedule: Report schedule (cron expression)
            format: Output format (pdf, excel, csv)
            recipients: Email recipients
            
        Returns:
            Created report
        """
        data = {
            "name": name,
            "template": template,
            "description": description,
            "parameters": parameters or {},
            "schedule": schedule,
            "format": format,
            "recipients": recipients or []
        }
        
        response = await self.post("/reports", json_data=data)
        
        return Report(
            id=response["id"],
            name=response["name"],
            description=response.get("description"),
            owner=response["owner"],
            template=response["template"],
            parameters=response.get("parameters", {}),
            schedule=response.get("schedule"),
            format=response.get("format", "pdf"),
            recipients=response.get("recipients", []),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at")
        )
        
    async def generate_report(
        self,
        report_id: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate report.
        
        Args:
            report_id: Report ID
            parameters: Runtime parameters
            
        Returns:
            Job ID for report generation
        """
        data = {
            "parameters": parameters or {}
        }
        
        response = await self.post(f"/reports/{report_id}/generate", json_data=data)
        return response["job_id"]
        
    async def get_report_status(
        self,
        report_id: str,
        job_id: str
    ) -> Dict[str, Any]:
        """
        Get report generation status.
        
        Args:
            report_id: Report ID
            job_id: Generation job ID
            
        Returns:
            Status information
        """
        return await self.get(f"/reports/{report_id}/jobs/{job_id}")
        
    async def download_report(
        self,
        report_id: str,
        job_id: str
    ) -> bytes:
        """
        Download generated report.
        
        Args:
            report_id: Report ID
            job_id: Generation job ID
            
        Returns:
            Report content
        """
        response = await self.get(
            f"/reports/{report_id}/jobs/{job_id}/download",
            raw_response=True
        )
        return response
        
    # Metric Operations
    
    async def create_metric(
        self,
        name: str,
        formula: str,
        description: Optional[str] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        aggregation: AggregationType = AggregationType.SUM
    ) -> Metric:
        """
        Create a new metric.
        
        Args:
            name: Metric name
            formula: Metric formula
            description: Metric description
            dimensions: Metric dimensions
            filters: Default filters
            aggregation: Aggregation type
            
        Returns:
            Created metric
        """
        data = {
            "name": name,
            "formula": formula,
            "description": description,
            "dimensions": dimensions or [],
            "filters": filters or {},
            "aggregation": aggregation.value
        }
        
        response = await self.post("/metrics", json_data=data)
        
        return Metric(
            id=response["id"],
            name=response["name"],
            formula=response["formula"],
            description=response.get("description"),
            dimensions=response.get("dimensions", []),
            filters=response.get("filters", {}),
            aggregation=AggregationType(response.get("aggregation", "sum")),
            metadata=response.get("metadata", {})
        )
        
    async def compute_metric(
        self,
        metric_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Compute metric values.
        
        Args:
            metric_id: Metric ID
            start_date: Start date
            end_date: End date
            dimensions: Dimensions to group by
            filters: Additional filters
            
        Returns:
            Computed metric values
        """
        params = {}
        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()
        if dimensions:
            params["dimensions"] = ",".join(dimensions)
            
        data = {
            "filters": filters or {}
        }
        
        return await self.post(f"/metrics/{metric_id}/compute", 
                              json_data=data, params=params)
        
    # Time Series Operations
    
    async def time_series_analysis(
        self,
        dataset_id: str,
        metric_column: str,
        time_column: str,
        analysis_type: str = "trend",
        granularity: str = "day",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        dimensions: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Perform time series analysis.
        
        Args:
            dataset_id: Dataset ID
            metric_column: Metric column name
            time_column: Time column name
            analysis_type: Type of analysis (trend, forecast, anomaly)
            granularity: Time granularity (hour, day, week, month)
            start_date: Start date
            end_date: End date
            dimensions: Grouping dimensions
            
        Returns:
            Analysis results
        """
        data = {
            "dataset_id": dataset_id,
            "metric_column": metric_column,
            "time_column": time_column,
            "analysis_type": analysis_type,
            "granularity": granularity,
            "dimensions": dimensions or []
        }
        
        if start_date:
            data["start_date"] = start_date.isoformat()
        if end_date:
            data["end_date"] = end_date.isoformat()
            
        return await self.post("/timeseries/analyze", json_data=data)
        
    # Statistical Operations
    
    async def statistical_analysis(
        self,
        dataset_id: str,
        columns: List[str],
        analysis_type: str = "descriptive",
        options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Perform statistical analysis.
        
        Args:
            dataset_id: Dataset ID
            columns: Columns to analyze
            analysis_type: Type of analysis
            options: Analysis options
            
        Returns:
            Analysis results
        """
        data = {
            "dataset_id": dataset_id,
            "columns": columns,
            "analysis_type": analysis_type,
            "options": options or {}
        }
        
        return await self.post("/statistics/analyze", json_data=data)
        
    async def correlation_analysis(
        self,
        dataset_id: str,
        columns: List[str],
        method: str = "pearson"
    ) -> Dict[str, Any]:
        """
        Perform correlation analysis.
        
        Args:
            dataset_id: Dataset ID
            columns: Columns to analyze
            method: Correlation method (pearson, spearman, kendall)
            
        Returns:
            Correlation matrix
        """
        data = {
            "dataset_id": dataset_id,
            "columns": columns,
            "method": method
        }
        
        return await self.post("/statistics/correlation", json_data=data)
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get analytics-specific configuration from Consul"""
        if self.consul_client:
            config = await self.consul_client.get_key(
                f"config/{self.config.service_name}/client"
            )
            return config or {}
        return {} 