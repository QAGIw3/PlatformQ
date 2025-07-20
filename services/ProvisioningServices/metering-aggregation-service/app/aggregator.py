"""Metrics Aggregator

Aggregates usage and cost data from CloudKitty and OpenMeter.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .config import Settings

logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates metrics from multiple sources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        
        # CloudKitty client
        self.cloudkitty_base_url = settings.cloudkitty_url
        self.cloudkitty_client = httpx.AsyncClient(timeout=30.0)
        
        # OpenMeter client
        self.openmeter_base_url = settings.openmeter_url
        self.openmeter_client = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "Authorization": f"Bearer {settings.openmeter_api_key}" if settings.openmeter_api_key else ""
            }
        )
        
        # Cache for frequently accessed data
        self._cache: Dict[str, Any] = {}
        self._cache_ttl = 300  # 5 minutes
    
    async def initialize(self):
        """Initialize aggregator connections"""
        logger.info("Initializing metrics aggregator")
    
    async def close(self):
        """Close client connections"""
        await self.cloudkitty_client.aclose()
        await self.openmeter_client.aclose()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_cloudkitty_summary(
        self,
        tenant_id: str,
        start_date: datetime,
        end_date: datetime,
        group_by: List[str]
    ) -> Dict[str, Any]:
        """Get cost summary from CloudKitty"""
        
        endpoint = f"{self.cloudkitty_base_url}/v2/summary"
        
        params = {
            "begin": start_date.isoformat(),
            "end": end_date.isoformat(),
            "groupby": group_by + ["tenant_id"],
            "filters": {
                "tenant_id": tenant_id
            }
        }
        
        response = await self.cloudkitty_client.get(endpoint, params=params)
        response.raise_for_status()
        
        return response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_openmeter_metrics(
        self,
        meter_slug: str,
        tenant_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get usage metrics from OpenMeter"""
        
        endpoint = f"{self.openmeter_base_url}/api/v1/meters/{meter_slug}/query"
        
        query = {}
        
        if tenant_id:
            query["filterGroupBy"] = {"tenant_id": [tenant_id]}
        
        if start_date:
            query["from"] = start_date.isoformat()
        
        if end_date:
            query["to"] = end_date.isoformat()
        
        if group_by:
            query["groupBy"] = group_by
        
        response = await self.openmeter_client.post(endpoint, json=query)
        response.raise_for_status()
        
        return response.json()
    
    async def get_realtime_metrics(
        self,
        meter_slug: str,
        tenant_id: str,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get real-time metrics from OpenMeter"""
        
        # Use values endpoint for real-time data
        endpoint = f"{self.openmeter_base_url}/api/v1/meters/{meter_slug}/values"
        
        params = {
            "subject": tenant_id,
            "from": start_time.isoformat(),
            "to": end_time.isoformat()
        }
        
        response = await self.openmeter_client.get(endpoint, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Process into time series
        time_series = []
        for value in data.get("data", []):
            time_series.append({
                "timestamp": value["time"],
                "value": value["value"],
                "metadata": value.get("metadata", {})
            })
        
        return {
            "meter": meter_slug,
            "tenant_id": tenant_id,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "data_points": len(time_series),
            "time_series": time_series
        }
    
    async def get_usage_patterns(self, tenant_id: str) -> Dict[str, Any]:
        """Analyze usage patterns for recommendations"""
        
        # Get last 30 days of data
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=30)
        
        # Fetch data from multiple meters in parallel
        meters = ["compute_hours", "storage_gb_hours", "api_calls", "data_transfer_gb"]
        
        tasks = []
        for meter in meters:
            task = self.get_openmeter_metrics(
                meter_slug=meter,
                tenant_id=tenant_id,
                start_date=start_date,
                end_date=end_date,
                group_by=["hour"]
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        patterns = {}
        for meter, result in zip(meters, results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching {meter}: {result}")
                patterns[meter] = {"error": str(result)}
            else:
                patterns[meter] = self._analyze_pattern(result)
        
        return patterns
    
    def _analyze_pattern(self, metric_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze usage pattern from metric data"""
        
        # Extract values
        values = []
        for point in metric_data.get("data", []):
            values.append(point.get("value", 0))
        
        if not values:
            return {"pattern": "no_data"}
        
        # Calculate statistics
        avg_usage = sum(values) / len(values)
        max_usage = max(values)
        min_usage = min(values)
        
        # Detect patterns
        pattern = {
            "average": avg_usage,
            "maximum": max_usage,
            "minimum": min_usage,
            "variance": sum((x - avg_usage) ** 2 for x in values) / len(values),
            "trend": "stable"  # Could implement trend detection
        }
        
        # Detect idle periods (usage < 10% of average)
        idle_threshold = avg_usage * 0.1
        idle_count = sum(1 for v in values if v < idle_threshold)
        pattern["idle_percentage"] = (idle_count / len(values)) * 100
        
        return pattern
    
    async def check_cloudkitty_health(self) -> bool:
        """Check CloudKitty service health"""
        try:
            response = await self.cloudkitty_client.get(
                f"{self.cloudkitty_base_url}/v2/info",
                timeout=5.0
            )
            return response.status_code == 200
        except:
            return False
    
    async def check_openmeter_health(self) -> bool:
        """Check OpenMeter service health"""
        try:
            response = await self.openmeter_client.get(
                f"{self.openmeter_base_url}/api/v1/meters",
                timeout=5.0
            )
            return response.status_code == 200
        except:
            return False
    
    async def get_hierarchical_costs(
        self,
        hierarchy_level: str,
        entity_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Dict[str, Any]:
        """Get costs for hierarchical entities (reseller/customer/tenant)"""
        
        # Build appropriate filter based on hierarchy level
        if hierarchy_level == "reseller":
            filters = {"reseller_id": entity_id}
            group_by = ["customer_id", "service", "plan"]
        elif hierarchy_level == "customer":
            filters = {"customer_id": entity_id}
            group_by = ["tenant_id", "service", "plan"]
        else:  # tenant
            filters = {"tenant_id": entity_id}
            group_by = ["service", "plan"]
        
        endpoint = f"{self.cloudkitty_base_url}/v2/summary"
        
        params = {
            "begin": start_date.isoformat(),
            "end": end_date.isoformat(),
            "groupby": group_by,
            "filters": filters
        }
        
        response = await self.cloudkitty_client.get(endpoint, params=params)
        response.raise_for_status()
        
        return response.json() 