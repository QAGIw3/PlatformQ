"""OpenMeter client for real-time usage tracking"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import settings

logger = logging.getLogger(__name__)


class OpenMeterClient:
    """Client for OpenMeter usage tracking service"""
    
    def __init__(self):
        self.base_url = settings.openmeter_url
        self.api_key = settings.openmeter_api_key
        self.namespace = settings.openmeter_namespace
        
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=30.0
        )
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def ingest_events(
        self,
        events: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Ingest usage events into OpenMeter"""
        
        # For dev environment, return mock response
        if settings.environment == "development":
            return {
                "status": "success",
                "processed": len(events),
                "errors": []
            }
        
        # Format events for OpenMeter
        formatted_events = []
        for event in events:
            formatted_events.append({
                "specversion": "1.0",
                "id": event.get("id"),
                "source": event.get("source", "settlement-coordinator"),
                "type": event.get("type", "compute.usage"),
                "subject": event.get("subject"),  # User/tenant ID
                "time": event.get("time", datetime.utcnow().isoformat()),
                "data": event.get("data", {})
            })
        
        try:
            response = await self.client.post(
                "/api/v1/events",
                json=formatted_events
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to ingest events: {e}")
            raise
    
    async def track_usage(
        self,
        settlement_id: str,
        tenant_id: str,
        resource_type: str,
        quantity: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Track resource usage for a settlement"""
        
        event = {
            "id": f"usage-{settlement_id}-{datetime.utcnow().timestamp()}",
            "source": "settlement-coordinator",
            "type": f"compute.{resource_type}.usage",
            "subject": tenant_id,
            "time": datetime.utcnow().isoformat(),
            "data": {
                "settlement_id": settlement_id,
                "resource_type": resource_type,
                "quantity": quantity,
                "unit": self._get_unit_for_resource(resource_type),
                "namespace": self.namespace,
                **(metadata or {})
            }
        }
        
        return await self.ingest_events([event])
    
    async def get_meter_values(
        self,
        meter_id: str,
        subject: str,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        window_size: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get meter values for a subject (tenant)"""
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "data": [
                    {
                        "subject": subject,
                        "windowStart": from_time.isoformat() if from_time else "2024-01-01T00:00:00Z",
                        "windowEnd": to_time.isoformat() if to_time else "2024-01-01T01:00:00Z",
                        "value": 1000.5,
                        "usage": 850.25
                    }
                ]
            }
        
        params = {
            "subject": subject
        }
        
        if from_time:
            params["from"] = from_time.isoformat()
        if to_time:
            params["to"] = to_time.isoformat()
        if window_size:
            params["windowSize"] = window_size
        
        try:
            response = await self.client.get(
                f"/api/v1/meters/{meter_id}/values",
                params=params
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get meter values: {e}")
            raise
    
    async def create_meter(
        self,
        meter_id: str,
        description: str,
        aggregation: str = "SUM",
        event_type: str = "compute.usage",
        value_property: str = "$.quantity",
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Create a new meter for tracking usage"""
        
        # For dev environment, return mock response
        if settings.environment == "development":
            return {
                "id": meter_id,
                "description": description,
                "aggregation": aggregation,
                "created": True
            }
        
        data = {
            "slug": meter_id,
            "description": description,
            "aggregation": aggregation,
            "eventType": event_type,
            "valueProperty": value_property,
            "groupBy": group_by or ["subject"]
        }
        
        try:
            response = await self.client.post(
                "/api/v1/meters",
                json=data
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create meter: {e}")
            raise
    
    async def get_usage_report(
        self,
        tenant_id: str,
        from_time: datetime,
        to_time: datetime,
        group_by: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Get usage report for a tenant"""
        
        # For dev environment, return mock data
        if settings.environment == "development":
            return {
                "tenant_id": tenant_id,
                "period": {
                    "from": from_time.isoformat(),
                    "to": to_time.isoformat()
                },
                "usage": {
                    "cpu": {"total": 1000, "unit": "core-hours"},
                    "gpu": {"total": 500, "unit": "gpu-hours"},
                    "memory": {"total": 2048, "unit": "gb-hours"},
                    "storage": {"total": 5000, "unit": "gb-hours"}
                },
                "cost_estimate": 1500.00
            }
        
        # Get usage for different resource types
        resource_types = ["cpu", "gpu", "memory", "storage"]
        usage_data = {}
        
        for resource_type in resource_types:
            meter_id = f"{self.namespace}-{resource_type}"
            try:
                meter_values = await self.get_meter_values(
                    meter_id=meter_id,
                    subject=tenant_id,
                    from_time=from_time,
                    to_time=to_time
                )
                
                if meter_values.get("data"):
                    total_usage = sum(item.get("value", 0) for item in meter_values["data"])
                    usage_data[resource_type] = {
                        "total": total_usage,
                        "unit": self._get_unit_for_resource(resource_type)
                    }
            except Exception as e:
                logger.warning(f"Failed to get usage for {resource_type}: {e}")
                usage_data[resource_type] = {"total": 0, "unit": self._get_unit_for_resource(resource_type)}
        
        return {
            "tenant_id": tenant_id,
            "period": {
                "from": from_time.isoformat(),
                "to": to_time.isoformat()
            },
            "usage": usage_data
        }
    
    async def reset_meter(
        self,
        meter_id: str,
        subject: Optional[str] = None
    ) -> Dict[str, Any]:
        """Reset meter values"""
        
        # For dev environment, return mock response
        if settings.environment == "development":
            return {
                "status": "reset",
                "meter_id": meter_id,
                "subject": subject
            }
        
        params = {}
        if subject:
            params["subject"] = subject
        
        try:
            response = await self.client.post(
                f"/api/v1/meters/{meter_id}/reset",
                params=params
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to reset meter: {e}")
            raise
    
    def _get_unit_for_resource(self, resource_type: str) -> str:
        """Get unit for resource type"""
        units = {
            "cpu": "core-hours",
            "gpu": "gpu-hours",
            "memory": "gb-hours",
            "storage": "gb-hours",
            "network": "gb"
        }
        return units.get(resource_type.lower(), "units")
    
    async def create_feature(
        self,
        feature_key: str,
        name: str,
        meter_ids: List[str]
    ) -> Dict[str, Any]:
        """Create a feature for entitlement tracking"""
        
        # For dev environment, return mock response
        if settings.environment == "development":
            return {
                "key": feature_key,
                "name": name,
                "meters": meter_ids,
                "created": True
            }
        
        data = {
            "key": feature_key,
            "name": name,
            "meterSlugsByKey": {meter_id: meter_id for meter_id in meter_ids}
        }
        
        try:
            response = await self.client.post(
                "/api/v1/features",
                json=data
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create feature: {e}")
            raise
    
    async def check_entitlement(
        self,
        subject: str,
        feature_key: str
    ) -> Dict[str, Any]:
        """Check if subject has access to a feature"""
        
        # For dev environment, return mock response
        if settings.environment == "development":
            return {
                "hasAccess": True,
                "isSoftLimit": False,
                "usage": 800,
                "limit": 1000
            }
        
        try:
            response = await self.client.get(
                f"/api/v1/subjects/{subject}/entitlements/{feature_key}"
            )
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            logger.error(f"Failed to check entitlement: {e}")
            raise 