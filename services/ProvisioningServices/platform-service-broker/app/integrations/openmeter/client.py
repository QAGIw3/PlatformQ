"""OpenMeter integration for real-time usage analytics

Streams usage events to OpenMeter for real-time analytics and metering.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import json
from enum import Enum

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ...models.osb_models import HierarchicalTenant

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """OpenMeter event types"""
    PROVISION = "provision"
    DEPROVISION = "deprovision"
    USAGE = "usage"
    RESIZE = "resize"
    BIND = "bind"
    UNBIND = "unbind"


class OpenMeterClient:
    """Client for OpenMeter usage analytics"""
    
    def __init__(self, config: Dict[str, Any]):
        self.enabled = config.get("openmeter", {}).get("enabled", True)
        self.base_url = config.get("openmeter", {}).get("url", "http://openmeter:8080")
        self.api_key = config.get("openmeter", {}).get("api_key")
        
        # Initialize HTTP client with auth
        headers = {
            "Content-Type": "application/cloudevents+json",
            "Accept": "application/json"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=headers
        )
        
        # Event source identifier
        self.event_source = "platform-service-broker"
        
        # Meter configurations
        self.meters = {
            "compute_hours": {
                "slug": "compute_hours",
                "description": "Compute instance hours by flavor",
                "aggregation": "SUM",
                "windowSize": "HOUR",
                "groupBy": ["tenant_id", "customer_id", "reseller_id", "flavor"]
            },
            "storage_gb_hours": {
                "slug": "storage_gb_hours",
                "description": "Storage GB-hours by type",
                "aggregation": "SUM",
                "windowSize": "HOUR",
                "groupBy": ["tenant_id", "customer_id", "reseller_id", "volume_type"]
            },
            "api_calls": {
                "slug": "api_calls",
                "description": "API calls by service and operation",
                "aggregation": "COUNT",
                "windowSize": "MINUTE",
                "groupBy": ["tenant_id", "service_id", "operation"]
            },
            "data_transfer_gb": {
                "slug": "data_transfer_gb",
                "description": "Data transfer in GB",
                "aggregation": "SUM",
                "windowSize": "HOUR",
                "groupBy": ["tenant_id", "direction", "region"]
            },
            "service_instances": {
                "slug": "service_instances",
                "description": "Active service instances",
                "aggregation": "COUNT",
                "windowSize": "HOUR",
                "groupBy": ["tenant_id", "service_id", "plan_id"]
            }
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def ingest_event(
        self,
        event_type: EventType,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Ingest usage event to OpenMeter
        
        Args:
            event_type: Type of event
            tenant: Hierarchical tenant information
            service_id: Service identifier
            instance_id: Instance identifier
            data: Event data and metrics
            
        Returns:
            bool: Success status
        """
        if not self.enabled:
            logger.debug("OpenMeter integration disabled, skipping event ingestion")
            return True
        
        try:
            # Create CloudEvents format event
            event = self._create_cloudevent(
                event_type, tenant, service_id, instance_id, data
            )
            
            # Ingest to OpenMeter
            endpoint = f"{self.base_url}/api/v1/events"
            
            response = await self.client.post(
                endpoint,
                json=event
            )
            
            if response.status_code not in (200, 201, 202, 204):
                logger.error(
                    f"Failed to ingest event to OpenMeter: {response.status_code} {response.text}"
                )
                return False
            
            logger.debug(
                f"Successfully ingested {event_type} event for {service_id}/{instance_id}"
            )
            
            # Also send specific meter events
            await self._send_meter_events(event_type, tenant, service_id, data)
            
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting event to OpenMeter: {e}")
            return False
    
    def _create_cloudevent(
        self,
        event_type: EventType,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create CloudEvent format event"""
        
        # CloudEvents v1.0 format
        event = {
            "specversion": "1.0",
            "id": f"{instance_id}-{datetime.now(timezone.utc).timestamp()}",
            "source": self.event_source,
            "type": f"io.platformq.{event_type}",
            "time": datetime.now(timezone.utc).isoformat(),
            "subject": f"{service_id}/{instance_id}",
            "data": {
                "tenant": {
                    "reseller_id": tenant.reseller_id,
                    "reseller_name": tenant.reseller_name,
                    "customer_id": tenant.customer_id,
                    "customer_name": tenant.customer_name,
                    "tenant_id": tenant.tenant_id,
                    "tenant_name": tenant.tenant_name
                },
                "service_id": service_id,
                "instance_id": instance_id,
                "event_type": event_type,
                **data
            }
        }
        
        return event
    
    async def _send_meter_events(
        self,
        event_type: EventType,
        tenant: HierarchicalTenant,
        service_id: str,
        data: Dict[str, Any]
    ) -> None:
        """Send specific meter events based on event type"""
        
        meter_events = []
        
        if event_type == EventType.USAGE:
            # Compute hours meter
            if service_id == "openstack-compute" and "hours" in data:
                meter_events.append({
                    "specversion": "1.0",
                    "id": f"compute-{datetime.now(timezone.utc).timestamp()}",
                    "source": self.event_source,
                    "type": "compute_hours",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "value": data["hours"],
                        "tenant_id": tenant.tenant_id,
                        "customer_id": tenant.customer_id,
                        "reseller_id": tenant.reseller_id,
                        "flavor": data.get("instance_type", "unknown")
                    }
                })
            
            # Storage GB-hours meter
            elif service_id == "openstack-storage" and "size_gb" in data:
                meter_events.append({
                    "specversion": "1.0",
                    "id": f"storage-{datetime.now(timezone.utc).timestamp()}",
                    "source": self.event_source,
                    "type": "storage_gb_hours",
                    "time": datetime.now(timezone.utc).isoformat(),
                    "data": {
                        "value": data["size_gb"] * data.get("hours", 1),
                        "tenant_id": tenant.tenant_id,
                        "customer_id": tenant.customer_id,
                        "reseller_id": tenant.reseller_id,
                        "volume_type": data.get("volume_type", "standard")
                    }
                })
        
        # API calls meter (for all event types)
        meter_events.append({
            "specversion": "1.0",
            "id": f"api-{datetime.now(timezone.utc).timestamp()}",
            "source": self.event_source,
            "type": "api_calls",
            "time": datetime.now(timezone.utc).isoformat(),
            "data": {
                "value": 1,
                "tenant_id": tenant.tenant_id,
                "service_id": service_id,
                "operation": event_type
            }
        })
        
        # Send meter events
        for event in meter_events:
            try:
                endpoint = f"{self.base_url}/api/v1/events"
                response = await self.client.post(endpoint, json=event)
                if response.status_code not in (200, 201, 202, 204):
                    logger.warning(f"Failed to send meter event: {response.text}")
            except Exception as e:
                logger.warning(f"Error sending meter event: {e}")
    
    async def create_meter(self, meter_config: Dict[str, Any]) -> bool:
        """Create a new meter in OpenMeter
        
        Args:
            meter_config: Meter configuration
            
        Returns:
            bool: Success status
        """
        if not self.enabled:
            return True
        
        try:
            endpoint = f"{self.base_url}/api/v1/meters"
            
            response = await self.client.post(
                endpoint,
                json=meter_config
            )
            
            if response.status_code == 201:
                logger.info(f"Created meter: {meter_config['slug']}")
                return True
            elif response.status_code == 409:
                logger.debug(f"Meter already exists: {meter_config['slug']}")
                return True
            else:
                logger.error(
                    f"Failed to create meter: {response.status_code} {response.text}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error creating meter: {e}")
            return False
    
    async def initialize_meters(self) -> None:
        """Initialize all configured meters"""
        for meter_slug, meter_config in self.meters.items():
            await self.create_meter(meter_config)
    
    async def query_usage(
        self,
        meter_slug: str,
        tenant_id: Optional[str] = None,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None,
        group_by: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """Query usage data from OpenMeter
        
        Args:
            meter_slug: Meter identifier
            tenant_id: Optional tenant filter
            from_time: Start time
            to_time: End time
            group_by: Group by fields
            
        Returns:
            Query results or None if error
        """
        if not self.enabled:
            return None
        
        try:
            endpoint = f"{self.base_url}/api/v1/meters/{meter_slug}/query"
            
            # Build query
            query = {}
            
            if tenant_id:
                query["filterGroupBy"] = {"tenant_id": [tenant_id]}
            
            if from_time:
                query["from"] = from_time.isoformat()
            
            if to_time:
                query["to"] = to_time.isoformat()
            
            if group_by:
                query["groupBy"] = group_by
            
            response = await self.client.post(
                endpoint,
                json=query
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(
                    f"Failed to query usage: {response.status_code} {response.text}"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error querying usage: {e}")
            return None
    
    async def get_meter_values(
        self,
        meter_slug: str,
        subject: Optional[str] = None,
        from_time: Optional[datetime] = None,
        to_time: Optional[datetime] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """Get meter values
        
        Args:
            meter_slug: Meter identifier
            subject: Optional subject filter
            from_time: Start time
            to_time: End time
            
        Returns:
            List of values or None if error
        """
        if not self.enabled:
            return None
        
        try:
            endpoint = f"{self.base_url}/api/v1/meters/{meter_slug}/values"
            
            params = {}
            if subject:
                params["subject"] = subject
            if from_time:
                params["from"] = from_time.isoformat()
            if to_time:
                params["to"] = to_time.isoformat()
            
            response = await self.client.get(endpoint, params=params)
            
            if response.status_code == 200:
                return response.json().get("data", [])
            else:
                logger.error(
                    f"Failed to get meter values: {response.status_code}"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error getting meter values: {e}")
            return None
    
    async def create_customer(
        self,
        customer_id: str,
        name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Create customer in OpenMeter for billing
        
        Args:
            customer_id: Customer identifier
            name: Customer name
            metadata: Additional metadata
            
        Returns:
            bool: Success status
        """
        if not self.enabled:
            return True
        
        try:
            endpoint = f"{self.base_url}/api/v1/customers"
            
            customer = {
                "id": customer_id,
                "name": name,
                "metadata": metadata or {}
            }
            
            response = await self.client.post(endpoint, json=customer)
            
            if response.status_code in (201, 409):
                return True
            else:
                logger.error(
                    f"Failed to create customer: {response.status_code} {response.text}"
                )
                return False
                
        except Exception as e:
            logger.error(f"Error creating customer: {e}")
            return False
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 