"""CloudKitty integration for usage metering

Reports resource usage to CloudKitty for billing and cost tracking.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import asyncio
from decimal import Decimal

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from ...models.osb_models import HierarchicalTenant

logger = logging.getLogger(__name__)


class CloudKittyClient:
    """Client for CloudKitty rating and billing service"""
    
    def __init__(self, config: Dict[str, Any]):
        self.enabled = config.get("cloudkitty", {}).get("enabled", True)
        self.base_url = config.get("cloudkitty", {}).get("url", "http://cloudkitty:8889")
        self.api_version = "v2"
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Metric definitions for different services
        self.metric_definitions = {
            "openstack-compute": {
                "metric_name": "instance",
                "unit": "instance-hour",
                "groupby": ["flavor", "tenant_id", "project_id"]
            },
            "openstack-storage": {
                "metric_name": "volume.size",
                "unit": "GB-hour",
                "groupby": ["volume_type", "tenant_id", "project_id"]
            },
            "platform-cassandra": {
                "metric_name": "cassandra.keyspace",
                "unit": "keyspace-hour",
                "groupby": ["plan", "tenant_id", "replication_factor"]
            },
            "platform-ignite": {
                "metric_name": "ignite.cache",
                "unit": "cache-hour",
                "groupby": ["plan", "tenant_id", "cache_mode"]
            },
            "platform-pulsar": {
                "metric_name": "pulsar.namespace",
                "unit": "namespace-hour",
                "groupby": ["plan", "tenant_id", "tier"]
            },
            "platform-minio": {
                "metric_name": "minio.bucket",
                "unit": "bucket-hour",
                "groupby": ["plan", "tenant_id", "storage_class"]
            }
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def report_usage(
        self,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        usage_data: Dict[str, Any]
    ) -> bool:
        """Report usage data to CloudKitty
        
        Args:
            tenant: Hierarchical tenant information
            service_id: Service identifier (e.g., "openstack-compute")
            instance_id: Instance identifier
            usage_data: Usage metrics and metadata
            
        Returns:
            bool: Success status
        """
        if not self.enabled:
            logger.debug("CloudKitty integration disabled, skipping usage report")
            return True
        
        try:
            metric_def = self.metric_definitions.get(service_id)
            if not metric_def:
                logger.warning(f"No metric definition for service: {service_id}")
                return False
            
            # Prepare dataframes for CloudKitty v2 API
            dataframes = await self._prepare_dataframes(
                tenant, service_id, instance_id, usage_data, metric_def
            )
            
            # Submit to CloudKitty
            endpoint = f"{self.base_url}/v2/dataframes"
            
            for dataframe in dataframes:
                response = await self.client.post(
                    endpoint,
                    json={"dataframes": [dataframe]}
                )
                
                if response.status_code not in (200, 201, 204):
                    logger.error(
                        f"Failed to submit usage to CloudKitty: {response.status_code} {response.text}"
                    )
                    return False
            
            logger.info(
                f"Successfully reported usage for {service_id}/{instance_id} to CloudKitty"
            )
            return True
            
        except Exception as e:
            logger.error(f"Error reporting usage to CloudKitty: {e}")
            return False
    
    async def _prepare_dataframes(
        self,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        usage_data: Dict[str, Any],
        metric_def: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Prepare dataframes for CloudKitty v2 API format"""
        
        dataframes = []
        
        # Base dataframe structure
        dataframe = {
            "period": {
                "begin": usage_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
                "end": usage_data.get("timestamp", datetime.now(timezone.utc).isoformat())
            },
            "usage": {
                metric_def["metric_name"]: [
                    {
                        "vol": {
                            "unit": metric_def["unit"],
                            "qty": self._calculate_quantity(service_id, usage_data)
                        },
                        "rating": {
                            "price": self._calculate_price(service_id, usage_data)
                        },
                        "groupby": self._build_groupby(
                            tenant, service_id, instance_id, usage_data, metric_def
                        ),
                        "metadata": self._build_metadata(
                            tenant, service_id, instance_id, usage_data
                        )
                    }
                ]
            }
        }
        
        dataframes.append(dataframe)
        
        # Add additional metrics if present
        if "additional_metrics" in usage_data:
            for metric_name, metric_value in usage_data["additional_metrics"].items():
                additional_frame = {
                    "period": dataframe["period"],
                    "usage": {
                        metric_name: [
                            {
                                "vol": {
                                    "unit": metric_value.get("unit", "unit"),
                                    "qty": metric_value.get("quantity", 1)
                                },
                                "rating": {
                                    "price": metric_value.get("price", 0)
                                },
                                "groupby": self._build_groupby(
                                    tenant, service_id, instance_id, usage_data, metric_def
                                ),
                                "metadata": self._build_metadata(
                                    tenant, service_id, instance_id, usage_data
                                )
                            }
                        ]
                    }
                }
                dataframes.append(additional_frame)
        
        return dataframes
    
    def _calculate_quantity(self, service_id: str, usage_data: Dict[str, Any]) -> float:
        """Calculate quantity based on service type"""
        
        if service_id == "openstack-compute":
            # For compute, quantity is number of hours
            return usage_data.get("hours", 1.0)
        elif service_id == "openstack-storage":
            # For storage, quantity is GB-hours
            size_gb = usage_data.get("size_gb", 100)
            hours = usage_data.get("hours", 1.0)
            return size_gb * hours
        else:
            # For platform services, default to 1 unit per hour
            return usage_data.get("quantity", 1.0)
    
    def _calculate_price(self, service_id: str, usage_data: Dict[str, Any]) -> float:
        """Calculate price based on service and plan"""
        
        # This would typically look up pricing from a catalog
        # For now, return the price if provided in usage_data
        return usage_data.get("price", 0.0)
    
    def _build_groupby(
        self,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        usage_data: Dict[str, Any],
        metric_def: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build groupby fields for CloudKitty"""
        
        groupby = {
            "tenant_id": tenant.tenant_id,
            "project_id": f"platformq-{tenant.tenant_id}",
            "reseller_id": tenant.reseller_id,
            "customer_id": tenant.customer_id
        }
        
        # Add service-specific groupby fields
        if service_id == "openstack-compute":
            groupby["flavor"] = usage_data.get("instance_type", "unknown")
        elif service_id == "openstack-storage":
            groupby["volume_type"] = usage_data.get("volume_type", "standard")
        elif service_id.startswith("platform-"):
            groupby["plan"] = usage_data.get("plan_id", "default")
        
        return groupby
    
    def _build_metadata(
        self,
        tenant: HierarchicalTenant,
        service_id: str,
        instance_id: str,
        usage_data: Dict[str, Any]
    ) -> Dict[str, str]:
        """Build metadata for CloudKitty"""
        
        metadata = {
            "instance_id": instance_id,
            "service_id": service_id,
            "tenant_name": tenant.tenant_name,
            "customer_name": tenant.customer_name,
            "reseller_name": tenant.reseller_name,
            "action": usage_data.get("action", "usage")
        }
        
        # Add any additional metadata from usage_data
        if "metadata" in usage_data:
            metadata.update(usage_data["metadata"])
        
        return metadata
    
    async def get_tenant_summary(
        self,
        tenant_id: str,
        begin: datetime,
        end: datetime
    ) -> Optional[Dict[str, Any]]:
        """Get usage summary for a tenant
        
        Args:
            tenant_id: Tenant identifier
            begin: Start time
            end: End time
            
        Returns:
            Usage summary or None if error
        """
        if not self.enabled:
            return None
        
        try:
            endpoint = f"{self.base_url}/v2/summary"
            
            params = {
                "begin": begin.isoformat(),
                "end": end.isoformat(),
                "groupby": ["tenant_id", "service", "res_type"],
                "filters": {
                    "tenant_id": tenant_id
                }
            }
            
            response = await self.client.get(endpoint, params=params)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(
                    f"Failed to get summary from CloudKitty: {response.status_code}"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error getting summary from CloudKitty: {e}")
            return None
    
    async def create_hashmap_mapping(
        self,
        service_name: str,
        field_mappings: Dict[str, Any]
    ) -> bool:
        """Create or update hashmap mappings for pricing
        
        Args:
            service_name: Service name for mapping
            field_mappings: Field to price mappings
            
        Returns:
            bool: Success status
        """
        if not self.enabled:
            return True
        
        try:
            # CloudKitty v1 hashmap API for backward compatibility
            endpoint = f"{self.base_url}/v1/rating/module_config/hashmap/services"
            
            # Check if service exists
            response = await self.client.get(endpoint)
            services = response.json().get("services", [])
            
            service_id = None
            for service in services:
                if service["name"] == service_name:
                    service_id = service["service_id"]
                    break
            
            # Create service if not exists
            if not service_id:
                response = await self.client.post(
                    endpoint,
                    json={"name": service_name}
                )
                if response.status_code == 201:
                    service_id = response.json()["service_id"]
                else:
                    logger.error(f"Failed to create service in CloudKitty: {response.text}")
                    return False
            
            # Create field mappings
            for field_name, field_config in field_mappings.items():
                field_endpoint = f"{endpoint}/{service_id}/fields"
                
                response = await self.client.post(
                    field_endpoint,
                    json={"name": field_name}
                )
                
                if response.status_code == 201:
                    field_id = response.json()["field_id"]
                    
                    # Create mappings for field values
                    mapping_endpoint = f"{field_endpoint}/{field_id}/mappings"
                    
                    for value, price in field_config.get("mappings", {}).items():
                        await self.client.post(
                            mapping_endpoint,
                            json={
                                "value": value,
                                "cost": price,
                                "type": "flat"
                            }
                        )
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating hashmap mapping: {e}")
            return False
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose() 