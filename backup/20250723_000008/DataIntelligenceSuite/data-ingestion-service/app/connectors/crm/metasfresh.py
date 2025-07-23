"""
Metasfresh ERP connector for data ingestion
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

from ..base import BaseIngestionConnector

logger = logging.getLogger(__name__)


class MetasfreshConnector(BaseIngestionConnector):
    """
    Connector for Metasfresh ERP to sync products, orders, invoices, and business partners.
    Uses SeaTunnel's HTTP source for efficient data extraction.
    """
    
    @property
    def connector_type(self) -> str:
        return "metasfresh"
    
    @property
    def schedule(self) -> Optional[str]:
        # Run every 30 minutes by default
        return self.config.get("schedule", "*/30 * * * *")
    
    def __init__(self, config: Dict[str, Any], schema_registry=None):
        super().__init__(config, schema_registry)
        self.base_url = config.get("base_url", "").rstrip("/")
        self.api_key = config.get("api_key")
        self.entities = config.get("entities", ["products", "businessPartners", "orders", "invoices"])
        
    async def validate_connection(self) -> bool:
        """Validate Metasfresh connection"""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                # Test with a simple API call
                headers = {
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json"
                }
                response = await client.get(
                    f"{self.base_url}/api/v2/products?pageSize=1",
                    headers=headers,
                    timeout=10.0
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"Metasfresh connection validation failed: {e}")
            return False
    
    async def get_source_config(self) -> Dict[str, Any]:
        """Get SeaTunnel HTTP source configuration for Metasfresh"""
        configs = []
        
        for entity in self.entities:
            config = {
                "type": "Http",
                "url": f"{self.base_url}/api/v2/{entity}",
                "method": "GET",
                "headers": {
                    "Authorization": "Bearer ${METASFRESH_API_KEY}",
                    "Accept": "application/json"
                },
                "params": {
                    "pageSize": "100",
                    "page": "0"
                },
                "format": "json",
                "json_path": f"$.{entity}[*]",
                "schema": await self._get_entity_schema(entity),
                "poll_interval_ms": 300000,  # Poll every 5 minutes
                "enable_multi_lines": True,
                "pagination": {
                    "type": "page_number",
                    "page_param": "page",
                    "size_param": "pageSize",
                    "start_page": 0
                }
            }
            
            # Add date filter if we have a last sync time
            if self.last_sync_time:
                config["params"]["updatedAfter"] = self.last_sync_time
            
            configs.append(config)
        
        return {
            "type": "Http",
            "name": f"metasfresh_{self.tenant_id}",
            "parallelism": len(self.entities),
            "sources": configs
        }
    
    async def get_transform_config(self) -> Optional[Dict[str, Any]]:
        """Transform Metasfresh data to unified format"""
        return {
            "type": "Sql",
            "sql": """
                SELECT 
                    CAST(id AS STRING) as external_id,
                    '${ENTITY_TYPE}' as entity_type,
                    COALESCE(name, value, documentNo) as name,
                    created as created_date,
                    updated as modified_date,
                    '${TENANT_ID}' as tenant_id,
                    'metasfresh' as source_system,
                    * as raw_data
                FROM source
            """
        }
    
    async def _get_entity_schema(self, entity: str) -> Dict[str, Any]:
        """Get schema for a specific Metasfresh entity"""
        schemas = {
            "products": {
                "id": "long",
                "value": "string",
                "name": "string",
                "description": "string",
                "productCategoryId": "long",
                "uomId": "long",
                "isStocked": "boolean",
                "created": "string",
                "updated": "string"
            },
            "businessPartners": {
                "id": "long",
                "value": "string",
                "name": "string",
                "isCustomer": "boolean",
                "isVendor": "boolean",
                "created": "string",
                "updated": "string"
            },
            "orders": {
                "id": "long",
                "documentNo": "string",
                "businessPartnerId": "long",
                "dateOrdered": "string",
                "grandTotal": "double",
                "created": "string",
                "updated": "string"
            },
            "invoices": {
                "id": "long",
                "documentNo": "string",
                "businessPartnerId": "long",
                "dateInvoiced": "string",
                "grandTotal": "double",
                "isPaid": "boolean",
                "created": "string",
                "updated": "string"
            }
        }
        
        return schemas.get(entity, {
            "id": "long",
            "created": "string",
            "updated": "string"
        })
    
    async def get_schema(self) -> Optional[Dict[str, Any]]:
        """Get unified schema for ERP data"""
        return {
            "type": "record",
            "name": "ERPData",
            "fields": [
                {"name": "external_id", "type": "string"},
                {"name": "entity_type", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": None},
                {"name": "created_date", "type": "string"},
                {"name": "modified_date", "type": "string"},
                {"name": "tenant_id", "type": "string"},
                {"name": "source_system", "type": "string"},
                {"name": "raw_data", "type": {"type": "map", "values": "string"}}
            ]
        } 