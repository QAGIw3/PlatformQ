"""
SuiteCRM connector for data ingestion
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

from ..base import BaseIngestionConnector

logger = logging.getLogger(__name__)


class SuiteCRMConnector(BaseIngestionConnector):
    """
    Connector for SuiteCRM to sync contacts, accounts, opportunities, and other CRM data.
    Uses SeaTunnel's HTTP source for efficient data extraction.
    """
    
    @property
    def connector_type(self) -> str:
        return "suitecrm"
    
    @property
    def schedule(self) -> Optional[str]:
        # Run every hour by default
        return self.config.get("schedule", "0 * * * *")
    
    def __init__(self, config: Dict[str, Any], schema_registry=None):
        super().__init__(config, schema_registry)
        self.base_url = config.get("base_url", "").rstrip("/")
        self.username = config.get("username")
        self.password = config.get("password")
        self.client_id = config.get("client_id", "sugar")
        self.client_secret = config.get("client_secret", "")
        self.modules = config.get("modules", ["Contacts", "Accounts", "Opportunities"])
        
    async def validate_connection(self) -> bool:
        """Validate SuiteCRM connection"""
        try:
            # Test authentication endpoint
            import httpx
            async with httpx.AsyncClient() as client:
                auth_url = f"{self.base_url}/Api/access_token"
                data = {
                    "grant_type": "password",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "username": self.username,
                    "password": self.password,
                    "platform": "platformq"
                }
                response = await client.post(auth_url, data=data, timeout=10.0)
                return response.status_code == 200
        except Exception as e:
            logger.error(f"SuiteCRM connection validation failed: {e}")
            return False
    
    async def get_source_config(self) -> Dict[str, Any]:
        """Get SeaTunnel HTTP source configuration for SuiteCRM"""
        # Create a multi-module configuration
        configs = []
        
        for module in self.modules:
            config = {
                "type": "Http",
                "url": f"{self.base_url}/Api/V8/module/{module}",
                "method": "GET",
                "headers": {
                    "Authorization": "Bearer ${SUITECRM_TOKEN}",
                    "Content-Type": "application/json"
                },
                "params": {
                    "page[size]": "100",
                    "sort": "-date_modified"
                },
                "format": "json",
                "json_path": "$.data[*]",
                "schema": await self._get_module_schema(module),
                "poll_interval_ms": 60000,  # Poll every minute
                "enable_multi_lines": True
            }
            
            # Add date filter if we have a last sync time
            if self.last_sync_time:
                config["params"]["filter"] = json.dumps({
                    "date_modified": {"$gte": self.last_sync_time}
                })
            
            configs.append(config)
        
        # Return composite source configuration
        return {
            "type": "Http",
            "name": f"suitecrm_{self.tenant_id}",
            "parallelism": len(self.modules),
            "sources": configs
        }
    
    async def get_transform_config(self) -> Optional[Dict[str, Any]]:
        """Transform SuiteCRM data to unified format"""
        return {
            "type": "Sql",
            "sql": """
                SELECT 
                    id as external_id,
                    type as record_type,
                    attributes.name as name,
                    attributes.date_entered as created_date,
                    attributes.date_modified as modified_date,
                    '${TENANT_ID}' as tenant_id,
                    'suitecrm' as source_system,
                    attributes as raw_data
                FROM source
            """
        }
    
    async def _get_module_schema(self, module: str) -> Dict[str, Any]:
        """Get schema for a specific SuiteCRM module"""
        # Define schemas for common modules
        schemas = {
            "Contacts": {
                "id": "string",
                "type": "string",
                "attributes": {
                    "first_name": "string",
                    "last_name": "string",
                    "email1": "string",
                    "phone_work": "string",
                    "account_id": "string",
                    "date_entered": "string",
                    "date_modified": "string"
                }
            },
            "Accounts": {
                "id": "string",
                "type": "string",
                "attributes": {
                    "name": "string",
                    "account_type": "string",
                    "industry": "string",
                    "website": "string",
                    "date_entered": "string",
                    "date_modified": "string"
                }
            },
            "Opportunities": {
                "id": "string",
                "type": "string",
                "attributes": {
                    "name": "string",
                    "account_id": "string",
                    "amount": "string",
                    "sales_stage": "string",
                    "probability": "string",
                    "date_closed": "string",
                    "date_entered": "string",
                    "date_modified": "string"
                }
            }
        }
        
        return schemas.get(module, {
            "id": "string",
            "type": "string",
            "attributes": "map<string,string>"
        })
    
    async def get_schema(self) -> Optional[Dict[str, Any]]:
        """Get unified schema for CRM data"""
        return {
            "type": "record",
            "name": "CRMData",
            "fields": [
                {"name": "external_id", "type": "string"},
                {"name": "record_type", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": None},
                {"name": "created_date", "type": "string"},
                {"name": "modified_date", "type": "string"},
                {"name": "tenant_id", "type": "string"},
                {"name": "source_system", "type": "string"},
                {"name": "raw_data", "type": {"type": "map", "values": "string"}}
            ]
        } 