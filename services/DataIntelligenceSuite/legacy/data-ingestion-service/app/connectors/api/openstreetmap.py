"""
OpenStreetMap connector for data ingestion
"""

import logging
from typing import Optional, Dict, Any
import json

from ..base import BaseIngestionConnector

logger = logging.getLogger(__name__)


class OpenStreetMapConnector(BaseIngestionConnector):
    """
    Connector for OpenStreetMap data via Overpass API.
    Supports both scheduled queries and on-demand data extraction.
    """
    
    @property
    def connector_type(self) -> str:
        return "openstreetmap"
    
    def __init__(self, config: Dict[str, Any], schema_registry=None):
        super().__init__(config, schema_registry)
        self.overpass_url = config.get("overpass_url", "https://overpass-api.de/api/interpreter")
        self.queries = config.get("queries", [])
        self.default_timeout = config.get("timeout", 180)
        
    async def validate_connection(self) -> bool:
        """Validate Overpass API connection"""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                # Simple test query
                test_query = "[out:json][timeout:1];node[\"name\"=\"Test\"](0,0,0.0001,0.0001);out count;"
                response = await client.post(
                    self.overpass_url,
                    data=test_query,
                    timeout=5.0
                )
                return response.status_code == 200
        except Exception as e:
            logger.error(f"OpenStreetMap connection validation failed: {e}")
            return False
    
    async def get_source_config(self) -> Dict[str, Any]:
        """Get SeaTunnel configuration for OpenStreetMap queries"""
        if not self.queries:
            raise ValueError("No queries configured for OpenStreetMap connector")
        
        configs = []
        for idx, query_config in enumerate(self.queries):
            query = query_config.get("query", "")
            name = query_config.get("name", f"query_{idx}")
            
            config = {
                "type": "Http",
                "url": self.overpass_url,
                "method": "POST",
                "headers": {
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                "body": query,
                "format": "json",
                "schema": {
                    "version": "double",
                    "generator": "string",
                    "elements": "array<map<string,string>>"
                },
                "enable_multi_lines": True
            }
            
            configs.append(config)
        
        return {
            "type": "Http",
            "name": f"openstreetmap_{self.tenant_id}",
            "parallelism": len(self.queries),
            "sources": configs
        }
    
    async def get_transform_config(self) -> Optional[Dict[str, Any]]:
        """Transform OpenStreetMap data to unified format"""
        return {
            "type": "Sql",
            "sql": """
                SELECT 
                    CAST(id AS STRING) as osm_id,
                    type as osm_type,
                    tags.name as name,
                    lat,
                    lon,
                    tags,
                    '${TENANT_ID}' as tenant_id,
                    'openstreetmap' as source_system,
                    CURRENT_TIMESTAMP() as ingestion_time
                FROM (
                    SELECT 
                        e.id,
                        e.type,
                        e.lat,
                        e.lon,
                        e.tags
                    FROM source 
                    LATERAL VIEW explode(elements) t AS e
                )
            """
        }
    
    async def get_schema(self) -> Optional[Dict[str, Any]]:
        """Get schema for OpenStreetMap data"""
        return {
            "type": "record",
            "name": "OSMData",
            "fields": [
                {"name": "osm_id", "type": "string"},
                {"name": "osm_type", "type": "string"},
                {"name": "name", "type": ["null", "string"], "default": None},
                {"name": "lat", "type": ["null", "double"], "default": None},
                {"name": "lon", "type": ["null", "double"], "default": None},
                {"name": "tags", "type": {"type": "map", "values": "string"}},
                {"name": "tenant_id", "type": "string"},
                {"name": "source_system", "type": "string"},
                {"name": "ingestion_time", "type": "string"}
            ]
        }
    
    def add_query(self, name: str, query: str):
        """Add a new Overpass query"""
        self.queries.append({
            "name": name,
            "query": query
        }) 