"""
Base connector class for data ingestion service
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
from datetime import datetime
import httpx

from app.core.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class BaseIngestionConnector(ABC):
    """
    Abstract base class for all data ingestion connectors.
    Integrates with SeaTunnel for efficient data movement.
    """

    @property
    @abstractmethod
    def connector_type(self) -> str:
        """Unique identifier for the connector"""
        pass

    @property
    def schedule(self) -> Optional[str]:
        """Optional cron-style schedule string"""
        return None

    def __init__(self, config: Dict[str, Any], schema_registry: Optional[SchemaRegistry] = None):
        self.config = config
        self.schema_registry = schema_registry
        self.tenant_id = config.get("tenant_id")
        self.last_sync_time = config.get("last_sync_time")
        logger.info(f"Initialized {self.connector_type} connector")

    @abstractmethod
    async def get_source_config(self) -> Dict[str, Any]:
        """
        Get SeaTunnel source configuration for this connector.
        This will be used to create SeaTunnel jobs.
        """
        pass

    @abstractmethod
    async def validate_connection(self) -> bool:
        """Validate that the connector can connect to its source"""
        pass

    async def get_transform_config(self) -> Optional[Dict[str, Any]]:
        """Optional transformation configuration for SeaTunnel"""
        return None

    async def get_sink_config(self, destination: str = "cassandra") -> Dict[str, Any]:
        """Get SeaTunnel sink configuration"""
        if destination == "cassandra":
            return {
                "type": "Cassandra",
                "host": "${CASSANDRA_HOSTS}",
                "keyspace": "ingestion",
                "table": f"{self.connector_type}_data",
                "username": "${CASSANDRA_USERNAME}",
                "password": "${CASSANDRA_PASSWORD}"
            }
        elif destination == "minio":
            return {
                "type": "S3File",
                "bucket": "${MINIO_BUCKET_RAW}",
                "path": f"/{self.tenant_id}/{self.connector_type}/",
                "access_key": "${MINIO_ACCESS_KEY}",
                "secret_key": "${MINIO_SECRET_KEY}",
                "endpoint": "${MINIO_ENDPOINT}",
                "format": "parquet"
            }
        else:
            raise ValueError(f"Unsupported destination: {destination}")

    async def get_schema(self) -> Optional[Dict[str, Any]]:
        """Get or infer schema for this connector's data"""
        return None

    async def register_schema(self, schema: Dict[str, Any]) -> str:
        """Register schema with the schema registry"""
        if self.schema_registry:
            return await self.schema_registry.register_schema(
                name=f"{self.connector_type}_schema",
                schema=schema,
                schema_type="avro"
            )
        return ""

    def update_last_sync_time(self):
        """Update the last sync timestamp"""
        self.last_sync_time = datetime.utcnow().isoformat()
        self.config["last_sync_time"] = self.last_sync_time 