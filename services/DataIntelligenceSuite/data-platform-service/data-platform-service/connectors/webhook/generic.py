"""
Generic webhook connector for data ingestion
"""

import logging
from typing import Optional, Dict, Any, List
import json
from datetime import datetime

from data_intelligence_common.base_service import BaseIngestionConnector

logger = logging.getLogger(__name__)


class WebhookConnector(BaseIngestionConnector):
    """
    Generic webhook connector for ingesting data from HTTP webhooks.
    Supports various data formats and transformation rules.
    """
    
    @property
    def connector_type(self) -> str:
        return self.config.get("webhook_type", "generic_webhook")
    
    def __init__(self, config: Dict[str, Any], schema_registry=None):
        super().__init__(config, schema_registry)
        self.webhook_type = config.get("webhook_type", "generic")
        self.transform_rules = config.get("transform_rules", {})
        self.validation_rules = config.get("validation_rules", {})
        
    async def validate_connection(self) -> bool:
        """Webhooks don't require connection validation"""
        return True
    
    async def get_source_config(self) -> Dict[str, Any]:
        """
        Webhook data is pushed, not pulled, so we configure a Pulsar source
        to consume webhook events that are published by the webhook endpoint
        """
        return {
            "type": "Pulsar",
            "servers": "${PULSAR_URL}",
            "topic": f"persistent://public/default/webhooks-{self.tenant_id}-{self.webhook_type}",
            "subscription": f"webhook-ingestion-{self.webhook_type}",
            "subscription_type": "Shared",
            "format": "json",
            "schema": await self.get_schema()
        }
    
    async def process_webhook_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process incoming webhook data and publish to Pulsar topic.
        This method is called by the webhook endpoint.
        """
        try:
            # Validate payload
            if not await self._validate_payload(payload):
                raise ValueError("Payload validation failed")
            
            # Transform payload
            transformed = await self._transform_payload(payload)
            
            # Add metadata
            transformed.update({
                "webhook_type": self.webhook_type,
                "tenant_id": self.tenant_id,
                "received_at": datetime.utcnow().isoformat(),
                "source_system": "webhook"
            })
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error processing webhook data: {e}")
            raise
    
    async def _validate_payload(self, payload: Dict[str, Any]) -> bool:
        """Validate webhook payload against rules"""
        if not self.validation_rules:
            return True
        
        # Check required fields
        required_fields = self.validation_rules.get("required_fields", [])
        for field in required_fields:
            if field not in payload:
                logger.error(f"Missing required field: {field}")
                return False
        
        # Check field types
        field_types = self.validation_rules.get("field_types", {})
        for field, expected_type in field_types.items():
            if field in payload:
                actual_type = type(payload[field]).__name__
                if actual_type != expected_type:
                    logger.error(f"Field {field} has type {actual_type}, expected {expected_type}")
                    return False
        
        return True
    
    async def _transform_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Transform webhook payload according to rules"""
        if not self.transform_rules:
            return payload
        
        transformed = {}
        
        # Field mappings
        field_mappings = self.transform_rules.get("field_mappings", {})
        for source_field, target_field in field_mappings.items():
            if source_field in payload:
                transformed[target_field] = payload[source_field]
        
        # Include unmapped fields if configured
        if self.transform_rules.get("include_unmapped", True):
            for key, value in payload.items():
                if key not in field_mappings and key not in transformed:
                    transformed[key] = value
        
        # Apply custom transformations
        custom_transforms = self.transform_rules.get("custom_transforms", {})
        for field, transform_type in custom_transforms.items():
            if field in transformed:
                if transform_type == "lowercase":
                    transformed[field] = str(transformed[field]).lower()
                elif transform_type == "uppercase":
                    transformed[field] = str(transformed[field]).upper()
                # Add more transform types as needed
        
        return transformed
    
    async def get_transform_config(self) -> Optional[Dict[str, Any]]:
        """Transform webhook data to unified format"""
        return {
            "type": "Sql",
            "sql": """
                SELECT 
                    webhook_id as external_id,
                    webhook_type,
                    payload,
                    tenant_id,
                    source_system,
                    received_at as ingestion_time
                FROM source
            """
        }
    
    async def get_schema(self) -> Optional[Dict[str, Any]]:
        """Get schema for webhook data"""
        return {
            "type": "record",
            "name": "WebhookData",
            "fields": [
                {"name": "webhook_id", "type": "string"},
                {"name": "webhook_type", "type": "string"},
                {"name": "payload", "type": {"type": "map", "values": "string"}},
                {"name": "tenant_id", "type": "string"},
                {"name": "source_system", "type": "string"},
                {"name": "received_at", "type": "string"}
            ]
        } 