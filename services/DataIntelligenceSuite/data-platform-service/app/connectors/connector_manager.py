"""
Connector Manager for managing external data source connectors
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio

from app.connectors import CONNECTOR_REGISTRY, BaseIngestionConnector
from app.core.config import Settings
from app.core.seatunnel_manager import SeaTunnelManager, JobType
from app.core.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class ConnectorManager:
    """Manages external data source connectors"""
    
    def __init__(self, config: Settings, seatunnel: SeaTunnelManager, schema_registry: SchemaRegistry):
        self.config = config
        self.seatunnel = seatunnel
        self.schema_registry = schema_registry
        self.connectors: Dict[str, BaseIngestionConnector] = {}
        self.scheduled_jobs: Dict[str, str] = {}  # connector_id -> job_id
        
    async def initialize(self):
        """Initialize connector manager"""
        logger.info("Initializing Connector Manager")
        
        # Load configured connectors
        await self._load_connectors()
        
        # Schedule connectors that have schedules
        await self._schedule_connectors()
        
    async def cleanup(self):
        """Cleanup connector manager"""
        logger.info("Cleaning up Connector Manager")
        
        # Cancel all scheduled jobs
        for connector_id, job_id in self.scheduled_jobs.items():
            try:
                await self.seatunnel.cancel_job(job_id)
            except Exception as e:
                logger.error(f"Error cancelling job {job_id}: {e}")
                
        self.scheduled_jobs.clear()
        self.connectors.clear()
        
    async def _load_connectors(self):
        """Load connectors from configuration"""
        connector_configs = self.config.connector_configs or {}
        
        for connector_id, config in connector_configs.items():
            connector_type = config.get("type")
            if connector_type not in CONNECTOR_REGISTRY:
                logger.error(f"Unknown connector type: {connector_type}")
                continue
                
            try:
                # Create connector instance
                connector_class = CONNECTOR_REGISTRY[connector_type]
                connector = connector_class(config, self.schema_registry)
                
                # Validate connection
                if await connector.validate_connection():
                    self.connectors[connector_id] = connector
                    logger.info(f"Loaded connector: {connector_id} ({connector_type})")
                else:
                    logger.error(f"Failed to validate connector: {connector_id}")
                    
            except Exception as e:
                logger.error(f"Failed to load connector {connector_id}: {e}")
                
    async def _schedule_connectors(self):
        """Schedule connectors that have schedules"""
        for connector_id, connector in self.connectors.items():
            if connector.schedule:
                try:
                    job_id = await self._create_connector_job(connector_id, connector)
                    self.scheduled_jobs[connector_id] = job_id
                    logger.info(f"Scheduled connector {connector_id} with schedule: {connector.schedule}")
                except Exception as e:
                    logger.error(f"Failed to schedule connector {connector_id}: {e}")
                    
    async def _create_connector_job(self, connector_id: str, connector: BaseIngestionConnector) -> str:
        """Create a SeaTunnel job for a connector"""
        # Get connector configurations
        source_config = await connector.get_source_config()
        transform_config = await connector.get_transform_config()
        sink_config = await connector.get_sink_config(
            self.config.connector_default_destination
        )
        
        # Register schema if available
        schema = await connector.get_schema()
        if schema:
            await connector.register_schema(schema)
        
        # Create SeaTunnel job
        job_id = await self.seatunnel.create_job(
            job_type=JobType.BATCH,
            source_config=source_config,
            sink_config=sink_config,
            transform_config=transform_config,
            job_name=f"connector_{connector_id}",
            schedule=connector.schedule
        )
        
        return job_id
        
    async def add_connector(self, connector_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Add a new connector"""
        connector_type = config.get("type")
        if connector_type not in CONNECTOR_REGISTRY:
            raise ValueError(f"Unknown connector type: {connector_type}")
            
        # Create connector instance
        connector_class = CONNECTOR_REGISTRY[connector_type]
        connector = connector_class(config, self.schema_registry)
        
        # Validate connection
        if not await connector.validate_connection():
            raise ValueError(f"Failed to validate connector connection")
            
        # Add to registry
        self.connectors[connector_id] = connector
        
        # Schedule if needed
        if connector.schedule:
            job_id = await self._create_connector_job(connector_id, connector)
            self.scheduled_jobs[connector_id] = job_id
            
        return {
            "connector_id": connector_id,
            "type": connector_type,
            "scheduled": connector.schedule is not None,
            "job_id": self.scheduled_jobs.get(connector_id)
        }
        
    async def remove_connector(self, connector_id: str) -> bool:
        """Remove a connector"""
        if connector_id not in self.connectors:
            return False
            
        # Cancel scheduled job if exists
        if connector_id in self.scheduled_jobs:
            job_id = self.scheduled_jobs[connector_id]
            await self.seatunnel.cancel_job(job_id)
            del self.scheduled_jobs[connector_id]
            
        # Remove connector
        del self.connectors[connector_id]
        return True
        
    async def list_connectors(self) -> List[Dict[str, Any]]:
        """List all connectors"""
        result = []
        for connector_id, connector in self.connectors.items():
            result.append({
                "connector_id": connector_id,
                "type": connector.connector_type,
                "scheduled": connector.schedule is not None,
                "schedule": connector.schedule,
                "job_id": self.scheduled_jobs.get(connector_id)
            })
        return result
        
    async def trigger_connector(self, connector_id: str) -> str:
        """Manually trigger a connector"""
        if connector_id not in self.connectors:
            raise ValueError(f"Connector {connector_id} not found")
            
        connector = self.connectors[connector_id]
        
        # Create one-time job
        source_config = await connector.get_source_config()
        transform_config = await connector.get_transform_config()
        sink_config = await connector.get_sink_config(
            self.config.connector_default_destination
        )
        
        job_id = await self.seatunnel.create_job(
            job_type=JobType.BATCH,
            source_config=source_config,
            sink_config=sink_config,
            transform_config=transform_config,
            job_name=f"manual_connector_{connector_id}_{datetime.utcnow().timestamp()}"
        )
        
        # Update last sync time
        connector.update_last_sync_time()
        
        return job_id
        
    async def process_webhook(self, webhook_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Process webhook data"""
        # Find webhook connector
        webhook_connector = None
        for connector in self.connectors.values():
            if connector.connector_type == "webhook" and connector.webhook_type == webhook_type:
                webhook_connector = connector
                break
                
        if not webhook_connector:
            raise ValueError(f"No webhook connector found for type: {webhook_type}")
            
        # Process the webhook data
        processed_data = await webhook_connector.process_webhook_data(payload)
        
        # TODO: Publish to Pulsar topic for ingestion
        # This would be handled by the stream ingestion manager
        
        return processed_data 