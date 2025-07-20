"""
Vault and Consul Integration for Trading Risk Intelligence Service

Manages secrets, service registration, and configuration.
"""

import os
import logging
import json
import asyncio
from typing import Dict, Any, Optional
import hvac
import consul.aio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class VaultConsulIntegration:
    """Manages Vault and Consul integration for the service"""
    
    def __init__(self):
        # Vault configuration
        self.vault_addr = os.environ.get('VAULT_ADDR', 'http://vault:8200')
        self.vault_token = os.environ.get('VAULT_TOKEN')
        self.service_name = os.environ.get('SERVICE_NAME', 'trading-risk-intelligence-service')
        
        # Consul configuration
        self.consul_host = os.environ.get('CONSUL_HOST', 'consul')
        self.consul_port = int(os.environ.get('CONSUL_PORT', '8500'))
        
        # Service paths
        self.vault_graph_path = f"secret/data/graph/{self.service_name}"
        self.consul_kv_prefix = f"graph/{self.service_name}"
        
        # Clients
        self.vault_client = None
        self.consul_client = None
        
        # Cached configurations
        self.janusgraph_config = {}
        self._service_id = None
        
    async def initialize(self):
        """Initialize Vault and Consul connections"""
        logger.info("Initializing Vault and Consul integration")
        
        # Initialize Vault client
        self.vault_client = hvac.Client(
            url=self.vault_addr,
            token=self.vault_token
        )
        
        if not self.vault_client.is_authenticated():
            raise RuntimeError("Vault authentication failed")
            
        # Initialize Consul client
        self.consul_client = consul.aio.Consul(
            host=self.consul_host,
            port=self.consul_port
        )
        
        # Register service with Consul
        await self._register_service()
        
        # Load JanusGraph configuration
        await self._load_janusgraph_config()
        
        logger.info("Vault and Consul integration initialized successfully")
        
    async def _register_service(self):
        """Register service with Consul"""
        service_port = int(os.environ.get('SERVICE_PORT', '8000'))
        
        self._service_id = f"{self.service_name}-{os.environ.get('HOSTNAME', 'local')}"
        
        service_config = {
            "ID": self._service_id,
            "Name": self.service_name,
            "Tags": ["trading", "risk", "graph", "analytics"],
            "Port": service_port,
            "Check": {
                "HTTP": f"http://localhost:{service_port}/health",
                "Interval": "10s",
                "Timeout": "5s"
            },
            "Meta": {
                "version": "1.0.0",
                "graph_db": "janusgraph"
            }
        }
        
        await self.consul_client.agent.service.register(**service_config)
        logger.info(f"Service registered with Consul: {self._service_id}")
        
    async def _load_janusgraph_config(self):
        """Load JanusGraph configuration from Vault"""
        try:
            # Read JanusGraph credentials from Vault
            response = self.vault_client.secrets.kv.v2.read_secret_version(
                path=f"graph/{self.service_name}/janusgraph"
            )
            
            if response and 'data' in response:
                data = response['data']['data']
                self.janusgraph_config = {
                    'gremlin_url': data.get('gremlin_url', 'ws://janusgraph:8182/gremlin'),
                    'cassandra_hosts': data.get('cassandra_hosts', 'cassandra'),
                    'cassandra_keyspace': data.get('cassandra_keyspace', 'platformq')
                }
            else:
                # Use defaults if not in Vault
                self.janusgraph_config = {
                    'gremlin_url': 'ws://janusgraph:8182/gremlin',
                    'cassandra_hosts': 'cassandra',
                    'cassandra_keyspace': 'platformq'
                }
                
        except Exception as e:
            logger.warning(f"Failed to load JanusGraph config from Vault: {e}, using defaults")
            self.janusgraph_config = {
                'gremlin_url': 'ws://janusgraph:8182/gremlin',
                'cassandra_hosts': 'cassandra',
                'cassandra_keyspace': 'platformq'
            }
            
    async def check_health(self) -> bool:
        """Check Vault and Consul health"""
        try:
            # Check Vault
            vault_healthy = self.vault_client.is_authenticated()
            
            # Check Consul
            consul_healthy = await self.consul_client.health.node(
                node=os.environ.get('HOSTNAME', 'local')
            )
            
            return vault_healthy and bool(consul_healthy)
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
            
    async def close(self):
        """Clean up resources"""
        if self.consul_client and self._service_id:
            try:
                await self.consul_client.agent.service.deregister(self._service_id)
                logger.info(f"Service deregistered from Consul: {self._service_id}")
            except Exception as e:
                logger.error(f"Failed to deregister service: {e}")
                
        if self.consul_client:
            await self.consul_client.close() 