"""
Chain Manager - Manages connections to multiple blockchain networks
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime, timedelta

from pyignite import AsyncClient as IgniteClient
from platformq_consul import ConsulClient, HealthCheckRegistry
from prometheus_client import Counter, Gauge, Histogram

from ..config import Settings, ChainConfig
from ..models.chain_types import ChainType
from ..adapters.base import BaseChainAdapter
from ..adapters.ethereum import EthereumAdapter
from ..adapters.solana import SolanaAdapter
from ..adapters.cosmos import CosmosAdapter
from ..adapters.near import NearAdapter

logger = logging.getLogger(__name__)

# Metrics
chain_requests = Counter(
    'blockchain_requests_total',
    'Total blockchain requests',
    ['chain', 'method', 'status']
)

chain_latency = Histogram(
    'blockchain_request_latency_seconds',
    'Blockchain request latency',
    ['chain', 'method']
)

active_connections = Gauge(
    'blockchain_active_connections',
    'Active blockchain connections',
    ['chain']
)

chain_health = Gauge(
    'blockchain_chain_health',
    'Blockchain health status (1=healthy, 0=unhealthy)',
    ['chain']
)


class ChainManager:
    """Manages blockchain connections and operations"""
    
    def __init__(
        self,
        settings: Settings,
        consul_client: ConsulClient,
        ignite_client: IgniteClient
    ):
        self.settings = settings
        self.consul = consul_client
        self.ignite = ignite_client
        self.health_registry = HealthCheckRegistry()
        
        # Chain configurations and adapters
        self.chain_configs: Dict[ChainType, ChainConfig] = {}
        self.adapters: Dict[ChainType, BaseChainAdapter] = {}
        
        # Connection pool management
        self.connection_pools: Dict[ChainType, List[BaseChainAdapter]] = {}
        self.connection_semaphores: Dict[ChainType, asyncio.Semaphore] = {}
        
        # Health monitoring
        self._health_check_tasks: Dict[ChainType, asyncio.Task] = {}
        self._endpoint_health: Dict[ChainType, Dict[str, float]] = {}
        
        self._running = False
        
    async def initialize(self):
        """Initialize chain manager"""
        logger.info("Initializing Chain Manager")
        
        # Load chain configurations from Consul
        await self._load_chain_configs()
        
        # Initialize adapters for each chain
        await self._initialize_adapters()
        
        # Start health monitoring
        self._running = True
        await self._start_health_monitoring()
        
        # Register with Consul
        await self._register_service()
        
        logger.info("Chain Manager initialized")
        
    async def shutdown(self):
        """Shutdown chain manager"""
        logger.info("Shutting down Chain Manager")
        self._running = False
        
        # Stop health checks
        for task in self._health_check_tasks.values():
            task.cancel()
            
        # Disconnect all adapters
        for chain_type, adapters in self.connection_pools.items():
            for adapter in adapters:
                try:
                    await adapter.disconnect()
                except Exception as e:
                    logger.error(f"Error disconnecting {chain_type}: {e}")
                    
        logger.info("Chain Manager shut down")
        
    async def _load_chain_configs(self):
        """Load chain configurations from Consul"""
        try:
            # Get configurations from Consul
            configs = await self.consul.get_value("blockchain/chains/configs")
            
            if configs:
                for chain_name, config_data in configs.items():
                    try:
                        chain_type = ChainType(chain_name)
                        config = ChainConfig(**config_data)
                        self.chain_configs[chain_type] = config
                        logger.info(f"Loaded config for {chain_name}")
                    except Exception as e:
                        logger.error(f"Error loading config for {chain_name}: {e}")
            else:
                # Use default configs
                self._load_default_configs()
                
        except Exception as e:
            logger.error(f"Error loading configs from Consul: {e}")
            self._load_default_configs()
            
    def _load_default_configs(self):
        """Load default chain configurations"""
        # Default Ethereum config
        self.chain_configs[ChainType.ETHEREUM] = ChainConfig(
            chain_type="ethereum",
            chain_id=1,
            name="Ethereum Mainnet",
            symbol="ETH",
            explorer_url="https://etherscan.io",
            endpoints=[{
                "url": "https://eth-mainnet.g.alchemy.com/v2/demo",
                "priority": 1,
                "rate_limit": 100
            }],
            features={
                "smart_contracts": True,
                "eip1559": True,
                "tokens": True,
                "nfts": True
            }
        )
        
    async def _initialize_adapters(self):
        """Initialize adapters for each chain"""
        adapter_classes = {
            ChainType.ETHEREUM: EthereumAdapter,
            ChainType.POLYGON: EthereumAdapter,
            ChainType.ARBITRUM: EthereumAdapter,
            ChainType.OPTIMISM: EthereumAdapter,
            ChainType.BSC: EthereumAdapter,
            ChainType.AVALANCHE: EthereumAdapter,
            ChainType.SOLANA: SolanaAdapter,
            ChainType.COSMOS: CosmosAdapter,
            ChainType.NEAR: NearAdapter,
        }
        
        for chain_type, config in self.chain_configs.items():
            adapter_class = adapter_classes.get(chain_type)
            if not adapter_class:
                logger.warning(f"No adapter class for {chain_type}")
                continue
                
            # Create connection pool
            pool = []
            for i in range(self.settings.MAX_CONNECTIONS_PER_CHAIN):
                try:
                    adapter = adapter_class(chain_type, config)
                    await adapter.connect()
                    pool.append(adapter)
                except Exception as e:
                    logger.error(f"Error creating adapter for {chain_type}: {e}")
                    
            if pool:
                self.connection_pools[chain_type] = pool
                self.connection_semaphores[chain_type] = asyncio.Semaphore(len(pool))
                active_connections.labels(chain=chain_type.value).set(len(pool))
                logger.info(f"Created {len(pool)} connections for {chain_type}")
                
    async def _start_health_monitoring(self):
        """Start health monitoring for all chains"""
        for chain_type in self.chain_configs:
            task = asyncio.create_task(self._monitor_chain_health(chain_type))
            self._health_check_tasks[chain_type] = task
            
    async def _monitor_chain_health(self, chain_type: ChainType):
        """Monitor health of a specific chain"""
        while self._running:
            try:
                # Check each endpoint
                config = self.chain_configs.get(chain_type)
                if not config:
                    continue
                    
                endpoint_scores = {}
                for endpoint in config.endpoints:
                    score = await self._check_endpoint_health(chain_type, endpoint.url)
                    endpoint_scores[endpoint.url] = score
                    
                self._endpoint_health[chain_type] = endpoint_scores
                
                # Update overall chain health
                avg_health = sum(endpoint_scores.values()) / len(endpoint_scores) if endpoint_scores else 0
                chain_health.labels(chain=chain_type.value).set(avg_health)
                
                # Log if unhealthy
                if avg_health < 0.5:
                    logger.warning(f"{chain_type} health degraded: {avg_health}")
                    
            except Exception as e:
                logger.error(f"Error monitoring {chain_type}: {e}")
                
            await asyncio.sleep(self.settings.HEALTH_CHECK_INTERVAL)
            
    async def _check_endpoint_health(self, chain_type: ChainType, endpoint: str) -> float:
        """Check health of a specific endpoint"""
        try:
            # Get an adapter from the pool
            adapter = await self._get_adapter_from_pool(chain_type)
            if not adapter:
                return 0.0
                
            # Try to get latest block
            start_time = asyncio.get_event_loop().time()
            block_number = await adapter.get_latest_block()
            latency = asyncio.get_event_loop().time() - start_time
            
            # Return adapter to pool
            await self._return_adapter_to_pool(chain_type, adapter)
            
            # Calculate health score based on latency
            if latency < 1.0:
                return 1.0
            elif latency < 3.0:
                return 0.8
            elif latency < 5.0:
                return 0.5
            else:
                return 0.2
                
        except Exception as e:
            logger.error(f"Health check failed for {chain_type} {endpoint}: {e}")
            return 0.0
            
    async def _get_adapter_from_pool(self, chain_type: ChainType) -> Optional[BaseChainAdapter]:
        """Get an adapter from the connection pool"""
        pool = self.connection_pools.get(chain_type, [])
        if not pool:
            return None
            
        # Use semaphore to limit concurrent usage
        semaphore = self.connection_semaphores.get(chain_type)
        if semaphore:
            await semaphore.acquire()
            
        # Simple round-robin selection
        # In production, use more sophisticated selection based on health
        return pool[0] if pool else None
        
    async def _return_adapter_to_pool(self, chain_type: ChainType, adapter: BaseChainAdapter):
        """Return an adapter to the pool"""
        semaphore = self.connection_semaphores.get(chain_type)
        if semaphore:
            semaphore.release()
            
    async def _register_service(self):
        """Register service with Consul"""
        await self.consul.register_service(
            name=self.settings.SERVICE_NAME,
            service_id=f"{self.settings.SERVICE_NAME}-{self.settings.SERVICE_PORT}",
            address="localhost",
            port=self.settings.SERVICE_PORT,
            tags=["blockchain", "connector"],
            check={
                "http": f"http://localhost:{self.settings.SERVICE_PORT}/health",
                "interval": "10s",
                "timeout": "5s"
            }
        )
        
    # Public API methods
    
    async def get_balance(
        self,
        chain_type: ChainType,
        address: str,
        token_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get balance for an address"""
        adapter = await self._get_adapter_from_pool(chain_type)
        if not adapter:
            raise ValueError(f"No adapter available for {chain_type}")
            
        try:
            with chain_latency.labels(chain=chain_type.value, method="get_balance").time():
                result = await adapter.get_balance(address, token_address)
                
            chain_requests.labels(
                chain=chain_type.value,
                method="get_balance",
                status="success"
            ).inc()
            
            return result
            
        except Exception as e:
            chain_requests.labels(
                chain=chain_type.value,
                method="get_balance",
                status="error"
            ).inc()
            raise
        finally:
            await self._return_adapter_to_pool(chain_type, adapter)
            
    async def get_transaction(
        self,
        chain_type: ChainType,
        tx_hash: str
    ) -> Dict[str, Any]:
        """Get transaction details"""
        adapter = await self._get_adapter_from_pool(chain_type)
        if not adapter:
            raise ValueError(f"No adapter available for {chain_type}")
            
        try:
            with chain_latency.labels(chain=chain_type.value, method="get_transaction").time():
                result = await adapter.get_transaction(tx_hash)
                
            chain_requests.labels(
                chain=chain_type.value,
                method="get_transaction",
                status="success"
            ).inc()
            
            return result
            
        except Exception as e:
            chain_requests.labels(
                chain=chain_type.value,
                method="get_transaction",
                status="error"
            ).inc()
            raise
        finally:
            await self._return_adapter_to_pool(chain_type, adapter)
            
    async def broadcast_transaction(
        self,
        chain_type: ChainType,
        signed_tx: str
    ) -> str:
        """Broadcast a signed transaction"""
        adapter = await self._get_adapter_from_pool(chain_type)
        if not adapter:
            raise ValueError(f"No adapter available for {chain_type}")
            
        try:
            with chain_latency.labels(chain=chain_type.value, method="broadcast_transaction").time():
                tx_hash = await adapter.broadcast_transaction(signed_tx)
                
            chain_requests.labels(
                chain=chain_type.value,
                method="broadcast_transaction",
                status="success"
            ).inc()
            
            # Cache transaction for monitoring
            await self._cache_transaction(chain_type, tx_hash, signed_tx)
            
            return tx_hash
            
        except Exception as e:
            chain_requests.labels(
                chain=chain_type.value,
                method="broadcast_transaction",
                status="error"
            ).inc()
            raise
        finally:
            await self._return_adapter_to_pool(chain_type, adapter)
            
    async def estimate_gas(
        self,
        chain_type: ChainType,
        from_address: str,
        to_address: str,
        value: str,
        data: Optional[str] = None
    ) -> Dict[str, Any]:
        """Estimate gas for a transaction"""
        adapter = await self._get_adapter_from_pool(chain_type)
        if not adapter:
            raise ValueError(f"No adapter available for {chain_type}")
            
        try:
            with chain_latency.labels(chain=chain_type.value, method="estimate_gas").time():
                result = await adapter.estimate_gas(from_address, to_address, value, data)
                
            chain_requests.labels(
                chain=chain_type.value,
                method="estimate_gas",
                status="success"
            ).inc()
            
            return result
            
        except Exception as e:
            chain_requests.labels(
                chain=chain_type.value,
                method="estimate_gas",
                status="error"
            ).inc()
            raise
        finally:
            await self._return_adapter_to_pool(chain_type, adapter)
            
    async def _cache_transaction(self, chain_type: ChainType, tx_hash: str, signed_tx: str):
        """Cache transaction for monitoring"""
        try:
            cache = await self.ignite.get_or_create_cache("transactions")
            await cache.put(
                f"{chain_type.value}:{tx_hash}",
                {
                    "chain": chain_type.value,
                    "tx_hash": tx_hash,
                    "signed_tx": signed_tx,
                    "timestamp": datetime.utcnow().isoformat(),
                    "status": "pending"
                }
            )
        except Exception as e:
            logger.error(f"Error caching transaction: {e}")
            
    def get_supported_chains(self) -> List[ChainType]:
        """Get list of supported chains"""
        return list(self.chain_configs.keys())
        
    def get_chain_info(self, chain_type: ChainType) -> Optional[ChainConfig]:
        """Get configuration for a chain"""
        return self.chain_configs.get(chain_type) 