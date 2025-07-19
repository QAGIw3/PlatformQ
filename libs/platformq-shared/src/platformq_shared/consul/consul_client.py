"""
HashiCorp Consul Client

Provides service discovery, health checking, KV store, and distributed configuration.
Integrates with PlatformQ services for dynamic service mesh capabilities.
"""

import consul
import logging
from typing import Dict, Any, Optional, List, Tuple, Callable
from datetime import datetime, timedelta
import json
import asyncio
from dataclasses import dataclass, field
from enum import Enum
import socket
import uuid

logger = logging.getLogger(__name__)


class ServiceStatus(Enum):
    """Service health status"""
    PASSING = "passing"
    WARNING = "warning"
    CRITICAL = "critical"
    MAINTENANCE = "maintenance"


@dataclass
class ConsulConfig:
    """Consul configuration"""
    host: str = "consul"
    port: int = 8500
    scheme: str = "http"
    token: Optional[str] = None
    dc: Optional[str] = None
    verify: bool = True
    cert: Optional[Tuple[str, str]] = None
    consistency: str = "default"
    timeout: int = 30


@dataclass
class ServiceDefinition:
    """Service definition for registration"""
    name: str
    service_id: Optional[str] = None
    address: Optional[str] = None
    port: int = 8000
    tags: List[str] = field(default_factory=list)
    meta: Dict[str, str] = field(default_factory=dict)
    check: Optional[Dict[str, Any]] = None
    checks: List[Dict[str, Any]] = field(default_factory=list)
    enable_tag_override: bool = False
    weights: Optional[Dict[str, int]] = None


@dataclass
class ServiceInstance:
    """Discovered service instance"""
    service_id: str
    service_name: str
    address: str
    port: int
    tags: List[str]
    meta: Dict[str, str]
    status: ServiceStatus
    node: str
    datacenter: str


class ConsulClient:
    """
    HashiCorp Consul client for PlatformQ.
    
    Features:
    - Service registration and discovery
    - Health checking
    - KV store operations
    - Distributed configuration
    - Service mesh integration
    - Leader election
    - Distributed locks
    - Event propagation
    """
    
    def __init__(self, config: ConsulConfig):
        self.config = config
        self.client: Optional[consul.Consul] = None
        self._session_id: Optional[str] = None
        self._watch_handlers: Dict[str, Callable] = {}
        self._health_check_tasks: Dict[str, asyncio.Task] = {}
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize Consul client"""
        try:
            # Create client
            self.client = consul.Consul(
                host=self.config.host,
                port=self.config.port,
                scheme=self.config.scheme,
                token=self.config.token,
                dc=self.config.dc,
                verify=self.config.verify,
                cert=self.config.cert,
                consistency=self.config.consistency
            )
            
            # Test connection
            await asyncio.to_thread(self.client.agent.self)
            
            # Create session for locks
            session_data = await asyncio.to_thread(
                self.client.session.create,
                name=f"platformq-{socket.gethostname()}",
                ttl=30,
                behavior="delete"
            )
            self._session_id = session_data['ID']
            
            # Start session renewal
            asyncio.create_task(self._renew_session())
            
            self._initialized = True
            logger.info("Consul client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Consul client: {e}")
            raise
    
    # Service Registration & Discovery
    
    async def register_service(self, service: ServiceDefinition) -> str:
        """Register a service with Consul"""
        await self._ensure_initialized()
        
        # Generate service ID if not provided
        if not service.service_id:
            service.service_id = f"{service.name}-{uuid.uuid4().hex[:8]}"
        
        # Get host address if not provided
        if not service.address:
            service.address = socket.gethostname()
        
        # Prepare service data
        service_data = {
            "ID": service.service_id,
            "Name": service.name,
            "Address": service.address,
            "Port": service.port,
            "Tags": service.tags,
            "Meta": service.meta,
            "EnableTagOverride": service.enable_tag_override
        }
        
        if service.weights:
            service_data["Weights"] = service.weights
        
        # Add health checks
        if service.check:
            service_data["Check"] = service.check
        elif service.checks:
            service_data["Checks"] = service.checks
        else:
            # Default HTTP health check
            service_data["Check"] = {
                "HTTP": f"http://{service.address}:{service.port}/health",
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "60s"
            }
        
        # Register service
        await asyncio.to_thread(
            self.client.agent.service.register,
            **service_data
        )
        
        logger.info(f"Registered service: {service.service_id}")
        return service.service_id
        
    async def deregister_service(self, service_id: str) -> None:
        """Deregister a service"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.agent.service.deregister,
            service_id
        )
        
        logger.info(f"Deregistered service: {service_id}")
        
    async def discover_service(self, service_name: str, 
                             passing_only: bool = True,
                             tags: Optional[List[str]] = None) -> List[ServiceInstance]:
        """Discover service instances"""
        await self._ensure_initialized()
        
        # Query health endpoint
        index, nodes = await asyncio.to_thread(
            self.client.health.service,
            service_name,
            passing=passing_only,
            tag=tags[0] if tags else None
        )
        
        instances = []
        for node in nodes:
            service = node['Service']
            checks = node['Checks']
            
            # Determine overall status
            status = ServiceStatus.PASSING
            for check in checks:
                if check['Status'] == 'critical':
                    status = ServiceStatus.CRITICAL
                    break
                elif check['Status'] == 'warning':
                    status = ServiceStatus.WARNING
            
            instance = ServiceInstance(
                service_id=service['ID'],
                service_name=service['Service'],
                address=service['Address'] or node['Node']['Address'],
                port=service['Port'],
                tags=service.get('Tags', []),
                meta=service.get('Meta', {}),
                status=status,
                node=node['Node']['Node'],
                datacenter=node['Node']['Datacenter']
            )
            
            # Filter by additional tags if specified
            if tags and len(tags) > 1:
                if all(tag in instance.tags for tag in tags):
                    instances.append(instance)
            else:
                instances.append(instance)
        
        return instances
        
    async def get_service(self, service_id: str) -> Optional[Dict[str, Any]]:
        """Get specific service details"""
        await self._ensure_initialized()
        
        services = await asyncio.to_thread(self.client.agent.services)
        return services.get(service_id)
    
    # Health Checking
    
    async def register_check(self, check_id: str, check_data: Dict[str, Any]) -> None:
        """Register a health check"""
        await self._ensure_initialized()
        
        await asyncio.to_thread(
            self.client.agent.check.register,
            name=check_id,
            **check_data
        )
        
    async def update_check(self, check_id: str, status: ServiceStatus,
                          output: Optional[str] = None) -> None:
        """Update health check status"""
        await self._ensure_initialized()
        
        if status == ServiceStatus.PASSING:
            await asyncio.to_thread(
                self.client.agent.check.ttl_pass,
                check_id,
                output
            )
        elif status == ServiceStatus.WARNING:
            await asyncio.to_thread(
                self.client.agent.check.ttl_warn,
                check_id,
                output
            )
        elif status == ServiceStatus.CRITICAL:
            await asyncio.to_thread(
                self.client.agent.check.ttl_fail,
                check_id,
                output
            )
    
    # KV Store Operations
    
    async def kv_put(self, key: str, value: Any, flags: int = 0) -> bool:
        """Put value in KV store"""
        await self._ensure_initialized()
        
        # Convert value to JSON if not string
        if not isinstance(value, str):
            value = json.dumps(value)
        
        return await asyncio.to_thread(
            self.client.kv.put,
            key,
            value,
            flags=flags
        )
        
    async def kv_get(self, key: str, recurse: bool = False) -> Optional[Any]:
        """Get value from KV store"""
        await self._ensure_initialized()
        
        index, data = await asyncio.to_thread(
            self.client.kv.get,
            key,
            recurse=recurse
        )
        
        if not data:
            return None
        
        if recurse:
            # Return list of key-value pairs
            result = []
            for item in data:
                value = item['Value']
                if value:
                    try:
                        value = json.loads(value.decode('utf-8'))
                    except:
                        value = value.decode('utf-8')
                result.append({
                    'key': item['Key'],
                    'value': value,
                    'flags': item['Flags']
                })
            return result
        else:
            # Return single value
            value = data['Value']
            if value:
                try:
                    return json.loads(value.decode('utf-8'))
                except:
                    return value.decode('utf-8')
            return None
            
    async def kv_delete(self, key: str, recurse: bool = False) -> bool:
        """Delete key from KV store"""
        await self._ensure_initialized()
        
        return await asyncio.to_thread(
            self.client.kv.delete,
            key,
            recurse=recurse
        )
        
    async def kv_watch(self, key: str, handler: Callable, 
                      recurse: bool = False) -> str:
        """Watch KV key for changes"""
        await self._ensure_initialized()
        
        watch_id = f"watch_{key}_{uuid.uuid4().hex[:8]}"
        self._watch_handlers[watch_id] = handler
        
        # Start watch task
        asyncio.create_task(self._watch_key(watch_id, key, recurse))
        
        return watch_id
        
    async def kv_unwatch(self, watch_id: str) -> None:
        """Stop watching KV key"""
        self._watch_handlers.pop(watch_id, None)
    
    # Distributed Locking
    
    async def acquire_lock(self, key: str, ttl: int = 15) -> bool:
        """Acquire distributed lock"""
        await self._ensure_initialized()
        
        if not self._session_id:
            raise Exception("No session available for locking")
        
        return await asyncio.to_thread(
            self.client.kv.put,
            key,
            "",
            acquire=self._session_id
        )
        
    async def release_lock(self, key: str) -> bool:
        """Release distributed lock"""
        await self._ensure_initialized()
        
        if not self._session_id:
            raise Exception("No session available for locking")
        
        return await asyncio.to_thread(
            self.client.kv.put,
            key,
            "",
            release=self._session_id
        )
    
    # Leader Election
    
    async def leader_election(self, key: str, node_id: str) -> bool:
        """Participate in leader election"""
        await self._ensure_initialized()
        
        # Try to acquire leadership
        acquired = await self.acquire_lock(f"leader/{key}")
        
        if acquired:
            # Store leader info
            await self.kv_put(
                f"leader/{key}/info",
                {
                    "node_id": node_id,
                    "elected_at": datetime.utcnow().isoformat(),
                    "session_id": self._session_id
                }
            )
            logger.info(f"Became leader for {key}")
            
        return acquired
        
    async def get_leader(self, key: str) -> Optional[Dict[str, Any]]:
        """Get current leader info"""
        return await self.kv_get(f"leader/{key}/info")
    
    # Configuration Management
    
    async def get_config(self, path: str) -> Dict[str, Any]:
        """Get configuration from Consul"""
        config_data = await self.kv_get(f"config/{path}", recurse=True)
        
        if not config_data:
            return {}
        
        # Convert list of KV pairs to nested dict
        config = {}
        for item in config_data:
            key_parts = item['key'].split('/')[2:]  # Remove 'config/path/'
            current = config
            
            for i, part in enumerate(key_parts[:-1]):
                if part not in current:
                    current[part] = {}
                current = current[part]
            
            current[key_parts[-1]] = item['value']
        
        return config
        
    async def set_config(self, path: str, config: Dict[str, Any]) -> None:
        """Set configuration in Consul"""
        await self._set_config_recursive(f"config/{path}", config)
        
    async def _set_config_recursive(self, prefix: str, data: Dict[str, Any]) -> None:
        """Recursively set configuration values"""
        for key, value in data.items():
            full_key = f"{prefix}/{key}"
            
            if isinstance(value, dict):
                await self._set_config_recursive(full_key, value)
            else:
                await self.kv_put(full_key, value)
    
    # Service Mesh Features
    
    async def register_sidecar(self, service_id: str, sidecar_port: int = 21000) -> str:
        """Register Envoy sidecar proxy"""
        sidecar_id = f"{service_id}-sidecar"
        
        # Get parent service
        service = await self.get_service(service_id)
        if not service:
            raise ValueError(f"Service {service_id} not found")
        
        # Register sidecar
        sidecar_def = ServiceDefinition(
            name=f"{service['Service']}-sidecar",
            service_id=sidecar_id,
            address=service['Address'],
            port=sidecar_port,
            tags=["envoy", "sidecar"],
            meta={
                "parent_service": service_id,
                "envoy_version": "1.24.0"
            },
            check={
                "TCP": f"{service['Address']}:{sidecar_port}",
                "Interval": "10s",
                "Timeout": "5s"
            }
        )
        
        await self.register_service(sidecar_def)
        
        # Configure service intentions
        await self._configure_service_intentions(service['Service'])
        
        return sidecar_id
        
    async def _configure_service_intentions(self, service_name: str) -> None:
        """Configure service mesh intentions"""
        # This would configure Consul Connect intentions
        # For now, just log
        logger.info(f"Configuring intentions for {service_name}")
    
    # Event System
    
    async def fire_event(self, name: str, payload: str = "") -> str:
        """Fire a Consul event"""
        await self._ensure_initialized()
        
        event_id = await asyncio.to_thread(
            self.client.event.fire,
            name,
            payload
        )
        
        return event_id
        
    async def watch_events(self, handler: Callable) -> None:
        """Watch for Consul events"""
        await self._ensure_initialized()
        
        asyncio.create_task(self._watch_events(handler))
    
    # Utility Methods
    
    async def _ensure_initialized(self) -> None:
        """Ensure client is initialized"""
        if not self._initialized:
            await self.initialize()
            
    async def _renew_session(self) -> None:
        """Renew session periodically"""
        while self._initialized and self._session_id:
            try:
                await asyncio.sleep(15)  # Renew every 15 seconds
                await asyncio.to_thread(
                    self.client.session.renew,
                    self._session_id
                )
            except Exception as e:
                logger.error(f"Failed to renew session: {e}")
                # Try to recreate session
                try:
                    session_data = await asyncio.to_thread(
                        self.client.session.create,
                        name=f"platformq-{socket.gethostname()}",
                        ttl=30,
                        behavior="delete"
                    )
                    self._session_id = session_data['ID']
                except Exception as e2:
                    logger.error(f"Failed to recreate session: {e2}")
                    
    async def _watch_key(self, watch_id: str, key: str, recurse: bool) -> None:
        """Watch a key for changes"""
        index = 0
        
        while watch_id in self._watch_handlers:
            try:
                # Long poll for changes
                new_index, data = await asyncio.to_thread(
                    self.client.kv.get,
                    key,
                    index=index,
                    wait='30s',
                    recurse=recurse
                )
                
                if new_index != index:
                    # Change detected
                    handler = self._watch_handlers.get(watch_id)
                    if handler:
                        await handler(key, data)
                    index = new_index
                    
            except Exception as e:
                logger.error(f"Error watching key {key}: {e}")
                await asyncio.sleep(5)
                
    async def _watch_events(self, handler: Callable) -> None:
        """Watch for events"""
        index = 0
        
        while self._initialized:
            try:
                # Long poll for events
                new_index, events = await asyncio.to_thread(
                    self.client.event.list,
                    index=index,
                    wait='30s'
                )
                
                if new_index != index:
                    # New events
                    for event in events:
                        await handler(event)
                    index = new_index
                    
            except Exception as e:
                logger.error(f"Error watching events: {e}")
                await asyncio.sleep(5)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Consul health"""
        try:
            leader = await asyncio.to_thread(self.client.status.leader)
            peers = await asyncio.to_thread(self.client.status.peers)
            
            return {
                "healthy": True,
                "leader": leader,
                "peers": peers,
                "session_id": self._session_id,
                "watches": len(self._watch_handlers)
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"healthy": False, "error": str(e)}
            
    async def close(self) -> None:
        """Close Consul client"""
        # Cancel all watch tasks
        self._watch_handlers.clear()
        
        # Cancel health check tasks
        for task in self._health_check_tasks.values():
            task.cancel()
        self._health_check_tasks.clear()
        
        # Destroy session
        if self._session_id:
            try:
                await asyncio.to_thread(
                    self.client.session.destroy,
                    self._session_id
                )
            except Exception as e:
                logger.error(f"Failed to destroy session: {e}")
                
        self._initialized = False
        logger.info("Consul client closed") 