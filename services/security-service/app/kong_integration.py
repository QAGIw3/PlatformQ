"""
Kong API Gateway Integration

Manages Kong API Gateway configuration, plugins, and security policies.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json
import httpx

import hvac
import consul.aio

logger = logging.getLogger(__name__)


class KongPluginType(Enum):
    """Supported Kong plugins"""
    OAUTH2 = "oauth2"
    JWT = "jwt"
    KEY_AUTH = "key-auth"
    BASIC_AUTH = "basic-auth"
    ACL = "acl"
    RATE_LIMITING = "rate-limiting"
    REQUEST_TRANSFORMER = "request-transformer"
    RESPONSE_TRANSFORMER = "response-transformer"
    CORRELATION_ID = "correlation-id"
    REQUEST_VALIDATION = "request-validation"
    PROMETHEUS = "prometheus"
    ZIPKIN = "zipkin"
    BOT_DETECTION = "bot-detection"
    IP_RESTRICTION = "ip-restriction"


@dataclass
class KongService:
    """Kong service configuration"""
    name: str
    protocol: str = "http"
    host: str = "localhost"
    port: int = 80
    path: Optional[str] = None
    retries: int = 5
    connect_timeout: int = 60000
    write_timeout: int = 60000
    read_timeout: int = 60000
    tags: List[str] = None


@dataclass
class KongRoute:
    """Kong route configuration"""
    name: str
    service_id: str
    protocols: List[str] = None
    methods: List[str] = None
    hosts: List[str] = None
    paths: List[str] = None
    headers: Dict[str, str] = None
    https_redirect_status_code: int = 426
    regex_priority: int = 0
    strip_path: bool = True
    preserve_host: bool = False
    tags: List[str] = None


@dataclass
class KongConsumer:
    """Kong consumer configuration"""
    username: str
    custom_id: Optional[str] = None
    tags: List[str] = None


class KongAPIGatewayManager:
    """
    Manages Kong API Gateway configuration and policies.
    
    Features:
    - Service and route management
    - Plugin configuration
    - OAuth2/OIDC integration
    - Rate limiting
    - API key management
    - Security policies
    - Monitoring integration
    """
    
    def __init__(self,
                 kong_admin_url: str,
                 vault_client: hvac.Client,
                 consul_client: consul.aio.Consul):
        self.kong_admin_url = kong_admin_url
        self.vault = vault_client
        self.consul = consul_client
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self._services: Dict[str, Dict[str, Any]] = {}
        self._routes: Dict[str, Dict[str, Any]] = {}
        self._consumers: Dict[str, Dict[str, Any]] = {}
        self._plugins: Dict[str, List[Dict[str, Any]]] = {}
        
    async def initialize(self):
        """Initialize Kong manager"""
        logger.info("Initializing Kong API Gateway manager")
        
        # Verify Kong connectivity
        await self._verify_kong_connection()
        
        # Load existing configuration
        await self._load_configuration()
        
        # Sync with Consul
        await self._sync_with_consul()
        
        # Start configuration monitor
        asyncio.create_task(self._monitor_configuration())
        
        logger.info("Kong API Gateway manager initialized")
        
    async def _verify_kong_connection(self):
        """Verify connection to Kong Admin API"""
        try:
            response = await self.http_client.get(f"{self.kong_admin_url}/")
            response.raise_for_status()
            
            info = response.json()
            logger.info(f"Connected to Kong {info.get('version', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to connect to Kong: {e}")
            raise
            
    async def register_service(self, service_config: Dict[str, Any]) -> Dict[str, Any]:
        """Register a service with Kong"""
        try:
            # Create service object
            service = KongService(
                name=service_config["name"],
                protocol=service_config.get("protocol", "http"),
                host=service_config.get("host", "localhost"),
                port=service_config.get("port", 80),
                path=service_config.get("path"),
                retries=service_config.get("retries", 5),
                connect_timeout=service_config.get("connect_timeout", 60000),
                write_timeout=service_config.get("write_timeout", 60000),
                read_timeout=service_config.get("read_timeout", 60000),
                tags=service_config.get("tags", [])
            )
            
            # Create or update service
            response = await self.http_client.put(
                f"{self.kong_admin_url}/services/{service.name}",
                json={
                    "name": service.name,
                    "protocol": service.protocol,
                    "host": service.host,
                    "port": service.port,
                    "path": service.path,
                    "retries": service.retries,
                    "connect_timeout": service.connect_timeout,
                    "write_timeout": service.write_timeout,
                    "read_timeout": service.read_timeout,
                    "tags": service.tags
                }
            )
            response.raise_for_status()
            
            service_data = response.json()
            self._services[service.name] = service_data
            
            # Store in Consul
            await self._store_service_config(service.name, service_data)
            
            logger.info(f"Registered service {service.name}")
            return service_data
            
        except Exception as e:
            logger.error(f"Failed to register service: {e}")
            raise
            
    async def create_route(self, route_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a route for a service"""
        try:
            route = KongRoute(
                name=route_config["name"],
                service_id=route_config["service_id"],
                protocols=route_config.get("protocols", ["http", "https"]),
                methods=route_config.get("methods"),
                hosts=route_config.get("hosts"),
                paths=route_config.get("paths"),
                headers=route_config.get("headers"),
                https_redirect_status_code=route_config.get("https_redirect_status_code", 426),
                regex_priority=route_config.get("regex_priority", 0),
                strip_path=route_config.get("strip_path", True),
                preserve_host=route_config.get("preserve_host", False),
                tags=route_config.get("tags", [])
            )
            
            # Create route
            response = await self.http_client.post(
                f"{self.kong_admin_url}/services/{route.service_id}/routes",
                json={
                    "name": route.name,
                    "protocols": route.protocols,
                    "methods": route.methods,
                    "hosts": route.hosts,
                    "paths": route.paths,
                    "headers": route.headers,
                    "https_redirect_status_code": route.https_redirect_status_code,
                    "regex_priority": route.regex_priority,
                    "strip_path": route.strip_path,
                    "preserve_host": route.preserve_host,
                    "tags": route.tags
                }
            )
            response.raise_for_status()
            
            route_data = response.json()
            self._routes[route.name] = route_data
            
            logger.info(f"Created route {route.name}")
            return route_data
            
        except Exception as e:
            logger.error(f"Failed to create route: {e}")
            raise
            
    async def configure_plugin(self, 
                             service_name: str,
                             plugin_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure a plugin for a service"""
        try:
            plugin_type = plugin_config["name"]
            
            # Get plugin-specific configuration
            if plugin_type == KongPluginType.OAUTH2.value:
                config = await self._configure_oauth2(plugin_config)
            elif plugin_type == KongPluginType.JWT.value:
                config = await self._configure_jwt(plugin_config)
            elif plugin_type == KongPluginType.RATE_LIMITING.value:
                config = self._configure_rate_limiting(plugin_config)
            elif plugin_type == KongPluginType.KEY_AUTH.value:
                config = await self._configure_key_auth(plugin_config)
            else:
                config = plugin_config.get("config", {})
                
            # Create plugin
            response = await self.http_client.post(
                f"{self.kong_admin_url}/services/{service_name}/plugins",
                json={
                    "name": plugin_type,
                    "config": config,
                    "enabled": plugin_config.get("enabled", True),
                    "tags": plugin_config.get("tags", [])
                }
            )
            response.raise_for_status()
            
            plugin_data = response.json()
            
            # Track plugin
            if service_name not in self._plugins:
                self._plugins[service_name] = []
            self._plugins[service_name].append(plugin_data)
            
            logger.info(f"Configured {plugin_type} plugin for {service_name}")
            return plugin_data
            
        except Exception as e:
            logger.error(f"Failed to configure plugin: {e}")
            raise
            
    async def create_consumer(self, username: str, custom_id: Optional[str] = None) -> Dict[str, Any]:
        """Create a consumer"""
        try:
            consumer = KongConsumer(
                username=username,
                custom_id=custom_id,
                tags=["platformq"]
            )
            
            response = await self.http_client.put(
                f"{self.kong_admin_url}/consumers/{username}",
                json={
                    "username": consumer.username,
                    "custom_id": consumer.custom_id,
                    "tags": consumer.tags
                }
            )
            response.raise_for_status()
            
            consumer_data = response.json()
            self._consumers[username] = consumer_data
            
            logger.info(f"Created consumer {username}")
            return consumer_data
            
        except Exception as e:
            logger.error(f"Failed to create consumer: {e}")
            raise
            
    async def create_api_key(self, username: str) -> Dict[str, str]:
        """Create API key for a consumer"""
        try:
            # Generate secure API key
            api_key = await self._generate_api_key()
            
            # Create key-auth credential
            response = await self.http_client.post(
                f"{self.kong_admin_url}/consumers/{username}/key-auth",
                json={"key": api_key}
            )
            response.raise_for_status()
            
            # Store key in Vault
            await self._store_api_key(username, api_key)
            
            logger.info(f"Created API key for {username}")
            return {"username": username, "api_key": api_key}
            
        except Exception as e:
            logger.error(f"Failed to create API key: {e}")
            raise
            
    async def create_oauth2_application(self, 
                                      consumer_username: str,
                                      app_name: str,
                                      redirect_uris: List[str]) -> Dict[str, Any]:
        """Create OAuth2 application for a consumer"""
        try:
            # Create OAuth2 credential
            response = await self.http_client.post(
                f"{self.kong_admin_url}/consumers/{consumer_username}/oauth2",
                json={
                    "name": app_name,
                    "redirect_uris": redirect_uris,
                    "client_type": "confidential",
                    "hash_secret": True
                }
            )
            response.raise_for_status()
            
            oauth_data = response.json()
            
            # Store credentials in Vault
            await self._store_oauth_credentials(consumer_username, app_name, oauth_data)
            
            logger.info(f"Created OAuth2 application {app_name} for {consumer_username}")
            return oauth_data
            
        except Exception as e:
            logger.error(f"Failed to create OAuth2 application: {e}")
            raise
            
    async def update_rate_limits(self, 
                               service_name: str,
                               limits: Dict[str, int]) -> Dict[str, Any]:
        """Update rate limits for a service"""
        try:
            # Find existing rate-limiting plugin
            plugins = self._plugins.get(service_name, [])
            rate_limit_plugin = next(
                (p for p in plugins if p["name"] == "rate-limiting"),
                None
            )
            
            config = {
                "second": limits.get("second"),
                "minute": limits.get("minute"),
                "hour": limits.get("hour"),
                "day": limits.get("day"),
                "month": limits.get("month"),
                "year": limits.get("year"),
                "limit_by": limits.get("limit_by", "consumer"),
                "policy": limits.get("policy", "local"),
                "fault_tolerant": limits.get("fault_tolerant", True),
                "hide_client_headers": limits.get("hide_client_headers", False)
            }
            
            # Remove None values
            config = {k: v for k, v in config.items() if v is not None}
            
            if rate_limit_plugin:
                # Update existing plugin
                response = await self.http_client.patch(
                    f"{self.kong_admin_url}/plugins/{rate_limit_plugin['id']}",
                    json={"config": config}
                )
            else:
                # Create new plugin
                response = await self.http_client.post(
                    f"{self.kong_admin_url}/services/{service_name}/plugins",
                    json={
                        "name": "rate-limiting",
                        "config": config,
                        "enabled": True
                    }
                )
                
            response.raise_for_status()
            
            logger.info(f"Updated rate limits for {service_name}")
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to update rate limits: {e}")
            raise
            
    async def configure_security_plugins(self, service_name: str) -> List[Dict[str, Any]]:
        """Configure standard security plugins for a service"""
        plugins = []
        
        # Correlation ID for request tracking
        correlation_plugin = await self.configure_plugin(service_name, {
            "name": "correlation-id",
            "config": {
                "header_name": "X-Request-ID",
                "generator": "uuid",
                "echo_downstream": True
            }
        })
        plugins.append(correlation_plugin)
        
        # Bot detection
        bot_plugin = await self.configure_plugin(service_name, {
            "name": "bot-detection",
            "config": {
                "allow": ["curl/7.*", "PostmanRuntime/7.*"],
                "deny": ["bot", "crawler", "spider"]
            }
        })
        plugins.append(bot_plugin)
        
        # Request transformer for security headers
        transformer_plugin = await self.configure_plugin(service_name, {
            "name": "request-transformer",
            "config": {
                "add": {
                    "headers": [
                        "X-Frame-Options:DENY",
                        "X-Content-Type-Options:nosniff",
                        "X-XSS-Protection:1; mode=block",
                        "Strict-Transport-Security:max-age=31536000; includeSubDomains"
                    ]
                }
            }
        })
        plugins.append(transformer_plugin)
        
        # Prometheus metrics
        metrics_plugin = await self.configure_plugin(service_name, {
            "name": "prometheus",
            "config": {
                "per_consumer": True,
                "status_code_metrics": True,
                "latency_metrics": True,
                "bandwidth_metrics": True,
                "upstream_health_metrics": True
            }
        })
        plugins.append(metrics_plugin)
        
        logger.info(f"Configured security plugins for {service_name}")
        return plugins
        
    async def health_check(self) -> bool:
        """Check Kong health"""
        try:
            response = await self.http_client.get(f"{self.kong_admin_url}/status")
            response.raise_for_status()
            
            status = response.json()
            return status.get("database", {}).get("reachable", False)
        except Exception:
            return False
            
    async def get_metrics(self) -> Dict[str, Any]:
        """Get Kong metrics"""
        try:
            metrics = {
                "services": len(self._services),
                "routes": len(self._routes),
                "consumers": len(self._consumers),
                "plugins": sum(len(plugins) for plugins in self._plugins.values())
            }
            
            # Get node info
            response = await self.http_client.get(f"{self.kong_admin_url}/")
            if response.status_code == 200:
                info = response.json()
                metrics["node_id"] = info.get("node_id")
                metrics["version"] = info.get("version")
                
            # Get status
            response = await self.http_client.get(f"{self.kong_admin_url}/status")
            if response.status_code == 200:
                status = response.json()
                metrics["connections_active"] = status.get("server", {}).get("connections_active", 0)
                metrics["connections_accepted"] = status.get("server", {}).get("connections_accepted", 0)
                metrics["connections_handled"] = status.get("server", {}).get("connections_handled", 0)
                metrics["total_requests"] = status.get("server", {}).get("total_requests", 0)
                
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {}
            
    async def sync_service_config(self, service_config: Dict[str, Any]):
        """Sync service configuration from Consul"""
        try:
            service_name = service_config["name"]
            
            # Update or create service
            await self.register_service(service_config)
            
            # Configure routes
            if "routes" in service_config:
                for route_config in service_config["routes"]:
                    route_config["service_id"] = service_name
                    await self.create_route(route_config)
                    
            # Configure plugins
            if "plugins" in service_config:
                for plugin_config in service_config["plugins"]:
                    await self.configure_plugin(service_name, plugin_config)
                    
            logger.info(f"Synced configuration for {service_name}")
            
        except Exception as e:
            logger.error(f"Failed to sync service config: {e}")
            
    async def _configure_oauth2(self, plugin_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure OAuth2 plugin"""
        # Get OAuth2 configuration from Vault
        oauth_config = await self._get_oauth_config()
        
        return {
            "scopes": plugin_config.get("scopes", ["openid", "profile", "email"]),
            "mandatory_scope": plugin_config.get("mandatory_scope", True),
            "provision_key": oauth_config["provision_key"],
            "token_expiration": plugin_config.get("token_expiration", 7200),
            "enable_authorization_code": True,
            "enable_client_credentials": True,
            "enable_implicit_grant": False,
            "enable_password_grant": False,
            "hide_credentials": True,
            "accept_http_if_already_terminated": True,
            "anonymous": plugin_config.get("anonymous"),
            "global_credentials": plugin_config.get("global_credentials", False),
            "auth_header_name": "Authorization",
            "refresh_token_ttl": plugin_config.get("refresh_token_ttl", 2592000),
            "reuse_refresh_token": plugin_config.get("reuse_refresh_token", False)
        }
        
    async def _configure_jwt(self, plugin_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure JWT plugin"""
        # Get JWT configuration from Vault
        jwt_config = await self._get_jwt_config()
        
        return {
            "uri_param_names": plugin_config.get("uri_param_names", ["jwt"]),
            "cookie_names": plugin_config.get("cookie_names", []),
            "header_names": plugin_config.get("header_names", ["Authorization"]),
            "claims_to_verify": plugin_config.get("claims_to_verify", ["exp", "nbf"]),
            "key_claim_name": plugin_config.get("key_claim_name", "iss"),
            "secret_is_base64": plugin_config.get("secret_is_base64", False),
            "anonymous": plugin_config.get("anonymous"),
            "run_on_preflight": plugin_config.get("run_on_preflight", True),
            "maximum_expiration": plugin_config.get("maximum_expiration", 0)
        }
        
    def _configure_rate_limiting(self, plugin_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure rate limiting plugin"""
        return {
            "second": plugin_config.get("second"),
            "minute": plugin_config.get("minute"),
            "hour": plugin_config.get("hour"),
            "day": plugin_config.get("day"),
            "month": plugin_config.get("month"),
            "year": plugin_config.get("year"),
            "limit_by": plugin_config.get("limit_by", "consumer"),
            "header_name": plugin_config.get("header_name"),
            "policy": plugin_config.get("policy", "local"),
            "fault_tolerant": plugin_config.get("fault_tolerant", True),
            "redis_host": plugin_config.get("redis_host"),
            "redis_port": plugin_config.get("redis_port", 6379),
            "redis_password": plugin_config.get("redis_password"),
            "redis_timeout": plugin_config.get("redis_timeout", 2000),
            "redis_database": plugin_config.get("redis_database", 0),
            "hide_client_headers": plugin_config.get("hide_client_headers", False)
        }
        
    async def _configure_key_auth(self, plugin_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure key-auth plugin"""
        return {
            "key_names": plugin_config.get("key_names", ["apikey", "X-API-Key"]),
            "key_in_body": plugin_config.get("key_in_body", False),
            "key_in_header": plugin_config.get("key_in_header", True),
            "key_in_query": plugin_config.get("key_in_query", True),
            "hide_credentials": plugin_config.get("hide_credentials", True),
            "anonymous": plugin_config.get("anonymous"),
            "run_on_preflight": plugin_config.get("run_on_preflight", True)
        }
        
    async def _generate_api_key(self) -> str:
        """Generate secure API key"""
        import secrets
        return secrets.token_urlsafe(32)
        
    async def _store_api_key(self, username: str, api_key: str):
        """Store API key in Vault"""
        try:
            self.vault.write(
                f"secret/kong/consumers/{username}/api-key",
                api_key=api_key,
                created_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            logger.error(f"Failed to store API key: {e}")
            
    async def _store_oauth_credentials(self, username: str, app_name: str, oauth_data: Dict[str, Any]):
        """Store OAuth credentials in Vault"""
        try:
            self.vault.write(
                f"secret/kong/consumers/{username}/oauth2/{app_name}",
                client_id=oauth_data["client_id"],
                client_secret=oauth_data["client_secret"],
                created_at=datetime.utcnow().isoformat()
            )
        except Exception as e:
            logger.error(f"Failed to store OAuth credentials: {e}")
            
    async def _get_oauth_config(self) -> Dict[str, Any]:
        """Get OAuth configuration from Vault"""
        try:
            response = self.vault.read("secret/kong/oauth2/config")
            if response and "data" in response:
                return response["data"]["data"]
                
            # Generate provision key if not exists
            provision_key = await self._generate_api_key()
            self.vault.write(
                "secret/kong/oauth2/config",
                provision_key=provision_key
            )
            return {"provision_key": provision_key}
            
        except Exception as e:
            logger.error(f"Failed to get OAuth config: {e}")
            return {"provision_key": "default-provision-key"}
            
    async def _get_jwt_config(self) -> Dict[str, Any]:
        """Get JWT configuration from Vault"""
        try:
            response = self.vault.read("secret/kong/jwt/config")
            if response and "data" in response:
                return response["data"]["data"]
            return {}
        except Exception:
            return {}
            
    async def _store_service_config(self, service_name: str, config: Dict[str, Any]):
        """Store service configuration in Consul"""
        try:
            await self.consul.kv.put(
                f"kong/services/{service_name}",
                json.dumps(config)
            )
        except Exception as e:
            logger.error(f"Failed to store service config: {e}")
            
    async def _load_configuration(self):
        """Load Kong configuration"""
        try:
            # Load services
            response = await self.http_client.get(f"{self.kong_admin_url}/services")
            if response.status_code == 200:
                services = response.json()
                for service in services.get("data", []):
                    self._services[service["name"]] = service
                    
            # Load routes
            response = await self.http_client.get(f"{self.kong_admin_url}/routes")
            if response.status_code == 200:
                routes = response.json()
                for route in routes.get("data", []):
                    if "name" in route:
                        self._routes[route["name"]] = route
                        
            # Load consumers
            response = await self.http_client.get(f"{self.kong_admin_url}/consumers")
            if response.status_code == 200:
                consumers = response.json()
                for consumer in consumers.get("data", []):
                    self._consumers[consumer["username"]] = consumer
                    
            logger.info(
                f"Loaded Kong configuration: {len(self._services)} services, "
                f"{len(self._routes)} routes, {len(self._consumers)} consumers"
            )
            
        except Exception as e:
            logger.error(f"Failed to load Kong configuration: {e}")
            
    async def _sync_with_consul(self):
        """Sync configuration with Consul"""
        try:
            # Get Kong configuration from Consul
            _, services = await self.consul.kv.get("kong/services", recurse=True)
            
            if services:
                for service_kv in services:
                    if service_kv["Key"].count("/") == 2:  # kong/services/{name}
                        service_config = json.loads(service_kv["Value"])
                        await self.sync_service_config(service_config)
                        
        except Exception as e:
            logger.error(f"Failed to sync with Consul: {e}")
            
    async def _monitor_configuration(self):
        """Monitor configuration changes"""
        while True:
            try:
                # Reload configuration periodically
                await self._load_configuration()
                
                # Check for configuration drift
                await self._check_configuration_drift()
                
            except Exception as e:
                logger.error(f"Configuration monitor error: {e}")
                
            await asyncio.sleep(300)  # Check every 5 minutes
            
    async def _check_configuration_drift(self):
        """Check for configuration drift between Kong and Consul"""
        try:
            # Get expected configuration from Consul
            _, consul_services = await self.consul.kv.get("kong/services", recurse=True)
            
            if consul_services:
                expected_services = set()
                for service_kv in consul_services:
                    if service_kv["Key"].count("/") == 2:
                        service_name = service_kv["Key"].split("/")[-1]
                        expected_services.add(service_name)
                        
                # Check for missing services
                actual_services = set(self._services.keys())
                missing = expected_services - actual_services
                extra = actual_services - expected_services
                
                if missing:
                    logger.warning(f"Missing Kong services: {missing}")
                if extra:
                    logger.warning(f"Extra Kong services: {extra}")
                    
        except Exception as e:
            logger.error(f"Failed to check configuration drift: {e}")
            
    async def shutdown(self):
        """Shutdown Kong manager"""
        logger.info("Shutting down Kong API Gateway manager")
        
        # Close HTTP client
        await self.http_client.aclose()
        
        logger.info("Kong API Gateway manager shutdown complete") 