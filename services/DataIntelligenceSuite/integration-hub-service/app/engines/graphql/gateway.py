"""
GraphQL Gateway Engine

Provides a unified GraphQL API for all DataIntelligenceSuite services.
"""

import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime

from strawberry import Schema
from strawberry.extensions import AddValidationRules, QueryDepthLimiter, MaxTokensLimiter
from strawberry.schema.config import StrawberryConfig
from strawberry.dataloader import DataLoader

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

from .schema_builder import SchemaBuilder
from .resolver_manager import ResolverManager
from .federation_manager import FederationManager
from .dataloader_registry import DataLoaderRegistry

logger = StructuredLogger.get_logger(__name__)


class GraphQLGateway:
    """
    Main GraphQL Gateway engine that provides unified API
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        
        # Core components
        self.schema_builder = SchemaBuilder()
        self.resolver_manager = ResolverManager(vault_consul)
        self.federation_manager = FederationManager(vault_consul)
        self.dataloader_registry = DataLoaderRegistry()
        
        # GraphQL schema
        self.schema: Optional[Schema] = None
        
        # Configuration
        self.config = {
            "max_query_depth": 10,
            "max_token_count": 1000,
            "enable_introspection": True,
            "enable_federation": True,
            "cache_ttl": 300,
            "batch_interval_ms": 10
        }
        
        # Metrics
        self.metrics = {
            "queries_total": 0,
            "mutations_total": 0,
            "subscriptions_total": 0,
            "errors_total": 0,
            "avg_response_time_ms": 0
        }
    
    async def initialize(self):
        """Initialize the GraphQL gateway"""
        logger.info("initializing_graphql_gateway")
        
        try:
            # Load configuration from Consul
            await self._load_configuration()
            
            # Initialize resolver manager
            await self.resolver_manager.initialize()
            
            # Initialize federation manager if enabled
            if self.config["enable_federation"]:
                await self.federation_manager.initialize()
            
            # Build GraphQL schema
            await self._build_schema()
            
            # Setup monitoring
            self._setup_monitoring()
            
            logger.info("graphql_gateway_initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize GraphQL gateway: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        logger.info("cleaning_up_graphql_gateway")
        
        await self.resolver_manager.cleanup()
        await self.federation_manager.cleanup()
        
        logger.info("graphql_gateway_cleaned_up")
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/graphql-gateway")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def _build_schema(self):
        """Build the GraphQL schema"""
        logger.info("building_graphql_schema")
        
        # Get schema components
        query = await self.schema_builder.build_query(self.resolver_manager)
        mutation = await self.schema_builder.build_mutation(self.resolver_manager)
        subscription = await self.schema_builder.build_subscription(self.resolver_manager)
        
        # Configure schema
        config = StrawberryConfig(auto_camel_case=True)
        
        # Create schema with extensions
        self.schema = Schema(
            query=query,
            mutation=mutation,
            subscription=subscription,
            config=config,
            extensions=[
                QueryDepthLimiter(max_depth=self.config["max_query_depth"]),
                MaxTokensLimiter(max_token_count=self.config["max_token_count"]),
                AddValidationRules([])
            ]
        )
        
        logger.info("graphql_schema_built")
    
    def _setup_monitoring(self):
        """Setup monitoring for GraphQL operations"""
        # This would integrate with the metrics collector
        pass
    
    def get_context(self) -> Dict[str, Any]:
        """Get GraphQL context for resolvers"""
        return {
            "resolver_manager": self.resolver_manager,
            "dataloader_registry": self.dataloader_registry,
            "event_bus": self.event_bus,
            "vault_consul": self.vault_consul,
            "metrics": self.metrics
        }
    
    async def execute_query(self, query: str, variables: Optional[Dict] = None, 
                          operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Execute a GraphQL query"""
        start_time = datetime.utcnow()
        
        try:
            # Execute query
            result = await self.schema.execute(
                query,
                variable_values=variables,
                operation_name=operation_name,
                context_value=self.get_context()
            )
            
            # Update metrics
            self.metrics["queries_total"] += 1
            
            # Calculate response time
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_response_time(response_time)
            
            return result
            
        except Exception as e:
            self.metrics["errors_total"] += 1
            logger.error(f"Query execution error: {e}")
            raise
    
    async def execute_mutation(self, mutation: str, variables: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute a GraphQL mutation"""
        start_time = datetime.utcnow()
        
        try:
            # Execute mutation
            result = await self.schema.execute(
                mutation,
                variable_values=variables,
                context_value=self.get_context()
            )
            
            # Update metrics
            self.metrics["mutations_total"] += 1
            
            # Calculate response time
            response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_response_time(response_time)
            
            # Emit event for mutation
            await self._emit_mutation_event(mutation, result)
            
            return result
            
        except Exception as e:
            self.metrics["errors_total"] += 1
            logger.error(f"Mutation execution error: {e}")
            raise
    
    async def execute_subscription(self, subscription: str, variables: Optional[Dict] = None):
        """Execute a GraphQL subscription"""
        try:
            # Execute subscription
            async for result in self.schema.subscribe(
                subscription,
                variable_values=variables,
                context_value=self.get_context()
            ):
                self.metrics["subscriptions_total"] += 1
                yield result
                
        except Exception as e:
            self.metrics["errors_total"] += 1
            logger.error(f"Subscription execution error: {e}")
            raise
    
    def _update_avg_response_time(self, response_time: float):
        """Update average response time metric"""
        total_requests = (self.metrics["queries_total"] + 
                         self.metrics["mutations_total"])
        
        if total_requests == 1:
            self.metrics["avg_response_time_ms"] = response_time
        else:
            # Calculate running average
            current_avg = self.metrics["avg_response_time_ms"]
            self.metrics["avg_response_time_ms"] = (
                (current_avg * (total_requests - 1) + response_time) / total_requests
            )
    
    async def _emit_mutation_event(self, mutation: str, result: Dict[str, Any]):
        """Emit event for mutation execution"""
        # Parse mutation to determine type
        mutation_type = self._parse_mutation_type(mutation)
        
        await self.event_bus.publish(
            "graphql.mutation.executed",
            {
                "mutation_type": mutation_type,
                "timestamp": datetime.utcnow().isoformat(),
                "success": "errors" not in result
            }
        )
    
    def _parse_mutation_type(self, mutation: str) -> str:
        """Parse mutation string to extract type"""
        # Simple parsing logic - would be more sophisticated in production
        if "createPipeline" in mutation:
            return "create_pipeline"
        elif "updatePipeline" in mutation:
            return "update_pipeline"
        elif "deletePipeline" in mutation:
            return "delete_pipeline"
        else:
            return "unknown"
    
    async def get_schema_sdl(self) -> str:
        """Get schema SDL for federation"""
        return self.schema.as_str() if self.schema else ""
    
    async def health_check(self) -> Dict[str, Any]:
        """Check gateway health"""
        return {
            "healthy": self.schema is not None,
            "resolver_manager": await self.resolver_manager.is_healthy(),
            "federation_enabled": self.config["enable_federation"],
            "metrics": self.metrics
        } 