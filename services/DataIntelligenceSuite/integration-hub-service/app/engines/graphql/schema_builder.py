"""
GraphQL Schema Builder

Builds the unified GraphQL schema from all service definitions.
"""

from typing import List, Dict, Any, Optional, Type
import strawberry
from strawberry.types import Info

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class SchemaBuilder:
    """
    Builds GraphQL schema by combining types and resolvers from all services
    """
    
    def __init__(self):
        self.type_registry: Dict[str, Type] = {}
        self.query_fields: Dict[str, Any] = {}
        self.mutation_fields: Dict[str, Any] = {}
        self.subscription_fields: Dict[str, Any] = {}
    
    async def build_query(self, resolver_manager) -> Type:
        """Build the root Query type"""
        logger.info("building_query_type")
        
        # Define query fields
        @strawberry.type
        class Query:
            # Data Catalog queries
            @strawberry.field
            async def search_catalog(
                self,
                info: Info,
                query: str,
                filters: Optional[Dict[str, Any]] = None,
                limit: int = 10,
                offset: int = 0
            ) -> List[Dict[str, Any]]:
                """Search the data catalog"""
                resolver = info.context["resolver_manager"]
                return await resolver.search_catalog(query, filters, limit, offset)
            
            @strawberry.field
            async def get_entity(
                self,
                info: Info,
                entity_id: str
            ) -> Optional[Dict[str, Any]]:
                """Get a specific catalog entity"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_entity(entity_id)
            
            @strawberry.field
            async def get_lineage(
                self,
                info: Info,
                entity_id: str,
                depth: int = 3,
                direction: str = "both"
            ) -> Dict[str, Any]:
                """Get data lineage for an entity"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_lineage(entity_id, depth, direction)
            
            # Pipeline queries
            @strawberry.field
            async def list_pipelines(
                self,
                info: Info,
                filter: Optional[Dict[str, Any]] = None,
                limit: int = 20,
                offset: int = 0
            ) -> List[Dict[str, Any]]:
                """List pipelines with optional filtering"""
                resolver = info.context["resolver_manager"]
                return await resolver.list_pipelines(filter, limit, offset)
            
            @strawberry.field
            async def get_pipeline(
                self,
                info: Info,
                pipeline_id: str
            ) -> Optional[Dict[str, Any]]:
                """Get a specific pipeline"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_pipeline(pipeline_id)
            
            @strawberry.field
            async def get_pipeline_executions(
                self,
                info: Info,
                pipeline_id: str,
                limit: int = 10
            ) -> List[Dict[str, Any]]:
                """Get pipeline execution history"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_pipeline_executions(pipeline_id, limit)
            
            # Data Quality queries
            @strawberry.field
            async def get_quality_profile(
                self,
                info: Info,
                dataset: str
            ) -> Dict[str, Any]:
                """Get data quality profile for a dataset"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_quality_profile(dataset)
            
            @strawberry.field
            async def list_quality_issues(
                self,
                info: Info,
                filter: Optional[Dict[str, Any]] = None,
                limit: int = 50,
                offset: int = 0
            ) -> List[Dict[str, Any]]:
                """List data quality issues"""
                resolver = info.context["resolver_manager"]
                return await resolver.list_quality_issues(filter, limit, offset)
            
            @strawberry.field
            async def list_quality_rules(
                self,
                info: Info
            ) -> List[Dict[str, Any]]:
                """List all quality rules"""
                resolver = info.context["resolver_manager"]
                return await resolver.list_quality_rules()
            
            # ML Model queries
            @strawberry.field
            async def list_models(
                self,
                info: Info,
                filter: Optional[Dict[str, Any]] = None,
                limit: int = 20,
                offset: int = 0
            ) -> List[Dict[str, Any]]:
                """List ML models"""
                resolver = info.context["resolver_manager"]
                return await resolver.list_models(filter, limit, offset)
            
            @strawberry.field
            async def get_model(
                self,
                info: Info,
                model_id: str
            ) -> Optional[Dict[str, Any]]:
                """Get a specific ML model"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_model(model_id)
            
            @strawberry.field
            async def get_model_versions(
                self,
                info: Info,
                model_id: str
            ) -> List[Dict[str, Any]]:
                """Get versions of an ML model"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_model_versions(model_id)
            
            # Graph queries
            @strawberry.field
            async def query_graph(
                self,
                info: Info,
                query: str,
                bindings: Optional[Dict[str, Any]] = None
            ) -> Dict[str, Any]:
                """Execute a graph query"""
                resolver = info.context["resolver_manager"]
                return await resolver.query_graph(query, bindings)
            
            @strawberry.field
            async def get_graph_analytics(
                self,
                info: Info,
                graph_id: str,
                algorithm: str,
                params: Optional[Dict[str, Any]] = None
            ) -> Dict[str, Any]:
                """Run graph analytics algorithm"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_graph_analytics(graph_id, algorithm, params)
            
            # Service Health queries
            @strawberry.field
            async def service_health(
                self,
                info: Info
            ) -> List[Dict[str, Any]]:
                """Get health status of all services"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_service_health()
            
            @strawberry.field
            async def system_metrics(
                self,
                info: Info,
                metric_names: List[str]
            ) -> Dict[str, Any]:
                """Get system metrics"""
                resolver = info.context["resolver_manager"]
                return await resolver.get_system_metrics(metric_names)
        
        return Query
    
    async def build_mutation(self, resolver_manager) -> Type:
        """Build the root Mutation type"""
        logger.info("building_mutation_type")
        
        @strawberry.type
        class Mutation:
            # Pipeline mutations
            @strawberry.mutation
            async def create_pipeline(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Create a new pipeline"""
                resolver = info.context["resolver_manager"]
                result = await resolver.create_pipeline(input)
                
                # Emit event
                await info.context["event_bus"].publish(
                    "pipeline.created",
                    {"pipeline_id": result.get("id"), "name": input.get("name")}
                )
                
                return result
            
            @strawberry.mutation
            async def update_pipeline(
                self,
                info: Info,
                pipeline_id: str,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Update an existing pipeline"""
                resolver = info.context["resolver_manager"]
                result = await resolver.update_pipeline(pipeline_id, input)
                
                # Emit event
                await info.context["event_bus"].publish(
                    "pipeline.updated",
                    {"pipeline_id": pipeline_id}
                )
                
                return result
            
            @strawberry.mutation
            async def execute_pipeline(
                self,
                info: Info,
                pipeline_id: str,
                params: Optional[Dict[str, Any]] = None
            ) -> Dict[str, Any]:
                """Execute a pipeline"""
                resolver = info.context["resolver_manager"]
                result = await resolver.execute_pipeline(pipeline_id, params)
                
                # Emit event
                await info.context["event_bus"].publish(
                    "pipeline.executed",
                    {"pipeline_id": pipeline_id, "execution_id": result.get("execution_id")}
                )
                
                return result
            
            # Data Quality mutations
            @strawberry.mutation
            async def run_quality_check(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Run a data quality check"""
                resolver = info.context["resolver_manager"]
                return await resolver.run_quality_check(input)
            
            @strawberry.mutation
            async def create_quality_rule(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Create a new quality rule"""
                resolver = info.context["resolver_manager"]
                return await resolver.create_quality_rule(input)
            
            # ML Operations mutations
            @strawberry.mutation
            async def train_model(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Train a new ML model"""
                resolver = info.context["resolver_manager"]
                result = await resolver.train_model(input)
                
                # Emit event
                await info.context["event_bus"].publish(
                    "model.training_started",
                    {"model_id": result.get("model_id"), "job_id": result.get("job_id")}
                )
                
                return result
            
            @strawberry.mutation
            async def deploy_model(
                self,
                info: Info,
                model_id: str,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Deploy an ML model"""
                resolver = info.context["resolver_manager"]
                result = await resolver.deploy_model(model_id, input)
                
                # Emit event
                await info.context["event_bus"].publish(
                    "model.deployed",
                    {"model_id": model_id, "deployment_id": result.get("deployment_id")}
                )
                
                return result
            
            # Graph mutations
            @strawberry.mutation
            async def create_graph_entity(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Create a graph entity"""
                resolver = info.context["resolver_manager"]
                return await resolver.create_graph_entity(input)
            
            @strawberry.mutation
            async def create_graph_relationship(
                self,
                info: Info,
                input: Dict[str, Any]
            ) -> Dict[str, Any]:
                """Create a graph relationship"""
                resolver = info.context["resolver_manager"]
                return await resolver.create_graph_relationship(input)
            
            # Cache management
            @strawberry.mutation
            async def invalidate_cache(
                self,
                info: Info,
                region: str,
                keys: Optional[List[str]] = None
            ) -> Dict[str, Any]:
                """Invalidate cache entries"""
                resolver = info.context["resolver_manager"]
                return await resolver.invalidate_cache(region, keys)
            
            # Lineage updates
            @strawberry.mutation
            async def trigger_lineage_update(
                self,
                info: Info,
                entity_id: str
            ) -> Dict[str, Any]:
                """Trigger lineage update for an entity"""
                resolver = info.context["resolver_manager"]
                return await resolver.trigger_lineage_update(entity_id)
        
        return Mutation
    
    async def build_subscription(self, resolver_manager) -> Type:
        """Build the root Subscription type"""
        logger.info("building_subscription_type")
        
        @strawberry.type
        class Subscription:
            @strawberry.subscription
            async def pipeline_status(
                self,
                info: Info,
                pipeline_id: str
            ) -> AsyncGenerator[Dict[str, Any], None]:
                """Subscribe to pipeline execution status updates"""
                resolver = info.context["resolver_manager"]
                async for update in resolver.subscribe_pipeline_status(pipeline_id):
                    yield update
            
            @strawberry.subscription
            async def quality_alerts(
                self,
                info: Info,
                severity: Optional[str] = None
            ) -> AsyncGenerator[Dict[str, Any], None]:
                """Subscribe to data quality alerts"""
                resolver = info.context["resolver_manager"]
                async for alert in resolver.subscribe_quality_alerts(severity):
                    yield alert
            
            @strawberry.subscription
            async def model_metrics(
                self,
                info: Info,
                model_id: str
            ) -> AsyncGenerator[Dict[str, Any], None]:
                """Subscribe to model performance metrics"""
                resolver = info.context["resolver_manager"]
                async for metrics in resolver.subscribe_model_metrics(model_id):
                    yield metrics
            
            @strawberry.subscription
            async def system_events(
                self,
                info: Info,
                services: Optional[List[str]] = None
            ) -> AsyncGenerator[Dict[str, Any], None]:
                """Subscribe to system events"""
                resolver = info.context["resolver_manager"]
                async for event in resolver.subscribe_system_events(services):
                    yield event
        
        return Subscription
    
    def register_type(self, name: str, type_class: Type):
        """Register a custom type"""
        self.type_registry[name] = type_class
    
    def add_query_field(self, name: str, field: Any):
        """Add a field to the Query type"""
        self.query_fields[name] = field
    
    def add_mutation_field(self, name: str, field: Any):
        """Add a field to the Mutation type"""
        self.mutation_fields[name] = field
    
    def add_subscription_field(self, name: str, field: Any):
        """Add a field to the Subscription type"""
        self.subscription_fields[name] = field 