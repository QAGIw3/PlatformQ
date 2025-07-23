"""
GraphQL Federation for Metadata

Provides unified GraphQL API for accessing metadata across multiple systems.
"""

from typing import Any, Dict, List, Optional, Union, Callable, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import strawberry
from strawberry.federation import FederationVersion
from strawberry.schema.config import StrawberryConfig
from strawberry.extensions import Extension
from strawberry.types import Info
from strawberry.dataloader import DataLoader
import aiohttp

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


# GraphQL Types

@strawberry.enum
class DataPlatform:
    """Supported data platforms"""
    SPARK = "SPARK"
    KAFKA = "KAFKA"
    PULSAR = "PULSAR"
    POSTGRES = "POSTGRES"
    CASSANDRA = "CASSANDRA"
    S3 = "S3"
    ICEBERG = "ICEBERG"
    DELTA = "DELTA"
    HUDI = "HUDI"


@strawberry.enum
class EntityType:
    """Metadata entity types"""
    DATASET = "DATASET"
    DATAFLOW = "DATAFLOW"
    DATAJOB = "DATAJOB"
    DASHBOARD = "DASHBOARD"
    MLMODEL = "MLMODEL"
    FEATURE = "FEATURE"
    METRIC = "METRIC"


@strawberry.type
class DatasetSchema:
    """Dataset schema"""
    fields: List["SchemaField"]
    schema_version: int
    created_at: datetime
    
    @strawberry.field
    def field_count(self) -> int:
        return len(self.fields)


@strawberry.type
class SchemaField:
    """Schema field"""
    name: str
    type: str
    nullable: bool
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    @strawberry.field
    def is_primary_key(self) -> bool:
        return "primary_key" in self.tags
    
    @strawberry.field
    def is_partition_key(self) -> bool:
        return "partition_key" in self.tags


@strawberry.type
class DataQuality:
    """Data quality metrics"""
    completeness: float
    accuracy: float
    consistency: float
    timeliness: float
    uniqueness: float
    validity: float
    
    @strawberry.field
    def overall_score(self) -> float:
        scores = [
            self.completeness,
            self.accuracy,
            self.consistency,
            self.timeliness,
            self.uniqueness,
            self.validity
        ]
        return sum(scores) / len(scores)


@strawberry.type
class Lineage:
    """Data lineage"""
    upstream: List["Dataset"]
    downstream: List["Dataset"]
    jobs: List["DataJob"]
    
    @strawberry.field
    def upstream_count(self) -> int:
        return len(self.upstream)
    
    @strawberry.field
    def downstream_count(self) -> int:
        return len(self.downstream)


@strawberry.type
class Owner:
    """Entity owner"""
    username: str
    email: str
    team: Optional[str] = None
    
    @strawberry.field
    def display_name(self) -> str:
        return f"{self.username} ({self.team})" if self.team else self.username


@strawberry.federation.type(keys=["urn"])
class Dataset:
    """Dataset entity"""
    urn: strawberry.ID
    name: str
    platform: DataPlatform
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    owners: List[Owner] = field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    
    @strawberry.field
    async def schema(self, info: Info) -> Optional[DatasetSchema]:
        """Get dataset schema"""
        loader = info.context["schema_loader"]
        return await loader.load(self.urn)
    
    @strawberry.field
    async def quality(self, info: Info) -> Optional[DataQuality]:
        """Get data quality metrics"""
        loader = info.context["quality_loader"]
        return await loader.load(self.urn)
    
    @strawberry.field
    async def lineage(self, info: Info) -> Optional[Lineage]:
        """Get data lineage"""
        loader = info.context["lineage_loader"]
        return await loader.load(self.urn)
    
    @strawberry.field
    def properties(self) -> Dict[str, str]:
        """Get custom properties"""
        # This would fetch from metadata store
        return {}
    
    @classmethod
    async def resolve_reference(cls, urn: strawberry.ID, info: Info) -> Optional["Dataset"]:
        """Resolve dataset by URN for federation"""
        loader = info.context["dataset_loader"]
        return await loader.load(urn)


@strawberry.federation.type(keys=["urn"])
class DataJob:
    """Data job entity"""
    urn: strawberry.ID
    name: str
    type: str
    description: Optional[str] = None
    owners: List[Owner] = field(default_factory=list)
    
    @strawberry.field
    async def inputs(self, info: Info) -> List[Dataset]:
        """Get input datasets"""
        loader = info.context["job_inputs_loader"]
        return await loader.load(self.urn) or []
    
    @strawberry.field
    async def outputs(self, info: Info) -> List[Dataset]:
        """Get output datasets"""
        loader = info.context["job_outputs_loader"]
        return await loader.load(self.urn) or []
    
    @strawberry.field
    async def runs(
        self,
        info: Info,
        limit: int = 10,
        status: Optional[str] = None
    ) -> List["JobRun"]:
        """Get job runs"""
        loader = info.context["job_runs_loader"]
        runs = await loader.load(self.urn) or []
        
        # Filter by status if specified
        if status:
            runs = [r for r in runs if r.status == status]
        
        return runs[:limit]


@strawberry.type
class JobRun:
    """Job run instance"""
    run_id: strawberry.ID
    job_urn: strawberry.ID
    status: str
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    
    @strawberry.field
    def is_success(self) -> bool:
        return self.status == "SUCCESS"
    
    @strawberry.field
    def is_running(self) -> bool:
        return self.status == "RUNNING"


@strawberry.federation.type(keys=["urn"])
class MLModel:
    """ML model entity"""
    urn: strawberry.ID
    name: str
    algorithm: str
    version: str
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    owners: List[Owner] = field(default_factory=list)
    
    @strawberry.field
    async def features(self, info: Info) -> List["Feature"]:
        """Get model features"""
        loader = info.context["model_features_loader"]
        return await loader.load(self.urn) or []
    
    @strawberry.field
    async def metrics(self, info: Info) -> Dict[str, float]:
        """Get model metrics"""
        loader = info.context["model_metrics_loader"]
        return await loader.load(self.urn) or {}
    
    @strawberry.field
    async def deployments(self, info: Info) -> List["ModelDeployment"]:
        """Get model deployments"""
        loader = info.context["model_deployments_loader"]
        return await loader.load(self.urn) or []


@strawberry.type
class Feature:
    """ML feature"""
    name: str
    type: str
    description: Optional[str] = None
    importance: Optional[float] = None
    
    @strawberry.field
    def is_categorical(self) -> bool:
        return self.type in ["string", "category", "boolean"]


@strawberry.type
class ModelDeployment:
    """Model deployment"""
    environment: str
    endpoint: str
    status: str
    deployed_at: datetime
    
    @strawberry.field
    def is_active(self) -> bool:
        return self.status == "ACTIVE"


@strawberry.input
class SearchFilter:
    """Search filter input"""
    platforms: Optional[List[DataPlatform]] = None
    tags: Optional[List[str]] = None
    owners: Optional[List[str]] = None
    entity_types: Optional[List[EntityType]] = None


@strawberry.type
class SearchResult:
    """Search result"""
    entity: Union[Dataset, DataJob, MLModel]
    score: float
    highlights: Dict[str, List[str]] = field(default_factory=dict)
    
    @strawberry.field
    def entity_type(self) -> EntityType:
        if isinstance(self.entity, Dataset):
            return EntityType.DATASET
        elif isinstance(self.entity, DataJob):
            return EntityType.DATAJOB
        elif isinstance(self.entity, MLModel):
            return EntityType.MLMODEL
        else:
            raise ValueError(f"Unknown entity type: {type(self.entity)}")


@strawberry.type
class Query:
    """Root query type"""
    
    @strawberry.field
    async def dataset(self, info: Info, urn: strawberry.ID) -> Optional[Dataset]:
        """Get dataset by URN"""
        loader = info.context["dataset_loader"]
        return await loader.load(urn)
    
    @strawberry.field
    async def search(
        self,
        info: Info,
        query: str,
        filters: Optional[SearchFilter] = None,
        limit: int = 10,
        offset: int = 0
    ) -> List[SearchResult]:
        """Search across all metadata"""
        search_service = info.context["search_service"]
        return await search_service.search(query, filters, limit, offset)
    
    @strawberry.field
    async def datasets(
        self,
        info: Info,
        platform: Optional[DataPlatform] = None,
        limit: int = 100
    ) -> List[Dataset]:
        """List datasets"""
        catalog_service = info.context["catalog_service"]
        return await catalog_service.list_datasets(platform, limit)
    
    @strawberry.field
    async def data_job(self, info: Info, urn: strawberry.ID) -> Optional[DataJob]:
        """Get data job by URN"""
        loader = info.context["job_loader"]
        return await loader.load(urn)
    
    @strawberry.field
    async def ml_model(self, info: Info, urn: strawberry.ID) -> Optional[MLModel]:
        """Get ML model by URN"""
        loader = info.context["model_loader"]
        return await loader.load(urn)
    
    @strawberry.field
    async def lineage_graph(
        self,
        info: Info,
        root_urn: strawberry.ID,
        depth: int = 2,
        direction: str = "BOTH"
    ) -> Dict[str, Any]:
        """Get lineage graph"""
        lineage_service = info.context["lineage_service"]
        return await lineage_service.get_graph(root_urn, depth, direction)


@strawberry.type
class Mutation:
    """Root mutation type"""
    
    @strawberry.mutation
    async def update_dataset_tags(
        self,
        info: Info,
        urn: strawberry.ID,
        tags: List[str]
    ) -> Dataset:
        """Update dataset tags"""
        catalog_service = info.context["catalog_service"]
        return await catalog_service.update_tags(urn, tags)
    
    @strawberry.mutation
    async def update_dataset_owners(
        self,
        info: Info,
        urn: strawberry.ID,
        owner_usernames: List[str]
    ) -> Dataset:
        """Update dataset owners"""
        catalog_service = info.context["catalog_service"]
        return await catalog_service.update_owners(urn, owner_usernames)
    
    @strawberry.mutation
    async def create_data_quality_assertion(
        self,
        info: Info,
        dataset_urn: strawberry.ID,
        assertion_type: str,
        parameters: Dict[str, Any]
    ) -> bool:
        """Create data quality assertion"""
        quality_service = info.context["quality_service"]
        return await quality_service.create_assertion(
            dataset_urn,
            assertion_type,
            parameters
        )


@strawberry.type
class Subscription:
    """Root subscription type"""
    
    @strawberry.subscription
    async def dataset_updates(
        self,
        info: Info,
        dataset_urns: Optional[List[strawberry.ID]] = None
    ) -> Dataset:
        """Subscribe to dataset updates"""
        event_service = info.context["event_service"]
        async for dataset in event_service.dataset_updates(dataset_urns):
            yield dataset
    
    @strawberry.subscription
    async def job_status_updates(
        self,
        info: Info,
        job_urn: strawberry.ID
    ) -> JobRun:
        """Subscribe to job status updates"""
        event_service = info.context["event_service"]
        async for run in event_service.job_status_updates(job_urn):
            yield run


# Data Loaders

class MetadataDataLoaders:
    """Collection of data loaders for batching"""
    
    def __init__(self, metadata_service):
        self.metadata_service = metadata_service
        
        # Dataset loaders
        self.dataset_loader = DataLoader(load_fn=self._batch_load_datasets)
        self.schema_loader = DataLoader(load_fn=self._batch_load_schemas)
        self.quality_loader = DataLoader(load_fn=self._batch_load_quality)
        self.lineage_loader = DataLoader(load_fn=self._batch_load_lineage)
        
        # Job loaders
        self.job_loader = DataLoader(load_fn=self._batch_load_jobs)
        self.job_inputs_loader = DataLoader(load_fn=self._batch_load_job_inputs)
        self.job_outputs_loader = DataLoader(load_fn=self._batch_load_job_outputs)
        self.job_runs_loader = DataLoader(load_fn=self._batch_load_job_runs)
        
        # Model loaders
        self.model_loader = DataLoader(load_fn=self._batch_load_models)
        self.model_features_loader = DataLoader(load_fn=self._batch_load_model_features)
        self.model_metrics_loader = DataLoader(load_fn=self._batch_load_model_metrics)
        self.model_deployments_loader = DataLoader(load_fn=self._batch_load_model_deployments)
    
    async def _batch_load_datasets(self, urns: List[str]) -> List[Optional[Dataset]]:
        """Batch load datasets"""
        return await self.metadata_service.get_datasets_batch(urns)
    
    async def _batch_load_schemas(self, urns: List[str]) -> List[Optional[DatasetSchema]]:
        """Batch load schemas"""
        return await self.metadata_service.get_schemas_batch(urns)
    
    async def _batch_load_quality(self, urns: List[str]) -> List[Optional[DataQuality]]:
        """Batch load quality metrics"""
        return await self.metadata_service.get_quality_batch(urns)
    
    async def _batch_load_lineage(self, urns: List[str]) -> List[Optional[Lineage]]:
        """Batch load lineage"""
        return await self.metadata_service.get_lineage_batch(urns)
    
    async def _batch_load_jobs(self, urns: List[str]) -> List[Optional[DataJob]]:
        """Batch load jobs"""
        return await self.metadata_service.get_jobs_batch(urns)
    
    async def _batch_load_job_inputs(self, urns: List[str]) -> List[List[Dataset]]:
        """Batch load job inputs"""
        return await self.metadata_service.get_job_inputs_batch(urns)
    
    async def _batch_load_job_outputs(self, urns: List[str]) -> List[List[Dataset]]:
        """Batch load job outputs"""
        return await self.metadata_service.get_job_outputs_batch(urns)
    
    async def _batch_load_job_runs(self, urns: List[str]) -> List[List[JobRun]]:
        """Batch load job runs"""
        return await self.metadata_service.get_job_runs_batch(urns)
    
    async def _batch_load_models(self, urns: List[str]) -> List[Optional[MLModel]]:
        """Batch load models"""
        return await self.metadata_service.get_models_batch(urns)
    
    async def _batch_load_model_features(self, urns: List[str]) -> List[List[Feature]]:
        """Batch load model features"""
        return await self.metadata_service.get_model_features_batch(urns)
    
    async def _batch_load_model_metrics(self, urns: List[str]) -> List[Dict[str, float]]:
        """Batch load model metrics"""
        return await self.metadata_service.get_model_metrics_batch(urns)
    
    async def _batch_load_model_deployments(self, urns: List[str]) -> List[List[ModelDeployment]]:
        """Batch load model deployments"""
        return await self.metadata_service.get_model_deployments_batch(urns)


# Extensions

class MetricsExtension(Extension):
    """GraphQL metrics collection"""
    
    def __init__(self):
        self.metrics = {}
    
    async def on_request_start(self):
        self.start_time = datetime.now()
    
    async def on_request_end(self):
        duration = (datetime.now() - self.start_time).total_seconds()
        # Log metrics
        logger.info(f"GraphQL request completed in {duration:.3f}s")
    
    def on_validation_start(self):
        pass
    
    def on_validation_end(self):
        pass
    
    def on_parsing_start(self):
        pass
    
    def on_parsing_end(self):
        pass


class AuthExtension(Extension):
    """Authentication and authorization"""
    
    async def on_request_start(self):
        # Validate auth token
        request = self.execution_context.context.get("request")
        if request:
            auth_header = request.headers.get("Authorization")
            if not auth_header:
                raise Exception("Missing authorization header")
            
            # Validate token
            # This would integrate with your auth service
            
    def on_validation_start(self):
        pass
    
    def on_validation_end(self):
        pass
    
    def on_parsing_start(self):
        pass
    
    def on_parsing_end(self):
        pass


# Federation Schema

def create_federation_schema(
    metadata_service,
    catalog_service,
    search_service,
    lineage_service,
    quality_service,
    event_service
) -> strawberry.Schema:
    """Create federated GraphQL schema"""
    
    # Create data loaders
    loaders = MetadataDataLoaders(metadata_service)
    
    # Create context function
    async def get_context(request=None):
        return {
            "request": request,
            "dataset_loader": loaders.dataset_loader,
            "schema_loader": loaders.schema_loader,
            "quality_loader": loaders.quality_loader,
            "lineage_loader": loaders.lineage_loader,
            "job_loader": loaders.job_loader,
            "job_inputs_loader": loaders.job_inputs_loader,
            "job_outputs_loader": loaders.job_outputs_loader,
            "job_runs_loader": loaders.job_runs_loader,
            "model_loader": loaders.model_loader,
            "model_features_loader": loaders.model_features_loader,
            "model_metrics_loader": loaders.model_metrics_loader,
            "model_deployments_loader": loaders.model_deployments_loader,
            "catalog_service": catalog_service,
            "search_service": search_service,
            "lineage_service": lineage_service,
            "quality_service": quality_service,
            "event_service": event_service
        }
    
    # Create schema with federation
    schema = strawberry.federation.Schema(
        query=Query,
        mutation=Mutation,
        subscription=Subscription,
        federation_version=FederationVersion.VERSION_2_0,
        extensions=[
            MetricsExtension,
            AuthExtension
        ],
        config=StrawberryConfig(auto_camel_case=True)
    )
    
    return schema


# Service Gateway

class MetadataGateway:
    """Gateway for multiple metadata services"""
    
    def __init__(
        self,
        services: Dict[str, str],
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.services = services
        self.vault_client = vault_client
        self.consul_client = consul_client
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
    
    async def query_service(
        self,
        service_name: str,
        query: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Query a specific service"""
        url = self.services.get(service_name)
        if not url:
            raise ValueError(f"Unknown service: {service_name}")
        
        payload = {
            "query": query,
            "variables": variables or {}
        }
        
        async with self._session.post(url, json=payload) as response:
            return await response.json()
    
    async def federated_query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Execute federated query across services"""
        # Parse query to determine which services to query
        # This is a simplified implementation
        
        results = {}
        
        # Query all services in parallel
        tasks = []
        for service_name, url in self.services.items():
            task = self.query_service(service_name, query, variables)
            tasks.append(task)
        
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Merge results
        for service_name, response in zip(self.services.keys(), responses):
            if not isinstance(response, Exception):
                results[service_name] = response
        
        return results


# Apollo Federation Router Integration

class ApolloRouterConfig:
    """Configuration for Apollo Router integration"""
    
    @staticmethod
    def generate_supergraph_config(services: List[Dict[str, str]]) -> Dict[str, Any]:
        """Generate Apollo Router supergraph configuration"""
        return {
            "federation_version": "2",
            "subgraphs": {
                service["name"]: {
                    "routing_url": service["url"],
                    "schema": {
                        "subgraph_url": service["url"]
                    }
                }
                for service in services
            }
        }
    
    @staticmethod
    def generate_router_config() -> Dict[str, Any]:
        """Generate Apollo Router configuration"""
        return {
            "supergraph": {
                "listen": "0.0.0.0:4000",
                "introspection": True
            },
            "cors": {
                "origins": ["*"],
                "methods": ["GET", "POST", "OPTIONS"],
                "headers": ["Content-Type", "Authorization"]
            },
            "telemetry": {
                "tracing": {
                    "trace_config": {
                        "service_name": "metadata-gateway",
                        "sampler": "always_on"
                    }
                }
            },
            "authentication": {
                "jwt": {
                    "jwks_url": "https://auth.platformq.io/.well-known/jwks.json"
                }
            },
            "ratelimit": {
                "all": {
                    "capacity": 10000,
                    "interval": "1m"
                }
            }
        }


# Example Usage

async def example_usage():
    """Example of using the GraphQL federation"""
    
    # Mock services
    class MockMetadataService:
        async def get_datasets_batch(self, urns):
            return [
                Dataset(
                    urn=urn,
                    name=f"dataset_{urn}",
                    platform=DataPlatform.SPARK,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                for urn in urns
            ]
        
        async def get_schemas_batch(self, urns):
            return [
                DatasetSchema(
                    fields=[
                        SchemaField(
                            name="id",
                            type="long",
                            nullable=False,
                            tags=["primary_key"]
                        ),
                        SchemaField(
                            name="value",
                            type="double",
                            nullable=True
                        )
                    ],
                    schema_version=1,
                    created_at=datetime.now()
                )
                for _ in urns
            ]
    
    class MockSearchService:
        async def search(self, query, filters, limit, offset):
            return [
                SearchResult(
                    entity=Dataset(
                        urn="dataset:1",
                        name="sample_dataset",
                        platform=DataPlatform.SPARK,
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    ),
                    score=0.95,
                    highlights={"name": ["sample"]}
                )
            ]
    
    # Create services
    metadata_service = MockMetadataService()
    search_service = MockSearchService()
    
    # Create schema
    schema = create_federation_schema(
        metadata_service=metadata_service,
        catalog_service=None,
        search_service=search_service,
        lineage_service=None,
        quality_service=None,
        event_service=None
    )
    
    # Execute query
    query = """
        query SearchDatasets($query: String!) {
            search(query: $query) {
                entity {
                    ... on Dataset {
                        urn
                        name
                        platform
                        schema {
                            fieldCount
                            fields {
                                name
                                type
                                isPrimaryKey
                            }
                        }
                    }
                }
                score
            }
        }
    """
    
    result = await schema.execute(
        query,
        variable_values={"query": "sample"},
        context_value=await get_context()
    )
    
    print(result.data)


if __name__ == "__main__":
    asyncio.run(example_usage()) 