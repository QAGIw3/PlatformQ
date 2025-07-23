"""
DataHub Client Integration

Provides centralized metadata management platform integration.
"""

from typing import Any, Dict, List, Optional, Union, Tuple, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    SchemaMetadataClass,
    SchemaFieldClass,
    MySqlDDLClass,
    DataPlatformInstanceClass,
    TagAssociationClass,
    GlossaryTermAssociationClass,
    OwnershipClass,
    OwnerClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    DataProcessInstancePropertiesClass,
    DataJobInputOutputClass,
    MLModelPropertiesClass,
    MLModelFactorPromptsClass,
    MLHyperParamClass,
    MLMetricClass,
    CostClass,
    DataQualityMetricClass
)
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig
from ..monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class EntityType(str, Enum):
    """DataHub entity types"""
    DATASET = "dataset"
    DATA_JOB = "dataJob"
    DATA_FLOW = "dataFlow"
    ML_MODEL = "mlModel"
    ML_MODEL_GROUP = "mlModelGroup"
    ML_FEATURE = "mlFeature"
    ML_FEATURE_TABLE = "mlFeatureTable"
    DASHBOARD = "dashboard"
    CHART = "chart"
    GLOSSARY_TERM = "glossaryTerm"
    TAG = "tag"
    CONTAINER = "container"
    DOMAIN = "domain"


class DataPlatform(str, Enum):
    """Supported data platforms"""
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SNOWFLAKE = "snowflake"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"
    S3 = "s3"
    GCS = "gcs"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    SPARK = "spark"
    AIRFLOW = "airflow"
    DBT = "dbt"
    CUSTOM = "custom"


@dataclass
class DataHubConfig(ClientConfig):
    """Configuration for DataHub client"""
    # DataHub server
    gms_url: str = "http://localhost:8080"
    frontend_url: str = "http://localhost:9002"
    
    # Authentication
    token: Optional[str] = None
    
    # Ingestion settings
    enable_auto_ingestion: bool = True
    batch_size: int = 100
    
    # Graph client settings
    enable_graph_client: bool = True
    graph_timeout_seconds: int = 30
    
    # Default platform
    default_platform: DataPlatform = DataPlatform.CUSTOM
    default_env: str = "PROD"
    
    def __post_init__(self):
        super().__post_init__()
        self.service_name = self.service_name or "datahub"


@dataclass
class DatasetMetadata:
    """Dataset metadata"""
    platform: DataPlatform
    name: str
    env: str = "PROD"
    properties: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    glossary_terms: List[str] = field(default_factory=list)
    owners: List[str] = field(default_factory=list)
    schema: Optional[List[Dict[str, Any]]] = None
    upstream_datasets: List[str] = field(default_factory=list)
    custom_properties: Dict[str, str] = field(default_factory=dict)
    
    def get_urn(self) -> str:
        """Get dataset URN"""
        return f"urn:li:dataset:(urn:li:dataPlatform:{self.platform.value},{self.name},{self.env})"


@dataclass
class MLModelMetadata:
    """ML Model metadata"""
    name: str
    platform: str = "mlflow"
    version: Optional[str] = None
    description: Optional[str] = None
    algorithm: Optional[str] = None
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)
    features: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    owners: List[str] = field(default_factory=list)
    
    def get_urn(self) -> str:
        """Get ML model URN"""
        model_name = f"{self.name}_{self.version}" if self.version else self.name
        return f"urn:li:mlModel:(urn:li:dataPlatform:{self.platform},{model_name},PROD)"


@dataclass
class DataQualityMetric:
    """Data quality metric"""
    dataset_urn: str
    metric_name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    dimension: str = "completeness"
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataHubClient(BaseServiceClient):
    """
    DataHub client for centralized metadata management.
    
    Features:
    - Dataset cataloging
    - Schema management
    - Lineage tracking
    - ML model registry
    - Data quality metrics
    - Business glossary
    - Tag management
    - Search and discovery
    """
    
    def __init__(
        self,
        config: Optional[DataHubConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        **kwargs
    ):
        if not config:
            config = DataHubConfig()
            
        super().__init__(config, vault_client, consul_client, **kwargs)
        self.config: DataHubConfig = config
        self._emitter: Optional[DatahubRestEmitter] = None
        self._graph: Optional[DataHubGraph] = None
        
    async def connect(self):
        """Connect to DataHub"""
        await super().connect()
        
        try:
            # Get token from Vault if configured
            if self.config.use_vault_credentials and not self.config.token:
                creds = await self._get_credentials()
                if creds:
                    self.config.token = creds.get("token")
            
            # Create REST emitter
            self._emitter = DatahubRestEmitter(
                gms_server=self.config.gms_url,
                token=self.config.token,
                connect_timeout_sec=self.config.connection_timeout,
                read_timeout_sec=self.config.read_timeout
            )
            
            # Test connection
            self._emitter.test_connection()
            
            # Create graph client if enabled
            if self.config.enable_graph_client:
                graph_config = DatahubClientConfig(
                    server=self.config.gms_url,
                    token=self.config.token
                )
                self._graph = DataHubGraph(graph_config)
            
            logger.info(f"Connected to DataHub: {self.config.gms_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to DataHub: {e}")
            raise
    
    async def ingest_dataset(
        self,
        dataset: DatasetMetadata
    ) -> bool:
        """
        Ingest dataset metadata.
        
        Args:
            dataset: Dataset metadata
            
        Returns:
            Success status
        """
        try:
            mcps = []
            dataset_urn = dataset.get_urn()
            
            # Dataset properties
            if dataset.properties or dataset.custom_properties:
                all_properties = dataset.properties.copy()
                all_properties.update(dataset.custom_properties)
                
                dataset_properties = DatasetPropertiesClass(
                    description=dataset.properties.get("description"),
                    customProperties=all_properties
                )
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="datasetProperties",
                    aspect=dataset_properties
                ))
            
            # Schema metadata
            if dataset.schema:
                schema_fields = []
                for field in dataset.schema:
                    schema_field = SchemaFieldClass(
                        fieldPath=field["name"],
                        type=field.get("type", "string"),
                        nativeDataType=field.get("native_type", field.get("type", "string")),
                        description=field.get("description"),
                        nullable=field.get("nullable", True)
                    )
                    schema_fields.append(schema_field)
                
                schema_metadata = SchemaMetadataClass(
                    schemaName=dataset.name,
                    platform=f"urn:li:dataPlatform:{dataset.platform.value}",
                    version=0,
                    hash="",
                    platformSchema=MySqlDDLClass(tableSchema=""),
                    fields=schema_fields
                )
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="schemaMetadata",
                    aspect=schema_metadata
                ))
            
            # Tags
            if dataset.tags:
                tag_associations = []
                for tag in dataset.tags:
                    tag_urn = f"urn:li:tag:{tag}"
                    tag_associations.append(TagAssociationClass(tag=tag_urn))
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="globalTags",
                    aspect={"tags": tag_associations}
                ))
            
            # Glossary terms
            if dataset.glossary_terms:
                term_associations = []
                for term in dataset.glossary_terms:
                    term_urn = f"urn:li:glossaryTerm:{term}"
                    term_associations.append(GlossaryTermAssociationClass(urn=term_urn))
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="glossaryTerms",
                    aspect={"terms": term_associations}
                ))
            
            # Ownership
            if dataset.owners:
                owners = []
                for owner in dataset.owners:
                    owner_urn = f"urn:li:corpuser:{owner}"
                    owners.append(OwnerClass(
                        owner=owner_urn,
                        type="DATAOWNER"
                    ))
                
                ownership = OwnershipClass(owners=owners)
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="ownership",
                    aspect=ownership
                ))
            
            # Lineage
            if dataset.upstream_datasets:
                upstreams = []
                for upstream in dataset.upstream_datasets:
                    upstreams.append(UpstreamClass(
                        dataset=upstream,
                        type=DatasetLineageTypeClass.TRANSFORMED
                    ))
                
                upstream_lineage = UpstreamLineageClass(upstreams=upstreams)
                
                mcps.append(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=dataset_urn,
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage
                ))
            
            # Emit all metadata
            for mcp in mcps:
                self._emitter.emit_mcp(mcp)
            
            logger.info(f"Ingested dataset: {dataset_urn}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest dataset: {e}")
            return False
    
    async def ingest_ml_model(
        self,
        model: MLModelMetadata
    ) -> bool:
        """
        Ingest ML model metadata.
        
        Args:
            model: ML model metadata
            
        Returns:
            Success status
        """
        try:
            model_urn = model.get_urn()
            
            # Model properties
            model_properties = MLModelPropertiesClass(
                description=model.description,
                version=model.version,
                type=model.algorithm,
                customProperties={
                    "platform": model.platform,
                    "algorithm": model.algorithm or "unknown"
                }
            )
            
            self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                entityType="mlModel",
                changeType="UPSERT",
                entityUrn=model_urn,
                aspectName="mlModelProperties",
                aspect=model_properties
            ))
            
            # Hyperparameters
            if model.hyperparameters:
                hyperparams = []
                for name, value in model.hyperparameters.items():
                    hyperparams.append(MLHyperParamClass(
                        name=name,
                        value=str(value)
                    ))
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="mlModel",
                    changeType="UPSERT",
                    entityUrn=model_urn,
                    aspectName="mlModelFactorPrompts",
                    aspect=MLModelFactorPromptsClass(hyperParams=hyperparams)
                ))
            
            # Metrics
            if model.metrics:
                metrics = []
                for name, value in model.metrics.items():
                    metrics.append(MLMetricClass(
                        name=name,
                        value=str(value)
                    ))
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="mlModel",
                    changeType="UPSERT",
                    entityUrn=model_urn,
                    aspectName="mlModelMetrics",
                    aspect={"performanceMetrics": metrics}
                ))
            
            # Features (as upstream datasets)
            if model.features:
                upstreams = []
                for feature in model.features:
                    # Assume features are datasets
                    feature_urn = f"urn:li:dataset:(urn:li:dataPlatform:mlfeature,{feature},PROD)"
                    upstreams.append(UpstreamClass(
                        dataset=feature_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED
                    ))
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="mlModel",
                    changeType="UPSERT",
                    entityUrn=model_urn,
                    aspectName="upstreamLineage",
                    aspect=UpstreamLineageClass(upstreams=upstreams)
                ))
            
            # Tags
            if model.tags:
                tag_associations = []
                for tag in model.tags:
                    tag_urn = f"urn:li:tag:{tag}"
                    tag_associations.append(TagAssociationClass(tag=tag_urn))
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="mlModel",
                    changeType="UPSERT",
                    entityUrn=model_urn,
                    aspectName="globalTags",
                    aspect={"tags": tag_associations}
                ))
            
            # Ownership
            if model.owners:
                owners = []
                for owner in model.owners:
                    owner_urn = f"urn:li:corpuser:{owner}"
                    owners.append(OwnerClass(
                        owner=owner_urn,
                        type="DATAOWNER"
                    ))
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="mlModel",
                    changeType="UPSERT",
                    entityUrn=model_urn,
                    aspectName="ownership",
                    aspect=OwnershipClass(owners=owners)
                ))
            
            logger.info(f"Ingested ML model: {model_urn}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest ML model: {e}")
            return False
    
    async def ingest_data_quality_metrics(
        self,
        metrics: List[DataQualityMetric]
    ) -> bool:
        """
        Ingest data quality metrics.
        
        Args:
            metrics: List of data quality metrics
            
        Returns:
            Success status
        """
        try:
            for metric in metrics:
                quality_metric = DataQualityMetricClass(
                    timestampMillis=int(metric.timestamp.timestamp() * 1000),
                    name=metric.metric_name,
                    value=metric.value,
                    dimension=metric.dimension,
                    metadata=json.dumps(metric.metadata) if metric.metadata else None
                )
                
                self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType="UPSERT",
                    entityUrn=metric.dataset_urn,
                    aspectName="dataQualityMetrics",
                    aspect={"metrics": [quality_metric]}
                ))
            
            logger.info(f"Ingested {len(metrics)} data quality metrics")
            return True
            
        except Exception as e:
            logger.error(f"Failed to ingest data quality metrics: {e}")
            return False
    
    async def search_datasets(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Search for datasets.
        
        Args:
            query: Search query
            filters: Optional filters
            limit: Maximum results
            
        Returns:
            List of search results
        """
        try:
            if not self._graph:
                logger.error("Graph client not enabled")
                return []
            
            # Build filter
            filter_dict = {}
            if filters:
                if "platform" in filters:
                    filter_dict["platform"] = filters["platform"]
                if "tags" in filters:
                    filter_dict["tags"] = filters["tags"]
                if "owners" in filters:
                    filter_dict["owners"] = filters["owners"]
            
            # Search
            results = self._graph.search(
                query=query,
                entity_types=["dataset"],
                filters=filter_dict,
                count=limit
            )
            
            # Process results
            datasets = []
            for result in results:
                dataset = {
                    "urn": result.entity,
                    "name": result.matched_fields.get("name", ""),
                    "platform": result.matched_fields.get("platform", ""),
                    "description": result.matched_fields.get("description", ""),
                    "score": result.score
                }
                datasets.append(dataset)
            
            return datasets
            
        except Exception as e:
            logger.error(f"Failed to search datasets: {e}")
            return []
    
    async def get_dataset_lineage(
        self,
        dataset_urn: str,
        direction: str = "BOTH",
        depth: int = 1
    ) -> Dict[str, Any]:
        """
        Get dataset lineage.
        
        Args:
            dataset_urn: Dataset URN
            direction: UPSTREAM, DOWNSTREAM, or BOTH
            depth: Lineage depth
            
        Returns:
            Lineage graph
        """
        try:
            if not self._graph:
                logger.error("Graph client not enabled")
                return {}
            
            # Get lineage
            lineage = self._graph.get_lineage(
                entity_urn=dataset_urn,
                direction=direction,
                depth=depth
            )
            
            # Process lineage
            nodes = []
            edges = []
            
            # Add root node
            nodes.append({
                "urn": dataset_urn,
                "type": "dataset",
                "level": 0
            })
            
            # Process upstream
            if lineage.upstream_entities:
                for level, entities in enumerate(lineage.upstream_entities, 1):
                    for entity in entities:
                        nodes.append({
                            "urn": entity.urn,
                            "type": entity.type,
                            "level": -level
                        })
                        edges.append({
                            "source": entity.urn,
                            "target": dataset_urn,
                            "type": "upstream"
                        })
            
            # Process downstream
            if lineage.downstream_entities:
                for level, entities in enumerate(lineage.downstream_entities, 1):
                    for entity in entities:
                        nodes.append({
                            "urn": entity.urn,
                            "type": entity.type,
                            "level": level
                        })
                        edges.append({
                            "source": dataset_urn,
                            "target": entity.urn,
                            "type": "downstream"
                        })
            
            return {
                "root": dataset_urn,
                "nodes": nodes,
                "edges": edges,
                "depth": depth
            }
            
        except Exception as e:
            logger.error(f"Failed to get dataset lineage: {e}")
            return {}
    
    async def create_tag(
        self,
        name: str,
        description: Optional[str] = None
    ) -> bool:
        """
        Create a tag.
        
        Args:
            name: Tag name
            description: Tag description
            
        Returns:
            Success status
        """
        try:
            tag_urn = f"urn:li:tag:{name}"
            
            tag_properties = {
                "name": name,
                "description": description or f"Tag: {name}"
            }
            
            self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                entityType="tag",
                changeType="UPSERT",
                entityUrn=tag_urn,
                aspectName="tagProperties",
                aspect=tag_properties
            ))
            
            logger.info(f"Created tag: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tag: {e}")
            return False
    
    async def create_glossary_term(
        self,
        name: str,
        definition: str,
        parent_term: Optional[str] = None
    ) -> bool:
        """
        Create a glossary term.
        
        Args:
            name: Term name
            definition: Term definition
            parent_term: Parent term name
            
        Returns:
            Success status
        """
        try:
            term_urn = f"urn:li:glossaryTerm:{name}"
            
            term_properties = {
                "name": name,
                "definition": definition
            }
            
            if parent_term:
                term_properties["parentNode"] = f"urn:li:glossaryTerm:{parent_term}"
            
            self._emitter.emit_mcp(MetadataChangeProposalWrapper(
                entityType="glossaryTerm",
                changeType="UPSERT",
                entityUrn=term_urn,
                aspectName="glossaryTermInfo",
                aspect=term_properties
            ))
            
            logger.info(f"Created glossary term: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create glossary term: {e}")
            return False
    
    async def get_dataset_schema(
        self,
        dataset_urn: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get dataset schema.
        
        Args:
            dataset_urn: Dataset URN
            
        Returns:
            Schema fields or None
        """
        try:
            if not self._graph:
                logger.error("Graph client not enabled")
                return None
            
            # Get schema metadata
            schema = self._graph.get_aspect(
                entity_urn=dataset_urn,
                aspect_type=SchemaMetadataClass
            )
            
            if not schema:
                return None
            
            # Convert to list of dicts
            fields = []
            for field in schema.fields:
                fields.append({
                    "name": field.fieldPath,
                    "type": field.type,
                    "native_type": field.nativeDataType,
                    "description": field.description,
                    "nullable": field.nullable
                })
            
            return fields
            
        except Exception as e:
            logger.error(f"Failed to get dataset schema: {e}")
            return None
    
    async def close(self):
        """Close DataHub connections"""
        if self._emitter:
            self._emitter.close()
        
        await super().close()
    
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get DataHub specific configuration"""
        return {
            "gms_url": self.config.gms_url,
            "frontend_url": self.config.frontend_url,
            "default_platform": self.config.default_platform.value,
            "default_env": self.config.default_env,
            "enable_graph_client": self.config.enable_graph_client
        } 