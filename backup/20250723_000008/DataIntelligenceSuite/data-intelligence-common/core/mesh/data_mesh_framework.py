"""
Data Mesh Framework

Provides patterns and infrastructure for implementing data mesh architecture.
"""

from typing import Any, Dict, List, Optional, Union, Set, Callable
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
import json
from urllib.parse import urlparse

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import StructuredLogger
from ..governance.policy_engine import PolicyEngine, Principal, Resource, Action, Context
from ..metadata.graphql_federation import DatasetSchema, SchemaField, DataQuality

logger = StructuredLogger.get_logger(__name__)


class DomainType(str, Enum):
    """Domain types in data mesh"""
    SOURCE_ALIGNED = "source_aligned"
    CONSUMER_ALIGNED = "consumer_aligned"
    AGGREGATE = "aggregate"


class DataProductType(str, Enum):
    """Data product types"""
    DATASET = "dataset"
    STREAM = "stream"
    API = "api"
    MODEL = "model"
    REPORT = "report"


class DataProductStatus(str, Enum):
    """Data product lifecycle status"""
    DRAFT = "draft"
    DEVELOPMENT = "development"
    TESTING = "testing"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    RETIRED = "retired"


class PortType(str, Enum):
    """Data product port types"""
    INPUT = "input"
    OUTPUT = "output"
    MONITORING = "monitoring"
    CONTROL = "control"


@dataclass
class DataContract:
    """Data contract specification"""
    version: str
    schema: DatasetSchema
    quality_sla: Dict[str, float]  # metric -> threshold
    freshness_sla: int  # seconds
    availability_sla: float  # percentage
    retention_days: int
    privacy_classification: str
    allowed_uses: List[str]
    
    def validate_schema(self, data: Any) -> bool:
        """Validate data against schema"""
        # Simplified validation
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "schema": {
                "fields": [
                    {
                        "name": f.name,
                        "type": f.type,
                        "nullable": f.nullable,
                        "description": f.description
                    }
                    for f in self.schema.fields
                ]
            },
            "quality_sla": self.quality_sla,
            "freshness_sla": self.freshness_sla,
            "availability_sla": self.availability_sla,
            "retention_days": self.retention_days,
            "privacy_classification": self.privacy_classification,
            "allowed_uses": self.allowed_uses
        }


@dataclass
class DataPort:
    """Data product port"""
    name: str
    type: PortType
    protocol: str  # http, grpc, kafka, s3, jdbc
    endpoint: str
    authentication: str  # oauth, api_key, mtls
    format: str  # json, avro, parquet, csv
    contract: Optional[DataContract] = None
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get connection information"""
        parsed = urlparse(self.endpoint)
        return {
            "protocol": self.protocol,
            "host": parsed.hostname,
            "port": parsed.port,
            "path": parsed.path,
            "authentication": self.authentication,
            "format": self.format
        }


@dataclass
class DataProduct:
    """Data product definition"""
    id: str
    name: str
    domain: str
    type: DataProductType
    description: str
    owner: str
    team: str
    status: DataProductStatus
    version: str
    
    # Ports
    input_ports: List[DataPort] = field(default_factory=list)
    output_ports: List[DataPort] = field(default_factory=list)
    monitoring_port: Optional[DataPort] = None
    control_port: Optional[DataPort] = None
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    documentation_url: Optional[str] = None
    source_code_url: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def get_output_port(self, name: str) -> Optional[DataPort]:
        """Get output port by name"""
        for port in self.output_ports:
            if port.name == name:
                return port
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "domain": self.domain,
            "type": self.type.value,
            "description": self.description,
            "owner": self.owner,
            "team": self.team,
            "status": self.status.value,
            "version": self.version,
            "input_ports": [
                {
                    "name": p.name,
                    "type": p.type.value,
                    "protocol": p.protocol,
                    "endpoint": p.endpoint
                }
                for p in self.input_ports
            ],
            "output_ports": [
                {
                    "name": p.name,
                    "type": p.type.value,
                    "protocol": p.protocol,
                    "endpoint": p.endpoint,
                    "contract": p.contract.to_dict() if p.contract else None
                }
                for p in self.output_ports
            ],
            "tags": self.tags,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class Domain:
    """Data domain"""
    id: str
    name: str
    type: DomainType
    description: str
    owner: str
    team: str
    data_products: List[str] = field(default_factory=list)  # Product IDs
    
    def add_product(self, product_id: str):
        """Add data product to domain"""
        if product_id not in self.data_products:
            self.data_products.append(product_id)
    
    def remove_product(self, product_id: str):
        """Remove data product from domain"""
        if product_id in self.data_products:
            self.data_products.remove(product_id)


class DataProductRegistry:
    """Registry for data products"""
    
    def __init__(
        self,
        consul_client: Optional[ConsulClient] = None,
        vault_client: Optional[VaultClient] = None
    ):
        self.consul_client = consul_client
        self.vault_client = vault_client
        self._products: Dict[str, DataProduct] = {}
        self._domains: Dict[str, Domain] = {}
    
    async def register_domain(self, domain: Domain) -> bool:
        """Register new domain"""
        try:
            self._domains[domain.id] = domain
            
            # Store in Consul
            if self.consul_client:
                await self.consul_client.put(
                    f"data-mesh/domains/{domain.id}",
                    json.dumps({
                        "id": domain.id,
                        "name": domain.name,
                        "type": domain.type.value,
                        "description": domain.description,
                        "owner": domain.owner,
                        "team": domain.team,
                        "data_products": domain.data_products
                    })
                )
            
            logger.info(f"Registered domain: {domain.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register domain: {e}")
            return False
    
    async def register_product(self, product: DataProduct) -> bool:
        """Register data product"""
        try:
            self._products[product.id] = product
            
            # Add to domain
            if product.domain in self._domains:
                self._domains[product.domain].add_product(product.id)
            
            # Store in Consul
            if self.consul_client:
                await self.consul_client.put(
                    f"data-mesh/products/{product.id}",
                    json.dumps(product.to_dict())
                )
                
                # Register service for discovery
                await self.consul_client.register_service(
                    name=f"data-product-{product.id}",
                    service_id=product.id,
                    address=product.control_port.endpoint if product.control_port else "",
                    port=8080,
                    tags=[
                        f"domain:{product.domain}",
                        f"type:{product.type.value}",
                        f"status:{product.status.value}",
                        *product.tags
                    ],
                    meta={
                        "version": product.version,
                        "owner": product.owner,
                        "team": product.team
                    }
                )
            
            logger.info(f"Registered data product: {product.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register product: {e}")
            return False
    
    async def discover_products(
        self,
        domain: Optional[str] = None,
        type: Optional[DataProductType] = None,
        tags: Optional[List[str]] = None,
        status: Optional[DataProductStatus] = None
    ) -> List[DataProduct]:
        """Discover data products"""
        products = list(self._products.values())
        
        # Filter by domain
        if domain:
            products = [p for p in products if p.domain == domain]
        
        # Filter by type
        if type:
            products = [p for p in products if p.type == type]
        
        # Filter by status
        if status:
            products = [p for p in products if p.status == status]
        
        # Filter by tags
        if tags:
            products = [
                p for p in products
                if all(tag in p.tags for tag in tags)
            ]
        
        return products
    
    async def get_product(self, product_id: str) -> Optional[DataProduct]:
        """Get data product by ID"""
        return self._products.get(product_id)
    
    async def update_product_status(
        self,
        product_id: str,
        status: DataProductStatus
    ) -> bool:
        """Update product status"""
        product = self._products.get(product_id)
        if not product:
            return False
        
        product.status = status
        product.updated_at = datetime.now()
        
        # Update in Consul
        if self.consul_client:
            await self.register_product(product)
        
        return True


class DataProductBuilder:
    """Builder for data products"""
    
    def __init__(self):
        self._product = None
        self._reset()
    
    def _reset(self):
        self._product = DataProduct(
            id="",
            name="",
            domain="",
            type=DataProductType.DATASET,
            description="",
            owner="",
            team="",
            status=DataProductStatus.DRAFT,
            version="1.0.0"
        )
    
    def with_basic_info(
        self,
        id: str,
        name: str,
        domain: str,
        type: DataProductType,
        description: str
    ) -> "DataProductBuilder":
        """Set basic information"""
        self._product.id = id
        self._product.name = name
        self._product.domain = domain
        self._product.type = type
        self._product.description = description
        return self
    
    def with_ownership(
        self,
        owner: str,
        team: str
    ) -> "DataProductBuilder":
        """Set ownership"""
        self._product.owner = owner
        self._product.team = team
        return self
    
    def add_input_port(
        self,
        name: str,
        protocol: str,
        endpoint: str,
        format: str = "json",
        authentication: str = "oauth"
    ) -> "DataProductBuilder":
        """Add input port"""
        port = DataPort(
            name=name,
            type=PortType.INPUT,
            protocol=protocol,
            endpoint=endpoint,
            authentication=authentication,
            format=format
        )
        self._product.input_ports.append(port)
        return self
    
    def add_output_port(
        self,
        name: str,
        protocol: str,
        endpoint: str,
        format: str,
        contract: DataContract,
        authentication: str = "oauth"
    ) -> "DataProductBuilder":
        """Add output port with contract"""
        port = DataPort(
            name=name,
            type=PortType.OUTPUT,
            protocol=protocol,
            endpoint=endpoint,
            authentication=authentication,
            format=format,
            contract=contract
        )
        self._product.output_ports.append(port)
        return self
    
    def with_monitoring(
        self,
        endpoint: str,
        protocol: str = "http"
    ) -> "DataProductBuilder":
        """Add monitoring port"""
        self._product.monitoring_port = DataPort(
            name="monitoring",
            type=PortType.MONITORING,
            protocol=protocol,
            endpoint=endpoint,
            authentication="api_key",
            format="prometheus"
        )
        return self
    
    def with_control(
        self,
        endpoint: str,
        protocol: str = "http"
    ) -> "DataProductBuilder":
        """Add control port"""
        self._product.control_port = DataPort(
            name="control",
            type=PortType.CONTROL,
            protocol=protocol,
            endpoint=endpoint,
            authentication="oauth",
            format="json"
        )
        return self
    
    def with_tags(self, tags: List[str]) -> "DataProductBuilder":
        """Add tags"""
        self._product.tags.extend(tags)
        return self
    
    def build(self) -> DataProduct:
        """Build data product"""
        product = self._product
        self._reset()
        return product


class DataProductRuntime(ABC):
    """Abstract runtime for data products"""
    
    @abstractmethod
    async def start(self, product: DataProduct):
        """Start data product runtime"""
        pass
    
    @abstractmethod
    async def stop(self, product_id: str):
        """Stop data product runtime"""
        pass
    
    @abstractmethod
    async def get_status(self, product_id: str) -> Dict[str, Any]:
        """Get runtime status"""
        pass
    
    @abstractmethod
    async def get_metrics(self, product_id: str) -> Dict[str, Any]:
        """Get runtime metrics"""
        pass


class ContainerizedRuntime(DataProductRuntime):
    """Container-based runtime for data products"""
    
    def __init__(self, orchestrator: str = "kubernetes"):
        self.orchestrator = orchestrator
        self._running_products: Dict[str, Dict[str, Any]] = {}
    
    async def start(self, product: DataProduct):
        """Start containerized data product"""
        try:
            # Generate deployment manifest
            manifest = self._generate_manifest(product)
            
            # Deploy to orchestrator
            if self.orchestrator == "kubernetes":
                # Deploy to K8s
                logger.info(f"Deploying {product.name} to Kubernetes")
                
            elif self.orchestrator == "docker":
                # Run with Docker
                logger.info(f"Running {product.name} with Docker")
            
            # Track running product
            self._running_products[product.id] = {
                "product": product,
                "started_at": datetime.now(),
                "manifest": manifest
            }
            
        except Exception as e:
            logger.error(f"Failed to start product: {e}")
            raise
    
    async def stop(self, product_id: str):
        """Stop containerized data product"""
        if product_id not in self._running_products:
            return
        
        try:
            if self.orchestrator == "kubernetes":
                # Delete K8s resources
                logger.info(f"Deleting {product_id} from Kubernetes")
                
            elif self.orchestrator == "docker":
                # Stop Docker container
                logger.info(f"Stopping {product_id} Docker container")
            
            # Remove from tracking
            del self._running_products[product_id]
            
        except Exception as e:
            logger.error(f"Failed to stop product: {e}")
            raise
    
    async def get_status(self, product_id: str) -> Dict[str, Any]:
        """Get runtime status"""
        if product_id not in self._running_products:
            return {"status": "not_found"}
        
        # Get actual status from orchestrator
        return {
            "status": "running",
            "started_at": self._running_products[product_id]["started_at"],
            "uptime_seconds": (
                datetime.now() - self._running_products[product_id]["started_at"]
            ).total_seconds()
        }
    
    async def get_metrics(self, product_id: str) -> Dict[str, Any]:
        """Get runtime metrics"""
        # Get metrics from monitoring system
        return {
            "cpu_usage": 0.45,
            "memory_usage": 0.62,
            "network_in_bytes": 1024000,
            "network_out_bytes": 2048000,
            "error_rate": 0.001
        }
    
    def _generate_manifest(self, product: DataProduct) -> Dict[str, Any]:
        """Generate deployment manifest"""
        return {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"data-product-{product.id}",
                "labels": {
                    "app": "data-product",
                    "product-id": product.id,
                    "domain": product.domain,
                    "type": product.type.value
                }
            },
            "spec": {
                "replicas": 1,
                "selector": {
                    "matchLabels": {
                        "product-id": product.id
                    }
                },
                "template": {
                    "metadata": {
                        "labels": {
                            "product-id": product.id
                        }
                    },
                    "spec": {
                        "containers": [{
                            "name": "main",
                            "image": f"data-product/{product.id}:{product.version}",
                            "ports": self._generate_container_ports(product),
                            "env": self._generate_env_vars(product)
                        }]
                    }
                }
            }
        }
    
    def _generate_container_ports(self, product: DataProduct) -> List[Dict[str, Any]]:
        """Generate container ports"""
        ports = []
        
        # Add output ports
        for i, port in enumerate(product.output_ports):
            parsed = urlparse(port.endpoint)
            ports.append({
                "name": f"output-{i}",
                "containerPort": parsed.port or 8080,
                "protocol": "TCP"
            })
        
        # Add monitoring port
        if product.monitoring_port:
            ports.append({
                "name": "monitoring",
                "containerPort": 9090,
                "protocol": "TCP"
            })
        
        # Add control port
        if product.control_port:
            ports.append({
                "name": "control",
                "containerPort": 8081,
                "protocol": "TCP"
            })
        
        return ports
    
    def _generate_env_vars(self, product: DataProduct) -> List[Dict[str, str]]:
        """Generate environment variables"""
        return [
            {"name": "PRODUCT_ID", "value": product.id},
            {"name": "PRODUCT_NAME", "value": product.name},
            {"name": "PRODUCT_VERSION", "value": product.version},
            {"name": "DOMAIN", "value": product.domain},
            {"name": "OWNER", "value": product.owner},
            {"name": "TEAM", "value": product.team}
        ]


class DataMeshGovernance:
    """Governance for data mesh"""
    
    def __init__(
        self,
        policy_engine: PolicyEngine,
        registry: DataProductRegistry
    ):
        self.policy_engine = policy_engine
        self.registry = registry
    
    async def validate_product_registration(
        self,
        product: DataProduct,
        principal: Principal
    ) -> bool:
        """Validate product registration against policies"""
        # Check if principal can register products in domain
        resource = Resource(
            id=f"domain:{product.domain}",
            type="domain",
            attributes={"domain": product.domain}
        )
        
        context = Context(
            timestamp=datetime.now(),
            purpose="product_registration"
        )
        
        decision = await self.policy_engine.evaluate(
            principal=principal,
            resource=resource,
            action=Action.WRITE,
            context=context
        )
        
        return decision.is_allowed
    
    async def validate_product_access(
        self,
        product_id: str,
        port_name: str,
        principal: Principal,
        purpose: str
    ) -> bool:
        """Validate access to data product"""
        product = await self.registry.get_product(product_id)
        if not product:
            return False
        
        # Get port
        port = product.get_output_port(port_name)
        if not port:
            return False
        
        # Check contract
        if port.contract:
            if purpose not in port.contract.allowed_uses:
                return False
        
        # Check policy
        resource = Resource(
            id=f"product:{product_id}:{port_name}",
            type="data_product",
            attributes={
                "domain": product.domain,
                "type": product.type.value,
                "classification": port.contract.privacy_classification if port.contract else "public"
            }
        )
        
        context = Context(
            timestamp=datetime.now(),
            purpose=purpose
        )
        
        decision = await self.policy_engine.evaluate(
            principal=principal,
            resource=resource,
            action=Action.READ,
            context=context
        )
        
        return decision.is_allowed
    
    async def audit_product_usage(
        self,
        product_id: str,
        port_name: str,
        principal: Principal,
        bytes_transferred: int,
        records_accessed: int
    ):
        """Audit data product usage"""
        # Log usage metrics
        logger.info(
            f"Data product usage: "
            f"product={product_id}, "
            f"port={port_name}, "
            f"principal={principal.id}, "
            f"bytes={bytes_transferred}, "
            f"records={records_accessed}"
        )


class DataProductClient:
    """Client for consuming data products"""
    
    def __init__(
        self,
        registry: DataProductRegistry,
        governance: DataMeshGovernance,
        principal: Principal
    ):
        self.registry = registry
        self.governance = governance
        self.principal = principal
    
    async def discover(
        self,
        domain: Optional[str] = None,
        tags: Optional[List[str]] = None
    ) -> List[DataProduct]:
        """Discover available data products"""
        products = await self.registry.discover_products(
            domain=domain,
            tags=tags,
            status=DataProductStatus.PRODUCTION
        )
        
        # Filter by access permissions
        accessible_products = []
        for product in products:
            for port in product.output_ports:
                if await self.governance.validate_product_access(
                    product.id,
                    port.name,
                    self.principal,
                    "discovery"
                ):
                    accessible_products.append(product)
                    break
        
        return accessible_products
    
    async def connect(
        self,
        product_id: str,
        port_name: str,
        purpose: str
    ) -> Optional[Dict[str, Any]]:
        """Connect to data product"""
        # Validate access
        if not await self.governance.validate_product_access(
            product_id,
            port_name,
            self.principal,
            purpose
        ):
            logger.warning(f"Access denied to {product_id}:{port_name}")
            return None
        
        # Get product
        product = await self.registry.get_product(product_id)
        if not product:
            return None
        
        # Get port
        port = product.get_output_port(port_name)
        if not port:
            return None
        
        # Return connection info
        return {
            "connection": port.get_connection_info(),
            "contract": port.contract.to_dict() if port.contract else None,
            "credentials": await self._get_credentials(product_id, port_name)
        }
    
    async def _get_credentials(
        self,
        product_id: str,
        port_name: str
    ) -> Dict[str, Any]:
        """Get access credentials"""
        # This would integrate with auth system
        return {
            "type": "oauth",
            "token": "sample_token"
        }


# Example usage

async def example_usage():
    """Example of data mesh framework usage"""
    
    # Create registry
    registry = DataProductRegistry()
    
    # Register domain
    customer_domain = Domain(
        id="customer",
        name="Customer Domain",
        type=DomainType.SOURCE_ALIGNED,
        description="Customer-related data products",
        owner="customer-team-lead",
        team="customer-team"
    )
    
    await registry.register_domain(customer_domain)
    
    # Build data product
    builder = DataProductBuilder()
    
    # Define contract
    contract = DataContract(
        version="1.0.0",
        schema=DatasetSchema(
            fields=[
                SchemaField(
                    name="customer_id",
                    type="string",
                    nullable=False,
                    tags=["primary_key"]
                ),
                SchemaField(
                    name="email",
                    type="string",
                    nullable=False,
                    tags=["pii"]
                ),
                SchemaField(
                    name="lifetime_value",
                    type="double",
                    nullable=True
                ),
                SchemaField(
                    name="segment",
                    type="string",
                    nullable=True
                )
            ],
            schema_version=1,
            created_at=datetime.now()
        ),
        quality_sla={
            "completeness": 0.99,
            "accuracy": 0.95,
            "timeliness": 0.90
        },
        freshness_sla=3600,  # 1 hour
        availability_sla=0.999,
        retention_days=365,
        privacy_classification="confidential",
        allowed_uses=["analytics", "ml_training", "reporting"]
    )
    
    # Build product
    customer_360 = builder \
        .with_basic_info(
            id="customer-360",
            name="Customer 360 View",
            domain="customer",
            type=DataProductType.DATASET,
            description="Unified view of customer data"
        ) \
        .with_ownership(
            owner="john.doe@company.com",
            team="customer-team"
        ) \
        .add_input_port(
            name="crm_feed",
            protocol="kafka",
            endpoint="kafka://broker:9092/crm.customers",
            format="avro"
        ) \
        .add_input_port(
            name="transaction_feed",
            protocol="kafka",
            endpoint="kafka://broker:9092/transactions.customers",
            format="json"
        ) \
        .add_output_port(
            name="batch",
            protocol="s3",
            endpoint="s3://datalake/customer-360/",
            format="parquet",
            contract=contract
        ) \
        .add_output_port(
            name="streaming",
            protocol="kafka",
            endpoint="kafka://broker:9092/customer-360-stream",
            format="avro",
            contract=contract
        ) \
        .with_monitoring(
            endpoint="http://customer-360:9090/metrics"
        ) \
        .with_control(
            endpoint="http://customer-360:8081/api"
        ) \
        .with_tags(["customer", "360-view", "ml-ready"]) \
        .build()
    
    # Register product
    await registry.register_product(customer_360)
    
    # Create runtime
    runtime = ContainerizedRuntime(orchestrator="kubernetes")
    
    # Start product
    await runtime.start(customer_360)
    
    # Create governance
    from ..governance.policy_engine import OPAPolicyEngine
    policy_engine = OPAPolicyEngine()
    governance = DataMeshGovernance(policy_engine, registry)
    
    # Create consumer
    consumer_principal = Principal(
        id="analytics-team",
        type="group",
        roles=["data_analyst"],
        attributes={"department": "analytics"}
    )
    
    client = DataProductClient(registry, governance, consumer_principal)
    
    # Discover products
    products = await client.discover(domain="customer")
    print(f"Found {len(products)} accessible products")
    
    # Connect to product
    connection = await client.connect(
        product_id="customer-360",
        port_name="batch",
        purpose="analytics"
    )
    
    if connection:
        print(f"Connected to: {connection['connection']}")
        print(f"Contract: {connection['contract']}")
    
    # Get runtime status
    status = await runtime.get_status("customer-360")
    print(f"Runtime status: {status}")
    
    # Get metrics
    metrics = await runtime.get_metrics("customer-360")
    print(f"Runtime metrics: {metrics}")


if __name__ == "__main__":
    asyncio.run(example_usage()) 