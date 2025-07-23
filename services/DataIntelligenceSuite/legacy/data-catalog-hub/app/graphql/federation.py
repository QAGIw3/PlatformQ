"""
GraphQL Federation Setup for Data Catalog Hub

Configures the federated GraphQL service to participate in the platform-wide schema.
"""

from typing import Dict, Any
import logging

from data_intelligence_common.integrations.graphql_federation import (
    GraphQLFederationService,
    FederationConfig,
    FederatedType,
    FederatedField
)

from .schema import schema

logger = logging.getLogger(__name__)


class DataCatalogFederationService:
    """Manages GraphQL federation for the Data Catalog Hub"""
    
    def __init__(self, container):
        self.container = container
        self.federation_service = None
        
    async def initialize(self):
        """Initialize the federation service"""
        # Get configuration from container
        config = self.container.config()
        
        federation_config = FederationConfig(
            service_name="data-catalog-hub",
            service_url=f"http://{config.HOST}:{config.PORT}/graphql",
            schema_sdl=schema.as_str(),
            health_check_path="/health",
            vault_client=self.container.vault_client() if hasattr(self.container, 'vault_client') else None,
            consul_client=self.container.consul_client() if hasattr(self.container, 'consul_client') else None
        )
        
        self.federation_service = GraphQLFederationService(federation_config)
        
        # Register federated types
        await self._register_federated_types()
        
        # Start the service
        await self.federation_service.start()
        
        logger.info("GraphQL federation service initialized")
    
    async def _register_federated_types(self):
        """Register federated types with the gateway"""
        # Register DataAsset as a federated type
        data_asset_type = FederatedType(
            name="DataAsset",
            key_fields=["guid"],
            fields=[
                FederatedField(name="guid", type="ID!", description="Unique identifier"),
                FederatedField(name="name", type="String!", description="Asset name"),
                FederatedField(name="qualifiedName", type="String!", description="Fully qualified name"),
                FederatedField(name="typeName", type="String!", description="Asset type"),
                FederatedField(name="description", type="String", description="Asset description"),
                FederatedField(name="owner", type="String", description="Asset owner"),
                FederatedField(name="createdTime", type="DateTime!", description="Creation time"),
                FederatedField(name="modifiedTime", type="DateTime!", description="Last modified time"),
                FederatedField(name="attributes", type="JSON!", description="Additional attributes"),
                FederatedField(name="classifications", type="[String!]!", description="Classifications"),
                # Extended fields
                FederatedField(name="lineage", type="DataLineage", description="Lineage information"),
                FederatedField(name="qualityScore", type="Float", description="Quality score"),
                FederatedField(name="glossaryTerms", type="[GlossaryTerm!]!", description="Associated terms"),
                FederatedField(name="accessAnalytics", type="AccessAnalytics", description="Access analytics")
            ],
            resolve_reference=self._resolve_data_asset_reference
        )
        
        await self.federation_service.register_type(data_asset_type)
        
        # Register GlossaryTerm as a federated type
        glossary_term_type = FederatedType(
            name="GlossaryTerm",
            key_fields=["guid"],
            fields=[
                FederatedField(name="guid", type="ID!", description="Unique identifier"),
                FederatedField(name="name", type="String!", description="Term name"),
                FederatedField(name="definition", type="String!", description="Term definition"),
                FederatedField(name="abbreviation", type="String", description="Abbreviation"),
                FederatedField(name="status", type="String!", description="Term status"),
                FederatedField(name="createdBy", type="String!", description="Creator"),
                FederatedField(name="createdTime", type="DateTime!", description="Creation time"),
                FederatedField(name="relatedTerms", type="[GlossaryTerm!]!", description="Related terms"),
                FederatedField(name="assignedEntities", type="[DataAsset!]!", description="Assigned entities"),
                FederatedField(name="aiSuggestions", type="[String!]!", description="AI suggestions")
            ],
            resolve_reference=self._resolve_glossary_term_reference
        )
        
        await self.federation_service.register_type(glossary_term_type)
        
        logger.info("Registered federated types: DataAsset, GlossaryTerm")
    
    async def _resolve_data_asset_reference(self, reference: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve a DataAsset reference from another service"""
        guid = reference.get("guid")
        if not guid:
            return None
        
        entity_service = self.container.entity_service()
        entity = await entity_service.get_entity(guid)
        
        if entity:
            return {
                "guid": entity.guid,
                "name": entity.attributes.get("name"),
                "qualifiedName": entity.attributes.get("qualifiedName"),
                "typeName": entity.type_name,
                "description": entity.attributes.get("description"),
                "owner": entity.attributes.get("owner"),
                "createdTime": entity.created_time,
                "modifiedTime": entity.modified_time,
                "attributes": entity.attributes,
                "classifications": [c.type_name for c in entity.classifications]
            }
        
        return None
    
    async def _resolve_glossary_term_reference(self, reference: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve a GlossaryTerm reference from another service"""
        guid = reference.get("guid")
        if not guid:
            return None
        
        glossary_service = self.container.glossary_manager()
        term = await glossary_service.get_term(guid)
        
        if term:
            return {
                "guid": term.guid,
                "name": term.name,
                "definition": term.definition,
                "abbreviation": term.abbreviation,
                "status": term.status,
                "createdBy": term.created_by,
                "createdTime": term.created_time,
                "relatedTerms": [],  # Would be populated
                "assignedEntities": []  # Would be populated
            }
        
        return None
    
    async def get_schema_sdl(self) -> str:
        """Get the schema SDL for federation"""
        return self.federation_service.get_schema_sdl()
    
    async def shutdown(self):
        """Shutdown the federation service"""
        if self.federation_service:
            await self.federation_service.stop()
            logger.info("GraphQL federation service stopped")


# Create a singleton instance
federation_schema = None


async def initialize_federation(container):
    """Initialize the federation schema"""
    global federation_schema
    federation_schema = DataCatalogFederationService(container)
    await federation_schema.initialize()
    return federation_schema


async def get_federation_schema():
    """Get the federation schema instance"""
    return federation_schema 