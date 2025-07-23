"""
Catalog Service Client

Client for data catalog operations.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime

from .base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


@dataclass
class CatalogItem:
    """Catalog item model"""
    id: str
    name: str
    type: str
    qualified_name: str
    description: Optional[str] = None
    owner: Optional[str] = None
    tags: List[str] = None
    metadata: Dict[str, Any] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
        if self.metadata is None:
            self.metadata = {}


@dataclass
class LineageInfo:
    """Data lineage information"""
    entity_id: str
    upstream: List[str]
    downstream: List[str]
    impact_analysis: Optional[Dict[str, Any]] = None


@dataclass
class DataQualityReport:
    """Data quality report"""
    entity_id: str
    overall_score: float
    dimensions: Dict[str, float]
    issues: List[Dict[str, Any]]
    recommendations: List[str]
    timestamp: datetime


class CatalogServiceClient(BaseServiceClient):
    """
    Client for catalog service operations.
    
    Features:
    - Entity management
    - Metadata operations
    - Lineage tracking
    - Quality integration
    - Search capabilities
    """
    
    def __init__(self, config: Optional[ClientConfig] = None, **kwargs):
        if not config:
            config = ClientConfig(service_name="data-catalog-hub")
        super().__init__(config, **kwargs)
        
    # Entity Management
    
    async def create_entity(
        self,
        name: str,
        entity_type: str,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> CatalogItem:
        """
        Create a new catalog entity.
        
        Args:
            name: Entity name
            entity_type: Type of entity (dataset, table, model, etc.)
            description: Entity description
            owner: Entity owner
            tags: List of tags
            metadata: Additional metadata
            **kwargs: Additional entity attributes
            
        Returns:
            Created catalog item
        """
        data = {
            "name": name,
            "type": entity_type,
            "description": description,
            "owner": owner,
            "tags": tags or [],
            "metadata": metadata or {},
            **kwargs
        }
        
        response = await self.post("/entities", json_data=data)
        
        return CatalogItem(
            id=response["id"],
            name=response["name"],
            type=response["type"],
            qualified_name=response["qualified_name"],
            description=response.get("description"),
            owner=response.get("owner"),
            tags=response.get("tags", []),
            metadata=response.get("metadata", {}),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at")
        )
        
    async def get_entity(self, entity_id: str) -> Optional[CatalogItem]:
        """
        Get entity by ID.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Catalog item if found
        """
        try:
            response = await self.get(f"/entities/{entity_id}")
            
            return CatalogItem(
                id=response["id"],
                name=response["name"],
                type=response["type"],
                qualified_name=response["qualified_name"],
                description=response.get("description"),
                owner=response.get("owner"),
                tags=response.get("tags", []),
                metadata=response.get("metadata", {}),
                created_at=response.get("created_at"),
                updated_at=response.get("updated_at")
            )
        except Exception as e:
            logger.error(f"Failed to get entity {entity_id}: {e}")
            return None
            
    async def update_entity(
        self,
        entity_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> CatalogItem:
        """
        Update catalog entity.
        
        Args:
            entity_id: Entity ID
            name: New name
            description: New description
            owner: New owner
            tags: New tags (replaces existing)
            metadata: New metadata (merges with existing)
            **kwargs: Additional updates
            
        Returns:
            Updated catalog item
        """
        data = {}
        if name is not None:
            data["name"] = name
        if description is not None:
            data["description"] = description
        if owner is not None:
            data["owner"] = owner
        if tags is not None:
            data["tags"] = tags
        if metadata is not None:
            data["metadata"] = metadata
            
        data.update(kwargs)
        
        response = await self.patch(f"/entities/{entity_id}", json_data=data)
        
        return CatalogItem(
            id=response["id"],
            name=response["name"],
            type=response["type"],
            qualified_name=response["qualified_name"],
            description=response.get("description"),
            owner=response.get("owner"),
            tags=response.get("tags", []),
            metadata=response.get("metadata", {}),
            created_at=response.get("created_at"),
            updated_at=response.get("updated_at")
        )
        
    async def delete_entity(self, entity_id: str) -> bool:
        """
        Delete catalog entity.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Success status
        """
        response = await self.delete(f"/entities/{entity_id}")
        return response.get("success", False)
        
    # Search Operations
    
    async def search(
        self,
        query: str,
        entity_types: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
        owners: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[CatalogItem]:
        """
        Search catalog entities.
        
        Args:
            query: Search query
            entity_types: Filter by entity types
            tags: Filter by tags
            owners: Filter by owners
            limit: Maximum results
            offset: Pagination offset
            
        Returns:
            List of matching catalog items
        """
        params = {
            "q": query,
            "limit": limit,
            "offset": offset
        }
        
        if entity_types:
            params["types"] = ",".join(entity_types)
        if tags:
            params["tags"] = ",".join(tags)
        if owners:
            params["owners"] = ",".join(owners)
            
        response = await self.get("/search", params=params)
        
        return [
            CatalogItem(
                id=item["id"],
                name=item["name"],
                type=item["type"],
                qualified_name=item["qualified_name"],
                description=item.get("description"),
                owner=item.get("owner"),
                tags=item.get("tags", []),
                metadata=item.get("metadata", {}),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at")
            )
            for item in response.get("results", [])
        ]
        
    # Lineage Operations
    
    async def get_lineage(
        self,
        entity_id: str,
        direction: str = "both",
        depth: int = 1
    ) -> LineageInfo:
        """
        Get entity lineage.
        
        Args:
            entity_id: Entity ID
            direction: Lineage direction (upstream, downstream, both)
            depth: Traversal depth
            
        Returns:
            Lineage information
        """
        params = {
            "direction": direction,
            "depth": depth
        }
        
        response = await self.get(f"/entities/{entity_id}/lineage", params=params)
        
        return LineageInfo(
            entity_id=entity_id,
            upstream=response.get("upstream", []),
            downstream=response.get("downstream", []),
            impact_analysis=response.get("impact_analysis")
        )
        
    async def add_lineage(
        self,
        source_id: str,
        target_id: str,
        relationship_type: str = "derives_from"
    ) -> bool:
        """
        Add lineage relationship.
        
        Args:
            source_id: Source entity ID
            target_id: Target entity ID
            relationship_type: Type of relationship
            
        Returns:
            Success status
        """
        data = {
            "source_id": source_id,
            "target_id": target_id,
            "relationship_type": relationship_type
        }
        
        response = await self.post("/lineage", json_data=data)
        return response.get("success", False)
        
    # Quality Operations
    
    async def get_quality_report(
        self,
        entity_id: str
    ) -> Optional[DataQualityReport]:
        """
        Get data quality report for entity.
        
        Args:
            entity_id: Entity ID
            
        Returns:
            Quality report if available
        """
        try:
            response = await self.get(f"/entities/{entity_id}/quality")
            
            return DataQualityReport(
                entity_id=entity_id,
                overall_score=response["overall_score"],
                dimensions=response["dimensions"],
                issues=response.get("issues", []),
                recommendations=response.get("recommendations", []),
                timestamp=response.get("timestamp", datetime.utcnow())
            )
        except Exception as e:
            logger.error(f"Failed to get quality report for {entity_id}: {e}")
            return None
            
    async def trigger_quality_check(
        self,
        entity_id: str,
        check_types: Optional[List[str]] = None
    ) -> str:
        """
        Trigger quality check for entity.
        
        Args:
            entity_id: Entity ID
            check_types: Specific checks to run
            
        Returns:
            Job ID for the quality check
        """
        data = {
            "entity_id": entity_id,
            "check_types": check_types or []
        }
        
        response = await self.post("/quality/check", json_data=data)
        return response["job_id"]
        
    # Tag Management
    
    async def add_tags(
        self,
        entity_id: str,
        tags: List[str]
    ) -> bool:
        """
        Add tags to entity.
        
        Args:
            entity_id: Entity ID
            tags: Tags to add
            
        Returns:
            Success status
        """
        data = {"tags": tags}
        response = await self.post(f"/entities/{entity_id}/tags", json_data=data)
        return response.get("success", False)
        
    async def remove_tags(
        self,
        entity_id: str,
        tags: List[str]
    ) -> bool:
        """
        Remove tags from entity.
        
        Args:
            entity_id: Entity ID
            tags: Tags to remove
            
        Returns:
            Success status
        """
        data = {"tags": tags}
        response = await self.delete(f"/entities/{entity_id}/tags", json_data=data)
        return response.get("success", False)
        
    # Metadata Operations
    
    async def update_metadata(
        self,
        entity_id: str,
        metadata: Dict[str, Any],
        merge: bool = True
    ) -> bool:
        """
        Update entity metadata.
        
        Args:
            entity_id: Entity ID
            metadata: Metadata to update
            merge: Whether to merge with existing metadata
            
        Returns:
            Success status
        """
        data = {
            "metadata": metadata,
            "merge": merge
        }
        
        response = await self.patch(f"/entities/{entity_id}/metadata", json_data=data)
        return response.get("success", False)
        
    # Bulk Operations
    
    async def bulk_create(
        self,
        entities: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Create multiple entities.
        
        Args:
            entities: List of entity data
            
        Returns:
            List of created entity IDs
        """
        response = await self.post("/entities/bulk", json_data={"entities": entities})
        return response.get("entity_ids", [])
        
    async def bulk_update(
        self,
        updates: List[Dict[str, Any]]
    ) -> Dict[str, bool]:
        """
        Update multiple entities.
        
        Args:
            updates: List of updates (must include entity_id)
            
        Returns:
            Map of entity_id to success status
        """
        response = await self.patch("/entities/bulk", json_data={"updates": updates})
        return response.get("results", {})
        
    # Discovery Operations
    
    async def discover_assets(
        self,
        source_type: str,
        connection_info: Dict[str, Any],
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Discover assets from a data source.
        
        Args:
            source_type: Type of source (s3, database, etc.)
            connection_info: Connection details
            filters: Discovery filters
            
        Returns:
            List of discovered assets
        """
        data = {
            "source_type": source_type,
            "connection_info": connection_info,
            "filters": filters or {}
        }
        
        response = await self.post("/discover", json_data=data)
        return response.get("assets", [])
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get catalog-specific configuration from Consul"""
        if self.consul_client:
            config = await self.consul_client.get_key(
                f"config/{self.config.service_name}/client"
            )
            return config or {}
        return {} 