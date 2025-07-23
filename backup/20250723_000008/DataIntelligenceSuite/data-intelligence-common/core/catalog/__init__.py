"""
Data catalog core components.

Provides comprehensive catalog functionality for data intelligence.
"""

from .base_catalog import (
    BaseCatalog,
    CatalogConfig,
    CatalogEntity,
    EntityType,
    EntityStatus
)

from .metadata_manager import (
    MetadataManager,
    MetadataSchema,
    MetadataField,
    MetadataType,
    FieldType
)

from .lineage_tracker import (
    LineageTracker,
    LineageNode,
    LineageEdge,
    LineageGraph,
    LineageType,
    LineageDirection
)

from .discovery_engine import (
    DiscoveryEngine,
    DataSource,
    DiscoveryPattern,
    DiscoveryResult,
    AssetType,
    DiscoveryStatus
)

from .quality_integrator import (
    QualityIntegrator,
    QualityRule,
    QualityProfile,
    QualityCheckResult,
    QualityDimension,
    QualityStatus
)

from .search_interface import (
    CatalogSearch,
    SearchQuery,
    SearchResult,
    SearchHit,
    SearchIntent,
    SearchOperator
)

from .glossary_manager import (
    GlossaryManager,
    GlossaryTerm,
    TermCategory,
    TermRelationship,
    TermStatus,
    RelationType
)

from .access_controller import (
    AccessController,
    Principal,
    Role,
    AccessPolicy,
    Permission,
    PolicyEffect
)

__all__ = [
    # Base
    "BaseCatalog",
    "CatalogConfig",
    "CatalogEntity",
    "EntityType",
    "EntityStatus",
    
    # Metadata
    "MetadataManager",
    "MetadataSchema",
    "MetadataField",
    "MetadataType",
    "FieldType",
    
    # Lineage
    "LineageTracker",
    "LineageNode",
    "LineageEdge",
    "LineageGraph",
    "LineageType",
    "LineageDirection",
    
    # Discovery
    "DiscoveryEngine",
    "DataSource",
    "DiscoveryPattern",
    "DiscoveryResult",
    "AssetType",
    "DiscoveryStatus",
    
    # Quality
    "QualityIntegrator",
    "QualityRule",
    "QualityProfile",
    "QualityCheckResult",
    "QualityDimension",
    "QualityStatus",
    
    # Search
    "CatalogSearch",
    "SearchQuery",
    "SearchResult",
    "SearchHit",
    "SearchIntent",
    "SearchOperator",
    
    # Glossary
    "GlossaryManager",
    "GlossaryTerm",
    "TermCategory",
    "TermRelationship",
    "TermStatus",
    "RelationType",
    
    # Access
    "AccessController",
    "Principal",
    "Role",
    "AccessPolicy",
    "Permission",
    "PolicyEffect"
] 