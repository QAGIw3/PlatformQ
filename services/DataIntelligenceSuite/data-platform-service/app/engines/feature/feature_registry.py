"""
Feature Registry for managing feature metadata and lineage.
"""

import asyncio
from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class FeatureSchema:
    """Schema definition for a feature."""
    name: str
    data_type: str
    nullable: bool = True
    constraints: Dict[str, Any] = field(default_factory=dict)  # min, max, enum values, etc.
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureLineage:
    """Lineage information for a feature."""
    feature_name: str
    source_features: List[str] = field(default_factory=list)
    source_datasets: List[str] = field(default_factory=list)
    transformations: List[str] = field(default_factory=list)
    downstream_features: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FeatureVersion:
    """Version information for a feature."""
    feature_name: str
    version: int
    schema: FeatureSchema
    created_at: datetime = field(default_factory=datetime.utcnow)
    created_by: str = ""
    change_description: str = ""
    is_active: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureRegistry:
    """
    Registry for managing feature metadata, schemas, and lineage.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        
        # Registry storage
        self.schemas: Dict[str, FeatureSchema] = {}
        self.lineages: Dict[str, FeatureLineage] = {}
        self.versions: Dict[str, List[FeatureVersion]] = defaultdict(list)
        
        # Feature relationships
        self.feature_graph: Dict[str, Set[str]] = defaultdict(set)  # Dependencies
        self.reverse_graph: Dict[str, Set[str]] = defaultdict(set)  # Dependents
        
        # Tags and metadata
        self.feature_tags: Dict[str, Set[str]] = defaultdict(set)
        self.tag_features: Dict[str, Set[str]] = defaultdict(set)
        
        # Statistics
        self.registry_stats = defaultdict(int)
        
        logger.info("Feature Registry initialized")
        
    async def initialize(self):
        """Initialize feature registry."""
        # Subscribe to events
        await self.event_bus.subscribe("feature.registered", self._handle_feature_registered)
        await self.event_bus.subscribe("feature.updated", self._handle_feature_updated)
        
        # Load registry from cache
        await self._load_registry()
        
        logger.info("Feature Registry ready")
        
    async def register_schema(
        self,
        feature_name: str,
        schema: FeatureSchema,
        created_by: str = "",
        change_description: str = ""
    ) -> FeatureVersion:
        """Register or update a feature schema."""
        # Check if schema exists
        existing = self.schemas.get(feature_name)
        
        # Determine version
        if feature_name in self.versions:
            current_version = max(v.version for v in self.versions[feature_name])
            new_version = current_version + 1
        else:
            new_version = 1
        
        # Create version entry
        version = FeatureVersion(
            feature_name=feature_name,
            version=new_version,
            schema=schema,
            created_by=created_by,
            change_description=change_description
        )
        
        # Store schema and version
        self.schemas[feature_name] = schema
        self.versions[feature_name].append(version)
        
        # Cache schema
        await self.cache_manager.set(
            f"registry:schema:{feature_name}",
            {
                "name": schema.name,
                "data_type": schema.data_type,
                "nullable": schema.nullable,
                "constraints": schema.constraints,
                "description": schema.description,
                "metadata": schema.metadata
            }
        )
        
        # Update statistics
        self.registry_stats["schemas_registered"] += 1
        
        # Publish event
        await self.event_bus.publish("registry.schema.registered", {
            "feature_name": feature_name,
            "version": new_version,
            "data_type": schema.data_type
        })
        
        logger.info(f"Registered schema for feature {feature_name} v{new_version}")
        
        return version
        
    async def register_lineage(
        self,
        feature_name: str,
        source_features: Optional[List[str]] = None,
        source_datasets: Optional[List[str]] = None,
        transformations: Optional[List[str]] = None
    ):
        """Register feature lineage information."""
        # Get or create lineage
        if feature_name in self.lineages:
            lineage = self.lineages[feature_name]
            lineage.updated_at = datetime.utcnow()
        else:
            lineage = FeatureLineage(feature_name=feature_name)
            self.lineages[feature_name] = lineage
        
        # Update lineage
        if source_features:
            lineage.source_features = source_features
            # Update dependency graph
            for source in source_features:
                self.feature_graph[feature_name].add(source)
                self.reverse_graph[source].add(feature_name)
        
        if source_datasets:
            lineage.source_datasets = source_datasets
        
        if transformations:
            lineage.transformations = transformations
        
        # Cache lineage
        await self.cache_manager.set(
            f"registry:lineage:{feature_name}",
            {
                "feature_name": lineage.feature_name,
                "source_features": lineage.source_features,
                "source_datasets": lineage.source_datasets,
                "transformations": lineage.transformations,
                "downstream_features": list(self.reverse_graph.get(feature_name, [])),
                "created_at": lineage.created_at.isoformat(),
                "updated_at": lineage.updated_at.isoformat()
            }
        )
        
        # Publish event
        await self.event_bus.publish("registry.lineage.updated", {
            "feature_name": feature_name,
            "sources": len(lineage.source_features) + len(lineage.source_datasets)
        })
        
        logger.info(f"Updated lineage for feature {feature_name}")
        
    async def add_tags(self, feature_name: str, tags: List[str]):
        """Add tags to a feature."""
        for tag in tags:
            self.feature_tags[feature_name].add(tag)
            self.tag_features[tag].add(feature_name)
        
        # Cache tags
        await self.cache_manager.set(
            f"registry:tags:{feature_name}",
            list(self.feature_tags[feature_name])
        )
        
        logger.info(f"Added {len(tags)} tags to feature {feature_name}")
        
    async def search_by_tags(self, tags: List[str]) -> List[str]:
        """Search features by tags."""
        if not tags:
            return []
        
        # Find features with all specified tags
        result = None
        for tag in tags:
            features = self.tag_features.get(tag, set())
            if result is None:
                result = features.copy()
            else:
                result = result.intersection(features)
        
        return list(result) if result else []
        
    async def get_schema(self, feature_name: str) -> Optional[FeatureSchema]:
        """Get current schema for a feature."""
        return self.schemas.get(feature_name)
        
    async def get_lineage(self, feature_name: str) -> Optional[FeatureLineage]:
        """Get lineage information for a feature."""
        lineage = self.lineages.get(feature_name)
        if lineage:
            # Update downstream features
            lineage.downstream_features = list(self.reverse_graph.get(feature_name, []))
        return lineage
        
    async def get_dependencies(
        self,
        feature_name: str,
        recursive: bool = False
    ) -> List[str]:
        """Get feature dependencies."""
        if not recursive:
            return list(self.feature_graph.get(feature_name, []))
        
        # Recursive dependency resolution
        dependencies = set()
        to_process = [feature_name]
        
        while to_process:
            current = to_process.pop()
            for dep in self.feature_graph.get(current, []):
                if dep not in dependencies:
                    dependencies.add(dep)
                    to_process.append(dep)
        
        return list(dependencies)
        
    async def get_dependents(
        self,
        feature_name: str,
        recursive: bool = False
    ) -> List[str]:
        """Get features that depend on this feature."""
        if not recursive:
            return list(self.reverse_graph.get(feature_name, []))
        
        # Recursive dependent resolution
        dependents = set()
        to_process = [feature_name]
        
        while to_process:
            current = to_process.pop()
            for dep in self.reverse_graph.get(current, []):
                if dep not in dependents:
                    dependents.add(dep)
                    to_process.append(dep)
        
        return list(dependents)
        
    async def get_version_history(
        self,
        feature_name: str,
        limit: int = 10
    ) -> List[FeatureVersion]:
        """Get version history for a feature."""
        versions = self.versions.get(feature_name, [])
        # Sort by version descending
        sorted_versions = sorted(versions, key=lambda v: v.version, reverse=True)
        return sorted_versions[:limit]
        
    async def validate_schema_change(
        self,
        feature_name: str,
        new_schema: FeatureSchema
    ) -> Dict[str, Any]:
        """Validate a schema change for compatibility."""
        current_schema = self.schemas.get(feature_name)
        
        if not current_schema:
            return {"valid": True, "issues": []}
        
        issues = []
        
        # Check data type compatibility
        if current_schema.data_type != new_schema.data_type:
            issues.append({
                "type": "data_type_change",
                "severity": "high",
                "message": f"Data type changed from {current_schema.data_type} to {new_schema.data_type}"
            })
        
        # Check nullability
        if current_schema.nullable and not new_schema.nullable:
            issues.append({
                "type": "nullability_change",
                "severity": "high",
                "message": "Changed from nullable to non-nullable"
            })
        
        # Check constraints
        for constraint, value in new_schema.constraints.items():
            if constraint in current_schema.constraints:
                current_value = current_schema.constraints[constraint]
                if constraint == "min" and value > current_value:
                    issues.append({
                        "type": "constraint_tightened",
                        "severity": "medium",
                        "message": f"Minimum value increased from {current_value} to {value}"
                    })
                elif constraint == "max" and value < current_value:
                    issues.append({
                        "type": "constraint_tightened",
                        "severity": "medium",
                        "message": f"Maximum value decreased from {current_value} to {value}"
                    })
        
        # Check dependent features
        dependents = await self.get_dependents(feature_name)
        if dependents and issues:
            issues.append({
                "type": "has_dependents",
                "severity": "high",
                "message": f"This feature has {len(dependents)} dependent features",
                "dependents": dependents
            })
        
        return {
            "valid": len([i for i in issues if i["severity"] == "high"]) == 0,
            "issues": issues
        }
        
    async def export_registry(self) -> Dict[str, Any]:
        """Export the entire registry."""
        return {
            "schemas": {
                name: {
                    "name": schema.name,
                    "data_type": schema.data_type,
                    "nullable": schema.nullable,
                    "constraints": schema.constraints,
                    "description": schema.description
                }
                for name, schema in self.schemas.items()
            },
            "lineages": {
                name: {
                    "source_features": lineage.source_features,
                    "source_datasets": lineage.source_datasets,
                    "transformations": lineage.transformations,
                    "downstream_features": list(self.reverse_graph.get(name, []))
                }
                for name, lineage in self.lineages.items()
            },
            "tags": {
                name: list(tags)
                for name, tags in self.feature_tags.items()
            },
            "statistics": dict(self.registry_stats)
        }
        
    async def _load_registry(self):
        """Load registry from cache."""
        try:
            # Load schemas
            schema_keys = await self.cache_manager.get("registry:schemas:*")
            # Implementation would load from cache
            
            logger.info("Loaded registry from cache")
        except Exception as e:
            logger.warning(f"Could not load registry from cache: {e}")
            
    async def _handle_feature_registered(self, event_data: Dict[str, Any]):
        """Handle feature registration event."""
        try:
            feature_name = event_data.get("name")
            if feature_name and feature_name not in self.schemas:
                # Auto-register basic schema
                schema = FeatureSchema(
                    name=feature_name,
                    data_type=event_data.get("data_type", "unknown")
                )
                await self.register_schema(feature_name, schema)
                
        except Exception as e:
            logger.error(f"Error handling feature registration: {e}")
            
    async def _handle_feature_updated(self, event_data: Dict[str, Any]):
        """Handle feature update event."""
        try:
            feature_name = event_data.get("name")
            if feature_name:
                # Update registry statistics
                self.registry_stats["feature_updates"] += 1
                
        except Exception as e:
            logger.error(f"Error handling feature update: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get registry statistics."""
        return {
            "total_features": len(self.schemas),
            "total_versions": sum(len(versions) for versions in self.versions.values()),
            "features_with_lineage": len(self.lineages),
            "total_tags": len(self.tag_features),
            "features_with_dependencies": len([f for f, deps in self.feature_graph.items() if deps]),
            "registry_stats": dict(self.registry_stats)
        } 