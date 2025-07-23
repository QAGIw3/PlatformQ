"""
Business glossary management for catalog.

Provides comprehensive glossary term management with relationships and governance.
"""

import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field
import re
from collections import defaultdict
import json

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class TermStatus(str, Enum):
    """Glossary term status"""
    DRAFT = "draft"
    PROPOSED = "proposed"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    REJECTED = "rejected"


class RelationType(str, Enum):
    """Term relationship types"""
    SYNONYM = "synonym"
    ANTONYM = "antonym"
    RELATED = "related"
    PARENT = "parent"
    CHILD = "child"
    SEE_ALSO = "see_also"
    REPLACES = "replaces"
    REPLACED_BY = "replaced_by"


@dataclass
class TermRelationship:
    """Relationship between glossary terms"""
    source_term_id: str
    target_term_id: str
    relationship_type: RelationType
    description: Optional[str] = None
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "source_term_id": self.source_term_id,
            "target_term_id": self.target_term_id,
            "relationship_type": self.relationship_type.value,
            "description": self.description,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class TermCategory:
    """Glossary term category"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    parent_category_id: Optional[str] = None
    icon: Optional[str] = None
    color: Optional[str] = None
    order: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "parent_category_id": self.parent_category_id,
            "icon": self.icon,
            "color": self.color,
            "order": self.order
        }


@dataclass
class GlossaryTerm:
    """Business glossary term"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    term: str = ""
    definition: str = ""
    
    # Status and governance
    status: TermStatus = TermStatus.DRAFT
    version: str = "1.0"
    
    # Categorization
    category_id: Optional[str] = None
    domain: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    # Additional content
    acronym: Optional[str] = None
    pronunciation: Optional[str] = None
    usage_notes: Optional[str] = None
    examples: List[str] = field(default_factory=list)
    
    # Ownership
    owner: Optional[str] = None
    steward: Optional[str] = None
    
    # Approval workflow
    proposed_by: Optional[str] = None
    proposed_at: Optional[datetime] = None
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None
    
    # Metadata
    created_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_by: Optional[str] = None
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Linked assets
    linked_assets: List[str] = field(default_factory=list)
    
    # Relationships (stored separately but cached here)
    relationships: List[TermRelationship] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "term": self.term,
            "definition": self.definition,
            "status": self.status.value,
            "version": self.version,
            "category_id": self.category_id,
            "domain": self.domain,
            "tags": self.tags,
            "acronym": self.acronym,
            "pronunciation": self.pronunciation,
            "usage_notes": self.usage_notes,
            "examples": self.examples,
            "owner": self.owner,
            "steward": self.steward,
            "proposed_by": self.proposed_by,
            "proposed_at": self.proposed_at.isoformat() if self.proposed_at else None,
            "approved_by": self.approved_by,
            "approved_at": self.approved_at.isoformat() if self.approved_at else None,
            "rejection_reason": self.rejection_reason,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_by": self.updated_by,
            "updated_at": self.updated_at.isoformat(),
            "linked_assets": self.linked_assets,
            "relationships": [r.to_dict() for r in self.relationships]
        }


@dataclass
class GlossaryVersion:
    """Glossary version for tracking changes"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    term_id: str = ""
    version: str = ""
    changes: Dict[str, Any] = field(default_factory=dict)
    changed_by: str = ""
    changed_at: datetime = field(default_factory=datetime.utcnow)
    change_reason: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "term_id": self.term_id,
            "version": self.version,
            "changes": self.changes,
            "changed_by": self.changed_by,
            "changed_at": self.changed_at.isoformat(),
            "change_reason": self.change_reason
        }


class GlossaryManager:
    """
    Manages business glossary terms and relationships.
    
    Features:
    - Term lifecycle management
    - Hierarchical categories
    - Relationship management
    - Approval workflow
    - Version control
    - Asset linking
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._terms: Dict[str, GlossaryTerm] = {}
        self._categories: Dict[str, TermCategory] = {}
        self._relationships: Dict[Tuple[str, str], TermRelationship] = {}
        self._versions: Dict[str, List[GlossaryVersion]] = defaultdict(list)
        
        # Indexes
        self._term_index: Dict[str, str] = {}  # term -> id
        self._category_terms: Dict[str, Set[str]] = defaultdict(set)
        self._asset_terms: Dict[str, Set[str]] = defaultdict(set)
        
        # Initialize default categories
        self._initialize_default_categories()
        
    def _initialize_default_categories(self):
        """Initialize default term categories"""
        default_categories = [
            TermCategory(name="Business", description="Business-related terms", order=1),
            TermCategory(name="Technical", description="Technical terms", order=2),
            TermCategory(name="Data", description="Data-related terms", order=3),
            TermCategory(name="Process", description="Process and workflow terms", order=4),
            TermCategory(name="Compliance", description="Compliance and regulatory terms", order=5)
        ]
        
        for category in default_categories:
            self._categories[category.id] = category
            
    def create_term(
        self,
        term: str,
        definition: str,
        category_id: Optional[str] = None,
        owner: Optional[str] = None,
        auto_approve: bool = False,
        **kwargs
    ) -> GlossaryTerm:
        """Create new glossary term"""
        # Check if term already exists
        if term.lower() in self._term_index:
            raise ValueError(f"Term '{term}' already exists")
            
        # Create term
        glossary_term = GlossaryTerm(
            term=term,
            definition=definition,
            category_id=category_id,
            owner=owner,
            status=TermStatus.APPROVED if auto_approve else TermStatus.DRAFT,
            created_by=kwargs.get("created_by"),
            **kwargs
        )
        
        # Store term
        self._terms[glossary_term.id] = glossary_term
        self._term_index[term.lower()] = glossary_term.id
        
        # Update indexes
        if category_id:
            self._category_terms[category_id].add(glossary_term.id)
            
        # Cache term
        if self.cache:
            cache_key = f"glossary:term:{glossary_term.id}"
            self.cache.set(cache_key, glossary_term.to_dict(), ttl=3600)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="glossary.term.created",
                source="glossary_manager",
                data={
                    "term_id": glossary_term.id,
                    "term": term,
                    "status": glossary_term.status.value
                }
            ))
            
        logger.info(f"Created glossary term: {term}")
        return glossary_term
        
    def update_term(
        self,
        term_id: str,
        updates: Dict[str, Any],
        updated_by: str,
        reason: Optional[str] = None
    ) -> GlossaryTerm:
        """Update glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        # Track version
        old_version = term.version
        changes = {}
        
        # Apply updates
        for field, value in updates.items():
            if hasattr(term, field):
                old_value = getattr(term, field)
                if old_value != value:
                    changes[field] = {"old": old_value, "new": value}
                    setattr(term, field, value)
                    
        if changes:
            # Update metadata
            term.updated_by = updated_by
            term.updated_at = datetime.utcnow()
            
            # Increment version
            version_parts = term.version.split(".")
            version_parts[-1] = str(int(version_parts[-1]) + 1)
            term.version = ".".join(version_parts)
            
            # Create version record
            version = GlossaryVersion(
                term_id=term_id,
                version=term.version,
                changes=changes,
                changed_by=updated_by,
                change_reason=reason
            )
            self._versions[term_id].append(version)
            
            # Update indexes if term name changed
            if "term" in changes:
                old_term = changes["term"]["old"]
                new_term = changes["term"]["new"]
                del self._term_index[old_term.lower()]
                self._term_index[new_term.lower()] = term_id
                
            # Clear cache
            if self.cache:
                cache_key = f"glossary:term:{term_id}"
                self.cache.delete(cache_key)
                
            # Publish event
            if self.event_bus:
                self.event_bus.publish(Event(
                    type="glossary.term.updated",
                    source="glossary_manager",
                    data={
                        "term_id": term_id,
                        "version": term.version,
                        "changes": list(changes.keys())
                    }
                ))
                
            logger.info(f"Updated glossary term: {term.term} to version {term.version}")
            
        return term
        
    def add_relationship(
        self,
        source_term_id: str,
        target_term_id: str,
        relationship_type: RelationType,
        created_by: Optional[str] = None,
        bidirectional: bool = False
    ) -> TermRelationship:
        """Add relationship between terms"""
        # Validate terms exist
        if source_term_id not in self._terms:
            raise ValueError(f"Source term not found: {source_term_id}")
        if target_term_id not in self._terms:
            raise ValueError(f"Target term not found: {target_term_id}")
            
        # Create relationship
        relationship = TermRelationship(
            source_term_id=source_term_id,
            target_term_id=target_term_id,
            relationship_type=relationship_type,
            created_by=created_by
        )
        
        # Store relationship
        key = (source_term_id, target_term_id)
        self._relationships[key] = relationship
        
        # Add to term's relationships
        self._terms[source_term_id].relationships.append(relationship)
        
        # Handle bidirectional relationships
        if bidirectional:
            inverse_type = self._get_inverse_relationship_type(relationship_type)
            if inverse_type:
                inverse_rel = TermRelationship(
                    source_term_id=target_term_id,
                    target_term_id=source_term_id,
                    relationship_type=inverse_type,
                    created_by=created_by
                )
                self._relationships[(target_term_id, source_term_id)] = inverse_rel
                self._terms[target_term_id].relationships.append(inverse_rel)
                
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="glossary.relationship.added",
                source="glossary_manager",
                data={
                    "source_term_id": source_term_id,
                    "target_term_id": target_term_id,
                    "relationship_type": relationship_type.value
                }
            ))
            
        return relationship
        
    def _get_inverse_relationship_type(self, rel_type: RelationType) -> Optional[RelationType]:
        """Get inverse relationship type"""
        inverse_map = {
            RelationType.PARENT: RelationType.CHILD,
            RelationType.CHILD: RelationType.PARENT,
            RelationType.REPLACES: RelationType.REPLACED_BY,
            RelationType.REPLACED_BY: RelationType.REPLACES,
            RelationType.SYNONYM: RelationType.SYNONYM,
            RelationType.ANTONYM: RelationType.ANTONYM,
            RelationType.RELATED: RelationType.RELATED,
            RelationType.SEE_ALSO: RelationType.SEE_ALSO
        }
        return inverse_map.get(rel_type)
        
    def link_asset(self, term_id: str, asset_id: str):
        """Link asset to glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        if asset_id not in term.linked_assets:
            term.linked_assets.append(asset_id)
            self._asset_terms[asset_id].add(term_id)
            
            # Clear cache
            if self.cache:
                cache_key = f"glossary:term:{term_id}"
                self.cache.delete(cache_key)
                
            logger.info(f"Linked asset {asset_id} to term {term.term}")
            
    def unlink_asset(self, term_id: str, asset_id: str):
        """Unlink asset from glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        if asset_id in term.linked_assets:
            term.linked_assets.remove(asset_id)
            self._asset_terms[asset_id].discard(term_id)
            
            # Clear cache
            if self.cache:
                cache_key = f"glossary:term:{term_id}"
                self.cache.delete(cache_key)
                
    def approve_term(
        self,
        term_id: str,
        approved_by: str,
        notes: Optional[str] = None
    ) -> GlossaryTerm:
        """Approve glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        if term.status not in [TermStatus.DRAFT, TermStatus.PROPOSED]:
            raise ValueError(f"Term cannot be approved from status: {term.status}")
            
        # Update term
        term.status = TermStatus.APPROVED
        term.approved_by = approved_by
        term.approved_at = datetime.utcnow()
        term.updated_by = approved_by
        term.updated_at = datetime.utcnow()
        
        # Clear cache
        if self.cache:
            cache_key = f"glossary:term:{term_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="glossary.term.approved",
                source="glossary_manager",
                data={
                    "term_id": term_id,
                    "term": term.term,
                    "approved_by": approved_by
                }
            ))
            
        logger.info(f"Approved glossary term: {term.term}")
        return term
        
    def reject_term(
        self,
        term_id: str,
        rejected_by: str,
        reason: str
    ) -> GlossaryTerm:
        """Reject glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        if term.status not in [TermStatus.DRAFT, TermStatus.PROPOSED]:
            raise ValueError(f"Term cannot be rejected from status: {term.status}")
            
        # Update term
        term.status = TermStatus.REJECTED
        term.rejection_reason = reason
        term.updated_by = rejected_by
        term.updated_at = datetime.utcnow()
        
        # Clear cache
        if self.cache:
            cache_key = f"glossary:term:{term_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="glossary.term.rejected",
                source="glossary_manager",
                data={
                    "term_id": term_id,
                    "term": term.term,
                    "rejected_by": rejected_by,
                    "reason": reason
                }
            ))
            
        logger.info(f"Rejected glossary term: {term.term}")
        return term
        
    def deprecate_term(
        self,
        term_id: str,
        deprecated_by: str,
        replacement_term_id: Optional[str] = None,
        reason: Optional[str] = None
    ) -> GlossaryTerm:
        """Deprecate glossary term"""
        term = self._terms.get(term_id)
        if not term:
            raise ValueError(f"Term not found: {term_id}")
            
        # Update term
        term.status = TermStatus.DEPRECATED
        term.updated_by = deprecated_by
        term.updated_at = datetime.utcnow()
        
        # Add replacement relationship if provided
        if replacement_term_id:
            self.add_relationship(
                term_id,
                replacement_term_id,
                RelationType.REPLACED_BY,
                deprecated_by,
                bidirectional=True
            )
            
        # Clear cache
        if self.cache:
            cache_key = f"glossary:term:{term_id}"
            self.cache.delete(cache_key)
            
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="glossary.term.deprecated",
                source="glossary_manager",
                data={
                    "term_id": term_id,
                    "term": term.term,
                    "deprecated_by": deprecated_by,
                    "replacement_term_id": replacement_term_id
                }
            ))
            
        logger.info(f"Deprecated glossary term: {term.term}")
        return term
        
    def search_terms(
        self,
        query: str,
        category_id: Optional[str] = None,
        status: Optional[TermStatus] = None,
        domain: Optional[str] = None,
        limit: int = 50
    ) -> List[GlossaryTerm]:
        """Search glossary terms"""
        results = []
        query_lower = query.lower()
        
        for term in self._terms.values():
            # Apply filters
            if category_id and term.category_id != category_id:
                continue
            if status and term.status != status:
                continue
            if domain and term.domain != domain:
                continue
                
            # Search in term, definition, and acronym
            if (query_lower in term.term.lower() or
                query_lower in term.definition.lower() or
                (term.acronym and query_lower in term.acronym.lower())):
                results.append(term)
                
        # Sort by relevance (simple scoring)
        results.sort(key=lambda t: (
            query_lower == t.term.lower(),  # Exact match
            query_lower in t.term.lower(),   # Term contains query
            len(t.term)                      # Shorter terms first
        ), reverse=True)
        
        return results[:limit]
        
    def get_term(self, term_id: str) -> Optional[GlossaryTerm]:
        """Get term by ID"""
        # Check cache first
        if self.cache:
            cache_key = f"glossary:term:{term_id}"
            cached = self.cache.get(cache_key)
            if cached:
                return self._dict_to_term(cached)
                
        return self._terms.get(term_id)
        
    def get_term_by_name(self, term: str) -> Optional[GlossaryTerm]:
        """Get term by name"""
        term_id = self._term_index.get(term.lower())
        if term_id:
            return self.get_term(term_id)
        return None
        
    def get_terms_by_category(
        self,
        category_id: str,
        include_subcategories: bool = True
    ) -> List[GlossaryTerm]:
        """Get all terms in category"""
        term_ids = set(self._category_terms.get(category_id, set()))
        
        if include_subcategories:
            # Get subcategory terms
            for cat_id, category in self._categories.items():
                if category.parent_category_id == category_id:
                    term_ids.update(self._category_terms.get(cat_id, set()))
                    
        return [self._terms[tid] for tid in term_ids if tid in self._terms]
        
    def get_terms_for_asset(self, asset_id: str) -> List[GlossaryTerm]:
        """Get all terms linked to asset"""
        term_ids = self._asset_terms.get(asset_id, set())
        return [self._terms[tid] for tid in term_ids if tid in self._terms]
        
    def get_related_terms(
        self,
        term_id: str,
        relationship_types: Optional[List[RelationType]] = None
    ) -> List[Tuple[GlossaryTerm, TermRelationship]]:
        """Get related terms"""
        term = self._terms.get(term_id)
        if not term:
            return []
            
        related = []
        for rel in term.relationships:
            if relationship_types and rel.relationship_type not in relationship_types:
                continue
                
            target_term = self._terms.get(rel.target_term_id)
            if target_term:
                related.append((target_term, rel))
                
        return related
        
    def get_term_hierarchy(self, term_id: str) -> Dict[str, Any]:
        """Get term hierarchy (parents and children)"""
        hierarchy = {
            "term": self._terms.get(term_id),
            "parents": [],
            "children": []
        }
        
        if not hierarchy["term"]:
            return hierarchy
            
        # Get parents and children
        for rel in hierarchy["term"].relationships:
            if rel.relationship_type == RelationType.CHILD:
                parent = self._terms.get(rel.target_term_id)
                if parent:
                    hierarchy["parents"].append(parent)
            elif rel.relationship_type == RelationType.PARENT:
                child = self._terms.get(rel.target_term_id)
                if child:
                    hierarchy["children"].append(child)
                    
        return hierarchy
        
    def get_term_history(self, term_id: str) -> List[GlossaryVersion]:
        """Get term version history"""
        return self._versions.get(term_id, [])
        
    def create_category(
        self,
        name: str,
        description: Optional[str] = None,
        parent_category_id: Optional[str] = None
    ) -> TermCategory:
        """Create term category"""
        category = TermCategory(
            name=name,
            description=description,
            parent_category_id=parent_category_id
        )
        
        self._categories[category.id] = category
        
        logger.info(f"Created glossary category: {name}")
        return category
        
    def get_categories(self) -> List[TermCategory]:
        """Get all categories"""
        return list(self._categories.values())
        
    def export_glossary(
        self,
        format: str = "json",
        include_relationships: bool = True,
        include_history: bool = False
    ) -> Union[Dict[str, Any], str]:
        """Export glossary"""
        export_data = {
            "metadata": {
                "exported_at": datetime.utcnow().isoformat(),
                "total_terms": len(self._terms),
                "total_categories": len(self._categories),
                "total_relationships": len(self._relationships)
            },
            "categories": [c.to_dict() for c in self._categories.values()],
            "terms": []
        }
        
        # Export terms
        for term in self._terms.values():
            term_data = term.to_dict()
            
            if not include_relationships:
                term_data.pop("relationships", None)
                
            if include_history:
                term_data["history"] = [
                    v.to_dict() for v in self._versions.get(term.id, [])
                ]
                
            export_data["terms"].append(term_data)
            
        if format == "json":
            return export_data
        elif format == "csv":
            # Convert to CSV format
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=["term", "definition", "category", "status", "owner"]
            )
            writer.writeheader()
            
            for term in self._terms.values():
                category = self._categories.get(term.category_id, {})
                writer.writerow({
                    "term": term.term,
                    "definition": term.definition,
                    "category": category.get("name", ""),
                    "status": term.status.value,
                    "owner": term.owner or ""
                })
                
            return output.getvalue()
        else:
            raise ValueError(f"Unsupported export format: {format}")
            
    def import_glossary(
        self,
        data: Union[Dict[str, Any], str],
        format: str = "json",
        merge: bool = True
    ) -> Dict[str, int]:
        """Import glossary data"""
        if format == "json":
            import_data = data if isinstance(data, dict) else json.loads(data)
        else:
            raise ValueError(f"Unsupported import format: {format}")
            
        counts = {
            "categories_imported": 0,
            "terms_imported": 0,
            "terms_updated": 0,
            "relationships_imported": 0
        }
        
        # Import categories
        for cat_data in import_data.get("categories", []):
            if not merge or cat_data["id"] not in self._categories:
                category = TermCategory(**cat_data)
                self._categories[category.id] = category
                counts["categories_imported"] += 1
                
        # Import terms
        for term_data in import_data.get("terms", []):
            term_name = term_data["term"]
            existing_id = self._term_index.get(term_name.lower())
            
            if existing_id and merge:
                # Update existing term
                updates = {k: v for k, v in term_data.items() if k != "id"}
                self.update_term(existing_id, updates, "import")
                counts["terms_updated"] += 1
            else:
                # Create new term
                relationships = term_data.pop("relationships", [])
                term = GlossaryTerm(**term_data)
                self._terms[term.id] = term
                self._term_index[term_name.lower()] = term.id
                
                # Import relationships
                for rel_data in relationships:
                    rel = TermRelationship(**rel_data)
                    key = (rel.source_term_id, rel.target_term_id)
                    self._relationships[key] = rel
                    counts["relationships_imported"] += 1
                    
                counts["terms_imported"] += 1
                
        logger.info(f"Imported glossary: {counts}")
        return counts
        
    def _dict_to_term(self, data: Dict[str, Any]) -> GlossaryTerm:
        """Convert dictionary to GlossaryTerm"""
        # Handle datetime fields
        for field in ["created_at", "updated_at", "proposed_at", "approved_at"]:
            if field in data and data[field]:
                data[field] = datetime.fromisoformat(data[field])
                
        # Handle enum fields
        if "status" in data:
            data["status"] = TermStatus(data["status"])
            
        # Handle relationships
        relationships = []
        for rel_data in data.get("relationships", []):
            rel_data["relationship_type"] = RelationType(rel_data["relationship_type"])
            rel_data["created_at"] = datetime.fromisoformat(rel_data["created_at"])
            relationships.append(TermRelationship(**rel_data))
        data["relationships"] = relationships
        
        return GlossaryTerm(**data) 