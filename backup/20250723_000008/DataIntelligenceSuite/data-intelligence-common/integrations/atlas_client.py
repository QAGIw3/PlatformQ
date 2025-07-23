"""
Apache Atlas Client Integration

Provides high-level client for Apache Atlas data governance and metadata management.
"""

import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import json

logger = logging.getLogger(__name__)


@dataclass
class AtlasConfig:
    """Configuration for Atlas client"""
    base_url: str = "http://localhost:21000"
    
    # Authentication
    username: str = "admin"
    password: str = "admin"
    
    # Timeouts
    request_timeout: int = 30
    
    # Defaults
    default_type_version: str = "1.0"
    default_classification: Optional[str] = None


@dataclass
class AtlasEntity:
    """Atlas entity representation"""
    guid: Optional[str] = None
    type_name: str = ""
    attributes: Dict[str, Any] = field(default_factory=dict)
    status: str = "ACTIVE"
    proxy_for: Optional[Dict[str, Any]] = None
    relationship_attributes: Dict[str, Any] = field(default_factory=dict)
    classifications: List[Dict[str, Any]] = field(default_factory=list)
    meanings: List[Dict[str, Any]] = field(default_factory=list)
    custom_attributes: Dict[str, Any] = field(default_factory=dict)
    labels: List[str] = field(default_factory=list)
    version: int = 0
    create_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None


@dataclass
class AtlasClassification:
    """Atlas classification (tag)"""
    type_name: str
    attributes: Dict[str, Any] = field(default_factory=dict)
    entity_guid: Optional[str] = None
    entity_status: Optional[str] = None
    propagate: bool = True
    validity_periods: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AtlasLineage:
    """Atlas lineage information"""
    guid: str
    depth: int
    direction: str  # "INPUT" or "OUTPUT" or "BOTH"
    base_entity_guid: str
    lineage_depth: int
    lineage_direction: str
    guid_entity_map: Dict[str, Any] = field(default_factory=dict)
    relations: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class AtlasSearchResult:
    """Atlas search result"""
    query_type: str
    query_text: Optional[str] = None
    type_name: Optional[str] = None
    classification: Optional[str] = None
    entities: List[AtlasEntity] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)
    full_text_result: List[Dict[str, Any]] = field(default_factory=list)
    referred_entities: Dict[str, Any] = field(default_factory=dict)


class AtlasClient:
    """
    High-level client for Apache Atlas operations.
    
    Features:
    - Entity CRUD operations
    - Type definitions management
    - Classification (tagging)
    - Lineage tracking
    - Search capabilities
    - Glossary management
    """
    
    def __init__(self, config: AtlasConfig):
        self.config = config
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(config.username, config.password)
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        
    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> requests.Response:
        """Make HTTP request to Atlas API"""
        url = f"{self.config.base_url}/api/atlas/v2{endpoint}"
        
        kwargs.setdefault("timeout", self.config.request_timeout)
        
        response = self._session.request(method, url, **kwargs)
        response.raise_for_status()
        
        return response
        
    def _get(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """GET request"""
        response = self._request("GET", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _post(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """POST request"""
        response = self._request("POST", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _put(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """PUT request"""
        response = self._request("PUT", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    def _delete(self, endpoint: str, **kwargs) -> Union[Dict, List]:
        """DELETE request"""
        response = self._request("DELETE", endpoint, **kwargs)
        return response.json() if response.text else {}
        
    # Entity operations
    
    def create_entity(
        self,
        entity: Union[AtlasEntity, Dict[str, Any]]
    ) -> AtlasEntity:
        """Create a new entity"""
        if isinstance(entity, AtlasEntity):
            entity_dict = {
                "typeName": entity.type_name,
                "attributes": entity.attributes,
                "status": entity.status,
                "classifications": entity.classifications,
                "labels": entity.labels
            }
        else:
            entity_dict = entity
            
        payload = {"entity": entity_dict}
        
        result = self._post("/entity", json=payload)
        
        created_entity = result.get("entity", {})
        return self._dict_to_entity(created_entity)
        
    def create_entities(
        self,
        entities: List[Union[AtlasEntity, Dict[str, Any]]]
    ) -> List[AtlasEntity]:
        """Create multiple entities"""
        entities_list = []
        
        for entity in entities:
            if isinstance(entity, AtlasEntity):
                entity_dict = {
                    "typeName": entity.type_name,
                    "attributes": entity.attributes,
                    "status": entity.status,
                    "classifications": entity.classifications,
                    "labels": entity.labels
                }
            else:
                entity_dict = entity
            entities_list.append(entity_dict)
            
        payload = {"entities": entities_list}
        
        result = self._post("/entity/bulk", json=payload)
        
        created_entities = []
        for guid in result.get("guidAssignments", {}).values():
            entity = self.get_entity_by_guid(guid)
            if entity:
                created_entities.append(entity)
                
        return created_entities
        
    def get_entity_by_guid(
        self,
        guid: str,
        min_ext_info: bool = False,
        ignore_relationships: bool = False
    ) -> Optional[AtlasEntity]:
        """Get entity by GUID"""
        params = {
            "minExtInfo": min_ext_info,
            "ignoreRelationships": ignore_relationships
        }
        
        try:
            result = self._get(f"/entity/guid/{guid}", params=params)
            entity_dict = result.get("entity", {})
            return self._dict_to_entity(entity_dict)
        except Exception:
            return None
            
    def get_entity_by_attribute(
        self,
        type_name: str,
        attr_name: str,
        attr_value: str,
        min_ext_info: bool = False,
        ignore_relationships: bool = False
    ) -> Optional[AtlasEntity]:
        """Get entity by unique attribute"""
        params = {
            "minExtInfo": min_ext_info,
            "ignoreRelationships": ignore_relationships,
            "attr:qualifiedName": attr_value  # Most common unique attribute
        }
        
        try:
            result = self._get(
                f"/entity/uniqueAttribute/type/{type_name}",
                params=params
            )
            entity_dict = result.get("entity", {})
            return self._dict_to_entity(entity_dict)
        except Exception:
            return None
            
    def update_entity(
        self,
        entity: Union[AtlasEntity, Dict[str, Any]]
    ) -> AtlasEntity:
        """Update an entity"""
        if isinstance(entity, AtlasEntity):
            entity_dict = {
                "guid": entity.guid,
                "typeName": entity.type_name,
                "attributes": entity.attributes,
                "status": entity.status,
                "classifications": entity.classifications,
                "labels": entity.labels
            }
        else:
            entity_dict = entity
            
        payload = {"entity": entity_dict}
        
        result = self._post("/entity", json=payload)
        
        updated_entity = result.get("entity", {})
        return self._dict_to_entity(updated_entity)
        
    def delete_entity_by_guid(self, guid: str) -> bool:
        """Delete entity by GUID"""
        try:
            self._delete(f"/entity/guid/{guid}")
            return True
        except Exception:
            return False
            
    def add_classifications(
        self,
        guid: str,
        classifications: List[Union[AtlasClassification, Dict[str, Any]]]
    ) -> bool:
        """Add classifications to an entity"""
        classifications_list = []
        
        for classification in classifications:
            if isinstance(classification, AtlasClassification):
                class_dict = {
                    "typeName": classification.type_name,
                    "attributes": classification.attributes,
                    "propagate": classification.propagate
                }
            else:
                class_dict = classification
            classifications_list.append(class_dict)
            
        try:
            self._post(
                f"/entity/guid/{guid}/classifications",
                json=classifications_list
            )
            return True
        except Exception:
            return False
            
    def remove_classification(
        self,
        guid: str,
        classification_name: str
    ) -> bool:
        """Remove classification from an entity"""
        try:
            self._delete(
                f"/entity/guid/{guid}/classification/{classification_name}"
            )
            return True
        except Exception:
            return False
            
    # Type operations
    
    def get_type_definition(self, type_name: str) -> Dict[str, Any]:
        """Get type definition"""
        return self._get(f"/types/typedef/name/{type_name}")
        
    def create_type_definitions(
        self,
        entity_defs: Optional[List[Dict[str, Any]]] = None,
        enum_defs: Optional[List[Dict[str, Any]]] = None,
        struct_defs: Optional[List[Dict[str, Any]]] = None,
        classification_defs: Optional[List[Dict[str, Any]]] = None,
        relationship_defs: Optional[List[Dict[str, Any]]] = None,
        business_metadata_defs: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Create type definitions"""
        payload = {
            "entityDefs": entity_defs or [],
            "enumDefs": enum_defs or [],
            "structDefs": struct_defs or [],
            "classificationDefs": classification_defs or [],
            "relationshipDefs": relationship_defs or [],
            "businessMetadataDefs": business_metadata_defs or []
        }
        
        return self._post("/types/typedefs", json=payload)
        
    def update_type_definitions(
        self,
        entity_defs: Optional[List[Dict[str, Any]]] = None,
        enum_defs: Optional[List[Dict[str, Any]]] = None,
        struct_defs: Optional[List[Dict[str, Any]]] = None,
        classification_defs: Optional[List[Dict[str, Any]]] = None,
        relationship_defs: Optional[List[Dict[str, Any]]] = None,
        business_metadata_defs: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Update type definitions"""
        payload = {
            "entityDefs": entity_defs or [],
            "enumDefs": enum_defs or [],
            "structDefs": struct_defs or [],
            "classificationDefs": classification_defs or [],
            "relationshipDefs": relationship_defs or [],
            "businessMetadataDefs": business_metadata_defs or []
        }
        
        return self._put("/types/typedefs", json=payload)
        
    def delete_type_definition(self, type_name: str) -> bool:
        """Delete type definition"""
        try:
            self._delete(f"/types/typedef/name/{type_name}")
            return True
        except Exception:
            return False
            
    # Search operations
    
    def basic_search(
        self,
        query: Optional[str] = None,
        type_name: Optional[str] = None,
        classification: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        sort_by: Optional[str] = None,
        sort_order: Optional[str] = None
    ) -> AtlasSearchResult:
        """Basic search for entities"""
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if query:
            params["query"] = query
        if type_name:
            params["typeName"] = type_name
        if classification:
            params["classification"] = classification
        if sort_by:
            params["sortBy"] = sort_by
        if sort_order:
            params["sortOrder"] = sort_order
            
        result = self._get("/search/basic", params=params)
        
        return self._parse_search_result(result, "basic")
        
    def dsl_search(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0
    ) -> AtlasSearchResult:
        """DSL search for entities"""
        params = {
            "query": query,
            "limit": limit,
            "offset": offset
        }
        
        result = self._get("/search/dsl", params=params)
        
        return self._parse_search_result(result, "dsl")
        
    def full_text_search(
        self,
        query: str,
        limit: int = 100,
        offset: int = 0
    ) -> AtlasSearchResult:
        """Full text search"""
        params = {
            "query": query,
            "limit": limit,
            "offset": offset
        }
        
        result = self._get("/search/fulltext", params=params)
        
        return self._parse_search_result(result, "fulltext")
        
    # Lineage operations
    
    def get_lineage(
        self,
        guid: str,
        direction: str = "BOTH",
        depth: int = 3
    ) -> AtlasLineage:
        """Get lineage for an entity"""
        params = {
            "direction": direction,
            "depth": depth
        }
        
        result = self._get(f"/lineage/{guid}", params=params)
        
        return AtlasLineage(
            guid=guid,
            depth=depth,
            direction=direction,
            base_entity_guid=result.get("baseEntityGuid", guid),
            lineage_depth=result.get("lineageDepth", depth),
            lineage_direction=result.get("lineageDirection", direction),
            guid_entity_map=result.get("guidEntityMap", {}),
            relations=result.get("relations", [])
        )
        
    # Glossary operations
    
    def create_glossary(
        self,
        name: str,
        short_description: Optional[str] = None,
        long_description: Optional[str] = None,
        language: str = "en",
        usage: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a glossary"""
        payload = {
            "name": name,
            "shortDescription": short_description,
            "longDescription": long_description,
            "language": language,
            "usage": usage
        }
        
        return self._post("/glossary", json=payload)
        
    def get_glossary(self, glossary_guid: str) -> Dict[str, Any]:
        """Get glossary by GUID"""
        return self._get(f"/glossary/{glossary_guid}")
        
    def create_glossary_term(
        self,
        glossary_guid: str,
        name: str,
        short_description: Optional[str] = None,
        long_description: Optional[str] = None,
        examples: Optional[List[str]] = None,
        abbreviation: Optional[str] = None,
        usage: Optional[str] = None,
        additional_attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a glossary term"""
        payload = {
            "anchor": {"glossaryGuid": glossary_guid},
            "name": name,
            "shortDescription": short_description,
            "longDescription": long_description,
            "examples": examples,
            "abbreviation": abbreviation,
            "usage": usage
        }
        
        if additional_attributes:
            payload["additionalAttributes"] = additional_attributes
            
        return self._post("/glossary/term", json=payload)
        
    def assign_term_to_entity(
        self,
        term_guid: str,
        entity_guid: str
    ) -> bool:
        """Assign glossary term to an entity"""
        payload = [
            {
                "guid": entity_guid
            }
        ]
        
        try:
            self._post(
                f"/glossary/terms/{term_guid}/assignedEntities",
                json=payload
            )
            return True
        except Exception:
            return False
            
    # Relationship operations
    
    def create_relationship(
        self,
        relationship_type: str,
        end1_guid: str,
        end2_guid: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a relationship between entities"""
        payload = {
            "typeName": relationship_type,
            "end1": {"guid": end1_guid},
            "end2": {"guid": end2_guid},
            "attributes": attributes or {}
        }
        
        return self._post("/relationship", json=payload)
        
    def get_relationship(self, guid: str) -> Dict[str, Any]:
        """Get relationship by GUID"""
        return self._get(f"/relationship/guid/{guid}")
        
    def delete_relationship(self, guid: str) -> bool:
        """Delete relationship by GUID"""
        try:
            self._delete(f"/relationship/guid/{guid}")
            return True
        except Exception:
            return False
            
    # Utility methods
    
    def _dict_to_entity(self, entity_dict: Dict[str, Any]) -> AtlasEntity:
        """Convert dictionary to AtlasEntity"""
        entity = AtlasEntity()
        
        entity.guid = entity_dict.get("guid")
        entity.type_name = entity_dict.get("typeName", "")
        entity.attributes = entity_dict.get("attributes", {})
        entity.status = entity_dict.get("status", "ACTIVE")
        entity.proxy_for = entity_dict.get("proxyFor")
        entity.relationship_attributes = entity_dict.get("relationshipAttributes", {})
        entity.classifications = entity_dict.get("classifications", [])
        entity.meanings = entity_dict.get("meanings", [])
        entity.custom_attributes = entity_dict.get("customAttributes", {})
        entity.labels = entity_dict.get("labels", [])
        entity.version = entity_dict.get("version", 0)
        
        if entity_dict.get("createTime"):
            entity.create_time = datetime.fromtimestamp(
                entity_dict["createTime"] / 1000
            )
        if entity_dict.get("modifiedTime"):
            entity.modified_time = datetime.fromtimestamp(
                entity_dict["modifiedTime"] / 1000
            )
            
        entity.created_by = entity_dict.get("createdBy")
        entity.modified_by = entity_dict.get("modifiedBy")
        
        return entity
        
    def _parse_search_result(
        self,
        result: Dict[str, Any],
        query_type: str
    ) -> AtlasSearchResult:
        """Parse search result"""
        search_result = AtlasSearchResult(query_type=query_type)
        
        search_result.query_text = result.get("queryText")
        search_result.type_name = result.get("typeName")
        search_result.classification = result.get("classification")
        search_result.attributes = result.get("attributes", {})
        search_result.full_text_result = result.get("fullTextResult", [])
        search_result.referred_entities = result.get("referredEntities", {})
        
        # Parse entities
        entities = []
        for entity_dict in result.get("entities", []):
            entities.append(self._dict_to_entity(entity_dict))
        search_result.entities = entities
        
        return search_result
        
    def bulk_import(
        self,
        import_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Bulk import entities, types, and relationships"""
        return self._post("/entity/bulk/import", json=import_data)
        
    def bulk_export(
        self,
        guids: Optional[List[str]] = None,
        type_names: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Bulk export entities"""
        payload = {}
        
        if guids:
            payload["itemsToExport"] = [{"guid": guid} for guid in guids]
        if type_names:
            payload["options"] = {"typeNames": type_names}
            
        return self._post("/entity/bulk/export", json=payload)
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get Atlas metrics"""
        return self._get("/admin/metrics")
        
    def get_audit_events(
        self,
        start_key: Optional[str] = None,
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """Get audit events"""
        params = {"count": count}
        
        if start_key:
            params["startKey"] = start_key
            
        result = self._get("/admin/audit", params=params)
        return result.get("events", []) 