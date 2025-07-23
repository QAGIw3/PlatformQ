"""
Apache Atlas client for metadata management
"""

import json
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from enum import Enum

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from platformq_shared.logging import get_logger
from ..core.config import Settings

logger = get_logger(__name__)


class AtlasEntityStatus(str, Enum):
    """Atlas entity status"""
    ACTIVE = "ACTIVE"
    DELETED = "DELETED"


class AtlasTypeCategory(str, Enum):
    """Atlas type categories"""
    PRIMITIVE = "PRIMITIVE"
    OBJECT_ID_TYPE = "OBJECT_ID_TYPE"
    ENUM = "ENUM"
    STRUCT = "STRUCT"
    CLASSIFICATION = "CLASSIFICATION"
    ENTITY = "ENTITY"
    ARRAY = "ARRAY"
    MAP = "MAP"
    RELATIONSHIP = "RELATIONSHIP"


class AtlasClient:
    """Client for interacting with Apache Atlas"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = settings.atlas_url.rstrip('/')
        self.auth = (settings.atlas_username, settings.atlas_password)
        self.client: Optional[httpx.AsyncClient] = None
        self._type_cache: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize the Atlas client"""
        logger.info("Initializing Apache Atlas client")
        
        # Create HTTP client with connection pooling
        self.client = httpx.AsyncClient(
            auth=self.auth,
            timeout=self.settings.atlas_client_timeout,
            limits=httpx.Limits(
                max_connections=self.settings.connection_pool_size,
                max_keepalive_connections=self.settings.connection_pool_size
            )
        )
        
        # Verify connectivity
        await self._verify_connectivity()
        
        # Load type definitions
        await self._load_type_definitions()
        
        # Create custom types if needed
        await self._ensure_custom_types()
        
        logger.info("Apache Atlas client initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.client:
            await self.client.aclose()
            
    async def _verify_connectivity(self):
        """Verify Atlas connectivity"""
        try:
            response = await self.client.get(f"{self.base_url}/api/atlas/v2/types/typedefs")
            response.raise_for_status()
            logger.info("Successfully connected to Apache Atlas")
        except Exception as e:
            logger.error(f"Failed to connect to Atlas: {e}")
            raise
            
    async def _load_type_definitions(self):
        """Load and cache type definitions"""
        try:
            response = await self.client.get(f"{self.base_url}/api/atlas/v2/types/typedefs")
            response.raise_for_status()
            
            typedefs = response.json()
            
            # Cache entity types
            for entity_def in typedefs.get('entityDefs', []):
                self._type_cache[entity_def['name']] = entity_def
                
            # Cache classification types
            for class_def in typedefs.get('classificationDefs', []):
                self._type_cache[class_def['name']] = class_def
                
            logger.info(f"Loaded {len(self._type_cache)} type definitions")
            
        except Exception as e:
            logger.error(f"Failed to load type definitions: {e}")
            
    async def _ensure_custom_types(self):
        """Ensure custom types exist for the platform"""
        custom_types = {
            "entityDefs": [
                {
                    "name": "platformq_dataset",
                    "superTypes": ["DataSet"],
                    "serviceType": "platformq",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "platform",
                            "typeName": "string",
                            "isOptional": False,
                            "cardinality": "SINGLE"
                        },
                        {
                            "name": "dataQualityScore",
                            "typeName": "float",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        },
                        {
                            "name": "lastQualityCheck",
                            "typeName": "date",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        }
                    ]
                },
                {
                    "name": "platformq_pipeline",
                    "superTypes": ["Process"],
                    "serviceType": "platformq",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "pipelineType",
                            "typeName": "string",
                            "isOptional": False,
                            "cardinality": "SINGLE"
                        },
                        {
                            "name": "schedule",
                            "typeName": "string",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        },
                        {
                            "name": "lastRunTime",
                            "typeName": "date",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        }
                    ]
                }
            ],
            "classificationDefs": [
                {
                    "name": "PII",
                    "serviceType": "platformq",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "type",
                            "typeName": "string",
                            "isOptional": False,
                            "cardinality": "SINGLE"
                        }
                    ]
                },
                {
                    "name": "DataQuality",
                    "serviceType": "platformq",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "level",
                            "typeName": "string",
                            "isOptional": False,
                            "cardinality": "SINGLE",
                            "defaultValue": "UNKNOWN"
                        }
                    ]
                }
            ]
        }
        
        # Check if types already exist
        for entity_def in custom_types['entityDefs']:
            if entity_def['name'] not in self._type_cache:
                logger.info(f"Creating custom type: {entity_def['name']}")
                await self.create_typedef(custom_types)
                break
                
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def create_entity(self, entity: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new entity"""
        try:
            # Validate entity type
            type_name = entity.get('typeName')
            if not type_name:
                raise ValueError("Entity must have a typeName")
                
            # Create entity request
            request_body = {
                "entity": entity
            }
            
            response = await self.client.post(
                f"{self.base_url}/api/atlas/v2/entity",
                json=request_body
            )
            response.raise_for_status()
            
            result = response.json()
            guid = result.get('guidAssignments', {}).get('-1')
            
            if guid:
                logger.info(f"Created entity {type_name} with GUID: {guid}")
                # Return the created entity
                return await self.get_entity_by_guid(guid)
            else:
                raise RuntimeError("Failed to get GUID for created entity")
                
        except Exception as e:
            logger.error(f"Failed to create entity: {e}")
            raise
            
    async def get_entity_by_guid(self, guid: str) -> Optional[Dict[str, Any]]:
        """Get entity by GUID"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/entity/guid/{guid}"
            )
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            return response.json().get('entity')
            
        except Exception as e:
            logger.error(f"Failed to get entity {guid}: {e}")
            raise
            
    async def get_entity_by_attribute(self, 
                                    type_name: str,
                                    attr_name: str,
                                    attr_value: str) -> Optional[Dict[str, Any]]:
        """Get entity by unique attribute"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}",
                params={attr_name: attr_value}
            )
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            return response.json().get('entity')
            
        except Exception as e:
            logger.error(f"Failed to get entity by attribute: {e}")
            raise
            
    async def update_entity(self, guid: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Update entity attributes"""
        try:
            # Get current entity
            entity = await self.get_entity_by_guid(guid)
            if not entity:
                raise ValueError(f"Entity {guid} not found")
                
            # Update attributes
            entity['attributes'].update(attributes)
            
            # Send update
            response = await self.client.put(
                f"{self.base_url}/api/atlas/v2/entity/guid/{guid}",
                json={"entity": entity}
            )
            response.raise_for_status()
            
            logger.info(f"Updated entity {guid}")
            return response.json().get('entity')
            
        except Exception as e:
            logger.error(f"Failed to update entity: {e}")
            raise
            
    async def delete_entity(self, guid: str) -> bool:
        """Delete entity by GUID"""
        try:
            response = await self.client.delete(
                f"{self.base_url}/api/atlas/v2/entity/guid/{guid}"
            )
            response.raise_for_status()
            
            logger.info(f"Deleted entity {guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete entity: {e}")
            return False
            
    async def search_entities(self,
                            query: str,
                            type_name: Optional[str] = None,
                            classification: Optional[str] = None,
                            limit: int = 100,
                            offset: int = 0) -> Dict[str, Any]:
        """Search for entities"""
        try:
            params = {
                "query": query,
                "limit": limit,
                "offset": offset
            }
            
            if type_name:
                params["typeName"] = type_name
                
            if classification:
                params["classification"] = classification
                
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/search/basic",
                params=params
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to search entities: {e}")
            raise
            
    async def get_lineage(self,
                         guid: str,
                         direction: str = "BOTH",
                         depth: int = 3) -> Dict[str, Any]:
        """Get lineage for an entity"""
        try:
            params = {
                "direction": direction,
                "depth": depth
            }
            
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/lineage/{guid}",
                params=params
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            raise
            
    async def add_classification(self,
                               guid: str,
                               classification_name: str,
                               attributes: Optional[Dict[str, Any]] = None) -> bool:
        """Add classification to entity"""
        try:
            classification = {
                "typeName": classification_name,
                "attributes": attributes or {}
            }
            
            response = await self.client.post(
                f"{self.base_url}/api/atlas/v2/entity/guid/{guid}/classification",
                json=classification
            )
            response.raise_for_status()
            
            logger.info(f"Added classification {classification_name} to entity {guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add classification: {e}")
            return False
            
    async def remove_classification(self, guid: str, classification_name: str) -> bool:
        """Remove classification from entity"""
        try:
            response = await self.client.delete(
                f"{self.base_url}/api/atlas/v2/entity/guid/{guid}/classification/{classification_name}"
            )
            response.raise_for_status()
            
            logger.info(f"Removed classification {classification_name} from entity {guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove classification: {e}")
            return False
            
    async def create_typedef(self, typedef: Dict[str, Any]) -> Dict[str, Any]:
        """Create type definition"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/atlas/v2/types/typedefs",
                json=typedef
            )
            response.raise_for_status()
            
            # Reload type cache
            await self._load_type_definitions()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to create typedef: {e}")
            raise
            
    async def bulk_create_entities(self, entities: List[Dict[str, Any]]) -> Dict[str, str]:
        """Create multiple entities in bulk"""
        try:
            request_body = {
                "entities": entities
            }
            
            response = await self.client.post(
                f"{self.base_url}/api/atlas/v2/entity/bulk",
                json=request_body
            )
            response.raise_for_status()
            
            result = response.json()
            guid_assignments = result.get('guidAssignments', {})
            
            logger.info(f"Created {len(guid_assignments)} entities in bulk")
            return guid_assignments
            
        except Exception as e:
            logger.error(f"Failed to bulk create entities: {e}")
            raise
            
    async def get_glossary(self) -> List[Dict[str, Any]]:
        """Get all glossaries"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/glossary"
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get glossaries: {e}")
            raise
            
    async def create_glossary_term(self,
                                 glossary_guid: str,
                                 term: Dict[str, Any]) -> Dict[str, Any]:
        """Create a glossary term"""
        try:
            term['anchor'] = {"glossaryGuid": glossary_guid}
            
            response = await self.client.post(
                f"{self.base_url}/api/atlas/v2/glossary/term",
                json=term
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to create glossary term: {e}")
            raise
            
    async def get_metrics(self) -> Dict[str, Any]:
        """Get Atlas metrics"""
        try:
            response = await self.client.get(
                f"{self.base_url}/api/atlas/admin/metrics"
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            return {}
            
    async def get_audit_events(self,
                             start_time: Optional[datetime] = None,
                             end_time: Optional[datetime] = None,
                             limit: int = 100) -> List[Dict[str, Any]]:
        """Get audit events"""
        try:
            params = {"limit": limit}
            
            if start_time:
                params["startTime"] = int(start_time.timestamp() * 1000)
                
            if end_time:
                params["endTime"] = int(end_time.timestamp() * 1000)
                
            response = await self.client.get(
                f"{self.base_url}/api/atlas/v2/audit",
                params=params
            )
            response.raise_for_status()
            
            return response.json().get('events', [])
            
        except Exception as e:
            logger.error(f"Failed to get audit events: {e}")
            return [] 