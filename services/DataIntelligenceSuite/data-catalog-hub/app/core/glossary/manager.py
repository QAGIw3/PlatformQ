"""
Basic Glossary Manager

Handles core glossary operations with Apache Atlas.
"""

import json
from typing import Dict, Any, List, Optional

from platformq_shared.logging import get_logger
from ..config import Settings
from ..atlas_client import AtlasClient
from ..cache_manager import CacheManager
from .models import TermStatus, BusinessTerm

logger = get_logger(__name__)


class GlossaryManager:
    """Manages business glossary operations"""
    
    def __init__(self, settings: Settings, atlas_client: AtlasClient, cache_manager: CacheManager):
        self.settings = settings
        self.atlas_client = atlas_client
        self.cache_manager = cache_manager
        self.glossaries: Dict[str, Dict[str, Any]] = {}
        self.default_glossary_guid: Optional[str] = None
        
    async def initialize(self):
        """Initialize the glossary manager"""
        logger.info("Initializing glossary manager")
        
        # Load existing glossaries
        await self._load_glossaries()
        
        # Ensure default glossary exists
        await self._ensure_default_glossary()
        
        logger.info("Glossary manager initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        pass
        
    async def _load_glossaries(self):
        """Load existing glossaries from Atlas"""
        try:
            glossaries = await self.atlas_client.get_glossary()
            for glossary in glossaries:
                self.glossaries[glossary['guid']] = glossary
                if glossary.get('name') == 'PlatformQ Business Glossary':
                    self.default_glossary_guid = glossary['guid']
                    
            logger.info(f"Loaded {len(self.glossaries)} glossaries")
        except Exception as e:
            logger.error(f"Failed to load glossaries: {e}")
            
    async def _ensure_default_glossary(self):
        """Ensure default glossary exists"""
        if not self.default_glossary_guid:
            try:
                glossary = {
                    "name": "PlatformQ Business Glossary",
                    "shortDescription": "Default business glossary for PlatformQ",
                    "longDescription": "Centralized repository of business terms and definitions"
                }
                
                response = await self.atlas_client.client.post(
                    f"{self.atlas_client.base_url}/api/atlas/v2/glossary",
                    json=glossary
                )
                response.raise_for_status()
                
                created = response.json()
                self.default_glossary_guid = created['guid']
                self.glossaries[created['guid']] = created
                
                logger.info(f"Created default glossary: {created['guid']}")
            except Exception as e:
                logger.error(f"Failed to create default glossary: {e}")
                
    async def create_term(self,
                        name: str,
                        definition: str,
                        glossary_guid: Optional[str] = None,
                        abbreviation: Optional[str] = None,
                        usage: Optional[str] = None,
                        examples: Optional[List[str]] = None,
                        related_terms: Optional[List[str]] = None,
                        categories: Optional[List[str]] = None,
                        status: TermStatus = TermStatus.DRAFT) -> Dict[str, Any]:
        """Create a new glossary term"""
        try:
            glossary_guid = glossary_guid or self.default_glossary_guid
            
            term = {
                "name": name,
                "shortDescription": definition[:100],
                "longDescription": definition,
                "abbreviation": abbreviation,
                "usage": usage,
                "examples": json.dumps(examples) if examples else None,
                "anchor": {"glossaryGuid": glossary_guid},
                "status": status.value
            }
            
            # Remove None values
            term = {k: v for k, v in term.items() if v is not None}
            
            # Create term
            created_term = await self.atlas_client.create_glossary_term(glossary_guid, term)
            
            # Cache the term
            cache_key = f"glossary:term:{created_term['guid']}"
            await self.cache_manager.set(cache_key, created_term, cache_name="catalog_glossary")
            
            # Add related terms if specified
            if related_terms and created_term.get('guid'):
                await self._add_related_terms(created_term['guid'], related_terms)
                
            logger.info(f"Created glossary term: {name}")
            return created_term
            
        except Exception as e:
            logger.error(f"Failed to create term: {e}")
            raise
            
    async def _add_related_terms(self, term_guid: str, related_term_names: List[str]):
        """Add related terms to a glossary term"""
        for related_name in related_term_names:
            try:
                related_term = await self.find_term_by_name(related_name)
                if related_term:
                    # Create relationship
                    await self.atlas_client.client.post(
                        f"{self.atlas_client.base_url}/api/atlas/v2/glossary/terms/{term_guid}/related",
                        json={"termGuid": related_term['guid']}
                    )
            except Exception as e:
                logger.warning(f"Failed to add related term {related_name}: {e}")
                
    async def get_term(self, term_guid: str) -> Optional[Dict[str, Any]]:
        """Get glossary term by GUID"""
        try:
            # Check cache first
            cache_key = f"glossary:term:{term_guid}"
            cached = await self.cache_manager.get(cache_key, cache_name="catalog_glossary")
            if cached:
                return cached
                
            # Get from Atlas
            response = await self.atlas_client.client.get(
                f"{self.atlas_client.base_url}/api/atlas/v2/glossary/term/{term_guid}"
            )
            
            if response.status_code == 404:
                return None
                
            response.raise_for_status()
            term = response.json()
            
            # Cache it
            await self.cache_manager.set(cache_key, term, cache_name="catalog_glossary")
            
            return term
            
        except Exception as e:
            logger.error(f"Failed to get term: {e}")
            return None
            
    async def find_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find glossary term by name"""
        # This would search Atlas glossary
        # Simplified for now
        return None
        
    async def update_term(self,
                        term_guid: str,
                        updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update glossary term"""
        try:
            # Get current term
            term = await self.get_term(term_guid)
            if not term:
                raise ValueError(f"Term {term_guid} not found")
                
            # Apply updates
            for key, value in updates.items():
                if key in ['name', 'shortDescription', 'longDescription', 'abbreviation', 'usage', 'status']:
                    term[key] = value
                    
            # Update in Atlas
            response = await self.atlas_client.client.put(
                f"{self.atlas_client.base_url}/api/atlas/v2/glossary/term/{term_guid}",
                json=term
            )
            response.raise_for_status()
            
            updated = response.json()
            
            # Update cache
            cache_key = f"glossary:term:{term_guid}"
            await self.cache_manager.set(cache_key, updated, cache_name="catalog_glossary")
            
            logger.info(f"Updated glossary term: {term_guid}")
            return updated
            
        except Exception as e:
            logger.error(f"Failed to update term: {e}")
            raise
            
    async def delete_term(self, term_guid: str) -> bool:
        """Delete glossary term"""
        try:
            response = await self.atlas_client.client.delete(
                f"{self.atlas_client.base_url}/api/atlas/v2/glossary/term/{term_guid}"
            )
            response.raise_for_status()
            
            # Remove from cache
            cache_key = f"glossary:term:{term_guid}"
            await self.cache_manager.delete(cache_key, cache_name="catalog_glossary")
            
            logger.info(f"Deleted glossary term: {term_guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete term: {e}")
            return False
            
    async def list_terms(self,
                       glossary_guid: Optional[str] = None,
                       status: Optional[TermStatus] = None,
                       limit: int = 100,
                       offset: int = 0) -> List[Dict[str, Any]]:
        """List glossary terms"""
        try:
            glossary_guid = glossary_guid or self.default_glossary_guid
            
            response = await self.atlas_client.client.get(
                f"{self.atlas_client.base_url}/api/atlas/v2/glossary/{glossary_guid}/terms",
                params={"limit": limit, "offset": offset}
            )
            response.raise_for_status()
            
            terms = response.json()
            
            # Filter by status if specified
            if status:
                terms = [t for t in terms if t.get('status') == status.value]
                
            return terms
            
        except Exception as e:
            logger.error(f"Failed to list terms: {e}")
            return []
            
    async def assign_term_to_entity(self,
                                  term_guid: str,
                                  entity_guid: str) -> bool:
        """Assign glossary term to entity"""
        try:
            # Create term assignment
            assignment = {
                "guid": entity_guid,
                "relationshipAttributes": {
                    "meanings": [{"guid": term_guid}]
                }
            }
            
            response = await self.atlas_client.client.post(
                f"{self.atlas_client.base_url}/api/atlas/v2/entity/guid/{entity_guid}/terms",
                json=[{"termGuid": term_guid}]
            )
            response.raise_for_status()
            
            logger.info(f"Assigned term {term_guid} to entity {entity_guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to assign term: {e}")
            return False
            
    async def remove_term_from_entity(self,
                                    term_guid: str,
                                    entity_guid: str) -> bool:
        """Remove glossary term from entity"""
        try:
            response = await self.atlas_client.client.delete(
                f"{self.atlas_client.base_url}/api/atlas/v2/entity/guid/{entity_guid}/terms/{term_guid}"
            )
            response.raise_for_status()
            
            logger.info(f"Removed term {term_guid} from entity {entity_guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove term: {e}")
            return False
            
    async def get_assigned_entities(self, term_guid: str) -> List[Dict[str, Any]]:
        """Get all entities assigned to a term"""
        try:
            response = await self.atlas_client.client.get(
                f"{self.atlas_client.base_url}/api/atlas/v2/glossary/term/{term_guid}/assignedEntities"
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get assigned entities: {e}")
            return [] 