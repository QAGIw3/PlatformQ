"""
Business Glossary Manager
"""

import json
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from enum import Enum

from platformq_shared.logging import get_logger
from ..core.config import Settings
from ..core.atlas_client import AtlasClient
from ..core.cache_manager import CacheManager

logger = get_logger(__name__)


class TermStatus(str, Enum):
    """Glossary term status"""
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    OBSOLETE = "OBSOLETE"


class GlossaryManager:
    """Manages business glossary terms and definitions"""
    
    def __init__(self, settings: Settings, atlas_client: AtlasClient, cache_manager: CacheManager):
        self.settings = settings
        self.atlas = atlas_client
        self.cache = cache_manager
        self.glossaries: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize the glossary manager"""
        logger.info("Initializing Glossary Manager")
        
        # Load existing glossaries
        await self._load_glossaries()
        
        # Ensure default glossary exists
        await self._ensure_default_glossary()
        
        logger.info("Glossary Manager initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        pass
        
    async def _load_glossaries(self):
        """Load existing glossaries from Atlas"""
        try:
            glossaries = await self.atlas.get_glossary()
            
            for glossary in glossaries:
                self.glossaries[glossary['guid']] = glossary
                
            logger.info(f"Loaded {len(self.glossaries)} glossaries")
            
        except Exception as e:
            logger.error(f"Failed to load glossaries: {e}")
            
    async def _ensure_default_glossary(self):
        """Ensure default glossary exists"""
        if not self.glossaries:
            # Create default glossary
            default_glossary = {
                "name": "PlatformQ Business Glossary",
                "shortDescription": "Default business glossary for PlatformQ",
                "longDescription": "Central repository for business terms and definitions used across the platform",
                "language": self.settings.glossary_default_language,
                "usage": "Used for standardizing business terminology"
            }
            
            try:
                response = await self.atlas.client.post(
                    f"{self.atlas.base_url}/api/atlas/v2/glossary",
                    json=default_glossary
                )
                response.raise_for_status()
                
                created = response.json()
                self.glossaries[created['guid']] = created
                logger.info("Created default glossary")
                
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
        logger.info(f"Creating glossary term: {name}")
        
        # Use default glossary if not specified
        if not glossary_guid:
            glossary_guid = list(self.glossaries.keys())[0]
            
        # Build term object
        term = {
            "name": name,
            "shortDescription": definition[:100] + "..." if len(definition) > 100 else definition,
            "longDescription": definition,
            "abbreviation": abbreviation,
            "usage": usage,
            "examples": examples or [],
            "anchor": {"glossaryGuid": glossary_guid},
            "status": status.value,
            "categories": categories or [],
            "language": self.settings.glossary_default_language
        }
        
        # Check if approval required
        if self.settings.glossary_approval_required and status == TermStatus.ACTIVE:
            term["status"] = TermStatus.DRAFT.value
            term["additionalAttributes"] = {
                "pendingApproval": True,
                "requestedStatus": TermStatus.ACTIVE.value
            }
            
        # Create term
        created = await self.atlas.create_glossary_term(glossary_guid, term)
        
        # Add relationships to related terms
        if related_terms:
            await self._add_related_terms(created['guid'], related_terms)
            
        # Clear cache
        await self.cache.delete(f"glossary:terms:{glossary_guid}")
        
        return created
        
    async def _add_related_terms(self, term_guid: str, related_term_names: List[str]):
        """Add relationships to related terms"""
        for related_name in related_term_names:
            # Find related term
            related_term = await self.find_term_by_name(related_name)
            if related_term:
                # Create relationship
                try:
                    await self.atlas.client.post(
                        f"{self.atlas.base_url}/api/atlas/v2/glossary/terms/{term_guid}/related",
                        json={"termGuid": related_term['guid']}
                    )
                except Exception as e:
                    logger.error(f"Failed to add related term {related_name}: {e}")
                    
    async def get_term(self, term_guid: str) -> Optional[Dict[str, Any]]:
        """Get glossary term by GUID"""
        # Check cache
        cache_key = f"glossary:term:{term_guid}"
        cached = await self.cache.get(cache_key)
        if cached:
            return cached
            
        try:
            response = await self.atlas.client.get(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/term/{term_guid}"
            )
            response.raise_for_status()
            
            term = response.json()
            
            # Cache result
            await self.cache.set(cache_key, term, ttl=self.settings.cache_ttl)
            
            return term
            
        except Exception as e:
            logger.error(f"Failed to get term {term_guid}: {e}")
            return None
            
    async def find_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find glossary term by name"""
        # Search all glossaries
        for glossary_guid in self.glossaries:
            terms = await self.list_terms(glossary_guid)
            for term in terms:
                if term['name'].lower() == name.lower():
                    return term
                    
        return None
        
    async def update_term(self,
                        term_guid: str,
                        updates: Dict[str, Any]) -> Dict[str, Any]:
        """Update glossary term"""
        # Get current term
        term = await self.get_term(term_guid)
        if not term:
            raise ValueError(f"Term {term_guid} not found")
            
        # Apply updates
        for key, value in updates.items():
            if key in ['name', 'shortDescription', 'longDescription', 'abbreviation', 'usage', 'status']:
                term[key] = value
                
        # Update term
        try:
            response = await self.atlas.client.put(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/term/{term_guid}",
                json=term
            )
            response.raise_for_status()
            
            updated = response.json()
            
            # Clear cache
            await self.cache.delete(f"glossary:term:{term_guid}")
            
            return updated
            
        except Exception as e:
            logger.error(f"Failed to update term: {e}")
            raise
            
    async def delete_term(self, term_guid: str) -> bool:
        """Delete glossary term"""
        try:
            response = await self.atlas.client.delete(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/term/{term_guid}"
            )
            response.raise_for_status()
            
            # Clear cache
            await self.cache.delete(f"glossary:term:{term_guid}")
            
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
        # Check cache
        cache_key = f"glossary:terms:{glossary_guid or 'all'}:{status or 'all'}"
        cached = await self.cache.get(cache_key)
        if cached:
            # Apply pagination to cached results
            return cached[offset:offset + limit]
            
        terms = []
        
        if glossary_guid:
            # Get terms for specific glossary
            try:
                response = await self.atlas.client.get(
                    f"{self.atlas.base_url}/api/atlas/v2/glossary/{glossary_guid}/terms",
                    params={"limit": 1000}  # Get all for caching
                )
                response.raise_for_status()
                terms = response.json()
            except Exception as e:
                logger.error(f"Failed to list terms: {e}")
        else:
            # Get terms from all glossaries
            for gid in self.glossaries:
                glossary_terms = await self.list_terms(gid)
                terms.extend(glossary_terms)
                
        # Filter by status if specified
        if status:
            terms = [t for t in terms if t.get('status') == status.value]
            
        # Cache full results
        await self.cache.set(cache_key, terms, ttl=self.settings.cache_ttl)
        
        # Return paginated results
        return terms[offset:offset + limit]
        
    async def assign_term_to_entity(self,
                                  term_guid: str,
                                  entity_guid: str) -> bool:
        """Assign glossary term to an entity"""
        try:
            # Create assignment
            response = await self.atlas.client.post(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/terms/{term_guid}/assignedEntities",
                json=[{"guid": entity_guid}]
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
        """Remove glossary term from an entity"""
        try:
            response = await self.atlas.client.delete(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/terms/{term_guid}/assignedEntities",
                json=[{"guid": entity_guid}]
            )
            response.raise_for_status()
            
            logger.info(f"Removed term {term_guid} from entity {entity_guid}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove term: {e}")
            return False
            
    async def get_assigned_entities(self, term_guid: str) -> List[Dict[str, Any]]:
        """Get entities assigned to a term"""
        try:
            response = await self.atlas.client.get(
                f"{self.atlas.base_url}/api/atlas/v2/glossary/terms/{term_guid}/assignedEntities"
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get assigned entities: {e}")
            return []
            
    async def approve_term(self, term_guid: str, approver: str) -> Dict[str, Any]:
        """Approve a glossary term"""
        # Get term
        term = await self.get_term(term_guid)
        if not term:
            raise ValueError(f"Term {term_guid} not found")
            
        # Check if pending approval
        if not term.get('additionalAttributes', {}).get('pendingApproval'):
            raise ValueError("Term is not pending approval")
            
        # Update status
        requested_status = term['additionalAttributes'].get('requestedStatus', TermStatus.ACTIVE.value)
        term['status'] = requested_status
        term['additionalAttributes']['approvedBy'] = approver
        term['additionalAttributes']['approvedDate'] = datetime.utcnow().isoformat()
        term['additionalAttributes']['pendingApproval'] = False
        
        # Update term
        return await self.update_term(term_guid, {
            'status': requested_status,
            'additionalAttributes': term['additionalAttributes']
        })
        
    async def search_terms(self,
                         query: str,
                         glossary_guid: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search glossary terms"""
        all_terms = await self.list_terms(glossary_guid)
        
        # Simple text search - could be enhanced with fuzzy matching
        query_lower = query.lower()
        results = []
        
        for term in all_terms:
            score = 0
            
            # Check name
            if query_lower in term['name'].lower():
                score += 3
                
            # Check definition
            if query_lower in term.get('longDescription', '').lower():
                score += 2
                
            # Check abbreviation
            if term.get('abbreviation') and query_lower in term['abbreviation'].lower():
                score += 2
                
            # Check usage
            if term.get('usage') and query_lower in term['usage'].lower():
                score += 1
                
            if score > 0:
                term['_score'] = score
                results.append(term)
                
        # Sort by score
        results.sort(key=lambda x: x['_score'], reverse=True)
        
        return results
        
    async def export_glossary(self,
                            glossary_guid: Optional[str] = None,
                            format: str = "json") -> Any:
        """Export glossary terms"""
        terms = await self.list_terms(glossary_guid, limit=10000)
        
        if format == "json":
            return terms
        elif format == "csv":
            # Convert to CSV format
            import csv
            import io
            
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=['name', 'definition', 'abbreviation', 'usage', 'status', 'categories']
            )
            writer.writeheader()
            
            for term in terms:
                writer.writerow({
                    'name': term['name'],
                    'definition': term.get('longDescription', ''),
                    'abbreviation': term.get('abbreviation', ''),
                    'usage': term.get('usage', ''),
                    'status': term.get('status', ''),
                    'categories': ','.join(term.get('categories', []))
                })
                
            return output.getvalue()
        else:
            raise ValueError(f"Unsupported export format: {format}")
            
    async def import_glossary(self,
                            data: Any,
                            glossary_guid: Optional[str] = None,
                            format: str = "json") -> Dict[str, Any]:
        """Import glossary terms"""
        if format == "json":
            terms = data if isinstance(data, list) else [data]
        elif format == "csv":
            # Parse CSV
            import csv
            import io
            
            reader = csv.DictReader(io.StringIO(data))
            terms = list(reader)
        else:
            raise ValueError(f"Unsupported import format: {format}")
            
        # Import terms
        imported = 0
        failed = 0
        errors = []
        
        for term_data in terms:
            try:
                await self.create_term(
                    name=term_data['name'],
                    definition=term_data.get('definition', term_data.get('longDescription', '')),
                    glossary_guid=glossary_guid,
                    abbreviation=term_data.get('abbreviation'),
                    usage=term_data.get('usage'),
                    categories=term_data.get('categories', '').split(',') if isinstance(term_data.get('categories'), str) else term_data.get('categories', [])
                )
                imported += 1
            except Exception as e:
                failed += 1
                errors.append({
                    'term': term_data.get('name', 'Unknown'),
                    'error': str(e)
                })
                
        return {
            "imported": imported,
            "failed": failed,
            "errors": errors
        } 