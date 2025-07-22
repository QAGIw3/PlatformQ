"""
Glossary Repository Implementation

Handles glossary and business term persistence.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging
import uuid

from app.core.glossary.manager import GlossaryManager
from app.core.glossary.ai_enhancements import AIGlossaryEnhancements
from app.core.glossary.models import TermStatus, TermCategory
from app.services.storage import IgniteCacheAdapter

logger = logging.getLogger(__name__)


class GlossaryRepository:
    """
    Repository for glossary management.
    
    Handles glossary, term, and category persistence.
    """
    
    def __init__(
        self,
        glossary_manager: GlossaryManager,
        ai_enhancements: AIGlossaryEnhancements,
        cache_manager: IgniteCacheAdapter
    ):
        self.glossary_manager = glossary_manager
        self.ai_enhancements = ai_enhancements
        self.cache_manager = cache_manager
        self.cache_prefix = "glossary"
        
    async def create_glossary(
        self,
        name: str,
        short_description: str,
        long_description: Optional[str] = None,
        language: str = "en",
        usage: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a new glossary"""
        try:
            # Create through manager
            glossary = await self.glossary_manager.create_glossary(
                name=name,
                short_description=short_description,
                long_description=long_description,
                language=language,
                usage=usage
            )
            
            # Cache it
            cache_key = f"{self.cache_prefix}:{glossary['guid']}"
            await self.cache_manager.set(cache_key, glossary, ttl=3600)
            
            # Also cache by name
            name_key = f"{self.cache_prefix}:name:{name}"
            await self.cache_manager.set(name_key, glossary['guid'], ttl=3600)
            
            return glossary
            
        except Exception as e:
            logger.error(f"Failed to create glossary: {e}")
            raise
            
    async def get_glossary(self, glossary_guid: str) -> Optional[Dict[str, Any]]:
        """Get glossary by GUID"""
        try:
            # Check cache
            cache_key = f"{self.cache_prefix}:{glossary_guid}"
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
                
            # Get from manager
            glossary = await self.glossary_manager.get_glossary(glossary_guid)
            
            if glossary:
                # Cache it
                await self.cache_manager.set(cache_key, glossary, ttl=3600)
                
            return glossary
            
        except Exception as e:
            logger.error(f"Failed to get glossary: {e}")
            raise
            
    async def find_glossary_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find glossary by name"""
        try:
            # Check cache
            name_key = f"{self.cache_prefix}:name:{name}"
            glossary_guid = await self.cache_manager.get(name_key)
            
            if glossary_guid:
                return await self.get_glossary(glossary_guid)
                
            # Search in Atlas
            glossaries = await self.list_glossaries()
            for glossary in glossaries:
                if glossary.get('name') == name:
                    # Cache the mapping
                    await self.cache_manager.set(
                        name_key,
                        glossary['guid'],
                        ttl=3600
                    )
                    return glossary
                    
            return None
            
        except Exception as e:
            logger.error(f"Failed to find glossary by name: {e}")
            raise
            
    async def list_glossaries(self) -> List[Dict[str, Any]]:
        """List all glossaries"""
        try:
            return await self.glossary_manager.list_glossaries()
            
        except Exception as e:
            logger.error(f"Failed to list glossaries: {e}")
            raise
            
    async def create_term(
        self,
        name: str,
        definition: str,
        glossary_guid: Optional[str] = None,
        abbreviation: Optional[str] = None,
        usage: Optional[str] = None,
        examples: Optional[List[str]] = None,
        related_terms: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        status: TermStatus = TermStatus.DRAFT
    ) -> Dict[str, Any]:
        """Create a new term"""
        try:
            # Create through manager
            term = await self.glossary_manager.create_term(
                name=name,
                definition=definition,
                glossary_guid=glossary_guid,
                abbreviation=abbreviation,
                usage=usage,
                examples=examples,
                related_terms=related_terms,
                categories=categories,
                status=status
            )
            
            # Cache it
            cache_key = f"{self.cache_prefix}:term:{term['guid']}"
            await self.cache_manager.set(cache_key, term, ttl=3600)
            
            # Also cache by name
            name_key = f"{self.cache_prefix}:term:name:{name}"
            await self.cache_manager.set(name_key, term['guid'], ttl=3600)
            
            return term
            
        except Exception as e:
            logger.error(f"Failed to create term: {e}")
            raise
            
    async def get_term(self, term_guid: str) -> Optional[Dict[str, Any]]:
        """Get term by GUID"""
        try:
            # Check cache
            cache_key = f"{self.cache_prefix}:term:{term_guid}"
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
                
            # Get from manager
            term = await self.glossary_manager.get_term(term_guid)
            
            if term:
                # Cache it
                await self.cache_manager.set(cache_key, term, ttl=3600)
                
            return term
            
        except Exception as e:
            logger.error(f"Failed to get term: {e}")
            raise
            
    async def find_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find term by name"""
        try:
            # Check cache
            name_key = f"{self.cache_prefix}:term:name:{name}"
            term_guid = await self.cache_manager.get(name_key)
            
            if term_guid:
                return await self.get_term(term_guid)
                
            # Search in manager
            term = await self.glossary_manager.find_term_by_name(name)
            
            if term:
                # Cache the mapping
                await self.cache_manager.set(
                    name_key,
                    term['guid'],
                    ttl=3600
                )
                
            return term
            
        except Exception as e:
            logger.error(f"Failed to find term by name: {e}")
            raise
            
    async def update_term(
        self,
        term_guid: str,
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update an existing term"""
        try:
            # Update through manager
            updated_term = await self.glossary_manager.update_term(
                term_guid=term_guid,
                updates=updates
            )
            
            # Invalidate cache
            cache_key = f"{self.cache_prefix}:term:{term_guid}"
            await self.cache_manager.delete(cache_key)
            
            # Re-cache
            await self.cache_manager.set(cache_key, updated_term, ttl=3600)
            
            return updated_term
            
        except Exception as e:
            logger.error(f"Failed to update term: {e}")
            raise
            
    async def list_terms(
        self,
        glossary_guid: str,
        status: Optional[TermStatus] = None,
        category_guid: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """List terms in a glossary"""
        try:
            # Get all terms
            all_terms = await self.glossary_manager.list_terms(
                glossary_guid=glossary_guid,
                status=status,
                limit=limit + offset  # Get enough for pagination
            )
            
            # Filter by category if specified
            if category_guid:
                all_terms = [
                    term for term in all_terms
                    if category_guid in term.get('categories', [])
                ]
                
            # Paginate
            total = len(all_terms)
            paginated_terms = all_terms[offset:offset + limit]
            
            return paginated_terms, total
            
        except Exception as e:
            logger.error(f"Failed to list terms: {e}")
            raise
            
    async def assign_term_to_entities(
        self,
        term_guid: str,
        entity_guids: List[str],
        semantic_assignment: bool = False
    ) -> Dict[str, Any]:
        """Assign term to entities"""
        try:
            results = []
            
            for entity_guid in entity_guids:
                success = await self.glossary_manager.assign_term_to_entity(
                    term_guid=term_guid,
                    entity_guid=entity_guid
                )
                results.append({
                    "entity_guid": entity_guid,
                    "success": success
                })
                
            return {
                "term_guid": term_guid,
                "assignments": results,
                "success_count": len([r for r in results if r['success']])
            }
            
        except Exception as e:
            logger.error(f"Failed to assign term to entities: {e}")
            raise
            
    async def remove_term_from_entity(
        self,
        term_guid: str,
        entity_guid: str
    ) -> bool:
        """Remove term from entity"""
        try:
            return await self.glossary_manager.remove_term_from_entity(
                term_guid=term_guid,
                entity_guid=entity_guid
            )
            
        except Exception as e:
            logger.error(f"Failed to remove term from entity: {e}")
            raise
            
    async def search_terms(
        self,
        query: str,
        glossary_guid: Optional[str] = None,
        status: Optional[TermStatus] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Search for terms"""
        try:
            # Simple search implementation
            # In real implementation, this would use Atlas search
            all_terms = []
            
            if glossary_guid:
                terms, _ = await self.list_terms(
                    glossary_guid=glossary_guid,
                    status=status,
                    limit=1000
                )
                all_terms.extend(terms)
            else:
                # Get terms from all glossaries
                glossaries = await self.list_glossaries()
                for glossary in glossaries:
                    terms, _ = await self.list_terms(
                        glossary_guid=glossary['guid'],
                        status=status,
                        limit=100
                    )
                    all_terms.extend(terms)
                    
            # Filter by query
            query_lower = query.lower()
            matching_terms = [
                term for term in all_terms
                if query_lower in term.get('name', '').lower() or
                   query_lower in term.get('definition', '').lower()
            ]
            
            # Limit results
            return matching_terms[:limit]
            
        except Exception as e:
            logger.error(f"Failed to search terms: {e}")
            raise
            
    async def create_category(
        self,
        name: str,
        glossary_guid: str,
        short_description: str,
        long_description: Optional[str] = None,
        parent_category_guid: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a glossary category"""
        try:
            # Create category (simplified - in real implementation would use Atlas)
            category = {
                "guid": str(uuid.uuid4()),
                "name": name,
                "glossary_guid": glossary_guid,
                "short_description": short_description,
                "long_description": long_description,
                "parent_category_guid": parent_category_guid,
                "created_date": datetime.utcnow().isoformat()
            }
            
            # Cache it
            cache_key = f"{self.cache_prefix}:category:{category['guid']}"
            await self.cache_manager.set(cache_key, category, ttl=None)
            
            return category
            
        except Exception as e:
            logger.error(f"Failed to create category: {e}")
            raise
            
    # AI-Enhanced Methods
    
    async def suggest_business_terms(
        self,
        technical_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """AI-powered term suggestions"""
        try:
            return await self.ai_enhancements.suggest_business_terms(
                technical_name=technical_name,
                context=context
            )
            
        except Exception as e:
            logger.error(f"Failed to suggest business terms: {e}")
            raise
            
    async def create_automatic_mappings(
        self,
        dataset_guid: str,
        approval_required: bool = True
    ) -> List[Dict[str, Any]]:
        """Create automatic term mappings"""
        try:
            mappings = await self.ai_enhancements.create_automatic_mappings(
                dataset_guid=dataset_guid,
                approval_required=approval_required
            )
            
            # Convert to dict format
            return [
                {
                    "term_id": m.term_id,
                    "asset_id": m.asset_id,
                    "confidence": m.confidence,
                    "mapping_type": m.mapping_type,
                    "approved": m.approved
                }
                for m in mappings
            ]
            
        except Exception as e:
            logger.error(f"Failed to create automatic mappings: {e}")
            raise
            
    async def analyze_term_usage(
        self,
        term_guid: str,
        time_range_days: int = 30
    ) -> Dict[str, Any]:
        """Analyze term usage patterns"""
        try:
            return await self.ai_enhancements.analyze_term_usage(
                term_guid=term_guid,
                time_range_days=time_range_days
            )
            
        except Exception as e:
            logger.error(f"Failed to analyze term usage: {e}")
            raise
            
    async def recommend_new_terms(
        self,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Recommend new terms based on usage"""
        try:
            return await self.ai_enhancements.recommend_new_terms(
                limit=limit
            )
            
        except Exception as e:
            logger.error(f"Failed to recommend new terms: {e}")
            raise
            
    async def validate_term_mappings(
        self,
        term_guid: Optional[str] = None,
        dataset_guid: Optional[str] = None
    ) -> Dict[str, Any]:
        """Validate term mappings"""
        try:
            # Simple validation (in real implementation would be more complex)
            issues = []
            
            if term_guid:
                term = await self.get_term(term_guid)
                if not term:
                    issues.append({
                        "type": "error",
                        "message": f"Term {term_guid} not found"
                    })
                    
            return {
                "valid": len(issues) == 0,
                "issues": issues,
                "checked_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to validate term mappings: {e}")
            raise
            
    async def import_glossary(
        self,
        file_path: str,
        format: str = "csv",
        glossary_guid: Optional[str] = None
    ) -> Dict[str, Any]:
        """Import glossary from file"""
        try:
            # Simplified implementation
            # In real implementation would parse file and create terms
            return {
                "imported_count": 0,
                "skipped_count": 0,
                "errors": [],
                "file_path": file_path,
                "format": format
            }
            
        except Exception as e:
            logger.error(f"Failed to import glossary: {e}")
            raise
            
    async def export_glossary(
        self,
        glossary_guid: str,
        format: str = "json",
        include_relationships: bool = True
    ) -> Dict[str, Any]:
        """Export glossary to file"""
        try:
            # Get glossary
            glossary = await self.get_glossary(glossary_guid)
            if not glossary:
                raise ValueError(f"Glossary {glossary_guid} not found")
                
            # Get terms
            terms, _ = await self.list_terms(glossary_guid, limit=10000)
            
            # Prepare export data
            export_data = {
                "glossary": glossary,
                "terms": terms,
                "export_date": datetime.utcnow().isoformat(),
                "format": format
            }
            
            # In real implementation would write to file
            file_path = f"/tmp/glossary_export_{glossary_guid}.{format}"
            
            return {
                "file_path": file_path,
                "term_count": len(terms),
                "format": format,
                "include_relationships": include_relationships
            }
            
        except Exception as e:
            logger.error(f"Failed to export glossary: {e}")
            raise 