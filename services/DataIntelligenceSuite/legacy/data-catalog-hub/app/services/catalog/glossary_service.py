"""
Glossary Service

Business logic for glossary and business term operations.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.infrastructure.repositories import GlossaryRepository, EntityRepository
from app.core.glossary.models import TermStatus, TermCategory
from app.events import EventBus
from app.services.interfaces import ServiceResult

logger = logging.getLogger(__name__)


class GlossaryService:
    """
    Service layer for glossary operations.
    
    Handles business term management, term-entity mapping, and AI-enhanced suggestions.
    """
    
    def __init__(
        self,
        repository: GlossaryRepository,
        entity_repository: EntityRepository,
        event_bus: EventBus
    ):
        self.repository = repository
        self.entity_repository = entity_repository
        self.event_bus = event_bus
        
    async def create_glossary(
        self,
        name: str,
        short_description: str,
        long_description: Optional[str] = None,
        language: str = "en",
        usage: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new glossary.
        
        Args:
            name: Glossary name
            short_description: Short description
            long_description: Detailed description
            language: Language code
            usage: Usage guidelines
            
        Returns:
            ServiceResult with created glossary
        """
        try:
            # Check if glossary exists
            existing = await self.repository.find_glossary_by_name(name)
            if existing:
                return ServiceResult.failure(
                    error="Glossary already exists",
                    details={"name": name}
                )
            
            # Create glossary
            glossary = await self.repository.create_glossary(
                name=name,
                short_description=short_description,
                long_description=long_description,
                language=language,
                usage=usage
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "GlossaryCreated",
                "glossary_guid": glossary['guid'],
                "name": name
            })
            
            return ServiceResult.success(glossary)
            
        except Exception as e:
            logger.error(f"Failed to create glossary: {e}")
            return ServiceResult.failure(
                error="Failed to create glossary",
                details={"error": str(e)}
            )
    
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
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new glossary term.
        
        Args:
            name: Term name
            definition: Term definition
            glossary_guid: Parent glossary GUID
            abbreviation: Term abbreviation
            usage: Usage guidelines
            examples: Usage examples
            related_terms: Related term names
            categories: Category GUIDs
            status: Term status
            
        Returns:
            ServiceResult with created term
        """
        try:
            # Validate glossary exists
            if glossary_guid:
                glossary = await self.repository.get_glossary(glossary_guid)
                if not glossary:
                    return ServiceResult.failure(
                        error="Glossary not found",
                        details={"glossary_guid": glossary_guid}
                    )
            
            # Check if term exists
            existing = await self.repository.find_term_by_name(name)
            if existing:
                return ServiceResult.failure(
                    error="Term already exists",
                    details={"name": name}
                )
            
            # Create term
            term = await self.repository.create_term(
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
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "TermCreated",
                "term_guid": term['guid'],
                "name": name,
                "status": status.value
            })
            
            return ServiceResult.success(term)
            
        except Exception as e:
            logger.error(f"Failed to create term: {e}")
            return ServiceResult.failure(
                error="Failed to create term",
                details={"error": str(e)}
            )
    
    async def update_term(
        self,
        term_guid: str,
        updates: Dict[str, Any]
    ) -> ServiceResult[Dict[str, Any]]:
        """Update an existing term"""
        try:
            # Get existing term
            term = await self.repository.get_term(term_guid)
            if not term:
                return ServiceResult.failure(
                    error="Term not found",
                    details={"term_guid": term_guid}
                )
            
            # Update term
            updated_term = await self.repository.update_term(
                term_guid=term_guid,
                updates=updates
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "TermUpdated",
                "term_guid": term_guid,
                "updates": list(updates.keys())
            })
            
            return ServiceResult.success(updated_term)
            
        except Exception as e:
            logger.error(f"Failed to update term: {e}")
            return ServiceResult.failure(
                error="Failed to update term",
                details={"error": str(e)}
            )
    
    async def assign_term_to_entities(
        self,
        term_guid: str,
        entity_guids: List[str],
        semantic_assignment: bool = False
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Assign term to entities.
        
        Args:
            term_guid: Term GUID
            entity_guids: List of entity GUIDs
            semantic_assignment: Use AI for semantic assignment
            
        Returns:
            ServiceResult with assignment results
        """
        try:
            # Validate term exists
            term = await self.repository.get_term(term_guid)
            if not term:
                return ServiceResult.failure(
                    error="Term not found",
                    details={"term_guid": term_guid}
                )
            
            # Validate entities exist
            invalid_entities = []
            for entity_guid in entity_guids:
                entity = await self.entity_repository.find_by_id(entity_guid)
                if not entity:
                    invalid_entities.append(entity_guid)
            
            if invalid_entities:
                return ServiceResult.failure(
                    error="Invalid entities",
                    details={"invalid_entity_guids": invalid_entities}
                )
            
            # Assign term
            results = await self.repository.assign_term_to_entities(
                term_guid=term_guid,
                entity_guids=entity_guids,
                semantic_assignment=semantic_assignment
            )
            
            # Update entities
            for entity_guid in entity_guids:
                entity = await self.entity_repository.find_by_id(entity_guid)
                entity.glossary_terms.append(term_guid)
                await self.entity_repository.save(entity)
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "TermAssigned",
                "term_guid": term_guid,
                "entity_guids": entity_guids,
                "count": len(entity_guids)
            })
            
            return ServiceResult.success(results)
            
        except Exception as e:
            logger.error(f"Failed to assign term to entities: {e}")
            return ServiceResult.failure(
                error="Failed to assign term to entities",
                details={"error": str(e)}
            )
    
    async def remove_term_from_entity(
        self,
        term_guid: str,
        entity_guid: str
    ) -> ServiceResult[bool]:
        """Remove term from entity"""
        try:
            success = await self.repository.remove_term_from_entity(
                term_guid=term_guid,
                entity_guid=entity_guid
            )
            
            if success:
                # Update entity
                entity = await self.entity_repository.find_by_id(entity_guid)
                if entity and term_guid in entity.glossary_terms:
                    entity.glossary_terms.remove(term_guid)
                    await self.entity_repository.save(entity)
                
                # Publish event
                await self.event_bus.publish({
                    "event_type": "TermRemoved",
                    "term_guid": term_guid,
                    "entity_guid": entity_guid
                })
            
            return ServiceResult.success(success)
            
        except Exception as e:
            logger.error(f"Failed to remove term from entity: {e}")
            return ServiceResult.failure(
                error="Failed to remove term from entity",
                details={"error": str(e)}
            )
    
    async def approve_term(
        self,
        term_guid: str,
        approver_notes: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Approve a draft term"""
        try:
            # Get term
            term = await self.repository.get_term(term_guid)
            if not term:
                return ServiceResult.failure(
                    error="Term not found",
                    details={"term_guid": term_guid}
                )
            
            # Check status
            if term['status'] != TermStatus.DRAFT.value:
                return ServiceResult.failure(
                    error="Term is not in draft status",
                    details={"current_status": term['status']}
                )
            
            # Update status
            updates = {
                "status": TermStatus.APPROVED.value,
                "approver_notes": approver_notes,
                "approved_date": datetime.utcnow().isoformat()
            }
            
            updated_term = await self.repository.update_term(
                term_guid=term_guid,
                updates=updates
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "TermApproved",
                "term_guid": term_guid,
                "approver_notes": approver_notes
            })
            
            return ServiceResult.success(updated_term)
            
        except Exception as e:
            logger.error(f"Failed to approve term: {e}")
            return ServiceResult.failure(
                error="Failed to approve term",
                details={"error": str(e)}
            )
    
    async def deprecate_term(
        self,
        term_guid: str,
        reason: str,
        replacement_guid: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Deprecate a term"""
        try:
            # Get term
            term = await self.repository.get_term(term_guid)
            if not term:
                return ServiceResult.failure(
                    error="Term not found",
                    details={"term_guid": term_guid}
                )
            
            # Validate replacement if provided
            if replacement_guid:
                replacement = await self.repository.get_term(replacement_guid)
                if not replacement:
                    return ServiceResult.failure(
                        error="Replacement term not found",
                        details={"replacement_guid": replacement_guid}
                    )
            
            # Update status
            updates = {
                "status": TermStatus.DEPRECATED.value,
                "deprecation_reason": reason,
                "replacement_term": replacement_guid,
                "deprecated_date": datetime.utcnow().isoformat()
            }
            
            updated_term = await self.repository.update_term(
                term_guid=term_guid,
                updates=updates
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "TermDeprecated",
                "term_guid": term_guid,
                "reason": reason,
                "replacement_guid": replacement_guid
            })
            
            return ServiceResult.success(updated_term)
            
        except Exception as e:
            logger.error(f"Failed to deprecate term: {e}")
            return ServiceResult.failure(
                error="Failed to deprecate term",
                details={"error": str(e)}
            )
    
    async def search_terms(
        self,
        query: str,
        glossary_guid: Optional[str] = None,
        status: Optional[TermStatus] = None,
        limit: int = 20
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Search for terms"""
        try:
            terms = await self.repository.search_terms(
                query=query,
                glossary_guid=glossary_guid,
                status=status,
                limit=limit
            )
            
            return ServiceResult.success(terms)
            
        except Exception as e:
            logger.error(f"Failed to search terms: {e}")
            return ServiceResult.failure(
                error="Failed to search terms",
                details={"error": str(e)}
            )
    
    async def suggest_business_terms(
        self,
        technical_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        AI-powered suggestion of business terms for technical names.
        
        Args:
            technical_name: Technical field/column name
            context: Additional context (schema, data type, etc.)
            
        Returns:
            ServiceResult with suggested terms
        """
        try:
            suggestions = await self.repository.suggest_business_terms(
                technical_name=technical_name,
                context=context
            )
            
            return ServiceResult.success(suggestions)
            
        except Exception as e:
            logger.error(f"Failed to suggest business terms: {e}")
            return ServiceResult.failure(
                error="Failed to suggest business terms",
                details={"error": str(e)}
            )
    
    async def create_automatic_mappings(
        self,
        dataset_guid: str,
        approval_required: bool = True
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """
        Create automatic term mappings for a dataset.
        
        Args:
            dataset_guid: Dataset entity GUID
            approval_required: Whether mappings require approval
            
        Returns:
            ServiceResult with created mappings
        """
        try:
            # Get dataset
            dataset = await self.entity_repository.find_by_id(dataset_guid)
            if not dataset:
                return ServiceResult.failure(
                    error="Dataset not found",
                    details={"dataset_guid": dataset_guid}
                )
            
            # Create mappings
            mappings = await self.repository.create_automatic_mappings(
                dataset_guid=dataset_guid,
                approval_required=approval_required
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "AutomaticMappingsCreated",
                "dataset_guid": dataset_guid,
                "mapping_count": len(mappings),
                "approval_required": approval_required
            })
            
            return ServiceResult.success(mappings)
            
        except Exception as e:
            logger.error(f"Failed to create automatic mappings: {e}")
            return ServiceResult.failure(
                error="Failed to create automatic mappings",
                details={"error": str(e)}
            )
    
    async def analyze_term_usage(
        self,
        term_guid: str,
        time_range_days: int = 30
    ) -> ServiceResult[Dict[str, Any]]:
        """Analyze term usage patterns"""
        try:
            # Get term
            term = await self.repository.get_term(term_guid)
            if not term:
                return ServiceResult.failure(
                    error="Term not found",
                    details={"term_guid": term_guid}
                )
            
            # Analyze usage
            usage_analysis = await self.repository.analyze_term_usage(
                term_guid=term_guid,
                time_range_days=time_range_days
            )
            
            return ServiceResult.success(usage_analysis)
            
        except Exception as e:
            logger.error(f"Failed to analyze term usage: {e}")
            return ServiceResult.failure(
                error="Failed to analyze term usage",
                details={"error": str(e)}
            )
    
    async def recommend_new_terms(
        self,
        limit: int = 20
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """Recommend new terms based on usage patterns"""
        try:
            recommendations = await self.repository.recommend_new_terms(
                limit=limit
            )
            
            return ServiceResult.success(recommendations)
            
        except Exception as e:
            logger.error(f"Failed to recommend new terms: {e}")
            return ServiceResult.failure(
                error="Failed to recommend new terms",
                details={"error": str(e)}
            )
    
    async def import_glossary(
        self,
        file_path: str,
        format: str = "csv",
        glossary_guid: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Import glossary from file"""
        try:
            # Validate format
            if format not in ["csv", "json", "excel"]:
                return ServiceResult.failure(
                    error="Unsupported format",
                    details={"format": format, "supported": ["csv", "json", "excel"]}
                )
            
            # Import glossary
            result = await self.repository.import_glossary(
                file_path=file_path,
                format=format,
                glossary_guid=glossary_guid
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "GlossaryImported",
                "file_path": file_path,
                "format": format,
                "imported_count": result.get('imported_count', 0)
            })
            
            return ServiceResult.success(result)
            
        except Exception as e:
            logger.error(f"Failed to import glossary: {e}")
            return ServiceResult.failure(
                error="Failed to import glossary",
                details={"error": str(e)}
            )
    
    async def export_glossary(
        self,
        glossary_guid: str,
        format: str = "json",
        include_relationships: bool = True
    ) -> ServiceResult[Dict[str, Any]]:
        """Export glossary to file"""
        try:
            # Get glossary
            glossary = await self.repository.get_glossary(glossary_guid)
            if not glossary:
                return ServiceResult.failure(
                    error="Glossary not found",
                    details={"glossary_guid": glossary_guid}
                )
            
            # Export glossary
            result = await self.repository.export_glossary(
                glossary_guid=glossary_guid,
                format=format,
                include_relationships=include_relationships
            )
            
            return ServiceResult.success(result)
            
        except Exception as e:
            logger.error(f"Failed to export glossary: {e}")
            return ServiceResult.failure(
                error="Failed to export glossary",
                details={"error": str(e)}
            )
    
    async def list_glossaries(self) -> ServiceResult[List[Dict[str, Any]]]:
        """List all glossaries"""
        try:
            glossaries = await self.repository.list_glossaries()
            
            return ServiceResult.success(glossaries)
            
        except Exception as e:
            logger.error(f"Failed to list glossaries: {e}")
            return ServiceResult.failure(
                error="Failed to list glossaries",
                details={"error": str(e)}
            )
    
    async def list_terms(
        self,
        glossary_guid: str,
        status: Optional[TermStatus] = None,
        category_guid: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> ServiceResult[Tuple[List[Dict[str, Any]], int]]:
        """List terms in a glossary"""
        try:
            terms, total = await self.repository.list_terms(
                glossary_guid=glossary_guid,
                status=status,
                category_guid=category_guid,
                limit=limit,
                offset=offset
            )
            
            return ServiceResult.success((terms, total))
            
        except Exception as e:
            logger.error(f"Failed to list terms: {e}")
            return ServiceResult.failure(
                error="Failed to list terms",
                details={"error": str(e)}
            )
    
    async def create_category(
        self,
        name: str,
        glossary_guid: str,
        short_description: str,
        long_description: Optional[str] = None,
        parent_category_guid: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a glossary category"""
        try:
            # Validate glossary exists
            glossary = await self.repository.get_glossary(glossary_guid)
            if not glossary:
                return ServiceResult.failure(
                    error="Glossary not found",
                    details={"glossary_guid": glossary_guid}
                )
            
            # Create category
            category = await self.repository.create_category(
                name=name,
                glossary_guid=glossary_guid,
                short_description=short_description,
                long_description=long_description,
                parent_category_guid=parent_category_guid
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "CategoryCreated",
                "category_guid": category['guid'],
                "name": name,
                "glossary_guid": glossary_guid
            })
            
            return ServiceResult.success(category)
            
        except Exception as e:
            logger.error(f"Failed to create category: {e}")
            return ServiceResult.failure(
                error="Failed to create category",
                details={"error": str(e)}
            )
    
    async def validate_term_mappings(
        self,
        term_guid: Optional[str] = None,
        dataset_guid: Optional[str] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """Validate term mappings for consistency"""
        try:
            validation_result = await self.repository.validate_term_mappings(
                term_guid=term_guid,
                dataset_guid=dataset_guid
            )
            
            return ServiceResult.success(validation_result)
            
        except Exception as e:
            logger.error(f"Failed to validate term mappings: {e}")
            return ServiceResult.failure(
                error="Failed to validate term mappings",
                details={"error": str(e)}
            ) 