"""
Classification Service

Business logic for classification operations.
"""

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import logging

from app.core.classifier import Classifier, ClassificationType
from app.infrastructure.repositories import EntityRepository
from app.events import EventBus
from app.services.interfaces import ServiceResult

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service layer for classification operations.
    
    Handles classification management, auto-classification, and rule-based classification.
    """
    
    def __init__(
        self,
        classifier: Classifier,
        entity_repository: EntityRepository,
        event_bus: EventBus
    ):
        self.classifier = classifier
        self.entity_repository = entity_repository
        self.event_bus = event_bus
        
    async def create_classification(
        self,
        name: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        parent: Optional[str] = None,
        entity_types: Optional[List[str]] = None,
        attribute_defs: Optional[List[Dict[str, Any]]] = None
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Create a new classification definition.
        
        Args:
            name: Classification name
            display_name: Display name
            description: Classification description
            parent: Parent classification name
            entity_types: Applicable entity types
            attribute_defs: Attribute definitions
            
        Returns:
            ServiceResult with created classification
        """
        try:
            # Validate classification doesn't exist
            existing = await self.classifier.get_classification(name)
            if existing:
                return ServiceResult.failure(
                    error="Classification already exists",
                    details={"name": name}
                )
            
            # Create classification
            classification = await self.classifier.create_classification(
                name=name,
                display_name=display_name,
                description=description,
                parent=parent,
                entity_types=entity_types,
                attribute_defs=attribute_defs
            )
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "ClassificationCreated",
                "classification_name": name,
                "entity_types": entity_types
            })
            
            return ServiceResult.success(classification)
            
        except Exception as e:
            logger.error(f"Failed to create classification: {e}")
            return ServiceResult.failure(
                error="Failed to create classification",
                details={"error": str(e)}
            )
    
    async def assign_classification(
        self,
        entity_guid: str,
        classification_name: str,
        attributes: Optional[Dict[str, Any]] = None,
        propagate: bool = True
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Assign classification to an entity.
        
        Args:
            entity_guid: Entity GUID
            classification_name: Classification to assign
            attributes: Classification attributes
            propagate: Whether to propagate to related entities
            
        Returns:
            ServiceResult with assignment result
        """
        try:
            # Validate entity exists
            entity = await self.entity_repository.find_by_id(entity_guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"entity_guid": entity_guid}
                )
            
            # Validate classification exists
            classification = await self.classifier.get_classification(classification_name)
            if not classification:
                return ServiceResult.failure(
                    error="Classification not found",
                    details={"classification_name": classification_name}
                )
            
            # Check if applicable to entity type
            if classification.get('entity_types'):
                if entity.type_name not in classification['entity_types']:
                    return ServiceResult.failure(
                        error="Classification not applicable to entity type",
                        details={
                            "entity_type": entity.type_name,
                            "applicable_types": classification['entity_types']
                        }
                    )
            
            # Assign classification
            result = await self.classifier.assign_classification(
                entity_guid=entity_guid,
                classification_name=classification_name,
                attributes=attributes,
                propagate=propagate
            )
            
            # Update entity in repository
            entity.add_classification(classification_name)
            await self.entity_repository.save(entity)
            
            # Publish event
            await self.event_bus.publish({
                "event_type": "ClassificationAssigned",
                "entity_guid": entity_guid,
                "classification_name": classification_name,
                "propagated": propagate
            })
            
            return ServiceResult.success(result)
            
        except Exception as e:
            logger.error(f"Failed to assign classification: {e}")
            return ServiceResult.failure(
                error="Failed to assign classification",
                details={"error": str(e)}
            )
    
    async def remove_classification(
        self,
        entity_guid: str,
        classification_name: str
    ) -> ServiceResult[bool]:
        """Remove classification from entity"""
        try:
            # Validate entity exists
            entity = await self.entity_repository.find_by_id(entity_guid)
            if not entity:
                return ServiceResult.failure(
                    error="Entity not found",
                    details={"entity_guid": entity_guid}
                )
            
            # Remove classification
            success = await self.classifier.remove_classification(
                entity_guid=entity_guid,
                classification_name=classification_name
            )
            
            if success:
                # Update entity
                entity.remove_classification(classification_name)
                await self.entity_repository.save(entity)
                
                # Publish event
                await self.event_bus.publish({
                    "event_type": "ClassificationRemoved",
                    "entity_guid": entity_guid,
                    "classification_name": classification_name
                })
            
            return ServiceResult.success(success)
            
        except Exception as e:
            logger.error(f"Failed to remove classification: {e}")
            return ServiceResult.failure(
                error="Failed to remove classification",
                details={"error": str(e)}
            )
    
    async def auto_classify(
        self,
        entity_guid: Optional[str] = None,
        entity_type: Optional[str] = None,
        sample_size: int = 1000,
        classifiers: Optional[List[str]] = None,
        confidence_threshold: float = 0.8,
        dry_run: bool = False
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Automatically classify entities based on content.
        
        Args:
            entity_guid: Specific entity to classify
            entity_type: Type of entities to classify
            sample_size: Sample size for classification
            classifiers: Specific classifiers to run
            confidence_threshold: Minimum confidence for classification
            dry_run: Preview without applying
            
        Returns:
            ServiceResult with classification results
        """
        try:
            # Get entities to classify
            if entity_guid:
                entities = [await self.entity_repository.find_by_id(entity_guid)]
                if not entities[0]:
                    return ServiceResult.failure(
                        error="Entity not found",
                        details={"entity_guid": entity_guid}
                    )
            else:
                entities = await self.entity_repository.find_by_type(
                    entity_type,
                    limit=sample_size
                )
            
            # Default classifiers
            if not classifiers:
                classifiers = ["pii", "financial", "healthcare"]
            
            results = []
            for entity in entities:
                # Get sample data
                sample_data = await self._get_entity_sample_data(entity)
                
                # Run classification
                classifications = await self.classifier.classify_entity(
                    entity_guid=entity.guid,
                    sample_data=sample_data
                )
                
                # Filter by confidence
                detected = {
                    clf: conf 
                    for clf, conf in classifications.get('classifications', {}).items()
                    if conf >= confidence_threshold and clf in classifiers
                }
                
                if detected and not dry_run:
                    # Apply classifications
                    for clf_name in detected.keys():
                        await self.assign_classification(
                            entity_guid=entity.guid,
                            classification_name=clf_name,
                            attributes={"confidence": detected[clf_name]}
                        )
                
                results.append({
                    "entity_guid": entity.guid,
                    "entity_name": entity.name,
                    "detected_classifications": detected,
                    "applied": not dry_run
                })
            
            summary = {
                "total_entities": len(entities),
                "total_classified": len([r for r in results if r['detected_classifications']]),
                "dry_run": dry_run,
                "results": results
            }
            
            return ServiceResult.success(summary)
            
        except Exception as e:
            logger.error(f"Failed to auto-classify: {e}")
            return ServiceResult.failure(
                error="Failed to auto-classify",
                details={"error": str(e)}
            )
    
    async def create_classification_rule(
        self,
        name: str,
        description: Optional[str],
        rule_type: str,
        pattern: Optional[str],
        classification: str,
        confidence: float = 0.9,
        entity_types: Optional[List[str]] = None,
        enabled: bool = True
    ) -> ServiceResult[Dict[str, Any]]:
        """Create a classification rule"""
        try:
            # Validate classification exists
            clf = await self.classifier.get_classification(classification)
            if not clf:
                return ServiceResult.failure(
                    error="Classification not found",
                    details={"classification": classification}
                )
            
            # Create rule
            rule = await self.classifier.create_classification_rule(
                name=name,
                description=description,
                rule_type=rule_type,
                pattern=pattern,
                classification=classification,
                confidence=confidence,
                entity_types=entity_types,
                enabled=enabled
            )
            
            return ServiceResult.success(rule)
            
        except Exception as e:
            logger.error(f"Failed to create classification rule: {e}")
            return ServiceResult.failure(
                error="Failed to create classification rule",
                details={"error": str(e)}
            )
    
    async def scan_for_classifications(
        self,
        entity_type: Optional[str] = None,
        limit: int = 100,
        async_mode: bool = True
    ) -> ServiceResult[Dict[str, Any]]:
        """Scan entities for classifications"""
        try:
            if async_mode:
                # Start async scan
                scan_id = await self.classifier.start_classification_scan(
                    entity_type=entity_type,
                    limit=limit
                )
                
                return ServiceResult.success({
                    "scan_id": scan_id,
                    "status": "scanning",
                    "async": True
                })
            else:
                # Synchronous scan
                results = await self.auto_classify(
                    entity_type=entity_type,
                    sample_size=limit,
                    dry_run=False
                )
                
                return results
                
        except Exception as e:
            logger.error(f"Failed to scan for classifications: {e}")
            return ServiceResult.failure(
                error="Failed to scan for classifications",
                details={"error": str(e)}
            )
    
    async def get_scan_status(
        self,
        scan_id: str
    ) -> ServiceResult[Dict[str, Any]]:
        """Get classification scan status"""
        try:
            status = await self.classifier.get_scan_status(scan_id)
            
            if not status:
                return ServiceResult.failure(
                    error="Scan not found",
                    details={"scan_id": scan_id}
                )
            
            return ServiceResult.success(status)
            
        except Exception as e:
            logger.error(f"Failed to get scan status: {e}")
            return ServiceResult.failure(
                error="Failed to get scan status",
                details={"error": str(e)}
            )
    
    async def list_classifications(
        self,
        include_sub: bool = True,
        entity_type: Optional[str] = None
    ) -> ServiceResult[List[Dict[str, Any]]]:
        """List available classifications"""
        try:
            classifications = await self.classifier.list_classifications()
            
            # Filter by entity type if specified
            if entity_type:
                classifications = [
                    clf for clf in classifications
                    if not clf.get('entity_types') or entity_type in clf.get('entity_types', [])
                ]
            
            # Include sub-classifications if requested
            if include_sub:
                result = []
                for clf in classifications:
                    result.append(clf)
                    # Get sub-classifications
                    sub_clfs = await self._get_sub_classifications(clf['name'])
                    result.extend(sub_clfs)
                classifications = result
            
            return ServiceResult.success(classifications)
            
        except Exception as e:
            logger.error(f"Failed to list classifications: {e}")
            return ServiceResult.failure(
                error="Failed to list classifications",
                details={"error": str(e)}
            )
    
    async def get_classification_stats(
        self
    ) -> ServiceResult[Dict[str, Any]]:
        """Get classification statistics"""
        try:
            stats = await self.classifier.get_classification_report()
            
            return ServiceResult.success(stats)
            
        except Exception as e:
            logger.error(f"Failed to get classification stats: {e}")
            return ServiceResult.failure(
                error="Failed to get classification stats",
                details={"error": str(e)}
            )
    
    async def bulk_assign_classifications(
        self,
        assignments: List[Dict[str, Any]]
    ) -> ServiceResult[Dict[str, Any]]:
        """
        Bulk assign classifications to entities.
        
        Args:
            assignments: List of assignment dicts with entity_guid, classification_name
            
        Returns:
            ServiceResult with bulk operation results
        """
        try:
            results = []
            success_count = 0
            failure_count = 0
            
            for assignment in assignments:
                entity_guid = assignment.get('entity_guid')
                classification_name = assignment.get('classification_name')
                attributes = assignment.get('attributes')
                
                result = await self.assign_classification(
                    entity_guid=entity_guid,
                    classification_name=classification_name,
                    attributes=attributes
                )
                
                if result.success:
                    success_count += 1
                else:
                    failure_count += 1
                
                results.append({
                    "entity_guid": entity_guid,
                    "classification_name": classification_name,
                    "success": result.success,
                    "error": result.error if not result.success else None
                })
            
            summary = {
                "total": len(assignments),
                "success_count": success_count,
                "failure_count": failure_count,
                "results": results
            }
            
            return ServiceResult.success(summary)
            
        except Exception as e:
            logger.error(f"Failed to bulk assign classifications: {e}")
            return ServiceResult.failure(
                error="Failed to bulk assign classifications",
                details={"error": str(e)}
            )
    
    async def _get_entity_sample_data(
        self,
        entity: Any,
        sample_size: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        """Get sample data for classification"""
        # TODO: Implement data sampling based on entity type
        # This would connect to the actual data source
        return entity.attributes.get('sample_data')
    
    async def _get_sub_classifications(
        self,
        parent_name: str
    ) -> List[Dict[str, Any]]:
        """Get sub-classifications of a parent"""
        # TODO: Implement hierarchical classification retrieval
        return [] 