"""
Data Classifier for automated classification and tagging
"""

import re
import asyncio
from typing import Dict, Any, List, Optional, Set, Pattern
from datetime import datetime, timedelta
from collections import defaultdict

from platformq_shared.logging import get_logger
from ..core.config import Settings
from ..core.atlas_client import AtlasClient

logger = get_logger(__name__)


class ClassificationType(str):
    """Standard classification types"""
    PII = "PII"
    PCI = "PCI"
    PHI = "PHI"
    CONFIDENTIAL = "CONFIDENTIAL"
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    RESTRICTED = "RESTRICTED"


class Classifier:
    """Automated data classification engine"""
    
    def __init__(self, settings: Settings, atlas_client: AtlasClient):
        self.settings = settings
        self.atlas = atlas_client
        self.classifiers: Dict[str, Dict[str, Any]] = {}
        self.scan_queue: asyncio.Queue = asyncio.Queue()
        self.scan_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize the classifier"""
        logger.info("Initializing Classifier")
        
        # Load classification rules
        self._load_classification_rules()
        
        # Start scanning task if enabled
        if self.settings.auto_classification_enabled:
            self.scan_task = asyncio.create_task(self._auto_classification_loop())
            
        logger.info("Classifier initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.scan_task:
            self.scan_task.cancel()
            
    def _load_classification_rules(self):
        """Load classification rules and patterns"""
        # PII patterns
        self.classifiers[ClassificationType.PII] = {
            "patterns": {
                "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
                "ssn": re.compile(r'\b\d{3}-\d{2}-\d{4}\b'),
                "phone": re.compile(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'),
                "credit_card": re.compile(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'),
                "ip_address": re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'),
                "date_of_birth": re.compile(r'\b(0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])[- /.](19|20)\d\d\b')
            },
            "column_names": [
                "email", "e_mail", "emailaddress",
                "ssn", "social_security", "socialsecurity",
                "phone", "phonenumber", "telephone", "mobile",
                "creditcard", "credit_card", "cc_number",
                "ip", "ipaddress", "ip_address",
                "dob", "dateofbirth", "birth_date", "birthdate"
            ],
            "attributes": {"type": "personal"}
        }
        
        # PCI patterns
        self.classifiers[ClassificationType.PCI] = {
            "patterns": {
                "credit_card": re.compile(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b'),
                "cvv": re.compile(r'\b\d{3,4}\b'),
                "expiry": re.compile(r'\b(0[1-9]|1[0-2])[\/-]\d{2,4}\b')
            },
            "column_names": [
                "creditcard", "credit_card", "cc_number", "card_number",
                "cvv", "cvc", "security_code",
                "expiry", "expiration", "exp_date"
            ],
            "attributes": {"type": "payment"}
        }
        
        # PHI patterns (healthcare)
        self.classifiers[ClassificationType.PHI] = {
            "patterns": {
                "mrn": re.compile(r'\b[A-Z]{2,3}\d{6,10}\b'),  # Medical Record Number
                "npi": re.compile(r'\b\d{10}\b'),  # National Provider Identifier
                "diagnosis_code": re.compile(r'\b[A-Z]\d{2}\.\d{1,2}\b')  # ICD-10
            },
            "column_names": [
                "mrn", "medical_record", "patient_id",
                "diagnosis", "icd", "icd10", "procedure_code",
                "medication", "prescription", "treatment"
            ],
            "attributes": {"type": "healthcare"}
        }
        
    async def classify_entity(self,
                            entity_guid: str,
                            sample_data: Optional[List[Dict[str, Any]]] = None,
                            force: bool = False) -> Dict[str, Any]:
        """Classify a single entity"""
        logger.info(f"Classifying entity: {entity_guid}")
        
        # Get entity
        entity = await self.atlas.get_entity_by_guid(entity_guid)
        if not entity:
            raise ValueError(f"Entity {entity_guid} not found")
            
        # Check if already classified and not forcing
        if not force and entity.get('classifications'):
            return {
                "entity_guid": entity_guid,
                "existing_classifications": [c['typeName'] for c in entity['classifications']],
                "skipped": True
            }
            
        # Perform classification
        classifications = await self._classify_data(entity, sample_data)
        
        # Apply classifications
        applied = []
        for classification, confidence in classifications.items():
            if confidence >= self.settings.classification_confidence_threshold:
                success = await self.atlas.add_classification(
                    entity_guid,
                    classification,
                    {"confidence": confidence}
                )
                if success:
                    applied.append(classification)
                    
        return {
            "entity_guid": entity_guid,
            "classifications": classifications,
            "applied": applied,
            "threshold": self.settings.classification_confidence_threshold
        }
        
    async def _classify_data(self,
                           entity: Dict[str, Any],
                           sample_data: Optional[List[Dict[str, Any]]]) -> Dict[str, float]:
        """Perform actual classification"""
        classifications = {}
        
        # Check entity name and attributes
        entity_name = entity['attributes'].get('name', '').lower()
        
        # Check against each classifier
        for class_type, classifier in self.classifiers.items():
            confidence = 0.0
            matches = defaultdict(int)
            
            # Check column name patterns
            for pattern in classifier['column_names']:
                if pattern in entity_name:
                    confidence = max(confidence, 0.8)
                    matches['name_match'] += 1
                    
            # If we have sample data, check content
            if sample_data and confidence < 1.0:
                total_rows = len(sample_data)
                pattern_matches = defaultdict(int)
                
                for row in sample_data[:self.settings.classification_sample_size]:
                    for field, value in row.items():
                        if value:
                            str_value = str(value)
                            for pattern_name, pattern in classifier['patterns'].items():
                                if pattern.search(str_value):
                                    pattern_matches[pattern_name] += 1
                                    
                # Calculate confidence based on matches
                if pattern_matches:
                    max_match_rate = max(
                        count / total_rows 
                        for count in pattern_matches.values()
                    )
                    confidence = max(confidence, min(max_match_rate * 1.5, 1.0))
                    
            if confidence > 0:
                classifications[class_type] = confidence
                
        # Add general classifications based on metadata
        if entity['attributes'].get('description'):
            desc_lower = entity['attributes']['description'].lower()
            
            if any(word in desc_lower for word in ['confidential', 'secret', 'private']):
                classifications[ClassificationType.CONFIDENTIAL] = 0.7
            elif any(word in desc_lower for word in ['public', 'open']):
                classifications[ClassificationType.PUBLIC] = 0.7
            else:
                classifications[ClassificationType.INTERNAL] = 0.5
                
        return classifications
        
    async def classify_bulk(self,
                          entity_guids: List[str],
                          sample_data_map: Optional[Dict[str, List[Dict[str, Any]]]] = None) -> List[Dict[str, Any]]:
        """Classify multiple entities"""
        results = []
        
        for guid in entity_guids:
            sample_data = sample_data_map.get(guid) if sample_data_map else None
            try:
                result = await self.classify_entity(guid, sample_data)
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to classify entity {guid}: {e}")
                results.append({
                    "entity_guid": guid,
                    "error": str(e)
                })
                
        return results
        
    async def create_custom_classifier(self,
                                     name: str,
                                     patterns: Optional[Dict[str, str]] = None,
                                     column_names: Optional[List[str]] = None,
                                     rules: Optional[Dict[str, Any]] = None) -> bool:
        """Create a custom classifier"""
        try:
            # Create classification type in Atlas
            classification_def = {
                "classificationDefs": [{
                    "name": name,
                    "serviceType": "platformq",
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "confidence",
                            "typeName": "float",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        },
                        {
                            "name": "matchedPatterns",
                            "typeName": "array<string>",
                            "isOptional": True,
                            "cardinality": "SINGLE"
                        }
                    ]
                }]
            }
            
            await self.atlas.create_typedef(classification_def)
            
            # Add to local classifiers
            self.classifiers[name] = {
                "patterns": {k: re.compile(v) for k, v in (patterns or {}).items()},
                "column_names": column_names or [],
                "rules": rules or {},
                "custom": True
            }
            
            logger.info(f"Created custom classifier: {name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create custom classifier: {e}")
            return False
            
    async def scan_for_sensitive_data(self,
                                    type_names: Optional[List[str]] = None,
                                    limit: int = 100) -> Dict[str, Any]:
        """Scan entities for sensitive data"""
        logger.info("Starting sensitive data scan")
        
        # Get entities to scan
        query = "*"
        if type_names:
            query = f"typeName:({' OR '.join(type_names)})"
            
        offset = 0
        total_scanned = 0
        findings = defaultdict(list)
        
        while True:
            result = await self.atlas.search_entities(
                query=query,
                limit=limit,
                offset=offset
            )
            
            entities = result.get('entities', [])
            if not entities:
                break
                
            # Scan each entity
            for entity in entities:
                # Skip if already has sensitive classifications
                existing_sensitive = [
                    c['typeName'] for c in entity.get('classifications', [])
                    if c['typeName'] in [ClassificationType.PII, ClassificationType.PCI, ClassificationType.PHI]
                ]
                
                if not existing_sensitive:
                    # Add to scan queue
                    await self.scan_queue.put(entity['guid'])
                    
            total_scanned += len(entities)
            offset += limit
            
            # Avoid overwhelming the system
            await asyncio.sleep(0.1)
            
        return {
            "total_scanned": total_scanned,
            "queued_for_classification": self.scan_queue.qsize(),
            "scan_started": datetime.utcnow().isoformat()
        }
        
    async def _auto_classification_loop(self):
        """Background task for auto-classification"""
        while True:
            try:
                # Process scan queue
                if not self.scan_queue.empty():
                    guid = await self.scan_queue.get()
                    try:
                        await self.classify_entity(guid)
                    except Exception as e:
                        logger.error(f"Auto-classification failed for {guid}: {e}")
                        
                else:
                    # Periodic scan
                    await asyncio.sleep(self.settings.classification_scan_interval)
                    
                    # Trigger scan for recent entities
                    await self._scan_recent_entities()
                    
            except Exception as e:
                logger.error(f"Auto-classification loop error: {e}")
                await asyncio.sleep(60)
                
    async def _scan_recent_entities(self):
        """Scan recently created/updated entities"""
        # Get entities created/updated in last hour
        cutoff = datetime.utcnow() - timedelta(hours=1)
        
        # This would need proper query support in Atlas
        # For now, simplified implementation
        result = await self.atlas.search_entities(
            query="*",
            limit=50
        )
        
        for entity in result.get('entities', []):
            # Check if needs classification
            if not entity.get('classifications'):
                await self.scan_queue.put(entity['guid'])
                
    async def get_classification_report(self) -> Dict[str, Any]:
        """Get classification statistics"""
        # Get all classified entities
        classified_counts = defaultdict(int)
        total_entities = 0
        
        # Get counts for each classification
        for class_type in self.classifiers.keys():
            result = await self.atlas.search_entities(
                query="*",
                classification=class_type,
                limit=0  # Just need count
            )
            classified_counts[class_type] = result.get('approximateCount', 0)
            
        # Get total entity count
        total_result = await self.atlas.search_entities(
            query="*",
            limit=0
        )
        total_entities = total_result.get('approximateCount', 0)
        
        return {
            "total_entities": total_entities,
            "classified_entities": sum(classified_counts.values()),
            "classification_coverage": sum(classified_counts.values()) / max(total_entities, 1),
            "by_classification": dict(classified_counts),
            "classifiers": list(self.classifiers.keys()),
            "auto_classification_enabled": self.settings.auto_classification_enabled,
            "scan_queue_size": self.scan_queue.qsize()
        } 