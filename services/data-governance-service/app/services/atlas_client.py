"""Apache Atlas Client Stub"""

import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class AtlasClient:
    """Stub client for Apache Atlas integration"""
    
    def __init__(self):
        logger.info("Initializing Atlas client (stub)")
    
    async def create_entity(self, entity_type: str, qualified_name: str, name: str, owner: str, description: Optional[str] = None) -> str:
        """Create entity in Atlas"""
        logger.info(f"Creating Atlas entity: {qualified_name}")
        return f"atlas-guid-{qualified_name}"
    
    async def create_lineage(self, source_guid: str, target_guid: str, process_name: str, process_details: Dict[str, Any]):
        """Create lineage relationship"""
        logger.info(f"Creating lineage: {source_guid} -> {target_guid}")
    
    async def add_classification(self, entity_guid: str, classification_name: str):
        """Add classification to entity"""
        logger.info(f"Adding classification {classification_name} to {entity_guid}") 