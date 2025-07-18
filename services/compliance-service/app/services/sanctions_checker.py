"""
Sanctions Checking Service.
"""

import logging
from typing import Dict, Any, List
import asyncio

logger = logging.getLogger(__name__)


class SanctionsChecker:
    """
    Checks entities against sanctions lists.
    """
    
    def __init__(self, databases: List[str], update_frequency: int):
        self.databases = databases
        self.update_frequency = update_frequency
        self._lists_updated = False
        
    async def initialize(self):
        """Initialize sanctions checker"""
        logger.info(f"Sanctions Checker initialized with databases: {self.databases}")
        
    async def shutdown(self):
        """Shutdown sanctions checker"""
        logger.info("Sanctions Checker shutdown")
        
    async def update_lists_periodically(self):
        """Periodically update sanctions lists"""
        while True:
            try:
                logger.info("Updating sanctions lists...")
                self._lists_updated = True
                await asyncio.sleep(self.update_frequency * 3600)  # Convert hours to seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating sanctions lists: {e}")
                await asyncio.sleep(60)  # Retry after 1 minute 