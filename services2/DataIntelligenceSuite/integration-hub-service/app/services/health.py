"""
Health checking service
"""

from typing import Dict, List, Any
from datetime import datetime


class HealthChecker:
    """Service health checker"""
    
    def __init__(self, cache_manager, event_bus):
        self.cache = cache_manager
        self.event_bus = event_bus
        
    async def get_status(self) -> Dict[str, Any]:
        """Get current health status"""
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "2.0.0"
        }
        
    async def check_all(self) -> List[Dict[str, Any]]:
        """Check all dependencies"""
        checks = []
        
        # Check cache
        try:
            await self.cache.ping()
            checks.append({"name": "cache", "status": "healthy"})
        except Exception as e:
            checks.append({"name": "cache", "status": "unhealthy", "error": str(e)})
            
        # Check event bus
        try:
            await self.event_bus.ping()
            checks.append({"name": "event_bus", "status": "healthy"})
        except Exception as e:
            checks.append({"name": "event_bus", "status": "unhealthy", "error": str(e)})
            
        return checks
