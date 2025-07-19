"""
Base class for compute providers
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any

from app.models import ResourceSpec, ResourceAllocation


class ComputeProvider(ABC):
    """Base class for compute providers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get("name", "unknown")
        self.api_endpoint = config.get("api_endpoint")
        self.credentials = config.get("credentials", {})
        
    @abstractmethod
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check if resources are available"""
        pass
        
    @abstractmethod
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Allocate resources and return access details"""
        pass
        
    @abstractmethod
    async def deallocate_resources(
        self,
        allocation_id: str
    ) -> bool:
        """Deallocate resources"""
        pass
        
    @abstractmethod
    async def get_metrics(
        self,
        allocation_id: str
    ) -> Dict[str, Any]:
        """Get resource metrics"""
        pass 