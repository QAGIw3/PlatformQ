"""
Resource Matcher Service

Finds matching resources based on requirements.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

from platformq_shared.models import ResourceType, ServiceTier
from platformq_shared.database import AsyncSessionLocal
from sqlalchemy import select, and_, or_
from sqlalchemy.orm import AsyncSession

logger = logging.getLogger(__name__)


class ResourceMatcher:
    """Service for matching resource requests with available capacity"""
    
    def __init__(self):
        self._resource_registry = {}  # Mock resource registry
        self._provider_registry = {}  # Mock provider registry
        
    async def find_matches(
        self,
        resource_type: ResourceType,
        amount: int,
        tier: ServiceTier,
        region: str,
        duration: int,
        max_price: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Find matching resources based on requirements
        
        Args:
            resource_type: Type of resource needed
            amount: Amount required
            tier: Service tier required
            region: Target region
            duration: Duration in seconds
            max_price: Maximum price per unit
            
        Returns:
            List of matching resources sorted by score
        """
        try:
            # In production, this would query the resource registry
            # For now, return mock matches
            matches = []
            
            # Mock data based on resource type
            if resource_type == ResourceType.CPU:
                matches = [
                    {
                        "token_id": 1001,
                        "provider": "provider-1",
                        "location": region,
                        "available": amount * 2,
                        "price": 0.05,
                        "tier": tier.value,
                        "specs": {
                            "cores": 16,
                            "frequency": "3.5GHz",
                            "architecture": "x86_64"
                        },
                        "reputation": 95
                    },
                    {
                        "token_id": 1002,
                        "provider": "provider-2",
                        "location": f"{region.split('-')[0]}-west-1",
                        "available": amount * 1.5,
                        "price": 0.04,
                        "tier": tier.value,
                        "specs": {
                            "cores": 32,
                            "frequency": "3.2GHz",
                            "architecture": "x86_64"
                        },
                        "reputation": 88
                    }
                ]
            elif resource_type == ResourceType.GPU:
                matches = [
                    {
                        "token_id": 2001,
                        "provider": "provider-3",
                        "location": region,
                        "available": amount,
                        "price": 0.50,
                        "tier": tier.value,
                        "specs": {
                            "model": "A100",
                            "memory": "80GB",
                            "compute_capability": "8.0"
                        },
                        "reputation": 92
                    }
                ]
            elif resource_type == ResourceType.STORAGE:
                matches = [
                    {
                        "token_id": 3001,
                        "provider": "provider-4",
                        "location": region,
                        "available": amount * 10,
                        "price": 0.001,
                        "tier": tier.value,
                        "specs": {
                            "type": "SSD",
                            "iops": 100000,
                            "throughput": "5GB/s"
                        },
                        "reputation": 90
                    }
                ]
                
            # Filter by price if specified
            if max_price:
                matches = [m for m in matches if m["price"] <= max_price]
                
            # Filter by available capacity
            matches = [m for m in matches if m["available"] >= amount]
            
            # Sort by score (price, location, reputation)
            matches.sort(key=lambda x: self._calculate_match_score(x, region), reverse=True)
            
            return matches
            
        except Exception as e:
            logger.error(f"Error finding matches: {e}")
            return []
            
    def _calculate_match_score(self, match: Dict[str, Any], target_region: str) -> float:
        """Calculate match score based on multiple factors"""
        score = 0.0
        
        # Price score (lower is better)
        price_score = 100 / (1 + match["price"] * 100)
        score += price_score * 0.4
        
        # Location score
        if match["location"] == target_region:
            score += 30
        elif match["location"].split("-")[0] == target_region.split("-")[0]:
            score += 20
        else:
            score += 10
            
        # Reputation score
        score += match["reputation"] * 0.3
        
        return score
        
    async def register_resource(
        self,
        provider: str,
        resource_type: ResourceType,
        amount: int,
        tier: ServiceTier,
        location: str,
        specs: Dict[str, Any],
        price: float
    ) -> int:
        """
        Register a new resource in the matcher
        
        Returns:
            Token ID of the registered resource
        """
        # In production, this would register with the blockchain
        # For now, return mock token ID
        token_id = hash(f"{provider}{resource_type}{datetime.utcnow()}") % 10000
        
        self._resource_registry[token_id] = {
            "provider": provider,
            "resource_type": resource_type,
            "amount": amount,
            "tier": tier,
            "location": location,
            "specs": specs,
            "price": price,
            "registered_at": datetime.utcnow()
        }
        
        return token_id
        
    async def update_availability(
        self,
        token_id: int,
        available_amount: int
    ):
        """Update available amount for a resource"""
        if token_id in self._resource_registry:
            self._resource_registry[token_id]["available"] = available_amount
            
    async def get_provider_resources(
        self,
        provider: str
    ) -> List[Dict[str, Any]]:
        """Get all resources for a provider"""
        resources = []
        for token_id, resource in self._resource_registry.items():
            if resource["provider"] == provider:
                resources.append({
                    "token_id": token_id,
                    **resource
                })
        return resources 