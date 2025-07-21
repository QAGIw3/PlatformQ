"""
Market Client for interacting with compute market services
"""
import logging
import httpx
from typing import Dict, List, Optional, Any
from datetime import datetime

from ..config import settings


logger = logging.getLogger(__name__)


class MarketClient:
    """Client for interacting with quantum, AI, and network market services"""
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT)
        
    async def cleanup(self):
        """Cleanup client connections"""
        await self.client.aclose()
    
    # Quantum Market Methods
    async def search_quantum_resources(
        self,
        min_qubit_count: int = None,
        min_coherence_minutes: float = None,
        max_error_rate: float = None
    ) -> List[Dict]:
        """Search available quantum resources"""
        try:
            params = {}
            if min_qubit_count:
                params['min_qubit_count'] = min_qubit_count
            if min_coherence_minutes:
                params['min_coherence_time_minutes'] = min_coherence_minutes
            if max_error_rate:
                params['max_error_rate'] = max_error_rate
            
            response = await self.client.get(
                f"{settings.QUANTUM_MARKET_URL}/api/v1/resources/search",
                params=params
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to search quantum resources: {e}")
            return []
    
    async def get_quantum_spot_prices(self) -> List[Dict]:
        """Get current spot prices for quantum resources"""
        try:
            response = await self.client.get(
                f"{settings.QUANTUM_MARKET_URL}/api/v1/pricing/spot"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get quantum spot prices: {e}")
            return []
    
    async def get_quantum_futures_prices(self) -> List[Dict]:
        """Get futures prices for quantum resources"""
        try:
            response = await self.client.get(
                f"{settings.QUANTUM_MARKET_URL}/api/v1/pricing/futures"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get quantum futures prices: {e}")
            return []
    
    async def get_quantum_resources_with_quality(self) -> List[Dict]:
        """Get quantum resources with quality scores"""
        try:
            # Get resources
            resources = await self.search_quantum_resources()
            
            # Enrich with quality scores from oracle
            for resource in resources:
                quality = await self.get_resource_quality(
                    "quantum",
                    resource['qpu_id']
                )
                resource['quality_score'] = quality.get('overall_score', 80)
            
            return resources
            
        except Exception as e:
            logger.error(f"Failed to get quantum resources with quality: {e}")
            return []
    
    # AI Market Methods
    async def search_ai_accelerators(
        self,
        accelerator_type: str = None,
        min_tflops: float = None,
        max_price_per_hour: float = None
    ) -> List[Dict]:
        """Search available AI accelerators"""
        try:
            params = {}
            if accelerator_type:
                params['accelerator_type'] = accelerator_type
            if min_tflops:
                params['min_tflops'] = min_tflops
            if max_price_per_hour:
                params['max_price_per_hour'] = max_price_per_hour
            
            response = await self.client.get(
                f"{settings.AI_MARKET_URL}/api/v1/accelerators/search",
                params=params
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to search AI accelerators: {e}")
            return []
    
    async def get_ai_spot_prices(self) -> List[Dict]:
        """Get current spot prices for AI accelerators"""
        try:
            response = await self.client.get(
                f"{settings.AI_MARKET_URL}/api/v1/pricing/spot"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get AI spot prices: {e}")
            return []
    
    async def get_ai_reserved_prices(self) -> List[Dict]:
        """Get reserved instance prices for AI accelerators"""
        try:
            response = await self.client.get(
                f"{settings.AI_MARKET_URL}/api/v1/pricing/reserved"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get AI reserved prices: {e}")
            return []
    
    # Network Market Methods
    async def search_network_paths(
        self,
        source: str,
        destination: str,
        min_bandwidth_mbps: int = None,
        max_latency_ms: float = None
    ) -> List[Dict]:
        """Search available network paths"""
        try:
            params = {
                'source_node': source,
                'destination_node': destination
            }
            if min_bandwidth_mbps:
                params['min_bandwidth_mbps'] = min_bandwidth_mbps
            if max_latency_ms:
                params['max_latency_ms'] = max_latency_ms
            
            response = await self.client.get(
                f"{settings.NETWORK_MARKET_URL}/api/v1/paths/search",
                params=params
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to search network paths: {e}")
            return []
    
    async def get_network_paths(self) -> List[Dict]:
        """Get all available network paths"""
        try:
            response = await self.client.get(
                f"{settings.NETWORK_MARKET_URL}/api/v1/paths"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get network paths: {e}")
            return []
    
    async def get_network_qos_pricing(self, path_id: str) -> List[Dict]:
        """Get QoS-based pricing for a network path"""
        try:
            response = await self.client.get(
                f"{settings.NETWORK_MARKET_URL}/api/v1/pricing/qos/{path_id}"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get network QoS pricing: {e}")
            return []
    
    # Oracle Service Methods
    async def get_resource_quality(
        self,
        resource_type: str,
        resource_id: str
    ) -> Dict:
        """Get quality score from oracle service"""
        try:
            response = await self.client.get(
                f"{settings.ORACLE_SERVICE_URL}/api/v1/quality/{resource_type}/{resource_id}"
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to get resource quality: {e}")
            return {"overall_score": 80}  # Default
    
    # Allocation Methods
    async def allocate_quantum_resource(
        self,
        qpu_id: str,
        duration_minutes: int,
        user_address: str
    ) -> Dict:
        """Allocate quantum resource"""
        try:
            payload = {
                "qpu_id": qpu_id,
                "duration_minutes": duration_minutes,
                "user_address": user_address
            }
            
            response = await self.client.post(
                f"{settings.QUANTUM_MARKET_URL}/api/v1/allocations",
                json=payload
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to allocate quantum resource: {e}")
            raise
    
    async def allocate_ai_accelerator(
        self,
        accelerator_id: str,
        duration_hours: float,
        user_address: str
    ) -> Dict:
        """Allocate AI accelerator"""
        try:
            payload = {
                "accelerator_id": accelerator_id,
                "duration_hours": duration_hours,
                "user_address": user_address
            }
            
            response = await self.client.post(
                f"{settings.AI_MARKET_URL}/api/v1/allocations",
                json=payload
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to allocate AI accelerator: {e}")
            raise
    
    async def allocate_network_bandwidth(
        self,
        path_id: str,
        bandwidth_mbps: int,
        duration_hours: float,
        qos_class: str,
        user_address: str
    ) -> Dict:
        """Allocate network bandwidth"""
        try:
            payload = {
                "path_id": path_id,
                "bandwidth_mbps": bandwidth_mbps,
                "duration_hours": duration_hours,
                "qos_class": qos_class,
                "user_address": user_address
            }
            
            response = await self.client.post(
                f"{settings.NETWORK_MARKET_URL}/api/v1/bandwidth/allocate",
                json=payload
            )
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to allocate network bandwidth: {e}")
            raise
    
    # Market Comparison Methods
    async def get_all_market_prices(
        self,
        resource_type: str,
        specifications: Dict[str, Any]
    ) -> Dict[str, List[Dict]]:
        """Get prices from all markets for comparison"""
        try:
            prices = {}
            
            if resource_type == "quantum":
                prices['spot'] = await self.get_quantum_spot_prices()
                prices['futures'] = await self.get_quantum_futures_prices()
                
            elif resource_type == "ai":
                prices['spot'] = await self.get_ai_spot_prices()
                prices['reserved'] = await self.get_ai_reserved_prices()
                
            elif resource_type == "network":
                # Get paths matching specifications
                paths = await self.search_network_paths(
                    source=specifications.get('source_node', ''),
                    destination=specifications.get('destination_node', '')
                )
                
                # Get QoS pricing for each path
                prices['paths'] = []
                for path in paths[:5]:  # Limit to top 5
                    qos_pricing = await self.get_network_qos_pricing(path['path_id'])
                    prices['paths'].append({
                        'path': path,
                        'qos_pricing': qos_pricing
                    })
            
            return prices
            
        except Exception as e:
            logger.error(f"Failed to get market prices: {e}")
            return {}
    
    # Batch Operations
    async def get_resource_availability(
        self,
        requirements: List[Dict]
    ) -> Dict[str, bool]:
        """Check availability for multiple resource requirements"""
        availability = {}
        
        for req in requirements:
            resource_type = req.get('resource_type')
            
            try:
                if resource_type == 'quantum':
                    resources = await self.search_quantum_resources(
                        min_qubit_count=req.get('min_qubit_count'),
                        min_coherence_minutes=req.get('min_coherence_minutes')
                    )
                    availability[resource_type] = len(resources) > 0
                    
                elif resource_type == 'ai':
                    resources = await self.search_ai_accelerators(
                        accelerator_type=req.get('accelerator_type'),
                        min_tflops=req.get('min_tflops')
                    )
                    availability[resource_type] = len(resources) > 0
                    
                elif resource_type == 'network':
                    resources = await self.search_network_paths(
                        source=req.get('source_node'),
                        destination=req.get('destination_node'),
                        min_bandwidth_mbps=req.get('min_bandwidth_mbps')
                    )
                    availability[resource_type] = len(resources) > 0
                    
            except Exception as e:
                logger.error(f"Failed to check availability for {resource_type}: {e}")
                availability[resource_type] = False
        
        return availability 