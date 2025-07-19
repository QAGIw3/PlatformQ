"""
Client classes for external services
"""

import aiohttp
from typing import Dict, Any, Optional, List
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class StateManagementClient:
    """Client for State Management Service"""
    
    def __init__(self, base_url: str = "http://state-management-service:8000"):
        self.base_url = base_url
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def create_cache(self, cache_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new cache region"""
        async with self.session.post(
            f"{self.base_url}/api/v1/caches",
            json=cache_config
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get(self, cache: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        async with self.session.get(
            f"{self.base_url}/api/v1/caches/{cache}/keys/{key}"
        ) as response:
            if response.status == 404:
                return None
            response.raise_for_status()
            result = await response.json()
            return result.get("value")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def put(self, cache: str, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Put value into cache"""
        data = {"value": value}
        if ttl:
            data["ttl"] = ttl
        
        async with self.session.put(
            f"{self.base_url}/api/v1/caches/{cache}/keys/{key}",
            json=data
        ) as response:
            response.raise_for_status()
            return response.status == 200
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def delete(self, cache: str, key: str) -> bool:
        """Delete value from cache"""
        async with self.session.delete(
            f"{self.base_url}/api/v1/caches/{cache}/keys/{key}"
        ) as response:
            return response.status == 200
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def bulk_get(self, cache: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values from cache"""
        async with self.session.post(
            f"{self.base_url}/api/v1/caches/{cache}/bulk/get",
            json={"keys": keys}
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def bulk_put(self, cache: str, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        """Put multiple values into cache"""
        data = {"items": items}
        if ttl:
            data["ttl"] = ttl
        
        async with self.session.post(
            f"{self.base_url}/api/v1/caches/{cache}/bulk/put",
            json=data
        ) as response:
            response.raise_for_status()
            result = await response.json()
            return result.get("count", 0)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def query(self, cache: str, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        data = {"sql": sql}
        if params:
            data["params"] = params
        
        async with self.session.post(
            f"{self.base_url}/api/v1/caches/{cache}/query",
            json=data
        ) as response:
            response.raise_for_status()
            result = await response.json()
            return result.get("rows", [])
    
    async def begin_transaction(self) -> str:
        """Begin a new transaction"""
        async with self.session.post(
            f"{self.base_url}/api/v1/transactions"
        ) as response:
            response.raise_for_status()
            result = await response.json()
            return result["transaction_id"]
    
    async def commit_transaction(self, tx_id: str) -> bool:
        """Commit a transaction"""
        async with self.session.put(
            f"{self.base_url}/api/v1/transactions/{tx_id}",
            json={"action": "commit"}
        ) as response:
            response.raise_for_status()
            return response.status == 200
    
    async def rollback_transaction(self, tx_id: str) -> bool:
        """Rollback a transaction"""
        async with self.session.put(
            f"{self.base_url}/api/v1/transactions/{tx_id}",
            json={"action": "rollback"}
        ) as response:
            response.raise_for_status()
            return response.status == 200
    
    async def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        async with self.session.get(
            f"{self.base_url}/api/v1/health"
        ) as response:
            response.raise_for_status()
            return await response.json()


class ComputeAllocationClient:
    """Client for Compute Allocation Service"""
    
    def __init__(self, base_url: str = "http://compute-allocation-service:8000"):
        self.base_url = base_url
        self.session = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def allocate(self,
                      workload_type: str,
                      workload_id: str,
                      requirements: Dict[str, Any],
                      strategy: str = "BALANCED",
                      duration_hours: float = 1.0) -> Dict[str, Any]:
        """Request compute resources"""
        data = {
            "workload_type": workload_type,
            "workload_id": workload_id,
            "requirements": requirements,
            "strategy": strategy,
            "duration_hours": duration_hours
        }
        
        async with self.session.post(
            f"{self.base_url}/api/v1/allocations",
            json=data
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_allocation(self, allocation_id: str) -> Dict[str, Any]:
        """Get allocation status"""
        async with self.session.get(
            f"{self.base_url}/api/v1/allocations/{allocation_id}"
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def modify_allocation(self, allocation_id: str, modifications: Dict[str, Any]) -> Dict[str, Any]:
        """Modify existing allocation"""
        async with self.session.put(
            f"{self.base_url}/api/v1/allocations/{allocation_id}",
            json=modifications
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def release_allocation(self, allocation_id: str) -> bool:
        """Release allocated resources"""
        async with self.session.delete(
            f"{self.base_url}/api/v1/allocations/{allocation_id}"
        ) as response:
            return response.status == 200
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_current_pricing(self) -> Dict[str, Any]:
        """Get current spot pricing"""
        async with self.session.get(
            f"{self.base_url}/api/v1/pricing/current"
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def create_futures_contract(self, contract_params: Dict[str, Any]) -> Dict[str, Any]:
        """Create futures contract for capacity"""
        async with self.session.post(
            f"{self.base_url}/api/v1/contracts/futures",
            json=contract_params
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_cost_forecast(self, 
                               workload_type: str,
                               requirements: Dict[str, Any],
                               duration_hours: float) -> Dict[str, Any]:
        """Get cost forecast for workload"""
        params = {
            "workload_type": workload_type,
            "duration_hours": duration_hours,
            **requirements
        }
        
        async with self.session.get(
            f"{self.base_url}/api/v1/costs/forecast",
            params=params
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def get_available_resources(self, resource_type: Optional[str] = None) -> Dict[str, Any]:
        """Get available resources"""
        params = {}
        if resource_type:
            params["type"] = resource_type
        
        async with self.session.get(
            f"{self.base_url}/api/v1/resources/available",
            params=params
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    async def create_sla_derivative(self, sla_params: Dict[str, Any]) -> Dict[str, Any]:
        """Create SLA performance derivative"""
        async with self.session.post(
            f"{self.base_url}/api/v1/derivatives/sla",
            json=sla_params
        ) as response:
            response.raise_for_status()
            return await response.json()
    
    async def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        async with self.session.get(
            f"{self.base_url}/api/v1/health"
        ) as response:
            response.raise_for_status()
            return await response.json()


# Singleton instances for reuse
_state_client: Optional[StateManagementClient] = None
_compute_client: Optional[ComputeAllocationClient] = None


async def get_state_client() -> StateManagementClient:
    """Get or create state management client"""
    global _state_client
    if _state_client is None:
        _state_client = StateManagementClient()
        await _state_client.__aenter__()
    return _state_client


async def get_compute_client() -> ComputeAllocationClient:
    """Get or create compute allocation client"""
    global _compute_client
    if _compute_client is None:
        _compute_client = ComputeAllocationClient()
        await _compute_client.__aenter__()
    return _compute_client


async def cleanup_clients():
    """Cleanup client connections"""
    global _state_client, _compute_client
    
    if _state_client:
        await _state_client.__aexit__(None, None, None)
        _state_client = None
    
    if _compute_client:
        await _compute_client.__aexit__(None, None, None)
        _compute_client = None 