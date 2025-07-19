"""
Blockchain Event Bridge Client Integration
"""

import httpx
from typing import Dict, Any, Optional
from decimal import Decimal
import logging

logger = logging.getLogger(__name__)


class BlockchainEventBridgeClient:
    """Client for interacting with blockchain event bridge service"""
    
    def __init__(self, base_url: str = "http://blockchain-gateway-service:8000"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def deploy_contract(
        self,
        contract_type: str,
        contract_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy a smart contract"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/contracts/deploy",
                json={
                    "contract_type": contract_type,
                    "data": contract_data
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to deploy contract: {e}")
            return {"success": False, "error": str(e)}
            
    async def execute_transaction(
        self,
        chain: str,
        contract_address: str,
        method: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a blockchain transaction"""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/v1/transactions/execute",
                json={
                    "chain": chain,
                    "contract_address": contract_address,
                    "method": method,
                    "params": params
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to execute transaction: {e}")
            return {"success": False, "error": str(e)}
            
    async def get_balance(
        self,
        chain: str,
        address: str,
        token_address: Optional[str] = None
    ) -> Decimal:
        """Get balance of an address"""
        try:
            params = {"chain": chain, "address": address}
            if token_address:
                params["token_address"] = token_address
                
            response = await self.client.get(
                f"{self.base_url}/api/v1/balances",
                params=params
            )
            response.raise_for_status()
            data = response.json()
            return Decimal(data.get("balance", "0"))
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return Decimal("0")
            
    async def close(self):
        """Close the client"""
        await self.client.aclose() 