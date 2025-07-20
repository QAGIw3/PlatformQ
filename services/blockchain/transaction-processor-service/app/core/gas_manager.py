"""
Gas Manager - Manages gas pricing and optimization
"""

import asyncio
import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
from prometheus_client import Gauge

from ..config import Settings
from ..models.transaction import Transaction, TransactionType, TransactionPriority

logger = logging.getLogger(__name__)

# Metrics
gas_price_gauge = Gauge(
    'blockchain_gas_price_wei',
    'Current gas price in wei',
    ['chain', 'priority']
)


class GasManager:
    """Manages gas pricing and estimation"""
    
    def __init__(self, blockchain_connector_url: str, settings: Settings):
        self.blockchain_connector_url = blockchain_connector_url
        self.settings = settings
        
        # Gas price cache
        self._gas_prices: Dict[str, Dict[str, Any]] = {}
        self._gas_price_updated: Dict[str, datetime] = {}
        
        # HTTP client
        self.http_client = httpx.AsyncClient(timeout=10.0)
        
        # Background task
        self._update_task = None
        self._running = False
        
    async def start(self):
        """Start gas manager"""
        logger.info("Starting Gas Manager")
        
        # Start background price updates
        self._running = True
        self._update_task = asyncio.create_task(self._update_gas_prices())
        
        # Initial price fetch
        await self._fetch_all_gas_prices()
        
        logger.info("Gas Manager started")
        
    async def stop(self):
        """Stop gas manager"""
        logger.info("Stopping Gas Manager")
        
        self._running = False
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
                
        await self.http_client.aclose()
        
        logger.info("Gas Manager stopped")
        
    async def get_optimal_gas(
        self,
        chain: str,
        transaction: Transaction
    ) -> Dict[str, Any]:
        """Get optimal gas settings for a transaction"""
        # Get current gas prices
        gas_prices = await self._get_gas_prices(chain)
        
        # Estimate gas limit if not provided
        if not transaction.gas_limit:
            gas_limit = await self._estimate_gas_limit(chain, transaction)
        else:
            gas_limit = transaction.gas_limit
            
        # Select gas price based on priority
        gas_price_info = self._select_gas_price(gas_prices, transaction.priority)
        
        # Apply multiplier for safety
        gas_limit = int(gas_limit * 1.1)  # 10% buffer
        
        result = {
            'gas_limit': gas_limit,
            'gas_price': gas_price_info.get('gas_price'),
            'estimated_cost_wei': str(gas_limit * int(gas_price_info.get('gas_price', 0)))
        }
        
        # Add EIP-1559 fields if supported
        if 'max_fee_per_gas' in gas_price_info:
            result['max_fee_per_gas'] = gas_price_info['max_fee_per_gas']
            result['max_priority_fee_per_gas'] = gas_price_info['max_priority_fee_per_gas']
            
        return result
        
    async def _get_gas_prices(self, chain: str) -> Dict[str, Any]:
        """Get cached gas prices or fetch if stale"""
        # Check cache
        if chain in self._gas_prices:
            last_update = self._gas_price_updated.get(chain)
            if last_update and datetime.utcnow() - last_update < timedelta(seconds=30):
                return self._gas_prices[chain]
                
        # Fetch fresh prices
        return await self._fetch_gas_prices(chain)
        
    async def _fetch_gas_prices(self, chain: str) -> Dict[str, Any]:
        """Fetch gas prices from blockchain connector"""
        try:
            response = await self.http_client.get(
                f"{self.blockchain_connector_url}/api/v1/gas/price/{chain}"
            )
            response.raise_for_status()
            
            prices = response.json()
            self._gas_prices[chain] = prices
            self._gas_price_updated[chain] = datetime.utcnow()
            
            # Update metrics
            for priority in ['slow', 'standard', 'fast', 'instant']:
                if priority in prices:
                    gas_price_gauge.labels(
                        chain=chain,
                        priority=priority
                    ).set(float(prices[priority]))
                    
            return prices
            
        except Exception as e:
            logger.error(f"Error fetching gas prices for {chain}: {e}")
            # Return cached prices if available
            if chain in self._gas_prices:
                return self._gas_prices[chain]
            # Return defaults
            return {
                'standard': '20000000000',  # 20 gwei
                'slow': '15000000000',      # 15 gwei
                'fast': '30000000000',      # 30 gwei
                'instant': '50000000000'    # 50 gwei
            }
            
    def _select_gas_price(
        self,
        gas_prices: Dict[str, Any],
        priority: TransactionPriority
    ) -> Dict[str, Any]:
        """Select appropriate gas price based on priority"""
        priority_map = {
            TransactionPriority.LOW: 'slow',
            TransactionPriority.NORMAL: 'standard',
            TransactionPriority.HIGH: 'fast',
            TransactionPriority.URGENT: 'instant'
        }
        
        price_key = priority_map[priority]
        
        # Check if EIP-1559 is supported
        if 'maxFeePerGas' in gas_prices:
            # Use EIP-1559 pricing
            base_fee = int(gas_prices.get('baseFeePerGas', 0))
            
            if price_key == 'slow':
                priority_fee = int(base_fee * 0.1)  # 10% of base fee
            elif price_key == 'standard':
                priority_fee = int(base_fee * 0.2)  # 20% of base fee
            elif price_key == 'fast':
                priority_fee = int(base_fee * 0.5)  # 50% of base fee
            else:  # instant
                priority_fee = base_fee  # 100% of base fee
                
            max_fee = base_fee * 2 + priority_fee
            
            return {
                'max_fee_per_gas': str(max_fee),
                'max_priority_fee_per_gas': str(priority_fee),
                'gas_price': str(max_fee)  # For estimation
            }
        else:
            # Use legacy pricing
            return {
                'gas_price': gas_prices.get(price_key, gas_prices.get('standard'))
            }
            
    async def _estimate_gas_limit(
        self,
        chain: str,
        transaction: Transaction
    ) -> int:
        """Estimate gas limit for a transaction"""
        try:
            response = await self.http_client.post(
                f"{self.blockchain_connector_url}/api/v1/gas/estimate",
                json={
                    'chain': chain,
                    'from_address': transaction.from_address,
                    'to_address': transaction.to_address,
                    'value': transaction.value,
                    'data': transaction.data
                }
            )
            response.raise_for_status()
            
            return response.json()['gasLimit']
            
        except Exception as e:
            logger.error(f"Error estimating gas for {transaction.id}: {e}")
            # Return default based on transaction type
            return self._get_default_gas_limit(transaction.type)
            
    def _get_default_gas_limit(self, tx_type: TransactionType) -> int:
        """Get default gas limit by transaction type"""
        defaults = {
            TransactionType.TRANSFER: 21000,
            TransactionType.TOKEN_TRANSFER: 65000,
            TransactionType.NFT_TRANSFER: 85000,
            TransactionType.CONTRACT_CALL: 150000,
            TransactionType.CONTRACT_DEPLOY: 3000000,
            TransactionType.SWAP: 200000,
            TransactionType.BRIDGE: 250000,
            TransactionType.BATCH: 500000
        }
        return defaults.get(tx_type, 100000)
        
    async def _update_gas_prices(self):
        """Background task to update gas prices"""
        while self._running:
            try:
                await asyncio.sleep(self.settings.GAS_PRICE_REFRESH_INTERVAL)
                await self._fetch_all_gas_prices()
            except Exception as e:
                logger.error(f"Error updating gas prices: {e}")
                await asyncio.sleep(5)
                
    async def _fetch_all_gas_prices(self):
        """Fetch gas prices for all supported chains"""
        try:
            # Get supported chains
            response = await self.http_client.get(
                f"{self.blockchain_connector_url}/api/v1/chains"
            )
            response.raise_for_status()
            
            chains = [chain['type'] for chain in response.json()['chains']]
            
            # Fetch prices for each chain
            tasks = [self._fetch_gas_prices(chain) for chain in chains]
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            logger.error(f"Error fetching chain list: {e}")
            
    def get_cached_gas_price(self, chain: str, priority: str = 'standard') -> Optional[str]:
        """Get cached gas price without fetching"""
        if chain in self._gas_prices:
            return self._gas_prices[chain].get(priority)
        return None 