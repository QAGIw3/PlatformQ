"""
DeFi Manager - Core component for managing DeFi protocols and blockchain connections.
"""

import logging
from typing import Dict, Any, Optional, List, AsyncContextManager
from contextlib import asynccontextmanager
import asyncio

from platformq_blockchain_common import (
    ConnectionPool,
    IBlockchainAdapter,
    ChainType
)
from prometheus_client import Counter, Histogram, Gauge

logger = logging.getLogger(__name__)

# Re-export metrics for use in API routers
DEFI_TRANSACTIONS = Counter(
    'defi_transactions_total', 
    'Total DeFi transactions',
    ['chain', 'protocol', 'operation']
)
TRANSACTION_LATENCY = Histogram(
    'defi_transaction_latency_seconds',
    'DeFi transaction latency',
    ['chain', 'protocol']
)


class DeFiManager:
    """
    Central manager for DeFi operations across multiple blockchains.
    
    Handles:
    - Connection pooling for blockchain adapters
    - Cross-chain coordination
    - Risk management integration
    - Price oracle integration
    - Transaction monitoring
    """
    
    def __init__(self,
                 connection_pool: ConnectionPool,
                 price_oracle: 'PriceOracle',
                 risk_calculator: 'RiskCalculator'):
        self.connection_pool = connection_pool
        self.price_oracle = price_oracle
        self.risk_calculator = risk_calculator
        self._monitoring_task: Optional[asyncio.Task] = None
        self._positions: Dict[str, Dict[str, Any]] = {}  # user -> positions
        
    async def initialize(self):
        """Initialize DeFi manager"""
        logger.info("Initializing DeFi Manager")
        
    async def shutdown(self):
        """Shutdown DeFi manager"""
        logger.info("Shutting down DeFi Manager")
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
                
    @asynccontextmanager
    async def get_adapter(self, chain: str) -> AsyncContextManager[IBlockchainAdapter]:
        """
        Get a blockchain adapter from the connection pool.
        
        Args:
            chain: Chain identifier (e.g., "ethereum", "polygon")
            
        Yields:
            Blockchain adapter instance
        """
        chain_type = ChainType(chain)
        async with self.connection_pool.get_connection(chain_type) as adapter:
            yield adapter
            
    def get_supported_chains(self) -> List[ChainType]:
        """Get list of supported blockchain types"""
        return [
            ChainType.ETHEREUM,
            ChainType.POLYGON,
            ChainType.ARBITRUM,
            ChainType.OPTIMISM,
            ChainType.AVALANCHE,
            ChainType.BSC,
            ChainType.SOLANA,
            ChainType.COSMOS,
            ChainType.POLKADOT
        ]
        
    async def get_total_value_locked(self) -> Dict[str, Dict[str, float]]:
        """
        Calculate total value locked across all protocols and chains.
        
        Returns:
            TVL organized by chain and protocol
        """
        tvl_data = {}
        
        for chain_type in self.get_supported_chains():
            chain = chain_type.value
            tvl_data[chain] = {
                "lending": 0.0,
                "auctions": 0.0,
                "yield_farming": 0.0,
                "liquidity": 0.0
            }
            
            # Get TVL data from each protocol
            # This would query smart contracts or indexers
            # For now, return mock data
            tvl_data[chain]["lending"] = 1000000.0  # $1M
            tvl_data[chain]["yield_farming"] = 500000.0  # $500k
            tvl_data[chain]["liquidity"] = 2000000.0  # $2M
            
        return tvl_data
        
    async def monitor_positions(self):
        """
        Background task to monitor user positions and liquidations.
        """
        while True:
            try:
                # Monitor lending positions for liquidation risk
                await self._check_lending_positions()
                
                # Monitor auction expirations
                await self._check_auction_expirations()
                
                # Monitor yield farming rewards
                await self._check_yield_farming_rewards()
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(60)
                
    async def _check_lending_positions(self):
        """Check lending positions for liquidation risk"""
        # Implementation would check collateral ratios
        # and trigger alerts/liquidations as needed
        pass
        
    async def _check_auction_expirations(self):
        """Check for expiring auctions"""
        # Implementation would finalize expired auctions
        pass
        
    async def _check_yield_farming_rewards(self):
        """Check and distribute yield farming rewards"""
        # Implementation would calculate and distribute rewards
        pass
        
    async def validate_transaction(self,
                                 chain: str,
                                 user: str,
                                 operation: str,
                                 amount: float) -> Dict[str, Any]:
        """
        Validate a DeFi transaction before execution.
        
        Args:
            chain: Blockchain identifier
            user: User address
            operation: Operation type
            amount: Transaction amount
            
        Returns:
            Validation result with risk assessment
        """
        # Get current gas prices
        gas_estimate = await self._estimate_gas_cost(chain, operation)
        
        # Check user balance
        has_balance = await self._check_user_balance(chain, user, amount + gas_estimate)
        
        # Assess risk
        risk_score = await self.risk_calculator.calculate_risk(
            chain=chain,
            user=user,
            operation=operation,
            amount=amount
        )
        
        return {
            "valid": has_balance and risk_score < 0.8,
            "gas_estimate": gas_estimate,
            "risk_score": risk_score,
            "warnings": self._get_risk_warnings(risk_score)
        }
        
    async def _estimate_gas_cost(self, chain: str, operation: str) -> float:
        """Estimate gas cost for an operation"""
        # Implementation would use real gas prices
        # For now, return mock estimate
        gas_prices = {
            "ethereum": 50.0,
            "polygon": 0.01,
            "arbitrum": 0.5,
            "solana": 0.001
        }
        return gas_prices.get(chain, 1.0)
        
    async def _check_user_balance(self, chain: str, user: str, required: float) -> bool:
        """Check if user has sufficient balance"""
        # Implementation would check actual balances
        # For now, return True
        return True
        
    def _get_risk_warnings(self, risk_score: float) -> List[str]:
        """Get risk warnings based on score"""
        warnings = []
        
        if risk_score > 0.9:
            warnings.append("CRITICAL: Extremely high risk transaction")
        elif risk_score > 0.7:
            warnings.append("WARNING: High risk transaction")
        elif risk_score > 0.5:
            warnings.append("CAUTION: Moderate risk transaction")
            
        return warnings
        
    def add_position(self, user: str, position: Dict[str, Any]):
        """Add a user position for tracking"""
        if user not in self._positions:
            self._positions[user] = {}
            
        position_id = position.get("id", f"{position['type']}_{position['chain']}_{len(self._positions[user])}")
        self._positions[user][position_id] = position
        
    def get_user_positions(self, user: str) -> Dict[str, Any]:
        """Get all positions for a user"""
        return self._positions.get(user, {}) 