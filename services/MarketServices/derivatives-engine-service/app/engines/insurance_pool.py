"""
Insurance Pool Manager for handling insurance fund operations
"""

from decimal import Decimal
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class InsurancePoolManager:
    """
    Manages the insurance pool for covering losses
    """
    
    def __init__(self):
        self.pool_balance = Decimal("1000000")  # $1M initial fund
        self.reserved_funds = Decimal("0")
        
    async def get_available_funds(self) -> Decimal:
        """Get available funds in insurance pool"""
        return self.pool_balance - self.reserved_funds
        
    async def claim_insurance(
        self,
        amount: Decimal,
        claim_type: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Claim funds from insurance pool"""
        available = await self.get_available_funds()
        if amount <= available:
            self.pool_balance -= amount
            logger.info(f"Insurance claim approved: {amount} for {claim_type}")
            return True
        return False
        
    async def contribute_to_pool(self, amount: Decimal):
        """Add funds to insurance pool"""
        self.pool_balance += amount
        logger.info(f"Insurance pool contribution: {amount}")
        
    async def reserve_funds(self, amount: Decimal) -> bool:
        """Reserve funds for potential claims"""
        available = await self.get_available_funds()
        if amount <= available:
            self.reserved_funds += amount
            return True
        return False
        
    async def release_reserved_funds(self, amount: Decimal):
        """Release reserved funds back to available pool"""
        self.reserved_funds = max(Decimal("0"), self.reserved_funds - amount) 