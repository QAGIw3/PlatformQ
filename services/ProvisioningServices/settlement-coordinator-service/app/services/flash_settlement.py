"""
Flash Settlement Service

Integrates flash provisioning with settlement coordinator for atomic swaps
and ensuring proper repayment.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio

from platformq_shared.models import ResourceType, ServiceTier
from platformq_shared.blockchain import BlockchainClient
from ..models import Settlement, SettlementStatus
from ..services.resource_tokenizer import ResourceTokenizer

logger = logging.getLogger(__name__)


class FlashSettlementService:
    """Service for handling flash loan settlements and atomic swaps"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        resource_tokenizer: ResourceTokenizer,
        flash_provider_address: str
    ):
        self.blockchain = blockchain_client
        self.tokenizer = resource_tokenizer
        self.flash_provider_address = flash_provider_address
        
        # Track active flash settlements
        self._active_flash_settlements = {}
        
        # Flash loan receiver implementation
        self._receiver_contract = None
        
    async def initialize(self):
        """Initialize flash settlement service"""
        # Deploy flash receiver contract if needed
        await self._deploy_receiver_contract()
        
        # Start monitoring
        asyncio.create_task(self._monitor_flash_settlements())
        
        logger.info("Flash Settlement Service initialized")
        
    async def create_flash_settlement(
        self,
        resource_request: Dict[str, Any],
        provider: str,
        consumer: str,
        duration: int
    ) -> Settlement:
        """
        Create a settlement that uses flash loans for instant provisioning
        
        Args:
            resource_request: Resource requirements
            provider: Provider address
            consumer: Consumer address  
            duration: Duration in seconds
            
        Returns:
            Flash settlement object
        """
        settlement_id = f"FLASH-SETTLE-{datetime.utcnow().timestamp()}"
        
        # Calculate resource tokens needed
        token_amount = await self._calculate_token_amount(resource_request)
        
        # Get token ID for the resource
        token_id = await self.tokenizer.get_or_create_token_id(
            resource_type=resource_request["resource_type"],
            tier=resource_request.get("tier", ServiceTier.STANDARD),
            region=resource_request["region"],
            provider=provider
        )
        
        # Calculate fees
        flash_fee = await self._calculate_flash_fee(
            resource_request["resource_type"],
            token_amount
        )
        
        # Create settlement record
        settlement = Settlement(
            settlement_id=settlement_id,
            resource_type=resource_request["resource_type"],
            amount=resource_request["amount"],
            provider=provider,
            consumer=consumer,
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow() + timedelta(seconds=duration),
            token_id=token_id,
            token_amount=token_amount,
            flash_fee=flash_fee,
            status=SettlementStatus.PENDING,
            is_flash=True
        )
        
        self._active_flash_settlements[settlement_id] = settlement
        
        return settlement
        
    async def execute_flash_provision(
        self,
        settlement: Settlement,
        callback_data: bytes = b""
    ) -> Dict[str, Any]:
        """
        Execute flash provision for a settlement
        
        Args:
            settlement: Settlement to execute
            callback_data: Data to pass to flash receiver
            
        Returns:
            Execution result
        """
        try:
            # Prepare flash loan call
            flash_provider = await self.blockchain.get_contract(
                self.flash_provider_address,
                "FlashResourceProvider"
            )
            
            # Execute flash loan through receiver contract
            tx = await flash_provider.functions.flashLoan(
                self._receiver_contract.address,
                settlement.token_id,
                settlement.token_amount,
                callback_data
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            if receipt.status == 1:
                settlement.status = SettlementStatus.ACTIVE
                settlement.tx_hash = receipt.transactionHash.hex()
                
                # Start usage tracking
                asyncio.create_task(self._track_flash_usage(settlement))
                
                return {
                    "success": True,
                    "settlement_id": settlement.settlement_id,
                    "tx_hash": settlement.tx_hash,
                    "status": "active"
                }
            else:
                settlement.status = SettlementStatus.FAILED
                return {
                    "success": False,
                    "error": "Flash loan transaction failed"
                }
                
        except Exception as e:
            logger.error(f"Error executing flash provision: {e}")
            settlement.status = SettlementStatus.FAILED
            return {
                "success": False,
                "error": str(e)
            }
            
    async def execute_atomic_swap(
        self,
        from_settlement: Settlement,
        to_resource_type: ResourceType,
        to_amount: int,
        pool_id: int
    ) -> Dict[str, Any]:
        """
        Execute atomic resource swap using flash loans
        
        Args:
            from_settlement: Source settlement
            to_resource_type: Target resource type
            to_amount: Target amount needed
            pool_id: AMM pool ID
            
        Returns:
            Swap result
        """
        try:
            flash_provider = await self.blockchain.get_contract(
                self.flash_provider_address,
                "FlashResourceProvider"
            )
            
            # Find target token ID
            to_token_id = await self.tokenizer.get_or_create_token_id(
                resource_type=to_resource_type,
                tier=from_settlement.tier,
                region=from_settlement.region,
                provider="amm-pool"
            )
            
            # Execute flash swap
            tx = await flash_provider.functions.flashSwap(
                from_settlement.token_id,
                to_token_id,
                from_settlement.token_amount,
                pool_id
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            if receipt.status == 1:
                # Create new settlement for swapped resources
                new_settlement = await self.create_flash_settlement(
                    resource_request={
                        "resource_type": to_resource_type,
                        "amount": to_amount,
                        "region": from_settlement.region,
                        "tier": from_settlement.tier
                    },
                    provider="amm-pool",
                    consumer=from_settlement.consumer,
                    duration=int((from_settlement.end_time - datetime.utcnow()).total_seconds())
                )
                
                new_settlement.status = SettlementStatus.ACTIVE
                new_settlement.parent_settlement = from_settlement.settlement_id
                
                # Mark original settlement as swapped
                from_settlement.status = SettlementStatus.COMPLETED
                from_settlement.completion_type = "swapped"
                
                return {
                    "success": True,
                    "new_settlement_id": new_settlement.settlement_id,
                    "from_token": from_settlement.token_id,
                    "to_token": to_token_id,
                    "tx_hash": receipt.transactionHash.hex()
                }
            else:
                return {
                    "success": False,
                    "error": "Flash swap transaction failed"
                }
                
        except Exception as e:
            logger.error(f"Error executing atomic swap: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def repay_flash_loan(
        self,
        settlement: Settlement
    ) -> bool:
        """
        Ensure flash loan is repaid with fees
        
        Args:
            settlement: Settlement to repay
            
        Returns:
            True if repayment successful
        """
        try:
            # In the flash loan pattern, repayment happens atomically
            # within the same transaction. This method verifies it happened.
            
            resource_token = await self.blockchain.get_contract(
                await self.tokenizer.get_token_address(),
                "ResourceToken"
            )
            
            # Check contract balance to ensure fee was paid
            receiver_balance = await resource_token.functions.balanceOf(
                self._receiver_contract.address,
                settlement.token_id
            ).call()
            
            # Should have at least the fee amount
            if receiver_balance >= settlement.flash_fee:
                settlement.fee_paid = True
                return True
            else:
                logger.warning(f"Insufficient fee payment for {settlement.settlement_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error checking flash loan repayment: {e}")
            return False
            
    async def handle_flash_provision_callback(
        self,
        initiator: str,
        token_id: int,
        amount: int,
        fee: int,
        data: bytes
    ) -> bytes:
        """
        Handle callback from flash loan provider
        
        This is called by the smart contract during flash loan execution
        
        Args:
            initiator: Address that initiated the flash loan
            token_id: Resource token ID
            amount: Amount borrowed
            fee: Fee to be paid
            data: Callback data
            
        Returns:
            Success response for contract
        """
        try:
            # Find the settlement
            settlement = None
            for s in self._active_flash_settlements.values():
                if s.token_id == token_id and s.consumer == initiator:
                    settlement = s
                    break
                    
            if not settlement:
                raise ValueError("Settlement not found")
                
            # Provision the resources to the consumer
            await self._provision_resources(settlement)
            
            # Ensure we have tokens to repay (amount + fee)
            # In production, this would involve:
            # 1. Consumer providing collateral
            # 2. Or consumer having pre-approved spending
            # 3. Or using a credit system
            
            # Return success response
            # keccak256("ERC3156FlashBorrower.onFlashLoan")
            return bytes.fromhex("439148f0bbc682ca079e46d6e2c2f0c1e3b820f1a291b069d8882abf8cf18dd9")
            
        except Exception as e:
            logger.error(f"Error in flash provision callback: {e}")
            raise
            
    async def _deploy_receiver_contract(self):
        """Deploy flash loan receiver contract"""
        # In production, this would deploy an actual contract
        # For now, create a mock receiver
        self._receiver_contract = type('obj', (object,), {
            'address': '0xFLASHRECEIVER...'
        })
        
    async def _calculate_token_amount(
        self,
        resource_request: Dict[str, Any]
    ) -> int:
        """Calculate token amount for resource request"""
        # Simple calculation - in production would be more complex
        base_amount = resource_request["amount"]
        
        # Adjust for tier
        tier_multipliers = {
            ServiceTier.STANDARD: 1,
            ServiceTier.PREMIUM: 2,
            ServiceTier.GUARANTEED: 3
        }
        
        tier = resource_request.get("tier", ServiceTier.STANDARD)
        return base_amount * tier_multipliers.get(tier, 1)
        
    async def _calculate_flash_fee(
        self,
        resource_type: ResourceType,
        amount: int
    ) -> int:
        """Calculate flash loan fee"""
        # Fee rates in basis points
        fee_rates = {
            ResourceType.CPU: 10,  # 0.1%
            ResourceType.GPU: 20,  # 0.2%
            ResourceType.STORAGE: 5,  # 0.05%
            ResourceType.BANDWIDTH: 15,  # 0.15%
            ResourceType.MEMORY: 10  # 0.1%
        }
        
        rate = fee_rates.get(resource_type, 10)
        return (amount * rate) // 10000
        
    async def _provision_resources(self, settlement: Settlement):
        """Provision resources to consumer"""
        # In production, this would:
        # 1. Allocate actual compute resources
        # 2. Update routing tables
        # 3. Configure access controls
        # 4. Start monitoring
        
        logger.info(f"Provisioning resources for {settlement.settlement_id}")
        
    async def _track_flash_usage(self, settlement: Settlement):
        """Track resource usage during flash provision"""
        while settlement.status == SettlementStatus.ACTIVE:
            try:
                # In production, collect actual usage metrics
                usage = await self._collect_usage_metrics(settlement)
                
                # Update settlement with usage
                settlement.usage_data = usage
                
                # Check if nearing end time
                remaining = (settlement.end_time - datetime.utcnow()).total_seconds()
                if remaining < 300:  # 5 minutes warning
                    logger.warning(f"Flash provision {settlement.settlement_id} expiring soon")
                    
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error tracking flash usage: {e}")
                await asyncio.sleep(60)
                
    async def _collect_usage_metrics(
        self,
        settlement: Settlement
    ) -> Dict[str, Any]:
        """Collect usage metrics for settlement"""
        # Mock metrics - in production would query actual usage
        return {
            "cpu_hours": 10.5,
            "memory_gb_hours": 42.0,
            "network_gb": 100.0,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def _monitor_flash_settlements(self):
        """Monitor active flash settlements"""
        while True:
            try:
                current_time = datetime.utcnow()
                expired = []
                
                for settlement_id, settlement in self._active_flash_settlements.items():
                    if settlement.end_time <= current_time:
                        expired.append(settlement_id)
                        
                # Handle expired settlements
                for settlement_id in expired:
                    settlement = self._active_flash_settlements.pop(settlement_id)
                    settlement.status = SettlementStatus.COMPLETED
                    
                    # Verify flash loan was repaid
                    repaid = await self.repay_flash_loan(settlement)
                    if not repaid:
                        logger.error(f"Flash loan not properly repaid: {settlement_id}")
                        
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring flash settlements: {e}")
                await asyncio.sleep(60) 