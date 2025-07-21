"""
Flash Provisioning Protocol

Manages instant resource provisioning using flash loans for just-in-time scaling.
"""

from typing import Dict, Any, List, Optional, Callable
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from platformq_shared.models import ResourceType, ServiceTier
from platformq_shared.blockchain import BlockchainClient
from ..models import FlashProvisionRequest, ResourceAllocation, ProvisioningStatus
from ..services.resource_matcher import ResourceMatcher
from ..services.capacity_monitor import CapacityMonitor

logger = logging.getLogger(__name__)


class FlashProvisioningProtocol:
    """Protocol for instant resource provisioning using flash loans"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        resource_matcher: ResourceMatcher,
        capacity_monitor: CapacityMonitor,
        flash_provider_address: str,
        resource_token_address: str
    ):
        self.blockchain = blockchain_client
        self.matcher = resource_matcher
        self.monitor = capacity_monitor
        self.flash_provider_address = flash_provider_address
        self.resource_token_address = resource_token_address
        
        # Active flash provisions
        self._active_provisions = {}  # provision_id -> provision details
        
        # Resource pools for different tiers
        self._resource_pools = {
            ServiceTier.STANDARD: {},
            ServiceTier.PREMIUM: {},
            ServiceTier.GUARANTEED: {}
        }
        
        # JIT scaling configurations
        self._scaling_configs = {}
        
    async def initialize(self):
        """Initialize the flash provisioning protocol"""
        # Connect to smart contracts
        self.flash_provider_contract = await self.blockchain.get_contract(
            self.flash_provider_address,
            "FlashResourceProvider"
        )
        
        self.resource_token_contract = await self.blockchain.get_contract(
            self.resource_token_address,
            "ResourceToken"
        )
        
        # Start monitoring tasks
        asyncio.create_task(self._monitor_provisions())
        asyncio.create_task(self._monitor_capacity())
        
        logger.info("Flash Provisioning Protocol initialized")
        
    async def flash_provision(
        self,
        request: FlashProvisionRequest,
        callback: Optional[Callable] = None
    ) -> Dict[str, Any]:
        """
        Execute instant resource provisioning using flash loans
        
        Args:
            request: Flash provisioning request
            callback: Optional callback for when resources are ready
            
        Returns:
            Provisioning result with allocation details
        """
        try:
            provision_id = f"FLASH-{datetime.utcnow().timestamp()}"
            
            # Find matching resources
            matches = await self.matcher.find_matches(
                resource_type=request.resource_type,
                amount=request.amount,
                tier=request.tier,
                region=request.region,
                duration=request.duration
            )
            
            if not matches:
                raise ValueError("No matching resources available")
                
            # Select best match based on criteria
            selected = self._select_best_match(matches, request)
            
            # Prepare flash loan
            token_id = selected["token_id"]
            flash_amount = request.amount
            
            # Calculate fees
            fee_rate = await self._get_flash_fee_rate(request.resource_type)
            fee_amount = flash_amount * fee_rate / 10000
            
            # Execute flash loan
            tx_hash = await self._execute_flash_loan(
                token_id=token_id,
                amount=flash_amount,
                receiver=request.receiver_address,
                data=request.callback_data
            )
            
            # Create provision record
            provision = {
                "provision_id": provision_id,
                "request": request,
                "token_id": token_id,
                "amount": flash_amount,
                "fee": fee_amount,
                "provider": selected["provider"],
                "status": ProvisioningStatus.ACTIVE,
                "start_time": datetime.utcnow(),
                "end_time": datetime.utcnow() + timedelta(seconds=request.duration),
                "tx_hash": tx_hash,
                "callback": callback
            }
            
            self._active_provisions[provision_id] = provision
            
            # Execute callback if provided
            if callback:
                await callback({
                    "provision_id": provision_id,
                    "resources": selected,
                    "status": "ready"
                })
                
            # Create allocation record
            allocation = ResourceAllocation(
                allocation_id=provision_id,
                resource_type=request.resource_type,
                amount=flash_amount,
                provider=selected["provider"],
                consumer=request.receiver_address,
                start_time=provision["start_time"],
                end_time=provision["end_time"],
                status=ProvisioningStatus.ACTIVE
            )
            
            return {
                "provision_id": provision_id,
                "allocation": allocation,
                "fee": float(fee_amount),
                "tx_hash": tx_hash,
                "estimated_cost": self._calculate_cost(request, fee_amount),
                "resources": {
                    "token_id": token_id,
                    "provider": selected["provider"],
                    "location": selected["location"],
                    "specs": selected["specs"]
                }
            }
            
        except Exception as e:
            logger.error(f"Error in flash provision: {e}")
            raise
            
    async def flash_swap(
        self,
        from_resource: Dict[str, Any],
        to_resource_type: ResourceType,
        to_amount: int,
        max_slippage: Decimal = Decimal("0.03")
    ) -> Dict[str, Any]:
        """
        Atomically swap one resource type for another
        
        Args:
            from_resource: Source resource details
            to_resource_type: Target resource type
            to_amount: Amount of target resource needed
            max_slippage: Maximum acceptable slippage
            
        Returns:
            Swap result
        """
        try:
            # Find AMM pool for the swap
            pool_id = await self._find_pool(
                from_resource["token_id"],
                to_resource_type
            )
            
            if pool_id is None:
                raise ValueError("No liquidity pool available for swap")
                
            # Get swap quote
            quote = await self._get_swap_quote(
                pool_id=pool_id,
                from_amount=from_resource["amount"]
            )
            
            # Check slippage
            expected_rate = to_amount / from_resource["amount"]
            actual_rate = quote["output_amount"] / from_resource["amount"]
            slippage = abs(expected_rate - actual_rate) / expected_rate
            
            if slippage > max_slippage:
                raise ValueError(f"Slippage {slippage} exceeds maximum {max_slippage}")
                
            # Execute flash swap
            tx_hash = await self.flash_provider_contract.functions.flashSwap(
                from_resource["token_id"],
                quote["to_token_id"],
                from_resource["amount"],
                pool_id
            ).transact()
            
            return {
                "swap_id": f"SWAP-{datetime.utcnow().timestamp()}",
                "from_token": from_resource["token_id"],
                "to_token": quote["to_token_id"],
                "from_amount": from_resource["amount"],
                "to_amount": quote["output_amount"],
                "slippage": float(slippage),
                "tx_hash": tx_hash.hex()
            }
            
        except Exception as e:
            logger.error(f"Error in flash swap: {e}")
            raise
            
    async def enable_jit_scaling(
        self,
        resource_type: ResourceType,
        scaling_config: Dict[str, Any]
    ):
        """
        Enable just-in-time scaling for a resource type
        
        Args:
            resource_type: Type of resource
            scaling_config: Scaling configuration
        """
        self._scaling_configs[resource_type] = {
            "enabled": True,
            "min_capacity": scaling_config.get("min_capacity", 100),
            "max_capacity": scaling_config.get("max_capacity", 10000),
            "scale_up_threshold": scaling_config.get("scale_up_threshold", 0.8),
            "scale_down_threshold": scaling_config.get("scale_down_threshold", 0.2),
            "cooldown_period": scaling_config.get("cooldown_period", 300),
            "last_scale_time": None
        }
        
        logger.info(f"JIT scaling enabled for {resource_type}")
        
    async def provision_burst_capacity(
        self,
        resource_type: ResourceType,
        burst_amount: int,
        duration: int,
        max_price: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Provision burst capacity for sudden demand spikes
        
        Args:
            resource_type: Type of resource
            burst_amount: Amount of burst capacity needed
            duration: Duration in seconds
            max_price: Maximum price willing to pay
            
        Returns:
            Burst provisioning result
        """
        try:
            # Find available burst capacity
            burst_providers = await self._find_burst_providers(
                resource_type=resource_type,
                amount=burst_amount,
                max_price=max_price
            )
            
            if not burst_providers:
                raise ValueError("No burst capacity available")
                
            provisions = []
            remaining = burst_amount
            
            # Provision from multiple providers if needed
            for provider in burst_providers:
                if remaining <= 0:
                    break
                    
                provision_amount = min(remaining, provider["available"])
                
                # Create flash provision request
                request = FlashProvisionRequest(
                    resource_type=resource_type,
                    amount=provision_amount,
                    tier=ServiceTier.STANDARD,  # Burst is usually standard tier
                    duration=duration,
                    region=provider["region"],
                    receiver_address=provider["address"]
                )
                
                result = await self.flash_provision(request)
                provisions.append(result)
                remaining -= provision_amount
                
            return {
                "burst_id": f"BURST-{datetime.utcnow().timestamp()}",
                "total_amount": burst_amount - remaining,
                "provisions": provisions,
                "total_cost": sum(p["estimated_cost"] for p in provisions),
                "duration": duration
            }
            
        except Exception as e:
            logger.error(f"Error provisioning burst capacity: {e}")
            raise
            
    async def _execute_flash_loan(
        self,
        token_id: int,
        amount: int,
        receiver: str,
        data: bytes
    ) -> str:
        """Execute flash loan transaction"""
        tx = await self.flash_provider_contract.functions.flashLoan(
            receiver,
            token_id,
            amount,
            data
        ).transact()
        
        return tx.hex()
        
    async def _monitor_provisions(self):
        """Monitor active provisions and handle expirations"""
        while True:
            try:
                current_time = datetime.utcnow()
                expired = []
                
                for provision_id, provision in self._active_provisions.items():
                    if provision["end_time"] <= current_time:
                        expired.append(provision_id)
                        
                # Handle expired provisions
                for provision_id in expired:
                    await self._handle_provision_expiry(provision_id)
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring provisions: {e}")
                await asyncio.sleep(30)
                
    async def _monitor_capacity(self):
        """Monitor capacity and trigger JIT scaling"""
        while True:
            try:
                for resource_type, config in self._scaling_configs.items():
                    if not config["enabled"]:
                        continue
                        
                    # Check if in cooldown
                    if config["last_scale_time"]:
                        cooldown_end = config["last_scale_time"] + timedelta(seconds=config["cooldown_period"])
                        if datetime.utcnow() < cooldown_end:
                            continue
                            
                    # Get current utilization
                    utilization = await self.monitor.get_utilization(resource_type)
                    
                    # Scale up if needed
                    if utilization > config["scale_up_threshold"]:
                        await self._scale_up(resource_type, config)
                    # Scale down if needed
                    elif utilization < config["scale_down_threshold"]:
                        await self._scale_down(resource_type, config)
                        
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring capacity: {e}")
                await asyncio.sleep(60)
                
    async def _scale_up(self, resource_type: ResourceType, config: Dict):
        """Scale up capacity for a resource type"""
        try:
            current_capacity = await self.monitor.get_total_capacity(resource_type)
            target_capacity = min(
                int(current_capacity * 1.5),  # 50% increase
                config["max_capacity"]
            )
            
            increase_amount = target_capacity - current_capacity
            
            if increase_amount > 0:
                # Provision additional capacity
                await self.provision_burst_capacity(
                    resource_type=resource_type,
                    burst_amount=increase_amount,
                    duration=3600  # 1 hour burst
                )
                
                config["last_scale_time"] = datetime.utcnow()
                logger.info(f"Scaled up {resource_type} by {increase_amount} units")
                
        except Exception as e:
            logger.error(f"Error scaling up: {e}")
            
    async def _scale_down(self, resource_type: ResourceType, config: Dict):
        """Scale down capacity for a resource type"""
        # In flash provisioning, scale down happens automatically
        # when provisions expire
        config["last_scale_time"] = datetime.utcnow()
        logger.info(f"Scale down scheduled for {resource_type}")
        
    async def _handle_provision_expiry(self, provision_id: str):
        """Handle expired provision"""
        provision = self._active_provisions.pop(provision_id, None)
        if not provision:
            return
            
        provision["status"] = ProvisioningStatus.COMPLETED
        
        # Execute callback if provided
        if provision.get("callback"):
            await provision["callback"]({
                "provision_id": provision_id,
                "status": "expired"
            })
            
        logger.info(f"Provision {provision_id} expired")
        
    def _select_best_match(
        self,
        matches: List[Dict],
        request: FlashProvisionRequest
    ) -> Dict[str, Any]:
        """Select best resource match based on criteria"""
        # Score matches based on:
        # 1. Price
        # 2. Location proximity
        # 3. Provider reputation
        # 4. Available capacity
        
        best_score = -1
        best_match = None
        
        for match in matches:
            score = 0
            
            # Price score (lower is better)
            price_score = 100 / (1 + match["price"])
            score += price_score * 0.4
            
            # Location score
            if match["location"] == request.region:
                score += 30
            elif match["location"].split("-")[0] == request.region.split("-")[0]:
                score += 20
                
            # Reputation score
            score += match.get("reputation", 50) * 0.2
            
            # Capacity score
            capacity_ratio = match["available"] / request.amount
            score += min(capacity_ratio * 20, 20)
            
            if score > best_score:
                best_score = score
                best_match = match
                
        return best_match
        
    async def _get_flash_fee_rate(self, resource_type: ResourceType) -> int:
        """Get flash loan fee rate for resource type"""
        fee = await self.flash_provider_contract.functions.flashFees(
            resource_type.value
        ).call()
        
        return fee
        
    def _calculate_cost(
        self,
        request: FlashProvisionRequest,
        fee_amount: Decimal
    ) -> float:
        """Calculate estimated cost for provision"""
        # Base cost = resource amount * duration * price per unit
        # Plus flash loan fee
        
        # Mock calculation - in production would use oracle prices
        base_prices = {
            ResourceType.CPU: Decimal("0.05"),
            ResourceType.GPU: Decimal("0.50"),
            ResourceType.STORAGE: Decimal("0.001"),
            ResourceType.BANDWIDTH: Decimal("0.01"),
            ResourceType.MEMORY: Decimal("0.02")
        }
        
        base_price = base_prices.get(request.resource_type, Decimal("0.1"))
        duration_hours = request.duration / 3600
        
        resource_cost = request.amount * base_price * Decimal(duration_hours)
        total_cost = resource_cost + fee_amount
        
        return float(total_cost)
        
    async def _find_pool(
        self,
        from_token_id: int,
        to_resource_type: ResourceType
    ) -> Optional[int]:
        """Find AMM pool for token swap"""
        # In production, this would query the AMM contract
        # For now, return mock pool ID
        return 1
        
    async def _get_swap_quote(
        self,
        pool_id: int,
        from_amount: int
    ) -> Dict[str, Any]:
        """Get swap quote from AMM"""
        # Mock quote - in production would call AMM contract
        return {
            "to_token_id": 2,
            "output_amount": int(from_amount * 0.95),  # 5% slippage
            "fee": int(from_amount * 0.003)  # 0.3% fee
        }
        
    async def _find_burst_providers(
        self,
        resource_type: ResourceType,
        amount: int,
        max_price: Optional[Decimal]
    ) -> List[Dict[str, Any]]:
        """Find providers with burst capacity"""
        # Mock providers - in production would query capacity registry
        return [
            {
                "address": "0x123...",
                "available": amount // 2,
                "price": Decimal("0.06"),
                "region": "us-east-1"
            },
            {
                "address": "0x456...",
                "available": amount,
                "price": Decimal("0.07"),
                "region": "us-west-1"
            }
        ]
        
    async def get_provision_status(self, provision_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a provision"""
        provision = self._active_provisions.get(provision_id)
        if not provision:
            return None
            
        return {
            "provision_id": provision_id,
            "status": provision["status"].value,
            "resource_type": provision["request"].resource_type.value,
            "amount": provision["amount"],
            "start_time": provision["start_time"].isoformat(),
            "end_time": provision["end_time"].isoformat(),
            "remaining_time": max(0, (provision["end_time"] - datetime.utcnow()).total_seconds())
        }
        
    async def get_flash_statistics(self) -> Dict[str, Any]:
        """Get flash provisioning statistics"""
        active_count = len(self._active_provisions)
        
        # Aggregate by resource type
        by_type = {}
        total_amount = 0
        
        for provision in self._active_provisions.values():
            resource_type = provision["request"].resource_type.value
            if resource_type not in by_type:
                by_type[resource_type] = {"count": 0, "amount": 0}
                
            by_type[resource_type]["count"] += 1
            by_type[resource_type]["amount"] += provision["amount"]
            total_amount += provision["amount"]
            
        return {
            "active_provisions": active_count,
            "total_resources": total_amount,
            "by_resource_type": by_type,
            "jit_scaling_enabled": list(self._scaling_configs.keys())
        } 