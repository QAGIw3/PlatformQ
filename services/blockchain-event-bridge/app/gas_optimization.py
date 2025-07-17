"""
Gas Optimization Service

Implements meta-transactions and gas station networks for gasless transactions
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
from web3 import Web3
from eth_account import Account
from eth_account.messages import encode_defunct
import json
import hashlib
from decimal import Decimal

logger = logging.getLogger(__name__)


class GasPaymentMethod(Enum):
    USER_PAYS = "user_pays"  # Traditional - user pays gas
    RELAYER_PAYS = "relayer_pays"  # Meta-transaction - relayer pays
    TOKEN_PAYMENT = "token_payment"  # Pay gas in tokens
    SUBSIDIZED = "subsidized"  # Platform subsidizes gas


class RelayerStatus(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    INSUFFICIENT_FUNDS = "insufficient_funds"
    RATE_LIMITED = "rate_limited"


@dataclass
class MetaTransaction:
    """EIP-712 compliant meta-transaction"""
    from_address: str
    to_address: str
    value: int
    data: bytes
    nonce: int
    chain_id: int
    gas_limit: int
    gas_price: int
    signature: Optional[str] = None
    deadline: Optional[int] = None


@dataclass
class GasRelayer:
    """Gas relayer node information"""
    relayer_id: str
    address: str
    chains: List[int]  # Supported chain IDs
    balance: Decimal
    min_balance: Decimal
    status: RelayerStatus
    success_rate: float
    avg_response_time: float  # seconds
    fee_percentage: Decimal


@dataclass
class GasSubsidy:
    """Gas subsidy configuration"""
    user_id: str
    max_transactions: int
    max_gas_per_tx: int
    total_gas_limit: int
    used_transactions: int
    used_gas: int
    expires_at: datetime
    conditions: Dict[str, Any]


@dataclass
class GaslessTransaction:
    """Gasless transaction request"""
    tx_id: str
    user_address: str
    meta_tx: MetaTransaction
    payment_method: GasPaymentMethod
    relayer_id: Optional[str]
    submitted_at: datetime
    executed_at: Optional[datetime]
    tx_hash: Optional[str]
    status: str
    gas_used: Optional[int]
    gas_price_paid: Optional[int]


class GasOptimizationService:
    """Manages gas optimization strategies"""
    
    def __init__(self,
                 eip712_domain_name: str = "PlatformQ",
                 eip712_domain_version: str = "1"):
        self.domain_name = eip712_domain_name
        self.domain_version = eip712_domain_version
        
        # Relayer network
        self.relayers: Dict[str, GasRelayer] = {}
        self.relayer_pools: Dict[int, List[str]] = {}  # chain_id -> relayer_ids
        
        # Transaction management
        self.pending_transactions: Dict[str, GaslessTransaction] = {}
        self.transaction_history: List[GaslessTransaction] = []
        
        # Subsidies
        self.user_subsidies: Dict[str, GasSubsidy] = {}
        
        # Gas price cache
        self.gas_price_cache: Dict[int, Dict[str, int]] = {}
        
        # Monitoring
        self._monitoring_task: Optional[asyncio.Task] = None
        self._relayer_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start gas optimization service"""
        # Initialize relayers
        await self._initialize_relayers()
        
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self._relayer_task = asyncio.create_task(self._relayer_loop())
        
        logger.info("Gas optimization service started")
        
    async def stop(self):
        """Stop gas optimization service"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._relayer_task:
            self._relayer_task.cancel()
            
        logger.info("Gas optimization service stopped")
        
    async def create_meta_transaction(self,
                                    from_address: str,
                                    to_address: str,
                                    value: int,
                                    data: str,
                                    chain_id: int,
                                    gas_limit: Optional[int] = None) -> MetaTransaction:
        """Create a meta-transaction"""
        # Get nonce for user
        nonce = await self._get_meta_nonce(from_address, chain_id)
        
        # Estimate gas if not provided
        if not gas_limit:
            gas_limit = await self._estimate_gas(to_address, value, data, chain_id)
            
        # Get current gas price
        gas_price = await self._get_optimal_gas_price(chain_id)
        
        # Set deadline (5 minutes from now)
        deadline = int((datetime.utcnow() + timedelta(minutes=5)).timestamp())
        
        meta_tx = MetaTransaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=bytes.fromhex(data[2:]) if data.startswith('0x') else bytes.fromhex(data),
            nonce=nonce,
            chain_id=chain_id,
            gas_limit=gas_limit,
            gas_price=gas_price,
            deadline=deadline
        )
        
        return meta_tx
        
    async def sign_meta_transaction(self,
                                  meta_tx: MetaTransaction,
                                  private_key: str) -> MetaTransaction:
        """Sign a meta-transaction using EIP-712"""
        # Create EIP-712 domain
        domain = {
            "name": self.domain_name,
            "version": self.domain_version,
            "chainId": meta_tx.chain_id,
            "verifyingContract": "0x0000000000000000000000000000000000000000"  # Forwarder contract
        }
        
        # Create message types
        types = {
            "MetaTransaction": [
                {"name": "from", "type": "address"},
                {"name": "to", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
                {"name": "nonce", "type": "uint256"},
                {"name": "gasLimit", "type": "uint256"},
                {"name": "gasPrice", "type": "uint256"},
                {"name": "deadline", "type": "uint256"}
            ]
        }
        
        # Create message
        message = {
            "from": meta_tx.from_address,
            "to": meta_tx.to_address,
            "value": meta_tx.value,
            "data": meta_tx.data,
            "nonce": meta_tx.nonce,
            "gasLimit": meta_tx.gas_limit,
            "gasPrice": meta_tx.gas_price,
            "deadline": meta_tx.deadline
        }
        
        # Sign using EIP-712
        account = Account.from_key(private_key)
        signable_message = encode_defunct(text=json.dumps(message))  # Simplified
        signature = account.sign_message(signable_message)
        
        meta_tx.signature = signature.signature.hex()
        return meta_tx
        
    async def submit_gasless_transaction(self,
                                       meta_tx: MetaTransaction,
                                       payment_method: GasPaymentMethod = GasPaymentMethod.RELAYER_PAYS,
                                       preferred_relayer: Optional[str] = None) -> str:
        """Submit a gasless transaction"""
        # Validate signature
        if not meta_tx.signature:
            raise ValueError("Meta-transaction must be signed")
            
        # Check if user is eligible for gasless transactions
        if payment_method == GasPaymentMethod.SUBSIDIZED:
            if not await self._check_subsidy_eligibility(meta_tx.from_address):
                raise ValueError("User not eligible for subsidized gas")
                
        # Create transaction record
        tx_id = self._generate_tx_id(meta_tx)
        
        gasless_tx = GaslessTransaction(
            tx_id=tx_id,
            user_address=meta_tx.from_address,
            meta_tx=meta_tx,
            payment_method=payment_method,
            relayer_id=preferred_relayer,
            submitted_at=datetime.utcnow(),
            executed_at=None,
            tx_hash=None,
            status="pending",
            gas_used=None,
            gas_price_paid=None
        )
        
        self.pending_transactions[tx_id] = gasless_tx
        
        # Queue for processing
        if payment_method == GasPaymentMethod.RELAYER_PAYS:
            await self._queue_for_relayer(gasless_tx)
        elif payment_method == GasPaymentMethod.TOKEN_PAYMENT:
            await self._process_token_payment(gasless_tx)
        elif payment_method == GasPaymentMethod.SUBSIDIZED:
            await self._process_subsidized(gasless_tx)
            
        logger.info(f"Submitted gasless transaction: {tx_id}")
        return tx_id
        
    async def get_transaction_status(self, tx_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a gasless transaction"""
        # Check pending
        if tx_id in self.pending_transactions:
            tx = self.pending_transactions[tx_id]
        else:
            # Check history
            tx = next((t for t in self.transaction_history if t.tx_id == tx_id), None)
            
        if not tx:
            return None
            
        return {
            "tx_id": tx_id,
            "status": tx.status,
            "user_address": tx.user_address,
            "payment_method": tx.payment_method.value,
            "submitted_at": tx.submitted_at.isoformat(),
            "executed_at": tx.executed_at.isoformat() if tx.executed_at else None,
            "tx_hash": tx.tx_hash,
            "gas_used": tx.gas_used,
            "gas_price_paid": tx.gas_price_paid,
            "relayer": tx.relayer_id
        }
        
    async def add_gas_subsidy(self,
                            user_id: str,
                            max_transactions: int,
                            max_gas_per_tx: int,
                            total_gas_limit: int,
                            duration_days: int,
                            conditions: Optional[Dict[str, Any]] = None) -> bool:
        """Add gas subsidy for a user"""
        subsidy = GasSubsidy(
            user_id=user_id,
            max_transactions=max_transactions,
            max_gas_per_tx=max_gas_per_tx,
            total_gas_limit=total_gas_limit,
            used_transactions=0,
            used_gas=0,
            expires_at=datetime.utcnow() + timedelta(days=duration_days),
            conditions=conditions or {}
        )
        
        self.user_subsidies[user_id] = subsidy
        logger.info(f"Added gas subsidy for user {user_id}")
        return True
        
    async def estimate_gasless_cost(self,
                                  to_address: str,
                                  value: int,
                                  data: str,
                                  chain_id: int,
                                  payment_method: GasPaymentMethod) -> Dict[str, Any]:
        """Estimate cost for gasless transaction"""
        # Estimate gas
        gas_limit = await self._estimate_gas(to_address, value, data, chain_id)
        gas_price = await self._get_optimal_gas_price(chain_id)
        
        total_gas_cost = gas_limit * gas_price
        
        # Calculate cost based on payment method
        if payment_method == GasPaymentMethod.USER_PAYS:
            user_cost = total_gas_cost
            platform_cost = 0
            
        elif payment_method == GasPaymentMethod.RELAYER_PAYS:
            # Add relayer fee
            relayer_fee = await self._get_relayer_fee(chain_id)
            user_cost = int(total_gas_cost * (1 + relayer_fee))
            platform_cost = 0
            
        elif payment_method == GasPaymentMethod.TOKEN_PAYMENT:
            # Convert to token amount
            token_price = await self._get_gas_token_price(chain_id)
            user_cost = int(total_gas_cost / token_price)
            platform_cost = 0
            
        elif payment_method == GasPaymentMethod.SUBSIDIZED:
            user_cost = 0
            platform_cost = total_gas_cost
            
        else:
            user_cost = total_gas_cost
            platform_cost = 0
            
        return {
            "gas_limit": gas_limit,
            "gas_price": gas_price,
            "total_gas_wei": total_gas_cost,
            "user_cost": user_cost,
            "platform_cost": platform_cost,
            "payment_method": payment_method.value,
            "estimated_time": await self._estimate_confirmation_time(chain_id)
        }
        
    async def _initialize_relayers(self):
        """Initialize relayer network"""
        # In production, load from configuration
        self.relayers["relayer_1"] = GasRelayer(
            relayer_id="relayer_1",
            address="0x1234567890123456789012345678901234567890",
            chains=[1, 137, 56],  # Ethereum, Polygon, BSC
            balance=Decimal("10"),
            min_balance=Decimal("1"),
            status=RelayerStatus.ACTIVE,
            success_rate=0.98,
            avg_response_time=2.5,
            fee_percentage=Decimal("2.5")  # 2.5% fee
        )
        
        self.relayers["relayer_2"] = GasRelayer(
            relayer_id="relayer_2",
            address="0x2345678901234567890123456789012345678901",
            chains=[137, 43114],  # Polygon, Avalanche
            balance=Decimal("5"),
            min_balance=Decimal("0.5"),
            status=RelayerStatus.ACTIVE,
            success_rate=0.95,
            avg_response_time=3.0,
            fee_percentage=Decimal("3.0")  # 3% fee
        )
        
        # Build chain pools
        for relayer_id, relayer in self.relayers.items():
            for chain_id in relayer.chains:
                if chain_id not in self.relayer_pools:
                    self.relayer_pools[chain_id] = []
                self.relayer_pools[chain_id].append(relayer_id)
                
    async def _queue_for_relayer(self, gasless_tx: GaslessTransaction):
        """Queue transaction for relayer processing"""
        # Select best relayer
        chain_id = gasless_tx.meta_tx.chain_id
        
        if gasless_tx.relayer_id:
            # Use preferred relayer if specified
            relayer_id = gasless_tx.relayer_id
        else:
            # Select optimal relayer
            relayer_id = await self._select_optimal_relayer(chain_id)
            
        if not relayer_id:
            gasless_tx.status = "no_relayer_available"
            logger.error(f"No relayer available for chain {chain_id}")
            return
            
        gasless_tx.relayer_id = relayer_id
        gasless_tx.status = "queued"
        
    async def _select_optimal_relayer(self, chain_id: int) -> Optional[str]:
        """Select optimal relayer for chain"""
        available_relayers = self.relayer_pools.get(chain_id, [])
        
        if not available_relayers:
            return None
            
        # Score relayers
        best_relayer = None
        best_score = -1
        
        for relayer_id in available_relayers:
            relayer = self.relayers[relayer_id]
            
            # Skip if not active or insufficient funds
            if relayer.status != RelayerStatus.ACTIVE:
                continue
                
            if relayer.balance < relayer.min_balance:
                continue
                
            # Calculate score
            score = (
                relayer.success_rate * 100 +
                (10 / relayer.avg_response_time) +
                (10 / float(relayer.fee_percentage))
            )
            
            if score > best_score:
                best_score = score
                best_relayer = relayer_id
                
        return best_relayer
        
    async def _process_relayer_transaction(self, gasless_tx: GaslessTransaction):
        """Process transaction through relayer"""
        relayer = self.relayers.get(gasless_tx.relayer_id)
        if not relayer:
            gasless_tx.status = "relayer_not_found"
            return
            
        try:
            # In production, forward to actual relayer
            # For now, simulate execution
            await asyncio.sleep(2)  # Simulate processing
            
            # Mark as executed
            gasless_tx.tx_hash = f"0x{'0' * 63}1"  # Dummy hash
            gasless_tx.executed_at = datetime.utcnow()
            gasless_tx.status = "success"
            gasless_tx.gas_used = gasless_tx.meta_tx.gas_limit
            gasless_tx.gas_price_paid = gasless_tx.meta_tx.gas_price
            
            # Update relayer balance
            gas_cost = Decimal(gasless_tx.gas_used * gasless_tx.gas_price_paid) / Decimal(10**18)
            relayer.balance -= gas_cost
            
            logger.info(f"Executed gasless transaction {gasless_tx.tx_id} via relayer {relayer.relayer_id}")
            
        except Exception as e:
            logger.error(f"Failed to execute gasless transaction: {e}")
            gasless_tx.status = "failed"
            
    async def _process_token_payment(self, gasless_tx: GaslessTransaction):
        """Process gas payment in tokens"""
        # In production, implement token payment logic
        # For now, mark as pending implementation
        gasless_tx.status = "token_payment_not_implemented"
        
    async def _process_subsidized(self, gasless_tx: GaslessTransaction):
        """Process subsidized transaction"""
        subsidy = self.user_subsidies.get(gasless_tx.user_address)
        if not subsidy:
            gasless_tx.status = "no_subsidy"
            return
            
        # Check limits
        if subsidy.used_transactions >= subsidy.max_transactions:
            gasless_tx.status = "subsidy_tx_limit_reached"
            return
            
        estimated_gas = gasless_tx.meta_tx.gas_limit
        if estimated_gas > subsidy.max_gas_per_tx:
            gasless_tx.status = "exceeds_gas_limit"
            return
            
        if subsidy.used_gas + estimated_gas > subsidy.total_gas_limit:
            gasless_tx.status = "subsidy_gas_exhausted"
            return
            
        # Process as relayer transaction
        gasless_tx.payment_method = GasPaymentMethod.SUBSIDIZED
        await self._queue_for_relayer(gasless_tx)
        
        # Update subsidy usage
        subsidy.used_transactions += 1
        subsidy.used_gas += estimated_gas
        
    async def _check_subsidy_eligibility(self, user_address: str) -> bool:
        """Check if user is eligible for subsidy"""
        subsidy = self.user_subsidies.get(user_address)
        
        if not subsidy:
            return False
            
        # Check expiration
        if datetime.utcnow() > subsidy.expires_at:
            del self.user_subsidies[user_address]
            return False
            
        # Check limits
        if subsidy.used_transactions >= subsidy.max_transactions:
            return False
            
        return True
        
    async def _get_meta_nonce(self, address: str, chain_id: int) -> int:
        """Get meta-transaction nonce for address"""
        # In production, track per-forwarder nonces
        # For now, use timestamp
        return int(datetime.utcnow().timestamp())
        
    async def _estimate_gas(self, to: str, value: int, data: str, chain_id: int) -> int:
        """Estimate gas for transaction"""
        # Base estimates
        if not data or data == "0x":
            return 21000  # Simple transfer
        else:
            # Contract interaction
            return 100000 + len(data) * 16  # Rough estimate
            
    async def _get_optimal_gas_price(self, chain_id: int) -> int:
        """Get optimal gas price for chain"""
        # Check cache
        if chain_id in self.gas_price_cache:
            prices = self.gas_price_cache[chain_id]
            if prices.get("timestamp", 0) > datetime.utcnow().timestamp() - 60:
                return prices["standard"]
                
        # In production, query gas oracle
        # For now, use defaults
        default_prices = {
            1: 30 * 10**9,     # Ethereum: 30 gwei
            137: 30 * 10**9,   # Polygon: 30 gwei
            56: 5 * 10**9,     # BSC: 5 gwei
            43114: 25 * 10**9  # Avalanche: 25 gwei
        }
        
        gas_price = default_prices.get(chain_id, 20 * 10**9)
        
        # Update cache
        self.gas_price_cache[chain_id] = {
            "slow": int(gas_price * 0.8),
            "standard": gas_price,
            "fast": int(gas_price * 1.2),
            "timestamp": datetime.utcnow().timestamp()
        }
        
        return gas_price
        
    async def _get_relayer_fee(self, chain_id: int) -> float:
        """Get average relayer fee for chain"""
        relayer_ids = self.relayer_pools.get(chain_id, [])
        
        if not relayer_ids:
            return 0.03  # Default 3%
            
        fees = []
        for relayer_id in relayer_ids:
            relayer = self.relayers.get(relayer_id)
            if relayer and relayer.status == RelayerStatus.ACTIVE:
                fees.append(float(relayer.fee_percentage) / 100)
                
        return sum(fees) / len(fees) if fees else 0.03
        
    async def _get_gas_token_price(self, chain_id: int) -> float:
        """Get gas token price in native currency"""
        # In production, integrate with price oracle
        # For now, return dummy values
        return 1.0
        
    async def _estimate_confirmation_time(self, chain_id: int) -> int:
        """Estimate confirmation time in seconds"""
        confirmation_times = {
            1: 180,      # Ethereum: ~3 minutes
            137: 10,     # Polygon: ~10 seconds
            56: 15,      # BSC: ~15 seconds
            43114: 5     # Avalanche: ~5 seconds
        }
        
        return confirmation_times.get(chain_id, 60)
        
    def _generate_tx_id(self, meta_tx: MetaTransaction) -> str:
        """Generate unique transaction ID"""
        data = f"{meta_tx.from_address}:{meta_tx.nonce}:{meta_tx.chain_id}:{datetime.utcnow().timestamp()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
        
    async def _monitoring_loop(self):
        """Monitor gas prices and relayer health"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Update gas prices
                for chain_id in [1, 137, 56, 43114]:
                    await self._get_optimal_gas_price(chain_id)
                    
                # Check relayer health
                for relayer in self.relayers.values():
                    if relayer.balance < relayer.min_balance:
                        relayer.status = RelayerStatus.INSUFFICIENT_FUNDS
                        logger.warning(f"Relayer {relayer.relayer_id} has insufficient funds")
                        
                # Clean up old transactions
                await self._cleanup_old_transactions()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                
    async def _relayer_loop(self):
        """Process queued transactions"""
        while True:
            try:
                await asyncio.sleep(5)  # Every 5 seconds
                
                # Process queued transactions
                for tx_id, gasless_tx in list(self.pending_transactions.items()):
                    if gasless_tx.status == "queued":
                        await self._process_relayer_transaction(gasless_tx)
                        
                        # Move to history if completed
                        if gasless_tx.status in ["success", "failed"]:
                            self.transaction_history.append(gasless_tx)
                            del self.pending_transactions[tx_id]
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in relayer loop: {e}")
                
    async def _cleanup_old_transactions(self):
        """Clean up old transaction history"""
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        self.transaction_history = [
            tx for tx in self.transaction_history
            if tx.submitted_at > cutoff
        ]
        
    async def get_relayer_statistics(self) -> Dict[str, Any]:
        """Get relayer network statistics"""
        total_transactions = len(self.transaction_history)
        successful = sum(1 for tx in self.transaction_history if tx.status == "success")
        
        relayer_stats = {}
        for relayer_id, relayer in self.relayers.items():
            relayer_txs = [tx for tx in self.transaction_history if tx.relayer_id == relayer_id]
            relayer_stats[relayer_id] = {
                "status": relayer.status.value,
                "balance": str(relayer.balance),
                "success_rate": relayer.success_rate,
                "total_transactions": len(relayer_txs),
                "chains": relayer.chains
            }
            
        return {
            "total_transactions": total_transactions,
            "success_rate": successful / total_transactions if total_transactions > 0 else 0,
            "pending_transactions": len(self.pending_transactions),
            "active_relayers": sum(1 for r in self.relayers.values() if r.status == RelayerStatus.ACTIVE),
            "relayers": relayer_stats,
            "subsidized_users": len(self.user_subsidies)
        } 