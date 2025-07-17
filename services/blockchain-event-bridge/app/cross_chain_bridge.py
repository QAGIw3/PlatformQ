"""
Cross-Chain Bridge

Enables asset bridging between supported blockchain networks
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
from web3 import Web3
from eth_account import Account
import json
import hashlib
from decimal import Decimal

logger = logging.getLogger(__name__)


class BridgeStatus(Enum):
    PENDING = "pending"
    LOCKED = "locked"
    MINTED = "minted"
    COMPLETED = "completed"
    FAILED = "failed"
    REFUNDED = "refunded"


class BridgeNetwork(Enum):
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    BSC = "bsc"
    AVALANCHE = "avalanche"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    FANTOM = "fantom"


@dataclass
class BridgeTransaction:
    bridge_id: str
    user_address: str
    source_chain: BridgeNetwork
    destination_chain: BridgeNetwork
    asset_address: str
    amount: Decimal
    source_tx_hash: Optional[str]
    destination_tx_hash: Optional[str]
    status: BridgeStatus
    created_at: datetime
    completed_at: Optional[datetime]
    fee_amount: Decimal
    validator_signatures: List[str]
    metadata: Dict[str, Any]


@dataclass
class BridgeAsset:
    asset_id: str
    name: str
    symbol: str
    decimals: int
    native_chain: BridgeNetwork
    wrapped_contracts: Dict[BridgeNetwork, str]  # chain -> contract address
    is_enabled: bool
    min_bridge_amount: Decimal
    max_bridge_amount: Decimal
    bridge_fee_percentage: Decimal


@dataclass
class ValidatorNode:
    node_id: str
    address: str
    chains: List[BridgeNetwork]
    stake_amount: Decimal
    is_active: bool
    reliability_score: float


class CrossChainBridge:
    """Manages cross-chain asset bridging"""
    
    def __init__(self,
                 validator_threshold: int = 3,
                 confirmation_blocks: Dict[BridgeNetwork, int] = None):
        self.validator_threshold = validator_threshold
        self.confirmation_blocks = confirmation_blocks or {
            BridgeNetwork.ETHEREUM: 12,
            BridgeNetwork.POLYGON: 128,
            BridgeNetwork.BSC: 15,
            BridgeNetwork.AVALANCHE: 6,
            BridgeNetwork.ARBITRUM: 10,
            BridgeNetwork.OPTIMISM: 10,
            BridgeNetwork.FANTOM: 10
        }
        
        # Bridge state
        self.bridge_transactions: Dict[str, BridgeTransaction] = {}
        self.supported_assets: Dict[str, BridgeAsset] = {}
        self.validator_nodes: Dict[str, ValidatorNode] = {}
        
        # Chain connections
        self.web3_connections: Dict[BridgeNetwork, Web3] = {}
        self.bridge_contracts: Dict[BridgeNetwork, Any] = {}
        
        # Monitoring
        self._monitoring_tasks: Dict[BridgeNetwork, asyncio.Task] = {}
        self._validator_task: Optional[asyncio.Task] = None
        
        # Initialize supported assets
        self._initialize_assets()
        
    def _initialize_assets(self):
        """Initialize supported bridge assets"""
        # USDC
        self.supported_assets["USDC"] = BridgeAsset(
            asset_id="USDC",
            name="USD Coin",
            symbol="USDC",
            decimals=6,
            native_chain=BridgeNetwork.ETHEREUM,
            wrapped_contracts={
                BridgeNetwork.ETHEREUM: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                BridgeNetwork.POLYGON: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
                BridgeNetwork.BSC: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
                BridgeNetwork.AVALANCHE: "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
                BridgeNetwork.ARBITRUM: "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                BridgeNetwork.OPTIMISM: "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
                BridgeNetwork.FANTOM: "0x04068DA6C83AFCFA0e13ba15A6696662335D5B75"
            },
            is_enabled=True,
            min_bridge_amount=Decimal("10"),
            max_bridge_amount=Decimal("1000000"),
            bridge_fee_percentage=Decimal("0.1")  # 0.1%
        )
        
        # WETH
        self.supported_assets["WETH"] = BridgeAsset(
            asset_id="WETH",
            name="Wrapped Ether",
            symbol="WETH",
            decimals=18,
            native_chain=BridgeNetwork.ETHEREUM,
            wrapped_contracts={
                BridgeNetwork.ETHEREUM: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                BridgeNetwork.POLYGON: "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
                BridgeNetwork.BSC: "0x2170Ed0880ac9A755fd29B2688956BD959F933F8",
                BridgeNetwork.AVALANCHE: "0x49D5c2BdFfac6CE2BFdB6640F4F80f226bc10bAB",
                BridgeNetwork.ARBITRUM: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                BridgeNetwork.OPTIMISM: "0x4200000000000000000000000000000000000006",
                BridgeNetwork.FANTOM: "0x74b23882a30290451A17c44f4F05243b6b58C76d"
            },
            is_enabled=True,
            min_bridge_amount=Decimal("0.01"),
            max_bridge_amount=Decimal("1000"),
            bridge_fee_percentage=Decimal("0.15")  # 0.15%
        )
        
    async def initialize(self, chain_configs: Dict[BridgeNetwork, Dict[str, Any]]):
        """Initialize bridge with chain configurations"""
        # Connect to chains
        for chain, config in chain_configs.items():
            try:
                self.web3_connections[chain] = Web3(Web3.HTTPProvider(config["rpc_url"]))
                
                # Load bridge contract
                with open(config["contract_abi_path"], 'r') as f:
                    abi = json.load(f)
                    
                self.bridge_contracts[chain] = self.web3_connections[chain].eth.contract(
                    address=config["bridge_contract"],
                    abi=abi
                )
                
                # Start monitoring
                task = asyncio.create_task(self._monitor_chain(chain))
                self._monitoring_tasks[chain] = task
                
                logger.info(f"Connected to {chain.value} bridge")
                
            except Exception as e:
                logger.error(f"Failed to connect to {chain.value}: {e}")
                
        # Start validator service
        self._validator_task = asyncio.create_task(self._validator_loop())
        
    async def stop(self):
        """Stop bridge services"""
        # Cancel monitoring tasks
        for task in self._monitoring_tasks.values():
            task.cancel()
            
        if self._validator_task:
            self._validator_task.cancel()
            
    async def initiate_bridge(self,
                            user_address: str,
                            source_chain: BridgeNetwork,
                            destination_chain: BridgeNetwork,
                            asset_id: str,
                            amount: Decimal) -> str:
        """Initiate a bridge transaction"""
        # Validate request
        asset = self.supported_assets.get(asset_id)
        if not asset or not asset.is_enabled:
            raise ValueError(f"Asset {asset_id} not supported for bridging")
            
        if amount < asset.min_bridge_amount:
            raise ValueError(f"Amount below minimum: {asset.min_bridge_amount}")
            
        if amount > asset.max_bridge_amount:
            raise ValueError(f"Amount exceeds maximum: {asset.max_bridge_amount}")
            
        if source_chain not in asset.wrapped_contracts:
            raise ValueError(f"Asset not available on {source_chain.value}")
            
        if destination_chain not in asset.wrapped_contracts:
            raise ValueError(f"Asset not available on {destination_chain.value}")
            
        # Calculate fee
        fee_amount = amount * asset.bridge_fee_percentage / 100
        
        # Create bridge transaction
        bridge_id = self._generate_bridge_id(user_address, source_chain, destination_chain, asset_id, amount)
        
        bridge_tx = BridgeTransaction(
            bridge_id=bridge_id,
            user_address=user_address,
            source_chain=source_chain,
            destination_chain=destination_chain,
            asset_address=asset.wrapped_contracts[source_chain],
            amount=amount,
            source_tx_hash=None,
            destination_tx_hash=None,
            status=BridgeStatus.PENDING,
            created_at=datetime.utcnow(),
            completed_at=None,
            fee_amount=fee_amount,
            validator_signatures=[],
            metadata={
                "asset_id": asset_id,
                "net_amount": str(amount - fee_amount),
                "destination_address": asset.wrapped_contracts[destination_chain]
            }
        )
        
        self.bridge_transactions[bridge_id] = bridge_tx
        
        logger.info(f"Bridge initiated: {bridge_id} ({amount} {asset_id} from {source_chain.value} to {destination_chain.value})")
        return bridge_id
        
    async def process_lock_transaction(self, bridge_id: str, tx_hash: str) -> bool:
        """Process asset lock on source chain"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx:
            logger.error(f"Bridge transaction not found: {bridge_id}")
            return False
            
        try:
            # Verify transaction on source chain
            web3 = self.web3_connections[bridge_tx.source_chain]
            tx_receipt = web3.eth.get_transaction_receipt(tx_hash)
            
            if not tx_receipt or tx_receipt.status != 1:
                bridge_tx.status = BridgeStatus.FAILED
                logger.error(f"Lock transaction failed: {tx_hash}")
                return False
                
            # Wait for confirmations
            current_block = web3.eth.block_number
            confirmations = current_block - tx_receipt.blockNumber
            required_confirmations = self.confirmation_blocks[bridge_tx.source_chain]
            
            if confirmations < required_confirmations:
                logger.info(f"Waiting for confirmations: {confirmations}/{required_confirmations}")
                return False
                
            # Update bridge status
            bridge_tx.source_tx_hash = tx_hash
            bridge_tx.status = BridgeStatus.LOCKED
            
            logger.info(f"Assets locked for bridge {bridge_id}: {tx_hash}")
            
            # Request validator signatures
            await self._request_validator_signatures(bridge_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing lock transaction: {e}")
            bridge_tx.status = BridgeStatus.FAILED
            return False
            
    async def mint_on_destination(self, bridge_id: str) -> Optional[str]:
        """Mint assets on destination chain"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx:
            return None
            
        # Check if enough validator signatures
        if len(bridge_tx.validator_signatures) < self.validator_threshold:
            logger.info(f"Insufficient validator signatures: {len(bridge_tx.validator_signatures)}/{self.validator_threshold}")
            return None
            
        try:
            # Prepare mint transaction
            web3 = self.web3_connections[bridge_tx.destination_chain]
            bridge_contract = self.bridge_contracts[bridge_tx.destination_chain]
            
            # Build transaction
            net_amount = Web3.toWei(bridge_tx.amount - bridge_tx.fee_amount, 'ether')
            
            tx_data = bridge_contract.functions.mintBridgedAsset(
                bridge_tx.user_address,
                bridge_tx.metadata["destination_address"],
                net_amount,
                bridge_tx.source_tx_hash,
                bridge_tx.validator_signatures
            ).buildTransaction({
                'from': web3.eth.accounts[0],  # Bridge operator account
                'gas': 500000,
                'gasPrice': web3.eth.gas_price,
                'nonce': web3.eth.get_transaction_count(web3.eth.accounts[0])
            })
            
            # Sign and send transaction
            # In production, use secure key management
            signed_tx = web3.eth.account.sign_transaction(tx_data, private_key='BRIDGE_OPERATOR_PRIVATE_KEY')
            tx_hash = web3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            bridge_tx.destination_tx_hash = tx_hash.hex()
            bridge_tx.status = BridgeStatus.MINTED
            
            logger.info(f"Minted assets on destination chain: {tx_hash.hex()}")
            return tx_hash.hex()
            
        except Exception as e:
            logger.error(f"Error minting on destination: {e}")
            bridge_tx.status = BridgeStatus.FAILED
            return None
            
    async def complete_bridge(self, bridge_id: str) -> bool:
        """Complete bridge transaction after destination confirmation"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx or not bridge_tx.destination_tx_hash:
            return False
            
        try:
            # Verify destination transaction
            web3 = self.web3_connections[bridge_tx.destination_chain]
            tx_receipt = web3.eth.get_transaction_receipt(bridge_tx.destination_tx_hash)
            
            if not tx_receipt or tx_receipt.status != 1:
                bridge_tx.status = BridgeStatus.FAILED
                return False
                
            # Check confirmations
            current_block = web3.eth.block_number
            confirmations = current_block - tx_receipt.blockNumber
            required_confirmations = self.confirmation_blocks[bridge_tx.destination_chain]
            
            if confirmations >= required_confirmations:
                bridge_tx.status = BridgeStatus.COMPLETED
                bridge_tx.completed_at = datetime.utcnow()
                
                # Distribute fees to validators
                await self._distribute_fees(bridge_id)
                
                logger.info(f"Bridge completed: {bridge_id}")
                return True
                
        except Exception as e:
            logger.error(f"Error completing bridge: {e}")
            
        return False
        
    async def get_bridge_status(self, bridge_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a bridge transaction"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx:
            return None
            
        return {
            "bridge_id": bridge_id,
            "status": bridge_tx.status.value,
            "source_chain": bridge_tx.source_chain.value,
            "destination_chain": bridge_tx.destination_chain.value,
            "asset": bridge_tx.metadata.get("asset_id"),
            "amount": str(bridge_tx.amount),
            "fee": str(bridge_tx.fee_amount),
            "net_amount": str(bridge_tx.amount - bridge_tx.fee_amount),
            "source_tx": bridge_tx.source_tx_hash,
            "destination_tx": bridge_tx.destination_tx_hash,
            "created_at": bridge_tx.created_at.isoformat(),
            "completed_at": bridge_tx.completed_at.isoformat() if bridge_tx.completed_at else None,
            "validator_signatures": len(bridge_tx.validator_signatures)
        }
        
    async def get_supported_routes(self) -> List[Dict[str, Any]]:
        """Get all supported bridge routes"""
        routes = []
        
        for asset_id, asset in self.supported_assets.items():
            if not asset.is_enabled:
                continue
                
            chains = list(asset.wrapped_contracts.keys())
            
            for source in chains:
                for destination in chains:
                    if source != destination:
                        routes.append({
                            "asset": asset_id,
                            "source_chain": source.value,
                            "destination_chain": destination.value,
                            "min_amount": str(asset.min_bridge_amount),
                            "max_amount": str(asset.max_bridge_amount),
                            "fee_percentage": str(asset.bridge_fee_percentage),
                            "estimated_time": self._estimate_bridge_time(source, destination)
                        })
                        
        return routes
        
    def _generate_bridge_id(self, user: str, source: BridgeNetwork, dest: BridgeNetwork, asset: str, amount: Decimal) -> str:
        """Generate unique bridge ID"""
        data = f"{user}:{source.value}:{dest.value}:{asset}:{amount}:{datetime.utcnow().timestamp()}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]
        
    def _estimate_bridge_time(self, source: BridgeNetwork, destination: BridgeNetwork) -> int:
        """Estimate bridge time in minutes"""
        # Base time for confirmations
        source_time = self.confirmation_blocks[source] * 15 / 60  # Assuming 15 sec blocks
        dest_time = self.confirmation_blocks[destination] * 15 / 60
        
        # Add processing time
        processing_time = 5  # minutes
        
        return int(source_time + dest_time + processing_time)
        
    async def _monitor_chain(self, chain: BridgeNetwork):
        """Monitor chain for bridge events"""
        while True:
            try:
                await asyncio.sleep(15)  # Check every 15 seconds
                
                # Monitor for lock events
                await self._check_lock_events(chain)
                
                # Monitor for mint confirmations
                await self._check_mint_confirmations(chain)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring {chain.value}: {e}")
                
    async def _check_lock_events(self, chain: BridgeNetwork):
        """Check for new lock events on chain"""
        try:
            contract = self.bridge_contracts.get(chain)
            if not contract:
                return
                
            # Get recent lock events
            # In production, track last processed block
            events = contract.events.AssetLocked().getLogs(fromBlock='latest')
            
            for event in events:
                # Process lock event
                user = event.args.user
                amount = event.args.amount
                tx_hash = event.transactionHash.hex()
                
                # Find corresponding bridge transaction
                for bridge_tx in self.bridge_transactions.values():
                    if (bridge_tx.user_address.lower() == user.lower() and
                        bridge_tx.source_chain == chain and
                        bridge_tx.status == BridgeStatus.PENDING):
                        
                        await self.process_lock_transaction(bridge_tx.bridge_id, tx_hash)
                        break
                        
        except Exception as e:
            logger.error(f"Error checking lock events: {e}")
            
    async def _check_mint_confirmations(self, chain: BridgeNetwork):
        """Check mint transaction confirmations"""
        for bridge_tx in self.bridge_transactions.values():
            if (bridge_tx.destination_chain == chain and
                bridge_tx.status == BridgeStatus.MINTED and
                bridge_tx.destination_tx_hash):
                
                await self.complete_bridge(bridge_tx.bridge_id)
                
    async def _request_validator_signatures(self, bridge_id: str):
        """Request signatures from validator nodes"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx:
            return
            
        # Message to sign
        message = self._create_bridge_message(bridge_tx)
        message_hash = Web3.keccak(text=message)
        
        # Request signatures from active validators
        for validator in self.validator_nodes.values():
            if validator.is_active and bridge_tx.source_chain in validator.chains:
                # In production, send signature request to validator
                # For now, simulate signature
                signature = self._simulate_validator_signature(validator.node_id, message_hash)
                bridge_tx.validator_signatures.append(signature)
                
                if len(bridge_tx.validator_signatures) >= self.validator_threshold:
                    break
                    
    def _create_bridge_message(self, bridge_tx: BridgeTransaction) -> str:
        """Create message for validators to sign"""
        return (
            f"{bridge_tx.bridge_id}:"
            f"{bridge_tx.user_address}:"
            f"{bridge_tx.source_chain.value}:"
            f"{bridge_tx.destination_chain.value}:"
            f"{bridge_tx.asset_address}:"
            f"{bridge_tx.amount}:"
            f"{bridge_tx.source_tx_hash}"
        )
        
    def _simulate_validator_signature(self, validator_id: str, message_hash: bytes) -> str:
        """Simulate validator signature (for testing)"""
        # In production, validators would sign with their private keys
        private_key = f"0x{'0' * 63}{validator_id[-1]}"  # Dummy key
        account = Account.from_key(private_key)
        signature = account.signHash(message_hash)
        return signature.signature.hex()
        
    async def _distribute_fees(self, bridge_id: str):
        """Distribute bridge fees to validators"""
        bridge_tx = self.bridge_transactions.get(bridge_id)
        if not bridge_tx:
            return
            
        # Calculate fee per validator
        participating_validators = len(bridge_tx.validator_signatures)
        if participating_validators == 0:
            return
            
        fee_per_validator = bridge_tx.fee_amount / participating_validators
        
        # In production, execute fee distribution on-chain
        logger.info(f"Distributed {fee_per_validator} to {participating_validators} validators")
        
    async def _validator_loop(self):
        """Main validator service loop"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Process pending bridges
                for bridge_tx in self.bridge_transactions.values():
                    if bridge_tx.status == BridgeStatus.LOCKED:
                        # Check if ready to mint
                        if len(bridge_tx.validator_signatures) >= self.validator_threshold:
                            await self.mint_on_destination(bridge_tx.bridge_id)
                            
                # Clean up old transactions
                await self._cleanup_old_transactions()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in validator loop: {e}")
                
    async def _cleanup_old_transactions(self):
        """Clean up old completed/failed transactions"""
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        to_remove = []
        for bridge_id, bridge_tx in self.bridge_transactions.items():
            if bridge_tx.status in [BridgeStatus.COMPLETED, BridgeStatus.FAILED]:
                if bridge_tx.created_at < cutoff:
                    to_remove.append(bridge_id)
                    
        for bridge_id in to_remove:
            del self.bridge_transactions[bridge_id]
            
    async def add_validator(self, validator: ValidatorNode) -> bool:
        """Add a new validator node"""
        if validator.stake_amount < Decimal("10000"):  # Minimum stake requirement
            logger.error(f"Insufficient stake for validator {validator.node_id}")
            return False
            
        self.validator_nodes[validator.node_id] = validator
        logger.info(f"Added validator {validator.node_id}")
        return True
        
    async def get_bridge_statistics(self) -> Dict[str, Any]:
        """Get bridge statistics"""
        total_volume = Decimal("0")
        total_fees = Decimal("0")
        by_status = {}
        by_route = {}
        
        for bridge_tx in self.bridge_transactions.values():
            total_volume += bridge_tx.amount
            total_fees += bridge_tx.fee_amount
            
            # Count by status
            status = bridge_tx.status.value
            by_status[status] = by_status.get(status, 0) + 1
            
            # Count by route
            route = f"{bridge_tx.source_chain.value}->{bridge_tx.destination_chain.value}"
            if route not in by_route:
                by_route[route] = {"count": 0, "volume": Decimal("0")}
            by_route[route]["count"] += 1
            by_route[route]["volume"] += bridge_tx.amount
            
        return {
            "total_transactions": len(self.bridge_transactions),
            "total_volume": str(total_volume),
            "total_fees_collected": str(total_fees),
            "by_status": by_status,
            "by_route": {k: {"count": v["count"], "volume": str(v["volume"])} for k, v in by_route.items()},
            "active_validators": len([v for v in self.validator_nodes.values() if v.is_active])
        } 