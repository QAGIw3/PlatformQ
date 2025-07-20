from typing import Dict, List, Optional, Any, Tuple
import asyncio
import logging
from datetime import datetime, timedelta
import uuid
import json
from collections import defaultdict

import aiopulsar
from pyignite import AsyncClient as IgniteClient
import httpx

from ..config import config
from ..models.bridge_models import (
    BridgeTransfer, BridgeTransferRequest, BridgeAttestation,
    BridgeEvent, TransferStatus, TokenType, BridgeRoute,
    BridgeStatistics, BridgeHealthStatus, TransferStatusResponse
)
from ..bridges.base_bridge import BaseBridge
from ..bridges.evm_bridge import EVMBridge


class BridgeManager:
    """Manages cross-chain bridge operations"""
    
    def __init__(
        self,
        pulsar_client: aiopulsar.Client,
        ignite_client: IgniteClient,
        key_mgmt_client: httpx.AsyncClient,
        blockchain_client: httpx.AsyncClient,
        tx_processor_client: httpx.AsyncClient
    ):
        self.pulsar_client = pulsar_client
        self.ignite_client = ignite_client
        self.key_mgmt_client = key_mgmt_client
        self.blockchain_client = blockchain_client
        self.tx_processor_client = tx_processor_client
        
        self.logger = logging.getLogger(__name__)
        self.bridges: Dict[str, BaseBridge] = {}
        self.validators: List[str] = []  # Validator addresses
        
        # Event consumers and producers
        self.transfer_consumer: Optional[aiopulsar.Consumer] = None
        self.attestation_consumer: Optional[aiopulsar.Consumer] = None
        self.event_producer: Optional[aiopulsar.Producer] = None
        
        # Background tasks
        self.tasks: List[asyncio.Task] = []
        self._running = False
        
    async def initialize(self) -> None:
        """Initialize bridge manager"""
        self.logger.info("Initializing bridge manager")
        
        # Create Ignite caches
        await self._create_caches()
        
        # Initialize bridges
        await self._initialize_bridges()
        
        # Setup Pulsar consumers/producers
        await self._setup_pulsar()
        
        # Load validator list
        await self._load_validators()
        
        self._running = True
        
        # Start background tasks
        self.tasks.append(asyncio.create_task(self._process_transfers()))
        self.tasks.append(asyncio.create_task(self._process_attestations()))
        self.tasks.append(asyncio.create_task(self._monitor_transfers()))
        self.tasks.append(asyncio.create_task(self._cleanup_expired_transfers()))
        
    async def shutdown(self) -> None:
        """Shutdown bridge manager"""
        self.logger.info("Shutting down bridge manager")
        self._running = False
        
        # Cancel background tasks
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close Pulsar resources
        if self.transfer_consumer:
            await self.transfer_consumer.close()
        if self.attestation_consumer:
            await self.attestation_consumer.close()
        if self.event_producer:
            await self.event_producer.close()
    
    async def _create_caches(self) -> None:
        """Create Ignite caches"""
        # Transfer cache
        self.transfer_cache = await self.ignite_client.get_or_create_cache({
            'name': 'bridge_transfers',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        # Attestation cache
        self.attestation_cache = await self.ignite_client.get_or_create_cache({
            'name': 'bridge_attestations',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        # Event cache
        self.event_cache = await self.ignite_client.get_or_create_cache({
            'name': 'bridge_events',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        # Statistics cache
        self.stats_cache = await self.ignite_client.get_or_create_cache({
            'name': 'bridge_statistics',
            'key_type': 'str',
            'value_type': 'str'
        })
    
    async def _initialize_bridges(self) -> None:
        """Initialize configured bridges"""
        for bridge_config in config.bridges:
            try:
                source_chain_config = config.get_chain(bridge_config.source_chain)
                target_chain_config = config.get_chain(bridge_config.target_chain)
                
                if not source_chain_config or not target_chain_config:
                    self.logger.error(f"Chain config not found for bridge {bridge_config.name}")
                    continue
                
                # Create bridge instance based on chain types
                if self._is_evm_chain(bridge_config.source_chain) and self._is_evm_chain(bridge_config.target_chain):
                    bridge = EVMBridge(
                        source_chain=bridge_config.source_chain,
                        target_chain=bridge_config.target_chain,
                        source_rpc=source_chain_config.rpc_url,
                        target_rpc=target_chain_config.rpc_url,
                        config={
                            'source_bridge_contract': source_chain_config.bridge_contract,
                            'target_bridge_contract': target_chain_config.bridge_contract,
                            'wrapped_tokens': target_chain_config.wrapped_token_contracts,
                            'fee_percentage': bridge_config.fee_percentage,
                            'min_amount': bridge_config.min_amount,
                            'max_amount': bridge_config.max_amount,
                            'relayer_address': bridge_config.relayer_address,
                            'source_confirmations': source_chain_config.confirmations_required,
                            'target_confirmations': target_chain_config.confirmations_required
                        }
                    )
                    
                    await bridge.initialize()
                    self.bridges[bridge_config.name] = bridge
                    self.logger.info(f"Initialized bridge: {bridge_config.name}")
                else:
                    self.logger.warning(f"Bridge type not implemented for {bridge_config.name}")
                    
            except Exception as e:
                self.logger.error(f"Failed to initialize bridge {bridge_config.name}: {e}")
    
    def _is_evm_chain(self, chain: str) -> bool:
        """Check if chain is EVM-compatible"""
        evm_chains = ['ethereum', 'polygon', 'bsc', 'avalanche', 'arbitrum', 'optimism']
        return chain in evm_chains
    
    async def _setup_pulsar(self) -> None:
        """Setup Pulsar consumers and producers"""
        # Transfer request consumer
        self.transfer_consumer = await self.pulsar_client.subscribe(
            config.transfer_requests_topic,
            subscription_name=f"{config.service_name}-transfers",
            consumer_type=aiopulsar.ConsumerType.Shared
        )
        
        # Attestation consumer
        self.attestation_consumer = await self.pulsar_client.subscribe(
            config.attestation_topic,
            subscription_name=f"{config.service_name}-attestations",
            consumer_type=aiopulsar.ConsumerType.Shared
        )
        
        # Event producer
        self.event_producer = await self.pulsar_client.create_producer(
            config.bridge_events_topic
        )
    
    async def _load_validators(self) -> None:
        """Load validator addresses"""
        # In production, would load from configuration or smart contract
        self.validators = [
            "0x1234567890123456789012345678901234567890",
            "0x2345678901234567890123456789012345678901"
        ]
    
    async def initiate_transfer(self, request: BridgeTransferRequest) -> BridgeTransfer:
        """Initiate a new cross-chain transfer"""
        # Validate bridge exists
        bridge = self.bridges.get(request.bridge_name)
        if not bridge:
            raise ValueError(f"Bridge not found: {request.bridge_name}")
        
        # Create transfer record
        transfer = BridgeTransfer(
            transfer_id=str(uuid.uuid4()),
            bridge_name=request.bridge_name,
            source_chain=bridge.source_chain,
            target_chain=bridge.target_chain,
            from_address=request.from_address,
            to_address=request.to_address,
            token_address=request.token_address,
            token_type=request.token_type,
            amount=request.amount,
            fee_amount=bridge.calculate_bridge_fee(request.amount),
            attestations_required=config.min_attestations,
            metadata=request.metadata or {}
        )
        
        # Check bridge limits
        is_valid, error = await bridge.check_bridge_limits(transfer)
        if not is_valid:
            transfer.status = TransferStatus.FAILED
            transfer.error_message = error
            await self._save_transfer(transfer)
            raise ValueError(error)
        
        # Check rate limits
        if not await self._check_rate_limit(request.from_address):
            transfer.status = TransferStatus.FAILED
            transfer.error_message = "Rate limit exceeded"
            await self._save_transfer(transfer)
            raise ValueError("Rate limit exceeded")
        
        # Save transfer
        await self._save_transfer(transfer)
        
        # Emit initial event
        await self._emit_event(bridge.create_bridge_event(
            transfer_id=transfer.transfer_id,
            event_type="transfer_initiated",
            chain=bridge.source_chain,
            data={
                'from': request.from_address,
                'to': request.to_address,
                'amount': request.amount,
                'token': request.token_address
            }
        ))
        
        # Queue for processing
        await self.pulsar_client.create_producer(
            config.transfer_requests_topic
        ).send(transfer.json().encode())
        
        return transfer
    
    async def _process_transfers(self) -> None:
        """Process transfer requests"""
        while self._running:
            try:
                msg = await self.transfer_consumer.receive(timeout_millis=1000)
                
                try:
                    transfer_data = json.loads(msg.data().decode())
                    transfer = BridgeTransfer(**transfer_data)
                    
                    await self._handle_transfer(transfer)
                    await self.transfer_consumer.acknowledge(msg)
                    
                except Exception as e:
                    self.logger.error(f"Error processing transfer: {e}")
                    await self.transfer_consumer.negative_acknowledge(msg)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in transfer processor: {e}")
                await asyncio.sleep(1)
    
    async def _handle_transfer(self, transfer: BridgeTransfer) -> None:
        """Handle a transfer through its lifecycle"""
        bridge = self.bridges.get(transfer.bridge_name)
        if not bridge:
            self.logger.error(f"Bridge not found: {transfer.bridge_name}")
            transfer.status = TransferStatus.FAILED
            transfer.error_message = "Bridge not found"
            await self._save_transfer(transfer)
            return
        
        try:
            if transfer.status == TransferStatus.PENDING:
                # Lock tokens on source chain
                await self._lock_tokens(transfer, bridge)
                
            elif transfer.status == TransferStatus.LOCKED:
                # Wait for attestations
                await self._check_attestations(transfer, bridge)
                
            elif transfer.status == TransferStatus.ATTESTING:
                # Check if we have enough attestations
                attestations = await self._get_attestations(transfer.transfer_id)
                if len(attestations) >= transfer.attestations_required:
                    transfer.status = TransferStatus.MINTING
                    await self._save_transfer(transfer)
                    await self._mint_tokens(transfer, bridge, attestations)
                    
            elif transfer.status == TransferStatus.MINTING:
                # Check mint status
                await self._check_mint_status(transfer, bridge)
                
        except Exception as e:
            self.logger.error(f"Error handling transfer {transfer.transfer_id}: {e}")
            transfer.status = TransferStatus.FAILED
            transfer.error_message = str(e)
            transfer.retry_count += 1
            await self._save_transfer(transfer)
            
            # Emit failure event
            await self._emit_event(bridge.create_bridge_event(
                transfer_id=transfer.transfer_id,
                event_type="transfer_failed",
                chain=bridge.source_chain,
                data={'error': str(e), 'retry_count': transfer.retry_count}
            ))
    
    async def _lock_tokens(self, transfer: BridgeTransfer, bridge: BaseBridge) -> None:
        """Lock tokens on source chain"""
        self.logger.info(f"Locking tokens for transfer {transfer.transfer_id}")
        
        # Get signing key through key management service
        # In production, would properly manage keys
        private_key = "0x0000000000000000000000000000000000000000000000000000000000000001"  # Dummy key
        
        # Execute lock transaction
        tx_hash, tx_data = await bridge.lock_tokens(transfer, private_key)
        
        transfer.lock_tx_hash = tx_hash
        transfer.locked_at = datetime.utcnow()
        transfer.status = TransferStatus.LOCKED
        await self._save_transfer(transfer)
        
        # Emit lock event
        await self._emit_event(bridge.create_bridge_event(
            transfer_id=transfer.transfer_id,
            event_type="tokens_locked",
            chain=bridge.source_chain,
            data=tx_data,
            tx_hash=tx_hash
        ))
        
        # Create attestation for this lock if we're a validator
        if await self._is_validator():
            await self._create_attestation(transfer, bridge)
    
    async def _create_attestation(self, transfer: BridgeTransfer, bridge: BaseBridge) -> None:
        """Create attestation for a locked transfer"""
        try:
            # Get validator key
            validator_key = "0x0000000000000000000000000000000000000000000000000000000000000002"  # Dummy key
            
            attestation = await bridge.create_attestation(
                transfer,
                transfer.lock_tx_hash,
                validator_key
            )
            
            # Save attestation
            await self.attestation_cache.put(
                f"{transfer.transfer_id}:{attestation.attestation_id}",
                attestation.json()
            )
            
            # Publish attestation
            await self.pulsar_client.create_producer(
                config.attestation_topic
            ).send(attestation.json().encode())
            
            self.logger.info(f"Created attestation for transfer {transfer.transfer_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to create attestation: {e}")
    
    async def _process_attestations(self) -> None:
        """Process incoming attestations"""
        while self._running:
            try:
                msg = await self.attestation_consumer.receive(timeout_millis=1000)
                
                try:
                    attestation_data = json.loads(msg.data().decode())
                    attestation = BridgeAttestation(**attestation_data)
                    
                    await self._handle_attestation(attestation)
                    await self.attestation_consumer.acknowledge(msg)
                    
                except Exception as e:
                    self.logger.error(f"Error processing attestation: {e}")
                    await self.attestation_consumer.negative_acknowledge(msg)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in attestation processor: {e}")
                await asyncio.sleep(1)
    
    async def _handle_attestation(self, attestation: BridgeAttestation) -> None:
        """Handle incoming attestation"""
        # Load transfer
        transfer = await self._load_transfer(attestation.transfer_id)
        if not transfer:
            self.logger.error(f"Transfer not found for attestation: {attestation.transfer_id}")
            return
        
        bridge = self.bridges.get(transfer.bridge_name)
        if not bridge:
            return
        
        # Verify attestation
        if not await bridge.verify_attestation(attestation, transfer):
            self.logger.error(f"Invalid attestation for transfer {transfer.transfer_id}")
            return
        
        # Save attestation
        await self.attestation_cache.put(
            f"{transfer.transfer_id}:{attestation.attestation_id}",
            attestation.json()
        )
        
        # Update transfer
        if attestation.attestation_id not in transfer.attestation_ids:
            transfer.attestation_ids.append(attestation.attestation_id)
            transfer.attestations_received = len(transfer.attestation_ids)
            
            if transfer.attestations_received >= transfer.attestations_required:
                transfer.status = TransferStatus.ATTESTING
                
            await self._save_transfer(transfer)
            
            # Emit attestation event
            await self._emit_event(bridge.create_bridge_event(
                transfer_id=transfer.transfer_id,
                event_type="attestation_received",
                chain=bridge.source_chain,
                data={
                    'attestation_id': attestation.attestation_id,
                    'validator': attestation.validator_address,
                    'total_attestations': transfer.attestations_received
                }
            ))
    
    async def _mint_tokens(
        self,
        transfer: BridgeTransfer,
        bridge: BaseBridge,
        attestations: List[BridgeAttestation]
    ) -> None:
        """Mint tokens on target chain"""
        self.logger.info(f"Minting tokens for transfer {transfer.transfer_id}")
        
        # Get relayer key
        relayer_key = "0x0000000000000000000000000000000000000000000000000000000000000003"  # Dummy key
        
        # Execute mint transaction
        tx_hash, tx_data = await bridge.mint_tokens(transfer, attestations, relayer_key)
        
        transfer.mint_tx_hash = tx_hash
        transfer.status = TransferStatus.MINTING
        await self._save_transfer(transfer)
        
        # Emit mint event
        await self._emit_event(bridge.create_bridge_event(
            transfer_id=transfer.transfer_id,
            event_type="tokens_minting",
            chain=bridge.target_chain,
            data=tx_data,
            tx_hash=tx_hash
        ))
    
    async def _check_attestations(self, transfer: BridgeTransfer, bridge: BaseBridge) -> None:
        """Check if transfer has received enough attestations"""
        attestations = await self._get_attestations(transfer.transfer_id)
        
        if len(attestations) >= transfer.attestations_required:
            transfer.status = TransferStatus.ATTESTING
            await self._save_transfer(transfer)
            
            # Process can continue
            await self._handle_transfer(transfer)
    
    async def _check_mint_status(self, transfer: BridgeTransfer, bridge: BaseBridge) -> None:
        """Check status of mint transaction"""
        if not transfer.mint_tx_hash:
            return
        
        is_valid, tx_details = await bridge.verify_mint_transaction(
            transfer.mint_tx_hash,
            transfer
        )
        
        if is_valid and tx_details.get('confirmations', 0) >= await bridge.get_confirmations_required(bridge.target_chain):
            transfer.status = TransferStatus.COMPLETED
            transfer.completed_at = datetime.utcnow()
            await self._save_transfer(transfer)
            
            # Emit completion event
            await self._emit_event(bridge.create_bridge_event(
                transfer_id=transfer.transfer_id,
                event_type="transfer_completed",
                chain=bridge.target_chain,
                data={
                    'duration_seconds': (transfer.completed_at - transfer.created_at).total_seconds()
                }
            ))
            
            # Update statistics
            await self._update_statistics(transfer)
    
    async def _monitor_transfers(self) -> None:
        """Monitor active transfers"""
        while self._running:
            try:
                # Get all active transfers
                active_statuses = [
                    TransferStatus.PENDING,
                    TransferStatus.LOCKED,
                    TransferStatus.ATTESTING,
                    TransferStatus.MINTING
                ]
                
                # In production, would query from database
                # For now, check cached transfers
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error monitoring transfers: {e}")
                await asyncio.sleep(30)
    
    async def _cleanup_expired_transfers(self) -> None:
        """Cleanup expired transfers"""
        while self._running:
            try:
                # Check for expired transfers every hour
                await asyncio.sleep(3600)
                
                # Would implement cleanup logic here
                
            except Exception as e:
                self.logger.error(f"Error in cleanup task: {e}")
    
    async def get_transfer_status(self, transfer_id: str) -> Optional[TransferStatusResponse]:
        """Get detailed transfer status"""
        transfer = await self._load_transfer(transfer_id)
        if not transfer:
            return None
        
        # Get events
        events = await self._get_transfer_events(transfer_id)
        
        # Calculate estimated completion
        estimated_completion = None
        if transfer.status in [TransferStatus.PENDING, TransferStatus.LOCKED, TransferStatus.ATTESTING]:
            # Rough estimate based on average times
            remaining_seconds = 300  # 5 minutes average
            estimated_completion = datetime.utcnow() + timedelta(seconds=remaining_seconds)
        
        # Determine next action
        next_action = None
        if transfer.status == TransferStatus.PENDING:
            next_action = "Waiting for token lock transaction"
        elif transfer.status == TransferStatus.LOCKED:
            next_action = f"Waiting for attestations ({transfer.attestations_received}/{transfer.attestations_required})"
        elif transfer.status == TransferStatus.ATTESTING:
            next_action = "Preparing to mint tokens"
        elif transfer.status == TransferStatus.MINTING:
            next_action = "Waiting for mint confirmation"
        
        return TransferStatusResponse(
            transfer=transfer,
            events=events,
            estimated_completion_time=estimated_completion,
            next_action=next_action
        )
    
    async def get_bridge_routes(self) -> List[BridgeRoute]:
        """Get available bridge routes"""
        routes = []
        
        for bridge_config in config.bridges:
            bridge = self.bridges.get(bridge_config.name)
            if not bridge:
                continue
            
            # Get supported tokens (simplified)
            supported_tokens = [
                {'symbol': 'ETH', 'address': None, 'name': 'Ethereum'},
                {'symbol': 'USDC', 'address': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 'name': 'USD Coin'},
                {'symbol': 'USDT', 'address': '0xdAC17F958D2ee523a2206206994597C13D831ec7', 'name': 'Tether'}
            ]
            
            route = BridgeRoute(
                name=bridge_config.name,
                source_chain=bridge_config.source_chain,
                target_chain=bridge_config.target_chain,
                supported_tokens=supported_tokens,
                fee_percentage=bridge_config.fee_percentage,
                min_amount=str(bridge_config.min_amount),
                max_amount=str(bridge_config.max_amount),
                estimated_time_seconds=300,  # 5 minutes estimate
                is_active=True
            )
            routes.append(route)
        
        return routes
    
    async def get_bridge_statistics(self, bridge_name: str) -> Optional[BridgeStatistics]:
        """Get bridge statistics"""
        # Load from cache
        stats_data = await self.stats_cache.get(bridge_name)
        if stats_data:
            return BridgeStatistics(**json.loads(stats_data))
        
        # Return empty stats
        return BridgeStatistics(bridge_name=bridge_name)
    
    async def get_bridge_health(self, bridge_name: str) -> Optional[BridgeHealthStatus]:
        """Get bridge health status"""
        bridge = self.bridges.get(bridge_name)
        if not bridge:
            return None
        
        # Check chain connections
        source_connected = True  # Would actually check
        target_connected = True
        
        # Check relayer status
        relayer_status = "active"  # Would check balance and activity
        
        # Count pending transfers
        pending_count = 0  # Would query from database
        
        # Get last successful transfer
        last_success = None  # Would query from database
        
        issues = []
        if not source_connected:
            issues.append("Source chain connection issue")
        if not target_connected:
            issues.append("Target chain connection issue")
        
        return BridgeHealthStatus(
            bridge_name=bridge_name,
            is_operational=source_connected and target_connected,
            source_chain_connected=source_connected,
            target_chain_connected=target_connected,
            relayer_status=relayer_status,
            pending_transfers=pending_count,
            last_successful_transfer=last_success,
            issues=issues
        )
    
    # Helper methods
    
    async def _save_transfer(self, transfer: BridgeTransfer) -> None:
        """Save transfer to cache"""
        await self.transfer_cache.put(transfer.transfer_id, transfer.json())
    
    async def _load_transfer(self, transfer_id: str) -> Optional[BridgeTransfer]:
        """Load transfer from cache"""
        data = await self.transfer_cache.get(transfer_id)
        if data:
            return BridgeTransfer(**json.loads(data))
        return None
    
    async def _get_attestations(self, transfer_id: str) -> List[BridgeAttestation]:
        """Get attestations for a transfer"""
        attestations = []
        # In production, would query from database
        # For now, scan cache keys
        return attestations
    
    async def _get_transfer_events(self, transfer_id: str) -> List[BridgeEvent]:
        """Get events for a transfer"""
        events = []
        # In production, would query from database
        return events
    
    async def _emit_event(self, event: BridgeEvent) -> None:
        """Emit bridge event"""
        await self.event_producer.send(event.json().encode())
        # Also save to cache
        await self.event_cache.put(
            f"{event.transfer_id}:{event.event_id}",
            event.json()
        )
    
    async def _check_rate_limit(self, address: str) -> bool:
        """Check if address is within rate limits"""
        # Simple rate limiting
        key = f"rate_limit:{address}"
        count = await self.transfer_cache.get(key)
        
        if count:
            current_count = int(count)
            if current_count >= config.rate_limit_per_address:
                return False
        else:
            current_count = 0
        
        # Increment counter with TTL
        await self.transfer_cache.put(
            key,
            str(current_count + 1),
            ttl=config.rate_limit_window_seconds
        )
        
        return True
    
    async def _is_validator(self) -> bool:
        """Check if this node is a validator"""
        # In production, would check properly
        return True
    
    async def _update_statistics(self, transfer: BridgeTransfer) -> None:
        """Update bridge statistics"""
        stats = await self.get_bridge_statistics(transfer.bridge_name)
        if not stats:
            stats = BridgeStatistics(bridge_name=transfer.bridge_name)
        
        stats.total_transfers += 1
        if transfer.status == TransferStatus.COMPLETED:
            stats.successful_transfers += 1
            # Update volume
            current_volume = int(stats.total_volume)
            current_volume += int(transfer.amount)
            stats.total_volume = str(current_volume)
            
            # Update completion time
            if transfer.completed_at and transfer.created_at:
                duration = (transfer.completed_at - transfer.created_at).total_seconds()
                # Simple average calculation
                if stats.average_completion_time_seconds > 0:
                    stats.average_completion_time_seconds = (
                        stats.average_completion_time_seconds + duration
                    ) / 2
                else:
                    stats.average_completion_time_seconds = duration
        elif transfer.status == TransferStatus.FAILED:
            stats.failed_transfers += 1
        
        # Save updated stats
        await self.stats_cache.put(transfer.bridge_name, stats.json()) 