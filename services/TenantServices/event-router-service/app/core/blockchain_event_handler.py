"""
Blockchain Event Handler

Handles routing and processing of blockchain events from various chains.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
import json

from ..event_router import EventRouter
from ..schema_registry import SchemaRegistry
from ..event_store import EventStore

logger = logging.getLogger(__name__)


class BlockchainEventType(Enum):
    """Types of blockchain events"""
    TRANSACTION_CONFIRMED = "transaction.confirmed"
    TRANSACTION_FAILED = "transaction.failed"
    CONTRACT_DEPLOYED = "contract.deployed"
    CONTRACT_CALLED = "contract.called"
    TOKEN_TRANSFER = "token.transfer"
    NFT_MINTED = "nft.minted"
    NFT_TRANSFERRED = "nft.transferred"
    CREDENTIAL_ANCHORED = "credential.anchored"
    CREDENTIAL_REVOKED = "credential.revoked"
    DEFI_LOAN_CREATED = "defi.loan.created"
    DEFI_AUCTION_CREATED = "defi.auction.created"
    DEFI_POOL_CREATED = "defi.pool.created"


class BlockchainEventHandler:
    """Handles blockchain event routing and processing"""
    
    def __init__(
        self,
        event_router: EventRouter,
        schema_registry: SchemaRegistry,
        event_store: EventStore
    ):
        self.event_router = event_router
        self.schema_registry = schema_registry
        self.event_store = event_store
        self._event_mappings: Dict[str, List[str]] = {}
        self._initialize_mappings()
        
    def _initialize_mappings(self):
        """Initialize event type to topic mappings"""
        # Map blockchain events to Pulsar topics
        self._event_mappings = {
            BlockchainEventType.TRANSACTION_CONFIRMED.value: [
                "blockchain.transactions.confirmed",
                "audit.blockchain.transactions"
            ],
            BlockchainEventType.TRANSACTION_FAILED.value: [
                "blockchain.transactions.failed",
                "alerts.blockchain.failures"
            ],
            BlockchainEventType.CONTRACT_DEPLOYED.value: [
                "blockchain.contracts.deployed",
                "audit.blockchain.contracts"
            ],
            BlockchainEventType.CREDENTIAL_ANCHORED.value: [
                "credentials.anchored",
                "audit.credentials.blockchain"
            ],
            BlockchainEventType.CREDENTIAL_REVOKED.value: [
                "credentials.revoked",
                "alerts.credentials.revoked"
            ],
            BlockchainEventType.DEFI_LOAN_CREATED.value: [
                "defi.loans.created",
                "analytics.defi.loans"
            ],
            BlockchainEventType.DEFI_AUCTION_CREATED.value: [
                "defi.auctions.created",
                "analytics.defi.auctions"
            ],
            BlockchainEventType.NFT_MINTED.value: [
                "nfts.minted",
                "marketplace.nfts.new"
            ],
            BlockchainEventType.NFT_TRANSFERRED.value: [
                "nfts.transferred",
                "marketplace.nfts.activity"
            ]
        }
        
    async def handle_blockchain_event(
        self,
        event_type: str,
        chain: str,
        event_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Handle incoming blockchain event.
        
        Args:
            event_type: Type of blockchain event
            chain: Source blockchain
            event_data: Event payload
            
        Returns:
            Processing result
        """
        try:
            # Enrich event with metadata
            enriched_event = {
                "event_type": event_type,
                "chain": chain,
                "timestamp": datetime.utcnow().isoformat(),
                "event_id": f"{chain}_{event_type}_{event_data.get('tx_hash', '')}",
                "data": event_data
            }
            
            # Store event
            await self.event_store.store_event(
                event_id=enriched_event["event_id"],
                event_type=event_type,
                source="blockchain",
                data=enriched_event
            )
            
            # Get target topics for this event type
            target_topics = self._event_mappings.get(
                event_type,
                ["blockchain.events.unknown"]
            )
            
            # Route to appropriate topics
            routing_results = []
            for topic in target_topics:
                result = await self.event_router.route_event(
                    source_topic=f"blockchain.{chain}",
                    target_topic=topic,
                    event=enriched_event
                )
                routing_results.append({
                    "topic": topic,
                    "success": result.get("success", False),
                    "message_id": result.get("message_id")
                })
                
            # Special handling for certain event types
            await self._handle_special_cases(event_type, chain, event_data)
            
            return {
                "success": True,
                "event_id": enriched_event["event_id"],
                "routed_to": routing_results,
                "stored": True
            }
            
        except Exception as e:
            logger.error(f"Error handling blockchain event: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _handle_special_cases(
        self,
        event_type: str,
        chain: str,
        event_data: Dict[str, Any]
    ):
        """Handle special processing for certain event types"""
        
        # Send alerts for failed transactions
        if event_type == BlockchainEventType.TRANSACTION_FAILED.value:
            await self._send_failure_alert(chain, event_data)
            
        # Update analytics for DeFi events
        elif event_type in [
            BlockchainEventType.DEFI_LOAN_CREATED.value,
            BlockchainEventType.DEFI_AUCTION_CREATED.value,
            BlockchainEventType.DEFI_POOL_CREATED.value
        ]:
            await self._update_defi_analytics(event_type, chain, event_data)
            
        # Handle credential events
        elif event_type in [
            BlockchainEventType.CREDENTIAL_ANCHORED.value,
            BlockchainEventType.CREDENTIAL_REVOKED.value
        ]:
            await self._process_credential_event(event_type, chain, event_data)
            
    async def _send_failure_alert(
        self,
        chain: str,
        event_data: Dict[str, Any]
    ):
        """Send alert for failed blockchain transaction"""
        alert = {
            "severity": "warning",
            "chain": chain,
            "transaction_hash": event_data.get("tx_hash"),
            "error": event_data.get("error", "Unknown error"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.event_router.route_event(
            source_topic=f"blockchain.{chain}",
            target_topic="alerts.blockchain.critical",
            event=alert
        )
        
    async def _update_defi_analytics(
        self,
        event_type: str,
        chain: str,
        event_data: Dict[str, Any]
    ):
        """Update DeFi analytics based on events"""
        analytics_event = {
            "event_type": event_type,
            "chain": chain,
            "protocol": event_data.get("protocol", "unknown"),
            "value_usd": event_data.get("value_usd", 0),
            "user": event_data.get("user_address"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        await self.event_router.route_event(
            source_topic=f"blockchain.{chain}",
            target_topic="analytics.defi.aggregate",
            event=analytics_event
        )
        
    async def _process_credential_event(
        self,
        event_type: str,
        chain: str,
        event_data: Dict[str, Any]
    ):
        """Process credential-related blockchain events"""
        credential_event = {
            "event_type": event_type,
            "chain": chain,
            "credential_id": event_data.get("credential_id"),
            "issuer": event_data.get("issuer"),
            "transaction_hash": event_data.get("tx_hash"),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Route to credential service
        await self.event_router.route_event(
            source_topic=f"blockchain.{chain}",
            target_topic="credentials.blockchain.updates",
            event=credential_event
        )
        
    async def get_event_mappings(self) -> Dict[str, List[str]]:
        """Get current event type to topic mappings"""
        return self._event_mappings
        
    async def update_event_mapping(
        self,
        event_type: str,
        topics: List[str]
    ):
        """Update routing for a specific event type"""
        self._event_mappings[event_type] = topics
        logger.info(f"Updated mapping for {event_type}: {topics}")
        
    async def get_blockchain_event_stats(self) -> Dict[str, Any]:
        """Get statistics about blockchain events"""
        # This would query the event store for stats
        return {
            "total_events": 0,
            "events_by_chain": {},
            "events_by_type": {},
            "recent_failures": []
        } 