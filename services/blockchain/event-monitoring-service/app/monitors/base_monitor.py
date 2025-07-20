from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import asyncio
import logging
from datetime import datetime

from ..models.event_models import (
    BlockchainEvent, EventStatus, EventType,
    MonitorStatus, ContractABI
)
from ..config import MonitorConfig, EventFilterConfig


class BaseMonitor(ABC):
    """Base class for blockchain event monitors"""
    
    def __init__(
        self,
        chain: str,
        config: MonitorConfig,
        event_queue: asyncio.Queue
    ):
        self.chain = chain
        self.config = config
        self.event_queue = event_queue
        self.logger = logging.getLogger(f"{__name__}.{chain}")
        
        self.current_block = config.start_block or 0
        self.target_block = 0
        self.is_running = False
        self.events_processed = 0
        self.errors: List[str] = []
        self.last_scan_at: Optional[datetime] = None
        
        # Contract ABIs cache
        self.contract_abis: Dict[str, ContractABI] = {}
        
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize monitor connections"""
        pass
    
    @abstractmethod
    async def get_current_block_number(self) -> int:
        """Get current block number from blockchain"""
        pass
    
    @abstractmethod
    async def get_block_timestamp(self, block_number: int) -> datetime:
        """Get timestamp for a specific block"""
        pass
    
    @abstractmethod
    async def scan_block_range(
        self,
        from_block: int,
        to_block: int,
        filters: List[EventFilterConfig]
    ) -> List[BlockchainEvent]:
        """Scan blocks for events matching filters"""
        pass
    
    @abstractmethod
    async def decode_event(
        self,
        raw_event: Dict[str, Any],
        abi: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode event data using ABI"""
        pass
    
    async def start(self) -> None:
        """Start monitoring blockchain events"""
        self.logger.info(f"Starting {self.chain} monitor")
        self.is_running = True
        
        try:
            await self.initialize()
            
            # Get starting block if not set
            if self.current_block == 0:
                self.current_block = await self._get_safe_starting_block()
            
            # Main monitoring loop
            while self.is_running:
                try:
                    await self._monitor_cycle()
                    await asyncio.sleep(self.config.polling_interval)
                except Exception as e:
                    self.logger.error(f"Error in monitor cycle: {e}")
                    self.errors.append(str(e))
                    if len(self.errors) > 100:
                        self.errors = self.errors[-100:]  # Keep last 100 errors
                    await asyncio.sleep(self.config.polling_interval * 2)  # Back off on error
                    
        except Exception as e:
            self.logger.error(f"Fatal error in monitor: {e}")
            raise
        finally:
            self.is_running = False
    
    async def stop(self) -> None:
        """Stop monitoring"""
        self.logger.info(f"Stopping {self.chain} monitor")
        self.is_running = False
    
    async def _monitor_cycle(self) -> None:
        """Single monitoring cycle"""
        # Get current block
        self.target_block = await self.get_current_block_number()
        
        # Calculate safe block based on confirmations
        safe_block = self.target_block - self.config.block_confirmations
        
        if self.current_block >= safe_block:
            # Already caught up
            return
        
        # Calculate batch size
        to_block = min(
            self.current_block + self.config.batch_size,
            safe_block,
            self.current_block + self.config.max_blocks_per_scan
        )
        
        self.logger.info(
            f"Scanning blocks {self.current_block} to {to_block} "
            f"(target: {self.target_block}, behind: {self.target_block - self.current_block})"
        )
        
        # Scan for events
        events = await self.scan_block_range(
            self.current_block,
            to_block,
            self.config.event_filters
        )
        
        # Process events
        for event in events:
            await self.event_queue.put(event)
            self.events_processed += 1
        
        # Update position
        self.current_block = to_block + 1
        self.last_scan_at = datetime.utcnow()
        
        # Save checkpoint
        await self._save_checkpoint()
    
    async def _get_safe_starting_block(self) -> int:
        """Get safe starting block number"""
        current_block = await self.get_current_block_number()
        
        # Start from recent block minus some buffer
        if self.config.process_historical_blocks:
            # Start from configured number of blocks back
            return max(0, current_block - 1000)
        else:
            # Start from current block
            return current_block
    
    async def _save_checkpoint(self) -> None:
        """Save current progress checkpoint"""
        # In production, would save to database or cache
        self.logger.debug(f"Checkpoint: block {self.current_block}")
    
    async def load_contract_abi(
        self,
        contract_address: str,
        abi: Optional[List[Dict[str, Any]]] = None
    ) -> ContractABI:
        """Load and cache contract ABI"""
        address_lower = contract_address.lower()
        
        if address_lower in self.contract_abis:
            return self.contract_abis[address_lower]
        
        if not abi:
            # In production, would fetch from blockchain explorer or database
            abi = []
        
        contract_abi = ContractABI(
            contract_address=address_lower,
            chain=self.chain,
            abi=abi
        )
        
        self.contract_abis[address_lower] = contract_abi
        return contract_abi
    
    def get_status(self) -> MonitorStatus:
        """Get current monitor status"""
        return MonitorStatus(
            chain=self.chain,
            is_active=self.is_running,
            current_block=self.current_block,
            target_block=self.target_block,
            blocks_behind=max(0, self.target_block - self.current_block),
            last_scan_at=self.last_scan_at,
            events_processed=self.events_processed,
            errors=self.errors[-10:]  # Last 10 errors
        )
    
    def classify_event_type(self, event_name: str, decoded_data: Dict[str, Any]) -> EventType:
        """Classify event into standard types"""
        event_name_lower = event_name.lower()
        
        if event_name_lower == "transfer":
            if decoded_data.get("from") == "0x0000000000000000000000000000000000000000":
                return EventType.MINT
            elif decoded_data.get("to") == "0x0000000000000000000000000000000000000000":
                return EventType.BURN
            else:
                return EventType.TRANSFER
        elif event_name_lower == "approval":
            return EventType.APPROVAL
        elif event_name_lower == "swap":
            return EventType.SWAP
        elif "add" in event_name_lower and "liquidity" in event_name_lower:
            return EventType.LIQUIDITY_ADD
        elif "remove" in event_name_lower and "liquidity" in event_name_lower:
            return EventType.LIQUIDITY_REMOVE
        else:
            return EventType.CUSTOM
    
    def create_event_id(
        self,
        chain: str,
        block_number: int,
        tx_hash: str,
        log_index: int
    ) -> str:
        """Create unique event ID"""
        return f"{chain}:{block_number}:{tx_hash}:{log_index}"
    
    async def validate_event_filters(
        self,
        filters: List[EventFilterConfig]
    ) -> List[EventFilterConfig]:
        """Validate and normalize event filters"""
        valid_filters = []
        
        for filter_config in filters:
            # Normalize contract address
            if filter_config.contract_address:
                filter_config.contract_address = filter_config.contract_address.lower()
            
            # Validate ABI
            if not filter_config.abi:
                self.logger.warning(
                    f"No ABI provided for {filter_config.event_name} "
                    f"on {filter_config.contract_address}"
                )
                continue
            
            valid_filters.append(filter_config)
        
        return valid_filters
    
    def matches_filter(
        self,
        event: BlockchainEvent,
        filter_config: EventFilterConfig
    ) -> bool:
        """Check if event matches filter criteria"""
        if filter_config.contract_address:
            if event.contract_address.lower() != filter_config.contract_address.lower():
                return False
        
        if filter_config.event_name:
            if event.event_name != filter_config.event_name:
                return False
        
        if filter_config.topics:
            # Check if topics match
            for i, topic in enumerate(filter_config.topics):
                if topic and i < len(event.topics):
                    if event.topics[i].lower() != topic.lower():
                        return False
        
        return True 