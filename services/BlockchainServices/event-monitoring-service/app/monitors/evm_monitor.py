from typing import List, Dict, Any, Optional
from web3 import Web3
from web3.types import LogReceipt, FilterParams
from eth_abi import decode_abi
from eth_utils import encode_hex, event_abi_to_log_topic
import asyncio
from datetime import datetime
import uuid

from .base_monitor import BaseMonitor
from ..models.event_models import BlockchainEvent, EventStatus
from ..config import EventFilterConfig


class EVMMonitor(BaseMonitor):
    """Monitor for EVM-compatible blockchains"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.w3: Optional[Web3] = None
        
    async def initialize(self) -> None:
        """Initialize Web3 connection"""
        self.logger.info(f"Initializing EVM monitor for {self.chain}")
        
        # Create Web3 instance
        self.w3 = Web3(Web3.HTTPProvider(self.config.rpc_url))
        
        # Wait for connection
        connected = await self._wait_for_connection()
        if not connected:
            raise Exception(f"Failed to connect to {self.chain} RPC")
        
        # Validate event filters
        self.config.event_filters = await self.validate_event_filters(
            self.config.event_filters
        )
        
        self.logger.info(f"EVM monitor initialized for {self.chain}")
    
    async def _wait_for_connection(self, timeout: int = 30) -> bool:
        """Wait for Web3 connection"""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            if self.w3.isConnected():
                chain_id = self.w3.eth.chain_id
                self.logger.info(f"Connected to {self.chain} (chain_id: {chain_id})")
                return True
            await asyncio.sleep(1)
        
        return False
    
    async def get_current_block_number(self) -> int:
        """Get current block number"""
        return self.w3.eth.block_number
    
    async def get_block_timestamp(self, block_number: int) -> datetime:
        """Get block timestamp"""
        block = self.w3.eth.get_block(block_number)
        return datetime.fromtimestamp(block['timestamp'])
    
    async def scan_block_range(
        self,
        from_block: int,
        to_block: int,
        filters: List[EventFilterConfig]
    ) -> List[BlockchainEvent]:
        """Scan block range for events"""
        all_events = []
        
        for filter_config in filters:
            try:
                events = await self._scan_with_filter(
                    from_block,
                    to_block,
                    filter_config
                )
                all_events.extend(events)
            except Exception as e:
                self.logger.error(
                    f"Error scanning with filter {filter_config.event_name}: {e}"
                )
        
        # Sort by block number and log index
        all_events.sort(key=lambda e: (e.block_number, e.transaction_index, e.log_index))
        
        return all_events
    
    async def _scan_with_filter(
        self,
        from_block: int,
        to_block: int,
        filter_config: EventFilterConfig
    ) -> List[BlockchainEvent]:
        """Scan using a specific filter"""
        events = []
        
        # Build filter parameters
        filter_params: FilterParams = {
            'fromBlock': from_block,
            'toBlock': to_block
        }
        
        if filter_config.contract_address:
            filter_params['address'] = Web3.toChecksumAddress(
                filter_config.contract_address
            )
        
        # Add event signature to topics
        if filter_config.event_name and filter_config.abi:
            event_abi = self._find_event_abi(
                filter_config.abi,
                filter_config.event_name
            )
            if event_abi:
                event_signature = event_abi_to_log_topic(event_abi)
                filter_params['topics'] = [encode_hex(event_signature)]
                
                # Add additional topic filters if specified
                if filter_config.topics:
                    filter_params['topics'].extend(filter_config.topics)
        
        # Get logs
        logs = self.w3.eth.get_logs(filter_params)
        
        # Process logs
        for log in logs:
            try:
                event = await self._process_log(log, filter_config)
                if event:
                    events.append(event)
            except Exception as e:
                self.logger.error(f"Error processing log: {e}")
        
        return events
    
    async def _process_log(
        self,
        log: LogReceipt,
        filter_config: EventFilterConfig
    ) -> Optional[BlockchainEvent]:
        """Process a single log entry"""
        try:
            # Get block timestamp
            timestamp = await self.get_block_timestamp(log['blockNumber'])
            
            # Create event ID
            event_id = self.create_event_id(
                self.chain,
                log['blockNumber'],
                log['transactionHash'].hex(),
                log['logIndex']
            )
            
            # Extract basic info
            event = BlockchainEvent(
                event_id=event_id,
                chain=self.chain,
                block_number=log['blockNumber'],
                block_hash=log['blockHash'].hex(),
                transaction_hash=log['transactionHash'].hex(),
                transaction_index=log['transactionIndex'],
                log_index=log['logIndex'],
                contract_address=log['address'].lower(),
                event_name=filter_config.event_name or "Unknown",
                topics=[encode_hex(topic) for topic in log['topics']],
                data=encode_hex(log['data']),
                timestamp=timestamp,
                status=EventStatus.PENDING
            )
            
            # Decode event data if ABI is available
            if filter_config.abi and filter_config.event_name:
                event_abi = self._find_event_abi(
                    filter_config.abi,
                    filter_config.event_name
                )
                if event_abi:
                    decoded_data = await self.decode_event(log, event_abi)
                    event.decoded_data = decoded_data
                    event.event_type = self.classify_event_type(
                        filter_config.event_name,
                        decoded_data
                    )
            
            return event
            
        except Exception as e:
            self.logger.error(f"Error processing log: {e}")
            return None
    
    async def decode_event(
        self,
        log: LogReceipt,
        event_abi: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Decode event data"""
        decoded = {}
        
        try:
            # Decode indexed parameters (topics)
            indexed_inputs = [
                input_param for input_param in event_abi['inputs']
                if input_param.get('indexed')
            ]
            
            # First topic is event signature, skip it
            for i, input_param in enumerate(indexed_inputs):
                if i + 1 < len(log['topics']):
                    topic_data = log['topics'][i + 1]
                    param_name = input_param['name']
                    param_type = input_param['type']
                    
                    # Decode based on type
                    if param_type == 'address':
                        decoded[param_name] = '0x' + topic_data.hex()[-40:]
                    elif param_type in ['uint256', 'int256']:
                        decoded[param_name] = int(topic_data.hex(), 16)
                    else:
                        decoded[param_name] = encode_hex(topic_data)
            
            # Decode non-indexed parameters (data)
            non_indexed_inputs = [
                input_param for input_param in event_abi['inputs']
                if not input_param.get('indexed')
            ]
            
            if non_indexed_inputs and log['data']:
                types = [input_param['type'] for input_param in non_indexed_inputs]
                names = [input_param['name'] for input_param in non_indexed_inputs]
                
                # Decode data
                decoded_values = decode_abi(types, log['data'])
                
                for name, value in zip(names, decoded_values):
                    if isinstance(value, bytes):
                        decoded[name] = encode_hex(value)
                    else:
                        decoded[name] = value
            
        except Exception as e:
            self.logger.error(f"Error decoding event: {e}")
            
        return decoded
    
    def _find_event_abi(
        self,
        abi: List[Dict[str, Any]],
        event_name: str
    ) -> Optional[Dict[str, Any]]:
        """Find event ABI by name"""
        for item in abi:
            if item.get('type') == 'event' and item.get('name') == event_name:
                return item
        return None
    
    async def get_transaction_details(
        self,
        tx_hash: str
    ) -> Optional[Dict[str, Any]]:
        """Get transaction details"""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            
            return {
                'from': tx['from'],
                'to': tx['to'],
                'value': str(tx['value']),
                'gas_used': receipt['gasUsed'],
                'gas_price': str(tx.get('gasPrice', 0)),
                'status': receipt['status']
            }
        except Exception as e:
            self.logger.error(f"Error getting transaction details: {e}")
            return None
    
    async def estimate_scan_time(
        self,
        from_block: int,
        to_block: int
    ) -> float:
        """Estimate time to scan block range"""
        blocks = to_block - from_block + 1
        
        # Rough estimate: 100 blocks per second
        return blocks / 100.0
    
    async def get_contract_code(self, address: str) -> Optional[str]:
        """Get contract bytecode"""
        try:
            code = self.w3.eth.get_code(Web3.toChecksumAddress(address))
            return encode_hex(code)
        except Exception:
            return None
    
    def is_contract(self, address: str) -> bool:
        """Check if address is a contract"""
        try:
            code = self.w3.eth.get_code(Web3.toChecksumAddress(address))
            return len(code) > 0
        except Exception:
            return False 