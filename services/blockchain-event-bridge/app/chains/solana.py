"""
Solana chain adapter
"""

import asyncio
import json
import logging
import base58
from typing import Dict, Any, List, Optional, Callable
from solana.rpc.async_api import AsyncClient
from solana.publickey import PublicKey
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer
from anchorpy import Program, Provider, Wallet
from anchorpy.error import ProgramError

from .base import ChainAdapter, ChainType

logger = logging.getLogger(__name__)


class SolanaAdapter(ChainAdapter):
    """Adapter for Solana blockchain"""
    
    def __init__(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]):
        super().__init__(chain_type, chain_id, config)
        self.client = None
        self.programs = {}
        self.websocket_tasks = {}
        
    async def connect(self) -> bool:
        """Connect to Solana RPC"""
        try:
            self.client = AsyncClient(self.config['rpc_url'])
            
            # Test connection
            result = await self.client.get_version()
            if result["result"]:
                self._connected = True
                logger.info(f"Connected to Solana RPC: {result['result']['solana-core']}")
                return True
            else:
                logger.error("Failed to connect to Solana RPC")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Solana: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Solana"""
        # Cancel all websocket tasks
        for task in self.websocket_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        if self.client:
            await self.client.close()
        
        self._connected = False
        logger.info("Disconnected from Solana")
    
    def load_program(self, program_id: str, idl_path: str, wallet: Optional[Wallet] = None) -> Program:
        """Load an Anchor program"""
        try:
            with open(idl_path) as f:
                idl = json.load(f)
            
            # Create provider
            if wallet is None:
                # Use dummy wallet for read-only operations
                wallet = Wallet.local()
            
            provider = Provider(self.client, wallet)
            
            # Load program
            program = Program(
                idl,
                PublicKey(program_id),
                provider
            )
            
            self.programs[program_id] = program
            return program
            
        except Exception as e:
            logger.error(f"Error loading Solana program: {e}")
            raise
    
    async def subscribe_to_events(self, contract_address: str, event_name: str,
                                 handler: Callable[[Dict[str, Any]], None]) -> bool:
        """Subscribe to program events"""
        try:
            program = self.programs.get(contract_address)
            if not program:
                logger.error(f"Program {contract_address} not loaded")
                return False
            
            # Create websocket subscription for program logs
            async def log_listener():
                async for logs in self.client.logs_subscribe(
                    PublicKey(contract_address),
                    commitment="confirmed"
                ):
                    if logs:
                        event_data = self._parse_program_logs(logs, event_name)
                        if event_data:
                            handler(event_data)
            
            # Start listener task
            task_key = f"{contract_address}:{event_name}"
            self.websocket_tasks[task_key] = asyncio.create_task(log_listener())
            
            logger.info(f"Subscribed to {event_name} events on Solana program {contract_address}")
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to Solana events: {e}")
            return False
    
    def _parse_program_logs(self, logs: Dict[str, Any], event_name: str) -> Optional[Dict[str, Any]]:
        """Parse program logs for events"""
        # This is simplified - real implementation would parse Anchor events
        try:
            if 'logs' in logs:
                for log in logs['logs']:
                    if event_name.lower() in log.lower():
                        return {
                            'event': event_name,
                            'signature': logs.get('signature'),
                            'slot': logs.get('slot'),
                            'logs': logs['logs'],
                            'chainType': self.chain_type.value,
                            'chainId': self.chain_id
                        }
            return None
        except:
            return None
    
    async def get_latest_block(self) -> int:
        """Get latest slot"""
        result = await self.client.get_slot()
        return result["result"]
    
    async def get_reputation_balance(self, address: str) -> int:
        """Get reputation from on-chain program"""
        if 'reputation_program_id' not in self.config:
            return 0
        
        program = self.programs.get(self.config['reputation_program_id'])
        if not program:
            return 0
        
        try:
            # Call reputation view method
            # This depends on your specific program structure
            user_pubkey = PublicKey(address)
            reputation_account = await program.account.UserReputation.fetch(user_pubkey)
            return reputation_account.score if reputation_account else 0
            
        except Exception as e:
            logger.error(f"Error getting Solana reputation: {e}")
            return 0
    
    async def submit_proposal(self, proposal_data: Dict[str, Any]) -> str:
        """Submit proposal to Solana governance program"""
        if 'governance_program_id' not in self.config:
            raise Exception("Governance program not configured")
        
        program = self.programs.get(self.config['governance_program_id'])
        if not program:
            raise Exception("Governance program not loaded")
        
        try:
            # Build instruction for creating proposal
            # This is highly dependent on your program's IDL
            tx = await program.rpc.create_proposal(
                proposal_data['title'],
                proposal_data['description'],
                proposal_data['actions'],  # Program-specific actions
                ctx={
                    'accounts': {
                        'proposer': PublicKey(proposal_data['proposer']),
                        'governance': PublicKey(self.config['governance_account']),
                        # Add other required accounts
                    }
                }
            )
            
            # Return transaction signature
            return str(tx)
            
        except ProgramError as e:
            logger.error(f"Solana program error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error submitting Solana proposal: {e}")
            raise
    
    async def cast_vote(self, proposal_id: str, support: bool,
                       voter_address: str, signature: Optional[str] = None) -> str:
        """Cast vote on Solana proposal"""
        program = self.programs.get(self.config['governance_program_id'])
        if not program:
            raise Exception("Governance program not loaded")
        
        try:
            # Cast vote instruction
            vote_option = 1 if support else 0
            
            tx = await program.rpc.cast_vote(
                PublicKey(proposal_id),
                vote_option,
                ctx={
                    'accounts': {
                        'voter': PublicKey(voter_address),
                        'proposal': PublicKey(proposal_id),
                        'governance': PublicKey(self.config['governance_account']),
                    }
                }
            )
            
            return str(tx)
            
        except Exception as e:
            logger.error(f"Error casting Solana vote: {e}")
            raise
    
    async def get_proposal_state(self, proposal_id: str) -> Dict[str, Any]:
        """Get Solana proposal state"""
        program = self.programs.get(self.config['governance_program_id'])
        if not program:
            raise Exception("Governance program not loaded")
        
        try:
            # Fetch proposal account
            proposal = await program.account.Proposal.fetch(PublicKey(proposal_id))
            
            if not proposal:
                raise Exception(f"Proposal {proposal_id} not found")
            
            return {
                'proposalId': proposal_id,
                'state': proposal.state,  # Depends on your enum
                'forVotes': str(proposal.for_votes),
                'againstVotes': str(proposal.against_votes),
                'abstainVotes': '0',  # If supported
                'startSlot': proposal.voting_start_slot,
                'endSlot': proposal.voting_end_slot
            }
            
        except Exception as e:
            logger.error(f"Error getting Solana proposal state: {e}")
            raise
    
    async def get_voting_power(self, address: str, block_number: Optional[int] = None) -> int:
        """Get voting power on Solana"""
        # In Solana, this might be based on staked tokens or NFTs
        program = self.programs.get(self.config['governance_program_id'])
        if not program:
            return 0
        
        try:
            voter_record = await program.account.VoterRecord.fetch(PublicKey(address))
            return voter_record.voting_power if voter_record else 0
            
        except:
            return 0
    
    async def execute_proposal(self, proposal_id: str) -> str:
        """Execute Solana proposal"""
        program = self.programs.get(self.config['governance_program_id'])
        if not program:
            raise Exception("Governance program not loaded")
        
        try:
            tx = await program.rpc.execute_proposal(
                PublicKey(proposal_id),
                ctx={
                    'accounts': {
                        'proposal': PublicKey(proposal_id),
                        'governance': PublicKey(self.config['governance_account']),
                        # Add execution accounts
                    }
                }
            )
            
            return str(tx)
            
        except Exception as e:
            logger.error(f"Error executing Solana proposal: {e}")
            raise
    
    def validate_address(self, address: str) -> bool:
        """Validate Solana address"""
        try:
            pubkey = PublicKey(address)
            return pubkey.is_on_curve()
        except:
            return False
    
    def format_address(self, address: str) -> str:
        """Format Solana address"""
        try:
            return str(PublicKey(address))
        except:
            return address 