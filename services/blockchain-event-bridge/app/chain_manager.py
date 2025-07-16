"""
Chain manager for multi-chain support
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional

from pyignite import Client as IgniteClient
import pulsar

from .chains import (
    ChainAdapter, ChainType, 
    EthereumAdapter, SolanaAdapter, HyperledgerAdapter,
    CosmosAdapter, NEARAdapter, AvalancheAdapter
)
from .voting import (
    VotingStrategy, VotingMechanism,
    QuadraticVoting, TimeWeightedVoting, ConvictionVoting, VoteDelegation
)
from .execution import CrossChainExecutor, ExecutionStrategyFactory

logger = logging.getLogger(__name__)


class ChainManager:
    """
    Manages multiple blockchain connections and cross-chain operations
    """
    
    def __init__(self, ignite_client: IgniteClient, pulsar_client: pulsar.Client):
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        self.chains: Dict[str, ChainAdapter] = {}
        self.voting_strategies: Dict[VotingMechanism, VotingStrategy] = {}
        self.executor = None
        
        # Initialize Pulsar producer
        self.pulsar_producer = pulsar_client.create_producer(
            'persistent://public/default/blockchain-events'
        )
        
        # Initialize caches
        self._init_caches()
        
    def _init_caches(self):
        """Initialize Ignite caches"""
        # Cross-chain proposals cache
        self.ignite_client.get_or_create_cache('crossChainProposals')
        
        # Reputation cache
        self.ignite_client.get_or_create_cache('crossChainReputation')
        
        # Vote aggregation cache
        self.ignite_client.get_or_create_cache('voteAggregations')
        
        # Execution plans cache
        self.ignite_client.get_or_create_cache('executionPlans')
        
    async def add_chain(self, chain_id: str, chain_type: ChainType, config: Dict[str, Any]):
        """Add a new chain to the manager"""
        adapter = self._create_adapter(chain_type, chain_id, config)
        
        if await adapter.connect():
            self.chains[chain_id] = adapter
            logger.info(f"Successfully added chain {chain_id} ({chain_type.value})")
            
            # Subscribe to governance events
            await self._subscribe_to_governance_events(adapter, chain_id)
        else:
            logger.error(f"Failed to connect to chain {chain_id}")
            
    def _create_adapter(self, chain_type: ChainType, chain_id: str, config: Dict[str, Any]) -> ChainAdapter:
        """Create appropriate adapter for chain type"""
        if chain_type == ChainType.ETHEREUM:
            return EthereumAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.POLYGON:
            return EthereumAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.ARBITRUM:
            return EthereumAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.SOLANA:
            return SolanaAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.HYPERLEDGER:
            return HyperledgerAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.COSMOS:
            return CosmosAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.NEAR:
            return NEARAdapter(chain_type, chain_id, config)
        elif chain_type == ChainType.AVALANCHE:
            return AvalancheAdapter(chain_type, chain_id, config)
        else:
            raise ValueError(f"Unsupported chain type: {chain_type}")
            
    async def _subscribe_to_governance_events(self, adapter: ChainAdapter, chain_id: str):
        """Subscribe to governance events on a chain"""
        # Define event handlers
        async def handle_proposal_created(event: Dict[str, Any]):
            event['chainId'] = chain_id
            await self._handle_cross_chain_event('ProposalCreated', event)
            
        async def handle_vote_cast(event: Dict[str, Any]):
            event['chainId'] = chain_id
            await self._handle_cross_chain_event('VoteCast', event)
            
        async def handle_proposal_executed(event: Dict[str, Any]):
            event['chainId'] = chain_id
            await self._handle_cross_chain_event('ProposalExecuted', event)
            
        # Subscribe to events (contract addresses from config)
        governance_contract = adapter.config.get('governance_contract')
        if governance_contract:
            await adapter.subscribe_to_events(governance_contract, 'ProposalCreated', handle_proposal_created)
            await adapter.subscribe_to_events(governance_contract, 'VoteCast', handle_vote_cast)
            await adapter.subscribe_to_events(governance_contract, 'ProposalExecuted', handle_proposal_executed)
            
    async def _handle_cross_chain_event(self, event_type: str, event_data: Dict[str, Any]):
        """Handle cross-chain governance events"""
        # Store in Ignite
        cache_name = f"{event_type}Events"
        cache = self.ignite_client.get_or_create_cache(cache_name)
        
        event_id = f"{event_data['chainId']}_{event_data.get('proposalId', event_data.get('txHash'))}"
        cache.put(event_id, event_data)
        
        # Publish to Pulsar
        import json
        self.pulsar_producer.send(json.dumps({
            'eventType': event_type,
            'data': event_data
        }).encode('utf-8'))
        
        # Trigger aggregation if needed
        if event_type == 'VoteCast':
            await self._aggregate_cross_chain_votes(event_data.get('proposalId'))
            
    async def _aggregate_cross_chain_votes(self, proposal_id: str):
        """Aggregate votes across all chains for a proposal"""
        vote_cache = self.ignite_client.get_cache('VoteCastEvents')
        aggregation_cache = self.ignite_client.get_cache('voteAggregations')
        
        # Get all votes for this proposal
        # In production, use Ignite SQL queries
        all_votes = []  # Would query vote_cache
        
        # Aggregate by chain
        chain_aggregations = {}
        for vote in all_votes:
            chain_id = vote['chainId']
            if chain_id not in chain_aggregations:
                chain_aggregations[chain_id] = {
                    'forVotes': 0,
                    'againstVotes': 0,
                    'totalVoters': 0
                }
                
            if vote['support']:
                chain_aggregations[chain_id]['forVotes'] += vote['weight']
            else:
                chain_aggregations[chain_id]['againstVotes'] += vote['weight']
            chain_aggregations[chain_id]['totalVoters'] += 1
            
        # Store aggregation
        aggregation_cache.put(proposal_id, {
            'proposalId': proposal_id,
            'chainAggregations': chain_aggregations,
            'lastUpdated': asyncio.get_event_loop().time()
        })
        
    async def synchronize_reputation(self, address: str):
        """Synchronize reputation across all chains"""
        reputation_cache = self.ignite_client.get_cache('crossChainReputation')
        
        # Get reputation from all chains
        reputation_by_chain = {}
        
        for chain_id, adapter in self.chains.items():
            try:
                reputation = await adapter.get_reputation_balance(address)
                reputation_by_chain[chain_id] = reputation
            except Exception as e:
                logger.error(f"Failed to get reputation from {chain_id}: {e}")
                
        # Calculate aggregate reputation
        total_reputation = sum(reputation_by_chain.values())
        avg_reputation = total_reputation // len(reputation_by_chain) if reputation_by_chain else 0
        
        # Store in cache
        reputation_cache.put(address, {
            'address': address,
            'chainReputation': reputation_by_chain,
            'totalReputation': total_reputation,
            'averageReputation': avg_reputation,
            'lastSynced': asyncio.get_event_loop().time()
        })
        
        # Publish sync event
        import json
        self.pulsar_producer.send(json.dumps({
            'eventType': 'ReputationSynced',
            'data': {
                'address': address,
                'totalReputation': total_reputation,
                'chains': list(reputation_by_chain.keys())
            }
        }).encode('utf-8'))
        
        return reputation_by_chain
        
    async def remove_chain(self, chain_id: str):
        """Remove a chain from the manager"""
        if chain_id in self.chains:
            await self.chains[chain_id].disconnect()
            del self.chains[chain_id]
            logger.info(f"Removed chain {chain_id}")
            
    async def close(self):
        """Close all chain connections"""
        for chain_id, adapter in self.chains.items():
            await adapter.disconnect()
        self.chains.clear()
        
        if self.executor:
            await self.executor.stop()
        
        self.pulsar_producer.close()
        logger.info("Chain manager closed")
        
    def add_voting_strategy(self, mechanism: VotingMechanism, strategy: VotingStrategy):
        """Add a voting strategy"""
        self.voting_strategies[mechanism] = strategy
        logger.info(f"Added voting strategy: {mechanism.value}")
        
    def init_voting_strategies(self, config: Dict[str, Any]):
        """Initialize voting strategies from config"""
        # Quadratic voting
        if config.get('enable_quadratic_voting', True):
            self.add_voting_strategy(
                VotingMechanism.QUADRATIC,
                QuadraticVoting(config.get('quadratic_voting', {}))
            )
            
        # Time-weighted voting  
        if config.get('enable_time_weighted_voting', True):
            self.add_voting_strategy(
                VotingMechanism.TIME_WEIGHTED,
                TimeWeightedVoting(config.get('time_weighted_voting', {}))
            )
            
        # Conviction voting
        if config.get('enable_conviction_voting', True):
            self.add_voting_strategy(
                VotingMechanism.CONVICTION,
                ConvictionVoting(config.get('conviction_voting', {}))
            )
            
        # Delegation
        if config.get('enable_delegation', True):
            self.add_voting_strategy(
                VotingMechanism.DELEGATION,
                VoteDelegation(config.get('delegation', {}))
            )
            
    def init_executor(self, config: Dict[str, Any]):
        """Initialize cross-chain executor"""
        self.executor = CrossChainExecutor(
            self,
            self.ignite_client,
            config.get('executor', {})
        )
        
    async def start_executor(self):
        """Start the cross-chain executor"""
        if self.executor:
            await self.executor.start()
            
    async def schedule_execution(self, proposal_id: str, strategy_type: str = None):
        """Schedule a proposal for execution"""
        if not self.executor:
            raise Exception("Executor not initialized")
            
        # Get or create execution strategy
        if strategy_type:
            strategy = ExecutionStrategyFactory.create_strategy(
                strategy_type,
                {}  # Use defaults
            )
        else:
            # Determine from proposal type
            proposal_cache = self.ignite_client.get_cache('crossChainProposals')
            proposal = proposal_cache.get(proposal_id)
            proposal_type = proposal.get('proposalType', 'general')
            strategy = ExecutionStrategyFactory.get_default_strategy(proposal_type)
            
        return await self.executor.schedule_execution(proposal_id, strategy) 