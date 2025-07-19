"""
Vote delegation implementation

Allows users to delegate their voting power to trusted representatives,
enabling liquid democracy and reducing voter fatigue.
"""

from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import networkx as nx

from .strategy import VotingStrategy, VotingMechanism


class VoteDelegation(VotingStrategy):
    """
    Vote delegation mechanism where:
    - Users can delegate voting power to representatives
    - Supports transitive delegation (A -> B -> C)
    - Prevents circular delegation
    - Allows topic-specific delegation
    - Representatives can sub-delegate with reduced weight
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mechanism = VotingMechanism.DELEGATION
        
        # Delegation parameters
        self.max_delegation_depth = config.get('max_delegation_depth', 5)
        self.delegation_decay = config.get('delegation_decay', 0.9)  # Power reduction per hop
        self.allow_sub_delegation = config.get('allow_sub_delegation', True)
        self.topic_delegation = config.get('topic_delegation', True)
        self.min_self_stake = config.get('min_self_stake', 0)  # Min tokens to be a delegate
        
        # Delegation graph
        self.delegation_graph = nx.DiGraph()
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """
        Calculate voting power including delegated power
        """
        if base_power == 0:
            return 0
        
        # Get all delegations to this voter
        delegated_power = await self._calculate_delegated_power(
            voter,
            proposal_data,
            chain_id
        )
        
        # Total power = own power + delegated power
        total_power = Decimal(str(base_power)) + delegated_power
        
        return int(total_power.to_integral_value(ROUND_DOWN))
    
    async def _calculate_delegated_power(
        self,
        delegate: str,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> Decimal:
        """
        Calculate total power delegated to a representative
        """
        delegated_power = Decimal('0')
        
        # Get delegation info from chain/cache
        delegations = await self._get_delegations_to(
            delegate,
            proposal_data.get('topic', 'general'),
            chain_id
        )
        
        for delegation in delegations:
            delegator = delegation['delegator']
            delegator_power = Decimal(str(delegation['power']))
            depth = delegation['depth']
            
            # Apply decay based on delegation depth
            decay_factor = Decimal(str(self.delegation_decay)) ** depth
            effective_power = delegator_power * decay_factor
            
            delegated_power += effective_power
        
        return delegated_power
    
    async def _get_delegations_to(
        self,
        delegate: str,
        topic: str,
        chain_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all delegations to a specific delegate
        In production, this would query on-chain delegation data
        """
        # Placeholder - would build delegation graph from chain data
        # For demo, return mock delegations
        import random
        
        num_delegators = random.randint(0, 5)
        delegations = []
        
        for i in range(num_delegators):
            delegations.append({
                'delegator': f"delegator_{i}_{delegate}",
                'power': random.randint(100, 10000),
                'depth': random.randint(1, self.max_delegation_depth),
                'topic': topic,
                'timestamp': datetime.utcnow().timestamp()
            })
        
        return delegations
    
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate votes with delegation
        """
        # Build delegation graph
        await self._build_delegation_graph(votes, proposal_data)
        
        aggregated = {
            'for_votes': Decimal('0'),
            'against_votes': Decimal('0'),
            'abstain_votes': Decimal('0'),
            'total_voters': 0,
            'total_delegates': 0,
            'delegation_stats': {
                'direct_voters': 0,
                'delegated_voters': 0,
                'avg_delegation_depth': 0,
                'max_delegation_chain': 0
            }
        }
        
        # Track who has voted (directly or through delegation)
        voted_addresses = set()
        delegates_voted = set()
        
        # First pass: Process direct votes
        for vote in votes:
            voter = vote['voter']
            if voter in voted_addresses:
                continue
            
            # Check if this voter delegated their vote
            delegation = await self._get_delegation(
                voter,
                proposal_data.get('topic', 'general'),
                vote.get('chain_id', 'unknown')
            )
            
            if delegation:
                # Vote is delegated, will be counted through delegate
                aggregated['delegation_stats']['delegated_voters'] += 1
                continue
            
            # Direct vote
            voted_addresses.add(voter)
            aggregated['total_voters'] += 1
            aggregated['delegation_stats']['direct_voters'] += 1
            
            # Calculate voting power including delegations TO this voter
            voting_power = await self.calculate_voting_power(
                voter,
                vote['base_power'],
                proposal_data,
                vote.get('chain_id', 'unknown')
            )
            
            # If voter has delegated power, they're also a delegate
            if voting_power > vote['base_power']:
                delegates_voted.add(voter)
                aggregated['total_delegates'] += 1
            
            # Apply vote
            support = vote.get('support', vote.get('vote_type'))
            
            if support in [True, 'for', 'yes', 1]:
                aggregated['for_votes'] += Decimal(str(voting_power))
            elif support in [False, 'against', 'no', 0]:
                aggregated['against_votes'] += Decimal(str(voting_power))
            else:
                aggregated['abstain_votes'] += Decimal(str(voting_power))
        
        # Calculate delegation statistics
        if self.delegation_graph.number_of_edges() > 0:
            # Average delegation depth
            depths = [self._get_delegation_depth(node) 
                     for node in self.delegation_graph.nodes()]
            aggregated['delegation_stats']['avg_delegation_depth'] = (
                sum(depths) / len(depths) if depths else 0
            )
            
            # Longest delegation chain
            aggregated['delegation_stats']['max_delegation_chain'] = max(depths) if depths else 0
        
        return {
            'for_votes': str(aggregated['for_votes']),
            'against_votes': str(aggregated['against_votes']),
            'abstain_votes': str(aggregated['abstain_votes']),
            'total_voters': aggregated['total_voters'],
            'total_delegates': aggregated['total_delegates'],
            'delegation_stats': aggregated['delegation_stats'],
            'mechanism': 'delegation'
        }
    
    async def _build_delegation_graph(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ):
        """Build delegation graph from votes and chain data"""
        self.delegation_graph.clear()
        
        # Add nodes for all voters
        for vote in votes:
            voter = vote['voter']
            self.delegation_graph.add_node(
                voter,
                power=vote['base_power']
            )
        
        # Add delegation edges
        for vote in votes:
            voter = vote['voter']
            delegation = await self._get_delegation(
                voter,
                proposal_data.get('topic', 'general'),
                vote.get('chain_id', 'unknown')
            )
            
            if delegation:
                delegate = delegation['delegate']
                self.delegation_graph.add_edge(
                    voter,
                    delegate,
                    weight=delegation.get('weight', 1.0)
                )
    
    async def _get_delegation(
        self,
        voter: str,
        topic: str,
        chain_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get delegation info for a voter
        In production, query on-chain delegation registry
        """
        # Placeholder - would check delegation registry
        import random
        
        # 30% chance of having delegation
        if random.random() < 0.3:
            return {
                'delegate': f"delegate_{random.randint(1, 10)}",
                'weight': 1.0,
                'topic': topic,
                'timestamp': datetime.utcnow().timestamp()
            }
        
        return None
    
    def _get_delegation_depth(self, node: str) -> int:
        """Get maximum delegation depth for a node"""
        if not self.delegation_graph.has_node(node):
            return 0
        
        # Find longest path from this node
        max_depth = 0
        for target in self.delegation_graph.nodes():
            if node != target:
                try:
                    paths = list(nx.all_simple_paths(
                        self.delegation_graph,
                        node,
                        target,
                        cutoff=self.max_delegation_depth
                    ))
                    if paths:
                        max_depth = max(max_depth, max(len(p) - 1 for p in paths))
                except nx.NetworkXNoPath:
                    continue
        
        return max_depth
    
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Validate delegation vote
        """
        # Check for circular delegation
        if await self._has_circular_delegation(
            voter,
            proposal_data.get('topic', 'general'),
            vote_data.get('chain_id', 'unknown')
        ):
            return False
        
        # Check delegation depth
        delegation_chain = await self._get_delegation_chain(
            voter,
            proposal_data.get('topic', 'general'),
            vote_data.get('chain_id', 'unknown')
        )
        
        if len(delegation_chain) > self.max_delegation_depth:
            return False
        
        # Check if delegate meets minimum requirements
        if 'delegate_to' in vote_data:
            delegate = vote_data['delegate_to']
            delegate_power = await self._get_voting_power(
                delegate,
                vote_data.get('chain_id', 'unknown')
            )
            
            if delegate_power < self.min_self_stake:
                return False
        
        return True
    
    async def _has_circular_delegation(
        self,
        voter: str,
        topic: str,
        chain_id: str
    ) -> bool:
        """Check if adding delegation would create a cycle"""
        # In production, check delegation graph for cycles
        # For now, use networkx cycle detection
        try:
            cycles = list(nx.simple_cycles(self.delegation_graph))
            for cycle in cycles:
                if voter in cycle:
                    return True
        except:
            pass
        
        return False
    
    async def _get_delegation_chain(
        self,
        voter: str,
        topic: str,
        chain_id: str
    ) -> List[str]:
        """Get full delegation chain starting from voter"""
        chain = [voter]
        current = voter
        
        for _ in range(self.max_delegation_depth):
            delegation = await self._get_delegation(current, topic, chain_id)
            if not delegation:
                break
            
            current = delegation['delegate']
            if current in chain:  # Circular delegation
                break
            
            chain.append(current)
        
        return chain
    
    async def _get_voting_power(self, address: str, chain_id: str) -> int:
        """Get base voting power for an address"""
        # Placeholder - would query chain
        import random
        return random.randint(0, 10000)
    
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate outcome with delegation statistics
        """
        for_votes = Decimal(aggregated_votes['for_votes'])
        against_votes = Decimal(aggregated_votes['against_votes'])
        abstain_votes = Decimal(aggregated_votes['abstain_votes'])
        
        total_votes = for_votes + against_votes + abstain_votes
        
        # Calculate participation including delegated votes
        direct_voters = aggregated_votes['delegation_stats']['direct_voters']
        delegated_voters = aggregated_votes['delegation_stats']['delegated_voters']
        total_participants = direct_voters + delegated_voters
        
        eligible_voters = proposal_data.get('eligible_voters', total_participants * 2)
        participation_rate = (total_participants / eligible_voters * 100) if eligible_voters > 0 else 0
        
        # Determine outcome
        if total_votes == 0:
            status = 'NO_VOTES'
            approval_rate = 0
        else:
            approval_rate = float((for_votes / (for_votes + against_votes) * 100)) \
                if (for_votes + against_votes) > 0 else 0
            
            # Check quorum
            quorum_required = proposal_data.get('quorum_percentage', 20)
            
            # Bonus for high delegation participation
            if delegated_voters > direct_voters:
                # Reduce quorum requirement if delegation is high
                quorum_required = max(15, quorum_required - 5)
            
            if participation_rate < quorum_required:
                status = 'FAILED_QUORUM'
            elif approval_rate >= proposal_data.get('approval_threshold', 50):
                status = 'APPROVED'
            else:
                status = 'REJECTED'
        
        return {
            'status': status,
            'for_votes': str(for_votes),
            'against_votes': str(against_votes),
            'abstain_votes': str(abstain_votes),
            'total_votes': str(total_votes),
            'approval_rate': approval_rate,
            'participation_rate': participation_rate,
            'direct_participation_rate': (direct_voters / eligible_voters * 100) if eligible_voters > 0 else 0,
            'delegation_rate': (delegated_voters / total_participants * 100) if total_participants > 0 else 0,
            'total_delegates': aggregated_votes['total_delegates'],
            'delegation_stats': aggregated_votes['delegation_stats'],
            'adjusted_quorum': quorum_required,
            'mechanism': 'delegation'
        } 