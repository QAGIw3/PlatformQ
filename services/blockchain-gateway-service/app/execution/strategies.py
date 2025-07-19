"""
Execution strategies for cross-chain proposals
"""

from enum import Enum
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


class ExecutionType(Enum):
    """Types of execution strategies"""
    PARALLEL = "PARALLEL"  # Execute on all chains simultaneously
    SEQUENTIAL = "SEQUENTIAL"  # Execute one chain at a time
    PRIORITY = "PRIORITY"  # Execute by priority groups
    CONDITIONAL = "CONDITIONAL"  # Execute based on conditions
    OPTIMISTIC = "OPTIMISTIC"  # Execute optimistically, rollback on failure


@dataclass
class ExecutionStrategy:
    """Base execution strategy configuration"""
    execution_type: ExecutionType
    timeout_seconds: int = 300
    max_gas_price: Optional[Dict[str, int]] = None  # Chain-specific max gas prices
    retry_on_failure: bool = True
    require_all_chains: bool = True
    
    def validate(self) -> bool:
        """Validate strategy configuration"""
        return True


@dataclass
class ParallelExecutionStrategy(ExecutionStrategy):
    """Execute on all chains in parallel"""
    execution_type: ExecutionType = ExecutionType.PARALLEL
    batch_size: int = 10  # Max chains to execute simultaneously
    fail_fast: bool = False  # Stop all if one fails
    

@dataclass
class SequentialExecutionStrategy(ExecutionStrategy):
    """Execute chains in sequence"""
    execution_type: ExecutionType = ExecutionType.SEQUENTIAL
    chain_order: Dict[str, int] = field(default_factory=dict)  # Chain execution order
    stop_on_failure: bool = True
    delay_between_chains: int = 5  # Seconds
    

@dataclass 
class PriorityExecutionStrategy(ExecutionStrategy):
    """Execute by priority groups"""
    execution_type: ExecutionType = ExecutionType.PRIORITY
    chain_priorities: Dict[str, int] = field(default_factory=dict)  # Lower = higher priority
    stop_on_priority_failure: bool = True
    parallel_within_priority: bool = True
    

@dataclass
class ConditionalExecutionStrategy(ExecutionStrategy):
    """Execute based on conditions"""
    execution_type: ExecutionType = ExecutionType.CONDITIONAL
    
    # Conditions for execution
    min_chains_approved: int = 1
    required_chains: List[str] = field(default_factory=list)
    execute_if_quorum_on: List[str] = field(default_factory=list)  # Chains that must reach quorum
    
    # Time-based conditions
    execute_after_timestamp: Optional[int] = None
    execute_before_timestamp: Optional[int] = None
    
    # Value-based conditions
    min_total_votes: Optional[int] = None
    min_approval_percentage: float = 50.0
    

@dataclass
class OptimisticExecutionStrategy(ExecutionStrategy):
    """Execute optimistically with rollback capability"""
    execution_type: ExecutionType = ExecutionType.OPTIMISTIC
    
    # Optimistic execution settings
    execute_immediately: bool = True
    challenge_period_hours: int = 24
    rollback_on_challenge: bool = True
    required_bond: Dict[str, int] = field(default_factory=dict)  # Chain-specific bonds
    
    # Rollback configuration
    rollback_strategy: str = "REVERSE"  # REVERSE, COMPENSATE, FREEZE
    compensation_actions: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)


class ExecutionStrategyFactory:
    """Factory for creating execution strategies"""
    
    @staticmethod
    def create_strategy(
        strategy_type: str,
        config: Dict[str, Any]
    ) -> ExecutionStrategy:
        """Create an execution strategy from configuration"""
        
        if strategy_type == "parallel":
            return ParallelExecutionStrategy(
                batch_size=config.get('batch_size', 10),
                fail_fast=config.get('fail_fast', False),
                timeout_seconds=config.get('timeout_seconds', 300),
                retry_on_failure=config.get('retry_on_failure', True)
            )
            
        elif strategy_type == "sequential":
            return SequentialExecutionStrategy(
                chain_order=config.get('chain_order', {}),
                stop_on_failure=config.get('stop_on_failure', True),
                delay_between_chains=config.get('delay_between_chains', 5),
                timeout_seconds=config.get('timeout_seconds', 300)
            )
            
        elif strategy_type == "priority":
            return PriorityExecutionStrategy(
                chain_priorities=config.get('chain_priorities', {}),
                stop_on_priority_failure=config.get('stop_on_priority_failure', True),
                parallel_within_priority=config.get('parallel_within_priority', True),
                timeout_seconds=config.get('timeout_seconds', 300)
            )
            
        elif strategy_type == "conditional":
            return ConditionalExecutionStrategy(
                min_chains_approved=config.get('min_chains_approved', 1),
                required_chains=config.get('required_chains', []),
                execute_if_quorum_on=config.get('execute_if_quorum_on', []),
                execute_after_timestamp=config.get('execute_after_timestamp'),
                execute_before_timestamp=config.get('execute_before_timestamp'),
                min_total_votes=config.get('min_total_votes'),
                min_approval_percentage=config.get('min_approval_percentage', 50.0)
            )
            
        elif strategy_type == "optimistic":
            return OptimisticExecutionStrategy(
                execute_immediately=config.get('execute_immediately', True),
                challenge_period_hours=config.get('challenge_period_hours', 24),
                rollback_on_challenge=config.get('rollback_on_challenge', True),
                required_bond=config.get('required_bond', {}),
                rollback_strategy=config.get('rollback_strategy', 'REVERSE'),
                compensation_actions=config.get('compensation_actions', {})
            )
            
        else:
            # Default to parallel
            return ParallelExecutionStrategy()
    
    @staticmethod
    def get_default_strategy(proposal_type: str) -> ExecutionStrategy:
        """Get default strategy for a proposal type"""
        
        if proposal_type == "emergency":
            # Fast execution on critical chains first
            return PriorityExecutionStrategy(
                chain_priorities={
                    "1": 1,  # Ethereum mainnet first
                    "137": 2,  # Then Polygon
                    "42161": 3,  # Then Arbitrum
                },
                stop_on_priority_failure=False,  # Try all chains
                timeout_seconds=120  # Faster timeout
            )
            
        elif proposal_type == "funding":
            # Sequential execution with validation
            return SequentialExecutionStrategy(
                chain_order={
                    "1": 1,  # Start with mainnet
                    "137": 2,
                    "42161": 3,
                },
                stop_on_failure=True,  # Don't continue if funding fails
                delay_between_chains=10  # More time between chains
            )
            
        elif proposal_type == "upgrade":
            # Conditional execution
            return ConditionalExecutionStrategy(
                min_chains_approved=2,  # Need at least 2 chains
                required_chains=["1"],  # Must include mainnet
                min_approval_percentage=66.7,  # Higher threshold
                execute_after_timestamp=None  # Can add delay
            )
            
        else:
            # Default parallel execution
            return ParallelExecutionStrategy(
                batch_size=5,
                fail_fast=False,
                retry_on_failure=True
            ) 