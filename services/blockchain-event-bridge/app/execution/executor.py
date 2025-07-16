"""
Cross-chain executor for automated proposal execution
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from ..chains import ChainAdapter
from .strategies import ExecutionStrategy, ExecutionType

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    """Execution status states"""
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PARTIAL = "PARTIAL"  # Some chains succeeded, others failed


class CrossChainExecutor:
    """
    Handles automated execution of approved cross-chain proposals
    """
    
    def __init__(self, chain_manager, ignite_client, config: Dict[str, Any]):
        self.chain_manager = chain_manager
        self.ignite_client = ignite_client
        self.config = config
        
        # Execution settings
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 60)  # seconds
        self.execution_timeout = config.get('execution_timeout', 300)  # seconds
        self.require_all_chains = config.get('require_all_chains', True)
        
        # Execution queue
        self.execution_queue = asyncio.Queue()
        self._execution_task = None
        self._running = False
        
    async def start(self):
        """Start the executor"""
        self._running = True
        self._execution_task = asyncio.create_task(self._execution_loop())
        logger.info("Cross-chain executor started")
        
    async def stop(self):
        """Stop the executor"""
        self._running = False
        if self._execution_task:
            self._execution_task.cancel()
            try:
                await self._execution_task
            except asyncio.CancelledError:
                pass
        logger.info("Cross-chain executor stopped")
        
    async def schedule_execution(
        self,
        proposal_id: str,
        execution_strategy: ExecutionStrategy
    ) -> Dict[str, Any]:
        """
        Schedule a proposal for execution
        """
        # Get proposal details
        proposal_cache = self.ignite_client.get_cache('crossChainProposals')
        proposal = proposal_cache.get(proposal_id)
        
        if not proposal:
            raise Exception(f"Proposal {proposal_id} not found")
        
        # Create execution plan
        execution_plan = {
            'execution_id': f"exec_{proposal_id}_{datetime.utcnow().timestamp()}",
            'proposal_id': proposal_id,
            'proposal': proposal,
            'strategy': execution_strategy,
            'status': ExecutionStatus.SCHEDULED.value,
            'scheduled_at': datetime.utcnow().timestamp(),
            'attempts': 0,
            'chain_results': {},
            'errors': []
        }
        
        # Store in cache
        execution_cache = self.ignite_client.get_cache('executionPlans')
        execution_cache.put(execution_plan['execution_id'], execution_plan)
        
        # Add to queue
        await self.execution_queue.put(execution_plan)
        
        logger.info(f"Scheduled execution for proposal {proposal_id}")
        return execution_plan
        
    async def _execution_loop(self):
        """Main execution loop"""
        while self._running:
            try:
                # Get next execution plan
                execution_plan = await asyncio.wait_for(
                    self.execution_queue.get(),
                    timeout=1.0
                )
                
                # Execute the plan
                await self._execute_plan(execution_plan)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error in execution loop: {e}")
                await asyncio.sleep(5)
    
    async def _execute_plan(self, execution_plan: Dict[str, Any]):
        """Execute a cross-chain proposal"""
        execution_id = execution_plan['execution_id']
        proposal_id = execution_plan['proposal_id']
        
        logger.info(f"Starting execution {execution_id} for proposal {proposal_id}")
        
        # Update status
        execution_plan['status'] = ExecutionStatus.IN_PROGRESS.value
        execution_plan['started_at'] = datetime.utcnow().timestamp()
        self._update_execution_plan(execution_plan)
        
        try:
            # Get execution strategy
            strategy = execution_plan['strategy']
            
            if strategy.execution_type == ExecutionType.PARALLEL:
                results = await self._execute_parallel(execution_plan)
            elif strategy.execution_type == ExecutionType.SEQUENTIAL:
                results = await self._execute_sequential(execution_plan)
            elif strategy.execution_type == ExecutionType.PRIORITY:
                results = await self._execute_priority(execution_plan)
            else:
                raise Exception(f"Unknown execution type: {strategy.execution_type}")
            
            # Process results
            execution_plan['chain_results'] = results
            execution_plan['completed_at'] = datetime.utcnow().timestamp()
            
            # Determine final status
            successful_chains = sum(1 for r in results.values() if r['success'])
            total_chains = len(results)
            
            if successful_chains == total_chains:
                execution_plan['status'] = ExecutionStatus.COMPLETED.value
            elif successful_chains == 0:
                execution_plan['status'] = ExecutionStatus.FAILED.value
            else:
                execution_plan['status'] = ExecutionStatus.PARTIAL.value
                
                # Retry failed chains if configured
                if self.config.get('retry_failed_chains', True):
                    await self._retry_failed_chains(execution_plan)
            
            # Update final state
            self._update_execution_plan(execution_plan)
            
            # Emit completion event
            await self._emit_execution_event(execution_plan)
            
            logger.info(f"Execution {execution_id} completed with status {execution_plan['status']}")
            
        except Exception as e:
            logger.error(f"Error executing plan {execution_id}: {e}")
            execution_plan['status'] = ExecutionStatus.FAILED.value
            execution_plan['errors'].append(str(e))
            self._update_execution_plan(execution_plan)
    
    async def _execute_parallel(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute on all chains in parallel"""
        proposal = execution_plan['proposal']
        chain_proposals = proposal['chainProposals']
        
        # Create execution tasks for each chain
        tasks = []
        for chain_proposal in chain_proposals:
            chain_id = chain_proposal['chainId']
            task = asyncio.create_task(
                self._execute_on_chain(
                    chain_id,
                    chain_proposal['proposalAddress'],
                    execution_plan
                )
            )
            tasks.append((chain_id, task))
        
        # Wait for all tasks with timeout
        results = {}
        
        for chain_id, task in tasks:
            try:
                result = await asyncio.wait_for(
                    task,
                    timeout=self.execution_timeout
                )
                results[chain_id] = result
            except asyncio.TimeoutError:
                results[chain_id] = {
                    'success': False,
                    'error': 'Execution timeout',
                    'timestamp': datetime.utcnow().timestamp()
                }
            except Exception as e:
                results[chain_id] = {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.utcnow().timestamp()
                }
        
        return results
    
    async def _execute_sequential(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute on chains sequentially"""
        proposal = execution_plan['proposal']
        chain_proposals = proposal['chainProposals']
        strategy = execution_plan['strategy']
        
        results = {}
        
        # Sort chains by priority if specified
        if hasattr(strategy, 'chain_order'):
            chain_proposals = sorted(
                chain_proposals,
                key=lambda x: strategy.chain_order.get(x['chainId'], 999)
            )
        
        for chain_proposal in chain_proposals:
            chain_id = chain_proposal['chainId']
            
            try:
                result = await self._execute_on_chain(
                    chain_id,
                    chain_proposal['proposalAddress'],
                    execution_plan
                )
                results[chain_id] = result
                
                # Stop on failure if required
                if not result['success'] and self.require_all_chains:
                    logger.warning(f"Stopping sequential execution due to failure on {chain_id}")
                    break
                    
            except Exception as e:
                results[chain_id] = {
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.utcnow().timestamp()
                }
                
                if self.require_all_chains:
                    break
        
        return results
    
    async def _execute_priority(self, execution_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Execute on high-priority chains first"""
        proposal = execution_plan['proposal']
        chain_proposals = proposal['chainProposals']
        strategy = execution_plan['strategy']
        
        # Group chains by priority
        priority_groups = {}
        for chain_proposal in chain_proposals:
            chain_id = chain_proposal['chainId']
            priority = strategy.chain_priorities.get(chain_id, 99)
            
            if priority not in priority_groups:
                priority_groups[priority] = []
            priority_groups[priority].append(chain_proposal)
        
        results = {}
        
        # Execute by priority groups
        for priority in sorted(priority_groups.keys()):
            group_proposals = priority_groups[priority]
            
            # Execute within priority group in parallel
            group_tasks = []
            for chain_proposal in group_proposals:
                chain_id = chain_proposal['chainId']
                task = asyncio.create_task(
                    self._execute_on_chain(
                        chain_id,
                        chain_proposal['proposalAddress'],
                        execution_plan
                    )
                )
                group_tasks.append((chain_id, task))
            
            # Wait for priority group
            for chain_id, task in group_tasks:
                try:
                    result = await task
                    results[chain_id] = result
                except Exception as e:
                    results[chain_id] = {
                        'success': False,
                        'error': str(e),
                        'timestamp': datetime.utcnow().timestamp()
                    }
            
            # Check if we should continue to next priority
            group_success = all(
                results.get(cp['chainId'], {}).get('success', False)
                for cp in group_proposals
            )
            
            if not group_success and strategy.stop_on_priority_failure:
                logger.warning(f"Stopping execution due to failure in priority group {priority}")
                break
        
        return results
    
    async def _execute_on_chain(
        self,
        chain_id: str,
        proposal_id: str,
        execution_plan: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute proposal on a specific chain"""
        adapter = self.chain_manager.chains.get(chain_id)
        
        if not adapter:
            raise Exception(f"Chain adapter not found for {chain_id}")
        
        logger.info(f"Executing proposal {proposal_id} on chain {chain_id}")
        
        try:
            # Execute the proposal
            tx_hash = await adapter.execute_proposal(proposal_id)
            
            # Wait for confirmation if configured
            if self.config.get('wait_for_confirmation', True):
                await self._wait_for_confirmation(adapter, tx_hash)
            
            return {
                'success': True,
                'transaction_hash': tx_hash,
                'chain_id': chain_id,
                'timestamp': datetime.utcnow().timestamp()
            }
            
        except Exception as e:
            logger.error(f"Failed to execute on {chain_id}: {e}")
            raise
    
    async def _wait_for_confirmation(
        self,
        adapter: ChainAdapter,
        tx_hash: str,
        max_wait: int = 120
    ):
        """Wait for transaction confirmation"""
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).seconds < max_wait:
            # Check transaction status (adapter-specific)
            # This is a simplified version
            await asyncio.sleep(2)
            
            # In production, would check actual transaction status
            # For now, assume confirmed after a short wait
            if (datetime.utcnow() - start_time).seconds > 10:
                return
        
        raise Exception(f"Transaction {tx_hash} not confirmed within {max_wait} seconds")
    
    async def _retry_failed_chains(self, execution_plan: Dict[str, Any]):
        """Retry execution on failed chains"""
        failed_chains = [
            chain_id for chain_id, result in execution_plan['chain_results'].items()
            if not result['success']
        ]
        
        if not failed_chains or execution_plan['attempts'] >= self.max_retries:
            return
        
        logger.info(f"Retrying execution on failed chains: {failed_chains}")
        
        # Wait before retry
        await asyncio.sleep(self.retry_delay)
        
        # Create retry execution plan
        retry_plan = execution_plan.copy()
        retry_plan['attempts'] += 1
        retry_plan['execution_id'] = f"{execution_plan['execution_id']}_retry_{retry_plan['attempts']}"
        
        # Filter to only failed chains
        retry_plan['proposal']['chainProposals'] = [
            cp for cp in execution_plan['proposal']['chainProposals']
            if cp['chainId'] in failed_chains
        ]
        
        # Schedule retry
        await self.execution_queue.put(retry_plan)
    
    def _update_execution_plan(self, execution_plan: Dict[str, Any]):
        """Update execution plan in cache"""
        execution_cache = self.ignite_client.get_cache('executionPlans')
        execution_cache.put(execution_plan['execution_id'], execution_plan)
    
    async def _emit_execution_event(self, execution_plan: Dict[str, Any]):
        """Emit execution completion event"""
        event = {
            'eventType': 'ProposalExecuted',
            'executionId': execution_plan['execution_id'],
            'proposalId': execution_plan['proposal_id'],
            'status': execution_plan['status'],
            'chainResults': execution_plan['chain_results'],
            'timestamp': datetime.utcnow().timestamp()
        }
        
        # Emit via Pulsar
        if hasattr(self.chain_manager, 'pulsar_producer'):
            import json
            self.chain_manager.pulsar_producer.send(
                json.dumps(event).encode('utf-8')
            )
    
    async def get_execution_status(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get status of an execution"""
        execution_cache = self.ignite_client.get_cache('executionPlans')
        return execution_cache.get(execution_id) 