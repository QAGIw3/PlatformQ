"""
Proposal execution for DAO governance.
"""

import logging
import asyncio
from typing import Dict, Optional, List, Any
from datetime import datetime
from enum import Enum

from platformq_blockchain_common import ChainType, Transaction, TransactionType
from .proposals import ProposalStatus

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    """Execution status states"""
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"  # Some chains succeeded, others failed


class ProposalExecutor:
    """Executes passed governance proposals on-chain"""
    
    def __init__(self, blockchain_gateway, proposal_manager):
        self.blockchain_gateway = blockchain_gateway
        self.proposal_manager = proposal_manager
        self._execution_tasks = {}
        
    async def execute_proposal(self, proposal_id: str) -> Dict[str, Any]:
        """Execute a passed proposal across all specified chains"""
        proposal = await self.proposal_manager.get_proposal(proposal_id)
        
        if not proposal:
            raise ValueError(f"Proposal {proposal_id} not found")
            
        if proposal.status != ProposalStatus.QUEUED:
            raise ValueError(f"Proposal {proposal_id} is not queued for execution")
            
        # Check execution delay has passed
        execution_time = proposal.voting_end.timestamp() + proposal.execution_delay
        if datetime.utcnow().timestamp() < execution_time:
            raise ValueError(f"Execution delay has not passed for proposal {proposal_id}")
            
        logger.info(f"Starting execution of proposal {proposal_id}")
        
        # Update status
        await self.proposal_manager.update_proposal_status(
            proposal_id,
            ProposalStatus.EXECUTED
        )
        
        # Execute on each chain
        results = {}
        success_count = 0
        
        for i, chain in enumerate(proposal.chains):
            try:
                chain_type = ChainType(chain.lower())
                
                # Prepare transaction data
                if i < len(proposal.targets):
                    result = await self._execute_on_chain(
                        chain_type=chain_type,
                        target=proposal.targets[i],
                        value=proposal.values[i],
                        calldata=proposal.calldatas[i],
                        proposal_id=proposal_id
                    )
                    
                    results[chain] = {
                        'status': 'success',
                        'tx_hash': result['tx_hash'],
                        'block_number': result.get('block_number')
                    }
                    success_count += 1
                else:
                    logger.warning(f"No execution data for chain {chain}")
                    results[chain] = {
                        'status': 'skipped',
                        'reason': 'no_execution_data'
                    }
                    
            except Exception as e:
                logger.error(f"Failed to execute on {chain}: {e}")
                results[chain] = {
                    'status': 'failed',
                    'error': str(e)
                }
                
        # Determine overall status
        if success_count == len(proposal.chains):
            execution_status = ExecutionStatus.COMPLETED
        elif success_count > 0:
            execution_status = ExecutionStatus.PARTIAL
        else:
            execution_status = ExecutionStatus.FAILED
            
        # Update proposal with results
        proposal.executed_at = datetime.utcnow()
        proposal.execution_tx = results
        await self.proposal_manager._proposals_cache.put(proposal_id, proposal)
        
        logger.info(
            f"Proposal {proposal_id} execution {execution_status.value}: "
            f"{success_count}/{len(proposal.chains)} chains succeeded"
        )
        
        return {
            'proposal_id': proposal_id,
            'status': execution_status.value,
            'chain_results': results,
            'executed_at': proposal.executed_at.isoformat()
        }
        
    async def _execute_on_chain(
        self,
        chain_type: ChainType,
        target: str,
        value: int,
        calldata: str,
        proposal_id: str
    ) -> Dict[str, Any]:
        """Execute proposal transaction on a specific chain"""
        logger.info(f"Executing on {chain_type.value}: target={target}, value={value}")
        
        # Use blockchain gateway to send transaction
        result = await self.blockchain_gateway.send_transaction(
            chain_type=chain_type,
            from_address="0x...",  # DAO treasury/executor address
            to_address=target,
            value=value,
            data=calldata,
            gas_strategy="standard"
        )
        
        # Wait for confirmation
        tx_status = await self.blockchain_gateway.get_transaction_status(
            result['tx_hash']
        )
        
        return {
            'tx_hash': result['tx_hash'],
            'status': tx_status['status'],
            'block_number': tx_status['block_number']
        }
        
    async def schedule_execution(
        self,
        proposal_id: str
    ) -> Dict[str, Any]:
        """Schedule a proposal for execution when delay expires"""
        proposal = await self.proposal_manager.get_proposal(proposal_id)
        
        if not proposal:
            raise ValueError(f"Proposal {proposal_id} not found")
            
        if proposal.status != ProposalStatus.SUCCEEDED:
            raise ValueError(f"Proposal {proposal_id} has not succeeded")
            
        # Calculate execution time
        execution_time = proposal.voting_end.timestamp() + proposal.execution_delay
        delay_seconds = max(0, execution_time - datetime.utcnow().timestamp())
        
        # Update status to queued
        await self.proposal_manager.update_proposal_status(
            proposal_id,
            ProposalStatus.QUEUED
        )
        
        # Schedule execution
        task = asyncio.create_task(self._delayed_execution(proposal_id, delay_seconds))
        self._execution_tasks[proposal_id] = task
        
        logger.info(
            f"Scheduled proposal {proposal_id} for execution in {delay_seconds} seconds"
        )
        
        return {
            'proposal_id': proposal_id,
            'execution_time': datetime.fromtimestamp(execution_time).isoformat(),
            'delay_seconds': delay_seconds
        }
        
    async def _delayed_execution(self, proposal_id: str, delay_seconds: float):
        """Execute proposal after delay"""
        try:
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
                
            await self.execute_proposal(proposal_id)
            
        except Exception as e:
            logger.error(f"Failed to execute proposal {proposal_id}: {e}")
            
        finally:
            # Clean up task
            if proposal_id in self._execution_tasks:
                del self._execution_tasks[proposal_id]
                
    async def cancel_execution(self, proposal_id: str) -> bool:
        """Cancel a scheduled execution"""
        if proposal_id in self._execution_tasks:
            task = self._execution_tasks[proposal_id]
            task.cancel()
            del self._execution_tasks[proposal_id]
            
            # Update status back to succeeded
            await self.proposal_manager.update_proposal_status(
                proposal_id,
                ProposalStatus.SUCCEEDED
            )
            
            logger.info(f"Cancelled execution of proposal {proposal_id}")
            return True
            
        return False
        
    def get_pending_executions(self) -> List[str]:
        """Get list of proposals pending execution"""
        return list(self._execution_tasks.keys()) 