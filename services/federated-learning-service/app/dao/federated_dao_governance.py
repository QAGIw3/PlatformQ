"""
DAO-Governed Federated Learning

Implements decentralized governance for federated learning sessions,
model selection, and resource allocation.
"""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import hashlib

from web3 import Web3
from web3.middleware import geth_poa_middleware
from pydantic import BaseModel, Field
import numpy as np

logger = logging.getLogger(__name__)


class ProposalType(Enum):
    """Types of DAO proposals"""
    CREATE_FL_SESSION = "create_fl_session"
    MODIFY_FL_PARAMETERS = "modify_fl_parameters"
    ALLOCATE_RESOURCES = "allocate_resources"
    APPROVE_MODEL_UPDATE = "approve_model_update"
    DISTRIBUTE_REWARDS = "distribute_rewards"
    EMERGENCY_STOP = "emergency_stop"


class VoteType(Enum):
    """Vote options"""
    FOR = "for"
    AGAINST = "against"
    ABSTAIN = "abstain"


class DAOProposal(BaseModel):
    """DAO proposal model"""
    proposal_id: str
    proposal_type: ProposalType
    proposer: str
    title: str
    description: str
    parameters: Dict[str, Any]
    created_at: datetime
    voting_deadline: datetime
    execution_deadline: Optional[datetime] = None
    quorum_required: float = Field(default=0.1, ge=0.0, le=1.0)
    approval_threshold: float = Field(default=0.5, ge=0.0, le=1.0)


class FederatedLearningDAO:
    """DAO governance for federated learning"""
    
    def __init__(self,
                 web3_provider_url: str,
                 dao_contract_address: str,
                 dao_contract_abi: List[Dict[str, Any]],
                 token_contract_address: str,
                 token_contract_abi: List[Dict[str, Any]]):
        
        # Initialize Web3
        self.w3 = Web3(Web3.HTTPProvider(web3_provider_url))
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
        
        # Initialize contracts
        self.dao_contract = self.w3.eth.contract(
            address=Web3.toChecksumAddress(dao_contract_address),
            abi=dao_contract_abi
        )
        
        self.token_contract = self.w3.eth.contract(
            address=Web3.toChecksumAddress(token_contract_address),
            abi=token_contract_abi
        )
        
        # Proposal cache
        self.proposals = {}
        
        # Governance parameters
        self.min_proposal_stake = 100  # Minimum tokens to create proposal
        self.voting_period = timedelta(days=3)
        self.execution_period = timedelta(days=2)
        
    async def create_proposal(self,
                            proposal_type: ProposalType,
                            title: str,
                            description: str,
                            parameters: Dict[str, Any],
                            proposer_address: str,
                            private_key: str) -> Dict[str, Any]:
        """Create a new DAO proposal"""
        try:
            # Check proposer has sufficient stake
            stake = await self._get_voting_power(proposer_address)
            if stake < self.min_proposal_stake:
                return {
                    "success": False,
                    "error": f"Insufficient stake. Required: {self.min_proposal_stake}, Have: {stake}"
                }
                
            # Create proposal
            proposal = DAOProposal(
                proposal_id=self._generate_proposal_id(),
                proposal_type=proposal_type,
                proposer=proposer_address,
                title=title,
                description=description,
                parameters=parameters,
                created_at=datetime.utcnow(),
                voting_deadline=datetime.utcnow() + self.voting_period,
                execution_deadline=datetime.utcnow() + self.voting_period + self.execution_period
            )
            
            # Submit to blockchain
            tx_hash = await self._submit_proposal_to_chain(proposal, private_key)
            
            # Cache proposal
            self.proposals[proposal.proposal_id] = proposal
            
            # Emit event
            await self._emit_proposal_created_event(proposal)
            
            return {
                "success": True,
                "proposal_id": proposal.proposal_id,
                "tx_hash": tx_hash.hex(),
                "voting_deadline": proposal.voting_deadline.isoformat(),
                "execution_deadline": proposal.execution_deadline.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error creating proposal: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def cast_vote(self,
                       proposal_id: str,
                       vote: VoteType,
                       voter_address: str,
                       private_key: str,
                       delegation: Optional[str] = None) -> Dict[str, Any]:
        """Cast vote on a proposal"""
        try:
            # Get proposal
            proposal = self.proposals.get(proposal_id)
            if not proposal:
                proposal = await self._fetch_proposal_from_chain(proposal_id)
                
            if not proposal:
                return {
                    "success": False,
                    "error": "Proposal not found"
                }
                
            # Check voting period
            if datetime.utcnow() > proposal.voting_deadline:
                return {
                    "success": False,
                    "error": "Voting period has ended"
                }
                
            # Get voting power
            voting_power = await self._get_voting_power(voter_address, delegation)
            
            if voting_power == 0:
                return {
                    "success": False,
                    "error": "No voting power"
                }
                
            # Submit vote to blockchain
            tx_hash = await self._submit_vote_to_chain(
                proposal_id,
                vote,
                voting_power,
                voter_address,
                private_key
            )
            
            return {
                "success": True,
                "tx_hash": tx_hash.hex(),
                "voting_power": voting_power,
                "vote": vote.value
            }
            
        except Exception as e:
            logger.error(f"Error casting vote: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def execute_proposal(self,
                             proposal_id: str,
                             executor_address: str,
                             private_key: str) -> Dict[str, Any]:
        """Execute an approved proposal"""
        try:
            # Get proposal
            proposal = await self._get_proposal_with_results(proposal_id)
            
            if not proposal:
                return {
                    "success": False,
                    "error": "Proposal not found"
                }
                
            # Check execution conditions
            execution_check = await self._check_execution_conditions(proposal)
            
            if not execution_check["can_execute"]:
                return {
                    "success": False,
                    "error": execution_check["reason"]
                }
                
            # Execute based on proposal type
            execution_result = await self._execute_proposal_action(
                proposal,
                executor_address,
                private_key
            )
            
            if execution_result["success"]:
                # Mark as executed on chain
                tx_hash = await self._mark_proposal_executed(
                    proposal_id,
                    executor_address,
                    private_key
                )
                
                execution_result["execution_tx_hash"] = tx_hash.hex()
                
            return execution_result
            
        except Exception as e:
            logger.error(f"Error executing proposal: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def get_proposal_status(self, proposal_id: str) -> Dict[str, Any]:
        """Get current status of a proposal"""
        try:
            # Get proposal with voting results
            proposal_data = await self._get_proposal_with_results(proposal_id)
            
            if not proposal_data:
                return {
                    "found": False
                }
                
            proposal = proposal_data["proposal"]
            voting_results = proposal_data["voting_results"]
            
            # Calculate status
            status = self._calculate_proposal_status(proposal, voting_results)
            
            return {
                "found": True,
                "proposal": {
                    "id": proposal.proposal_id,
                    "type": proposal.proposal_type.value,
                    "title": proposal.title,
                    "proposer": proposal.proposer,
                    "created_at": proposal.created_at.isoformat(),
                    "voting_deadline": proposal.voting_deadline.isoformat()
                },
                "status": status,
                "voting_results": voting_results,
                "can_execute": status == "approved" and datetime.utcnow() > proposal.voting_deadline
            }
            
        except Exception as e:
            logger.error(f"Error getting proposal status: {e}")
            raise
            
    async def delegate_voting_power(self,
                                  delegator: str,
                                  delegate: str,
                                  private_key: str) -> Dict[str, Any]:
        """Delegate voting power to another address"""
        try:
            # Submit delegation to blockchain
            tx_hash = await self._submit_delegation_to_chain(
                delegator,
                delegate,
                private_key
            )
            
            return {
                "success": True,
                "tx_hash": tx_hash.hex(),
                "delegator": delegator,
                "delegate": delegate
            }
            
        except Exception as e:
            logger.error(f"Error delegating voting power: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _get_voting_power(self,
                              address: str,
                              delegation: Optional[str] = None) -> int:
        """Get voting power of an address"""
        try:
            # Get token balance
            balance = self.token_contract.functions.balanceOf(address).call()
            
            # Get delegated power if applicable
            if delegation:
                delegated = self.dao_contract.functions.getDelegatedPower(
                    delegation,
                    address
                ).call()
                balance += delegated
                
            return balance
            
        except Exception as e:
            logger.error(f"Error getting voting power: {e}")
            return 0
            
    async def _submit_proposal_to_chain(self,
                                      proposal: DAOProposal,
                                      private_key: str) -> str:
        """Submit proposal to blockchain"""
        try:
            # Prepare proposal data
            proposal_data = {
                "proposalType": proposal.proposal_type.value,
                "title": proposal.title,
                "description": proposal.description,
                "parameters": json.dumps(proposal.parameters),
                "votingDeadline": int(proposal.voting_deadline.timestamp()),
                "quorumRequired": int(proposal.quorum_required * 10000),  # Basis points
                "approvalThreshold": int(proposal.approval_threshold * 10000)
            }
            
            # Build transaction
            account = self.w3.eth.account.from_key(private_key)
            nonce = self.w3.eth.get_transaction_count(account.address)
            
            tx = self.dao_contract.functions.createProposal(
                proposal_data
            ).buildTransaction({
                'chainId': self.w3.eth.chain_id,
                'gas': 500000,
                'gasPrice': self.w3.toWei('20', 'gwei'),
                'nonce': nonce
            })
            
            # Sign and send
            signed_tx = self.w3.eth.account.sign_transaction(tx, private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            
            return tx_hash
            
        except Exception as e:
            logger.error(f"Error submitting proposal to chain: {e}")
            raise
            
    async def _execute_proposal_action(self,
                                     proposal_data: Dict[str, Any],
                                     executor: str,
                                     private_key: str) -> Dict[str, Any]:
        """Execute the action specified in the proposal"""
        proposal = proposal_data["proposal"]
        
        try:
            if proposal.proposal_type == ProposalType.CREATE_FL_SESSION:
                return await self._execute_create_fl_session(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            elif proposal.proposal_type == ProposalType.MODIFY_FL_PARAMETERS:
                return await self._execute_modify_fl_parameters(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            elif proposal.proposal_type == ProposalType.ALLOCATE_RESOURCES:
                return await self._execute_allocate_resources(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            elif proposal.proposal_type == ProposalType.APPROVE_MODEL_UPDATE:
                return await self._execute_approve_model_update(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            elif proposal.proposal_type == ProposalType.DISTRIBUTE_REWARDS:
                return await self._execute_distribute_rewards(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            elif proposal.proposal_type == ProposalType.EMERGENCY_STOP:
                return await self._execute_emergency_stop(
                    proposal.parameters,
                    executor,
                    private_key
                )
                
            else:
                return {
                    "success": False,
                    "error": f"Unknown proposal type: {proposal.proposal_type}"
                }
                
        except Exception as e:
            logger.error(f"Error executing proposal action: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _execute_create_fl_session(self,
                                       parameters: Dict[str, Any],
                                       executor: str,
                                       private_key: str) -> Dict[str, Any]:
        """Execute creation of federated learning session"""
        try:
            # Extract parameters
            session_config = {
                "model_type": parameters["model_type"],
                "min_participants": parameters["min_participants"],
                "max_participants": parameters["max_participants"],
                "rounds": parameters["rounds"],
                "learning_rate": parameters["learning_rate"],
                "privacy_budget": parameters.get("privacy_budget", 1.0),
                "reward_pool": parameters.get("reward_pool", 0)
            }
            
            # Call FL service to create session
            # This would integrate with the actual FL service
            session_id = await self._create_fl_session_internal(session_config)
            
            return {
                "success": True,
                "action": "create_fl_session",
                "session_id": session_id,
                "config": session_config
            }
            
        except Exception as e:
            logger.error(f"Error executing FL session creation: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _check_execution_conditions(self,
                                        proposal_data: Dict[str, Any]) -> Dict[str, Any]:
        """Check if proposal can be executed"""
        proposal = proposal_data["proposal"]
        voting_results = proposal_data["voting_results"]
        
        # Check if voting period ended
        if datetime.utcnow() <= proposal.voting_deadline:
            return {
                "can_execute": False,
                "reason": "Voting period not ended"
            }
            
        # Check if execution deadline passed
        if proposal.execution_deadline and datetime.utcnow() > proposal.execution_deadline:
            return {
                "can_execute": False,
                "reason": "Execution deadline passed"
            }
            
        # Check quorum
        total_votes = voting_results["for"] + voting_results["against"] + voting_results["abstain"]
        total_supply = await self._get_total_voting_power()
        
        if total_supply > 0:
            participation = total_votes / total_supply
            if participation < proposal.quorum_required:
                return {
                    "can_execute": False,
                    "reason": f"Quorum not met. Required: {proposal.quorum_required:.1%}, Got: {participation:.1%}"
                }
                
        # Check approval threshold
        if total_votes > 0:
            approval_rate = voting_results["for"] / total_votes
            if approval_rate < proposal.approval_threshold:
                return {
                    "can_execute": False,
                    "reason": f"Approval threshold not met. Required: {proposal.approval_threshold:.1%}, Got: {approval_rate:.1%}"
                }
                
        # Check if already executed
        is_executed = await self._is_proposal_executed(proposal.proposal_id)
        if is_executed:
            return {
                "can_execute": False,
                "reason": "Proposal already executed"
            }
            
        return {
            "can_execute": True,
            "reason": "All conditions met"
        }
        
    def _generate_proposal_id(self) -> str:
        """Generate unique proposal ID"""
        timestamp = datetime.utcnow().isoformat()
        random_bytes = np.random.bytes(16)
        data = f"{timestamp}:{random_bytes.hex()}".encode()
        return hashlib.sha256(data).hexdigest()[:16]
        
    def _calculate_proposal_status(self,
                                 proposal: DAOProposal,
                                 voting_results: Dict[str, int]) -> str:
        """Calculate current status of proposal"""
        now = datetime.utcnow()
        
        if now <= proposal.voting_deadline:
            return "voting"
            
        total_votes = sum(voting_results.values())
        if total_votes == 0:
            return "failed_no_votes"
            
        approval_rate = voting_results["for"] / total_votes
        
        if approval_rate >= proposal.approval_threshold:
            if proposal.execution_deadline and now > proposal.execution_deadline:
                return "expired"
            return "approved"
        else:
            return "rejected"


class FLGovernanceToken:
    """Governance token for federated learning DAO"""
    
    CONTRACT_ABI = [
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function"
        },
        {
            "constant": False,
            "inputs": [
                {"name": "_to", "type": "address"},
                {"name": "_value", "type": "uint256"}
            ],
            "name": "transfer",
            "outputs": [{"name": "success", "type": "bool"}],
            "type": "function"
        },
        {
            "constant": False,
            "inputs": [{"name": "_spender", "type": "address"}],
            "name": "delegate",
            "outputs": [],
            "type": "function"
        }
    ]


class FLDAOContract:
    """DAO contract interface"""
    
    CONTRACT_ABI = [
        {
            "constant": False,
            "inputs": [{"name": "proposalData", "type": "tuple"}],
            "name": "createProposal",
            "outputs": [{"name": "proposalId", "type": "bytes32"}],
            "type": "function"
        },
        {
            "constant": False,
            "inputs": [
                {"name": "proposalId", "type": "bytes32"},
                {"name": "vote", "type": "uint8"},
                {"name": "votingPower", "type": "uint256"}
            ],
            "name": "castVote",
            "outputs": [],
            "type": "function"
        },
        {
            "constant": True,
            "inputs": [{"name": "proposalId", "type": "bytes32"}],
            "name": "getProposal",
            "outputs": [{"name": "", "type": "tuple"}],
            "type": "function"
        },
        {
            "constant": True,
            "inputs": [
                {"name": "delegator", "type": "address"},
                {"name": "delegate", "type": "address"}
            ],
            "name": "getDelegatedPower",
            "outputs": [{"name": "", "type": "uint256"}],
            "type": "function"
        }
    ] 