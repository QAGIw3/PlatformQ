"""
Blockchain Event Bridge Service - Multi-chain governance support
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from pydantic import BaseModel
import time

from pyignite import Client as IgniteClient
import pulsar
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse
import web3

from platformq_shared import get_logger, init_tracing
from platformq_shared.config import get_config
from platformq_shared.auth import require_api_key

from .chain_manager import ChainManager
from .chains import ChainType
from .voting import VotingMechanism

# Initialize logging
logger = get_logger(__name__)

# Metrics
CHAIN_EVENTS = Counter('blockchain_events_total', 'Total blockchain events', ['chain', 'event_type'])
CROSS_CHAIN_PROPOSALS = Counter('cross_chain_proposals_total', 'Total cross-chain proposals')
REPUTATION_SYNCS = Counter('reputation_syncs_total', 'Total reputation synchronizations')
VOTES_AGGREGATED = Counter('votes_aggregated_total', 'Total votes aggregated', ['mechanism'])
EXECUTION_LATENCY = Histogram('execution_latency_seconds', 'Proposal execution latency')

# Global instances
chain_manager: Optional[ChainManager] = None
ignite_client: Optional[IgniteClient] = None
pulsar_client: Optional[pulsar.Client] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global chain_manager, ignite_client, pulsar_client
    
    # Startup
    config = get_config()
    
    # Initialize Ignite
    ignite_client = IgniteClient()
    ignite_client.connect(config.get('IGNITE_HOSTS', ['localhost:10800']))
    
    # Initialize Pulsar
    pulsar_client = pulsar.Client(
        config.get('PULSAR_URL', 'pulsar://localhost:6650')
    )
    
    # Initialize chain manager
    chain_manager = ChainManager(ignite_client, pulsar_client)
    
    # Initialize voting strategies
    chain_manager.init_voting_strategies(config.get('voting', {}))
    
    # Initialize executor
    chain_manager.init_executor(config.get('execution', {}))
    await chain_manager.start_executor()
    
    # Add configured chains
    chains_config = config.get('chains', {})
    for chain_id, chain_config in chains_config.items():
        chain_type = ChainType[chain_config['type']]
        await chain_manager.add_chain(chain_id, chain_type, chain_config)
    
    yield
    
    # Shutdown
    await chain_manager.close()
    pulsar_client.close()
    ignite_client.close()


# Create FastAPI app
app = FastAPI(
    title="Blockchain Event Bridge",
    description="Multi-chain governance and event synchronization",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize tracing
init_tracing("blockchain-event-bridge")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "chains": list(chain_manager.chains.keys()) if chain_manager else [],
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return PlainTextResponse(generate_latest())


@app.get("/api/v1/chains")
async def get_chains():
    """Get list of connected chains"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    chains = []
    for chain_id, adapter in chain_manager.chains.items():
        chains.append({
            "chainId": chain_id,
            "chainType": adapter.chain_type.value,
            "connected": adapter.is_connected,
            "latestBlock": await adapter.get_latest_block() if adapter.is_connected else None
        })
    
    return {"chains": chains}


@app.post("/api/v1/chains/{chain_id}")
async def add_chain(chain_id: str, chain_data: Dict[str, Any]):
    """Add a new chain"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        chain_type = ChainType[chain_data['type']]
        config = chain_data.get('config', {})
        
        await chain_manager.add_chain(chain_id, chain_type, config)
        
        return {
            "status": "success",
            "chainId": chain_id,
            "message": f"Chain {chain_id} added successfully"
        }
    except Exception as e:
        logger.error(f"Failed to add chain {chain_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/api/v1/chains/{chain_id}")
async def remove_chain(chain_id: str):
    """Remove a chain"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    await chain_manager.remove_chain(chain_id)
    
    return {
        "status": "success",
        "message": f"Chain {chain_id} removed"
    }


@app.post("/api/v1/proposals/cross-chain")
async def create_cross_chain_proposal(proposal_data: Dict[str, Any]):
    """Create a cross-chain proposal"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Validate chains exist
        target_chains = proposal_data.get('targetChains', [])
        for chain_id in target_chains:
            if chain_id not in chain_manager.chains:
                raise HTTPException(status_code=400, detail=f"Chain {chain_id} not found")
        
        # Create proposal on each chain
        proposal_id = f"cross_{datetime.utcnow().timestamp()}"
        chain_proposals = []
        
        for chain_id in target_chains:
            adapter = chain_manager.chains[chain_id]
            chain_proposal_id = await adapter.submit_proposal({
                "title": proposal_data['title'],
                "description": proposal_data['description'],
                "actions": proposal_data.get('actions', []),
                "proposer": proposal_data['proposer']
            })
            
            chain_proposals.append({
                "chainId": chain_id,
                "proposalAddress": chain_proposal_id
            })
        
        # Store cross-chain proposal
        cross_chain_proposal = {
            "proposalId": proposal_id,
            "title": proposal_data['title'],
            "description": proposal_data['description'],
            "chainProposals": chain_proposals,
            "votingMechanism": proposal_data.get('votingMechanism', 'SIMPLE'),
            "aggregationStrategy": proposal_data.get('aggregationStrategy', 'WEIGHTED_AVG'),
            "executionStrategy": proposal_data.get('executionStrategy', 'parallel'),
            "createdAt": datetime.utcnow().timestamp(),
            "status": "ACTIVE"
        }
        
        cache = ignite_client.get_cache('crossChainProposals')
        cache.put(proposal_id, cross_chain_proposal)
        
        CROSS_CHAIN_PROPOSALS.inc()
        
        return {
            "proposalId": proposal_id,
            "chainProposals": chain_proposals,
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Failed to create cross-chain proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/proposals/{proposal_id}/state")
async def get_proposal_state(proposal_id: str):
    """Get aggregated state of a cross-chain proposal"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Get proposal
        cache = ignite_client.get_cache('crossChainProposals')
        proposal = cache.get(proposal_id)
        
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found")
        
        # Get state from each chain
        chain_states = []
        for chain_proposal in proposal['chainProposals']:
            chain_id = chain_proposal['chainId']
            adapter = chain_manager.chains.get(chain_id)
            
            if adapter and adapter.is_connected:
                state = await adapter.get_proposal_state(chain_proposal['proposalAddress'])
                state['chainId'] = chain_id
                chain_states.append(state)
        
        # Aggregate based on strategy
        aggregation_strategy = proposal.get('aggregationStrategy', 'WEIGHTED_AVG')
        aggregated_state = aggregate_proposal_states(chain_states, aggregation_strategy)
        
        return {
            "proposalId": proposal_id,
            "chainStates": chain_states,
            "aggregatedState": aggregated_state,
            "votingMechanism": proposal.get('votingMechanism', 'SIMPLE')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get proposal state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/reputation/sync")
async def sync_reputation(sync_data: Dict[str, Any]):
    """Synchronize reputation across chains"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    address = sync_data.get('address')
    if not address:
        raise HTTPException(status_code=400, detail="Address required")
    
    try:
        reputation_by_chain = await chain_manager.synchronize_reputation(address)
        
        REPUTATION_SYNCS.inc()
        
        return {
            "address": address,
            "reputation": reputation_by_chain,
            "syncedAt": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to sync reputation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/vote")
async def cast_vote(vote_data: Dict[str, Any]):
    """Cast a vote on a specific chain"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    chain_id = vote_data.get('chainId')
    proposal_id = vote_data.get('proposalId')
    
    if chain_id not in chain_manager.chains:
        raise HTTPException(status_code=400, detail=f"Chain {chain_id} not found")
    
    try:
        adapter = chain_manager.chains[chain_id]
        tx_hash = await adapter.cast_vote(
            proposal_id,
            vote_data.get('support', True),
            vote_data.get('voter'),
            vote_data.get('signature')
        )
        
        CHAIN_EVENTS.labels(chain=chain_id, event_type='VoteCast').inc()
        
        return {
            "status": "success",
            "transactionHash": tx_hash,
            "chainId": chain_id
        }
        
    except Exception as e:
        logger.error(f"Failed to cast vote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/proposals/{proposal_id}/aggregate-votes")
async def aggregate_votes(proposal_id: str, aggregation_params: Dict[str, Any]):
    """Aggregate votes for a cross-chain proposal using advanced voting mechanisms"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Get proposal
        cache = ignite_client.get_cache('crossChainProposals')
        proposal = cache.get(proposal_id)
        
        if not proposal:
            raise HTTPException(status_code=404, detail="Proposal not found")
        
        # Get voting mechanism
        mechanism_name = aggregation_params.get('mechanism', proposal.get('votingMechanism', 'SIMPLE'))
        mechanism = VotingMechanism[mechanism_name]
        
        voting_strategy = chain_manager.voting_strategies.get(mechanism)
        if not voting_strategy:
            raise HTTPException(status_code=400, detail=f"Voting mechanism {mechanism_name} not available")
        
        # Collect votes from all chains
        all_votes = []
        vote_cache = ignite_client.get_cache('VoteCastEvents')
        
        # In production, use SQL queries on Ignite
        # For now, simulate vote collection
        for chain_proposal in proposal['chainProposals']:
            chain_id = chain_proposal['chainId']
            # Would query votes for this proposal on this chain
            # Mock data for demo
            votes = [
                {
                    'voter': f'voter_{i}',
                    'support': i % 2 == 0,
                    'base_power': 1000 * (i + 1),
                    'chain_id': chain_id,
                    'timestamp': datetime.utcnow().timestamp()
                }
                for i in range(5)
            ]
            all_votes.extend(votes)
        
        # Aggregate using voting strategy
        aggregated = await voting_strategy.aggregate_votes(all_votes, proposal)
        
        # Calculate outcome
        outcome = await voting_strategy.calculate_outcome(aggregated, proposal)
        
        # Store aggregation result
        aggregation_cache = ignite_client.get_cache('voteAggregations')
        aggregation_cache.put(proposal_id, {
            'proposalId': proposal_id,
            'mechanism': mechanism_name,
            'aggregatedVotes': aggregated,
            'outcome': outcome,
            'timestamp': datetime.utcnow().timestamp()
        })
        
        VOTES_AGGREGATED.labels(mechanism=mechanism_name).inc()
        
        return {
            'proposalId': proposal_id,
            'mechanism': mechanism_name,
            'aggregatedVotes': aggregated,
            'outcome': outcome
        }
        
    except Exception as e:
        logger.error(f"Failed to aggregate votes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/proposals/{proposal_id}/execute")
async def execute_proposal(proposal_id: str, execution_params: Dict[str, Any]):
    """Schedule cross-chain proposal execution"""
    if not chain_manager:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Check if proposal is approved
        aggregation_cache = ignite_client.get_cache('voteAggregations')
        aggregation = aggregation_cache.get(proposal_id)
        
        if not aggregation or aggregation['outcome']['status'] != 'APPROVED':
            raise HTTPException(status_code=400, detail="Proposal not approved")
        
        # Schedule execution
        strategy_type = execution_params.get('strategy', 'parallel')
        
        with EXECUTION_LATENCY.time():
            execution_plan = await chain_manager.schedule_execution(
                proposal_id,
                strategy_type
            )
        
        return {
            'proposalId': proposal_id,
            'executionId': execution_plan['execution_id'],
            'strategy': strategy_type,
            'status': 'scheduled'
        }
        
    except Exception as e:
        logger.error(f"Failed to execute proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/execution/{execution_id}")
async def get_execution_status(execution_id: str):
    """Get status of proposal execution"""
    if not chain_manager or not chain_manager.executor:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    execution_status = await chain_manager.executor.get_execution_status(execution_id)
    
    if not execution_status:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    return execution_status


def aggregate_proposal_states(chain_states: List[Dict[str, Any]], strategy: str) -> Dict[str, Any]:
    """Aggregate proposal states from multiple chains"""
    if not chain_states:
        return {"status": "NO_DATA"}
    
    total_for = sum(int(state.get('forVotes', 0)) for state in chain_states)
    total_against = sum(int(state.get('againstVotes', 0)) for state in chain_states)
    
    if strategy == 'WEIGHTED_AVG':
        # Weight by total votes per chain
        total_votes = total_for + total_against
        if total_votes == 0:
            approval_rate = 0
        else:
            approval_rate = (total_for / total_votes) * 100
    elif strategy == 'MAJORITY_ALL':
        # Require majority on all chains
        approvals = sum(1 for state in chain_states 
                       if int(state.get('forVotes', 0)) > int(state.get('againstVotes', 0)))
        approval_rate = 100 if approvals == len(chain_states) else 0
    else:
        # Simple average
        approval_rate = (total_for / (total_for + total_against) * 100) if (total_for + total_against) > 0 else 0
    
    return {
        "totalForVotes": str(total_for),
        "totalAgainstVotes": str(total_against),
        "approvalRate": approval_rate,
        "participatingChains": len(chain_states),
        "status": "APPROVED" if approval_rate >= 50 else "REJECTED"
    }


class MintNFTRequest(BaseModel):
    chain_id: str
    to: str
    uri: str
    royalty_recipient: str
    royalty_fraction: int

class CreateLicenseOfferRequest(BaseModel):
    chain_id: str
    asset_id: str
    price: int
    duration: int
    license_type: str
    max_usage: int
    royalty_percentage: int

class PurchaseLicenseRequest(BaseModel):
    chain_id: str
    asset_id: str
    offer_index: int
    license_type: int

class DistributeRoyaltyRequest(BaseModel):
    chain_id: str
    token_id: int
    sale_price: int

@app.post("/api/v1/marketplace/mint-nft")
@require_api_key
async def mint_nft(request: MintNFTRequest):
    try:
        # Check cache first
        cache_key = f"mint_{request.chain_id}_{request.to}_{request.uri}"
        cache = ignite_client.get_or_create_cache('marketplace_transactions')
        cached_result = cache.get(cache_key)
        
        if cached_result and cached_result.get('timestamp', 0) > time.time() - 300:  # 5 min cache
            return cached_result['data']
        
        tx_hash = await chain_manager.mint_asset_nft(
            request.chain_id,
            request.to,
            request.uri,
            request.royalty_recipient,
            request.royalty_fraction
        )
        # Wait for transaction receipt
        w3 = web3.Web3(web3.Web3.HTTPProvider('http://ethereum-node:8545'))  # Assume Ethereum node URL
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        # Extract token_id from Transfer event
        contract = w3.eth.contract(address='PLATFORM_ASSET_ADDRESS', abi=PLATFORM_ASSET_ABI)  # Assume these are defined
        logs = contract.events.Transfer().process_receipt(receipt)
        token_id = logs[0]['args']['tokenId']
        
        result = {"token_id": token_id}
        
        # Cache result
        cache.put(cache_key, {'data': result, 'timestamp': time.time()})
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/marketplace/create-license-offer")
@require_api_key
async def create_license_offer(request: CreateLicenseOfferRequest):
    try:
        tx_hash = await chain_manager.create_license_offer(
            request.chain_id,
            request.asset_id,
            request.price,
            request.duration,
            request.license_type,
            request.max_usage,
            request.royalty_percentage
        )
        return {"tx_hash": tx_hash}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/marketplace/purchase-license")
@require_api_key
async def purchase_license(request: PurchaseLicenseRequest):
    try:
        tx_hash = await chain_manager.purchase_license(
            request.chain_id,
            request.asset_id,
            request.offer_index,
            request.license_type
        )
        return {"tx_hash": tx_hash}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/marketplace/distribute-royalty")
@require_api_key
async def distribute_royalty(request: DistributeRoyaltyRequest):
    try:
        tx_hash = await chain_manager.distribute_royalty(
            request.chain_id,
            request.token_id,
            request.sale_price
        )
        return {"tx_hash": tx_hash}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
