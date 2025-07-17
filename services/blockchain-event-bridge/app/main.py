"""
Blockchain Event Bridge Service - Enhanced with DeFi and DAO Features

Bridges blockchain events with platform services, including marketplace, DeFi, and DAO operations
"""

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
import logging
import asyncio
import json
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge
import time

from .chain_adapters import ChainAdapterFactory
from .event_processor import EventProcessor
from .transaction_manager import TransactionManager
from platformq_shared.cache import CacheManager
from platformq_shared.auth import get_current_user
from platformq_shared.base_service import create_base_app

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
BLOCKCHAIN_TRANSACTIONS = Counter('blockchain_transactions_total', 'Total blockchain transactions', ['chain', 'operation'])
TRANSACTION_LATENCY = Histogram('blockchain_transaction_latency_seconds', 'Transaction latency', ['chain'])
GAS_FEES = Gauge('blockchain_gas_fees', 'Current gas fees', ['chain'])

# Create FastAPI app
app = create_base_app(service_name="blockchain-event-bridge")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
chain_factory = ChainAdapterFactory()
event_processor = EventProcessor()
transaction_manager = TransactionManager()
cache_manager = CacheManager()

# Request models
class NFTMintRequest(BaseModel):
    chain: str
    asset_id: str
    owner_address: str
    metadata_uri: str
    royalty_percentage: float = 10.0

class LicenseOfferRequest(BaseModel):
    chain: str
    nft_contract: str
    token_id: int
    license_type: str
    price: float
    duration: int
    max_uses: Optional[int] = None

class LicensePurchaseRequest(BaseModel):
    chain: str
    offer_id: int
    buyer_address: str

class RoyaltyDistributionRequest(BaseModel):
    chain: str
    nft_contract: str
    token_id: int
    sale_price: float

# DeFi Request Models
class AuctionCreateRequest(BaseModel):
    chain: str
    token_id: int
    auction_type: str  # "english" or "dutch"
    start_price: float
    end_price: Optional[float] = None  # For Dutch auctions
    duration: int  # In seconds
    min_bid_increment: Optional[float] = None  # For English auctions
    price_decrement: Optional[float] = None  # For Dutch auctions

class LoanOfferRequest(BaseModel):
    chain: str
    nft_contract: str
    max_loan_amount: float
    interest_rate: float  # Annual percentage
    min_duration: int  # In days
    max_duration: int
    payment_token: str

class YieldFarmingPoolRequest(BaseModel):
    chain: str
    staking_token: str
    reward_rate: float
    duration: int  # In days
    min_stake_amount: float
    lock_period: int  # In days

# DAO Request Models
class ProposalCreateRequest(BaseModel):
    chain: str
    title: str
    description: str
    proposal_type: str  # "fee_change", "policy_change", "treasury_allocation"
    proposal_data: Dict[str, Any]

class VoteRequest(BaseModel):
    chain: str
    proposal_id: int
    vote_type: str  # "for", "against", "abstain"

# Existing endpoints...
# ... existing code ...

# DeFi Endpoints

@app.post("/api/v1/defi/auctions/create")
async def create_auction(
    request: AuctionCreateRequest,
    current_user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Create a Dutch or English auction for an NFT"""
    try:
        adapter = chain_factory.get_adapter(request.chain)
        
        # Validate NFT ownership
        owner = await adapter.get_nft_owner(request.token_id)
        if owner.lower() != current_user["wallet_address"].lower():
            raise HTTPException(status_code=403, detail="Not NFT owner")
        
        # Create auction based on type
        if request.auction_type.lower() == "dutch":
            result = await adapter.create_dutch_auction(
                token_id=request.token_id,
                start_price=request.start_price,
                end_price=request.end_price,
                price_decrement=request.price_decrement,
                duration=request.duration
            )
        else:  # English auction
            result = await adapter.create_english_auction(
                token_id=request.token_id,
                start_price=request.start_price,
                min_bid_increment=request.min_bid_increment,
                duration=request.duration
            )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=request.chain, operation="create_auction").inc()
        
        # Process event in background
        background_tasks.add_task(
            event_processor.process_auction_created,
            request.chain,
            result["auction_id"],
            request.token_id,
            request.auction_type
        )
        
        return {
            "auction_id": result["auction_id"],
            "transaction_hash": result["tx_hash"],
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating auction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/defi/auctions/{auction_id}/bid")
async def bid_on_auction(
    auction_id: int,
    chain: str,
    bid_amount: float,
    current_user: Dict = Depends(get_current_user),
    background_tasks: BackgroundTasks = BackgroundTasks()
):
    """Place a bid on an English auction or buy from Dutch auction"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        # Get auction details
        auction = await adapter.get_auction_details(auction_id)
        
        if auction["type"] == "english":
            result = await adapter.bid_english_auction(
                auction_id=auction_id,
                bid_amount=bid_amount,
                bidder=current_user["wallet_address"]
            )
        else:  # Dutch auction
            result = await adapter.buy_dutch_auction(
                auction_id=auction_id,
                buyer=current_user["wallet_address"]
            )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=chain, operation="auction_bid").inc()
        
        return {
            "transaction_hash": result["tx_hash"],
            "status": "bid_placed" if auction["type"] == "english" else "purchased"
        }
        
    except Exception as e:
        logger.error(f"Error bidding on auction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/defi/lending/create-offer")
async def create_loan_offer(
    request: LoanOfferRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Create a loan offer for NFT collateral"""
    try:
        adapter = chain_factory.get_adapter(request.chain)
        
        result = await adapter.create_loan_offer(
            nft_contract=request.nft_contract,
            max_loan_amount=request.max_loan_amount,
            interest_rate=request.interest_rate,
            min_duration=request.min_duration,
            max_duration=request.max_duration,
            payment_token=request.payment_token,
            lender=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=request.chain, operation="create_loan_offer").inc()
        
        return {
            "offer_id": result["offer_id"],
            "transaction_hash": result["tx_hash"],
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating loan offer: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/defi/lending/borrow")
async def borrow_against_nft(
    chain: str,
    offer_id: int,
    token_id: int,
    loan_amount: float,
    duration: int,
    current_user: Dict = Depends(get_current_user)
):
    """Borrow against NFT collateral"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        result = await adapter.borrow_against_nft(
            offer_id=offer_id,
            token_id=token_id,
            loan_amount=loan_amount,
            duration=duration,
            borrower=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=chain, operation="borrow_against_nft").inc()
        
        return {
            "loan_id": result["loan_id"],
            "transaction_hash": result["tx_hash"],
            "repayment_due": result["repayment_due"],
            "total_repayment": result["total_repayment"]
        }
        
    except Exception as e:
        logger.error(f"Error borrowing against NFT: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/defi/yield-farming/create-pool")
async def create_yield_farming_pool(
    request: YieldFarmingPoolRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Create a yield farming pool"""
    try:
        # Admin only
        if not current_user.get("is_admin"):
            raise HTTPException(status_code=403, detail="Admin only")
        
        adapter = chain_factory.get_adapter(request.chain)
        
        result = await adapter.create_yield_pool(
            staking_token=request.staking_token,
            reward_rate=request.reward_rate,
            duration=request.duration,
            min_stake_amount=request.min_stake_amount,
            lock_period=request.lock_period
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=request.chain, operation="create_yield_pool").inc()
        
        return {
            "pool_id": result["pool_id"],
            "transaction_hash": result["tx_hash"],
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating yield farming pool: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/defi/yield-farming/stake")
async def stake_in_pool(
    chain: str,
    pool_id: int,
    amount: float,
    current_user: Dict = Depends(get_current_user)
):
    """Stake tokens in yield farming pool"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        result = await adapter.stake_tokens(
            pool_id=pool_id,
            amount=amount,
            staker=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=chain, operation="stake_tokens").inc()
        
        return {
            "transaction_hash": result["tx_hash"],
            "staked_amount": amount,
            "lock_until": result["lock_until"]
        }
        
    except Exception as e:
        logger.error(f"Error staking tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# DAO Endpoints

@app.post("/api/v1/dao/proposals/create")
async def create_dao_proposal(
    request: ProposalCreateRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Create a DAO proposal"""
    try:
        adapter = chain_factory.get_adapter(request.chain)
        
        # Check if user has enough governance tokens
        balance = await adapter.get_governance_token_balance(current_user["wallet_address"])
        if balance < 100000:  # Minimum threshold
            raise HTTPException(status_code=403, detail="Insufficient governance tokens")
        
        result = await adapter.create_proposal(
            title=request.title,
            description=request.description,
            proposal_type=request.proposal_type,
            proposal_data=request.proposal_data,
            proposer=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=request.chain, operation="create_proposal").inc()
        
        return {
            "proposal_id": result["proposal_id"],
            "transaction_hash": result["tx_hash"],
            "voting_starts": result["voting_starts"],
            "voting_ends": result["voting_ends"]
        }
        
    except Exception as e:
        logger.error(f"Error creating proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/dao/proposals/{proposal_id}/vote")
async def vote_on_proposal(
    proposal_id: int,
    request: VoteRequest,
    current_user: Dict = Depends(get_current_user)
):
    """Vote on a DAO proposal"""
    try:
        adapter = chain_factory.get_adapter(request.chain)
        
        result = await adapter.vote_on_proposal(
            proposal_id=proposal_id,
            vote_type=request.vote_type,
            voter=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=request.chain, operation="vote_proposal").inc()
        
        # Cache vote for quick retrieval
        cache_key = f"dao_vote:{request.chain}:{proposal_id}:{current_user['wallet_address']}"
        await cache_manager.set(cache_key, request.vote_type, ttl=86400)  # 24 hours
        
        return {
            "transaction_hash": result["tx_hash"],
            "vote_weight": result["vote_weight"],
            "status": "voted"
        }
        
    except Exception as e:
        logger.error(f"Error voting on proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/dao/proposals/{proposal_id}")
async def get_proposal_details(
    proposal_id: int,
    chain: str
):
    """Get DAO proposal details"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        proposal = await adapter.get_proposal_details(proposal_id)
        
        return {
            "proposal_id": proposal_id,
            "title": proposal["title"],
            "description": proposal["description"],
            "proposer": proposal["proposer"],
            "status": proposal["status"],
            "for_votes": proposal["for_votes"],
            "against_votes": proposal["against_votes"],
            "abstain_votes": proposal["abstain_votes"],
            "voting_ends": proposal["voting_ends"]
        }
        
    except Exception as e:
        logger.error(f"Error getting proposal details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/dao/proposals/{proposal_id}/execute")
async def execute_proposal(
    proposal_id: int,
    chain: str,
    current_user: Dict = Depends(get_current_user)
):
    """Execute a successful DAO proposal"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        # Check if proposal passed
        proposal = await adapter.get_proposal_details(proposal_id)
        if proposal["status"] != "succeeded":
            raise HTTPException(status_code=400, detail="Proposal not succeeded")
        
        result = await adapter.execute_proposal(
            proposal_id=proposal_id,
            executor=current_user["wallet_address"]
        )
        
        # Track metrics
        BLOCKCHAIN_TRANSACTIONS.labels(chain=chain, operation="execute_proposal").inc()
        
        return {
            "transaction_hash": result["tx_hash"],
            "status": "executed"
        }
        
    except Exception as e:
        logger.error(f"Error executing proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Reputation System Endpoints

@app.post("/api/v1/reputation/record-transaction")
async def record_reputation_transaction(
    chain: str,
    buyer: str,
    seller: str,
    amount: float,
    transaction_type: str = "marketplace"
):
    """Record a transaction for reputation tracking"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        result = await adapter.record_reputation_transaction(
            buyer=buyer,
            seller=seller,
            amount=amount,
            transaction_type=transaction_type
        )
        
        return {
            "transaction_id": result["transaction_id"],
            "status": "recorded"
        }
        
    except Exception as e:
        logger.error(f"Error recording reputation transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/reputation/submit-review")
async def submit_review(
    chain: str,
    transaction_id: int,
    rating: int,
    comment: str,
    current_user: Dict = Depends(get_current_user)
):
    """Submit a review for a transaction"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        result = await adapter.submit_review(
            transaction_id=transaction_id,
            rating=rating,
            comment=comment,
            reviewer=current_user["wallet_address"]
        )
        
        return {
            "transaction_hash": result["tx_hash"],
            "status": "submitted"
        }
        
    except Exception as e:
        logger.error(f"Error submitting review: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/reputation/{address}")
async def get_reputation_score(
    address: str,
    chain: str
):
    """Get reputation score for an address"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        reputation = await adapter.get_reputation_details(address)
        
        return {
            "address": address,
            "score": reputation["score"],
            "total_transactions": reputation["total_transactions"],
            "successful_transactions": reputation["successful_transactions"],
            "average_rating": reputation["average_rating"],
            "is_verified": reputation["is_verified"]
        }
        
    except Exception as e:
        logger.error(f"Error getting reputation score: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Analytics Endpoints

@app.get("/api/v1/analytics/defi/overview")
async def get_defi_analytics(
    chain: Optional[str] = None,
    days: int = 30
):
    """Get DeFi analytics overview"""
    try:
        analytics = {
            "total_value_locked": 0,
            "active_loans": 0,
            "total_loan_volume": 0,
            "active_auctions": 0,
            "total_auction_volume": 0,
            "yield_farming_tvl": 0,
            "unique_users": 0
        }
        
        # Aggregate across chains if not specified
        chains = [chain] if chain else ["ethereum", "polygon", "avalanche"]
        
        for c in chains:
            adapter = chain_factory.get_adapter(c)
            chain_analytics = await adapter.get_defi_analytics(days)
            
            for key in analytics:
                analytics[key] += chain_analytics.get(key, 0)
        
        return analytics
        
    except Exception as e:
        logger.error(f"Error getting DeFi analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/dao/participation")
async def get_dao_participation_analytics(
    chain: str,
    days: int = 30
):
    """Get DAO participation analytics"""
    try:
        adapter = chain_factory.get_adapter(chain)
        
        analytics = await adapter.get_dao_analytics(days)
        
        return {
            "total_proposals": analytics["total_proposals"],
            "active_proposals": analytics["active_proposals"],
            "unique_voters": analytics["unique_voters"],
            "average_participation_rate": analytics["participation_rate"],
            "proposal_success_rate": analytics["success_rate"],
            "top_contributors": analytics["top_contributors"]
        }
        
    except Exception as e:
        logger.error(f"Error getting DAO analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
