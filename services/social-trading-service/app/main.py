"""
PlatformQ Social Trading Service
Next-generation social trading with verifiable performance and Strategy NFTs
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging

from app.api import strategies, copy_trading, reputation, analytics, social, strategy_markets, automated_trading
from app.trading.strategy_engine import StrategyEngine
from app.trading.copy_executor import CopyTradingExecutor
from app.reputation.reputation_engine import ReputationEngine
from app.analytics.performance_tracker import PerformanceTracker
from app.copy.portfolio_copier import PortfolioCopier
from app.dao.trader_dao import TraderDAO
from app.integration import (
    IgniteCache,
    PulsarEventPublisher,
    ElasticsearchClient,
    JanusGraphClient,
    BlockchainClient,
    DerivativesEngineClient,
    GraphIntelligenceClient,
    NeuromorphicClient
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TradingSignalType(Enum):
    OPEN_POSITION = "open_position"
    CLOSE_POSITION = "close_position"
    ADJUST_POSITION = "adjust_position"
    HEDGE = "hedge"
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"


@dataclass
class StrategyNFT:
    """Represents a tokenized trading strategy"""
    token_id: int
    strategy_id: str
    creator: str
    name: str
    description: str
    performance_data: Dict
    verified_metrics: Dict
    royalty_percentage: Decimal
    total_followers: int
    aum: Decimal  # Assets Under Management
    created_at: datetime


@dataclass
class TraderProfile:
    """Trader profile with reputation and performance data"""
    trader_id: str
    username: str
    reputation_score: Decimal
    sharpe_ratio: Decimal
    max_drawdown: Decimal
    win_rate: Decimal
    total_pnl: Decimal
    followers_count: int
    strategies_count: int
    verified: bool
    dao_member: bool


# Global instances
strategy_engine: Optional[StrategyEngine] = None
copy_executor: Optional[CopyTradingExecutor] = None
reputation_engine: Optional[ReputationEngine] = None
performance_tracker: Optional[PerformanceTracker] = None
portfolio_copier: Optional[PortfolioCopier] = None
trader_dao: Optional[TraderDAO] = None
websocket_connections: Set[WebSocket] = set()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Social Trading Service...")
    
    # Initialize storage clients
    ignite = IgniteCache()
    await ignite.connect()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    elasticsearch = ElasticsearchClient()
    await elasticsearch.connect()
    
    janusgraph = JanusGraphClient()
    await janusgraph.connect()
    
    blockchain = BlockchainClient()
    await blockchain.connect()
    
    derivatives_client = DerivativesEngineClient()
    graph_client = GraphIntelligenceClient()
    neuromorphic_client = NeuromorphicClient()
    
    # Initialize components
    global strategy_engine, copy_executor, reputation_engine
    global performance_tracker, portfolio_copier, trader_dao
    
    strategy_engine = StrategyEngine(
        ignite=ignite,
        pulsar=pulsar,
        blockchain=blockchain,
        derivatives_client=derivatives_client
    )
    
    copy_executor = CopyTradingExecutor(
        ignite=ignite,
        pulsar=pulsar,
        derivatives_client=derivatives_client,
        neuromorphic_client=neuromorphic_client
    )
    
    reputation_engine = ReputationEngine(
        ignite=ignite,
        elasticsearch=elasticsearch,
        janusgraph=janusgraph,
        graph_client=graph_client
    )
    
    performance_tracker = PerformanceTracker(
        ignite=ignite,
        elasticsearch=elasticsearch,
        pulsar=pulsar
    )
    
    portfolio_copier = PortfolioCopier(
        ignite=ignite,
        pulsar=pulsar,
        derivatives_client=derivatives_client
    )
    
    trader_dao = TraderDAO(
        blockchain=blockchain,
        ignite=ignite,
        pulsar=pulsar
    )
    
    # Start background tasks
    asyncio.create_task(strategy_engine.start_monitoring())
    asyncio.create_task(copy_executor.start_execution_loop())
    asyncio.create_task(reputation_engine.start_reputation_updates())
    asyncio.create_task(performance_tracker.start_tracking())
    asyncio.create_task(trader_dao.start_governance_monitoring())
    
    logger.info("Social Trading Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Social Trading Service...")
    
    # Stop background tasks
    await strategy_engine.stop()
    await copy_executor.stop()
    await reputation_engine.stop()
    await performance_tracker.stop()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    await elasticsearch.close()
    await janusgraph.close()
    await blockchain.close()
    
    logger.info("Social Trading Service shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="PlatformQ Social Trading",
    description="Next-gen social trading with Strategy NFTs and verifiable performance",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(strategies.router, prefix="/api/v1/strategies", tags=["strategies"])
app.include_router(copy_trading.router, prefix="/api/v1/copy", tags=["copy-trading"])
app.include_router(reputation.router, prefix="/api/v1/reputation", tags=["reputation"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(social.router, prefix="/api/v1/social", tags=["social"])
app.include_router(strategy_markets.router)  # Already has prefix in router definition
app.include_router(automated_trading.router)  # Already has prefix in router definition


# Health check
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "social-trading",
        "version": "1.0.0",
        "components": {
            "strategy_engine": strategy_engine is not None,
            "copy_executor": copy_executor is not None,
            "reputation_engine": reputation_engine is not None,
            "performance_tracker": performance_tracker is not None,
            "trader_dao": trader_dao is not None
        }
    }


# Strategy NFT endpoints
@app.post("/api/v1/strategy-nft/create")
async def create_strategy_nft(request: Dict):
    """
    Create a new Strategy NFT from a trading strategy
    """
    try:
        # Validate strategy performance
        strategy_id = request['strategy_id']
        performance = await performance_tracker.get_strategy_performance(strategy_id)
        
        # Minimum requirements for NFT creation
        if performance['sharpe_ratio'] < 1.0 or performance['trade_count'] < 100:
            raise HTTPException(
                status_code=400,
                detail="Strategy does not meet minimum performance requirements"
            )
            
        # Create NFT metadata
        metadata = {
            'name': request['name'],
            'description': request['description'],
            'strategy_id': strategy_id,
            'creator': request['creator'],
            'performance': {
                'sharpe_ratio': float(performance['sharpe_ratio']),
                'max_drawdown': float(performance['max_drawdown']),
                'total_return': float(performance['total_return']),
                'win_rate': float(performance['win_rate'])
            },
            'royalty_percentage': float(request.get('royalty_percentage', 2.0)),
            'created_at': datetime.utcnow().isoformat()
        }
        
        # Mint NFT on blockchain
        token_id = await strategy_engine.mint_strategy_nft(metadata)
        
        # Create StrategyNFT record
        strategy_nft = StrategyNFT(
            token_id=token_id,
            strategy_id=strategy_id,
            creator=request['creator'],
            name=request['name'],
            description=request['description'],
            performance_data=performance,
            verified_metrics=await _verify_metrics(strategy_id),
            royalty_percentage=Decimal(str(request.get('royalty_percentage', 2.0))),
            total_followers=0,
            aum=Decimal('0'),
            created_at=datetime.utcnow()
        )
        
        # Store NFT data
        await ignite.put(f'strategy_nft:{token_id}', strategy_nft)
        
        # Emit event
        await pulsar.publish('strategy.nft.created', {
            'token_id': token_id,
            'strategy_id': strategy_id,
            'creator': request['creator'],
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'token_id': token_id,
            'strategy_nft': strategy_nft,
            'transaction_hash': await strategy_engine.get_mint_transaction(token_id)
        }
        
    except Exception as e:
        logger.error(f"Error creating Strategy NFT: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/traders/{trader_id}/profile")
async def get_trader_profile(trader_id: str):
    """
    Get comprehensive trader profile with reputation and performance
    """
    try:
        # Get trader data
        trader_data = await reputation_engine.get_trader_data(trader_id)
        
        # Get performance metrics
        performance = await performance_tracker.get_trader_performance(trader_id)
        
        # Get reputation score
        reputation = await reputation_engine.calculate_reputation_score(trader_id)
        
        # Get social metrics
        followers = await _get_follower_count(trader_id)
        strategies = await strategy_engine.get_trader_strategies(trader_id)
        
        # Check DAO membership
        dao_member = await trader_dao.is_member(trader_id)
        
        profile = TraderProfile(
            trader_id=trader_id,
            username=trader_data.get('username', 'Anonymous'),
            reputation_score=reputation['score'],
            sharpe_ratio=Decimal(str(performance['sharpe_ratio'])),
            max_drawdown=Decimal(str(performance['max_drawdown'])),
            win_rate=Decimal(str(performance['win_rate'])),
            total_pnl=Decimal(str(performance['total_pnl'])),
            followers_count=followers,
            strategies_count=len(strategies),
            verified=trader_data.get('verified', False),
            dao_member=dao_member
        )
        
        return {
            'profile': profile,
            'recent_trades': await _get_recent_trades(trader_id, limit=10),
            'top_strategies': await _get_top_strategies(trader_id, limit=5),
            'social_links': trader_data.get('social_links', {})
        }
        
    except Exception as e:
        logger.error(f"Error getting trader profile: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/copy/follow")
async def follow_trader(request: Dict):
    """
    Start following a trader or strategy
    """
    try:
        follower_id = request['follower_id']
        target_type = request['target_type']  # 'trader' or 'strategy'
        target_id = request['target_id']
        
        # Allocation settings
        allocation = {
            'type': request.get('allocation_type', 'fixed'),  # 'fixed' or 'percentage'
            'amount': Decimal(str(request.get('amount', '1000'))),
            'percentage': Decimal(str(request.get('percentage', '10'))),
            'max_positions': request.get('max_positions', 10),
            'risk_multiplier': Decimal(str(request.get('risk_multiplier', '1.0')))
        }
        
        # Risk settings
        risk_settings = {
            'max_drawdown': Decimal(str(request.get('max_drawdown', '20'))),
            'stop_loss_override': request.get('stop_loss_override', False),
            'position_size_limit': Decimal(str(request.get('position_size_limit', '5000')))
        }
        
        # Create copy trading relationship
        copy_id = await copy_executor.create_copy_relationship(
            follower_id=follower_id,
            target_type=target_type,
            target_id=target_id,
            allocation=allocation,
            risk_settings=risk_settings
        )
        
        # Update follower count
        if target_type == 'trader':
            await reputation_engine.increment_followers(target_id)
        else:
            await strategy_engine.increment_strategy_followers(target_id)
            
        # Start copying existing positions if requested
        if request.get('copy_existing_positions', False):
            await portfolio_copier.copy_existing_positions(
                copy_id,
                target_type,
                target_id,
                follower_id,
                allocation
            )
            
        return {
            'copy_id': copy_id,
            'status': 'active',
            'target': {
                'type': target_type,
                'id': target_id
            },
            'allocation': allocation,
            'risk_settings': risk_settings
        }
        
    except Exception as e:
        logger.error(f"Error following trader: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/social/signals/{asset}")
async def get_social_signals(asset: str):
    """
    Get aggregated social signals for an asset
    """
    try:
        # Get sentiment from various sources
        twitter_sentiment = await _get_twitter_sentiment(asset)
        discord_sentiment = await _get_discord_sentiment(asset)
        telegram_sentiment = await _get_telegram_sentiment(asset)
        
        # Get whale activity
        whale_activity = await _get_whale_activity(asset)
        
        # Get crowd positioning
        crowd_positioning = await _get_crowd_positioning(asset)
        
        # Get influencer activity
        influencer_trades = await _get_influencer_trades(asset)
        
        # Calculate composite score
        composite_score = _calculate_social_score(
            twitter_sentiment,
            discord_sentiment,
            telegram_sentiment,
            whale_activity,
            crowd_positioning,
            influencer_trades
        )
        
        return {
            'asset': asset,
            'sentiment': {
                'twitter': twitter_sentiment,
                'discord': discord_sentiment,
                'telegram': telegram_sentiment,
                'composite': composite_score['sentiment']
            },
            'whale_activity': whale_activity,
            'crowd_positioning': crowd_positioning,
            'influencer_activity': influencer_trades,
            'signal_strength': composite_score['signal_strength'],
            'recommendation': composite_score['recommendation'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting social signals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket endpoint for real-time trading signals
@app.websocket("/ws/signals/{trader_id}")
async def websocket_trading_signals(websocket: WebSocket, trader_id: str):
    """
    WebSocket endpoint for real-time trading signal streaming
    """
    await websocket.accept()
    websocket_connections.add(websocket)
    
    # Subscribe to trader's signals
    subscription_id = await strategy_engine.subscribe_to_trader_signals(
        trader_id,
        lambda signal: asyncio.create_task(_broadcast_signal(websocket, signal))
    )
    
    try:
        while True:
            # Keep connection alive and handle messages
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message['type'] == 'ping':
                await websocket.send_json({'type': 'pong'})
            elif message['type'] == 'filter':
                # Update signal filters
                await strategy_engine.update_signal_filters(
                    subscription_id,
                    message.get('filters', {})
                )
                
    except WebSocketDisconnect:
        websocket_connections.remove(websocket)
        await strategy_engine.unsubscribe_from_signals(subscription_id)


# Trader DAO endpoints
@app.post("/api/v1/dao/propose")
async def create_dao_proposal(request: Dict):
    """
    Create a new DAO proposal
    """
    try:
        proposal = await trader_dao.create_proposal(
            proposer=request['proposer'],
            proposal_type=request['type'],
            title=request['title'],
            description=request['description'],
            parameters=request.get('parameters', {})
        )
        
        return {
            'proposal_id': proposal['id'],
            'status': 'created',
            'voting_ends': proposal['voting_ends'],
            'proposal': proposal
        }
        
    except Exception as e:
        logger.error(f"Error creating DAO proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/dao/vote")
async def vote_on_proposal(request: Dict):
    """
    Vote on a DAO proposal
    """
    try:
        vote_result = await trader_dao.cast_vote(
            proposal_id=request['proposal_id'],
            voter=request['voter'],
            vote=request['vote'],  # 'for', 'against', 'abstain'
            reason=request.get('reason', '')
        )
        
        return vote_result
        
    except Exception as e:
        logger.error(f"Error voting on proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper functions
async def _verify_metrics(strategy_id: str) -> Dict:
    """Verify strategy metrics on-chain"""
    # This would verify metrics using blockchain data
    return {
        'verified': True,
        'verification_hash': 'mock_hash',
        'verified_at': datetime.utcnow().isoformat()
    }


async def _get_follower_count(trader_id: str) -> int:
    """Get follower count for a trader"""
    followers = await ignite.get(f'followers:{trader_id}', [])
    return len(followers)


async def _get_recent_trades(trader_id: str, limit: int) -> List[Dict]:
    """Get recent trades for a trader"""
    # Query from derivatives engine
    return await derivatives_client.get_trader_trades(trader_id, limit)


async def _get_top_strategies(trader_id: str, limit: int) -> List[Dict]:
    """Get top performing strategies for a trader"""
    strategies = await strategy_engine.get_trader_strategies(trader_id)
    
    # Sort by performance
    sorted_strategies = sorted(
        strategies,
        key=lambda s: s['performance']['sharpe_ratio'],
        reverse=True
    )
    
    return sorted_strategies[:limit]


async def _get_twitter_sentiment(asset: str) -> Dict:
    """Get Twitter sentiment for asset"""
    # This would integrate with Twitter API or sentiment service
    return {
        'score': 0.65,
        'volume': 1250,
        'trend': 'increasing'
    }


async def _get_whale_activity(asset: str) -> Dict:
    """Get whale trading activity"""
    # Query large trades from derivatives engine
    large_trades = await derivatives_client.get_large_trades(
        asset,
        min_size=Decimal('100000')
    )
    
    return {
        'buy_volume': sum(t['amount'] for t in large_trades if t['side'] == 'buy'),
        'sell_volume': sum(t['amount'] for t in large_trades if t['side'] == 'sell'),
        'net_flow': 'positive' if buy_volume > sell_volume else 'negative',
        'whale_count': len(set(t['trader'] for t in large_trades))
    }


async def _get_crowd_positioning(asset: str) -> Dict:
    """Get retail crowd positioning"""
    positions = await derivatives_client.get_position_distribution(asset)
    
    return {
        'long_percentage': positions['long_percentage'],
        'short_percentage': positions['short_percentage'],
        'average_leverage': positions['average_leverage'],
        'funding_rate': positions['funding_rate']
    }


async def _broadcast_signal(websocket: WebSocket, signal: Dict):
    """Broadcast trading signal to websocket"""
    try:
        await websocket.send_json({
            'type': 'trading_signal',
            'signal': signal,
            'timestamp': datetime.utcnow().isoformat()
        })
    except Exception as e:
        logger.error(f"Error broadcasting signal: {e}")


def _calculate_social_score(*args) -> Dict:
    """Calculate composite social score"""
    # Simplified calculation - would be more sophisticated
    sentiment_scores = [arg['score'] for arg in args[:3] if 'score' in arg]
    avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
    
    signal_strength = 'strong' if avg_sentiment > 0.7 else 'moderate' if avg_sentiment > 0.5 else 'weak'
    recommendation = 'buy' if avg_sentiment > 0.6 else 'sell' if avg_sentiment < 0.4 else 'neutral'
    
    return {
        'sentiment': avg_sentiment,
        'signal_strength': signal_strength,
        'recommendation': recommendation
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8008) 