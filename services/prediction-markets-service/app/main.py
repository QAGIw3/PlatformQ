"""
PlatformQ Prediction Markets Service
Comprehensive prediction market infrastructure for real-world events
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional, Set, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import json

from app.api import markets, trading, resolution, analytics, governance
from app.markets.market_engine import MarketEngine
from app.markets.conditional_engine import ConditionalMarketEngine
from app.resolution.oracle_resolver import OracleResolver
from app.liquidity.amm_pool import PredictionAMM
from app.governance.market_dao import MarketGovernanceDAO
from app.integration import (
    IgniteCache,
    PulsarEventPublisher,
    ElasticsearchClient,
    BlockchainClient,
    OracleAggregatorClient,
    GraphIntelligenceClient,
    SocialDataClient
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MarketType(Enum):
    BINARY = "binary"  # Yes/No outcomes
    CATEGORICAL = "categorical"  # Multiple discrete outcomes
    SCALAR = "scalar"  # Range-based outcomes
    COMBINATORIAL = "combinatorial"  # Multiple correlated outcomes
    CONDITIONAL = "conditional"  # Dependent on other outcomes


class MarketCategory(Enum):
    POLITICS = "politics"
    SPORTS = "sports"
    WEATHER = "weather"
    TECHNOLOGY = "technology"
    FINANCE = "finance"
    ENTERTAINMENT = "entertainment"
    SCIENCE = "science"
    CRYPTO = "crypto"
    DAO_GOVERNANCE = "dao_governance"


class ResolutionSource(Enum):
    ORACLE = "oracle"
    DAO_VOTE = "dao_vote"
    AUTOMATED = "automated"
    MULTI_ORACLE = "multi_oracle"


@dataclass
class Market:
    """Represents a prediction market"""
    market_id: str
    title: str
    description: str
    category: MarketCategory
    market_type: MarketType
    outcomes: List[str]
    resolution_source: ResolutionSource
    resolution_criteria: Dict
    created_by: str
    created_at: datetime
    trading_ends: datetime
    resolution_time: datetime
    liquidity: Decimal
    volume: Decimal
    status: str  # 'active', 'closed', 'resolving', 'resolved', 'disputed'


@dataclass
class MarketPosition:
    """User position in a market"""
    user_id: str
    market_id: str
    outcome: str
    shares: Decimal
    average_price: Decimal
    current_value: Decimal
    pnl: Decimal
    created_at: datetime


# Global instances
market_engine: Optional[MarketEngine] = None
conditional_engine: Optional[ConditionalMarketEngine] = None
oracle_resolver: Optional[OracleResolver] = None
prediction_amm: Optional[PredictionAMM] = None
market_dao: Optional[MarketGovernanceDAO] = None
websocket_connections: Dict[str, Set[WebSocket]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Prediction Markets Service...")
    
    # Initialize storage clients
    ignite = IgniteCache()
    await ignite.connect()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    elasticsearch = ElasticsearchClient()
    await elasticsearch.connect()
    
    blockchain = BlockchainClient()
    await blockchain.connect()
    
    oracle_client = OracleAggregatorClient()
    graph_client = GraphIntelligenceClient()
    social_client = SocialDataClient()
    
    # Initialize components
    global market_engine, conditional_engine, oracle_resolver
    global prediction_amm, market_dao
    
    market_engine = MarketEngine(
        ignite=ignite,
        pulsar=pulsar,
        blockchain=blockchain,
        elasticsearch=elasticsearch
    )
    
    conditional_engine = ConditionalMarketEngine(
        market_engine=market_engine,
        ignite=ignite,
        graph_client=graph_client
    )
    
    oracle_resolver = OracleResolver(
        ignite=ignite,
        pulsar=pulsar,
        oracle_client=oracle_client,
        social_client=social_client
    )
    
    prediction_amm = PredictionAMM(
        ignite=ignite,
        blockchain=blockchain,
        market_engine=market_engine
    )
    
    market_dao = MarketGovernanceDAO(
        blockchain=blockchain,
        ignite=ignite,
        pulsar=pulsar
    )
    
    # Start background tasks
    asyncio.create_task(market_engine.start_market_monitoring())
    asyncio.create_task(oracle_resolver.start_resolution_monitoring())
    asyncio.create_task(prediction_amm.start_liquidity_management())
    asyncio.create_task(market_dao.start_governance_monitoring())
    asyncio.create_task(_ai_market_discovery_loop())
    
    logger.info("Prediction Markets Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Prediction Markets Service...")
    
    # Stop background tasks
    await market_engine.stop()
    await oracle_resolver.stop()
    await prediction_amm.stop()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    await elasticsearch.close()
    await blockchain.close()
    
    logger.info("Prediction Markets Service shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="PlatformQ Prediction Markets",
    description="Comprehensive prediction market infrastructure",
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
app.include_router(markets.router, prefix="/api/v1/markets", tags=["markets"])
app.include_router(trading.router, prefix="/api/v1/trading", tags=["trading"])
app.include_router(resolution.router, prefix="/api/v1/resolution", tags=["resolution"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(governance.router, prefix="/api/v1/governance", tags=["governance"])


# Health check
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "prediction-markets",
        "version": "1.0.0",
        "components": {
            "market_engine": market_engine is not None,
            "conditional_engine": conditional_engine is not None,
            "oracle_resolver": oracle_resolver is not None,
            "prediction_amm": prediction_amm is not None,
            "market_dao": market_dao is not None
        }
    }


# Market creation endpoints
@app.post("/api/v1/markets/create")
async def create_market(request: Dict):
    """
    Create a new prediction market
    """
    try:
        # Validate market parameters
        market_params = {
            'title': request['title'],
            'description': request['description'],
            'category': MarketCategory(request['category']),
            'market_type': MarketType(request['market_type']),
            'outcomes': request['outcomes'],
            'resolution_source': ResolutionSource(request['resolution_source']),
            'resolution_criteria': request['resolution_criteria'],
            'trading_ends': datetime.fromisoformat(request['trading_ends']),
            'resolution_time': datetime.fromisoformat(request['resolution_time']),
            'creator': request['creator']
        }
        
        # Validate based on market type
        if market_params['market_type'] == MarketType.BINARY:
            if len(market_params['outcomes']) != 2:
                raise ValueError("Binary markets must have exactly 2 outcomes")
        elif market_params['market_type'] == MarketType.SCALAR:
            if 'min_value' not in request or 'max_value' not in request:
                raise ValueError("Scalar markets require min_value and max_value")
            market_params['scalar_range'] = {
                'min': Decimal(str(request['min_value'])),
                'max': Decimal(str(request['max_value']))
            }
            
        # Create market
        market = await market_engine.create_market(market_params)
        
        # Initialize liquidity if provided
        if 'initial_liquidity' in request:
            liquidity_amount = Decimal(str(request['initial_liquidity']))
            await prediction_amm.provide_initial_liquidity(
                market.market_id,
                market_params['creator'],
                liquidity_amount
            )
            
        # Emit event
        await pulsar.publish('market.created', {
            'market_id': market.market_id,
            'title': market.title,
            'category': market.category.value,
            'creator': market.created_by,
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'market_id': market.market_id,
            'market': market,
            'status': 'created'
        }
        
    except Exception as e:
        logger.error(f"Error creating market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/markets/create-conditional")
async def create_conditional_market(request: Dict):
    """
    Create a conditional prediction market
    """
    try:
        # Create conditional market
        conditional_market = await conditional_engine.create_conditional_market(
            title=request['title'],
            description=request['description'],
            condition_market_id=request['condition_market_id'],
            condition_outcome=request['condition_outcome'],
            outcomes=request['outcomes'],
            creator=request['creator'],
            trading_ends=datetime.fromisoformat(request['trading_ends']),
            resolution_time=datetime.fromisoformat(request['resolution_time'])
        )
        
        return {
            'market_id': conditional_market['market_id'],
            'condition': {
                'market_id': request['condition_market_id'],
                'outcome': request['condition_outcome']
            },
            'status': 'created'
        }
        
    except Exception as e:
        logger.error(f"Error creating conditional market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/markets/{market_id}")
async def get_market(market_id: str):
    """
    Get market details
    """
    try:
        market = await market_engine.get_market(market_id)
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
            
        # Get current prices
        prices = await prediction_amm.get_market_prices(market_id)
        
        # Get market stats
        stats = await market_engine.get_market_stats(market_id)
        
        return {
            'market': market,
            'prices': prices,
            'stats': stats,
            'liquidity': await prediction_amm.get_market_liquidity(market_id)
        }
        
    except Exception as e:
        logger.error(f"Error getting market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/markets/category/{category}")
async def get_markets_by_category(
    category: MarketCategory,
    status: Optional[str] = "active",
    limit: int = 50,
    offset: int = 0
):
    """
    Get markets by category
    """
    try:
        markets = await market_engine.get_markets_by_category(
            category=category,
            status=status,
            limit=limit,
            offset=offset
        )
        
        # Enhance with current prices
        enhanced_markets = []
        for market in markets:
            prices = await prediction_amm.get_market_prices(market.market_id)
            enhanced_markets.append({
                'market': market,
                'prices': prices
            })
            
        return {
            'markets': enhanced_markets,
            'total': len(enhanced_markets),
            'category': category.value
        }
        
    except Exception as e:
        logger.error(f"Error getting markets by category: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Trading endpoints
@app.post("/api/v1/trading/buy")
async def buy_shares(request: Dict):
    """
    Buy shares in a prediction market
    """
    try:
        # Execute trade through AMM
        trade_result = await prediction_amm.buy_shares(
            market_id=request['market_id'],
            outcome=request['outcome'],
            amount=Decimal(str(request['amount'])),
            buyer=request['buyer'],
            max_price=Decimal(str(request.get('max_price', '0.99')))
        )
        
        # Update position
        position = await market_engine.update_position(
            user_id=request['buyer'],
            market_id=request['market_id'],
            outcome=request['outcome'],
            shares=trade_result['shares_bought'],
            price=trade_result['average_price']
        )
        
        # Emit trade event
        await pulsar.publish('market.trade', {
            'market_id': request['market_id'],
            'trader': request['buyer'],
            'side': 'buy',
            'outcome': request['outcome'],
            'shares': str(trade_result['shares_bought']),
            'price': str(trade_result['average_price']),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            'trade_id': trade_result['trade_id'],
            'shares_bought': trade_result['shares_bought'],
            'average_price': trade_result['average_price'],
            'total_cost': trade_result['total_cost'],
            'position': position
        }
        
    except Exception as e:
        logger.error(f"Error buying shares: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/trading/sell")
async def sell_shares(request: Dict):
    """
    Sell shares in a prediction market
    """
    try:
        # Execute trade through AMM
        trade_result = await prediction_amm.sell_shares(
            market_id=request['market_id'],
            outcome=request['outcome'],
            shares=Decimal(str(request['shares'])),
            seller=request['seller'],
            min_price=Decimal(str(request.get('min_price', '0.01')))
        )
        
        # Update position
        position = await market_engine.update_position(
            user_id=request['seller'],
            market_id=request['market_id'],
            outcome=request['outcome'],
            shares=-trade_result['shares_sold'],
            price=trade_result['average_price']
        )
        
        return {
            'trade_id': trade_result['trade_id'],
            'shares_sold': trade_result['shares_sold'],
            'average_price': trade_result['average_price'],
            'total_received': trade_result['total_received'],
            'position': position
        }
        
    except Exception as e:
        logger.error(f"Error selling shares: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Resolution endpoints
@app.post("/api/v1/resolution/resolve/{market_id}")
async def resolve_market(market_id: str, request: Dict):
    """
    Resolve a prediction market
    """
    try:
        market = await market_engine.get_market(market_id)
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
            
        # Check if market can be resolved
        if datetime.utcnow() < market.resolution_time:
            raise HTTPException(
                status_code=400,
                detail="Market resolution time not reached"
            )
            
        # Resolve based on source
        if market.resolution_source == ResolutionSource.ORACLE:
            resolution = await oracle_resolver.resolve_with_oracle(
                market_id,
                market.resolution_criteria
            )
        elif market.resolution_source == ResolutionSource.DAO_VOTE:
            # Initiate DAO vote
            resolution = await market_dao.initiate_resolution_vote(
                market_id,
                proposer=request['proposer']
            )
            return {
                'status': 'vote_initiated',
                'proposal_id': resolution['proposal_id'],
                'voting_ends': resolution['voting_ends']
            }
        else:
            resolution = await oracle_resolver.resolve_automated(
                market_id,
                market.resolution_criteria
            )
            
        # Process resolution
        await market_engine.resolve_market(
            market_id,
            resolution['outcome'],
            resolution['evidence']
        )
        
        # Distribute winnings
        await prediction_amm.distribute_winnings(market_id, resolution['outcome'])
        
        return {
            'market_id': market_id,
            'resolved_outcome': resolution['outcome'],
            'evidence': resolution['evidence'],
            'status': 'resolved'
        }
        
    except Exception as e:
        logger.error(f"Error resolving market: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/resolution/dispute/{market_id}")
async def dispute_resolution(market_id: str, request: Dict):
    """
    Dispute a market resolution
    """
    try:
        dispute = await market_dao.create_dispute(
            market_id=market_id,
            disputer=request['disputer'],
            reason=request['reason'],
            evidence=request.get('evidence', {}),
            stake=Decimal(str(request['stake']))
        )
        
        return {
            'dispute_id': dispute['id'],
            'market_id': market_id,
            'status': 'dispute_created',
            'review_period_ends': dispute['review_period_ends']
        }
        
    except Exception as e:
        logger.error(f"Error creating dispute: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Analytics endpoints
@app.get("/api/v1/analytics/market/{market_id}/history")
async def get_market_history(
    market_id: str,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    interval: str = "1h"
):
    """
    Get historical price data for a market
    """
    try:
        if not start_time:
            start_time = datetime.utcnow() - timedelta(days=7)
        if not end_time:
            end_time = datetime.utcnow()
            
        history = await market_engine.get_price_history(
            market_id=market_id,
            start_time=start_time,
            end_time=end_time,
            interval=interval
        )
        
        return {
            'market_id': market_id,
            'period': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            },
            'interval': interval,
            'data': history
        }
        
    except Exception as e:
        logger.error(f"Error getting market history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics/leaderboard")
async def get_prediction_leaderboard(
    timeframe: str = "all_time",
    category: Optional[MarketCategory] = None,
    limit: int = 100
):
    """
    Get prediction accuracy leaderboard
    """
    try:
        leaderboard = await market_engine.get_leaderboard(
            timeframe=timeframe,
            category=category,
            limit=limit
        )
        
        return {
            'timeframe': timeframe,
            'category': category.value if category else 'all',
            'leaderboard': leaderboard,
            'generated_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# WebSocket endpoint for real-time market data
@app.websocket("/ws/market/{market_id}")
async def websocket_market_data(websocket: WebSocket, market_id: str):
    """
    WebSocket endpoint for real-time market data streaming
    """
    await websocket.accept()
    
    # Add to market connections
    if market_id not in websocket_connections:
        websocket_connections[market_id] = set()
    websocket_connections[market_id].add(websocket)
    
    try:
        # Send initial market data
        market = await market_engine.get_market(market_id)
        prices = await prediction_amm.get_market_prices(market_id)
        
        await websocket.send_json({
            'type': 'initial_data',
            'market': market,
            'prices': prices
        })
        
        # Subscribe to market updates
        subscription = await market_engine.subscribe_to_market(
            market_id,
            lambda update: asyncio.create_task(
                _broadcast_market_update(market_id, update)
            )
        )
        
        # Keep connection alive
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            if message['type'] == 'ping':
                await websocket.send_json({'type': 'pong'})
                
    except WebSocketDisconnect:
        websocket_connections[market_id].remove(websocket)
        if not websocket_connections[market_id]:
            del websocket_connections[market_id]
        await market_engine.unsubscribe_from_market(subscription)


# AI Market Discovery
async def _ai_market_discovery_loop():
    """
    AI-powered market discovery from news and social media
    """
    while True:
        try:
            # Scan news sources
            predictable_events = await _scan_news_for_events()
            
            # Scan social media
            trending_topics = await _scan_social_for_topics()
            
            # Generate market suggestions
            for event in predictable_events + trending_topics:
                if await _is_suitable_for_market(event):
                    suggestion = await _generate_market_suggestion(event)
                    
                    # Store suggestion
                    await ignite.put(
                        f'market_suggestion:{suggestion["id"]}',
                        suggestion
                    )
                    
                    # Emit event
                    await pulsar.publish('market.suggestion', suggestion)
                    
        except Exception as e:
            logger.error(f"Error in AI market discovery: {e}")
            
        await asyncio.sleep(300)  # Run every 5 minutes


async def _scan_news_for_events() -> List[Dict]:
    """Scan news sources for predictable events"""
    # This would integrate with news APIs
    return []


async def _scan_social_for_topics() -> List[Dict]:
    """Scan social media for trending predictable topics"""
    # This would integrate with social media APIs
    return []


async def _is_suitable_for_market(event: Dict) -> bool:
    """Check if an event is suitable for a prediction market"""
    # Check criteria like:
    # - Clear resolution criteria
    # - Definite timeline
    # - Public interest
    # - Not duplicate
    return True


async def _generate_market_suggestion(event: Dict) -> Dict:
    """Generate market parameters from an event"""
    return {
        'id': f'suggestion_{datetime.utcnow().timestamp()}',
        'title': event.get('title', ''),
        'description': event.get('description', ''),
        'category': _categorize_event(event),
        'suggested_outcomes': _extract_outcomes(event),
        'resolution_source': _suggest_resolution_source(event),
        'confidence': 0.85,
        'source': event.get('source', 'unknown'),
        'created_at': datetime.utcnow().isoformat()
    }


def _categorize_event(event: Dict) -> str:
    """Categorize an event"""
    # Simple keyword-based categorization
    text = (event.get('title', '') + ' ' + event.get('description', '')).lower()
    
    if any(word in text for word in ['election', 'vote', 'president', 'senate']):
        return MarketCategory.POLITICS.value
    elif any(word in text for word in ['game', 'match', 'championship', 'team']):
        return MarketCategory.SPORTS.value
    elif any(word in text for word in ['launch', 'release', 'announce', 'product']):
        return MarketCategory.TECHNOLOGY.value
    else:
        return MarketCategory.CRYPTO.value


def _extract_outcomes(event: Dict) -> List[str]:
    """Extract possible outcomes from event"""
    # Default to binary
    return ["Yes", "No"]


def _suggest_resolution_source(event: Dict) -> str:
    """Suggest resolution source for event"""
    if 'official' in event.get('description', '').lower():
        return ResolutionSource.ORACLE.value
    else:
        return ResolutionSource.DAO_VOTE.value


async def _broadcast_market_update(market_id: str, update: Dict):
    """Broadcast market update to all connected websockets"""
    if market_id in websocket_connections:
        disconnected = set()
        for websocket in websocket_connections[market_id]:
            try:
                await websocket.send_json({
                    'type': 'market_update',
                    'update': update,
                    'timestamp': datetime.utcnow().isoformat()
                })
            except:
                disconnected.add(websocket)
                
        # Remove disconnected websockets
        for ws in disconnected:
            websocket_connections[market_id].remove(ws)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8009) 