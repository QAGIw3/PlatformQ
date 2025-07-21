"""
Derivatives API endpoints for options and perpetual futures.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import logging

from ..dependencies import get_derivatives_protocol, get_current_user
from ..protocols.derivatives_protocol import DerivativesProtocol
from ..models.derivatives import (
    WriteOptionRequest, BuyOptionRequest, ExerciseOptionRequest,
    OpenPerpetualRequest, ClosePerpetualRequest, AddMarginRequest,
    CreateOptionsPoolRequest, AddOptionsLiquidityRequest,
    RemoveOptionsLiquidityRequest, OptionResponse, ExerciseResponse,
    PerpetualPositionResponse, PositionInfoResponse, GreeksResponse,
    OptionsPoolResponse, OptionPremiumQuote, MarketDataResponse,
    DerivativesStats, Option, PerpetualMarket, OptionsPool
)

router = APIRouter(prefix="/derivatives", tags=["derivatives"])
security = HTTPBearer()
logger = logging.getLogger(__name__)


# Options Endpoints

@router.post("/options/write", response_model=OptionResponse)
async def write_option(
    request: WriteOptionRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> OptionResponse:
    """
    Write a new option contract.
    
    Requires collateral:
    - CALL: Lock resource tokens equal to amount
    - PUT: Lock stablecoins equal to strike_price * amount
    """
    try:
        result = await derivatives.write_option(
            writer_address=current_user,
            resource_token_id=request.resource_token_id,
            strike_price=request.strike_price,
            expiry=request.expiry,
            option_type=request.option_type,
            style=request.style,
            amount=request.amount
        )
        
        return OptionResponse(
            option_id=result["option_id"],
            tx_hash=result["tx_hash"],
            collateral_locked=result.get("collateral_locked")
        )
        
    except Exception as e:
        logger.error(f"Error writing option: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/options/buy", response_model=OptionResponse)
async def buy_option(
    request: BuyOptionRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> OptionResponse:
    """
    Buy an option from the writer.
    
    Premium is calculated using Black-Scholes approximation.
    """
    try:
        result = await derivatives.buy_option(
            buyer_address=current_user,
            option_id=request.option_id
        )
        
        return OptionResponse(
            option_id=request.option_id,
            tx_hash=result["tx_hash"],
            premium=result["premium_paid"]
        )
        
    except Exception as e:
        logger.error(f"Error buying option: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/options/exercise", response_model=ExerciseResponse)
async def exercise_option(
    request: ExerciseOptionRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> ExerciseResponse:
    """
    Exercise an option if it's in the money.
    
    - European options: Can only be exercised near expiry
    - American options: Can be exercised anytime
    """
    try:
        result = await derivatives.exercise_option(
            holder_address=current_user,
            option_id=request.option_id
        )
        
        return ExerciseResponse(**result)
        
    except Exception as e:
        logger.error(f"Error exercising option: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/options/{option_id}", response_model=Option)
async def get_option(
    option_id: int,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> Option:
    """Get option details by ID."""
    try:
        option = derivatives._options.get(option_id)
        if not option:
            # Fetch from contract
            option_data = await derivatives.options_contract.functions.getOption(option_id).call()
            option = derivatives._parse_option_data(option_id, option_data)
            
        return option
        
    except Exception as e:
        logger.error(f"Error getting option: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Option not found"
        )


@router.get("/options/{option_id}/greeks", response_model=GreeksResponse)
async def get_option_greeks(
    option_id: int,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> GreeksResponse:
    """Calculate option Greeks (delta, gamma, theta, vega)."""
    try:
        greeks = await derivatives.calculate_option_greeks(option_id)
        
        return GreeksResponse(
            option_id=option_id,
            delta=greeks.delta,
            gamma=greeks.gamma,
            theta=greeks.theta,
            vega=greeks.vega,
            rho=greeks.rho
        )
        
    except Exception as e:
        logger.error(f"Error calculating Greeks: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


# Perpetuals Endpoints

@router.post("/perpetuals/open", response_model=PerpetualPositionResponse)
async def open_perpetual_position(
    request: OpenPerpetualRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> PerpetualPositionResponse:
    """
    Open a perpetual futures position.
    
    Maximum leverage: 20x
    Minimum margin: 5%
    """
    try:
        result = await derivatives.open_perpetual_position(
            trader_address=current_user,
            resource_token_id=request.resource_token_id,
            size=request.size,
            margin=request.margin,
            is_long=request.is_long
        )
        
        return PerpetualPositionResponse(**result)
        
    except Exception as e:
        logger.error(f"Error opening perpetual position: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/perpetuals/close", response_model=Dict[str, Any])
async def close_perpetual_position(
    request: ClosePerpetualRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Close a perpetual position.
    
    Size = 0 closes the entire position.
    """
    try:
        result = await derivatives.close_perpetual_position(
            trader_address=current_user,
            resource_token_id=request.resource_token_id,
            size=request.size
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error closing perpetual position: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/perpetuals/add-margin", response_model=Dict[str, Any])
async def add_margin(
    request: AddMarginRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> Dict[str, Any]:
    """Add margin to reduce liquidation risk."""
    try:
        result = await derivatives.add_margin(
            trader_address=current_user,
            resource_token_id=request.resource_token_id,
            amount=request.amount
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error adding margin: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/perpetuals/position/{resource_token_id}", response_model=PositionInfoResponse)
async def get_position_info(
    resource_token_id: int,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> PositionInfoResponse:
    """Get detailed position information including PnL and margin ratio."""
    try:
        info = await derivatives.get_position_info(
            trader_address=current_user,
            resource_token_id=resource_token_id
        )
        
        return PositionInfoResponse(**info)
        
    except Exception as e:
        logger.error(f"Error getting position info: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Position not found"
        )


@router.get("/perpetuals/markets", response_model=List[PerpetualMarket])
async def get_perpetual_markets(
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> List[PerpetualMarket]:
    """Get all active perpetual markets."""
    try:
        markets = []
        for resource_id in range(1, 4):  # CPU, GPU, Storage
            try:
                market_data = await derivatives.perpetuals_contract.functions.markets(
                    resource_id
                ).call()
                
                if market_data[7]:  # isActive
                    market = PerpetualMarket(
                        resource_token_id=resource_id,
                        open_interest=market_data[1],
                        long_open_interest=market_data[2],
                        short_open_interest=market_data[3],
                        funding_rate=market_data[4],
                        cumulative_funding=market_data[5],
                        last_funding_time=datetime.fromtimestamp(market_data[6]),
                        max_open_interest=market_data[7],
                        is_active=market_data[8],
                        index_price=derivatives._index_prices.get(resource_id),
                        mark_price=derivatives._mark_prices.get(resource_id)
                    )
                    markets.append(market)
            except:
                pass
                
        return markets
        
    except Exception as e:
        logger.error(f"Error getting perpetual markets: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch markets"
        )


# Options AMM Endpoints

@router.post("/amm/create-pool", response_model=OptionsPoolResponse)
async def create_options_pool(
    request: CreateOptionsPoolRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> OptionsPoolResponse:
    """Create a new options AMM pool for automated market making."""
    try:
        # Check LP role
        if not await _has_lp_role(current_user, derivatives):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="LP role required"
            )
            
        result = await derivatives.create_options_pool(
            creator_address=current_user,
            resource_token_id=request.resource_token_id,
            resource_amount=request.resource_amount,
            stablecoin_amount=request.stablecoin_amount,
            base_iv=request.base_iv
        )
        
        return OptionsPoolResponse(**result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating options pool: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/amm/add-liquidity", response_model=Dict[str, Any])
async def add_options_liquidity(
    request: AddOptionsLiquidityRequest,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol),
    current_user: str = Depends(get_current_user)
) -> Dict[str, Any]:
    """Add liquidity to an options AMM pool."""
    try:
        # Check LP role
        if not await _has_lp_role(current_user, derivatives):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="LP role required"
            )
            
        result = await derivatives.add_options_liquidity(
            provider_address=current_user,
            resource_token_id=request.resource_token_id,
            resource_amount=request.resource_amount,
            stablecoin_amount=request.stablecoin_amount
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding options liquidity: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/amm/quote", response_model=OptionPremiumQuote)
async def get_option_quote(
    resource_token_id: int = Query(..., ge=0),
    strike_price: int = Query(..., gt=0),
    expiry: datetime = Query(...),
    option_type: str = Query(..., regex="^(call|put)$"),
    amount: int = Query(..., gt=0),
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> OptionPremiumQuote:
    """Get a premium quote from the AMM for an option."""
    try:
        from ..models.derivatives import OptionType
        
        result = await derivatives.sell_option_via_amm(
            buyer_address="0x0000000000000000000000000000000000000000",  # Quote only
            resource_token_id=resource_token_id,
            strike_price=strike_price,
            expiry=expiry,
            option_type=OptionType(option_type),
            amount=amount
        )
        
        # Get pool info for utilization
        pool = derivatives._options_pools.get(resource_token_id)
        utilization = pool.utilization / 100 if pool else 0
        
        return OptionPremiumQuote(
            premium=result["premium"],
            strike_price=strike_price,
            expiry=result["expiry"],
            option_type=option_type,
            amount=amount,
            iv=pool.base_iv / 100 if pool else 0,
            pool_utilization=utilization
        )
        
    except Exception as e:
        logger.error(f"Error getting option quote: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/amm/pools", response_model=List[OptionsPool])
async def get_options_pools(
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> List[OptionsPool]:
    """Get all active options AMM pools."""
    try:
        pools = []
        for resource_id in range(1, 4):  # CPU, GPU, Storage
            try:
                pool_data = await derivatives.options_amm_contract.functions.pools(
                    resource_id
                ).call()
                
                if pool_data[6]:  # isActive
                    pool = OptionsPool(
                        resource_token_id=resource_id,
                        total_liquidity=pool_data[1],
                        resource_reserve=pool_data[2],
                        stablecoin_reserve=pool_data[3],
                        utilization=pool_data[4],
                        base_iv=pool_data[5],
                        is_active=pool_data[6],
                        created_at=datetime.utcnow()  # Would need to track this
                    )
                    pools.append(pool)
            except:
                pass
                
        return pools
        
    except Exception as e:
        logger.error(f"Error getting options pools: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch pools"
        )


# Market Data Endpoints

@router.get("/market/{resource_token_id}", response_model=MarketDataResponse)
async def get_market_data(
    resource_token_id: int,
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> MarketDataResponse:
    """Get comprehensive market data for a resource."""
    try:
        # Get perpetuals market data
        market_data = await derivatives.perpetuals_contract.functions.markets(
            resource_token_id
        ).call()
        
        # Get prices
        spot_price = derivatives._spot_prices.get(resource_token_id, 0)
        mark_price = derivatives._mark_prices.get(resource_token_id, 0)
        index_price = derivatives._index_prices.get(resource_token_id, 0)
        
        return MarketDataResponse(
            resource_token_id=resource_token_id,
            spot_price=spot_price,
            mark_price=mark_price,
            index_price=index_price,
            funding_rate=market_data[4],
            open_interest=market_data[1],
            long_open_interest=market_data[2],
            short_open_interest=market_data[3],
            volume_24h=0  # Would need to track this
        )
        
    except Exception as e:
        logger.error(f"Error getting market data: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Market not found"
        )


@router.get("/stats", response_model=DerivativesStats)
async def get_derivatives_stats(
    derivatives: DerivativesProtocol = Depends(get_derivatives_protocol)
) -> DerivativesStats:
    """Get overall derivatives platform statistics."""
    try:
        stats = await derivatives.get_derivatives_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Error getting derivatives stats: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch stats"
        )


# Helper functions

async def _has_lp_role(user_address: str, derivatives: DerivativesProtocol) -> bool:
    """Check if user has LP role in options AMM contract."""
    try:
        lp_role = await derivatives.options_amm_contract.functions.LP_ROLE().call()
        has_role = await derivatives.options_amm_contract.functions.hasRole(
            lp_role,
            user_address
        ).call()
        return has_role
    except:
        return False 