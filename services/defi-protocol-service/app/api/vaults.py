"""
Vault API endpoints

Handles infrastructure vault operations and strategies.
"""

from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status

from platformq_shared.auth import get_current_user
from ..dependencies import get_vault_protocol
from ..models import (
    CreateVaultRequest, AddStrategyRequest, DepositRequest,
    VaultWithdrawRequest, UpdateStrategyRequest, HarvestRequest,
    VaultResponse, StrategyResponse, DepositResponse,
    VaultWithdrawResponse, HarvestResponse, VaultStats,
    UserVaultBalance, StrategyDetails, VaultPerformance,
    StrategyType
)
from ..protocols.vault_protocol import VaultProtocol

router = APIRouter(prefix="/vaults", tags=["vaults"])


@router.post("/", response_model=VaultResponse)
async def create_vault(
    request: CreateVaultRequest,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """
    Create a new infrastructure vault.
    
    Requires VAULT_CREATOR role.
    """
    # Check permissions
    if "VAULT_CREATOR" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Vault creator role required"
        )
    
    try:
        result = await protocol.create_vault(
            resource_token_id=request.resource_token_id,
            name=request.name,
            symbol=request.symbol,
            management_fee=request.management_fee,
            performance_fee=request.performance_fee
        )
        
        return VaultResponse(
            vault_address=result["vault_address"],
            tx_hash=result["tx_hash"],
            name=result["name"],
            symbol=result["symbol"],
            resource_token_id=request.resource_token_id
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/{vault_address}/strategies", response_model=StrategyResponse)
async def add_strategy(
    vault_address: str,
    request: AddStrategyRequest,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """
    Add a strategy to a vault.
    
    Requires STRATEGIST role.
    """
    # Check permissions
    if "STRATEGIST" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Strategist role required"
        )
    
    try:
        result = await protocol.add_strategy(
            vault_address=vault_address,
            strategy_type=request.strategy_type,
            strategy_config=request.strategy_config,
            debt_ratio=request.debt_ratio,
            min_debt_per_harvest=request.min_debt_per_harvest,
            max_debt_per_harvest=request.max_debt_per_harvest
        )
        
        return StrategyResponse(
            strategy_address=result["strategy_address"],
            tx_hash=result["tx_hash"],
            strategy_type=result["strategy_type"],
            debt_ratio=result["debt_ratio"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/deposit", response_model=DepositResponse)
async def deposit(
    request: DepositRequest,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Deposit resources into a vault."""
    try:
        result = await protocol.deposit(
            vault_address=request.vault_address,
            user_address=current_user["wallet_address"],
            amount=request.amount
        )
        
        # Calculate value
        value = result["shares"] * result["price_per_share"] // 10**18
        
        return DepositResponse(
            tx_hash=result["tx_hash"],
            shares=result["shares"],
            price_per_share=result["price_per_share"],
            value=value
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/withdraw", response_model=VaultWithdrawResponse)
async def withdraw(
    request: VaultWithdrawRequest,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Withdraw from a vault."""
    try:
        result = await protocol.withdraw(
            vault_address=request.vault_address,
            user_address=current_user["wallet_address"],
            shares=request.shares,
            max_loss=request.max_loss
        )
        
        return VaultWithdrawResponse(
            tx_hash=result["tx_hash"],
            amount=result["amount"],
            shares_burned=result["shares_burned"],
            loss=result.get("loss")
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/strategies/{strategy_address}/harvest", response_model=HarvestResponse)
async def harvest_strategy(
    strategy_address: str,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """
    Harvest a strategy.
    
    Can be called by keepers or strategists.
    """
    try:
        result = await protocol.harvest_strategy(strategy_address)
        
        return HarvestResponse(
            tx_hash=result["tx_hash"],
            profit=result["profit"],
            loss=result["loss"],
            apy=result["apy"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/strategies/{strategy_address}", response_model=Dict[str, Any])
async def update_strategy(
    strategy_address: str,
    request: UpdateStrategyRequest,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """
    Update strategy parameters.
    
    Requires STRATEGIST role.
    """
    # Check permissions
    if "STRATEGIST" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Strategist role required"
        )
    
    try:
        # Get vault address from strategy
        strategy = protocol._strategies.get(strategy_address)
        if not strategy:
            raise ValueError("Strategy not found")
        
        result = await protocol.update_strategy_debt_ratio(
            vault_address=strategy.vault_address,
            strategy_address=strategy_address,
            new_debt_ratio=request.debt_ratio
        )
        
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/{vault_address}/emergency-shutdown", response_model=Dict[str, Any])
async def emergency_shutdown(
    vault_address: str,
    active: bool,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """
    Activate or deactivate emergency shutdown.
    
    Requires GUARDIAN role.
    """
    # Check permissions
    if "GUARDIAN" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Guardian role required"
        )
    
    try:
        result = await protocol.emergency_shutdown(vault_address, active)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/{vault_address}/stats", response_model=VaultStats)
async def get_vault_stats(
    vault_address: str,
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Get vault statistics."""
    try:
        stats = await protocol.get_vault_stats(vault_address)
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{vault_address}/user/balance", response_model=UserVaultBalance)
async def get_user_balance(
    vault_address: str,
    current_user: dict = Depends(get_current_user),
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Get user's vault balance."""
    try:
        balance = await protocol.get_user_balance(
            vault_address,
            current_user["wallet_address"]
        )
        
        # Calculate profit/loss if user has deposits
        profit_loss = None
        percentage_gain = None
        
        if current_user["wallet_address"] in protocol._user_deposits:
            deposits = protocol._user_deposits[current_user["wallet_address"]].get(vault_address, [])
            if deposits:
                total_deposited = sum(d.amount for d in deposits)
                profit_loss = balance["value"] - total_deposited
                if total_deposited > 0:
                    percentage_gain = (profit_loss / total_deposited) * 100
        
        return UserVaultBalance(
            shares=balance["shares"],
            value=balance["value"],
            price_per_share=balance["price_per_share"],
            profit_loss=profit_loss,
            percentage_gain=percentage_gain
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/{vault_address}/strategies", response_model=List[StrategyDetails])
async def get_vault_strategies(
    vault_address: str,
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Get all strategies for a vault."""
    strategies = []
    
    for strategy_address, strategy in protocol._strategies.items():
        if strategy.vault_address == vault_address:
            # Calculate health score
            total_value = strategy.total_debt + strategy.total_gain - strategy.total_loss
            health_score = 100.0
            if strategy.total_debt > 0:
                loss_ratio = strategy.total_loss / strategy.total_debt
                health_score = max(0, 100 * (1 - loss_ratio))
            
            strategies.append(StrategyDetails(
                address=strategy_address,
                name=f"{strategy.strategy_type.value} Strategy",
                strategy_type=strategy.strategy_type,
                debt_ratio=strategy.debt_ratio,
                total_debt=strategy.total_debt,
                total_gain=strategy.total_gain,
                total_loss=strategy.total_loss,
                estimated_apy=await protocol._calculate_strategy_apy(strategy_address),
                last_report=strategy.last_report,
                health_score=health_score
            ))
    
    return strategies


@router.get("/{vault_address}/performance", response_model=VaultPerformance)
async def get_vault_performance(
    vault_address: str,
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """Get vault performance metrics."""
    vault = protocol._vaults.get(vault_address)
    if not vault:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Vault not found"
        )
    
    # Calculate APYs (simplified - in production would use historical data)
    yearly_apy = await protocol._calculate_vault_apy(vault_address)
    
    # Calculate strategy allocations
    total_debt = 0
    strategy_allocations = {}
    
    for strategy in protocol._strategies.values():
        if strategy.vault_address == vault_address:
            total_debt += strategy.total_debt
            if strategy.strategy_type.value not in strategy_allocations:
                strategy_allocations[strategy.strategy_type.value] = 0
            strategy_allocations[strategy.strategy_type.value] += strategy.total_debt
    
    # Convert to percentages
    if total_debt > 0:
        for strategy_type in strategy_allocations:
            strategy_allocations[strategy_type] = (
                strategy_allocations[strategy_type] / total_debt * 100
            )
    
    return VaultPerformance(
        vault_address=vault_address,
        daily_apy=yearly_apy / 365,
        weekly_apy=yearly_apy / 52,
        monthly_apy=yearly_apy / 12,
        yearly_apy=yearly_apy,
        total_returns=vault.total_assets - total_debt,
        strategy_allocations=strategy_allocations
    )


@router.get("/", response_model=List[Dict[str, Any]])
async def list_vaults(
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """List all vaults."""
    vaults = []
    
    for address, vault in protocol._vaults.items():
        stats = await protocol.get_vault_stats(address)
        
        vaults.append({
            "address": address,
            "name": vault.name,
            "symbol": vault.symbol,
            "resource_token_id": vault.resource_token_id,
            "tvl": stats.tvl,
            "apy": stats.apy,
            "total_shares": stats.total_shares,
            "price_per_share": stats.price_per_share,
            "emergency_shutdown": stats.emergency_shutdown
        })
    
    return vaults


@router.get("/strategies", response_model=List[Dict[str, Any]])
async def list_all_strategies(
    protocol: VaultProtocol = Depends(get_vault_protocol)
):
    """List all strategies across all vaults."""
    strategies = []
    
    for address, strategy in protocol._strategies.items():
        strategies.append({
            "address": address,
            "vault_address": strategy.vault_address,
            "strategy_type": strategy.strategy_type.value,
            "debt_ratio": strategy.debt_ratio,
            "total_debt": strategy.total_debt,
            "total_gain": strategy.total_gain,
            "total_loss": strategy.total_loss,
            "is_active": strategy.is_active,
            "last_report": strategy.last_report.isoformat()
        })
    
    return strategies 