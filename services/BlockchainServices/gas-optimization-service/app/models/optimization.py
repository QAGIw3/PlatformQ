"""
Optimization models
"""

from enum import Enum
from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field


class OptimizationStrategy(str, Enum):
    """Optimization strategies"""
    STANDARD = "standard"
    BATCH = "batch"
    META_TRANSACTION = "meta_transaction"
    L2_MIGRATION = "l2_migration"
    TIME_BASED = "time_based"
    HYBRID = "hybrid"


class GasPriceLevel(str, Enum):
    """Gas price levels"""
    SLOW = "slow"
    STANDARD = "standard"
    FAST = "fast"
    INSTANT = "instant"


class OptimizationRequest(BaseModel):
    """Gas optimization request"""
    chain: str = Field(..., description="Blockchain identifier")
    transaction_type: str = Field(..., description="Type of transaction")
    from_address: str = Field(..., description="Sender address")
    to_address: str = Field(..., description="Recipient address")
    value: str = Field("0", description="Transaction value")
    data: Optional[str] = Field(None, description="Transaction data")
    
    # Optional parameters
    urgency: GasPriceLevel = Field(GasPriceLevel.STANDARD, description="Transaction urgency")
    max_wait_time: Optional[int] = Field(None, description="Max wait time in seconds")
    batch_eligible: bool = Field(True, description="Can be batched")
    meta_tx_eligible: bool = Field(True, description="Can use meta-transactions")
    
    # Context
    estimated_gas: Optional[int] = Field(None, description="Estimated gas usage")
    deadline: Optional[datetime] = Field(None, description="Transaction deadline")


class GasRecommendation(BaseModel):
    """Gas optimization recommendation"""
    strategy: OptimizationStrategy = Field(..., description="Recommended strategy")
    
    # Gas pricing
    gas_price: str = Field(..., description="Recommended gas price (wei)")
    max_fee_per_gas: Optional[str] = Field(None, description="Max fee per gas (EIP-1559)")
    max_priority_fee_per_gas: Optional[str] = Field(None, description="Max priority fee (EIP-1559)")
    
    # Cost estimates
    estimated_cost: str = Field(..., description="Estimated cost in wei")
    estimated_savings: str = Field("0", description="Estimated savings in wei")
    savings_percentage: float = Field(0.0, description="Savings percentage")
    
    # Timing
    recommended_time: Optional[datetime] = Field(None, description="Recommended execution time")
    expected_confirmation_time: int = Field(..., description="Expected confirmation time in seconds")
    
    # Additional info
    confidence_score: float = Field(..., description="Recommendation confidence (0-1)")
    reasoning: str = Field(..., description="Explanation of recommendation")
    alternatives: List[Dict[str, Any]] = Field(default_factory=list, description="Alternative strategies")


class BatchOptimization(BaseModel):
    """Batch transaction optimization"""
    batch_id: str = Field(..., description="Batch identifier")
    transactions: List[str] = Field(..., description="Transaction IDs in batch")
    total_gas_saved: str = Field(..., description="Total gas saved")
    batch_gas_cost: str = Field(..., description="Total batch gas cost")
    individual_gas_cost: str = Field(..., description="Sum of individual gas costs")
    savings_percentage: float = Field(..., description="Savings percentage")
    
    # Batch details
    batch_contract: str = Field(..., description="Batch execution contract")
    execution_time: datetime = Field(..., description="Planned execution time")
    status: str = Field("pending", description="Batch status")


class L2Suggestion(BaseModel):
    """Layer 2 migration suggestion"""
    current_chain: str = Field(..., description="Current chain")
    suggested_chain: str = Field(..., description="Suggested L2 chain")
    
    # Cost comparison
    l1_cost: str = Field(..., description="L1 transaction cost")
    l2_cost: str = Field(..., description="L2 transaction cost")
    bridge_cost: str = Field(..., description="Bridge cost (if needed)")
    total_savings: str = Field(..., description="Total potential savings")
    
    # Bridge info
    bridge_available: bool = Field(..., description="Is bridge available")
    bridge_contract: Optional[str] = Field(None, description="Bridge contract address")
    bridge_time: Optional[int] = Field(None, description="Bridge time in seconds")
    
    # Considerations
    security_score: float = Field(..., description="L2 security score (0-1)")
    liquidity_available: bool = Field(..., description="Sufficient liquidity on L2")
    compatibility_issues: List[str] = Field(default_factory=list, description="Known issues")


class MetaTransactionOption(BaseModel):
    """Meta-transaction option"""
    relayer_address: str = Field(..., description="Relayer address")
    relayer_fee: str = Field(..., description="Relayer fee in wei")
    total_cost: str = Field(..., description="Total cost including fee")
    
    # Comparison
    direct_cost: str = Field(..., description="Direct transaction cost")
    savings: str = Field(..., description="Savings (can be negative)")
    
    # Relayer info
    relayer_reputation: float = Field(..., description="Relayer reputation (0-1)")
    average_confirmation_time: int = Field(..., description="Average confirmation seconds")
    success_rate: float = Field(..., description="Success rate (0-1)")


class GasPricePrediction(BaseModel):
    """Gas price prediction"""
    timestamp: datetime = Field(..., description="Prediction timestamp")
    chain: str = Field(..., description="Blockchain identifier")
    
    # Predictions
    predictions: Dict[int, Dict[str, str]] = Field(
        ..., 
        description="Predictions by minute offset"
    )
    
    # Model info
    model_confidence: float = Field(..., description="Model confidence (0-1)")
    features_used: List[str] = Field(..., description="Features used for prediction")
    
    # Recommendations
    best_time_window: Dict[str, Any] = Field(..., description="Best time window for transaction")
    potential_savings: str = Field(..., description="Potential savings by waiting")


class OptimizationMetrics(BaseModel):
    """Service metrics"""
    total_optimizations: int = Field(..., description="Total optimizations performed")
    total_gas_saved: str = Field(..., description="Total gas saved in wei")
    average_savings_percentage: float = Field(..., description="Average savings percentage")
    
    # Strategy breakdown
    strategy_usage: Dict[str, int] = Field(..., description="Usage by strategy")
    strategy_savings: Dict[str, str] = Field(..., description="Savings by strategy")
    
    # Performance
    average_response_time: float = Field(..., description="Average response time in ms")
    model_accuracy: float = Field(..., description="Prediction model accuracy")
    
    # Time window
    period_start: datetime = Field(..., description="Metrics period start")
    period_end: datetime = Field(..., description="Metrics period end") 