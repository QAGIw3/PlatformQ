"""Stress test models."""

from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class StressTestScenario(BaseModel):
    """Stress test scenario definition."""
    scenario_id: str
    name: str
    description: str
    
    # Market shocks (market_id -> price change percentage)
    market_shocks: Dict[str, Decimal] = Field(default_factory=dict)
    
    # Volatility shocks (market_id -> volatility multiplier)
    volatility_shocks: Dict[str, Decimal] = Field(default_factory=dict)
    
    # Liquidity haircuts (market_id -> haircut percentage)
    liquidity_haircuts: Dict[str, Decimal] = Field(default_factory=dict)
    
    # Correlation shifts
    correlation_shifts: Dict[str, Decimal] = Field(default_factory=dict)
    
    # Scenario parameters
    duration_days: int = 1
    severity: str = "moderate"  # mild, moderate, severe, extreme
    
    # Metadata
    created_by: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    is_system: bool = False


class StressTestResult(BaseModel):
    """Result of a stress test on a portfolio."""
    test_id: str
    scenario_id: str
    portfolio_id: str
    
    # Value impacts
    portfolio_value: Decimal
    stressed_value: Decimal
    loss_amount: Decimal
    loss_percentage: Decimal
    
    # Risk metrics under stress
    stressed_var: Decimal
    stressed_leverage: Decimal
    stressed_margin_ratio: Decimal
    
    # Breach indicators
    var_breach: bool = False
    margin_call: bool = False
    liquidations: List[str] = Field(default_factory=list)  # Position IDs
    
    # Detailed impacts
    position_impacts: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    
    # Execution
    execution_time_ms: float
    timestamp: datetime = Field(default_factory=datetime.utcnow)
