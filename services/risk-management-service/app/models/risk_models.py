"""Risk models for ML-based risk assessment"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class MarketRisk:
    """Market-level risk assessment"""
    market_id: str
    timestamp: datetime
    current_volatility: Decimal
    predicted_volatility: Decimal
    anomaly_score: float
    var_95: Decimal  # Value at Risk at 95% confidence
    var_99: Decimal  # Value at Risk at 99% confidence
    liquidity_score: Decimal
    correlation_risk: Decimal
    recommended_params: Dict[str, Decimal]
    risk_level: str  # "low", "medium", "high", "critical"
    warnings: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses"""
        return {
            "market_id": self.market_id,
            "timestamp": self.timestamp.isoformat(),
            "current_volatility": str(self.current_volatility),
            "predicted_volatility": str(self.predicted_volatility),
            "anomaly_score": self.anomaly_score,
            "var_95": str(self.var_95),
            "var_99": str(self.var_99),
            "liquidity_score": str(self.liquidity_score),
            "correlation_risk": str(self.correlation_risk),
            "recommended_params": {k: str(v) for k, v in self.recommended_params.items()},
            "risk_level": self.risk_level,
            "warnings": self.warnings
        }


@dataclass
class PositionRisk:
    """Position-level risk assessment"""
    position_id: str
    market_risk: MarketRisk
    liquidation_probability: Decimal
    expected_shortfall: Decimal
    margin_utilization: Decimal
    health_factor: Decimal
    stress_test_results: List[Dict]
    recommendations: List[str]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API responses"""
        return {
            "position_id": self.position_id,
            "market_risk": self.market_risk.to_dict(),
            "liquidation_probability": str(self.liquidation_probability),
            "expected_shortfall": str(self.expected_shortfall),
            "margin_utilization": str(self.margin_utilization),
            "health_factor": str(self.health_factor),
            "stress_test_results": self.stress_test_results,
            "recommendations": self.recommendations
        }
    
    @property
    def risk_score(self) -> float:
        """Calculate overall risk score (0-1, 1 being highest risk)"""
        # Weighted combination of risk factors
        liq_risk = float(self.liquidation_probability)
        margin_risk = float(self.margin_utilization)
        health_risk = max(0, 1 - float(self.health_factor))
        market_risk_score = 0.25 if self.market_risk.risk_level == "low" else \
                           0.5 if self.market_risk.risk_level == "medium" else \
                           0.75 if self.market_risk.risk_level == "high" else 1.0
        
        # Weighted average
        risk_score = (
            liq_risk * 0.4 +
            margin_risk * 0.2 +
            health_risk * 0.2 +
            market_risk_score * 0.2
        )
        
        return min(1.0, risk_score) 