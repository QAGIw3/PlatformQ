"""
Fraud Patterns Module

Defines common fraud patterns for detection.
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any, List, Optional


class FraudPatternType(Enum):
    """Types of fraud patterns"""
    MONEY_LAUNDERING = "money_laundering"
    STRUCTURING = "structuring"
    RAPID_MOVEMENT = "rapid_movement"
    CIRCULAR_FLOW = "circular_flow"
    HIGH_RISK_JURISDICTION = "high_risk_jurisdiction"
    ABNORMAL_BEHAVIOR = "abnormal_behavior"
    NETWORK_ANOMALY = "network_anomaly"
    IDENTITY_THEFT = "identity_theft"
    ACCOUNT_TAKEOVER = "account_takeover"
    SYNTHETIC_IDENTITY = "synthetic_identity"


@dataclass
class FraudPattern:
    """Definition of a fraud pattern"""
    pattern_id: str
    pattern_type: FraudPatternType
    name: str
    description: str
    risk_weight: float
    graph_query: Optional[str] = None
    rules: Optional[Dict[str, Any]] = None
    indicators: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "pattern_id": self.pattern_id,
            "pattern_type": self.pattern_type.value,
            "name": self.name,
            "description": self.description,
            "risk_weight": self.risk_weight,
            "graph_query": self.graph_query,
            "rules": self.rules or {},
            "indicators": self.indicators or []
        }


# Common fraud patterns
COMMON_PATTERNS = [
    FraudPattern(
        pattern_id="ml_layering",
        pattern_type=FraudPatternType.MONEY_LAUNDERING,
        name="Layering Pattern",
        description="Multiple rapid transactions to obscure money trail",
        risk_weight=0.8,
        indicators=[
            "high_transaction_velocity",
            "multiple_intermediate_accounts",
            "cross_border_transfers"
        ]
    ),
    FraudPattern(
        pattern_id="structuring_001",
        pattern_type=FraudPatternType.STRUCTURING,
        name="Transaction Structuring",
        description="Breaking large transactions into smaller ones to avoid reporting",
        risk_weight=0.7,
        rules={
            "transaction_count": {"min": 3, "timeframe_hours": 24},
            "amount_threshold": {"near_limit": 0.9, "reporting_limit": 10000}
        }
    ),
    FraudPattern(
        pattern_id="circular_flow",
        pattern_type=FraudPatternType.CIRCULAR_FLOW,
        name="Circular Money Flow",
        description="Funds returning to origin through multiple hops",
        risk_weight=0.9,
        graph_query="g.V().has('entity_id', SOURCE_ID).repeat(out().simplePath()).until(has('entity_id', SOURCE_ID).or().loops().is(gt(MAX_DEPTH))).path()"
    ),
    FraudPattern(
        pattern_id="rapid_movement",
        pattern_type=FraudPatternType.RAPID_MOVEMENT,
        name="Rapid Fund Movement",
        description="Unusually fast movement of funds through accounts",
        risk_weight=0.6,
        rules={
            "velocity": {"transactions_per_hour": 10},
            "amount": {"min": 1000}
        }
    ),
    FraudPattern(
        pattern_id="high_risk_jurisdiction",
        pattern_type=FraudPatternType.HIGH_RISK_JURISDICTION,
        name="High Risk Jurisdiction",
        description="Transactions involving high-risk countries",
        risk_weight=0.5,
        indicators=[
            "fatf_grey_list",
            "fatf_black_list",
            "sanctions_country"
        ]
    )
] 