"""Risk state models"""

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from platformq_trading_common.risk.models import RiskMetrics


class AlertLevel(Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class MarginStatus:
    """Margin status for a trader"""
    margin_level: Decimal  # Percentage
    margin_used: Decimal
    free_margin: Decimal
    equity: Decimal
    is_margin_call: bool = False
    is_liquidation: bool = False
    
    @property
    def health_status(self) -> str:
        """Get health status based on margin level"""
        if self.is_liquidation:
            return "liquidation"
        elif self.is_margin_call:
            return "margin_call"
        elif self.margin_level < Decimal("150"):
            return "warning"
        elif self.margin_level < Decimal("200"):
            return "moderate"
        else:
            return "healthy"


@dataclass
class RiskState:
    """Current risk state for a trader"""
    trader_id: str
    risk_metrics: RiskMetrics
    margin_status: MarginStatus
    active_alerts: List[Dict]
    last_check: datetime
    
    @property
    def has_critical_alerts(self) -> bool:
        """Check if there are any critical alerts"""
        return any(alert.get("level") == AlertLevel.CRITICAL for alert in self.active_alerts)
    
    @property
    def has_high_alerts(self) -> bool:
        """Check if there are any high alerts"""
        return any(alert.get("level") == AlertLevel.HIGH for alert in self.active_alerts) 