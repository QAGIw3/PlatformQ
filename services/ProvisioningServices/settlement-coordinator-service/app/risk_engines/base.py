"""Base risk engine interface"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from app.models.settlement import Settlement, RiskAssessment, ProviderMetrics


class BaseRiskEngine(ABC):
    """Abstract base class for risk calculation engines"""
    
    @abstractmethod
    async def calculate_risk(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics,
        market_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Calculate risk for a settlement
        
        Args:
            settlement: Settlement to assess
            provider_metrics: Historical metrics for the provider
            market_data: Optional market data for advanced calculations
            
        Returns:
            Risk calculation results
        """
        pass
    
    @abstractmethod
    def get_engine_name(self) -> str:
        """Return the name of this risk engine"""
        pass
    
    def validate_inputs(
        self,
        settlement: Settlement,
        provider_metrics: ProviderMetrics
    ) -> bool:
        """Validate input data before risk calculation"""
        if not settlement or not provider_metrics:
            return False
        
        if settlement.total_value <= 0:
            return False
            
        if provider_metrics.uptime_percentage < 0 or provider_metrics.uptime_percentage > 1:
            return False
            
        return True 