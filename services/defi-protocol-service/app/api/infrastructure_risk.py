"""
Infrastructure Risk API endpoints

Provides endpoints for unified risk assessment of infrastructure resources.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..services.infrastructure_risk_engine import InfrastructureRiskEngine
from ..models import ResourceType, ServiceTier

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class RiskAssessmentRequest(BaseModel):
    """Request for risk assessment"""
    resource_type: ResourceType
    service_tier: ServiceTier
    provider: str = Field(..., description="Provider address")
    amount: int = Field(..., gt=0, description="Amount of resources")
    duration_days: int = Field(..., ge=1, le=365, description="Duration in days")
    region: str = Field(default="us-east-1", description="Geographic region")
    loan_amount: Optional[Decimal] = Field(None, description="Loan amount if applicable")


class PortfolioPosition(BaseModel):
    """Single position in a portfolio"""
    id: str
    resource_type: ResourceType
    service_tier: ServiceTier
    provider: str
    amount: int
    value: Decimal
    duration_days: int
    region: str
    loan_amount: Optional[Decimal] = None


class PortfolioRiskRequest(BaseModel):
    """Request for portfolio risk assessment"""
    positions: List[PortfolioPosition]


class RiskAssessmentResponse(BaseModel):
    """Risk assessment results"""
    risk_score: float = Field(..., ge=0, le=100, description="Overall risk score (0-100)")
    risk_level: str = Field(..., description="Risk level: low, medium, high, critical, extreme")
    risk_factors: Dict[str, float] = Field(..., description="Individual risk factor scores")
    risk_premium: float = Field(..., description="Additional interest rate premium")
    required_collateral_ratio: float = Field(..., description="Required collateral ratio")
    timestamp: str


class PortfolioRiskResponse(BaseModel):
    """Portfolio risk assessment results"""
    portfolio_risk_score: float
    portfolio_risk_level: str
    risk_breakdown: Dict[str, float]
    high_risk_positions: List[Dict[str, Any]]
    total_positions: int
    total_value: float
    recommendations: List[str]


# Initialize risk engine
risk_engine = None


async def get_risk_engine() -> InfrastructureRiskEngine:
    """Get infrastructure risk engine instance"""
    global risk_engine
    if not risk_engine:
        from ..main import risk_calculator, price_oracle
        from ..services.resource_valuation import ResourceValuationService
        
        valuation_service = ResourceValuationService(price_oracle)
        risk_engine = InfrastructureRiskEngine(
            risk_calculator=risk_calculator,
            price_oracle=price_oracle,
            valuation_service=valuation_service
        )
    return risk_engine


@router.post("/assess", response_model=RiskAssessmentResponse)
async def assess_infrastructure_risk(
    request: RiskAssessmentRequest,
    current_user: Dict = Depends(get_current_user)
) -> RiskAssessmentResponse:
    """
    Assess risk for infrastructure resources
    
    - Combines DeFi and infrastructure-specific risks
    - Considers provider reliability, resource volatility, time decay
    - Returns risk score, level, and mitigation recommendations
    """
    engine = await get_risk_engine()
    
    try:
        result = await engine.calculate_unified_risk(
            resource_type=request.resource_type,
            service_tier=request.service_tier,
            provider=request.provider,
            amount=request.amount,
            duration_days=request.duration_days,
            region=request.region,
            loan_amount=request.loan_amount
        )
        
        return RiskAssessmentResponse(**result)
        
    except Exception as e:
        logger.error(f"Error assessing risk: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/portfolio", response_model=PortfolioRiskResponse)
async def assess_portfolio_risk(
    request: PortfolioRiskRequest,
    current_user: Dict = Depends(get_current_user)
) -> PortfolioRiskResponse:
    """
    Assess risk across a portfolio of infrastructure resources
    
    - Aggregates risk across multiple positions
    - Identifies concentration risks
    - Provides portfolio-level recommendations
    """
    engine = await get_risk_engine()
    
    try:
        # Convert positions to dict format
        portfolio = [position.dict() for position in request.positions]
        
        result = await engine.monitor_portfolio_risk(portfolio)
        
        return PortfolioRiskResponse(**result)
        
    except Exception as e:
        logger.error(f"Error assessing portfolio risk: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factors")
async def get_risk_factors() -> Dict[str, Any]:
    """
    Get information about risk factors and their weights
    
    - Lists all risk factors considered
    - Shows relative weights in risk calculation
    - Provides descriptions of each factor
    """
    from ..services.infrastructure_risk_engine import RiskFactor, InfrastructureRiskEngine
    
    factors = {}
    for factor in RiskFactor:
        weight = InfrastructureRiskEngine.RISK_WEIGHTS.get(factor, 0)
        factors[factor.value] = {
            "weight": weight / 10000,  # Convert to percentage
            "description": _get_factor_description(factor)
        }
        
    return {
        "risk_factors": factors,
        "total_weight": sum(f["weight"] for f in factors.values())
    }


@router.get("/thresholds")
async def get_risk_thresholds() -> Dict[str, float]:
    """Get risk score thresholds for different risk levels"""
    engine = await get_risk_engine()
    
    return {
        level: float(threshold)
        for level, threshold in engine.RISK_THRESHOLDS.items()
    }


@router.get("/recommendations/{risk_level}")
async def get_risk_recommendations(risk_level: str) -> Dict[str, Any]:
    """
    Get general risk mitigation recommendations for a risk level
    
    - Provides actionable recommendations
    - Suggests risk reduction strategies
    - Includes best practices
    """
    recommendations = {
        "low": [
            "Maintain current risk management practices",
            "Consider slightly increasing exposure for higher returns",
            "Monitor for any changes in provider reliability"
        ],
        "medium": [
            "Diversify across multiple providers and regions",
            "Consider shorter duration commitments",
            "Implement regular risk monitoring"
        ],
        "high": [
            "Reduce exposure to volatile resources",
            "Increase collateral ratios",
            "Consider hedging strategies",
            "Avoid long-duration commitments"
        ],
        "critical": [
            "Immediate risk reduction required",
            "Exit high-risk positions",
            "Increase monitoring frequency",
            "Consider insurance coverage"
        ],
        "extreme": [
            "Emergency risk mitigation needed",
            "Liquidate risky positions immediately",
            "Halt new exposures",
            "Seek professional risk management advice"
        ]
    }
    
    if risk_level not in recommendations:
        raise HTTPException(status_code=400, detail="Invalid risk level")
        
    return {
        "risk_level": risk_level,
        "recommendations": recommendations[risk_level],
        "priority": _get_risk_priority(risk_level)
    }


@router.post("/simulate")
async def simulate_risk_scenario(
    base_request: RiskAssessmentRequest,
    scenarios: List[Dict[str, Any]],
    current_user: Dict = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """
    Simulate risk under different scenarios
    
    - Test impact of parameter changes
    - Stress test positions
    - Evaluate risk mitigation strategies
    """
    engine = await get_risk_engine()
    results = []
    
    try:
        # Calculate base scenario
        base_result = await engine.calculate_unified_risk(
            resource_type=base_request.resource_type,
            service_tier=base_request.service_tier,
            provider=base_request.provider,
            amount=base_request.amount,
            duration_days=base_request.duration_days,
            region=base_request.region,
            loan_amount=base_request.loan_amount
        )
        
        results.append({
            "scenario": "base",
            "parameters": base_request.dict(),
            "risk_score": base_result["risk_score"],
            "risk_level": base_result["risk_level"]
        })
        
        # Calculate scenario variations
        for i, scenario in enumerate(scenarios):
            # Apply scenario modifications
            scenario_params = base_request.dict()
            scenario_params.update(scenario)
            
            scenario_result = await engine.calculate_unified_risk(
                resource_type=ResourceType(scenario_params["resource_type"]),
                service_tier=ServiceTier(scenario_params["service_tier"]),
                provider=scenario_params["provider"],
                amount=scenario_params["amount"],
                duration_days=scenario_params["duration_days"],
                region=scenario_params["region"],
                loan_amount=scenario_params.get("loan_amount")
            )
            
            results.append({
                "scenario": f"scenario_{i+1}",
                "parameters": scenario_params,
                "risk_score": scenario_result["risk_score"],
                "risk_level": scenario_result["risk_level"],
                "risk_change": scenario_result["risk_score"] - base_result["risk_score"]
            })
            
    except Exception as e:
        logger.error(f"Error simulating risk scenarios: {e}")
        raise HTTPException(status_code=500, detail=str(e))
        
    return results


def _get_factor_description(factor) -> str:
    """Get description for a risk factor"""
    descriptions = {
        "provider_reliability": "Risk based on provider's track record, uptime, and SLA compliance",
        "resource_volatility": "Price volatility risk for the specific resource type",
        "time_decay": "Risk from resource value decay over time",
        "utilization_rate": "Risk from high resource utilization affecting availability",
        "geographic_concentration": "Risk from geographic factors and regional stability",
        "technology_obsolescence": "Risk of technology becoming outdated",
        "regulatory_compliance": "Risk from regulatory changes and compliance requirements",
        "counterparty_risk": "Risk of provider default on obligations"
    }
    
    return descriptions.get(factor.value, "Unknown risk factor")


def _get_risk_priority(risk_level: str) -> str:
    """Get priority level for risk mitigation"""
    priorities = {
        "low": "routine",
        "medium": "moderate",
        "high": "urgent",
        "critical": "immediate",
        "extreme": "emergency"
    }
    
    return priorities.get(risk_level, "unknown") 