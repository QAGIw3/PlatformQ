"""Stress testing API endpoints."""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..core import StressTester
from ..models import StressTestScenario, StressTestResult
from ..dependencies import get_stress_tester, get_state_manager
from ..auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stress-test", tags=["Stress Testing"])


class ScenarioRequest(BaseModel):
    """Request to create a stress test scenario."""
    name: str = Field(..., description="Scenario name")
    description: str = Field(..., description="Scenario description")
    market_shocks: Dict[str, Decimal] = Field(..., description="Market price shocks")
    volatility_shocks: Optional[Dict[str, Decimal]] = Field(None, description="Volatility shocks")
    liquidity_haircuts: Optional[Dict[str, Decimal]] = Field(None, description="Liquidity haircuts")
    correlation_shifts: Optional[Dict[str, Decimal]] = Field(None, description="Correlation shifts")
    duration_days: int = Field(1, ge=1, le=30, description="Scenario duration")
    severity: str = Field("moderate", pattern="^(mild|moderate|severe|extreme)$")


class StressTestRequest(BaseModel):
    """Request to run a stress test."""
    scenario_ids: List[str] = Field(..., description="Scenarios to run")
    portfolio_ids: Optional[List[str]] = Field(None, description="Specific portfolios to test")
    include_correlations: bool = Field(True, description="Include correlation effects")
    include_liquidity: bool = Field(True, description="Include liquidity impacts")


class StressTestResponse(BaseModel):
    """Stress test results."""
    test_id: str
    timestamp: str
    scenarios_run: int
    portfolios_tested: int
    worst_case_loss: str
    worst_case_scenario: str
    results_by_scenario: Dict[str, Dict[str, Any]]
    aggregated_metrics: Dict[str, str]
    warnings: List[str]
    execution_time: float


@router.post("/scenarios", response_model=Dict[str, str])
async def create_scenario(
    request: ScenarioRequest,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, str]:
    """Create a new stress test scenario."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Create scenario
    scenario = StressTestScenario(
        scenario_id=f"sts_{uuid.uuid4().hex[:8]}",
        name=request.name,
        description=request.description,
        market_shocks=request.market_shocks,
        volatility_shocks=request.volatility_shocks or {},
        liquidity_haircuts=request.liquidity_haircuts or {},
        correlation_shifts=request.correlation_shifts or {},
        duration_days=request.duration_days,
        severity=request.severity,
        created_by=current_user["user_id"],
        created_at=datetime.utcnow()
    )
    
    # Store scenario
    await state_manager.create_stress_scenario(scenario)
    
    return {
        "scenario_id": scenario.scenario_id,
        "name": scenario.name,
        "status": "created"
    }


@router.get("/scenarios")
async def list_scenarios(
    severity: Optional[str] = Query(None, pattern="^(mild|moderate|severe|extreme)$"),
    include_system: bool = Query(True, description="Include system scenarios"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """List available stress test scenarios."""
    scenarios = await state_manager.get_stress_scenarios(
        severity=severity,
        include_system=include_system,
        created_by=current_user["user_id"] if not include_system else None
    )
    
    return [
        {
            "scenario_id": s.scenario_id,
            "name": s.name,
            "description": s.description,
            "severity": s.severity,
            "market_shocks": {k: str(v) for k, v in s.market_shocks.items()},
            "created_by": s.created_by,
            "created_at": s.created_at.isoformat()
        }
        for s in scenarios
    ]


@router.post("/run", response_model=StressTestResponse)
async def run_stress_test(
    request: StressTestRequest,
    current_user: Dict = Depends(get_current_user),
    stress_tester: StressTester = Depends(get_stress_tester),
    state_manager = Depends(get_state_manager)
) -> StressTestResponse:
    """Run stress tests on portfolios."""
    # Start timer
    start_time = datetime.utcnow()
    
    # Get scenarios
    scenarios = []
    for scenario_id in request.scenario_ids:
        scenario = await state_manager.get_stress_scenario(scenario_id)
        if not scenario:
            raise HTTPException(status_code=404, detail=f"Scenario {scenario_id} not found")
        scenarios.append(scenario)
    
    # Get portfolios to test
    if request.portfolio_ids:
        portfolio_ids = request.portfolio_ids
    else:
        # Test all user portfolios
        portfolio_ids = await state_manager.get_user_portfolios(current_user["user_id"])
    
    # Run stress tests
    test_id = f"st_{datetime.utcnow().timestamp()}"
    results_by_scenario = {}
    worst_loss = Decimal("0")
    worst_scenario = None
    warnings = []
    
    for scenario in scenarios:
        scenario_results = {}
        
        for portfolio_id in portfolio_ids:
            # Get portfolio positions
            positions = await state_manager.get_portfolio_positions(portfolio_id)
            if not positions:
                continue
            
            # Get market data
            market_ids = list(set(p.get("market_id") for p in positions))
            market_data = await state_manager.get_market_data_batch(market_ids)
            
            # Run stress test
            result = await stress_tester.run_scenario(
                scenario=scenario,
                positions=positions,
                market_data=market_data,
                include_correlations=request.include_correlations,
                include_liquidity=request.include_liquidity
            )
            
            scenario_results[portfolio_id] = {
                "portfolio_value": str(result.portfolio_value),
                "stressed_value": str(result.stressed_value),
                "loss_amount": str(result.loss_amount),
                "loss_percentage": str(result.loss_percentage),
                "var_breach": result.var_breach,
                "margin_call": result.margin_call,
                "liquidations": len(result.liquidations)
            }
            
            # Track worst case
            if result.loss_amount > worst_loss:
                worst_loss = result.loss_amount
                worst_scenario = scenario.name
            
            # Collect warnings
            if result.var_breach:
                warnings.append(f"VaR breach in {portfolio_id} under {scenario.name}")
            if result.margin_call:
                warnings.append(f"Margin call triggered in {portfolio_id} under {scenario.name}")
            if result.liquidations:
                warnings.append(f"{len(result.liquidations)} positions would be liquidated in {portfolio_id}")
        
        results_by_scenario[scenario.scenario_id] = scenario_results
    
    # Calculate aggregated metrics
    total_portfolios = len(portfolio_ids)
    total_scenarios = len(scenarios)
    
    # Calculate execution time
    exec_time = (datetime.utcnow() - start_time).total_seconds()
    
    # Store results
    await state_manager.store_stress_test_results(
        test_id=test_id,
        user_id=current_user["user_id"],
        results=results_by_scenario,
        metadata={
            "scenarios": request.scenario_ids,
            "portfolios": portfolio_ids,
            "worst_loss": str(worst_loss),
            "worst_scenario": worst_scenario,
            "warnings": warnings
        }
    )
    
    return StressTestResponse(
        test_id=test_id,
        timestamp=datetime.utcnow().isoformat(),
        scenarios_run=total_scenarios,
        portfolios_tested=total_portfolios,
        worst_case_loss=str(worst_loss),
        worst_case_scenario=worst_scenario or "None",
        results_by_scenario=results_by_scenario,
        aggregated_metrics={
            "total_tests": str(total_scenarios * total_portfolios),
            "var_breaches": str(sum(1 for r in results_by_scenario.values() for p in r.values() if p.get("var_breach"))),
            "margin_calls": str(sum(1 for r in results_by_scenario.values() for p in r.values() if p.get("margin_call"))),
            "total_liquidations": str(sum(p.get("liquidations", 0) for r in results_by_scenario.values() for p in r.values()))
        },
        warnings=warnings,
        execution_time=exec_time
    )


@router.get("/results/{test_id}")
async def get_test_results(
    test_id: str,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Get detailed stress test results."""
    # Get results
    results = await state_manager.get_stress_test_results(test_id)
    if not results:
        raise HTTPException(status_code=404, detail="Test results not found")
    
    # Check authorization
    if results["user_id"] != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    return results


@router.get("/history")
async def get_test_history(
    days: int = Query(30, ge=1, le=365),
    portfolio_id: Optional[str] = Query(None, description="Filter by portfolio"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get stress test history."""
    # Get history
    history = await state_manager.get_stress_test_history(
        user_id=current_user["user_id"],
        days=days,
        portfolio_id=portfolio_id
    )
    
    return [
        {
            "test_id": test["test_id"],
            "timestamp": test["timestamp"].isoformat(),
            "scenarios_run": test["scenarios_run"],
            "portfolios_tested": test["portfolios_tested"],
            "worst_case_loss": str(test["worst_case_loss"]),
            "worst_case_scenario": test["worst_case_scenario"],
            "warnings_count": len(test.get("warnings", []))
        }
        for test in history
    ]


@router.post("/schedule")
async def schedule_stress_test(
    scenario_ids: List[str],
    frequency: str = Query(..., pattern="^(daily|weekly|monthly)$"),
    time_utc: str = Query(..., pattern="^([01]?[0-9]|2[0-3]):[0-5][0-9]$"),
    portfolio_ids: Optional[List[str]] = None,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Schedule recurring stress tests."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Create schedule
    schedule = {
        "schedule_id": f"sch_{uuid.uuid4().hex[:8]}",
        "scenario_ids": scenario_ids,
        "portfolio_ids": portfolio_ids,
        "frequency": frequency,
        "time_utc": time_utc,
        "created_by": current_user["user_id"],
        "created_at": datetime.utcnow(),
        "active": True
    }
    
    await state_manager.create_stress_test_schedule(schedule)
    
    return {
        "schedule_id": schedule["schedule_id"],
        "status": "scheduled",
        "next_run": _calculate_next_run(frequency, time_utc)
    }


@router.delete("/scenarios/{scenario_id}")
async def delete_scenario(
    scenario_id: str,
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, str]:
    """Delete a stress test scenario."""
    # Get scenario
    scenario = await state_manager.get_stress_scenario(scenario_id)
    if not scenario:
        raise HTTPException(status_code=404, detail="Scenario not found")
    
    # Check authorization
    if scenario.created_by != current_user["user_id"] and "admin" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Delete scenario
    await state_manager.delete_stress_scenario(scenario_id)
    
    return {
        "scenario_id": scenario_id,
        "status": "deleted"
    }


def _calculate_next_run(frequency: str, time_utc: str) -> str:
    """Calculate next scheduled run time."""
    from datetime import timedelta
    
    now = datetime.utcnow()
    hour, minute = map(int, time_utc.split(":"))
    
    # Calculate next occurrence
    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    
    if next_run <= now:
        if frequency == "daily":
            next_run += timedelta(days=1)
        elif frequency == "weekly":
            next_run += timedelta(days=7)
        elif frequency == "monthly":
            # Simple approximation
            next_run += timedelta(days=30)
    
    return next_run.isoformat() 