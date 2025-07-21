"""
Unified Risk Manager

Integrates risk assessment across all trading services with real-time state management.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from platformq_shared.state_management import StateManagementClient, CacheConfig
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class UnifiedPosition:
    """Unified position across all trading types"""
    position_id: str
    user_id: str
    tenant_id: str
    market_type: str  # spot, futures, options, defi
    market_id: str
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal
    collateral: Decimal
    borrowed: Decimal
    unrealized_pnl: Decimal
    margin_ratio: Decimal
    liquidation_price: Optional[Decimal]
    metadata: Dict[str, Any]


class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class UnifiedRiskManager:
    """
    Centralized risk management across all trading services.
    Integrates with risk-engine-service and insurance-pool-service.
    """
    
    def __init__(self,
                 state_client: StateManagementClient,
                 risk_engine_client,
                 insurance_pool_client,
                 ml_risk_engine,
                 event_publisher: EventPublisher):
        self.state = state_client
        self.risk_engine = risk_engine_client
        self.insurance_pool = insurance_pool_client
        self.ml_risk = ml_risk_engine
        self.events = event_publisher
        
        # Initialize risk caches
        self._init_task = asyncio.create_task(self._initialize_caches())
        
    async def _initialize_caches(self):
        """Initialize distributed risk caches"""
        # User positions cache
        await self.state.create_cache(CacheConfig(
            name="unified_positions",
            cache_mode="PARTITIONED",
            backups=2,
            atomicity_mode="TRANSACTIONAL",
            eviction_policy="LRU",
            eviction_max_size=5000000
        ))
        
        # Risk metrics cache
        await self.state.create_cache(CacheConfig(
            name="risk_metrics",
            cache_mode="REPLICATED",
            backups=2,
            atomicity_mode="ATOMIC",
            eviction_policy="LRU",
            eviction_max_size=100000
        ))
        
        # Cross-margin accounts
        await self.state.create_cache(CacheConfig(
            name="cross_margin_accounts",
            cache_mode="PARTITIONED",
            backups=2,
            atomicity_mode="TRANSACTIONAL"
        ))
        
        logger.info("Risk management caches initialized")
    
    async def assess_unified_risk(self, user_id: str, tenant_id: str) -> Dict[str, Any]:
        """
        Assess unified risk across all positions for a user.
        Integrates spot, futures, options, and DeFi positions.
        """
        # Get all positions from state
        positions = await self._get_all_user_positions(user_id, tenant_id)
        
        # Calculate portfolio metrics
        portfolio_value = Decimal("0")
        total_collateral = Decimal("0")
        total_borrowed = Decimal("0")
        total_unrealized_pnl = Decimal("0")
        
        position_risks = []
        
        for position in positions:
            portfolio_value += position.size * position.mark_price
            total_collateral += position.collateral
            total_borrowed += position.borrowed
            total_unrealized_pnl += position.unrealized_pnl
            
            # Assess individual position risk
            position_risk = await self._assess_position_risk(position)
            position_risks.append(position_risk)
        
        # Get ML-based risk assessment
        ml_assessment = await self.ml_risk.assess_portfolio_risk(
            user_id=user_id,
            positions=[p.dict() for p in positions]
        )
        
        # Calculate aggregate metrics
        leverage = portfolio_value / total_collateral if total_collateral > 0 else Decimal("0")
        margin_ratio = total_collateral / portfolio_value if portfolio_value > 0 else Decimal("1")
        
        # Determine overall risk level
        risk_level = self._determine_risk_level(
            leverage=leverage,
            margin_ratio=margin_ratio,
            ml_risk_score=ml_assessment.get("risk_score", 50)
        )
        
        # Check insurance coverage
        insurance_coverage = await self._check_insurance_coverage(
            user_id=user_id,
            portfolio_value=portfolio_value,
            risk_level=risk_level
        )
        
        risk_metrics = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "timestamp": datetime.utcnow().isoformat(),
            "portfolio_value": str(portfolio_value),
            "total_collateral": str(total_collateral),
            "total_borrowed": str(total_borrowed),
            "unrealized_pnl": str(total_unrealized_pnl),
            "leverage": str(leverage),
            "margin_ratio": str(margin_ratio),
            "risk_level": risk_level.value,
            "position_count": len(positions),
            "position_risks": position_risks,
            "ml_assessment": ml_assessment,
            "insurance_coverage": insurance_coverage,
            "warnings": self._generate_risk_warnings(
                risk_level, leverage, margin_ratio, position_risks
            ),
            "recommendations": self._generate_risk_recommendations(
                risk_level, leverage, position_risks
            )
        }
        
        # Cache risk metrics
        await self.state.put(
            cache_name="risk_metrics",
            key=f"user:{user_id}",
            value=risk_metrics,
            ttl=300  # 5 minute cache
        )
        
        # Publish risk event if critical
        if risk_level == RiskLevel.CRITICAL:
            await self.events.publish_event(
                topic="risk.alerts",
                event_type="critical_risk",
                data=risk_metrics
            )
        
        return risk_metrics
    
    async def validate_order_risk(self, 
                                 order: Dict[str, Any],
                                 user_id: str,
                                 tenant_id: str) -> Dict[str, Any]:
        """
        Validate if an order can be placed based on risk constraints.
        Used by both trading-platform-service and derivatives-engine-service.
        """
        # Get current risk metrics
        current_risk = await self.assess_unified_risk(user_id, tenant_id)
        
        # Simulate order impact
        simulated_position = await self._simulate_order_impact(order, current_risk)
        
        # Check risk limits
        validation_result = {
            "approved": True,
            "reason": "",
            "risk_metrics": current_risk,
            "simulated_metrics": simulated_position
        }
        
        # Check leverage limits
        max_leverage = await self._get_max_leverage(user_id, order["market_type"])
        if simulated_position["leverage"] > max_leverage:
            validation_result["approved"] = False
            validation_result["reason"] = f"Exceeds max leverage of {max_leverage}x"
            return validation_result
        
        # Check margin requirements
        margin_requirement = await self._calculate_margin_requirement(
            order, 
            current_risk["risk_level"]
        )
        
        if simulated_position["free_collateral"] < margin_requirement:
            validation_result["approved"] = False
            validation_result["reason"] = "Insufficient margin"
            validation_result["required_margin"] = str(margin_requirement)
            return validation_result
        
        # Check with ML risk engine
        ml_validation = await self.ml_risk.validate_order(
            order=order,
            current_portfolio=current_risk
        )
        
        if not ml_validation["approved"]:
            validation_result["approved"] = False
            validation_result["reason"] = ml_validation.get("reason", "ML risk check failed")
            return validation_result
        
        # Check insurance requirements for high-risk trades
        if current_risk["risk_level"] == "high" or current_risk["risk_level"] == "critical":
            insurance_check = await self._check_insurance_requirements(
                user_id, order, simulated_position
            )
            if not insurance_check["covered"]:
                validation_result["approved"] = False
                validation_result["reason"] = "Insurance coverage required for high-risk trades"
                validation_result["insurance_required"] = insurance_check["required_coverage"]
                return validation_result
        
        return validation_result
    
    async def handle_cross_margin_liquidation(self,
                                            user_id: str,
                                            tenant_id: str) -> Dict[str, Any]:
        """
        Handle cross-margin liquidation across all positions.
        Coordinates with insurance pool for coverage.
        """
        # Get all positions
        positions = await self._get_all_user_positions(user_id, tenant_id)
        
        # Sort by profitability (liquidate losing positions first)
        sorted_positions = sorted(
            positions, 
            key=lambda p: p.unrealized_pnl / p.collateral if p.collateral > 0 else float('-inf')
        )
        
        liquidation_plan = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "positions_to_liquidate": [],
            "expected_recovery": Decimal("0"),
            "insurance_claim": Decimal("0")
        }
        
        total_debt = sum(p.borrowed for p in positions)
        recovered = Decimal("0")
        
        # Liquidate positions
        for position in sorted_positions:
            if recovered >= total_debt:
                break
                
            liquidation_value = await self._calculate_liquidation_value(position)
            recovered += liquidation_value
            
            liquidation_plan["positions_to_liquidate"].append({
                "position_id": position.position_id,
                "market_type": position.market_type,
                "size": str(position.size),
                "liquidation_value": str(liquidation_value)
            })
        
        liquidation_plan["expected_recovery"] = recovered
        
        # Check if insurance is needed
        if recovered < total_debt:
            shortfall = total_debt - recovered
            
            # Claim from insurance pool
            insurance_claim = await self.insurance_pool.claim_liquidation_coverage(
                user_id=user_id,
                shortfall=shortfall,
                liquidation_details=liquidation_plan
            )
            
            liquidation_plan["insurance_claim"] = insurance_claim["amount_covered"]
        
        # Execute liquidation
        await self._execute_liquidation_plan(liquidation_plan)
        
        return liquidation_plan
    
    async def enable_cross_margin(self,
                                user_id: str,
                                tenant_id: str,
                                initial_deposit: Decimal) -> Dict[str, Any]:
        """
        Enable cross-margin mode for a user account.
        Integrates with insurance pool for coverage.
        """
        # Check minimum requirements
        min_deposit = await self._get_min_cross_margin_deposit()
        if initial_deposit < min_deposit:
            return {
                "success": False,
                "reason": f"Minimum deposit of {min_deposit} required"
            }
        
        # Create cross-margin account
        account = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "enabled_at": datetime.utcnow().isoformat(),
            "total_collateral": str(initial_deposit),
            "insurance_tier": "standard",  # Can be upgraded
            "risk_parameters": {
                "max_leverage": 10,
                "liquidation_threshold": 0.8,
                "auto_deleverage_enabled": True
            }
        }
        
        # Store in state
        await self.state.put(
            cache_name="cross_margin_accounts",
            key=f"{tenant_id}:{user_id}",
            value=account
        )
        
        # Register with insurance pool
        insurance_registration = await self.insurance_pool.register_cross_margin_account(
            user_id=user_id,
            initial_collateral=initial_deposit
        )
        
        account["insurance_coverage"] = insurance_registration
        
        # Publish event
        await self.events.publish_event(
            topic="accounts.updates",
            event_type="cross_margin_enabled",
            data=account
        )
        
        return {
            "success": True,
            "account": account
        }
    
    async def _get_all_user_positions(self, 
                                    user_id: str, 
                                    tenant_id: str) -> List[UnifiedPosition]:
        """Get all positions across services from distributed state"""
        positions = []
        
        # Query positions from state with SQL
        query = f"""
            SELECT * FROM unified_positions 
            WHERE user_id = ? AND tenant_id = ?
        """
        
        results = await self.state.query(
            cache_name="unified_positions",
            sql=query,
            params=[user_id, tenant_id]
        )
        
        for row in results:
            positions.append(UnifiedPosition(**row))
        
        return positions
    
    def _determine_risk_level(self, 
                            leverage: Decimal,
                            margin_ratio: Decimal,
                            ml_risk_score: int) -> RiskLevel:
        """Determine overall risk level based on multiple factors"""
        if leverage > 20 or margin_ratio < 0.1 or ml_risk_score > 80:
            return RiskLevel.CRITICAL
        elif leverage > 10 or margin_ratio < 0.2 or ml_risk_score > 60:
            return RiskLevel.HIGH
        elif leverage > 5 or margin_ratio < 0.3 or ml_risk_score > 40:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW
    
    async def _check_insurance_coverage(self,
                                      user_id: str,
                                      portfolio_value: Decimal,
                                      risk_level: RiskLevel) -> Dict[str, Any]:
        """Check insurance pool coverage for the portfolio"""
        coverage = await self.insurance_pool.get_user_coverage(user_id)
        
        coverage_ratio = coverage.get("total_coverage", 0) / portfolio_value if portfolio_value > 0 else 0
        
        return {
            "has_coverage": coverage.get("active", False),
            "coverage_amount": coverage.get("total_coverage", 0),
            "coverage_ratio": float(coverage_ratio),
            "tier": coverage.get("tier", "none"),
            "recommended_coverage": float(portfolio_value * Decimal("0.2"))  # 20% recommended
        }
    
    def _generate_risk_warnings(self,
                              risk_level: RiskLevel,
                              leverage: Decimal,
                              margin_ratio: Decimal,
                              position_risks: List[Dict]) -> List[str]:
        """Generate human-readable risk warnings"""
        warnings = []
        
        if risk_level == RiskLevel.CRITICAL:
            warnings.append("⚠️ CRITICAL RISK: Immediate action required to reduce exposure")
        elif risk_level == RiskLevel.HIGH:
            warnings.append("⚠️ High risk detected - consider reducing positions")
        
        if leverage > 15:
            warnings.append(f"Leverage of {leverage:.1f}x exceeds safe levels")
        
        if margin_ratio < 0.15:
            warnings.append(f"Low margin ratio of {margin_ratio:.2%} - liquidation risk")
        
        # Check for concentrated positions
        high_risk_positions = [p for p in position_risks if p.get("risk_level") == "high"]
        if len(high_risk_positions) > 3:
            warnings.append(f"{len(high_risk_positions)} positions at high risk")
        
        return warnings
    
    def _generate_risk_recommendations(self,
                                     risk_level: RiskLevel,
                                     leverage: Decimal,
                                     position_risks: List[Dict]) -> List[str]:
        """Generate actionable risk recommendations"""
        recommendations = []
        
        if risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            recommendations.append("Consider closing or reducing your highest risk positions")
            
            if leverage > 10:
                target_leverage = 5
                reduction_needed = (1 - target_leverage / leverage) * 100
                recommendations.append(
                    f"Reduce leverage by {reduction_needed:.0f}% to reach safer levels"
                )
        
        # Insurance recommendations
        if risk_level != RiskLevel.LOW:
            recommendations.append("Consider purchasing insurance pool coverage")
        
        # Position-specific recommendations
        losing_positions = [p for p in position_risks if p.get("unrealized_pnl", 0) < -1000]
        if losing_positions:
            recommendations.append(
                f"Review {len(losing_positions)} losing positions for potential exit"
            )
        
        return recommendations 