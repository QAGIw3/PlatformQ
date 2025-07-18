"""
Derivative Lending Module

Risk-based lending system that accepts derivative positions as collateral.
Uses oracle risk engine for dynamic collateral valuation and interest rates.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass
import numpy as np

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from app.models.position import Position
from app.risk.oracle_risk_engine import OracleRiskEngine

logger = logging.getLogger(__name__)


class LoanStatus(Enum):
    """Loan status types"""
    ACTIVE = "active"
    REPAID = "repaid"
    LIQUIDATED = "liquidated"
    DEFAULTED = "defaulted"
    MARGIN_CALL = "margin_call"


class CollateralType(Enum):
    """Types of collateral accepted"""
    DERIVATIVE_POSITION = "derivative_position"
    LP_TOKEN = "lp_token"
    OPTION_CONTRACT = "option_contract"
    FUTURES_CONTRACT = "futures_contract"
    STRUCTURED_PRODUCT = "structured_product"


@dataclass
class LoanTerms:
    """Loan terms and conditions"""
    min_duration: timedelta = timedelta(days=7)
    max_duration: timedelta = timedelta(days=365)
    min_loan_amount: Decimal = Decimal("1000")
    max_loan_amount: Decimal = Decimal("10000000")
    
    # Risk-based parameters
    base_interest_rate: Decimal = Decimal("0.05")  # 5% APR base
    max_interest_rate: Decimal = Decimal("0.50")  # 50% APR max
    
    # Collateral requirements
    min_ltv: Decimal = Decimal("0.20")  # 20% minimum
    max_ltv: Decimal = Decimal("0.80")  # 80% maximum
    initial_margin: Decimal = Decimal("1.5")  # 150% collateralization
    maintenance_margin: Decimal = Decimal("1.2")  # 120% maintenance
    
    # Liquidation parameters
    liquidation_discount: Decimal = Decimal("0.10")  # 10% discount
    liquidation_delay: timedelta = timedelta(hours=4)  # Grace period


@dataclass
class Loan:
    """Active loan details"""
    loan_id: str
    borrower_id: str
    tenant_id: str
    
    # Loan details
    principal: Decimal
    interest_rate: Decimal  # Annual rate
    origination_date: datetime
    maturity_date: datetime
    
    # Collateral
    collateral_positions: List[str]  # Position IDs
    collateral_value: Decimal
    collateral_type: CollateralType
    
    # Status
    status: LoanStatus
    outstanding_balance: Decimal
    accrued_interest: Decimal
    last_interest_update: datetime
    
    # Risk metrics
    current_ltv: Decimal
    risk_score: int
    liquidation_price: Optional[Decimal]
    
    # Metadata
    metadata: Dict[str, Any]


class DerivativeLending:
    """
    Risk-based lending system using derivatives as collateral
    """
    
    def __init__(self):
        self.risk_engine = OracleRiskEngine()
        
        self.compliance_client = ServiceClient(
            service_name="compliance-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        self.graph_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        # Loan storage (would be database in production)
        self.active_loans: Dict[str, Loan] = {}
        self.loan_history: List[Dict[str, Any]] = []
        
        # Risk parameters
        self.risk_multipliers = {
            "low": Decimal("1.0"),
            "medium": Decimal("1.5"),
            "high": Decimal("2.0"),
            "extreme": Decimal("3.0")
        }
        
        # Interest rate curve
        self.rate_curve = {
            0: Decimal("0.05"),     # 0-20 risk score
            20: Decimal("0.08"),    # 20-40 risk score
            40: Decimal("0.12"),    # 40-60 risk score
            60: Decimal("0.20"),    # 60-80 risk score
            80: Decimal("0.35"),    # 80-90 risk score
            90: Decimal("0.50")     # 90-100 risk score
        }
        
        # Flash loan parameters
        self.flash_loan_fee_rate = Decimal("0.0009")  # 0.09%
        self.pool_liquidity: Dict[str, Decimal] = {}
        self.pool_id = f"lending-pool-{datetime.now().timestamp()}"
        
        # Metrics tracking
        self.metrics = {
            'flash_loans_executed': 0,
            'flash_loan_volume': Decimal("0"),
            'flash_fees_collected': Decimal("0"),
            'total_loans_originated': 0,
            'total_loan_volume': Decimal("0")
        }
        
    async def request_loan(
        self,
        tenant_id: str,
        borrower_id: str,
        loan_amount: Decimal,
        duration_days: int,
        collateral_positions: List[str],
        loan_terms: Optional[LoanTerms] = None
    ) -> ProcessingResult:
        """
        Request a loan using derivative positions as collateral
        """
        try:
            if not loan_terms:
                loan_terms = LoanTerms()
            
            # Validate loan parameters
            validation = self._validate_loan_request(
                loan_amount,
                duration_days,
                collateral_positions,
                loan_terms
            )
            
            if not validation["valid"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=validation["reason"]
                )
            
            # Check borrower compliance
            compliance_check = await self._check_borrower_compliance(
                tenant_id,
                borrower_id
            )
            
            if not compliance_check["eligible"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Borrower not eligible: {compliance_check['reason']}"
                )
            
            # Get collateral positions
            positions = await self._get_positions(collateral_positions)
            
            if not positions:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Invalid collateral positions"
                )
            
            # Verify ownership
            if not all(p.user_id == borrower_id for p in positions):
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Borrower does not own all collateral positions"
                )
            
            # Calculate collateral value and risk
            collateral_analysis = await self._analyze_collateral(
                tenant_id,
                borrower_id,
                positions
            )
            
            if collateral_analysis["status"] != "success":
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=collateral_analysis["error"]
                )
            
            # Check LTV requirements
            max_borrowable = collateral_analysis["collateral_value"] * loan_terms.max_ltv
            
            if loan_amount > max_borrowable:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Loan amount exceeds maximum borrowable: {max_borrowable}"
                )
            
            # Calculate interest rate based on risk
            interest_rate = self._calculate_interest_rate(
                collateral_analysis["risk_score"],
                duration_days,
                loan_amount / collateral_analysis["collateral_value"]
            )
            
            # Create loan
            loan = Loan(
                loan_id=f"LOAN_{borrower_id}_{datetime.utcnow().timestamp()}",
                borrower_id=borrower_id,
                tenant_id=tenant_id,
                principal=loan_amount,
                interest_rate=interest_rate,
                origination_date=datetime.utcnow(),
                maturity_date=datetime.utcnow() + timedelta(days=duration_days),
                collateral_positions=collateral_positions,
                collateral_value=collateral_analysis["collateral_value"],
                collateral_type=CollateralType.DERIVATIVE_POSITION,
                status=LoanStatus.ACTIVE,
                outstanding_balance=loan_amount,
                accrued_interest=Decimal("0"),
                last_interest_update=datetime.utcnow(),
                current_ltv=loan_amount / collateral_analysis["collateral_value"],
                risk_score=collateral_analysis["risk_score"],
                liquidation_price=collateral_analysis.get("liquidation_price"),
                metadata={
                    "duration_days": duration_days,
                    "collateral_analysis": collateral_analysis,
                    "approval_timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Lock collateral positions
            await self._lock_collateral(collateral_positions, loan.loan_id)
            
            # Store loan
            self.active_loans[loan.loan_id] = loan
            
            # Record in history
            self.loan_history.append({
                "loan_id": loan.loan_id,
                "borrower_id": borrower_id,
                "action": "originated",
                "timestamp": datetime.utcnow(),
                "details": {
                    "principal": str(loan_amount),
                    "interest_rate": str(interest_rate),
                    "duration_days": duration_days
                }
            })
            
            logger.info(
                f"Loan {loan.loan_id} approved for {borrower_id}: "
                f"{loan_amount} at {interest_rate:.2%} APR"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "loan": self._serialize_loan(loan),
                    "repayment_schedule": self._generate_repayment_schedule(loan)
                }
            )
            
        except Exception as e:
            logger.error(f"Error processing loan request: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def monitor_loan_health(self, loan_id: str) -> ProcessingResult:
        """
        Monitor loan health and trigger margin calls if needed
        """
        try:
            loan = self.active_loans.get(loan_id)
            if not loan:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Loan not found"
                )
            
            if loan.status != LoanStatus.ACTIVE:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data={"status": loan.status.value}
                )
            
            # Get current collateral positions
            positions = await self._get_positions(loan.collateral_positions)
            
            # Recalculate collateral value
            collateral_analysis = await self._analyze_collateral(
                loan.tenant_id,
                loan.borrower_id,
                positions
            )
            
            if collateral_analysis["status"] != "success":
                logger.error(f"Failed to analyze collateral for loan {loan_id}")
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Collateral analysis failed"
                )
            
            # Update loan metrics
            loan.collateral_value = collateral_analysis["collateral_value"]
            loan.current_ltv = loan.outstanding_balance / loan.collateral_value
            loan.risk_score = collateral_analysis["risk_score"]
            loan.liquidation_price = collateral_analysis.get("liquidation_price")
            
            # Check margin requirements
            loan_terms = LoanTerms()  # Use default terms
            
            if loan.current_ltv > loan_terms.maintenance_margin:
                # Margin call required
                loan.status = LoanStatus.MARGIN_CALL
                
                margin_call_amount = (
                    loan.outstanding_balance -
                    (loan.collateral_value * loan_terms.max_ltv)
                )
                
                # Notify borrower
                await self._send_margin_call_notification(
                    loan,
                    margin_call_amount,
                    loan_terms.liquidation_delay
                )
                
                logger.warning(
                    f"Margin call for loan {loan_id}: "
                    f"LTV {loan.current_ltv:.2%}, required {margin_call_amount}"
                )
                
                # Schedule liquidation if not resolved
                asyncio.create_task(
                    self._schedule_liquidation(
                        loan_id,
                        loan_terms.liquidation_delay
                    )
                )
                
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data={
                        "status": "margin_call",
                        "current_ltv": str(loan.current_ltv),
                        "margin_call_amount": str(margin_call_amount),
                        "deadline": (
                            datetime.utcnow() + loan_terms.liquidation_delay
                        ).isoformat()
                    }
                )
            
            # Update interest
            await self._update_interest(loan)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "status": "healthy",
                    "current_ltv": str(loan.current_ltv),
                    "collateral_value": str(loan.collateral_value),
                    "outstanding_balance": str(loan.outstanding_balance),
                    "risk_score": loan.risk_score
                }
            )
            
        except Exception as e:
            logger.error(f"Error monitoring loan health: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def repay_loan(
        self,
        tenant_id: str,
        borrower_id: str,
        loan_id: str,
        repayment_amount: Decimal
    ) -> ProcessingResult:
        """
        Process loan repayment
        """
        try:
            loan = self.active_loans.get(loan_id)
            
            if not loan:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Loan not found"
                )
            
            # Verify borrower
            if loan.borrower_id != borrower_id:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Unauthorized"
                )
            
            # Update interest before repayment
            await self._update_interest(loan)
            
            # Apply repayment
            total_owed = loan.outstanding_balance + loan.accrued_interest
            
            if repayment_amount >= total_owed:
                # Full repayment
                actual_repayment = total_owed
                loan.outstanding_balance = Decimal("0")
                loan.accrued_interest = Decimal("0")
                loan.status = LoanStatus.REPAID
                
                # Release collateral
                await self._release_collateral(loan.collateral_positions)
                
                # Calculate refund if overpaid
                refund = repayment_amount - actual_repayment
                
                logger.info(f"Loan {loan_id} fully repaid")
                
            else:
                # Partial repayment
                actual_repayment = repayment_amount
                
                # Apply to interest first, then principal
                if repayment_amount >= loan.accrued_interest:
                    repayment_amount -= loan.accrued_interest
                    loan.accrued_interest = Decimal("0")
                    loan.outstanding_balance -= repayment_amount
                else:
                    loan.accrued_interest -= repayment_amount
                
                refund = Decimal("0")
                
                # Clear margin call if LTV improved
                if loan.status == LoanStatus.MARGIN_CALL:
                    await self.monitor_loan_health(loan_id)
            
            # Record repayment
            self.loan_history.append({
                "loan_id": loan_id,
                "borrower_id": borrower_id,
                "action": "repayment",
                "timestamp": datetime.utcnow(),
                "details": {
                    "amount": str(repayment_amount),
                    "remaining_balance": str(loan.outstanding_balance),
                    "status": loan.status.value
                }
            })
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "loan_id": loan_id,
                    "repayment_amount": str(actual_repayment),
                    "remaining_balance": str(loan.outstanding_balance),
                    "accrued_interest": str(loan.accrued_interest),
                    "status": loan.status.value,
                    "refund": str(refund) if refund > 0 else None
                }
            )
            
        except Exception as e:
            logger.error(f"Error processing repayment: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def add_collateral(
        self,
        tenant_id: str,
        borrower_id: str,
        loan_id: str,
        additional_positions: List[str]
    ) -> ProcessingResult:
        """
        Add additional collateral to improve loan health
        """
        try:
            loan = self.active_loans.get(loan_id)
            
            if not loan:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Loan not found"
                )
            
            # Verify borrower
            if loan.borrower_id != borrower_id:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Unauthorized"
                )
            
            # Get new positions
            new_positions = await self._get_positions(additional_positions)
            
            # Verify ownership
            if not all(p.user_id == borrower_id for p in new_positions):
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Borrower does not own all positions"
                )
            
            # Add to collateral
            loan.collateral_positions.extend(additional_positions)
            
            # Lock new collateral
            await self._lock_collateral(additional_positions, loan_id)
            
            # Recalculate loan health
            health_check = await self.monitor_loan_health(loan_id)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "loan_id": loan_id,
                    "total_collateral_positions": len(loan.collateral_positions),
                    "loan_health": health_check.data if health_check.status == ProcessingStatus.SUCCESS else None
                }
            )
            
        except Exception as e:
            logger.error(f"Error adding collateral: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def get_lending_opportunities(
        self,
        tenant_id: str,
        user_positions: List[str]
    ) -> ProcessingResult:
        """
        Analyze user's positions and suggest lending opportunities
        """
        try:
            if not user_positions:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data={"opportunities": []}
                )
            
            # Get positions
            positions = await self._get_positions(user_positions)
            
            # Analyze each position
            opportunities = []
            
            for position in positions:
                # Get risk analysis
                risk_result = await self.risk_engine.calculate_portfolio_risk(
                    tenant_id,
                    position.user_id,
                    [position]
                )
                
                if risk_result.status == ProcessingStatus.SUCCESS:
                    risk_data = risk_result.data
                    risk_score = risk_data["risk_profile"]["risk_score"]
                    
                    # Calculate borrowing capacity
                    position_value = position.quantity * position.current_price
                    
                    # Adjust max LTV based on risk
                    risk_adjusted_ltv = self._get_risk_adjusted_ltv(risk_score)
                    max_borrowable = position_value * risk_adjusted_ltv
                    
                    # Estimate interest rate
                    estimated_rate = self._calculate_interest_rate(
                        risk_score,
                        30,  # 30 day loan
                        risk_adjusted_ltv
                    )
                    
                    opportunities.append({
                        "position_id": position.position_id,
                        "asset": position.asset_id,
                        "position_value": str(position_value),
                        "max_borrowable": str(max_borrowable),
                        "estimated_apr": str(estimated_rate),
                        "risk_score": risk_score,
                        "ltv_ratio": str(risk_adjusted_ltv)
                    })
            
            # Sort by borrowing capacity
            opportunities.sort(
                key=lambda x: Decimal(x["max_borrowable"]),
                reverse=True
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "opportunities": opportunities,
                    "total_borrowing_capacity": str(
                        sum(Decimal(o["max_borrowable"]) for o in opportunities)
                    )
                }
            )
            
        except Exception as e:
            logger.error(f"Error analyzing lending opportunities: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    def _calculate_interest_rate(
        self,
        risk_score: int,
        duration_days: int,
        ltv_ratio: Decimal
    ) -> Decimal:
        """
        Calculate risk-based interest rate
        """
        # Base rate from risk score
        base_rate = Decimal("0.05")  # Default 5%
        
        for threshold, rate in sorted(self.rate_curve.items()):
            if risk_score >= threshold:
                base_rate = rate
        
        # Duration adjustment (higher rates for longer loans)
        duration_factor = Decimal("1") + (Decimal(str(duration_days)) / Decimal("365"))
        
        # LTV adjustment (higher rates for higher LTV)
        ltv_factor = Decimal("1") + (ltv_ratio * Decimal("0.5"))
        
        # Calculate final rate
        final_rate = base_rate * duration_factor * ltv_factor
        
        # Cap at maximum
        return min(final_rate, LoanTerms().max_interest_rate)
    
    def _get_risk_adjusted_ltv(self, risk_score: int) -> Decimal:
        """
        Get maximum LTV based on risk score
        """
        if risk_score < 20:
            return Decimal("0.80")  # 80% for low risk
        elif risk_score < 40:
            return Decimal("0.70")  # 70% for medium-low risk
        elif risk_score < 60:
            return Decimal("0.60")  # 60% for medium risk
        elif risk_score < 80:
            return Decimal("0.50")  # 50% for medium-high risk
        else:
            return Decimal("0.40")  # 40% for high risk
    
    async def _analyze_collateral(
        self,
        tenant_id: str,
        borrower_id: str,
        positions: List[Position]
    ) -> Dict[str, Any]:
        """
        Analyze collateral positions for risk and value
        """
        try:
            # Get comprehensive risk analysis
            risk_result = await self.risk_engine.calculate_portfolio_risk(
                tenant_id,
                borrower_id,
                positions
            )
            
            if risk_result.status != ProcessingStatus.SUCCESS:
                return {
                    "status": "failed",
                    "error": "Risk analysis failed"
                }
            
            risk_data = risk_result.data
            risk_profile = risk_data["risk_profile"]
            
            # Calculate total collateral value
            total_value = sum(
                p.quantity * p.current_price
                for p in positions
            )
            
            # Apply haircuts based on risk
            risk_score = risk_profile["risk_score"]
            haircut = self._calculate_haircut(risk_score)
            adjusted_value = total_value * (Decimal("1") - haircut)
            
            # Get liquidation scenarios
            liquidation_result = await self.risk_engine.predict_liquidation_scenarios(
                tenant_id,
                positions,
                [1, 4, 24]  # 1h, 4h, 24h horizons
            )
            
            worst_case_liquidation = None
            if liquidation_result.status == ProcessingStatus.SUCCESS:
                scenarios = liquidation_result.data["liquidation_scenarios"]
                
                # Find worst case
                for horizon, scenario in scenarios.items():
                    if scenario["positions_at_risk"]:
                        worst_case_liquidation = min(
                            p["expected_loss"]
                            for p in scenario["positions_at_risk"]
                        )
            
            return {
                "status": "success",
                "collateral_value": adjusted_value,
                "raw_value": total_value,
                "haircut": haircut,
                "risk_score": risk_score,
                "risk_profile": risk_profile,
                "liquidation_price": worst_case_liquidation,
                "position_count": len(positions)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing collateral: {str(e)}")
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def _calculate_haircut(self, risk_score: int) -> Decimal:
        """
        Calculate collateral haircut based on risk
        """
        if risk_score < 20:
            return Decimal("0.10")  # 10% haircut
        elif risk_score < 40:
            return Decimal("0.15")  # 15% haircut
        elif risk_score < 60:
            return Decimal("0.20")  # 20% haircut
        elif risk_score < 80:
            return Decimal("0.30")  # 30% haircut
        else:
            return Decimal("0.40")  # 40% haircut
    
    async def _update_interest(self, loan: Loan) -> None:
        """
        Update accrued interest for a loan
        """
        time_since_update = datetime.utcnow() - loan.last_interest_update
        days_elapsed = time_since_update.total_seconds() / 86400
        
        if days_elapsed > 0:
            # Calculate daily interest
            daily_rate = loan.interest_rate / Decimal("365")
            interest_accrued = loan.outstanding_balance * daily_rate * Decimal(str(days_elapsed))
            
            loan.accrued_interest += interest_accrued
            loan.last_interest_update = datetime.utcnow()
    
    async def _schedule_liquidation(
        self,
        loan_id: str,
        delay: timedelta
    ) -> None:
        """
        Schedule liquidation after delay if margin call not resolved
        """
        await asyncio.sleep(delay.total_seconds())
        
        loan = self.active_loans.get(loan_id)
        if loan and loan.status == LoanStatus.MARGIN_CALL:
            await self._liquidate_loan(loan_id)
    
    async def _liquidate_loan(self, loan_id: str) -> ProcessingResult:
        """
        Liquidate loan collateral
        """
        try:
            loan = self.active_loans.get(loan_id)
            if not loan:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Loan not found"
                )
            
            logger.warning(f"Liquidating loan {loan_id}")
            
            # Mark as liquidated
            loan.status = LoanStatus.LIQUIDATED
            
            # Calculate liquidation proceeds
            liquidation_value = loan.collateral_value * (
                Decimal("1") - LoanTerms().liquidation_discount
            )
            
            # Apply to outstanding balance
            remaining_debt = loan.outstanding_balance + loan.accrued_interest
            
            if liquidation_value >= remaining_debt:
                # Full recovery
                surplus = liquidation_value - remaining_debt
                loan.outstanding_balance = Decimal("0")
                loan.accrued_interest = Decimal("0")
            else:
                # Partial recovery
                loan.outstanding_balance = remaining_debt - liquidation_value
                surplus = Decimal("0")
            
            # Record liquidation
            self.loan_history.append({
                "loan_id": loan_id,
                "borrower_id": loan.borrower_id,
                "action": "liquidation",
                "timestamp": datetime.utcnow(),
                "details": {
                    "collateral_value": str(loan.collateral_value),
                    "liquidation_proceeds": str(liquidation_value),
                    "debt_recovered": str(min(liquidation_value, remaining_debt)),
                    "remaining_debt": str(loan.outstanding_balance),
                    "surplus": str(surplus) if surplus > 0 else None
                }
            })
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "loan_id": loan_id,
                    "liquidation_complete": True,
                    "remaining_debt": str(loan.outstanding_balance)
                }
            )
            
        except Exception as e:
            logger.error(f"Error liquidating loan: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    def _validate_loan_request(
        self,
        loan_amount: Decimal,
        duration_days: int,
        collateral_positions: List[str],
        loan_terms: LoanTerms
    ) -> Dict[str, Any]:
        """
        Validate loan request parameters
        """
        if loan_amount < loan_terms.min_loan_amount:
            return {
                "valid": False,
                "reason": f"Minimum loan amount is {loan_terms.min_loan_amount}"
            }
        
        if loan_amount > loan_terms.max_loan_amount:
            return {
                "valid": False,
                "reason": f"Maximum loan amount is {loan_terms.max_loan_amount}"
            }
        
        duration = timedelta(days=duration_days)
        if duration < loan_terms.min_duration:
            return {
                "valid": False,
                "reason": f"Minimum duration is {loan_terms.min_duration.days} days"
            }
        
        if duration > loan_terms.max_duration:
            return {
                "valid": False,
                "reason": f"Maximum duration is {loan_terms.max_duration.days} days"
            }
        
        if not collateral_positions:
            return {
                "valid": False,
                "reason": "No collateral positions provided"
            }
        
        return {"valid": True}
    
    async def _check_borrower_compliance(
        self,
        tenant_id: str,
        borrower_id: str
    ) -> Dict[str, Any]:
        """
        Check if borrower meets compliance requirements
        """
        try:
            # Check KYC status
            response = await self.compliance_client.get(
                f"/api/v1/kyc/status/{borrower_id}",
                params={"tenant_id": tenant_id}
            )
            
            if response.status_code == 200:
                kyc_data = response.json()
                
                # Require at least Tier 2 KYC for lending
                if kyc_data.get("kyc_tier", 0) < 2:
                    return {
                        "eligible": False,
                        "reason": "Minimum Tier 2 KYC required"
                    }
                
                # Check jurisdiction restrictions
                jurisdiction = kyc_data.get("jurisdiction", "unknown")
                restricted_jurisdictions = ["sanctioned_country"]  # Add actual list
                
                if jurisdiction in restricted_jurisdictions:
                    return {
                        "eligible": False,
                        "reason": "Jurisdiction not supported"
                    }
                
                return {"eligible": True}
            else:
                return {
                    "eligible": False,
                    "reason": "Unable to verify compliance status"
                }
                
        except Exception as e:
            logger.error(f"Error checking compliance: {str(e)}")
            return {
                "eligible": False,
                "reason": "Compliance check failed"
            }
    
    async def _get_positions(self, position_ids: List[str]) -> List[Position]:
        """
        Get position details
        """
        # This would fetch from database
        # For now, return mock positions
        mock_positions = []
        
        for pid in position_ids:
            mock_positions.append(
                Position(
                    position_id=pid,
                    user_id="user_123",  # Would come from DB
                    asset_id="BTC-USD",
                    quantity=Decimal("1.0"),
                    entry_price=Decimal("50000"),
                    current_price=Decimal("52000"),
                    initial_margin=Decimal("10000"),
                    used_margin=Decimal("9000"),
                    realized_pnl=Decimal("0"),
                    unrealized_pnl=Decimal("2000"),
                    created_at=datetime.utcnow()
                )
            )
        
        return mock_positions
    
    async def _lock_collateral(
        self,
        position_ids: List[str],
        loan_id: str
    ) -> None:
        """
        Lock collateral positions to prevent trading
        """
        # This would update database to mark positions as locked
        logger.info(f"Locked {len(position_ids)} positions for loan {loan_id}")
    
    async def _release_collateral(self, position_ids: List[str]) -> None:
        """
        Release locked collateral positions
        """
        # This would update database to mark positions as unlocked
        logger.info(f"Released {len(position_ids)} collateral positions")
    
    async def _send_margin_call_notification(
        self,
        loan: Loan,
        margin_required: Decimal,
        deadline: timedelta
    ) -> None:
        """
        Send margin call notification to borrower
        """
        # This would send actual notification
        logger.warning(
            f"Margin call sent to {loan.borrower_id}: "
            f"Required {margin_required} by {deadline}"
        )
    
    def _serialize_loan(self, loan: Loan) -> Dict[str, Any]:
        """
        Serialize loan for API response
        """
        return {
            "loan_id": loan.loan_id,
            "borrower_id": loan.borrower_id,
            "principal": str(loan.principal),
            "interest_rate": str(loan.interest_rate),
            "origination_date": loan.origination_date.isoformat(),
            "maturity_date": loan.maturity_date.isoformat(),
            "collateral_positions": loan.collateral_positions,
            "collateral_value": str(loan.collateral_value),
            "status": loan.status.value,
            "outstanding_balance": str(loan.outstanding_balance),
            "accrued_interest": str(loan.accrued_interest),
            "current_ltv": str(loan.current_ltv),
            "risk_score": loan.risk_score
        }
    
    def _generate_repayment_schedule(self, loan: Loan) -> List[Dict[str, Any]]:
        """
        Generate loan repayment schedule
        """
        # Simple bullet loan - principal and interest due at maturity
        total_interest = loan.principal * loan.interest_rate * (
            (loan.maturity_date - loan.origination_date).days / Decimal("365")
        )
        
        return [{
            "payment_date": loan.maturity_date.isoformat(),
            "principal": str(loan.principal),
            "interest": str(total_interest),
            "total_payment": str(loan.principal + total_interest)
        }] 

    # ============ Flash Loan Functionality ============
    
    async def execute_flash_loan(
        self,
        borrower_id: str,
        asset: str,
        amount: Decimal,
        callback_contract: str,
        callback_data: Dict[str, Any]
    ) -> ProcessingResult:
        """
        Execute a flash loan - borrow and repay within the same transaction
        
        Args:
            borrower_id: ID of the flash loan borrower
            asset: Asset to borrow
            amount: Amount to borrow
            callback_contract: Contract to call with borrowed funds
            callback_data: Data to pass to callback
            
        Returns:
            Result of the flash loan execution
        """
        try:
            # Check flash loan parameters
            if amount <= 0:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Invalid flash loan amount"
                )
            
            # Check available liquidity
            available_liquidity = await self._get_available_liquidity(asset)
            if amount > available_liquidity:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Insufficient liquidity. Available: {available_liquidity}"
                )
            
            # Calculate flash loan fee (0.09% standard)
            flash_fee = amount * self.flash_loan_fee_rate
            
            # Record initial balance
            initial_balance = await self._get_pool_balance(asset)
            
            # Transfer funds to borrower (simulated)
            await self._transfer_funds(
                from_pool=self.pool_id,
                to_address=callback_contract,
                asset=asset,
                amount=amount
            )
            
            # Execute callback
            callback_result = await self._execute_callback(
                contract=callback_contract,
                method="executeFlashLoan",
                params={
                    "initiator": borrower_id,
                    "asset": asset,
                    "amount": str(amount),
                    "fee": str(flash_fee),
                    "data": callback_data
                }
            )
            
            if callback_result.status != ProcessingStatus.COMPLETED:
                # Revert if callback failed
                await self._revert_flash_loan(asset, amount)
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Callback execution failed: {callback_result.error}"
                )
            
            # Verify repayment
            final_balance = await self._get_pool_balance(asset)
            expected_balance = initial_balance + flash_fee
            
            if final_balance < expected_balance:
                # Insufficient repayment
                await self._revert_flash_loan(asset, amount)
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Insufficient repayment. Expected: {expected_balance}, Got: {final_balance}"
                )
            
            # Record flash loan
            flash_loan_record = {
                "loan_id": f"FL-{datetime.now().timestamp()}",
                "borrower_id": borrower_id,
                "asset": asset,
                "amount": amount,
                "fee": flash_fee,
                "timestamp": datetime.now(),
                "callback_contract": callback_contract,
                "success": True
            }
            
            await self._record_flash_loan(flash_loan_record)
            
            # Update metrics
            self.metrics['flash_loans_executed'] += 1
            self.metrics['flash_loan_volume'] += amount
            self.metrics['flash_fees_collected'] += flash_fee
            
            logger.info(
                f"Flash loan executed: {amount} {asset} to {borrower_id}, "
                f"fee: {flash_fee}"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                data={
                    "flash_loan": flash_loan_record,
                    "fee_paid": str(flash_fee)
                }
            )
            
        except Exception as e:
            logger.error(f"Flash loan execution error: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def execute_flash_loan_arbitrage(
        self,
        arbitrageur_id: str,
        strategy: Dict[str, Any]
    ) -> ProcessingResult:
        """
        Execute flash loan for arbitrage opportunities
        
        Args:
            arbitrageur_id: ID of the arbitrageur
            strategy: Arbitrage strategy details including:
                - borrow_asset: Asset to borrow
                - borrow_amount: Amount to borrow
                - target_markets: Markets to arbitrage between
                - expected_profit: Expected profit from arbitrage
                
        Returns:
            Result of arbitrage execution
        """
        try:
            borrow_asset = strategy['borrow_asset']
            borrow_amount = Decimal(strategy['borrow_amount'])
            
            # Validate expected profit covers flash loan fee
            flash_fee = borrow_amount * self.flash_loan_fee_rate
            expected_profit = Decimal(strategy.get('expected_profit', '0'))
            
            if expected_profit <= flash_fee:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Expected profit {expected_profit} doesn't cover fee {flash_fee}"
                )
            
            # Create arbitrage callback contract
            callback_contract = await self._deploy_arbitrage_contract(
                arbitrageur_id,
                strategy
            )
            
            # Execute flash loan with arbitrage callback
            result = await self.execute_flash_loan(
                borrower_id=arbitrageur_id,
                asset=borrow_asset,
                amount=borrow_amount,
                callback_contract=callback_contract,
                callback_data={
                    "strategy": strategy,
                    "min_profit": str(flash_fee * Decimal("1.1"))  # 10% margin
                }
            )
            
            if result.status == ProcessingStatus.COMPLETED:
                # Calculate actual profit
                actual_profit = await self._calculate_arbitrage_profit(
                    callback_contract,
                    borrow_asset
                )
                
                result.data['arbitrage_profit'] = str(actual_profit)
                result.data['net_profit'] = str(actual_profit - flash_fee)
            
            return result
            
        except Exception as e:
            logger.error(f"Flash loan arbitrage error: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def get_flash_loan_fee(
        self,
        asset: str,
        amount: Decimal
    ) -> Decimal:
        """
        Calculate flash loan fee for given amount
        
        Args:
            asset: Asset to borrow
            amount: Amount to borrow
            
        Returns:
            Flash loan fee amount
        """
        # Dynamic fee based on utilization
        utilization = await self._get_pool_utilization(asset)
        
        # Higher utilization = higher fee
        if utilization > Decimal("0.9"):
            fee_rate = self.flash_loan_fee_rate * Decimal("2")  # Double fee
        elif utilization > Decimal("0.8"):
            fee_rate = self.flash_loan_fee_rate * Decimal("1.5")
        else:
            fee_rate = self.flash_loan_fee_rate
        
        return amount * fee_rate
    
    async def _get_available_liquidity(self, asset: str) -> Decimal:
        """Get available liquidity for flash loans"""
        total_liquidity = await self._get_pool_balance(asset)
        
        # Reserve 10% for normal operations
        reserved = total_liquidity * Decimal("0.1")
        
        return max(Decimal("0"), total_liquidity - reserved)
    
    async def _get_pool_balance(self, asset: str) -> Decimal:
        """Get current pool balance for asset"""
        # This would query actual pool balance
        # Simplified for example
        return self.pool_liquidity.get(asset, Decimal("0"))
    
    async def _transfer_funds(
        self,
        from_pool: str,
        to_address: str,
        asset: str,
        amount: Decimal
    ) -> None:
        """Transfer funds from pool to address"""
        # This would execute actual transfer
        # Update pool balance
        if asset in self.pool_liquidity:
            self.pool_liquidity[asset] -= amount
    
    async def _execute_callback(
        self,
        contract: str,
        method: str,
        params: Dict[str, Any]
    ) -> ProcessingResult:
        """Execute callback contract method"""
        # This would call the actual contract
        # Simplified for example
        try:
            # Simulate successful arbitrage
            if "arbitrage" in contract:
                # Return borrowed amount + fee
                asset = params['asset']
                amount = Decimal(params['amount'])
                fee = Decimal(params['fee'])
                
                if asset in self.pool_liquidity:
                    self.pool_liquidity[asset] += amount + fee
                
                return ProcessingResult(
                    status=ProcessingStatus.COMPLETED,
                    data={"profit": str(fee * Decimal("2"))}  # Simulated profit
                )
            
            return ProcessingResult(status=ProcessingStatus.COMPLETED)
            
        except Exception as e:
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _revert_flash_loan(self, asset: str, amount: Decimal) -> None:
        """Revert flash loan in case of failure"""
        # This would revert the transaction
        # Simplified - just restore balance
        if asset in self.pool_liquidity:
            self.pool_liquidity[asset] += amount
    
    async def _record_flash_loan(self, record: Dict[str, Any]) -> None:
        """Record flash loan in database"""
        # Store in database/event log
        if 'flash_loans' not in self.metrics:
            self.metrics['flash_loans'] = []
        self.metrics['flash_loans'].append(record)
    
    async def _deploy_arbitrage_contract(
        self,
        deployer: str,
        strategy: Dict[str, Any]
    ) -> str:
        """Deploy arbitrage execution contract"""
        # This would deploy actual contract
        # Return mock contract address
        return f"arbitrage-{deployer}-{datetime.now().timestamp()}"
    
    async def _calculate_arbitrage_profit(
        self,
        contract: str,
        asset: str
    ) -> Decimal:
        """Calculate actual profit from arbitrage"""
        # This would query contract for profit
        # Simplified calculation
        return Decimal("100")  # Mock profit
    
    async def _get_pool_utilization(self, asset: str) -> Decimal:
        """Get current pool utilization rate"""
        total_liquidity = self.pool_liquidity.get(asset, Decimal("0"))
        if total_liquidity == 0:
            return Decimal("0")
        
        # Calculate based on outstanding loans
        borrowed = sum(
            loan.principal 
            for loan in self.active_loans.values()
            if loan.status == LoanStatus.ACTIVE
        )
        
        return borrowed / total_liquidity 