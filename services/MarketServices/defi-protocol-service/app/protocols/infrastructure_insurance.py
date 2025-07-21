"""
Infrastructure Insurance Extension

Extends the base insurance protocol to cover infrastructure-specific risks.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio

from ..models import ResourceType, ServiceTier, RiskTier, ClaimStatus
from ..services.infrastructure_risk_engine import InfrastructureRiskEngine
from .insurance import InsuranceProtocol

logger = logging.getLogger(__name__)


class InfrastructureClaimType:
    """Types of infrastructure insurance claims"""
    PROVIDER_FAILURE = "provider_failure"        # Provider goes offline
    SLA_BREACH = "sla_breach"                   # Service level agreement violated
    RESOURCE_UNAVAILABILITY = "resource_unavail" # Resources not available when needed
    PERFORMANCE_DEGRADATION = "perf_degradation" # Performance below specifications
    DATA_LOSS = "data_loss"                     # Storage data loss
    NETWORK_OUTAGE = "network_outage"           # Network connectivity issues
    SECURITY_BREACH = "security_breach"         # Security incident
    REGULATORY_VIOLATION = "regulatory_violation" # Compliance issues


class InfrastructureInsuranceExtension:
    """Extension for infrastructure-specific insurance coverage"""
    
    # Premium rates by claim type (basis points of coverage per day)
    PREMIUM_RATES = {
        InfrastructureClaimType.PROVIDER_FAILURE: 10,      # 0.1% per day
        InfrastructureClaimType.SLA_BREACH: 5,             # 0.05% per day
        InfrastructureClaimType.RESOURCE_UNAVAILABILITY: 8,# 0.08% per day
        InfrastructureClaimType.PERFORMANCE_DEGRADATION: 4,# 0.04% per day
        InfrastructureClaimType.DATA_LOSS: 15,            # 0.15% per day
        InfrastructureClaimType.NETWORK_OUTAGE: 6,        # 0.06% per day
        InfrastructureClaimType.SECURITY_BREACH: 20,      # 0.2% per day
        InfrastructureClaimType.REGULATORY_VIOLATION: 12  # 0.12% per day
    }
    
    # Coverage limits by resource type (percentage of value)
    COVERAGE_LIMITS = {
        ResourceType.CPU: Decimal("0.95"),      # 95% coverage
        ResourceType.GPU: Decimal("0.90"),      # 90% coverage
        ResourceType.STORAGE: Decimal("0.98"),  # 98% coverage
        ResourceType.BANDWIDTH: Decimal("0.92"),# 92% coverage
        ResourceType.MEMORY: Decimal("0.95")    # 95% coverage
    }
    
    def __init__(
        self,
        insurance_protocol: InsuranceProtocol,
        risk_engine: InfrastructureRiskEngine
    ):
        self.insurance_protocol = insurance_protocol
        self.risk_engine = risk_engine
        self._active_policies = {}  # policy_id -> policy details
        self._claim_history = {}    # provider -> claim history
        
    async def create_infrastructure_policy(
        self,
        policyholder: str,
        resource_type: ResourceType,
        service_tier: ServiceTier,
        provider: str,
        coverage_amount: Decimal,
        coverage_types: List[str],
        duration_days: int,
        region: str
    ) -> Dict[str, Any]:
        """
        Create an insurance policy for infrastructure resources
        
        Args:
            policyholder: Address of the policy buyer
            resource_type: Type of resource being insured
            service_tier: Service quality tier
            provider: Infrastructure provider address
            coverage_amount: Maximum coverage amount in USD
            coverage_types: List of claim types to cover
            duration_days: Policy duration in days
            region: Geographic region
            
        Returns:
            Policy details including ID and premium
        """
        try:
            # Validate coverage types
            for coverage_type in coverage_types:
                if not hasattr(InfrastructureClaimType, coverage_type.upper()):
                    raise ValueError(f"Invalid coverage type: {coverage_type}")
                    
            # Calculate risk score
            risk_result = await self.risk_engine.calculate_unified_risk(
                resource_type=resource_type,
                service_tier=service_tier,
                provider=provider,
                amount=1000,  # Normalized amount for risk calculation
                duration_days=duration_days,
                region=region
            )
            
            # Calculate base premium
            base_premium = await self._calculate_premium(
                coverage_amount=coverage_amount,
                coverage_types=coverage_types,
                duration_days=duration_days,
                risk_score=Decimal(risk_result["risk_score"])
            )
            
            # Apply resource-specific adjustments
            resource_multiplier = self._get_resource_multiplier(resource_type)
            tier_discount = self._get_tier_discount(service_tier)
            
            total_premium = base_premium * resource_multiplier * tier_discount
            
            # Generate policy ID
            policy_id = f"INFRA-{policyholder[:8]}-{datetime.utcnow().timestamp()}"
            
            # Store policy details
            policy = {
                "policy_id": policy_id,
                "policyholder": policyholder,
                "resource_type": resource_type,
                "service_tier": service_tier,
                "provider": provider,
                "coverage_amount": coverage_amount,
                "coverage_types": coverage_types,
                "duration_days": duration_days,
                "region": region,
                "premium": total_premium,
                "risk_score": risk_result["risk_score"],
                "risk_level": risk_result["risk_level"],
                "start_date": datetime.utcnow(),
                "end_date": datetime.utcnow() + timedelta(days=duration_days),
                "status": "active",
                "claims": []
            }
            
            self._active_policies[policy_id] = policy
            
            # Register with base insurance protocol for capital backing
            await self._register_with_base_protocol(policy)
            
            return {
                "policy_id": policy_id,
                "premium": float(total_premium),
                "coverage_amount": float(coverage_amount),
                "risk_level": risk_result["risk_level"],
                "start_date": policy["start_date"].isoformat(),
                "end_date": policy["end_date"].isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error creating infrastructure policy: {e}")
            raise
            
    async def file_infrastructure_claim(
        self,
        policy_id: str,
        claim_type: str,
        claim_amount: Decimal,
        evidence: Dict[str, Any],
        description: str
    ) -> Dict[str, Any]:
        """
        File a claim against an infrastructure insurance policy
        
        Args:
            policy_id: Insurance policy ID
            claim_type: Type of claim (from InfrastructureClaimType)
            claim_amount: Amount being claimed
            evidence: Supporting evidence (logs, metrics, etc.)
            description: Detailed description of the incident
            
        Returns:
            Claim details including ID and status
        """
        try:
            # Validate policy
            policy = self._active_policies.get(policy_id)
            if not policy:
                raise ValueError("Policy not found")
                
            if policy["status"] != "active":
                raise ValueError("Policy is not active")
                
            if datetime.utcnow() > policy["end_date"]:
                raise ValueError("Policy has expired")
                
            # Validate claim type
            if claim_type not in policy["coverage_types"]:
                raise ValueError(f"Claim type {claim_type} not covered by policy")
                
            # Validate claim amount
            if claim_amount > policy["coverage_amount"]:
                claim_amount = policy["coverage_amount"]  # Cap at coverage limit
                
            # Generate claim ID
            claim_id = f"CLAIM-{policy_id}-{len(policy['claims'])}"
            
            # Validate evidence based on claim type
            is_valid = await self._validate_claim_evidence(
                claim_type=claim_type,
                evidence=evidence,
                policy=policy
            )
            
            # Create claim
            claim = {
                "claim_id": claim_id,
                "policy_id": policy_id,
                "claim_type": claim_type,
                "claim_amount": claim_amount,
                "evidence": evidence,
                "description": description,
                "filed_date": datetime.utcnow(),
                "status": ClaimStatus.PENDING if is_valid else ClaimStatus.REJECTED,
                "validation_result": is_valid,
                "payout_amount": None,
                "payout_date": None
            }
            
            policy["claims"].append(claim)
            
            # If valid, process payout
            if is_valid:
                payout_result = await self._process_claim_payout(policy, claim)
                claim["payout_amount"] = payout_result["amount"]
                claim["payout_date"] = payout_result["date"]
                claim["status"] = ClaimStatus.APPROVED
                
            # Update claim history
            self._update_claim_history(policy["provider"], claim)
            
            return {
                "claim_id": claim_id,
                "status": claim["status"].value,
                "payout_amount": float(claim["payout_amount"]) if claim["payout_amount"] else None,
                "validation_result": is_valid
            }
            
        except Exception as e:
            logger.error(f"Error filing infrastructure claim: {e}")
            raise
            
    async def _calculate_premium(
        self,
        coverage_amount: Decimal,
        coverage_types: List[str],
        duration_days: int,
        risk_score: Decimal
    ) -> Decimal:
        """Calculate insurance premium based on coverage and risk"""
        total_rate = Decimal("0")
        
        # Sum rates for all coverage types
        for coverage_type in coverage_types:
            rate = self.PREMIUM_RATES.get(coverage_type, 10)
            total_rate += Decimal(rate)
            
        # Calculate base premium
        daily_premium = coverage_amount * total_rate / Decimal("10000")
        base_premium = daily_premium * Decimal(duration_days)
        
        # Apply risk multiplier
        risk_multiplier = Decimal("1") + (risk_score / Decimal("100"))
        
        return base_premium * risk_multiplier
        
    def _get_resource_multiplier(self, resource_type: ResourceType) -> Decimal:
        """Get premium multiplier based on resource type"""
        multipliers = {
            ResourceType.CPU: Decimal("1.0"),
            ResourceType.GPU: Decimal("1.5"),     # Higher risk
            ResourceType.STORAGE: Decimal("0.8"), # Lower risk
            ResourceType.BANDWIDTH: Decimal("1.2"),
            ResourceType.MEMORY: Decimal("1.1")
        }
        
        return multipliers.get(resource_type, Decimal("1.0"))
        
    def _get_tier_discount(self, service_tier: ServiceTier) -> Decimal:
        """Get premium discount based on service tier"""
        discounts = {
            ServiceTier.STANDARD: Decimal("1.0"),   # No discount
            ServiceTier.PREMIUM: Decimal("0.9"),    # 10% discount
            ServiceTier.GUARANTEED: Decimal("0.8")  # 20% discount
        }
        
        return discounts.get(service_tier, Decimal("1.0"))
        
    async def _validate_claim_evidence(
        self,
        claim_type: str,
        evidence: Dict[str, Any],
        policy: Dict[str, Any]
    ) -> bool:
        """Validate claim evidence based on claim type"""
        try:
            if claim_type == InfrastructureClaimType.PROVIDER_FAILURE:
                # Check uptime monitoring data
                required_keys = ["downtime_start", "downtime_end", "monitoring_logs"]
                if not all(key in evidence for key in required_keys):
                    return False
                    
                # Verify downtime duration
                downtime_start = datetime.fromisoformat(evidence["downtime_start"])
                downtime_end = datetime.fromisoformat(evidence["downtime_end"])
                downtime_hours = (downtime_end - downtime_start).total_seconds() / 3600
                
                return downtime_hours >= 1  # Minimum 1 hour downtime
                
            elif claim_type == InfrastructureClaimType.SLA_BREACH:
                # Check performance metrics against SLA
                required_keys = ["metric_type", "actual_value", "sla_threshold", "duration"]
                if not all(key in evidence for key in required_keys):
                    return False
                    
                return evidence["actual_value"] < evidence["sla_threshold"]
                
            elif claim_type == InfrastructureClaimType.DATA_LOSS:
                # Verify data loss evidence
                required_keys = ["data_size", "recovery_attempted", "loss_timestamp"]
                if not all(key in evidence for key in required_keys):
                    return False
                    
                return evidence["data_size"] > 0 and evidence["recovery_attempted"]
                
            # Add more validation logic for other claim types
            
            return True  # Default to valid if no specific validation
            
        except Exception as e:
            logger.error(f"Error validating claim evidence: {e}")
            return False
            
    async def _process_claim_payout(
        self,
        policy: Dict[str, Any],
        claim: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Process claim payout"""
        # Calculate payout amount based on claim type and coverage
        payout_amount = claim["claim_amount"]
        
        # Apply coverage limits
        coverage_limit = self.COVERAGE_LIMITS.get(
            policy["resource_type"],
            Decimal("0.9")
        )
        max_payout = policy["coverage_amount"] * coverage_limit
        
        if payout_amount > max_payout:
            payout_amount = max_payout
            
        # Deduct deductible if applicable
        deductible = policy["coverage_amount"] * Decimal("0.05")  # 5% deductible
        payout_amount = max(Decimal("0"), payout_amount - deductible)
        
        # Process payout through base insurance protocol
        # In production, this would trigger actual token transfer
        
        return {
            "amount": payout_amount,
            "date": datetime.utcnow()
        }
        
    def _update_claim_history(self, provider: str, claim: Dict[str, Any]):
        """Update provider's claim history"""
        if provider not in self._claim_history:
            self._claim_history[provider] = []
            
        self._claim_history[provider].append({
            "claim_id": claim["claim_id"],
            "claim_type": claim["claim_type"],
            "amount": claim["claim_amount"],
            "status": claim["status"],
            "date": claim["filed_date"]
        })
        
    async def _register_with_base_protocol(self, policy: Dict[str, Any]):
        """Register policy with base insurance protocol for capital backing"""
        # Map infrastructure risk to base protocol risk tiers
        risk_mapping = {
            "low": RiskTier.LOW,
            "medium": RiskTier.MEDIUM,
            "high": RiskTier.HIGH,
            "critical": RiskTier.HIGH,
            "extreme": RiskTier.HIGH
        }
        
        risk_tier = risk_mapping.get(policy["risk_level"], RiskTier.MEDIUM)
        
        # Register for capital allocation
        # This ensures the insurance pool has sufficient capital
        # In production, this would interact with the base protocol
        pass
        
    async def get_policy_details(self, policy_id: str) -> Optional[Dict[str, Any]]:
        """Get details of a specific policy"""
        policy = self._active_policies.get(policy_id)
        if not policy:
            return None
            
        return {
            "policy_id": policy["policy_id"],
            "policyholder": policy["policyholder"],
            "resource_type": policy["resource_type"].value,
            "service_tier": policy["service_tier"].value,
            "provider": policy["provider"],
            "coverage_amount": float(policy["coverage_amount"]),
            "coverage_types": policy["coverage_types"],
            "premium": float(policy["premium"]),
            "risk_level": policy["risk_level"],
            "status": policy["status"],
            "start_date": policy["start_date"].isoformat(),
            "end_date": policy["end_date"].isoformat(),
            "claims": len(policy["claims"]),
            "total_claims_amount": float(sum(c["claim_amount"] for c in policy["claims"]))
        }
        
    async def get_provider_risk_profile(self, provider: str) -> Dict[str, Any]:
        """Get risk profile for a provider based on claim history"""
        claims = self._claim_history.get(provider, [])
        
        if not claims:
            return {
                "provider": provider,
                "risk_score": 50,  # Neutral score
                "total_claims": 0,
                "claim_rate": 0,
                "average_claim_amount": 0
            }
            
        # Calculate metrics
        total_claims = len(claims)
        approved_claims = sum(1 for c in claims if c["status"] == ClaimStatus.APPROVED)
        total_claim_amount = sum(c["amount"] for c in claims)
        
        # Calculate risk score based on claim history
        claim_rate = approved_claims / total_claims if total_claims > 0 else 0
        risk_score = min(100, 50 + (claim_rate * 100))
        
        return {
            "provider": provider,
            "risk_score": risk_score,
            "total_claims": total_claims,
            "approved_claims": approved_claims,
            "claim_rate": claim_rate,
            "average_claim_amount": float(total_claim_amount / total_claims),
            "claim_types": self._get_claim_type_breakdown(claims)
        }
        
    def _get_claim_type_breakdown(self, claims: List[Dict]) -> Dict[str, int]:
        """Get breakdown of claims by type"""
        breakdown = {}
        for claim in claims:
            claim_type = claim["claim_type"]
            breakdown[claim_type] = breakdown.get(claim_type, 0) + 1
        return breakdown
        
    async def calculate_pool_requirements(self) -> Dict[str, Any]:
        """Calculate capital requirements for infrastructure insurance pool"""
        total_coverage = Decimal("0")
        total_premiums = Decimal("0")
        risk_weighted_coverage = Decimal("0")
        
        for policy in self._active_policies.values():
            if policy["status"] == "active":
                total_coverage += policy["coverage_amount"]
                total_premiums += policy["premium"]
                
                # Weight coverage by risk score
                risk_weight = Decimal(policy["risk_score"]) / Decimal("100")
                risk_weighted_coverage += policy["coverage_amount"] * risk_weight
                
        # Calculate required capital (risk-based)
        # Higher risk requires more capital backing
        required_capital = risk_weighted_coverage * Decimal("1.5")  # 150% of risk-weighted coverage
        
        return {
            "total_active_policies": len([p for p in self._active_policies.values() if p["status"] == "active"]),
            "total_coverage": float(total_coverage),
            "total_premiums": float(total_premiums),
            "risk_weighted_coverage": float(risk_weighted_coverage),
            "required_capital": float(required_capital),
            "capital_utilization": float(risk_weighted_coverage / required_capital) if required_capital > 0 else 0
        } 