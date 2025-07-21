"""
Infrastructure Insurance API endpoints

Provides endpoints for infrastructure-specific insurance coverage.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, validator

from platformq_shared import get_current_user
from ..protocols.infrastructure_insurance import InfrastructureInsuranceExtension, InfrastructureClaimType
from ..models import ResourceType, ServiceTier
from ..services.infrastructure_risk_engine import InfrastructureRiskEngine

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class CreatePolicyRequest(BaseModel):
    """Request to create an infrastructure insurance policy"""
    resource_type: ResourceType
    service_tier: ServiceTier
    provider: str = Field(..., description="Infrastructure provider address")
    coverage_amount: Decimal = Field(..., gt=0, description="Maximum coverage amount in USD")
    coverage_types: List[str] = Field(..., description="List of claim types to cover")
    duration_days: int = Field(..., ge=1, le=365, description="Policy duration in days")
    region: str = Field(default="us-east-1", description="Geographic region")
    
    @validator('coverage_types')
    def validate_coverage_types(cls, v):
        valid_types = [
            "provider_failure", "sla_breach", "resource_unavail",
            "perf_degradation", "data_loss", "network_outage",
            "security_breach", "regulatory_violation"
        ]
        for coverage_type in v:
            if coverage_type not in valid_types:
                raise ValueError(f"Invalid coverage type: {coverage_type}")
        return v


class FileClaimRequest(BaseModel):
    """Request to file an insurance claim"""
    policy_id: str = Field(..., description="Insurance policy ID")
    claim_type: str = Field(..., description="Type of claim")
    claim_amount: Decimal = Field(..., gt=0, description="Amount being claimed")
    evidence: Dict[str, Any] = Field(..., description="Supporting evidence")
    description: str = Field(..., description="Detailed description of incident")


class PolicyResponse(BaseModel):
    """Insurance policy details"""
    policy_id: str
    premium: float
    coverage_amount: float
    risk_level: str
    start_date: str
    end_date: str


class ClaimResponse(BaseModel):
    """Insurance claim response"""
    claim_id: str
    status: str
    payout_amount: Optional[float]
    validation_result: bool


class ProviderRiskProfile(BaseModel):
    """Risk profile for an infrastructure provider"""
    provider: str
    risk_score: float
    total_claims: int
    approved_claims: int
    claim_rate: float
    average_claim_amount: float
    claim_types: Dict[str, int]


# Initialize insurance extension
insurance_extension = None


async def get_insurance_extension() -> InfrastructureInsuranceExtension:
    """Get infrastructure insurance extension instance"""
    global insurance_extension
    if not insurance_extension:
        from ..main import insurance_protocol, risk_calculator, price_oracle
        from ..services.resource_valuation import ResourceValuationService
        
        valuation_service = ResourceValuationService(price_oracle)
        risk_engine = InfrastructureRiskEngine(
            risk_calculator=risk_calculator,
            price_oracle=price_oracle,
            valuation_service=valuation_service
        )
        
        insurance_extension = InfrastructureInsuranceExtension(
            insurance_protocol=insurance_protocol,
            risk_engine=risk_engine
        )
    return insurance_extension


@router.post("/policies/create", response_model=PolicyResponse)
async def create_infrastructure_policy(
    request: CreatePolicyRequest,
    current_user: Dict = Depends(get_current_user)
) -> PolicyResponse:
    """
    Create an insurance policy for infrastructure resources
    
    - Covers various infrastructure risks
    - Premium based on risk assessment
    - Automatic claim validation
    """
    extension = await get_insurance_extension()
    
    try:
        result = await extension.create_infrastructure_policy(
            policyholder=current_user["wallet_address"],
            resource_type=request.resource_type,
            service_tier=request.service_tier,
            provider=request.provider,
            coverage_amount=request.coverage_amount,
            coverage_types=request.coverage_types,
            duration_days=request.duration_days,
            region=request.region
        )
        
        return PolicyResponse(**result)
        
    except Exception as e:
        logger.error(f"Error creating policy: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/claims/file", response_model=ClaimResponse)
async def file_infrastructure_claim(
    request: FileClaimRequest,
    current_user: Dict = Depends(get_current_user)
) -> ClaimResponse:
    """
    File a claim against an infrastructure insurance policy
    
    - Validates claim evidence automatically
    - Processes payout if approved
    - Updates provider risk profile
    """
    extension = await get_insurance_extension()
    
    try:
        # Verify user owns the policy
        policy = await extension.get_policy_details(request.policy_id)
        if not policy or policy["policyholder"] != current_user["wallet_address"]:
            raise HTTPException(status_code=403, detail="Not authorized to file claim")
        
        result = await extension.file_infrastructure_claim(
            policy_id=request.policy_id,
            claim_type=request.claim_type,
            claim_amount=request.claim_amount,
            evidence=request.evidence,
            description=request.description
        )
        
        return ClaimResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error filing claim: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies/{policy_id}")
async def get_policy_details(
    policy_id: str,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get details of a specific insurance policy"""
    extension = await get_insurance_extension()
    
    policy = await extension.get_policy_details(policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    # Verify user owns the policy or is an admin
    if policy["policyholder"] != current_user["wallet_address"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    return policy


@router.get("/policies/user/{address}")
async def get_user_policies(
    address: str,
    current_user: Dict = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """Get all policies for a user"""
    # Verify user is querying their own policies
    if address.lower() != current_user["wallet_address"].lower():
        raise HTTPException(status_code=403, detail="Not authorized")
    
    extension = await get_insurance_extension()
    
    # Filter policies by user
    user_policies = []
    for policy_id, policy in extension._active_policies.items():
        if policy["policyholder"].lower() == address.lower():
            user_policies.append(await extension.get_policy_details(policy_id))
    
    return user_policies


@router.get("/providers/{provider}/risk-profile", response_model=ProviderRiskProfile)
async def get_provider_risk_profile(provider: str) -> ProviderRiskProfile:
    """
    Get risk profile for an infrastructure provider
    
    - Based on claim history
    - Shows reliability metrics
    - Helps in policy pricing
    """
    extension = await get_insurance_extension()
    
    profile = await extension.get_provider_risk_profile(provider)
    
    return ProviderRiskProfile(**profile)


@router.get("/coverage-types")
async def get_coverage_types() -> Dict[str, Any]:
    """Get available coverage types and their descriptions"""
    coverage_types = {
        "provider_failure": {
            "name": "Provider Failure",
            "description": "Coverage for provider downtime or service unavailability",
            "premium_rate": 0.1,  # % per day
            "min_downtime_hours": 1
        },
        "sla_breach": {
            "name": "SLA Breach",
            "description": "Coverage for service level agreement violations",
            "premium_rate": 0.05,
            "requirements": ["metric_type", "actual_value", "sla_threshold"]
        },
        "resource_unavail": {
            "name": "Resource Unavailability",
            "description": "Coverage when requested resources are not available",
            "premium_rate": 0.08,
            "requirements": ["requested_resources", "availability_timestamp"]
        },
        "perf_degradation": {
            "name": "Performance Degradation",
            "description": "Coverage for performance below specifications",
            "premium_rate": 0.04,
            "requirements": ["expected_performance", "actual_performance"]
        },
        "data_loss": {
            "name": "Data Loss",
            "description": "Coverage for storage data loss incidents",
            "premium_rate": 0.15,
            "requirements": ["data_size", "recovery_attempted"]
        },
        "network_outage": {
            "name": "Network Outage",
            "description": "Coverage for network connectivity issues",
            "premium_rate": 0.06,
            "requirements": ["outage_duration", "affected_services"]
        },
        "security_breach": {
            "name": "Security Breach",
            "description": "Coverage for security incidents and breaches",
            "premium_rate": 0.2,
            "requirements": ["incident_type", "impact_assessment"]
        },
        "regulatory_violation": {
            "name": "Regulatory Violation",
            "description": "Coverage for compliance and regulatory issues",
            "premium_rate": 0.12,
            "requirements": ["violation_type", "regulatory_body"]
        }
    }
    
    return coverage_types


@router.get("/pool/requirements")
async def get_pool_requirements() -> Dict[str, Any]:
    """
    Get capital requirements for infrastructure insurance pool
    
    - Shows total coverage and premiums
    - Risk-weighted capital requirements
    - Pool utilization metrics
    """
    extension = await get_insurance_extension()
    
    return await extension.calculate_pool_requirements()


@router.post("/quote")
async def get_insurance_quote(
    request: CreatePolicyRequest,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get a quote for infrastructure insurance
    
    - No policy created, just pricing
    - Shows risk assessment
    - Breakdown of premium calculation
    """
    extension = await get_insurance_extension()
    
    try:
        # Calculate risk
        risk_result = await extension.risk_engine.calculate_unified_risk(
            resource_type=request.resource_type,
            service_tier=request.service_tier,
            provider=request.provider,
            amount=1000,
            duration_days=request.duration_days,
            region=request.region
        )
        
        # Calculate premium
        base_premium = await extension._calculate_premium(
            coverage_amount=request.coverage_amount,
            coverage_types=request.coverage_types,
            duration_days=request.duration_days,
            risk_score=Decimal(risk_result["risk_score"])
        )
        
        # Apply adjustments
        resource_multiplier = extension._get_resource_multiplier(request.resource_type)
        tier_discount = extension._get_tier_discount(request.service_tier)
        total_premium = base_premium * resource_multiplier * tier_discount
        
        return {
            "coverage_amount": float(request.coverage_amount),
            "premium": float(total_premium),
            "daily_premium": float(total_premium / request.duration_days),
            "risk_score": risk_result["risk_score"],
            "risk_level": risk_result["risk_level"],
            "risk_factors": risk_result["risk_factors"],
            "premium_breakdown": {
                "base_premium": float(base_premium),
                "resource_multiplier": float(resource_multiplier),
                "tier_discount": float(tier_discount)
            }
        }
        
    except Exception as e:
        logger.error(f"Error generating quote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_insurance_stats() -> Dict[str, Any]:
    """Get infrastructure insurance statistics"""
    extension = await get_insurance_extension()
    
    # Calculate statistics
    total_policies = len(extension._active_policies)
    active_policies = sum(1 for p in extension._active_policies.values() if p["status"] == "active")
    total_claims = sum(len(p["claims"]) for p in extension._active_policies.values())
    
    # Claims by type
    claims_by_type = {}
    for policy in extension._active_policies.values():
        for claim in policy["claims"]:
            claim_type = claim["claim_type"]
            claims_by_type[claim_type] = claims_by_type.get(claim_type, 0) + 1
    
    # Coverage by resource type
    coverage_by_resource = {}
    for policy in extension._active_policies.values():
        if policy["status"] == "active":
            resource_type = policy["resource_type"].value
            coverage_by_resource[resource_type] = coverage_by_resource.get(resource_type, 0) + float(policy["coverage_amount"])
    
    pool_requirements = await extension.calculate_pool_requirements()
    
    return {
        "total_policies": total_policies,
        "active_policies": active_policies,
        "total_claims": total_claims,
        "claims_by_type": claims_by_type,
        "coverage_by_resource": coverage_by_resource,
        "pool_requirements": pool_requirements
    } 