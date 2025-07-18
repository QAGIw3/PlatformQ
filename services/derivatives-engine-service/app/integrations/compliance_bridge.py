"""
DeFi Compliance Bridge

Implements compliant DeFi pools that automatically enforce KYC requirements
based on jurisdiction and regulatory requirements.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Set, Any
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from app.models.liquidity_pool import LiquidityPool, PoolConfig

logger = logging.getLogger(__name__)


class ComplianceLevel(Enum):
    """Compliance levels for DeFi pools"""
    UNRESTRICTED = "unrestricted"  # No KYC required (testnet only)
    BASIC_KYC = "basic_kyc"  # Tier 1 KYC required
    ENHANCED_KYC = "enhanced_kyc"  # Tier 2 KYC required
    INSTITUTIONAL = "institutional"  # Tier 3 KYC required
    ACCREDITED_ONLY = "accredited_only"  # Accredited investor verification


class PoolRestriction(Enum):
    """Types of pool restrictions"""
    JURISDICTION_BASED = "jurisdiction_based"
    AMOUNT_BASED = "amount_based"
    TIME_BASED = "time_based"
    TOKEN_BASED = "token_based"
    COMBINATION = "combination"


@dataclass
class ComplianceConfig:
    """Configuration for compliant pool"""
    min_kyc_tier: int
    allowed_jurisdictions: List[str]
    blocked_jurisdictions: List[str]
    max_position_size: Optional[Decimal]
    require_accredited: bool = False
    cooldown_period: Optional[timedelta] = None
    require_zkp: bool = True  # Use zero-knowledge proofs


class ComplianceBridge:
    """
    Bridges DeFi functionality with compliance requirements
    """
    
    def __init__(self):
        self.compliance_client = ServiceClient(
            service_name="compliance-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        self.vc_client = ServiceClient(
            service_name="verifiable-credential-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        # Cache for user compliance status
        self._compliance_cache: Dict[str, Dict] = {}
        self._cache_ttl = timedelta(minutes=15)
        
    async def create_compliant_pool(
        self,
        tenant_id: str,
        pool_config: PoolConfig,
        compliance_config: ComplianceConfig
    ) -> ProcessingResult:
        """
        Create a new liquidity pool with compliance requirements
        """
        try:
            # Validate compliance configuration
            validation_result = await self._validate_compliance_config(
                tenant_id,
                compliance_config
            )
            
            if not validation_result["valid"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Invalid compliance config: {validation_result['reason']}"
                )
            
            # Create pool with compliance metadata
            pool = LiquidityPool(
                pool_id=f"COMPLIANT_{pool_config.token_a}_{pool_config.token_b}_{datetime.utcnow().timestamp()}",
                tenant_id=tenant_id,
                config=pool_config,
                metadata={
                    "compliance": {
                        "min_kyc_tier": compliance_config.min_kyc_tier,
                        "allowed_jurisdictions": compliance_config.allowed_jurisdictions,
                        "blocked_jurisdictions": compliance_config.blocked_jurisdictions,
                        "max_position_size": str(compliance_config.max_position_size) if compliance_config.max_position_size else None,
                        "require_accredited": compliance_config.require_accredited,
                        "cooldown_period_seconds": compliance_config.cooldown_period.total_seconds() if compliance_config.cooldown_period else None,
                        "require_zkp": compliance_config.require_zkp
                    }
                }
            )
            
            # Deploy smart contract with compliance hooks
            deployment_result = await self._deploy_compliant_contract(pool, compliance_config)
            
            if deployment_result["success"]:
                pool.contract_address = deployment_result["address"]
                
                logger.info(
                    f"Created compliant pool {pool.pool_id} with "
                    f"min KYC tier {compliance_config.min_kyc_tier}"
                )
                
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data=pool.to_dict()
                )
            else:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=deployment_result["error"]
                )
                
        except Exception as e:
            logger.error(f"Error creating compliant pool: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def check_user_compliance(
        self,
        tenant_id: str,
        user_id: str,
        pool_id: str,
        action: str,  # "deposit", "withdraw", "swap", "provide_liquidity"
        amount: Optional[Decimal] = None
    ) -> ProcessingResult:
        """
        Check if user meets compliance requirements for pool action
        """
        try:
            # Check cache first
            cache_key = f"{user_id}:{pool_id}"
            if cache_key in self._compliance_cache:
                cached_data = self._compliance_cache[cache_key]
                if datetime.utcnow() - cached_data["timestamp"] < self._cache_ttl:
                    return ProcessingResult(
                        status=ProcessingStatus.SUCCESS,
                        data=cached_data["compliance"]
                    )
            
            # Get pool compliance requirements
            pool_requirements = await self._get_pool_requirements(tenant_id, pool_id)
            
            # Get user compliance status
            user_compliance = await self._get_user_compliance_status(tenant_id, user_id)
            
            # Perform compliance checks
            checks = {
                "kyc_check": await self._check_kyc_requirement(
                    user_compliance,
                    pool_requirements
                ),
                "jurisdiction_check": await self._check_jurisdiction(
                    user_compliance,
                    pool_requirements
                ),
                "amount_check": await self._check_amount_limits(
                    user_id,
                    pool_id,
                    action,
                    amount,
                    pool_requirements
                ),
                "accreditation_check": await self._check_accreditation(
                    user_compliance,
                    pool_requirements
                ),
                "cooldown_check": await self._check_cooldown_period(
                    user_id,
                    pool_id,
                    pool_requirements
                )
            }
            
            # Check if ZKP is required and available
            if pool_requirements.get("require_zkp", True):
                checks["zkp_available"] = await self._check_zkp_credentials(
                    tenant_id,
                    user_id,
                    pool_requirements["min_kyc_tier"]
                )
            
            # Overall compliance status
            is_compliant = all(
                check["passed"] for check in checks.values()
                if isinstance(check, dict) and "passed" in check
            )
            
            compliance_result = {
                "user_id": user_id,
                "pool_id": pool_id,
                "action": action,
                "is_compliant": is_compliant,
                "checks": checks,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Cache result
            self._compliance_cache[cache_key] = {
                "timestamp": datetime.utcnow(),
                "compliance": compliance_result
            }
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data=compliance_result
            )
            
        except Exception as e:
            logger.error(f"Error checking compliance: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def create_jurisdiction_pools(
        self,
        tenant_id: str,
        base_config: PoolConfig,
        jurisdictions: List[str]
    ) -> ProcessingResult:
        """
        Create separate pools for different jurisdictions with appropriate compliance
        """
        try:
            created_pools = []
            
            for jurisdiction in jurisdictions:
                # Get jurisdiction-specific requirements
                requirements = await self._get_jurisdiction_requirements(jurisdiction)
                
                # Create compliance config
                compliance_config = ComplianceConfig(
                    min_kyc_tier=requirements["min_kyc_tier"],
                    allowed_jurisdictions=[jurisdiction],
                    blocked_jurisdictions=requirements.get("blocked_countries", []),
                    max_position_size=Decimal(requirements.get("max_position", "1000000")),
                    require_accredited=requirements.get("require_accredited", False),
                    cooldown_period=timedelta(
                        hours=requirements.get("cooldown_hours", 0)
                    ) if requirements.get("cooldown_hours") else None
                )
                
                # Adjust pool config for jurisdiction
                adjusted_config = PoolConfig(
                    token_a=base_config.token_a,
                    token_b=base_config.token_b,
                    fee_tier=base_config.fee_tier,
                    initial_liquidity_a=base_config.initial_liquidity_a,
                    initial_liquidity_b=base_config.initial_liquidity_b,
                    concentrated_liquidity=base_config.concentrated_liquidity,
                    metadata={
                        **base_config.metadata,
                        "jurisdiction": jurisdiction
                    }
                )
                
                # Create compliant pool
                result = await self.create_compliant_pool(
                    tenant_id,
                    adjusted_config,
                    compliance_config
                )
                
                if result.status == ProcessingStatus.SUCCESS:
                    created_pools.append(result.data)
                else:
                    logger.error(
                        f"Failed to create pool for {jurisdiction}: {result.error}"
                    )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "pools_created": len(created_pools),
                    "pools": created_pools
                }
            )
            
        except Exception as e:
            logger.error(f"Error creating jurisdiction pools: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def generate_compliance_report(
        self,
        tenant_id: str,
        pool_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> ProcessingResult:
        """
        Generate compliance report for a pool
        """
        try:
            # Get all transactions for the pool
            transactions = await self._get_pool_transactions(
                tenant_id,
                pool_id,
                start_date,
                end_date
            )
            
            # Analyze compliance
            report_data = {
                "pool_id": pool_id,
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "total_transactions": len(transactions),
                "compliant_transactions": 0,
                "rejected_transactions": 0,
                "jurisdictions": {},
                "kyc_tiers": {},
                "suspicious_activity": [],
                "total_volume": Decimal("0")
            }
            
            for tx in transactions:
                if tx["compliant"]:
                    report_data["compliant_transactions"] += 1
                else:
                    report_data["rejected_transactions"] += 1
                
                # Track by jurisdiction
                jurisdiction = tx.get("user_jurisdiction", "unknown")
                if jurisdiction not in report_data["jurisdictions"]:
                    report_data["jurisdictions"][jurisdiction] = {
                        "count": 0,
                        "volume": Decimal("0")
                    }
                report_data["jurisdictions"][jurisdiction]["count"] += 1
                report_data["jurisdictions"][jurisdiction]["volume"] += Decimal(
                    tx.get("amount", "0")
                )
                
                # Track by KYC tier
                kyc_tier = str(tx.get("kyc_tier", "0"))
                if kyc_tier not in report_data["kyc_tiers"]:
                    report_data["kyc_tiers"][kyc_tier] = 0
                report_data["kyc_tiers"][kyc_tier] += 1
                
                # Check for suspicious patterns
                if await self._is_suspicious_activity(tx):
                    report_data["suspicious_activity"].append({
                        "tx_id": tx["id"],
                        "user_id": tx["user_id"],
                        "reason": tx.get("suspicious_reason", "Pattern detected")
                    })
                
                report_data["total_volume"] += Decimal(tx.get("amount", "0"))
            
            # Convert Decimals to strings for JSON serialization
            report_data["total_volume"] = str(report_data["total_volume"])
            for jurisdiction in report_data["jurisdictions"].values():
                jurisdiction["volume"] = str(jurisdiction["volume"])
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data=report_data
            )
            
        except Exception as e:
            logger.error(f"Error generating compliance report: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _validate_compliance_config(
        self,
        tenant_id: str,
        config: ComplianceConfig
    ) -> Dict[str, Any]:
        """Validate compliance configuration"""
        # Check for conflicting jurisdictions
        if set(config.allowed_jurisdictions) & set(config.blocked_jurisdictions):
            return {
                "valid": False,
                "reason": "Jurisdictions cannot be both allowed and blocked"
            }
        
        # Validate KYC tier
        if config.min_kyc_tier not in [1, 2, 3]:
            return {
                "valid": False,
                "reason": "Invalid KYC tier"
            }
        
        # Validate jurisdictions
        for jurisdiction in config.allowed_jurisdictions + config.blocked_jurisdictions:
            if not await self._is_valid_jurisdiction(jurisdiction):
                return {
                    "valid": False,
                    "reason": f"Invalid jurisdiction: {jurisdiction}"
                }
        
        return {"valid": True}
    
    async def _deploy_compliant_contract(
        self,
        pool: LiquidityPool,
        config: ComplianceConfig
    ) -> Dict[str, Any]:
        """Deploy smart contract with compliance hooks"""
        # This would deploy actual smart contract
        # For now, return mock deployment
        return {
            "success": True,
            "address": f"0x{''.join(['abcdef1234567890'[i % 16] for i in range(40)])}"
        }
    
    async def _get_pool_requirements(
        self,
        tenant_id: str,
        pool_id: str
    ) -> Dict[str, Any]:
        """Get compliance requirements for a pool"""
        # This would fetch from database/contract
        # For now, return mock data
        return {
            "min_kyc_tier": 2,
            "allowed_jurisdictions": ["US", "UK", "EU"],
            "blocked_jurisdictions": ["restricted_country"],
            "max_position_size": "100000",
            "require_accredited": False,
            "cooldown_period_seconds": 3600,
            "require_zkp": True
        }
    
    async def _get_user_compliance_status(
        self,
        tenant_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Get user's compliance status from compliance service"""
        try:
            response = await self.compliance_client.get(
                f"/api/v1/kyc/status/{user_id}",
                params={"tenant_id": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "kyc_tier": 0,
                    "jurisdiction": "unknown",
                    "is_accredited": False
                }
                
        except Exception as e:
            logger.error(f"Error fetching compliance status: {str(e)}")
            return {
                "kyc_tier": 0,
                "jurisdiction": "unknown",
                "is_accredited": False
            }
    
    async def _check_kyc_requirement(
        self,
        user_compliance: Dict[str, Any],
        pool_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if user meets KYC requirements"""
        user_tier = user_compliance.get("kyc_tier", 0)
        required_tier = pool_requirements.get("min_kyc_tier", 1)
        
        return {
            "passed": user_tier >= required_tier,
            "user_tier": user_tier,
            "required_tier": required_tier
        }
    
    async def _check_jurisdiction(
        self,
        user_compliance: Dict[str, Any],
        pool_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check jurisdiction restrictions"""
        user_jurisdiction = user_compliance.get("jurisdiction", "unknown")
        allowed = pool_requirements.get("allowed_jurisdictions", [])
        blocked = pool_requirements.get("blocked_jurisdictions", [])
        
        passed = True
        reason = "Jurisdiction allowed"
        
        if blocked and user_jurisdiction in blocked:
            passed = False
            reason = f"Jurisdiction {user_jurisdiction} is blocked"
        elif allowed and user_jurisdiction not in allowed:
            passed = False
            reason = f"Jurisdiction {user_jurisdiction} is not in allowed list"
        
        return {
            "passed": passed,
            "user_jurisdiction": user_jurisdiction,
            "reason": reason
        }
    
    async def _check_amount_limits(
        self,
        user_id: str,
        pool_id: str,
        action: str,
        amount: Optional[Decimal],
        pool_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check position size limits"""
        if not amount:
            return {"passed": True, "reason": "No amount specified"}
        
        max_position = Decimal(pool_requirements.get("max_position_size", "1000000"))
        
        # Get current position
        current_position = await self._get_user_position(user_id, pool_id)
        
        new_position = current_position
        if action in ["deposit", "provide_liquidity"]:
            new_position += amount
        elif action == "withdraw":
            new_position -= amount
        
        return {
            "passed": new_position <= max_position,
            "current_position": str(current_position),
            "new_position": str(new_position),
            "max_allowed": str(max_position)
        }
    
    async def _check_accreditation(
        self,
        user_compliance: Dict[str, Any],
        pool_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check accredited investor status"""
        if not pool_requirements.get("require_accredited", False):
            return {"passed": True, "required": False}
        
        is_accredited = user_compliance.get("is_accredited", False)
        
        return {
            "passed": is_accredited,
            "required": True,
            "user_accredited": is_accredited
        }
    
    async def _check_cooldown_period(
        self,
        user_id: str,
        pool_id: str,
        pool_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if user is in cooldown period"""
        cooldown_seconds = pool_requirements.get("cooldown_period_seconds", 0)
        
        if not cooldown_seconds:
            return {"passed": True, "cooldown_required": False}
        
        # Get last transaction time
        last_tx_time = await self._get_last_transaction_time(user_id, pool_id)
        
        if not last_tx_time:
            return {"passed": True, "first_transaction": True}
        
        time_since_last = (datetime.utcnow() - last_tx_time).total_seconds()
        
        return {
            "passed": time_since_last >= cooldown_seconds,
            "cooldown_seconds": cooldown_seconds,
            "time_since_last": time_since_last,
            "remaining_cooldown": max(0, cooldown_seconds - time_since_last)
        }
    
    async def _check_zkp_credentials(
        self,
        tenant_id: str,
        user_id: str,
        required_tier: int
    ) -> bool:
        """Check if user has ZKP credentials for required KYC tier"""
        try:
            response = await self.vc_client.get(
                f"/api/v1/credentials/{user_id}/kyc-zkp",
                params={
                    "tenant_id": tenant_id,
                    "min_tier": required_tier
                }
            )
            
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Error checking ZKP credentials: {str(e)}")
            return False
    
    async def _get_jurisdiction_requirements(
        self,
        jurisdiction: str
    ) -> Dict[str, Any]:
        """Get regulatory requirements for a jurisdiction"""
        # Jurisdiction-specific requirements
        requirements_map = {
            "US": {
                "min_kyc_tier": 2,
                "require_accredited": True,
                "max_position": "250000",
                "cooldown_hours": 24,
                "blocked_countries": ["sanctioned_country"]
            },
            "EU": {
                "min_kyc_tier": 2,
                "require_accredited": False,
                "max_position": "100000",
                "cooldown_hours": 0,
                "blocked_countries": ["sanctioned_country"]
            },
            "UK": {
                "min_kyc_tier": 2,
                "require_accredited": False,
                "max_position": "100000",
                "cooldown_hours": 0,
                "blocked_countries": ["sanctioned_country"]
            },
            "JP": {
                "min_kyc_tier": 3,
                "require_accredited": False,
                "max_position": "50000",
                "cooldown_hours": 48,
                "blocked_countries": ["sanctioned_country"]
            }
        }
        
        return requirements_map.get(jurisdiction, {
            "min_kyc_tier": 1,
            "require_accredited": False,
            "max_position": "10000",
            "cooldown_hours": 0,
            "blocked_countries": []
        })
    
    async def _get_pool_transactions(
        self,
        tenant_id: str,
        pool_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get all transactions for a pool in date range"""
        # This would fetch from database
        # For now, return mock data
        return []
    
    async def _is_suspicious_activity(
        self,
        transaction: Dict[str, Any]
    ) -> bool:
        """Check if transaction shows suspicious patterns"""
        # Implement suspicious activity detection
        # For now, return False
        return False
    
    async def _is_valid_jurisdiction(self, jurisdiction: str) -> bool:
        """Check if jurisdiction code is valid"""
        # This would validate against ISO country codes
        return len(jurisdiction) == 2
    
    async def _get_user_position(
        self,
        user_id: str,
        pool_id: str
    ) -> Decimal:
        """Get user's current position in pool"""
        # This would fetch from blockchain/database
        # For now, return mock data
        return Decimal("0")
    
    async def _get_last_transaction_time(
        self,
        user_id: str,
        pool_id: str
    ) -> Optional[datetime]:
        """Get user's last transaction time for pool"""
        # This would fetch from database
        # For now, return None
        return None 