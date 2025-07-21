"""
Compute Resource Insurance Protocol

Provides risk management and insurance coverage for compute resources including:
- Quality Degradation Insurance
- Availability Insurance
- Performance Guarantee Insurance
- Slashing Insurance for staked providers
- Smart Contract Coverage
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
import numpy as np
from collections import defaultdict
import hashlib
import json

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType
from ..models import (
    InsurancePool, InsurancePolicy, InsuranceClaim,
    RiskAssessment, PremiumCalculation
)

logger = logging.getLogger(__name__)


class InsuranceCoverageType(str, Enum):
    QUALITY_DEGRADATION = "quality_degradation"  # Protection against quality drop
    AVAILABILITY = "availability"  # Coverage for downtime/unavailability
    PERFORMANCE_GUARANTEE = "performance_guarantee"  # Performance benchmarks
    SLASHING = "slashing"  # Protection for staked providers
    SMART_CONTRACT = "smart_contract"  # Smart contract exploit coverage
    BUNDLE = "bundle"  # Combined coverage package


class ClaimStatus(str, Enum):
    PENDING = "pending"
    INVESTIGATING = "investigating"
    APPROVED = "approved"
    REJECTED = "rejected"
    PAID = "paid"
    APPEALED = "appealed"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ComputeResourceInsurance:
    """Insurance protocol for compute resource protection"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        insurance_factory_address: str,
        oracle_address: str,
        quantum_market_address: str,
        ai_market_address: str,
        network_market_address: str,
        quality_oracle_address: str,
        availability_monitor_address: str,
        treasury_address: str
    ):
        self.blockchain = blockchain_client
        self.insurance_factory_address = insurance_factory_address
        self.oracle_address = oracle_address
        self.quality_oracle_address = quality_oracle_address
        self.availability_monitor_address = availability_monitor_address
        self.treasury_address = treasury_address
        
        # Market addresses
        self.quantum_market_address = quantum_market_address
        self.ai_market_address = ai_market_address
        self.network_market_address = network_market_address
        
        # Insurance parameters
        self.base_premium_rates = {
            InsuranceCoverageType.QUALITY_DEGRADATION: Decimal("0.02"),  # 2% base
            InsuranceCoverageType.AVAILABILITY: Decimal("0.025"),  # 2.5% base
            InsuranceCoverageType.PERFORMANCE_GUARANTEE: Decimal("0.03"),  # 3% base
            InsuranceCoverageType.SLASHING: Decimal("0.04"),  # 4% base
            InsuranceCoverageType.SMART_CONTRACT: Decimal("0.015"),  # 1.5% base
        }
        
        # Coverage limits
        self.max_coverage_ratios = {
            InsuranceCoverageType.QUALITY_DEGRADATION: Decimal("0.8"),  # 80% of value
            InsuranceCoverageType.AVAILABILITY: Decimal("1.0"),  # 100% of value
            InsuranceCoverageType.PERFORMANCE_GUARANTEE: Decimal("1.2"),  # 120% of value
            InsuranceCoverageType.SLASHING: Decimal("1.0"),  # 100% of stake
            InsuranceCoverageType.SMART_CONTRACT: Decimal("2.0"),  # 200% of value
        }
        
        # Risk parameters
        self.risk_multipliers = {
            RiskLevel.LOW: Decimal("0.8"),
            RiskLevel.MEDIUM: Decimal("1.0"),
            RiskLevel.HIGH: Decimal("1.5"),
            RiskLevel.CRITICAL: Decimal("2.5")
        }
        
        # Deductibles
        self.default_deductibles = {
            InsuranceCoverageType.QUALITY_DEGRADATION: Decimal("0.05"),  # 5%
            InsuranceCoverageType.AVAILABILITY: Decimal("0.10"),  # 10%
            InsuranceCoverageType.PERFORMANCE_GUARANTEE: Decimal("0.15"),  # 15%
            InsuranceCoverageType.SLASHING: Decimal("0.0"),  # No deductible
            InsuranceCoverageType.SMART_CONTRACT: Decimal("0.20"),  # 20%
        }
        
        # Internal tracking
        self._insurance_pools = {}  # pool_id -> pool_data
        self._active_policies = {}  # policy_id -> policy_data
        self._claims = {}  # claim_id -> claim_data
        self._risk_profiles = {}  # address -> risk_profile
        
        # Monitoring tasks
        self._monitoring_tasks = {}
        
    async def create_insurance_pool(
        self,
        resource_type: str,
        coverage_type: InsuranceCoverageType,
        initial_capital: Decimal,
        target_size: Decimal,
        reserve_ratio: Decimal = Decimal("0.2")  # 20% reserves
    ) -> Dict[str, Any]:
        """
        Create an insurance pool for specific coverage
        
        Args:
            resource_type: Type of compute resource (quantum/ai/network)
            coverage_type: Type of insurance coverage
            initial_capital: Initial pool capital
            target_size: Target pool size
            reserve_ratio: Reserve requirement ratio
            
        Returns:
            Pool creation result
        """
        try:
            # Deploy insurance pool contract
            factory = await self.blockchain.get_contract(
                self.insurance_factory_address,
                "InsurancePoolFactory"
            )
            
            tx = await factory.functions.createInsurancePool(
                resource_type,
                coverage_type,
                Web3.toWei(initial_capital, 'ether'),
                Web3.toWei(target_size, 'ether'),
                int(reserve_ratio * 10000)  # Basis points
            ).transact({'value': Web3.toWei(initial_capital, 'ether')})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get pool details from event
            pool_created = receipt.events.get('InsurancePoolCreated')
            pool_id = pool_created['poolId']
            pool_address = pool_created['poolAddress']
            
            # Initialize pool tracking
            pool_data = {
                'pool_id': pool_id,
                'pool_address': pool_address,
                'resource_type': resource_type,
                'coverage_type': coverage_type,
                'total_capital': initial_capital,
                'available_capital': initial_capital,
                'reserved_capital': initial_capital * reserve_ratio,
                'target_size': target_size,
                'reserve_ratio': reserve_ratio,
                'active_coverage': Decimal("0"),
                'total_premiums_collected': Decimal("0"),
                'total_claims_paid': Decimal("0"),
                'created_at': datetime.utcnow(),
                'policies': []
            }
            
            self._insurance_pools[pool_id] = pool_data
            
            logger.info(f"Created insurance pool {pool_id} for {resource_type} {coverage_type}")
            
            return {
                'pool_id': pool_id,
                'pool_address': pool_address,
                'initial_capital': initial_capital,
                'available_for_coverage': initial_capital * (1 - reserve_ratio),
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create insurance pool: {e}")
            raise
    
    async def purchase_policy(
        self,
        pool_id: str,
        policyholder: str,
        resource_ids: List[int],
        coverage_amount: Decimal,
        coverage_period_days: int,
        deductible_override: Optional[Decimal] = None,
        bundle_discount: bool = False
    ) -> Dict[str, Any]:
        """
        Purchase insurance policy for compute resources
        
        Args:
            pool_id: Insurance pool ID
            policyholder: Policy purchaser address
            resource_ids: Resources to insure
            coverage_amount: Maximum coverage amount
            coverage_period_days: Coverage duration
            deductible_override: Custom deductible percentage
            bundle_discount: Apply bundle discount for multiple coverages
            
        Returns:
            Policy details
        """
        try:
            pool_data = self._insurance_pools.get(pool_id)
            if not pool_data:
                raise ValueError("Insurance pool not found")
            
            # Assess risk for resources
            risk_assessment = await self._assess_resource_risk(
                resource_ids,
                pool_data['resource_type'],
                pool_data['coverage_type']
            )
            
            # Calculate premium
            base_rate = self.base_premium_rates[pool_data['coverage_type']]
            risk_multiplier = self.risk_multipliers[risk_assessment['risk_level']]
            
            # Duration adjustment (longer = slight discount)
            duration_factor = 1 - min(coverage_period_days / 365, 0.2)  # Max 20% discount
            
            # Deductible adjustment
            deductible = deductible_override or self.default_deductibles[pool_data['coverage_type']]
            deductible_factor = 1 - (deductible * Decimal("0.5"))  # Higher deductible = lower premium
            
            # Calculate annual premium
            annual_premium = coverage_amount * base_rate * risk_multiplier * duration_factor * deductible_factor
            
            # Apply bundle discount if applicable
            if bundle_discount:
                annual_premium *= Decimal("0.85")  # 15% bundle discount
            
            # Pro-rate for actual period
            total_premium = annual_premium * coverage_period_days / 365
            
            # Check pool capacity
            if coverage_amount > pool_data['available_capital'] - pool_data['reserved_capital']:
                raise ValueError("Insufficient pool capacity for coverage")
            
            # Create policy on-chain
            pool_contract = await self.blockchain.get_contract(
                pool_data['pool_address'],
                "InsurancePool"
            )
            
            tx = await pool_contract.functions.createPolicy(
                policyholder,
                resource_ids,
                Web3.toWei(coverage_amount, 'ether'),
                coverage_period_days * 86400,  # Convert to seconds
                int(deductible * 10000),  # Basis points
                Web3.toWei(total_premium, 'ether')
            ).transact({'value': Web3.toWei(total_premium, 'ether')})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get policy details from event
            policy_created = receipt.events.get('PolicyCreated')
            policy_id = policy_created['policyId']
            
            # Store policy data
            policy_data = {
                'policy_id': policy_id,
                'pool_id': pool_id,
                'policyholder': policyholder,
                'resource_ids': resource_ids,
                'coverage_type': pool_data['coverage_type'],
                'coverage_amount': coverage_amount,
                'premium': total_premium,
                'deductible': deductible,
                'start_date': datetime.utcnow(),
                'end_date': datetime.utcnow() + timedelta(days=coverage_period_days),
                'risk_assessment': risk_assessment,
                'active': True,
                'claims': []
            }
            
            self._active_policies[policy_id] = policy_data
            pool_data['policies'].append(policy_id)
            pool_data['active_coverage'] += coverage_amount
            pool_data['total_premiums_collected'] += total_premium
            pool_data['available_capital'] += total_premium
            
            # Start monitoring for this policy
            asyncio.create_task(self._monitor_policy(policy_id))
            
            return {
                'policy_id': policy_id,
                'coverage_amount': coverage_amount,
                'premium': total_premium,
                'annual_rate': float(annual_premium / coverage_amount),
                'deductible': deductible,
                'risk_level': risk_assessment['risk_level'],
                'coverage_period': f"{coverage_period_days} days",
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to purchase policy: {e}")
            raise
    
    async def file_claim(
        self,
        policy_id: str,
        claim_type: str,
        incident_data: Dict[str, Any],
        requested_amount: Decimal,
        evidence_hashes: List[str]
    ) -> Dict[str, Any]:
        """
        File an insurance claim
        
        Args:
            policy_id: Policy ID
            claim_type: Type of claim (matches coverage type)
            incident_data: Details about the incident
            requested_amount: Amount being claimed
            evidence_hashes: IPFS hashes of evidence
            
        Returns:
            Claim details
        """
        try:
            policy_data = self._active_policies.get(policy_id)
            if not policy_data:
                raise ValueError("Policy not found")
            
            if not policy_data['active']:
                raise ValueError("Policy is not active")
            
            if datetime.utcnow() > policy_data['end_date']:
                raise ValueError("Policy has expired")
            
            # Validate claim type matches coverage
            if claim_type != policy_data['coverage_type']:
                raise ValueError(f"Policy covers {policy_data['coverage_type']}, not {claim_type}")
            
            # Apply deductible
            deductible_amount = requested_amount * policy_data['deductible']
            claimable_amount = min(
                requested_amount - deductible_amount,
                policy_data['coverage_amount']
            )
            
            # Create claim on-chain
            pool_data = self._insurance_pools[policy_data['pool_id']]
            pool_contract = await self.blockchain.get_contract(
                pool_data['pool_address'],
                "InsurancePool"
            )
            
            # Create incident hash
            incident_hash = hashlib.sha256(
                json.dumps(incident_data, sort_keys=True).encode()
            ).hexdigest()
            
            tx = await pool_contract.functions.fileClaim(
                policy_id,
                Web3.toWei(requested_amount, 'ether'),
                incident_hash,
                evidence_hashes
            ).transact({'from': policy_data['policyholder']})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get claim ID from event
            claim_filed = receipt.events.get('ClaimFiled')
            claim_id = claim_filed['claimId']
            
            # Store claim data
            claim_data = {
                'claim_id': claim_id,
                'policy_id': policy_id,
                'claim_type': claim_type,
                'status': ClaimStatus.PENDING,
                'incident_data': incident_data,
                'incident_hash': incident_hash,
                'requested_amount': requested_amount,
                'deductible_amount': deductible_amount,
                'claimable_amount': claimable_amount,
                'evidence_hashes': evidence_hashes,
                'filed_at': datetime.utcnow(),
                'investigation_notes': [],
                'decision': None
            }
            
            self._claims[claim_id] = claim_data
            policy_data['claims'].append(claim_id)
            
            # Start claim investigation
            asyncio.create_task(self._investigate_claim(claim_id))
            
            return {
                'claim_id': claim_id,
                'status': ClaimStatus.PENDING,
                'requested_amount': requested_amount,
                'deductible_applied': deductible_amount,
                'maximum_payout': claimable_amount,
                'investigation_eta': '24-48 hours',
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to file claim: {e}")
            raise
    
    async def provide_liquidity(
        self,
        pool_id: str,
        provider: str,
        amount: Decimal
    ) -> Dict[str, Any]:
        """
        Provide liquidity to insurance pool
        
        Args:
            pool_id: Pool to provide liquidity to
            provider: Liquidity provider address
            amount: Amount to provide
            
        Returns:
            LP token details
        """
        try:
            pool_data = self._insurance_pools.get(pool_id)
            if not pool_data:
                raise ValueError("Pool not found")
            
            # Calculate LP tokens based on pool share
            current_total = pool_data['total_capital']
            lp_tokens = amount * 1000  # Initial rate
            
            if current_total > 0:
                # Proportional to existing capital
                lp_tokens = (amount / current_total) * pool_data.get('total_lp_tokens', current_total * 1000)
            
            # Add liquidity on-chain
            pool_contract = await self.blockchain.get_contract(
                pool_data['pool_address'],
                "InsurancePool"
            )
            
            tx = await pool_contract.functions.addLiquidity(
                provider
            ).transact({
                'from': provider,
                'value': Web3.toWei(amount, 'ether')
            })
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update pool data
            pool_data['total_capital'] += amount
            pool_data['available_capital'] += amount
            pool_data['total_lp_tokens'] = pool_data.get('total_lp_tokens', current_total * 1000) + lp_tokens
            
            # Calculate APY based on premiums and claims
            apy = await self._calculate_pool_apy(pool_data)
            
            return {
                'tx_hash': tx,
                'lp_tokens_received': lp_tokens,
                'pool_share': float(lp_tokens / pool_data['total_lp_tokens']),
                'current_apy': apy,
                'total_pool_capital': pool_data['total_capital']
            }
            
        except Exception as e:
            logger.error(f"Failed to provide liquidity: {e}")
            raise
    
    async def stake_for_slashing_insurance(
        self,
        provider_address: str,
        stake_amount: Decimal,
        resource_type: str,
        coverage_multiplier: Decimal = Decimal("10")  # 10x coverage
    ) -> Dict[str, Any]:
        """
        Stake tokens to get slashing insurance for compute providers
        
        Args:
            provider_address: Compute provider address
            stake_amount: Amount to stake
            resource_type: Type of compute resource provided
            coverage_multiplier: Coverage amount multiplier
            
        Returns:
            Staking and insurance details
        """
        try:
            # Find or create slashing insurance pool
            slashing_pool = None
            for pool_id, pool_data in self._insurance_pools.items():
                if (pool_data['resource_type'] == resource_type and 
                    pool_data['coverage_type'] == InsuranceCoverageType.SLASHING):
                    slashing_pool = pool_data
                    break
            
            if not slashing_pool:
                raise ValueError(f"No slashing insurance pool for {resource_type}")
            
            # Calculate coverage amount
            coverage_amount = stake_amount * coverage_multiplier
            
            # Special premium calculation for staking insurance
            # Lower premium since stake itself provides security
            base_premium_rate = Decimal("0.005")  # 0.5% annual
            
            # Risk assessment based on provider history
            risk_assessment = await self._assess_provider_risk(provider_address)
            risk_multiplier = self.risk_multipliers[risk_assessment['risk_level']]
            
            annual_premium = coverage_amount * base_premium_rate * risk_multiplier
            
            # Create staking position with insurance
            staking_contract = await self.blockchain.get_contract(
                slashing_pool['pool_address'],
                "SlashingInsurance"
            )
            
            tx = await staking_contract.functions.stakeWithInsurance(
                provider_address,
                Web3.toWei(stake_amount, 'ether'),
                Web3.toWei(coverage_amount, 'ether'),
                Web3.toWei(annual_premium, 'ether')
            ).transact({
                'from': provider_address,
                'value': Web3.toWei(stake_amount + annual_premium, 'ether')
            })
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get staking details from event
            stake_created = receipt.events.get('StakeWithInsuranceCreated')
            stake_id = stake_created['stakeId']
            policy_id = stake_created['policyId']
            
            # Create automatic policy
            policy_data = {
                'policy_id': policy_id,
                'pool_id': slashing_pool['pool_id'],
                'policyholder': provider_address,
                'coverage_type': InsuranceCoverageType.SLASHING,
                'coverage_amount': coverage_amount,
                'premium': annual_premium,
                'deductible': Decimal("0"),  # No deductible for slashing
                'start_date': datetime.utcnow(),
                'end_date': datetime.utcnow() + timedelta(days=365),
                'stake_amount': stake_amount,
                'stake_id': stake_id,
                'auto_renew': True,
                'active': True,
                'claims': []
            }
            
            self._active_policies[policy_id] = policy_data
            
            return {
                'stake_id': stake_id,
                'policy_id': policy_id,
                'stake_amount': stake_amount,
                'coverage_amount': coverage_amount,
                'annual_premium': annual_premium,
                'effective_apr': float((coverage_amount - annual_premium) / stake_amount - 1),
                'risk_level': risk_assessment['risk_level'],
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to stake for slashing insurance: {e}")
            raise
    
    async def process_claim_payout(
        self,
        claim_id: str,
        approver: str
    ) -> Dict[str, Any]:
        """
        Process approved claim payout
        
        Args:
            claim_id: Claim to process
            approver: Authorized approver address
            
        Returns:
            Payout details
        """
        try:
            claim_data = self._claims.get(claim_id)
            if not claim_data:
                raise ValueError("Claim not found")
            
            if claim_data['status'] != ClaimStatus.APPROVED:
                raise ValueError("Claim not approved")
            
            policy_data = self._active_policies[claim_data['policy_id']]
            pool_data = self._insurance_pools[policy_data['pool_id']]
            
            # Execute payout on-chain
            pool_contract = await self.blockchain.get_contract(
                pool_data['pool_address'],
                "InsurancePool"
            )
            
            tx = await pool_contract.functions.processPayout(
                claim_id,
                policy_data['policyholder'],
                Web3.toWei(claim_data['claimable_amount'], 'ether')
            ).transact({'from': approver})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update claim status
            claim_data['status'] = ClaimStatus.PAID
            claim_data['paid_at'] = datetime.utcnow()
            claim_data['payout_tx'] = tx
            
            # Update pool statistics
            pool_data['total_claims_paid'] += claim_data['claimable_amount']
            pool_data['available_capital'] -= claim_data['claimable_amount']
            
            # Update risk profile
            await self._update_risk_profile(
                policy_data['policyholder'],
                claim_data
            )
            
            return {
                'success': True,
                'claim_id': claim_id,
                'payout_amount': claim_data['claimable_amount'],
                'recipient': policy_data['policyholder'],
                'tx_hash': tx,
                'pool_remaining_capital': pool_data['available_capital']
            }
            
        except Exception as e:
            logger.error(f"Failed to process claim payout: {e}")
            raise
    
    # Risk Assessment Methods
    
    async def _assess_resource_risk(
        self,
        resource_ids: List[int],
        resource_type: str,
        coverage_type: InsuranceCoverageType
    ) -> Dict[str, Any]:
        """Assess risk for compute resources"""
        risk_scores = []
        
        for resource_id in resource_ids:
            # Get resource quality history
            quality_history = await self._get_quality_history(resource_id)
            
            # Get availability history
            availability_history = await self._get_availability_history(resource_id)
            
            # Calculate risk factors
            quality_volatility = np.std(quality_history) if quality_history else 0
            availability_rate = np.mean(availability_history) if availability_history else 1.0
            
            # Coverage-specific risk calculation
            if coverage_type == InsuranceCoverageType.QUALITY_DEGRADATION:
                risk_score = quality_volatility * 10  # Higher volatility = higher risk
            elif coverage_type == InsuranceCoverageType.AVAILABILITY:
                risk_score = (1 - availability_rate) * 100  # Lower availability = higher risk
            elif coverage_type == InsuranceCoverageType.PERFORMANCE_GUARANTEE:
                risk_score = quality_volatility * 5 + (1 - availability_rate) * 50
            else:
                risk_score = 25  # Default medium risk
            
            risk_scores.append(risk_score)
        
        avg_risk_score = np.mean(risk_scores) if risk_scores else 50
        
        # Determine risk level
        if avg_risk_score < 20:
            risk_level = RiskLevel.LOW
        elif avg_risk_score < 50:
            risk_level = RiskLevel.MEDIUM
        elif avg_risk_score < 80:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.CRITICAL
        
        return {
            'risk_level': risk_level,
            'risk_score': avg_risk_score,
            'factors': {
                'quality_volatility': float(np.mean([quality_volatility])),
                'availability_rate': float(availability_rate),
                'resource_count': len(resource_ids)
            }
        }
    
    async def _assess_provider_risk(
        self,
        provider_address: str
    ) -> Dict[str, Any]:
        """Assess risk for compute provider"""
        # Get provider history from markets
        slash_history = await self._get_provider_slash_history(provider_address)
        quality_history = await self._get_provider_quality_history(provider_address)
        
        # Calculate risk factors
        slash_count = len(slash_history)
        avg_quality = np.mean(quality_history) if quality_history else 80
        
        # Risk scoring
        risk_score = slash_count * 20 + (100 - avg_quality)
        
        if risk_score < 20:
            risk_level = RiskLevel.LOW
        elif risk_score < 40:
            risk_level = RiskLevel.MEDIUM
        elif risk_score < 60:
            risk_level = RiskLevel.HIGH
        else:
            risk_level = RiskLevel.CRITICAL
        
        return {
            'risk_level': risk_level,
            'risk_score': risk_score,
            'slash_count': slash_count,
            'average_quality': avg_quality
        }
    
    # Claim Investigation Methods
    
    async def _investigate_claim(self, claim_id: str):
        """Automated claim investigation"""
        try:
            claim_data = self._claims[claim_id]
            policy_data = self._active_policies[claim_data['policy_id']]
            
            investigation_result = {
                'valid': True,
                'confidence': Decimal("0"),
                'reasons': []
            }
            
            # Investigation based on claim type
            if claim_data['claim_type'] == InsuranceCoverageType.QUALITY_DEGRADATION:
                result = await self._investigate_quality_claim(claim_data, policy_data)
                investigation_result.update(result)
                
            elif claim_data['claim_type'] == InsuranceCoverageType.AVAILABILITY:
                result = await self._investigate_availability_claim(claim_data, policy_data)
                investigation_result.update(result)
                
            elif claim_data['claim_type'] == InsuranceCoverageType.PERFORMANCE_GUARANTEE:
                result = await self._investigate_performance_claim(claim_data, policy_data)
                investigation_result.update(result)
                
            elif claim_data['claim_type'] == InsuranceCoverageType.SLASHING:
                result = await self._investigate_slashing_claim(claim_data, policy_data)
                investigation_result.update(result)
                
            elif claim_data['claim_type'] == InsuranceCoverageType.SMART_CONTRACT:
                result = await self._investigate_smart_contract_claim(claim_data, policy_data)
                investigation_result.update(result)
            
            # Make decision
            if investigation_result['valid'] and investigation_result['confidence'] >= Decimal("0.8"):
                claim_data['status'] = ClaimStatus.APPROVED
                claim_data['decision'] = {
                    'approved': True,
                    'reason': 'Automated investigation passed',
                    'confidence': investigation_result['confidence']
                }
            elif investigation_result['confidence'] >= Decimal("0.5"):
                claim_data['status'] = ClaimStatus.INVESTIGATING
                claim_data['decision'] = {
                    'pending_review': True,
                    'reason': 'Requires manual review',
                    'confidence': investigation_result['confidence']
                }
            else:
                claim_data['status'] = ClaimStatus.REJECTED
                claim_data['decision'] = {
                    'approved': False,
                    'reason': investigation_result['reasons'],
                    'confidence': investigation_result['confidence']
                }
            
            # Update on-chain
            await self._update_claim_status(claim_id, claim_data['status'])
            
        except Exception as e:
            logger.error(f"Failed to investigate claim {claim_id}: {e}")
            claim_data['status'] = ClaimStatus.INVESTIGATING
    
    async def _investigate_quality_claim(
        self,
        claim_data: Dict[str, Any],
        policy_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Investigate quality degradation claim"""
        resource_ids = policy_data['resource_ids']
        incident_data = claim_data['incident_data']
        
        # Get quality data from oracle
        quality_oracle = await self.blockchain.get_contract(
            self.quality_oracle_address,
            "QualityOracle"
        )
        
        valid_degradation = True
        confidence = Decimal("0.9")
        reasons = []
        
        for resource_id in resource_ids:
            # Get quality at incident time
            incident_quality = await quality_oracle.functions.getHistoricalQuality(
                resource_id,
                int(incident_data['timestamp'])
            ).call()
            
            # Get baseline quality
            baseline_quality = await quality_oracle.functions.getBaselineQuality(
                resource_id
            ).call()
            
            degradation = (baseline_quality - incident_quality) / baseline_quality
            
            if degradation < Decimal("0.1"):  # Less than 10% degradation
                valid_degradation = False
                confidence *= Decimal("0.5")
                reasons.append(f"Resource {resource_id} degradation below threshold")
        
        return {
            'valid': valid_degradation,
            'confidence': confidence,
            'reasons': reasons
        }
    
    async def _investigate_availability_claim(
        self,
        claim_data: Dict[str, Any],
        policy_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Investigate availability claim"""
        resource_ids = policy_data['resource_ids']
        incident_data = claim_data['incident_data']
        
        # Get availability data from monitor
        monitor = await self.blockchain.get_contract(
            self.availability_monitor_address,
            "AvailabilityMonitor"
        )
        
        downtime_confirmed = True
        confidence = Decimal("0.95")
        reasons = []
        
        for resource_id in resource_ids:
            # Check downtime records
            downtime_data = await monitor.functions.getDowntimeRecord(
                resource_id,
                int(incident_data['start_time']),
                int(incident_data['end_time'])
            ).call()
            
            if downtime_data['duration'] < incident_data['claimed_duration'] * 0.9:
                downtime_confirmed = False
                confidence *= Decimal("0.6")
                reasons.append(f"Resource {resource_id} downtime not fully confirmed")
        
        return {
            'valid': downtime_confirmed,
            'confidence': confidence,
            'reasons': reasons
        }
    
    async def _investigate_performance_claim(
        self,
        claim_data: Dict[str, Any],
        policy_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Investigate performance guarantee claim"""
        # Combine quality and availability investigations
        quality_result = await self._investigate_quality_claim(claim_data, policy_data)
        availability_result = await self._investigate_availability_claim(claim_data, policy_data)
        
        # Performance claim valid if either quality or availability failed
        valid = quality_result['valid'] or availability_result['valid']
        confidence = max(quality_result['confidence'], availability_result['confidence'])
        reasons = quality_result['reasons'] + availability_result['reasons']
        
        return {
            'valid': valid,
            'confidence': confidence,
            'reasons': reasons
        }
    
    async def _investigate_slashing_claim(
        self,
        claim_data: Dict[str, Any],
        policy_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Investigate slashing event claim"""
        # Check on-chain slashing events
        market_addresses = [
            self.quantum_market_address,
            self.ai_market_address,
            self.network_market_address
        ]
        
        slashing_confirmed = False
        confidence = Decimal("1.0")  # On-chain events are definitive
        reasons = []
        
        for market_address in market_addresses:
            market = await self.blockchain.get_contract(market_address, "ComputeMarket")
            
            # Check for slashing events
            slash_events = await market.functions.getSlashingEvents(
                policy_data['policyholder'],
                int(claim_data['incident_data']['timestamp']) - 3600,
                int(claim_data['incident_data']['timestamp']) + 3600
            ).call()
            
            if slash_events:
                slashing_confirmed = True
                break
        
        if not slashing_confirmed:
            confidence = Decimal("0")
            reasons.append("No slashing event found on-chain")
        
        return {
            'valid': slashing_confirmed,
            'confidence': confidence,
            'reasons': reasons
        }
    
    async def _investigate_smart_contract_claim(
        self,
        claim_data: Dict[str, Any],
        policy_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Investigate smart contract exploit claim"""
        incident_data = claim_data['incident_data']
        
        # Check for known exploits
        exploit_confirmed = False
        confidence = Decimal("0.7")  # Requires more investigation
        reasons = []
        
        # Check transaction patterns
        if 'exploit_tx' in incident_data:
            # Verify exploit transaction
            tx_data = await self.blockchain.get_transaction(incident_data['exploit_tx'])
            
            if tx_data and tx_data['to'] in [
                self.quantum_market_address,
                self.ai_market_address,
                self.network_market_address
            ]:
                exploit_confirmed = True
                confidence = Decimal("0.9")
            else:
                reasons.append("Transaction not related to insured contracts")
        
        return {
            'valid': exploit_confirmed,
            'confidence': confidence,
            'reasons': reasons
        }
    
    # Monitoring Methods
    
    async def _monitor_policy(self, policy_id: str):
        """Monitor policy for automatic claim triggers"""
        policy_data = self._active_policies[policy_id]
        
        while policy_data['active'] and datetime.utcnow() < policy_data['end_date']:
            try:
                if policy_data['coverage_type'] == InsuranceCoverageType.QUALITY_DEGRADATION:
                    await self._monitor_quality(policy_id)
                elif policy_data['coverage_type'] == InsuranceCoverageType.AVAILABILITY:
                    await self._monitor_availability(policy_id)
                elif policy_data['coverage_type'] == InsuranceCoverageType.PERFORMANCE_GUARANTEE:
                    await self._monitor_performance(policy_id)
                
                # Check every hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                logger.error(f"Error monitoring policy {policy_id}: {e}")
                await asyncio.sleep(300)  # Retry in 5 minutes
    
    async def _monitor_quality(self, policy_id: str):
        """Monitor resource quality for automatic claims"""
        policy_data = self._active_policies[policy_id]
        
        for resource_id in policy_data['resource_ids']:
            current_quality = await self._get_current_quality(resource_id)
            baseline_quality = await self._get_baseline_quality(resource_id)
            
            degradation = (baseline_quality - current_quality) / baseline_quality
            
            if degradation > Decimal("0.2"):  # 20% degradation threshold
                # Auto-file claim
                await self.file_claim(
                    policy_id,
                    InsuranceCoverageType.QUALITY_DEGRADATION,
                    {
                        'resource_id': resource_id,
                        'baseline_quality': baseline_quality,
                        'current_quality': current_quality,
                        'degradation_percentage': degradation * 100,
                        'timestamp': datetime.utcnow().timestamp(),
                        'auto_triggered': True
                    },
                    policy_data['coverage_amount'] * degradation,
                    []  # Evidence will be on-chain
                )
    
    async def _monitor_availability(self, policy_id: str):
        """Monitor resource availability"""
        policy_data = self._active_policies[policy_id]
        
        for resource_id in policy_data['resource_ids']:
            is_available = await self._check_resource_availability(resource_id)
            
            if not is_available:
                # Start tracking downtime
                if resource_id not in self._monitoring_tasks:
                    self._monitoring_tasks[resource_id] = {
                        'downtime_start': datetime.utcnow(),
                        'policy_id': policy_id
                    }
            else:
                # Check if was down
                if resource_id in self._monitoring_tasks:
                    downtime_data = self._monitoring_tasks[resource_id]
                    downtime_duration = (datetime.utcnow() - downtime_data['downtime_start']).total_seconds()
                    
                    if downtime_duration > 3600:  # More than 1 hour
                        # Auto-file claim
                        await self.file_claim(
                            policy_id,
                            InsuranceCoverageType.AVAILABILITY,
                            {
                                'resource_id': resource_id,
                                'start_time': downtime_data['downtime_start'].timestamp(),
                                'end_time': datetime.utcnow().timestamp(),
                                'duration_hours': downtime_duration / 3600,
                                'auto_triggered': True
                            },
                            policy_data['coverage_amount'] * Decimal(str(downtime_duration / 86400)),  # Pro-rated
                            []
                        )
                    
                    del self._monitoring_tasks[resource_id]
    
    # Helper Methods
    
    async def _calculate_pool_apy(self, pool_data: Dict[str, Any]) -> Decimal:
        """Calculate APY for insurance pool"""
        if pool_data['total_capital'] == 0:
            return Decimal("0")
        
        # Simple APY based on premiums minus claims
        annual_premiums = pool_data['total_premiums_collected']
        annual_claims = pool_data['total_claims_paid']
        
        net_income = annual_premiums - annual_claims
        apy = (net_income / pool_data['total_capital']) * 100
        
        return max(Decimal("0"), apy)  # Can't be negative
    
    async def _get_quality_history(self, resource_id: int) -> List[float]:
        """Get quality score history"""
        # In production, query from oracle
        # Mock data for demonstration
        return [85, 87, 86, 88, 85, 84, 86, 87, 88, 86]
    
    async def _get_availability_history(self, resource_id: int) -> List[float]:
        """Get availability history"""
        # In production, query from monitor
        # Mock data for demonstration
        return [0.99, 0.98, 0.99, 1.0, 0.97, 0.99, 0.98, 0.99, 1.0, 0.99]
    
    async def _get_provider_slash_history(self, provider: str) -> List[Dict]:
        """Get provider slashing history"""
        # In production, query from markets
        return []  # Mock: no slashing history
    
    async def _get_provider_quality_history(self, provider: str) -> List[float]:
        """Get provider quality history"""
        # In production, aggregate from all resources
        return [85, 88, 87, 89, 90, 88, 87, 89, 91, 90]
    
    async def _get_current_quality(self, resource_id: int) -> Decimal:
        """Get current quality score"""
        quality_oracle = await self.blockchain.get_contract(
            self.quality_oracle_address,
            "QualityOracle"
        )
        
        quality = await quality_oracle.functions.getCurrentQuality(resource_id).call()
        return Decimal(str(quality))
    
    async def _get_baseline_quality(self, resource_id: int) -> Decimal:
        """Get baseline quality score"""
        # In production, would be set at policy creation
        return Decimal("90")  # Mock baseline
    
    async def _check_resource_availability(self, resource_id: int) -> bool:
        """Check if resource is currently available"""
        monitor = await self.blockchain.get_contract(
            self.availability_monitor_address,
            "AvailabilityMonitor"
        )
        
        return await monitor.functions.isAvailable(resource_id).call()
    
    async def _update_claim_status(self, claim_id: str, status: ClaimStatus):
        """Update claim status on-chain"""
        claim_data = self._claims[claim_id]
        policy_data = self._active_policies[claim_data['policy_id']]
        pool_data = self._insurance_pools[policy_data['pool_id']]
        
        pool_contract = await self.blockchain.get_contract(
            pool_data['pool_address'],
            "InsurancePool"
        )
        
        status_map = {
            ClaimStatus.APPROVED: 1,
            ClaimStatus.REJECTED: 2,
            ClaimStatus.INVESTIGATING: 3
        }
        
        await pool_contract.functions.updateClaimStatus(
            claim_id,
            status_map.get(status, 0)
        ).transact()
    
    async def _update_risk_profile(self, address: str, claim_data: Dict[str, Any]):
        """Update risk profile after claim"""
        if address not in self._risk_profiles:
            self._risk_profiles[address] = {
                'total_claims': 0,
                'total_amount': Decimal("0"),
                'last_claim': None
            }
        
        profile = self._risk_profiles[address]
        profile['total_claims'] += 1
        profile['total_amount'] += claim_data['claimable_amount']
        profile['last_claim'] = datetime.utcnow() 