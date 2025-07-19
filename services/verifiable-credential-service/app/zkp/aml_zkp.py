"""
AML Zero-Knowledge Proof Module
Implements privacy-preserving AML compliance verification
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor

from app.zkp.zkp_manager import ZKPManager
from app.zkp.selective_disclosure import SelectiveDisclosure
from app.did.did_manager import DIDManager
from app.zkp.ignite_zkp_compute import IgniteZKPCompute, ProofTask, ProofResult

logger = logging.getLogger(__name__)


class AMLAttribute(Enum):
    """AML attributes that can be proven"""
    RISK_SCORE_BELOW_THRESHOLD = "risk_score_below_threshold"
    NOT_SANCTIONED = "not_sanctioned"
    RISK_LEVEL_ACCEPTABLE = "risk_level_acceptable"
    TRANSACTION_VOLUME_COMPLIANT = "transaction_volume_compliant"
    NO_HIGH_RISK_COUNTRIES = "no_high_risk_countries"
    BLOCKCHAIN_ANALYTICS_CLEAN = "blockchain_analytics_clean"
    MONITORING_COMPLIANT = "monitoring_compliant"
    LAST_CHECK_RECENT = "last_check_recent"


@dataclass
class AMLProof:
    """Zero-knowledge proof of AML compliance"""
    proof_id: str
    holder_did: str
    attributes_proven: List[AMLAttribute]
    proof_data: Dict[str, Any]
    constraints_satisfied: Dict[str, bool]
    created_at: datetime
    expires_at: datetime
    issuer_signature: str


@dataclass
class AMLCredential:
    """AML Compliance Verifiable Credential"""
    credential_id: str
    holder_did: str
    issuer_did: str
    credential_type: str  # risk_assessment, sanctions_check, monitoring_summary
    attributes: Dict[str, Any]
    issued_at: datetime
    expires_at: datetime
    credential_hash: str


class AMLZeroKnowledgeProof:
    """
    Handles zero-knowledge proofs for AML compliance
    Allows proving compliance without revealing sensitive details
    """
    
    def __init__(
        self,
        zkp_manager: ZKPManager,
        selective_disclosure: SelectiveDisclosure,
        did_manager: DIDManager,
        ignite_zkp_compute: Optional[IgniteZKPCompute] = None
    ):
        self.zkp_manager = zkp_manager
        self.selective_disclosure = selective_disclosure
        self.did_manager = did_manager
        self.ignite_zkp_compute = ignite_zkp_compute
        
        # Thread pool for CPU-intensive operations
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # ZK circuits for AML proofs
        self.circuits = {
            AMLAttribute.RISK_SCORE_BELOW_THRESHOLD: "range_proof_circuit",
            AMLAttribute.NOT_SANCTIONED: "boolean_proof_circuit",
            AMLAttribute.RISK_LEVEL_ACCEPTABLE: "set_membership_circuit",
            AMLAttribute.TRANSACTION_VOLUME_COMPLIANT: "range_proof_circuit",
            AMLAttribute.NO_HIGH_RISK_COUNTRIES: "set_non_membership_circuit",
            AMLAttribute.BLOCKCHAIN_ANALYTICS_CLEAN: "threshold_proof_circuit",
            AMLAttribute.MONITORING_COMPLIANT: "boolean_proof_circuit",
            AMLAttribute.LAST_CHECK_RECENT: "timestamp_range_circuit"
        }
        
        # Proof templates for common AML scenarios
        self.proof_templates = {
            "standard_aml": {
                "description": "Standard AML compliance check",
                "required_attributes": [
                    AMLAttribute.RISK_SCORE_BELOW_THRESHOLD,
                    AMLAttribute.NOT_SANCTIONED,
                    AMLAttribute.LAST_CHECK_RECENT
                ],
                "constraints": {
                    "max_risk_score": 0.7,
                    "check_recency_hours": 24
                }
            },
            "enhanced_aml": {
                "description": "Enhanced due diligence check",
                "required_attributes": [
                    AMLAttribute.RISK_SCORE_BELOW_THRESHOLD,
                    AMLAttribute.NOT_SANCTIONED,
                    AMLAttribute.NO_HIGH_RISK_COUNTRIES,
                    AMLAttribute.BLOCKCHAIN_ANALYTICS_CLEAN,
                    AMLAttribute.LAST_CHECK_RECENT
                ],
                "constraints": {
                    "max_risk_score": 0.5,
                    "min_analytics_score": 0.8,
                    "check_recency_hours": 12
                }
            },
            "transaction_compliance": {
                "description": "Transaction-specific compliance check",
                "required_attributes": [
                    AMLAttribute.TRANSACTION_VOLUME_COMPLIANT,
                    AMLAttribute.MONITORING_COMPLIANT,
                    AMLAttribute.NOT_SANCTIONED
                ],
                "constraints": {
                    "max_daily_volume": 100000
                }
            }
        }
        
        # Cache for proof components
        self._proof_cache = {}
        self._cache_ttl = timedelta(hours=1)
        
    async def generate_aml_proof(
        self,
        holder_did: str,
        credential: AMLCredential,
        attributes_to_prove: List[AMLAttribute],
        constraints: Dict[str, Any],
        verifier_challenge: Optional[str] = None
    ) -> AMLProof:
        """Generate a zero-knowledge proof of AML compliance"""
        
        proof_id = f"aml_proof_{holder_did}_{datetime.utcnow().timestamp()}"
        
        # Check if distributed compute is available
        if self.ignite_zkp_compute and len(attributes_to_prove) > 2:
            # Use distributed proof generation for complex proofs
            return await self._generate_distributed_proof(
                proof_id, holder_did, credential, attributes_to_prove, 
                constraints, verifier_challenge
            )
        
        # Generate proofs for each attribute
        attribute_proofs = {}
        constraints_satisfied = {}
        
        for attribute in attributes_to_prove:
            circuit = self.circuits.get(attribute)
            if not circuit:
                raise ValueError(f"No circuit available for {attribute.value}")
                
            # Generate specific proof based on attribute type
            if attribute == AMLAttribute.RISK_SCORE_BELOW_THRESHOLD:
                proof_data, satisfied = await self._prove_risk_score(
                    credential, constraints.get("max_risk_score", 0.7)
                )
            elif attribute == AMLAttribute.NOT_SANCTIONED:
                proof_data, satisfied = await self._prove_not_sanctioned(credential)
            elif attribute == AMLAttribute.RISK_LEVEL_ACCEPTABLE:
                proof_data, satisfied = await self._prove_risk_level(
                    credential, constraints.get("acceptable_risk_levels", ["LOW", "MEDIUM"])
                )
            elif attribute == AMLAttribute.TRANSACTION_VOLUME_COMPLIANT:
                proof_data, satisfied = await self._prove_volume_compliance(
                    credential, constraints.get("max_daily_volume", 100000)
                )
            elif attribute == AMLAttribute.NO_HIGH_RISK_COUNTRIES:
                proof_data, satisfied = await self._prove_no_high_risk_countries(credential)
            elif attribute == AMLAttribute.BLOCKCHAIN_ANALYTICS_CLEAN:
                proof_data, satisfied = await self._prove_blockchain_analytics(
                    credential, constraints.get("min_analytics_score", 0.8)
                )
            elif attribute == AMLAttribute.MONITORING_COMPLIANT:
                proof_data, satisfied = await self._prove_monitoring_compliant(credential)
            elif attribute == AMLAttribute.LAST_CHECK_RECENT:
                proof_data, satisfied = await self._prove_recent_check(
                    credential, constraints.get("check_recency_hours", 24)
                )
            else:
                raise ValueError(f"Unsupported attribute: {attribute.value}")
                
            attribute_proofs[attribute.value] = proof_data
            constraints_satisfied[attribute.value] = satisfied
            
        # Create proof object
        proof_data = {
            "credential_id": credential.credential_id,
            "attribute_proofs": attribute_proofs,
            "constraints": constraints,
            "timestamp": datetime.utcnow().isoformat(),
            "challenge": verifier_challenge
        }
        
        # Generate signature
        signature = await self._generate_proof_signature(proof_data, holder_did)
        
        proof = AMLProof(
            proof_id=proof_id,
            holder_did=holder_did,
            attributes_proven=attributes_to_prove,
            proof_data=proof_data,
            constraints_satisfied=constraints_satisfied,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=1),
            issuer_signature=signature
        )
        
        logger.info(f"Generated AML proof {proof_id} for {holder_did}")
        return proof
        
    async def _generate_distributed_proof(
        self,
        proof_id: str,
        holder_did: str,
        credential: AMLCredential,
        attributes_to_prove: List[AMLAttribute],
        constraints: Dict[str, Any],
        verifier_challenge: Optional[str] = None
    ) -> AMLProof:
        """Generate proof using distributed compute grid"""
        logger.info(f"Generating distributed proof for {len(attributes_to_prove)} attributes")
        
        # Prepare tasks for parallel processing
        tasks = []
        for attribute in attributes_to_prove:
            circuit = self.circuits.get(attribute)
            if not circuit:
                raise ValueError(f"No circuit available for {attribute.value}")
                
            task_data = {
                "proof_type": "aml_attribute",
                "input_data": {
                    "attribute": attribute.value,
                    "credential_data": credential.attributes,
                    "constraints": self._get_attribute_constraints(attribute, constraints)
                },
                "circuit_name": circuit
            }
            tasks.append(task_data)
            
        # Submit tasks to Ignite compute grid
        task_ids = await self.ignite_zkp_compute.submit_batch_tasks(tasks)
        
        # Get results
        results = await self.ignite_zkp_compute.get_batch_results(task_ids)
        
        # Process results
        attribute_proofs = {}
        constraints_satisfied = {}
        
        for i, (task_id, result) in enumerate(results.items()):
            attribute = attributes_to_prove[i]
            if result.success:
                attribute_proofs[attribute.value] = result.proof_data
                constraints_satisfied[attribute.value] = result.proof_data.get("satisfied", True)
            else:
                logger.error(f"Failed to generate proof for {attribute.value}: {result.error}")
                attribute_proofs[attribute.value] = {"error": result.error}
                constraints_satisfied[attribute.value] = False
                
        # Create composite proof
        proof_data = {
            "credential_id": credential.credential_id,
            "attribute_proofs": attribute_proofs,
            "constraints": constraints,
            "timestamp": datetime.utcnow().isoformat(),
            "challenge": verifier_challenge,
            "distributed": True,
            "compute_stats": {
                "total_tasks": len(task_ids),
                "successful": sum(1 for r in results.values() if r.success),
                "avg_compute_time_ms": sum(r.compute_time_ms for r in results.values()) / len(results)
            }
        }
        
        # Generate signature
        signature = await self._generate_proof_signature(proof_data, holder_did)
        
        proof = AMLProof(
            proof_id=proof_id,
            holder_did=holder_did,
            attributes_proven=attributes_to_prove,
            proof_data=proof_data,
            constraints_satisfied=constraints_satisfied,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=1),
            issuer_signature=signature
        )
        
        logger.info(f"Generated distributed AML proof {proof_id} in {proof_data['compute_stats']['avg_compute_time_ms']:.2f}ms")
        return proof
        
    def _get_attribute_constraints(self, attribute: AMLAttribute, constraints: Dict[str, Any]) -> Dict[str, Any]:
        """Get constraints specific to an attribute"""
        if attribute == AMLAttribute.RISK_SCORE_BELOW_THRESHOLD:
            return {"max_risk_score": constraints.get("max_risk_score", 0.7)}
        elif attribute == AMLAttribute.RISK_LEVEL_ACCEPTABLE:
            return {"acceptable_risk_levels": constraints.get("acceptable_risk_levels", ["LOW", "MEDIUM"])}
        elif attribute == AMLAttribute.TRANSACTION_VOLUME_COMPLIANT:
            return {"max_daily_volume": constraints.get("max_daily_volume", 100000)}
        elif attribute == AMLAttribute.NO_HIGH_RISK_COUNTRIES:
            return {"high_risk_countries": constraints.get("high_risk_countries", [])}
        elif attribute == AMLAttribute.BLOCKCHAIN_ANALYTICS_CLEAN:
            return {"min_analytics_score": constraints.get("min_analytics_score", 0.8)}
        elif attribute == AMLAttribute.LAST_CHECK_RECENT:
            return {"check_recency_hours": constraints.get("check_recency_hours", 24)}
        else:
            return {}
            
    async def generate_batch_proofs(
        self,
        proof_requests: List[Dict[str, Any]]
    ) -> List[AMLProof]:
        """Generate multiple AML proofs in parallel"""
        if not self.ignite_zkp_compute:
            # Fallback to sequential processing
            proofs = []
            for request in proof_requests:
                proof = await self.generate_aml_proof(**request)
                proofs.append(proof)
            return proofs
            
        # Use map-reduce for batch processing
        logger.info(f"Generating {len(proof_requests)} AML proofs in parallel")
        
        # Prepare data chunks
        data_chunks = []
        for request in proof_requests:
            chunk = {
                "holder_did": request["holder_did"],
                "credential_data": request["credential"].attributes,
                "attributes": [attr.value for attr in request["attributes_to_prove"]],
                "constraints": request["constraints"]
            }
            data_chunks.append(chunk)
            
        # Execute map-reduce proof generation
        batch_results = await self.ignite_zkp_compute.execute_map_reduce_proof(
            proof_type="aml_batch",
            data_chunks=data_chunks,
            circuit_name="aml_batch_circuit",
            reduce_func=self._reduce_batch_proofs
        )
        
        # Convert results to AMLProof objects
        proofs = []
        for i, request in enumerate(proof_requests):
            proof_data = batch_results["proofs"][i]
            proof = AMLProof(
                proof_id=proof_data["proof_id"],
                holder_did=request["holder_did"],
                attributes_proven=request["attributes_to_prove"],
                proof_data=proof_data,
                constraints_satisfied=proof_data["constraints_satisfied"],
                created_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(hours=1),
                issuer_signature=proof_data["signature"]
            )
            proofs.append(proof)
            
        return proofs
        
    def _reduce_batch_proofs(self, proof_values: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reduce function for batch proof generation"""
        return {
            "proofs": proof_values,
            "batch_size": len(proof_values),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def cache_proof_components(
        self,
        credential: AMLCredential,
        attributes: List[AMLAttribute]
    ) -> str:
        """Pre-compute and cache proof components for faster generation"""
        cache_key = self._generate_cache_key(credential.credential_id, attributes)
        
        # Check if already cached
        if cache_key in self._proof_cache:
            cached_data = self._proof_cache[cache_key]
            if cached_data["expires_at"] > datetime.utcnow():
                return cache_key
                
        # Generate components in parallel using Ignite
        if self.ignite_zkp_compute:
            components = await self._generate_cached_components_distributed(
                credential, attributes
            )
        else:
            components = await self._generate_cached_components_local(
                credential, attributes
            )
            
        # Cache components
        self._proof_cache[cache_key] = {
            "components": components,
            "expires_at": datetime.utcnow() + self._cache_ttl
        }
        
        logger.info(f"Cached proof components for {len(attributes)} attributes")
        return cache_key
        
    async def _generate_cached_components_distributed(
        self,
        credential: AMLCredential,
        attributes: List[AMLAttribute]
    ) -> Dict[str, Any]:
        """Generate cached components using distributed compute"""
        tasks = []
        for attribute in attributes:
            circuit = self.circuits.get(attribute)
            if circuit:
                task_data = {
                    "proof_type": "aml_component",
                    "input_data": {
                        "attribute": attribute.value,
                        "credential_data": credential.attributes
                    },
                    "circuit_name": f"{circuit}_component"
                }
                tasks.append(task_data)
                
        # Submit tasks
        task_ids = await self.ignite_zkp_compute.submit_batch_tasks(tasks)
        
        # Get results
        results = await self.ignite_zkp_compute.get_batch_results(task_ids)
        
        # Build components map
        components = {}
        for i, (task_id, result) in enumerate(results.items()):
            if result.success:
                attribute = attributes[i]
                components[attribute.value] = result.proof_data
                
        return components
        
    async def _generate_cached_components_local(
        self,
        credential: AMLCredential,
        attributes: List[AMLAttribute]
    ) -> Dict[str, Any]:
        """Generate cached components locally"""
        components = {}
        for attribute in attributes:
            # Generate base components for each attribute
            if attribute == AMLAttribute.RISK_SCORE_BELOW_THRESHOLD:
                components[attribute.value] = {
                    "risk_score": credential.attributes.get("riskScore", 0),
                    "commitment": self._generate_commitment(
                        credential.attributes.get("riskScore", 0)
                    )
                }
            # Add other attributes...
                
        return components
        
    def _generate_cache_key(
        self,
        credential_id: str,
        attributes: List[AMLAttribute]
    ) -> str:
        """Generate cache key for proof components"""
        attr_str = "|".join(sorted([a.value for a in attributes]))
        return hashlib.sha256(f"{credential_id}:{attr_str}".encode()).hexdigest()[:16]
        
    def _generate_commitment(self, value: Any) -> str:
        """Generate cryptographic commitment"""
        # In production, use proper commitment scheme
        return hashlib.sha256(str(value).encode()).hexdigest()
        
    async def verify_aml_proof(
        self,
        proof: AMLProof,
        required_attributes: List[AMLAttribute],
        constraints: Dict[str, Any],
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify a zero-knowledge proof of AML compliance"""
        
        # Check proof expiry
        if datetime.utcnow() > proof.expires_at:
            return {
                "valid": False,
                "reason": "Proof has expired"
            }
            
        # Verify all required attributes are proven
        proven_attributes = set(proof.attributes_proven)
        required_set = set(required_attributes)
        
        if not required_set.issubset(proven_attributes):
            missing = required_set - proven_attributes
            return {
                "valid": False,
                "reason": f"Missing required attributes: {[a.value for a in missing]}"
            }
            
        # Verify constraints are satisfied
        for attribute in required_attributes:
            if not proof.constraints_satisfied.get(attribute.value, False):
                return {
                    "valid": False,
                    "reason": f"Constraint not satisfied for {attribute.value}"
                }
                
        # Verify challenge if provided
        if verifier_challenge and proof.proof_data.get("challenge") != verifier_challenge:
            return {
                "valid": False,
                "reason": "Challenge mismatch"
            }
            
        # Verify signature
        signature_valid = await self._verify_signature(
            proof.proof_data,
            proof.issuer_signature,
            proof.holder_did
        )
        
        if not signature_valid:
            return {
                "valid": False,
                "reason": "Invalid signature"
            }
            
        return {
            "valid": True,
            "attributes_verified": [a.value for a in proof.attributes_proven],
            "constraints_satisfied": proof.constraints_satisfied,
            "verified_at": datetime.utcnow().isoformat()
        }
        
    async def _prove_risk_score(
        self,
        credential: AMLCredential,
        max_score: float
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate range proof for risk score"""
        risk_score = credential.attributes.get("riskScore", 1.0)
        
        # Create range proof
        proof = await self.zkp_manager.create_range_proof(
            value=risk_score,
            min_value=0.0,
            max_value=max_score
        )
        
        satisfied = risk_score <= max_score
        
        return {
            "proof_type": "range_proof",
            "proof": proof,
            "public_inputs": {"max_score": max_score}
        }, satisfied
        
    async def _prove_not_sanctioned(
        self,
        credential: AMLCredential
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate boolean proof for sanctions status"""
        is_sanctioned = credential.attributes.get("isSanctioned", True)
        
        # Create boolean proof
        proof = await self.zkp_manager.create_boolean_proof(
            value=not is_sanctioned
        )
        
        return {
            "proof_type": "boolean_proof",
            "proof": proof,
            "public_inputs": {"expected": False}
        }, not is_sanctioned
        
    async def _prove_risk_level(
        self,
        credential: AMLCredential,
        acceptable_levels: List[str]
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate set membership proof for risk level"""
        risk_level = credential.attributes.get("riskLevel", "HIGH")
        
        # Create set membership proof
        proof = await self.zkp_manager.create_set_membership_proof(
            value=risk_level,
            valid_set=acceptable_levels
        )
        
        satisfied = risk_level in acceptable_levels
        
        return {
            "proof_type": "set_membership_proof",
            "proof": proof,
            "public_inputs": {"acceptable_levels": acceptable_levels}
        }, satisfied
        
    async def _prove_volume_compliance(
        self,
        credential: AMLCredential,
        max_volume: float
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate range proof for transaction volume"""
        total_volume = float(credential.attributes.get("totalVolume", 0))
        
        # Create range proof
        proof = await self.zkp_manager.create_range_proof(
            value=total_volume,
            min_value=0.0,
            max_value=max_volume
        )
        
        satisfied = total_volume <= max_volume
        
        return {
            "proof_type": "range_proof",
            "proof": proof,
            "public_inputs": {"max_volume": max_volume}
        }, satisfied
        
    async def _prove_no_high_risk_countries(
        self,
        credential: AMLCredential
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate proof of no exposure to high-risk countries"""
        high_risk_countries = ["IR", "KP", "SY", "CU", "VE"]
        exposed_countries = credential.attributes.get("exposedCountries", [])
        
        # Check for intersection
        has_high_risk = bool(set(exposed_countries) & set(high_risk_countries))
        
        # Create boolean proof
        proof = await self.zkp_manager.create_boolean_proof(
            value=not has_high_risk
        )
        
        return {
            "proof_type": "set_non_membership_proof",
            "proof": proof,
            "public_inputs": {"high_risk_countries": high_risk_countries}
        }, not has_high_risk
        
    async def _prove_blockchain_analytics(
        self,
        credential: AMLCredential,
        min_score: float
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate proof for blockchain analytics score"""
        analytics_score = credential.attributes.get("analyticsScore", 0.0)
        
        # Create threshold proof
        proof = await self.zkp_manager.create_threshold_proof(
            value=analytics_score,
            threshold=min_score,
            comparison="greater_than_or_equal"
        )
        
        satisfied = analytics_score >= min_score
        
        return {
            "proof_type": "threshold_proof",
            "proof": proof,
            "public_inputs": {"min_score": min_score}
        }, satisfied
        
    async def _prove_monitoring_compliant(
        self,
        credential: AMLCredential
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate proof of monitoring compliance"""
        compliance_status = credential.attributes.get("complianceStatus", "non_compliant")
        
        # Create boolean proof
        proof = await self.zkp_manager.create_boolean_proof(
            value=compliance_status == "compliant"
        )
        
        satisfied = compliance_status == "compliant"
        
        return {
            "proof_type": "boolean_proof",
            "proof": proof,
            "public_inputs": {"expected_status": "compliant"}
        }, satisfied
        
    async def _prove_recent_check(
        self,
        credential: AMLCredential,
        max_hours: int
    ) -> Tuple[Dict[str, Any], bool]:
        """Generate proof that check was done recently"""
        check_date_str = credential.attributes.get("checkDate", "")
        if not check_date_str:
            return {"proof_type": "timestamp_proof", "proof": None}, False
            
        check_date = datetime.fromisoformat(check_date_str.replace('Z', '+00:00'))
        hours_ago = (datetime.utcnow() - check_date).total_seconds() / 3600
        
        # Create range proof for timestamp
        proof = await self.zkp_manager.create_range_proof(
            value=hours_ago,
            min_value=0,
            max_value=max_hours
        )
        
        satisfied = hours_ago <= max_hours
        
        return {
            "proof_type": "timestamp_range_proof",
            "proof": proof,
            "public_inputs": {"max_hours_ago": max_hours}
        }, satisfied
        
    async def _generate_proof_signature(
        self,
        proof_data: Dict[str, Any],
        holder_did: str
    ) -> str:
        """Generate signature for proof"""
        # In production, use proper cryptographic signature
        proof_string = json.dumps(proof_data, sort_keys=True)
        return hashlib.sha256(f"{proof_string}:{holder_did}".encode()).hexdigest()
        
    async def _verify_signature(
        self,
        proof_data: Dict[str, Any],
        signature: str,
        holder_did: str
    ) -> bool:
        """Verify proof signature"""
        expected_signature = await self._generate_proof_signature(proof_data, holder_did)
        return signature == expected_signature 