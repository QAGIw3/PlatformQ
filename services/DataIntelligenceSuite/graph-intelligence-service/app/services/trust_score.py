from ..db.janusgraph_client import janusgraph_service
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import httpx
import asyncio
import json
import logging
from dataclasses import dataclass
from enum import Enum
from gremlin_python.process.graph_traversal import __
import numpy as np

logger = logging.getLogger(__name__)

# VC Service configuration
VC_SERVICE_URL = "http://verifiable-credential-service:8000"


class CredentialWeight(Enum):
    """Weights for different credential types"""
    PROJECT_COMPLETION = 10
    PEER_REVIEW = 5
    GOVERNANCE_PARTICIPATION = 8
    ASSET_CONTRIBUTION = 6
    SKILL_VERIFICATION = 7
    PROPOSAL_APPROVAL = 12
    MILESTONE_ACHIEVEMENT = 9
    COLLABORATION = 4


@dataclass
class TrustScoreResult:
    """Result of trust score calculation"""
    score: float
    evidence: List[Dict[str, Any]]
    calculated_at: datetime
    valid_until: datetime
    user_did: str
    blockchain_address: Optional[str] = None


class VerifiableTrustScoreService:
    def __init__(self):
        self.g = janusgraph_service.g if janusgraph_service else None
        self.credential_weights = {
            "ProjectCompletionCredential": CredentialWeight.PROJECT_COMPLETION.value,
            "PeerReviewCredential": CredentialWeight.PEER_REVIEW.value,
            "GovernanceParticipationCredential": CredentialWeight.GOVERNANCE_PARTICIPATION.value,
            "AssetContributionCredential": CredentialWeight.ASSET_CONTRIBUTION.value,
            "SkillVerificationCredential": CredentialWeight.SKILL_VERIFICATION.value,
            "ProposalApprovalCredential": CredentialWeight.PROPOSAL_APPROVAL.value,
            "MilestoneAchievementCredential": CredentialWeight.MILESTONE_ACHIEVEMENT.value,
            "CollaborationCredential": CredentialWeight.COLLABORATION.value
        }
        self.time_decay_factor = 0.95  # 5% decay per month
        self.network_effect_multiplier = 1.2

    def add_user_activity(self, user_id: str, activity_type: str, activity_id: str):
        """
        Adds a user activity to the graph.
        """
        if self.g:
            # Create user vertex if it doesn't exist
            user_vertex = self.g.V().has("user", "user_id", user_id).fold().coalesce(
                __.unfold(),
                __.addV("user").property("user_id", user_id)
            ).next()
            
            # Create activity vertex with timestamp
            activity_vertex = self.g.addV(activity_type)\
                .property("activity_id", activity_id)\
                .property("timestamp", datetime.utcnow().isoformat())\
                .next()
            
            # Create edge with timestamp
            self.g.V(user_vertex).addE("participated_in")\
                .to(activity_vertex)\
                .property("timestamp", datetime.utcnow().isoformat())\
                .iterate()

    async def fetch_user_credentials(self, user_did: str) -> List[Dict[str, Any]]:
        """Fetch all VCs for a user from the VC service"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{VC_SERVICE_URL}/api/v1/dids/{user_did}/credentials",
                    timeout=30.0
                )
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Failed to fetch credentials: {response.status_code}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching credentials for {user_did}: {e}")
            return []

    def calculate_vc_value(self, vc: Dict[str, Any]) -> float:
        """Calculate the value of a single VC based on its properties"""
        base_value = 1.0
        
        # Check for specific valuable claims
        subject = vc.get("credentialSubject", {})
        
        # Scale by quantity/amount claims
        if "amount" in subject:
            base_value *= min(subject["amount"] / 100, 5.0)  # Cap at 5x
        
        if "level" in subject:
            base_value *= subject["level"]
            
        if "score" in subject:
            base_value *= subject["score"] / 100
            
        # Bonus for verified claims
        if vc.get("proof", {}).get("blockchainAnchor"):
            base_value *= 1.5
            
        return base_value

    def apply_time_decay(self, score: float, vcs: List[Dict[str, Any]]) -> float:
        """Apply time decay to older credentials"""
        if not vcs:
            return score
            
        # Calculate average age of credentials
        total_age_months = 0
        count = 0
        
        for vc in vcs:
            issuance_date_str = vc.get("issuanceDate", "")
            if issuance_date_str:
                try:
                    issuance_date = datetime.fromisoformat(issuance_date_str.replace("Z", "+00:00"))
                    age = datetime.utcnow() - issuance_date
                    age_months = age.days / 30
                    total_age_months += age_months
                    count += 1
                except:
                    pass
        
        if count > 0:
            avg_age_months = total_age_months / count
            decay_multiplier = self.time_decay_factor ** avg_age_months
            return score * decay_multiplier
            
        return score

    async def apply_trust_network_bonus(self, user_did: str, score: float) -> float:
        """Apply bonus based on trust network connections"""
        if not self.g:
            return score
            
        try:
            # Extract user ID from DID
            user_id = user_did.split(":")[-1]
            
            # Count trusted connections (both directions)
            trusted_connections = self.g.V().has("user", "user_id", user_id)\
                .both("TRUSTS").dedup().count().next()
                
            # Apply network effect bonus (diminishing returns)
            if trusted_connections > 0:
                bonus = min(trusted_connections * 0.02, 0.5)  # Max 50% bonus
                return score * (1 + bonus)
                
        except Exception as e:
            logger.error(f"Error applying network bonus: {e}")
            
        return score

    def calculate_trust_score(self, user_id: str) -> float:
        """
        Legacy method - simple calculation based on activity count
        """
        if self.g:
            score = self.g.V().has("user", "user_id", user_id).outE().count().next()
            return float(score)
        return 0.0

    async def calculate_verifiable_trust_score(self, user_did: str, blockchain_address: Optional[str] = None) -> TrustScoreResult:
        """
        Calculate comprehensive trust score based on verifiable credentials
        """
        # Fetch all VCs for user
        vcs = await self.fetch_user_credentials(user_did)
        
        # Calculate weighted score
        score = 0.0
        evidence = []
        
        for vc in vcs:
            vc_types = vc.get("type", [])
            if isinstance(vc_types, str):
                vc_types = [vc_types]
                
            # Find matching credential type
            for vc_type in vc_types:
                if vc_type in self.credential_weights:
                    weight = self.credential_weights[vc_type]
                    vc_value = self.calculate_vc_value(vc)
                    contribution = weight * vc_value
                    score += contribution
                    
                    evidence.append({
                        "vc_id": vc.get("id", "unknown"),
                        "type": vc_type,
                        "contribution": contribution,
                        "issued_by": vc.get("issuer", "unknown"),
                        "issuance_date": vc.get("issuanceDate", "unknown")
                    })
                    break
        
        # Apply time decay
        score = self.apply_time_decay(score, vcs)
        
        # Apply network effects
        score = await self.apply_trust_network_bonus(user_did, score)
        
        # Normalize score to 0-100 range
        normalized_score = min(score, 100.0)
        
        # Create result
        calculated_at = datetime.utcnow()
        valid_until = calculated_at + timedelta(days=30)
        
        return TrustScoreResult(
            score=normalized_score,
            evidence=evidence,
            calculated_at=calculated_at,
            valid_until=valid_until,
            user_did=user_did,
            blockchain_address=blockchain_address
        )

    def federated_trust_score(self, embeddings):
        return np.mean(embeddings)

    def consume_events(self):
        """
        Consumes events from Pulsar and updates the graph.
        Placeholder for now.
        """
        # TODO: Implement Pulsar consumer for VC-related events
        pass

# Export both legacy and new services
trust_score_service = VerifiableTrustScoreService()
# For backward compatibility
TrustScoreService = VerifiableTrustScoreService 