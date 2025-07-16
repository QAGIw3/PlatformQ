"""
Trust Network and Reputation System for Verifiable Credentials

This module implements a decentralized trust network with reputation scoring,
trust graphs, and credential verification networks.
"""

import asyncio
import json
import math
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import networkx as nx
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class TrustLevel(Enum):
    UNTRUSTED = 0
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    VERIFIED = 4


class CredentialStatus(Enum):
    VALID = "valid"
    REVOKED = "revoked"
    EXPIRED = "expired"
    DISPUTED = "disputed"


@dataclass
class TrustScore:
    """Represents a trust score for an entity"""
    entity_id: str
    score: float  # 0.0 to 1.0
    level: TrustLevel
    factors: Dict[str, float]
    last_updated: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrustRelationship:
    """Represents a trust relationship between two entities"""
    from_entity: str
    to_entity: str
    trust_value: float  # -1.0 to 1.0 (negative for distrust)
    credential_type: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    evidence: List[str] = field(default_factory=list)  # Credential IDs as evidence


@dataclass
class ReputationEvent:
    """An event that affects reputation"""
    entity_id: str
    event_type: str
    impact: float  # Impact on reputation score
    timestamp: datetime
    credential_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class TrustNetwork:
    """
    Manages a decentralized trust network for credential issuers and holders
    """
    
    def __init__(self):
        self.trust_graph = nx.DiGraph()
        self.reputation_scores: Dict[str, TrustScore] = {}
        self.reputation_events: Dict[str, List[ReputationEvent]] = defaultdict(list)
        self.credential_verifications: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
    def add_entity(self, entity_id: str, initial_trust: TrustLevel = TrustLevel.LOW):
        """Add a new entity to the trust network"""
        if entity_id not in self.trust_graph:
            self.trust_graph.add_node(entity_id, trust_level=initial_trust)
            self.reputation_scores[entity_id] = TrustScore(
                entity_id=entity_id,
                score=0.3 if initial_trust == TrustLevel.LOW else 0.5,
                level=initial_trust,
                factors={
                    "base_trust": 0.3,
                    "verification_count": 0.0,
                    "relationship_trust": 0.0,
                    "activity_score": 0.0
                },
                last_updated=datetime.utcnow()
            )
            
    def add_trust_relationship(
        self,
        from_entity: str,
        to_entity: str,
        trust_value: float,
        credential_type: Optional[str] = None,
        evidence: List[str] = None
    ):
        """Add or update a trust relationship between entities"""
        # Ensure entities exist
        self.add_entity(from_entity)
        self.add_entity(to_entity)
        
        # Create relationship
        relationship = TrustRelationship(
            from_entity=from_entity,
            to_entity=to_entity,
            trust_value=max(-1.0, min(1.0, trust_value)),  # Clamp to [-1, 1]
            credential_type=credential_type,
            evidence=evidence or []
        )
        
        # Add to graph
        self.trust_graph.add_edge(
            from_entity,
            to_entity,
            weight=relationship.trust_value,
            relationship=relationship
        )
        
        # Update reputation scores
        self._propagate_trust_update(from_entity, to_entity, trust_value)
        
    def _propagate_trust_update(
        self,
        from_entity: str,
        to_entity: str,
        trust_value: float,
        depth: int = 3
    ):
        """Propagate trust updates through the network"""
        # Use PageRank-like algorithm for trust propagation
        affected_nodes = set()
        
        # BFS to find affected nodes within depth
        queue = [(to_entity, 0)]
        while queue:
            node, current_depth = queue.pop(0)
            if current_depth >= depth:
                continue
                
            affected_nodes.add(node)
            for neighbor in self.trust_graph.successors(node):
                queue.append((neighbor, current_depth + 1))
                
        # Recalculate trust scores for affected nodes
        for node in affected_nodes:
            self._update_reputation_score(node)
            
    def _update_reputation_score(self, entity_id: str):
        """Update reputation score based on multiple factors"""
        if entity_id not in self.reputation_scores:
            return
            
        score = self.reputation_scores[entity_id]
        
        # Factor 1: Incoming trust relationships
        incoming_trust = 0.0
        incoming_count = 0
        for predecessor in self.trust_graph.predecessors(entity_id):
            edge_data = self.trust_graph[predecessor][entity_id]
            trust_value = edge_data['weight']
            predecessor_score = self.reputation_scores.get(predecessor, TrustScore(
                entity_id=predecessor, score=0.5, level=TrustLevel.LOW,
                factors={}, last_updated=datetime.utcnow()
            )).score
            incoming_trust += trust_value * predecessor_score
            incoming_count += 1
            
        relationship_trust = incoming_trust / max(1, incoming_count)
        
        # Factor 2: Verification activity
        recent_verifications = [
            v for v in self.credential_verifications.get(entity_id, [])
            if datetime.utcnow() - v['timestamp'] < timedelta(days=90)
        ]
        verification_score = min(1.0, len(recent_verifications) / 10.0)
        
        # Factor 3: Reputation events
        recent_events = [
            e for e in self.reputation_events.get(entity_id, [])
            if datetime.utcnow() - e.timestamp < timedelta(days=180)
        ]
        event_impact = sum(e.impact for e in recent_events)
        event_score = max(0.0, min(1.0, 0.5 + event_impact))
        
        # Combine factors with weights
        new_score = (
            0.2 * score.factors["base_trust"] +
            0.4 * relationship_trust +
            0.2 * verification_score +
            0.2 * event_score
        )
        
        # Update score
        score.score = new_score
        score.factors.update({
            "relationship_trust": relationship_trust,
            "verification_count": verification_score,
            "activity_score": event_score
        })
        score.last_updated = datetime.utcnow()
        
        # Update trust level
        if new_score >= 0.8:
            score.level = TrustLevel.VERIFIED
        elif new_score >= 0.6:
            score.level = TrustLevel.HIGH
        elif new_score >= 0.4:
            score.level = TrustLevel.MEDIUM
        elif new_score >= 0.2:
            score.level = TrustLevel.LOW
        else:
            score.level = TrustLevel.UNTRUSTED
            
    def record_verification(
        self,
        verifier_id: str,
        credential_id: str,
        issuer_id: str,
        result: bool,
        details: Optional[Dict[str, Any]] = None
    ):
        """Record a credential verification event"""
        verification = {
            "verifier_id": verifier_id,
            "credential_id": credential_id,
            "issuer_id": issuer_id,
            "result": result,
            "timestamp": datetime.utcnow(),
            "details": details or {}
        }
        
        # Store verification
        self.credential_verifications[verifier_id].append(verification)
        self.credential_verifications[issuer_id].append(verification)
        
        # Create reputation event
        impact = 0.01 if result else -0.05  # Negative impact for failed verifications
        for entity_id in [verifier_id, issuer_id]:
            event = ReputationEvent(
                entity_id=entity_id,
                event_type="credential_verification",
                impact=impact,
                timestamp=datetime.utcnow(),
                credential_id=credential_id,
                details={"result": result}
            )
            self.reputation_events[entity_id].append(event)
            
        # Update scores
        self._update_reputation_score(verifier_id)
        self._update_reputation_score(issuer_id)
        
    def get_trust_path(
        self,
        from_entity: str,
        to_entity: str,
        max_length: int = 6
    ) -> Optional[List[str]]:
        """Find a trust path between two entities"""
        try:
            path = nx.shortest_path(
                self.trust_graph,
                from_entity,
                to_entity,
                weight=lambda u, v, d: 1 - d['weight']  # Inverse weight for trust
            )
            if len(path) <= max_length:
                return path
        except nx.NetworkXNoPath:
            pass
        return None
        
    def calculate_transitive_trust(
        self,
        from_entity: str,
        to_entity: str
    ) -> float:
        """Calculate transitive trust between two entities"""
        path = self.get_trust_path(from_entity, to_entity)
        if not path:
            return 0.0
            
        # Multiply trust values along the path
        trust = 1.0
        for i in range(len(path) - 1):
            edge_data = self.trust_graph[path[i]][path[i + 1]]
            trust *= max(0, edge_data['weight'])  # Only positive trust
            
        # Decay based on path length
        decay = 0.9 ** (len(path) - 2)
        return trust * decay
        
    def get_entity_reputation(self, entity_id: str) -> Optional[TrustScore]:
        """Get the current reputation score for an entity"""
        return self.reputation_scores.get(entity_id)
        
    def get_network_statistics(self) -> Dict[str, Any]:
        """Get statistics about the trust network"""
        return {
            "total_entities": self.trust_graph.number_of_nodes(),
            "total_relationships": self.trust_graph.number_of_edges(),
            "average_trust": sum(s.score for s in self.reputation_scores.values()) / max(1, len(self.reputation_scores)),
            "trust_distribution": {
                level.name: sum(1 for s in self.reputation_scores.values() if s.level == level)
                for level in TrustLevel
            },
            "network_density": nx.density(self.trust_graph),
            "average_clustering": nx.average_clustering(self.trust_graph.to_undirected())
        }


class CredentialVerificationNetwork:
    """
    Manages a network of credential verifiers with consensus mechanisms
    """
    
    def __init__(self, trust_network: TrustNetwork):
        self.trust_network = trust_network
        self.verification_requests: Dict[str, Dict[str, Any]] = {}
        self.verification_results: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
    async def request_verification(
        self,
        credential_id: str,
        credential_data: Dict[str, Any],
        requester_id: str,
        required_verifiers: int = 3,
        consensus_threshold: float = 0.66
    ) -> Dict[str, Any]:
        """
        Request verification of a credential from the network
        
        Args:
            credential_id: ID of the credential to verify
            credential_data: The credential data
            requester_id: ID of the entity requesting verification
            required_verifiers: Minimum number of verifiers
            consensus_threshold: Required consensus ratio (0.0 to 1.0)
            
        Returns:
            Verification result with consensus data
        """
        # Create verification request
        request_id = f"ver_req_{credential_id}_{datetime.utcnow().timestamp()}"
        request = {
            "request_id": request_id,
            "credential_id": credential_id,
            "credential_data": credential_data,
            "requester_id": requester_id,
            "required_verifiers": required_verifiers,
            "consensus_threshold": consensus_threshold,
            "created_at": datetime.utcnow(),
            "status": "pending"
        }
        self.verification_requests[request_id] = request
        
        # Select verifiers based on reputation
        verifiers = self._select_verifiers(
            credential_data.get("issuer"),
            required_verifiers * 2  # Request more than required
        )
        
        # Request verification from each verifier
        verification_tasks = []
        for verifier_id in verifiers:
            task = self._request_single_verification(
                verifier_id,
                credential_id,
                credential_data
            )
            verification_tasks.append(task)
            
        # Wait for verifications with timeout
        results = await asyncio.gather(*verification_tasks, return_exceptions=True)
        
        # Process results
        valid_results = []
        for i, result in enumerate(results):
            if not isinstance(result, Exception) and result is not None:
                valid_results.append({
                    "verifier_id": verifiers[i],
                    "result": result,
                    "timestamp": datetime.utcnow()
                })
                self.verification_results[credential_id].append(valid_results[-1])
                
        # Check consensus
        consensus_result = self._calculate_consensus(valid_results, consensus_threshold)
        
        # Update request status
        request["status"] = "completed"
        request["consensus_result"] = consensus_result
        request["completed_at"] = datetime.utcnow()
        
        # Record verifications in trust network
        for result in valid_results:
            self.trust_network.record_verification(
                verifier_id=result["verifier_id"],
                credential_id=credential_id,
                issuer_id=credential_data.get("issuer", "unknown"),
                result=result["result"]["is_valid"],
                details=result["result"]
            )
            
        return consensus_result
        
    def _select_verifiers(
        self,
        issuer_id: Optional[str],
        count: int
    ) -> List[str]:
        """Select verifiers based on reputation and relationships"""
        candidates = []
        
        # Get all entities with high reputation
        for entity_id, score in self.trust_network.reputation_scores.items():
            if score.level in [TrustLevel.HIGH, TrustLevel.VERIFIED]:
                # Calculate relevance score
                relevance = score.score
                
                # Boost if connected to issuer
                if issuer_id and self.trust_network.get_trust_path(entity_id, issuer_id):
                    relevance *= 1.5
                    
                candidates.append((entity_id, relevance))
                
        # Sort by relevance and select top candidates
        candidates.sort(key=lambda x: x[1], reverse=True)
        return [entity_id for entity_id, _ in candidates[:count]]
        
    async def _request_single_verification(
        self,
        verifier_id: str,
        credential_id: str,
        credential_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Request verification from a single verifier (simulated)"""
        # In a real implementation, this would make an API call to the verifier
        # For now, simulate verification based on trust network
        
        await asyncio.sleep(0.1)  # Simulate network delay
        
        # Get verifier reputation
        verifier_rep = self.trust_network.get_entity_reputation(verifier_id)
        if not verifier_rep:
            return None
            
        # Simulate verification logic
        # Higher reputation verifiers are more likely to verify correctly
        is_valid = verifier_rep.score > 0.3  # Simplified logic
        
        return {
            "is_valid": is_valid,
            "confidence": verifier_rep.score,
            "checks_performed": [
                "signature_verification",
                "issuer_verification",
                "schema_validation",
                "revocation_check"
            ],
            "verifier_id": verifier_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    def _calculate_consensus(
        self,
        results: List[Dict[str, Any]],
        threshold: float
    ) -> Dict[str, Any]:
        """Calculate consensus from verification results"""
        if not results:
            return {
                "consensus_reached": False,
                "is_valid": False,
                "confidence": 0.0,
                "verifier_count": 0
            }
            
        # Count valid/invalid votes weighted by verifier reputation
        valid_weight = 0.0
        invalid_weight = 0.0
        
        for result in results:
            verifier_rep = self.trust_network.get_entity_reputation(
                result["verifier_id"]
            )
            weight = verifier_rep.score if verifier_rep else 0.5
            
            if result["result"]["is_valid"]:
                valid_weight += weight
            else:
                invalid_weight += weight
                
        total_weight = valid_weight + invalid_weight
        consensus_ratio = valid_weight / total_weight if total_weight > 0 else 0
        
        return {
            "consensus_reached": consensus_ratio >= threshold or consensus_ratio <= (1 - threshold),
            "is_valid": consensus_ratio >= threshold,
            "confidence": abs(consensus_ratio - 0.5) * 2,  # 0 at 50/50, 1 at 100/0
            "consensus_ratio": consensus_ratio,
            "verifier_count": len(results),
            "weighted_votes": {
                "valid": valid_weight,
                "invalid": invalid_weight
            }
        }
        
    def get_verification_history(
        self,
        credential_id: str
    ) -> List[Dict[str, Any]]:
        """Get verification history for a credential"""
        return self.verification_results.get(credential_id, []) 