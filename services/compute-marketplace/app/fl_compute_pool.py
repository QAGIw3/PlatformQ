"""
Federated Learning Compute Pool Manager

Manages specialized compute offerings for federated learning workloads with
homomorphic encryption support and privacy-preserving features.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import json
import logging
import hashlib
from enum import Enum

import httpx
from pyignite import Client as IgniteClient
import numpy as np

from platformq_shared.event_publisher import EventPublisher
from .models import ComputeOffering, ComputePurchase, ResourceType

logger = logging.getLogger(__name__)


class PrivacyLevel(str, Enum):
    """Privacy levels for FL compute"""
    BASIC = "basic"  # Standard isolation
    ENHANCED = "enhanced"  # With differential privacy
    MAXIMUM = "maximum"  # With HE and TEE


@dataclass
class FLComputeCapabilities:
    """Federated learning specific compute capabilities"""
    supports_homomorphic_encryption: bool = False
    he_schemes: List[str] = field(default_factory=list)  # CKKS, Paillier, etc.
    sgx_enabled: bool = False
    tee_attestation: Optional[str] = None
    differential_privacy_enabled: bool = True
    secure_aggregation_supported: bool = True
    max_fl_participants: int = 100
    supported_ml_frameworks: List[str] = field(default_factory=list)
    network_bandwidth_mbps: float = 1000.0
    p2p_enabled: bool = False
    gpu_memory_gb: Optional[float] = None
    cuda_capability: Optional[str] = None


@dataclass
class FLSession:
    """Active federated learning session"""
    session_id: str
    model_type: str
    min_participants: int
    max_participants: int
    privacy_config: Dict[str, Any]
    current_participants: Set[str] = field(default_factory=set)
    compute_providers: Set[str] = field(default_factory=set)
    status: str = "waiting"
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    current_round: int = 0
    total_rounds: int = 10


class FederatedLearningComputePool:
    """
    Manages compute resources specifically optimized for federated learning.
    Integrates with FL service to auto-provision compute for sessions.
    """
    
    def __init__(
        self,
        fl_service_url: str,
        ignite_client: IgniteClient,
        event_publisher: EventPublisher,
        vc_service_url: str,
        neuromorphic_url: Optional[str] = None
    ):
        self.fl_service_url = fl_service_url
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.vc_service_url = vc_service_url
        self.neuromorphic_url = neuromorphic_url
        
        # HTTP client
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Active FL sessions
        self.active_sessions: Dict[str, FLSession] = {}
        
        # FL-optimized compute providers
        self.fl_providers: Dict[str, FLComputeCapabilities] = {}
        
        # Session monitoring task
        self.monitor_task = None
        
    async def initialize(self):
        """Initialize FL compute pool"""
        # Start monitoring FL sessions
        self.monitor_task = asyncio.create_task(self._monitor_fl_sessions())
        
        # Load FL-capable providers
        await self._load_fl_providers()
        
        logger.info("Federated Learning Compute Pool initialized")
    
    async def create_fl_compute_offering(
        self,
        provider_id: str,
        base_offering: ComputeOffering,
        fl_capabilities: FLComputeCapabilities,
        premium_percentage: float = 20.0  # FL compute commands premium
    ) -> Dict[str, Any]:
        """
        Create specialized FL compute offering with privacy features.
        
        Args:
            provider_id: Provider ID
            base_offering: Base compute offering
            fl_capabilities: FL-specific capabilities
            premium_percentage: Premium pricing for FL features
            
        Returns:
            FL compute offering details
        """
        try:
            # Generate FL-specific offering ID
            fl_offering_id = f"fl_{base_offering.offering_id}"
            
            # Calculate FL pricing (premium for privacy features)
            base_price = base_offering.price_per_hour
            fl_price = base_price * (1 + premium_percentage / 100)
            
            # Add more premium for advanced features
            if fl_capabilities.supports_homomorphic_encryption:
                fl_price *= 1.3  # 30% extra for HE
            if fl_capabilities.sgx_enabled:
                fl_price *= 1.2  # 20% extra for SGX
            
            # Create FL offering
            fl_offering_data = {
                "offering_id": fl_offering_id,
                "base_offering_id": base_offering.offering_id,
                "provider_id": provider_id,
                "tenant_id": base_offering.tenant_id,
                "resource_type": base_offering.resource_type.value,
                "resource_specs": {
                    **base_offering.resource_specs,
                    "fl_optimized": True,
                    "privacy_level": self._determine_privacy_level(fl_capabilities),
                    "fl_capabilities": fl_capabilities.__dict__
                },
                "location": base_offering.location,
                "price_per_hour": fl_price,
                "min_duration_minutes": 60,  # Minimum 1 hour for FL
                "tags": base_offering.tags + ["federated_learning", "privacy_preserving"],
                "status": "active",
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Save to Ignite
            cache = self.ignite.get_cache("fl_compute_offerings")
            cache.put(fl_offering_id, fl_offering_data)
            
            # Register capabilities
            self.fl_providers[provider_id] = fl_capabilities
            
            # Issue verifiable credential for FL capabilities
            vc = await self._issue_fl_capability_credential(
                provider_id,
                fl_capabilities
            )
            
            # Emit FL offering created event
            await self.event_publisher.publish_event({
                "event_type": "fl_compute_offering_created",
                "offering_id": fl_offering_id,
                "provider_id": provider_id,
                "privacy_level": fl_offering_data["resource_specs"]["privacy_level"],
                "capabilities": fl_capabilities.__dict__,
                "credential_id": vc["id"],
                "timestamp": datetime.utcnow().isoformat()
            }, "persistent://public/default/fl-compute-events")
            
            return {
                "success": True,
                "offering_id": fl_offering_id,
                "privacy_level": fl_offering_data["resource_specs"]["privacy_level"],
                "price_per_hour": fl_price,
                "credential": vc
            }
            
        except Exception as e:
            logger.error(f"Failed to create FL compute offering: {e}")
            raise
    
    async def join_fl_session(
        self,
        session_id: str,
        compute_provider_id: str,
        allocated_resources: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Join compute provider to FL session as infrastructure participant.
        
        Args:
            session_id: FL session ID
            compute_provider_id: Provider offering compute
            allocated_resources: Resources allocated for session
            
        Returns:
            Join confirmation details
        """
        try:
            # Get FL session details
            session = await self._get_fl_session(session_id)
            if not session:
                raise ValueError(f"FL session {session_id} not found")
            
            # Verify provider capabilities
            capabilities = self.fl_providers.get(compute_provider_id)
            if not capabilities:
                raise ValueError(f"Provider {compute_provider_id} not FL-capable")
            
            # Check privacy requirements
            if not await self._verify_privacy_requirements(
                session.privacy_config,
                capabilities
            ):
                raise ValueError("Provider doesn't meet privacy requirements")
            
            # Allocate compute for FL participant
            allocation = await self._allocate_fl_compute(
                session_id,
                compute_provider_id,
                allocated_resources,
                session.privacy_config
            )
            
            # Register with FL service
            response = await self.http_client.post(
                f"{self.fl_service_url}/api/v1/sessions/{session_id}/compute-providers",
                json={
                    "provider_id": compute_provider_id,
                    "compute_allocation": allocation,
                    "capabilities": capabilities.__dict__,
                    "connection_info": {
                        "endpoint": f"grpc://{compute_provider_id}.compute.platformq.io:50051",
                        "public_key": await self._get_provider_public_key(compute_provider_id)
                    }
                }
            )
            response.raise_for_status()
            
            # Update session tracking
            if session_id in self.active_sessions:
                self.active_sessions[session_id].compute_providers.add(compute_provider_id)
            
            # Emit event
            await self.event_publisher.publish_event({
                "event_type": "fl_compute_provider_joined",
                "session_id": session_id,
                "provider_id": compute_provider_id,
                "allocation": allocation,
                "timestamp": datetime.utcnow().isoformat()
            }, "persistent://public/default/fl-compute-events")
            
            return {
                "success": True,
                "session_id": session_id,
                "allocation_id": allocation["allocation_id"],
                "status": "active"
            }
            
        except Exception as e:
            logger.error(f"Failed to join FL session: {e}")
            raise
    
    async def optimize_fl_placement(
        self,
        session_requirements: Dict[str, Any],
        participant_locations: List[str]
    ) -> List[Dict[str, Any]]:
        """
        Optimize compute placement for FL session using neuromorphic matching.
        
        Args:
            session_requirements: FL session compute requirements
            participant_locations: Geographic locations of participants
            
        Returns:
            Optimized compute placements
        """
        if not self.neuromorphic_url:
            # Fallback to simple placement
            return await self._simple_fl_placement(
                session_requirements,
                participant_locations
            )
        
        try:
            # Prepare optimization request for neuromorphic service
            optimization_request = {
                "problem_type": "fl_compute_placement",
                "requirements": {
                    "privacy_level": session_requirements.get("privacy_level", "enhanced"),
                    "total_participants": session_requirements.get("participants", 10),
                    "compute_per_participant": session_requirements.get("compute_per_participant", {}),
                    "network_requirements": session_requirements.get("network_requirements", {})
                },
                "constraints": {
                    "geographic_distribution": participant_locations,
                    "latency_requirements": session_requirements.get("max_latency_ms", 100),
                    "bandwidth_requirements": session_requirements.get("min_bandwidth_mbps", 100)
                },
                "optimization_targets": [
                    "minimize_latency",
                    "maximize_privacy",
                    "minimize_cost"
                ]
            }
            
            # Submit to neuromorphic optimizer
            response = await self.http_client.post(
                f"{self.neuromorphic_url}/api/v1/optimize/placement",
                json=optimization_request
            )
            response.raise_for_status()
            
            result = response.json()
            placements = result["placements"]
            
            # Enhance with FL-specific details
            enhanced_placements = []
            for placement in placements:
                provider_id = placement["provider_id"]
                capabilities = self.fl_providers.get(provider_id)
                
                if capabilities:
                    enhanced_placements.append({
                        **placement,
                        "fl_capabilities": capabilities.__dict__,
                        "privacy_score": self._calculate_privacy_score(capabilities),
                        "estimated_round_time": self._estimate_round_time(
                            capabilities,
                            session_requirements
                        )
                    })
            
            return enhanced_placements
            
        except Exception as e:
            logger.error(f"FL placement optimization failed: {e}")
            return await self._simple_fl_placement(
                session_requirements,
                participant_locations
            )
    
    async def create_fl_data_room(
        self,
        session_id: str,
        participants: List[str],
        privacy_requirements: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Create secure data room for FL session with end-to-end encryption.
        
        Args:
            session_id: FL session ID
            participants: List of participant IDs
            privacy_requirements: Privacy and security requirements
            
        Returns:
            Data room configuration
        """
        try:
            # Generate encryption keys
            data_room_id = f"dr_{session_id}"
            master_key = hashlib.sha256(
                f"{session_id}:{datetime.utcnow().isoformat()}".encode()
            ).hexdigest()
            
            # Configure based on privacy level
            if privacy_requirements.get("homomorphic_encryption"):
                encryption_config = {
                    "type": "homomorphic",
                    "scheme": privacy_requirements.get("encryption_scheme", "CKKS"),
                    "parameters": self._generate_he_parameters(privacy_requirements)
                }
            else:
                encryption_config = {
                    "type": "symmetric",
                    "algorithm": "AES-256-GCM",
                    "key_derivation": "PBKDF2"
                }
            
            # Create data room configuration
            data_room_config = {
                "data_room_id": data_room_id,
                "session_id": session_id,
                "encryption_config": encryption_config,
                "access_control": {
                    "participants": participants,
                    "permissions": self._generate_permissions(participants),
                    "audit_enabled": True
                },
                "storage": {
                    "provider": "minio",
                    "bucket": f"fl-session-{session_id}",
                    "encryption_at_rest": True
                },
                "networking": {
                    "protocol": "grpc+tls",
                    "compression": "zstd",
                    "max_message_size": 100 * 1024 * 1024  # 100MB
                }
            }
            
            # Save configuration
            cache = self.ignite.get_cache("fl_data_rooms")
            cache.put(data_room_id, data_room_config)
            
            # Issue access credentials for participants
            access_credentials = {}
            for participant in participants:
                credential = await self._issue_data_room_credential(
                    participant,
                    data_room_id,
                    encryption_config
                )
                access_credentials[participant] = credential
            
            return {
                "data_room_id": data_room_id,
                "encryption_type": encryption_config["type"],
                "access_credentials": access_credentials,
                "storage_location": data_room_config["storage"]["bucket"]
            }
            
        except Exception as e:
            logger.error(f"Failed to create FL data room: {e}")
            raise
    
    def _determine_privacy_level(self, capabilities: FLComputeCapabilities) -> str:
        """Determine privacy level based on capabilities"""
        if capabilities.supports_homomorphic_encryption and capabilities.sgx_enabled:
            return PrivacyLevel.MAXIMUM.value
        elif capabilities.differential_privacy_enabled and capabilities.secure_aggregation_supported:
            return PrivacyLevel.ENHANCED.value
        else:
            return PrivacyLevel.BASIC.value
    
    async def _verify_privacy_requirements(
        self,
        requirements: Dict[str, Any],
        capabilities: FLComputeCapabilities
    ) -> bool:
        """Verify provider meets privacy requirements"""
        # Check HE support
        if requirements.get("homomorphic_encryption") and not capabilities.supports_homomorphic_encryption:
            return False
        
        # Check specific HE scheme
        if requirements.get("encryption_scheme"):
            if requirements["encryption_scheme"] not in capabilities.he_schemes:
                return False
        
        # Check TEE support
        if requirements.get("trusted_execution") and not capabilities.sgx_enabled:
            return False
        
        # Check DP support
        if requirements.get("differential_privacy_enabled") and not capabilities.differential_privacy_enabled:
            return False
        
        return True
    
    async def _allocate_fl_compute(
        self,
        session_id: str,
        provider_id: str,
        resources: Dict[str, Any],
        privacy_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Allocate compute resources for FL session"""
        allocation_id = f"fl_alloc_{session_id}_{provider_id}"
        
        # Configure based on privacy requirements
        allocation = {
            "allocation_id": allocation_id,
            "session_id": session_id,
            "provider_id": provider_id,
            "resources": resources,
            "isolation_type": "container" if privacy_config.get("basic") else "vm",
            "network_isolation": True,
            "encrypted_memory": privacy_config.get("encrypted_memory", False),
            "secure_channels": True,
            "allocated_at": datetime.utcnow().isoformat()
        }
        
        # Save allocation
        cache = self.ignite.get_cache("fl_compute_allocations")
        cache.put(allocation_id, allocation)
        
        return allocation
    
    async def _get_fl_session(self, session_id: str) -> Optional[FLSession]:
        """Get FL session details"""
        # Check local cache first
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
        
        # Query FL service
        try:
            response = await self.http_client.get(
                f"{self.fl_service_url}/api/v1/sessions/{session_id}"
            )
            if response.status_code == 200:
                data = response.json()
                session = FLSession(
                    session_id=session_id,
                    model_type=data["model_type"],
                    min_participants=data["min_participants"],
                    max_participants=data["max_participants"],
                    privacy_config=data["privacy_parameters"],
                    status=data["status"],
                    current_round=data.get("current_round", 0),
                    total_rounds=data.get("total_rounds", 10)
                )
                self.active_sessions[session_id] = session
                return session
                
        except Exception as e:
            logger.error(f"Failed to get FL session: {e}")
            
        return None
    
    async def _monitor_fl_sessions(self):
        """Monitor active FL sessions and auto-provision compute"""
        while True:
            try:
                # Get active FL sessions from service
                response = await self.http_client.get(
                    f"{self.fl_service_url}/api/v1/sessions",
                    params={"status": "waiting_for_participants"}
                )
                
                if response.status_code == 200:
                    sessions = response.json()
                    
                    for session_data in sessions:
                        session_id = session_data["session_id"]
                        
                        # Check if we should auto-provision compute
                        if await self._should_provision_compute(session_data):
                            await self._auto_provision_compute(session_data)
                
                # Sleep before next check
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Error monitoring FL sessions: {e}")
                await asyncio.sleep(60)
    
    async def _should_provision_compute(self, session_data: Dict[str, Any]) -> bool:
        """Determine if compute should be auto-provisioned"""
        # Check if session needs more compute providers
        current_providers = len(session_data.get("compute_providers", []))
        min_providers = max(
            1,
            session_data.get("min_participants", 3) // 3  # 1 provider per 3 participants
        )
        
        return current_providers < min_providers
    
    async def _auto_provision_compute(self, session_data: Dict[str, Any]):
        """Auto-provision compute for FL session"""
        try:
            session_id = session_data["session_id"]
            requirements = session_data["privacy_parameters"]
            
            # Find suitable providers
            suitable_providers = await self._find_suitable_providers(requirements)
            
            if suitable_providers:
                # Pick best provider
                provider = suitable_providers[0]
                
                # Allocate resources
                resources = {
                    "vcpus": 4,
                    "memory_gb": 16,
                    "gpu_count": 1 if requirements.get("gpu_required") else 0
                }
                
                # Join session
                await self.join_fl_session(
                    session_id,
                    provider["provider_id"],
                    resources
                )
                
                logger.info(f"Auto-provisioned compute for FL session {session_id}")
                
        except Exception as e:
            logger.error(f"Failed to auto-provision compute: {e}")
    
    async def _find_suitable_providers(
        self,
        requirements: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Find providers that meet FL requirements"""
        suitable = []
        
        for provider_id, capabilities in self.fl_providers.items():
            if await self._verify_privacy_requirements(requirements, capabilities):
                privacy_score = self._calculate_privacy_score(capabilities)
                suitable.append({
                    "provider_id": provider_id,
                    "capabilities": capabilities,
                    "privacy_score": privacy_score
                })
        
        # Sort by privacy score
        suitable.sort(key=lambda x: x["privacy_score"], reverse=True)
        
        return suitable
    
    def _calculate_privacy_score(self, capabilities: FLComputeCapabilities) -> float:
        """Calculate privacy score for capabilities"""
        score = 0.0
        
        if capabilities.supports_homomorphic_encryption:
            score += 30.0
        if capabilities.sgx_enabled:
            score += 25.0
        if capabilities.differential_privacy_enabled:
            score += 20.0
        if capabilities.secure_aggregation_supported:
            score += 15.0
        if capabilities.p2p_enabled:
            score += 10.0
            
        return min(score, 100.0)
    
    def _estimate_round_time(
        self,
        capabilities: FLComputeCapabilities,
        requirements: Dict[str, Any]
    ) -> float:
        """Estimate FL round completion time in seconds"""
        base_time = 60.0  # Base 1 minute per round
        
        # Adjust for encryption overhead
        if requirements.get("homomorphic_encryption"):
            base_time *= 3.0  # HE adds significant overhead
        elif requirements.get("differential_privacy_enabled"):
            base_time *= 1.5
        
        # Adjust for network speed
        bandwidth_factor = 1000.0 / capabilities.network_bandwidth_mbps
        base_time *= bandwidth_factor
        
        # Adjust for GPU acceleration
        if capabilities.gpu_memory_gb and requirements.get("gpu_required"):
            base_time *= 0.5  # GPU speeds up training
        
        return base_time
    
    async def _simple_fl_placement(
        self,
        requirements: Dict[str, Any],
        locations: List[str]
    ) -> List[Dict[str, Any]]:
        """Simple placement strategy when neuromorphic not available"""
        placements = []
        
        # Find providers in preferred locations
        for location in locations[:3]:  # Top 3 locations
            providers = await self._find_providers_in_location(location)
            
            for provider in providers[:2]:  # 2 providers per location
                if provider["provider_id"] in self.fl_providers:
                    placements.append({
                        "provider_id": provider["provider_id"],
                        "location": location,
                        "score": 0.8  # Default good score
                    })
        
        return placements
    
    async def _find_providers_in_location(self, location: str) -> List[Dict[str, Any]]:
        """Find FL providers in specific location"""
        providers = []
        
        cache = self.ignite.get_cache("fl_compute_offerings")
        # In production, use proper Ignite query
        # This is simplified
        
        for provider_id in self.fl_providers:
            # Mock check - in production query actual location
            providers.append({
                "provider_id": provider_id,
                "location": location
            })
        
        return providers
    
    def _generate_he_parameters(self, requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Generate homomorphic encryption parameters"""
        scheme = requirements.get("encryption_scheme", "CKKS")
        
        if scheme == "CKKS":
            return {
                "poly_modulus_degree": 16384,
                "coeff_modulus": [60, 40, 40, 60],
                "scale": 2**40,
                "security_level": 128
            }
        elif scheme == "Paillier":
            return {
                "key_size": 2048,
                "certainty": 64
            }
        else:
            return {}
    
    def _generate_permissions(self, participants: List[str]) -> Dict[str, List[str]]:
        """Generate data room permissions"""
        permissions = {}
        
        for participant in participants:
            permissions[participant] = [
                "read_model",
                "submit_update",
                "view_metrics"
            ]
        
        return permissions
    
    async def _issue_fl_capability_credential(
        self,
        provider_id: str,
        capabilities: FLComputeCapabilities
    ) -> Dict[str, Any]:
        """Issue verifiable credential for FL capabilities"""
        credential_request = {
            "type": ["VerifiableCredential", "FLComputeCapabilityCredential"],
            "credentialSubject": {
                "provider_id": provider_id,
                "privacy_level": self._determine_privacy_level(capabilities),
                "capabilities": {
                    "homomorphic_encryption": capabilities.supports_homomorphic_encryption,
                    "he_schemes": capabilities.he_schemes,
                    "trusted_execution": capabilities.sgx_enabled,
                    "differential_privacy": capabilities.differential_privacy_enabled,
                    "secure_aggregation": capabilities.secure_aggregation_supported,
                    "max_participants": capabilities.max_fl_participants
                },
                "issued_at": datetime.utcnow().isoformat()
            },
            "expirationDate": (datetime.utcnow() + timedelta(days=180)).isoformat()
        }
        
        response = await self.http_client.post(
            f"{self.vc_service_url}/api/v1/credentials/issue",
            json=credential_request
        )
        response.raise_for_status()
        
        return response.json()
    
    async def _issue_data_room_credential(
        self,
        participant_id: str,
        data_room_id: str,
        encryption_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Issue credential for data room access"""
        credential_request = {
            "type": ["VerifiableCredential", "DataRoomAccessCredential"],
            "credentialSubject": {
                "participant_id": participant_id,
                "data_room_id": data_room_id,
                "permissions": ["read_model", "submit_update", "view_metrics"],
                "encryption_type": encryption_config["type"],
                "valid_until": (datetime.utcnow() + timedelta(days=7)).isoformat()
            }
        }
        
        response = await self.http_client.post(
            f"{self.vc_service_url}/api/v1/credentials/issue",
            json=credential_request
        )
        response.raise_for_status()
        
        return response.json()
    
    async def _get_provider_public_key(self, provider_id: str) -> str:
        """Get provider's public key for secure communication"""
        # In production, retrieve from key management service
        # This is a placeholder
        return f"-----BEGIN PUBLIC KEY-----\n{provider_id}_public_key\n-----END PUBLIC KEY-----"
    
    async def _load_fl_providers(self):
        """Load FL-capable providers from cache"""
        try:
            cache = self.ignite.get_cache("fl_providers")
            # In production, use proper Ignite query
            # This is simplified
            logger.info(f"Loaded {len(self.fl_providers)} FL providers")
        except Exception as e:
            logger.error(f"Failed to load FL providers: {e}")
    
    async def close(self):
        """Cleanup resources"""
        if self.monitor_task:
            self.monitor_task.cancel()
        await self.http_client.aclose()