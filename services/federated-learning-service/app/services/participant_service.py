import logging
import time
from typing import Dict, Any, List, Optional
import httpx
import json
from pyignite import Client as IgniteClient
from datetime import datetime

from ..core.config import settings

logger = logging.getLogger(__name__)

class ParticipantService:
    def __init__(self, ignite_client: IgniteClient, http_client: httpx.AsyncClient):
        self.ignite_client = ignite_client
        self.http_client = http_client

    async def join_session(self, session_id: str, request: Dict[str, Any], tenant_id: str, user_id: str) -> Dict[str, Any]:
        participant_id = f"{tenant_id}:{user_id}"
        
        session_data = self._get_session(session_id)
        if not session_data:
            raise ValueError("Session not found")

        if session_data["status"] not in ["WAITING_FOR_PARTICIPANTS"]:
            raise ValueError("Session is not accepting participants")

        dq_requirements = session_data.get("data_quality_requirements", {})
        min_score = dq_requirements.get("min_overall_score", 0.0)

        quality_profile = await self.profile_participant_data(
            request.dataset_uri, tenant_id, session_id
        )

        if not quality_profile:
            raise ConnectionError("Data quality service unavailable or profiling failed.")

        overall_score = quality_profile.get("overall_score", 0.0)
        if overall_score < min_score:
            raise PermissionError(f"Data quality score {overall_score:.2f} is below the required minimum of {min_score:.2f}.")

        criteria = session_data["participation_criteria"]
        
        required_credentials = criteria.get("required_credentials", [])
        if required_credentials:
            verified_creds = await self.verify_participant_credentials(
                user_id,
                tenant_id,
                required_credentials
            )
            
            if len(verified_creds) < len(required_credentials):
                raise PermissionError("Missing required credentials for participation")
        
        min_reputation = criteria.get("min_reputation_score")
        if min_reputation is not None:
            reputation = await self.get_user_reputation(user_id, tenant_id)
            if reputation is None or reputation < min_reputation:
                raise PermissionError(f"Reputation score {reputation} is below required {min_reputation}")
        
        allowed_tenants = criteria.get("allowed_tenants")
        if allowed_tenants and tenant_id not in allowed_tenants:
            raise PermissionError("Tenant not allowed in this session")

        participant_data = {
            "participant_id": participant_id,
            "tenant_id": tenant_id,
            "joined_at": datetime.utcnow().isoformat(),
            "public_key": request.public_key,
            "dataset_stats": request.dataset_stats,
            "compute_capabilities": request.compute_capabilities,
            "status": "APPROVED",
            "data_quality_score": overall_score
        }
        
        self.add_participant_to_ignite(session_id, participant_data)
        
        session_data["participants"].append(participant_id)
        self._update_session(session_id, {"participants": session_data["participants"]})
        
        return {
            "status": "APPROVED",
            "session_id": session_id,
            "public_key": session_data.get("homomorphic_encryption", {}).get("public_key")
        }

    async def verify_participant_credentials(self, participant_id: str, tenant_id: str, required_credentials: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        verified_credentials = []
        
        for req_cred in required_credentials:
            try:
                response = await self.http_client.get(
                    f"{settings.vc_service_url}/api/v1/dids/{participant_id}/credentials",
                    params={"credential_type": req_cred["credential_type"]},
                    headers={"X-Tenant-ID": tenant_id}
                )
                
                if response.status_code == 200:
                    creds = response.json()
                    
                    for cred in creds:
                        verify_response = await self.http_client.post(
                            f"{settings.vc_service_url}/api/v1/verify",
                            json={"credential": cred},
                            headers={"X-Tenant-ID": tenant_id}
                        )
                        
                        if verify_response.status_code == 200:
                            result = verify_response.json()
                            if result.get("verified", False):
                                if req_cred.get("issuer") and cred.get("issuer") != req_cred["issuer"]:
                                    continue
                                
                                if req_cred.get("min_trust_score"):
                                    trust_score = cred.get("credentialSubject", {}).get("trustScore", 0)
                                    if trust_score < req_cred["min_trust_score"]:
                                        continue
                                
                                verified_credentials.append({
                                    "credential_id": cred.get("id"),
                                    "credential_type": req_cred["credential_type"],
                                    "issuer": cred.get("issuer"),
                                    "verification_timestamp": int(time.time() * 1000)
                                })
                                break
                
            except Exception as e:
                logger.error(f"Failed to verify credential {req_cred}: {e}")
        
        return verified_credentials

    async def get_user_reputation(self, user_id: str, tenant_id: str) -> Optional[int]:
        try:
            response = await self.http_client.get(
                f"{settings.auth_service_url}/api/v1/users/{user_id}/reputation",
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 200:
                return response.json().get("reputation_score", 0)
        except Exception as e:
            logger.error(f"Failed to get reputation score: {e}")
        
        return None

    async def profile_participant_data(self, dataset_uri: str, tenant_id: str, session_id: str) -> Optional[Dict[str, Any]]:
        try:
            response = await self.http_client.post(
                f"{settings.data_lake_service_url}/api/v1/profiles",
                json={
                    "dataset_uri": dataset_uri,
                    "profile_type": "federated_participant",
                    "metadata": {
                        "fl_session_id": session_id,
                        "tenant_id": tenant_id
                    }
                },
                headers={"X-Tenant-ID": tenant_id},
                timeout=300.0
            )

            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Failed to profile dataset {dataset_uri}: {e}")

        return None

    def add_participant_to_ignite(self, session_id: str, participant_data: Dict[str, Any]):
        cache = self.ignite_client.get_or_create_cache("fl_participants")
        key = f"{session_id}:{participant_data['participant_id']}"
        cache.put(key, json.dumps(participant_data))

    def _get_session(self, session_id: str) -> Dict[str, Any]:
        cache = self.ignite_client.get_cache("fl_sessions")
        session_json = cache.get(session_id)
        if not session_json:
            return None
        return json.loads(session_json)

    def _update_session(self, session_id: str, updates: Dict[str, Any]):
        cache = self.ignite_client.get_cache("fl_sessions")
        session_data = json.loads(cache.get(session_id))
        session_data.update(updates)
        cache.put(session_id, json.dumps(session_data)) 