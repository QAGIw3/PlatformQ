"""
Verifiable Credentials for Workflows

Implements W3C Verifiable Credentials for workflow execution,
attestations, and cross-organizational trust.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import uuid

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
import jwt
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class CredentialType(Enum):
    """Types of workflow credentials"""
    WORKFLOW_COMPLETION = "WorkflowCompletionCredential"
    TASK_ATTESTATION = "TaskAttestationCredential"
    RESOURCE_AUTHORIZATION = "ResourceAuthorizationCredential"
    QUALITY_CERTIFICATION = "QualityCertificationCredential"
    COMPLIANCE_VERIFICATION = "ComplianceVerificationCredential"


class CredentialStatus(Enum):
    """Credential status"""
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class VerifiableCredential(BaseModel):
    """W3C Verifiable Credential model"""
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1"])
    id: str
    type: List[str]
    issuer: Dict[str, Any]
    issuance_date: datetime
    expiration_date: Optional[datetime] = None
    credential_subject: Dict[str, Any]
    proof: Optional[Dict[str, Any]] = None
    credential_status: Optional[Dict[str, Any]] = None


class VerifiablePresentation(BaseModel):
    """W3C Verifiable Presentation model"""
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1"])
    type: List[str] = Field(default=["VerifiablePresentation"])
    verifiable_credential: List[VerifiableCredential]
    proof: Optional[Dict[str, Any]] = None


class WorkflowCredentialManager:
    """Manages verifiable credentials for workflows"""
    
    def __init__(self,
                 issuer_did: str,
                 issuer_name: str,
                 private_key_path: Optional[str] = None):
        self.issuer_did = issuer_did
        self.issuer_name = issuer_name
        
        # Initialize cryptographic keys
        if private_key_path:
            with open(private_key_path, 'rb') as f:
                self.private_key = serialization.load_pem_private_key(
                    f.read(),
                    password=None,
                    backend=default_backend()
                )
        else:
            self.private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048,
                backend=default_backend()
            )
            
        self.public_key = self.private_key.public_key()
        
        # Credential registry
        self.credential_registry = {}
        
        # Revocation list
        self.revocation_list = set()
        
    async def issue_workflow_completion_credential(self,
                                                 workflow_id: str,
                                                 workflow_name: str,
                                                 executor_did: str,
                                                 completion_data: Dict[str, Any],
                                                 validity_period: timedelta = timedelta(days=365)) -> VerifiableCredential:
        """Issue credential for workflow completion"""
        try:
            credential_id = f"urn:uuid:{uuid.uuid4()}"
            
            # Create credential
            credential = VerifiableCredential(
                context=[
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.io/credentials/workflow/v1"
                ],
                id=credential_id,
                type=["VerifiableCredential", CredentialType.WORKFLOW_COMPLETION.value],
                issuer={
                    "id": self.issuer_did,
                    "name": self.issuer_name
                },
                issuance_date=datetime.utcnow(),
                expiration_date=datetime.utcnow() + validity_period,
                credential_subject={
                    "id": executor_did,
                    "workflow": {
                        "id": workflow_id,
                        "name": workflow_name,
                        "completedAt": completion_data["completed_at"],
                        "duration": completion_data["duration"],
                        "tasksCompleted": completion_data["tasks_completed"],
                        "outputHash": self._hash_output(completion_data.get("output", {}))
                    },
                    "attestation": {
                        "success": completion_data["success"],
                        "qualityScore": completion_data.get("quality_score", 1.0),
                        "resourcesUsed": completion_data.get("resources_used", {})
                    }
                },
                credential_status={
                    "id": f"{credential_id}#status",
                    "type": "CredentialStatusList2021"
                }
            )
            
            # Sign credential
            credential.proof = await self._create_proof(credential)
            
            # Register credential
            self.credential_registry[credential_id] = {
                "credential": credential,
                "issued_at": datetime.utcnow(),
                "status": CredentialStatus.ACTIVE
            }
            
            return credential
            
        except Exception as e:
            logger.error(f"Error issuing workflow completion credential: {e}")
            raise
            
    async def issue_task_attestation_credential(self,
                                              task_id: str,
                                              task_type: str,
                                              performer_did: str,
                                              attestation_data: Dict[str, Any]) -> VerifiableCredential:
        """Issue credential for task attestation"""
        try:
            credential_id = f"urn:uuid:{uuid.uuid4()}"
            
            credential = VerifiableCredential(
                context=[
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.io/credentials/task/v1"
                ],
                id=credential_id,
                type=["VerifiableCredential", CredentialType.TASK_ATTESTATION.value],
                issuer={
                    "id": self.issuer_did,
                    "name": self.issuer_name
                },
                issuance_date=datetime.utcnow(),
                expiration_date=datetime.utcnow() + timedelta(days=90),
                credential_subject={
                    "id": performer_did,
                    "task": {
                        "id": task_id,
                        "type": task_type,
                        "performedAt": attestation_data["performed_at"],
                        "result": attestation_data["result"],
                        "evidence": attestation_data.get("evidence", [])
                    },
                    "verification": {
                        "method": attestation_data.get("verification_method", "automated"),
                        "verifiedBy": attestation_data.get("verified_by", self.issuer_did),
                        "confidence": attestation_data.get("confidence", 1.0)
                    }
                }
            )
            
            # Sign credential
            credential.proof = await self._create_proof(credential)
            
            # Register credential
            self.credential_registry[credential_id] = {
                "credential": credential,
                "issued_at": datetime.utcnow(),
                "status": CredentialStatus.ACTIVE
            }
            
            return credential
            
        except Exception as e:
            logger.error(f"Error issuing task attestation credential: {e}")
            raise
            
    async def issue_resource_authorization_credential(self,
                                                    resource_id: str,
                                                    resource_type: str,
                                                    authorized_did: str,
                                                    permissions: List[str],
                                                    conditions: Dict[str, Any],
                                                    validity_period: timedelta) -> VerifiableCredential:
        """Issue credential for resource authorization"""
        try:
            credential_id = f"urn:uuid:{uuid.uuid4()}"
            
            credential = VerifiableCredential(
                context=[
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.io/credentials/authorization/v1"
                ],
                id=credential_id,
                type=["VerifiableCredential", CredentialType.RESOURCE_AUTHORIZATION.value],
                issuer={
                    "id": self.issuer_did,
                    "name": self.issuer_name
                },
                issuance_date=datetime.utcnow(),
                expiration_date=datetime.utcnow() + validity_period,
                credential_subject={
                    "id": authorized_did,
                    "authorization": {
                        "resource": {
                            "id": resource_id,
                            "type": resource_type
                        },
                        "permissions": permissions,
                        "conditions": conditions,
                        "delegatable": conditions.get("delegatable", False)
                    }
                }
            )
            
            # Sign credential
            credential.proof = await self._create_proof(credential)
            
            # Register credential
            self.credential_registry[credential_id] = {
                "credential": credential,
                "issued_at": datetime.utcnow(),
                "status": CredentialStatus.ACTIVE
            }
            
            return credential
            
        except Exception as e:
            logger.error(f"Error issuing resource authorization credential: {e}")
            raise
            
    async def create_verifiable_presentation(self,
                                           credentials: List[VerifiableCredential],
                                           holder_did: str,
                                           verifier_did: Optional[str] = None,
                                           challenge: Optional[str] = None) -> VerifiablePresentation:
        """Create a verifiable presentation from credentials"""
        try:
            presentation = VerifiablePresentation(
                context=[
                    "https://www.w3.org/2018/credentials/v1",
                    "https://platformq.io/credentials/presentation/v1"
                ],
                type=["VerifiablePresentation"],
                verifiable_credential=credentials
            )
            
            # Create presentation proof
            presentation_proof = {
                "type": "RsaSignature2018",
                "created": datetime.utcnow().isoformat(),
                "proofPurpose": "authentication",
                "verificationMethod": f"{holder_did}#keys-1",
                "holder": holder_did
            }
            
            if challenge:
                presentation_proof["challenge"] = challenge
                
            if verifier_did:
                presentation_proof["domain"] = verifier_did
                
            # Sign presentation
            presentation_data = presentation.dict()
            presentation_data.pop("proof", None)
            
            signature = await self._sign_data(presentation_data)
            presentation_proof["jws"] = signature
            
            presentation.proof = presentation_proof
            
            return presentation
            
        except Exception as e:
            logger.error(f"Error creating verifiable presentation: {e}")
            raise
            
    async def verify_credential(self,
                              credential: VerifiableCredential,
                              check_revocation: bool = True) -> Dict[str, Any]:
        """Verify a credential"""
        try:
            verification_result = {
                "verified": False,
                "checks": {},
                "errors": []
            }
            
            # Check expiration
            if credential.expiration_date:
                if datetime.utcnow() > credential.expiration_date:
                    verification_result["errors"].append("Credential expired")
                    verification_result["checks"]["expiration"] = False
                else:
                    verification_result["checks"]["expiration"] = True
            else:
                verification_result["checks"]["expiration"] = True
                
            # Check issuance date
            if credential.issuance_date > datetime.utcnow():
                verification_result["errors"].append("Invalid issuance date")
                verification_result["checks"]["issuance_date"] = False
            else:
                verification_result["checks"]["issuance_date"] = True
                
            # Verify proof
            if credential.proof:
                proof_valid = await self._verify_proof(credential)
                verification_result["checks"]["proof"] = proof_valid
                
                if not proof_valid:
                    verification_result["errors"].append("Invalid proof")
            else:
                verification_result["errors"].append("No proof found")
                verification_result["checks"]["proof"] = False
                
            # Check revocation status
            if check_revocation:
                is_revoked = await self._check_revocation(credential.id)
                verification_result["checks"]["revocation"] = not is_revoked
                
                if is_revoked:
                    verification_result["errors"].append("Credential revoked")
                    
            # Overall verification
            verification_result["verified"] = (
                all(verification_result["checks"].values()) and
                len(verification_result["errors"]) == 0
            )
            
            return verification_result
            
        except Exception as e:
            logger.error(f"Error verifying credential: {e}")
            return {
                "verified": False,
                "checks": {},
                "errors": [str(e)]
            }
            
    async def revoke_credential(self,
                              credential_id: str,
                              reason: str) -> Dict[str, Any]:
        """Revoke a credential"""
        try:
            if credential_id not in self.credential_registry:
                return {
                    "success": False,
                    "error": "Credential not found"
                }
                
            # Add to revocation list
            self.revocation_list.add(credential_id)
            
            # Update registry
            self.credential_registry[credential_id]["status"] = CredentialStatus.REVOKED
            self.credential_registry[credential_id]["revoked_at"] = datetime.utcnow()
            self.credential_registry[credential_id]["revocation_reason"] = reason
            
            # Publish revocation event
            await self._publish_revocation(credential_id, reason)
            
            return {
                "success": True,
                "credential_id": credential_id,
                "revoked_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error revoking credential: {e}")
            return {
                "success": False,
                "error": str(e)
            }
            
    async def _create_proof(self, credential: VerifiableCredential) -> Dict[str, Any]:
        """Create cryptographic proof for credential"""
        try:
            # Prepare proof
            proof = {
                "type": "RsaSignature2018",
                "created": datetime.utcnow().isoformat(),
                "proofPurpose": "assertionMethod",
                "verificationMethod": f"{self.issuer_did}#keys-1"
            }
            
            # Create canonical representation
            credential_data = credential.dict()
            credential_data.pop("proof", None)
            
            # Sign credential
            signature = await self._sign_data(credential_data)
            proof["jws"] = signature
            
            return proof
            
        except Exception as e:
            logger.error(f"Error creating proof: {e}")
            raise
            
    async def _sign_data(self, data: Dict[str, Any]) -> str:
        """Sign data with private key"""
        try:
            # Create canonical JSON
            canonical_json = json.dumps(data, sort_keys=True, separators=(',', ':'))
            
            # Sign with private key
            signature = self.private_key.sign(
                canonical_json.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            # Create JWS
            header = {"alg": "PS256", "b64": False, "crit": ["b64"]}
            
            jws_parts = [
                json.dumps(header).encode().hex(),
                signature.hex()
            ]
            
            return ".".join(jws_parts)
            
        except Exception as e:
            logger.error(f"Error signing data: {e}")
            raise
            
    async def _verify_proof(self, credential: VerifiableCredential) -> bool:
        """Verify credential proof"""
        try:
            if not credential.proof or "jws" not in credential.proof:
                return False
                
            # Extract JWS
            jws = credential.proof["jws"]
            parts = jws.split(".")
            
            if len(parts) != 2:
                return False
                
            # Decode header and signature
            header = json.loads(bytes.fromhex(parts[0]).decode())
            signature = bytes.fromhex(parts[1])
            
            # Recreate canonical data
            credential_data = credential.dict()
            credential_data.pop("proof", None)
            canonical_json = json.dumps(credential_data, sort_keys=True, separators=(',', ':'))
            
            # Verify signature
            try:
                self.public_key.verify(
                    signature,
                    canonical_json.encode(),
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                return True
            except:
                return False
                
        except Exception as e:
            logger.error(f"Error verifying proof: {e}")
            return False
            
    async def _check_revocation(self, credential_id: str) -> bool:
        """Check if credential is revoked"""
        return credential_id in self.revocation_list
        
    def _hash_output(self, output: Dict[str, Any]) -> str:
        """Create hash of workflow output"""
        output_json = json.dumps(output, sort_keys=True)
        return hashlib.sha256(output_json.encode()).hexdigest()
        
    async def _publish_revocation(self, credential_id: str, reason: str):
        """Publish credential revocation event"""
        # This would publish to a distributed ledger or event stream
        logger.info(f"Credential {credential_id} revoked: {reason}")


class WorkflowCredentialVerifier:
    """Verifies workflow credentials and presentations"""
    
    def __init__(self, trusted_issuers: List[str]):
        self.trusted_issuers = trusted_issuers
        self.verification_cache = {}
        
    async def verify_workflow_authorization(self,
                                          presentation: VerifiablePresentation,
                                          required_permissions: List[str],
                                          resource_id: str) -> Dict[str, Any]:
        """Verify workflow authorization from presentation"""
        try:
            # Verify presentation
            presentation_valid = await self._verify_presentation(presentation)
            
            if not presentation_valid:
                return {
                    "authorized": False,
                    "error": "Invalid presentation"
                }
                
            # Find authorization credentials
            auth_credentials = [
                cred for cred in presentation.verifiable_credential
                if CredentialType.RESOURCE_AUTHORIZATION.value in cred.type
            ]
            
            # Check each credential
            for credential in auth_credentials:
                # Verify credential
                verification = await self._verify_credential_full(credential)
                
                if not verification["verified"]:
                    continue
                    
                # Check resource match
                auth_subject = credential.credential_subject
                if auth_subject["authorization"]["resource"]["id"] != resource_id:
                    continue
                    
                # Check permissions
                granted_permissions = set(auth_subject["authorization"]["permissions"])
                required_permissions_set = set(required_permissions)
                
                if required_permissions_set.issubset(granted_permissions):
                    return {
                        "authorized": True,
                        "credential_id": credential.id,
                        "permissions": list(granted_permissions),
                        "conditions": auth_subject["authorization"]["conditions"]
                    }
                    
            return {
                "authorized": False,
                "error": "No valid authorization found"
            }
            
        except Exception as e:
            logger.error(f"Error verifying workflow authorization: {e}")
            return {
                "authorized": False,
                "error": str(e)
            }
            
    async def _verify_presentation(self, presentation: VerifiablePresentation) -> bool:
        """Verify a verifiable presentation"""
        try:
            # Verify presentation proof if present
            if presentation.proof:
                # TODO: Implement presentation proof verification
                pass
                
            # Verify all credentials
            for credential in presentation.verifiable_credential:
                verification = await self._verify_credential_full(credential)
                if not verification["verified"]:
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error verifying presentation: {e}")
            return False
            
    async def _verify_credential_full(self, credential: VerifiableCredential) -> Dict[str, Any]:
        """Full credential verification including issuer trust"""
        try:
            # Check cache
            cache_key = credential.id
            if cache_key in self.verification_cache:
                cached_result, cached_time = self.verification_cache[cache_key]
                if (datetime.utcnow() - cached_time).seconds < 300:  # 5 min cache
                    return cached_result
                    
            # Verify issuer trust
            issuer_id = credential.issuer.get("id")
            if issuer_id not in self.trusted_issuers:
                return {
                    "verified": False,
                    "error": "Untrusted issuer"
                }
                
            # Basic credential verification
            manager = WorkflowCredentialManager(issuer_id, "Verifier")
            verification = await manager.verify_credential(credential)
            
            # Cache result
            self.verification_cache[cache_key] = (verification, datetime.utcnow())
            
            return verification
            
        except Exception as e:
            logger.error(f"Error in full credential verification: {e}")
            return {
                "verified": False,
                "error": str(e)
            } 