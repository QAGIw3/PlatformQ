"""
Verifiable credential attestation for workflows
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import base64

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature

from platformq_shared.logging import get_logger
from pyignite import AsyncClient
from ..core.config import settings

logger = get_logger(__name__)


class CredentialType(str, Enum):
    """Credential types"""
    WORKFLOW_COMPLETION = "workflow_completion"
    DATA_PROCESSING = "data_processing"
    QUALITY_ATTESTATION = "quality_attestation"
    COMPLIANCE_VERIFICATION = "compliance_verification"
    AUDIT_TRAIL = "audit_trail"


class CredentialStatus(str, Enum):
    """Credential status"""
    ACTIVE = "active"
    REVOKED = "revoked"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class CredentialAttestor:
    """Issues and verifies workflow attestations"""
    
    def __init__(self):
        self.ignite_client: Optional[AsyncClient] = None
        self.private_key: Optional[rsa.RSAPrivateKey] = None
        self.public_key: Optional[rsa.RSAPublicKey] = None
        self.issued_credentials: Dict[str, Dict[str, Any]] = {}
        self.trusted_issuers: Dict[str, str] = {}  # issuer_id -> public_key
        
    async def initialize(self):
        """Initialize the credential attestor"""
        logger.info("Initializing credential attestor")
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
        
        # Load or generate key pair
        await self._initialize_keys()
        
        # Load issued credentials
        await self._load_credentials()
        
        # Load trusted issuers
        await self._load_trusted_issuers()
        
        # Start credential monitoring
        asyncio.create_task(self._monitor_credentials())
        
        logger.info("Credential attestor initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.ignite_client:
            await self.ignite_client.close()
            
    async def _initialize_keys(self):
        """Initialize cryptographic keys"""
        try:
            # Try to load existing keys
            if settings.credential_key_path and settings.credential_key_path.exists():
                with open(settings.credential_key_path, 'rb') as f:
                    self.private_key = serialization.load_pem_private_key(
                        f.read(),
                        password=None,
                        backend=default_backend()
                    )
                self.public_key = self.private_key.public_key()
                logger.info("Loaded existing credential keys")
            else:
                # Generate new key pair
                self.private_key = rsa.generate_private_key(
                    public_exponent=65537,
                    key_size=2048,
                    backend=default_backend()
                )
                self.public_key = self.private_key.public_key()
                
                # Save keys if path is configured
                if settings.credential_key_path:
                    # Save private key
                    private_pem = self.private_key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    with open(settings.credential_key_path, 'wb') as f:
                        f.write(private_pem)
                        
                    # Save public key
                    public_pem = self.public_key.public_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PublicFormat.SubjectPublicKeyInfo
                    )
                    with open(settings.credential_key_path.with_suffix('.pub'), 'wb') as f:
                        f.write(public_pem)
                        
                logger.info("Generated new credential keys")
                
        except Exception as e:
            logger.error(f"Failed to initialize keys: {e}")
            raise
            
    async def _load_credentials(self):
        """Load issued credentials from storage"""
        if self.ignite_client:
            try:
                cache = await self.ignite_client.get_or_create_cache("credentials")
                # Load credentials (simplified - real implementation would paginate)
                async for key, value in cache.scan():
                    credential = json.loads(value)
                    self.issued_credentials[key] = credential
                logger.info(f"Loaded {len(self.issued_credentials)} credentials")
            except Exception as e:
                logger.error(f"Failed to load credentials: {e}")
                
    async def _load_trusted_issuers(self):
        """Load trusted credential issuers"""
        # In a real implementation, this would load from a trusted registry
        # For now, we'll accept our own credentials
        our_public_key = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
        
        self.trusted_issuers[settings.organization_id] = our_public_key
        
    async def issue_credential(self,
                             subject: Dict[str, Any],
                             credential_type: CredentialType,
                             claims: Dict[str, Any],
                             validity_hours: int = 24) -> Dict[str, Any]:
        """Issue a verifiable credential"""
        logger.info(f"Issuing {credential_type} credential for {subject.get('id')}")
        
        credential_id = str(uuid.uuid4())
        
        # Create credential structure
        credential = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://platformq.io/contexts/workflow/v1"
            ],
            "id": f"urn:uuid:{credential_id}",
            "type": ["VerifiableCredential", credential_type.value],
            "issuer": {
                "id": f"did:platformq:{settings.organization_id}",
                "name": settings.organization_name
            },
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(hours=validity_hours)).isoformat(),
            "credentialSubject": {
                **subject,
                **claims
            },
            "credentialStatus": {
                "id": f"https://platformq.io/credentials/status/{credential_id}",
                "type": "RevocationList2020"
            }
        }
        
        # Create proof
        proof = await self._create_proof(credential)
        credential["proof"] = proof
        
        # Store credential
        self.issued_credentials[credential_id] = {
            "credential": credential,
            "status": CredentialStatus.ACTIVE,
            "issued_at": datetime.utcnow().isoformat(),
            "metadata": {
                "type": credential_type,
                "subject_id": subject.get('id'),
                "validity_hours": validity_hours
            }
        }
        
        # Persist to storage
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("credentials")
            await cache.put(credential_id, json.dumps(self.issued_credentials[credential_id]))
            
        return credential
        
    async def _create_proof(self, credential: Dict[str, Any]) -> Dict[str, Any]:
        """Create cryptographic proof for credential"""
        # Remove any existing proof
        credential_copy = credential.copy()
        credential_copy.pop('proof', None)
        
        # Canonicalize credential (simplified - real implementation would use JSON-LD)
        canonical = json.dumps(credential_copy, sort_keys=True)
        
        # Create signature
        signature = self.private_key.sign(
            canonical.encode('utf-8'),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        
        # Create proof object
        proof = {
            "type": "RsaSignature2018",
            "created": datetime.utcnow().isoformat(),
            "verificationMethod": f"did:platformq:{settings.organization_id}#keys-1",
            "proofPurpose": "assertionMethod",
            "jws": base64.b64encode(signature).decode('utf-8')
        }
        
        return proof
        
    async def verify_credential(self, credential: Dict[str, Any]) -> Dict[str, Any]:
        """Verify a credential"""
        logger.info(f"Verifying credential {credential.get('id')}")
        
        verification_result = {
            "verified": False,
            "checks": {
                "signature": False,
                "expiration": False,
                "revocation": False,
                "issuer": False
            },
            "errors": []
        }
        
        try:
            # Check expiration
            expiration_date = datetime.fromisoformat(
                credential.get('expirationDate', '').replace('Z', '+00:00')
            )
            if datetime.utcnow() > expiration_date:
                verification_result['errors'].append("Credential has expired")
            else:
                verification_result['checks']['expiration'] = True
                
            # Check issuer trust
            issuer_id = credential.get('issuer', {}).get('id', '').split(':')[-1]
            if issuer_id in self.trusted_issuers:
                verification_result['checks']['issuer'] = True
            else:
                verification_result['errors'].append(f"Unknown issuer: {issuer_id}")
                
            # Verify signature
            if await self._verify_signature(credential, issuer_id):
                verification_result['checks']['signature'] = True
            else:
                verification_result['errors'].append("Invalid signature")
                
            # Check revocation status
            credential_id = credential.get('id', '').split(':')[-1]
            if await self._check_revocation(credential_id):
                verification_result['errors'].append("Credential has been revoked")
            else:
                verification_result['checks']['revocation'] = True
                
            # Overall verification
            verification_result['verified'] = all(verification_result['checks'].values())
            
        except Exception as e:
            logger.error(f"Credential verification failed: {e}")
            verification_result['errors'].append(f"Verification error: {str(e)}")
            
        return verification_result
        
    async def _verify_signature(self, credential: Dict[str, Any], issuer_id: str) -> bool:
        """Verify credential signature"""
        try:
            # Get issuer's public key
            public_key_pem = self.trusted_issuers.get(issuer_id)
            if not public_key_pem:
                return False
                
            public_key = serialization.load_pem_public_key(
                public_key_pem.encode('utf-8'),
                backend=default_backend()
            )
            
            # Extract proof
            proof = credential.get('proof', {})
            signature = base64.b64decode(proof.get('jws', ''))
            
            # Remove proof from credential
            credential_copy = credential.copy()
            credential_copy.pop('proof', None)
            
            # Canonicalize
            canonical = json.dumps(credential_copy, sort_keys=True)
            
            # Verify signature
            public_key.verify(
                signature,
                canonical.encode('utf-8'),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            
            return True
            
        except InvalidSignature:
            return False
        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False
            
    async def _check_revocation(self, credential_id: str) -> bool:
        """Check if credential has been revoked"""
        stored = self.issued_credentials.get(credential_id)
        if stored:
            return stored.get('status') == CredentialStatus.REVOKED
        return False
        
    async def revoke_credential(self, credential_id: str, reason: str = "") -> bool:
        """Revoke a credential"""
        if credential_id not in self.issued_credentials:
            return False
            
        logger.info(f"Revoking credential {credential_id}: {reason}")
        
        # Update status
        self.issued_credentials[credential_id]['status'] = CredentialStatus.REVOKED
        self.issued_credentials[credential_id]['revoked_at'] = datetime.utcnow().isoformat()
        self.issued_credentials[credential_id]['revocation_reason'] = reason
        
        # Persist update
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("credentials")
            await cache.put(credential_id, json.dumps(self.issued_credentials[credential_id]))
            
        return True
        
    async def create_workflow_attestation(self,
                                        workflow_id: str,
                                        workflow_name: str,
                                        execution_id: str,
                                        execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create attestation for completed workflow"""
        subject = {
            "id": f"urn:workflow:{workflow_id}",
            "workflow_name": workflow_name,
            "execution_id": execution_id
        }
        
        claims = {
            "executedAt": execution_result.get('completed_at', datetime.utcnow().isoformat()),
            "status": execution_result.get('status', 'unknown'),
            "duration": execution_result.get('duration'),
            "stepsCompleted": execution_result.get('steps_completed', []),
            "dataProcessed": execution_result.get('data_processed', {}),
            "qualityMetrics": execution_result.get('quality_metrics', {}),
            "resourcesUsed": execution_result.get('resources_used', {}),
            "outputHash": self._calculate_output_hash(execution_result.get('outputs', {}))
        }
        
        # Add compliance claims if applicable
        if execution_result.get('compliance_checks'):
            claims['complianceChecks'] = execution_result['compliance_checks']
            
        return await self.issue_credential(
            subject=subject,
            credential_type=CredentialType.WORKFLOW_COMPLETION,
            claims=claims,
            validity_hours=settings.credential_validity_hours
        )
        
    def _calculate_output_hash(self, outputs: Dict[str, Any]) -> str:
        """Calculate hash of workflow outputs"""
        # Create deterministic representation
        output_str = json.dumps(outputs, sort_keys=True)
        
        # Calculate SHA-256 hash
        hash_obj = hashlib.sha256(output_str.encode('utf-8'))
        return hash_obj.hexdigest()
        
    async def create_data_processing_attestation(self,
                                               dataset_id: str,
                                               processing_type: str,
                                               processing_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create attestation for data processing"""
        subject = {
            "id": f"urn:dataset:{dataset_id}",
            "processing_type": processing_type
        }
        
        claims = {
            "processedAt": datetime.utcnow().isoformat(),
            "recordsProcessed": processing_result.get('record_count', 0),
            "transformations": processing_result.get('transformations', []),
            "dataQuality": processing_result.get('quality_scores', {}),
            "lineage": processing_result.get('lineage', {}),
            "checksumBefore": processing_result.get('checksum_before'),
            "checksumAfter": processing_result.get('checksum_after')
        }
        
        return await self.issue_credential(
            subject=subject,
            credential_type=CredentialType.DATA_PROCESSING,
            claims=claims,
            validity_hours=settings.credential_validity_hours * 7  # Longer validity for data
        )
        
    async def create_quality_attestation(self,
                                       dataset_id: str,
                                       quality_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create attestation for data quality verification"""
        subject = {
            "id": f"urn:dataset:{dataset_id}",
            "type": "DataQualityAttestation"
        }
        
        claims = {
            "verifiedAt": datetime.utcnow().isoformat(),
            "qualityDimensions": quality_result.get('dimensions', {}),
            "overallScore": quality_result.get('overall_score', 0),
            "rulesApplied": quality_result.get('rules_applied', []),
            "issuesFound": quality_result.get('issues_found', 0),
            "remediationApplied": quality_result.get('remediation_applied', False)
        }
        
        return await self.issue_credential(
            subject=subject,
            credential_type=CredentialType.QUALITY_ATTESTATION,
            claims=claims,
            validity_hours=settings.credential_validity_hours
        )
        
    async def create_compliance_verification(self,
                                           entity_id: str,
                                           compliance_type: str,
                                           verification_result: Dict[str, Any]) -> Dict[str, Any]:
        """Create attestation for compliance verification"""
        subject = {
            "id": f"urn:entity:{entity_id}",
            "compliance_type": compliance_type
        }
        
        claims = {
            "verifiedAt": datetime.utcnow().isoformat(),
            "standards": verification_result.get('standards', []),
            "requirements": verification_result.get('requirements', {}),
            "passed": verification_result.get('passed', False),
            "findings": verification_result.get('findings', []),
            "auditor": verification_result.get('auditor', {})
        }
        
        return await self.issue_credential(
            subject=subject,
            credential_type=CredentialType.COMPLIANCE_VERIFICATION,
            claims=claims,
            validity_hours=settings.credential_validity_hours * 30  # 30 days for compliance
        )
        
    async def create_presentation(self,
                                credentials: List[Dict[str, Any]],
                                holder: Dict[str, Any],
                                verifier: Optional[str] = None) -> Dict[str, Any]:
        """Create a verifiable presentation from multiple credentials"""
        presentation_id = str(uuid.uuid4())
        
        presentation = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1"
            ],
            "type": ["VerifiablePresentation"],
            "id": f"urn:uuid:{presentation_id}",
            "holder": holder,
            "verifiableCredential": credentials,
            "created": datetime.utcnow().isoformat()
        }
        
        if verifier:
            presentation["verifier"] = verifier
            
        # Create proof for presentation
        proof = await self._create_proof(presentation)
        presentation["proof"] = proof
        
        return presentation
        
    async def get_credential_status(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get credential status"""
        stored = self.issued_credentials.get(credential_id)
        if not stored:
            return None
            
        return {
            "credential_id": credential_id,
            "status": stored['status'],
            "issued_at": stored['issued_at'],
            "type": stored['metadata']['type'],
            "subject_id": stored['metadata']['subject_id'],
            "revoked_at": stored.get('revoked_at'),
            "revocation_reason": stored.get('revocation_reason')
        }
        
    async def list_credentials(self,
                             credential_type: Optional[CredentialType] = None,
                             subject_id: Optional[str] = None,
                             status: Optional[CredentialStatus] = None) -> List[Dict[str, Any]]:
        """List issued credentials with filtering"""
        credentials = []
        
        for cred_id, stored in self.issued_credentials.items():
            # Apply filters
            if credential_type and stored['metadata']['type'] != credential_type:
                continue
            if subject_id and stored['metadata']['subject_id'] != subject_id:
                continue
            if status and stored['status'] != status:
                continue
                
            credentials.append({
                "id": cred_id,
                "type": stored['metadata']['type'],
                "subject_id": stored['metadata']['subject_id'],
                "status": stored['status'],
                "issued_at": stored['issued_at'],
                "credential": stored['credential']
            })
            
        return credentials
        
    async def _monitor_credentials(self):
        """Monitor credential expiration"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                for cred_id, stored in self.issued_credentials.items():
                    if stored['status'] == CredentialStatus.ACTIVE:
                        # Check expiration
                        credential = stored['credential']
                        expiration = datetime.fromisoformat(
                            credential.get('expirationDate', '').replace('Z', '+00:00')
                        )
                        
                        if current_time > expiration:
                            stored['status'] = CredentialStatus.EXPIRED
                            logger.info(f"Credential {cred_id} expired")
                            
                            # Update in storage
                            if self.ignite_client:
                                cache = await self.ignite_client.get_or_create_cache("credentials")
                                await cache.put(cred_id, json.dumps(stored))
                                
            except Exception as e:
                logger.error(f"Credential monitoring error: {e}")
                
            await asyncio.sleep(3600)  # Check every hour 