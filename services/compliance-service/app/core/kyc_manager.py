"""
KYC Manager - Core KYC verification logic.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import json
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class KYCStatus(Enum):
    """KYC verification status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class KYCLevel(Enum):
    """KYC verification levels"""
    BASIC = 1  # Email + phone verification
    STANDARD = 2  # ID verification + address proof
    ENHANCED = 3  # Enhanced due diligence


@dataclass
class KYCVerification:
    """KYC verification record"""
    id: str
    user_id: str
    level: KYCLevel
    status: KYCStatus
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    required_documents: List[str] = None
    completed_checks: List[str] = None
    pending_checks: List[str] = None
    result: Optional[Dict[str, Any]] = None
    

class KYCManager:
    """
    Manages KYC verification workflows.
    
    Features:
    - Multi-level KYC (Basic, Standard, Enhanced)
    - Document verification
    - Identity verification
    - Address verification
    - Sanctions screening
    - Automated workflows
    """
    
    def __init__(self, identity_verifier, document_storage, encryption_key):
        self.identity_verifier = identity_verifier
        self.document_storage = document_storage
        self.encryption_key = encryption_key
        self._verifications = {}  # In production, use persistent storage
        
    async def initialize(self):
        """Initialize KYC manager"""
        logger.info("KYC Manager initialized")
        
    async def shutdown(self):
        """Shutdown KYC manager"""
        logger.info("KYC Manager shutdown")
        
    async def initiate_verification(
        self,
        user_id: str,
        kyc_level: KYCLevel,
        country: str,
        document_type: str
    ) -> KYCVerification:
        """Initiate a new KYC verification"""
        # Generate verification ID
        verification_id = self._generate_verification_id(user_id)
        
        # Determine required documents based on level and country
        required_docs = self._get_required_documents(kyc_level, country, document_type)
        
        # Create verification record
        verification = KYCVerification(
            id=verification_id,
            user_id=user_id,
            level=kyc_level,
            status=KYCStatus.PENDING,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=30),
            required_documents=required_docs,
            completed_checks=[],
            pending_checks=self._get_required_checks(kyc_level)
        )
        
        # Store verification
        self._verifications[verification_id] = verification
        
        logger.info(f"Initiated KYC verification {verification_id} for user {user_id}")
        return verification
        
    async def upload_document(
        self,
        verification_id: str,
        document_type: str,
        file_data: bytes,
        file_name: str,
        content_type: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Upload a document for verification"""
        # Get verification
        verification = self._verifications.get(verification_id)
        if not verification:
            raise ValueError("Verification not found")
            
        if verification.user_id != user_id:
            raise ValueError("Unauthorized")
            
        # Encrypt document
        encrypted_data = self._encrypt_document(file_data)
        
        # Generate document ID
        doc_id = hashlib.sha256(f"{verification_id}:{document_type}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:16]
        
        # Store document metadata
        document = {
            "id": doc_id,
            "verification_id": verification_id,
            "type": document_type,
            "file_name": file_name,
            "content_type": content_type,
            "uploaded_at": datetime.utcnow().isoformat(),
            "status": "uploaded",
            "storage_path": f"{self.document_storage}/{verification_id}/{doc_id}"
        }
        
        # In production, store encrypted_data to secure storage
        # For now, just update verification
        verification.status = KYCStatus.IN_PROGRESS
        verification.updated_at = datetime.utcnow()
        
        logger.info(f"Document {doc_id} uploaded for verification {verification_id}")
        return document
        
    async def submit_for_verification(
        self,
        verification_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Submit documents for verification"""
        # Get verification
        verification = self._verifications.get(verification_id)
        if not verification:
            raise ValueError("Verification not found")
            
        if verification.user_id != user_id:
            raise ValueError("Unauthorized")
            
        # Start verification process
        verification.status = KYCStatus.IN_PROGRESS
        verification.updated_at = datetime.utcnow()
        
        # Trigger identity verification
        # In production, this would call actual verification services
        identity_result = await self.identity_verifier.verify(
            verification_id=verification_id,
            level=verification.level.value
        )
        
        # Update verification based on result
        if identity_result["success"]:
            verification.status = KYCStatus.APPROVED
            verification.completed_at = datetime.utcnow()
            verification.expires_at = datetime.utcnow() + timedelta(days=365)
            verification.completed_checks = verification.pending_checks
            verification.pending_checks = []
            verification.result = identity_result
        else:
            verification.status = KYCStatus.REJECTED
            verification.result = identity_result
            
        logger.info(f"Verification {verification_id} submitted with status {verification.status.value}")
        
        return {
            "status": verification.status.value,
            "estimated_completion": "24 hours"
        }
        
    async def get_user_status(self, user_id: str) -> Any:
        """Get KYC status for a user"""
        # Find latest verification for user
        user_verifications = [
            v for v in self._verifications.values()
            if v.user_id == user_id
        ]
        
        if not user_verifications:
            # Return default status
            return type('obj', (object,), {
                'status': KYCStatus.PENDING,
                'level': KYCLevel.BASIC,
                'verified_at': None,
                'expires_at': None,
                'required_documents': ['government_id', 'proof_of_address'],
                'completed_checks': [],
                'pending_checks': ['identity', 'address', 'sanctions']
            })
            
        # Get most recent verification
        latest = sorted(user_verifications, key=lambda x: x.created_at, reverse=True)[0]
        
        return type('obj', (object,), {
            'status': latest.status,
            'level': latest.level,
            'verified_at': latest.completed_at,
            'expires_at': latest.expires_at,
            'required_documents': latest.required_documents or [],
            'completed_checks': latest.completed_checks or [],
            'pending_checks': latest.pending_checks or []
        })
        
    async def get_verification_history(
        self,
        user_id: str,
        limit: int = 10
    ) -> List[KYCVerification]:
        """Get verification history for a user"""
        user_verifications = [
            v for v in self._verifications.values()
            if v.user_id == user_id
        ]
        
        # Sort by created date descending
        user_verifications.sort(key=lambda x: x.created_at, reverse=True)
        
        return user_verifications[:limit]
        
    async def refresh_verification(self, user_id: str) -> Dict[str, Any]:
        """Refresh an expired verification"""
        # Get current status
        status = await self.get_user_status(user_id)
        
        # Create new verification with same level
        new_verification = await self.initiate_verification(
            user_id=user_id,
            kyc_level=status.level,
            country="US",  # In production, get from user profile
            document_type="passport"
        )
        
        return {
            "verification_id": new_verification.id,
            "required_documents": new_verification.required_documents,
            "previous_level": status.level.value
        }
        
    async def delete_document(self, document_id: str, user_id: str):
        """Delete a document"""
        # In production, verify ownership and delete from storage
        logger.info(f"Document {document_id} deleted by user {user_id}")
        
    def _generate_verification_id(self, user_id: str) -> str:
        """Generate unique verification ID"""
        timestamp = datetime.utcnow().isoformat()
        return hashlib.sha256(f"{user_id}:{timestamp}".encode()).hexdigest()[:16]
        
    def _get_required_documents(
        self,
        level: KYCLevel,
        country: str,
        document_type: str
    ) -> List[str]:
        """Get required documents based on level and jurisdiction"""
        if level == KYCLevel.BASIC:
            return []
        elif level == KYCLevel.STANDARD:
            return [document_type, "proof_of_address"]
        else:  # ENHANCED
            return [
                document_type,
                "proof_of_address",
                "bank_statement",
                "source_of_funds"
            ]
            
    def _get_required_checks(self, level: KYCLevel) -> List[str]:
        """Get required checks based on level"""
        if level == KYCLevel.BASIC:
            return ["email", "phone"]
        elif level == KYCLevel.STANDARD:
            return ["identity", "address", "sanctions"]
        else:  # ENHANCED
            return [
                "identity",
                "address",
                "sanctions",
                "pep",
                "adverse_media",
                "source_of_funds"
            ]
            
    def _encrypt_document(self, data: bytes) -> bytes:
        """Encrypt document data"""
        # In production, use proper encryption
        return data 