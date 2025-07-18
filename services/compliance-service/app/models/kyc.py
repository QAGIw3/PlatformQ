"""
KYC data models.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class DocumentType(Enum):
    """Types of KYC documents"""
    PASSPORT = "passport"
    ID_CARD = "id_card"
    DRIVER_LICENSE = "driver_license"
    PROOF_OF_ADDRESS = "proof_of_address"
    BANK_STATEMENT = "bank_statement"
    SOURCE_OF_FUNDS = "source_of_funds"


class DocumentStatus(Enum):
    """Document verification status"""
    PENDING = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass
class Document:
    """KYC document"""
    id: str
    verification_id: str
    type: DocumentType
    status: DocumentStatus
    file_name: str
    content_type: str
    uploaded_at: datetime
    verified_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None
    storage_path: Optional[str] = None


@dataclass
class KYCVerification:
    """KYC verification record"""
    id: str
    user_id: str
    level: int
    status: str
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    required_documents: List[str] = None
    completed_checks: List[str] = None
    pending_checks: List[str] = None
    result: Optional[Dict[str, Any]] = None 