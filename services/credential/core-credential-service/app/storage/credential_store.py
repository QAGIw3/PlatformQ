"""
Credential Store - Handles database and storage operations
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import base64

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, DateTime, Boolean, JSON, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import select, and_, or_
import httpx

from platformq_shared.vault import VaultClient

logger = logging.getLogger(__name__)

Base = declarative_base()


class CredentialRecord(Base):
    """Database model for credential metadata"""
    __tablename__ = "credentials"
    
    credential_id = Column(String, primary_key=True)
    issuer_did = Column(String, nullable=False, index=True)
    subject_did = Column(String, index=True)
    credential_type = Column(String, nullable=False, index=True)
    tenant_id = Column(String, index=True)
    
    # Status fields
    status = Column(String, default="active", index=True)
    revoked = Column(Boolean, default=False)
    revocation_reason = Column(String)
    revoked_at = Column(DateTime)
    
    # Timestamps
    issued_at = Column(DateTime, nullable=False)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Storage info
    storage_type = Column(String)  # ipfs, minio, database
    storage_id = Column(String)    # IPFS CID or MinIO object ID
    encrypted = Column(Boolean, default=False)
    
    # Blockchain info
    blockchain_anchors = Column(JSON)  # List of anchor info
    
    # Additional metadata
    metadata = Column(JSON)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_tenant_type', 'tenant_id', 'credential_type'),
        Index('idx_issuer_status', 'issuer_did', 'status'),
        Index('idx_subject_type', 'subject_did', 'credential_type'),
    )


class CredentialStore:
    """Manages credential storage in database and external storage"""
    
    def __init__(self, database_url: str, storage_service_url: str, 
                 http_client: httpx.AsyncClient, vault_client: Optional[VaultClient] = None):
        self.database_url = database_url
        self.storage_service_url = storage_service_url
        self.http_client = http_client
        self.vault_client = vault_client
        self._engine = None
        self._session_factory = None
        
    async def initialize(self):
        """Initialize database connection"""
        try:
            self._engine = create_async_engine(
                self.database_url,
                pool_size=20,
                max_overflow=10,
                pool_pre_ping=True
            )
            
            self._session_factory = sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            # Create tables if they don't exist
            async with self._engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                
            logger.info("Initialized credential store")
            
        except Exception as e:
            logger.error(f"Failed to initialize credential store: {e}")
            raise
    
    async def store_credential(
        self,
        credential_id: str,
        credential: Dict[str, Any],
        issuer_did: str,
        subject_did: Optional[str],
        credential_type: str,
        tenant_id: Optional[str] = None,
        store_on_ipfs: bool = True,
        encrypt: bool = True
    ) -> Dict[str, Any]:
        """Store credential in database and optionally in external storage"""
        
        # Extract metadata
        issued_at = datetime.fromisoformat(
            credential.get("issuanceDate", datetime.utcnow().isoformat()).replace('Z', '+00:00')
        )
        
        expires_at = None
        if credential.get("expirationDate"):
            expires_at = datetime.fromisoformat(
                credential["expirationDate"].replace('Z', '+00:00')
            )
        
        # Store credential data externally if requested
        storage_info = None
        if store_on_ipfs:
            storage_info = await self._store_external(
                credential_id, credential, encrypt
            )
        
        # Create database record
        async with self._session_factory() as session:
            record = CredentialRecord(
                credential_id=credential_id,
                issuer_did=issuer_did,
                subject_did=subject_did,
                credential_type=credential_type,
                tenant_id=tenant_id,
                status="active",
                issued_at=issued_at,
                expires_at=expires_at,
                storage_type=storage_info.get("type") if storage_info else "database",
                storage_id=storage_info.get("id") if storage_info else None,
                encrypted=encrypt,
                metadata={
                    "name": credential.get("name"),
                    "description": credential.get("description")
                }
            )
            
            session.add(record)
            await session.commit()
            
        return {
            "credential_id": credential_id,
            "storage_info": storage_info
        }
    
    async def get_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve credential by ID"""
        async with self._session_factory() as session:
            result = await session.execute(
                select(CredentialRecord).where(
                    CredentialRecord.credential_id == credential_id
                )
            )
            record = result.scalar_one_or_none()
            
            if not record:
                return None
                
            # Get credential data
            credential_data = None
            if record.storage_type == "database":
                # Would be stored in a separate table
                credential_data = record.metadata.get("credential_data")
            else:
                # Retrieve from external storage
                credential_data = await self._retrieve_external(
                    record.storage_type,
                    record.storage_id,
                    record.encrypted
                )
            
            return {
                "credential": credential_data,
                "metadata": {
                    "credential_id": record.credential_id,
                    "status": record.status,
                    "revoked": record.revoked,
                    "issued_at": record.issued_at.isoformat(),
                    "expires_at": record.expires_at.isoformat() if record.expires_at else None
                }
            }
    
    async def update_credential_status(
        self,
        credential_id: str,
        status: str,
        reason: Optional[str] = None
    ) -> bool:
        """Update credential status"""
        async with self._session_factory() as session:
            result = await session.execute(
                select(CredentialRecord).where(
                    CredentialRecord.credential_id == credential_id
                )
            )
            record = result.scalar_one_or_none()
            
            if not record:
                return False
                
            record.status = status
            record.updated_at = datetime.utcnow()
            
            if status == "revoked":
                record.revoked = True
                record.revocation_reason = reason
                record.revoked_at = datetime.utcnow()
            
            await session.commit()
            return True
    
    async def add_blockchain_anchor(
        self,
        credential_id: str,
        blockchain: str,
        transaction_hash: str,
        block_number: Optional[int] = None
    ):
        """Add blockchain anchor information"""
        async with self._session_factory() as session:
            result = await session.execute(
                select(CredentialRecord).where(
                    CredentialRecord.credential_id == credential_id
                )
            )
            record = result.scalar_one_or_none()
            
            if record:
                anchors = record.blockchain_anchors or []
                anchors.append({
                    "blockchain": blockchain,
                    "transaction_hash": transaction_hash,
                    "block_number": block_number,
                    "anchored_at": datetime.utcnow().isoformat()
                })
                record.blockchain_anchors = anchors
                await session.commit()
    
    async def search_credentials(
        self,
        issuer: Optional[str] = None,
        subject: Optional[str] = None,
        credential_type: Optional[str] = None,
        tenant_id: Optional[str] = None,
        status: Optional[str] = None,
        include_revoked: bool = False,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Search credentials with filters"""
        async with self._session_factory() as session:
            query = select(CredentialRecord)
            
            # Build filters
            filters = []
            if issuer:
                filters.append(CredentialRecord.issuer_did == issuer)
            if subject:
                filters.append(CredentialRecord.subject_did == subject)
            if credential_type:
                filters.append(CredentialRecord.credential_type == credential_type)
            if tenant_id:
                filters.append(CredentialRecord.tenant_id == tenant_id)
            if status:
                filters.append(CredentialRecord.status == status)
            if not include_revoked:
                filters.append(CredentialRecord.revoked == False)
                
            if filters:
                query = query.where(and_(*filters))
                
            # Count total
            count_query = select(func.count()).select_from(query.subquery())
            total_result = await session.execute(count_query)
            total = total_result.scalar()
            
            # Get page
            query = query.offset(offset).limit(limit)
            result = await session.execute(query)
            records = result.scalars().all()
            
            return {
                "total": total,
                "offset": offset,
                "limit": limit,
                "results": [
                    {
                        "credential_id": r.credential_id,
                        "issuer_did": r.issuer_did,
                        "subject_did": r.subject_did,
                        "credential_type": r.credential_type,
                        "status": r.status,
                        "issued_at": r.issued_at.isoformat(),
                        "expires_at": r.expires_at.isoformat() if r.expires_at else None
                    }
                    for r in records
                ]
            }
    
    async def _store_external(
        self,
        credential_id: str,
        credential: Dict[str, Any],
        encrypt: bool
    ) -> Dict[str, Any]:
        """Store credential in external storage (IPFS/MinIO)"""
        try:
            # Convert to JSON
            data = json.dumps(credential)
            
            # Encrypt if requested
            if encrypt and self.vault_client:
                # Get encryption key from Vault
                key_name = "credential-encryption-key"
                encrypted_response = await self.vault_client.encrypt_data(
                    mount_point="transit",
                    key_name=key_name,
                    plaintext=base64.b64encode(data.encode()).decode()
                )
                data = encrypted_response["ciphertext"]
            
            # Store via storage service
            response = await self.http_client.post(
                f"{self.storage_service_url}/api/v1/store",
                json={
                    "content": data,
                    "content_type": "application/json",
                    "metadata": {
                        "credential_id": credential_id,
                        "encrypted": encrypt
                    }
                }
            )
            response.raise_for_status()
            
            result = response.json()
            return {
                "type": result.get("storage_type", "ipfs"),
                "id": result.get("storage_id"),
                "url": result.get("url")
            }
            
        except Exception as e:
            logger.error(f"Failed to store credential externally: {e}")
            raise
    
    async def _retrieve_external(
        self,
        storage_type: str,
        storage_id: str,
        encrypted: bool
    ) -> Optional[Dict[str, Any]]:
        """Retrieve credential from external storage"""
        try:
            # Retrieve via storage service
            response = await self.http_client.get(
                f"{self.storage_service_url}/api/v1/retrieve/{storage_id}"
            )
            response.raise_for_status()
            
            data = response.json().get("content")
            
            # Decrypt if needed
            if encrypted and self.vault_client:
                key_name = "credential-encryption-key"
                decrypted_response = await self.vault_client.decrypt_data(
                    mount_point="transit",
                    key_name=key_name,
                    ciphertext=data
                )
                data = base64.b64decode(decrypted_response["plaintext"]).decode()
            
            return json.loads(data)
            
        except Exception as e:
            logger.error(f"Failed to retrieve credential from storage: {e}")
            return None
    
    async def health_check(self) -> bool:
        """Check if storage is healthy"""
        try:
            async with self._session_factory() as session:
                await session.execute(select(1))
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    async def close(self):
        """Close database connections"""
        if self._engine:
            await self._engine.dispose()


# Import after class definition to avoid circular imports
from sqlalchemy import func 