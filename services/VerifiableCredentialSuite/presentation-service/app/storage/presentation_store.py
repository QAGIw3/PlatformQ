"""
Presentation Storage using Async SQLAlchemy
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON, Boolean, Index, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

Base = declarative_base()


class PresentationRecord(Base):
    """Presentation database model"""
    __tablename__ = "presentations"
    
    presentation_id = Column(String, primary_key=True)
    holder_did = Column(String, nullable=False, index=True)
    verifier_did = Column(String, index=True)
    presentation = Column(JSON, nullable=False)
    credential_ids = Column(JSON)  # List of credential IDs
    challenge = Column(String)
    domain = Column(String)
    status = Column(String, nullable=False, index=True)
    session_id = Column(String, index=True)
    submitted_at = Column(DateTime(timezone=True))
    verified_at = Column(DateTime(timezone=True))
    revoked_at = Column(DateTime(timezone=True))
    revocation_reason = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Indexes
    __table_args__ = (
        Index('idx_holder_status', 'holder_did', 'status'),
        Index('idx_verifier_status', 'verifier_did', 'status'),
        Index('idx_session', 'session_id'),
        Index('idx_created_at', 'created_at'),
    )


class VerificationRecord(Base):
    """Verification attempt record"""
    __tablename__ = "verification_records"
    
    id = Column(String, primary_key=True)
    presentation_id = Column(String, nullable=False, index=True)
    verifier = Column(String, nullable=False, index=True)
    result = Column(String, nullable=False)  # valid, invalid, expired, revoked
    details = Column(JSON)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Index
    __table_args__ = (
        Index('idx_presentation_verifier', 'presentation_id', 'verifier'),
        Index('idx_verifier_timestamp', 'verifier', 'timestamp'),
    )


class PresentationStore:
    """
    Manages presentation storage operations
    """
    
    def __init__(
        self,
        database_url: str,
        vault_client: Optional[Any] = None
    ):
        self.database_url = database_url
        self.vault_client = vault_client
        self.engine = None
        self.async_session = None
    
    async def initialize(self):
        """Initialize database connection"""
        # Create async engine
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True
        )
        
        # Create session factory
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # Create tables
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def close(self):
        """Close database connection"""
        if self.engine:
            await self.engine.dispose()
    
    async def health_check(self) -> bool:
        """Check database health"""
        try:
            async with self.async_session() as session:
                await session.execute(select(1))
                return True
        except Exception:
            return False
    
    async def create(
        self,
        presentation_id: str,
        holder_did: str,
        verifier_did: Optional[str],
        presentation: Dict[str, Any],
        credential_ids: List[str],
        challenge: Optional[str],
        domain: Optional[str],
        status: str,
        session_id: Optional[str] = None
    ) -> PresentationRecord:
        """Create new presentation record"""
        # Encrypt sensitive data if Vault is available
        encrypted_presentation = presentation
        if self.vault_client and hasattr(self.vault_client.secrets, 'transit'):
            try:
                # Encrypt the presentation
                encrypted_response = self.vault_client.secrets.transit.encrypt_data(
                    name='presentation-encryption',
                    plaintext=base64.b64encode(json.dumps(presentation).encode()).decode()
                )
                encrypted_presentation = {
                    "encrypted": True,
                    "ciphertext": encrypted_response['data']['ciphertext']
                }
            except Exception as e:
                print(f"Failed to encrypt presentation: {str(e)}")
        
        record = PresentationRecord(
            presentation_id=presentation_id,
            holder_did=holder_did,
            verifier_did=verifier_did,
            presentation=encrypted_presentation,
            credential_ids=credential_ids,
            challenge=challenge,
            domain=domain,
            status=status,
            session_id=session_id
        )
        
        async with self.async_session() as session:
            session.add(record)
            await session.commit()
            await session.refresh(record)
            
        return record
    
    async def get(self, presentation_id: str) -> Optional[PresentationRecord]:
        """Get presentation by ID"""
        async with self.async_session() as session:
            result = await session.execute(
                select(PresentationRecord).where(
                    PresentationRecord.presentation_id == presentation_id
                )
            )
            record = result.scalar_one_or_none()
            
            if record and self._is_encrypted(record.presentation):
                # Decrypt if encrypted
                record.presentation = await self._decrypt_presentation(record.presentation)
            
            return record
    
    async def update(
        self,
        presentation_id: str,
        **kwargs
    ) -> Optional[PresentationRecord]:
        """Update presentation record"""
        async with self.async_session() as session:
            result = await session.execute(
                select(PresentationRecord).where(
                    PresentationRecord.presentation_id == presentation_id
                )
            )
            record = result.scalar_one_or_none()
            
            if not record:
                return None
            
            # Update fields
            for key, value in kwargs.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            
            record.updated_at = datetime.now(timezone.utc)
            
            await session.commit()
            await session.refresh(record)
            
            return record
    
    async def list_presentations(
        self,
        holder_did: Optional[str] = None,
        verifier_did: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[PresentationRecord]:
        """List presentations with filters"""
        async with self.async_session() as session:
            query = select(PresentationRecord)
            
            if holder_did:
                query = query.where(PresentationRecord.holder_did == holder_did)
            
            if verifier_did:
                query = query.where(PresentationRecord.verifier_did == verifier_did)
            
            if status:
                query = query.where(PresentationRecord.status == status)
            
            query = query.order_by(PresentationRecord.created_at.desc())
            query = query.limit(limit).offset(offset)
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            # Decrypt presentations if needed
            for record in records:
                if self._is_encrypted(record.presentation):
                    record.presentation = await self._decrypt_presentation(record.presentation)
            
            return records
    
    async def record_verification(
        self,
        presentation_id: str,
        verifier: str,
        result: str,
        details: Dict[str, Any]
    ) -> VerificationRecord:
        """Record a verification attempt"""
        import uuid
        
        verification = VerificationRecord(
            id=str(uuid.uuid4()),
            presentation_id=presentation_id,
            verifier=verifier,
            result=result,
            details=details
        )
        
        async with self.async_session() as session:
            session.add(verification)
            await session.commit()
            await session.refresh(verification)
            
        return verification
    
    async def get_verification_history(
        self,
        presentation_id: Optional[str] = None,
        verifier: Optional[str] = None,
        limit: int = 100
    ) -> List[VerificationRecord]:
        """Get verification history"""
        async with self.async_session() as session:
            query = select(VerificationRecord)
            
            if presentation_id:
                query = query.where(VerificationRecord.presentation_id == presentation_id)
            
            if verifier:
                query = query.where(VerificationRecord.verifier == verifier)
            
            query = query.order_by(VerificationRecord.timestamp.desc())
            query = query.limit(limit)
            
            result = await session.execute(query)
            return result.scalars().all()
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics"""
        async with self.async_session() as session:
            # Total presentations
            total_result = await session.execute(
                select(func.count(PresentationRecord.presentation_id))
            )
            total_presentations = total_result.scalar()
            
            # Presentations by status
            status_result = await session.execute(
                select(
                    PresentationRecord.status,
                    func.count(PresentationRecord.presentation_id)
                ).group_by(PresentationRecord.status)
            )
            status_counts = {row[0]: row[1] for row in status_result}
            
            # Unique holders
            holders_result = await session.execute(
                select(func.count(func.distinct(PresentationRecord.holder_did)))
            )
            unique_holders = holders_result.scalar()
            
            # Unique verifiers
            verifiers_result = await session.execute(
                select(func.count(func.distinct(PresentationRecord.verifier_did)))
            )
            unique_verifiers = verifiers_result.scalar()
            
            # Total verifications
            verifications_result = await session.execute(
                select(func.count(VerificationRecord.id))
            )
            total_verifications = verifications_result.scalar()
            
            # Verification results
            verification_results = await session.execute(
                select(
                    VerificationRecord.result,
                    func.count(VerificationRecord.id)
                ).group_by(VerificationRecord.result)
            )
            verification_counts = {row[0]: row[1] for row in verification_results}
            
            return {
                "total_presentations": total_presentations,
                "by_status": status_counts,
                "unique_holders": unique_holders,
                "unique_verifiers": unique_verifiers,
                "total_verifications": total_verifications,
                "verification_results": verification_counts
            }
    
    async def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[PresentationRecord]:
        """Search presentations"""
        async with self.async_session() as session:
            stmt = select(PresentationRecord)
            
            # Apply search query
            if query:
                stmt = stmt.where(
                    PresentationRecord.presentation_id.ilike(f"%{query}%") |
                    PresentationRecord.holder_did.ilike(f"%{query}%") |
                    PresentationRecord.verifier_did.ilike(f"%{query}%") |
                    PresentationRecord.session_id.ilike(f"%{query}%")
                )
            
            # Apply filters
            if filters:
                if "status" in filters:
                    stmt = stmt.where(PresentationRecord.status == filters["status"])
                if "holder_did" in filters:
                    stmt = stmt.where(PresentationRecord.holder_did == filters["holder_did"])
                if "verifier_did" in filters:
                    stmt = stmt.where(PresentationRecord.verifier_did == filters["verifier_did"])
                if "start_date" in filters:
                    stmt = stmt.where(PresentationRecord.created_at >= filters["start_date"])
                if "end_date" in filters:
                    stmt = stmt.where(PresentationRecord.created_at <= filters["end_date"])
            
            # Apply pagination
            stmt = stmt.order_by(PresentationRecord.created_at.desc())
            stmt = stmt.limit(limit).offset(offset)
            
            result = await session.execute(stmt)
            records = result.scalars().all()
            
            # Decrypt presentations if needed
            for record in records:
                if self._is_encrypted(record.presentation):
                    record.presentation = await self._decrypt_presentation(record.presentation)
            
            return records
    
    def _is_encrypted(self, data: Dict[str, Any]) -> bool:
        """Check if data is encrypted"""
        return isinstance(data, dict) and data.get("encrypted") == True
    
    async def _decrypt_presentation(self, encrypted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt presentation data"""
        if not self.vault_client or not self._is_encrypted(encrypted_data):
            return encrypted_data
        
        try:
            import base64
            
            # Decrypt using Vault
            decrypted_response = self.vault_client.secrets.transit.decrypt_data(
                name='presentation-encryption',
                ciphertext=encrypted_data['ciphertext']
            )
            
            # Decode from base64 and parse JSON
            plaintext = base64.b64decode(decrypted_response['data']['plaintext']).decode()
            return json.loads(plaintext)
            
        except Exception as e:
            print(f"Failed to decrypt presentation: {str(e)}")
            return encrypted_data 