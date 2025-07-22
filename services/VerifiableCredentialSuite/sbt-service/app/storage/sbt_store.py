"""
SoulBound Token Storage
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, JSON, Integer, Boolean, Index, select, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class SBTRecord(Base):
    """SBT database model"""
    __tablename__ = "sbt_records"
    
    id = Column(String, primary_key=True)
    credential_id = Column(String, nullable=False, index=True)
    token_id = Column(String, index=True)
    chain = Column(String, nullable=False, index=True)
    contract_address = Column(String, nullable=False)
    recipient = Column(String, nullable=False, index=True)
    issuer = Column(String, nullable=False, index=True)
    metadata_uri = Column(String)
    metadata = Column(JSON)
    status = Column(String, nullable=False, index=True)
    minted_at = Column(DateTime(timezone=True))
    transaction_hash = Column(String)
    revocation_date = Column(DateTime(timezone=True))
    revocation_reason = Column(String)
    revoked_by = Column(String)
    revocation_tx_hash = Column(String)
    burn_date = Column(DateTime(timezone=True))
    burn_tx_hash = Column(String)
    error = Column(String)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Indexes
    __table_args__ = (
        Index('idx_credential_status', 'credential_id', 'status'),
        Index('idx_recipient_chain', 'recipient', 'chain'),
        Index('idx_issuer_status', 'issuer', 'status'),
    )


class TransferAttempt(Base):
    """Transfer attempt record"""
    __tablename__ = "transfer_attempts"
    
    id = Column(String, primary_key=True)
    sbt_id = Column(String, nullable=False, index=True)
    from_address = Column(String, nullable=False)
    to_address = Column(String, nullable=False)
    result = Column(String, nullable=False)
    transaction_hash = Column(String)
    error_message = Column(String)
    timestamp = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Index
    __table_args__ = (
        Index('idx_sbt_timestamp', 'sbt_id', 'timestamp'),
    )


class SBTStore:
    """
    Manages SBT storage operations
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
        credential_id: str,
        token_id: Optional[str],
        chain: str,
        contract_address: str,
        recipient: str,
        issuer: str,
        metadata_uri: str,
        metadata: Dict[str, Any],
        status: str
    ) -> SBTRecord:
        """Create new SBT record"""
        import uuid
        
        sbt = SBTRecord(
            id=str(uuid.uuid4()),
            credential_id=credential_id,
            token_id=token_id,
            chain=chain,
            contract_address=contract_address,
            recipient=recipient.lower(),
            issuer=issuer,
            metadata_uri=metadata_uri,
            metadata=metadata,
            status=status
        )
        
        async with self.async_session() as session:
            session.add(sbt)
            await session.commit()
            await session.refresh(sbt)
            
        return sbt
    
    async def get(self, sbt_id: str) -> Optional[SBTRecord]:
        """Get SBT by ID"""
        async with self.async_session() as session:
            result = await session.execute(
                select(SBTRecord).where(SBTRecord.id == sbt_id)
            )
            return result.scalar_one_or_none()
    
    async def get_by_credential_id(self, credential_id: str) -> Optional[SBTRecord]:
        """Get SBT by credential ID"""
        async with self.async_session() as session:
            result = await session.execute(
                select(SBTRecord).where(
                    SBTRecord.credential_id == credential_id
                ).order_by(SBTRecord.created_at.desc())
            )
            return result.scalar_one_or_none()
    
    async def get_by_token_id(self, chain: str, token_id: str) -> Optional[SBTRecord]:
        """Get SBT by chain and token ID"""
        async with self.async_session() as session:
            result = await session.execute(
                select(SBTRecord).where(
                    SBTRecord.chain == chain,
                    SBTRecord.token_id == token_id
                )
            )
            return result.scalar_one_or_none()
    
    async def get_by_recipient(
        self,
        recipient: str,
        chain: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[SBTRecord]:
        """Get all SBTs for a recipient"""
        async with self.async_session() as session:
            query = select(SBTRecord).where(
                SBTRecord.recipient == recipient.lower()
            )
            
            if chain:
                query = query.where(SBTRecord.chain == chain)
            
            if status:
                query = query.where(SBTRecord.status == status)
            
            query = query.order_by(SBTRecord.created_at.desc())
            
            result = await session.execute(query)
            return result.scalars().all()
    
    async def get_by_issuer(
        self,
        issuer: str,
        chain: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[SBTRecord]:
        """Get all SBTs issued by an issuer"""
        async with self.async_session() as session:
            query = select(SBTRecord).where(
                SBTRecord.issuer == issuer
            )
            
            if chain:
                query = query.where(SBTRecord.chain == chain)
            
            if status:
                query = query.where(SBTRecord.status == status)
            
            query = query.order_by(SBTRecord.created_at.desc())
            
            result = await session.execute(query)
            return result.scalars().all()
    
    async def update(
        self,
        sbt_id: str,
        **kwargs
    ) -> Optional[SBTRecord]:
        """Update SBT record"""
        async with self.async_session() as session:
            result = await session.execute(
                select(SBTRecord).where(SBTRecord.id == sbt_id)
            )
            sbt = result.scalar_one_or_none()
            
            if not sbt:
                return None
            
            # Update fields
            for key, value in kwargs.items():
                if hasattr(sbt, key):
                    setattr(sbt, key, value)
            
            sbt.updated_at = datetime.now(timezone.utc)
            
            await session.commit()
            await session.refresh(sbt)
            
            return sbt
    
    async def record_transfer_attempt(
        self,
        sbt_id: str,
        from_address: str,
        to_address: str,
        result: str,
        transaction_hash: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> TransferAttempt:
        """Record a transfer attempt"""
        import uuid
        
        attempt = TransferAttempt(
            id=str(uuid.uuid4()),
            sbt_id=sbt_id,
            from_address=from_address.lower(),
            to_address=to_address.lower(),
            result=result,
            transaction_hash=transaction_hash,
            error_message=error_message
        )
        
        async with self.async_session() as session:
            session.add(attempt)
            await session.commit()
            await session.refresh(attempt)
            
        return attempt
    
    async def get_transfer_attempts(
        self,
        sbt_id: str,
        limit: int = 100
    ) -> List[TransferAttempt]:
        """Get transfer attempts for an SBT"""
        async with self.async_session() as session:
            result = await session.execute(
                select(TransferAttempt)
                .where(TransferAttempt.sbt_id == sbt_id)
                .order_by(TransferAttempt.timestamp.desc())
                .limit(limit)
            )
            return result.scalars().all()
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get storage statistics"""
        async with self.async_session() as session:
            # Total SBTs
            total_result = await session.execute(
                select(func.count(SBTRecord.id))
            )
            total_sbts = total_result.scalar()
            
            # SBTs by status
            status_result = await session.execute(
                select(
                    SBTRecord.status,
                    func.count(SBTRecord.id)
                ).group_by(SBTRecord.status)
            )
            status_counts = {row[0]: row[1] for row in status_result}
            
            # SBTs by chain
            chain_result = await session.execute(
                select(
                    SBTRecord.chain,
                    func.count(SBTRecord.id)
                ).group_by(SBTRecord.chain)
            )
            chain_counts = {row[0]: row[1] for row in chain_result}
            
            # Transfer attempts
            transfer_result = await session.execute(
                select(func.count(TransferAttempt.id))
            )
            total_transfer_attempts = transfer_result.scalar()
            
            return {
                "total_sbts": total_sbts,
                "by_status": status_counts,
                "by_chain": chain_counts,
                "total_transfer_attempts": total_transfer_attempts
            }
    
    async def search(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[SBTRecord]:
        """Search SBTs"""
        async with self.async_session() as session:
            stmt = select(SBTRecord)
            
            # Apply search query
            if query:
                stmt = stmt.where(
                    SBTRecord.recipient.ilike(f"%{query}%") |
                    SBTRecord.issuer.ilike(f"%{query}%") |
                    SBTRecord.credential_id.ilike(f"%{query}%") |
                    SBTRecord.token_id.ilike(f"%{query}%")
                )
            
            # Apply filters
            if filters:
                if "chain" in filters:
                    stmt = stmt.where(SBTRecord.chain == filters["chain"])
                if "status" in filters:
                    stmt = stmt.where(SBTRecord.status == filters["status"])
                if "issuer" in filters:
                    stmt = stmt.where(SBTRecord.issuer == filters["issuer"])
                if "recipient" in filters:
                    stmt = stmt.where(SBTRecord.recipient == filters["recipient"].lower())
            
            # Apply pagination
            stmt = stmt.order_by(SBTRecord.created_at.desc())
            stmt = stmt.limit(limit).offset(offset)
            
            result = await session.execute(stmt)
            return result.scalars().all() 