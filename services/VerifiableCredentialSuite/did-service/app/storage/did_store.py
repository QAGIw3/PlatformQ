"""
DID Document Storage Layer
"""

import json
import base64
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from sqlalchemy import Column, String, JSON, DateTime, Boolean, Integer, select, and_, func, or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool

Base = declarative_base()


class DIDDocument(Base):
    """DID Document database model"""
    __tablename__ = "did_documents"
    
    did = Column(String, primary_key=True, index=True)
    method = Column(String, index=True, nullable=False)
    controller = Column(String, index=True)
    did_document = Column(JSON, nullable=False)
    metadata = Column(JSON)
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    
    # Encryption fields
    encrypted = Column(Boolean, default=False)
    encryption_key_version = Column(String)


class DIDStore:
    """
    Manages persistent storage of DID documents with encryption
    """
    
    def __init__(
        self,
        database_url: str,
        vault_client: Optional[Any] = None,
        encryption_enabled: bool = True
    ):
        self.database_url = database_url
        self.vault_client = vault_client
        self.encryption_enabled = encryption_enabled and vault_client is not None
        
        # Database engine and session
        self.engine = None
        self.async_session = None
        
    async def initialize(self):
        """Initialize database connection and create tables"""
        # Create async engine
        self.engine = create_async_engine(
            self.database_url,
            echo=False,
            poolclass=NullPool  # Disable connection pooling for async
        )
        
        # Create session factory
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # Create tables if they don't exist
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def close(self):
        """Close database connections"""
        if self.engine:
            await self.engine.dispose()
    
    async def health_check(self) -> bool:
        """Check if storage is healthy"""
        try:
            async with self.async_session() as session:
                # Simple query to check connection
                result = await session.execute(select(func.count()).select_from(DIDDocument))
                count = result.scalar()
                return count is not None
        except Exception:
            return False
    
    def _extract_method(self, did: str) -> str:
        """Extract DID method from DID string"""
        parts = did.split(":")
        return parts[1] if len(parts) >= 3 else "unknown"
    
    def _extract_controller(self, did_document: Dict[str, Any]) -> Optional[str]:
        """Extract controller from DID document"""
        controller = did_document.get("controller")
        if isinstance(controller, str):
            return controller
        elif isinstance(controller, list) and controller:
            return controller[0]  # Take first controller
        return None
    
    async def _encrypt_document(self, did_document: Dict[str, Any]) -> tuple[str, str]:
        """Encrypt DID document using Vault"""
        if not self.encryption_enabled:
            return json.dumps(did_document), None
        
        try:
            # Convert document to JSON string
            plaintext = json.dumps(did_document)
            
            # Encrypt using Vault Transit engine
            response = self.vault_client.secrets.transit.encrypt_data(
                name="did-documents",
                plaintext=base64.b64encode(plaintext.encode()).decode()
            )
            
            ciphertext = response["data"]["ciphertext"]
            key_version = str(response["data"]["key_version"])
            
            return ciphertext, key_version
            
        except Exception as e:
            print(f"Encryption failed, storing in plaintext: {str(e)}")
            return json.dumps(did_document), None
    
    async def _decrypt_document(self, ciphertext: str, key_version: str = None) -> Dict[str, Any]:
        """Decrypt DID document using Vault"""
        if not self.encryption_enabled:
            return json.loads(ciphertext) if isinstance(ciphertext, str) else ciphertext
        
        try:
            # If it's already a dict (not encrypted), return it
            if isinstance(ciphertext, dict):
                return ciphertext
            
            # Check if it's encrypted (starts with vault:v)
            if not ciphertext.startswith("vault:v"):
                return json.loads(ciphertext)
            
            # Decrypt using Vault Transit engine
            response = self.vault_client.secrets.transit.decrypt_data(
                name="did-documents",
                ciphertext=ciphertext
            )
            
            plaintext = base64.b64decode(response["data"]["plaintext"]).decode()
            return json.loads(plaintext)
            
        except Exception as e:
            print(f"Decryption failed: {str(e)}")
            # Try to parse as plain JSON
            try:
                return json.loads(ciphertext) if isinstance(ciphertext, str) else ciphertext
            except:
                return ciphertext
    
    async def store_did_document(
        self,
        did: str,
        did_document: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Store a DID document"""
        async with self.async_session() as session:
            # Encrypt document if enabled
            if self.encryption_enabled:
                encrypted_doc, key_version = await self._encrypt_document(did_document)
                doc_to_store = encrypted_doc
                is_encrypted = True
            else:
                doc_to_store = did_document
                key_version = None
                is_encrypted = False
            
            # Extract method and controller
            method = self._extract_method(did)
            controller = self._extract_controller(did_document)
            
            # Create or update record
            existing = await session.get(DIDDocument, did)
            
            if existing:
                # Update existing
                existing.did_document = doc_to_store
                existing.metadata = metadata
                existing.method = method
                existing.controller = controller
                existing.encrypted = is_encrypted
                existing.encryption_key_version = key_version
                existing.updated_at = datetime.now(timezone.utc)
            else:
                # Create new
                new_doc = DIDDocument(
                    did=did,
                    method=method,
                    controller=controller,
                    did_document=doc_to_store,
                    metadata=metadata,
                    encrypted=is_encrypted,
                    encryption_key_version=key_version
                )
                session.add(new_doc)
            
            await session.commit()
    
    async def get_did_document(self, did: str) -> Optional[Dict[str, Any]]:
        """Get a DID document"""
        async with self.async_session() as session:
            result = await session.get(DIDDocument, did)
            
            if not result or not result.is_active:
                return None
            
            # Decrypt if encrypted
            if result.encrypted:
                did_document = await self._decrypt_document(
                    result.did_document,
                    result.encryption_key_version
                )
            else:
                did_document = result.did_document
            
            return {
                "did": result.did,
                "did_document": did_document,
                "metadata": result.metadata,
                "created_at": result.created_at,
                "updated_at": result.updated_at
            }
    
    async def update_did_document(
        self,
        did: str,
        did_document: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Update a DID document"""
        async with self.async_session() as session:
            existing = await session.get(DIDDocument, did)
            
            if not existing:
                raise ValueError(f"DID not found: {did}")
            
            # Encrypt document if enabled
            if self.encryption_enabled:
                encrypted_doc, key_version = await self._encrypt_document(did_document)
                doc_to_store = encrypted_doc
                is_encrypted = True
            else:
                doc_to_store = did_document
                key_version = None
                is_encrypted = False
            
            # Update fields
            existing.did_document = doc_to_store
            existing.encrypted = is_encrypted
            existing.encryption_key_version = key_version
            existing.controller = self._extract_controller(did_document)
            
            if metadata:
                existing.metadata = metadata
                
            existing.updated_at = datetime.now(timezone.utc)
            
            await session.commit()
    
    async def deactivate_did(self, did: str):
        """Deactivate a DID"""
        async with self.async_session() as session:
            existing = await session.get(DIDDocument, did)
            
            if not existing:
                raise ValueError(f"DID not found: {did}")
            
            existing.is_active = False
            existing.updated_at = datetime.now(timezone.utc)
            
            await session.commit()
    
    async def list_dids(
        self,
        method: Optional[str] = None,
        controller: Optional[str] = None,
        active_only: bool = True,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """List DIDs with filtering and pagination"""
        async with self.async_session() as session:
            # Build query
            query = select(DIDDocument)
            
            # Apply filters
            conditions = []
            
            if active_only:
                conditions.append(DIDDocument.is_active == True)
                
            if method:
                conditions.append(DIDDocument.method == method)
                
            if controller:
                conditions.append(DIDDocument.controller == controller)
            
            if conditions:
                query = query.where(and_(*conditions))
            
            # Count total
            count_query = select(func.count()).select_from(DIDDocument)
            if conditions:
                count_query = count_query.where(and_(*conditions))
                
            total_result = await session.execute(count_query)
            total = total_result.scalar()
            
            # Apply pagination
            offset = (page - 1) * page_size
            query = query.offset(offset).limit(page_size)
            query = query.order_by(DIDDocument.created_at.desc())
            
            # Execute query
            result = await session.execute(query)
            records = result.scalars().all()
            
            # Format results
            dids = []
            for record in records:
                # Decrypt if needed
                if record.encrypted:
                    did_document = await self._decrypt_document(
                        record.did_document,
                        record.encryption_key_version
                    )
                else:
                    did_document = record.did_document
                
                dids.append({
                    "did": record.did,
                    "did_document": did_document,
                    "metadata": record.metadata,
                    "created_at": record.created_at,
                    "updated_at": record.updated_at
                })
            
            return {
                "dids": dids,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    
    async def search_dids(
        self,
        search_term: str,
        search_in: List[str] = ["did", "metadata"],
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """Search DIDs by term"""
        async with self.async_session() as session:
            # Build search conditions
            conditions = [DIDDocument.is_active == True]
            
            search_conditions = []
            if "did" in search_in:
                search_conditions.append(DIDDocument.did.contains(search_term))
                
            if "metadata" in search_in:
                # This is database-specific, example for PostgreSQL
                search_conditions.append(
                    func.cast(DIDDocument.metadata, String).contains(search_term)
                )
            
            if search_conditions:
                conditions.append(or_(*search_conditions))
            
            # Build query
            query = select(DIDDocument).where(and_(*conditions))
            
            # Count total
            count_query = select(func.count()).select_from(DIDDocument).where(and_(*conditions))
            total_result = await session.execute(count_query)
            total = total_result.scalar()
            
            # Apply pagination
            offset = (page - 1) * page_size
            query = query.offset(offset).limit(page_size)
            query = query.order_by(DIDDocument.created_at.desc())
            
            # Execute query
            result = await session.execute(query)
            records = result.scalars().all()
            
            # Format results
            dids = []
            for record in records:
                # Decrypt if needed
                if record.encrypted:
                    did_document = await self._decrypt_document(
                        record.did_document,
                        record.encryption_key_version
                    )
                else:
                    did_document = record.did_document
                
                dids.append({
                    "did": record.did,
                    "did_document": did_document,
                    "metadata": record.metadata,
                    "created_at": record.created_at,
                    "updated_at": record.updated_at
                })
            
            return {
                "dids": dids,
                "total": total,
                "page": page,
                "page_size": page_size
            }
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get DID statistics"""
        async with self.async_session() as session:
            # Total DIDs
            total_result = await session.execute(
                select(func.count()).select_from(DIDDocument)
            )
            total = total_result.scalar()
            
            # Active DIDs
            active_result = await session.execute(
                select(func.count()).select_from(DIDDocument).where(
                    DIDDocument.is_active == True
                )
            )
            active = active_result.scalar()
            
            # DIDs by method
            method_result = await session.execute(
                select(
                    DIDDocument.method,
                    func.count(DIDDocument.did).label("count")
                ).group_by(DIDDocument.method)
            )
            by_method = {row.method: row.count for row in method_result}
            
            return {
                "total": total,
                "active": active,
                "inactive": total - active,
                "by_method": by_method
            } 