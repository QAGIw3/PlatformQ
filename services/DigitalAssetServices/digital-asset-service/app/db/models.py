import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey, Table, TypeDecorator, JSON, LargeBinary, Integer, BigInteger, Boolean, Numeric
from sqlalchemy.orm import relationship, Mapped
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint
from ..postgres_db import Base

# --- Compatibility Type for Testing ---
# This decorator allows our model to use JSONB in PostgreSQL
# but fall back to a standard JSON type in other databases like SQLite for testing.
class JsonBCompat(TypeDecorator):
    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(JSONB())
        else:
            return dialect.type_descriptor(JSON())

# Association Table for the many-to-many relationship between assets
asset_links = Table('asset_links', Base.metadata,
    Column('source_asset_cid', String, ForeignKey('digital_assets.cid'), primary_key=True),
    Column('target_asset_cid', String, ForeignKey('digital_assets.cid'), primary_key=True),
    Column('link_type', String, nullable=False)
)

class AssetProcessingRule(Base):
    __tablename__ = 'asset_processing_rules'

    # Using asset_type as the key for the rule for simplicity.
    # A composite key with tenant_id would be needed for tenant-specific rules.
    asset_type = Column(String, primary_key=True, index=True)
    wasm_module_id = Column(String, nullable=False, comment="The ID/name of the WASM module in the functions-service")
    
    # Auditability
    tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_by_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship to DigitalAsset
    # This allows us to potentially look up assets by their rule
    assets = relationship("DigitalAsset", back_populates="processing_rule")


class DigitalAsset(Base):
    __tablename__ = 'digital_assets'

    # Core Fields
    cid = Column(String, primary_key=True, index=True)
    asset_name = Column(String, nullable=False)
    asset_type = Column(String, ForeignKey('asset_processing_rules.asset_type'), nullable=False, index=True)
    owner_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Source & Raw Data
    source_tool = Column(String)
    source_asset_id = Column(String)
    raw_data_uri = Column(String)

    # Flexible Metadata using JSONB
    tags = Column(JsonBCompat)
    asset_metadata = Column("metadata", JsonBCompat)
    payload_schema_version = Column(String, nullable=True) # New field
    payload = Column(LargeBinary, nullable=True) # New field for Avro-serialized data
    
    # Verifiable Credential fields
    creation_vc_id = Column(String, nullable=True)
    lineage_vc_ids = Column(JsonBCompat, default=list)
    latest_processing_vc_id = Column(String, nullable=True)
    
    # Marketplace fields
    is_for_sale = Column(Boolean, default=False)
    sale_price = Column(Numeric(20, 6), nullable=True)  # Price in ETH/MATIC
    is_licensable = Column(Boolean, default=False)
    license_terms = Column(JsonBCompat, nullable=True)  # {duration, price, type, etc.}
    royalty_percentage = Column(Integer, default=250)  # Basis points (250 = 2.5%)
    blockchain_address = Column(String, nullable=True)  # Owner's blockchain address
    smart_contract_addresses = Column(JsonBCompat, default=dict)  # {royalty: "0x...", license: "0x..."}
    nft_token_id = Column(BigInteger, nullable=True)  # NFT token ID on the blockchain
    version = Column(Integer, default=0, nullable=False)

    # Relationships
    processing_rule = relationship("AssetProcessingRule", back_populates="assets")
    
    # This relationship represents the assets linked *from* this asset.
    links: Mapped[list["DigitalAsset"]] = relationship(
        "DigitalAsset",
        secondary=asset_links,
        primaryjoin=cid == asset_links.c.source_asset_cid,
        secondaryjoin=cid == asset_links.c.target_asset_cid,
        back_populates="linked_from"
    )

    # This relationship represents the assets linked *to* this asset.
    linked_from: Mapped[list["DigitalAsset"]] = relationship(
        "DigitalAsset",
        secondary=asset_links,
        primaryjoin=cid == asset_links.c.target_asset_cid,
        secondaryjoin=cid == asset_links.c.source_asset_cid,
        back_populates="links"
    )
    
    # CAD collaboration relationships
    cad_sessions = relationship("CADSession", back_populates="asset")
    geometry_versions = relationship("GeometryVersion", back_populates="asset")


class CADSession(Base):
    __tablename__ = 'cad_sessions'
    
    session_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_cid = Column(String, ForeignKey('digital_assets.cid'), nullable=False)
    tenant_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_by = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    closed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Session state
    active_users = Column(JsonBCompat, default=list)
    crdt_state = Column(LargeBinary, nullable=True)  # Compressed CRDT state
    vector_clock = Column(JsonBCompat, default=dict)
    operation_count = Column(Integer, default=0)
    last_checkpoint_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Session metadata
    session_metadata = Column(JsonBCompat, default=dict)
    
    # Relationships
    asset = relationship("DigitalAsset", back_populates="cad_sessions")
    checkpoints = relationship("CADCheckpoint", back_populates="session")


class CADCheckpoint(Base):
    __tablename__ = 'cad_checkpoints'
    
    checkpoint_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    session_id = Column(UUID(as_uuid=True), ForeignKey('cad_sessions.session_id'), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Checkpoint data
    snapshot_uri = Column(String, nullable=False)  # MinIO URI
    operation_range_start = Column(String, nullable=False)
    operation_range_end = Column(String, nullable=False)
    operation_count = Column(Integer, nullable=False)
    crdt_state_size = Column(BigInteger, nullable=False)
    
    # Relationships
    session = relationship("CADSession", back_populates="checkpoints")


class GeometryVersion(Base):
    __tablename__ = 'geometry_versions'
    
    version_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_cid = Column(String, ForeignKey('digital_assets.cid'), nullable=False)
    version_number = Column(Integer, nullable=False)
    created_by = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Version data
    geometry_snapshot_uri = Column(String, nullable=True)  # Full snapshot in MinIO
    geometry_diff = Column(LargeBinary, nullable=True)  # Compressed diff from previous
    parent_version_id = Column(UUID(as_uuid=True), nullable=True)
    
    # Statistics
    vertex_count = Column(Integer, nullable=True)
    edge_count = Column(Integer, nullable=True)
    face_count = Column(Integer, nullable=True)
    file_size_bytes = Column(BigInteger, nullable=True)
    
    # Metadata
    version_metadata = Column(JsonBCompat, default=dict)
    commit_message = Column(String, nullable=True)
    
    # Relationships
    asset = relationship("DigitalAsset", back_populates="geometry_versions")
    
    # Add unique constraint to ensure version numbers are unique per asset
    __table_args__ = (
        UniqueConstraint('asset_cid', 'version_number', name='_asset_version_uc'),
    ) 