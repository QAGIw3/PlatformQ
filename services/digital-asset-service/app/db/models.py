import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey, Table, TypeDecorator, JSON
from sqlalchemy.orm import relationship, Mapped
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
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
    Column('source_asset_id', UUID(as_uuid=True), ForeignKey('digital_assets.asset_id'), primary_key=True),
    Column('target_asset_id', UUID(as_uuid=True), ForeignKey('digital_assets.asset_id'), primary_key=True),
    Column('link_type', String, nullable=False)
)

class DigitalAsset(Base):
    __tablename__ = 'digital_assets'

    # Core Fields
    asset_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    asset_name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False, index=True)
    owner_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Source & Raw Data
    source_tool = Column(String)
    source_asset_id = Column(String)
    raw_data_uri = Column(String)

    # Flexible Metadata using JSONB
    tags = Column(JsonBCompat)
    asset_metadata = Column("metadata", JsonBCompat)

    # Relationships
    # This relationship represents the assets linked *from* this asset.
    links: Mapped[list["DigitalAsset"]] = relationship(
        "DigitalAsset",
        secondary=asset_links,
        primaryjoin=asset_id == asset_links.c.source_asset_id,
        secondaryjoin=asset_id == asset_links.c.target_asset_id,
        back_populates="linked_from"
    )

    # This relationship represents the assets linked *to* this asset.
    linked_from: Mapped[list["DigitalAsset"]] = relationship(
        "DigitalAsset",
        secondary=asset_links,
        primaryjoin=asset_id == asset_links.c.target_asset_id,
        secondaryjoin=asset_id == asset_links.c.source_asset_id,
        back_populates="links"
    ) 