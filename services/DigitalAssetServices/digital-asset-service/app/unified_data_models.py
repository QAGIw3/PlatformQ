"""Unified data models for digital asset service

This demonstrates how to migrate from SQLAlchemy models to unified data models.
"""

from platformq_unified_data.models import (
    BaseModel,
    StringField,
    IntegerField,
    FloatField,
    DateTimeField,
    JSONField,
    UUIDField,
    BooleanField,
    ListField
)


class UnifiedDigitalAsset(BaseModel):
    """Digital asset model using unified data access"""
    __tablename__ = "digital_assets"
    __keyspace__ = "platformq"
    __stores__ = ["cassandra", "elasticsearch"]  # Primary in Cassandra, searchable in ES
    __cache_ttl__ = 1800  # 30 minutes
    
    # Primary key
    id = UUIDField(primary_key=True)
    
    # Core fields
    name = StringField(required=True, indexed=True, max_length=255)
    description = StringField(max_length=2000)
    asset_type = StringField(required=True, indexed=True, choices=["model", "document", "image", "video", "audio", "dataset"])
    file_format = StringField(indexed=True)
    file_size = IntegerField()
    
    # Ownership and access
    owner_id = StringField(required=True, indexed=True)
    tenant_id = StringField(required=True, indexed=True)
    is_public = BooleanField(default=False)
    access_level = StringField(choices=["private", "team", "organization", "public"], default="private")
    
    # Content addressing
    cid = StringField(unique=True, indexed=True)  # IPFS CID
    file_hash = StringField(indexed=True)
    storage_path = StringField()
    
    # Metadata
    metadata = JSONField()  # Flexible metadata
    tags = ListField()  # For search
    categories = ListField()
    
    # Versioning
    version = IntegerField(default=1)
    previous_version_id = UUIDField()
    
    # Timestamps
    created_at = DateTimeField(auto_now_add=True, indexed=True)
    updated_at = DateTimeField(auto_now=True)
    
    # Blockchain anchoring
    blockchain_tx_hash = StringField()
    blockchain_anchored_at = DateTimeField()
    
    # Status
    status = StringField(
        choices=["draft", "processing", "active", "archived", "deleted"],
        default="draft",
        indexed=True
    )
    
    # Analytics
    view_count = IntegerField(default=0)
    download_count = IntegerField(default=0)
    last_accessed_at = DateTimeField()


class UnifiedAssetLineage(BaseModel):
    """Asset lineage tracking model"""
    __tablename__ = "asset_lineage"
    __keyspace__ = "platformq"
    __stores__ = ["cassandra", "janusgraph"]  # Also stored in graph for traversal
    __cache_ttl__ = 3600
    
    id = UUIDField(primary_key=True)
    
    # Lineage relationship
    parent_asset_id = UUIDField(required=True, indexed=True)
    child_asset_id = UUIDField(required=True, indexed=True)
    relationship_type = StringField(
        required=True,
        choices=["derived_from", "version_of", "component_of", "transformed_from"],
        indexed=True
    )
    
    # Provenance details
    transformation_details = JSONField()
    tool_used = StringField()
    parameters_used = JSONField()
    
    # Actor information
    created_by = StringField(required=True, indexed=True)
    created_at = DateTimeField(auto_now_add=True, indexed=True)
    
    # Blockchain anchoring
    blockchain_tx_hash = StringField()
    blockchain_event_index = IntegerField()


class UnifiedProvenanceCertificate(BaseModel):
    """Provenance certificate for verifiable asset history"""
    __tablename__ = "provenance_certificates"
    __keyspace__ = "platformq"
    __stores__ = ["cassandra"]
    __cache_ttl__ = 7200  # 2 hours
    
    id = UUIDField(primary_key=True)
    asset_id = UUIDField(required=True, indexed=True)
    
    # Certificate data
    certificate_hash = StringField(unique=True, indexed=True)
    certificate_data = JSONField(required=True)
    signature = StringField(required=True)
    signer_public_key = StringField(required=True)
    
    # Validity
    issued_at = DateTimeField(auto_now_add=True)
    expires_at = DateTimeField()
    is_revoked = BooleanField(default=False)
    
    # Blockchain proof
    blockchain_proof = JSONField()
    merkle_root = StringField()
    merkle_proof = ListField()


class UnifiedAssetMetrics(BaseModel):
    """Real-time metrics for assets"""
    __tablename__ = "asset_metrics"
    __keyspace__ = "platformq"
    __stores__ = ["ignite"]  # In-memory for fast access
    __cache_ttl__ = 300  # 5 minutes
    
    id = UUIDField(primary_key=True)
    asset_id = UUIDField(required=True, indexed=True)
    
    # Real-time metrics
    current_viewers = IntegerField(default=0)
    processing_jobs = IntegerField(default=0)
    quality_score = FloatField()
    
    # Aggregated metrics (last hour)
    views_last_hour = IntegerField(default=0)
    downloads_last_hour = IntegerField(default=0)
    transformations_last_hour = IntegerField(default=0)
    
    # Performance metrics
    avg_load_time_ms = FloatField()
    avg_processing_time_ms = FloatField()
    
    # Updated frequently
    last_updated = DateTimeField(auto_now=True) 