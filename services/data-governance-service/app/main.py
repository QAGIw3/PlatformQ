from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Set, Tuple
import logging
from datetime import datetime, timedelta
import asyncio
from sqlalchemy import Column, String, DateTime, JSON, Float, Integer, Boolean, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker, relationship
from sqlalchemy import create_engine
import networkx as nx
import json
from enum import Enum
import pandas as pd
import numpy as np
from great_expectations import DataContext
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx

# Import Atlas and Ranger clients
from atlasclient.client import Atlas
from atlasclient.models import Entity, EntityDef, ClassificationDef

logger = logging.getLogger(__name__)

# Configuration
ATLAS_URL = "http://atlas:21000"
RANGER_URL = "http://ranger:6080"
TRINO_URL = "http://trino:8080"

# Database models
Base = declarative_base()

class DataAsset(Base):
    __tablename__ = "data_assets"
    
    asset_id = Column(String, primary_key=True)
    asset_name = Column(String, nullable=False)
    asset_type = Column(String, nullable=False)
    catalog = Column(String, nullable=False)
    schema = Column(String, nullable=False)
    table = Column(String, nullable=False)
    owner = Column(String)
    description = Column(String)
    tags = Column(JSON)
    metadata = Column(JSON)
    classification = Column(JSON)  # Sensitivity classifications
    atlas_guid = Column(String)  # Apache Atlas GUID
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    quality_profiles = relationship("DataQualityProfile", back_populates="asset")
    access_policies = relationship("AccessPolicy", back_populates="asset")

class DataLineage(Base):
    __tablename__ = "data_lineage"
    
    lineage_id = Column(String, primary_key=True)
    source_asset_id = Column(String, nullable=False)
    target_asset_id = Column(String, nullable=False)
    transformation_type = Column(String)
    transformation_details = Column(JSON)
    pipeline_id = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class DataQualityMetric(Base):
    __tablename__ = "data_quality_metrics"
    
    metric_id = Column(String, primary_key=True)
    asset_id = Column(String, nullable=False)
    metric_type = Column(String, nullable=False)
    metric_value = Column(Float, nullable=False)
    threshold = Column(Float)
    passed = Column(Boolean)
    details = Column(JSON)
    measured_at = Column(DateTime, default=datetime.utcnow)

class DataQualityProfile(Base):
    __tablename__ = "data_quality_profiles"
    
    profile_id = Column(String, primary_key=True)
    asset_id = Column(String, ForeignKey("data_assets.asset_id"))
    profile_date = Column(DateTime, default=datetime.utcnow)
    row_count = Column(Integer)
    column_profiles = Column(JSON)  # Detailed column statistics
    data_patterns = Column(JSON)  # Detected patterns
    anomalies = Column(JSON)  # Detected anomalies
    completeness_score = Column(Float)
    validity_score = Column(Float)
    consistency_score = Column(Float)
    
    # Relationship
    asset = relationship("DataAsset", back_populates="quality_profiles")

class DataPolicy(Base):
    __tablename__ = "data_policies"
    
    policy_id = Column(String, primary_key=True)
    policy_name = Column(String, nullable=False)
    policy_type = Column(String, nullable=False)
    rules = Column(JSON, nullable=False)
    applies_to = Column(JSON)  # List of asset patterns
    active = Column(Boolean, default=True)
    ranger_policy_id = Column(String)  # Apache Ranger policy ID
    created_at = Column(DateTime, default=datetime.utcnow)

class AccessPolicy(Base):
    __tablename__ = "access_policies"
    
    policy_id = Column(String, primary_key=True)
    asset_id = Column(String, ForeignKey("data_assets.asset_id"))
    policy_name = Column(String, nullable=False)
    policy_type = Column(String)  # read, write, admin
    principals = Column(JSON)  # Users/groups with access
    conditions = Column(JSON)  # Access conditions
    ranger_policy_id = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    asset = relationship("DataAsset", back_populates="access_policies")

class ComplianceCheck(Base):
    __tablename__ = "compliance_checks"
    
    check_id = Column(String, primary_key=True)
    asset_id = Column(String, nullable=False)
    compliance_type = Column(String)  # GDPR, CCPA, HIPAA, etc.
    check_date = Column(DateTime, default=datetime.utcnow)
    status = Column(String)  # passed, failed, warning
    findings = Column(JSON)
    remediation_required = Column(Boolean, default=False)
    remediation_details = Column(Text)

# Initialize database
DATABASE_URL = "postgresql://platformq:password@postgres:5432/data_governance"
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Initialize services
scheduler = AsyncIOScheduler()

# Atlas client
try:
    atlas_client = Atlas(ATLAS_URL, username="admin", password="admin")
except Exception as e:
    logger.warning(f"Failed to connect to Apache Atlas: {e}")
    atlas_client = None

# Presidio analyzer for PII detection
analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()

# Pydantic models
class DataAssetCreate(BaseModel):
    asset_name: str
    asset_type: str
    catalog: str
    schema: str
    table: str
    owner: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = []
    metadata: Optional[Dict[str, Any]] = {}

class LineageCreate(BaseModel):
    source_asset_id: str
    target_asset_id: str
    transformation_type: str
    transformation_details: Optional[Dict[str, Any]] = {}
    pipeline_id: Optional[str] = None

class QualityMetricCreate(BaseModel):
    asset_id: str
    metric_type: str = Field(..., description="completeness, accuracy, consistency, validity, uniqueness")
    metric_value: float = Field(..., ge=0, le=1)
    threshold: Optional[float] = Field(0.95, ge=0, le=1)
    details: Optional[Dict[str, Any]] = {}

class PolicyCreate(BaseModel):
    policy_name: str
    policy_type: str = Field(..., description="retention, access, quality, privacy")
    rules: Dict[str, Any]
    applies_to: List[str] = Field(..., description="Asset patterns like 'cassandra.*' or 'hive.bronze.*'")

class AccessPolicyCreate(BaseModel):
    asset_id: str
    policy_name: str
    policy_type: str = Field(..., description="read, write, admin")
    principals: List[str] = Field(..., description="Users or groups")
    conditions: Optional[Dict[str, Any]] = Field(None, description="Access conditions")

class DataClassification(BaseModel):
    classification_type: str = Field(..., description="public, internal, confidential, restricted")
    contains_pii: bool = False
    pii_types: Optional[List[str]] = []
    encryption_required: bool = False
    retention_days: Optional[int] = None

class DataCatalogEntry(BaseModel):
    asset_id: str
    asset_name: str
    full_name: str  # catalog.schema.table
    asset_type: str
    owner: Optional[str]
    description: Optional[str]
    tags: List[str]
    metadata: Dict[str, Any]
    classification: Optional[DataClassification]
    quality_score: Optional[float]
    last_updated: datetime
    upstream_count: int
    downstream_count: int
    access_level: Optional[str]

# Create FastAPI app
app = create_base_app(
    service_name="data-governance-service",
    db_session_dependency=get_db,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# Enhanced Lineage Graph Manager
class EnhancedLineageGraphManager:
    def __init__(self):
        self.graph = nx.DiGraph()
    
    def add_lineage(self, source: str, target: str, metadata: Dict[str, Any]):
        self.graph.add_edge(source, target, **metadata)
        
        # Sync with Atlas if available
        if atlas_client:
            try:
                self._sync_lineage_to_atlas(source, target, metadata)
            except Exception as e:
                logger.error(f"Failed to sync lineage to Atlas: {e}")
    
    def _sync_lineage_to_atlas(self, source: str, target: str, metadata: Dict[str, Any]):
        """Sync lineage to Apache Atlas"""
        # Create process entity in Atlas
        process_entity = Entity({
            "typeName": "Process",
            "attributes": {
                "name": f"{source}_to_{target}",
                "qualifiedName": f"{source}_to_{target}@platformq",
                "inputs": [{"guid": source}],
                "outputs": [{"guid": target}],
                "processType": metadata.get("transformation_type", "ETL")
            }
        })
        atlas_client.entity.create(process_entity)
    
    def get_upstream(self, asset_id: str, levels: int = -1) -> Set[str]:
        """Get all upstream assets"""
        if asset_id not in self.graph:
            return set()
        
        if levels == -1:
            return nx.ancestors(self.graph, asset_id)
        else:
            upstream = set()
            current = {asset_id}
            for _ in range(levels):
                next_level = set()
                for node in current:
                    next_level.update(self.graph.predecessors(node))
                upstream.update(next_level)
                current = next_level
            return upstream
    
    def get_downstream(self, asset_id: str, levels: int = -1) -> Set[str]:
        """Get all downstream assets"""
        if asset_id not in self.graph:
            return set()
        
        if levels == -1:
            return nx.descendants(self.graph, asset_id)
        else:
            downstream = set()
            current = {asset_id}
            for _ in range(levels):
                next_level = set()
                for node in current:
                    next_level.update(self.graph.successors(node))
                downstream.update(next_level)
                current = next_level
            return downstream
    
    def get_lineage_path(self, source: str, target: str) -> List[List[str]]:
        """Get all paths between two assets"""
        try:
            return list(nx.all_simple_paths(self.graph, source, target))
        except nx.NetworkXNoPath:
            return []
    
    def get_impact_analysis(self, asset_id: str) -> Dict[str, Any]:
        """Analyze the impact of changes to an asset"""
        downstream = self.get_downstream(asset_id)
        upstream = self.get_upstream(asset_id)
        
        # Calculate criticality score
        criticality_score = self._calculate_criticality(asset_id, downstream, upstream)
        
        return {
            "asset_id": asset_id,
            "directly_impacted": list(self.graph.successors(asset_id)),
            "total_downstream_impact": len(downstream),
            "downstream_assets": list(downstream),
            "upstream_dependencies": len(upstream),
            "upstream_assets": list(upstream),
            "criticality_score": criticality_score,
            "is_critical_path": criticality_score > 0.7
        }
    
    def _calculate_criticality(self, asset_id: str, downstream: Set[str], upstream: Set[str]) -> float:
        """Calculate asset criticality score"""
        # Factors: number of dependencies, centrality, critical downstream assets
        downstream_score = min(len(downstream) / 10, 1.0)  # Normalize to 0-1
        centrality = nx.betweenness_centrality(self.graph).get(asset_id, 0)
        
        return (downstream_score * 0.6 + centrality * 0.4)

# Global lineage manager
lineage_manager = EnhancedLineageGraphManager()

# Data Quality Manager
class DataQualityManager:
    def __init__(self):
        self.ge_context = None
        try:
            self.ge_context = DataContext()
        except:
            logger.warning("Great Expectations context not initialized")
    
    async def profile_data_asset(self, asset_id: str, connection_string: str) -> Dict[str, Any]:
        """Profile a data asset using pandas profiling and Great Expectations"""
        try:
            # For demo, using a simple profiling approach
            # In production, connect to actual data source
            profile = {
                "row_count": 10000,
                "column_profiles": {},
                "data_patterns": {},
                "anomalies": [],
                "completeness_score": 0.98,
                "validity_score": 0.95,
                "consistency_score": 0.97
            }
            
            # Detect anomalies using statistical methods
            anomalies = await self._detect_anomalies(asset_id)
            profile["anomalies"] = anomalies
            
            return profile
            
        except Exception as e:
            logger.error(f"Failed to profile asset {asset_id}: {e}")
            raise
    
    async def _detect_anomalies(self, asset_id: str) -> List[Dict[str, Any]]:
        """Detect data anomalies using ML"""
        # Placeholder for anomaly detection
        # In production, use Prophet or Isolation Forest
        return []
    
    def create_quality_expectations(self, asset_id: str, rules: Dict[str, Any]):
        """Create Great Expectations suite for an asset"""
        if not self.ge_context:
            return None
        
        # Create expectation suite
        suite_name = f"{asset_id}_expectations"
        suite = self.ge_context.create_expectation_suite(
            expectation_suite_name=suite_name,
            overwrite_existing=True
        )
        
        # Add expectations based on rules
        # This would be expanded based on specific requirements
        
        return suite_name

# Initialize quality manager
quality_manager = DataQualityManager()

# Data Classification Manager
class DataClassificationManager:
    def __init__(self):
        self.analyzer = analyzer
        self.anonymizer = anonymizer
        
    async def classify_data_asset(self, asset_id: str, sample_data: List[Dict[str, Any]]) -> DataClassification:
        """Classify data asset based on content"""
        contains_pii = False
        pii_types = set()
        
        # Analyze sample data for PII
        for record in sample_data[:100]:  # Analyze first 100 records
            for field, value in record.items():
                if isinstance(value, str):
                    results = self.analyzer.analyze(text=value, language='en')
                    if results:
                        contains_pii = True
                        pii_types.update([r.entity_type for r in results])
        
        # Determine classification level
        if contains_pii:
            if any(pii in ['CREDIT_CARD', 'US_SSN', 'MEDICAL_LICENSE'] for pii in pii_types):
                classification_type = "restricted"
                encryption_required = True
                retention_days = 365
            else:
                classification_type = "confidential"
                encryption_required = True
                retention_days = 730
        else:
            classification_type = "internal"
            encryption_required = False
            retention_days = 1095
        
        return DataClassification(
            classification_type=classification_type,
            contains_pii=contains_pii,
            pii_types=list(pii_types),
            encryption_required=encryption_required,
            retention_days=retention_days
        )
    
    async def anonymize_pii(self, text: str) -> str:
        """Anonymize PII in text"""
        results = self.analyzer.analyze(text=text, language='en')
        anonymized = self.anonymizer.anonymize(text=text, analyzer_results=results)
        return anonymized.text

# Initialize classification manager
classification_manager = DataClassificationManager()

# Apache Ranger Integration
class RangerPolicyManager:
    def __init__(self):
        self.ranger_url = RANGER_URL
        
    async def create_access_policy(self, asset_id: str, policy: AccessPolicyCreate) -> str:
        """Create access policy in Apache Ranger"""
        try:
            # Parse asset_id to get database and table
            parts = asset_id.split('.')
            if len(parts) >= 3:
                database = f"{parts[0]}.{parts[1]}"
                table = parts[2]
            else:
                database = parts[0] if len(parts) > 0 else "default"
                table = parts[1] if len(parts) > 1 else "*"
            
            ranger_policy = {
                "name": policy.policy_name,
                "service": "platformq_trino",
                "resources": {
                    "database": {"values": [database]},
                    "table": {"values": [table]},
                    "column": {"values": ["*"]}
                },
                "policyItems": [{
                    "users": policy.principals,
                    "accesses": [{"type": policy.policy_type, "isAllowed": True}],
                    "conditions": policy.conditions or []
                }],
                "isEnabled": True,
                "isAuditEnabled": True
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.ranger_url}/service/public/v2/api/policy",
                    json=ranger_policy,
                    auth=("admin", "admin")
                )
                
                if response.status_code == 200:
                    return response.json()["id"]
                else:
                    logger.error(f"Failed to create Ranger policy: {response.text}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error creating Ranger policy: {e}")
            return None
    
    async def update_policy(self, policy_id: str, updates: Dict[str, Any]):
        """Update existing Ranger policy"""
        # Implementation for policy updates
        pass
    
    async def delete_policy(self, policy_id: str):
        """Delete Ranger policy"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.delete(
                    f"{self.ranger_url}/service/public/v2/api/policy/{policy_id}",
                    auth=("admin", "admin")
                )
                return response.status_code == 204
        except Exception as e:
            logger.error(f"Error deleting Ranger policy: {e}")
            return False

# Initialize Ranger manager
ranger_manager = RangerPolicyManager()

# API Endpoints
@app.post("/api/v1/assets", response_model=DataAsset)
async def register_data_asset(
    asset: DataAssetCreate,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Register a new data asset in the catalog"""
    import uuid
    
    asset_id = f"{asset.catalog}.{asset.schema}.{asset.table}"
    
    # Check if exists
    existing = db.query(DataAsset).filter(DataAsset.asset_id == asset_id).first()
    if existing:
        # Update existing
        for key, value in asset.dict().items():
            setattr(existing, key, value)
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return existing
    
    # Create new asset
    db_asset = DataAsset(
        asset_id=asset_id,
        **asset.dict()
    )
    
    # Register in Apache Atlas if available
    if atlas_client:
        try:
            atlas_entity = Entity({
                "typeName": "hive_table",
                "attributes": {
                    "name": asset.table,
                    "qualifiedName": f"{asset_id}@platformq",
                    "owner": asset.owner,
                    "description": asset.description,
                    "db": {"qualifiedName": f"{asset.catalog}.{asset.schema}@platformq"}
                }
            })
            result = atlas_client.entity.create(atlas_entity)
            db_asset.atlas_guid = result.guid
        except Exception as e:
            logger.error(f"Failed to register in Atlas: {e}")
    
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    
    # Schedule initial profiling
    background_tasks.add_task(profile_asset_task, asset_id)
    
    return db_asset

# Background task for asset profiling
async def profile_asset_task(asset_id: str):
    """Background task to profile data asset"""
    try:
        profile = await quality_manager.profile_data_asset(asset_id, "")
        
        # Store profile in database
        db = SessionLocal()
        try:
            db_profile = DataQualityProfile(
                profile_id=str(uuid.uuid4()),
                asset_id=asset_id,
                **profile
            )
            db.add(db_profile)
            db.commit()
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"Failed to profile asset {asset_id}: {e}")

@app.get("/api/v1/assets")
async def list_data_assets(
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    asset_type: Optional[str] = None,
    classification: Optional[str] = None,
    tags: Optional[List[str]] = None,
    db: Session = Depends(get_db)
):
    """List data assets with filtering"""
    query = db.query(DataAsset)
    
    if catalog:
        query = query.filter(DataAsset.catalog == catalog)
    if schema:
        query = query.filter(DataAsset.schema == schema)
    if asset_type:
        query = query.filter(DataAsset.asset_type == asset_type)
    
    assets = query.all()
    
    # Filter by tags if provided
    if tags:
        assets = [a for a in assets if any(tag in (a.tags or []) for tag in tags)]
    
    # Filter by classification
    if classification:
        assets = [a for a in assets if a.classification and a.classification.get("classification_type") == classification]
    
    # Enrich with additional metadata
    enriched_assets = []
    for asset in assets:
        upstream_count = len(lineage_manager.get_upstream(asset.asset_id, levels=1))
        downstream_count = len(lineage_manager.get_downstream(asset.asset_id, levels=1))
        
        # Get latest quality score
        latest_quality = db.query(DataQualityProfile).filter(
            DataQualityProfile.asset_id == asset.asset_id
        ).order_by(DataQualityProfile.profile_date.desc()).first()
        
        quality_score = None
        if latest_quality:
            quality_score = (
                latest_quality.completeness_score * 0.4 +
                latest_quality.validity_score * 0.3 +
                latest_quality.consistency_score * 0.3
            )
        
        enriched_assets.append(DataCatalogEntry(
            asset_id=asset.asset_id,
            asset_name=asset.asset_name,
            full_name=asset.asset_id,
            asset_type=asset.asset_type,
            owner=asset.owner,
            description=asset.description,
            tags=asset.tags or [],
            metadata=asset.metadata or {},
            classification=DataClassification(**asset.classification) if asset.classification else None,
            quality_score=quality_score,
            last_updated=asset.updated_at,
            upstream_count=upstream_count,
            downstream_count=downstream_count,
            access_level="read"  # Would be determined by Ranger
        ))
    
    return {"assets": enriched_assets, "total": len(enriched_assets)}

@app.post("/api/v1/assets/{asset_id}/classify")
async def classify_asset(
    asset_id: str,
    sample_size: int = 100,
    db: Session = Depends(get_db)
):
    """Classify a data asset based on its content"""
    # In production, fetch actual data samples
    # For demo, using mock data
    sample_data = [
        {"name": "John Doe", "email": "john@example.com", "ssn": "123-45-6789"},
        {"name": "Jane Smith", "email": "jane@example.com", "phone": "555-1234"}
    ]
    
    classification = await classification_manager.classify_data_asset(asset_id, sample_data)
    
    # Update asset with classification
    asset = db.query(DataAsset).filter(DataAsset.asset_id == asset_id).first()
    if asset:
        asset.classification = classification.dict()
        db.commit()
        
        # Create retention policy if needed
        if classification.retention_days:
            policy = DataPolicy(
                policy_id=str(uuid.uuid4()),
                policy_name=f"Retention policy for {asset_id}",
                policy_type="retention",
                rules={
                    "retention_days": classification.retention_days,
                    "delete_after_retention": True
                },
                applies_to=[asset_id]
            )
            db.add(policy)
            db.commit()
    
    return {"asset_id": asset_id, "classification": classification}

@app.post("/api/v1/assets/{asset_id}/access-policy")
async def create_asset_access_policy(
    asset_id: str,
    policy: AccessPolicyCreate,
    db: Session = Depends(get_db)
):
    """Create access policy for an asset"""
    import uuid
    
    # Create policy in Ranger
    ranger_policy_id = await ranger_manager.create_access_policy(asset_id, policy)
    
    # Store in database
    db_policy = AccessPolicy(
        policy_id=str(uuid.uuid4()),
        ranger_policy_id=ranger_policy_id,
        **policy.dict()
    )
    db.add(db_policy)
    db.commit()
    
    return {
        "policy_id": db_policy.policy_id,
        "ranger_policy_id": ranger_policy_id,
        "message": f"Access policy '{policy.policy_name}' created"
    }

@app.post("/api/v1/lineage")
async def add_lineage(
    lineage: LineageCreate,
    db: Session = Depends(get_db)
):
    """Add lineage relationship between assets"""
    import uuid
    
    lineage_id = str(uuid.uuid4())
    
    # Store in database
    db_lineage = DataLineage(
        lineage_id=lineage_id,
        **lineage.dict()
    )
    db.add(db_lineage)
    db.commit()
    
    # Update graph
    lineage_manager.add_lineage(
        lineage.source_asset_id,
        lineage.target_asset_id,
        {
            "transformation_type": lineage.transformation_type,
            "pipeline_id": lineage.pipeline_id,
            "created_at": datetime.utcnow().isoformat()
        }
    )
    
    return {"lineage_id": lineage_id, "status": "created"}

@app.get("/api/v1/lineage/{asset_id}")
async def get_asset_lineage(
    asset_id: str,
    direction: str = "both",  # upstream, downstream, both
    levels: int = -1,  # -1 for all levels
    format: str = "json",  # json, graphviz, vis
    db: Session = Depends(get_db)
):
    """Get lineage for a specific asset"""
    upstream = []
    downstream = []
    
    if direction in ["upstream", "both"]:
        upstream_ids = lineage_manager.get_upstream(asset_id, levels)
        upstream = [
            db.query(DataAsset).filter(DataAsset.asset_id == uid).first()
            for uid in upstream_ids
        ]
        upstream = [a for a in upstream if a]  # Filter None
    
    if direction in ["downstream", "both"]:
        downstream_ids = lineage_manager.get_downstream(asset_id, levels)
        downstream = [
            db.query(DataAsset).filter(DataAsset.asset_id == did).first()
            for did in downstream_ids
        ]
        downstream = [a for a in downstream if a]  # Filter None
    
    # Get direct connections with transformation details
    direct_upstream = []
    direct_downstream = []
    
    if asset_id in lineage_manager.graph:
        for pred in lineage_manager.graph.predecessors(asset_id):
            edge_data = lineage_manager.graph[pred][asset_id]
            direct_upstream.append({
                "asset_id": pred,
                "transformation_type": edge_data.get("transformation_type"),
                "pipeline_id": edge_data.get("pipeline_id")
            })
        
        for succ in lineage_manager.graph.successors(asset_id):
            edge_data = lineage_manager.graph[asset_id][succ]
            direct_downstream.append({
                "asset_id": succ,
                "transformation_type": edge_data.get("transformation_type"),
                "pipeline_id": edge_data.get("pipeline_id")
            })
    
    result = {
        "asset_id": asset_id,
        "upstream": {
            "direct": direct_upstream,
            "all": [{"asset_id": a.asset_id, "name": a.asset_name} for a in upstream]
        },
        "downstream": {
            "direct": direct_downstream,
            "all": [{"asset_id": a.asset_id, "name": a.asset_name} for a in downstream]
        }
    }
    
    # Generate visualization if requested
    if format == "graphviz":
        # Generate DOT format for Graphviz
        dot = nx.drawing.nx_pydot.to_pydot(lineage_manager.graph)
        result["visualization"] = dot.to_string()
    elif format == "vis":
        # Generate vis.js format
        nodes = []
        edges = []
        
        # Add nodes
        all_nodes = {asset_id}
        all_nodes.update(upstream_ids)
        all_nodes.update(downstream_ids)
        
        for node_id in all_nodes:
            asset = db.query(DataAsset).filter(DataAsset.asset_id == node_id).first()
            nodes.append({
                "id": node_id,
                "label": asset.asset_name if asset else node_id,
                "group": "upstream" if node_id in upstream_ids else "downstream" if node_id in downstream_ids else "current"
            })
        
        # Add edges
        for u, v, data in lineage_manager.graph.edges(data=True):
            if u in all_nodes and v in all_nodes:
                edges.append({
                    "from": u,
                    "to": v,
                    "label": data.get("transformation_type", ""),
                    "arrows": "to"
                })
        
        result["visualization"] = {"nodes": nodes, "edges": edges}
    
    return result

@app.get("/api/v1/lineage/impact/{asset_id}")
async def analyze_impact(asset_id: str):
    """Analyze the impact of changes to an asset"""
    return lineage_manager.get_impact_analysis(asset_id)

@app.post("/api/v1/quality/metrics")
async def record_quality_metric(
    metric: QualityMetricCreate,
    db: Session = Depends(get_db)
):
    """Record a data quality metric"""
    import uuid
    
    metric_id = str(uuid.uuid4())
    passed = metric.metric_value >= (metric.threshold or 0.95)
    
    db_metric = DataQualityMetric(
        metric_id=metric_id,
        passed=passed,
        **metric.dict()
    )
    db.add(db_metric)
    db.commit()
    
    # Check if quality degraded
    if not passed:
        # Trigger quality alert
        logger.warning(f"Data quality threshold not met for {metric.asset_id}: "
                      f"{metric.metric_type}={metric.metric_value} < {metric.threshold}")
        
        # Create quality issue in Atlas if available
        if atlas_client:
            try:
                classification = ClassificationDef({
                    "name": "DataQualityIssue",
                    "description": f"Quality issue: {metric.metric_type}",
                    "typeVersion": "1.0"
                })
                atlas_client.classification.create(classification)
            except:
                pass
    
    return {
        "metric_id": metric_id,
        "passed": passed,
        "message": "Quality metric recorded"
    }

@app.get("/api/v1/quality/profile/{asset_id}")
async def get_quality_profile(
    asset_id: str,
    db: Session = Depends(get_db)
):
    """Get detailed quality profile for an asset"""
    latest_profile = db.query(DataQualityProfile).filter(
        DataQualityProfile.asset_id == asset_id
    ).order_by(DataQualityProfile.profile_date.desc()).first()
    
    if not latest_profile:
        return HTTPException(status_code=404, detail="No quality profile found")
    
    # Get historical metrics
    metrics = db.query(DataQualityMetric).filter(
        DataQualityMetric.asset_id == asset_id
    ).order_by(DataQualityMetric.measured_at.desc()).limit(100).all()
    
    # Calculate trends
    metric_trends = {}
    for metric_type in ["completeness", "accuracy", "consistency", "validity"]:
        type_metrics = [m for m in metrics if m.metric_type == metric_type]
        if type_metrics:
            values = [m.metric_value for m in type_metrics]
            metric_trends[metric_type] = {
                "current": values[0] if values else 0,
                "avg_7d": np.mean(values[:7]) if len(values) >= 7 else np.mean(values),
                "avg_30d": np.mean(values[:30]) if len(values) >= 30 else np.mean(values),
                "trend": "improving" if len(values) > 1 and values[0] > values[-1] else "degrading"
            }
    
    return {
        "asset_id": asset_id,
        "profile": {
            "profile_date": latest_profile.profile_date,
            "row_count": latest_profile.row_count,
            "completeness_score": latest_profile.completeness_score,
            "validity_score": latest_profile.validity_score,
            "consistency_score": latest_profile.consistency_score,
            "overall_score": (
                latest_profile.completeness_score * 0.4 +
                latest_profile.validity_score * 0.3 +
                latest_profile.consistency_score * 0.3
            ),
            "column_profiles": latest_profile.column_profiles,
            "anomalies": latest_profile.anomalies
        },
        "trends": metric_trends
    }

@app.post("/api/v1/compliance/check")
async def run_compliance_check(
    asset_id: str,
    compliance_types: List[str] = ["GDPR", "CCPA"],
    db: Session = Depends(get_db)
):
    """Run compliance checks on an asset"""
    import uuid
    
    asset = db.query(DataAsset).filter(DataAsset.asset_id == asset_id).first()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset not found")
    
    results = []
    
    for compliance_type in compliance_types:
        check_id = str(uuid.uuid4())
        status = "passed"
        findings = []
        remediation_required = False
        
        # Check based on compliance type
        if compliance_type == "GDPR":
            # Check for PII handling
            if asset.classification and asset.classification.get("contains_pii"):
                if not asset.classification.get("encryption_required"):
                    status = "failed"
                    findings.append("PII data not encrypted")
                    remediation_required = True
                
                # Check retention policy
                retention_days = asset.classification.get("retention_days", 0)
                if retention_days > 730:  # GDPR typically requires deletion after 2 years
                    status = "warning"
                    findings.append(f"Retention period ({retention_days} days) may exceed GDPR requirements")
        
        elif compliance_type == "CCPA":
            # Similar checks for CCPA
            if asset.classification and asset.classification.get("contains_pii"):
                # Check for California residents' data
                if "CA" in asset.tags or []:
                    if not asset.metadata.get("deletion_mechanism"):
                        status = "failed"
                        findings.append("No deletion mechanism for California residents' data")
                        remediation_required = True
        
        # Store compliance check result
        db_check = ComplianceCheck(
            check_id=check_id,
            asset_id=asset_id,
            compliance_type=compliance_type,
            status=status,
            findings=findings,
            remediation_required=remediation_required,
            remediation_details="Implement encryption and data deletion mechanisms" if remediation_required else None
        )
        db.add(db_check)
        
        results.append({
            "compliance_type": compliance_type,
            "status": status,
            "findings": findings,
            "remediation_required": remediation_required
        })
    
    db.commit()
    
    return {
        "asset_id": asset_id,
        "compliance_results": results,
        "overall_compliant": all(r["status"] == "passed" for r in results)
    }

@app.get("/api/v1/governance/dashboard")
async def get_governance_dashboard(
    db: Session = Depends(get_db)
):
    """Get comprehensive governance dashboard data"""
    # Asset statistics
    total_assets = db.query(DataAsset).count()
    classified_assets = db.query(DataAsset).filter(
        DataAsset.classification != None
    ).count()
    
    assets_by_catalog = db.query(
        DataAsset.catalog,
        db.func.count(DataAsset.asset_id)
    ).group_by(DataAsset.catalog).all()
    
    # Classification distribution
    classification_dist = db.query(
        db.func.json_extract_path_text(DataAsset.classification, 'classification_type'),
        db.func.count(DataAsset.asset_id)
    ).filter(
        DataAsset.classification != None
    ).group_by(
        db.func.json_extract_path_text(DataAsset.classification, 'classification_type')
    ).all()
    
    # Quality statistics
    recent_profiles = db.query(DataQualityProfile).filter(
        DataQualityProfile.profile_date >= datetime.utcnow() - timedelta(days=7)
    ).all()
    
    avg_quality_score = 0
    if recent_profiles:
        scores = [
            (p.completeness_score * 0.4 + p.validity_score * 0.3 + p.consistency_score * 0.3)
            for p in recent_profiles
        ]
        avg_quality_score = np.mean(scores)
    
    # Compliance status
    recent_checks = db.query(ComplianceCheck).filter(
        ComplianceCheck.check_date >= datetime.utcnow() - timedelta(days=30)
    ).all()
    
    compliance_summary = {
        "total_checks": len(recent_checks),
        "passed": len([c for c in recent_checks if c.status == "passed"]),
        "failed": len([c for c in recent_checks if c.status == "failed"]),
        "warnings": len([c for c in recent_checks if c.status == "warning"])
    }
    
    # Lineage statistics
    total_lineage = db.query(DataLineage).count()
    critical_assets = sum(
        1 for node in lineage_manager.graph.nodes()
        if lineage_manager._calculate_criticality(
            node,
            lineage_manager.get_downstream(node),
            lineage_manager.get_upstream(node)
        ) > 0.7
    )
    
    # Policy statistics
    active_policies = db.query(DataPolicy).filter(DataPolicy.active == True).count()
    access_policies = db.query(AccessPolicy).count()
    
    return {
        "generated_at": datetime.utcnow(),
        "assets": {
            "total": total_assets,
            "classified": classified_assets,
            "classification_coverage": classified_assets / total_assets if total_assets > 0 else 0,
            "by_catalog": dict(assets_by_catalog),
            "by_classification": dict(classification_dist)
        },
        "quality": {
            "average_score": round(avg_quality_score, 3),
            "profiles_last_7d": len(recent_profiles),
            "assets_below_threshold": len([p for p in recent_profiles if 
                (p.completeness_score * 0.4 + p.validity_score * 0.3 + p.consistency_score * 0.3) < 0.8])
        },
        "compliance": compliance_summary,
        "lineage": {
            "total_relationships": total_lineage,
            "assets_with_lineage": len(lineage_manager.graph.nodes()),
            "critical_assets": critical_assets
        },
        "policies": {
            "governance_policies": active_policies,
            "access_policies": access_policies
        }
    }

# Scheduled tasks
@app.on_event("startup")
async def startup_event():
    """Initialize scheduled tasks"""
    # Schedule daily quality profiling
    scheduler.add_job(
        run_daily_quality_checks,
        CronTrigger(hour=2, minute=0),  # Run at 2 AM
        id="daily_quality_checks"
    )
    
    # Schedule weekly compliance checks
    scheduler.add_job(
        run_weekly_compliance_checks,
        CronTrigger(day_of_week="sun", hour=3, minute=0),  # Run Sunday at 3 AM
        id="weekly_compliance_checks"
    )
    
    scheduler.start()

async def run_daily_quality_checks():
    """Run daily quality checks on all assets"""
    db = SessionLocal()
    try:
        assets = db.query(DataAsset).all()
        for asset in assets:
            await profile_asset_task(asset.asset_id)
    finally:
        db.close()

async def run_weekly_compliance_checks():
    """Run weekly compliance checks"""
    db = SessionLocal()
    try:
        # Get assets with PII
        sensitive_assets = db.query(DataAsset).filter(
            db.func.json_extract_path_text(DataAsset.classification, 'contains_pii') == 'true'
        ).all()
        
        for asset in sensitive_assets:
            # Run compliance checks
            # Implementation would trigger compliance check endpoint
            pass
    finally:
        db.close()

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "atlas_connected": atlas_client is not None,
        "scheduler_running": scheduler.running
    } 