from platformq_shared.base_service import create_base_app
from fastapi import Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List, Set
import logging
from datetime import datetime, timedelta
import asyncio
from sqlalchemy import Column, String, DateTime, JSON, Float, Integer, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import create_engine
import networkx as nx
import json
from enum import Enum

logger = logging.getLogger(__name__)

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
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

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

class DataPolicy(Base):
    __tablename__ = "data_policies"
    
    policy_id = Column(String, primary_key=True)
    policy_name = Column(String, nullable=False)
    policy_type = Column(String, nullable=False)
    rules = Column(JSON, nullable=False)
    applies_to = Column(JSON)  # List of asset patterns
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

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

class DataCatalogEntry(BaseModel):
    asset_id: str
    asset_name: str
    full_name: str  # catalog.schema.table
    asset_type: str
    owner: Optional[str]
    description: Optional[str]
    tags: List[str]
    metadata: Dict[str, Any]
    quality_score: Optional[float]
    last_updated: datetime
    upstream_count: int
    downstream_count: int

# Create FastAPI app
app = create_base_app(
    service_name="data-governance-service",
    db_session_dependency=get_db,
    api_key_crud_dependency=lambda: None,
    user_crud_dependency=lambda: None,
    password_verifier_dependency=lambda: None
)

# Lineage graph manager
class LineageGraphManager:
    def __init__(self):
        self.graph = nx.DiGraph()
    
    def add_lineage(self, source: str, target: str, metadata: Dict[str, Any]):
        self.graph.add_edge(source, target, **metadata)
    
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
        
        return {
            "asset_id": asset_id,
            "directly_impacted": list(self.graph.successors(asset_id)),
            "total_downstream_impact": len(downstream),
            "downstream_assets": list(downstream),
            "upstream_dependencies": len(upstream),
            "upstream_assets": list(upstream),
            "is_critical_path": self._is_critical_path(asset_id)
        }
    
    def _is_critical_path(self, asset_id: str) -> bool:
        """Check if asset is on a critical path"""
        # Simple heuristic: asset with many downstream dependencies
        return len(self.get_downstream(asset_id)) > 5

# Global lineage manager
lineage_manager = LineageGraphManager()

# API Endpoints
@app.post("/api/v1/assets", response_model=DataAsset)
async def register_data_asset(
    asset: DataAssetCreate,
    db: Session = Depends(get_db)
):
    """Register a new data asset in the catalog"""
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
    
    # Create new
    db_asset = DataAsset(
        asset_id=asset_id,
        **asset.dict()
    )
    db.add(db_asset)
    db.commit()
    db.refresh(db_asset)
    
    return db_asset

@app.get("/api/v1/assets")
async def list_data_assets(
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    asset_type: Optional[str] = None,
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
    
    # Enrich with lineage counts
    enriched_assets = []
    for asset in assets:
        upstream_count = len(lineage_manager.get_upstream(asset.asset_id, levels=1))
        downstream_count = len(lineage_manager.get_downstream(asset.asset_id, levels=1))
        
        # Get latest quality score
        latest_quality = db.query(DataQualityMetric).filter(
            DataQualityMetric.asset_id == asset.asset_id
        ).order_by(DataQualityMetric.measured_at.desc()).first()
        
        enriched_assets.append(DataCatalogEntry(
            asset_id=asset.asset_id,
            asset_name=asset.asset_name,
            full_name=asset.asset_id,
            asset_type=asset.asset_type,
            owner=asset.owner,
            description=asset.description,
            tags=asset.tags or [],
            metadata=asset.metadata or {},
            quality_score=latest_quality.metric_value if latest_quality else None,
            last_updated=asset.updated_at,
            upstream_count=upstream_count,
            downstream_count=downstream_count
        ))
    
    return {"assets": enriched_assets, "total": len(enriched_assets)}

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
    
    return {
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
    
    return {
        "metric_id": metric_id,
        "passed": passed,
        "message": "Quality metric recorded"
    }

@app.get("/api/v1/quality/metrics/{asset_id}")
async def get_quality_metrics(
    asset_id: str,
    metric_type: Optional[str] = None,
    days: int = 7,
    db: Session = Depends(get_db)
):
    """Get quality metrics for an asset"""
    since = datetime.utcnow() - timedelta(days=days)
    
    query = db.query(DataQualityMetric).filter(
        DataQualityMetric.asset_id == asset_id,
        DataQualityMetric.measured_at >= since
    )
    
    if metric_type:
        query = query.filter(DataQualityMetric.metric_type == metric_type)
    
    metrics = query.order_by(DataQualityMetric.measured_at.desc()).all()
    
    # Calculate summary statistics
    if metrics:
        metric_types = set(m.metric_type for m in metrics)
        summary = {}
        
        for mtype in metric_types:
            type_metrics = [m for m in metrics if m.metric_type == mtype]
            values = [m.metric_value for m in type_metrics]
            summary[mtype] = {
                "current": type_metrics[0].metric_value,
                "average": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "trend": "improving" if values[0] > values[-1] else "degrading" if values[0] < values[-1] else "stable"
            }
    else:
        summary = {}
    
    return {
        "asset_id": asset_id,
        "period_days": days,
        "metric_count": len(metrics),
        "summary": summary,
        "metrics": [
            {
                "metric_id": m.metric_id,
                "metric_type": m.metric_type,
                "metric_value": m.metric_value,
                "threshold": m.threshold,
                "passed": m.passed,
                "measured_at": m.measured_at
            }
            for m in metrics
        ]
    }

@app.post("/api/v1/policies")
async def create_policy(
    policy: PolicyCreate,
    db: Session = Depends(get_db)
):
    """Create a data governance policy"""
    import uuid
    
    policy_id = str(uuid.uuid4())
    
    db_policy = DataPolicy(
        policy_id=policy_id,
        **policy.dict()
    )
    db.add(db_policy)
    db.commit()
    
    return {
        "policy_id": policy_id,
        "message": f"Policy '{policy.policy_name}' created"
    }

@app.get("/api/v1/policies")
async def list_policies(
    policy_type: Optional[str] = None,
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """List data governance policies"""
    query = db.query(DataPolicy)
    
    if policy_type:
        query = query.filter(DataPolicy.policy_type == policy_type)
    if active_only:
        query = query.filter(DataPolicy.active == True)
    
    policies = query.all()
    
    return {
        "policies": [
            {
                "policy_id": p.policy_id,
                "policy_name": p.policy_name,
                "policy_type": p.policy_type,
                "applies_to": p.applies_to,
                "active": p.active,
                "created_at": p.created_at
            }
            for p in policies
        ]
    }

@app.get("/api/v1/governance/report")
async def generate_governance_report(
    db: Session = Depends(get_db)
):
    """Generate overall data governance report"""
    # Asset statistics
    total_assets = db.query(DataAsset).count()
    assets_by_catalog = db.query(
        DataAsset.catalog,
        db.func.count(DataAsset.asset_id)
    ).group_by(DataAsset.catalog).all()
    
    # Quality statistics
    recent_metrics = db.query(DataQualityMetric).filter(
        DataQualityMetric.measured_at >= datetime.utcnow() - timedelta(days=1)
    ).all()
    
    quality_summary = {
        "total_checks": len(recent_metrics),
        "passed": len([m for m in recent_metrics if m.passed]),
        "failed": len([m for m in recent_metrics if not m.passed]),
        "pass_rate": len([m for m in recent_metrics if m.passed]) / len(recent_metrics) if recent_metrics else 0
    }
    
    # Lineage statistics
    lineage_count = db.query(DataLineage).count()
    
    # Policy statistics
    active_policies = db.query(DataPolicy).filter(DataPolicy.active == True).count()
    
    return {
        "generated_at": datetime.utcnow(),
        "assets": {
            "total": total_assets,
            "by_catalog": dict(assets_by_catalog)
        },
        "quality": quality_summary,
        "lineage": {
            "total_relationships": lineage_count,
            "assets_with_lineage": len(lineage_manager.graph.nodes())
        },
        "policies": {
            "active": active_policies
        }
    }

# Health check
@app.get("/health")
async def health_check():
    return {"status": "healthy"} 