"""
Data Governance Repository

Handles database operations for data governance, quality, and compliance.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import Session
import json

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from .models import (
    DataAsset,
    DataLineage,
    DataQualityMetric,
    DataQualityProfile,
    DataPolicy,
    AccessPolicy,
    ComplianceCheck,
    DataCatalogEntry,
    AssetType,
    PolicyType,
    ComplianceType,
    ComplianceStatus,
    DataClassification
)


class DataAssetRepository(BaseRepository[DataAsset]):
    """Repository for data asset management"""
    
    def __init__(self, db: Session):
        super().__init__(DataAsset, db)
    
    def get_by_asset_id(self, asset_id: str) -> Optional[DataAsset]:
        """Get asset by asset_id"""
        return self.db.query(DataAsset).filter(
            DataAsset.asset_id == asset_id
        ).first()
    
    def search_assets(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        asset_type: Optional[AssetType] = None,
        classification: Optional[DataClassification] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DataAsset]:
        """Search data assets with filters"""
        q = self.db.query(DataAsset).filter(DataAsset.tenant_id == tenant_id)
        
        if query:
            q = q.filter(
                or_(
                    DataAsset.asset_name.ilike(f"%{query}%"),
                    DataAsset.description.ilike(f"%{query}%")
                )
            )
        
        if asset_type:
            q = q.filter(DataAsset.asset_type == asset_type)
        
        if classification:
            q = q.filter(DataAsset.classification == classification)
        
        if owner:
            q = q.filter(DataAsset.owner == owner)
        
        if tags:
            for tag in tags:
                q = q.filter(DataAsset.tags.contains([tag]))
        
        return q.offset(offset).limit(limit).all()
    
    def get_sensitive_assets(self, tenant_id: str) -> List[DataAsset]:
        """Get all sensitive data assets"""
        return self.db.query(DataAsset).filter(
            and_(
                DataAsset.tenant_id == tenant_id,
                DataAsset.classification.in_([
                    DataClassification.CONFIDENTIAL,
                    DataClassification.RESTRICTED,
                    DataClassification.TOP_SECRET
                ])
            )
        ).all()
    
    def get_assets_by_catalog(self, tenant_id: str, catalog: str, schema: Optional[str] = None) -> List[DataAsset]:
        """Get assets by catalog and optionally schema"""
        q = self.db.query(DataAsset).filter(
            and_(
                DataAsset.tenant_id == tenant_id,
                DataAsset.catalog == catalog
            )
        )
        
        if schema:
            q = q.filter(DataAsset.schema == schema)
        
        return q.all()
    
    def update_access_stats(self, asset_id: str):
        """Update asset access statistics"""
        asset = self.get_by_asset_id(asset_id)
        if asset:
            asset.access_count += 1
            asset.last_accessed = datetime.utcnow()
            self.db.commit()


class DataLineageRepository(BaseRepository[DataLineage]):
    """Repository for data lineage tracking"""
    
    def __init__(self, db: Session):
        super().__init__(DataLineage, db)
    
    def get_asset_lineage(self, asset_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get lineage graph for an asset"""
        # Get immediate parents
        parents = self.db.query(DataLineage).filter(
            DataLineage.target_asset_id == asset_id
        ).all()
        
        # Get immediate children
        children = self.db.query(DataLineage).filter(
            DataLineage.source_asset_id == asset_id
        ).all()
        
        return {
            "asset_id": asset_id,
            "parents": [
                {
                    "asset_id": l.source_asset_id,
                    "transformation": l.transformation_type,
                    "pipeline_id": l.pipeline_id
                } for l in parents
            ],
            "children": [
                {
                    "asset_id": l.target_asset_id,
                    "transformation": l.transformation_type,
                    "pipeline_id": l.pipeline_id
                } for l in children
            ]
        }
    
    def get_pipeline_lineage(self, pipeline_id: str) -> List[DataLineage]:
        """Get all lineage records for a pipeline"""
        return self.db.query(DataLineage).filter(
            DataLineage.pipeline_id == pipeline_id
        ).order_by(DataLineage.created_at).all()
    
    def find_impact_analysis(self, asset_id: str) -> List[str]:
        """Find all downstream assets that would be impacted by changes"""
        impacted = set()
        to_check = [asset_id]
        
        while to_check:
            current = to_check.pop()
            children = self.db.query(DataLineage.target_asset_id).filter(
                DataLineage.source_asset_id == current
            ).all()
            
            for child in children:
                if child[0] not in impacted:
                    impacted.add(child[0])
                    to_check.append(child[0])
        
        return list(impacted)


class DataQualityMetricRepository(BaseRepository[DataQualityMetric]):
    """Repository for quality metrics"""
    
    def __init__(self, db: Session):
        super().__init__(DataQualityMetric, db)
    
    def get_latest_metrics(self, asset_id: str, metric_type: Optional[str] = None) -> List[DataQualityMetric]:
        """Get latest metrics for an asset"""
        q = self.db.query(DataQualityMetric).filter(
            DataQualityMetric.asset_id == asset_id
        )
        
        if metric_type:
            q = q.filter(DataQualityMetric.metric_type == metric_type)
        
        # Get latest metric for each type
        subq = q.distinct(DataQualityMetric.metric_type).subquery()
        
        return self.db.query(DataQualityMetric).filter(
            DataQualityMetric.id.in_(
                self.db.query(subq.c.id)
            )
        ).order_by(desc(DataQualityMetric.measured_at)).all()
    
    def get_metric_history(
        self,
        asset_id: str,
        metric_type: str,
        days: int = 30
    ) -> List[DataQualityMetric]:
        """Get metric history for trending"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return self.db.query(DataQualityMetric).filter(
            and_(
                DataQualityMetric.asset_id == asset_id,
                DataQualityMetric.metric_type == metric_type,
                DataQualityMetric.measured_at >= cutoff_date
            )
        ).order_by(DataQualityMetric.measured_at).all()
    
    def get_failing_metrics(self, tenant_id: str) -> List[DataQualityMetric]:
        """Get all currently failing metrics"""
        return self.db.query(DataQualityMetric).filter(
            and_(
                DataQualityMetric.tenant_id == tenant_id,
                DataQualityMetric.passed == False,
                DataQualityMetric.measured_at >= datetime.utcnow() - timedelta(hours=24)
            )
        ).all()


class DataQualityProfileRepository(BaseRepository[DataQualityProfile]):
    """Repository for quality profiles"""
    
    def __init__(self, db: Session):
        super().__init__(DataQualityProfile, db)
    
    def get_latest_profile(self, asset_id: str) -> Optional[DataQualityProfile]:
        """Get the most recent quality profile for an asset"""
        return self.db.query(DataQualityProfile).filter(
            DataQualityProfile.asset_id == asset_id
        ).order_by(desc(DataQualityProfile.profile_date)).first()
    
    def get_quality_trend(self, asset_id: str, days: int = 30) -> List[DataQualityProfile]:
        """Get quality trend over time"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return self.db.query(DataQualityProfile).filter(
            and_(
                DataQualityProfile.asset_id == asset_id,
                DataQualityProfile.profile_date >= cutoff_date
            )
        ).order_by(DataQualityProfile.profile_date).all()
    
    def get_low_quality_assets(self, tenant_id: str, threshold: float = 70.0) -> List[Dict[str, Any]]:
        """Get assets with low quality scores"""
        # Get latest profile for each asset
        subq = self.db.query(
            DataQualityProfile.asset_id,
            func.max(DataQualityProfile.profile_date).label('latest_date')
        ).filter(
            DataQualityProfile.tenant_id == tenant_id
        ).group_by(DataQualityProfile.asset_id).subquery()
        
        # Get profiles with low scores
        profiles = self.db.query(DataQualityProfile).join(
            subq,
            and_(
                DataQualityProfile.asset_id == subq.c.asset_id,
                DataQualityProfile.profile_date == subq.c.latest_date
            )
        ).filter(
            DataQualityProfile.overall_score < threshold
        ).all()
        
        return [
            {
                "asset_id": p.asset_id,
                "overall_score": p.overall_score,
                "completeness_score": p.completeness_score,
                "validity_score": p.validity_score,
                "profile_date": p.profile_date
            } for p in profiles
        ]


class DataPolicyRepository(BaseRepository[DataPolicy]):
    """Repository for data policies"""
    
    def __init__(self, db: Session):
        super().__init__(DataPolicy, db)
    
    def get_active_policies(
        self,
        tenant_id: str,
        policy_type: Optional[PolicyType] = None
    ) -> List[DataPolicy]:
        """Get all active policies"""
        q = self.db.query(DataPolicy).filter(
            and_(
                DataPolicy.tenant_id == tenant_id,
                DataPolicy.active == True
            )
        )
        
        if policy_type:
            q = q.filter(DataPolicy.policy_type == policy_type)
        
        return q.all()
    
    def get_policies_for_asset(self, asset: DataAsset) -> List[DataPolicy]:
        """Get policies that apply to a specific asset"""
        all_policies = self.get_active_policies(asset.tenant_id)
        
        applicable_policies = []
        for policy in all_policies:
            if self._policy_applies_to_asset(policy, asset):
                applicable_policies.append(policy)
        
        return applicable_policies
    
    def _policy_applies_to_asset(self, policy: DataPolicy, asset: DataAsset) -> bool:
        """Check if a policy applies to an asset"""
        if not policy.applies_to:
            return True  # Policy applies to all assets
        
        # Check patterns in applies_to
        for pattern in policy.applies_to:
            if pattern.get("asset_type") and pattern["asset_type"] != asset.asset_type.value:
                continue
            if pattern.get("catalog") and pattern["catalog"] != asset.catalog:
                continue
            if pattern.get("schema") and pattern["schema"] != asset.schema:
                continue
            if pattern.get("classification") and pattern["classification"] != asset.classification.value:
                continue
            if pattern.get("tags"):
                required_tags = set(pattern["tags"])
                asset_tags = set(asset.tags or [])
                if not required_tags.issubset(asset_tags):
                    continue
            
            return True
        
        return False


class AccessPolicyRepository(BaseRepository[AccessPolicy]):
    """Repository for access policies"""
    
    def __init__(self, db: Session):
        super().__init__(AccessPolicy, db)
    
    def get_asset_policies(self, asset_id: str) -> List[AccessPolicy]:
        """Get all access policies for an asset"""
        return self.db.query(AccessPolicy).filter(
            and_(
                AccessPolicy.asset_id == asset_id,
                AccessPolicy.active == True
            )
        ).all()
    
    def get_user_accessible_assets(self, tenant_id: str, user: str) -> List[str]:
        """Get all assets accessible by a user"""
        policies = self.db.query(AccessPolicy).filter(
            and_(
                AccessPolicy.tenant_id == tenant_id,
                AccessPolicy.active == True,
                or_(
                    AccessPolicy.principals.contains([user]),
                    AccessPolicy.principals.contains(["*"])  # Public access
                )
            )
        ).all()
        
        return list(set([p.asset_id for p in policies]))
    
    def check_access(self, asset_id: str, user: str, permission: str) -> bool:
        """Check if user has specific permission on asset"""
        policies = self.get_asset_policies(asset_id)
        
        for policy in policies:
            if user in (policy.principals or []) or "*" in (policy.principals or []):
                if permission in (policy.permissions or []) or "*" in (policy.permissions or []):
                    # Check conditions if any
                    if policy.conditions:
                        # Implement condition checking logic
                        pass
                    return True
        
        return False


class ComplianceCheckRepository(BaseRepository[ComplianceCheck]):
    """Repository for compliance checks"""
    
    def __init__(self, db: Session):
        super().__init__(ComplianceCheck, db)
    
    def get_latest_checks(
        self,
        asset_id: str,
        compliance_type: Optional[ComplianceType] = None
    ) -> List[ComplianceCheck]:
        """Get latest compliance checks for an asset"""
        q = self.db.query(ComplianceCheck).filter(
            ComplianceCheck.asset_id == asset_id
        )
        
        if compliance_type:
            q = q.filter(ComplianceCheck.compliance_type == compliance_type)
        
        # Get latest check for each compliance type
        subq = q.distinct(ComplianceCheck.compliance_type).subquery()
        
        return self.db.query(ComplianceCheck).filter(
            ComplianceCheck.id.in_(
                self.db.query(subq.c.id)
            )
        ).order_by(desc(ComplianceCheck.check_date)).all()
    
    def get_non_compliant_assets(
        self,
        tenant_id: str,
        compliance_type: Optional[ComplianceType] = None
    ) -> List[ComplianceCheck]:
        """Get all non-compliant assets"""
        q = self.db.query(ComplianceCheck).filter(
            and_(
                ComplianceCheck.tenant_id == tenant_id,
                ComplianceCheck.status.in_([
                    ComplianceStatus.FAILED,
                    ComplianceStatus.WARNING
                ])
            )
        )
        
        if compliance_type:
            q = q.filter(ComplianceCheck.compliance_type == compliance_type)
        
        return q.all()
    
    def get_compliance_summary(self, tenant_id: str) -> Dict[str, Any]:
        """Get compliance summary statistics"""
        total = self.db.query(func.count(ComplianceCheck.id)).filter(
            ComplianceCheck.tenant_id == tenant_id
        ).scalar()
        
        passed = self.db.query(func.count(ComplianceCheck.id)).filter(
            and_(
                ComplianceCheck.tenant_id == tenant_id,
                ComplianceCheck.status == ComplianceStatus.PASSED
            )
        ).scalar()
        
        by_type = self.db.query(
            ComplianceCheck.compliance_type,
            ComplianceCheck.status,
            func.count(ComplianceCheck.id)
        ).filter(
            ComplianceCheck.tenant_id == tenant_id
        ).group_by(
            ComplianceCheck.compliance_type,
            ComplianceCheck.status
        ).all()
        
        return {
            "total_checks": total,
            "passed_checks": passed,
            "compliance_rate": (passed / total * 100) if total > 0 else 0,
            "by_type": {
                compliance_type: {
                    status: count
                    for c_type, status, count in by_type
                    if c_type == compliance_type
                }
                for compliance_type in ComplianceType
            }
        }


class DataCatalogRepository(BaseRepository[DataCatalogEntry]):
    """Repository for data catalog entries"""
    
    def __init__(self, db: Session):
        super().__init__(DataCatalogEntry, db)
    
    def search_catalog(
        self,
        tenant_id: str,
        query: Optional[str] = None,
        domain: Optional[str] = None,
        categories: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[DataCatalogEntry]:
        """Search the data catalog"""
        q = self.db.query(DataCatalogEntry).filter(
            DataCatalogEntry.tenant_id == tenant_id
        )
        
        if query:
            q = q.filter(
                or_(
                    DataCatalogEntry.business_name.ilike(f"%{query}%"),
                    DataCatalogEntry.business_description.ilike(f"%{query}%"),
                    DataCatalogEntry.keywords.contains([query])
                )
            )
        
        if domain:
            q = q.filter(DataCatalogEntry.business_domain == domain)
        
        if categories:
            for category in categories:
                q = q.filter(DataCatalogEntry.categories.contains([category]))
        
        return q.limit(limit).all()
    
    def get_by_asset_id(self, asset_id: str) -> Optional[DataCatalogEntry]:
        """Get catalog entry for an asset"""
        return self.db.query(DataCatalogEntry).filter(
            DataCatalogEntry.asset_id == asset_id
        ).first()
    
    def get_glossary_terms(self, tenant_id: str) -> List[str]:
        """Get all unique glossary terms"""
        entries = self.db.query(DataCatalogEntry.glossary_terms).filter(
            DataCatalogEntry.tenant_id == tenant_id
        ).all()
        
        terms = set()
        for entry in entries:
            if entry.glossary_terms:
                terms.update(entry.glossary_terms)
        
        return sorted(list(terms)) 