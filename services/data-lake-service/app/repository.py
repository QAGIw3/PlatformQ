"""
Data Lake Repository

Handles database operations for data lake metadata and configurations.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func
from sqlalchemy.orm import Session

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from platformq_shared.database import get_db
from .models import (
    DataIngestion,
    DataQualityCheck,
    DataLineage,
    DataCatalogEntry,
    ProcessingJob
)


class DataIngestionRepository(BaseRepository[DataIngestion]):
    """Repository for data ingestion metadata"""
    
    def __init__(self, db: Session):
        super().__init__(DataIngestion, db)
    
    def get_by_source(self, source_name: str, tenant_id: str) -> List[DataIngestion]:
        """Get all ingestions for a specific source"""
        return self.db.query(DataIngestion).filter(
            and_(
                DataIngestion.source_name == source_name,
                DataIngestion.tenant_id == tenant_id
            )
        ).all()
    
    def get_active_ingestions(self, tenant_id: str) -> List[DataIngestion]:
        """Get all active ingestions"""
        return self.db.query(DataIngestion).filter(
            and_(
                DataIngestion.tenant_id == tenant_id,
                DataIngestion.status == "active"
            )
        ).all()
    
    def get_failed_ingestions(self, tenant_id: str, limit: int = 10) -> List[DataIngestion]:
        """Get recent failed ingestions"""
        return self.db.query(DataIngestion).filter(
            and_(
                DataIngestion.tenant_id == tenant_id,
                DataIngestion.status == "failed"
            )
        ).order_by(DataIngestion.created_at.desc()).limit(limit).all()


class DataQualityRepository(BaseRepository[DataQualityCheck]):
    """Repository for data quality checks"""
    
    def __init__(self, db: Session):
        super().__init__(DataQualityCheck, db)
    
    def get_by_dataset(self, dataset_id: str, tenant_id: str) -> List[DataQualityCheck]:
        """Get all quality checks for a dataset"""
        return self.db.query(DataQualityCheck).filter(
            and_(
                DataQualityCheck.dataset_id == dataset_id,
                DataQualityCheck.tenant_id == tenant_id
            )
        ).order_by(DataQualityCheck.check_date.desc()).all()
    
    def get_failed_checks(self, tenant_id: str, days: int = 7) -> List[DataQualityCheck]:
        """Get failed quality checks from recent days"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        return self.db.query(DataQualityCheck).filter(
            and_(
                DataQualityCheck.tenant_id == tenant_id,
                DataQualityCheck.status == "failed",
                DataQualityCheck.check_date >= cutoff_date
            )
        ).all()
    
    def get_quality_metrics(self, dataset_id: str, tenant_id: str) -> Dict[str, Any]:
        """Get quality metrics for a dataset"""
        total_checks = self.db.query(func.count(DataQualityCheck.id)).filter(
            and_(
                DataQualityCheck.dataset_id == dataset_id,
                DataQualityCheck.tenant_id == tenant_id
            )
        ).scalar()
        
        passed_checks = self.db.query(func.count(DataQualityCheck.id)).filter(
            and_(
                DataQualityCheck.dataset_id == dataset_id,
                DataQualityCheck.tenant_id == tenant_id,
                DataQualityCheck.status == "passed"
            )
        ).scalar()
        
        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "success_rate": passed_checks / total_checks if total_checks > 0 else 0
        }


class DataLineageRepository(BaseRepository[DataLineage]):
    """Repository for data lineage tracking"""
    
    def __init__(self, db: Session):
        super().__init__(DataLineage, db)
    
    def get_upstream_datasets(self, dataset_id: str, tenant_id: str) -> List[DataLineage]:
        """Get all upstream datasets"""
        return self.db.query(DataLineage).filter(
            and_(
                DataLineage.target_dataset_id == dataset_id,
                DataLineage.tenant_id == tenant_id
            )
        ).all()
    
    def get_downstream_datasets(self, dataset_id: str, tenant_id: str) -> List[DataLineage]:
        """Get all downstream datasets"""
        return self.db.query(DataLineage).filter(
            and_(
                DataLineage.source_dataset_id == dataset_id,
                DataLineage.tenant_id == tenant_id
            )
        ).all()
    
    def get_lineage_graph(self, dataset_id: str, tenant_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get lineage graph up to specified depth"""
        # This would be implemented with recursive queries or graph traversal
        # For now, return a simple structure
        upstream = self.get_upstream_datasets(dataset_id, tenant_id)
        downstream = self.get_downstream_datasets(dataset_id, tenant_id)
        
        return {
            "dataset_id": dataset_id,
            "upstream": [{"id": l.source_dataset_id, "transformation": l.transformation_type} for l in upstream],
            "downstream": [{"id": l.target_dataset_id, "transformation": l.transformation_type} for l in downstream]
        }


class DataCatalogRepository(BaseRepository[DataCatalogEntry]):
    """Repository for data catalog entries"""
    
    def __init__(self, db: Session):
        super().__init__(DataCatalogEntry, db)
    
    def search(self, query: str, tenant_id: str, layer: Optional[str] = None) -> List[DataCatalogEntry]:
        """Search catalog entries"""
        filters = [
            DataCatalogEntry.tenant_id == tenant_id,
            or_(
                DataCatalogEntry.name.ilike(f"%{query}%"),
                DataCatalogEntry.description.ilike(f"%{query}%"),
                DataCatalogEntry.tags.ilike(f"%{query}%")
            )
        ]
        
        if layer:
            filters.append(DataCatalogEntry.layer == layer)
        
        return self.db.query(DataCatalogEntry).filter(
            and_(*filters)
        ).all()
    
    def get_by_layer(self, layer: str, tenant_id: str) -> List[DataCatalogEntry]:
        """Get all entries for a specific layer"""
        return self.db.query(DataCatalogEntry).filter(
            and_(
                DataCatalogEntry.layer == layer,
                DataCatalogEntry.tenant_id == tenant_id
            )
        ).all()
    
    def get_by_tags(self, tags: List[str], tenant_id: str) -> List[DataCatalogEntry]:
        """Get entries by tags"""
        tag_filters = []
        for tag in tags:
            tag_filters.append(DataCatalogEntry.tags.ilike(f"%{tag}%"))
        
        return self.db.query(DataCatalogEntry).filter(
            and_(
                DataCatalogEntry.tenant_id == tenant_id,
                or_(*tag_filters)
            )
        ).all()


class ProcessingJobRepository(BaseRepository[ProcessingJob]):
    """Repository for processing job tracking"""
    
    def __init__(self, db: Session):
        super().__init__(ProcessingJob, db)
    
    def get_running_jobs(self, tenant_id: str) -> List[ProcessingJob]:
        """Get all running jobs"""
        return self.db.query(ProcessingJob).filter(
            and_(
                ProcessingJob.tenant_id == tenant_id,
                ProcessingJob.status == "running"
            )
        ).all()
    
    def get_job_history(self, dataset_id: str, tenant_id: str, limit: int = 10) -> List[ProcessingJob]:
        """Get job history for a dataset"""
        return self.db.query(ProcessingJob).filter(
            and_(
                ProcessingJob.dataset_id == dataset_id,
                ProcessingJob.tenant_id == tenant_id
            )
        ).order_by(ProcessingJob.started_at.desc()).limit(limit).all()
    
    def get_job_metrics(self, tenant_id: str) -> Dict[str, Any]:
        """Get job metrics"""
        total_jobs = self.db.query(func.count(ProcessingJob.id)).filter(
            ProcessingJob.tenant_id == tenant_id
        ).scalar()
        
        successful_jobs = self.db.query(func.count(ProcessingJob.id)).filter(
            and_(
                ProcessingJob.tenant_id == tenant_id,
                ProcessingJob.status == "completed"
            )
        ).scalar()
        
        avg_duration = self.db.query(
            func.avg(
                func.extract('epoch', ProcessingJob.completed_at - ProcessingJob.started_at)
            )
        ).filter(
            and_(
                ProcessingJob.tenant_id == tenant_id,
                ProcessingJob.status == "completed",
                ProcessingJob.completed_at.isnot(None)
            )
        ).scalar()
        
        return {
            "total_jobs": total_jobs,
            "successful_jobs": successful_jobs,
            "success_rate": successful_jobs / total_jobs if total_jobs > 0 else 0,
            "avg_duration_seconds": avg_duration or 0
        } 