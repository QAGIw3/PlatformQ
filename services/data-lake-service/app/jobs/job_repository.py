from typing import Dict, List, Optional
from ..ingestion.data_ingestion import IngestionJob
from ..processing.layer_processing import ProcessingJob

class JobRepository:
    def __init__(self):
        self.ingestion_jobs: Dict[str, IngestionJob] = {}
        self.processing_jobs: Dict[str, ProcessingJob] = {}

    def add_ingestion_job(self, job: IngestionJob):
        self.ingestion_jobs[job.job_id] = job

    def get_ingestion_job(self, job_id: str) -> Optional[IngestionJob]:
        return self.ingestion_jobs.get(job_id)

    def list_active_ingestion_jobs(self) -> List[IngestionJob]:
        return [job for job in self.ingestion_jobs.values() if job.status == "running"]

    def add_processing_job(self, job: ProcessingJob):
        self.processing_jobs[job.job_id] = job

    def get_processing_job(self, job_id: str) -> Optional[ProcessingJob]:
        return self.processing_jobs.get(job_id)

    def list_active_processing_jobs(self) -> List[ProcessingJob]:
        return [job for job in self.processing_jobs.values() if job.status in ["pending", "running"]] 