"""
Data Synchronization Engine for Integration Hub.
"""

import asyncio
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict

from data_intelligence_common.core.events import EventBus

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class SyncMode(str, Enum):
    """Data synchronization modes."""
    FULL = "full"  # Full sync of all data
    INCREMENTAL = "incremental"  # Sync only changes
    REAL_TIME = "real_time"  # Real-time streaming
    SCHEDULED = "scheduled"  # Scheduled batch sync
    ON_DEMAND = "on_demand"  # Manual trigger


class SyncStatus(str, Enum):
    """Sync job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class ConflictResolution(str, Enum):
    """Conflict resolution strategies."""
    SOURCE_WINS = "source_wins"
    TARGET_WINS = "target_wins"
    LATEST_WINS = "latest_wins"
    MANUAL = "manual"
    CUSTOM = "custom"


@dataclass
class SyncJob:
    """Represents a synchronization job."""
    job_id: str
    source: str
    target: str
    sync_mode: SyncMode
    status: SyncStatus = SyncStatus.PENDING
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Configuration
    filter_criteria: Optional[Dict[str, Any]] = None
    transform_rules: Optional[List[Dict[str, Any]]] = None
    conflict_resolution: ConflictResolution = ConflictResolution.LATEST_WINS
    batch_size: int = 1000
    
    # Progress tracking
    total_records: int = 0
    processed_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    
    # Error handling
    errors: List[Dict[str, Any]] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_records == 0:
            return 0.0
        return (self.processed_records / self.total_records) * 100
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate job duration in seconds."""
        if not self.started_at:
            return None
        
        end_time = self.completed_at or datetime.utcnow()
        return (end_time - self.started_at).total_seconds()


@dataclass
class SyncResult:
    """Result of a sync operation."""
    job_id: str
    success: bool
    records_synced: int
    records_failed: int
    records_skipped: int
    duration_seconds: float
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class SyncEngine:
    """
    Engine for managing data synchronization between sources.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        integration_hub: Any  # Avoid circular import
    ):
        self.event_bus = event_bus
        self.integration_hub = integration_hub
        
        # Sync job management
        self.active_jobs: Dict[str, SyncJob] = {}
        self.job_history: List[SyncJob] = []
        self.job_queue: asyncio.Queue = asyncio.Queue()
        
        # Sync handlers
        self.sync_handlers: Dict[str, Callable] = {}
        self.transform_functions: Dict[str, Callable] = {}
        
        # Scheduling
        self.scheduled_jobs: Dict[str, Dict[str, Any]] = {}
        
        # Metrics
        self.metrics = defaultdict(int)
        
        # Background tasks
        self._worker_task: Optional[asyncio.Task] = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._monitor_task: Optional[asyncio.Task] = None
        
        logger.info("Sync Engine initialized")
        
    async def initialize(self):
        """Initialize sync engine."""
        # Register default handlers
        self._register_default_handlers()
        
        # Subscribe to events
        await self.event_bus.subscribe("sync.job.create", self._handle_job_create)
        await self.event_bus.subscribe("sync.job.cancel", self._handle_job_cancel)
        
        # Start background tasks
        self._worker_task = asyncio.create_task(self._process_jobs())
        self._scheduler_task = asyncio.create_task(self._process_scheduled_jobs())
        self._monitor_task = asyncio.create_task(self._monitor_jobs())
        
        logger.info("Sync Engine ready")
        
    async def cleanup(self):
        """Cleanup sync engine resources."""
        # Cancel all active jobs
        for job_id in list(self.active_jobs.keys()):
            await self.cancel_job(job_id)
        
        # Cancel background tasks
        if self._worker_task:
            self._worker_task.cancel()
        if self._scheduler_task:
            self._scheduler_task.cancel()
        if self._monitor_task:
            self._monitor_task.cancel()
        
        logger.info("Sync Engine cleaned up")
        
    async def create_sync_job(
        self,
        source: str,
        target: str,
        sync_mode: SyncMode = SyncMode.INCREMENTAL,
        filter_criteria: Optional[Dict[str, Any]] = None,
        transform_rules: Optional[List[Dict[str, Any]]] = None,
        conflict_resolution: ConflictResolution = ConflictResolution.LATEST_WINS,
        batch_size: int = 1000,
        schedule: Optional[Dict[str, Any]] = None
    ) -> SyncJob:
        """Create a new sync job."""
        job_id = f"sync_{datetime.utcnow().timestamp()}"
        
        job = SyncJob(
            job_id=job_id,
            source=source,
            target=target,
            sync_mode=sync_mode,
            filter_criteria=filter_criteria,
            transform_rules=transform_rules,
            conflict_resolution=conflict_resolution,
            batch_size=batch_size
        )
        
        # Add to queue or schedule
        if schedule:
            self.scheduled_jobs[job_id] = {
                "job": job,
                "schedule": schedule,
                "next_run": self._calculate_next_run(schedule)
            }
            logger.info(f"Scheduled sync job: {job_id}")
        else:
            await self.job_queue.put(job)
            logger.info(f"Created sync job: {job_id}")
        
        # Publish event
        await self.event_bus.publish("sync.job.created", {
            "job_id": job_id,
            "source": source,
            "target": target,
            "mode": sync_mode.value
        })
        
        return job
        
    async def get_job_status(self, job_id: str) -> Optional[SyncJob]:
        """Get sync job status."""
        # Check active jobs
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        
        # Check history
        for job in self.job_history:
            if job.job_id == job_id:
                return job
        
        # Check scheduled jobs
        if job_id in self.scheduled_jobs:
            return self.scheduled_jobs[job_id]["job"]
        
        return None
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a sync job."""
        job = self.active_jobs.get(job_id)
        
        if not job:
            return False
        
        job.status = SyncStatus.CANCELLED
        job.completed_at = datetime.utcnow()
        
        # Move to history
        self.job_history.append(job)
        del self.active_jobs[job_id]
        
        # Publish event
        await self.event_bus.publish("sync.job.cancelled", {
            "job_id": job_id
        })
        
        logger.info(f"Cancelled sync job: {job_id}")
        return True
        
    async def pause_job(self, job_id: str) -> bool:
        """Pause a sync job."""
        job = self.active_jobs.get(job_id)
        
        if not job or job.status != SyncStatus.RUNNING:
            return False
        
        job.status = SyncStatus.PAUSED
        
        # Publish event
        await self.event_bus.publish("sync.job.paused", {
            "job_id": job_id
        })
        
        logger.info(f"Paused sync job: {job_id}")
        return True
        
    async def resume_job(self, job_id: str) -> bool:
        """Resume a paused sync job."""
        job = self.active_jobs.get(job_id)
        
        if not job or job.status != SyncStatus.PAUSED:
            return False
        
        job.status = SyncStatus.RUNNING
        
        # Publish event
        await self.event_bus.publish("sync.job.resumed", {
            "job_id": job_id
        })
        
        logger.info(f"Resumed sync job: {job_id}")
        return True
        
    def register_sync_handler(self, source_type: str, handler: Callable):
        """Register a sync handler for a source type."""
        self.sync_handlers[source_type] = handler
        logger.info(f"Registered sync handler for {source_type}")
        
    def register_transform_function(self, name: str, func: Callable):
        """Register a transform function."""
        self.transform_functions[name] = func
        logger.info(f"Registered transform function: {name}")
        
    async def _process_jobs(self):
        """Background task to process sync jobs."""
        while True:
            try:
                # Get job from queue
                job = await self.job_queue.get()
                
                # Add to active jobs
                self.active_jobs[job.job_id] = job
                
                # Process job
                try:
                    await self._execute_job(job)
                except Exception as e:
                    logger.error(f"Error executing job {job.job_id}: {e}")
                    job.status = SyncStatus.FAILED
                    job.errors.append({
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    })
                finally:
                    # Move to history
                    if job.job_id in self.active_jobs:
                        self.job_history.append(job)
                        del self.active_jobs[job.job_id]
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job processor: {e}")
                await asyncio.sleep(1)
                
    async def _execute_job(self, job: SyncJob):
        """Execute a sync job."""
        logger.info(f"Executing sync job: {job.job_id}")
        
        # Update job status
        job.status = SyncStatus.RUNNING
        job.started_at = datetime.utcnow()
        
        # Publish start event
        await self.event_bus.publish("sync.job.started", {
            "job_id": job.job_id,
            "source": job.source,
            "target": job.target
        })
        
        try:
            # Get sync handler
            source_type = await self._get_source_type(job.source)
            handler = self.sync_handlers.get(source_type)
            
            if not handler:
                # Use default handler
                handler = self._default_sync_handler
            
            # Execute sync
            result = await handler(job, self)
            
            # Update job status
            job.status = SyncStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.processed_records = result.records_synced
            job.failed_records = result.records_failed
            job.skipped_records = result.records_skipped
            
            # Update metrics
            self.metrics["total_syncs"] += 1
            self.metrics["records_synced"] += result.records_synced
            self.metrics["records_failed"] += result.records_failed
            
            # Publish completion event
            await self.event_bus.publish("sync.job.completed", {
                "job_id": job.job_id,
                "result": result.__dict__
            })
            
            logger.info(f"Completed sync job {job.job_id}: {result.records_synced} records synced")
            
        except Exception as e:
            logger.error(f"Sync job {job.job_id} failed: {e}")
            job.status = SyncStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.errors.append({
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            
            # Retry logic
            if job.retry_count < job.max_retries:
                job.retry_count += 1
                job.status = SyncStatus.PENDING
                await self.job_queue.put(job)
                logger.info(f"Retrying job {job.job_id} (attempt {job.retry_count})")
            else:
                # Publish failure event
                await self.event_bus.publish("sync.job.failed", {
                    "job_id": job.job_id,
                    "errors": job.errors
                })
                
    async def _default_sync_handler(self, job: SyncJob, engine: 'SyncEngine') -> SyncResult:
        """Default sync handler implementation."""
        records_synced = 0
        records_failed = 0
        records_skipped = 0
        errors = []
        
        try:
            # Get data from source
            source_data = await self._fetch_source_data(job)
            job.total_records = len(source_data)
            
            # Process in batches
            for i in range(0, len(source_data), job.batch_size):
                if job.status == SyncStatus.PAUSED:
                    # Wait while paused
                    while job.status == SyncStatus.PAUSED:
                        await asyncio.sleep(1)
                
                if job.status == SyncStatus.CANCELLED:
                    break
                
                batch = source_data[i:i + job.batch_size]
                
                # Apply transformations
                if job.transform_rules:
                    batch = await self._apply_transformations(batch, job.transform_rules)
                
                # Sync batch to target
                try:
                    success_count = await self._sync_batch_to_target(batch, job)
                    records_synced += success_count
                    records_failed += len(batch) - success_count
                except Exception as e:
                    logger.error(f"Error syncing batch: {e}")
                    records_failed += len(batch)
                    errors.append({
                        "batch": i,
                        "error": str(e)
                    })
                
                # Update progress
                job.processed_records = records_synced + records_failed + records_skipped
                
                # Publish progress event
                await self.event_bus.publish("sync.job.progress", {
                    "job_id": job.job_id,
                    "progress": job.progress_percentage,
                    "processed": job.processed_records,
                    "total": job.total_records
                })
        
        except Exception as e:
            logger.error(f"Error in sync handler: {e}")
            errors.append({"error": str(e)})
        
        # Create result
        duration = job.duration_seconds or 0
        
        return SyncResult(
            job_id=job.job_id,
            success=records_failed == 0,
            records_synced=records_synced,
            records_failed=records_failed,
            records_skipped=records_skipped,
            duration_seconds=duration,
            errors=errors
        )
        
    async def _fetch_source_data(self, job: SyncJob) -> List[Dict[str, Any]]:
        """Fetch data from source."""
        # This would implement actual data fetching logic
        # For now, return empty list
        return []
        
    async def _apply_transformations(
        self,
        data: List[Dict[str, Any]],
        rules: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Apply transformation rules to data."""
        transformed_data = data
        
        for rule in rules:
            rule_type = rule.get("type")
            
            if rule_type == "map":
                # Field mapping
                field_map = rule.get("field_map", {})
                transformed_data = [
                    {field_map.get(k, k): v for k, v in record.items()}
                    for record in transformed_data
                ]
            
            elif rule_type == "filter":
                # Filtering
                filter_func = rule.get("filter_func")
                if filter_func and filter_func in self.transform_functions:
                    func = self.transform_functions[filter_func]
                    transformed_data = [r for r in transformed_data if func(r)]
            
            elif rule_type == "custom":
                # Custom transformation
                transform_func = rule.get("transform_func")
                if transform_func and transform_func in self.transform_functions:
                    func = self.transform_functions[transform_func]
                    transformed_data = [func(r) for r in transformed_data]
        
        return transformed_data
        
    async def _sync_batch_to_target(
        self,
        batch: List[Dict[str, Any]],
        job: SyncJob
    ) -> int:
        """Sync batch of data to target."""
        # This would implement actual sync logic
        # For now, return batch size
        return len(batch)
        
    async def _get_source_type(self, source: str) -> str:
        """Get source type from source name."""
        # This would determine source type
        # For now, return default
        return "default"
        
    def _register_default_handlers(self):
        """Register default sync handlers."""
        # Register handlers for different source types
        pass
        
    async def _process_scheduled_jobs(self):
        """Background task to process scheduled jobs."""
        while True:
            try:
                now = datetime.utcnow()
                
                for job_id, scheduled in list(self.scheduled_jobs.items()):
                    if scheduled["next_run"] <= now:
                        # Create job instance
                        job = scheduled["job"]
                        
                        # Queue job
                        await self.job_queue.put(job)
                        
                        # Calculate next run
                        schedule = scheduled["schedule"]
                        scheduled["next_run"] = self._calculate_next_run(schedule)
                        
                        logger.info(f"Triggered scheduled job: {job_id}")
                
                # Sleep for 30 seconds
                await asyncio.sleep(30)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}")
                await asyncio.sleep(30)
                
    def _calculate_next_run(self, schedule: Dict[str, Any]) -> datetime:
        """Calculate next run time for scheduled job."""
        schedule_type = schedule.get("type", "interval")
        
        if schedule_type == "interval":
            # Interval-based scheduling
            interval_seconds = schedule.get("interval_seconds", 3600)
            return datetime.utcnow() + timedelta(seconds=interval_seconds)
        
        elif schedule_type == "cron":
            # Cron-based scheduling (simplified)
            # This would implement cron parsing
            return datetime.utcnow() + timedelta(hours=1)
        
        else:
            # Default to 1 hour
            return datetime.utcnow() + timedelta(hours=1)
            
    async def _monitor_jobs(self):
        """Background task to monitor job health."""
        while True:
            try:
                # Check for stuck jobs
                for job_id, job in list(self.active_jobs.items()):
                    if job.status == SyncStatus.RUNNING:
                        # Check if job is stuck
                        if job.duration_seconds and job.duration_seconds > 3600:  # 1 hour
                            logger.warning(f"Job {job_id} has been running for over 1 hour")
                            
                            # Publish warning event
                            await self.event_bus.publish("sync.job.stuck", {
                                "job_id": job_id,
                                "duration_seconds": job.duration_seconds
                            })
                
                # Report metrics
                await self.event_bus.publish("sync.metrics", {
                    "active_jobs": len(self.active_jobs),
                    "queued_jobs": self.job_queue.qsize(),
                    "scheduled_jobs": len(self.scheduled_jobs),
                    "total_syncs": self.metrics["total_syncs"],
                    "records_synced": self.metrics["records_synced"]
                })
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in job monitor: {e}")
                await asyncio.sleep(300)
                
    async def _handle_job_create(self, event_data: Dict[str, Any]):
        """Handle job creation event."""
        try:
            await self.create_sync_job(
                source=event_data["source"],
                target=event_data["target"],
                sync_mode=SyncMode(event_data.get("mode", "incremental")),
                filter_criteria=event_data.get("filter"),
                transform_rules=event_data.get("transforms")
            )
        except Exception as e:
            logger.error(f"Error handling job creation: {e}")
            
    async def _handle_job_cancel(self, event_data: Dict[str, Any]):
        """Handle job cancellation event."""
        try:
            job_id = event_data.get("job_id")
            if job_id:
                await self.cancel_job(job_id)
        except Exception as e:
            logger.error(f"Error handling job cancellation: {e}")
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get sync engine statistics."""
        return {
            "active_jobs": len(self.active_jobs),
            "queued_jobs": self.job_queue.qsize(),
            "scheduled_jobs": len(self.scheduled_jobs),
            "completed_jobs": len([j for j in self.job_history if j.status == SyncStatus.COMPLETED]),
            "failed_jobs": len([j for j in self.job_history if j.status == SyncStatus.FAILED]),
            "metrics": dict(self.metrics)
        } 