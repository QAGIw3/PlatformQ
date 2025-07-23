"""
Base Processor for DataIntelligenceSuite

Provides common processing patterns and abstractions.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, List, Callable, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import uuid

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import MetricsCollector
from ...core.caching import CacheManager

logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Processing status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class ProcessingMode(Enum):
    """Processing mode"""
    BATCH = "batch"
    STREAM = "stream"
    MICRO_BATCH = "micro_batch"


@dataclass
class ProcessorConfig:
    """Base configuration for processors"""
    name: str
    mode: ProcessingMode = ProcessingMode.BATCH
    parallelism: int = 1
    retry_attempts: int = 3
    retry_delay: timedelta = timedelta(seconds=5)
    timeout: Optional[timedelta] = None
    checkpoint_interval: Optional[timedelta] = None
    
    # Resource limits
    max_memory_mb: Optional[int] = None
    max_cpu_cores: Optional[float] = None
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True
    
    # Caching
    enable_caching: bool = True
    cache_results: bool = False
    cache_ttl: Optional[timedelta] = None
    
    # Security
    enable_encryption: bool = True
    encryption_key: str = "processor-data"
    required_roles: List[str] = field(default_factory=list)
    
    # Vault/Consul
    use_dynamic_credentials: bool = True
    credential_refresh_interval: timedelta = timedelta(hours=1)
    
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingResult:
    """Result of processing operation"""
    job_id: str
    status: ProcessingStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    
    # Metrics
    records_processed: int = 0
    records_failed: int = 0
    processing_time_ms: Optional[float] = None
    throughput_records_per_sec: Optional[float] = None
    
    # Output
    output_location: Optional[str] = None
    output_format: Optional[str] = None
    
    # Errors and warnings
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    
    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseProcessor(ABC):
    """
    Base processor for all data processing operations with Vault/Consul integration.
    
    Features:
    - Unified processing interface
    - Built-in retry logic
    - Progress tracking
    - Metrics collection
    - Caching support
    - Checkpointing
    - Dynamic credential management
    - Secure configuration
    """
    
    def __init__(
        self,
        config: ProcessorConfig,
        metrics_collector: Optional[MetricsCollector] = None,
        cache_manager: Optional[CacheManager] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.config = config
        self.metrics = metrics_collector
        self.cache = cache_manager
        self.vault_client = vault_client
        self.consul_client = consul_client
        
        # Processing state
        self._jobs: Dict[str, ProcessingResult] = {}
        self._active_jobs: Dict[str, asyncio.Task] = {}
        self._checkpoints: Dict[str, Any] = {}
        
        # Callbacks
        self._progress_callbacks: List[Callable] = []
        self._completion_callbacks: List[Callable] = []
        
        # Security
        self._credentials: Dict[str, Any] = {}
        self._credential_task: Optional[asyncio.Task] = None
        self._processor_config: Dict[str, Any] = {}
        self._config_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize processor with Vault/Consul"""
        # Load configuration from Consul
        if self.consul_client:
            await self._load_processor_config()
            self._config_task = asyncio.create_task(self._watch_config_changes())
            
        # Start credential renewal if using Vault
        if self.vault_client and self.config.use_dynamic_credentials:
            await self._refresh_credentials()
            self._credential_task = asyncio.create_task(self._credential_renewal_loop())
            
        logger.info(f"Initialized processor: {self.config.name}")
        
    async def shutdown(self):
        """Shutdown processor"""
        # Cancel background tasks
        if self._credential_task:
            self._credential_task.cancel()
            try:
                await self._credential_task
            except asyncio.CancelledError:
                pass
                
        if self._config_task:
            self._config_task.cancel()
            try:
                await self._config_task
            except asyncio.CancelledError:
                pass
                
        # Cancel active jobs
        for job_id, task in self._active_jobs.items():
            task.cancel()
            
        logger.info(f"Shutdown processor: {self.config.name}")
        
    async def _load_processor_config(self):
        """Load processor configuration from Consul"""
        try:
            config_key = f"data-intelligence/processors/{self.config.name}/config"
            config_data = await self.consul_client.kv_get(config_key)
            if config_data:
                self._processor_config = json.loads(config_data)
                # Update config with Consul values
                for key, value in self._processor_config.items():
                    if hasattr(self.config, key):
                        setattr(self.config, key, value)
                logger.info(f"Loaded processor config from Consul")
        except Exception as e:
            logger.error(f"Failed to load processor config: {e}")
            
    async def _watch_config_changes(self):
        """Watch for configuration changes in Consul"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                await self._load_processor_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config: {e}")
                
    async def _refresh_credentials(self):
        """Refresh credentials from Vault"""
        try:
            # Get database credentials if needed
            if self._processor_config.get("database_enabled"):
                db_mount = self._processor_config.get("database_mount", "database")
                db_role = self._processor_config.get("database_role", f"{self.config.name}-readonly")
                creds = await self.vault_client.get_database_credentials(db_mount, db_role)
                self._credentials["database"] = creds
                
            # Get API keys if needed
            if self._processor_config.get("api_keys"):
                for key_name in self._processor_config["api_keys"]:
                    secret_path = f"secret/data/processors/{self.config.name}/{key_name}"
                    secret = await self.vault_client.read_secret(secret_path)
                    if secret:
                        self._credentials[key_name] = secret.get("value")
                        
            logger.info("Refreshed processor credentials from Vault")
        except Exception as e:
            logger.error(f"Failed to refresh credentials: {e}")
            
    async def _credential_renewal_loop(self):
        """Background task to renew credentials"""
        while True:
            try:
                await asyncio.sleep(self.config.credential_refresh_interval.total_seconds())
                await self._refresh_credentials()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in credential renewal: {e}")
        
    @abstractmethod
    async def process(self, data: Any, job_id: Optional[str] = None) -> ProcessingResult:
        """Process data - must be implemented by subclasses"""
        pass
        
    async def submit_job(
        self,
        data: Any,
        job_id: Optional[str] = None,
        priority: int = 0,
        user_context: Optional[Dict[str, Any]] = None
    ) -> str:
        """Submit a processing job with access control"""
        if job_id is None:
            job_id = str(uuid.uuid4())
            
        # Check role requirements
        if self.config.required_roles and user_context:
            user_roles = user_context.get("roles", [])
            if not any(role in self.config.required_roles for role in user_roles):
                raise PermissionError(f"User lacks required roles: {self.config.required_roles}")
            
        # Check if job already exists
        if job_id in self._jobs:
            raise ValueError(f"Job {job_id} already exists")
            
        # Encrypt sensitive data if enabled
        if self.config.enable_encryption and self.vault_client:
            data = await self._encrypt_job_data(data)
            
        # Create job result
        result = ProcessingResult(
            job_id=job_id,
            status=ProcessingStatus.PENDING,
            started_at=datetime.utcnow(),
            metadata={"priority": priority, "user": user_context.get("user_id") if user_context else None}
        )
        self._jobs[job_id] = result
        
        # Start processing task
        task = asyncio.create_task(self._process_with_retry(data, job_id))
        self._active_jobs[job_id] = task
        
        logger.info(f"Submitted job {job_id} for processing")
        return job_id
        
    async def _encrypt_job_data(self, data: Any) -> Dict[str, Any]:
        """Encrypt sensitive job data"""
        import json
        plaintext = json.dumps(data, default=str)
        encrypted = await self.vault_client.transit_encrypt(
            self.config.encryption_key,
            plaintext
        )
        return {
            "encrypted": encrypted["ciphertext"],
            "metadata": {
                "encrypted_at": datetime.utcnow().isoformat(),
                "key_version": encrypted.get("key_version", 1)
            }
        }
        
    async def _decrypt_job_data(self, data: Dict[str, Any]) -> Any:
        """Decrypt job data"""
        if "encrypted" in data:
            decrypted = await self.vault_client.transit_decrypt(
                self.config.encryption_key,
                data["encrypted"]
            )
            import json
            return json.loads(decrypted)
        return data
        
    async def get_job_status(self, job_id: str) -> Optional[ProcessingResult]:
        """Get status of a processing job"""
        return self._jobs.get(job_id)
        
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a processing job"""
        if job_id not in self._active_jobs:
            return False
            
        task = self._active_jobs[job_id]
        task.cancel()
        
        # Update status
        if job_id in self._jobs:
            self._jobs[job_id].status = ProcessingStatus.CANCELLED
            self._jobs[job_id].completed_at = datetime.utcnow()
            
        logger.info(f"Cancelled job {job_id}")
        return True
        
    def add_progress_callback(self, callback: Callable):
        """Add progress callback"""
        self._progress_callbacks.append(callback)
        
    def add_completion_callback(self, callback: Callable):
        """Add completion callback"""
        self._completion_callbacks.append(callback)
        
    async def _process_with_retry(self, data: Any, job_id: str):
        """Process with retry logic"""
        result = self._jobs[job_id]
        result.status = ProcessingStatus.RUNNING
        result.started_at = datetime.utcnow()
        
        # Decrypt data if needed
        if isinstance(data, dict) and "encrypted" in data:
            data = await self._decrypt_job_data(data)
        
        attempts = 0
        last_error = None
        
        while attempts < self.config.retry_attempts:
            try:
                # Check cache if enabled
                if self.config.enable_caching and self.cache:
                    cache_key = self._generate_cache_key(data)
                    cached_result = await self.cache.get(f"{self.config.name}_results", cache_key)
                    if cached_result:
                        logger.info(f"Job {job_id} found in cache")
                        result.status = ProcessingStatus.COMPLETED
                        result.completed_at = datetime.utcnow()
                        result.metadata["cached"] = True
                        await self._notify_completion(result)
                        return
                        
                # Process data
                processing_result = await self._process_with_timeout(data, job_id)
                
                # Update result
                result.status = processing_result.status
                result.records_processed = processing_result.records_processed
                result.records_failed = processing_result.records_failed
                result.output_location = processing_result.output_location
                result.errors = processing_result.errors
                result.warnings = processing_result.warnings
                
                # Calculate metrics
                result.completed_at = datetime.utcnow()
                result.processing_time_ms = (
                    result.completed_at - result.started_at
                ).total_seconds() * 1000
                
                if result.records_processed > 0 and result.processing_time_ms > 0:
                    result.throughput_records_per_sec = (
                        result.records_processed / (result.processing_time_ms / 1000)
                    )
                    
                # Cache result if enabled
                if self.config.cache_results and self.cache and result.status == ProcessingStatus.COMPLETED:
                    await self.cache.put(
                        f"{self.config.name}_results",
                        cache_key,
                        processing_result,
                        self.config.cache_ttl
                    )
                    
                # Record metrics
                self._record_metrics(result)
                
                # Notify completion
                await self._notify_completion(result)
                
                # Success - exit retry loop
                break
                
            except asyncio.TimeoutError:
                logger.warning(f"Job {job_id} timed out (attempt {attempts + 1})")
                last_error = "Processing timeout"
                result.warnings.append({
                    "type": "timeout",
                    "attempt": attempts + 1,
                    "message": f"Timed out after {self.config.timeout}"
                })
                
            except Exception as e:
                logger.error(f"Job {job_id} failed (attempt {attempts + 1}): {e}")
                last_error = e
                result.errors.append({
                    "type": type(e).__name__,
                    "attempt": attempts + 1,
                    "message": str(e)
                })
                
            attempts += 1
            
            if attempts < self.config.retry_attempts:
                result.status = ProcessingStatus.RETRYING
                await asyncio.sleep(self.config.retry_delay.total_seconds())
                
        # All retries failed
        result.status = ProcessingStatus.FAILED
        result.completed_at = datetime.utcnow()
        result.metadata["final_error"] = str(last_error)
        
        await self._notify_completion(result)
        del self._active_jobs[job_id]
        
    async def _process_with_timeout(self, data: Any, job_id: str) -> ProcessingResult:
        """Process with timeout"""
        if self.config.timeout:
            return await asyncio.wait_for(
                self.process(data, job_id),
                self.config.timeout.total_seconds()
            )
        else:
            return await self.process(data, job_id)
            
    def _generate_cache_key(self, data: Any) -> str:
        """Generate cache key for data"""
        # Simple implementation - can be overridden
        import hashlib
        import json
        
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()
        
    async def _notify_progress(self, job_id: str, progress: float, message: Optional[str] = None):
        """Notify progress callbacks"""
        for callback in self._progress_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(job_id, progress, message)
                else:
                    callback(job_id, progress, message)
            except Exception as e:
                logger.error(f"Progress callback error: {e}")
                
    async def _notify_completion(self, result: ProcessingResult):
        """Notify completion callbacks"""
        for callback in self._completion_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(result)
                else:
                    callback(result)
            except Exception as e:
                logger.error(f"Completion callback error: {e}")
                
    def _record_metrics(self, result: ProcessingResult):
        """Record processing metrics"""
        if not self.metrics:
            return
            
        labels = {
            "processor": self.config.name,
            "mode": self.config.mode.value,
            "status": result.status.value
        }
        
        # Record counters
        self.metrics.increment_counter("processing_jobs_total", labels)
        self.metrics.increment_counter("processing_records_total", labels, result.records_processed)
        
        if result.records_failed > 0:
            self.metrics.increment_counter("processing_failures_total", labels, result.records_failed)
            
        # Record timing
        if result.processing_time_ms:
            self.metrics.observe_histogram(
                "processing_duration_ms",
                result.processing_time_ms,
                labels
            )
            
        # Record throughput
        if result.throughput_records_per_sec:
            self.metrics.set_gauge(
                "processing_throughput",
                result.throughput_records_per_sec,
                labels
            )
            
    async def checkpoint(self, job_id: str, state: Any):
        """Save checkpoint for job"""
        checkpoint_data = {
            "state": state,
            "timestamp": datetime.utcnow()
        }
        
        # Encrypt checkpoint if enabled
        if self.config.enable_encryption and self.vault_client:
            import json
            plaintext = json.dumps(checkpoint_data, default=str)
            encrypted = await self.vault_client.transit_encrypt(
                f"{self.config.encryption_key}-checkpoint",
                plaintext
            )
            checkpoint_data = {"encrypted": encrypted["ciphertext"]}
            
        self._checkpoints[job_id] = checkpoint_data
        
        # Also save to cache if available
        if self.cache and self.config.checkpoint_interval:
            await self.cache.put(
                f"{self.config.name}_checkpoints",
                job_id,
                checkpoint_data,
                self.config.checkpoint_interval * 2  # Keep for 2x checkpoint interval
            )
            
        logger.debug(f"Saved checkpoint for job {job_id}")
        
    async def restore_checkpoint(self, job_id: str) -> Optional[Any]:
        """Restore checkpoint for job"""
        checkpoint_data = self._checkpoints.get(job_id)
        
        if not checkpoint_data and self.cache:
            # Try to load from cache
            checkpoint_data = await self.cache.get(
                f"{self.config.name}_checkpoints",
                job_id
            )
            
        if checkpoint_data:
            # Decrypt if needed
            if isinstance(checkpoint_data, dict) and "encrypted" in checkpoint_data:
                decrypted = await self.vault_client.transit_decrypt(
                    f"{self.config.encryption_key}-checkpoint",
                    checkpoint_data["encrypted"]
                )
                import json
                checkpoint_data = json.loads(decrypted)
                
            return checkpoint_data.get("state")
            
        return None
        
    def get_credentials(self, key: str) -> Optional[Any]:
        """Get credentials for a specific key"""
        return self._credentials.get(key)
        
    def get_processor_config(self, key: str, default: Any = None) -> Any:
        """Get processor configuration value"""
        return self._processor_config.get(key, default) 