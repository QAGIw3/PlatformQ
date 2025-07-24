"""
Data Quality Service - Migrated Version

Uses unified quality checking stages from data-intelligence-common.
"""

from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig
from data_intelligence_common.core.processing import (
    UnifiedProcessor, ProcessingConfig, ProcessingMode,
    DataSource, DataSink, ProcessingContext,
    FileSource, DatabaseSource, EventBusSource,
    FileSink, DatabaseSink, EventBusSink,
    # Quality stages
    QualityLevel, QualityCheckType, QualityRule, QualityResult,
    QualityCheckStage, SchemaValidationStage, DataCleaningStage,
    DeduplicationStage, AnomalyDetectionStage, CommonQualityRules
)
from data_intelligence_common.core.events import Event, EventType, create_data_event
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


@dataclass
class DataQualityConfig(UnifiedServiceConfig):
    """Configuration for data quality service"""
    # Quality specific settings
    enable_profiling: bool = True
    enable_anomaly_detection: bool = True
    enable_auto_remediation: bool = False
    
    # Sampling
    default_sample_rate: float = 0.1
    full_scan_schedule: str = "0 2 * * *"  # Daily at 2 AM
    
    # Quality thresholds
    error_threshold: float = 0.01  # 1% error rate
    warning_threshold: float = 0.05  # 5% warning rate
    
    # Storage
    quality_results_retention_days: int = 90
    
    # Notification
    enable_notifications: bool = True
    notification_channels: List[str] = field(default_factory=lambda: ["email", "slack"])


class DataQualityService(DataIntelligenceBaseService):
    """
    Service for data quality management.
    
    Provides comprehensive data quality checking, profiling, and monitoring.
    """
    
    def __init__(self, config: DataQualityConfig):
        super().__init__(config)
        self.config = config
        
        # Quality processors
        self._quality_processors: Dict[str, UnifiedProcessor] = {}
        self._active_scans: Dict[str, asyncio.Task] = {}
        
    async def _initialize_internal(self):
        """Initialize quality-specific components"""
        await super()._initialize_internal()
        
        # Register quality-specific health checks
        self.register_health_check(
            "quality_engine",
            self._check_quality_engine_health,
            critical=True
        )
        
        # Start scheduled quality scans if configured
        if self.config.full_scan_schedule:
            self._start_background_task(self._scheduled_scan_loop())
            
        logger.info("Data quality service initialized")
        
    async def create_quality_pipeline(
        self,
        name: str,
        source: DataSource,
        sink: DataSink,
        rules: List[QualityRule],
        schema: Optional[Dict[str, Any]] = None,
        enable_cleaning: bool = True,
        enable_deduplication: bool = True,
        dedup_keys: Optional[List[str]] = None
    ) -> UnifiedProcessor:
        """
        Create a quality checking pipeline.
        
        Args:
            name: Pipeline name
            source: Data source
            sink: Data sink
            rules: Quality rules to apply
            schema: Optional schema for validation
            enable_cleaning: Enable data cleaning
            enable_deduplication: Enable deduplication
            dedup_keys: Fields to use for deduplication
            
        Returns:
            Configured quality processor
        """
        # Create processing configuration
        processing_config = ProcessingConfig(
            name=f"quality_pipeline_{name}",
            mode=ProcessingMode.ADAPTIVE,
            enable_quality_checks=True,
            quality_sample_rate=self.config.default_sample_rate,
            enable_lineage_tracking=True
        )
        
        # Build pipeline
        builder = UnifiedProcessor.pipeline(processing_config).from_source(source)
        
        # Add schema validation if provided
        if schema:
            builder = builder.transform(SchemaValidationStage(
                schema=schema,
                strict=False,
                coerce_types=True
            ))
            
        # Add data cleaning if enabled
        if enable_cleaning:
            builder = builder.transform(DataCleaningStage(
                trim_strings=True,
                remove_nulls=False,
                default_values={}
            ))
            
        # Add deduplication if enabled
        if enable_deduplication and dedup_keys:
            builder = builder.transform(DeduplicationStage(
                key_fields=dedup_keys,
                window_size=10000,
                strategy="keep_first"
            ))
            
        # Add quality checks
        builder = builder.transform(QualityCheckStage(
            rules=rules,
            fail_on_error=False,  # Don't fail, just mark
            sample_rate=self.config.default_sample_rate,
            collect_metrics=True
        ))
        
        # Add anomaly detection if enabled
        if self.config.enable_anomaly_detection:
            # Determine numeric fields from schema or first record
            numeric_fields = self._get_numeric_fields(schema)
            if numeric_fields:
                builder = builder.transform(AnomalyDetectionStage(
                    numeric_fields=numeric_fields,
                    method="zscore",
                    threshold=3.0,
                    window_size=1000
                ))
                
        # Add quality result processor
        builder = builder.transform(self._create_quality_result_stage())
        
        # Set sink
        builder = builder.to_sink(sink)
        
        # Build processor
        processor = builder.build(
            metrics_collector=self.metrics,
            event_bus=self.event_bus,
            cache_manager=self.cache
        )
        
        # Store processor
        self._quality_processors[name] = processor
        
        return processor
        
    def _create_quality_result_stage(self):
        """Create stage to process quality results"""
        service = self
        
        class QualityResultStage(ProcessingStage):
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                # Extract quality results from context
                quality_results = context.state.get("quality_results", [])
                
                if quality_results:
                    # Calculate quality score
                    total_checks = len(quality_results)
                    passed_checks = sum(1 for r in quality_results if r.passed)
                    quality_score = passed_checks / total_checks if total_checks > 0 else 1.0
                    
                    # Add quality metadata
                    data["_quality_score"] = quality_score
                    data["_quality_checks_total"] = total_checks
                    data["_quality_checks_passed"] = passed_checks
                    data["_quality_level"] = service._determine_quality_level(quality_score)
                    
                    # Emit quality event
                    await service.publish_event(
                        event_type="quality.check_completed",
                        data={
                            "pipeline": context.config.name,
                            "quality_score": quality_score,
                            "checks_total": total_checks,
                            "checks_passed": passed_checks,
                            "failures": [
                                {
                                    "rule": r.rule_name,
                                    "message": r.message,
                                    "level": r.level.value
                                }
                                for r in quality_results if not r.passed
                            ]
                        }
                    )
                    
                    # Send notifications if needed
                    if service.config.enable_notifications and quality_score < service.config.error_threshold:
                        await service._send_quality_alert(
                            pipeline=context.config.name,
                            quality_score=quality_score,
                            failures=quality_results
                        )
                        
                return data
                
        return QualityResultStage()
        
    def _determine_quality_level(self, score: float) -> str:
        """Determine quality level based on score"""
        if score >= 1.0 - self.config.error_threshold:
            return "excellent"
        elif score >= 1.0 - self.config.warning_threshold:
            return "good"
        elif score >= 0.8:
            return "fair"
        else:
            return "poor"
            
    def _get_numeric_fields(self, schema: Optional[Dict[str, Any]]) -> List[str]:
        """Extract numeric fields from schema"""
        if not schema:
            return []
            
        numeric_fields = []
        properties = schema.get("properties", {})
        
        for field, field_schema in properties.items():
            if field_schema.get("type") in ["number", "integer"]:
                numeric_fields.append(field)
                
        return numeric_fields
        
    async def run_quality_check(
        self,
        pipeline_name: str,
        wait_for_completion: bool = True,
        timeout: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """
        Run a quality check pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to run
            wait_for_completion: Whether to wait for completion
            timeout: Maximum time to wait
            
        Returns:
            Quality check results
        """
        processor = self._quality_processors.get(pipeline_name)
        if not processor:
            raise ValueError(f"Pipeline '{pipeline_name}' not found")
            
        # Start processing
        job_id = str(uuid.uuid4())
        
        if wait_for_completion:
            # Run synchronously
            result = await processor.process(job_id=job_id)
            
            # Record metrics
            self.record_operation("quality_check_completed", {
                "pipeline": pipeline_name,
                "records_processed": result.get("records_processed", 0),
                "duration": result.get("duration", 0)
            })
            
            return result
        else:
            # Run asynchronously
            task = asyncio.create_task(processor.process(job_id=job_id))
            self._active_scans[job_id] = task
            
            return {
                "job_id": job_id,
                "status": "running",
                "pipeline": pipeline_name
            }
            
    async def get_quality_metrics(
        self,
        pipeline_name: Optional[str] = None,
        time_range: str = "24h"
    ) -> Dict[str, Any]:
        """
        Get quality metrics for pipelines.
        
        Args:
            pipeline_name: Optional specific pipeline
            time_range: Time range for metrics
            
        Returns:
            Quality metrics
        """
        # This would query stored quality results
        # For now, return sample metrics
        return {
            "overall_quality_score": 0.95,
            "pipelines": {
                pipeline_name or "all": {
                    "quality_score": 0.95,
                    "checks_total": 1000,
                    "checks_passed": 950,
                    "common_failures": [
                        {
                            "rule": "email_format",
                            "count": 30,
                            "percentage": 3.0
                        },
                        {
                            "rule": "date_range",
                            "count": 20,
                            "percentage": 2.0
                        }
                    ]
                }
            },
            "time_range": time_range,
            "last_updated": datetime.utcnow().isoformat()
        }
        
    async def create_quality_rules(
        self,
        dataset_name: str,
        auto_generate: bool = True,
        custom_rules: Optional[List[Dict[str, Any]]] = None
    ) -> List[QualityRule]:
        """
        Create quality rules for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            auto_generate: Whether to auto-generate rules based on profiling
            custom_rules: Custom rules to add
            
        Returns:
            List of quality rules
        """
        rules = []
        
        if auto_generate:
            # Profile dataset to generate rules
            profile = await self._profile_dataset(dataset_name)
            
            # Generate rules based on profile
            for column, stats in profile.get("columns", {}).items():
                # Not null rule for low null percentage
                if stats.get("null_percentage", 0) < 5:
                    rules.append(CommonQualityRules.not_null(column))
                    
                # Range rule for numeric columns
                if stats.get("type") == "numeric":
                    min_val = stats.get("min", 0)
                    max_val = stats.get("max", 100)
                    rules.append(CommonQualityRules.in_range(
                        column,
                        min_val * 0.9,  # 10% tolerance
                        max_val * 1.1
                    ))
                    
                # Pattern rule for string columns
                if stats.get("type") == "string" and stats.get("pattern"):
                    rules.append(CommonQualityRules.matches_pattern(
                        column,
                        stats["pattern"]
                    ))
                    
        # Add custom rules
        if custom_rules:
            for rule_def in custom_rules:
                rule = QualityRule(
                    name=rule_def["name"],
                    check_type=QualityCheckType(rule_def["type"]),
                    level=QualityLevel(rule_def["level"]),
                    condition=eval(rule_def["condition"]),  # In production, use safe eval
                    message=rule_def["message"],
                    fields=rule_def.get("fields", [])
                )
                rules.append(rule)
                
        return rules
        
    async def _profile_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """Profile a dataset to understand its characteristics"""
        # This would analyze the dataset
        # For now, return sample profile
        return {
            "dataset": dataset_name,
            "row_count": 10000,
            "columns": {
                "customer_id": {
                    "type": "string",
                    "null_percentage": 0,
                    "unique_count": 10000,
                    "pattern": r"^CUST\d{6}$"
                },
                "age": {
                    "type": "numeric",
                    "null_percentage": 2,
                    "min": 18,
                    "max": 95,
                    "mean": 42.5,
                    "std": 15.3
                },
                "email": {
                    "type": "string",
                    "null_percentage": 5,
                    "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                }
            }
        }
        
    async def _send_quality_alert(
        self,
        pipeline: str,
        quality_score: float,
        failures: List[QualityResult]
    ):
        """Send quality alert notifications"""
        alert_data = {
            "pipeline": pipeline,
            "quality_score": quality_score,
            "timestamp": datetime.utcnow().isoformat(),
            "failures": [
                {
                    "rule": f.rule_name,
                    "level": f.level.value,
                    "message": f.message
                }
                for f in failures if not f.passed
            ]
        }
        
        # Send to configured channels
        for channel in self.config.notification_channels:
            await self.publish_event(
                event_type=f"quality.alert.{channel}",
                data=alert_data
            )
            
    async def _scheduled_scan_loop(self):
        """Run scheduled quality scans"""
        while True:
            try:
                # Wait for next scheduled time
                await asyncio.sleep(3600)  # Check every hour
                
                # Run full scans for all configured pipelines
                for pipeline_name in self._quality_processors:
                    logger.info(f"Running scheduled scan for pipeline: {pipeline_name}")
                    
                    try:
                        await self.run_quality_check(
                            pipeline_name,
                            wait_for_completion=False
                        )
                    except Exception as e:
                        logger.error(f"Scheduled scan failed for {pipeline_name}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in scheduled scan loop: {e}")
                await asyncio.sleep(60)  # Wait before retry
                
    async def _check_quality_engine_health(self) -> Dict[str, Any]:
        """Check quality engine health"""
        active_pipelines = len(self._quality_processors)
        active_scans = len([t for t in self._active_scans.values() if not t.done()])
        
        return {
            "healthy": active_pipelines > 0,
            "active_pipelines": active_pipelines,
            "active_scans": active_scans
        }
        
    async def _stop_internal(self):
        """Stop quality-specific components"""
        # Cancel active scans
        for task in self._active_scans.values():
            if not task.done():
                task.cancel()
                
        # Stop processors
        for processor in self._quality_processors.values():
            await processor.stop()
            
        await super()._stop_internal()
        
        logger.info("Data quality service stopped")


# Export main components
__all__ = [
    'DataQualityConfig',
    'DataQualityService'
] 