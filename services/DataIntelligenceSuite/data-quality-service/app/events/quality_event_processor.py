"""
Quality Event Processor

Processes events related to data quality operations.
"""

from typing import Dict, Any, Optional
import asyncio
from datetime import datetime
import json

from data_intelligence_common import BaseEventProcessor, EventRouter, StructuredLogger
from data_intelligence_common.event_handlers import DataIntelligenceEventHandlers
from platformq_shared.event_subscriber import EventSubscriber

logger = StructuredLogger.get_logger(__name__)


class QualityEventProcessor(BaseEventProcessor):
    """
    Processes data quality related events
    """
    
    def __init__(
        self,
        event_subscriber: EventSubscriber,
        quality_engine,
        quality_monitor,
        rule_engine
    ):
        super().__init__(event_subscriber)
        self.quality_engine = quality_engine
        self.quality_monitor = quality_monitor
        self.rule_engine = rule_engine
        
        # Configure event routing
        self._configure_routes()
    
    def _configure_routes(self):
        """Configure event routing"""
        # Data quality events
        self.router.add_route(
            "data.quality.check.requested",
            self._handle_quality_check_request
        )
        self.router.add_route(
            "data.quality.issue.detected",
            self._handle_quality_issue
        )
        self.router.add_route(
            "data.quality.remediation.requested",
            self._handle_remediation_request
        )
        
        # Dataset events
        self.router.add_route(
            "dataset.created",
            self._handle_dataset_created
        )
        self.router.add_route(
            "dataset.updated",
            self._handle_dataset_updated
        )
        self.router.add_route(
            "dataset.transformation.completed",
            self._handle_transformation_completed
        )
        
        # Pipeline events
        self.router.add_route(
            "pipeline.stage.completed",
            self._handle_pipeline_stage_completed
        )
        self.router.add_route(
            "pipeline.failed",
            self._handle_pipeline_failed
        )
        
        # ML events
        self.router.add_route(
            "ml.training.completed",
            DataIntelligenceEventHandlers.handle_ml_training_completed
        )
        
        # Use common handlers for some events
        self.router.add_route(
            "data.lineage.update",
            DataIntelligenceEventHandlers.handle_lineage_update
        )
        self.router.add_route(
            "error.occurred",
            DataIntelligenceEventHandlers.handle_error
        )
    
    async def _handle_quality_check_request(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle quality check request"""
        try:
            dataset = event.get("dataset")
            check_type = event.get("check_type", "full")
            rules = event.get("rules")
            
            logger.info(
                "processing_quality_check_request",
                dataset=dataset,
                check_type=check_type
            )
            
            # Perform quality check
            result = await self.quality_engine.check_quality(
                dataset=dataset,
                check_type=check_type,
                rule_ids=rules
            )
            
            # Update monitoring
            if self.quality_monitor:
                await self.quality_monitor._record_metrics(dataset, result["metrics"])
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            logger.error("quality_check_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_quality_issue(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle quality issue detection"""
        try:
            dataset = event.get("dataset")
            issue = event.get("issue")
            severity = event.get("severity", "medium")
            auto_remediate = event.get("auto_remediate", False)
            
            logger.info(
                "processing_quality_issue",
                dataset=dataset,
                issue_type=issue.get("type"),
                severity=severity
            )
            
            # Record issue
            issue_id = f"issue_{datetime.utcnow().timestamp()}"
            
            # Check if auto-remediation is enabled
            if auto_remediate and severity in ["low", "medium"]:
                # Queue for remediation
                remediation_result = await self.quality_engine._queue_remediation({
                    "issue_id": issue_id,
                    "dataset": dataset,
                    "issue": issue,
                    "severity": severity
                })
                
                return {
                    "success": True,
                    "issue_id": issue_id,
                    "remediation_queued": True,
                    "remediation_id": remediation_result.get("remediation_id")
                }
            
            return {
                "success": True,
                "issue_id": issue_id,
                "remediation_queued": False
            }
            
        except Exception as e:
            logger.error("quality_issue_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_remediation_request(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle remediation request"""
        try:
            dataset = event.get("dataset")
            issue_ids = event.get("issue_ids", [])
            strategy = event.get("strategy", "auto")
            
            logger.info(
                "processing_remediation_request",
                dataset=dataset,
                issue_count=len(issue_ids),
                strategy=strategy
            )
            
            # Perform remediation
            result = await self.quality_engine.remediation_orchestrator.remediate_issues(
                dataset=dataset,
                issue_ids=issue_ids,
                strategy=strategy
            )
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            logger.error("remediation_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_dataset_created(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle dataset creation"""
        try:
            dataset = event.get("dataset")
            
            logger.info("processing_dataset_created", dataset=dataset)
            
            # Profile new dataset
            profile = await self.quality_engine.profile_dataset(
                dataset=dataset,
                profile_type="initial"
            )
            
            # Add to monitoring
            datasets = await self.quality_monitor._get_monitored_datasets()
            if dataset not in datasets:
                datasets.append(dataset)
                await self.quality_monitor.vault_consul.consul.kv.put(
                    "data-quality/monitored-datasets",
                    json.dumps(datasets)
                )
            
            return {
                "success": True,
                "profile": profile
            }
            
        except Exception as e:
            logger.error("dataset_created_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_dataset_updated(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle dataset update"""
        try:
            dataset = event.get("dataset")
            update_type = event.get("update_type", "data")
            
            logger.info(
                "processing_dataset_updated",
                dataset=dataset,
                update_type=update_type
            )
            
            # Check quality after update
            result = await self.quality_engine.check_quality(
                dataset=dataset,
                check_type="incremental"
            )
            
            # Check for drift if significant update
            if update_type == "schema":
                drift = await self.quality_engine.profiler.detect_drift(
                    dataset=dataset,
                    reference_period_days=7,
                    current_period_days=1
                )
                result["drift_detected"] = drift.get("drift_detected", False)
            
            return {
                "success": True,
                "result": result
            }
            
        except Exception as e:
            logger.error("dataset_updated_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_transformation_completed(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle transformation completion"""
        try:
            source_dataset = event.get("source_dataset")
            target_dataset = event.get("target_dataset")
            transformation_id = event.get("transformation_id")
            
            logger.info(
                "processing_transformation_completed",
                source=source_dataset,
                target=target_dataset,
                transformation_id=transformation_id
            )
            
            # Validate transformation output
            validation_result = await self.quality_engine.validate_transformation({
                "source": source_dataset,
                "target": target_dataset,
                "transformation_id": transformation_id
            })
            
            # Profile target dataset
            if validation_result.get("valid", False):
                profile = await self.quality_engine.profile_dataset(
                    dataset=target_dataset,
                    profile_type="post_transformation"
                )
                validation_result["profile"] = profile
            
            return {
                "success": True,
                "validation": validation_result
            }
            
        except Exception as e:
            logger.error("transformation_completed_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_pipeline_stage_completed(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline stage completion"""
        try:
            pipeline_id = event.get("pipeline_id")
            stage_name = event.get("stage_name")
            output_dataset = event.get("output_dataset")
            
            logger.info(
                "processing_pipeline_stage_completed",
                pipeline_id=pipeline_id,
                stage=stage_name
            )
            
            # Run quality checks on stage output
            if output_dataset:
                result = await self.quality_engine.check_quality(
                    dataset=output_dataset,
                    check_type="pipeline_validation",
                    auto_remediate=False  # Don't auto-remediate pipeline outputs
                )
                
                # If critical issues found, may need to halt pipeline
                critical_issues = [
                    i for i in result.get("issues", [])
                    if i.get("severity") == "critical"
                ]
                
                if critical_issues:
                    logger.warning(
                        "critical_issues_in_pipeline",
                        pipeline_id=pipeline_id,
                        stage=stage_name,
                        issue_count=len(critical_issues)
                    )
                
                return {
                    "success": True,
                    "quality_check": result,
                    "proceed": len(critical_issues) == 0
                }
            
            return {"success": True}
            
        except Exception as e:
            logger.error("pipeline_stage_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _handle_pipeline_failed(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline failure"""
        try:
            pipeline_id = event.get("pipeline_id")
            stage_name = event.get("stage_name")
            error = event.get("error")
            
            logger.info(
                "processing_pipeline_failed",
                pipeline_id=pipeline_id,
                stage=stage_name,
                error=error
            )
            
            # Check if failure was due to data quality
            if "quality" in str(error).lower() or "validation" in str(error).lower():
                # Log quality-related failure
                await self._log_quality_failure({
                    "pipeline_id": pipeline_id,
                    "stage": stage_name,
                    "error": error,
                    "timestamp": datetime.utcnow()
                })
            
            return {"success": True}
            
        except Exception as e:
            logger.error("pipeline_failed_error", error=str(e))
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _log_quality_failure(self, failure_data: Dict[str, Any]):
        """Log quality-related failures"""
        try:
            # Store failure information for analysis
            key = f"data-quality/failures/{failure_data['pipeline_id']}/{datetime.utcnow().isoformat()}"
            await self.quality_monitor.vault_consul.consul.kv.put(
                key,
                json.dumps(failure_data)
            )
        except Exception as e:
            logger.error("log_failure_error", error=str(e)) 