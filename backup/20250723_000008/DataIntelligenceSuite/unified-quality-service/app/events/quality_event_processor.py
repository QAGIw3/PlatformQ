"""
Quality event processor for handling quality-related events
"""

import asyncio
import json
from typing import Dict, Any, Optional, List
from datetime import datetime

from platformq_shared.logging import get_logger
from platformq_shared.event_subscriber import EventSubscriber
from data_intelligence_common import BaseEventProcessor

logger = get_logger(__name__)


class QualityEventProcessor(BaseEventProcessor):
    """Processes quality-related events from the event stream"""
    
    def __init__(self, event_subscriber: EventSubscriber, quality_engine: Any,
                 remediation_orchestrator: Any):
        super().__init__(event_subscriber)
        self.quality_engine = quality_engine
        self.remediation_orchestrator = remediation_orchestrator
        
        # Event handlers
        self.event_handlers = {
            "data.ingestion.completed": self._handle_ingestion_completed,
            "data.transform.completed": self._handle_transform_completed,
            "data.pipeline.completed": self._handle_pipeline_completed,
            "quality.threshold.breached": self._handle_threshold_breached,
            "quality.anomaly.detected": self._handle_anomaly_detected,
            "quality.validation.requested": self._handle_validation_requested,
            "dataset.updated": self._handle_dataset_updated,
            "ml.model.deployed": self._handle_model_deployed
        }
        
        # Processing state
        self._processing_queue: asyncio.Queue = asyncio.Queue()
        self._active_validations: Dict[str, Any] = {}
    
    async def start(self):
        """Start event processing"""
        logger.info("Starting quality event processor")
        
        # Subscribe to events
        await self._subscribe_to_events()
        
        # Start processing task
        self._processing_task = asyncio.create_task(self._process_events())
        
        logger.info("Quality event processor started")
    
    async def stop(self):
        """Stop event processing"""
        logger.info("Stopping quality event processor")
        
        # Stop processing
        if hasattr(self, '_processing_task'):
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        # Unsubscribe from events
        await self._unsubscribe_from_events()
        
        logger.info("Quality event processor stopped")
    
    async def _subscribe_to_events(self):
        """Subscribe to relevant events"""
        for event_type in self.event_handlers.keys():
            await self.event_subscriber.subscribe(
                event_type,
                self._handle_event
            )
    
    async def _unsubscribe_from_events(self):
        """Unsubscribe from events"""
        for event_type in self.event_handlers.keys():
            await self.event_subscriber.unsubscribe(event_type)
    
    async def _handle_event(self, event: Dict[str, Any]):
        """Handle incoming event"""
        event_type = event.get("type")
        
        if event_type in self.event_handlers:
            # Queue event for processing
            await self._processing_queue.put(event)
        else:
            logger.warning(f"Unknown event type: {event_type}")
    
    async def _process_events(self):
        """Process queued events"""
        while True:
            try:
                # Get event from queue
                event = await self._processing_queue.get()
                
                # Process event
                event_type = event.get("type")
                handler = self.event_handlers.get(event_type)
                
                if handler:
                    try:
                        await handler(event)
                    except Exception as e:
                        logger.error(f"Error processing event {event_type}: {str(e)}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processing loop: {str(e)}")
                await asyncio.sleep(1)
    
    # Event handlers
    
    async def _handle_ingestion_completed(self, event: Dict[str, Any]):
        """Handle data ingestion completed event"""
        logger.info("Handling ingestion completed event")
        
        dataset_id = event.get("data", {}).get("dataset_id")
        if not dataset_id:
            return
        
        # Trigger quality validation
        try:
            validation_result = await self.quality_engine.validate_comprehensive(
                dataset_id=dataset_id,
                data=None,  # Load from storage
                custom_rules=None
            )
            
            # Check if remediation needed
            if validation_result.get("total_issues", 0) > 0:
                await self._trigger_remediation(dataset_id, validation_result)
            
            # Publish validation completed event
            await self._publish_event("quality.validation.completed", {
                "dataset_id": dataset_id,
                "quality_score": validation_result.get("overall_score", 0),
                "issues_found": validation_result.get("total_issues", 0)
            })
            
        except Exception as e:
            logger.error(f"Failed to validate ingested data: {str(e)}")
    
    async def _handle_transform_completed(self, event: Dict[str, Any]):
        """Handle data transformation completed event"""
        logger.info("Handling transform completed event")
        
        dataset_id = event.get("data", {}).get("output_dataset_id")
        if not dataset_id:
            return
        
        # Quick quality check
        try:
            validation_result = await self.quality_engine.validate_quick(
                dataset_id=dataset_id,
                data=None,
                dimensions=["completeness", "validity"]
            )
            
            # Log quality metrics
            logger.info(f"Transform quality score: {validation_result.get('overall_score', 0)}")
            
        except Exception as e:
            logger.error(f"Failed to validate transformed data: {str(e)}")
    
    async def _handle_pipeline_completed(self, event: Dict[str, Any]):
        """Handle pipeline completed event"""
        logger.info("Handling pipeline completed event")
        
        pipeline_id = event.get("data", {}).get("pipeline_id")
        output_dataset_id = event.get("data", {}).get("output_dataset_id")
        
        if output_dataset_id:
            # Full quality validation
            try:
                validation_result = await self.quality_engine.validate_comprehensive(
                    dataset_id=output_dataset_id,
                    data=None,
                    custom_rules=None
                )
                
                # Store pipeline quality metrics
                await self._store_pipeline_quality_metrics(
                    pipeline_id,
                    validation_result
                )
                
            except Exception as e:
                logger.error(f"Failed to validate pipeline output: {str(e)}")
    
    async def _handle_threshold_breached(self, event: Dict[str, Any]):
        """Handle quality threshold breached event"""
        logger.info("Handling threshold breached event")
        
        dataset_id = event.get("data", {}).get("dataset_id")
        dimension = event.get("data", {}).get("dimension")
        current_value = event.get("data", {}).get("current_value")
        threshold = event.get("data", {}).get("threshold")
        
        # Trigger automated remediation if configured
        if await self._should_auto_remediate(dataset_id, dimension):
            quality_issues = [{
                "dimension": dimension,
                "severity": "high",
                "current_value": current_value,
                "threshold": threshold,
                "dataset_id": dataset_id
            }]
            
            await self._trigger_remediation(dataset_id, {"issues": quality_issues})
    
    async def _handle_anomaly_detected(self, event: Dict[str, Any]):
        """Handle anomaly detected event"""
        logger.info("Handling anomaly detected event")
        
        dataset_id = event.get("data", {}).get("dataset_id")
        anomalies = event.get("data", {}).get("anomalies", [])
        
        if len(anomalies) > 10:  # Significant anomalies
            # Create quality issues from anomalies
            quality_issues = [
                {
                    "dimension": "accuracy",
                    "issue_type": "anomaly",
                    "column": anomaly.get("column"),
                    "severity": "medium",
                    "details": anomaly
                }
                for anomaly in anomalies[:20]  # Limit to top 20
            ]
            
            # Create remediation plan
            plan = await self.remediation_orchestrator.create_remediation_plan(
                dataset_id=dataset_id,
                quality_issues=quality_issues,
                mode="supervised"
            )
            
            logger.info(f"Created remediation plan {plan.plan_id} for anomalies")
    
    async def _handle_validation_requested(self, event: Dict[str, Any]):
        """Handle explicit validation request"""
        logger.info("Handling validation requested event")
        
        request_id = event.get("data", {}).get("request_id")
        dataset_id = event.get("data", {}).get("dataset_id")
        validation_config = event.get("data", {}).get("config", {})
        
        # Track active validation
        self._active_validations[request_id] = {
            "dataset_id": dataset_id,
            "start_time": datetime.utcnow(),
            "config": validation_config
        }
        
        try:
            # Perform validation
            result = await self.quality_engine.validate_comprehensive(
                dataset_id=dataset_id,
                data=None,
                custom_rules=validation_config.get("rules")
            )
            
            # Publish result
            await self._publish_event("quality.validation.completed", {
                "request_id": request_id,
                "dataset_id": dataset_id,
                "result": result
            })
            
        except Exception as e:
            logger.error(f"Validation failed for request {request_id}: {str(e)}")
            
            # Publish failure
            await self._publish_event("quality.validation.failed", {
                "request_id": request_id,
                "dataset_id": dataset_id,
                "error": str(e)
            })
        
        finally:
            # Remove from active
            self._active_validations.pop(request_id, None)
    
    async def _handle_dataset_updated(self, event: Dict[str, Any]):
        """Handle dataset updated event"""
        logger.info("Handling dataset updated event")
        
        dataset_id = event.get("data", {}).get("dataset_id")
        update_type = event.get("data", {}).get("update_type", "unknown")
        
        # Incremental validation for updates
        if update_type in ["append", "update"]:
            try:
                # Quick validation on updated portions
                result = await self.quality_engine.validate_quick(
                    dataset_id=dataset_id,
                    data=None,
                    dimensions=["completeness", "validity"]
                )
                
                # Update quality trends
                await self.quality_engine.update_quality_trends(dataset_id, result)
                
            except Exception as e:
                logger.error(f"Failed to validate dataset update: {str(e)}")
    
    async def _handle_model_deployed(self, event: Dict[str, Any]):
        """Handle ML model deployed event"""
        logger.info("Handling model deployed event")
        
        model_id = event.get("data", {}).get("model_id")
        model_type = event.get("data", {}).get("model_type")
        
        # Update ML optimizer if quality-related model
        if model_type in ["quality_predictor", "anomaly_detector"]:
            try:
                # Notify ML optimizer about new model
                if hasattr(self.quality_engine, 'ml_optimizer'):
                    # This would trigger model reload in production
                    logger.info(f"New {model_type} model deployed: {model_id}")
                    
            except Exception as e:
                logger.error(f"Failed to update ML models: {str(e)}")
    
    # Helper methods
    
    async def _trigger_remediation(self, dataset_id: str, validation_result: Dict[str, Any]):
        """Trigger remediation for quality issues"""
        quality_issues = validation_result.get("issues", [])
        
        if not quality_issues:
            return
        
        # Determine remediation mode
        mode = "automatic" if await self._should_auto_remediate(dataset_id) else "supervised"
        
        try:
            # Create remediation plan
            plan = await self.remediation_orchestrator.create_remediation_plan(
                dataset_id=dataset_id,
                quality_issues=quality_issues,
                mode=mode
            )
            
            # Execute if automatic
            if mode == "automatic":
                remediation_id = await self.remediation_orchestrator.execute_remediation(
                    plan_id=plan.plan_id,
                    executor_id="event_processor"
                )
                logger.info(f"Started automatic remediation {remediation_id}")
            else:
                # Publish event for manual review
                await self._publish_event("quality.remediation.plan_created", {
                    "plan_id": plan.plan_id,
                    "dataset_id": dataset_id,
                    "issue_count": len(quality_issues),
                    "requires_approval": True
                })
                
        except Exception as e:
            logger.error(f"Failed to trigger remediation: {str(e)}")
    
    async def _should_auto_remediate(self, dataset_id: str, dimension: Optional[str] = None) -> bool:
        """Check if auto-remediation is enabled for dataset/dimension"""
        # This would check configuration in production
        # For now, return False for safety
        return False
    
    async def _store_pipeline_quality_metrics(self, pipeline_id: str, validation_result: Dict[str, Any]):
        """Store quality metrics for pipeline execution"""
        # This would store metrics in time-series database
        logger.info(f"Storing quality metrics for pipeline {pipeline_id}")
    
    async def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish event to event stream"""
        if hasattr(self, 'event_publisher'):
            await self.event_publisher.publish_event(event_type, data)
        else:
            logger.debug(f"Would publish event {event_type}: {data}")


# Export
__all__ = ['QualityEventProcessor'] 