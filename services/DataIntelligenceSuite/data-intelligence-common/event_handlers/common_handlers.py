"""Common event handlers for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataIntelligenceEventHandlers:
    """Common event handlers for DataIntelligenceSuite services."""
    
    @staticmethod
    async def handle_data_quality_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data quality events."""
        dataset_id = event_data.get("dataset_id")
        quality_score = event_data.get("quality_score")
        issues = event_data.get("issues", [])
        
        logger.info(
            f"Data quality event received",
            extra={
                "dataset_id": dataset_id,
                "quality_score": quality_score,
                "issue_count": len(issues)
            }
        )
        
        # Process quality issues
        result = {
            "dataset_id": dataset_id,
            "processed_at": datetime.utcnow().isoformat(),
            "actions_taken": []
        }
        
        # Handle critical issues
        critical_issues = [i for i in issues if i.get("severity") == "critical"]
        if critical_issues:
            result["actions_taken"].append("flagged_for_review")
            
        return result
        
    @staticmethod
    async def handle_ml_training_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle ML training events."""
        model_id = event_data.get("model_id")
        status = event_data.get("status")
        metrics = event_data.get("metrics", {})
        
        logger.info(
            f"ML training event received",
            extra={
                "model_id": model_id,
                "status": status,
                "metrics": metrics
            }
        )
        
        result = {
            "model_id": model_id,
            "processed_at": datetime.utcnow().isoformat()
        }
        
        # Handle training completion
        if status == "completed":
            result["next_steps"] = ["validate_model", "update_registry"]
        elif status == "failed":
            result["next_steps"] = ["investigate_failure", "retry_training"]
            
        return result
        
    @staticmethod
    async def handle_lineage_update_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data lineage update events."""
        entity_id = event_data.get("entity_id")
        entity_type = event_data.get("entity_type")
        operation = event_data.get("operation")
        
        logger.info(
            f"Lineage update event received",
            extra={
                "entity_id": entity_id,
                "entity_type": entity_type,
                "operation": operation
            }
        )
        
        result = {
            "entity_id": entity_id,
            "processed_at": datetime.utcnow().isoformat(),
            "lineage_updated": True
        }
        
        return result
        
    @staticmethod
    async def handle_pipeline_status_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle pipeline status events."""
        pipeline_id = event_data.get("pipeline_id")
        status = event_data.get("status")
        stage = event_data.get("stage")
        
        logger.info(
            f"Pipeline status event received",
            extra={
                "pipeline_id": pipeline_id,
                "status": status,
                "stage": stage
            }
        )
        
        result = {
            "pipeline_id": pipeline_id,
            "processed_at": datetime.utcnow().isoformat()
        }
        
        # Handle pipeline failures
        if status == "failed":
            result["recovery_action"] = "trigger_retry"
            result["notification_sent"] = True
            
        return result
        
    @staticmethod
    async def handle_resource_allocation_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle resource allocation events."""
        resource_type = event_data.get("resource_type")
        action = event_data.get("action")
        quantity = event_data.get("quantity")
        
        logger.info(
            f"Resource allocation event received",
            extra={
                "resource_type": resource_type,
                "action": action,
                "quantity": quantity
            }
        )
        
        result = {
            "resource_type": resource_type,
            "action": action,
            "processed_at": datetime.utcnow().isoformat(),
            "success": True
        }
        
        return result
        
    @staticmethod
    async def handle_error_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Handle error events from any service."""
        service_name = event_data.get("service_name")
        error_type = event_data.get("error_type")
        error_message = event_data.get("error_message")
        
        logger.error(
            f"Error event received",
            extra={
                "service_name": service_name,
                "error_type": error_type,
                "error_message": error_message
            }
        )
        
        result = {
            "service_name": service_name,
            "error_type": error_type,
            "processed_at": datetime.utcnow().isoformat(),
            "actions_taken": []
        }
        
        # Determine actions based on error type
        if error_type == "database_connection":
            result["actions_taken"].append("alert_sent")
            result["actions_taken"].append("retry_scheduled")
        elif error_type == "out_of_memory":
            result["actions_taken"].append("resource_scaling_triggered")
            
        return result 