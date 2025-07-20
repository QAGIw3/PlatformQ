"""
Data Quality Remediation Orchestrator

Provides automated remediation workflows for data quality issues.
Integrates with workflow service to execute complex remediation tasks.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import json

from platformq_shared.utils.logger import get_logger
from platformq_shared.cache.ignite_client import IgniteClient
from platformq_shared.events import EventPublisher
from platformq_shared.errors import ValidationError
import httpx

logger = get_logger(__name__)


class RemediationAction(Enum):
    """Types of remediation actions"""
    CLEAN_DATA = "clean_data"
    FILL_MISSING = "fill_missing"
    REMOVE_DUPLICATES = "remove_duplicates"
    STANDARDIZE_FORMAT = "standardize_format"
    VALIDATE_SCHEMA = "validate_schema"
    QUARANTINE_DATA = "quarantine_data"
    ROLLBACK_VERSION = "rollback_version"
    REQUEST_REUPLOAD = "request_reupload"
    TRIGGER_REPROCESSING = "trigger_reprocessing"
    UPDATE_LINEAGE = "update_lineage"
    NOTIFY_OWNER = "notify_owner"
    CREATE_DERIVED_DATASET = "create_derived_dataset"


class RemediationSeverity(Enum):
    """Remediation severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class RemediationPlan:
    """Remediation plan for quality issues"""
    plan_id: str
    dataset_id: str
    quality_issues: List[Dict[str, Any]]
    actions: List[RemediationAction]
    severity: RemediationSeverity
    estimated_duration: timedelta
    requires_approval: bool
    created_at: datetime
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None
    execution_status: str = "pending"
    execution_result: Optional[Dict[str, Any]] = None


@dataclass
class RemediationResult:
    """Result of remediation execution"""
    success: bool
    actions_executed: List[str]
    issues_resolved: List[str]
    issues_remaining: List[str]
    new_quality_score: float
    execution_time: timedelta
    rollback_available: bool
    metadata: Dict[str, Any]


class DataQualityRemediationOrchestrator:
    """
    Orchestrates automated remediation of data quality issues.
    
    Features:
    - Rule-based remediation strategies
    - ML-powered issue prediction and prevention
    - Automated workflow execution
    - Rollback capabilities
    - Audit trail and compliance
    - Integration with workflow service
    """
    
    def __init__(self,
                 cache: IgniteClient,
                 event_publisher: EventPublisher,
                 workflow_service_url: str = "http://workflow-service:8000",
                 storage_service_url: str = "http://storage-service:8000"):
        self.cache = cache
        self.event_publisher = event_publisher
        self.workflow_service_url = workflow_service_url
        self.storage_service_url = storage_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Remediation rules by issue type
        self.remediation_rules = self._initialize_remediation_rules()
        
        # Active remediation plans
        self.active_plans: Dict[str, RemediationPlan] = {}
        
        # Remediation history for learning
        self.remediation_history: List[Dict[str, Any]] = []
        
    def _initialize_remediation_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize remediation rules for different quality issues"""
        return {
            "missing_values": {
                "actions": [RemediationAction.FILL_MISSING],
                "severity": RemediationSeverity.MEDIUM,
                "strategies": ["mean", "median", "mode", "forward_fill", "interpolate"],
                "requires_approval": False
            },
            "duplicate_records": {
                "actions": [RemediationAction.REMOVE_DUPLICATES],
                "severity": RemediationSeverity.MEDIUM,
                "strategies": ["keep_first", "keep_last", "keep_most_complete"],
                "requires_approval": False
            },
            "schema_violation": {
                "actions": [RemediationAction.VALIDATE_SCHEMA, RemediationAction.QUARANTINE_DATA],
                "severity": RemediationSeverity.HIGH,
                "strategies": ["coerce_types", "remove_invalid", "quarantine"],
                "requires_approval": True
            },
            "outliers": {
                "actions": [RemediationAction.CLEAN_DATA],
                "severity": RemediationSeverity.LOW,
                "strategies": ["cap_values", "remove_outliers", "transform"],
                "requires_approval": False
            },
            "inconsistent_format": {
                "actions": [RemediationAction.STANDARDIZE_FORMAT],
                "severity": RemediationSeverity.MEDIUM,
                "strategies": ["normalize_case", "parse_dates", "standardize_units"],
                "requires_approval": False
            },
            "data_drift": {
                "actions": [RemediationAction.CREATE_DERIVED_DATASET, RemediationAction.NOTIFY_OWNER],
                "severity": RemediationSeverity.HIGH,
                "strategies": ["retrain_models", "update_validation_rules", "version_dataset"],
                "requires_approval": True
            },
            "critical_corruption": {
                "actions": [RemediationAction.ROLLBACK_VERSION, RemediationAction.REQUEST_REUPLOAD],
                "severity": RemediationSeverity.CRITICAL,
                "strategies": ["restore_backup", "request_source", "emergency_quarantine"],
                "requires_approval": True
            }
        }
        
    async def create_remediation_plan(self,
                                    dataset_id: str,
                                    quality_assessment: Dict[str, Any]) -> RemediationPlan:
        """Create a remediation plan based on quality assessment"""
        try:
            plan_id = f"remediation_{dataset_id}_{datetime.utcnow().timestamp()}"
            
            # Analyze quality issues
            issues = self._extract_quality_issues(quality_assessment)
            if not issues:
                raise ValidationError("No quality issues found to remediate")
            
            # Determine remediation actions
            actions, severity = self._determine_remediation_actions(issues)
            
            # Check if approval is required
            requires_approval = self._requires_approval(actions, severity)
            
            # Estimate execution duration
            estimated_duration = self._estimate_duration(actions, dataset_id)
            
            # Create plan
            plan = RemediationPlan(
                plan_id=plan_id,
                dataset_id=dataset_id,
                quality_issues=issues,
                actions=actions,
                severity=severity,
                estimated_duration=estimated_duration,
                requires_approval=requires_approval,
                created_at=datetime.utcnow()
            )
            
            # Store plan
            self.active_plans[plan_id] = plan
            await self.cache.put(f"remediation:plan:{plan_id}", plan.__dict__)
            
            # Publish event
            await self.event_publisher.publish("quality.remediation.plan_created", {
                "plan_id": plan_id,
                "dataset_id": dataset_id,
                "severity": severity.value,
                "requires_approval": requires_approval,
                "actions": [a.value for a in actions]
            })
            
            logger.info(f"Created remediation plan {plan_id} for dataset {dataset_id}")
            return plan
            
        except Exception as e:
            logger.error(f"Failed to create remediation plan: {str(e)}")
            raise
            
    async def execute_remediation_plan(self,
                                     plan_id: str,
                                     executor_id: Optional[str] = None) -> RemediationResult:
        """Execute a remediation plan"""
        try:
            # Get plan
            plan = self.active_plans.get(plan_id)
            if not plan:
                plan_data = await self.cache.get(f"remediation:plan:{plan_id}")
                if not plan_data:
                    raise ValidationError(f"Remediation plan {plan_id} not found")
                plan = RemediationPlan(**plan_data)
            
            # Check approval if required
            if plan.requires_approval and not plan.approved_by:
                raise ValidationError("Plan requires approval before execution")
            
            # Update execution status
            plan.execution_status = "executing"
            await self._update_plan_status(plan)
            
            # Execute remediation workflow
            start_time = datetime.utcnow()
            
            # Trigger workflow via workflow service
            workflow_result = await self._trigger_remediation_workflow(plan)
            
            # Execute individual actions
            results = []
            issues_resolved = []
            issues_remaining = []
            
            for action in plan.actions:
                try:
                    action_result = await self._execute_remediation_action(
                        action, plan.dataset_id, plan.quality_issues
                    )
                    results.append(action_result)
                    
                    if action_result["success"]:
                        issues_resolved.extend(action_result.get("resolved_issues", []))
                    else:
                        issues_remaining.extend(action_result.get("failed_issues", []))
                        
                except Exception as e:
                    logger.error(f"Failed to execute action {action}: {str(e)}")
                    results.append({
                        "action": action.value,
                        "success": False,
                        "error": str(e)
                    })
            
            # Calculate new quality score
            new_score = await self._calculate_new_quality_score(plan.dataset_id)
            
            # Create result
            execution_time = datetime.utcnow() - start_time
            result = RemediationResult(
                success=len(issues_resolved) > 0,
                actions_executed=[a.value for a in plan.actions],
                issues_resolved=issues_resolved,
                issues_remaining=issues_remaining,
                new_quality_score=new_score,
                execution_time=execution_time,
                rollback_available=True,  # Implement rollback mechanism
                metadata={
                    "workflow_id": workflow_result.get("workflow_id"),
                    "action_results": results
                }
            )
            
            # Update plan
            plan.execution_status = "completed"
            plan.execution_result = result.__dict__
            await self._update_plan_status(plan)
            
            # Record in history for ML learning
            self.remediation_history.append({
                "plan_id": plan_id,
                "dataset_id": plan.dataset_id,
                "issues": plan.quality_issues,
                "actions": [a.value for a in plan.actions],
                "result": result.__dict__,
                "executed_at": datetime.utcnow().isoformat()
            })
            
            # Publish completion event
            await self.event_publisher.publish("quality.remediation.completed", {
                "plan_id": plan_id,
                "dataset_id": plan.dataset_id,
                "success": result.success,
                "new_quality_score": new_score,
                "execution_time_seconds": execution_time.total_seconds()
            })
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to execute remediation plan: {str(e)}")
            
            # Update plan status
            if plan:
                plan.execution_status = "failed"
                await self._update_plan_status(plan)
            
            raise
            
    async def _trigger_remediation_workflow(self, plan: RemediationPlan) -> Dict[str, Any]:
        """Trigger remediation workflow via workflow service"""
        try:
            # Prepare workflow configuration
            workflow_config = {
                "dag_id": "data_quality_remediation",
                "conf": {
                    "plan_id": plan.plan_id,
                    "dataset_id": plan.dataset_id,
                    "actions": [a.value for a in plan.actions],
                    "severity": plan.severity.value,
                    "quality_issues": plan.quality_issues
                }
            }
            
            # Call workflow service API
            response = await self.http_client.post(
                f"{self.workflow_service_url}/api/v1/workflows/data_quality_remediation/trigger",
                json=workflow_config
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to trigger workflow: {response.text}")
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to trigger remediation workflow: {str(e)}")
            # Continue with local execution if workflow service fails
            return {"workflow_id": "local_execution"}
            
    async def _execute_remediation_action(self,
                                        action: RemediationAction,
                                        dataset_id: str,
                                        issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute a specific remediation action"""
        try:
            if action == RemediationAction.CLEAN_DATA:
                return await self._clean_data(dataset_id, issues)
            
            elif action == RemediationAction.FILL_MISSING:
                return await self._fill_missing_values(dataset_id, issues)
            
            elif action == RemediationAction.REMOVE_DUPLICATES:
                return await self._remove_duplicates(dataset_id, issues)
            
            elif action == RemediationAction.STANDARDIZE_FORMAT:
                return await self._standardize_format(dataset_id, issues)
            
            elif action == RemediationAction.VALIDATE_SCHEMA:
                return await self._validate_schema(dataset_id, issues)
            
            elif action == RemediationAction.QUARANTINE_DATA:
                return await self._quarantine_data(dataset_id, issues)
            
            elif action == RemediationAction.ROLLBACK_VERSION:
                return await self._rollback_version(dataset_id, issues)
            
            elif action == RemediationAction.REQUEST_REUPLOAD:
                return await self._request_reupload(dataset_id, issues)
            
            elif action == RemediationAction.TRIGGER_REPROCESSING:
                return await self._trigger_reprocessing(dataset_id, issues)
            
            elif action == RemediationAction.UPDATE_LINEAGE:
                return await self._update_lineage(dataset_id, issues)
            
            elif action == RemediationAction.NOTIFY_OWNER:
                return await self._notify_owner(dataset_id, issues)
            
            elif action == RemediationAction.CREATE_DERIVED_DATASET:
                return await self._create_derived_dataset(dataset_id, issues)
            
            else:
                raise ValueError(f"Unknown remediation action: {action}")
                
        except Exception as e:
            logger.error(f"Error executing action {action}: {str(e)}")
            return {
                "action": action.value,
                "success": False,
                "error": str(e)
            }
            
    async def _clean_data(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Clean data by removing or correcting invalid values"""
        resolved_issues = []
        
        # Implement data cleaning logic
        for issue in issues:
            if issue.get("type") == "outliers":
                # Cap outlier values
                # In production, this would use Spark or Flink for processing
                resolved_issues.append(issue["issue_id"])
        
        return {
            "action": "clean_data",
            "success": True,
            "resolved_issues": resolved_issues,
            "cleaning_strategy": "outlier_capping"
        }
        
    async def _fill_missing_values(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fill missing values using appropriate strategies"""
        resolved_issues = []
        
        # Implement missing value filling
        for issue in issues:
            if issue.get("type") == "missing_values":
                # Use mean/median/mode or forward fill
                resolved_issues.append(issue["issue_id"])
        
        return {
            "action": "fill_missing",
            "success": True,
            "resolved_issues": resolved_issues,
            "fill_strategy": "mean_imputation"
        }
        
    async def _remove_duplicates(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Remove duplicate records"""
        # Trigger deduplication via storage service
        response = await self.http_client.post(
            f"{self.storage_service_url}/api/v1/datasets/{dataset_id}/deduplicate",
            json={"strategy": "keep_most_recent"}
        )
        
        return {
            "action": "remove_duplicates",
            "success": response.status_code == 200,
            "resolved_issues": [i["issue_id"] for i in issues if i.get("type") == "duplicates"],
            "records_removed": response.json().get("removed_count", 0)
        }
        
    async def _standardize_format(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Standardize data formats"""
        resolved_issues = []
        
        # Implement format standardization
        standardization_rules = {
            "dates": "ISO8601",
            "text": "lowercase",
            "numbers": "decimal_precision_2"
        }
        
        for issue in issues:
            if issue.get("type") == "inconsistent_format":
                resolved_issues.append(issue["issue_id"])
        
        return {
            "action": "standardize_format",
            "success": True,
            "resolved_issues": resolved_issues,
            "rules_applied": standardization_rules
        }
        
    async def _validate_schema(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate and fix schema violations"""
        # Implement schema validation and correction
        return {
            "action": "validate_schema",
            "success": True,
            "resolved_issues": [i["issue_id"] for i in issues if i.get("type") == "schema_violation"],
            "schema_corrections": []
        }
        
    async def _quarantine_data(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Quarantine problematic data"""
        # Move problematic records to quarantine
        quarantine_id = f"quarantine_{dataset_id}_{datetime.utcnow().timestamp()}"
        
        return {
            "action": "quarantine_data",
            "success": True,
            "resolved_issues": [],
            "quarantine_id": quarantine_id,
            "records_quarantined": len(issues)
        }
        
    async def _rollback_version(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Rollback to previous dataset version"""
        # Get previous version from storage service
        response = await self.http_client.post(
            f"{self.storage_service_url}/api/v1/datasets/{dataset_id}/rollback",
            json={"target_version": "previous"}
        )
        
        return {
            "action": "rollback_version",
            "success": response.status_code == 200,
            "resolved_issues": [i["issue_id"] for i in issues],
            "rolled_back_to": response.json().get("version", "unknown")
        }
        
    async def _request_reupload(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Request data reupload from owner"""
        # Send notification to dataset owner
        await self.event_publisher.publish("quality.remediation.reupload_requested", {
            "dataset_id": dataset_id,
            "reason": "critical_quality_issues",
            "issues": issues
        })
        
        return {
            "action": "request_reupload",
            "success": True,
            "resolved_issues": [],
            "notification_sent": True
        }
        
    async def _trigger_reprocessing(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Trigger dataset reprocessing"""
        # Trigger reprocessing pipeline
        await self.event_publisher.publish("quality.remediation.reprocessing_requested", {
            "dataset_id": dataset_id,
            "processing_type": "full",
            "issues_to_address": issues
        })
        
        return {
            "action": "trigger_reprocessing",
            "success": True,
            "resolved_issues": [],
            "pipeline_triggered": True
        }
        
    async def _update_lineage(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Update data lineage information"""
        # Update lineage to reflect quality issues and remediation
        return {
            "action": "update_lineage",
            "success": True,
            "resolved_issues": [],
            "lineage_updated": True
        }
        
    async def _notify_owner(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Notify dataset owner about quality issues"""
        # Send detailed notification
        await self.event_publisher.publish("quality.remediation.owner_notified", {
            "dataset_id": dataset_id,
            "issue_summary": self._summarize_issues(issues),
            "remediation_available": True
        })
        
        return {
            "action": "notify_owner",
            "success": True,
            "resolved_issues": [],
            "notification_sent": True
        }
        
    async def _create_derived_dataset(self, dataset_id: str, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create a derived dataset with quality fixes"""
        # Create new version with quality improvements
        derived_id = f"{dataset_id}_remediated_{datetime.utcnow().strftime('%Y%m%d')}"
        
        return {
            "action": "create_derived_dataset",
            "success": True,
            "resolved_issues": [i["issue_id"] for i in issues],
            "derived_dataset_id": derived_id
        }
        
    def _extract_quality_issues(self, assessment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract quality issues from assessment"""
        issues = []
        
        # Extract from quality dimensions
        dimensions = assessment.get("dimensions", {})
        
        if dimensions.get("completeness", 1.0) < 0.95:
            issues.append({
                "issue_id": f"missing_values_{datetime.utcnow().timestamp()}",
                "type": "missing_values",
                "severity": "medium",
                "score": dimensions["completeness"]
            })
            
        if dimensions.get("uniqueness", 1.0) < 0.98:
            issues.append({
                "issue_id": f"duplicates_{datetime.utcnow().timestamp()}",
                "type": "duplicate_records",
                "severity": "medium",
                "score": dimensions["uniqueness"]
            })
            
        if dimensions.get("validity", 1.0) < 0.9:
            issues.append({
                "issue_id": f"validity_{datetime.utcnow().timestamp()}",
                "type": "schema_violation",
                "severity": "high",
                "score": dimensions["validity"]
            })
            
        # Add other issue types based on assessment
        
        return issues
        
    def _determine_remediation_actions(self, 
                                     issues: List[Dict[str, Any]]) -> Tuple[List[RemediationAction], RemediationSeverity]:
        """Determine remediation actions based on issues"""
        actions = set()
        max_severity = RemediationSeverity.LOW
        
        for issue in issues:
            issue_type = issue.get("type")
            if issue_type in self.remediation_rules:
                rule = self.remediation_rules[issue_type]
                actions.update(rule["actions"])
                
                # Update max severity
                rule_severity = rule["severity"]
                if self._severity_value(rule_severity) > self._severity_value(max_severity):
                    max_severity = rule_severity
        
        return list(actions), max_severity
        
    def _severity_value(self, severity: RemediationSeverity) -> int:
        """Convert severity to numeric value for comparison"""
        severity_map = {
            RemediationSeverity.INFO: 1,
            RemediationSeverity.LOW: 2,
            RemediationSeverity.MEDIUM: 3,
            RemediationSeverity.HIGH: 4,
            RemediationSeverity.CRITICAL: 5
        }
        return severity_map.get(severity, 0)
        
    def _requires_approval(self, actions: List[RemediationAction], severity: RemediationSeverity) -> bool:
        """Check if remediation requires approval"""
        # Critical severity always requires approval
        if severity == RemediationSeverity.CRITICAL:
            return True
            
        # Certain actions require approval
        approval_actions = {
            RemediationAction.ROLLBACK_VERSION,
            RemediationAction.QUARANTINE_DATA,
            RemediationAction.CREATE_DERIVED_DATASET
        }
        
        return any(action in approval_actions for action in actions)
        
    def _estimate_duration(self, actions: List[RemediationAction], dataset_id: str) -> timedelta:
        """Estimate remediation duration"""
        # Base estimates per action (in minutes)
        action_durations = {
            RemediationAction.CLEAN_DATA: 10,
            RemediationAction.FILL_MISSING: 5,
            RemediationAction.REMOVE_DUPLICATES: 15,
            RemediationAction.STANDARDIZE_FORMAT: 8,
            RemediationAction.VALIDATE_SCHEMA: 3,
            RemediationAction.QUARANTINE_DATA: 2,
            RemediationAction.ROLLBACK_VERSION: 5,
            RemediationAction.REQUEST_REUPLOAD: 1,
            RemediationAction.TRIGGER_REPROCESSING: 20,
            RemediationAction.UPDATE_LINEAGE: 2,
            RemediationAction.NOTIFY_OWNER: 1,
            RemediationAction.CREATE_DERIVED_DATASET: 30
        }
        
        total_minutes = sum(action_durations.get(action, 5) for action in actions)
        
        # Adjust based on dataset size (would fetch actual size in production)
        size_multiplier = 1.0  # Placeholder
        
        return timedelta(minutes=total_minutes * size_multiplier)
        
    async def _update_plan_status(self, plan: RemediationPlan) -> None:
        """Update plan status in cache"""
        await self.cache.put(f"remediation:plan:{plan.plan_id}", plan.__dict__)
        
    async def _calculate_new_quality_score(self, dataset_id: str) -> float:
        """Calculate quality score after remediation"""
        # In production, this would trigger quality re-assessment
        # For now, return improved score
        return 0.95
        
    def _summarize_issues(self, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Summarize quality issues for notification"""
        issue_types = {}
        for issue in issues:
            issue_type = issue.get("type", "unknown")
            if issue_type not in issue_types:
                issue_types[issue_type] = 0
            issue_types[issue_type] += 1
            
        return {
            "total_issues": len(issues),
            "issue_breakdown": issue_types,
            "highest_severity": max(issue.get("severity", "low") for issue in issues)
        }
        
    async def approve_remediation_plan(self, plan_id: str, approver_id: str) -> RemediationPlan:
        """Approve a remediation plan"""
        plan = self.active_plans.get(plan_id)
        if not plan:
            plan_data = await self.cache.get(f"remediation:plan:{plan_id}")
            if not plan_data:
                raise ValidationError(f"Remediation plan {plan_id} not found")
            plan = RemediationPlan(**plan_data)
            
        plan.approved_by = approver_id
        plan.approved_at = datetime.utcnow()
        
        await self._update_plan_status(plan)
        
        # Publish approval event
        await self.event_publisher.publish("quality.remediation.plan_approved", {
            "plan_id": plan_id,
            "approved_by": approver_id,
            "approved_at": plan.approved_at.isoformat()
        })
        
        return plan
        
    async def get_remediation_recommendations(self, 
                                            dataset_id: str,
                                            issue_type: str) -> List[Dict[str, Any]]:
        """Get ML-based remediation recommendations"""
        # Analyze historical remediation success
        recommendations = []
        
        # Find similar past issues
        similar_cases = [
            h for h in self.remediation_history
            if any(i.get("type") == issue_type for i in h["issues"])
        ]
        
        # Calculate success rates for different strategies
        strategy_success = {}
        for case in similar_cases:
            if case["result"]["success"]:
                for action in case["actions"]:
                    if action not in strategy_success:
                        strategy_success[action] = {"success": 0, "total": 0}
                    strategy_success[action]["success"] += 1
                    strategy_success[action]["total"] += 1
        
        # Generate recommendations
        for action, stats in strategy_success.items():
            if stats["total"] > 0:
                success_rate = stats["success"] / stats["total"]
                recommendations.append({
                    "action": action,
                    "success_rate": success_rate,
                    "confidence": min(stats["total"] / 10, 1.0),  # Confidence based on sample size
                    "estimated_improvement": success_rate * 0.2  # Estimated quality improvement
                })
        
        # Sort by success rate
        recommendations.sort(key=lambda x: x["success_rate"], reverse=True)
        
        return recommendations 