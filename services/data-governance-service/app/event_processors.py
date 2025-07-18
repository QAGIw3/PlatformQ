"""
Data Governance Event Processors

Handles events for data governance, quality monitoring, and compliance.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import uuid
import json

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    DigitalAssetCreated,
    DigitalAssetUpdated,
    DataQualityCheckRequested,
    DataQualityCheckCompleted,
    DataLineageUpdated,
    ComplianceCheckRequested,
    ComplianceCheckCompleted,
    AccessPolicyChanged,
    DataClassificationChanged,
    AssetAccessRequested,
    PolicyViolationDetected
)
from platformq_shared.database import get_db
from .repository import (
    DataAssetRepository,
    DataLineageRepository,
    DataQualityMetricRepository,
    DataQualityProfileRepository,
    DataPolicyRepository,
    AccessPolicyRepository,
    ComplianceCheckRepository,
    DataCatalogRepository
)
from .models import (
    DataAsset,
    DataLineage,
    DataQualityMetric,
    DataQualityProfile,
    ComplianceCheck,
    AssetType,
    DataClassification,
    ComplianceType,
    ComplianceStatus
)
from .services.atlas_client import AtlasClient
from .services.ranger_client import RangerClient
from .services.quality_analyzer import QualityAnalyzer
from .services.compliance_checker import ComplianceChecker

logger = logging.getLogger(__name__)


class DataGovernanceEventProcessor(EventProcessor):
    """Event processor for data governance operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.asset_repo = None
        self.lineage_repo = None
        self.quality_metric_repo = None
        self.quality_profile_repo = None
        self.policy_repo = None
        self.access_repo = None
        self.compliance_repo = None
        self.catalog_repo = None
        
        # Initialize service clients
        self.atlas_client = None
        self.ranger_client = None
        self.quality_analyzer = None
        self.compliance_checker = None
    
    def initialize_resources(self):
        """Initialize repositories and service clients"""
        if not self.asset_repo:
            db = next(get_db())
            self.asset_repo = DataAssetRepository(db)
            self.lineage_repo = DataLineageRepository(db)
            self.quality_metric_repo = DataQualityMetricRepository(db)
            self.quality_profile_repo = DataQualityProfileRepository(db)
            self.policy_repo = DataPolicyRepository(db)
            self.access_repo = AccessPolicyRepository(db)
            self.compliance_repo = ComplianceCheckRepository(db)
            self.catalog_repo = DataCatalogRepository(db)
            
            # Initialize service clients
            self.atlas_client = AtlasClient()
            self.ranger_client = RangerClient()
            self.quality_analyzer = QualityAnalyzer()
            self.compliance_checker = ComplianceChecker()
    
    @event_handler("persistent://public/default/asset-events")
    async def handle_asset_created(self, event: DigitalAssetCreated):
        """Handle new asset creation - register in data governance"""
        self.initialize_resources()
        
        try:
            # Determine if this is a data asset
            if event.asset_type not in ["dataset", "table", "view", "stream", "database"]:
                return
            
            # Create data asset record
            asset = self.asset_repo.create({
                "asset_id": event.asset_id,
                "tenant_id": event.tenant_id,
                "asset_name": event.metadata.get("name", event.asset_id),
                "asset_type": self._map_asset_type(event.asset_type),
                "catalog": event.metadata.get("catalog", "default"),
                "schema": event.metadata.get("schema", "public"),
                "table": event.metadata.get("table"),
                "owner": event.metadata.get("owner", event.creator_id),
                "description": event.metadata.get("description"),
                "tags": event.metadata.get("tags", []),
                "metadata": event.metadata
            })
            
            # Register in Apache Atlas
            if self.atlas_client:
                atlas_guid = await self.atlas_client.create_entity(
                    entity_type=event.asset_type,
                    qualified_name=f"{asset.catalog}.{asset.schema}.{asset.table or asset.asset_name}",
                    name=asset.asset_name,
                    owner=asset.owner,
                    description=asset.description
                )
                
                self.asset_repo.update(asset.id, {"atlas_guid": atlas_guid})
            
            # Create default access policy in Ranger
            if self.ranger_client:
                ranger_policy_id = await self.ranger_client.create_resource_policy(
                    resource_type=asset.asset_type.value,
                    resource_path=f"{asset.catalog}/{asset.schema}/{asset.table or '*'}",
                    users=[asset.owner],
                    permissions=["all"]
                )
                
                self.access_repo.create({
                    "policy_id": str(uuid.uuid4()),
                    "tenant_id": event.tenant_id,
                    "asset_id": asset.asset_id,
                    "policy_name": f"Default policy for {asset.asset_name}",
                    "policy_type": "admin",
                    "principals": [asset.owner],
                    "permissions": ["all"],
                    "ranger_policy_id": ranger_policy_id
                })
            
            # Trigger initial quality assessment
            await self.publish_event(
                DataQualityCheckRequested(
                    asset_id=asset.asset_id,
                    check_type="full_profile",
                    tenant_id=event.tenant_id
                ),
                "persistent://public/default/data-quality-events"
            )
            
            # Trigger initial classification
            await self._classify_asset(asset)
            
            logger.info(f"Registered data asset {asset.asset_id} in governance system")
            
        except Exception as e:
            logger.error(f"Error handling asset created: {e}")
    
    @event_handler("persistent://public/default/data-quality-events")
    async def handle_quality_check_request(self, event: DataQualityCheckRequested):
        """Handle data quality check request"""
        self.initialize_resources()
        
        try:
            asset = self.asset_repo.get_by_asset_id(event.asset_id)
            if not asset:
                logger.error(f"Asset {event.asset_id} not found")
                return
            
            # Run quality analysis
            if event.check_type == "full_profile":
                profile_data = await self.quality_analyzer.profile_asset(
                    catalog=asset.catalog,
                    schema=asset.schema,
                    table=asset.table,
                    sample_size=event.metadata.get("sample_size", 10000)
                )
                
                # Create quality profile
                profile = self.quality_profile_repo.create({
                    "profile_id": str(uuid.uuid4()),
                    "tenant_id": asset.tenant_id,
                    "asset_id": asset.asset_id,
                    "profile_type": "full",
                    "row_count": profile_data["row_count"],
                    "null_count": profile_data["null_count"],
                    "duplicate_count": profile_data["duplicate_count"],
                    "completeness_score": profile_data["completeness_score"],
                    "validity_score": profile_data["validity_score"],
                    "consistency_score": profile_data["consistency_score"],
                    "accuracy_score": profile_data.get("accuracy_score", 0),
                    "uniqueness_score": profile_data["uniqueness_score"],
                    "timeliness_score": profile_data.get("timeliness_score", 100),
                    "overall_score": profile_data["overall_score"],
                    "column_profiles": profile_data["column_profiles"],
                    "data_patterns": profile_data.get("patterns", []),
                    "anomalies": profile_data.get("anomalies", [])
                })
                
                # Create individual metrics
                for metric_type, metric_value in profile_data["metrics"].items():
                    self.quality_metric_repo.create({
                        "metric_id": str(uuid.uuid4()),
                        "tenant_id": asset.tenant_id,
                        "asset_id": asset.asset_id,
                        "metric_type": metric_type,
                        "metric_name": metric_type,
                        "metric_value": metric_value,
                        "threshold": 80.0,  # Default threshold
                        "passed": metric_value >= 80.0
                    })
            
            else:
                # Run specific quality checks
                metrics = await self.quality_analyzer.run_quality_checks(
                    asset=asset,
                    checks=event.metadata.get("checks", ["completeness", "validity"])
                )
                
                for metric in metrics:
                    self.quality_metric_repo.create({
                        "metric_id": str(uuid.uuid4()),
                        "tenant_id": asset.tenant_id,
                        "asset_id": asset.asset_id,
                        "metric_type": metric["type"],
                        "metric_name": metric["name"],
                        "metric_value": metric["value"],
                        "threshold": metric.get("threshold", 80.0),
                        "passed": metric["value"] >= metric.get("threshold", 80.0),
                        "column_name": metric.get("column"),
                        "details": metric.get("details")
                    })
            
            # Publish completion event
            await self.publish_event(
                DataQualityCheckCompleted(
                    asset_id=asset.asset_id,
                    check_type=event.check_type,
                    status="completed",
                    tenant_id=asset.tenant_id
                ),
                "persistent://public/default/data-quality-events"
            )
            
            # Check for policy violations
            await self._check_quality_policies(asset)
            
        except Exception as e:
            logger.error(f"Error handling quality check: {e}")
    
    @event_handler("persistent://public/default/lineage-events")
    async def handle_lineage_update(self, event: DataLineageUpdated):
        """Handle data lineage updates"""
        self.initialize_resources()
        
        try:
            # Create lineage record
            lineage = self.lineage_repo.create({
                "lineage_id": str(uuid.uuid4()),
                "tenant_id": event.tenant_id,
                "source_asset_id": event.source_asset_id,
                "target_asset_id": event.target_asset_id,
                "transformation_type": event.transformation_type,
                "transformation_details": event.transformation_details,
                "pipeline_id": event.pipeline_id,
                "job_id": event.job_id,
                "execution_time": event.execution_time,
                "duration_seconds": event.duration_seconds,
                "records_processed": event.records_processed
            })
            
            # Update Atlas lineage
            if self.atlas_client:
                await self.atlas_client.create_lineage(
                    source_guid=await self._get_atlas_guid(event.source_asset_id),
                    target_guid=await self._get_atlas_guid(event.target_asset_id),
                    process_name=event.transformation_type,
                    process_details=event.transformation_details
                )
            
            # Propagate classifications downstream
            source_asset = self.asset_repo.get_by_asset_id(event.source_asset_id)
            target_asset = self.asset_repo.get_by_asset_id(event.target_asset_id)
            
            if source_asset and target_asset:
                if source_asset.classification.value > target_asset.classification.value:
                    # Target should inherit source's classification
                    self.asset_repo.update(target_asset.id, {
                        "classification": source_asset.classification
                    })
                    
                    await self.publish_event(
                        DataClassificationChanged(
                            asset_id=target_asset.asset_id,
                            old_classification=target_asset.classification.value,
                            new_classification=source_asset.classification.value,
                            reason="Inherited from lineage",
                            tenant_id=target_asset.tenant_id
                        ),
                        "persistent://public/default/governance-events"
                    )
            
            logger.info(f"Updated lineage: {event.source_asset_id} -> {event.target_asset_id}")
            
        except Exception as e:
            logger.error(f"Error handling lineage update: {e}")
    
    @event_handler("persistent://public/default/compliance-events")
    async def handle_compliance_check_request(self, event: ComplianceCheckRequested):
        """Handle compliance check request"""
        self.initialize_resources()
        
        try:
            asset = self.asset_repo.get_by_asset_id(event.asset_id)
            if not asset:
                logger.error(f"Asset {event.asset_id} not found")
                return
            
            # Run compliance checks
            compliance_results = await self.compliance_checker.check_compliance(
                asset=asset,
                compliance_types=event.compliance_types or list(ComplianceType)
            )
            
            for result in compliance_results:
                check = self.compliance_repo.create({
                    "check_id": str(uuid.uuid4()),
                    "tenant_id": asset.tenant_id,
                    "asset_id": asset.asset_id,
                    "compliance_type": result["compliance_type"],
                    "check_name": result["check_name"],
                    "status": result["status"],
                    "score": result.get("score", 0),
                    "findings": result.get("findings", []),
                    "violations": result.get("violations", []),
                    "recommendations": result.get("recommendations", []),
                    "remediation_required": len(result.get("violations", [])) > 0,
                    "remediation_details": result.get("remediation_details"),
                    "checked_by": "system"
                })
                
                # If violations found, create remediation tasks
                if check.remediation_required:
                    await self._create_remediation_tasks(asset, check)
            
            # Publish completion event
            await self.publish_event(
                ComplianceCheckCompleted(
                    asset_id=asset.asset_id,
                    compliance_types=event.compliance_types,
                    status="completed",
                    has_violations=any(r["status"] != "passed" for r in compliance_results),
                    tenant_id=asset.tenant_id
                ),
                "persistent://public/default/compliance-events"
            )
            
        except Exception as e:
            logger.error(f"Error handling compliance check: {e}")
    
    @event_handler("persistent://public/default/access-events")
    async def handle_access_request(self, event: AssetAccessRequested):
        """Handle asset access request"""
        self.initialize_resources()
        
        try:
            # Check if user has access
            has_access = self.access_repo.check_access(
                asset_id=event.asset_id,
                user=event.user_id,
                permission=event.permission
            )
            
            if has_access:
                # Update access statistics
                self.asset_repo.update_access_stats(event.asset_id)
                
                # Log access for audit
                logger.info(f"Access granted: {event.user_id} -> {event.asset_id} ({event.permission})")
            else:
                # Log access denial
                logger.warning(f"Access denied: {event.user_id} -> {event.asset_id} ({event.permission})")
                
                # Check if this is a policy violation
                asset = self.asset_repo.get_by_asset_id(event.asset_id)
                if asset and asset.classification in [DataClassification.RESTRICTED, DataClassification.TOP_SECRET]:
                    await self.publish_event(
                        PolicyViolationDetected(
                            policy_type="access",
                            asset_id=event.asset_id,
                            user_id=event.user_id,
                            violation_details={
                                "attempted_permission": event.permission,
                                "asset_classification": asset.classification.value
                            },
                            tenant_id=asset.tenant_id
                        ),
                        "persistent://public/default/governance-events"
                    )
            
        except Exception as e:
            logger.error(f"Error handling access request: {e}")
    
    async def _classify_asset(self, asset: DataAsset):
        """Classify asset based on content and metadata"""
        try:
            # Scan for sensitive data patterns
            sensitive_patterns = await self.quality_analyzer.scan_sensitive_data(
                catalog=asset.catalog,
                schema=asset.schema,
                table=asset.table
            )
            
            # Determine classification
            classification = DataClassification.PUBLIC
            sensitivity_labels = []
            
            if sensitive_patterns:
                if any(p["type"] in ["SSN", "credit_card", "bank_account"] for p in sensitive_patterns):
                    classification = DataClassification.RESTRICTED
                    sensitivity_labels.extend(["PII", "Financial"])
                elif any(p["type"] in ["email", "phone", "address"] for p in sensitive_patterns):
                    classification = DataClassification.CONFIDENTIAL
                    sensitivity_labels.append("PII")
                elif any(p["type"] in ["health_info", "medical_record"] for p in sensitive_patterns):
                    classification = DataClassification.RESTRICTED
                    sensitivity_labels.extend(["PHI", "HIPAA"])
                else:
                    classification = DataClassification.INTERNAL
            
            # Update asset classification
            self.asset_repo.update(asset.id, {
                "classification": classification,
                "sensitivity_labels": sensitivity_labels
            })
            
            # Update Atlas classification
            if self.atlas_client and asset.atlas_guid:
                for label in sensitivity_labels:
                    await self.atlas_client.add_classification(
                        entity_guid=asset.atlas_guid,
                        classification_name=label
                    )
            
        except Exception as e:
            logger.error(f"Error classifying asset: {e}")
    
    async def _check_quality_policies(self, asset: DataAsset):
        """Check if quality metrics violate any policies"""
        try:
            # Get applicable quality policies
            policies = self.policy_repo.get_policies_for_asset(asset)
            quality_policies = [p for p in policies if p.policy_type.value == "quality"]
            
            # Get latest metrics
            metrics = self.quality_metric_repo.get_latest_metrics(asset.asset_id)
            
            for policy in quality_policies:
                for rule in policy.rules:
                    metric_type = rule.get("metric_type")
                    threshold = rule.get("threshold")
                    
                    metric = next((m for m in metrics if m.metric_type == metric_type), None)
                    if metric and metric.metric_value < threshold:
                        await self.publish_event(
                            PolicyViolationDetected(
                                policy_type="quality",
                                policy_id=policy.policy_id,
                                asset_id=asset.asset_id,
                                violation_details={
                                    "metric_type": metric_type,
                                    "actual_value": metric.metric_value,
                                    "required_threshold": threshold
                                },
                                tenant_id=asset.tenant_id
                            ),
                            "persistent://public/default/governance-events"
                        )
            
        except Exception as e:
            logger.error(f"Error checking quality policies: {e}")
    
    async def _create_remediation_tasks(self, asset: DataAsset, compliance_check: ComplianceCheck):
        """Create remediation tasks for compliance violations"""
        # This would integrate with a task management system
        # For now, just log the requirement
        logger.info(f"Remediation required for {asset.asset_id}: {compliance_check.violations}")
    
    async def _get_atlas_guid(self, asset_id: str) -> Optional[str]:
        """Get Atlas GUID for an asset"""
        asset = self.asset_repo.get_by_asset_id(asset_id)
        return asset.atlas_guid if asset else None 