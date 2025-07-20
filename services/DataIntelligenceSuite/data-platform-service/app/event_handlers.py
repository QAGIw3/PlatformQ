"""
Event handlers for Data Platform Service
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json

from platformq_event_framework import BaseEventProcessor, EventMetrics

logger = logging.getLogger(__name__)


class DataPlatformEventHandler(BaseEventProcessor):
    """
    Event handler for data platform events.
    
    Handles query requests, data ingestion events, quality checks,
    governance actions, and pipeline events.
    """
    
    def __init__(self,
                 service_name: str,
                 pulsar_url: str,
                 metrics: EventMetrics,
                 federated_engine,
                 catalog_manager,
                 governance_manager,
                 quality_engine,
                 lineage_tracker,
                 lake_manager,
                 pipeline_manager):
        super().__init__(service_name, pulsar_url, metrics)
        self.federated_engine = federated_engine
        self.catalog_manager = catalog_manager
        self.governance_manager = governance_manager
        self.quality_engine = quality_engine
        self.lineage_tracker = lineage_tracker
        self.lake_manager = lake_manager
        self.pipeline_manager = pipeline_manager
        
    async def handle_query_request(self, event: Dict[str, Any]) -> None:
        """Handle federated query request events"""
        try:
            logger.info(f"Processing query request: {event.get('query_id')}")
            
            query = event.get('query')
            tenant_id = event.get('tenant_id')
            user_id = event.get('user_id')
            
            # Check governance policies
            allowed = await self.governance_manager.check_query_permission(
                user_id=user_id,
                query=query,
                tenant_id=tenant_id
            )
            
            if not allowed:
                await self.publish_event(
                    f"persistent://platformq/{tenant_id}/query-denied-events",
                    {
                        "query_id": event.get('query_id'),
                        "reason": "Governance policy violation",
                        "denied_at": datetime.utcnow().isoformat()
                    }
                )
                return
            
            # Execute federated query
            result = await self.federated_engine.execute_query(
                query=query,
                tenant_id=tenant_id,
                user_id=user_id
            )
            
            # Track lineage
            await self.lineage_tracker.track_query_lineage(
                query_id=result.query_id,
                query=query,
                accessed_assets=result.accessed_assets
            )
            
            # Publish completion event
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/query-completed-events",
                {
                    "query_id": result.query_id,
                    "status": "completed",
                    "rows_returned": result.row_count,
                    "execution_time_ms": result.execution_time_ms,
                    "completed_at": datetime.utcnow().isoformat()
                }
            )
            
            self.metrics.record_success("query_request")
            
        except Exception as e:
            logger.error(f"Error handling query request: {e}")
            self.metrics.record_failure("query_request", str(e))
            
    async def handle_data_ingestion(self, event: Dict[str, Any]) -> None:
        """Handle data ingestion events"""
        try:
            logger.info(f"Processing data ingestion: {event.get('job_id')}")
            
            dataset_id = event.get('dataset_id')
            source_config = event.get('source_config')
            tenant_id = event.get('tenant_id')
            
            # Start ingestion
            job = await self.lake_manager.ingest_data(
                dataset_id=dataset_id,
                source_config=source_config,
                tenant_id=tenant_id
            )
            
            # Register in catalog
            await self.catalog_manager.update_asset(
                asset_id=dataset_id,
                last_updated=datetime.utcnow(),
                size_bytes=job.bytes_ingested
            )
            
            # Trigger quality check
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/quality-check-requested-events",
                {
                    "asset_id": dataset_id,
                    "trigger": "post_ingestion",
                    "job_id": job.job_id
                }
            )
            
            self.metrics.record_success("data_ingestion")
            
        except Exception as e:
            logger.error(f"Error handling data ingestion: {e}")
            self.metrics.record_failure("data_ingestion", str(e))
            
    async def handle_quality_check(self, event: Dict[str, Any]) -> None:
        """Handle data quality check events"""
        try:
            logger.info(f"Processing quality check: {event.get('check_id')}")
            
            asset_id = event.get('asset_id')
            profile_id = event.get('profile_id')
            tenant_id = event.get('tenant_id')
            
            # Execute quality check
            result = await self.quality_engine.run_quality_check(
                asset_id=asset_id,
                profile_id=profile_id,
                tenant_id=tenant_id
            )
            
            # Update catalog with quality score
            await self.catalog_manager.update_asset(
                asset_id=asset_id,
                quality_score=result.overall_score,
                last_quality_check=datetime.utcnow()
            )
            
            # Check for anomalies
            if result.overall_score < 0.7:  # Threshold
                await self.publish_event(
                    f"persistent://platformq/{tenant_id}/quality-anomaly-detected-events",
                    {
                        "asset_id": asset_id,
                        "check_id": result.check_id,
                        "score": result.overall_score,
                        "issues": result.issues_found,
                        "detected_at": datetime.utcnow().isoformat()
                    }
                )
            
            self.metrics.record_success("quality_check")
            
        except Exception as e:
            logger.error(f"Error handling quality check: {e}")
            self.metrics.record_failure("quality_check", str(e))
            
    async def handle_governance_action(self, event: Dict[str, Any]) -> None:
        """Handle governance action events"""
        try:
            action_type = event.get('action_type')
            logger.info(f"Processing governance action: {action_type}")
            
            if action_type == 'access_request':
                await self._handle_access_request(event)
            elif action_type == 'policy_violation':
                await self._handle_policy_violation(event)
            elif action_type == 'compliance_check':
                await self._handle_compliance_check(event)
            
            self.metrics.record_success(f"governance_{action_type}")
            
        except Exception as e:
            logger.error(f"Error handling governance action: {e}")
            self.metrics.record_failure("governance_action", str(e))
            
    async def _handle_access_request(self, event: Dict[str, Any]) -> None:
        """Handle data access request"""
        request_id = event.get('request_id')
        asset_id = event.get('asset_id')
        user_id = event.get('user_id')
        tenant_id = event.get('tenant_id')
        
        # Check automated approval rules
        auto_approved = await self.governance_manager.check_auto_approval(
            user_id=user_id,
            asset_id=asset_id,
            purpose=event.get('purpose')
        )
        
        if auto_approved:
            await self.governance_manager.approve_access(
                request_id=request_id,
                approved_by='system',
                auto_approved=True
            )
            
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/access-granted-events",
                {
                    "request_id": request_id,
                    "asset_id": asset_id,
                    "user_id": user_id,
                    "auto_approved": True,
                    "granted_at": datetime.utcnow().isoformat()
                }
            )
            
    async def _handle_policy_violation(self, event: Dict[str, Any]) -> None:
        """Handle policy violation detection"""
        violation_id = event.get('violation_id')
        policy_id = event.get('policy_id')
        severity = event.get('severity')
        tenant_id = event.get('tenant_id')
        
        # Take remediation action based on severity
        if severity == 'critical':
            # Block access immediately
            await self.governance_manager.block_access(
                asset_id=event.get('asset_id'),
                user_id=event.get('user_id'),
                reason=f"Policy violation: {policy_id}"
            )
            
        # Log audit entry
        await self.governance_manager.log_audit(
            action='policy_violation',
            details=event,
            tenant_id=tenant_id
        )
        
    async def _handle_compliance_check(self, event: Dict[str, Any]) -> None:
        """Handle compliance check request"""
        standard = event.get('standard')  # GDPR, HIPAA, etc.
        tenant_id = event.get('tenant_id')
        
        # Run compliance assessment
        findings = await self.governance_manager.assess_compliance(
            standard=standard,
            tenant_id=tenant_id
        )
        
        if findings:
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/compliance-findings-events",
                {
                    "standard": standard,
                    "findings": findings,
                    "assessed_at": datetime.utcnow().isoformat()
                }
            )
            
    async def handle_pipeline_event(self, event: Dict[str, Any]) -> None:
        """Handle pipeline execution events"""
        try:
            event_type = event.get('event_type')
            logger.info(f"Processing pipeline event: {event_type}")
            
            pipeline_id = event.get('pipeline_id')
            tenant_id = event.get('tenant_id')
            
            if event_type == 'pipeline_started':
                # Track pipeline execution
                await self.pipeline_manager.track_execution(
                    pipeline_id=pipeline_id,
                    run_id=event.get('run_id'),
                    status='started'
                )
                
            elif event_type == 'pipeline_completed':
                # Update lineage with pipeline results
                await self.lineage_tracker.track_pipeline_lineage(
                    pipeline_id=pipeline_id,
                    sources=event.get('sources', []),
                    targets=event.get('targets', []),
                    transformations=event.get('transformations', [])
                )
                
                # Trigger quality checks on output datasets
                for target in event.get('targets', []):
                    await self.publish_event(
                        f"persistent://platformq/{tenant_id}/quality-check-requested-events",
                        {
                            "asset_id": target,
                            "trigger": "pipeline_output",
                            "pipeline_id": pipeline_id
                        }
                    )
                    
            elif event_type == 'pipeline_failed':
                # Handle pipeline failure
                await self.pipeline_manager.handle_failure(
                    pipeline_id=pipeline_id,
                    run_id=event.get('run_id'),
                    error=event.get('error')
                )
                
            self.metrics.record_success(f"pipeline_{event_type}")
            
        except Exception as e:
            logger.error(f"Error handling pipeline event: {e}")
            self.metrics.record_failure("pipeline_event", str(e))
            
    async def handle_catalog_update(self, event: Dict[str, Any]) -> None:
        """Handle catalog update events"""
        try:
            logger.info(f"Processing catalog update: {event.get('asset_id')}")
            
            asset_id = event.get('asset_id')
            update_type = event.get('update_type')
            tenant_id = event.get('tenant_id')
            
            if update_type == 'schema_change':
                # Analyze impact of schema changes
                impact = await self.lineage_tracker.analyze_impact(
                    node_id=asset_id,
                    change_type='schema',
                    changes=event.get('changes', {})
                )
                
                if impact.affected_nodes:
                    await self.publish_event(
                        f"persistent://platformq/{tenant_id}/schema-impact-detected-events",
                        {
                            "asset_id": asset_id,
                            "affected_assets": [n['node_id'] for n in impact.affected_nodes],
                            "impact_analysis": impact.dict(),
                            "detected_at": datetime.utcnow().isoformat()
                        }
                    )
                    
            # Update catalog metadata
            await self.catalog_manager.update_asset(
                asset_id=asset_id,
                **event.get('updates', {})
            )
            
            self.metrics.record_success("catalog_update")
            
        except Exception as e:
            logger.error(f"Error handling catalog update: {e}")
            self.metrics.record_failure("catalog_update", str(e)) 