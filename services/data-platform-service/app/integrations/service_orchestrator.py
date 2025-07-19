"""
Service Orchestrator

Coordinates interactions between all integrated services:
- ML Platform Service
- Event Router Service
- Analytics Service
- Storage Service
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio

from .ml_platform import MLPlatformIntegration
from .event_router import EventRouterIntegration
from .analytics import AnalyticsIntegration
from .storage import StorageIntegration

logger = logging.getLogger(__name__)


class ServiceOrchestrator:
    """Orchestrates cross-service operations"""
    
    def __init__(self,
                 ml_integration: MLPlatformIntegration,
                 event_integration: EventRouterIntegration,
                 analytics_integration: AnalyticsIntegration,
                 storage_integration: StorageIntegration):
        self.ml = ml_integration
        self.events = event_integration
        self.analytics = analytics_integration
        self.storage = storage_integration
        
        # Active workflows
        self.workflows = {}
        
    async def create_ml_training_workflow(self,
                                        data_source_query: str,
                                        feature_config: Dict[str, Any],
                                        model_config: Dict[str, Any],
                                        training_schedule: Optional[str] = None) -> str:
        """
        Create end-to-end ML training workflow
        
        1. Extract data using federated query
        2. Prepare features and save to lake
        3. Submit training job to ML platform
        4. Monitor training progress
        5. Archive model artifacts
        
        Args:
            data_source_query: Query to extract training data
            feature_config: Feature engineering configuration
            model_config: Model training configuration
            training_schedule: Optional schedule for recurring training
            
        Returns:
            Workflow ID
        """
        workflow_id = f"ml_workflow_{datetime.utcnow().timestamp()}"
        
        try:
            # Step 1: Execute query and save results
            query_result = await self.analytics.execute_unified_query(
                query=data_source_query,
                cache_results=False
            )
            
            # Step 2: Prepare training data
            training_data = await self.ml.prepare_training_data(
                source_path=query_result["storage_path"],
                feature_config=feature_config,
                experiment_id=model_config.get("experiment_id")
            )
            
            # Step 3: Submit training job
            training_response = await self.ml.http_client.post(
                f"{self.ml.ml_service_url}/api/v1/training/jobs",
                params={"tenant_id": "data-platform"},
                json={
                    "name": model_config.get("name", "Model Training"),
                    "config": {
                        "model_type": model_config["model_type"],
                        "framework": model_config["framework"],
                        "algorithm": model_config["algorithm"],
                        "hyperparameters": model_config.get("hyperparameters", {}),
                        "data_config": {
                            "dataset_path": training_data["path"],
                            "format": "parquet"
                        }
                    }
                }
            )
            training_response.raise_for_status()
            
            job_info = training_response.json()
            
            # Step 4: Set up monitoring
            await self._monitor_training_job(job_info["job_id"], workflow_id)
            
            # Step 5: Schedule archival
            if training_schedule:
                await self.storage.create_backup_pipeline(
                    source_pattern=f"ml_training/{model_config.get('experiment_id', '*')}/*",
                    backup_schedule=training_schedule,
                    retention_policy={"days": 30, "versions": 5}
                )
                
            # Store workflow info
            self.workflows[workflow_id] = {
                "type": "ml_training",
                "status": "active",
                "training_job_id": job_info["job_id"],
                "dataset_path": training_data["path"],
                "created_at": datetime.utcnow()
            }
            
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create ML workflow: {e}")
            raise
            
    async def setup_real_time_analytics_pipeline(self,
                                               event_sources: List[str],
                                               analytics_config: Dict[str, Any],
                                               dashboard_config: Optional[Dict[str, Any]] = None) -> str:
        """
        Set up real-time analytics pipeline
        
        1. Subscribe to event sources
        2. Configure streaming analytics
        3. Create dashboard if requested
        4. Set up anomaly detection
        
        Args:
            event_sources: List of event types to consume
            analytics_config: Analytics configuration
            dashboard_config: Optional dashboard configuration
            
        Returns:
            Pipeline ID
        """
        pipeline_id = f"rt_analytics_{datetime.utcnow().timestamp()}"
        
        try:
            # Step 1: Subscribe to events
            subscription_id = await self.events.subscribe_to_data_events(
                event_types=event_sources,
                ingestion_config={
                    "target_zone": "bronze",
                    "format": "parquet",
                    "real_time": True
                }
            )
            
            # Step 2: Create streaming pipeline
            streaming_pipeline = await self.events.create_streaming_pipeline(
                source_events=event_sources,
                transformations=analytics_config.get("transformations", []),
                sink_config={
                    "type": "analytics",
                    "metrics": analytics_config.get("metrics", []),
                    "aggregations": analytics_config.get("aggregations", [])
                }
            )
            
            # Step 3: Create dashboard if requested
            dashboard_id = None
            if dashboard_config:
                dashboard_id = await self.analytics.create_cross_service_dashboard(
                    dashboard_config=dashboard_config,
                    refresh_interval=30
                )
                
            # Step 4: Set up anomaly detection
            if analytics_config.get("anomaly_detection"):
                await self._setup_anomaly_monitoring(
                    data_source=f"streaming/{streaming_pipeline}",
                    metrics=analytics_config["metrics"],
                    sensitivity=analytics_config.get("sensitivity", 0.95)
                )
                
            # Store pipeline info
            self.workflows[pipeline_id] = {
                "type": "real_time_analytics",
                "status": "active",
                "subscription_id": subscription_id,
                "streaming_pipeline": streaming_pipeline,
                "dashboard_id": dashboard_id,
                "created_at": datetime.utcnow()
            }
            
            return pipeline_id
            
        except Exception as e:
            logger.error(f"Failed to setup analytics pipeline: {e}")
            raise
            
    async def create_data_archival_workflow(self,
                                          dataset_patterns: List[str],
                                          archival_policy: Dict[str, Any],
                                          disaster_recovery: bool = False) -> str:
        """
        Create data archival and backup workflow
        
        Args:
            dataset_patterns: Patterns for datasets to archive
            archival_policy: Archival policy configuration
            disaster_recovery: Enable disaster recovery
            
        Returns:
            Workflow ID
        """
        workflow_id = f"archival_workflow_{datetime.utcnow().timestamp()}"
        
        try:
            # Create backup pipelines for each pattern
            backup_pipelines = []
            
            for pattern in dataset_patterns:
                pipeline_id = await self.storage.create_backup_pipeline(
                    source_pattern=pattern,
                    backup_schedule=archival_policy.get("schedule", "0 2 * * *"),
                    retention_policy=archival_policy.get("retention", {"days": 90})
                )
                backup_pipelines.append(pipeline_id)
                
            # Set up disaster recovery if requested
            dr_config = None
            if disaster_recovery:
                # Find critical datasets
                critical_datasets = []
                for pattern in dataset_patterns:
                    if "gold" in pattern or "critical" in pattern:
                        critical_datasets.append(pattern)
                        
                if critical_datasets:
                    dr_config = await self.storage.setup_disaster_recovery(
                        critical_datasets=critical_datasets,
                        recovery_point_objective_hours=archival_policy.get("rpo_hours", 24)
                    )
                    
            # Set up cold storage migration
            if archival_policy.get("enable_cold_migration"):
                await self._schedule_cold_migration(
                    age_threshold_days=archival_policy.get("cold_threshold_days", 180)
                )
                
            # Store workflow info
            self.workflows[workflow_id] = {
                "type": "archival",
                "status": "active",
                "backup_pipelines": backup_pipelines,
                "disaster_recovery": dr_config,
                "created_at": datetime.utcnow()
            }
            
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create archival workflow: {e}")
            raise
            
    async def create_federated_learning_workflow(self,
                                               data_query: str,
                                               participant_column: str,
                                               model_config: Dict[str, Any]) -> str:
        """
        Create federated learning workflow across participants
        
        Args:
            data_query: Query to extract participant data
            participant_column: Column identifying participants
            model_config: Federated learning configuration
            
        Returns:
            Workflow ID
        """
        workflow_id = f"fl_workflow_{datetime.utcnow().timestamp()}"
        
        try:
            # Prepare federated data
            fl_session = await self.ml.prepare_federated_learning_data(
                query=data_query,
                participant_column=participant_column,
                min_samples_per_participant=model_config.get("min_samples", 100)
            )
            
            # Set up event-driven coordination
            await self.events.register_custom_handler(
                event_pattern="federated.participant.*",
                handler=self._handle_fl_participant_event,
                config={"session_id": fl_session["session_id"]}
            )
            
            # Create monitoring dashboard
            dashboard_id = await self.analytics.create_cross_service_dashboard(
                dashboard_config={
                    "name": f"Federated Learning - {fl_session['session_id']}",
                    "data_sources": [{
                        "type": "federated_query",
                        "query": f"SELECT * FROM fl_metrics WHERE session_id = '{fl_session['session_id']}'"
                    }],
                    "widgets": [
                        {"type": "participant_status"},
                        {"type": "model_performance"},
                        {"type": "privacy_metrics"}
                    ]
                }
            )
            
            # Store workflow info
            self.workflows[workflow_id] = {
                "type": "federated_learning",
                "status": "active",
                "fl_session": fl_session,
                "dashboard_id": dashboard_id,
                "created_at": datetime.utcnow()
            }
            
            return workflow_id
            
        except Exception as e:
            logger.error(f"Failed to create FL workflow: {e}")
            raise
            
    async def generate_platform_report(self,
                                     report_type: str = "comprehensive",
                                     time_range: str = "30d",
                                     format: str = "pdf") -> Dict[str, Any]:
        """
        Generate comprehensive platform report
        
        Args:
            report_type: Type of report to generate
            time_range: Time range for report data
            format: Output format
            
        Returns:
            Report information
        """
        try:
            # Gather platform metrics
            platform_metrics = await self.analytics.get_platform_metrics(
                scope="platform",
                time_range=time_range
            )
            
            # Create report query
            report_queries = {
                "comprehensive": """
                    WITH platform_summary AS (
                        SELECT 
                            COUNT(DISTINCT dataset_id) as total_datasets,
                            SUM(size_bytes) / 1024^3 as total_size_gb,
                            COUNT(DISTINCT pipeline_id) as active_pipelines,
                            AVG(quality_score) as avg_quality_score
                        FROM data_catalog
                    ),
                    usage_stats AS (
                        SELECT 
                            COUNT(*) as total_queries,
                            AVG(execution_time_ms) as avg_query_time,
                            COUNT(DISTINCT user_id) as active_users
                        FROM query_logs
                        WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '30 days'
                    )
                    SELECT * FROM platform_summary, usage_stats
                """,
                "quality": """
                    SELECT 
                        dataset_name,
                        quality_score,
                        completeness,
                        consistency,
                        timeliness,
                        last_profiled
                    FROM data_quality_metrics
                    ORDER BY quality_score DESC
                """,
                "usage": """
                    SELECT 
                        service_name,
                        COUNT(*) as request_count,
                        AVG(response_time_ms) as avg_response_time,
                        SUM(data_processed_gb) as data_processed_gb
                    FROM service_usage_logs
                    GROUP BY service_name
                """
            }
            
            query = report_queries.get(report_type, report_queries["comprehensive"])
            
            # Generate report
            report = await self.storage.generate_data_report(
                query=query,
                report_format=format,
                template=self._get_report_template(report_type)
            )
            
            # Archive report
            await self.storage.archive_dataset(
                dataset_path=f"reports/{report['report_id']}",
                archive_policy={"type": "report", "retention_days": 365},
                metadata={
                    "report_type": report_type,
                    "time_range": time_range,
                    "platform_metrics": platform_metrics
                }
            )
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate platform report: {e}")
            raise
            
    # Helper methods
    
    async def _monitor_training_job(self, job_id: str, workflow_id: str):
        """Monitor ML training job"""
        async def monitor():
            while True:
                try:
                    # Check job status
                    response = await self.ml.http_client.get(
                        f"{self.ml.ml_service_url}/api/v1/training/jobs/{job_id}",
                        params={"tenant_id": "data-platform"}
                    )
                    job_status = response.json()
                    
                    # Update workflow status
                    if workflow_id in self.workflows:
                        self.workflows[workflow_id]["training_status"] = job_status["status"]
                        
                    # Check if completed
                    if job_status["status"] in ["completed", "failed", "cancelled"]:
                        # Publish event
                        await self.events.publish_data_event(
                            event_type="ml.training.completed",
                            data={
                                "job_id": job_id,
                                "status": job_status["status"],
                                "workflow_id": workflow_id
                            }
                        )
                        break
                        
                    await asyncio.sleep(60)  # Check every minute
                    
                except Exception as e:
                    logger.error(f"Error monitoring training job: {e}")
                    await asyncio.sleep(60)
                    
        asyncio.create_task(monitor())
        
    async def _setup_anomaly_monitoring(self,
                                      data_source: str,
                                      metrics: List[str],
                                      sensitivity: float):
        """Set up anomaly monitoring"""
        # Register anomaly detection handler
        async def anomaly_handler(anomalies: List[Dict[str, Any]]):
            for anomaly in anomalies:
                # Publish anomaly event
                await self.events.publish_data_event(
                    event_type="analytics.anomaly.detected",
                    data=anomaly
                )
                
                # Log to storage for analysis
                await self.storage.archive_dataset(
                    dataset_path=f"anomalies/{data_source}/{datetime.utcnow().strftime('%Y%m%d')}",
                    archive_policy={"type": "anomaly", "retention_days": 90},
                    metadata={"anomaly": anomaly}
                )
                
        # Schedule periodic anomaly detection
        async def detect():
            while True:
                try:
                    anomalies = await self.analytics.detect_anomalies(
                        data_source=data_source,
                        metrics=metrics,
                        sensitivity=sensitivity
                    )
                    
                    if anomalies:
                        await anomaly_handler(anomalies)
                        
                    await asyncio.sleep(300)  # Every 5 minutes
                    
                except Exception as e:
                    logger.error(f"Anomaly detection error: {e}")
                    await asyncio.sleep(300)
                    
        asyncio.create_task(detect())
        
    async def _schedule_cold_migration(self, age_threshold_days: int):
        """Schedule cold storage migration"""
        async def migrate():
            while True:
                try:
                    # Run migration
                    result = await self.storage.migrate_to_cold_storage(
                        age_threshold_days=age_threshold_days,
                        dry_run=False
                    )
                    
                    # Log results
                    logger.info(f"Cold migration completed: {result}")
                    
                    # Wait for next run (daily)
                    await asyncio.sleep(86400)
                    
                except Exception as e:
                    logger.error(f"Cold migration error: {e}")
                    await asyncio.sleep(86400)
                    
        asyncio.create_task(migrate())
        
    async def _handle_fl_participant_event(self, message: Any, config: Dict[str, Any]):
        """Handle federated learning participant events"""
        # Implementation would coordinate FL participant updates
        pass
        
    def _get_report_template(self, report_type: str) -> Optional[str]:
        """Get report template by type"""
        # Implementation would return report templates
        return None
        
    async def get_workflow_status(self, workflow_id: str) -> Dict[str, Any]:
        """Get workflow status"""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow not found: {workflow_id}")
            
        return self.workflows[workflow_id]
        
    async def cancel_workflow(self, workflow_id: str):
        """Cancel active workflow"""
        if workflow_id not in self.workflows:
            raise ValueError(f"Workflow not found: {workflow_id}")
            
        workflow = self.workflows[workflow_id]
        workflow["status"] = "cancelled"
        
        # Cancel related operations based on type
        if workflow["type"] == "ml_training" and "training_job_id" in workflow:
            # Cancel training job
            await self.ml.http_client.post(
                f"{self.ml.ml_service_url}/api/v1/training/jobs/{workflow['training_job_id']}/cancel",
                params={"tenant_id": "data-platform"}
            )
            
        # Remove from active workflows
        del self.workflows[workflow_id] 