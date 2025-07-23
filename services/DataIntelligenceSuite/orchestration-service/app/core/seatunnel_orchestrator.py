"""
Apache SeaTunnel integration for data movement orchestration
"""

import asyncio
import json
import uuid
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import httpx
import yaml
from jinja2 import Template

from platformq_shared.logging import get_logger
from ..core.config import settings

logger = get_logger(__name__)


class SeaTunnelJobType(str, Enum):
    """SeaTunnel job types"""
    BATCH = "batch"
    STREAMING = "streaming"
    CDC = "cdc"  # Change Data Capture


class SeaTunnelJobStatus(str, Enum):
    """SeaTunnel job status"""
    SUBMITTED = "submitted"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ConnectorType(str, Enum):
    """SeaTunnel connector types"""
    # Sources
    JDBC = "jdbc"
    KAFKA = "kafka"
    PULSAR = "pulsar"
    FILE = "file"
    S3 = "s3"
    ELASTICSEARCH = "elasticsearch"
    MONGODB = "mongodb"
    REDIS = "redis"
    CASSANDRA = "cassandra"
    
    # Sinks
    CONSOLE = "console"
    IGNITE = "ignite"
    CLICKHOUSE = "clickhouse"
    DORIS = "doris"


class SeaTunnelOrchestrator:
    """Orchestrates data movement using Apache SeaTunnel"""
    
    def __init__(self):
        self.api_url = settings.seatunnel_api_url
        self.templates_path = Path(settings.seatunnel_orchestration_templates)
        self.client = httpx.AsyncClient(timeout=30.0)
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.templates: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize the SeaTunnel orchestrator"""
        logger.info("Initializing SeaTunnel orchestrator")
        
        # Load pipeline templates
        await self._load_templates()
        
        # Verify SeaTunnel connectivity
        await self._verify_connectivity()
        
        # Start job monitoring
        asyncio.create_task(self._monitor_jobs())
        
        logger.info("SeaTunnel orchestrator initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        await self.client.aclose()
        
    async def _load_templates(self):
        """Load SeaTunnel pipeline templates"""
        if not self.templates_path.exists():
            logger.warning(f"Templates directory {self.templates_path} not found")
            return
            
        for template_file in self.templates_path.glob("*.yaml"):
            try:
                with open(template_file, 'r') as f:
                    template = yaml.safe_load(f)
                    template_name = template_file.stem
                    self.templates[template_name] = template
                    logger.info(f"Loaded SeaTunnel template: {template_name}")
            except Exception as e:
                logger.error(f"Failed to load template {template_file}: {e}")
                
    async def _verify_connectivity(self):
        """Verify SeaTunnel API connectivity"""
        try:
            response = await self.client.get(f"{self.api_url}/health")
            if response.status_code == 200:
                logger.info("SeaTunnel API connectivity verified")
            else:
                logger.warning(f"SeaTunnel API returned status {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to connect to SeaTunnel API: {e}")
            
    async def create_pipeline(self,
                            name: str,
                            source: Dict[str, Any],
                            sink: Dict[str, Any],
                            transformations: Optional[List[Dict[str, Any]]] = None,
                            job_type: SeaTunnelJobType = SeaTunnelJobType.BATCH,
                            template: Optional[str] = None,
                            orchestration: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a SeaTunnel data pipeline"""
        logger.info(f"Creating SeaTunnel pipeline: {name}")
        
        pipeline_id = str(uuid.uuid4())
        
        # Use template if provided
        if template and template in self.templates:
            config = self.templates[template].copy()
            # Override with provided values
            if source:
                config['source'] = source
            if sink:
                config['sink'] = sink
            if transformations:
                config['transform'] = transformations
        else:
            # Build configuration from scratch
            config = await self._build_pipeline_config(
                name, source, sink, transformations, job_type
            )
            
        # Apply orchestration settings
        if orchestration:
            config['orchestration'] = orchestration
            
        # Create pipeline object
        pipeline = {
            "id": pipeline_id,
            "name": name,
            "job_type": job_type,
            "config": config,
            "created_at": datetime.utcnow().isoformat(),
            "status": SeaTunnelJobStatus.SUBMITTED,
            "orchestration": orchestration or {}
        }
        
        # Submit to SeaTunnel
        job_id = await self._submit_job(config, job_type)
        pipeline["job_id"] = job_id
        
        # Store pipeline
        self.jobs[pipeline_id] = pipeline
        
        # Schedule if requested
        if orchestration and orchestration.get('schedule'):
            await self._schedule_pipeline(pipeline_id, orchestration['schedule'])
            
        return pipeline
        
    async def _build_pipeline_config(self,
                                   name: str,
                                   source: Dict[str, Any],
                                   sink: Dict[str, Any],
                                   transformations: Optional[List[Dict[str, Any]]],
                                   job_type: SeaTunnelJobType) -> Dict[str, Any]:
        """Build SeaTunnel pipeline configuration"""
        config = {
            "env": {
                "execution.parallelism": settings.seatunnel_parallelism,
                "job.mode": job_type.value,
                "checkpoint.interval": 10000 if job_type == SeaTunnelJobType.STREAMING else None
            },
            "source": [await self._build_source_config(source)],
            "sink": [await self._build_sink_config(sink)]
        }
        
        # Add transformations if provided
        if transformations:
            config["transform"] = [
                await self._build_transform_config(t) for t in transformations
            ]
            
        return config
        
    async def _build_source_config(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """Build source connector configuration"""
        source_type = ConnectorType(source['type'])
        config = source.get('config', {})
        
        if source_type == ConnectorType.JDBC:
            return {
                "plugin_name": "Jdbc",
                "url": config['url'],
                "driver": config.get('driver', 'org.postgresql.Driver'),
                "user": config.get('user'),
                "password": config.get('password'),
                "query": config.get('query', f"select * from {config.get('table')}")
            }
        elif source_type == ConnectorType.PULSAR:
            return {
                "plugin_name": "Pulsar",
                "servers": config.get('servers', settings.pulsar_url),
                "topic": config['topic'],
                "subscription": config.get('subscription', f"{source.get('name', 'default')}_sub"),
                "subscription.type": config.get('subscription_type', 'Shared')
            }
        elif source_type == ConnectorType.FILE:
            return {
                "plugin_name": "File",
                "path": config['path'],
                "format": config.get('format', 'json'),
                "schema": config.get('schema', {})
            }
        elif source_type == ConnectorType.ELASTICSEARCH:
            return {
                "plugin_name": "Elasticsearch",
                "hosts": config['hosts'],
                "index": config['index'],
                "query": config.get('query', {"match_all": {}})
            }
        elif source_type == ConnectorType.CASSANDRA:
            return {
                "plugin_name": "Cassandra",
                "host": config['host'],
                "keyspace": config['keyspace'],
                "table": config['table'],
                "cql": config.get('cql')
            }
        else:
            # Generic configuration
            return {
                "plugin_name": source_type.value.capitalize(),
                **config
            }
            
    async def _build_sink_config(self, sink: Dict[str, Any]) -> Dict[str, Any]:
        """Build sink connector configuration"""
        sink_type = ConnectorType(sink['type'])
        config = sink.get('config', {})
        
        if sink_type == ConnectorType.JDBC:
            return {
                "plugin_name": "Jdbc",
                "url": config['url'],
                "driver": config.get('driver', 'org.postgresql.Driver'),
                "user": config.get('user'),
                "password": config.get('password'),
                "table": config['table'],
                "save_mode": config.get('save_mode', 'append')
            }
        elif sink_type == ConnectorType.ELASTICSEARCH:
            return {
                "plugin_name": "Elasticsearch",
                "hosts": config['hosts'],
                "index": config['index'],
                "index_type": config.get('index_type', '_doc'),
                "primary_keys": config.get('primary_keys', [])
            }
        elif sink_type == ConnectorType.IGNITE:
            return {
                "plugin_name": "Ignite",
                "host": config.get('host', settings.ignite_host),
                "port": config.get('port', settings.ignite_port),
                "cache_name": config['cache_name'],
                "save_mode": config.get('save_mode', 'append')
            }
        elif sink_type == ConnectorType.PULSAR:
            return {
                "plugin_name": "Pulsar",
                "servers": config.get('servers', settings.pulsar_url),
                "topic": config['topic'],
                "format": config.get('format', 'json')
            }
        elif sink_type == ConnectorType.CONSOLE:
            return {
                "plugin_name": "Console",
                "limit": config.get('limit', 100)
            }
        else:
            # Generic configuration
            return {
                "plugin_name": sink_type.value.capitalize(),
                **config
            }
            
    async def _build_transform_config(self, transform: Dict[str, Any]) -> Dict[str, Any]:
        """Build transformation configuration"""
        transform_type = transform['type']
        config = transform.get('config', {})
        
        if transform_type == 'sql':
            return {
                "plugin_name": "Sql",
                "sql": config['sql']
            }
        elif transform_type == 'quality_check':
            return {
                "plugin_name": "Assert",
                "rules": config.get('rules', [
                    {"field": "id", "type": "not_null"},
                    {"field": "timestamp", "type": "not_null"}
                ])
            }
        elif transform_type == 'encrypt_pii':
            return {
                "plugin_name": "Replace",
                "fields": config.get('fields', ['email', 'phone', 'ssn']),
                "pattern": ".*",
                "replacement": "***ENCRYPTED***"
            }
        elif transform_type == 'filter':
            return {
                "plugin_name": "Filter",
                "fields": config.get('fields', []),
                "condition": config.get('condition', {})
            }
        elif transform_type == 'aggregate':
            return {
                "plugin_name": "Aggregate",
                "group_by": config.get('group_by', []),
                "aggs": config.get('aggregations', {})
            }
        else:
            # Custom transformation
            return {
                "plugin_name": transform_type.capitalize(),
                **config
            }
            
    async def _submit_job(self, config: Dict[str, Any], job_type: SeaTunnelJobType) -> str:
        """Submit job to SeaTunnel"""
        try:
            # Convert config to SeaTunnel format
            job_config = yaml.dump(config)
            
            # Submit job
            response = await self.client.post(
                f"{self.api_url}/jobs/submit",
                json={
                    "config": job_config,
                    "job_type": job_type.value,
                    "engine": "spark" if job_type == SeaTunnelJobType.BATCH else "flink"
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                job_id = result.get('job_id')
                logger.info(f"Submitted SeaTunnel job: {job_id}")
                return job_id
            else:
                raise RuntimeError(f"Failed to submit job: {response.text}")
                
        except Exception as e:
            logger.error(f"Job submission failed: {e}")
            raise
            
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get SeaTunnel job status"""
        try:
            response = await self.client.get(f"{self.api_url}/jobs/{job_id}/status")
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to get job status: {response.text}")
                return {"status": "unknown", "error": response.text}
                
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            return {"status": "error", "error": str(e)}
            
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a running SeaTunnel job"""
        try:
            response = await self.client.post(f"{self.api_url}/jobs/{job_id}/cancel")
            
            if response.status_code == 200:
                logger.info(f"Cancelled SeaTunnel job: {job_id}")
                return True
            else:
                logger.warning(f"Failed to cancel job: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to cancel job: {e}")
            return False
            
    async def orchestrate_data_movement(self,
                                      name: str,
                                      movements: List[Dict[str, Any]],
                                      dependencies: Optional[Dict[str, List[str]]] = None,
                                      schedule: Optional[str] = None) -> Dict[str, Any]:
        """Orchestrate complex data movements across systems"""
        logger.info(f"Orchestrating data movement: {name}")
        
        orchestration_id = str(uuid.uuid4())
        
        # Create orchestration plan
        plan = {
            "id": orchestration_id,
            "name": name,
            "movements": movements,
            "dependencies": dependencies or {},
            "schedule": schedule,
            "created_at": datetime.utcnow().isoformat(),
            "status": "planning",
            "pipelines": []
        }
        
        # Create pipelines for each movement
        for movement in movements:
            pipeline = await self.create_pipeline(
                name=f"{name}_{movement['name']}",
                source=movement['source'],
                sink=movement['sink'],
                transformations=movement.get('transformations'),
                job_type=movement.get('job_type', SeaTunnelJobType.BATCH),
                orchestration={
                    "parent": orchestration_id,
                    "retries": movement.get('retries', 3),
                    "alerts": movement.get('alerts', [])
                }
            )
            plan['pipelines'].append(pipeline)
            
        # Execute based on dependencies
        if dependencies:
            await self._execute_with_dependencies(plan)
        else:
            # Execute all in parallel
            await self._execute_parallel(plan)
            
        plan['status'] = "executing"
        
        return plan
        
    async def _execute_with_dependencies(self, plan: Dict[str, Any]):
        """Execute pipelines respecting dependencies"""
        completed = set()
        pipelines_by_name = {p['name']: p for p in plan['pipelines']}
        
        while len(completed) < len(plan['pipelines']):
            # Find pipelines ready to execute
            ready = []
            for pipeline in plan['pipelines']:
                if pipeline['id'] in completed:
                    continue
                    
                deps = plan['dependencies'].get(pipeline['name'], [])
                if all(dep in completed for dep in deps):
                    ready.append(pipeline)
                    
            # Execute ready pipelines in parallel
            if ready:
                await asyncio.gather(*[
                    self._execute_pipeline(p) for p in ready
                ])
                completed.update(p['id'] for p in ready)
            else:
                # No pipelines ready - might be circular dependency
                logger.error("No pipelines ready to execute - check dependencies")
                break
                
    async def _execute_parallel(self, plan: Dict[str, Any]):
        """Execute all pipelines in parallel"""
        await asyncio.gather(*[
            self._execute_pipeline(p) for p in plan['pipelines']
        ])
        
    async def _execute_pipeline(self, pipeline: Dict[str, Any]):
        """Execute a single pipeline"""
        # Pipeline execution is handled by SeaTunnel
        # This method tracks execution status
        logger.info(f"Executing pipeline: {pipeline['name']}")
        
    async def _schedule_pipeline(self, pipeline_id: str, schedule: str):
        """Schedule pipeline execution"""
        logger.info(f"Scheduling pipeline {pipeline_id} with schedule: {schedule}")
        # Integration with scheduler (e.g., Airflow) would go here
        
    async def get_templates(self) -> List[Dict[str, Any]]:
        """Get available pipeline templates"""
        return [
            {
                "name": name,
                "description": template.get('description', ''),
                "source_type": template.get('source', {}).get('type'),
                "sink_type": template.get('sink', {}).get('type'),
                "transformations": len(template.get('transform', []))
            }
            for name, template in self.templates.items()
        ]
        
    async def _monitor_jobs(self):
        """Monitor running SeaTunnel jobs"""
        while True:
            try:
                for job_id, job in list(self.jobs.items()):
                    if job['status'] in [SeaTunnelJobStatus.RUNNING, SeaTunnelJobStatus.SUBMITTED]:
                        # Check job status
                        status = await self.get_job_status(job.get('job_id'))
                        
                        # Update local status
                        if status.get('status'):
                            job['status'] = SeaTunnelJobStatus(status['status'])
                            job['updated_at'] = datetime.utcnow().isoformat()
                            
                            # Handle completion
                            if job['status'] in [SeaTunnelJobStatus.FINISHED, SeaTunnelJobStatus.FAILED]:
                                await self._handle_job_completion(job)
                                
            except Exception as e:
                logger.error(f"Job monitoring error: {e}")
                
            await asyncio.sleep(30)  # Check every 30 seconds
            
    async def _handle_job_completion(self, job: Dict[str, Any]):
        """Handle job completion"""
        logger.info(f"Job {job['id']} completed with status: {job['status']}")
        
        # Send alerts if configured
        orchestration = job.get('orchestration', {})
        if orchestration.get('alerts') and job['status'] == SeaTunnelJobStatus.FAILED:
            # Send failure alerts
            logger.warning(f"Sending failure alerts for job {job['id']}")
            
        # Handle retries if configured
        if job['status'] == SeaTunnelJobStatus.FAILED and orchestration.get('retries', 0) > 0:
            # Implement retry logic
            logger.info(f"Retrying job {job['id']}")
            # Decrement retry count and resubmit 