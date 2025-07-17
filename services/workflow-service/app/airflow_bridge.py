"""
Airflow Bridge Module

This module provides integration between the PlatformQ workflow service
and Apache Airflow, allowing events to trigger DAG runs and providing
a unified interface for workflow management.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import pulsar
from pulsar.schema import AvroSchema
from platformq_shared.config import ConfigLoader

logger = logging.getLogger(__name__)


class AirflowBridge:
    """Bridge between PlatformQ events and Airflow DAGs"""
    
    def __init__(self, airflow_url: str, pulsar_client: pulsar.Client = None):
        """
        Initialize Airflow bridge
        
        :param airflow_url: Base URL for Airflow API
        :param pulsar_client: Pulsar client for emitting events
        """
        self.airflow_url = airflow_url.rstrip('/')
        self.session = requests.Session()
        config_loader = ConfigLoader()
        settings = config_loader.load_settings()
        self.airflow_user = settings.get("AIRFLOW_USER", "airflow")
        self.airflow_password = settings.get("AIRFLOW_PASSWORD")
        self.session.auth = (self.airflow_user, self.airflow_password)
        
        # Initialize Pulsar producer for workflow events
        self.pulsar_client = pulsar_client
        if self.pulsar_client:
            self.workflow_producer = self.pulsar_client.create_producer(
                "persistent://public/default/workflow-execution-started"
            )
            self.workflow_completed_producer = self.pulsar_client.create_producer(
                "persistent://public/default/workflow-execution-completed"
            )
        else:
            self.workflow_producer = None
            self.workflow_completed_producer = None
        
        # Event to DAG mapping
        self.event_dag_mapping = {
            'DigitalAssetCreated': 'digital_asset_processing',
            'ProjectCreationRequested': 'create_collaborative_project',
            'ProposalApproved': 'verifiable_credential_issuance',
            'SimulationStartRequested': 'simulation_orchestration',
            'DocumentUpdated': 'document_processing_workflow',
            # SeaTunnel pipeline orchestration
            'SeaTunnelPipelineRequested': 'seatunnel_pipeline_orchestration',
            'DataSourceDiscovered': 'seatunnel_pipeline_orchestration',
            'BatchDataReady': 'seatunnel_pipeline_orchestration'
        }
        
        # Track active runs for monitoring
        self.active_runs = {}
    
    def trigger_dag(self, dag_id: str, conf: Dict[str, Any], 
                   run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Trigger an Airflow DAG run
        
        :param dag_id: ID of the DAG to trigger
        :param conf: Configuration to pass to the DAG
        :param run_id: Optional custom run ID
        :return: Response from Airflow API
        """
        if not run_id:
            run_id = f"{dag_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        
        payload = {
            "dag_run_id": run_id,
            "conf": conf
        }
        
        try:
            # Get DAG info for expected duration
            dag_info = self._get_dag_info(dag_id)
            expected_duration = self._estimate_duration(dag_id, dag_info)
            
            # Record start time
            start_time = datetime.utcnow()
            
            response = requests.post(
                url,
                json=payload,
                auth=self.session.auth,
                headers={'Content-Type': 'application/json'}
            )
            
            response.raise_for_status()
            result = response.json()
            
            logger.info(f"Successfully triggered DAG {dag_id} with run_id {run_id}")
            
            # Emit workflow started event for neuromorphic monitoring
            if self.workflow_producer:
                workflow_started_event = {
                    "execution_id": run_id,
                    "workflow_id": dag_id,
                    "workflow_name": dag_info.get('dag_id', dag_id),
                    "tenant_id": conf.get('tenant_id', 'default'),
                    "trigger_type": "event" if conf.get('event_type') else "manual",
                    "triggered_by": conf.get('triggered_by', 'workflow-service'),
                    "configuration": conf,
                    "expected_duration_ms": expected_duration,
                    "started_at": int(start_time.timestamp() * 1000),
                    "metadata": {
                        "parallelism_degree": str(dag_info.get('max_active_tasks', 16)),
                        "schedule_interval": dag_info.get('schedule_interval'),
                        "tags": dag_info.get('tags', [])
                    }
                }
                
                self.workflow_producer.send(json.dumps(workflow_started_event).encode('utf-8'))
                logger.info(f"Emitted workflow started event for {dag_id}")
                
                # Track active run
                self.active_runs[run_id] = {
                    'dag_id': dag_id,
                    'started_at': start_time,
                    'expected_duration_ms': expected_duration,
                    'conf': conf
                }
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to trigger DAG {dag_id}: {str(e)}")
            raise

def federate_dag(dag_id: str, tenants: List[str]):
    for tenant in tenants:
        conf = {'tenant_id': tenant}
        trigger_dag(dag_id, conf)
    
    def get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """
        Get the status of a DAG run
        
        :param dag_id: ID of the DAG
        :param run_id: ID of the DAG run
        :return: DAG run status information
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        
        try:
            response = requests.get(url, auth=self.session.auth)
            response.raise_for_status()
            status_info = response.json()
            
            # Check if run completed and emit event
            if run_id in self.active_runs and status_info.get('state') in ['success', 'failed', 'skipped']:
                self._emit_workflow_completed_event(dag_id, run_id, status_info)
                
            return status_info
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get DAG run status: {str(e)}")
            raise
    
    def _emit_workflow_completed_event(self, dag_id: str, run_id: str, status_info: Dict[str, Any]):
        """Emit workflow completed event for neuromorphic monitoring"""
        if not self.workflow_completed_producer or run_id not in self.active_runs:
            return
            
        active_run = self.active_runs[run_id]
        start_time = active_run['started_at']
        end_time = datetime.fromisoformat(status_info['end_date'].replace('Z', '+00:00'))
        duration_ms = int((end_time - start_time).total_seconds() * 1000)
        
        # Get task instance statistics
        task_stats = self._get_task_instance_stats(dag_id, run_id)
        
        workflow_completed_event = {
            "execution_id": run_id,
            "workflow_id": dag_id,
            "workflow_name": dag_id,
            "tenant_id": active_run['conf'].get('tenant_id', 'default'),
            "status": status_info['state'],
            "started_at": int(start_time.timestamp() * 1000),
            "completed_at": int(end_time.timestamp() * 1000),
            "duration_ms": duration_ms,
            "task_metrics": {
                "total_tasks": task_stats.get('total', 0),
                "succeeded_tasks": task_stats.get('success', 0),
                "failed_tasks": task_stats.get('failed', 0),
                "skipped_tasks": task_stats.get('skipped', 0),
                "retry_count": task_stats.get('retries', 0)
            },
            "resource_usage": self._estimate_resource_usage(dag_id, duration_ms),
            "error_message": status_info.get('note') if status_info['state'] == 'failed' else None,
            "outputs": status_info.get('conf', {}).get('outputs'),
            "metadata": {
                "execution_date": status_info.get('execution_date'),
                "external_trigger": status_info.get('external_trigger', False),
                "critical_path_length": str(task_stats.get('critical_path_length', 0)),
                "resource_wait_time_ms": str(task_stats.get('wait_time_ms', 0))
            }
        }
        
        self.workflow_completed_producer.send(json.dumps(workflow_completed_event).encode('utf-8'))
        logger.info(f"Emitted workflow completed event for {dag_id}")
        
        # Clean up tracking
        del self.active_runs[run_id]
    
    def _get_dag_info(self, dag_id: str) -> Dict[str, Any]:
        """Get DAG information from Airflow"""
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}"
        
        try:
            response = requests.get(url, auth=self.session.auth)
            response.raise_for_status()
            return response.json()
        except:
            logger.warning(f"Could not get DAG info for {dag_id}")
            return {}
    
    def _get_task_instance_stats(self, dag_id: str, run_id: str) -> Dict[str, int]:
        """Get task instance statistics for a DAG run"""
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        
        try:
            response = requests.get(url, auth=self.session.auth)
            response.raise_for_status()
            task_instances = response.json().get('task_instances', [])
            
            stats = {
                'total': len(task_instances),
                'success': sum(1 for t in task_instances if t['state'] == 'success'),
                'failed': sum(1 for t in task_instances if t['state'] == 'failed'),
                'skipped': sum(1 for t in task_instances if t['state'] == 'skipped'),
                'retries': sum(t.get('try_number', 1) - 1 for t in task_instances),
                'critical_path_length': self._calculate_critical_path(task_instances),
                'wait_time_ms': self._calculate_wait_time(task_instances)
            }
            
            return stats
        except:
            logger.warning(f"Could not get task stats for {dag_id}/{run_id}")
            return {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'retries': 0}
    
    def _estimate_duration(self, dag_id: str, dag_info: Dict[str, Any]) -> int:
        """Estimate expected duration based on historical data"""
        # In a real implementation, this would query historical execution times
        # For now, return a default based on DAG type
        default_durations = {
            'digital_asset_processing': 30000,  # 30 seconds
            'create_collaborative_project': 5000,  # 5 seconds
            'verifiable_credential_issuance': 10000,  # 10 seconds
            'simulation_orchestration': 300000,  # 5 minutes
            'document_processing_workflow': 15000,  # 15 seconds
            'seatunnel_pipeline_orchestration': 60000  # 1 minute
        }
        
        return default_durations.get(dag_id, 60000)  # Default 1 minute
    
    def _estimate_resource_usage(self, dag_id: str, duration_ms: int) -> Dict[str, Any]:
        """Estimate resource usage for the workflow"""
        # In a real implementation, this would get actual metrics
        # For now, return estimates based on DAG type
        
        base_cpu_per_second = 0.5  # CPU cores
        base_memory_mb = 512
        
        resource_multipliers = {
            'simulation_orchestration': 4.0,
            'digital_asset_processing': 2.0,
            'seatunnel_pipeline_orchestration': 3.0
        }
        
        multiplier = resource_multipliers.get(dag_id, 1.0)
        duration_seconds = duration_ms / 1000.0
        
        return {
            "cpu_seconds": duration_seconds * base_cpu_per_second * multiplier,
            "memory_mb_seconds": duration_seconds * base_memory_mb * multiplier,
            "network_bytes": int(duration_seconds * 1024 * 100)  # 100KB/s estimate
        }
    
    def _calculate_critical_path(self, task_instances: List[Dict]) -> int:
        """Calculate critical path length from task instances"""
        # Simplified: count max dependency depth
        # In reality, would need to analyze task dependencies
        return len(task_instances)
    
    def _calculate_wait_time(self, task_instances: List[Dict]) -> int:
        """Calculate total resource wait time"""
        total_wait_ms = 0
        
        for task in task_instances:
            if task.get('queued_dttm') and task.get('start_date'):
                try:
                    queued = datetime.fromisoformat(task['queued_dttm'].replace('Z', '+00:00'))
                    started = datetime.fromisoformat(task['start_date'].replace('Z', '+00:00'))
                    wait_ms = int((started - queued).total_seconds() * 1000)
                    total_wait_ms += max(0, wait_ms)
                except:
                    pass
                    
        return total_wait_ms
    
    def list_dags(self) -> List[Dict[str, Any]]:
        """
        List all DAGs from Airflow
        
        :return: List of DAG information
        """
        url = f"{self.airflow_url}/api/v1/dags"
        
        try:
            response = requests.get(url, auth=self.session.auth)
            response.raise_for_status()
            return response.json().get('dags', [])
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to list DAGs: {str(e)}")
            raise
    
    def pause_dag(self, dag_id: str, is_paused: bool) -> Dict[str, Any]:
        """
        Pause or unpause a DAG
        
        :param dag_id: ID of the DAG
        :param is_paused: Whether to pause (True) or unpause (False)
        :return: Updated DAG information
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}"
        
        payload = {
            "is_paused": is_paused
        }
        
        try:
            response = requests.patch(
                url,
                json=payload,
                auth=self.session.auth,
                headers={'Content-Type': 'application/json'}
            )
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to update DAG {dag_id}: {str(e)}")
            raise
    
    def handle_event(self, event_type: str, event_data: Dict[str, Any], 
                    tenant_id: str) -> Optional[Dict[str, Any]]:
        """
        Handle a PlatformQ event and trigger appropriate DAG if mapped
        
        :param event_type: Type of the event
        :param event_data: Event payload
        :param tenant_id: Tenant ID from the event
        :return: DAG run information if triggered, None otherwise
        """
        dag_id = self.event_dag_mapping.get(event_type)
        
        if not dag_id:
            logger.debug(f"No DAG mapping found for event type: {event_type}")
            return None
        
        # Prepare DAG configuration
        dag_conf = {
            'event_type': event_type,
            'event_data': event_data,
            'tenant_id': tenant_id,
            'triggered_by': 'workflow-service',
            'triggered_at': datetime.utcnow().isoformat()
        }
        
        # Some events might need special handling
        if event_type == 'ProjectCreationRequested':
            # Extract specific fields for project creation
            dag_conf.update({
                'project_name': event_data.get('project_name'),
                'creator_id': event_data.get('creator_id'),
                'description': event_data.get('description', ''),
                'team_members': event_data.get('team_members', [])
            })
        
        try:
            result = self.trigger_dag(dag_id, dag_conf)
            logger.info(f"Triggered DAG {dag_id} for event {event_type}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to trigger DAG for event {event_type}: {str(e)}")
            return None
    
    def register_event_mapping(self, event_type: str, dag_id: str):
        """
        Register a new event to DAG mapping
        
        :param event_type: Event type to map
        :param dag_id: DAG ID to trigger for this event
        """
        self.event_dag_mapping[event_type] = dag_id
        logger.info(f"Registered mapping: {event_type} -> {dag_id}")
    
    def get_event_mappings(self) -> Dict[str, str]:
        """
        Get all registered event to DAG mappings
        
        :return: Dictionary of event type to DAG ID mappings
        """
        return self.event_dag_mapping.copy()


class EventToDAGProcessor:
    """Processes events and triggers corresponding Airflow DAGs"""
    
    def __init__(self, airflow_bridge: AirflowBridge, pulsar_client: pulsar.Client):
        """
        Initialize the event processor
        
        :param airflow_bridge: AirflowBridge instance
        :param pulsar_client: Pulsar client instance
        """
        self.airflow_bridge = airflow_bridge
        self.pulsar_client = pulsar_client
        self.consumers = {}
    
    def subscribe_to_events(self, topic_patterns: List[str], subscription_name: str):
        """
        Subscribe to event topics for DAG triggering
        
        :param topic_patterns: List of topic patterns to subscribe to
        :param subscription_name: Subscription name
        """
        for pattern in topic_patterns:
            try:
                consumer = self.pulsar_client.subscribe(
                    topic=pattern,
                    subscription_name=f"{subscription_name}-{pattern.split('/')[-1]}",
                    consumer_type=pulsar.ConsumerType.Shared
                )
                self.consumers[pattern] = consumer
                logger.info(f"Subscribed to topic pattern: {pattern}")
                
            except Exception as e:
                logger.error(f"Failed to subscribe to {pattern}: {str(e)}")
    
    def process_events(self):
        """Process events and trigger DAGs"""
        import re
        
        while True:
            for pattern, consumer in self.consumers.items():
                try:
                    # Try to receive a message (non-blocking)
                    msg = consumer.receive(timeout_millis=100)
                    
                    if msg:
                        # Extract event type from topic or message
                        topic = msg.topic_name()
                        
                        # Extract tenant ID from topic
                        tenant_match = re.search(r'platformq/([a-f0-9-]+)/', topic)
                        tenant_id = tenant_match.group(1) if tenant_match else 'unknown'
                        
                        # Parse event data
                        event_data = msg.value()
                        
                        # Determine event type (could be from schema or topic name)
                        event_type = self._get_event_type(topic, event_data)
                        
                        # Handle the event
                        result = self.airflow_bridge.handle_event(
                            event_type=event_type,
                            event_data=event_data if isinstance(event_data, dict) else event_data.__dict__,
                            tenant_id=tenant_id
                        )
                        
                        # Acknowledge the message
                        consumer.acknowledge(msg)
                        
                        if result:
                            logger.info(f"Successfully processed event {event_type} -> DAG run: {result.get('dag_run_id')}")
                        
                except Exception as e:
                    if "Timeout" not in str(e):
                        logger.error(f"Error processing events from {pattern}: {str(e)}")
    
    def _get_event_type(self, topic: str, event_data: Any) -> str:
        """
        Determine event type from topic or event data
        
        :param topic: Topic name
        :param event_data: Event data
        :return: Event type string
        """
        # Try to get from event data first
        if hasattr(event_data, '__class__'):
            return event_data.__class__.__name__
        
        # Extract from topic name
        if 'digital-asset-created' in topic:
            return 'DigitalAssetCreated'
        elif 'project-creation' in topic:
            return 'ProjectCreationRequested'
        elif 'proposal-approved' in topic:
            return 'ProposalApproved'
        elif 'document-updated' in topic:
            return 'DocumentUpdated'
        
        # Default
        return 'UnknownEvent'
    
    def stop(self):
        """Stop processing and close consumers"""
        for consumer in self.consumers.values():
            try:
                consumer.close()
            except:
                pass
        self.consumers.clear() 