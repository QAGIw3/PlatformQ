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
    
    def __init__(self, airflow_url: str):
        """
        Initialize Airflow bridge
        
        :param airflow_url: Base URL for Airflow API
        :param airflow_username: Username for Airflow authentication
        :param airflow_password: Password for Airflow authentication
        """
        self.airflow_url = airflow_url.rstrip('/')
        self.session = requests.Session()
        config_loader = ConfigLoader()
        settings = config_loader.load_settings()
        self.airflow_user = settings.get("AIRFLOW_USER", "airflow")
        self.airflow_password = settings.get("AIRFLOW_PASSWORD")
        self.session.auth = (self.airflow_user, self.airflow_password)
        
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
            response = requests.post(
                url,
                json=payload,
                auth=self.session.auth,
                headers={'Content-Type': 'application/json'}
            )
            
            response.raise_for_status()
            
            logger.info(f"Successfully triggered DAG {dag_id} with run_id {run_id}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to trigger DAG {dag_id}: {str(e)}")
            raise
    
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
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get DAG run status: {str(e)}")
            raise
    
    def list_dags(self) -> List[Dict[str, Any]]:
        """
        List all available DAGs
        
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
    
    def pause_dag(self, dag_id: str, is_paused: bool = True) -> Dict[str, Any]:
        """
        Pause or unpause a DAG
        
        :param dag_id: ID of the DAG
        :param is_paused: Whether to pause (True) or unpause (False)
        :return: Updated DAG information
        """
        url = f"{self.airflow_url}/api/v1/dags/{dag_id}"
        
        payload = {"is_paused": is_paused}
        
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
            logger.error(f"Failed to update DAG state: {str(e)}")
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