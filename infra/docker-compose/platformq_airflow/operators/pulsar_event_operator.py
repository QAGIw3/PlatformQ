"""
Pulsar Event Operator for Apache Airflow

This operator publishes events to Apache Pulsar topics with PlatformQ's
multi-tenant topic naming convention and Avro schema support.
"""

import json
import os
from typing import Type, Dict, Any, Optional
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pulsar
from pulsar.schema import AvroSchema


class PulsarEventOperator(BaseOperator):
    """
    Publishes events to Pulsar topics with tenant isolation
    
    :param topic_base: Base topic name (e.g., 'user-events', 'project-events')
    :param schema_class: Avro schema class for the event
    :param event_data: Dictionary containing event data
    :param tenant_id: Tenant ID for multi-tenant topic routing
    :param pulsar_url: Pulsar service URL (defaults to environment variable)
    """
    
    template_fields = ['event_data', 'topic_base', 'tenant_id']
    ui_color = '#FF6B6B'
    
    @apply_defaults
    def __init__(
        self,
        topic_base: str,
        schema_class: Type,
        event_data: Dict[str, Any],
        tenant_id: str,
        pulsar_url: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.topic_base = topic_base
        self.schema_class = schema_class
        self.event_data = event_data
        self.tenant_id = tenant_id
        self.pulsar_url = pulsar_url or os.getenv('PLATFORMQ_PULSAR_URL', 'pulsar://pulsar:6650')
    
    def execute(self, context):
        """Execute the operator to publish event to Pulsar"""
        self.log.info(f"Publishing event to Pulsar topic base: {self.topic_base}")
        
        # Construct the full topic name with tenant isolation
        full_topic = f"persistent://platformq/{self.tenant_id}/{self.topic_base}"
        self.log.info(f"Full topic name: {full_topic}")
        
        # Create Pulsar client
        client = pulsar.Client(self.pulsar_url)
        
        try:
            # Create producer with Avro schema
            producer = client.create_producer(
                topic=full_topic,
                schema=AvroSchema(self.schema_class)
            )
            
            # Create event instance from data
            if isinstance(self.event_data, dict):
                event = self.schema_class(**self.event_data)
            else:
                event = self.event_data
            
            # Send the event
            message_id = producer.send(event)
            self.log.info(f"Successfully published event with message ID: {message_id}")
            
            # Store message ID in XCom for downstream tasks
            context['ti'].xcom_push(key='message_id', value=str(message_id))
            
            producer.close()
            
            return str(message_id)
            
        except Exception as e:
            self.log.error(f"Failed to publish event: {str(e)}")
            raise
        finally:
            client.close()
    
    def on_kill(self):
        """Cleanup when task is killed"""
        self.log.info("Task killed, cleaning up Pulsar connections") 