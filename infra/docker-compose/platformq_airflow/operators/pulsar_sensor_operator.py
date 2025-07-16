"""
Pulsar Sensor Operator for Apache Airflow

This operator waits for and consumes events from Apache Pulsar topics
using PlatformQ's topic patterns and Avro schemas.
"""

import os
import re
from typing import Type, Optional, Callable, Any
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults, poke_mode_only
import pulsar
from pulsar.schema import AvroSchema


class PulsarSensorOperator(BaseSensorOperator):
    """
    Waits for and consumes events from Pulsar topics
    
    :param topic_pattern: Topic pattern to subscribe to (e.g., 'persistent://platformq/.*/user-events')
    :param schema_class: Avro schema class for the event
    :param subscription_name: Pulsar subscription name
    :param event_filter: Optional function to filter events
    :param pulsar_url: Pulsar service URL (defaults to environment variable)
    :param mode: Sensor mode ('poke' or 'reschedule')
    :param timeout: Timeout in seconds for receiving messages
    """
    
    template_fields = ['topic_pattern', 'subscription_name']
    ui_color = '#4ECDC4'
    
    @apply_defaults
    def __init__(
        self,
        topic_pattern: str,
        schema_class: Type,
        subscription_name: str,
        event_filter: Optional[Callable[[Any], bool]] = None,
        pulsar_url: Optional[str] = None,
        timeout: int = 1000,
        mode: str = 'poke',
        *args,
        **kwargs
    ):
        super().__init__(mode=mode, *args, **kwargs)
        self.topic_pattern = topic_pattern
        self.schema_class = schema_class
        self.subscription_name = subscription_name
        self.event_filter = event_filter
        self.pulsar_url = pulsar_url or os.getenv('PLATFORMQ_PULSAR_URL', 'pulsar://pulsar:6650')
        self.timeout = timeout
        self._client = None
        self._consumer = None
    
    def _get_consumer(self):
        """Get or create Pulsar consumer"""
        if self._consumer is None:
            self._client = pulsar.Client(self.pulsar_url)
            self._consumer = self._client.subscribe(
                topic=self.topic_pattern,
                subscription_name=self.subscription_name,
                consumer_type=pulsar.ConsumerType.Shared,
                schema=AvroSchema(self.schema_class),
                pattern_auto_discovery_period=60
            )
        return self._consumer
    
    @poke_mode_only
    def poke(self, context):
        """Check if an event is available"""
        self.log.info(f"Checking for events on pattern: {self.topic_pattern}")
        
        consumer = self._get_consumer()
        
        try:
            # Try to receive a message with timeout
            msg = consumer.receive(timeout_millis=self.timeout)
            
            if msg:
                event = msg.value()
                self.log.info(f"Received event: {event}")
                
                # Apply filter if provided
                if self.event_filter and not self.event_filter(event):
                    self.log.info("Event filtered out, acknowledging and continuing")
                    consumer.acknowledge(msg)
                    return False
                
                # Extract tenant ID from topic if possible
                topic = msg.topic_name()
                tenant_match = re.search(r'platformq/([a-f0-9-]+)/', topic)
                tenant_id = tenant_match.group(1) if tenant_match else None
                
                # Store event data in XCom for downstream tasks
                context['ti'].xcom_push(key='event', value=event.__dict__)
                context['ti'].xcom_push(key='topic', value=topic)
                context['ti'].xcom_push(key='tenant_id', value=tenant_id)
                context['ti'].xcom_push(key='message_id', value=msg.message_id())
                
                # Acknowledge the message
                consumer.acknowledge(msg)
                
                return True
                
        except Exception as e:
            if "Timeout" in str(e):
                self.log.debug("No message received within timeout")
                return False
            else:
                self.log.error(f"Error receiving message: {str(e)}")
                raise
        
        return False
    
    def execute(self, context):
        """Execute the sensor"""
        # Override to ensure proper cleanup
        try:
            return super().execute(context)
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Cleanup Pulsar connections"""
        if self._consumer:
            try:
                self._consumer.close()
            except:
                pass
            self._consumer = None
        
        if self._client:
            try:
                self._client.close()
            except:
                pass
            self._client = None
    
    def on_kill(self):
        """Cleanup when task is killed"""
        self.log.info("Task killed, cleaning up Pulsar connections")
        self._cleanup() 