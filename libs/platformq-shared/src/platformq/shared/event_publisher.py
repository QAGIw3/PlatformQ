import pulsar
from pulsar.schema import *
import logging
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self, pulsar_url: str, schema_registry_url: Optional[str] = None):
        self.pulsar_url = pulsar_url
        self.schema_registry_url = schema_registry_url or "http://schema-registry:8081"
        self.client = None
        self.producers = {}
        self.schema_registry_client = None
        self.serializers = {}

    def connect(self):
        try:
            self.client = pulsar.Client(self.pulsar_url)
            # Initialize schema registry client
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.schema_registry_url
            })
            logger.info(f"Successfully connected to Pulsar at {self.pulsar_url} and Schema Registry at {self.schema_registry_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Pulsar or Schema Registry: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("Pulsar client connection closed.")

    def get_producer(self, topic: str, schema: Record):
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic, schema=AvroSchema(schema))
        return self.producers[topic]

    def get_avro_serializer(self, subject: str, schema_dict: Dict[str, Any]) -> AvroSerializer:
        """Get or create an Avro serializer for a subject"""
        if subject not in self.serializers:
            # Register schema if not exists
            try:
                schema_str = json.dumps(schema_dict)
                latest = self.schema_registry_client.get_latest_version(subject)
                # Check if schema has evolved
                if latest.schema.schema_str != schema_str:
                    logger.info(f"Schema evolution detected for subject '{subject}'")
            except Exception:
                # Subject doesn't exist, register it
                logger.info(f"Registering new schema for subject '{subject}'")
                from confluent_kafka.schema_registry import Schema
                schema = Schema(json.dumps(schema_dict), schema_type="AVRO")
                self.schema_registry_client.register_schema(subject, schema)
            
            # Create serializer
            self.serializers[subject] = AvroSerializer(
                self.schema_registry_client,
                json.dumps(schema_dict)
            )
        
        return self.serializers[subject]

    def publish(self, topic_base: str, tenant_id: str, schema_class: Record, data: object):
        if not self.client:
            raise RuntimeError("Pulsar client is not connected.")

        topic = f"persistent://platformq/{tenant_id}/{topic_base}"

        try:
            producer = self.get_producer(topic, schema_class)
            producer.send(data)
            logger.info(f"Published Avro event to tenant topic '{topic}'")
        except Exception as e:
            logger.error(f"Failed to publish event to topic '{topic}': {e}")
            raise

    def publish_with_schema_registry(self, topic_base: str, tenant_id: str, subject: str, 
                                    schema_dict: Dict[str, Any], data: Dict[str, Any]):
        """Publish event using schema registry for schema management"""
        if not self.client:
            raise RuntimeError("Pulsar client is not connected.")

        topic = f"persistent://platformq/{tenant_id}/{topic_base}"
        
        try:
            # Get or create serializer
            serializer = self.get_avro_serializer(subject, schema_dict)
            
            # Serialize data
            serialized_data = serializer(data, None)
            
            # Create producer if not exists
            if topic not in self.producers:
                self.producers[topic] = self.client.create_producer(
                    topic,
                    producer_name=f"{topic_base}-producer",
                    batching_enabled=True,
                    batching_max_publish_delay_ms=10
                )
            
            # Send message with schema ID in headers
            producer = self.producers[topic]
            producer.send(
                serialized_data,
                properties={
                    'schema_subject': subject,
                    'schema_version': str(self.schema_registry_client.get_latest_version(subject).version)
                }
            )
            
            logger.info(f"Published event with schema registry to topic '{topic}' using subject '{subject}'")
        except Exception as e:
            logger.error(f"Failed to publish event with schema registry: {e}")
            raise

    def create_consumer_with_schema_registry(self, topic_pattern: str, subscription: str, 
                                           subject: str) -> pulsar.Consumer:
        """Create a consumer that uses schema registry for deserialization"""
        if not self.client:
            raise RuntimeError("Pulsar client is not connected.")
        
        try:
            # Get latest schema
            latest = self.schema_registry_client.get_latest_version(subject)
            schema_dict = json.loads(latest.schema.schema_str)
            
            # Create Avro schema
            from pulsar.schema import AvroSchema
            schema_class = type(subject.replace('-', '_'), (), {
                '__annotations__': self._dict_to_annotations(schema_dict)
            })
            
            consumer = self.client.subscribe(
                topic_pattern,
                subscription,
                schema=AvroSchema(schema_class),
                consumer_type=pulsar.ConsumerType.Shared
            )
            
            logger.info(f"Created consumer with schema registry for pattern '{topic_pattern}'")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create consumer with schema registry: {e}")
            raise

    def _dict_to_annotations(self, schema_dict: Dict[str, Any]) -> Dict[str, type]:
        """Convert Avro schema to Python type annotations"""
        type_mapping = {
            'string': str,
            'int': int,
            'long': int,
            'float': float,
            'double': float,
            'boolean': bool,
            'bytes': bytes
        }
        
        annotations = {}
        for field in schema_dict.get('fields', []):
            field_type = field['type']
            if isinstance(field_type, str) and field_type in type_mapping:
                annotations[field['name']] = type_mapping[field_type]
            elif isinstance(field_type, list):
                # Handle union types (nullable fields)
                for t in field_type:
                    if t != 'null' and t in type_mapping:
                        annotations[field['name']] = Optional[type_mapping[t]]
                        break
            else:
                # Complex types default to dict
                annotations[field['name']] = Dict[str, Any]
        
        return annotations
