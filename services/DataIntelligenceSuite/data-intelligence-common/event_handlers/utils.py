"""Utility functions for event handling."""

import logging
import requests
from typing import List, Optional

from .event_types import EventType

logger = logging.getLogger(__name__)


def create_standard_topics(admin_url: str, 
                          tenant: str = "public", 
                          namespace: str = "dataintelligence",
                          auth_header: Optional[str] = None) -> bool:
    """
    Create standard topics for DataIntelligence.
    
    Args:
        admin_url: Pulsar admin API URL
        tenant: Pulsar tenant name
        namespace: Pulsar namespace
        auth_header: Optional authorization header
        
    Returns:
        True if all topics created successfully
    """
    base_url = f"{admin_url}/admin/v2/persistent/{tenant}/{namespace}"
    
    # Headers
    headers = {"Content-Type": "application/json"}
    if auth_header:
        headers["Authorization"] = auth_header
    
    # Standard topics based on EventType enum
    topics = []
    
    # Add all event types as topics
    for event_type in EventType:
        # Replace dots with dashes for Pulsar topic names
        topic_name = event_type.value.replace(".", "-")
        topics.append(topic_name)
    
    # Additional system topics
    system_topics = [
        "event-store",
        "dead-letter-queue",
        "system-notifications",
        "audit-log"
    ]
    topics.extend(system_topics)
    
    success_count = 0
    
    for topic in topics:
        try:
            response = requests.put(
                f"{base_url}/{topic}",
                headers=headers
            )
            
            if response.status_code in [204, 409]:  # Created or already exists
                logger.info(f"Topic {topic} ready")
                success_count += 1
            else:
                logger.error(f"Failed to create topic {topic}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error creating topic {topic}: {e}")
            
    logger.info(f"Created {success_count}/{len(topics)} topics successfully")
    return success_count == len(topics)


def get_topic_stats(admin_url: str,
                   tenant: str = "public",
                   namespace: str = "dataintelligence",
                   topic: str = None,
                   auth_header: Optional[str] = None) -> dict:
    """
    Get statistics for topics.
    
    Args:
        admin_url: Pulsar admin API URL
        tenant: Pulsar tenant name
        namespace: Pulsar namespace
        topic: Specific topic name (if None, get all topics)
        auth_header: Optional authorization header
        
    Returns:
        Dictionary of topic statistics
    """
    headers = {}
    if auth_header:
        headers["Authorization"] = auth_header
        
    try:
        if topic:
            # Get stats for specific topic
            url = f"{admin_url}/admin/v2/persistent/{tenant}/{namespace}/{topic}/stats"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get stats for topic {topic}: {response.status_code}")
                return {}
        else:
            # Get list of all topics
            url = f"{admin_url}/admin/v2/persistent/{tenant}/{namespace}"
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                topics = response.json()
                stats = {}
                
                for topic_path in topics:
                    topic_name = topic_path.split("/")[-1]
                    topic_stats = get_topic_stats(
                        admin_url, tenant, namespace, topic_name, auth_header
                    )
                    if topic_stats:
                        stats[topic_name] = topic_stats
                        
                return stats
            else:
                logger.error(f"Failed to get topic list: {response.status_code}")
                return {}
                
    except Exception as e:
        logger.error(f"Error getting topic stats: {e}")
        return {}


def validate_event_schema(event: dict, event_type: EventType) -> bool:
    """
    Validate event against expected schema.
    
    Args:
        event: Event data to validate
        event_type: Expected event type
        
    Returns:
        True if valid, False otherwise
    """
    # Basic validation
    if not isinstance(event, dict):
        return False
        
    # Check for required metadata
    metadata = event.get("metadata")
    if not metadata:
        logger.error("Event missing metadata")
        return False
        
    # Check event type matches
    if metadata.get("event_type") != event_type.value:
        logger.error(f"Event type mismatch: expected {event_type.value}, got {metadata.get('event_type')}")
        return False
        
    # Check required metadata fields
    required_fields = ["event_id", "source_service", "timestamp"]
    for field in required_fields:
        if field not in metadata:
            logger.error(f"Missing required metadata field: {field}")
            return False
            
    # Event-type specific validation
    if event_type in [EventType.DATA_INGESTED, EventType.DATA_PROCESSED]:
        if "dataset_id" not in event:
            logger.error("Data event missing dataset_id")
            return False
            
    elif event_type in [EventType.MODEL_TRAINED, EventType.MODEL_DEPLOYED]:
        required = ["model_id", "model_name", "model_version"]
        for field in required:
            if field not in event:
                logger.error(f"Model event missing {field}")
                return False
                
    # Add more event-type specific validations as needed
    
    return True


def create_test_event(event_type: EventType, 
                     service_name: str = "test-service") -> dict:
    """
    Create a test event for development/testing.
    
    Args:
        event_type: Type of event to create
        service_name: Source service name
        
    Returns:
        Test event data
    """
    import uuid
    from datetime import datetime
    
    base_event = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type.value,
            "source_service": service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "version": "1.0"
        }
    }
    
    # Add event-type specific data
    if event_type == EventType.DATA_INGESTED:
        base_event.update({
            "dataset_id": f"test-dataset-{uuid.uuid4().hex[:8]}",
            "dataset_name": "Test Dataset",
            "operation": "insert",
            "record_count": 1000,
            "size_bytes": 1048576
        })
        
    elif event_type == EventType.MODEL_TRAINED:
        base_event.update({
            "model_id": f"test-model-{uuid.uuid4().hex[:8]}",
            "model_name": "Test Model",
            "model_version": "1.0.0",
            "model_type": "classification",
            "operation": "training_completed",
            "metrics": {
                "accuracy": 0.95,
                "precision": 0.94,
                "recall": 0.96,
                "f1_score": 0.95
            }
        })
        
    elif event_type == EventType.PIPELINE_COMPLETED:
        base_event.update({
            "pipeline_id": f"test-pipeline-{uuid.uuid4().hex[:8]}",
            "pipeline_name": "Test Pipeline",
            "status": "completed",
            "duration_seconds": 120.5,
            "stages_completed": 5
        })
        
    # Add more test event templates as needed
    
    return base_event 