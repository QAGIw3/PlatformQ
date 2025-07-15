from dataclasses import dataclass, field
from typing import ClassVar, Type
from avro.schema import RecordSchema, make_avsc_object
import uuid
import time

# Base class for all platform events
@dataclass
class PlatformEvent:
    # This will be overridden by subclasses
    _schema: ClassVar[RecordSchema] = None

@dataclass
class SimulationStartedEvent(PlatformEvent):
    tenant_id: str
    simulation_id: str
    
    _schema: ClassVar[RecordSchema] = make_avsc_object({
        "type": "record",
        "name": "SimulationStartedEvent",
        "namespace": "com.platformq.events",
        "fields": [
            {"name": "tenant_id", "type": "string"},
            {"name": "simulation_id", "type": "string"},
        ]
    })

@dataclass
class SimulationRunCompleted(PlatformEvent):
    tenant_id: str
    simulation_id: str
    run_id: str
    status: str # "SUCCESS" or "FAILURE"
    log_uri: str # URI to the detailed log file for this run
    
    _schema: ClassVar[RecordSchema] = make_avsc_object({
        "type": "record",
        "name": "SimulationRunCompleted",
        "namespace": "com.platformq.events",
        "fields": [
            {"name": "tenant_id", "type": "string"},
            {"name": "simulation_id", "type": "string"},
            {"name": "run_id", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "log_uri", "type": "string"},
        ]
    })

class UserCreatedEvent(Record):
    tenant_id = String(required=True)
    user_id = String(required=True)
    email = String(required=True)
    full_name = String()
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class SubscriptionChangedEvent(Record):
    tenant_id = String(required=True)
    user_id = String(required=True)
    subscription_id = String(required=True)
    new_tier = String(required=True)
    new_status = String(required=True)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

# ... (and so on for all other event types) 