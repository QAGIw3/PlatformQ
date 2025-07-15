from dataclasses import dataclass, field
from typing import ClassVar, Type
from avro.schema import RecordSchema, make_avsc_object
import uuid
import time
from pulsar.schema import *

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

class IssueVerifiableCredential(Record):
    tenant_id = String(required=True)
    proposal_id = String(required=True)
    approver_id = String(required=True)
    customer_name = String(required=True)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class VerifiableCredentialIssued(Record):
    tenant_id = String(required=True)
    proposal_id = String(required=True)
    credential_id = String(required=True)
    issuer_service = String(default="verifiable-credential-service")
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class ExecuteWasmFunction(Record):
    tenant_id = String(required=True)
    asset_id = String(required=True)
    asset_uri = String(required=True)
    wasm_module_id = String(required=True)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class FunctionExecutionCompleted(Record):
    tenant_id = String(required=True)
    asset_id = String(required=True)
    wasm_module_id = String(required=True)
    status = String(required=True)
    results = Map(String(), required=False)
    error_message = String(required=False)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

class DigitalAssetCreated(Record):
    tenant_id = String(required=True)
    asset_id = String(required=True)
    asset_name = String(required=True)
    asset_type = String(required=True)
    owner_id = String(required=True)
    raw_data_uri = String(required=False)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))

# ... (and so on for all other event types) 