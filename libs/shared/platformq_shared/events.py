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
    payload_schema_version = String(required=False)
    payload = Bytes(required=False)
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


class ProjectCreatedEvent(Record):
    tenant_id = String(required=True)
    project_id = String(required=True)
    name = String(required=True)
    openproject_id = Long(required=True)
    nextcloud_folder_path = String(required=True)
    zulip_stream_name = String(required=True)
    public = Boolean(required=True)
    dao_contract_address = String(required=False, default=None)
    dao_did = String(required=False, default=None)
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000))


class DAOEvent(Record):
    tenant_id = String(required=True)
    dao_id = String(required=True)
    event_type = String(required=True, doc="Type of DAO event, e.g., 'ProposalCreated', 'VoteCast', 'ProposalExecuted'")
    blockchain_tx_hash = String(required=False, doc="Transaction hash of the on-chain event")
    proposal_id = String(required=False, doc="ID of the proposal if the event is related to a proposal")
    voter_id = String(required=False, doc="ID of the voter if the event is a vote cast")
    event_data = String(required=False, doc="JSON string of additional event-specific data") # Generic field for flexibility
    event_timestamp = Long(required=True, default=lambda: int(time.time() * 1000)) 