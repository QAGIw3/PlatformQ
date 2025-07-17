from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

@dataclass
class PlatformEvent:
    event_id: str
    event_timestamp: int
    source_service: str

@dataclass
class DatasetLineageEvent(PlatformEvent):
    dataset_id: str
    dataset_name: str
    layer: str
    source_datasets: List[str]
    output_path: str
    schema: Dict[str, Any]
    quality_report: Dict[str, Any]
    is_gold_layer: bool = False
    triggered_by: Optional[str] = None

@dataclass
class IndexableEntityEvent(PlatformEvent):
    entity_id: str
    entity_type: str
    event_type: str  # CREATED, UPDATED, DELETED
    data: Dict[str, Any]

@dataclass
class SimulationMetricsEvent(PlatformEvent):
    simulation_id: str
    metrics: Dict[str, Any]

@dataclass
class ProactiveAlertEvent(PlatformEvent):
    simulation_id: str
    alert_type: str
    severity: str
    reason: str
    details: Dict[str, Any]

@dataclass
class LineageEvent(PlatformEvent):
    processor_name: str
    processor_version: str
    triggered_by: str # User ID
    input_assets: List[Dict[str, str]] # e.g., [{"asset_id": "...", "version": "..."}]
    output_assets: List[Dict[str, str]]
    parameters: Dict[str, Any]
    execution_time_ms: int 