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