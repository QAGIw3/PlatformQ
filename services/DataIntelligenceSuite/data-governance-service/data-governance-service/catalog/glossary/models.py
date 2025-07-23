"""
Glossary data models and enums
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, Any, List, Optional


class TermStatus(str, Enum):
    """Business term status"""
    DRAFT = "draft"
    APPROVED = "approved" 
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUGGESTED = "suggested"
    OBSOLETE = "obsolete"


class TermCategory(str, Enum):
    """Business term categories"""
    ENTITY = "entity"
    ATTRIBUTE = "attribute"
    METRIC = "metric"
    DIMENSION = "dimension"
    PROCESS = "process"
    RULE = "rule"
    OTHER = "other"


@dataclass
class BusinessTerm:
    """Represents a business term"""
    name: str
    display_name: str
    definition: str
    category: TermCategory
    status: TermStatus
    synonyms: List[str]
    related_terms: List[str]
    examples: List[str]
    owner: str
    steward: Optional[str]
    created_by: str
    created_date: datetime
    modified_date: datetime
    tags: List[str]
    metadata: Dict[str, Any]
    technical_mappings: List[Dict[str, Any]]  # Mapped technical assets
    guid: Optional[str] = None  # Atlas GUID


@dataclass
class TermMapping:
    """Mapping between business term and technical asset"""
    term_id: str
    asset_id: str
    confidence: float
    mapping_type: str  # direct, inferred, suggested
    created_date: datetime
    created_by: str
    approved: bool
    approval_date: Optional[datetime]
    approved_by: Optional[str] 