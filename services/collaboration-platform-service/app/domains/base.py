"""
Base Domain Adapter for Collaboration Platform

This module defines the interface that all collaboration domains must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from platformq_shared.crdt import BaseCRDT


class OperationType(str, Enum):
    """Standard operation types across all domains"""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    TRANSFORM = "transform"
    CUSTOM = "custom"


@dataclass
class DomainOperation:
    """Base operation for all domains"""
    operation_id: str
    operation_type: OperationType
    user_id: str
    session_id: str
    timestamp: datetime
    data: Dict[str, Any]
    vector_clock: Dict[str, int]
    parent_operations: List[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "operation_type": self.operation_type.value,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "vector_clock": self.vector_clock,
            "parent_operations": self.parent_operations or []
        }


@dataclass
class DomainState:
    """Base state representation for domains"""
    session_id: str
    domain_type: str
    version: int
    data: Dict[str, Any]
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "domain_type": self.domain_type,
            "version": self.version,
            "data": self.data,
            "metadata": self.metadata
        }


class BaseDomainAdapter(ABC):
    """
    Abstract base class for domain adapters.
    Each collaboration domain (simulation, CAD, etc.) must implement this interface.
    """
    
    def __init__(self, domain_name: str):
        self.domain_name = domain_name
        self._crdt = None
        self._operation_handlers = {}
        self._initialize_handlers()
    
    @abstractmethod
    def _initialize_handlers(self):
        """Initialize operation handlers for this domain"""
        pass
    
    @abstractmethod
    def create_crdt(self) -> BaseCRDT:
        """Create the appropriate CRDT for this domain"""
        pass
    
    @abstractmethod
    def validate_operation(self, operation: DomainOperation) -> Tuple[bool, Optional[str]]:
        """
        Validate if an operation is valid for this domain.
        Returns (is_valid, error_message)
        """
        pass
    
    @abstractmethod
    def apply_operation(self, operation: DomainOperation, state: DomainState) -> DomainState:
        """Apply an operation to the current state"""
        pass
    
    @abstractmethod
    def merge_states(self, state1: DomainState, state2: DomainState) -> DomainState:
        """Merge two states according to domain rules"""
        pass
    
    @abstractmethod
    def optimize_state(self, state: DomainState) -> DomainState:
        """Optimize state representation (e.g., compact history, merge operations)"""
        pass
    
    @abstractmethod
    def get_view_for_user(self, state: DomainState, user_id: str, 
                         viewport: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Get a user-specific view of the state.
        Can implement LOD, filtering, permissions, etc.
        """
        pass
    
    @abstractmethod
    def get_resource_requirements(self, state: DomainState) -> Dict[str, Any]:
        """Calculate compute resource requirements for current state"""
        pass
    
    def handle_operation(self, operation: DomainOperation, state: DomainState) -> DomainState:
        """Main entry point for handling operations"""
        # Validate
        is_valid, error = self.validate_operation(operation)
        if not is_valid:
            raise ValueError(f"Invalid operation: {error}")
        
        # Apply
        new_state = self.apply_operation(operation, state)
        
        # Optimize if needed
        if self._should_optimize(new_state):
            new_state = self.optimize_state(new_state)
        
        return new_state
    
    def _should_optimize(self, state: DomainState) -> bool:
        """Determine if state should be optimized"""
        # Default implementation - optimize every 100 versions
        return state.version % 100 == 0
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Return domain capabilities and requirements"""
        return {
            "domain_name": self.domain_name,
            "supported_operations": list(self._operation_handlers.keys()),
            "requires_gpu": False,
            "max_users": 100,
            "update_rate_hz": 60
        }
    
    def serialize_state(self, state: DomainState) -> bytes:
        """Serialize state for storage/transmission"""
        import pickle
        return pickle.dumps(state.to_dict())
    
    def deserialize_state(self, data: bytes) -> DomainState:
        """Deserialize state from storage/transmission"""
        import pickle
        state_dict = pickle.loads(data)
        return DomainState(**state_dict)
    
    def get_metrics(self, state: DomainState) -> Dict[str, Any]:
        """Get domain-specific metrics"""
        return {
            "version": state.version,
            "size_bytes": len(self.serialize_state(state)),
            "metadata": state.metadata
        }


class DomainRegistry:
    """Registry for all available domain adapters"""
    
    def __init__(self):
        self._adapters: Dict[str, BaseDomainAdapter] = {}
    
    def register(self, adapter: BaseDomainAdapter):
        """Register a domain adapter"""
        self._adapters[adapter.domain_name] = adapter
    
    def get(self, domain_name: str) -> BaseDomainAdapter:
        """Get a domain adapter by name"""
        if domain_name not in self._adapters:
            raise ValueError(f"Unknown domain: {domain_name}")
        return self._adapters[domain_name]
    
    def list_domains(self) -> List[str]:
        """List all registered domains"""
        return list(self._adapters.keys())
    
    def get_capabilities(self) -> Dict[str, Dict[str, Any]]:
        """Get capabilities of all domains"""
        return {
            name: adapter.get_capabilities()
            for name, adapter in self._adapters.items()
        } 