"""
Search Service Interfaces

Defines base interfaces for search services.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, TypeVar, Generic, Union
from dataclasses import dataclass
from datetime import datetime


T = TypeVar('T')


@dataclass
class ServiceResult(Generic[T]):
    """
    Result wrapper for service operations.
    
    Provides a consistent way to return success/failure from services.
    """
    success: bool
    data: Optional[T] = None
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    
    @classmethod
    def success(cls, data: T) -> 'ServiceResult[T]':
        """Create a successful result"""
        return cls(success=True, data=data)
    
    @classmethod
    def failure(cls, error: str, details: Optional[Dict[str, Any]] = None) -> 'ServiceResult[T]':
        """Create a failure result"""
        return cls(success=False, error=error, details=details)


@dataclass
class SearchResult:
    """Represents a single search result"""
    id: str
    type: str
    title: str
    description: Optional[str] = None
    score: float = 0.0
    highlights: Optional[Dict[str, List[str]]] = None
    metadata: Optional[Dict[str, Any]] = None
    source: Optional[str] = None


@dataclass
class SearchOptions:
    """Options for search operations"""
    filters: Optional[Dict[str, Any]] = None
    sort_by: Optional[str] = None
    sort_order: str = "desc"
    facets: Optional[List[str]] = None
    include_metadata: bool = True
    boost_recent: bool = False
    tenant_id: Optional[str] = None


class SearchStrategy(ABC):
    """Base interface for search strategies"""
    
    @abstractmethod
    async def search(
        self,
        query: str,
        options: SearchOptions,
        size: int = 10,
        from_: int = 0
    ) -> List[SearchResult]:
        """Execute search with this strategy"""
        pass
    
    @abstractmethod
    def supports_query_type(self, query_type: str) -> bool:
        """Check if this strategy supports the query type"""
        pass


class QueryAnalyzer(ABC):
    """Interface for query analysis"""
    
    @abstractmethod
    async def analyze(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Analyze query and extract intent, entities, etc."""
        pass


class EmbeddingProvider(ABC):
    """Interface for embedding generation"""
    
    @abstractmethod
    async def embed(
        self,
        content: Union[str, List[str]],
        content_type: str = "text"
    ) -> Any:  # Returns numpy array or similar
        """Generate embeddings for content"""
        pass
    
    @abstractmethod
    async def get_dimension(self, content_type: str = "text") -> int:
        """Get embedding dimension for content type"""
        pass 