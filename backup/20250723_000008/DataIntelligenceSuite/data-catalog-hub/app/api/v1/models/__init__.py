"""
API v1 Models

Request and response models for the Data Catalog Hub API v1.
"""

from .entity_models import (
    EntityCreateRequest,
    EntityUpdateRequest,
    EntityResponse,
    EntityListResponse,
    EntitySearchRequest
)

from .schema_models import (
    SchemaRegisterRequest,
    SchemaUpdateRequest,
    SchemaResponse,
    SchemaListResponse,
    SchemaValidationRequest,
    SchemaValidationResponse,
    SchemaInferenceRequest,
    SchemaInferenceResponse
)

from .lineage_models import (
    LineageCreateRequest,
    LineageResponse,
    LineageGraphResponse,
    ImpactAnalysisRequest,
    ImpactAnalysisResponse,
    TransformationTrackingRequest,
    TransformationTrackingResponse,
    ComplianceAuditRequest,
    ComplianceAuditResponse
)

from .classification_models import (
    ClassificationCreateRequest,
    ClassificationResponse,
    ClassificationAssignRequest,
    ClassificationAssignResponse,
    AutoClassifyRequest,
    AutoClassifyResponse,
    ClassificationRuleRequest,
    ClassificationRuleResponse,
    ClassificationScanRequest,
    ClassificationScanResponse
)

from .glossary_models import (
    GlossaryCreateRequest,
    GlossaryResponse,
    TermCreateRequest,
    TermUpdateRequest,
    TermResponse,
    TermListResponse,
    TermAssignmentRequest,
    TermAssignmentResponse,
    TermSuggestionRequest,
    TermSuggestionResponse,
    AutoMappingRequest,
    AutoMappingResponse
)

from .search_models import (
    UnifiedSearchRequest,
    UnifiedSearchResponse,
    TextSearchRequest,
    VectorSearchRequest,
    HybridSearchRequest,
    AISearchRequest,
    SearchResult,
    SearchFacet,
    SearchSuggestion,
    SavedSearch
)

from .common import (
    PaginationParams,
    PaginatedResponse,
    ErrorResponse,
    SuccessResponse,
    MetadataField,
    ClassificationAttribute,
    RelationshipInfo
)

__all__ = [
    # Entity models
    'EntityCreateRequest',
    'EntityUpdateRequest',
    'EntityResponse',
    'EntityListResponse',
    'EntitySearchRequest',
    
    # Schema models
    'SchemaRegisterRequest',
    'SchemaUpdateRequest',
    'SchemaResponse',
    'SchemaListResponse',
    'SchemaValidationRequest',
    'SchemaValidationResponse',
    'SchemaInferenceRequest',
    'SchemaInferenceResponse',
    
    # Lineage models
    'LineageCreateRequest',
    'LineageResponse',
    'LineageGraphResponse',
    'ImpactAnalysisRequest',
    'ImpactAnalysisResponse',
    'TransformationTrackingRequest',
    'TransformationTrackingResponse',
    'ComplianceAuditRequest',
    'ComplianceAuditResponse',
    
    # Classification models
    'ClassificationCreateRequest',
    'ClassificationResponse',
    'ClassificationAssignRequest',
    'ClassificationAssignResponse',
    'AutoClassifyRequest',
    'AutoClassifyResponse',
    'ClassificationRuleRequest',
    'ClassificationRuleResponse',
    'ClassificationScanRequest',
    'ClassificationScanResponse',
    
    # Glossary models
    'GlossaryCreateRequest',
    'GlossaryResponse',
    'TermCreateRequest',
    'TermUpdateRequest',
    'TermResponse',
    'TermListResponse',
    'TermAssignmentRequest',
    'TermAssignmentResponse',
    'TermSuggestionRequest',
    'TermSuggestionResponse',
    'AutoMappingRequest',
    'AutoMappingResponse',
    
    # Search models
    'UnifiedSearchRequest',
    'UnifiedSearchResponse',
    'TextSearchRequest',
    'VectorSearchRequest',
    'HybridSearchRequest',
    'AISearchRequest',
    'SearchResult',
    'SearchFacet',
    'SearchSuggestion',
    'SavedSearch',
    
    # Common models
    'PaginationParams',
    'PaginatedResponse',
    'ErrorResponse',
    'SuccessResponse',
    'MetadataField',
    'ClassificationAttribute',
    'RelationshipInfo'
] 