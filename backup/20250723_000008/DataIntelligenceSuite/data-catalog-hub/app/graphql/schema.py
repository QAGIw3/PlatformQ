"""
GraphQL Schema Definition for Data Catalog Hub

Defines the GraphQL schema with federation support for unified metadata access.
"""

import strawberry
from strawberry.federation import FederationSchema
from typing import List, Optional, Dict, Any
from datetime import datetime
import json

from data_intelligence_common.integrations.graphql_federation import (
    GraphQLFederationService,
    FederationConfig,
    FederatedType,
    FederatedField
)


# GraphQL Types

@strawberry.federation.type(keys=["guid"])
class DataAsset:
    """Represents a data asset in the catalog"""
    guid: str
    name: str
    qualified_name: str
    type_name: str
    description: Optional[str]
    owner: Optional[str]
    created_time: datetime
    modified_time: datetime
    attributes: strawberry.scalars.JSON
    classifications: List[str]
    
    @strawberry.field
    async def lineage(self, info) -> Optional["DataLineage"]:
        """Get lineage information for this asset"""
        lineage_service = info.context["container"].lineage_processor()
        return await lineage_service.get_lineage(self.guid)
    
    @strawberry.field
    async def quality_score(self, info) -> Optional[float]:
        """Get quality score for this asset"""
        quality_service = info.context["container"].quality_integration_engine()
        scores = await quality_service.get_quality_scores([self.guid])
        return scores.get(self.guid)
    
    @strawberry.field
    async def glossary_terms(self, info) -> List["GlossaryTerm"]:
        """Get associated glossary terms"""
        glossary_service = info.context["container"].glossary_manager()
        return await glossary_service.get_terms_for_entity(self.guid)
    
    @strawberry.federation.field(requires=["guid"])
    async def access_analytics(self, info) -> Optional["AccessAnalytics"]:
        """Get access analytics for this asset"""
        analytics_service = info.context["container"].access_analytics_engine()
        return await analytics_service.get_entity_analytics(self.guid)


@strawberry.type
class DataLineage:
    """Represents data lineage information"""
    entity_guid: str
    upstream_entities: List[DataAsset]
    downstream_entities: List[DataAsset]
    processes: List["DataProcess"]
    impact_radius: int
    
    @strawberry.field
    async def full_graph(self, info, depth: int = 3) -> strawberry.scalars.JSON:
        """Get full lineage graph as JSON"""
        lineage_service = info.context["container"].lineage_processor()
        return await lineage_service.get_lineage_graph(self.entity_guid, depth)


@strawberry.type
class DataProcess:
    """Represents a data transformation process"""
    guid: str
    name: str
    process_type: str
    inputs: List[DataAsset]
    outputs: List[DataAsset]
    created_time: datetime
    attributes: strawberry.scalars.JSON


@strawberry.federation.type(keys=["guid"])
class GlossaryTerm:
    """Represents a business glossary term"""
    guid: str
    name: str
    definition: str
    abbreviation: Optional[str]
    status: str
    created_by: str
    created_time: datetime
    related_terms: List["GlossaryTerm"]
    assigned_entities: List[DataAsset]
    
    @strawberry.field
    async def ai_suggestions(self, info) -> List[str]:
        """Get AI-powered suggestions for this term"""
        ai_service = info.context["container"].ai_glossary_enhancements()
        return await ai_service.suggest_related_terms(self.name, self.definition)


@strawberry.type
class AccessAnalytics:
    """Access analytics for a data asset"""
    entity_guid: str
    total_accesses: int
    unique_users: int
    recent_accesses: List["AccessRecord"]
    access_trend: str  # "increasing", "stable", "decreasing"
    popular_queries: List[str]


@strawberry.type
class AccessRecord:
    """Individual access record"""
    user_id: str
    timestamp: datetime
    operation: str
    duration_ms: int


@strawberry.type
class SearchResult:
    """Unified search result"""
    score: float
    entity: DataAsset
    highlights: Dict[str, List[str]]
    explanation: Optional[str]
    source: str  # "text", "vector", "hybrid", "ai"


@strawberry.type
class QualityMetrics:
    """Data quality metrics"""
    entity_guid: str
    completeness: float
    accuracy: float
    consistency: float
    timeliness: float
    overall_score: float
    issues: List["QualityIssue"]
    last_assessed: datetime


@strawberry.type
class QualityIssue:
    """Data quality issue"""
    type: str
    severity: str  # "critical", "high", "medium", "low"
    description: str
    affected_fields: List[str]
    suggested_fix: Optional[str]


@strawberry.type
class DataClassification:
    """Data classification result"""
    entity_guid: str
    classifications: List["Classification"]
    confidence_scores: Dict[str, float]
    auto_classified: bool
    classified_time: datetime


@strawberry.type
class Classification:
    """Individual classification"""
    name: str
    category: str  # "PII", "PCI", "PHI", "CONFIDENTIAL", etc.
    confidence: float
    propagated: bool
    source: str  # "manual", "auto", "inherited"


# Query Root

@strawberry.type
class Query:
    """Root query type for Data Catalog Hub"""
    
    @strawberry.field
    async def data_asset(self, info, guid: str) -> Optional[DataAsset]:
        """Get a data asset by GUID"""
        entity_service = info.context["container"].entity_service()
        entity = await entity_service.get_entity(guid)
        if entity:
            return DataAsset(
                guid=entity.guid,
                name=entity.attributes.get("name"),
                qualified_name=entity.attributes.get("qualifiedName"),
                type_name=entity.type_name,
                description=entity.attributes.get("description"),
                owner=entity.attributes.get("owner"),
                created_time=entity.created_time,
                modified_time=entity.modified_time,
                attributes=entity.attributes,
                classifications=[c.type_name for c in entity.classifications]
            )
        return None
    
    @strawberry.field
    async def search_assets(
        self, 
        info,
        query: str,
        limit: int = 20,
        offset: int = 0,
        entity_types: Optional[List[str]] = None,
        search_type: str = "hybrid"
    ) -> List[SearchResult]:
        """Search for data assets"""
        search_service = info.context["container"].unified_search_service()
        
        if search_type == "text":
            results = await search_service.text_search(
                query=query,
                limit=limit,
                offset=offset,
                entity_types=entity_types
            )
        elif search_type == "vector":
            results = await search_service.vector_search(
                query=query,
                limit=limit,
                entity_types=entity_types
            )
        elif search_type == "ai":
            results = await search_service.ai_search(
                query=query,
                limit=limit,
                use_rag=True
            )
        else:  # hybrid
            results = await search_service.hybrid_search(
                query=query,
                limit=limit,
                offset=offset,
                entity_types=entity_types
            )
        
        return [
            SearchResult(
                score=r.score,
                entity=DataAsset(
                    guid=r.entity.guid,
                    name=r.entity.attributes.get("name"),
                    qualified_name=r.entity.attributes.get("qualifiedName"),
                    type_name=r.entity.type_name,
                    description=r.entity.attributes.get("description"),
                    owner=r.entity.attributes.get("owner"),
                    created_time=r.entity.created_time,
                    modified_time=r.entity.modified_time,
                    attributes=r.entity.attributes,
                    classifications=[c.type_name for c in r.entity.classifications]
                ),
                highlights=r.highlights,
                explanation=r.explanation,
                source=search_type
            )
            for r in results
        ]
    
    @strawberry.field
    async def glossary_term(self, info, guid: str) -> Optional[GlossaryTerm]:
        """Get a glossary term by GUID"""
        glossary_service = info.context["container"].glossary_manager()
        term = await glossary_service.get_term(guid)
        if term:
            return GlossaryTerm(
                guid=term.guid,
                name=term.name,
                definition=term.definition,
                abbreviation=term.abbreviation,
                status=term.status,
                created_by=term.created_by,
                created_time=term.created_time,
                related_terms=[],  # Would be populated from service
                assigned_entities=[]  # Would be populated from service
            )
        return None
    
    @strawberry.field
    async def data_lineage(
        self,
        info,
        entity_guid: str,
        direction: str = "BOTH",
        depth: int = 3
    ) -> Optional[DataLineage]:
        """Get data lineage for an entity"""
        lineage_service = info.context["container"].lineage_processor()
        lineage = await lineage_service.get_lineage(
            entity_guid,
            direction=direction,
            depth=depth
        )
        
        if lineage:
            # Convert to GraphQL types
            upstream = [
                DataAsset(
                    guid=e.guid,
                    name=e.attributes.get("name"),
                    qualified_name=e.attributes.get("qualifiedName"),
                    type_name=e.type_name,
                    description=e.attributes.get("description"),
                    owner=e.attributes.get("owner"),
                    created_time=e.created_time,
                    modified_time=e.modified_time,
                    attributes=e.attributes,
                    classifications=[c.type_name for c in e.classifications]
                )
                for e in lineage.upstream_entities
            ]
            
            downstream = [
                DataAsset(
                    guid=e.guid,
                    name=e.attributes.get("name"),
                    qualified_name=e.attributes.get("qualifiedName"),
                    type_name=e.type_name,
                    description=e.attributes.get("description"),
                    owner=e.attributes.get("owner"),
                    created_time=e.created_time,
                    modified_time=e.modified_time,
                    attributes=e.attributes,
                    classifications=[c.type_name for c in e.classifications]
                )
                for e in lineage.downstream_entities
            ]
            
            processes = [
                DataProcess(
                    guid=p.guid,
                    name=p.name,
                    process_type=p.process_type,
                    inputs=[],  # Would be populated
                    outputs=[],  # Would be populated
                    created_time=p.created_time,
                    attributes=p.attributes
                )
                for p in lineage.processes
            ]
            
            return DataLineage(
                entity_guid=entity_guid,
                upstream_entities=upstream,
                downstream_entities=downstream,
                processes=processes,
                impact_radius=len(upstream) + len(downstream)
            )
        
        return None
    
    @strawberry.field
    async def quality_metrics(
        self,
        info,
        entity_guid: str
    ) -> Optional[QualityMetrics]:
        """Get quality metrics for an entity"""
        quality_service = info.context["container"].quality_integration_engine()
        metrics = await quality_service.get_quality_metrics(entity_guid)
        
        if metrics:
            issues = [
                QualityIssue(
                    type=issue["type"],
                    severity=issue["severity"],
                    description=issue["description"],
                    affected_fields=issue.get("affected_fields", []),
                    suggested_fix=issue.get("suggested_fix")
                )
                for issue in metrics.get("issues", [])
            ]
            
            return QualityMetrics(
                entity_guid=entity_guid,
                completeness=metrics["completeness"],
                accuracy=metrics["accuracy"],
                consistency=metrics["consistency"],
                timeliness=metrics["timeliness"],
                overall_score=metrics["overall_score"],
                issues=issues,
                last_assessed=metrics["last_assessed"]
            )
        
        return None


# Mutation Root

@strawberry.type
class Mutation:
    """Root mutation type for Data Catalog Hub"""
    
    @strawberry.mutation
    async def create_glossary_term(
        self,
        info,
        name: str,
        definition: str,
        abbreviation: Optional[str] = None,
        status: str = "DRAFT"
    ) -> GlossaryTerm:
        """Create a new glossary term"""
        glossary_service = info.context["container"].glossary_manager()
        ai_service = info.context["container"].ai_glossary_enhancements()
        
        # Get AI suggestions for the term
        suggestions = await ai_service.suggest_definition(name)
        
        term = await glossary_service.create_term(
            name=name,
            definition=definition,
            abbreviation=abbreviation,
            status=status,
            ai_suggestions=suggestions
        )
        
        return GlossaryTerm(
            guid=term.guid,
            name=term.name,
            definition=term.definition,
            abbreviation=term.abbreviation,
            status=term.status,
            created_by=term.created_by,
            created_time=term.created_time,
            related_terms=[],
            assigned_entities=[]
        )
    
    @strawberry.mutation
    async def classify_entity(
        self,
        info,
        entity_guid: str,
        classification_names: List[str],
        auto_propagate: bool = True
    ) -> DataClassification:
        """Classify a data entity"""
        classifier = info.context["container"].classifier()
        
        result = await classifier.classify_entity(
            entity_guid=entity_guid,
            classifications=classification_names,
            propagate=auto_propagate
        )
        
        classifications = [
            Classification(
                name=c["name"],
                category=c["category"],
                confidence=c["confidence"],
                propagated=c.get("propagated", False),
                source=c.get("source", "manual")
            )
            for c in result["classifications"]
        ]
        
        return DataClassification(
            entity_guid=entity_guid,
            classifications=classifications,
            confidence_scores=result["confidence_scores"],
            auto_classified=False,
            classified_time=datetime.utcnow()
        )
    
    @strawberry.mutation
    async def create_lineage(
        self,
        info,
        process_name: str,
        process_type: str,
        input_guids: List[str],
        output_guids: List[str]
    ) -> DataProcess:
        """Create a lineage relationship"""
        lineage_service = info.context["container"].lineage_processor()
        
        process = await lineage_service.create_process(
            name=process_name,
            process_type=process_type,
            inputs=input_guids,
            outputs=output_guids
        )
        
        return DataProcess(
            guid=process.guid,
            name=process.name,
            process_type=process.process_type,
            inputs=[],  # Would be populated
            outputs=[],  # Would be populated
            created_time=process.created_time,
            attributes=process.attributes
        )


# Create the schema

schema = strawberry.federation.Schema(
    query=Query,
    mutation=Mutation,
    enable_federation_2=True
) 