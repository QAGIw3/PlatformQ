"""
Enhanced Business Glossary

Provides intelligent mapping between technical metadata and business terms,
with AI-powered suggestions and automatic term discovery.
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
import asyncio
from dataclasses import dataclass
from enum import Enum
import re
from collections import defaultdict
import json

from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.corpus import wordnet
import Levenshtein

from app.core.atlas_client import AtlasClient
from app.core.config import settings

logger = logging.getLogger(__name__)

# Download required NLTK data
try:
    nltk.download('wordnet', quiet=True)
    nltk.download('averaged_perceptron_tagger', quiet=True)
except:
    pass


class TermStatus(str, Enum):
    """Business term status"""
    DRAFT = "draft"
    APPROVED = "approved"
    DEPRECATED = "deprecated"
    SUGGESTED = "suggested"


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


class BusinessGlossaryEnhanced:
    """
    Enhanced business glossary with AI-powered term mapping and discovery
    """
    
    def __init__(self, atlas_client: AtlasClient):
        self.atlas_client = atlas_client
        
        # Initialize sentence transformer for semantic similarity
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Term patterns for extraction
        self.term_patterns = {
            "camel_case": re.compile(r'[A-Z]([A-Z0-9]*[a-z][a-z0-9]*[A-Z]|[a-z0-9]*[A-Z][A-Z0-9]*[a-z])[A-Za-z0-9]*'),
            "snake_case": re.compile(r'[a-z]+(?:_[a-z]+)+'),
            "business_terms": re.compile(r'\b(?:customer|order|product|revenue|cost|profit|margin|sales|inventory|supplier|employee|account|transaction)\b', re.I)
        }
        
        # Common abbreviations and their expansions
        self.abbreviations = {
            "cust": "customer",
            "prod": "product",
            "inv": "inventory",
            "qty": "quantity",
            "amt": "amount",
            "dt": "date",
            "id": "identifier",
            "num": "number",
            "desc": "description",
            "addr": "address",
            "dept": "department",
            "emp": "employee",
            "mgr": "manager",
            "org": "organization",
            "acct": "account",
            "txn": "transaction",
            "rev": "revenue",
            "ytd": "year to date",
            "mtd": "month to date"
        }
        
        # Cache for embeddings
        self.embedding_cache = {}
        
    async def suggest_business_terms(
        self,
        technical_name: str,
        context: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Suggest business terms for a technical name using AI
        """
        suggestions = []
        
        # 1. Direct pattern matching
        pattern_suggestions = self._extract_terms_from_name(technical_name)
        
        # 2. Abbreviation expansion
        expanded_terms = self._expand_abbreviations(technical_name)
        
        # 3. Semantic similarity with existing terms
        similar_terms = await self._find_semantically_similar_terms(technical_name)
        
        # 4. Context-based suggestions
        if context:
            context_terms = self._suggest_from_context(technical_name, context)
            suggestions.extend(context_terms)
        
        # Combine and rank suggestions
        all_suggestions = []
        
        # Add pattern-based suggestions
        for term in pattern_suggestions:
            all_suggestions.append({
                "term": term,
                "confidence": 0.9,
                "source": "pattern_matching",
                "reason": "Extracted from technical name"
            })
        
        # Add expansion suggestions
        for original, expanded in expanded_terms.items():
            all_suggestions.append({
                "term": expanded,
                "confidence": 0.85,
                "source": "abbreviation_expansion",
                "reason": f"Expanded from '{original}'"
            })
        
        # Add semantic suggestions
        for term, similarity in similar_terms[:5]:
            all_suggestions.append({
                "term": term.name,
                "confidence": similarity,
                "source": "semantic_similarity",
                "reason": f"Similar to existing term '{term.name}'"
            })
        
        # Deduplicate and sort by confidence
        seen = set()
        unique_suggestions = []
        for sugg in sorted(all_suggestions, key=lambda x: x["confidence"], reverse=True):
            if sugg["term"].lower() not in seen:
                seen.add(sugg["term"].lower())
                unique_suggestions.append(sugg)
        
        return unique_suggestions[:10]  # Return top 10
    
    def _extract_terms_from_name(self, technical_name: str) -> List[str]:
        """
        Extract potential business terms from technical name
        """
        terms = []
        
        # Split camelCase
        camel_parts = re.findall(r'[A-Z][a-z]+|[a-z]+|[A-Z]+(?=[A-Z][a-z]|\b)', technical_name)
        if camel_parts:
            # Individual words
            terms.extend([part.lower() for part in camel_parts if len(part) > 2])
            # Full phrase
            terms.append(" ".join(camel_parts).lower())
        
        # Split snake_case
        if '_' in technical_name:
            parts = technical_name.split('_')
            terms.extend([part.lower() for part in parts if len(part) > 2])
            terms.append(" ".join(parts).lower())
        
        # Extract known business terms
        for match in self.term_patterns["business_terms"].finditer(technical_name):
            terms.append(match.group().lower())
        
        return list(set(terms))
    
    def _expand_abbreviations(self, technical_name: str) -> Dict[str, str]:
        """
        Expand abbreviations in technical name
        """
        expanded = {}
        
        # Split into parts
        parts = re.split(r'[_\s\-\.]+', technical_name.lower())
        
        for part in parts:
            if part in self.abbreviations:
                expanded[part] = self.abbreviations[part]
        
        # Also check for abbreviations at the start/end of camelCase
        for abbr, expansion in self.abbreviations.items():
            if technical_name.lower().startswith(abbr):
                expanded[abbr] = expansion
            if technical_name.lower().endswith(abbr):
                expanded[abbr] = expansion
                
        return expanded
    
    async def _find_semantically_similar_terms(
        self,
        technical_name: str,
        threshold: float = 0.6
    ) -> List[Tuple[BusinessTerm, float]]:
        """
        Find semantically similar business terms
        """
        # Get embedding for technical name
        tech_embedding = self._get_embedding(technical_name)
        
        # Get all approved business terms
        terms = await self._get_all_business_terms()
        
        similar_terms = []
        
        for term in terms:
            # Get embedding for business term
            term_text = f"{term.name} {term.definition}"
            term_embedding = self._get_embedding(term_text)
            
            # Calculate similarity
            similarity = cosine_similarity(
                tech_embedding.reshape(1, -1),
                term_embedding.reshape(1, -1)
            )[0][0]
            
            if similarity >= threshold:
                similar_terms.append((term, similarity))
        
        # Sort by similarity
        similar_terms.sort(key=lambda x: x[1], reverse=True)
        
        return similar_terms
    
    def _get_embedding(self, text: str) -> np.ndarray:
        """
        Get embedding for text with caching
        """
        if text in self.embedding_cache:
            return self.embedding_cache[text]
        
        embedding = self.embedder.encode(text)
        self.embedding_cache[text] = embedding
        
        return embedding
    
    def _suggest_from_context(
        self,
        technical_name: str,
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Suggest terms based on context (schema, table, database)
        """
        suggestions = []
        
        # Check schema/database context
        schema = context.get("schema", "")
        database = context.get("database", "")
        
        # Domain-specific suggestions
        if any(term in schema.lower() for term in ["sales", "revenue", "order"]):
            if "amount" in technical_name.lower():
                suggestions.append({
                    "term": "Revenue Amount",
                    "confidence": 0.8,
                    "source": "context",
                    "reason": "Common term in sales domain"
                })
        
        if any(term in schema.lower() for term in ["customer", "client"]):
            if "id" in technical_name.lower():
                suggestions.append({
                    "term": "Customer Identifier",
                    "confidence": 0.85,
                    "source": "context",
                    "reason": "Standard term for customer domain"
                })
        
        return suggestions
    
    async def create_automatic_mappings(
        self,
        dataset_guid: str,
        approval_required: bool = True
    ) -> List[TermMapping]:
        """
        Create automatic mappings between dataset columns and business terms
        """
        mappings = []
        
        try:
            # Get dataset details
            dataset = await self.atlas_client.get_entity(dataset_guid)
            
            # Get schema if available
            schema_str = dataset.get("attributes", {}).get("schema")
            if not schema_str:
                return mappings
            
            schema = json.loads(schema_str)
            
            # Process each column
            for field in schema.get("fields", []):
                column_name = field.get("name")
                column_type = field.get("type")
                
                # Get suggestions for column
                suggestions = await self.suggest_business_terms(
                    column_name,
                    context={
                        "schema": dataset.get("attributes", {}).get("name"),
                        "type": column_type,
                        "dataset": dataset.get("attributes", {}).get("qualifiedName")
                    }
                )
                
                # Create mapping for top suggestion if confidence is high
                if suggestions and suggestions[0]["confidence"] >= 0.7:
                    term = await self._find_or_create_term(suggestions[0]["term"])
                    
                    mapping = TermMapping(
                        term_id=term["guid"],
                        asset_id=f"{dataset_guid}#{column_name}",
                        confidence=suggestions[0]["confidence"],
                        mapping_type="suggested",
                        created_date=datetime.utcnow(),
                        created_by="auto-mapper",
                        approved=not approval_required,
                        approval_date=datetime.utcnow() if not approval_required else None,
                        approved_by="auto" if not approval_required else None
                    )
                    
                    mappings.append(mapping)
                    
                    # Create the mapping in Atlas
                    await self._create_mapping_in_atlas(mapping, dataset_guid, column_name)
            
        except Exception as e:
            logger.error(f"Error creating automatic mappings: {e}")
        
        return mappings
    
    async def _find_or_create_term(self, term_name: str) -> Dict[str, Any]:
        """
        Find existing term or create a suggested one
        """
        # Search for existing term
        search_result = await self.atlas_client.search_entities(
            query=term_name,
            type_name="AtlasGlossaryTerm",
            limit=1
        )
        
        if search_result and search_result.get("entities"):
            return search_result["entities"][0]
        
        # Create suggested term
        term = BusinessTerm(
            name=term_name.lower().replace(" ", "_"),
            display_name=term_name.title(),
            definition=f"Auto-suggested term for {term_name}",
            category=self._guess_category(term_name),
            status=TermStatus.SUGGESTED,
            synonyms=[],
            related_terms=[],
            examples=[],
            owner="auto-mapper",
            steward=None,
            created_by="auto-mapper",
            created_date=datetime.utcnow(),
            modified_date=datetime.utcnow(),
            tags=["auto-suggested"],
            metadata={},
            technical_mappings=[]
        )
        
        # Create in Atlas
        return await self._create_term_in_atlas(term)
    
    def _guess_category(self, term_name: str) -> TermCategory:
        """
        Guess the category of a term based on its name
        """
        term_lower = term_name.lower()
        
        # Metrics
        if any(word in term_lower for word in ["amount", "count", "total", "sum", "average", "rate"]):
            return TermCategory.METRIC
        
        # Dimensions
        if any(word in term_lower for word in ["date", "time", "location", "category", "type", "status"]):
            return TermCategory.DIMENSION
        
        # Entities
        if any(word in term_lower for word in ["customer", "product", "order", "employee", "supplier"]):
            return TermCategory.ENTITY
        
        # Attributes
        if any(word in term_lower for word in ["name", "description", "id", "code", "number"]):
            return TermCategory.ATTRIBUTE
        
        # Processes
        if any(word in term_lower for word in ["process", "workflow", "procedure", "method"]):
            return TermCategory.PROCESS
        
        return TermCategory.OTHER
    
    async def analyze_term_usage(
        self,
        term_guid: str,
        time_range_days: int = 30
    ) -> Dict[str, Any]:
        """
        Analyze how a business term is being used
        """
        try:
            # Get term details
            term = await self.atlas_client.get_entity(term_guid)
            
            # Get all mapped assets
            mapped_assets = await self._get_mapped_assets(term_guid)
            
            # Analyze usage patterns
            usage_stats = {
                "term_name": term.get("attributes", {}).get("name"),
                "total_mappings": len(mapped_assets),
                "mapping_types": defaultdict(int),
                "asset_types": defaultdict(int),
                "layers": defaultdict(int),
                "quality_scores": [],
                "last_accessed": [],
                "popular_queries": []
            }
            
            # Process each mapped asset
            for asset in mapped_assets:
                asset_type = asset.get("typeName", "unknown")
                usage_stats["asset_types"][asset_type] += 1
                
                # Get layer info
                layer = asset.get("attributes", {}).get("layer")
                if layer:
                    usage_stats["layers"][layer] += 1
                
                # Get quality score
                quality_score = asset.get("attributes", {}).get("dataQualityScore")
                if quality_score:
                    usage_stats["quality_scores"].append(quality_score)
            
            # Calculate statistics
            if usage_stats["quality_scores"]:
                usage_stats["avg_quality_score"] = sum(usage_stats["quality_scores"]) / len(usage_stats["quality_scores"])
            else:
                usage_stats["avg_quality_score"] = None
            
            # Get search queries containing this term
            usage_stats["popular_queries"] = await self._get_term_search_queries(
                term.get("attributes", {}).get("name"),
                time_range_days
            )
            
            return usage_stats
            
        except Exception as e:
            logger.error(f"Error analyzing term usage: {e}")
            return {}
    
    async def recommend_new_terms(
        self,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Recommend new business terms based on unmapped technical assets
        """
        recommendations = []
        
        try:
            # Get unmapped columns
            unmapped = await self._get_unmapped_columns()
            
            # Analyze patterns
            term_candidates = defaultdict(list)
            
            for column in unmapped:
                column_name = column.get("name")
                dataset_name = column.get("dataset")
                
                # Extract potential terms
                extracted_terms = self._extract_terms_from_name(column_name)
                
                for term in extracted_terms:
                    if len(term) > 3:  # Skip very short terms
                        term_candidates[term].append({
                            "column": column_name,
                            "dataset": dataset_name
                        })
            
            # Rank candidates
            for term, occurrences in term_candidates.items():
                if len(occurrences) >= 2:  # Term appears in multiple places
                    recommendations.append({
                        "suggested_term": term.title(),
                        "occurrences": len(occurrences),
                        "examples": occurrences[:5],
                        "confidence": min(0.9, len(occurrences) * 0.1),
                        "category": self._guess_category(term)
                    })
            
            # Sort by occurrence count
            recommendations.sort(key=lambda x: x["occurrences"], reverse=True)
            
            return recommendations[:limit]
            
        except Exception as e:
            logger.error(f"Error recommending terms: {e}")
            return []
    
    async def sync_with_business_systems(
        self,
        external_glossary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sync business terms with external business systems
        """
        sync_results = {
            "imported": 0,
            "updated": 0,
            "conflicts": [],
            "errors": []
        }
        
        try:
            for ext_term in external_glossary.get("terms", []):
                try:
                    # Check if term exists
                    existing = await self._find_term_by_name(ext_term["name"])
                    
                    if existing:
                        # Update if needed
                        if self._should_update_term(existing, ext_term):
                            await self._update_term(existing["guid"], ext_term)
                            sync_results["updated"] += 1
                    else:
                        # Import new term
                        await self._import_external_term(ext_term)
                        sync_results["imported"] += 1
                        
                except Exception as e:
                    sync_results["errors"].append({
                        "term": ext_term.get("name"),
                        "error": str(e)
                    })
            
        except Exception as e:
            logger.error(f"Error syncing with business systems: {e}")
            
        return sync_results
    
    async def _get_all_business_terms(self) -> List[BusinessTerm]:
        """
        Get all business terms from catalog
        """
        # This would fetch from Atlas
        # Simplified for illustration
        return []
    
    async def _create_term_in_atlas(self, term: BusinessTerm) -> Dict[str, Any]:
        """
        Create business term in Atlas
        """
        entity = {
            "typeName": "AtlasGlossaryTerm",
            "attributes": {
                "name": term.name,
                "displayName": term.display_name,
                "shortDescription": term.definition,
                "longDescription": term.definition,
                "examples": json.dumps(term.examples),
                "abbreviation": term.name.upper(),
                "usage": term.category.value,
                "additionalAttributes": {
                    "status": term.status.value,
                    "synonyms": term.synonyms,
                    "owner": term.owner,
                    "steward": term.steward
                }
            }
        }
        
        return await self.atlas_client.create_entity(entity)
    
    async def _create_mapping_in_atlas(
        self,
        mapping: TermMapping,
        dataset_guid: str,
        column_name: str
    ):
        """
        Create term mapping in Atlas
        """
        # This would create the relationship in Atlas
        pass
    
    async def _get_mapped_assets(self, term_guid: str) -> List[Dict[str, Any]]:
        """
        Get all assets mapped to a term
        """
        # This would query Atlas for relationships
        return []
    
    async def _get_term_search_queries(
        self,
        term_name: str,
        days: int
    ) -> List[str]:
        """
        Get search queries containing this term
        """
        # This would query search logs
        return []
    
    async def _get_unmapped_columns(self) -> List[Dict[str, Any]]:
        """
        Get columns without business term mappings
        """
        # This would query Atlas for unmapped columns
        return []
    
    async def _find_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find term by name
        """
        # This would search Atlas
        return None
    
    def _should_update_term(
        self,
        existing: Dict[str, Any],
        external: Dict[str, Any]
    ) -> bool:
        """
        Check if term should be updated
        """
        # Compare modification dates or content
        return False
    
    async def _update_term(self, guid: str, updates: Dict[str, Any]):
        """
        Update existing term
        """
        # This would update in Atlas
        pass
    
    async def _import_external_term(self, term: Dict[str, Any]):
        """
        Import term from external system
        """
        # This would create in Atlas
        pass 