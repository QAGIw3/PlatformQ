"""
Consolidated Query Analyzer

Merges query understanding and intent classification into a single service.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
import re
import asyncio
from datetime import datetime, timedelta

import spacy
from transformers import pipeline
import torch

from ..interfaces import QueryAnalyzer
from ..storage.ignite_cache_adapter import IgniteCacheAdapter

logger = logging.getLogger(__name__)


class UnifiedQueryAnalyzer(QueryAnalyzer):
    """
    Unified query analysis combining intent classification,
    entity extraction, and query enhancement.
    """
    
    def __init__(self, cache_adapter: Optional[IgniteCacheAdapter] = None):
        self.cache_adapter = cache_adapter
        self.nlp = None
        self.intent_classifier = None
        self.entity_classifier = None
        self._initialized = False
        
        # Query intents
        self.query_intents = [
            "find_specific_item",
            "explore_category",
            "compare_items",
            "get_recommendations",
            "find_similar",
            "technical_search",
            "transactional_search",
            "informational_search",
            "code_search",
            "image_search",
            "navigational",
            "analytical_query"
        ]
        
        # Entity patterns
        self.entity_patterns = {
            "file_type": r'\b(pdf|doc|docx|txt|jpg|png|gif|mp4|avi|stl|obj|fbx|blend)\b',
            "programming_language": r'\b(python|javascript|java|cpp|c\+\+|csharp|c#|go|rust|typescript)\b',
            "date_range": r'(last|past)\s+(\d+)\s+(days?|weeks?|months?|years?)',
            "user_mention": r'@(\w+)',
            "tag_mention": r'#(\w+)',
            "quoted_phrase": r'"([^"]+)"',
            "code_snippet": r'`([^`]+)`',
            "version": r'v?\d+\.\d+(?:\.\d+)?',
            "uuid": r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        }
        
        # Abbreviation expansions
        self.abbreviations = {
            "ml": "machine learning",
            "ai": "artificial intelligence",
            "ui": "user interface",
            "ux": "user experience",
            "api": "application programming interface",
            "db": "database",
            "auth": "authentication authorization",
            "3d": "three dimensional",
            "cad": "computer aided design",
            "sdk": "software development kit",
            "ci": "continuous integration",
            "cd": "continuous deployment"
        }
        
    async def initialize(self):
        """Initialize NLP models"""
        if self._initialized:
            return
            
        try:
            # Load spaCy model
            try:
                self.nlp = spacy.load("en_core_web_sm")
            except:
                import subprocess
                subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
                self.nlp = spacy.load("en_core_web_sm")
            
            # Intent classification pipeline
            self.intent_classifier = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=-1  # CPU
            )
            
            # Entity classification pipeline
            self.entity_classifier = pipeline(
                "token-classification",
                model="dbmdz/bert-large-cased-finetuned-conll03-english",
                aggregation_strategy="simple",
                device=-1
            )
            
            self._initialized = True
            logger.info("Query analyzer initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize query analyzer: {e}")
            raise
    
    async def analyze(
        self,
        query: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Comprehensive query analysis
        
        Returns:
            - original_query: Original query text
            - normalized_query: Normalized version
            - intent: Intent classification results
            - entities: Extracted entities
            - filters: Generated search filters
            - enhanced_query: Enhanced query for better search
            - suggestions: Search suggestions
            - context: Query context (session, user history)
        """
        if not self._initialized:
            await self.initialize()
            
        # Check cache first
        cache_key = f"query_analysis:{query}"
        if self.cache_adapter:
            cached = await self.cache_adapter.get(cache_key)
            if cached:
                return cached
                
        # Perform analysis
        analysis = {
            "original_query": query,
            "normalized_query": self._normalize_query(query),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Run analysis tasks in parallel
        intent_task = asyncio.create_task(self._analyze_intent(query))
        entities_task = asyncio.create_task(self._extract_entities(query))
        context_task = asyncio.create_task(self._analyze_context(query, context))
        
        # Wait for all tasks
        intent_result, entities, query_context = await asyncio.gather(
            intent_task, entities_task, context_task
        )
        
        # Add results
        analysis["intent"] = intent_result
        analysis["entities"] = entities
        analysis["context"] = query_context
        
        # Generate filters from entities
        analysis["filters"] = self._entities_to_filters(entities)
        
        # Enhance query
        analysis["enhanced_query"] = self._enhance_query(
            query, entities, intent_result
        )
        
        # Generate suggestions
        analysis["suggestions"] = self._generate_suggestions(
            query, entities, intent_result, query_context
        )
        
        # Cache result
        if self.cache_adapter:
            await self.cache_adapter.set(cache_key, analysis, ttl=300)  # 5 min cache
            
        return analysis
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query text"""
        # Convert to lowercase
        normalized = query.lower()
        
        # Remove extra whitespace
        normalized = " ".join(normalized.split())
        
        # Remove special characters except useful ones
        normalized = re.sub(r'[^\w\s\-\.\@\#\"\'`]', ' ', normalized)
        
        return normalized.strip()
    
    async def _analyze_intent(self, query: str) -> Dict[str, Any]:
        """Analyze query intent using zero-shot classification"""
        try:
            result = await asyncio.to_thread(
                self.intent_classifier,
                query,
                candidate_labels=self.query_intents,
                multi_label=True
            )
            
            # Get primary intent and confidence
            primary_intent = result["labels"][0]
            confidence = result["scores"][0]
            
            # Map to search strategy
            search_strategy = self._get_search_strategy(primary_intent)
            
            return {
                "primary_intent": primary_intent,
                "confidence": confidence,
                "search_strategy": search_strategy,
                "all_intents": dict(zip(result["labels"], result["scores"])),
                "multi_intent": len([s for s in result["scores"] if s > 0.3]) > 1
            }
            
        except Exception as e:
            logger.error(f"Intent analysis failed: {e}")
            return {
                "primary_intent": "general_search",
                "confidence": 0.5,
                "search_strategy": "hybrid",
                "error": str(e)
            }
    
    async def _extract_entities(self, query: str) -> Dict[str, List[Any]]:
        """Extract entities from query"""
        entities = {
            "keywords": [],
            "named_entities": [],
            "file_types": [],
            "languages": [],
            "dates": [],
            "users": [],
            "tags": [],
            "phrases": [],
            "code": [],
            "versions": [],
            "uuids": []
        }
        
        # Use spaCy for linguistic analysis
        doc = self.nlp(query)
        
        # Extract keywords
        for token in doc:
            if not token.is_stop and not token.is_punct and len(token.text) > 2:
                if token.pos_ in ["NOUN", "PROPN", "VERB"]:
                    entities["keywords"].append({
                        "text": token.text,
                        "lemma": token.lemma_,
                        "pos": token.pos_
                    })
        
        # Extract named entities using transformer model
        try:
            ner_results = await asyncio.to_thread(
                self.entity_classifier,
                query
            )
            
            for entity in ner_results:
                entities["named_entities"].append({
                    "text": entity["word"],
                    "type": entity["entity_group"],
                    "score": entity["score"],
                    "start": entity["start"],
                    "end": entity["end"]
                })
        except Exception as e:
            logger.warning(f"NER extraction failed: {e}")
        
        # Extract patterns using regex
        for pattern_type, pattern in self.entity_patterns.items():
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if pattern_type == "date_range":
                    for match in matches:
                        entities["dates"].append({
                            "text": f"{match[0]} {match[1]} {match[2]}",
                            "quantity": int(match[1]),
                            "unit": match[2].rstrip("s")
                        })
                else:
                    # Map pattern types to entity keys
                    entity_key = {
                        "file_type": "file_types",
                        "programming_language": "languages",
                        "user_mention": "users",
                        "tag_mention": "tags",
                        "quoted_phrase": "phrases",
                        "code_snippet": "code",
                        "version": "versions",
                        "uuid": "uuids"
                    }.get(pattern_type, pattern_type)
                    
                    entities[entity_key].extend(matches)
        
        return entities
    
    def _entities_to_filters(self, entities: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Convert entities to search filters"""
        filters = {}
        
        # File type filters
        if entities["file_types"]:
            filters["file_extension"] = list(set(entities["file_types"]))
        
        # Language filters
        if entities["languages"]:
            filters["programming_language"] = list(set(entities["languages"]))
        
        # User filters
        if entities["users"]:
            filters["created_by"] = list(set(entities["users"]))
        
        # Tag filters
        if entities["tags"]:
            filters["tags"] = list(set(entities["tags"]))
        
        # Date filters
        if entities["dates"]:
            for date_entity in entities["dates"]:
                quantity = date_entity["quantity"]
                unit = date_entity["unit"]
                
                # Calculate date range
                if unit == "day":
                    delta = timedelta(days=quantity)
                elif unit == "week":
                    delta = timedelta(weeks=quantity)
                elif unit == "month":
                    delta = timedelta(days=quantity * 30)
                elif unit == "year":
                    delta = timedelta(days=quantity * 365)
                else:
                    continue
                
                start_date = datetime.utcnow() - delta
                filters["created_at"] = {"gte": start_date.isoformat()}
        
        # Version filters
        if entities["versions"]:
            filters["version"] = list(set(entities["versions"]))
        
        return filters
    
    def _enhance_query(
        self,
        query: str,
        entities: Dict[str, List[Any]],
        intent: Dict[str, Any]
    ) -> str:
        """Enhance query for better search results"""
        enhanced = query
        
        # Remove filter-related parts
        for date in entities["dates"]:
            enhanced = enhanced.replace(date["text"], "")
        
        for file_type in entities["file_types"]:
            pattern = r'\b' + re.escape(file_type) + r'\b'
            enhanced = re.sub(pattern, "", enhanced, flags=re.IGNORECASE)
        
        # Expand abbreviations
        for abbr, expansion in self.abbreviations.items():
            if re.search(r'\b' + abbr + r'\b', enhanced, re.IGNORECASE):
                enhanced = f"{enhanced} {expansion}"
        
        # Add context based on intent
        intent_enhancements = {
            "technical_search": "documentation docs guide tutorial reference",
            "code_search": "code implementation example snippet function class",
            "find_similar": "similar related like comparable",
            "get_recommendations": "recommended suggest best top rated"
        }
        
        if intent["primary_intent"] in intent_enhancements:
            enhanced = f"{enhanced} {intent_enhancements[intent['primary_intent']]}"
        
        # Clean up
        enhanced = " ".join(enhanced.split())
        
        return enhanced
    
    async def _analyze_context(
        self,
        query: str,
        context: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Analyze query context"""
        query_context = {
            "is_followup": False,
            "session_queries": [],
            "related_terms": [],
            "user_preferences": {}
        }
        
        if not context:
            return query_context
        
        # Check if it's a follow-up query
        followup_indicators = ["more", "other", "similar", "else", "also", "another"]
        query_lower = query.lower()
        query_context["is_followup"] = any(
            indicator in query_lower for indicator in followup_indicators
        )
        
        # Analyze session history
        if "session_history" in context:
            query_context["session_queries"] = context["session_history"][-5:]
            
            # Extract common terms from session
            all_terms = []
            for hist_query in query_context["session_queries"]:
                doc = self.nlp(hist_query)
                terms = [
                    token.lemma_ for token in doc
                    if not token.is_stop and token.pos_ in ["NOUN", "PROPN"]
                ]
                all_terms.extend(terms)
            
            # Find most common terms
            from collections import Counter
            term_counts = Counter(all_terms)
            query_context["related_terms"] = [
                term for term, _ in term_counts.most_common(5)
            ]
        
        # User preferences
        if "user_preferences" in context:
            query_context["user_preferences"] = context["user_preferences"]
        
        return query_context
    
    def _generate_suggestions(
        self,
        query: str,
        entities: Dict[str, List[Any]],
        intent: Dict[str, Any],
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Generate search suggestions"""
        suggestions = []
        
        # Filter suggestions
        if entities["file_types"]:
            suggestions.append({
                "type": "filter",
                "text": f"Filter by file type: {', '.join(entities['file_types'])}",
                "action": {"add_filter": {"file_extension": entities["file_types"]}}
            })
        
        if entities["languages"]:
            suggestions.append({
                "type": "filter",
                "text": f"Filter by language: {', '.join(entities['languages'])}",
                "action": {"add_filter": {"programming_language": entities["languages"]}}
            })
        
        # Strategy suggestions
        strategy_suggestions = {
            "find_similar": {
                "type": "search_mode",
                "text": "Use similarity search for better results",
                "action": {"search_type": "vector"}
            },
            "code_search": {
                "type": "collection",
                "text": "Search in code repository",
                "action": {"collection": "code_embeddings"}
            },
            "technical_search": {
                "type": "boost",
                "text": "Boost technical documentation",
                "action": {"boost": {"document_type": "technical"}}
            }
        }
        
        if intent["primary_intent"] in strategy_suggestions:
            suggestions.append(strategy_suggestions[intent["primary_intent"]])
        
        # Query refinement suggestions
        if len(query.split()) < 3:
            suggestions.append({
                "type": "refinement",
                "text": "Try adding more descriptive terms",
                "action": {"tip": "descriptive_query"}
            })
        
        # Context-based suggestions
        if context.get("is_followup") and context.get("related_terms"):
            suggestions.append({
                "type": "context",
                "text": f"Include related terms: {', '.join(context['related_terms'][:3])}",
                "action": {"expand_query": context["related_terms"][:3]}
            })
        
        return suggestions[:5]  # Limit to 5 suggestions
    
    def _get_search_strategy(self, intent: str) -> str:
        """Map intent to search strategy"""
        strategy_map = {
            "find_specific_item": "exact",
            "explore_category": "faceted",
            "compare_items": "comparative",
            "get_recommendations": "recommendation",
            "find_similar": "vector",
            "technical_search": "hybrid",
            "transactional_search": "transactional",
            "informational_search": "hybrid",
            "code_search": "code",
            "image_search": "image",
            "navigational": "exact",
            "analytical_query": "analytical"
        }
        
        return strategy_map.get(intent, "hybrid") 