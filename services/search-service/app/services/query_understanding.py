"""
Query Understanding Service

Uses NLP to analyze and enhance search queries for better results.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
import re
import spacy
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch

logger = logging.getLogger(__name__)


class QueryUnderstandingService:
    """Analyzes and enhances search queries using NLP"""
    
    def __init__(self):
        # Load spaCy model for NER and linguistic analysis
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except:
            # Download if not available
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
        
        # Intent classification model (using zero-shot for flexibility)
        self.intent_classifier = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli"
        )
        
        # Query type candidates
        self.query_intents = [
            "search_by_name",
            "search_by_description", 
            "find_similar",
            "technical_documentation",
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
            "code_snippet": r'`([^`]+)`'
        }
        
    async def analyze_query(self, query: str) -> Dict[str, Any]:
        """
        Analyze query to understand intent and extract entities
        
        :param query: User's search query
        :return: Analysis results with intent, entities, and suggestions
        """
        analysis = {
            "original_query": query,
            "intent": None,
            "entities": {},
            "enhanced_query": query,
            "filters": {},
            "suggestions": []
        }
        
        # Detect intent
        intent_result = self._detect_intent(query)
        analysis["intent"] = intent_result
        
        # Extract entities
        entities = self._extract_entities(query)
        analysis["entities"] = entities
        
        # Generate filters from entities
        filters = self._entities_to_filters(entities)
        analysis["filters"] = filters
        
        # Enhance query
        enhanced_query = self._enhance_query(query, entities, intent_result)
        analysis["enhanced_query"] = enhanced_query
        
        # Generate search suggestions
        suggestions = self._generate_suggestions(query, entities, intent_result)
        analysis["suggestions"] = suggestions
        
        return analysis
    
    def _detect_intent(self, query: str) -> Dict[str, Any]:
        """Detect the intent of the search query"""
        try:
            # Use zero-shot classification
            result = self.intent_classifier(
                query,
                candidate_labels=self.query_intents,
                hypothesis_template="This text is about {}."
            )
            
            # Get top intent with confidence
            top_intent = result["labels"][0]
            confidence = result["scores"][0]
            
            # Map to specific search strategies
            intent_info = {
                "primary_intent": top_intent,
                "confidence": confidence,
                "search_strategy": self._get_search_strategy(top_intent),
                "all_intents": list(zip(result["labels"], result["scores"]))
            }
            
            return intent_info
            
        except Exception as e:
            logger.error(f"Intent detection failed: {e}")
            return {
                "primary_intent": "general_search",
                "confidence": 0.5,
                "search_strategy": "hybrid"
            }
    
    def _extract_entities(self, query: str) -> Dict[str, List[Any]]:
        """Extract named entities and patterns from query"""
        entities = {
            "keywords": [],
            "file_types": [],
            "languages": [],
            "dates": [],
            "users": [],
            "tags": [],
            "phrases": [],
            "code": [],
            "named_entities": []
        }
        
        # Use spaCy for NER
        doc = self.nlp(query)
        
        # Extract named entities
        for ent in doc.ents:
            entities["named_entities"].append({
                "text": ent.text,
                "label": ent.label_,
                "start": ent.start_char,
                "end": ent.end_char
            })
        
        # Extract keywords (important tokens)
        for token in doc:
            if not token.is_stop and not token.is_punct and len(token.text) > 2:
                if token.pos_ in ["NOUN", "PROPN", "VERB"]:
                    entities["keywords"].append({
                        "text": token.text,
                        "lemma": token.lemma_,
                        "pos": token.pos_
                    })
        
        # Extract patterns using regex
        for pattern_type, pattern in self.entity_patterns.items():
            matches = re.findall(pattern, query, re.IGNORECASE)
            if matches:
                if pattern_type == "file_type":
                    entities["file_types"].extend(matches)
                elif pattern_type == "programming_language":
                    entities["languages"].extend(matches)
                elif pattern_type == "user_mention":
                    entities["users"].extend(matches)
                elif pattern_type == "tag_mention":
                    entities["tags"].extend(matches)
                elif pattern_type == "quoted_phrase":
                    entities["phrases"].extend(matches)
                elif pattern_type == "code_snippet":
                    entities["code"].extend(matches)
        
        # Extract date ranges
        date_matches = re.findall(self.entity_patterns["date_range"], query, re.IGNORECASE)
        for match in date_matches:
            entities["dates"].append({
                "text": f"{match[0]} {match[1]} {match[2]}",
                "quantity": int(match[1]),
                "unit": match[2]
            })
        
        return entities
    
    def _entities_to_filters(self, entities: Dict[str, List[Any]]) -> Dict[str, Any]:
        """Convert extracted entities to search filters"""
        filters = {}
        
        # File type filters
        if entities["file_types"]:
            filters["file_extension"] = entities["file_types"]
        
        # Language filters
        if entities["languages"]:
            filters["programming_language"] = entities["languages"]
        
        # User filters
        if entities["users"]:
            filters["created_by"] = entities["users"]
        
        # Tag filters
        if entities["tags"]:
            filters["tags"] = entities["tags"]
        
        # Date filters
        if entities["dates"]:
            # Convert relative dates to actual dates
            from datetime import datetime, timedelta
            
            for date_entity in entities["dates"]:
                quantity = date_entity["quantity"]
                unit = date_entity["unit"].rstrip("s")  # Remove plural
                
                if unit == "day":
                    delta = timedelta(days=quantity)
                elif unit == "week":
                    delta = timedelta(weeks=quantity)
                elif unit == "month":
                    delta = timedelta(days=quantity * 30)  # Approximate
                elif unit == "year":
                    delta = timedelta(days=quantity * 365)  # Approximate
                
                start_date = datetime.utcnow() - delta
                filters["created_at"] = {"gte": start_date.isoformat()}
        
        return filters
    
    def _enhance_query(self, query: str, entities: Dict[str, List[Any]], 
                       intent: Dict[str, Any]) -> str:
        """Enhance query based on understanding"""
        enhanced = query
        
        # Remove filter-related parts that are now in filters
        # Remove date ranges
        for date_entity in entities["dates"]:
            enhanced = enhanced.replace(date_entity["text"], "")
        
        # Remove file type mentions if they're in filters
        for file_type in entities["file_types"]:
            pattern = r'\b' + re.escape(file_type) + r'\b'
            enhanced = re.sub(pattern, "", enhanced, flags=re.IGNORECASE)
        
        # Expand abbreviations based on context
        abbreviations = {
            "ml": "machine learning",
            "ai": "artificial intelligence",
            "ui": "user interface",
            "ux": "user experience",
            "api": "application programming interface",
            "db": "database",
            "auth": "authentication authorization"
        }
        
        for abbr, expansion in abbreviations.items():
            if re.search(r'\b' + abbr + r'\b', enhanced, re.IGNORECASE):
                # Add expansion as synonym
                enhanced = f"{enhanced} {expansion}"
        
        # Add synonyms based on intent
        if intent["primary_intent"] == "technical_documentation":
            enhanced = f"{enhanced} documentation docs guide tutorial"
        elif intent["primary_intent"] == "code_search":
            enhanced = f"{enhanced} code implementation example snippet"
        
        # Clean up extra spaces
        enhanced = " ".join(enhanced.split())
        
        return enhanced
    
    def _generate_suggestions(self, query: str, entities: Dict[str, List[Any]], 
                              intent: Dict[str, Any]) -> List[Dict[str, str]]:
        """Generate search suggestions based on query understanding"""
        suggestions = []
        
        # Suggest filters based on entities
        if entities["file_types"]:
            suggestions.append({
                "type": "filter",
                "text": f"Filter by file type: {', '.join(entities['file_types'])}",
                "action": {"add_filter": {"file_extension": entities["file_types"]}}
            })
        
        # Suggest related searches based on intent
        if intent["primary_intent"] == "find_similar":
            suggestions.append({
                "type": "search_mode",
                "text": "Use similarity search for better results",
                "action": {"search_type": "vector"}
            })
        elif intent["primary_intent"] == "code_search":
            suggestions.append({
                "type": "collection",
                "text": "Search in code repository",
                "action": {"collection": "code_embeddings"}
            })
        
        # Suggest query refinements
        if len(query.split()) < 3:
            suggestions.append({
                "type": "refinement",
                "text": "Try adding more descriptive terms",
                "action": {"tip": "descriptive_query"}
            })
        
        # Suggest advanced search features
        if not entities["phrases"] and len(query.split()) > 5:
            suggestions.append({
                "type": "syntax",
                "text": "Use quotes for exact phrase matching",
                "action": {"tip": "phrase_search"}
            })
        
        return suggestions
    
    def _get_search_strategy(self, intent: str) -> str:
        """Map intent to search strategy"""
        strategy_map = {
            "search_by_name": "text",
            "search_by_description": "hybrid",
            "find_similar": "vector",
            "technical_documentation": "hybrid",
            "code_search": "code",
            "image_search": "image",
            "navigational": "text",
            "analytical_query": "hybrid"
        }
        
        return strategy_map.get(intent, "hybrid")
    
    def correct_spelling(self, query: str) -> Tuple[str, List[str]]:
        """Correct spelling errors in query"""
        # Simple implementation - in production use a proper spell checker
        doc = self.nlp(query)
        corrections = []
        
        # This is a placeholder - implement actual spell checking
        corrected = query
        
        return corrected, corrections
    
    def extract_query_context(self, query: str, user_history: List[str]) -> Dict[str, Any]:
        """Extract context from query and user history"""
        context = {
            "is_followup": False,
            "related_queries": [],
            "session_intent": None
        }
        
        # Check if query references previous searches
        if any(word in query.lower() for word in ["more", "other", "similar", "like"]):
            context["is_followup"] = True
        
        # Analyze user history for session intent
        if user_history:
            # Find common themes in recent queries
            all_keywords = []
            for hist_query in user_history[-5:]:  # Last 5 queries
                doc = self.nlp(hist_query)
                keywords = [token.lemma_ for token in doc 
                           if not token.is_stop and token.pos_ in ["NOUN", "PROPN"]]
                all_keywords.extend(keywords)
            
            # Find most common keywords
            from collections import Counter
            keyword_counts = Counter(all_keywords)
            if keyword_counts:
                context["session_intent"] = keyword_counts.most_common(1)[0][0]
        
        return context 