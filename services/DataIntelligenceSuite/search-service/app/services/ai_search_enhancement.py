"""
AI-Powered Search Enhancement Service

Provides semantic search, query understanding, personalization,
and auto-categorization using advanced AI models.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import numpy as np
from collections import defaultdict
import json
import hashlib

from sentence_transformers import SentenceTransformer, CrossEncoder
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import openai
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
import pandas as pd

from elasticsearch import AsyncElasticsearch
from app.core.config import settings
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class QueryIntentClassifier:
    """Classifies user query intent for better search routing"""
    
    def __init__(self):
        self.intent_pipeline = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli"
        )
        
        self.search_intents = [
            "find_specific_item",
            "explore_category",
            "compare_items",
            "get_recommendations",
            "find_similar",
            "technical_search",
            "transactional_search",
            "informational_search"
        ]
        
        self.entity_classifier = pipeline(
            "token-classification",
            model="dbmdz/bert-large-cased-finetuned-conll03-english"
        )
    
    async def classify_intent(self, query: str) -> Dict[str, Any]:
        """Classify query intent and extract entities"""
        try:
            # Classify intent
            intent_result = await asyncio.to_thread(
                self.intent_pipeline,
                query,
                candidate_labels=self.search_intents,
                multi_label=True
            )
            
            # Extract entities
            entities = await asyncio.to_thread(
                self.entity_classifier,
                query
            )
            
            # Parse entities
            extracted_entities = self._parse_entities(entities)
            
            # Analyze query structure
            query_analysis = self._analyze_query_structure(query)
            
            return {
                "query": query,
                "primary_intent": intent_result["labels"][0],
                "intent_scores": dict(zip(intent_result["labels"], intent_result["scores"])),
                "entities": extracted_entities,
                "query_type": query_analysis["type"],
                "has_filters": query_analysis["has_filters"],
                "temporal_reference": query_analysis["temporal_reference"]
            }
            
        except Exception as e:
            logger.error(f"Error classifying intent: {e}")
            return {
                "query": query,
                "primary_intent": "informational_search",
                "intent_scores": {},
                "entities": [],
                "query_type": "general"
            }
    
    def _parse_entities(self, entities: List[Dict]) -> List[Dict[str, str]]:
        """Parse NER results"""
        parsed = []
        current_entity = None
        
        for token in entities:
            if token["entity"].startswith("B-"):
                if current_entity:
                    parsed.append(current_entity)
                current_entity = {
                    "text": token["word"],
                    "type": token["entity"][2:],
                    "score": token["score"]
                }
            elif token["entity"].startswith("I-") and current_entity:
                current_entity["text"] += " " + token["word"]
                current_entity["score"] = (current_entity["score"] + token["score"]) / 2
        
        if current_entity:
            parsed.append(current_entity)
        
        return parsed
    
    def _analyze_query_structure(self, query: str) -> Dict[str, Any]:
        """Analyze query structure and patterns"""
        query_lower = query.lower()
        
        # Check for filters
        has_filters = any(
            keyword in query_lower
            for keyword in ["with", "having", "where", "filter", "only"]
        )
        
        # Check for temporal references
        temporal_keywords = ["today", "yesterday", "last week", "recent", "latest", "new"]
        temporal_reference = next(
            (kw for kw in temporal_keywords if kw in query_lower),
            None
        )
        
        # Determine query type
        if "?" in query:
            query_type = "question"
        elif any(word in query_lower for word in ["compare", "vs", "versus", "difference"]):
            query_type = "comparison"
        elif any(word in query_lower for word in ["similar", "like", "related"]):
            query_type = "similarity"
        else:
            query_type = "general"
        
        return {
            "type": query_type,
            "has_filters": has_filters,
            "temporal_reference": temporal_reference
        }


class SemanticSearchEnhancer:
    """Enhances search with semantic understanding"""
    
    def __init__(self):
        # Sentence embedder for semantic search
        self.embedder = SentenceTransformer('all-mpnet-base-v2')
        
        # Cross-encoder for re-ranking
        self.reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
        
        # Cache for embeddings
        self.embedding_cache = {}
        self.cache_ttl = 3600  # 1 hour
    
    async def enhance_query(self, query: str) -> Dict[str, Any]:
        """Enhance query with semantic expansion"""
        try:
            # Generate query embedding
            query_embedding = await self._get_embedding(query)
            
            # Generate query variations
            variations = await self._generate_query_variations(query)
            
            # Get embeddings for variations
            variation_embeddings = []
            for var in variations:
                emb = await self._get_embedding(var)
                variation_embeddings.append(emb)
            
            # Find semantic keywords
            semantic_keywords = await self._extract_semantic_keywords(query)
            
            return {
                "original_query": query,
                "query_embedding": query_embedding.tolist(),
                "variations": variations,
                "variation_embeddings": [emb.tolist() for emb in variation_embeddings],
                "semantic_keywords": semantic_keywords,
                "enhanced_query": self._build_enhanced_query(query, semantic_keywords)
            }
            
        except Exception as e:
            logger.error(f"Error enhancing query: {e}")
            return {
                "original_query": query,
                "query_embedding": None,
                "variations": [query],
                "semantic_keywords": [],
                "enhanced_query": query
            }
    
    async def _get_embedding(self, text: str) -> np.ndarray:
        """Get embedding with caching"""
        cache_key = hashlib.md5(text.encode()).hexdigest()
        
        if cache_key in self.embedding_cache:
            cached = self.embedding_cache[cache_key]
            if (datetime.utcnow() - cached["timestamp"]).seconds < self.cache_ttl:
                return cached["embedding"]
        
        # Generate embedding
        embedding = await asyncio.to_thread(self.embedder.encode, text)
        
        # Cache it
        self.embedding_cache[cache_key] = {
            "embedding": embedding,
            "timestamp": datetime.utcnow()
        }
        
        return embedding
    
    async def _generate_query_variations(self, query: str) -> List[str]:
        """Generate semantic variations of the query"""
        variations = [query]
        
        # Simple variations
        # Remove stop words
        stop_words = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "with"}
        words = query.lower().split()
        filtered = " ".join(w for w in words if w not in stop_words)
        if filtered != query.lower():
            variations.append(filtered)
        
        # Add synonyms (simplified - in production use WordNet or similar)
        synonym_map = {
            "find": ["search", "locate", "discover"],
            "show": ["display", "list", "present"],
            "get": ["retrieve", "fetch", "obtain"],
            "latest": ["recent", "newest", "current"],
            "best": ["top", "highest rated", "premium"]
        }
        
        for word, synonyms in synonym_map.items():
            if word in query.lower():
                for syn in synonyms:
                    variations.append(query.lower().replace(word, syn))
        
        return list(set(variations))[:5]  # Limit to 5 variations
    
    async def _extract_semantic_keywords(self, query: str) -> List[str]:
        """Extract semantically relevant keywords"""
        # In production, use a more sophisticated keyword extraction
        # For now, use simple approach
        words = query.lower().split()
        
        # Filter out common words
        stop_words = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "is", "are", "was", "were"}
        keywords = [w for w in words if w not in stop_words and len(w) > 2]
        
        return keywords
    
    def _build_enhanced_query(self, original: str, keywords: List[str]) -> str:
        """Build enhanced query string"""
        # Combine original with extracted keywords
        enhanced_parts = [original]
        
        # Add keywords not already in query
        original_lower = original.lower()
        for kw in keywords:
            if kw not in original_lower:
                enhanced_parts.append(kw)
        
        return " ".join(enhanced_parts)
    
    async def rerank_results(
        self,
        query: str,
        results: List[Dict[str, Any]],
        top_k: int = 10
    ) -> List[Dict[str, Any]]:
        """Re-rank results using cross-encoder"""
        try:
            if not results:
                return results
            
            # Prepare pairs for reranking
            pairs = []
            for result in results:
                text = f"{result.get('title', '')} {result.get('description', '')}"
                pairs.append([query, text])
            
            # Get reranking scores
            scores = await asyncio.to_thread(self.reranker.predict, pairs)
            
            # Add scores to results
            for i, result in enumerate(results):
                result["rerank_score"] = float(scores[i])
            
            # Sort by rerank score
            reranked = sorted(results, key=lambda x: x["rerank_score"], reverse=True)
            
            return reranked[:top_k]
            
        except Exception as e:
            logger.error(f"Error reranking results: {e}")
            return results[:top_k]


class PersonalizationEngine:
    """Personalizes search results based on user behavior"""
    
    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis_client = redis_client
        self.user_profile_ttl = 30 * 24 * 3600  # 30 days
        
    async def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """Get user search profile"""
        if not self.redis_client:
            return {}
        
        try:
            profile_key = f"search_profile:{user_id}"
            profile_data = await self.redis_client.get(profile_key)
            
            if profile_data:
                return json.loads(profile_data)
            
            return {
                "user_id": user_id,
                "search_history": [],
                "clicked_items": [],
                "preferences": {},
                "interests": []
            }
            
        except Exception as e:
            logger.error(f"Error getting user profile: {e}")
            return {}
    
    async def update_user_profile(
        self,
        user_id: str,
        action: str,
        data: Dict[str, Any]
    ):
        """Update user profile with new action"""
        if not self.redis_client:
            return
        
        try:
            profile = await self.get_user_profile(user_id)
            
            if action == "search":
                profile["search_history"].append({
                    "query": data["query"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "result_count": data.get("result_count", 0)
                })
                # Keep only last 100 searches
                profile["search_history"] = profile["search_history"][-100:]
                
            elif action == "click":
                profile["clicked_items"].append({
                    "item_id": data["item_id"],
                    "item_type": data.get("item_type"),
                    "timestamp": datetime.utcnow().isoformat(),
                    "position": data.get("position", 0)
                })
                # Keep only last 200 clicks
                profile["clicked_items"] = profile["clicked_items"][-200:]
                
            # Update interests based on clicked items
            profile["interests"] = await self._extract_interests(profile)
            
            # Save profile
            profile_key = f"search_profile:{user_id}"
            await self.redis_client.setex(
                profile_key,
                self.user_profile_ttl,
                json.dumps(profile)
            )
            
        except Exception as e:
            logger.error(f"Error updating user profile: {e}")
    
    async def _extract_interests(self, profile: Dict[str, Any]) -> List[str]:
        """Extract user interests from behavior"""
        # Count clicked item types
        type_counts = defaultdict(int)
        for click in profile["clicked_items"]:
            if "item_type" in click:
                type_counts[click["item_type"]] += 1
        
        # Get top interests
        interests = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
        return [interest[0] for interest in interests[:10]]
    
    async def personalize_results(
        self,
        user_id: str,
        results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Personalize search results for user"""
        try:
            if not user_id:
                return results
            
            profile = await self.get_user_profile(user_id)
            if not profile or not profile.get("interests"):
                return results
            
            # Boost results matching user interests
            for result in results:
                boost = 1.0
                
                # Check if result type matches interests
                result_type = result.get("entity_type", "")
                if result_type in profile["interests"]:
                    interest_rank = profile["interests"].index(result_type)
                    boost += (10 - interest_rank) * 0.1  # Up to 100% boost
                
                # Check if user clicked similar items
                for click in profile["clicked_items"][-20:]:  # Last 20 clicks
                    if click.get("item_type") == result_type:
                        boost += 0.05  # 5% boost per recent click
                
                result["personalization_boost"] = boost
                result["personalized_score"] = result.get("score", 1.0) * boost
            
            # Re-sort by personalized score
            personalized = sorted(
                results,
                key=lambda x: x.get("personalized_score", x.get("score", 0)),
                reverse=True
            )
            
            return personalized
            
        except Exception as e:
            logger.error(f"Error personalizing results: {e}")
            return results


class AutoCategorizer:
    """Automatically categorizes and tags content"""
    
    def __init__(self):
        self.classifier = pipeline(
            "zero-shot-classification",
            model="facebook/bart-large-mnli"
        )
        
        # Predefined categories
        self.categories = {
            "technical": ["documentation", "api", "code", "tutorial", "guide"],
            "business": ["proposal", "contract", "report", "analysis", "strategy"],
            "creative": ["design", "artwork", "media", "content", "portfolio"],
            "data": ["dataset", "analytics", "metrics", "statistics", "insights"],
            "communication": ["message", "announcement", "discussion", "feedback"],
            "transaction": ["order", "payment", "invoice", "receipt", "purchase"]
        }
    
    async def categorize_content(
        self,
        content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Categorize content and generate tags"""
        try:
            # Prepare text for classification
            text = f"{content.get('title', '')} {content.get('description', '')} {content.get('content', '')}"[:500]
            
            # Classify into main categories
            category_labels = list(self.categories.keys())
            category_result = await asyncio.to_thread(
                self.classifier,
                text,
                candidate_labels=category_labels,
                multi_label=True
            )
            
            # Get top categories
            top_categories = []
            for label, score in zip(category_result["labels"], category_result["scores"]):
                if score > 0.3:  # Threshold
                    top_categories.append({
                        "category": label,
                        "confidence": score
                    })
            
            # Generate tags based on categories
            tags = set()
            for cat in top_categories:
                tags.update(self.categories[cat["category"]])
            
            # Extract additional tags from content
            extracted_tags = await self._extract_tags(text)
            tags.update(extracted_tags)
            
            return {
                "categories": top_categories[:3],  # Top 3 categories
                "tags": list(tags)[:10],  # Top 10 tags
                "primary_category": top_categories[0]["category"] if top_categories else "uncategorized"
            }
            
        except Exception as e:
            logger.error(f"Error categorizing content: {e}")
            return {
                "categories": [],
                "tags": [],
                "primary_category": "uncategorized"
            }
    
    async def _extract_tags(self, text: str) -> List[str]:
        """Extract tags from text"""
        # Simple keyword extraction
        # In production, use TF-IDF or similar
        words = text.lower().split()
        
        # Filter
        stop_words = {"the", "a", "an", "in", "on", "at", "to", "for", "of", "with", "is", "are", "was", "were"}
        min_length = 4
        
        tags = []
        for word in words:
            if (
                word not in stop_words and
                len(word) >= min_length and
                word.isalnum()
            ):
                tags.append(word)
        
        # Count frequency
        tag_counts = defaultdict(int)
        for tag in tags:
            tag_counts[tag] += 1
        
        # Get top tags by frequency
        sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
        return [tag[0] for tag in sorted_tags[:20]]


class AISearchOrchestrator:
    """Orchestrates all AI-powered search features"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        redis_client: Optional[redis.Redis] = None,
        openai_api_key: Optional[str] = None
    ):
        self.es_client = es_client
        self.redis_client = redis_client
        
        # Initialize components
        self.intent_classifier = QueryIntentClassifier()
        self.semantic_enhancer = SemanticSearchEnhancer()
        self.personalization = PersonalizationEngine(redis_client)
        self.categorizer = AutoCategorizer()
        
        # OpenAI for advanced features
        if openai_api_key:
            openai.api_key = openai_api_key
            self.use_openai = True
        else:
            self.use_openai = False
    
    async def process_search_query(
        self,
        query: str,
        user_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Process search query with all AI enhancements"""
        try:
            # 1. Classify intent
            intent_analysis = await self.intent_classifier.classify_intent(query)
            
            # 2. Enhance query semantically
            query_enhancement = await self.semantic_enhancer.enhance_query(query)
            
            # 3. Generate search explanation
            if self.use_openai:
                explanation = await self._generate_search_explanation(
                    query,
                    intent_analysis,
                    query_enhancement
                )
            else:
                explanation = f"Searching for: {query}"
            
            # 4. Get user profile for personalization
            user_profile = None
            if user_id:
                user_profile = await self.personalization.get_user_profile(user_id)
            
            return {
                "original_query": query,
                "intent_analysis": intent_analysis,
                "query_enhancement": query_enhancement,
                "explanation": explanation,
                "user_profile": user_profile,
                "search_config": self._generate_search_config(
                    intent_analysis,
                    query_enhancement,
                    user_profile
                )
            }
            
        except Exception as e:
            logger.error(f"Error processing search query: {e}")
            return {
                "original_query": query,
                "error": str(e)
            }
    
    async def _generate_search_explanation(
        self,
        query: str,
        intent: Dict[str, Any],
        enhancement: Dict[str, Any]
    ) -> str:
        """Generate natural language explanation of search"""
        try:
            prompt = f"""
            Explain this search query in a helpful way:
            Query: {query}
            Intent: {intent['primary_intent']}
            Keywords: {enhancement['semantic_keywords']}
            
            Provide a brief, friendly explanation of what we're searching for.
            """
            
            response = await asyncio.to_thread(
                openai.Completion.create,
                engine="text-davinci-003",
                prompt=prompt,
                max_tokens=100,
                temperature=0.7
            )
            
            return response.choices[0].text.strip()
            
        except Exception as e:
            logger.error(f"Error generating explanation: {e}")
            return f"Searching for: {query}"
    
    def _generate_search_config(
        self,
        intent: Dict[str, Any],
        enhancement: Dict[str, Any],
        user_profile: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate optimized search configuration"""
        config = {
            "use_semantic": True,
            "use_fuzzy": True,
            "boost_recent": False,
            "personalize": user_profile is not None
        }
        
        # Adjust based on intent
        if intent["primary_intent"] == "find_specific_item":
            config["use_fuzzy"] = False
            config["exact_match_boost"] = 2.0
        elif intent["primary_intent"] == "explore_category":
            config["use_facets"] = True
            config["expand_categories"] = True
        elif intent["primary_intent"] == "get_recommendations":
            config["use_similar"] = True
            config["diversity_factor"] = 0.3
        
        # Adjust based on query type
        if intent.get("query_type") == "question":
            config["extract_answer"] = True
        elif intent.get("query_type") == "comparison":
            config["enable_comparison"] = True
        
        # Add temporal boost if needed
        if intent.get("temporal_reference"):
            config["boost_recent"] = True
            config["recency_weight"] = 0.3
        
        return config
    
    async def process_search_results(
        self,
        results: List[Dict[str, Any]],
        query_data: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Process and enhance search results"""
        try:
            # 1. Re-rank results
            reranked = await self.semantic_enhancer.rerank_results(
                query_data["original_query"],
                results
            )
            
            # 2. Personalize if user provided
            if user_id:
                personalized = await self.personalization.personalize_results(
                    user_id,
                    reranked
                )
            else:
                personalized = reranked
            
            # 3. Generate insights
            insights = await self._generate_result_insights(
                personalized,
                query_data
            )
            
            # 4. Group results if needed
            if query_data.get("search_config", {}).get("enable_comparison"):
                grouped = self._group_for_comparison(personalized)
            else:
                grouped = None
            
            return {
                "results": personalized,
                "insights": insights,
                "grouped": grouped,
                "metadata": {
                    "reranked": True,
                    "personalized": user_id is not None,
                    "total_results": len(results),
                    "returned_results": len(personalized)
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing results: {e}")
            return {
                "results": results,
                "error": str(e)
            }
    
    async def _generate_result_insights(
        self,
        results: List[Dict[str, Any]],
        query_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate insights from search results"""
        insights = {
            "summary": "",
            "patterns": [],
            "recommendations": []
        }
        
        if not results:
            insights["summary"] = "No results found for your search."
            return insights
        
        # Analyze result distribution
        type_counts = defaultdict(int)
        service_counts = defaultdict(int)
        
        for result in results:
            type_counts[result.get("entity_type", "unknown")] += 1
            service_counts[result.get("service", "unknown")] += 1
        
        # Generate summary
        total = len(results)
        top_type = max(type_counts.items(), key=lambda x: x[1])
        insights["summary"] = f"Found {total} results, mostly {top_type[0]} items"
        
        # Identify patterns
        if len(type_counts) > 1:
            insights["patterns"].append({
                "type": "diverse_results",
                "description": f"Results span {len(type_counts)} different types"
            })
        
        # Generate recommendations
        if query_data.get("intent_analysis", {}).get("primary_intent") == "explore_category":
            insights["recommendations"].append({
                "action": "refine_search",
                "suggestion": "Try adding filters to narrow down results"
            })
        
        return insights
    
    def _group_for_comparison(
        self,
        results: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Group results for comparison"""
        grouped = defaultdict(list)
        
        for result in results:
            key = result.get("entity_type", "other")
            grouped[key].append(result)
        
        # Limit groups
        return dict(list(grouped.items())[:3])
    
    async def auto_categorize_new_content(
        self,
        content: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Auto-categorize new content for indexing"""
        categorization = await self.categorizer.categorize_content(content)
        
        # Add to content
        content["auto_categories"] = categorization["categories"]
        content["auto_tags"] = categorization["tags"]
        content["primary_category"] = categorization["primary_category"]
        
        return content 