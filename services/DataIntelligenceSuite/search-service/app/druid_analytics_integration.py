"""
Elasticsearch v8 + Apache Druid Analytics Integration

Combines:
- Elasticsearch v8: Semantic search, RAG, vector search
- Apache Druid: Time-series analytics, OLAP queries, real-time aggregations

This creates a comprehensive analytics stack where:
- Druid handles time-series and aggregation queries
- Elasticsearch handles search, semantic queries, and document retrieval
- Results are combined for rich analytics experiences
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import httpx

from elasticsearch import AsyncElasticsearch
from .services.es_vector_search import ESVectorSearchService

logger = logging.getLogger(__name__)


class DruidElasticsearchAnalytics:
    """
    Combines Elasticsearch v8 search with Apache Druid analytics
    for comprehensive data exploration
    """
    
    def __init__(self,
                 es_client: AsyncElasticsearch,
                 druid_url: str = "http://druid-broker:8082",
                 analytics_service_url: str = "http://analytics-service:8000"):
        self.es_client = es_client
        self.es_vector_service = ESVectorSearchService(es_client)
        self.druid_url = druid_url
        self.analytics_service_url = analytics_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def semantic_analytics_search(self,
                                      query: str,
                                      time_range: Tuple[datetime, datetime],
                                      datasource: str = "events",
                                      metrics: List[str] = None,
                                      dimensions: List[str] = None,
                                      tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Combine semantic search with time-series analytics
        
        1. Use Elasticsearch to find relevant entities via semantic search
        2. Use entity IDs to filter Druid time-series queries
        3. Return combined results with both search context and analytics
        """
        try:
            # Step 1: Semantic search in Elasticsearch
            search_results = await self.es_vector_service.semantic_search(
                query=query,
                k=20,
                tenant_id=tenant_id,
                include_explanations=True
            )
            
            # Extract entity IDs from search results
            entity_ids = [r["source"].get("entity_id") or r["id"] for r in search_results]
            entity_names = [r["source"].get("name") or r["source"].get("title", "Unknown") for r in search_results]
            
            # Step 2: Query Druid for analytics on these entities
            druid_query = self._build_druid_timeseries_query(
                datasource=datasource,
                time_range=time_range,
                entity_ids=entity_ids,
                metrics=metrics or ["count", "sum_value"],
                dimensions=dimensions or ["entity_id"],
                tenant_id=tenant_id
            )
            
            druid_response = await self._execute_druid_query(druid_query)
            
            # Step 3: Combine results
            combined_results = self._combine_search_and_analytics(
                search_results=search_results[:10],  # Top 10 search results
                analytics_data=druid_response,
                entity_names=dict(zip(entity_ids, entity_names))
            )
            
            return {
                "query": query,
                "time_range": {
                    "start": time_range[0].isoformat(),
                    "end": time_range[1].isoformat()
                },
                "search_results": combined_results,
                "total_entities": len(entity_ids),
                "datasource": datasource
            }
            
        except Exception as e:
            logger.error(f"Semantic analytics search failed: {e}")
            raise
    
    async def rag_enhanced_analytics(self,
                                   question: str,
                                   datasource: str = "events",
                                   time_range: Optional[Tuple[datetime, datetime]] = None,
                                   tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Use RAG to answer analytical questions by combining:
        - Document context from Elasticsearch
        - Real-time metrics from Druid
        
        Example: "What caused the spike in errors last week?"
        """
        try:
            # Default time range: last 7 days
            if not time_range:
                end_time = datetime.utcnow()
                start_time = end_time - timedelta(days=7)
                time_range = (start_time, end_time)
            
            # Step 1: Extract analytical intent from question
            analytical_context = await self._extract_analytical_context(question)
            
            # Step 2: Get relevant documents via RAG
            rag_results = await self.es_vector_service.rag_search(
                question=question,
                index="documents",
                k=5,
                tenant_id=tenant_id
            )
            
            # Step 3: Query Druid based on analytical context
            if analytical_context["requires_metrics"]:
                druid_data = await self._query_druid_for_context(
                    analytical_context=analytical_context,
                    datasource=datasource,
                    time_range=time_range,
                    tenant_id=tenant_id
                )
            else:
                druid_data = None
            
            # Step 4: Generate enhanced answer combining both sources
            enhanced_answer = await self._generate_analytics_answer(
                question=question,
                rag_answer=rag_results["answer"],
                rag_sources=rag_results.get("sources", []),
                druid_data=druid_data,
                analytical_context=analytical_context
            )
            
            return {
                "question": question,
                "answer": enhanced_answer,
                "document_sources": rag_results.get("sources", []),
                "analytics_data": druid_data,
                "time_range": {
                    "start": time_range[0].isoformat(),
                    "end": time_range[1].isoformat()
                } if time_range else None
            }
            
        except Exception as e:
            logger.error(f"RAG enhanced analytics failed: {e}")
            raise
    
    async def anomaly_detection_with_context(self,
                                           datasource: str,
                                           metric: str,
                                           time_range: Tuple[datetime, datetime],
                                           tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Detect anomalies in Druid metrics and find related context in Elasticsearch
        
        1. Query Druid for time-series anomalies
        2. For each anomaly, search for related events/documents in Elasticsearch
        3. Provide contextual explanation for anomalies
        """
        try:
            # Step 1: Detect anomalies via analytics service
            anomaly_response = await self.http_client.post(
                f"{self.analytics_service_url}/api/v1/analytics/anomalies",
                json={
                    "datasource": datasource,
                    "metric": metric,
                    "time_range": {
                        "start": time_range[0].isoformat(),
                        "end": time_range[1].isoformat()
                    },
                    "tenant_id": tenant_id
                }
            )
            anomaly_response.raise_for_status()
            anomalies = anomaly_response.json()["anomalies"]
            
            # Step 2: Find context for each anomaly
            contextualized_anomalies = []
            
            for anomaly in anomalies:
                # Search for events around anomaly time
                anomaly_time = datetime.fromisoformat(anomaly["timestamp"])
                time_window = (
                    anomaly_time - timedelta(hours=1),
                    anomaly_time + timedelta(hours=1)
                )
                
                # Search for related events
                context_query = f"{metric} anomaly error issue problem {anomaly.get('dimension_value', '')}"
                
                context_results = await self.es_vector_service.hybrid_search(
                    query=context_query,
                    filters={
                        "timestamp": {
                            "gte": time_window[0].isoformat(),
                            "lte": time_window[1].isoformat()
                        }
                    },
                    tenant_id=tenant_id,
                    k=5
                )
                
                # Try to explain the anomaly
                explanation = await self._explain_anomaly(
                    anomaly=anomaly,
                    context_results=context_results
                )
                
                contextualized_anomalies.append({
                    **anomaly,
                    "context_events": context_results,
                    "explanation": explanation
                })
            
            return {
                "datasource": datasource,
                "metric": metric,
                "time_range": {
                    "start": time_range[0].isoformat(),
                    "end": time_range[1].isoformat()
                },
                "anomaly_count": len(anomalies),
                "anomalies": contextualized_anomalies
            }
            
        except Exception as e:
            logger.error(f"Anomaly detection with context failed: {e}")
            raise
    
    async def cross_dimensional_search(self,
                                     search_query: str,
                                     druid_dimensions: List[str],
                                     time_range: Tuple[datetime, datetime],
                                     datasource: str = "events",
                                     tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Search across Elasticsearch and aggregate by Druid dimensions
        
        Example: Search for "payment failures" and aggregate by country, payment_method
        """
        try:
            # Step 1: Semantic search for relevant entities
            search_results = await self.es_vector_service.semantic_search(
                query=search_query,
                k=100,  # Get more results for aggregation
                tenant_id=tenant_id
            )
            
            # Extract entity IDs and metadata
            entity_data = {}
            for result in search_results:
                entity_id = result["source"].get("entity_id") or result["id"]
                entity_data[entity_id] = {
                    "name": result["source"].get("name", "Unknown"),
                    "type": result["source"].get("entity_type", "unknown"),
                    "score": result["score"]
                }
            
            # Step 2: Query Druid with dimension breakdown
            druid_query = {
                "queryType": "groupBy",
                "dataSource": datasource,
                "intervals": [f"{time_range[0].isoformat()}/{time_range[1].isoformat()}"],
                "granularity": "day",
                "dimensions": druid_dimensions,
                "filter": {
                    "type": "and",
                    "fields": [
                        {
                            "type": "in",
                            "dimension": "entity_id",
                            "values": list(entity_data.keys())
                        }
                    ]
                },
                "aggregations": [
                    {"type": "count", "name": "count"},
                    {"type": "doubleSum", "name": "total_value", "fieldName": "value"}
                ]
            }
            
            if tenant_id:
                druid_query["filter"]["fields"].append({
                    "type": "selector",
                    "dimension": "tenant_id",
                    "value": tenant_id
                })
            
            druid_response = await self._execute_druid_query(druid_query)
            
            # Step 3: Enrich Druid results with search context
            enriched_results = []
            for row in druid_response:
                dimension_values = {dim: row.get(dim) for dim in druid_dimensions}
                
                # Find top entities for this dimension combination
                top_entities = self._find_top_entities_for_dimensions(
                    row.get("entity_ids", []),
                    entity_data
                )
                
                enriched_results.append({
                    "dimensions": dimension_values,
                    "metrics": {
                        "count": row.get("count", 0),
                        "total_value": row.get("total_value", 0)
                    },
                    "top_entities": top_entities[:5],
                    "search_relevance": sum(e["score"] for e in top_entities[:5]) / 5 if top_entities else 0
                })
            
            # Sort by count
            enriched_results.sort(key=lambda x: x["metrics"]["count"], reverse=True)
            
            return {
                "search_query": search_query,
                "dimensions": druid_dimensions,
                "time_range": {
                    "start": time_range[0].isoformat(),
                    "end": time_range[1].isoformat()
                },
                "total_entities_found": len(entity_data),
                "dimension_breakdowns": enriched_results[:20]  # Top 20 combinations
            }
            
        except Exception as e:
            logger.error(f"Cross-dimensional search failed: {e}")
            raise
    
    def _build_druid_timeseries_query(self,
                                    datasource: str,
                                    time_range: Tuple[datetime, datetime],
                                    entity_ids: List[str],
                                    metrics: List[str],
                                    dimensions: List[str],
                                    tenant_id: Optional[str]) -> Dict[str, Any]:
        """Build Druid timeseries query"""
        query = {
            "queryType": "timeseries",
            "dataSource": datasource,
            "intervals": [f"{time_range[0].isoformat()}/{time_range[1].isoformat()}"],
            "granularity": "hour",
            "filter": {
                "type": "and",
                "fields": []
            },
            "aggregations": []
        }
        
        # Add entity filter
        if entity_ids:
            query["filter"]["fields"].append({
                "type": "in",
                "dimension": "entity_id",
                "values": entity_ids
            })
        
        # Add tenant filter
        if tenant_id:
            query["filter"]["fields"].append({
                "type": "selector",
                "dimension": "tenant_id",
                "value": tenant_id
            })
        
        # Add metric aggregations
        for metric in metrics:
            if metric == "count":
                query["aggregations"].append({
                    "type": "count",
                    "name": "count"
                })
            else:
                query["aggregations"].append({
                    "type": "doubleSum",
                    "name": metric,
                    "fieldName": metric.replace("sum_", "")
                })
        
        return query
    
    async def _execute_druid_query(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute Druid query"""
        try:
            response = await self.http_client.post(
                f"{self.druid_url}/druid/v2",
                json=query
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Druid query failed: {e}")
            return []
    
    def _combine_search_and_analytics(self,
                                    search_results: List[Dict[str, Any]],
                                    analytics_data: List[Dict[str, Any]],
                                    entity_names: Dict[str, str]) -> List[Dict[str, Any]]:
        """Combine search results with analytics data"""
        combined = []
        
        for result in search_results:
            entity_id = result["source"].get("entity_id") or result["id"]
            
            # Find analytics data for this entity
            entity_analytics = [
                row for row in analytics_data
                if row.get("entity_id") == entity_id
            ]
            
            combined.append({
                "entity": {
                    "id": entity_id,
                    "name": result["source"].get("name"),
                    "description": result["source"].get("description"),
                    "type": result["source"].get("entity_type")
                },
                "search": {
                    "score": result["score"],
                    "explanation": result.get("semantic_explanation")
                },
                "analytics": {
                    "timeseries": entity_analytics,
                    "total_events": sum(row.get("count", 0) for row in entity_analytics),
                    "total_value": sum(row.get("sum_value", 0) for row in entity_analytics)
                }
            })
        
        return combined
    
    async def _extract_analytical_context(self, question: str) -> Dict[str, Any]:
        """Extract analytical context from question"""
        # Simple keyword-based extraction
        context = {
            "requires_metrics": False,
            "time_focus": None,
            "metrics": [],
            "dimensions": []
        }
        
        # Check for metric-related keywords
        metric_keywords = ["spike", "increase", "decrease", "trend", "average", "sum", "count"]
        if any(keyword in question.lower() for keyword in metric_keywords):
            context["requires_metrics"] = True
        
        # Check for time-related keywords
        if "yesterday" in question.lower():
            context["time_focus"] = "yesterday"
        elif "last week" in question.lower():
            context["time_focus"] = "last_week"
        elif "today" in question.lower():
            context["time_focus"] = "today"
        
        # Extract potential metrics
        if "error" in question.lower():
            context["metrics"].append("error_count")
        if "latency" in question.lower() or "slow" in question.lower():
            context["metrics"].append("avg_latency")
        if "request" in question.lower() or "traffic" in question.lower():
            context["metrics"].append("request_count")
        
        return context
    
    async def _query_druid_for_context(self,
                                     analytical_context: Dict[str, Any],
                                     datasource: str,
                                     time_range: Tuple[datetime, datetime],
                                     tenant_id: Optional[str]) -> Dict[str, Any]:
        """Query Druid based on analytical context"""
        # Build appropriate query based on context
        metrics = analytical_context["metrics"] or ["count"]
        
        query = {
            "queryType": "timeseries",
            "dataSource": datasource,
            "intervals": [f"{time_range[0].isoformat()}/{time_range[1].isoformat()}"],
            "granularity": "hour",
            "aggregations": []
        }
        
        for metric in metrics:
            query["aggregations"].append({
                "type": "doubleSum" if metric != "count" else "count",
                "name": metric,
                "fieldName": metric if metric != "count" else None
            })
        
        if tenant_id:
            query["filter"] = {
                "type": "selector",
                "dimension": "tenant_id",
                "value": tenant_id
            }
        
        result = await self._execute_druid_query(query)
        
        return {
            "metrics": metrics,
            "data": result,
            "summary": self._summarize_metrics(result, metrics)
        }
    
    def _summarize_metrics(self, data: List[Dict[str, Any]], metrics: List[str]) -> Dict[str, Any]:
        """Summarize metric data"""
        if not data:
            return {}
        
        summary = {}
        for metric in metrics:
            values = [row.get(metric, 0) for row in data]
            summary[metric] = {
                "total": sum(values),
                "average": sum(values) / len(values) if values else 0,
                "max": max(values) if values else 0,
                "min": min(values) if values else 0
            }
        
        return summary
    
    async def _generate_analytics_answer(self,
                                       question: str,
                                       rag_answer: str,
                                       rag_sources: List[Dict[str, Any]],
                                       druid_data: Optional[Dict[str, Any]],
                                       analytical_context: Dict[str, Any]) -> str:
        """Generate enhanced answer combining RAG and analytics"""
        enhanced_answer = rag_answer
        
        if druid_data and druid_data.get("summary"):
            # Add metrics summary to answer
            metrics_summary = "\n\nBased on the analytics data:\n"
            
            for metric, stats in druid_data["summary"].items():
                metrics_summary += f"- {metric}: Total={stats['total']:,.0f}, "
                metrics_summary += f"Average={stats['average']:,.1f}, "
                metrics_summary += f"Max={stats['max']:,.0f}\n"
            
            enhanced_answer += metrics_summary
        
        return enhanced_answer
    
    async def _explain_anomaly(self,
                             anomaly: Dict[str, Any],
                             context_results: List[Dict[str, Any]]) -> str:
        """Generate explanation for anomaly based on context"""
        if not context_results:
            return "No related events found in the time window."
        
        # Simple explanation based on found events
        event_types = set()
        for result in context_results[:3]:  # Top 3 results
            event_type = result["source"].get("event_type", "event")
            event_types.add(event_type)
        
        explanation = f"Found {len(context_results)} related events around the anomaly time. "
        if event_types:
            explanation += f"Event types include: {', '.join(event_types)}."
        
        return explanation
    
    def _find_top_entities_for_dimensions(self,
                                        entity_ids: List[str],
                                        entity_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Find top entities for given dimension values"""
        entities = []
        
        for entity_id in entity_ids:
            if entity_id in entity_data:
                entities.append({
                    "id": entity_id,
                    **entity_data[entity_id]
                })
        
        # Sort by search score
        entities.sort(key=lambda x: x["score"], reverse=True)
        
        return entities 