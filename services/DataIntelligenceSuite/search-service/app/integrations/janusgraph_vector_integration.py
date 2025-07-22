"""
JanusGraph + Vector Search Integration

Demonstrates how to combine JanusGraph relationship queries with
Elasticsearch 8 vector search for powerful semantic + graph queries.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
import asyncio
import numpy as np
from collections import defaultdict

from elasticsearch import AsyncElasticsearch
from gremlin_python.driver import client, serializer
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T, P, Order

from ..services.enhanced_vector_search import EnhancedVectorSearchService

logger = logging.getLogger(__name__)


class JanusGraphVectorIntegration:
    """
    Combines JanusGraph graph queries with ES8 vector search
    """
    
    def __init__(self,
                 es_client: AsyncElasticsearch,
                 janusgraph_url: str = "ws://janusgraph:8182/gremlin",
                 vector_service: Optional[EnhancedVectorSearchService] = None):
        self.es_client = es_client
        self.janusgraph_url = janusgraph_url
        self.vector_service = vector_service or EnhancedVectorSearchService(es_client, janusgraph_url)
        self.graph_client = None
        
    async def initialize(self):
        """Initialize connections"""
        # Initialize vector service
        await self.vector_service.initialize()
        
        # Connect to JanusGraph
        self.graph_client = client.Client(
            self.janusgraph_url,
            'g',
            message_serializer=serializer.GraphSONSerializersV3d0()
        )
        
        # Create graph schema if needed
        await self._ensure_graph_schema()
        
        logger.info("JanusGraph-Vector integration initialized")
        
    async def _ensure_graph_schema(self):
        """Ensure required graph schema exists"""
        try:
            # Create property keys
            schema_commands = [
                # Node properties
                "mgmt = graph.openManagement()",
                "if (!mgmt.containsPropertyKey('doc_id')) mgmt.makePropertyKey('doc_id').dataType(String.class).cardinality(Cardinality.SINGLE).make()",
                "if (!mgmt.containsPropertyKey('doc_type')) mgmt.makePropertyKey('doc_type').dataType(String.class).cardinality(Cardinality.SINGLE).make()",
                "if (!mgmt.containsPropertyKey('tenant_id')) mgmt.makePropertyKey('tenant_id').dataType(String.class).cardinality(Cardinality.SINGLE).make()",
                "if (!mgmt.containsPropertyKey('title')) mgmt.makePropertyKey('title').dataType(String.class).cardinality(Cardinality.SINGLE).make()",
                "if (!mgmt.containsPropertyKey('created_at')) mgmt.makePropertyKey('created_at').dataType(Date.class).cardinality(Cardinality.SINGLE).make()",
                
                # Edge properties
                "if (!mgmt.containsPropertyKey('weight')) mgmt.makePropertyKey('weight').dataType(Float.class).cardinality(Cardinality.SINGLE).make()",
                "if (!mgmt.containsPropertyKey('confidence')) mgmt.makePropertyKey('confidence').dataType(Float.class).cardinality(Cardinality.SINGLE).make()",
                
                # Edge labels
                "if (!mgmt.containsEdgeLabel('references')) mgmt.makeEdgeLabel('references').multiplicity(Multiplicity.MULTI).make()",
                "if (!mgmt.containsEdgeLabel('similar_to')) mgmt.makeEdgeLabel('similar_to').multiplicity(Multiplicity.MULTI).make()",
                "if (!mgmt.containsEdgeLabel('derived_from')) mgmt.makeEdgeLabel('derived_from').multiplicity(Multiplicity.MANY2ONE).make()",
                "if (!mgmt.containsEdgeLabel('authored_by')) mgmt.makeEdgeLabel('authored_by').multiplicity(Multiplicity.MANY2ONE).make()",
                "if (!mgmt.containsEdgeLabel('tagged_with')) mgmt.makeEdgeLabel('tagged_with').multiplicity(Multiplicity.MULTI).make()",
                
                # Indexes
                "if (!mgmt.containsGraphIndex('byDocId')) mgmt.buildIndex('byDocId', Vertex.class).addKey(mgmt.getPropertyKey('doc_id')).buildCompositeIndex()",
                "if (!mgmt.containsGraphIndex('byTenant')) mgmt.buildIndex('byTenant', Vertex.class).addKey(mgmt.getPropertyKey('tenant_id')).buildCompositeIndex()",
                
                "mgmt.commit()"
            ]
            
            for cmd in schema_commands:
                await self.graph_client.submit(cmd)
                
        except Exception as e:
            logger.warning(f"Schema creation warning (may already exist): {e}")
            
    async def semantic_graph_search(self,
                                  query: str,
                                  start_type: str = "document",
                                  relationship_pattern: List[str] = None,
                                  max_depth: int = 2,
                                  k: int = 10,
                                  tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Semantic search with graph traversal patterns
        
        Example patterns:
        - ["authored_by", "authored"] - Find docs by same author
        - ["references", "referenced_by"] - Citation network
        - ["similar_to"] - Similarity network
        """
        try:
            # Step 1: Initial vector search
            initial_results = await self.vector_service.hybrid_search(
                query=query,
                k=k * 2,  # Get more candidates
                tenant_id=tenant_id,
                include_graph=False  # We'll do our own graph enrichment
            )
            
            if not initial_results:
                return []
                
            # Step 2: Graph traversal from initial results
            start_ids = [r["id"] for r in initial_results[:5]]  # Top 5 as seeds
            
            # Build graph query
            if relationship_pattern:
                graph_query = self._build_pattern_query(
                    start_ids,
                    relationship_pattern,
                    max_depth,
                    tenant_id
                )
            else:
                # Default: all relationships
                graph_query = f"""
                    g.V().has('doc_id', within({start_ids}))
                     .repeat(both().simplePath()).times({max_depth})
                     .dedup()
                     .limit(100)
                     .project('id', 'type', 'title', 'path')
                     .by('doc_id')
                     .by('doc_type')
                     .by('title')
                     .by(path().by('doc_id'))
                """
                
            graph_results = await self.graph_client.submit(graph_query)
            
            # Step 3: Combine and re-rank results
            combined_results = await self._combine_and_rerank(
                initial_results,
                graph_results,
                query,
                k
            )
            
            return combined_results
            
        except Exception as e:
            logger.error(f"Semantic graph search failed: {e}")
            raise
            
    async def create_similarity_graph(self,
                                    index: str = "unified",
                                    similarity_threshold: float = 0.8,
                                    batch_size: int = 100,
                                    tenant_id: Optional[str] = None):
        """
        Build a similarity graph based on vector embeddings
        """
        try:
            processed = 0
            relationships_created = 0
            
            # Scroll through all documents
            query = {"match_all": {}}
            if tenant_id:
                query = {"bool": {"filter": [{"term": {"tenant_id": tenant_id}}]}}
                
            async for doc in self.es_client.helpers.async_scan(
                client=self.es_client,
                index=f"vector_{index}",
                query=query,
                size=batch_size
            ):
                doc_id = doc["_id"]
                embedding = doc["_source"].get("embedding")
                
                if not embedding:
                    continue
                    
                # Find similar documents
                similar_docs = await self._find_similar_by_embedding(
                    embedding,
                    index,
                    similarity_threshold,
                    exclude_id=doc_id,
                    tenant_id=tenant_id
                )
                
                # Create graph relationships
                for similar in similar_docs:
                    await self._create_similarity_edge(
                        doc_id,
                        similar["id"],
                        similar["score"]
                    )
                    relationships_created += 1
                    
                processed += 1
                
                if processed % 100 == 0:
                    logger.info(f"Processed {processed} documents, created {relationships_created} relationships")
                    
            logger.info(f"Similarity graph complete: {processed} documents, {relationships_created} relationships")
            
        except Exception as e:
            logger.error(f"Error building similarity graph: {e}")
            raise
            
    async def knowledge_graph_rag(self,
                                 question: str,
                                 start_context: Optional[List[str]] = None,
                                 max_hops: int = 2,
                                 k_chunks: int = 5,
                                 tenant_id: Optional[str] = None) -> Dict[str, Any]:
        """
        RAG with knowledge graph context
        """
        try:
            # Step 1: Find relevant documents
            if start_context:
                # Start from specific nodes
                relevant_docs = await self._expand_context_graph(
                    start_context,
                    max_hops
                )
            else:
                # Start from vector search
                search_results = await self.vector_service.hybrid_search(
                    query=question,
                    k=10,
                    tenant_id=tenant_id
                )
                relevant_docs = [r["id"] for r in search_results[:5]]
                
            # Step 2: Get document chunks with graph context
            chunks_with_context = await self._get_chunks_with_graph_context(
                relevant_docs,
                question,
                k_chunks
            )
            
            # Step 3: Build enhanced context
            enhanced_context = self._build_graph_aware_context(
                chunks_with_context
            )
            
            # Step 4: Generate answer with graph context
            answer = await self._generate_graph_aware_answer(
                question,
                enhanced_context
            )
            
            return {
                "question": question,
                "answer": answer["answer"],
                "confidence": answer["confidence"],
                "graph_context": {
                    "nodes_explored": len(relevant_docs),
                    "relationships_used": answer["relationships"],
                    "key_entities": answer["entities"]
                },
                "sources": chunks_with_context[:k_chunks]
            }
            
        except Exception as e:
            logger.error(f"Knowledge graph RAG failed: {e}")
            raise
            
    async def impact_analysis(self,
                            doc_id: str,
                            change_type: str = "update",
                            max_depth: int = 3) -> Dict[str, Any]:
        """
        Analyze impact of document changes using graph relationships
        """
        try:
            # Find all dependent documents
            impact_query = f"""
                g.V().has('doc_id', '{doc_id}')
                 .union(
                   outE('references').inV(),
                   inE('derived_from').outV(),
                   both('similar_to')
                 )
                 .repeat(
                   union(
                     outE('references').inV(),
                     inE('derived_from').outV()
                   ).simplePath()
                 ).times({max_depth})
                 .dedup()
                 .project('id', 'type', 'title', 'impact_type', 'distance')
                 .by('doc_id')
                 .by('doc_type')
                 .by('title')
                 .by(constant(''))
                 .by(path().count(local))
            """
            
            impacted_docs = await self.graph_client.submit(impact_query)
            
            # Categorize impacts
            direct_impacts = []
            indirect_impacts = []
            
            for doc in impacted_docs:
                impact_score = 1.0 / doc["distance"]  # Closer = higher impact
                
                impact_info = {
                    "doc_id": doc["id"],
                    "title": doc["title"],
                    "type": doc["type"],
                    "impact_score": impact_score,
                    "distance": doc["distance"]
                }
                
                if doc["distance"] <= 1:
                    direct_impacts.append(impact_info)
                else:
                    indirect_impacts.append(impact_info)
                    
            # Get vector similarity impacts
            vector_impacts = await self._get_vector_similarity_impacts(
                doc_id,
                threshold=0.85
            )
            
            return {
                "source_doc": doc_id,
                "change_type": change_type,
                "impact_summary": {
                    "total_affected": len(impacted_docs),
                    "direct_impacts": len(direct_impacts),
                    "indirect_impacts": len(indirect_impacts),
                    "similarity_impacts": len(vector_impacts)
                },
                "direct_impacts": sorted(direct_impacts, key=lambda x: x["impact_score"], reverse=True),
                "indirect_impacts": sorted(indirect_impacts, key=lambda x: x["impact_score"], reverse=True),
                "similarity_impacts": vector_impacts,
                "recommendations": self._generate_impact_recommendations(
                    direct_impacts,
                    indirect_impacts,
                    change_type
                )
            }
            
        except Exception as e:
            logger.error(f"Impact analysis failed: {e}")
            raise
            
    def _build_pattern_query(self,
                           start_ids: List[str],
                           pattern: List[str],
                           max_depth: int,
                           tenant_id: Optional[str]) -> str:
        """Build graph query for relationship pattern"""
        # Convert pattern to Gremlin traversal
        traversal_steps = []
        for rel in pattern:
            traversal_steps.append(f"both('{rel}')")
            
        traversal = ".".join(traversal_steps)
        
        query = f"""
            g.V().has('doc_id', within({start_ids}))
        """
        
        if tenant_id:
            query += f".has('tenant_id', '{tenant_id}')"
            
        query += f"""
             .repeat({traversal}.simplePath()).times({max_depth})
             .dedup()
             .limit(100)
             .valueMap(true)
        """
        
        return query
        
    async def _combine_and_rerank(self,
                                vector_results: List[Dict],
                                graph_results: List[Any],
                                query: str,
                                k: int) -> List[Dict[str, Any]]:
        """Combine vector and graph results with re-ranking"""
        # Create result map
        combined = {}
        
        # Add vector results
        for i, result in enumerate(vector_results):
            combined[result["id"]] = {
                **result,
                "vector_rank": i + 1,
                "vector_score": result["score"],
                "graph_score": 0.0,
                "combined_score": result["score"]
            }
            
        # Process graph results
        graph_docs = []
        for g_result in graph_results:
            doc_id = g_result.get("id", g_result.get("doc_id", [None])[0])
            if doc_id:
                graph_docs.append({
                    "id": doc_id,
                    "type": g_result.get("type", g_result.get("doc_type", [None])[0]),
                    "title": g_result.get("title", [""])[0],
                    "path": g_result.get("path", [])
                })
                
        # Calculate graph scores
        for i, doc in enumerate(graph_docs):
            doc_id = doc["id"]
            graph_score = 1.0 / (i + 1)  # Rank-based score
            
            if doc_id in combined:
                combined[doc_id]["graph_score"] = graph_score
                combined[doc_id]["graph_path"] = doc.get("path", [])
            else:
                # Fetch document from ES
                try:
                    es_doc = await self.es_client.get(
                        index="vector_unified",
                        id=doc_id
                    )
                    combined[doc_id] = {
                        "id": doc_id,
                        "title": es_doc["_source"].get("title"),
                        "content": es_doc["_source"].get("content"),
                        "type": es_doc["_source"].get("type"),
                        "vector_score": 0.0,
                        "graph_score": graph_score,
                        "graph_path": doc.get("path", [])
                    }
                except:
                    pass
                    
        # Calculate combined scores
        for doc_id, doc in combined.items():
            doc["combined_score"] = (
                doc["vector_score"] * 0.6 +
                doc["graph_score"] * 0.4
            )
            
        # Sort by combined score
        results = sorted(
            combined.values(),
            key=lambda x: x["combined_score"],
            reverse=True
        )
        
        return results[:k]
        
    async def _find_similar_by_embedding(self,
                                       embedding: List[float],
                                       index: str,
                                       threshold: float,
                                       exclude_id: str,
                                       tenant_id: Optional[str]) -> List[Dict]:
        """Find similar documents by embedding"""
        query_body = {
            "size": 10,
            "query": {
                "bool": {
                    "must_not": [{"term": {"_id": exclude_id}}]
                }
            },
            "knn": {
                "field": "embedding",
                "query_vector": embedding,
                "k": 10,
                "num_candidates": 50
            },
            "min_score": threshold
        }
        
        if tenant_id:
            query_body["query"]["bool"]["filter"] = [{"term": {"tenant_id": tenant_id}}]
            
        response = await self.es_client.search(
            index=f"vector_{index}",
            body=query_body
        )
        
        return [
            {"id": hit["_id"], "score": hit["_score"]}
            for hit in response["hits"]["hits"]
        ]
        
    async def _create_similarity_edge(self,
                                    from_id: str,
                                    to_id: str,
                                    similarity: float):
        """Create similarity edge in graph"""
        query = f"""
            g.V().has('doc_id', '{from_id}').as('from')
             .V().has('doc_id', '{to_id}').as('to')
             .addE('similar_to')
             .from('from').to('to')
             .property('weight', {similarity})
             .property('created_at', new Date())
        """
        
        try:
            await self.graph_client.submit(query)
        except Exception as e:
            # Edge might already exist
            logger.debug(f"Edge creation warning: {e}")
            
    async def _expand_context_graph(self,
                                  start_nodes: List[str],
                                  max_hops: int) -> List[str]:
        """Expand context using graph traversal"""
        query = f"""
            g.V().has('doc_id', within({start_nodes}))
             .union(
               repeat(out('references', 'derived_from')).times({max_hops}),
               repeat(in('references')).times({max_hops - 1})
             )
             .dedup()
             .values('doc_id')
             .limit(50)
        """
        
        result = await self.graph_client.submit(query)
        return list(result)
        
    async def _get_chunks_with_graph_context(self,
                                           doc_ids: List[str],
                                           question: str,
                                           k_chunks: int) -> List[Dict]:
        """Get document chunks with graph context"""
        # Get chunks from vector search
        chunks = await self.vector_service._search_chunks(
            question,
            "unified",
            k_chunks * 2,
            None
        )
        
        # Filter to our document set
        relevant_chunks = [
            chunk for chunk in chunks
            if chunk["doc_id"] in doc_ids
        ]
        
        # Add graph context to each chunk
        for chunk in relevant_chunks:
            # Get relationships for this document
            relationships = await self._get_document_relationships(chunk["doc_id"])
            chunk["graph_context"] = relationships
            
        return relevant_chunks[:k_chunks]
        
    async def _get_document_relationships(self, doc_id: str) -> Dict[str, List[str]]:
        """Get relationships for a document"""
        query = f"""
            g.V().has('doc_id', '{doc_id}')
             .project('references', 'referenced_by', 'similar_to', 'tags')
             .by(out('references').values('title').fold())
             .by(in('references').values('title').fold())
             .by(both('similar_to').values('title').fold())
             .by(out('tagged_with').values('tag_name').fold())
        """
        
        result = await self.graph_client.submit(query)
        return result[0] if result else {}
        
    def _build_graph_aware_context(self,
                                 chunks_with_context: List[Dict]) -> str:
        """Build context string with graph relationships"""
        context_parts = []
        
        for i, chunk in enumerate(chunks_with_context):
            context = f"[{i+1}] {chunk['content']}"
            
            # Add relationship context
            if chunk.get("graph_context"):
                gc = chunk["graph_context"]
                if gc.get("references"):
                    context += f"\n  References: {', '.join(gc['references'][:3])}"
                if gc.get("similar_to"):
                    context += f"\n  Related: {', '.join(gc['similar_to'][:3])}"
                if gc.get("tags"):
                    context += f"\n  Tags: {', '.join(gc['tags'])}"
                    
            context_parts.append(context)
            
        return "\n\n".join(context_parts)
        
    async def _generate_graph_aware_answer(self,
                                         question: str,
                                         context: str) -> Dict[str, Any]:
        """Generate answer with graph awareness"""
        # This would use the actual RAG implementation
        # For now, return a structured response
        return {
            "answer": f"Based on the graph-enriched context: {context[:200]}...",
            "confidence": 0.85,
            "relationships": ["references", "similar_to"],
            "entities": ["doc1", "doc2"]
        }
        
    async def _get_vector_similarity_impacts(self,
                                           doc_id: str,
                                           threshold: float) -> List[Dict]:
        """Get documents impacted by vector similarity"""
        # Get document embedding
        doc = await self.es_client.get(
            index="vector_unified",
            id=doc_id
        )
        embedding = doc["_source"].get("embedding")
        
        if not embedding:
            return []
            
        # Find similar documents
        similar = await self._find_similar_by_embedding(
            embedding,
            "unified",
            threshold,
            doc_id,
            None
        )
        
        return [
            {
                "doc_id": s["id"],
                "similarity_score": s["score"],
                "impact_type": "high_similarity"
            }
            for s in similar
        ]
        
    def _generate_impact_recommendations(self,
                                       direct_impacts: List[Dict],
                                       indirect_impacts: List[Dict],
                                       change_type: str) -> List[str]:
        """Generate recommendations based on impact analysis"""
        recommendations = []
        
        if len(direct_impacts) > 10:
            recommendations.append(
                f"High impact: {len(direct_impacts)} documents directly depend on this. "
                "Consider versioning or gradual migration."
            )
            
        if len(indirect_impacts) > 50:
            recommendations.append(
                f"Cascading impact: {len(indirect_impacts)} documents indirectly affected. "
                "Plan phased updates."
            )
            
        if change_type == "delete":
            recommendations.append(
                "Deletion warning: Consider archiving instead of deletion to preserve relationships."
            )
            
        return recommendations 