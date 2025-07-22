# Enhanced Vector Search Guide

Comprehensive guide for Elasticsearch 8 native vector search with JanusGraph integration and RAG capabilities.

## 🎯 Overview

The enhanced vector search provides:
- **Native ES8 k-NN**: Hardware-accelerated HNSW algorithm for fast similarity search
- **JanusGraph Integration**: Relationship-aware search combining vectors with graph traversal
- **Multi-Modal Embeddings**: Support for text, code, images, and graph embeddings
- **RAG (Retrieval Augmented Generation)**: AI-powered answers with source attribution
- **Hybrid Search**: Combined BM25 text search with vector similarity

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Search Request                           │
└─────────────────┬───────────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────────┐
│              Enhanced Vector Search Service                  │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │  Embedders  │   ES8 k-NN   │  JanusGraph │    RAG     │ │
│  │  (Multi-    │   (Native    │   (Graph    │  (OpenAI/  │ │
│  │   Modal)    │    Vectors)  │  Traversal) │  LangChain)│ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 Key Features

### 1. Native Elasticsearch 8 k-NN

```json
// Index mapping with dense_vector
{
  "mappings": {
    "properties": {
      "embedding": {
        "type": "dense_vector",
        "dims": 768,
        "index": true,
        "similarity": "cosine"
      }
    }
  }
}
```

**Benefits:**
- Hardware-accelerated HNSW algorithm
- Sub-millisecond search latency
- Scales to billions of vectors
- No external vector database needed

### 2. Multi-Modal Embeddings

| Type | Model | Dimensions | Use Case |
|------|-------|------------|----------|
| Text | all-mpnet-base-v2 | 768 | General text search |
| Multilingual | paraphrase-multilingual-mpnet | 768 | Cross-language search |
| Code | microsoft/codebert-base | 768 | Code similarity |
| Image | CLIP ViT-B/32 | 512 | Visual search |
| Graph | Node2Vec/GraphSAGE | 128 | Relationship patterns |

### 3. Hybrid Search

Combines multiple scoring methods:
- **BM25**: Traditional text relevance
- **k-NN**: Vector similarity
- **Graph Distance**: Relationship proximity

```python
combined_score = (
    text_score * text_weight +
    vector_score * vector_weight +
    graph_score * graph_weight
)
```

### 4. Graph-Enhanced Search

Leverages JanusGraph relationships:
- **Citation Networks**: Find related research
- **Dependency Graphs**: Track software dependencies
- **Similarity Networks**: Discover related content
- **Knowledge Graphs**: Contextual understanding

### 5. RAG Capabilities

Retrieval Augmented Generation features:
- **Chunk-based Retrieval**: Optimized context windows
- **Source Attribution**: Track information provenance
- **Confidence Scoring**: Assess answer reliability
- **Multi-hop Reasoning**: Follow relationship chains

## 📡 API Reference

### Index Document

```bash
POST /api/v1/vector/index
{
  "doc_id": "doc123",
  "content": "Apache Iceberg provides ACID transactions...",
  "doc_type": "text",
  "title": "Data Lake Best Practices",
  "metadata": {
    "author": "John Doe",
    "tags": ["data-lake", "iceberg"]
  }
}
```

### Hybrid Search

```bash
POST /api/v1/vector/search/hybrid
{
  "query": "How to implement ACID transactions in data lakes?",
  "index": "unified",
  "k": 10,
  "text_weight": 0.4,
  "vector_weight": 0.6,
  "include_graph": true
}
```

Response:
```json
{
  "results": [
    {
      "id": "doc123",
      "score": 0.92,
      "title": "Data Lake Best Practices",
      "content": "...",
      "graph_context": {
        "related_documents": [...],
        "relationship_count": 5
      }
    }
  ]
}
```

### Graph-Enhanced Search

```bash
POST /api/v1/vector/search/graph
{
  "query": "distributed transaction patterns",
  "start_nodes": ["doc123", "doc456"],
  "max_depth": 2,
  "relationship_types": ["references", "implements"],
  "k": 10
}
```

### RAG Search

```bash
POST /api/v1/vector/search/rag
{
  "question": "What are the benefits of using Apache Iceberg?",
  "index": "unified",
  "k": 5,
  "model": "gpt-3.5-turbo"
}
```

Response:
```json
{
  "question": "What are the benefits of using Apache Iceberg?",
  "answer": "Apache Iceberg provides several key benefits:\n\n1. ACID Transactions: Ensures data consistency...\n2. Schema Evolution: Supports adding, dropping columns...\n3. Time Travel: Query historical data snapshots...",
  "sources": [
    {
      "doc_id": "doc123",
      "chunk_id": "doc123_chunk_2",
      "relevance_score": 0.89,
      "content_preview": "Iceberg provides ACID transactions..."
    }
  ],
  "confidence": 0.92
}
```

## 💡 Usage Examples

### Example 1: Semantic Code Search

```python
# Index code with embeddings
await vector_service.index_document(
    doc_id="func_123",
    content="""
    def calculate_metrics(data: pd.DataFrame) -> Dict[str, float]:
        return {
            'mean': data.mean(),
            'std': data.std(),
            'median': data.median()
        }
    """,
    doc_type="code",
    title="Statistical metrics function"
)

# Search for similar code
results = await vector_service.hybrid_search(
    query="function to compute statistics on dataframe",
    index="code",
    k=5
)
```

### Example 2: Knowledge Graph RAG

```python
# Build context from graph relationships
integration = JanusGraphVectorIntegration(es_client, janusgraph_url)

# Ask question with graph context
result = await integration.knowledge_graph_rag(
    question="How does the authentication service handle OAuth?",
    start_context=["auth_service_doc"],
    max_hops=2,
    k_chunks=5
)

print(f"Answer: {result['answer']}")
print(f"Explored {result['graph_context']['nodes_explored']} related documents")
```

### Example 3: Impact Analysis

```python
# Analyze impact of document changes
impact = await integration.impact_analysis(
    doc_id="api_spec_v2",
    change_type="update",
    max_depth=3
)

print(f"Direct impacts: {impact['impact_summary']['direct_impacts']}")
print(f"Indirect impacts: {impact['impact_summary']['indirect_impacts']}")

# Show recommendations
for rec in impact['recommendations']:
    print(f"- {rec}")
```

### Example 4: Multi-Modal Search

```python
# Index image with CLIP
await vector_service.index_document(
    doc_id="diagram_123",
    content="/path/to/architecture_diagram.png",
    doc_type="image",
    title="Microservices Architecture",
    metadata={"project": "platformQ"}
)

# Find similar images or diagrams
results = await vector_service.hybrid_search(
    query="kubernetes deployment architecture",
    index="image",
    k=10
)
```

## 🔧 Configuration

### Elasticsearch Index Settings

```yaml
# Optimize for vector search
index:
  knn: true
  knn.algo_param.ef_search: 100  # Higher = better recall
  knn.algo_param.ef_construction: 200  # Higher = better graph
  number_of_shards: 3
  number_of_replicas: 1
```

### Embedding Model Selection

```python
# Configure models based on use case
EMBEDDING_CONFIGS = {
    "general": {
        "model": "sentence-transformers/all-mpnet-base-v2",
        "dims": 768,
        "batch_size": 32
    },
    "domain_specific": {
        "model": "custom/domain-bert",
        "dims": 768,
        "batch_size": 16
    },
    "lightweight": {
        "model": "sentence-transformers/all-MiniLM-L6-v2",
        "dims": 384,
        "batch_size": 64
    }
}
```

### JanusGraph Schema

```groovy
// Define graph schema
mgmt = graph.openManagement()

// Properties
doc_id = mgmt.makePropertyKey('doc_id').dataType(String.class).make()
doc_type = mgmt.makePropertyKey('doc_type').dataType(String.class).make()
weight = mgmt.makePropertyKey('weight').dataType(Float.class).make()

// Edge labels
references = mgmt.makeEdgeLabel('references').multiplicity(MULTI).make()
similar_to = mgmt.makeEdgeLabel('similar_to').signature(weight).make()

// Indexes
mgmt.buildIndex('byDocId', Vertex.class).addKey(doc_id).buildCompositeIndex()

mgmt.commit()
```

## 📊 Performance Optimization

### 1. Indexing Optimization

```python
# Batch indexing
async def batch_index_documents(documents: List[Dict], batch_size: int = 100):
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        
        # Generate embeddings in batch
        texts = [doc['content'] for doc in batch]
        embeddings = model.encode(texts, batch_size=batch_size)
        
        # Bulk index to Elasticsearch
        actions = []
        for doc, embedding in zip(batch, embeddings):
            actions.append({
                "_index": "vector_unified",
                "_id": doc['id'],
                "_source": {
                    **doc,
                    "embedding": embedding.tolist()
                }
            })
            
        await async_bulk(es_client, actions)
```

### 2. Search Optimization

```python
# Pre-filter to reduce vector search space
query = {
    "query": {
        "bool": {
            "filter": [
                {"term": {"tenant_id": tenant_id}},
                {"range": {"created_at": {"gte": "2024-01-01"}}}
            ]
        }
    },
    "knn": {
        "field": "embedding",
        "query_vector": query_embedding,
        "k": 10,
        "num_candidates": 50  # Reduce candidates with filters
    }
}
```

### 3. Caching Strategy

```python
# Cache embeddings for frequent queries
@lru_cache(maxsize=1000)
def get_cached_embedding(text: str, model_name: str):
    return model.encode(text)

# Redis for distributed cache
async def get_embedding_with_cache(text: str):
    cache_key = f"emb:{hashlib.md5(text.encode()).hexdigest()}"
    
    # Check cache
    cached = await redis.get(cache_key)
    if cached:
        return np.frombuffer(cached, dtype=np.float32)
        
    # Generate and cache
    embedding = await generate_embedding(text)
    await redis.setex(cache_key, 3600, embedding.tobytes())
    
    return embedding
```

## 🔍 Monitoring & Debugging

### Metrics to Track

| Metric | Target | Alert Threshold |
|--------|--------|-----------------|
| Search Latency (p99) | <100ms | >200ms |
| Embedding Generation | <50ms | >100ms |
| Index Refresh Rate | <5s | >10s |
| Graph Traversal Time | <200ms | >500ms |
| RAG Response Time | <2s | >5s |

### Debug Queries

```bash
# Check vector field statistics
GET /vector_unified/_field_caps?fields=embedding

# Analyze search performance
POST /vector_unified/_search?explain=true
{
  "knn": {
    "field": "embedding",
    "query_vector": [...],
    "k": 10
  }
}

# Monitor index size
GET /vector_unified/_stats/store,docs
```

## 🚨 Common Issues

### 1. High Memory Usage
- **Cause**: Large vector dimensions or too many vectors in memory
- **Solution**: Use disk-based HNSW, reduce dimensions, or increase heap

### 2. Slow Indexing
- **Cause**: Sequential embedding generation
- **Solution**: Batch processing, GPU acceleration, or pre-computed embeddings

### 3. Poor Search Quality
- **Cause**: Domain mismatch in embedding model
- **Solution**: Fine-tune models or use domain-specific embeddings

### 4. Graph Traversal Timeout
- **Cause**: Unbounded graph queries
- **Solution**: Add depth limits and result caps 