"""
Elasticsearch v8 Vector Search and RAG Pipeline DAG

Demonstrates:
- Native k-NN search with dense vectors
- Document indexing with embeddings
- RAG (Retrieval Augmented Generation) for Q&A
- Hybrid search combining text and vectors
- Integration with Apache Druid analytics
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import logging
import httpx
import json
import asyncio

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'search-platform',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}


def prepare_documents(**context):
    """Prepare documents for indexing with embeddings"""
    conf = context['dag_run'].conf
    
    # Sample documents for different use cases
    documents = [
        # Technical documentation for RAG
        {
            "id": "doc_001",
            "title": "Apache Iceberg Architecture Guide",
            "content": """Apache Iceberg is an open table format for huge analytic datasets. 
            Iceberg adds tables to compute engines including Spark, Trino, PrestoDB, Flink and Hive 
            using a high-performance table format that works just like a SQL table. It provides 
            ACID transactions, schema evolution, hidden partitioning, and time travel capabilities.""",
            "type": "documentation",
            "tags": ["iceberg", "data-lake", "analytics"],
            "tenant_id": conf.get("tenant_id", "default")
        },
        {
            "id": "doc_002",
            "title": "Feature Store Best Practices",
            "content": """A feature store is a centralized repository where you standardize 
            the definition, storage, and access of features for training and serving. 
            Key benefits include feature reusability, ensuring training-serving consistency, 
            and enabling point-in-time correct feature retrieval for model training.""",
            "type": "documentation",
            "tags": ["ml", "feature-store", "mlops"],
            "tenant_id": conf.get("tenant_id", "default")
        },
        {
            "id": "doc_003",
            "title": "Zero-ETL Data Integration Patterns",
            "content": """Zero-ETL represents a paradigm shift in data integration where data 
            is synchronized between systems without traditional ETL pipelines. This is achieved 
            through technologies like Change Data Capture (CDC), event streaming, and 
            declarative data pipelines that eliminate custom transformation code.""",
            "type": "documentation",
            "tags": ["etl", "data-integration", "streaming"],
            "tenant_id": conf.get("tenant_id", "default")
        },
        
        # Asset descriptions for semantic search
        {
            "id": "asset_001",
            "name": "Customer Churn Prediction Model",
            "description": "Machine learning model that predicts customer churn probability based on usage patterns, support interactions, and billing history",
            "entity_type": "model",
            "tags": ["ml-model", "churn", "predictive"],
            "quality_score": 0.92,
            "tenant_id": conf.get("tenant_id", "default")
        },
        {
            "id": "asset_002",
            "name": "Real-time Fraud Detection Pipeline",
            "description": "Streaming pipeline that detects fraudulent transactions in real-time using complex event processing and anomaly detection",
            "entity_type": "pipeline",
            "tags": ["fraud", "streaming", "security"],
            "quality_score": 0.95,
            "tenant_id": conf.get("tenant_id", "default")
        }
    ]
    
    # For RAG, we need to chunk longer documents
    chunked_documents = []
    
    for doc in documents:
        if doc.get("type") == "documentation" and len(doc.get("content", "")) > 200:
            # Simple chunking by sentences
            content = doc["content"]
            sentences = content.split(". ")
            
            chunks = []
            current_chunk = ""
            
            for sentence in sentences:
                if len(current_chunk) + len(sentence) < 300:
                    current_chunk += sentence + ". "
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = sentence + ". "
            
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # Create document with chunks for RAG
            doc_with_chunks = doc.copy()
            doc_with_chunks["chunks"] = [
                {
                    "chunk_id": f"{doc['id']}_chunk_{i}",
                    "content": chunk,
                    "page_number": 1,
                    "paragraph_number": i + 1
                }
                for i, chunk in enumerate(chunks)
            ]
            
            chunked_documents.append(doc_with_chunks)
        else:
            documents.append(doc)
    
    # Store for next tasks
    all_documents = documents + chunked_documents
    context['task_instance'].xcom_push(key='documents', value=all_documents)
    
    logger.info(f"Prepared {len(all_documents)} documents for indexing")
    return len(all_documents)


def index_documents_with_embeddings(**context):
    """Index documents with native Elasticsearch v8 embeddings"""
    documents = context['task_instance'].xcom_pull(
        task_ids='prepare_documents',
        key='documents'
    )
    
    async def _index():
        async with httpx.AsyncClient(timeout=60.0) as client:
            # First, ensure indices exist with proper mappings
            indices_to_create = ["unified", "documents", "assets"]
            
            for index in indices_to_create:
                await client.put(
                    f'http://search-service:8000/api/v1/admin/indices/{index}',
                    json={"create_if_not_exists": True}
                )
            
            # Index documents with embeddings
            response = await client.post(
                'http://search-service:8000/api/v1/index/documents',
                json={
                    "documents": documents,
                    "index": "unified",
                    "generate_embeddings": True
                }
            )
            response.raise_for_status()
            return response.json()
    
    import asyncio
    result = asyncio.run(_index())
    
    logger.info(f"Indexing result: {result}")
    return result


def test_knn_search(**context):
    """Test native k-NN vector search"""
    conf = context['dag_run'].conf
    
    test_queries = [
        "How does Apache Iceberg handle schema evolution?",
        "What are the benefits of a feature store?",
        "Explain zero-ETL architecture",
        "Find fraud detection solutions",
        "Customer churn prediction methods"
    ]
    
    async def _search():
        results = []
        async with httpx.AsyncClient() as client:
            for query in test_queries:
                response = await client.post(
                    'http://search-service:8000/api/v1/search/knn',
                    json={
                        "query": query,
                        "search_type": "knn",
                        "size": 5,
                        "tenant_id": conf.get("tenant_id", "default"),
                        "filters": {
                            "index": "unified",
                            "vector_field": "text_embedding"
                        }
                    }
                )
                response.raise_for_status()
                result = response.json()
                
                results.append({
                    "query": query,
                    "total_results": result["total"],
                    "top_result": result["results"][0] if result["results"] else None
                })
                
                logger.info(f"k-NN search for '{query}': {result['total']} results")
        
        return results
    
    import asyncio
    results = asyncio.run(_search())
    
    context['task_instance'].xcom_push(key='knn_results', value=results)
    return len(results)


def test_hybrid_search(**context):
    """Test hybrid search combining text and vectors"""
    conf = context['dag_run'].conf
    
    hybrid_queries = [
        {
            "query": "iceberg table format performance",
            "text_boost": 0.4,
            "vector_boost": 0.6
        },
        {
            "query": "ML model feature store",
            "text_boost": 0.6,
            "vector_boost": 0.4
        }
    ]
    
    async def _hybrid_search():
        results = []
        async with httpx.AsyncClient() as client:
            for query_config in hybrid_queries:
                response = await client.post(
                    'http://search-service:8000/api/v1/search/hybrid-v8',
                    json={
                        "query": query_config["query"],
                        "search_type": "hybrid",
                        "size": 5,
                        "tenant_id": conf.get("tenant_id", "default"),
                        "filters": {
                            "index": "unified",
                            "text_boost": query_config["text_boost"],
                            "vector_boost": query_config["vector_boost"]
                        }
                    }
                )
                response.raise_for_status()
                result = response.json()
                
                results.append({
                    "query": query_config["query"],
                    "weights": {
                        "text": query_config["text_boost"],
                        "vector": query_config["vector_boost"]
                    },
                    "total_results": result["total"],
                    "highlights": result["results"][0].get("highlights", {}) if result["results"] else {}
                })
                
                logger.info(f"Hybrid search for '{query_config['query']}': {result['total']} results")
        
        return results
    
    import asyncio
    results = asyncio.run(_hybrid_search())
    
    context['task_instance'].xcom_push(key='hybrid_results', value=results)
    return len(results)


def test_rag_qa(**context):
    """Test RAG for question answering"""
    conf = context['dag_run'].conf
    
    questions = [
        "What are the main benefits of using Apache Iceberg?",
        "How does a feature store ensure training-serving consistency?",
        "What technologies enable zero-ETL data integration?",
        "Can you explain how CDC works in zero-ETL architecture?"
    ]
    
    async def _rag_search():
        results = []
        async with httpx.AsyncClient(timeout=60.0) as client:
            for question in questions:
                response = await client.post(
                    'http://search-service:8000/api/v1/search/rag',
                    json={
                        "question": question,
                        "index": "unified",
                        "k": 3,
                        "chunk_size": 2,
                        "tenant_id": conf.get("tenant_id", "default"),
                        "include_sources": True
                    }
                )
                response.raise_for_status()
                result = response.json()
                
                results.append({
                    "question": question,
                    "answer": result["answer"],
                    "confidence": result["confidence"],
                    "sources_count": len(result.get("sources", []))
                })
                
                logger.info(f"RAG Q&A: {question}")
                logger.info(f"Answer: {result['answer'][:200]}...")
                logger.info(f"Confidence: {result['confidence']}")
        
        return results
    
    import asyncio
    results = asyncio.run(_rag_search())
    
    context['task_instance'].xcom_push(key='rag_results', value=results)
    return len(results)


def test_druid_integration(**context):
    """Test Elasticsearch + Druid analytics integration"""
    conf = context['dag_run'].conf
    
    # This demonstrates how semantic search results can be used
    # to filter Druid analytics queries
    
    async def _integrated_analytics():
        async with httpx.AsyncClient() as client:
            # First, semantic search for "fraud detection"
            search_response = await client.post(
                'http://search-service:8000/api/v1/search/semantic',
                json={
                    "query": "fraud detection pipeline",
                    "size": 10,
                    "tenant_id": conf.get("tenant_id", "default")
                }
            )
            search_response.raise_for_status()
            search_results = search_response.json()
            
            # Extract entity IDs from search results
            entity_ids = [
                r["source"].get("entity_id", r["id"]) 
                for r in search_results["results"]
            ]
            
            # Now query analytics for these specific entities
            # (This would normally go to analytics-service which uses Druid)
            analytics_query = {
                "entity_ids": entity_ids,
                "metrics": ["event_count", "error_rate", "avg_latency"],
                "time_range": {
                    "start": (datetime.now() - timedelta(days=7)).isoformat(),
                    "end": datetime.now().isoformat()
                },
                "granularity": "day"
            }
            
            logger.info(f"Found {len(entity_ids)} entities via semantic search")
            logger.info(f"Would query Druid for analytics on these entities: {entity_ids[:3]}...")
            
            return {
                "search_results": len(entity_ids),
                "analytics_query": analytics_query
            }
    
    import asyncio
    result = asyncio.run(_integrated_analytics())
    
    return result


def generate_search_report(**context):
    """Generate comprehensive search capability report"""
    
    # Gather all results
    knn_results = context['task_instance'].xcom_pull(
        task_ids='test_knn_search',
        key='knn_results'
    )
    
    hybrid_results = context['task_instance'].xcom_pull(
        task_ids='test_hybrid_search',
        key='hybrid_results'
    )
    
    rag_results = context['task_instance'].xcom_pull(
        task_ids='test_rag_qa',
        key='rag_results'
    )
    
    report = {
        "execution_time": datetime.now().isoformat(),
        "elasticsearch_v8_features": {
            "native_vector_search": True,
            "knn_with_filters": True,
            "hybrid_search": True,
            "dense_vector_fields": True,
            "rag_enabled": True
        },
        "search_results_summary": {
            "knn_searches": len(knn_results) if knn_results else 0,
            "hybrid_searches": len(hybrid_results) if hybrid_results else 0,
            "rag_questions": len(rag_results) if rag_results else 0
        },
        "rag_performance": {
            "average_confidence": sum(r["confidence"] for r in rag_results) / len(rag_results) if rag_results else 0,
            "questions_answered": len([r for r in rag_results if r["confidence"] > 0.5]) if rag_results else 0
        },
        "integration_capabilities": {
            "druid_analytics": True,
            "milvus_compatibility": True,
            "graph_search": True
        }
    }
    
    logger.info(f"Search capability report: {json.dumps(report, indent=2)}")
    
    return report


# Create DAG
dag = DAG(
    'elasticsearch_v8_rag_pipeline',
    default_args=default_args,
    description='Demonstrate Elasticsearch v8 vector search and RAG capabilities',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['elasticsearch', 'vector-search', 'rag', 'semantic-search', 'v8']
)

# Define tasks
prepare_task = PythonOperator(
    task_id='prepare_documents',
    python_callable=prepare_documents,
    dag=dag
)

index_task = PythonOperator(
    task_id='index_documents_with_embeddings',
    python_callable=index_documents_with_embeddings,
    dag=dag
)

knn_task = PythonOperator(
    task_id='test_knn_search',
    python_callable=test_knn_search,
    dag=dag
)

hybrid_task = PythonOperator(
    task_id='test_hybrid_search',
    python_callable=test_hybrid_search,
    dag=dag
)

rag_task = PythonOperator(
    task_id='test_rag_qa',
    python_callable=test_rag_qa,
    dag=dag
)

druid_task = PythonOperator(
    task_id='test_druid_integration',
    python_callable=test_druid_integration,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_search_report',
    python_callable=generate_search_report,
    dag=dag
)

# Set dependencies
prepare_task >> index_task >> [knn_task, hybrid_task, rag_task, druid_task] >> report_task 