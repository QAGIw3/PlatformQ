"""
Elasticsearch v8 Index Mappings with Vector Search Support

Defines mappings for different entity types with:
- Dense vector fields for semantic search
- Text fields with multiple analyzers
- Completion fields for autocomplete
- Keyword fields for exact matching
"""

# Base mapping for all entities with vector support
BASE_MAPPING = {
    "properties": {
        # Core fields
        "entity_id": {"type": "keyword"},
        "entity_type": {"type": "keyword"},
        "source_service": {"type": "keyword"},
        "tenant_id": {"type": "keyword"},
        "event_timestamp": {"type": "date"},
        
        # Text fields with multiple analyzers
        "name": {
            "type": "text",
            "analyzer": "standard",
            "fields": {
                "keyword": {"type": "keyword"},
                "suggest": {"type": "completion"},
                "ngram": {"type": "text", "analyzer": "ngram_analyzer"}
            }
        },
        "title": {
            "type": "text",
            "analyzer": "standard",
            "fields": {
                "keyword": {"type": "keyword"},
                "suggest": {"type": "completion"}
            }
        },
        "description": {
            "type": "text",
            "analyzer": "standard",
            "fields": {
                "english": {"type": "text", "analyzer": "english"}
            }
        },
        "content": {
            "type": "text",
            "analyzer": "standard",
            "term_vector": "with_positions_offsets",
            "fields": {
                "english": {"type": "text", "analyzer": "english"}
            }
        },
        
        # Vector fields for semantic search
        "text_embedding": {
            "type": "dense_vector",
            "dims": 768,  # For BERT-based models
            "index": True,
            "similarity": "cosine"
        },
        "title_embedding": {
            "type": "dense_vector",
            "dims": 768,
            "index": True,
            "similarity": "cosine"
        },
        
        # Structured fields
        "tags": {"type": "keyword"},
        "categories": {"type": "keyword"},
        "owner": {"type": "keyword"},
        "collaborators": {"type": "keyword"},
        "status": {"type": "keyword"},
        
        # Metrics
        "quality_score": {"type": "float"},
        "relevance_score": {"type": "float"},
        "popularity_score": {"type": "float"},
        
        # Nested metadata
        "metadata": {
            "type": "object",
            "dynamic": True
        },
        
        # Geospatial
        "location": {"type": "geo_point"},
        
        # For highlighting
        "highlight_fields": {
            "type": "object",
            "enabled": False
        }
    }
}

# Digital Assets specific mapping
ASSET_MAPPING = {
    **BASE_MAPPING,
    "properties": {
        **BASE_MAPPING["properties"],
        "asset_type": {"type": "keyword"},
        "file_format": {"type": "keyword"},
        "file_size": {"type": "long"},
        "mime_type": {"type": "keyword"},
        "dimensions": {
            "properties": {
                "width": {"type": "integer"},
                "height": {"type": "integer"},
                "depth": {"type": "integer"}
            }
        },
        "image_embedding": {
            "type": "dense_vector",
            "dims": 512,  # For CLIP or similar models
            "index": True,
            "similarity": "cosine"
        },
        "thumbnail_url": {"type": "keyword"},
        "preview_url": {"type": "keyword"},
        "download_url": {"type": "keyword"},
        "blockchain_hash": {"type": "keyword"},
        "ipfs_hash": {"type": "keyword"}
    }
}

# Documents specific mapping (for RAG)
DOCUMENT_MAPPING = {
    **BASE_MAPPING,
    "properties": {
        **BASE_MAPPING["properties"],
        "document_type": {"type": "keyword"},
        "language": {"type": "keyword"},
        "page_count": {"type": "integer"},
        "word_count": {"type": "integer"},
        
        # For RAG - chunked content with embeddings
        "chunks": {
            "type": "nested",
            "properties": {
                "chunk_id": {"type": "keyword"},
                "content": {"type": "text"},
                "chunk_embedding": {
                    "type": "dense_vector",
                    "dims": 768,
                    "index": True,
                    "similarity": "cosine"
                },
                "page_number": {"type": "integer"},
                "paragraph_number": {"type": "integer"},
                "start_char": {"type": "integer"},
                "end_char": {"type": "integer"}
            }
        },
        
        # Document-level embedding
        "document_embedding": {
            "type": "dense_vector",
            "dims": 768,
            "index": True,
            "similarity": "cosine"
        }
    }
}

# Code/Simulation specific mapping
CODE_MAPPING = {
    **BASE_MAPPING,
    "properties": {
        **BASE_MAPPING["properties"],
        "language": {"type": "keyword"},
        "framework": {"type": "keyword"},
        "dependencies": {"type": "keyword"},
        "code_embedding": {
            "type": "dense_vector",
            "dims": 768,  # For CodeBERT or similar
            "index": True,
            "similarity": "cosine"
        },
        "functions": {
            "type": "nested",
            "properties": {
                "name": {"type": "keyword"},
                "signature": {"type": "text"},
                "docstring": {"type": "text"},
                "embedding": {
                    "type": "dense_vector",
                    "dims": 768,
                    "index": True,
                    "similarity": "cosine"
                }
            }
        }
    }
}

# Project/Workflow mapping
PROJECT_MAPPING = {
    **BASE_MAPPING,
    "properties": {
        **BASE_MAPPING["properties"],
        "project_type": {"type": "keyword"},
        "start_date": {"type": "date"},
        "end_date": {"type": "date"},
        "status": {"type": "keyword"},
        "team_members": {"type": "keyword"},
        "milestones": {
            "type": "nested",
            "properties": {
                "name": {"type": "text"},
                "due_date": {"type": "date"},
                "status": {"type": "keyword"}
            }
        }
    }
}

# Graph entity mapping
GRAPH_ENTITY_MAPPING = {
    **BASE_MAPPING,
    "properties": {
        **BASE_MAPPING["properties"],
        "node_type": {"type": "keyword"},
        "node_id": {"type": "keyword"},
        "relationships": {
            "type": "nested",
            "properties": {
                "type": {"type": "keyword"},
                "target_id": {"type": "keyword"},
                "weight": {"type": "float"},
                "properties": {"type": "object", "dynamic": True}
            }
        },
        "graph_embedding": {
            "type": "dense_vector",
            "dims": 128,  # For graph embeddings (Node2Vec, GraphSAGE)
            "index": True,
            "similarity": "cosine"
        }
    }
}

# Index settings for Elasticsearch v8
INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 1,
        "index.mapping.coerce": True,
        
        # Analysis settings
        "analysis": {
            "analyzer": {
                "ngram_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "ngram_filter"]
                },
                "code_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "code_synonyms"]
                }
            },
            "filter": {
                "ngram_filter": {
                    "type": "ngram",
                    "min_gram": 2,
                    "max_gram": 4
                },
                "code_synonyms": {
                    "type": "synonym",
                    "synonyms": [
                        "func,function,method",
                        "var,variable",
                        "param,parameter,arg,argument"
                    ]
                }
            }
        }
    }
}

# Mapping registry
INDEX_MAPPING = {
    "assets": {**INDEX_SETTINGS, "mappings": ASSET_MAPPING},
    "documents": {**INDEX_SETTINGS, "mappings": DOCUMENT_MAPPING},
    "code": {**INDEX_SETTINGS, "mappings": CODE_MAPPING},
    "projects": {**INDEX_SETTINGS, "mappings": PROJECT_MAPPING},
    "workflows": {**INDEX_SETTINGS, "mappings": PROJECT_MAPPING},
    "graph_entities": {**INDEX_SETTINGS, "mappings": GRAPH_ENTITY_MAPPING},
    
    # Unified search index with all fields
    "unified": {
        **INDEX_SETTINGS,
        "mappings": {
            "properties": {
                **ASSET_MAPPING["properties"],
                **DOCUMENT_MAPPING["properties"],
                **CODE_MAPPING["properties"],
                **PROJECT_MAPPING["properties"],
                **GRAPH_ENTITY_MAPPING["properties"]
            }
        }
    }
} 