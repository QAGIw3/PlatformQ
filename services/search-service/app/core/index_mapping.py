INDEX_MAPPING = {
    "properties": {
        "entity_id": {"type": "keyword"},
        "entity_type": {"type": "keyword"},
        "source_service": {"type": "keyword"},
        "event_timestamp": {"type": "date"},
        "name": {"type": "text", "analyzer": "standard"},
        "description": {"type": "text", "analyzer": "standard"},
        "tags": {"type": "keyword"},
        "owner": {"type": "keyword"},
        "layer": {"type": "keyword"},
        "quality_score": {"type": "double"},
        "data": {
            "type": "object",
            "dynamic": True
        }
    }
} 