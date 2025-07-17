from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from elasticsearch import AsyncElasticsearch
from ..dependencies import get_es_client

router = APIRouter()

@router.get("/search")
async def search(
    q: str,
    es_client: AsyncElasticsearch = Depends(get_es_client),
    index_name: str = "platformq_search",
    size: int = 10,
    from_: int = 0
):
    """Perform a search across all indexed entities"""
    try:
        query = {
            "from": from_,
            "size": size,
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["name", "description", "tags"]
                }
            }
        }
        
        response = await es_client.search(
            index=index_name,
            body=query
        )
        
        return {
            "total": response["hits"]["total"]["value"],
            "results": [hit["_source"] for hit in response["hits"]["hits"]]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
