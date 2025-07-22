"""
Database Abstraction Layer

Uses the Unified Data Gateway Service for all database operations.
"""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

import httpx
from platformq_shared import ServiceClients

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Client for interacting with databases through the Unified Data Gateway"""
    
    def __init__(self, gateway_url: str = "http://unified-data-gateway-service:8000"):
        self.gateway_url = gateway_url
        self.client = httpx.AsyncClient(base_url=gateway_url, timeout=30.0)
        
    async def query(self, 
                   query: str,
                   context: Optional[Dict[str, Any]] = None,
                   target_database: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Execute a query through the data gateway"""
        
        payload = {
            "query": query,
            "context": context or {},
            "target_database": target_database
        }
        
        response = await self.client.post(
            "/api/v1/query/execute",
            json=payload
        )
        
        response.raise_for_status()
        return response.json()
        
    async def multi_query(self, queries: List[Dict[str, Any]], 
                         context: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute multiple queries in parallel"""
        
        payload = {
            "queries": queries,
            "context": context or {}
        }
        
        response = await self.client.post(
            "/api/v1/query/multi",
            json=payload
        )
        
        response.raise_for_status()
        return response.json()
        
    async def explain(self, query: str, 
                     context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get query execution plan"""
        
        payload = {
            "query": query,
            "context": context or {}
        }
        
        response = await self.client.post(
            "/api/v1/query/explain",
            json=payload
        )
        
        response.raise_for_status()
        return response.json()
        
    async def close(self):
        """Close the client connection"""
        await self.client.aclose()


class PostgreSQLAdapter:
    """Adapter for PostgreSQL operations through the gateway"""
    
    def __init__(self, db_client: DatabaseClient, database: str = "platformq"):
        self.db_client = db_client
        self.database = database
        self.target_db = {
            "type": "postgresql",
            "name": database
        }
        
    async def fetch(self, query: str, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch rows from PostgreSQL"""
        
        result = await self.db_client.query(
            query=query,
            context={"tenant_id": tenant_id} if tenant_id else {},
            target_database=self.target_db
        )
        
        if result.get("error"):
            raise Exception(f"Query failed: {result['error']}")
            
        return result.get("data", [])
        
    async def fetchrow(self, query: str, tenant_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Fetch a single row"""
        
        rows = await self.fetch(query + " LIMIT 1", tenant_id)
        return rows[0] if rows else None
        
    async def execute(self, query: str, tenant_id: Optional[str] = None) -> Any:
        """Execute a query without returning results"""
        
        result = await self.db_client.query(
            query=query,
            context={"tenant_id": tenant_id} if tenant_id else {},
            target_database=self.target_db
        )
        
        if result.get("error"):
            raise Exception(f"Query failed: {result['error']}")
            
        return result.get("data")


# Global database client instance
_db_client: Optional[DatabaseClient] = None
_pg_adapter: Optional[PostgreSQLAdapter] = None


async def get_db_client() -> DatabaseClient:
    """Get or create database client"""
    global _db_client
    
    if _db_client is None:
        _db_client = DatabaseClient()
        
    return _db_client


async def get_pg_adapter() -> PostgreSQLAdapter:
    """Get or create PostgreSQL adapter"""
    global _pg_adapter
    
    if _pg_adapter is None:
        db_client = await get_db_client()
        _pg_adapter = PostgreSQLAdapter(db_client)
        
    return _pg_adapter


# Compatibility layer for existing code
async def get_db_session():
    """
    Compatibility function that returns a database adapter
    instead of a direct database session.
    """
    return await get_pg_adapter() 