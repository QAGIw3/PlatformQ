"""
Apache Ignite client for caching and distributed computing
"""
import logging
from typing import Dict, Any, Optional, List
import asyncio
from pyignite import Client
from pyignite.datatypes import String, IntObject

logger = logging.getLogger(__name__)


class IgniteClient:
    """
    Client for Apache Ignite operations
    """
    
    def __init__(self, host: str, port: int = 10800):
        self.host = host
        self.port = port
        self.client: Optional[Client] = None
        
    async def initialize(self):
        """Initialize Ignite client"""
        try:
            loop = asyncio.get_event_loop()
            
            def _connect():
                client = Client()
                client.connect(self.host, self.port)
                return client
                
            self.client = await loop.run_in_executor(None, _connect)
            logger.info(f"Ignite client initialized: {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Ignite client: {str(e)}")
            raise
    
    async def create_cache(self, cache_name: str, config: Optional[Dict[str, Any]] = None):
        """Create a cache"""
        loop = asyncio.get_event_loop()
        
        def _create_cache():
            cache = self.client.get_or_create_cache(cache_name)
            return cache
            
        await loop.run_in_executor(None, _create_cache)
        logger.info(f"Cache created: {cache_name}")
    
    async def put(self, cache_name: str, key: str, value: Any):
        """Put value in cache"""
        loop = asyncio.get_event_loop()
        
        def _put():
            cache = self.client.get_cache(cache_name)
            cache.put(key, value)
            
        await loop.run_in_executor(None, _put)
    
    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        loop = asyncio.get_event_loop()
        
        def _get():
            cache = self.client.get_cache(cache_name)
            return cache.get(key)
            
        return await loop.run_in_executor(None, _get)
    
    async def remove(self, cache_name: str, key: str):
        """Remove value from cache"""
        loop = asyncio.get_event_loop()
        
        def _remove():
            cache = self.client.get_cache(cache_name)
            cache.remove_key(key)
            
        await loop.run_in_executor(None, _remove)
    
    async def close(self):
        """Close Ignite client"""
        if self.client:
            loop = asyncio.get_event_loop()
            
            def _close():
                self.client.close()
                
            await loop.run_in_executor(None, _close)
            logger.info("Ignite client closed") 