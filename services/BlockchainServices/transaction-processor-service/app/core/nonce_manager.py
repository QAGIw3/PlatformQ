"""
Nonce Manager - Manages transaction nonces for each address
"""

import asyncio
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from pyignite import AsyncClient as IgniteClient
import httpx

from ..config import Settings

logger = logging.getLogger(__name__)


class NonceManager:
    """Manages nonces for blockchain addresses"""
    
    def __init__(self, ignite_client: IgniteClient, settings: Settings):
        self.ignite = ignite_client
        self.settings = settings
        
        # Nonce tracking
        self._nonce_cache = None
        self._pending_nonces: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._nonce_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        
        # HTTP client for blockchain queries
        self.http_client = httpx.AsyncClient(timeout=10.0)
        
        # Background task for syncing
        self._sync_task = None
        self._running = False
        
    async def start(self):
        """Start nonce manager"""
        logger.info("Starting Nonce Manager")
        
        # Initialize cache
        self._nonce_cache = await self.ignite.get_or_create_cache("nonces")
        
        # Start background sync
        self._running = True
        self._sync_task = asyncio.create_task(self._sync_nonces())
        
        logger.info("Nonce Manager started")
        
    async def stop(self):
        """Stop nonce manager"""
        logger.info("Stopping Nonce Manager")
        
        self._running = False
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
                
        await self.http_client.aclose()
        
        logger.info("Nonce Manager stopped")
        
    async def get_nonce(self, chain: str, address: str) -> int:
        """Get next available nonce for an address"""
        cache_key = f"{chain}:{address.lower()}"
        
        async with self._nonce_locks[cache_key]:
            # Get base nonce from cache or blockchain
            base_nonce = await self._get_base_nonce(chain, address)
            
            # Add pending nonces
            pending_count = self._pending_nonces[chain][address.lower()]
            nonce = base_nonce + pending_count
            
            # Increment pending count
            self._pending_nonces[chain][address.lower()] += 1
            
            logger.debug(f"Allocated nonce {nonce} for {address} on {chain}")
            return nonce
            
    async def release_nonce(self, chain: str, address: str, nonce: int):
        """Release a nonce (transaction failed before broadcast)"""
        cache_key = f"{chain}:{address.lower()}"
        
        async with self._nonce_locks[cache_key]:
            # Decrement pending count
            if self._pending_nonces[chain][address.lower()] > 0:
                self._pending_nonces[chain][address.lower()] -= 1
                
            logger.debug(f"Released nonce {nonce} for {address} on {chain}")
            
    async def confirm_nonce(self, chain: str, address: str, nonce: int):
        """Confirm a nonce was used (transaction broadcast)"""
        cache_key = f"{chain}:{address.lower()}"
        
        async with self._nonce_locks[cache_key]:
            # Update base nonce if this is the next expected
            cached_data = await self._nonce_cache.get(cache_key)
            if cached_data:
                base_nonce = cached_data['nonce']
                if nonce == base_nonce:
                    # This is the next sequential nonce
                    cached_data['nonce'] = nonce + 1
                    cached_data['updated_at'] = datetime.utcnow().isoformat()
                    await self._nonce_cache.put(cache_key, cached_data)
                    
            # Decrement pending count
            if self._pending_nonces[chain][address.lower()] > 0:
                self._pending_nonces[chain][address.lower()] -= 1
                
            logger.debug(f"Confirmed nonce {nonce} for {address} on {chain}")
            
    async def _get_base_nonce(self, chain: str, address: str) -> int:
        """Get base nonce from cache or blockchain"""
        cache_key = f"{chain}:{address.lower()}"
        
        # Check cache
        cached_data = await self._nonce_cache.get(cache_key)
        if cached_data:
            # Check if cache is still valid
            updated_at = datetime.fromisoformat(cached_data['updated_at'])
            if datetime.utcnow() - updated_at < timedelta(seconds=self.settings.NONCE_CACHE_TTL):
                return cached_data['nonce']
                
        # Query blockchain
        try:
            # Get blockchain connector URL from service discovery
            blockchain_url = "http://blockchain-connector:8010"  # TODO: Get from Consul
            
            response = await self.http_client.get(
                f"{blockchain_url}/api/v1/nonce/{chain}/{address}"
            )
            response.raise_for_status()
            
            nonce = response.json()['nonce']
            
            # Update cache
            await self._nonce_cache.put(cache_key, {
                'nonce': nonce,
                'updated_at': datetime.utcnow().isoformat()
            })
            
            return nonce
            
        except Exception as e:
            logger.error(f"Error getting nonce for {address} on {chain}: {e}")
            # Return cached value if available
            if cached_data:
                return cached_data['nonce']
            raise
            
    async def _sync_nonces(self):
        """Background task to sync nonces with blockchain"""
        while self._running:
            try:
                # Get all cached nonces
                cursor = self._nonce_cache.scan()
                
                for cache_key, cached_data in cursor:
                    if not self._running:
                        break
                        
                    try:
                        # Parse cache key
                        chain, address = cache_key.split(':', 1)
                        
                        # Check if sync needed
                        updated_at = datetime.fromisoformat(cached_data['updated_at'])
                        if datetime.utcnow() - updated_at > timedelta(seconds=60):
                            # Re-fetch from blockchain
                            await self._get_base_nonce(chain, address)
                            
                    except Exception as e:
                        logger.error(f"Error syncing nonce for {cache_key}: {e}")
                        
                # Wait before next sync
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"Error in nonce sync: {e}")
                await asyncio.sleep(5)
                
    async def reset_nonce(self, chain: str, address: str):
        """Reset nonce for an address (used for error recovery)"""
        cache_key = f"{chain}:{address.lower()}"
        
        async with self._nonce_locks[cache_key]:
            # Clear pending nonces
            self._pending_nonces[chain][address.lower()] = 0
            
            # Clear cache to force re-fetch
            await self._nonce_cache.remove(cache_key)
            
            logger.info(f"Reset nonce for {address} on {chain}") 