"""
Presentation Session Manager using Apache Ignite
"""

import json
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone, timedelta

from pyignite import AsyncClient
from pyignite.datatypes import String


class SessionManager:
    """
    Manages presentation sessions using Apache Ignite for distributed storage
    """
    
    def __init__(
        self,
        ignite_host: str = "localhost",
        ignite_port: int = 10800,
        session_ttl: int = 3600  # 1 hour default
    ):
        self.ignite_host = ignite_host
        self.ignite_port = ignite_port
        self.session_ttl = session_ttl
        self.client: Optional[AsyncClient] = None
        self.connected = False
        
        # Cache name
        self.SESSION_CACHE = "presentation_sessions"
        
        # Statistics
        self.sessions_created = 0
        self.sessions_accessed = 0
    
    async def connect(self):
        """Connect to Apache Ignite"""
        if self.connected:
            return
            
        try:
            self.client = AsyncClient()
            await self.client.connect(self.ignite_host, self.ignite_port)
            
            # Create session cache
            self.cache = await self.client.get_or_create_cache(self.SESSION_CACHE)
            
            self.connected = True
            print(f"Connected to Apache Ignite at {self.ignite_host}:{self.ignite_port}")
            
        except Exception as e:
            print(f"Failed to connect to Apache Ignite: {str(e)}")
            self.connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Apache Ignite"""
        if self.client and self.connected:
            await self.client.close()
            self.connected = False
    
    async def create_session(
        self,
        holder_did: str,
        verifier_did: str,
        presentation_id: Optional[str] = None,
        challenge: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new presentation session
        
        Args:
            holder_did: DID of the credential holder
            verifier_did: DID of the verifier
            presentation_id: Optional ID of associated presentation
            challenge: Optional challenge nonce
            metadata: Additional session metadata
            
        Returns:
            Created session details
        """
        if not self.connected:
            raise RuntimeError("Not connected to Ignite")
        
        # Generate session ID
        session_id = f"session-{uuid.uuid4()}"
        
        # Create session data
        session = {
            "id": session_id,
            "holder_did": holder_did,
            "verifier_did": verifier_did,
            "presentation_id": presentation_id,
            "challenge": challenge or str(uuid.uuid4()),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expires_at": (datetime.now(timezone.utc) + timedelta(seconds=self.session_ttl)).isoformat(),
            "status": "active",
            "metadata": metadata or {},
            "events": [{
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "event": "session_created"
            }]
        }
        
        # Store in Ignite with TTL
        await self.cache.put(
            session_id,
            json.dumps(session),
            ttl=self.session_ttl * 1000  # Convert to milliseconds
        )
        
        self.sessions_created += 1
        
        return session
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session by ID"""
        if not self.connected:
            return None
            
        try:
            cached_data = await self.cache.get(session_id)
            
            if cached_data:
                self.sessions_accessed += 1
                session = json.loads(cached_data)
                
                # Check if expired
                expires_at = datetime.fromisoformat(session["expires_at"].replace('Z', '+00:00'))
                if expires_at < datetime.now(timezone.utc):
                    await self.invalidate_session(session_id)
                    return None
                
                return session
                
            return None
            
        except Exception as e:
            print(f"Error getting session {session_id}: {str(e)}")
            return None
    
    async def update_session(
        self,
        session_id: str,
        updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update session data"""
        if not self.connected:
            return None
            
        # Get existing session
        session = await self.get_session(session_id)
        if not session:
            return None
        
        # Update fields
        for key, value in updates.items():
            if key not in ["id", "created_at", "expires_at"]:  # Protect immutable fields
                session[key] = value
        
        # Add event
        session["events"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": "session_updated",
            "updates": list(updates.keys())
        })
        
        # Calculate remaining TTL
        expires_at = datetime.fromisoformat(session["expires_at"].replace('Z', '+00:00'))
        remaining_ttl = int((expires_at - datetime.now(timezone.utc)).total_seconds())
        
        if remaining_ttl > 0:
            # Store updated session
            await self.cache.put(
                session_id,
                json.dumps(session),
                ttl=remaining_ttl * 1000
            )
            return session
        else:
            # Session expired
            await self.invalidate_session(session_id)
            return None
    
    async def extend_session(
        self,
        session_id: str,
        additional_seconds: int = 3600
    ) -> Optional[Dict[str, Any]]:
        """Extend session expiration"""
        if not self.connected:
            return None
            
        session = await self.get_session(session_id)
        if not session:
            return None
        
        # Calculate new expiration
        current_expires = datetime.fromisoformat(session["expires_at"].replace('Z', '+00:00'))
        new_expires = current_expires + timedelta(seconds=additional_seconds)
        
        # Update session
        session["expires_at"] = new_expires.isoformat()
        session["events"].append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": "session_extended",
            "additional_seconds": additional_seconds
        })
        
        # Store with new TTL
        new_ttl = int((new_expires - datetime.now(timezone.utc)).total_seconds())
        await self.cache.put(
            session_id,
            json.dumps(session),
            ttl=new_ttl * 1000
        )
        
        return session
    
    async def invalidate_session(self, session_id: str):
        """Invalidate/delete a session"""
        if not self.connected:
            return
            
        try:
            # Get session for final event
            session = await self.get_session(session_id)
            if session:
                session["status"] = "invalidated"
                session["events"].append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "event": "session_invalidated"
                })
                
                # Store briefly for audit
                await self.cache.put(
                    session_id,
                    json.dumps(session),
                    ttl=60000  # Keep for 1 minute
                )
            
            # Then remove
            await self.cache.remove(session_id)
            
        except Exception as e:
            print(f"Error invalidating session {session_id}: {str(e)}")
    
    async def get_active_sessions(
        self,
        holder_did: Optional[str] = None,
        verifier_did: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get active sessions filtered by holder or verifier
        Note: This is a simplified implementation. In production,
        you'd want to use Ignite SQL queries for efficiency.
        """
        if not self.connected:
            return []
            
        active_sessions = []
        
        try:
            # This would be more efficient with Ignite SQL
            # For now, we'll scan the cache (not recommended for large datasets)
            # In production, maintain separate indexes
            
            # Get all keys (limited approach)
            # Real implementation would use Ignite queries
            sessions_found = []
            
            for session in sessions_found:
                # Filter by holder/verifier if specified
                if holder_did and session.get("holder_did") != holder_did:
                    continue
                if verifier_did and session.get("verifier_did") != verifier_did:
                    continue
                    
                active_sessions.append(session)
            
            return active_sessions
            
        except Exception as e:
            print(f"Error getting active sessions: {str(e)}")
            return []
    
    async def add_presentation_to_session(
        self,
        session_id: str,
        presentation_id: str
    ) -> Optional[Dict[str, Any]]:
        """Add presentation to session"""
        return await self.update_session(
            session_id,
            {
                "presentation_id": presentation_id,
                "presentation_added_at": datetime.now(timezone.utc).isoformat()
            }
        )
    
    async def add_verification_result_to_session(
        self,
        session_id: str,
        verification_result: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Add verification result to session"""
        return await self.update_session(
            session_id,
            {
                "verification_result": verification_result,
                "verified_at": datetime.now(timezone.utc).isoformat(),
                "status": "verified" if verification_result.get("valid") else "rejected"
            }
        )
    
    async def get_session_by_challenge(self, challenge: str) -> Optional[Dict[str, Any]]:
        """
        Get session by challenge nonce
        Note: This requires maintaining a challenge->session_id index in production
        """
        if not self.connected:
            return None
            
        # In production, maintain a separate challenge index
        # For now, this is a placeholder
        return None
    
    async def cleanup_expired_sessions(self):
        """
        Cleanup expired sessions
        Note: Ignite handles TTL expiration automatically,
        this is for additional cleanup if needed
        """
        # Ignite automatically removes expired entries
        pass
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get session manager statistics"""
        return {
            "connected": self.connected,
            "sessions_created": self.sessions_created,
            "sessions_accessed": self.sessions_accessed,
            "cache_name": self.SESSION_CACHE,
            "default_ttl": self.session_ttl
        }
    
    async def health_check(self) -> bool:
        """Check if session manager is healthy"""
        if not self.connected:
            return False
            
        try:
            # Try to access cache
            test_key = "_health_check"
            await self.cache.put(test_key, "ok", ttl=1000)
            result = await self.cache.get(test_key)
            await self.cache.remove(test_key)
            return result == "ok"
        except Exception:
            return False 