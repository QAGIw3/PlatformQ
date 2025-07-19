"""
Core modules for collaboration platform
"""

from .session_manager import SessionManager, CollaborationSession, UserPresence

__all__ = [
    "SessionManager",
    "CollaborationSession",
    "UserPresence"
] 