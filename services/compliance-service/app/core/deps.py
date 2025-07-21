"""
Dependency injection for the compliance service
"""

from fastapi import Request, Depends, HTTPException
from typing import Dict, Any, Optional

from platformq_shared import security as shared_security

# Use shared security function for getting current user from trusted header
get_current_tenant_and_user = shared_security.get_current_user_from_trusted_header


def get_fraud_engine(request: Request):
    """Get fraud detection engine from app state"""
    if hasattr(request.app.state, 'fraud_engine'):
        return request.app.state.fraud_engine
    raise HTTPException(status_code=500, detail="Fraud engine not initialized")


def get_graph_client(request: Request):
    """Get graph intelligence client from app state"""
    if hasattr(request.app.state, 'graph_client'):
        return request.app.state.graph_client
    raise HTTPException(status_code=500, detail="Graph client not initialized") 