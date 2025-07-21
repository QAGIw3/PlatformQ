"""Structured Products Service - Main Application."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
import httpx

from .api import products, templates
from .models import ProductType
from .core.engine import StructuredProductEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Structured Products Service",
    description="Service for creating and managing structured financial products",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(products.router)
app.include_router(templates.router)

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Structured Products Service",
        "version": "1.0.0",
        "status": "operational",
        "supported_products": [
            "autocallable_note",
            "reverse_convertible",
            "range_accrual",
            "accumulator",
            "volatility_target"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "structured-products-service"
    }

@app.on_event("startup")
async def startup_event():
    """Initialize service on startup."""
    logger.info("Structured Products Service starting up...")
    # TODO: Initialize connections to Ignite, Pulsar, etc.
    
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    logger.info("Structured Products Service shutting down...")
    # TODO: Close connections gracefully 