"""
Quantum Market Service

Manages quantum computing resource markets including QPU time, coherence windows, and entanglement pairs.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings
from .core.qpu_registry import QPURegistry
from .core.coherence_manager import CoherenceWindowManager
from .core.entanglement_exchange import EntanglementExchange
from .core.quantum_pricing import QuantumPricingEngine
from .core.algorithm_matcher import AlgorithmMatcher
from .integrations.blockchain import QuantumResourceBlockchain
from .integrations.ignite_cache import IgniteQuantumCache
from .integrations.pulsar_events import QuantumEventPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global instances
settings: Optional[Settings] = None
qpu_registry: Optional[QPURegistry] = None
coherence_manager: Optional[CoherenceWindowManager] = None
entanglement_exchange: Optional[EntanglementExchange] = None
pricing_engine: Optional[QuantumPricingEngine] = None
algorithm_matcher: Optional[AlgorithmMatcher] = None
blockchain: Optional[QuantumResourceBlockchain] = None
cache: Optional[IgniteQuantumCache] = None
event_publisher: Optional[QuantumEventPublisher] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global settings, qpu_registry, coherence_manager, entanglement_exchange
    global pricing_engine, algorithm_matcher, blockchain, cache, event_publisher
    
    logger.info("Starting Quantum Market Service...")
    
    # Initialize configuration
    settings = Settings()
    
    # Initialize cache
    cache = IgniteQuantumCache(settings.ignite_host, settings.ignite_port)
    await cache.connect()
    
    # Initialize event publisher
    event_publisher = QuantumEventPublisher(settings.pulsar_url)
    await event_publisher.connect()
    
    # Initialize blockchain integration
    blockchain = QuantumResourceBlockchain(
        settings.blockchain_rpc_url,
        settings.resource_token_address,
        settings.quantum_manager_address
    )
    await blockchain.connect()
    
    # Initialize core components
    qpu_registry = QPURegistry(cache, event_publisher)
    coherence_manager = CoherenceWindowManager(
        qpu_registry, 
        blockchain, 
        cache, 
        event_publisher
    )
    entanglement_exchange = EntanglementExchange(
        qpu_registry,
        cache,
        event_publisher
    )
    pricing_engine = QuantumPricingEngine(
        qpu_registry,
        coherence_manager,
        settings
    )
    algorithm_matcher = AlgorithmMatcher(
        qpu_registry,
        pricing_engine
    )
    
    # Start background tasks
    asyncio.create_task(coherence_manager.monitor_coherence_decay())
    asyncio.create_task(entanglement_exchange.monitor_pair_expiry())
    asyncio.create_task(pricing_engine.update_spot_prices())
    
    logger.info("Quantum Market Service started successfully")
    
    yield
    
    logger.info("Shutting down Quantum Market Service...")
    
    # Cleanup
    await cache.disconnect()
    await event_publisher.disconnect()
    await blockchain.disconnect()
    
    logger.info("Quantum Market Service shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="Quantum Market Service",
    description="Quantum computing resource marketplace",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
        "service": "Quantum Market Service",
        "version": "1.0.0",
        "status": "operational",
        "features": {
            "qpu_management": True,
            "coherence_windows": True,
            "entanglement_trading": True,
            "algorithm_matching": True,
            "quantum_arbitrage": True
        },
        "resources": {
            "registered_qpus": await qpu_registry.get_qpu_count(),
            "active_windows": await coherence_manager.get_active_window_count(),
            "available_pairs": await entanglement_exchange.get_available_pair_count()
        },
        "endpoints": {
            "qpus": "/api/v1/qpus",
            "coherence": "/api/v1/coherence",
            "entanglement": "/api/v1/entanglement",
            "algorithms": "/api/v1/algorithms",
            "pricing": "/api/v1/pricing"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "cache": await cache.health_check(),
            "blockchain": await blockchain.health_check(),
            "event_publisher": await event_publisher.health_check()
        }
    }
    
    # Check if any component is unhealthy
    if not all(health_status["components"].values()):
        health_status["status"] = "degraded"
        
    return health_status


# Import and include API routers
from .api import qpus, coherence, entanglement, algorithms, pricing, arbitrage

app.include_router(qpus.router, prefix="/api/v1/qpus", tags=["QPUs"])
app.include_router(coherence.router, prefix="/api/v1/coherence", tags=["Coherence Windows"])
app.include_router(entanglement.router, prefix="/api/v1/entanglement", tags=["Entanglement"])
app.include_router(algorithms.router, prefix="/api/v1/algorithms", tags=["Algorithms"])
app.include_router(pricing.router, prefix="/api/v1/pricing", tags=["Pricing"])
app.include_router(arbitrage.router, prefix="/api/v1/arbitrage", tags=["Arbitrage"])


# Background tasks

async def monitor_qpu_performance():
    """Background task to monitor QPU performance and update quality scores."""
    while True:
        try:
            qpus = await qpu_registry.get_all_active_qpus()
            
            for qpu in qpus:
                # Check recent execution results
                recent_windows = await coherence_manager.get_recent_windows(
                    qpu.qpu_id,
                    hours=24
                )
                
                if recent_windows:
                    # Calculate success rate
                    successful = sum(1 for w in recent_windows if w.success)
                    success_rate = successful / len(recent_windows)
                    
                    # Calculate average coherence achievement
                    coherence_achievements = [
                        w.actual_coherence / w.expected_coherence
                        for w in recent_windows
                        if w.actual_coherence > 0
                    ]
                    avg_coherence = sum(coherence_achievements) / len(coherence_achievements) if coherence_achievements else 0
                    
                    # Update quality score
                    quality_score = int((success_rate * 0.7 + avg_coherence * 0.3) * 10000)
                    
                    await blockchain.update_quality_score(
                        qpu.token_id,
                        quality_score
                    )
                    
                    # Publish metrics
                    await event_publisher.publish_qpu_metrics({
                        "qpu_id": qpu.qpu_id,
                        "success_rate": success_rate,
                        "avg_coherence_achievement": avg_coherence,
                        "quality_score": quality_score,
                        "timestamp": datetime.utcnow().isoformat()
                    })
            
            await asyncio.sleep(300)  # Run every 5 minutes
            
        except Exception as e:
            logger.error(f"Error in QPU performance monitoring: {e}")
            await asyncio.sleep(60)


async def process_expired_windows():
    """Background task to process expired coherence windows."""
    while True:
        try:
            expired_windows = await coherence_manager.get_expired_windows()
            
            for window in expired_windows:
                if not window.executed:
                    # Mark as failed
                    await coherence_manager.mark_window_failed(window.window_id)
                    
                    # Refund if applicable
                    if window.refundable:
                        await blockchain.refund_window(
                            window.token_id,
                            window.user_address,
                            window.price
                        )
                    
                    # Publish event
                    await event_publisher.publish_window_expired({
                        "window_id": window.window_id,
                        "qpu_id": window.qpu_id,
                        "user": window.user_address,
                        "refunded": window.refundable
                    })
            
            await asyncio.sleep(10)  # Check every 10 seconds
            
        except Exception as e:
            logger.error(f"Error processing expired windows: {e}")
            await asyncio.sleep(30)


async def update_entanglement_fidelities():
    """Background task to update entanglement pair fidelities based on decay."""
    while True:
        try:
            active_pairs = await entanglement_exchange.get_active_pairs()
            
            for pair in active_pairs:
                # Calculate fidelity decay
                age = (datetime.utcnow() - pair.creation_time).total_seconds() * 1e6  # to microseconds
                decay_factor = 1 - (age / pair.expected_lifetime)
                
                if decay_factor <= 0:
                    # Pair has decayed completely
                    await entanglement_exchange.mark_pair_expired(pair.pair_id)
                else:
                    # Update fidelity
                    new_fidelity = pair.initial_fidelity * decay_factor
                    await entanglement_exchange.update_fidelity(
                        pair.pair_id,
                        new_fidelity
                    )
                    
                    # Alert if fidelity drops below threshold
                    if new_fidelity < settings.min_entanglement_fidelity:
                        await event_publisher.publish_low_fidelity_alert({
                            "pair_id": pair.pair_id,
                            "current_fidelity": new_fidelity,
                            "owner": pair.owner
                        })
            
            await asyncio.sleep(1)  # Check every second
            
        except Exception as e:
            logger.error(f"Error updating entanglement fidelities: {e}")
            await asyncio.sleep(5)


# Start background tasks
@app.on_event("startup")
async def startup_tasks():
    """Start background tasks."""
    asyncio.create_task(monitor_qpu_performance())
    asyncio.create_task(process_expired_windows())
    asyncio.create_task(update_entanglement_fidelities())


# Error handlers

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions."""
    return {
        "error": exc.detail,
        "status_code": exc.status_code,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}")
    return {
        "error": "Internal server error",
        "status_code": 500,
        "timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8024,
        reload=True,
        log_level="info"
    ) 