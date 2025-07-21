"""
AI Compute Market Service

Manages the marketplace for AI accelerators including TPUs, NPUs, and custom ASICs.
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
from .core.accelerator_registry import AcceleratorRegistry
from .core.performance_benchmarker import PerformanceBenchmarker
from .core.workload_scheduler import WorkloadScheduler
from .core.training_manager import TrainingManager
from .core.inference_router import InferenceRouter
from .core.pricing_engine import AIComputePricingEngine
from .core.thermal_monitor import ThermalMonitor
from .integrations.blockchain import AIAcceleratorBlockchain
from .integrations.ignite_cache import IgniteAICache
from .integrations.pulsar_events import AIComputeEventPublisher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global instances
settings: Optional[Settings] = None
accelerator_registry: Optional[AcceleratorRegistry] = None
performance_benchmarker: Optional[PerformanceBenchmarker] = None
workload_scheduler: Optional[WorkloadScheduler] = None
training_manager: Optional[TrainingManager] = None
inference_router: Optional[InferenceRouter] = None
pricing_engine: Optional[AIComputePricingEngine] = None
thermal_monitor: Optional[ThermalMonitor] = None
blockchain: Optional[AIAcceleratorBlockchain] = None
cache: Optional[IgniteAICache] = None
event_publisher: Optional[AIComputeEventPublisher] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global settings, accelerator_registry, performance_benchmarker, workload_scheduler
    global training_manager, inference_router, pricing_engine, thermal_monitor
    global blockchain, cache, event_publisher
    
    logger.info("Starting AI Compute Market Service...")
    
    # Initialize configuration
    settings = Settings()
    
    # Initialize cache
    cache = IgniteAICache(settings.ignite_host, settings.ignite_port)
    await cache.connect()
    
    # Initialize event publisher
    event_publisher = AIComputeEventPublisher(settings.pulsar_url)
    await event_publisher.connect()
    
    # Initialize blockchain integration
    blockchain = AIAcceleratorBlockchain(
        settings.blockchain_rpc_url,
        settings.resource_token_address,
        settings.ai_registry_address
    )
    await blockchain.connect()
    
    # Initialize core components
    accelerator_registry = AcceleratorRegistry(cache, event_publisher)
    performance_benchmarker = PerformanceBenchmarker(
        accelerator_registry,
        cache,
        event_publisher
    )
    thermal_monitor = ThermalMonitor(
        accelerator_registry,
        event_publisher
    )
    pricing_engine = AIComputePricingEngine(
        accelerator_registry,
        performance_benchmarker,
        settings
    )
    workload_scheduler = WorkloadScheduler(
        accelerator_registry,
        pricing_engine,
        cache
    )
    training_manager = TrainingManager(
        accelerator_registry,
        workload_scheduler,
        blockchain,
        cache,
        event_publisher
    )
    inference_router = InferenceRouter(
        accelerator_registry,
        workload_scheduler,
        pricing_engine,
        cache
    )
    
    # Start background tasks
    asyncio.create_task(thermal_monitor.monitor_temperatures())
    asyncio.create_task(performance_benchmarker.validate_benchmarks())
    asyncio.create_task(pricing_engine.update_spot_prices())
    asyncio.create_task(training_manager.monitor_training_progress())
    
    logger.info("AI Compute Market Service started successfully")
    
    yield
    
    logger.info("Shutting down AI Compute Market Service...")
    
    # Cleanup
    await cache.disconnect()
    await event_publisher.disconnect()
    await blockchain.disconnect()
    
    logger.info("AI Compute Market Service shutdown complete")


# Create FastAPI application
app = FastAPI(
    title="AI Compute Market Service",
    description="Marketplace for AI accelerators",
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
        "service": "AI Compute Market Service",
        "version": "1.0.0",
        "status": "operational",
        "features": {
            "accelerator_types": ["TPU", "NPU", "ASIC", "FPGA"],
            "training_contracts": True,
            "inference_routing": True,
            "performance_benchmarking": True,
            "thermal_monitoring": True
        },
        "resources": {
            "registered_accelerators": await accelerator_registry.get_accelerator_count(),
            "active_training_jobs": await training_manager.get_active_job_count(),
            "inference_requests_today": await inference_router.get_daily_request_count()
        },
        "endpoints": {
            "accelerators": "/api/v1/accelerators",
            "training": "/api/v1/training",
            "inference": "/api/v1/inference",
            "pricing": "/api/v1/pricing",
            "performance": "/api/v1/performance"
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
from .api import accelerators, training, inference, pricing, performance, markets

app.include_router(accelerators.router, prefix="/api/v1/accelerators", tags=["Accelerators"])
app.include_router(training.router, prefix="/api/v1/training", tags=["Training"])
app.include_router(inference.router, prefix="/api/v1/inference", tags=["Inference"])
app.include_router(pricing.router, prefix="/api/v1/pricing", tags=["Pricing"])
app.include_router(performance.router, prefix="/api/v1/performance", tags=["Performance"])
app.include_router(markets.router, prefix="/api/v1/markets", tags=["Markets"])


# Background tasks

async def monitor_training_checkpoints():
    """Monitor training jobs and collect checkpoints."""
    while True:
        try:
            active_contracts = await training_manager.get_active_contracts()
            
            for contract in active_contracts:
                # Check if checkpoint is due
                last_checkpoint = contract.checkpoints[-1] if contract.checkpoints else None
                checkpoint_due = (
                    not last_checkpoint or 
                    (datetime.utcnow() - last_checkpoint.timestamp).total_seconds() >= settings.checkpoint_interval
                )
                
                if checkpoint_due:
                    # Request checkpoint from accelerator
                    checkpoint = await training_manager.request_checkpoint(
                        contract.contract_id,
                        contract.accelerator_id
                    )
                    
                    if checkpoint:
                        # Update contract
                        await training_manager.add_checkpoint(
                            contract.contract_id,
                            checkpoint
                        )
                        
                        # Check if target achieved
                        if checkpoint.accuracy >= contract.target_accuracy:
                            await training_manager.mark_complete(
                                contract.contract_id,
                                success=True,
                                final_accuracy=checkpoint.accuracy
                            )
                            
                            # Publish completion event
                            await event_publisher.publish_training_complete({
                                "contract_id": contract.contract_id,
                                "user": contract.user,
                                "model": contract.model_architecture,
                                "accuracy": checkpoint.accuracy,
                                "duration": (datetime.utcnow() - contract.start_time).total_seconds()
                            })
            
            await asyncio.sleep(60)  # Check every minute
            
        except Exception as e:
            logger.error(f"Error monitoring training checkpoints: {e}")
            await asyncio.sleep(30)


async def update_benchmark_scores():
    """Update accelerator performance scores based on recent benchmarks."""
    while True:
        try:
            accelerators = await accelerator_registry.get_all_active_accelerators()
            
            for accelerator in accelerators:
                # Get recent benchmarks
                recent_benchmarks = await performance_benchmarker.get_recent_benchmarks(
                    accelerator.accelerator_id,
                    hours=24
                )
                
                if recent_benchmarks:
                    # Calculate average scores by benchmark type
                    benchmark_scores = {}
                    for benchmark in recent_benchmarks:
                        if benchmark.benchmark_type not in benchmark_scores:
                            benchmark_scores[benchmark.benchmark_type] = []
                        benchmark_scores[benchmark.benchmark_type].append(benchmark.score)
                    
                    # Update quality score
                    avg_scores = {
                        btype: sum(scores) / len(scores)
                        for btype, scores in benchmark_scores.items()
                    }
                    
                    # Calculate composite score
                    if "mlperf_training" in avg_scores and "mlperf_inference" in avg_scores:
                        composite_score = int(
                            (avg_scores["mlperf_training"] * 0.6 + 
                             avg_scores["mlperf_inference"] * 0.4) * 100
                        )
                        
                        await blockchain.update_quality_score(
                            accelerator.token_id,
                            composite_score
                        )
                        
                        # Update pricing multiplier
                        await pricing_engine.update_performance_multiplier(
                            accelerator.accelerator_id,
                            composite_score / 10000  # Normalize to 0-1
                        )
            
            await asyncio.sleep(300)  # Update every 5 minutes
            
        except Exception as e:
            logger.error(f"Error updating benchmark scores: {e}")
            await asyncio.sleep(60)


async def manage_thermal_throttling():
    """Manage thermal throttling for accelerators."""
    while True:
        try:
            accelerators = await accelerator_registry.get_all_active_accelerators()
            
            for accelerator in accelerators:
                current_temp = await thermal_monitor.get_temperature(accelerator.accelerator_id)
                
                if current_temp > accelerator.thermal_limit * 0.9:  # 90% of limit
                    # Throttle performance
                    await accelerator_registry.throttle_accelerator(
                        accelerator.accelerator_id,
                        reduction_percent=20
                    )
                    
                    # Alert provider
                    await event_publisher.publish_thermal_alert({
                        "accelerator_id": accelerator.accelerator_id,
                        "current_temperature": current_temp,
                        "thermal_limit": accelerator.thermal_limit,
                        "action": "throttled"
                    })
                    
                elif current_temp > accelerator.thermal_limit:
                    # Emergency shutdown
                    await accelerator_registry.pause_accelerator(accelerator.accelerator_id)
                    
                    # Migrate workloads
                    active_workloads = await workload_scheduler.get_accelerator_workloads(
                        accelerator.accelerator_id
                    )
                    
                    for workload in active_workloads:
                        alternative = await workload_scheduler.find_alternative_accelerator(
                            workload.requirements,
                            exclude=[accelerator.accelerator_id]
                        )
                        
                        if alternative:
                            await workload_scheduler.migrate_workload(
                                workload.workload_id,
                                alternative.accelerator_id
                            )
                    
                    await event_publisher.publish_thermal_shutdown({
                        "accelerator_id": accelerator.accelerator_id,
                        "temperature": current_temp,
                        "workloads_migrated": len(active_workloads)
                    })
            
            await asyncio.sleep(10)  # Check every 10 seconds
            
        except Exception as e:
            logger.error(f"Error managing thermal throttling: {e}")
            await asyncio.sleep(5)


async def aggregate_inference_batches():
    """Aggregate small inference requests into batches for efficiency."""
    while True:
        try:
            pending_requests = await inference_router.get_pending_requests()
            
            # Group by model and accelerator preference
            request_groups = {}
            for request in pending_requests:
                key = (request.model_id, request.preferred_accelerator_type)
                if key not in request_groups:
                    request_groups[key] = []
                request_groups[key].append(request)
            
            # Create batches
            for (model_id, accel_type), requests in request_groups.items():
                if len(requests) >= settings.min_batch_size:
                    # Create batch
                    batch = await inference_router.create_batch(
                        model_id=model_id,
                        requests=requests[:settings.max_batch_size],
                        accelerator_type=accel_type
                    )
                    
                    # Find best accelerator
                    accelerator = await workload_scheduler.find_best_inference_accelerator(
                        model_id=model_id,
                        batch_size=batch.total_size,
                        latency_requirement=batch.max_latency_requirement
                    )
                    
                    if accelerator:
                        # Route batch
                        await inference_router.route_batch(
                            batch.batch_id,
                            accelerator.accelerator_id
                        )
                        
                        # Update requests
                        for request in batch.requests:
                            await inference_router.update_request_status(
                                request.request_id,
                                status="batched",
                                batch_id=batch.batch_id
                            )
            
            await asyncio.sleep(1)  # Aggregate every second
            
        except Exception as e:
            logger.error(f"Error aggregating inference batches: {e}")
            await asyncio.sleep(5)


# Start background tasks
@app.on_event("startup")
async def startup_tasks():
    """Start background tasks."""
    asyncio.create_task(monitor_training_checkpoints())
    asyncio.create_task(update_benchmark_scores())
    asyncio.create_task(manage_thermal_throttling())
    asyncio.create_task(aggregate_inference_batches())


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
        port=8025,
        reload=True,
        log_level="info"
    ) 