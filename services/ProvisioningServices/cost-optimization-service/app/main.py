"""Cost Optimization Service

Analyzes costs and provides optimization recommendations.
"""

from contextlib import asynccontextmanager
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, Counter, Histogram, Gauge
import consul.aio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from platformq_cost_common import (
    CostAnalysis,
    CostRecommendation,
    CostRecommendationType
)

from .config import settings
from .api import router
from .cost_analyzer import CostAnalyzer
from .recommendation_engine import RecommendationEngine
from .budget_manager import BudgetManager
from .repository import CostRepository

# Configure logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Prometheus metrics
cost_analysis_counter = Counter(
    'cost_analysis_total',
    'Total number of cost analyses performed',
    ['tenant_id']
)
recommendation_counter = Counter(
    'recommendations_generated_total',
    'Total number of recommendations generated',
    ['tenant_id', 'type']
)
budget_alert_counter = Counter(
    'budget_alerts_total',
    'Total number of budget alerts triggered',
    ['tenant_id', 'alert_type']
)
analysis_duration_histogram = Histogram(
    'cost_analysis_duration_seconds',
    'Duration of cost analysis in seconds'
)
total_cost_gauge = Gauge(
    'tenant_total_cost_daily',
    'Daily total cost per tenant',
    ['tenant_id']
)

# Global instances
repository = None
cost_analyzer = None
recommendation_engine = None
budget_manager = None
consul_client = None
scheduler = None


async def register_with_consul():
    """Register service with Consul"""
    global consul_client
    
    try:
        consul_client = consul.aio.Consul(
            host=settings.consul_host,
            port=settings.consul_port
        )
        
        # Register service
        await consul_client.agent.service.register(
            name=settings.consul_service_name,
            service_id=settings.consul_service_id,
            address=settings.service_name,
            port=settings.service_port,
            tags=[
                "cost-optimization",
                "analytics",
                "recommendations"
            ],
            check=consul.Check.http(
                f"http://{settings.service_name}:{settings.service_port}/health",
                interval=settings.consul_health_check_interval,
                deregister=settings.consul_deregister_critical_after
            )
        )
        
        logger.info(f"Registered with Consul as {settings.consul_service_id}")
        
    except Exception as e:
        logger.error(f"Failed to register with Consul: {e}")


async def deregister_from_consul():
    """Deregister service from Consul"""
    if consul_client:
        try:
            await consul_client.agent.service.deregister(
                service_id=settings.consul_service_id
            )
            logger.info("Deregistered from Consul")
        except Exception as e:
            logger.error(f"Failed to deregister from Consul: {e}")
        finally:
            await consul_client.close()


async def scheduled_cost_analysis():
    """Scheduled task to perform cost analysis for all tenants"""
    logger.info("Running scheduled cost analysis")
    
    try:
        # In production, this would get list of all active tenants
        # For now, using mock tenant IDs
        tenant_ids = ["tenant-001", "tenant-002", "tenant-003"]
        
        for tenant_id in tenant_ids:
            try:
                # Analyze costs for last 24 hours
                end_date = datetime.now(timezone.utc)
                start_date = end_date - timedelta(hours=24)
                
                with analysis_duration_histogram.time():
                    analysis = await cost_analyzer.analyze_costs(
                        tenant_id=tenant_id,
                        start_date=start_date,
                        end_date=end_date
                    )
                
                # Update metrics
                cost_analysis_counter.labels(tenant_id=tenant_id).inc()
                total_cost_gauge.labels(tenant_id=tenant_id).set(analysis.total_cost)
                
                # Generate recommendations if significant costs
                if analysis.total_cost > 10:  # $10 threshold
                    # Get resource metrics (mock for now)
                    resource_metrics = []
                    
                    recommendations = await recommendation_engine.generate_recommendations(
                        tenant_id=tenant_id,
                        cost_analysis=analysis,
                        resource_metrics=resource_metrics
                    )
                    
                    for rec in recommendations:
                        recommendation_counter.labels(
                            tenant_id=tenant_id,
                            type=rec.recommendation_type.value
                        ).inc()
                
                # Check budgets
                alerts = await budget_manager.check_budgets(
                    tenant_id=tenant_id,
                    cost_analysis=analysis
                )
                
                for alert in alerts:
                    budget_alert_counter.labels(
                        tenant_id=tenant_id,
                        alert_type=alert.alert_type
                    ).inc()
                    
            except Exception as e:
                logger.error(f"Error analyzing costs for tenant {tenant_id}: {e}")
                
    except Exception as e:
        logger.error(f"Error in scheduled cost analysis: {e}")


async def scheduled_recommendation_refresh():
    """Scheduled task to refresh recommendations"""
    logger.info("Running scheduled recommendation refresh")
    
    # This would refresh recommendations based on latest cost data
    # Implementation depends on business requirements


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global repository, cost_analyzer, recommendation_engine, budget_manager, scheduler
    
    logger.info("Cost Optimization Service starting up...")
    
    # Initialize components
    repository = CostRepository()
    cost_analyzer = CostAnalyzer(repository)
    recommendation_engine = RecommendationEngine(repository)
    budget_manager = BudgetManager(repository)
    
    # Register with Consul
    await register_with_consul()
    
    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    
    # Schedule periodic tasks
    scheduler.add_job(
        scheduled_cost_analysis,
        'interval',
        hours=settings.cost_analysis_interval_hours,
        id='cost_analysis',
        max_instances=1
    )
    
    scheduler.add_job(
        scheduled_recommendation_refresh,
        'interval',
        hours=24,  # Daily refresh
        id='recommendation_refresh',
        max_instances=1
    )
    
    # Schedule budget checks
    scheduler.add_job(
        scheduled_cost_analysis,  # Reuse same function as it includes budget checks
        'interval',
        hours=settings.budget_check_interval_hours,
        id='budget_check',
        max_instances=1
    )
    
    scheduler.start()
    logger.info("Scheduler started")
    
    logger.info("Cost Optimization Service started")
    
    yield
    
    # Shutdown
    logger.info("Cost Optimization Service shutting down...")
    
    # Stop scheduler
    if scheduler:
        scheduler.shutdown()
    
    # Close connections
    if cost_analyzer:
        await cost_analyzer.close()
    if budget_manager:
        await budget_manager.close()
    if repository:
        await repository.close()
    
    # Deregister from Consul
    await deregister_from_consul()
    
    logger.info("Cost Optimization Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Cost Optimization Service",
    description="Analyzes costs and provides optimization recommendations",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    # Check if all components are initialized
    if not all([repository, cost_analyzer, recommendation_engine, budget_manager]):
        raise HTTPException(status_code=503, detail="Service not ready")
        
    # Check database connections
    try:
        # Simple query to verify connections
        await repository.get_budgets("test")
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Database not ready")
        
    return {
        "status": "ready",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.service_port,
        reload=False
    ) 