"""
Compliance Service

Handles KYC/AML verification, risk scoring, regulatory reporting,
and transaction monitoring for PlatformQ.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
import asyncio

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge
import time

from platformq_shared import (
    create_base_app,
    ConfigLoader,
    ErrorCode,
    AppException
)

from .core.config import settings
from .core.kyc_manager import KYCManager
from .core.aml_engine import AMLEngine
from .core.risk_scorer import RiskScorer
from .services.identity_verifier import IdentityVerifier
from .services.sanctions_checker import SanctionsChecker
from .services.transaction_monitor import TransactionMonitor
from .services.regulatory_reporter import RegulatoryReporter
from .api import kyc, aml, risk, reporting, monitoring

logger = logging.getLogger(__name__)

# Metrics
KYC_VERIFICATIONS = Counter(
    'kyc_verifications_total',
    'Total KYC verification attempts',
    ['status', 'level']
)
AML_CHECKS = Counter(
    'aml_checks_total',
    'Total AML checks performed',
    ['result', 'type']
)
RISK_SCORES = Histogram(
    'compliance_risk_scores',
    'Distribution of risk scores',
    ['entity_type']
)
TRANSACTION_ALERTS = Counter(
    'transaction_alerts_total',
    'Total transaction monitoring alerts',
    ['severity', 'type']
)

# Global instances
kyc_manager: Optional[KYCManager] = None
aml_engine: Optional[AMLEngine] = None
risk_scorer: Optional[RiskScorer] = None
identity_verifier: Optional[IdentityVerifier] = None
sanctions_checker: Optional[SanctionsChecker] = None
transaction_monitor: Optional[TransactionMonitor] = None
regulatory_reporter: Optional[RegulatoryReporter] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global kyc_manager, aml_engine, risk_scorer
    global identity_verifier, sanctions_checker
    global transaction_monitor, regulatory_reporter
    
    # Startup
    logger.info("Starting Compliance Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize identity verifier
    identity_verifier = IdentityVerifier(
        providers=config.get("identity_providers", ["jumio", "onfido"]),
        api_keys=config.get("identity_api_keys", {})
    )
    await identity_verifier.initialize()
    app.state.identity_verifier = identity_verifier
    
    # Initialize sanctions checker
    sanctions_checker = SanctionsChecker(
        databases=config.get("sanctions_databases", ["ofac", "eu", "un"]),
        update_frequency=int(config.get("sanctions_update_hours", 24))
    )
    await sanctions_checker.initialize()
    app.state.sanctions_checker = sanctions_checker
    
    # Initialize transaction monitor
    transaction_monitor = TransactionMonitor(
        rules_engine=config.get("transaction_rules_engine", "drools"),
        alert_thresholds=config.get("alert_thresholds", {})
    )
    await transaction_monitor.initialize()
    app.state.transaction_monitor = transaction_monitor
    
    # Initialize KYC manager
    kyc_manager = KYCManager(
        identity_verifier=identity_verifier,
        document_storage=config.get("document_storage_url"),
        encryption_key=config.get("kyc_encryption_key")
    )
    await kyc_manager.initialize()
    app.state.kyc_manager = kyc_manager
    
    # Initialize AML engine
    aml_engine = AMLEngine(
        sanctions_checker=sanctions_checker,
        transaction_monitor=transaction_monitor,
        ml_model_path=config.get("aml_model_path")
    )
    await aml_engine.initialize()
    app.state.aml_engine = aml_engine
    
    # Initialize risk scorer
    risk_scorer = RiskScorer(
        kyc_manager=kyc_manager,
        aml_engine=aml_engine,
        risk_factors=config.get("risk_factors", {})
    )
    await risk_scorer.initialize()
    app.state.risk_scorer = risk_scorer
    
    # Initialize regulatory reporter
    regulatory_reporter = RegulatoryReporter(
        reporting_apis=config.get("regulatory_apis", {}),
        jurisdictions=config.get("reporting_jurisdictions", [])
    )
    await regulatory_reporter.initialize()
    app.state.regulatory_reporter = regulatory_reporter
    
    # Start background tasks
    asyncio.create_task(sanctions_checker.update_lists_periodically())
    asyncio.create_task(transaction_monitor.monitor_transactions())
    asyncio.create_task(regulatory_reporter.process_reporting_queue())
    
    logger.info("Compliance Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Compliance Service...")
    
    # Stop services
    if identity_verifier:
        await identity_verifier.shutdown()
    if sanctions_checker:
        await sanctions_checker.shutdown()
    if transaction_monitor:
        await transaction_monitor.shutdown()
    if kyc_manager:
        await kyc_manager.shutdown()
    if aml_engine:
        await aml_engine.shutdown()
    if regulatory_reporter:
        await regulatory_reporter.shutdown()
    
    logger.info("Compliance Service shutdown complete")


# Create app
app = create_base_app(
    service_name="compliance-service",
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

# Include routers
app.include_router(kyc.router, prefix="/api/v1/kyc", tags=["kyc"])
app.include_router(aml.router, prefix="/api/v1/aml", tags=["aml"])
app.include_router(risk.router, prefix="/api/v1/risk", tags=["risk"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])
app.include_router(reporting.router, prefix="/api/v1/reporting", tags=["reporting"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "compliance-service",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "kyc-verification",
            "aml-screening",
            "risk-scoring",
            "transaction-monitoring",
            "sanctions-checking",
            "regulatory-reporting",
            "identity-verification",
            "document-management"
        ]
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    try:
        # Check component health
        checks = {
            "identity_verifier": identity_verifier is not None,
            "sanctions_checker": sanctions_checker is not None,
            "transaction_monitor": transaction_monitor is not None,
            "kyc_manager": kyc_manager is not None,
            "aml_engine": aml_engine is not None
        }
        
        all_healthy = all(checks.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "components": checks,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy") 