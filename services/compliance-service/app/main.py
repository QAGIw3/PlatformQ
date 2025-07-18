"""
Compliance Service

Handles KYC/AML verification, risk scoring, regulatory reporting,
and transaction monitoring for PlatformQ.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pyignite import Client as IgniteClient
import pulsar
import consul
import hvac

from platformq_shared import ConfigLoader, setup_logging
from .api import kyc, aml, risk, monitoring, reporting
from .core.config import Settings
from .core.aml_engine import AMLEngine
from .core.kyc_manager import KYCManager, KYCStatus, KYCLevel
from .core.risk_scorer import RiskScorer
from .services import (
    IdentityVerifier, 
    SanctionsChecker, 
    TransactionMonitor, 
    RegulatoryReporter,
    VCIntegrationService,
    AMLVCIntegrationService
)
from .aml.aml_engine import AMLEngine as AMLEngineImpl
from .aml.risk_assessment import RiskAssessmentEngine

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# Global service instances
kyc_manager: Optional[KYCManager] = None
aml_engine: Optional[AMLEngine] = None
risk_scorer: Optional[RiskScorer] = None
identity_verifier: Optional[IdentityVerifier] = None
sanctions_checker: Optional[SanctionsChecker] = None
transaction_monitor: Optional[TransactionMonitor] = None
regulatory_reporter: Optional[RegulatoryReporter] = None
vc_integration: Optional[VCIntegrationService] = None
aml_vc_integration: Optional[AMLVCIntegrationService] = None
ignite_client: Optional[IgniteClient] = None
pulsar_client: Optional[pulsar.Client] = None
vault_client: Optional[hvac.Client] = None
consul_client: Optional[consul.Consul] = None


class VaultConfigLoader:
    """Load configuration from HashiCorp Vault"""
    
    def __init__(self, vault_url: str, vault_token: str):
        self.client = hvac.Client(url=vault_url, token=vault_token)
        
    def load_config(self, path: str) -> dict:
        """Load configuration from Vault"""
        response = self.client.secrets.kv.v2.read_secret_version(path=path)
        return response['data']['data']
        
    def get_secret(self, key: str, default=None):
        """Get a specific secret"""
        try:
            config = self.load_config('compliance-service/config')
            return config.get(key, default)
        except Exception as e:
            logger.warning(f"Failed to load secret {key} from Vault: {e}")
            return default


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global kyc_manager, aml_engine, risk_scorer
    global identity_verifier, sanctions_checker
    global transaction_monitor, regulatory_reporter
    global vc_integration, aml_vc_integration
    global ignite_client, pulsar_client, vault_client, consul_client
    
    # Startup
    logger.info("Starting Compliance Service...")
    
    # Initialize Vault client
    vault_url = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN", "root")
    vault_loader = VaultConfigLoader(vault_url, vault_token)
    vault_client = vault_loader.client
    
    # Initialize Consul client for service discovery
    consul_client = consul.Consul(
        host=os.getenv("CONSUL_HOST", "localhost"),
        port=int(os.getenv("CONSUL_PORT", "8500"))
    )
    
    # Discover services via Consul
    _, vc_service = consul_client.health.service("verifiable-credential-service", passing=True)
    vc_service_url = f"http://{vc_service[0]['Service']['Address']}:{vc_service[0]['Service']['Port']}" if vc_service else "http://verifiable-credential-service:8000"
    
    # Initialize configuration from Vault
    config = {
        "identity_providers": vault_loader.get_secret("identity_providers", ["jumio", "onfido"]),
        "identity_api_keys": vault_loader.get_secret("identity_api_keys", {}),
        "sanctions_databases": vault_loader.get_secret("sanctions_databases", ["ofac", "eu", "un"]),
        "sanctions_update_hours": int(vault_loader.get_secret("sanctions_update_hours", 24)),
        "transaction_rules_engine": vault_loader.get_secret("transaction_rules_engine", "drools"),
        "alert_thresholds": vault_loader.get_secret("alert_thresholds", {}),
        "document_storage_url": vault_loader.get_secret("document_storage_url", "s3://compliance-docs"),
        "kyc_encryption_key": vault_loader.get_secret("kyc_encryption_key"),
        "aml_model_path": vault_loader.get_secret("aml_model_path"),
        "ignite_hosts": vault_loader.get_secret("ignite_hosts", ["localhost:10800"]),
        "pulsar_url": vault_loader.get_secret("pulsar_url", "pulsar://localhost:6650"),
        "issuer_did": vault_loader.get_secret("issuer_did", "did:platform:compliance-service")
    }
    
    # Initialize Apache Ignite client
    ignite_client = IgniteClient()
    ignite_client.connect(config["ignite_hosts"])
    app.state.ignite_client = ignite_client
    
    # Initialize Pulsar client
    pulsar_client = pulsar.Client(config["pulsar_url"])
    app.state.pulsar_client = pulsar_client
    
    # Initialize identity verifier
    identity_verifier = IdentityVerifier(
        kyc_manager=None,  # Will be set after KYC manager init
        config=config
    )
    await identity_verifier.initialize()
    app.state.identity_verifier = identity_verifier
    
    # Initialize sanctions checker
    sanctions_checker = SanctionsChecker(
        databases=config["sanctions_databases"],
        update_frequency=config["sanctions_update_hours"]
    )
    await sanctions_checker.initialize()
    app.state.sanctions_checker = sanctions_checker
    
    # Initialize transaction monitor
    transaction_monitor = TransactionMonitor(
        rules_engine=config["transaction_rules_engine"],
        alert_thresholds=config["alert_thresholds"]
    )
    await transaction_monitor.initialize()
    app.state.transaction_monitor = transaction_monitor
    
    # Initialize KYC manager
    kyc_manager = KYCManager(
        identity_verifier=identity_verifier,
        document_storage=config["document_storage_url"],
        encryption_key=config["kyc_encryption_key"]
    )
    await kyc_manager.initialize()
    app.state.kyc_manager = kyc_manager
    
    # Update identity verifier with KYC manager reference
    identity_verifier.kyc_manager = kyc_manager
    
    # Initialize AML engine with Ignite and Pulsar
    # Initialize risk assessment engine
    risk_assessment_engine = RiskAssessmentEngine(
        ignite_client=ignite_client,
        provider_configs=vault_loader.get_secret("compliance_providers", {})
    )
    await risk_assessment_engine.initialize()
    
    # Initialize AML engine
    aml_engine_impl = AMLEngineImpl(
        ignite_client=ignite_client,
        pulsar_client=pulsar_client,
        blockchain_gateway_url=vault_loader.get_secret("blockchain_gateway_url", "http://blockchain-gateway:8000"),
        blockchain_analytics_url=vault_loader.get_secret("blockchain_analytics_url"),
        high_value_threshold_usd=Decimal(vault_loader.get_secret("high_value_threshold_usd", "10000"))
    )
    await aml_engine_impl.initialize()
    
    # Initialize core AML engine wrapper
    aml_engine = AMLEngine(
        sanctions_checker=sanctions_checker,
        transaction_monitor=transaction_monitor,
        ml_model_path=config["aml_model_path"]
    )
    await aml_engine.initialize()
    app.state.aml_engine = aml_engine
    app.state.aml_engine_impl = aml_engine_impl
    app.state.risk_assessment_engine = risk_assessment_engine
    
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
        jurisdictions=config.get("reporting_jurisdictions", ["US", "EU", "UK"])
    )
    await regulatory_reporter.initialize()
    app.state.regulatory_reporter = regulatory_reporter
    
    # Initialize VC Integration Services
    vc_integration = VCIntegrationService(
        vc_service_url=vc_service_url,
        kyc_manager=kyc_manager,
        issuer_did=config["issuer_did"]
    )
    app.state.vc_integration = vc_integration
    
    aml_vc_integration = AMLVCIntegrationService(
        vc_service_url=vc_service_url,
        aml_engine=aml_engine_impl,
        issuer_did=config["issuer_did"]
    )
    app.state.aml_vc_integration = aml_vc_integration
    
    # Register service with Consul
    consul_client.agent.service.register(
        name="compliance-service",
        service_id="compliance-service-1",
        address=os.getenv("SERVICE_HOST", "localhost"),
        port=int(os.getenv("SERVICE_PORT", "8001")),
        tags=["compliance", "kyc", "aml", "api"],
        check=consul.Check.http(
            f"http://{os.getenv('SERVICE_HOST', 'localhost')}:{os.getenv('SERVICE_PORT', '8001')}/health",
            interval="10s"
        )
    )
    
    logger.info("Compliance Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Compliance Service...")
    
    # Deregister from Consul
    consul_client.agent.service.deregister("compliance-service-1")
    
    # Shutdown services
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
    if aml_engine_impl:
        await aml_engine_impl.shutdown()
    if risk_assessment_engine:
        await risk_assessment_engine.shutdown()
    if risk_scorer:
        await risk_scorer.shutdown()
    if regulatory_reporter:
        await regulatory_reporter.shutdown()
    if vc_integration:
        await vc_integration.close()
    if aml_vc_integration:
        await aml_vc_integration.close()
        
    # Close clients
    if ignite_client:
        ignite_client.close()
    if pulsar_client:
        pulsar_client.close()
        
    logger.info("Compliance Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Compliance Service",
    description="KYC, AML, and regulatory compliance service for PlatformQ",
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

# Include routers
app.include_router(kyc.router, prefix="/api/v1/kyc", tags=["KYC"])
app.include_router(aml.router, prefix="/api/v1/aml", tags=["AML"])
app.include_router(risk.router, prefix="/api/v1/risk", tags=["Risk"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["Monitoring"])
app.include_router(reporting.router, prefix="/api/v1/reporting", tags=["Reporting"])


@app.get("/")
async def root():
    return {
        "service": "Compliance Service",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "kyc": "/api/v1/kyc",
            "aml": "/api/v1/aml",
            "risk": "/api/v1/risk",
            "monitoring": "/api/v1/monitoring",
            "reporting": "/api/v1/reporting",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "services": {
            "kyc_manager": kyc_manager is not None,
            "aml_engine": aml_engine is not None,
            "risk_scorer": risk_scorer is not None,
            "identity_verifier": identity_verifier is not None,
            "sanctions_checker": sanctions_checker is not None,
            "transaction_monitor": transaction_monitor is not None,
            "vc_integration": vc_integration is not None,
            "aml_vc_integration": aml_vc_integration is not None,
            "ignite": ignite_client is not None and ignite_client.connected,
            "pulsar": pulsar_client is not None
        }
    }
    
    # Check if all services are healthy
    all_healthy = all(health_status["services"].values())
    
    if not all_healthy:
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    ) 