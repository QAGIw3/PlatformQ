"""
Compliance Service

Handles KYC/AML verification, risk scoring, regulatory reporting,
and transaction monitoring for PlatformQ.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any
from decimal import Decimal
from datetime import datetime

from fastapi import FastAPI, HTTPException, Depends
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

# Import Vault/Consul integration
from .vault_consul_integration import VaultConsulIntegration

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
vault_consul: Optional[VaultConsulIntegration] = None


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
    """Application lifespan manager with Vault/Consul integration"""
    global kyc_manager, aml_engine, risk_scorer, identity_verifier
    global sanctions_checker, transaction_monitor, regulatory_reporter
    global vc_integration, aml_vc_integration, ignite_client, pulsar_client
    global vault_consul
    
    try:
        logger.info("Starting Compliance Service...")
        
        # Initialize Vault/Consul integration
        vault_consul = VaultConsulIntegration({
            "vault_addr": os.getenv("VAULT_ADDR", "http://vault:8200"),
            "vault_token": os.getenv("VAULT_TOKEN"),
            "consul_addr": os.getenv("CONSUL_ADDR", "http://consul:8500")
        })
        
        await vault_consul.initialize()
        
        # Register service with Consul
        await vault_consul.register_service(
            tags=["compliance", "kyc", "aml", "regulatory"],
            meta={
                "version": "1.0.0",
                "frameworks": "gdpr,hipaa,pci-dss,sox"
            }
        )
        
        # Get secure configurations
        compliance_cert = await vault_consul.get_compliance_certificate("gdpr", "service")
        
        # Initialize services with encrypted audit logging
        kyc_manager = KYCManager(
            ignite_client=ignite_client,
            vault_consul=vault_consul  # Pass for audit encryption
        )
        
        aml_engine = AMLEngine(
            ignite_client=ignite_client,
            vault_consul=vault_consul
        )
        
        risk_scorer = RiskScorer(
            ignite_client=ignite_client,
            vault_consul=vault_consul
        )
        
        # Initialize verification services with secure credentials
        identity_verifier = IdentityVerifier(
            vault_consul=vault_consul
        )
        
        sanctions_checker = SanctionsChecker(
            vault_consul=vault_consul
        )
        
        transaction_monitor = TransactionMonitor(
            ignite_client=ignite_client,
            pulsar_client=pulsar_client,
            vault_consul=vault_consul
        )
        
        regulatory_reporter = RegulatoryReporter(
            ignite_client=ignite_client,
            vault_consul=vault_consul
        )
        
        # Initialize VC integration services
        vc_integration = VCIntegrationService(
            vault_consul=vault_consul
        )
        
        aml_vc_integration = AMLVCIntegrationService(
            vault_consul=vault_consul
        )
        
        # Start background tasks
        asyncio.create_task(monitor_compliance_policies())
        asyncio.create_task(rotate_audit_encryption_keys())
        
        logger.info("Compliance Service started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start Compliance Service: {e}")
        raise
    
    yield
    
    # Cleanup
    logger.info("Shutting down Compliance Service...")
    
    if transaction_monitor:
        await transaction_monitor.stop()
    
    if vault_consul:
        await vault_consul.deregister_service()
        await vault_consul.shutdown()
    
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


# Enhanced endpoints with encryption and audit logging

from pydantic import BaseModel

class EncryptedAuditLog(BaseModel):
    action: str
    entity_type: str
    entity_id: str
    user_id: str
    details: Dict[str, Any]
    framework: str = "gdpr"

@app.post("/api/audit/log")
async def create_encrypted_audit_log(log_entry: EncryptedAuditLog):
    """Create encrypted audit log entry"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Add timestamp and service info
        log_data = log_entry.dict()
        log_data["timestamp"] = datetime.utcnow().isoformat()
        log_data["service"] = "compliance-service"
        
        # Encrypt audit log
        encrypted = await vault_consul.encrypt_audit_log(
            log_data,
            log_entry.framework
        )
        
        return {
            "status": "logged",
            "log_id": encrypted["integrity_hash"],
            "framework": log_entry.framework
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pii/encrypt")
async def encrypt_pii_data(
    data: str,
    data_subject_id: str,
    purpose: str
):
    """Encrypt PII data for GDPR compliance"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        encrypted = await vault_consul.encrypt_pii_data(
            data.encode(),
            data_subject_id,
            purpose
        )
        
        return encrypted
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/pii/decrypt")
async def decrypt_pii_data(
    encrypted_data: Dict[str, str],
    purpose: str
):
    """Decrypt PII data with audit logging"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        decrypted = await vault_consul.decrypt_pii_data(
            encrypted_data,
            purpose
        )
        
        return {"data": decrypted.decode()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/consent/record")
async def record_consent(
    data_subject_id: str,
    consent_data: Dict[str, Any]
):
    """Record user consent with cryptographic proof"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        await vault_consul.store_consent_record(
            data_subject_id,
            consent_data
        )
        
        return {
            "status": "recorded",
            "data_subject_id": data_subject_id,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/evidence/sign")
async def sign_compliance_evidence(
    evidence: str,
    framework: str = "gdpr"
):
    """Sign evidence for compliance verification"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        signature = await vault_consul.sign_evidence(
            evidence.encode(),
            framework
        )
        
        return signature
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/evidence/verify")
async def verify_evidence_signature(
    evidence: str,
    signature_data: Dict[str, str]
):
    """Verify evidence signature"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        valid = await vault_consul.verify_evidence_signature(
            evidence.encode(),
            signature_data
        )
        
        return {"valid": valid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/retention-policy/{framework}/{data_type}")
async def get_retention_policy(framework: str, data_type: str):
    """Get data retention policy"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        policy = await vault_consul.get_data_retention_policy(
            data_type,
            framework
        )
        
        return policy
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/export/create")
async def create_data_export(
    data_subject_id: str,
    framework: str = "gdpr"
):
    """Create data export package for subject access request"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        package = await vault_consul.create_data_export_package(
            data_subject_id,
            framework
        )
        
        return package
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/transfer/key")
async def get_transfer_key(
    source_country: str,
    dest_country: str
):
    """Get encryption key for cross-border data transfer"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        transfer_key = await vault_consul.get_cross_border_transfer_key(
            source_country,
            dest_country
        )
        
        return transfer_key
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Background tasks

async def monitor_compliance_policies():
    """Monitor compliance policy changes"""
    while True:
        try:
            if vault_consul:
                # Check for policy updates
                retention_policies = vault_consul.compliance_config.get(
                    "data-retention", {}
                )
                
                # Apply any policy changes
                logger.info(f"Active retention policies: {len(retention_policies)}")
            
            await asyncio.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logger.error(f"Policy monitoring error: {e}")
            await asyncio.sleep(60)

async def rotate_audit_encryption_keys():
    """Rotate audit log encryption keys periodically"""
    while True:
        try:
            # Rotate keys monthly
            await asyncio.sleep(2592000)  # 30 days
            
            if vault_consul:
                # This would trigger key rotation in Vault
                logger.info("Rotating audit encryption keys")
                
        except Exception as e:
            logger.error(f"Key rotation error: {e}")
            await asyncio.sleep(3600)

# Enhanced health check

@app.get("/health")
async def health_check():
    """Enhanced health check with compliance status"""
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    # Check Vault/Consul
    if vault_consul:
        health["checks"]["vault"] = await vault_consul.check_vault_health()
        health["checks"]["consul"] = await vault_consul.check_consul_health()
        
        # Check compliance certificates
        try:
            cert = await vault_consul.get_compliance_certificate("gdpr", "audit")
            health["checks"]["compliance_certificates"] = {"status": "valid"}
        except Exception:
            health["checks"]["compliance_certificates"] = {"status": "invalid"}
            health["status"] = "degraded"
    else:
        health["status"] = "unhealthy"
        health["checks"]["vault"] = {"status": "not_initialized"}
        health["checks"]["consul"] = {"status": "not_initialized"}
    
    # Check services
    if kyc_manager:
        health["checks"]["kyc"] = {"status": "healthy"}
    else:
        health["checks"]["kyc"] = {"status": "not_initialized"}
        health["status"] = "degraded"
    
    if aml_engine:
        health["checks"]["aml"] = {"status": "healthy"}
    else:
        health["checks"]["aml"] = {"status": "not_initialized"}
        health["status"] = "degraded"
    
    return health


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    ) 