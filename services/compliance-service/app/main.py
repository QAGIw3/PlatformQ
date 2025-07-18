"""
PlatformQ Compliance Service
Multi-jurisdictional regulatory compliance engine
"""

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
import logging

from app.api import kyc, aml, jurisdiction, reporting, monitoring
from app.kyc.kyc_engine import KYCEngine
from app.aml import AMLEngine, RiskAssessmentEngine, TransactionMonitor
from app.jurisdiction.jurisdiction_engine import JurisdictionEngine
from app.reporting.reporting_engine import ReportingEngine
from app.integration import (
    IgniteCache,
    PulsarEventPublisher,
    ElasticsearchClient,
    JanusGraphClient,
    VaultClient,
    VerifiableCredentialClient
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
kyc_engine: Optional[KYCEngine] = None
aml_engine: Optional[AMLEngine] = None
jurisdiction_engine: Optional[JurisdictionEngine] = None
reporting_engine: Optional[ReportingEngine] = None
transaction_monitor: Optional[TransactionMonitor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Compliance Service...")
    
    # Initialize storage clients
    ignite = IgniteCache()
    await ignite.connect()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    elasticsearch = ElasticsearchClient()
    await elasticsearch.connect()
    
    janusgraph = JanusGraphClient()
    await janusgraph.connect()
    
    vault = VaultClient()
    await vault.connect()
    
    vc_client = VerifiableCredentialClient()
    
    # Initialize engines
    global kyc_engine, aml_engine, jurisdiction_engine
    global reporting_engine, transaction_monitor
    
    kyc_engine = KYCEngine(
        ignite=ignite,
        pulsar=pulsar,
        elasticsearch=elasticsearch,
        vault=vault,
        vc_client=vc_client
    )
    
    aml_engine = AMLEngine(
        ignite=ignite,
        pulsar=pulsar,
        elasticsearch=elasticsearch,
        janusgraph=janusgraph
    )
    
    jurisdiction_engine = JurisdictionEngine(
        ignite=ignite,
        elasticsearch=elasticsearch
    )
    
    reporting_engine = ReportingEngine(
        ignite=ignite,
        pulsar=pulsar,
        elasticsearch=elasticsearch
    )
    
    transaction_monitor = TransactionMonitor(
        ignite=ignite,
        pulsar=pulsar,
        elasticsearch=elasticsearch,
        janusgraph=janusgraph,
        aml_engine=aml_engine
    )
    
    # Start background tasks
    asyncio.create_task(transaction_monitor.start_monitoring())
    asyncio.create_task(aml_engine.start_risk_scoring_loop())
    asyncio.create_task(reporting_engine.start_scheduled_reports())
    
    # Load jurisdiction rules
    await jurisdiction_engine.load_jurisdiction_rules()
    
    logger.info("Compliance Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Compliance Service...")
    
    # Stop background tasks
    await transaction_monitor.stop_monitoring()
    await aml_engine.stop_risk_scoring_loop()
    await reporting_engine.stop_scheduled_reports()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    await elasticsearch.close()
    await janusgraph.close()
    await vault.close()
    
    logger.info("Compliance Service shut down successfully")


# Create FastAPI app
app = FastAPI(
    title="PlatformQ Compliance Service",
    description="Multi-jurisdictional regulatory compliance engine",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(kyc.router, prefix="/api/v1/kyc", tags=["KYC"])
app.include_router(aml.router, prefix="/api/v1/aml", tags=["AML"])
app.include_router(jurisdiction.router, prefix="/api/v1/jurisdiction", tags=["Jurisdiction"])
app.include_router(reporting.router, prefix="/api/v1/reporting", tags=["Reporting"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["Monitoring"])


# Health check
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "compliance-service",
        "version": "1.0.0",
        "engines": {
            "kyc": kyc_engine is not None,
            "aml": aml_engine is not None,
            "jurisdiction": jurisdiction_engine is not None,
            "reporting": reporting_engine is not None,
            "monitoring": transaction_monitor is not None
        }
    }


# Compliance status endpoint
@app.get("/api/v1/compliance/status/{user_id}")
async def get_compliance_status(user_id: str):
    """
    Get comprehensive compliance status for a user
    """
    try:
        # Get KYC status
        kyc_status = await kyc_engine.get_kyc_status(user_id)
        
        # Get AML risk score
        aml_risk = await aml_engine.get_risk_score(user_id)
        
        # Get jurisdiction restrictions
        user_jurisdiction = await jurisdiction_engine.get_user_jurisdiction(user_id)
        restrictions = await jurisdiction_engine.get_restrictions(user_jurisdiction)
        
        # Get any active alerts
        alerts = await transaction_monitor.get_user_alerts(user_id)
        
        return {
            "user_id": user_id,
            "kyc": {
                "status": kyc_status.status,
                "level": kyc_status.level,
                "verified_at": kyc_status.verified_at,
                "expires_at": kyc_status.expires_at
            },
            "aml": {
                "risk_score": aml_risk.score,
                "risk_level": aml_risk.level,
                "factors": aml_risk.factors,
                "last_updated": aml_risk.last_updated
            },
            "jurisdiction": {
                "detected": user_jurisdiction,
                "restrictions": restrictions,
                "allowed_products": restrictions.get('allowed_products', []),
                "max_leverage": restrictions.get('max_leverage', 1)
            },
            "alerts": alerts,
            "overall_status": _calculate_overall_status(kyc_status, aml_risk, alerts)
        }
        
    except Exception as e:
        logger.error(f"Error getting compliance status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Compliance check endpoint
@app.post("/api/v1/compliance/check")
async def check_compliance(request: Dict):
    """
    Check if a specific action is compliant
    """
    try:
        user_id = request['user_id']
        action = request['action']
        details = request.get('details', {})
        
        # Get user's compliance status
        kyc_status = await kyc_engine.get_kyc_status(user_id)
        if kyc_status.status != 'approved':
            return {
                "compliant": False,
                "reason": "KYC not approved",
                "required_action": "complete_kyc"
            }
        
        # Check jurisdiction restrictions
        user_jurisdiction = await jurisdiction_engine.get_user_jurisdiction(user_id)
        is_allowed = await jurisdiction_engine.check_action_allowed(
            user_jurisdiction, action, details
        )
        
        if not is_allowed:
            return {
                "compliant": False,
                "reason": "Action not allowed in jurisdiction",
                "jurisdiction": user_jurisdiction,
                "restrictions": await jurisdiction_engine.get_restrictions(user_jurisdiction)
            }
        
        # Check AML risk
        if action in ['large_withdrawal', 'high_value_trade']:
            aml_risk = await aml_engine.get_risk_score(user_id)
            if aml_risk.level == 'high':
                # Trigger enhanced due diligence
                await aml_engine.trigger_edd(user_id, action, details)
                return {
                    "compliant": False,
                    "reason": "Enhanced due diligence required",
                    "required_action": "edd_review"
                }
        
        # Check transaction limits
        if 'amount' in details:
            limit_check = await _check_transaction_limits(
                user_id, kyc_status.level, details['amount']
            )
            if not limit_check['allowed']:
                return {
                    "compliant": False,
                    "reason": limit_check['reason'],
                    "limit": limit_check['limit']
                }
        
        return {
            "compliant": True,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error checking compliance: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Audit trail endpoint
@app.get("/api/v1/compliance/audit/{user_id}")
async def get_audit_trail(
    user_id: str,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """
    Get compliance audit trail for a user
    """
    try:
        # Default to last 30 days
        if not start_date:
            start_date = datetime.utcnow() - timedelta(days=30)
        if not end_date:
            end_date = datetime.utcnow()
            
        # Get KYC events
        kyc_events = await kyc_engine.get_audit_trail(user_id, start_date, end_date)
        
        # Get AML events
        aml_events = await aml_engine.get_audit_trail(user_id, start_date, end_date)
        
        # Get transaction monitoring events
        monitoring_events = await transaction_monitor.get_audit_trail(
            user_id, start_date, end_date
        )
        
        # Combine and sort by timestamp
        all_events = kyc_events + aml_events + monitoring_events
        all_events.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return {
            "user_id": user_id,
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "events": all_events,
            "total_events": len(all_events)
        }
        
    except Exception as e:
        logger.error(f"Error getting audit trail: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _calculate_overall_status(kyc_status, aml_risk, alerts):
    """Calculate overall compliance status"""
    if kyc_status.status != 'approved':
        return 'restricted'
    if aml_risk.level == 'high' or len(alerts) > 0:
        return 'review_required'
    if aml_risk.level == 'medium':
        return 'monitored'
    return 'compliant'


async def _check_transaction_limits(user_id: str, kyc_level: int, amount: Decimal) -> Dict:
    """Check transaction limits based on KYC level"""
    limits = {
        1: {'daily': Decimal('10000'), 'monthly': Decimal('50000')},
        2: {'daily': Decimal('100000'), 'monthly': Decimal('1000000')},
        3: {'daily': Decimal('10000000'), 'monthly': Decimal('100000000')}
    }
    
    user_limits = limits.get(kyc_level, limits[1])
    
    # Get user's transaction history
    daily_volume = await transaction_monitor.get_daily_volume(user_id)
    monthly_volume = await transaction_monitor.get_monthly_volume(user_id)
    
    if daily_volume + amount > user_limits['daily']:
        return {
            'allowed': False,
            'reason': 'Daily limit exceeded',
            'limit': user_limits['daily']
        }
    
    if monthly_volume + amount > user_limits['monthly']:
        return {
            'allowed': False,
            'reason': 'Monthly limit exceeded',
            'limit': user_limits['monthly']
        }
    
    return {'allowed': True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006) 