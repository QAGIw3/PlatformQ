"""
AML Engine for transaction monitoring and compliance

Provides comprehensive anti-money laundering capabilities including:
- Transaction risk assessment
- Pattern detection
- Sanctions screening
- Regulatory reporting
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
import hashlib
import hmac

from pyignite import Client as IgniteClient
import pulsar
from ..core.ignite_cache import IgniteCacheManager

logger = logging.getLogger(__name__)


class RiskLevel(Enum):
    """Risk level categories"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class TransactionRiskAssessment:
    """Risk assessment for a transaction"""
    transaction_id: str
    user_id: str
    from_address: str
    to_address: str
    amount_usd: Decimal
    asset_type: str
    risk_score: float
    risk_level: RiskLevel
    risk_factors: List[str]
    requires_enhanced_verification: bool
    requires_manual_review: bool
    timestamp: datetime
    chain: str
    metadata: Dict[str, Any]


@dataclass
class AMLAlert:
    """Alert for suspicious activity"""
    alert_id: str
    user_id: str
    alert_type: str
    severity: str
    description: str
    triggered_by: List[str]  # Risk factors that triggered alert
    created_at: datetime
    status: str  # 'open', 'investigating', 'resolved', 'false_positive'
    metadata: Dict[str, Any]


class AMLEngine:
    """
    Core AML engine for transaction monitoring and compliance
    """
    
    def __init__(
        self,
        ignite_client: IgniteClient,
        pulsar_client: pulsar.Client,
        blockchain_gateway_url: str,
        blockchain_analytics_url: Optional[str] = None,
        high_value_threshold_usd: Decimal = Decimal("10000")
    ):
        self.ignite_client = ignite_client
        self.cache_manager = IgniteCacheManager(ignite_client)
        self.pulsar_client = pulsar_client
        self.blockchain_gateway_url = blockchain_gateway_url
        self.blockchain_analytics_url = blockchain_analytics_url
        self.high_value_threshold_usd = high_value_threshold_usd
        
        # Caches
        self._transaction_cache = None
        self._risk_cache = None
        self._alert_cache = None
        self._pattern_cache = None
        
        # Event producer
        self.event_producer = None
        
        # Risk rules
        self.risk_rules = self._initialize_risk_rules()
        
        # Monitoring task
        self._monitoring_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize the AML engine"""
        # Initialize caches using cache manager
        self._transaction_cache = await self.cache_manager.get_or_create_cache(
            "aml_transactions"
        )
        self._risk_cache = await self.cache_manager.get_or_create_cache(
            "aml_risk_assessments"
        )
        self._alert_cache = await self.cache_manager.get_or_create_cache(
            "aml_alerts"
        )
        self._pattern_cache = await self.cache_manager.get_or_create_cache(
            "aml_patterns"
        )
        
        # Create event producer
        self.event_producer = self.pulsar_client.create_producer(
            'persistent://public/compliance/aml-events'
        )
        
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("AML engine initialized")
        
    async def shutdown(self):
        """Shutdown the AML engine"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            await asyncio.gather(self._monitoring_task, return_exceptions=True)
            
        if self.event_producer:
            self.event_producer.close()
            
        logger.info("AML engine shutdown")
        
    def _initialize_risk_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize risk assessment rules"""
        return {
            "velocity_check": {
                "max_transactions_per_hour": 5,
                "max_transactions_per_day": 10,
                "max_volume_per_day_usd": Decimal("50000"),
                "max_volume_per_week_usd": Decimal("200000")
            },
            "amount_limits": {
                "unverified": {
                    "single_transaction": Decimal("500"),
                    "daily_limit": Decimal("1000"),
                    "monthly_limit": Decimal("5000")
                },
                "tier_1": {
                    "single_transaction": Decimal("10000"),
                    "daily_limit": Decimal("50000"),
                    "monthly_limit": Decimal("200000")
                },
                "tier_2": {
                    "single_transaction": Decimal("100000"),
                    "daily_limit": Decimal("500000"),
                    "monthly_limit": Decimal("2000000")
                },
                "tier_3": {
                    "single_transaction": Decimal("1000000"),
                    "daily_limit": Decimal("5000000"),
                    "monthly_limit": Decimal("20000000")
                }
            },
            "risk_indicators": {
                "high_frequency_trading": {"weight": 0.3, "threshold": 50},
                "large_single_transaction": {"weight": 0.4, "threshold": self.high_value_threshold_usd},
                "rapid_accumulation": {"weight": 0.3, "threshold": 10},
                "cross_border": {"weight": 0.2},
                "privacy_coin_usage": {"weight": 0.4},
                "mixer_usage": {"weight": 0.8},
                "new_account": {"weight": 0.2, "days": 30},
                "dormant_account_activity": {"weight": 0.3, "days": 180},
                "round_amount_pattern": {"weight": 0.1, "threshold": 5}
            },
            "pattern_detection": {
                "structuring_threshold": 0.9,  # 90% of reporting threshold
                "structuring_count": 3,  # Number of structured transactions
                "time_window_hours": 24
            }
        }
        
    async def assess_transaction_risk(
        self,
        transaction_id: str,
        user_id: str,
        from_address: str,
        to_address: str,
        amount_usd: Decimal,
        asset_type: str,
        chain: str,
        user_kyc_level: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> TransactionRiskAssessment:
        """
        Assess risk for a specific transaction
        """
        risk_factors = []
        risk_score = 0.0
        
        # Check user KYC level
        if not user_kyc_level or user_kyc_level == "unverified":
            risk_factors.append("unverified_user")
            risk_score += 0.3
            
        # Check amount against limits
        limits = self._get_user_limits(user_kyc_level)
        if amount_usd > limits["single_transaction"]:
            risk_factors.append("exceeds_transaction_limit")
            risk_score += 0.3
            
        # Check velocity
        velocity_risk = await self._check_velocity(user_id, amount_usd)
        if velocity_risk:
            risk_factors.extend(velocity_risk["factors"])
            risk_score += velocity_risk["score"]
            
        # Check if high value transaction
        if amount_usd >= self.high_value_threshold_usd:
            risk_factors.append("high_value_transaction")
            risk_score += 0.2
            
        # Check blockchain analytics
        if self.blockchain_analytics_url:
            address_risk = await self._analyze_addresses(from_address, to_address, chain)
            if address_risk["risk_score"] > 0.3:
                risk_factors.extend(address_risk["factors"])
                risk_score += address_risk["risk_score"]
                
        # Check for suspicious patterns
        pattern_risk = await self._check_patterns(user_id, amount_usd, asset_type)
        if pattern_risk:
            risk_factors.extend(pattern_risk["factors"])
            risk_score += pattern_risk["score"]
            
        # Check cross-border indicators
        if metadata and metadata.get("cross_border"):
            risk_factors.append("cross_border_transaction")
            risk_score += self.risk_rules["risk_indicators"]["cross_border"]["weight"]
            
        # Normalize risk score
        risk_score = min(risk_score, 1.0)
        risk_level = self._calculate_risk_level(risk_score)
        
        # Determine if enhanced verification needed
        requires_enhanced = (
            risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL] or
            amount_usd >= self.high_value_threshold_usd * 5 or
            (user_kyc_level in ["unverified", "tier_1"] and amount_usd >= self.high_value_threshold_usd)
        )
        
        # Determine if manual review needed
        requires_manual = (
            risk_level == RiskLevel.CRITICAL or
            len(risk_factors) >= 5 or
            any(factor in risk_factors for factor in [
                "sanctions_match", "pep_match", "mixer_usage", "darknet_exposure"
            ])
        )
        
        assessment = TransactionRiskAssessment(
            transaction_id=transaction_id,
            user_id=user_id,
            from_address=from_address,
            to_address=to_address,
            amount_usd=amount_usd,
            asset_type=asset_type,
            risk_score=risk_score,
            risk_level=risk_level,
            risk_factors=risk_factors,
            requires_enhanced_verification=requires_enhanced,
            requires_manual_review=requires_manual,
            timestamp=datetime.utcnow(),
            chain=chain,
            metadata=metadata or {}
        )
        
        # Store assessment
        await self._risk_cache.put(transaction_id, assessment)
        
        # Store transaction for pattern analysis
        user_key = f"user_txns:{user_id}"
        user_transactions = await self._transaction_cache.get(user_key) or []
        user_transactions.append({
            "transaction_id": transaction_id,
            "amount_usd": str(amount_usd),
            "timestamp": assessment.timestamp.isoformat(),
            "risk_level": risk_level.value
        })
        await self._transaction_cache.put(user_key, user_transactions)
        
        # Create alert if needed
        if requires_manual or risk_level == RiskLevel.CRITICAL:
            await self._create_alert(user_id, assessment)
            
        # Emit event
        await self._emit_event("risk_assessment_completed", {
            "transaction_id": transaction_id,
            "user_id": user_id,
            "risk_level": risk_level.value,
            "risk_score": risk_score,
            "requires_manual_review": requires_manual
        })
        
        logger.info(
            f"Transaction {transaction_id} risk assessment: "
            f"{risk_level.value} (score: {risk_score:.2f})"
        )
        
        return assessment
        
    async def _check_velocity(
        self,
        user_id: str,
        amount_usd: Decimal
    ) -> Optional[Dict[str, Any]]:
        """Check transaction velocity for user"""
        user_key = f"user_txns:{user_id}"
        user_transactions = await self._transaction_cache.get(user_key) or []
        
        if not user_transactions:
            return None
            
        now = datetime.utcnow()
        risk_score = 0.0
        factors = []
        
        # Parse transactions
        transactions = []
        for txn in user_transactions:
            transactions.append({
                "amount": Decimal(txn["amount_usd"]),
                "timestamp": datetime.fromisoformat(txn["timestamp"])
            })
            
        # Check hourly velocity
        hour_ago = now - timedelta(hours=1)
        hourly_txns = [t for t in transactions if t["timestamp"] > hour_ago]
        
        if len(hourly_txns) >= self.risk_rules["velocity_check"]["max_transactions_per_hour"]:
            factors.append("high_hourly_velocity")
            risk_score += 0.3
            
        # Check daily velocity
        day_ago = now - timedelta(days=1)
        daily_txns = [t for t in transactions if t["timestamp"] > day_ago]
        
        if len(daily_txns) >= self.risk_rules["velocity_check"]["max_transactions_per_day"]:
            factors.append("high_daily_velocity")
            risk_score += 0.3
            
        # Check daily volume
        daily_volume = sum(t["amount"] for t in daily_txns) + amount_usd
        if daily_volume > self.risk_rules["velocity_check"]["max_volume_per_day_usd"]:
            factors.append("high_daily_volume")
            risk_score += 0.4
            
        # Check weekly volume
        week_ago = now - timedelta(days=7)
        weekly_txns = [t for t in transactions if t["timestamp"] > week_ago]
        weekly_volume = sum(t["amount"] for t in weekly_txns) + amount_usd
        
        if weekly_volume > self.risk_rules["velocity_check"]["max_volume_per_week_usd"]:
            factors.append("high_weekly_volume")
            risk_score += 0.3
            
        if factors:
            return {"score": risk_score, "factors": factors}
            
        return None
        
    async def _check_patterns(
        self,
        user_id: str,
        amount: Decimal,
        asset_type: str
    ) -> Optional[Dict[str, Any]]:
        """Check for suspicious transaction patterns"""
        pattern_key = f"patterns:{user_id}"
        patterns = await self._pattern_cache.get(pattern_key) or {}
        
        risk_score = 0.0
        factors = []
        
        # Structuring detection
        threshold = self.high_value_threshold_usd * Decimal(str(
            self.risk_rules["pattern_detection"]["structuring_threshold"]
        ))
        
        if threshold <= amount < self.high_value_threshold_usd:
            # Check recent structuring patterns
            structuring_key = f"structuring:{user_id}"
            structuring_count = patterns.get("structuring_count", 0) + 1
            
            if structuring_count >= self.risk_rules["pattern_detection"]["structuring_count"]:
                factors.append("potential_structuring")
                risk_score += 0.5
                
            patterns["structuring_count"] = structuring_count
            patterns["last_structuring"] = datetime.utcnow().isoformat()
            
        # Round amount pattern (potential automation)
        if amount % 100 == 0 and amount > 1000:
            round_count = patterns.get("round_amount_count", 0) + 1
            
            if round_count >= self.risk_rules["risk_indicators"]["round_amount_pattern"]["threshold"]:
                factors.append("automated_trading_pattern")
                risk_score += self.risk_rules["risk_indicators"]["round_amount_pattern"]["weight"]
                
            patterns["round_amount_count"] = round_count
            
        # Rapid accumulation pattern
        accumulation_window = patterns.get("accumulation_start")
        if accumulation_window:
            window_start = datetime.fromisoformat(accumulation_window)
            if datetime.utcnow() - window_start < timedelta(hours=24):
                accumulation_amount = Decimal(patterns.get("accumulation_amount", "0"))
                accumulation_amount += amount
                
                if accumulation_amount > self.high_value_threshold_usd * 10:
                    factors.append("rapid_accumulation")
                    risk_score += self.risk_rules["risk_indicators"]["rapid_accumulation"]["weight"]
                    
                patterns["accumulation_amount"] = str(accumulation_amount)
            else:
                # Reset accumulation window
                patterns["accumulation_start"] = datetime.utcnow().isoformat()
                patterns["accumulation_amount"] = str(amount)
        else:
            patterns["accumulation_start"] = datetime.utcnow().isoformat()
            patterns["accumulation_amount"] = str(amount)
            
        # Update patterns cache
        await self._pattern_cache.put(pattern_key, patterns)
        
        if factors:
            return {"score": risk_score, "factors": factors}
            
        return None
        
    async def _analyze_addresses(
        self,
        from_address: str,
        to_address: str,
        chain: str
    ) -> Dict[str, Any]:
        """Analyze blockchain addresses for risk indicators"""
        # This would integrate with blockchain analytics providers
        # For now, return mock data
        risk_score = 0.0
        factors = []
        
        # In production, this would call external blockchain analytics APIs
        # Example: Chainalysis, Elliptic, TRM Labs
        
        return {"risk_score": risk_score, "factors": factors}
        
    async def _create_alert(
        self,
        user_id: str,
        assessment: TransactionRiskAssessment
    ):
        """Create an AML alert for manual review"""
        alert = AMLAlert(
            alert_id=f"alert_{assessment.transaction_id}",
            user_id=user_id,
            alert_type="high_risk_transaction",
            severity=assessment.risk_level.value,
            description=f"High risk transaction detected: {', '.join(assessment.risk_factors)}",
            triggered_by=assessment.risk_factors,
            created_at=datetime.utcnow(),
            status="open",
            metadata={
                "transaction_id": assessment.transaction_id,
                "amount_usd": str(assessment.amount_usd),
                "risk_score": assessment.risk_score
            }
        )
        
        await self._alert_cache.put(alert.alert_id, alert)
        
        # Emit alert event
        await self._emit_event("aml_alert_created", {
            "alert_id": alert.alert_id,
            "user_id": user_id,
            "severity": alert.severity,
            "alert_type": alert.alert_type
        })
        
    def _get_user_limits(self, kyc_level: Optional[str]) -> Dict[str, Decimal]:
        """Get transaction limits based on KYC level"""
        if not kyc_level or kyc_level == "unverified":
            return self.risk_rules["amount_limits"]["unverified"]
        return self.risk_rules["amount_limits"].get(
            kyc_level,
            self.risk_rules["amount_limits"]["tier_1"]
        )
        
    def _calculate_risk_level(self, risk_score: float) -> RiskLevel:
        """Calculate risk level from score"""
        if risk_score < 0.3:
            return RiskLevel.LOW
        elif risk_score < 0.6:
            return RiskLevel.MEDIUM
        elif risk_score < 0.8:
            return RiskLevel.HIGH
        else:
            return RiskLevel.CRITICAL
            
    async def _monitoring_loop(self):
        """Background monitoring for patterns and alerts"""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                
                # Clean old pattern data
                await self._clean_old_patterns()
                
                # Check for alert escalation
                await self._check_alert_escalation()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in AML monitoring loop: {e}")
                
    async def _clean_old_patterns(self):
        """Clean old pattern detection data"""
        # Implementation for cleaning old cached patterns
        pass
        
    async def _check_alert_escalation(self):
        """Check if any alerts need escalation"""
        # Implementation for alert escalation logic
        pass
        
    async def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit an AML event"""
        if self.event_producer:
            import json
            event = {
                "type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat(),
                "service": "compliance-aml"
            }
            
            self.event_producer.send(
                json.dumps(event).encode('utf-8')
            )
            
    async def get_user_risk_profile(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive risk profile for a user"""
        # Get transaction history
        user_key = f"user_txns:{user_id}"
        transactions = await self._transaction_cache.get(user_key) or []
        
        # Get patterns
        pattern_key = f"patterns:{user_id}"
        patterns = await self._pattern_cache.get(pattern_key) or {}
        
        # Calculate aggregate risk
        total_transactions = len(transactions)
        if total_transactions == 0:
            return {
                "user_id": user_id,
                "risk_level": RiskLevel.LOW.value,
                "total_transactions": 0,
                "patterns_detected": []
            }
            
        # Analyze transaction history
        high_risk_count = sum(
            1 for txn in transactions
            if txn.get("risk_level") in [RiskLevel.HIGH.value, RiskLevel.CRITICAL.value]
        )
        
        risk_ratio = high_risk_count / total_transactions
        
        # Determine overall risk level
        if risk_ratio > 0.5:
            overall_risk = RiskLevel.HIGH
        elif risk_ratio > 0.2:
            overall_risk = RiskLevel.MEDIUM
        else:
            overall_risk = RiskLevel.LOW
            
        return {
            "user_id": user_id,
            "risk_level": overall_risk.value,
            "total_transactions": total_transactions,
            "high_risk_transactions": high_risk_count,
            "patterns_detected": list(patterns.keys()),
            "last_activity": transactions[-1]["timestamp"] if transactions else None
        } 