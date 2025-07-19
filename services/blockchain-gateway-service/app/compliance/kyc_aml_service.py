"""
KYC/AML Service

Identity verification and anti-money laundering for high-value transactions
"""

import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import asyncio
import httpx
import hashlib
import hmac
from decimal import Decimal

logger = logging.getLogger(__name__)


class VerificationStatus(Enum):
    NOT_VERIFIED = "not_verified"
    PENDING = "pending"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"
    SUSPENDED = "suspended"


class RiskLevel(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class VerificationLevel(Enum):
    BASIC = "basic"  # Email + basic info
    STANDARD = "standard"  # ID verification
    ENHANCED = "enhanced"  # ID + address proof
    PREMIUM = "premium"  # Enhanced + source of funds


@dataclass
class UserVerification:
    user_id: str
    wallet_address: str
    verification_level: VerificationLevel
    status: VerificationStatus
    risk_score: float
    risk_level: RiskLevel
    verified_at: Optional[datetime]
    expires_at: Optional[datetime]
    documents: List[Dict[str, Any]]
    verification_data: Dict[str, Any]
    flags: List[str]


@dataclass
class TransactionRiskAssessment:
    transaction_id: str
    user_id: str
    amount_usd: Decimal
    risk_score: float
    risk_level: RiskLevel
    risk_factors: List[str]
    requires_enhanced_verification: bool
    requires_manual_review: bool
    timestamp: datetime


class KYCAMLService:
    """Manages KYC/AML verification for marketplace transactions"""
    
    def __init__(self,
                 verification_provider_url: str = None,
                 verification_api_key: str = None,
                 blockchain_analytics_url: str = None,
                 high_value_threshold_usd: Decimal = Decimal("10000")):
        self.verification_provider_url = verification_provider_url
        self.verification_api_key = verification_api_key
        self.blockchain_analytics_url = blockchain_analytics_url
        self.high_value_threshold_usd = high_value_threshold_usd
        
        # User verifications
        self.user_verifications: Dict[str, UserVerification] = {}
        
        # Transaction monitoring
        self.transaction_history: Dict[str, List[TransactionRiskAssessment]] = {}
        self.suspicious_patterns: Dict[str, List[Dict[str, Any]]] = {}
        
        # Sanctions and PEP lists (would be loaded from external source)
        self.sanctions_list: Set[str] = set()
        self.pep_list: Set[str] = set()
        
        # Risk rules
        self.risk_rules = self._initialize_risk_rules()
        
        # Monitoring
        self._monitoring_task: Optional[asyncio.Task] = None
        
    def _initialize_risk_rules(self) -> Dict[str, Dict[str, Any]]:
        """Initialize risk assessment rules"""
        return {
            "velocity_check": {
                "max_transactions_per_day": 10,
                "max_volume_per_day_usd": Decimal("50000"),
                "max_transactions_per_hour": 5
            },
            "amount_limits": {
                "basic": {
                    "single_transaction": Decimal("1000"),
                    "daily_limit": Decimal("5000"),
                    "monthly_limit": Decimal("20000")
                },
                "standard": {
                    "single_transaction": Decimal("10000"),
                    "daily_limit": Decimal("50000"),
                    "monthly_limit": Decimal("200000")
                },
                "enhanced": {
                    "single_transaction": Decimal("50000"),
                    "daily_limit": Decimal("250000"),
                    "monthly_limit": Decimal("1000000")
                },
                "premium": {
                    "single_transaction": Decimal("500000"),
                    "daily_limit": Decimal("2500000"),
                    "monthly_limit": Decimal("10000000")
                }
            },
            "risk_indicators": {
                "high_frequency_trading": {"weight": 0.3, "threshold": 50},
                "large_single_transaction": {"weight": 0.4, "threshold": self.high_value_threshold_usd},
                "rapid_accumulation": {"weight": 0.3, "threshold": 10},
                "cross_border": {"weight": 0.2},
                "privacy_coin_usage": {"weight": 0.4},
                "mixer_usage": {"weight": 0.8},
                "new_account": {"weight": 0.2, "days": 30}
            }
        }
        
    async def start(self):
        """Start KYC/AML service"""
        # Load sanctions and PEP lists
        await self._load_compliance_lists()
        
        # Start monitoring
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("KYC/AML service started")
        
    async def stop(self):
        """Stop KYC/AML service"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
        logger.info("KYC/AML service stopped")
        
    async def verify_user(self,
                         user_id: str,
                         wallet_address: str,
                         verification_data: Dict[str, Any],
                         requested_level: VerificationLevel = VerificationLevel.BASIC) -> UserVerification:
        """Perform user verification"""
        # Check if already verified
        if user_id in self.user_verifications:
            existing = self.user_verifications[user_id]
            if existing.status == VerificationStatus.VERIFIED and existing.expires_at > datetime.utcnow():
                return existing
                
        # Create verification record
        verification = UserVerification(
            user_id=user_id,
            wallet_address=wallet_address,
            verification_level=requested_level,
            status=VerificationStatus.PENDING,
            risk_score=0.0,
            risk_level=RiskLevel.LOW,
            verified_at=None,
            expires_at=None,
            documents=[],
            verification_data=verification_data,
            flags=[]
        )
        
        # Perform verification based on level
        if requested_level == VerificationLevel.BASIC:
            verification = await self._verify_basic(verification)
        elif requested_level == VerificationLevel.STANDARD:
            verification = await self._verify_standard(verification)
        elif requested_level == VerificationLevel.ENHANCED:
            verification = await self._verify_enhanced(verification)
        elif requested_level == VerificationLevel.PREMIUM:
            verification = await self._verify_premium(verification)
            
        # Blockchain analysis
        blockchain_risk = await self._analyze_blockchain_address(wallet_address)
        verification.risk_score = max(verification.risk_score, blockchain_risk["risk_score"])
        verification.flags.extend(blockchain_risk.get("flags", []))
        
        # Determine final risk level
        verification.risk_level = self._calculate_risk_level(verification.risk_score)
        
        # Store verification
        self.user_verifications[user_id] = verification
        
        logger.info(f"User {user_id} verification completed: {verification.status} ({verification.risk_level})")
        return verification
        
    async def assess_transaction_risk(self,
                                    transaction_id: str,
                                    user_id: str,
                                    from_address: str,
                                    to_address: str,
                                    amount_usd: Decimal,
                                    asset_type: str) -> TransactionRiskAssessment:
        """Assess risk for a specific transaction"""
        risk_factors = []
        risk_score = 0.0
        
        # Get user verification
        user_verification = self.user_verifications.get(user_id)
        
        if not user_verification:
            risk_factors.append("unverified_user")
            risk_score += 0.5
            
        # Check amount against limits
        if user_verification:
            limits = self.risk_rules["amount_limits"].get(
                user_verification.verification_level.value, 
                self.risk_rules["amount_limits"]["basic"]
            )
            
            if amount_usd > limits["single_transaction"]:
                risk_factors.append("exceeds_transaction_limit")
                risk_score += 0.3
                
            # Check daily volume
            daily_volume = await self._get_user_daily_volume(user_id)
            if daily_volume + amount_usd > limits["daily_limit"]:
                risk_factors.append("exceeds_daily_limit")
                risk_score += 0.4
                
        # Check velocity
        velocity_risk = await self._check_velocity(user_id)
        if velocity_risk:
            risk_factors.extend(velocity_risk["factors"])
            risk_score += velocity_risk["score"]
            
        # Check if high value transaction
        if amount_usd >= self.high_value_threshold_usd:
            risk_factors.append("high_value_transaction")
            risk_score += 0.2
            
        # Check blockchain analytics
        address_risk = await self._analyze_transaction_addresses(from_address, to_address)
        if address_risk["risk_score"] > 0.3:
            risk_factors.extend(address_risk["factors"])
            risk_score += address_risk["risk_score"]
            
        # Check for suspicious patterns
        pattern_risk = await self._check_suspicious_patterns(user_id, amount_usd)
        if pattern_risk:
            risk_factors.extend(pattern_risk["factors"])
            risk_score += pattern_risk["score"]
            
        # Normalize risk score
        risk_score = min(risk_score, 1.0)
        risk_level = self._calculate_risk_level(risk_score)
        
        # Determine if enhanced verification needed
        requires_enhanced = (
            risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL] or
            amount_usd >= self.high_value_threshold_usd * 5 or
            (user_verification and user_verification.verification_level == VerificationLevel.BASIC and amount_usd >= self.high_value_threshold_usd)
        )
        
        # Determine if manual review needed
        requires_manual = (
            risk_level == RiskLevel.CRITICAL or
            len(risk_factors) >= 5 or
            "sanctions_match" in risk_factors or
            "pep_match" in risk_factors
        )
        
        assessment = TransactionRiskAssessment(
            transaction_id=transaction_id,
            user_id=user_id,
            amount_usd=amount_usd,
            risk_score=risk_score,
            risk_level=risk_level,
            risk_factors=risk_factors,
            requires_enhanced_verification=requires_enhanced,
            requires_manual_review=requires_manual,
            timestamp=datetime.utcnow()
        )
        
        # Store assessment
        if user_id not in self.transaction_history:
            self.transaction_history[user_id] = []
        self.transaction_history[user_id].append(assessment)
        
        # Update user risk profile if needed
        if user_verification and risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            await self._update_user_risk_profile(user_id, assessment)
            
        logger.info(f"Transaction {transaction_id} risk assessment: {risk_level} (score: {risk_score:.2f})")
        return assessment
        
    async def _verify_basic(self, verification: UserVerification) -> UserVerification:
        """Perform basic verification (email, phone)"""
        try:
            # Verify email
            email_verified = verification.verification_data.get("email_verified", False)
            
            # Verify phone
            phone_verified = verification.verification_data.get("phone_verified", False)
            
            if email_verified and phone_verified:
                verification.status = VerificationStatus.VERIFIED
                verification.verified_at = datetime.utcnow()
                verification.expires_at = datetime.utcnow() + timedelta(days=365)
                verification.risk_score = 0.1
            else:
                verification.status = VerificationStatus.REJECTED
                verification.flags.append("incomplete_basic_verification")
                verification.risk_score = 0.5
                
        except Exception as e:
            logger.error(f"Basic verification failed: {e}")
            verification.status = VerificationStatus.REJECTED
            verification.risk_score = 0.8
            
        return verification
        
    async def _verify_standard(self, verification: UserVerification) -> UserVerification:
        """Perform standard verification (ID verification)"""
        if not self.verification_provider_url:
            # Simulate verification
            verification.status = VerificationStatus.VERIFIED
            verification.verified_at = datetime.utcnow()
            verification.expires_at = datetime.utcnow() + timedelta(days=365)
            verification.risk_score = 0.15
            return verification
            
        try:
            # Call external verification provider
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.verification_provider_url}/verify/id",
                    json={
                        "user_id": verification.user_id,
                        "documents": verification.verification_data.get("id_documents", [])
                    },
                    headers={"Authorization": f"Bearer {self.verification_api_key}"}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if result["verified"]:
                        verification.status = VerificationStatus.VERIFIED
                        verification.verified_at = datetime.utcnow()
                        verification.expires_at = datetime.utcnow() + timedelta(days=365)
                        verification.risk_score = result.get("risk_score", 0.2)
                        
                        # Check sanctions
                        if await self._check_sanctions(result.get("full_name", "")):
                            verification.status = VerificationStatus.REJECTED
                            verification.flags.append("sanctions_match")
                            verification.risk_score = 1.0
                    else:
                        verification.status = VerificationStatus.REJECTED
                        verification.flags.append("id_verification_failed")
                        verification.risk_score = 0.7
                        
        except Exception as e:
            logger.error(f"Standard verification failed: {e}")
            verification.status = VerificationStatus.REJECTED
            verification.risk_score = 0.8
            
        return verification
        
    async def _verify_enhanced(self, verification: UserVerification) -> UserVerification:
        """Perform enhanced verification (ID + address proof)"""
        # First do standard verification
        verification = await self._verify_standard(verification)
        
        if verification.status != VerificationStatus.VERIFIED:
            return verification
            
        # Additional address verification
        address_verified = verification.verification_data.get("address_verified", False)
        
        if not address_verified:
            verification.flags.append("address_not_verified")
            verification.risk_score += 0.1
            
        return verification
        
    async def _verify_premium(self, verification: UserVerification) -> UserVerification:
        """Perform premium verification (Enhanced + source of funds)"""
        # First do enhanced verification
        verification = await self._verify_enhanced(verification)
        
        if verification.status != VerificationStatus.VERIFIED:
            return verification
            
        # Source of funds verification
        sof_documents = verification.verification_data.get("source_of_funds", [])
        
        if not sof_documents:
            verification.flags.append("no_source_of_funds")
            verification.risk_score += 0.2
        else:
            # Verify source of funds documents
            sof_verified = await self._verify_source_of_funds(sof_documents)
            if not sof_verified:
                verification.flags.append("source_of_funds_unverified")
                verification.risk_score += 0.15
                
        return verification
        
    async def _analyze_blockchain_address(self, address: str) -> Dict[str, Any]:
        """Analyze blockchain address for risk indicators"""
        risk_score = 0.0
        flags = []
        
        if not self.blockchain_analytics_url:
            # Basic analysis
            return {"risk_score": 0.1, "flags": []}
            
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.blockchain_analytics_url}/address/{address}/risk",
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check various risk indicators
                    if data.get("is_mixer"):
                        flags.append("mixer_interaction")
                        risk_score += 0.5
                        
                    if data.get("is_exchange") and data.get("exchange_risk_score", 0) > 0.7:
                        flags.append("high_risk_exchange")
                        risk_score += 0.3
                        
                    if data.get("darknet_exposure", 0) > 0:
                        flags.append("darknet_exposure")
                        risk_score += 0.6
                        
                    if data.get("sanctions_exposure", 0) > 0:
                        flags.append("sanctions_exposure")
                        risk_score += 0.8
                        
                    risk_score = min(risk_score + data.get("risk_score", 0), 1.0)
                    
        except Exception as e:
            logger.error(f"Blockchain analysis failed: {e}")
            
        return {"risk_score": risk_score, "flags": flags}
        
    async def _check_velocity(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Check transaction velocity"""
        if user_id not in self.transaction_history:
            return None
            
        history = self.transaction_history[user_id]
        now = datetime.utcnow()
        
        risk_score = 0.0
        factors = []
        
        # Check hourly velocity
        hour_ago = now - timedelta(hours=1)
        hourly_txns = [t for t in history if t.timestamp > hour_ago]
        
        if len(hourly_txns) > self.risk_rules["velocity_check"]["max_transactions_per_hour"]:
            factors.append("high_hourly_velocity")
            risk_score += 0.3
            
        # Check daily velocity
        day_ago = now - timedelta(days=1)
        daily_txns = [t for t in history if t.timestamp > day_ago]
        
        if len(daily_txns) > self.risk_rules["velocity_check"]["max_transactions_per_day"]:
            factors.append("high_daily_velocity")
            risk_score += 0.3
            
        # Check daily volume
        daily_volume = sum(t.amount_usd for t in daily_txns)
        if daily_volume > self.risk_rules["velocity_check"]["max_volume_per_day_usd"]:
            factors.append("high_daily_volume")
            risk_score += 0.4
            
        if factors:
            return {"score": risk_score, "factors": factors}
            
        return None
        
    async def _check_suspicious_patterns(self, user_id: str, amount: Decimal) -> Optional[Dict[str, Any]]:
        """Check for suspicious transaction patterns"""
        if user_id not in self.transaction_history:
            return None
            
        history = self.transaction_history[user_id]
        
        risk_score = 0.0
        factors = []
        
        # Structuring: Multiple transactions just below reporting threshold
        threshold = self.high_value_threshold_usd * Decimal("0.9")
        recent_txns = history[-10:]
        
        structuring_txns = [t for t in recent_txns if threshold <= t.amount_usd < self.high_value_threshold_usd]
        if len(structuring_txns) >= 3:
            factors.append("potential_structuring")
            risk_score += 0.5
            
        # Rapid accumulation
        if len(history) >= 10:
            total_volume = sum(t.amount_usd for t in history[-10:])
            time_span = (history[-1].timestamp - history[-10].timestamp).total_seconds() / 3600
            
            if time_span < 24 and total_volume > self.high_value_threshold_usd * 10:
                factors.append("rapid_accumulation")
                risk_score += 0.4
                
        # Round amount transactions (potential automation)
        if amount % 100 == 0 and amount > 1000:
            round_txns = [t for t in recent_txns if t.amount_usd % 100 == 0]
            if len(round_txns) >= 5:
                factors.append("automated_trading_pattern")
                risk_score += 0.2
                
        if factors:
            # Store pattern for future reference
            if user_id not in self.suspicious_patterns:
                self.suspicious_patterns[user_id] = []
            self.suspicious_patterns[user_id].append({
                "timestamp": datetime.utcnow(),
                "patterns": factors,
                "risk_score": risk_score
            })
            
            return {"score": risk_score, "factors": factors}
            
        return None
        
    async def _get_user_daily_volume(self, user_id: str) -> Decimal:
        """Get user's daily transaction volume"""
        if user_id not in self.transaction_history:
            return Decimal("0")
            
        day_ago = datetime.utcnow() - timedelta(days=1)
        daily_txns = [t for t in self.transaction_history[user_id] if t.timestamp > day_ago]
        
        return sum(t.amount_usd for t in daily_txns)
        
    async def _analyze_transaction_addresses(self, from_address: str, to_address: str) -> Dict[str, Any]:
        """Analyze transaction addresses for risk"""
        from_risk = await self._analyze_blockchain_address(from_address)
        to_risk = await self._analyze_blockchain_address(to_address)
        
        combined_score = max(from_risk["risk_score"], to_risk["risk_score"])
        combined_factors = from_risk["flags"] + to_risk["flags"]
        
        return {"risk_score": combined_score, "factors": combined_factors}
        
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
            
    async def _check_sanctions(self, name: str) -> bool:
        """Check if name appears on sanctions list"""
        # Normalize name
        normalized_name = name.upper().strip()
        
        # In production, this would check against real sanctions lists
        return normalized_name in self.sanctions_list
        
    async def _verify_source_of_funds(self, documents: List[Dict[str, Any]]) -> bool:
        """Verify source of funds documents"""
        # In production, this would analyze documents
        # For now, simple validation
        required_docs = ["bank_statement", "employment_proof", "tax_return"]
        provided_types = [doc.get("type") for doc in documents]
        
        return all(req in provided_types for req in required_docs)
        
    async def _update_user_risk_profile(self, user_id: str, assessment: TransactionRiskAssessment):
        """Update user's risk profile based on transaction"""
        verification = self.user_verifications.get(user_id)
        if not verification:
            return
            
        # Increase risk score if multiple high-risk transactions
        high_risk_count = sum(
            1 for t in self.transaction_history[user_id][-10:]
            if t.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]
        )
        
        if high_risk_count >= 3:
            verification.risk_score = min(verification.risk_score + 0.1, 1.0)
            verification.risk_level = self._calculate_risk_level(verification.risk_score)
            verification.flags.append("multiple_high_risk_transactions")
            
    async def _load_compliance_lists(self):
        """Load sanctions and PEP lists"""
        # In production, load from official sources
        # For now, use sample data
        self.sanctions_list = {
            "SANCTIONED ENTITY 1",
            "SANCTIONED ENTITY 2",
            # ... more entries
        }
        
        self.pep_list = {
            "POLITICAL FIGURE 1",
            "POLITICAL FIGURE 2",
            # ... more entries
        }
        
    async def _monitoring_loop(self):
        """Monitor for compliance updates and expired verifications"""
        while True:
            try:
                await asyncio.sleep(3600)  # Check every hour
                
                # Update compliance lists
                await self._load_compliance_lists()
                
                # Check for expired verifications
                now = datetime.utcnow()
                for user_id, verification in self.user_verifications.items():
                    if verification.expires_at and verification.expires_at < now:
                        verification.status = VerificationStatus.EXPIRED
                        logger.info(f"User {user_id} verification expired")
                        
                # Clean old transaction history (keep 90 days)
                cutoff = now - timedelta(days=90)
                for user_id in list(self.transaction_history.keys()):
                    self.transaction_history[user_id] = [
                        t for t in self.transaction_history[user_id]
                        if t.timestamp > cutoff
                    ]
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                
    async def get_compliance_report(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate compliance report for period"""
        report = {
            "period": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "verifications": {
                "total": 0,
                "by_level": {},
                "by_status": {},
                "by_risk_level": {}
            },
            "transactions": {
                "total": 0,
                "total_volume_usd": Decimal("0"),
                "by_risk_level": {},
                "flagged_for_review": 0,
                "blocked": 0
            },
            "suspicious_activity": {
                "patterns_detected": 0,
                "users_flagged": 0,
                "sanctions_matches": 0
            }
        }
        
        # Aggregate verification data
        for verification in self.user_verifications.values():
            if verification.verified_at and start_date <= verification.verified_at <= end_date:
                report["verifications"]["total"] += 1
                
                level = verification.verification_level.value
                report["verifications"]["by_level"][level] = report["verifications"]["by_level"].get(level, 0) + 1
                
                status = verification.status.value
                report["verifications"]["by_status"][status] = report["verifications"]["by_status"].get(status, 0) + 1
                
                risk = verification.risk_level.value
                report["verifications"]["by_risk_level"][risk] = report["verifications"]["by_risk_level"].get(risk, 0) + 1
                
        # Aggregate transaction data
        for user_history in self.transaction_history.values():
            for transaction in user_history:
                if start_date <= transaction.timestamp <= end_date:
                    report["transactions"]["total"] += 1
                    report["transactions"]["total_volume_usd"] += transaction.amount_usd
                    
                    risk = transaction.risk_level.value
                    if risk not in report["transactions"]["by_risk_level"]:
                        report["transactions"]["by_risk_level"][risk] = {"count": 0, "volume": Decimal("0")}
                    report["transactions"]["by_risk_level"][risk]["count"] += 1
                    report["transactions"]["by_risk_level"][risk]["volume"] += transaction.amount_usd
                    
                    if transaction.requires_manual_review:
                        report["transactions"]["flagged_for_review"] += 1
                        
        # Count suspicious patterns
        for user_patterns in self.suspicious_patterns.values():
            user_flagged = False
            for pattern in user_patterns:
                if start_date <= pattern["timestamp"] <= end_date:
                    report["suspicious_activity"]["patterns_detected"] += 1
                    user_flagged = True
                    
            if user_flagged:
                report["suspicious_activity"]["users_flagged"] += 1
                
        return report 