"""
Risk Assessment Engine for comprehensive compliance risk evaluation
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
import asyncio

import httpx
from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


class ComplianceProvider(Enum):
    """External compliance providers"""
    CHAINALYSIS = "chainalysis"
    ELLIPTIC = "elliptic"
    TRM_LABS = "trm_labs"
    COINFIRM = "coinfirm"
    MERKLE_SCIENCE = "merkle_science"


@dataclass 
class BlockchainAnalytics:
    """Blockchain analytics results"""
    address: str
    risk_score: float
    risk_category: str
    exposure_types: List[str]
    last_activity: datetime
    total_received: Decimal
    total_sent: Decimal
    counterparties: int
    provider: ComplianceProvider
    raw_data: Dict[str, Any]


@dataclass
class SanctionsCheckResult:
    """Sanctions screening result"""
    entity_name: str
    is_sanctioned: bool
    match_score: float
    lists_matched: List[str]
    aliases: List[str]
    metadata: Dict[str, Any]


class RiskAssessmentEngine:
    """
    Advanced risk assessment engine that integrates with multiple
    blockchain analytics and compliance providers
    """
    
    def __init__(
        self,
        ignite_client: IgniteClient,
        provider_configs: Dict[str, Dict[str, str]]
    ):
        self.ignite_client = ignite_client
        self.provider_configs = provider_configs
        
        # Provider clients
        self.provider_clients = {}
        self._initialize_providers()
        
        # Caches
        self._analytics_cache = None
        self._sanctions_cache = None
        self._pep_cache = None
        
        # Lists (would be loaded from external sources)
        self.sanctions_lists = {
            "OFAC": set(),  # US Treasury
            "UN": set(),     # United Nations
            "EU": set(),     # European Union
            "UK": set()      # UK Treasury
        }
        
        self.pep_database = {}  # Politically Exposed Persons
        
    async def initialize(self):
        """Initialize the risk assessment engine"""
        # Initialize caches
        self._analytics_cache = await self.ignite_client.get_or_create_cache(
            "blockchain_analytics"
        )
        self._sanctions_cache = await self.ignite_client.get_or_create_cache(
            "sanctions_checks"
        )
        self._pep_cache = await self.ignite_client.get_or_create_cache(
            "pep_checks"
        )
        
        # Load compliance lists
        await self._load_compliance_lists()
        
        logger.info("Risk assessment engine initialized")
        
    def _initialize_providers(self):
        """Initialize provider HTTP clients"""
        for provider, config in self.provider_configs.items():
            self.provider_clients[provider] = httpx.AsyncClient(
                base_url=config.get("base_url"),
                headers={
                    "Authorization": f"Bearer {config.get('api_key')}",
                    "Content-Type": "application/json"
                },
                timeout=30.0
            )
            
    async def analyze_blockchain_address(
        self,
        address: str,
        chain: str,
        providers: Optional[List[ComplianceProvider]] = None
    ) -> Dict[str, BlockchainAnalytics]:
        """
        Analyze a blockchain address using multiple providers
        """
        # Check cache first
        cache_key = f"{chain}:{address}"
        cached = await self._analytics_cache.get(cache_key)
        
        if cached and self._is_cache_valid(cached.get("timestamp")):
            return cached["data"]
            
        # Use all providers if none specified
        if not providers:
            providers = list(ComplianceProvider)
            
        results = {}
        tasks = []
        
        for provider in providers:
            if provider.value in self.provider_clients:
                tasks.append(
                    self._analyze_with_provider(address, chain, provider)
                )
                
        # Run all provider checks in parallel
        provider_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in provider_results:
            if isinstance(result, BlockchainAnalytics):
                results[result.provider.value] = result
            else:
                logger.error(f"Provider analysis failed: {result}")
                
        # Cache results
        await self._analytics_cache.put(cache_key, {
            "data": results,
            "timestamp": datetime.utcnow()
        })
        
        return results
        
    async def _analyze_with_provider(
        self,
        address: str,
        chain: str,
        provider: ComplianceProvider
    ) -> BlockchainAnalytics:
        """Analyze address with specific provider"""
        client = self.provider_clients[provider.value]
        
        try:
            if provider == ComplianceProvider.CHAINALYSIS:
                response = await client.get(
                    f"/api/v2/addresses/{address}",
                    params={"chain": chain}
                )
            elif provider == ComplianceProvider.ELLIPTIC:
                response = await client.post(
                    "/v2/wallet/synchronous",
                    json={
                        "subject": {
                            "asset": chain.upper(),
                            "hash": address,
                            "type": "address"
                        }
                    }
                )
            elif provider == ComplianceProvider.TRM_LABS:
                response = await client.get(
                    f"/v1/entity/{chain}/{address}"
                )
            else:
                # Generic endpoint
                response = await client.get(
                    f"/address/{address}",
                    params={"blockchain": chain}
                )
                
            if response.status_code == 200:
                data = response.json()
                return self._parse_provider_response(
                    data, address, chain, provider
                )
            else:
                raise Exception(f"Provider returned {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error analyzing with {provider.value}: {e}")
            raise
            
    def _parse_provider_response(
        self,
        data: Dict[str, Any],
        address: str,
        chain: str,
        provider: ComplianceProvider
    ) -> BlockchainAnalytics:
        """Parse provider-specific response into standard format"""
        # Provider-specific parsing logic
        if provider == ComplianceProvider.CHAINALYSIS:
            return BlockchainAnalytics(
                address=address,
                risk_score=data.get("risk", 0.0),
                risk_category=data.get("category", "unknown"),
                exposure_types=data.get("exposures", []),
                last_activity=datetime.fromisoformat(
                    data.get("lastActivity", datetime.utcnow().isoformat())
                ),
                total_received=Decimal(str(data.get("totalReceived", 0))),
                total_sent=Decimal(str(data.get("totalSent", 0))),
                counterparties=data.get("counterparties", 0),
                provider=provider,
                raw_data=data
            )
        elif provider == ComplianceProvider.ELLIPTIC:
            return BlockchainAnalytics(
                address=address,
                risk_score=data.get("risk_score", 0.0) / 10,  # Normalize to 0-1
                risk_category=self._map_elliptic_category(data.get("type")),
                exposure_types=self._extract_elliptic_exposures(data),
                last_activity=datetime.utcnow(),  # Not provided by Elliptic
                total_received=Decimal("0"),  # Not provided
                total_sent=Decimal("0"),  # Not provided
                counterparties=0,  # Not provided
                provider=provider,
                raw_data=data
            )
        else:
            # Generic parsing
            return BlockchainAnalytics(
                address=address,
                risk_score=min(data.get("risk_score", 0.0), 1.0),
                risk_category=data.get("risk_category", "unknown"),
                exposure_types=data.get("risk_indicators", []),
                last_activity=datetime.utcnow(),
                total_received=Decimal(str(data.get("total_received", 0))),
                total_sent=Decimal(str(data.get("total_sent", 0))),
                counterparties=data.get("unique_counterparties", 0),
                provider=provider,
                raw_data=data
            )
            
    async def check_sanctions(
        self,
        entity_name: str,
        fuzzy_match: bool = True,
        threshold: float = 0.85
    ) -> SanctionsCheckResult:
        """
        Check entity against sanctions lists
        """
        # Check cache
        cache_key = f"sanctions:{entity_name.lower()}"
        cached = await self._sanctions_cache.get(cache_key)
        
        if cached and self._is_cache_valid(cached.get("timestamp")):
            return cached["result"]
            
        # Normalize name
        normalized_name = entity_name.upper().strip()
        
        is_sanctioned = False
        lists_matched = []
        aliases = []
        match_score = 0.0
        
        # Check each sanctions list
        for list_name, sanctioned_entities in self.sanctions_lists.items():
            if fuzzy_match:
                # Fuzzy matching for partial matches
                for entity in sanctioned_entities:
                    score = self._calculate_name_similarity(
                        normalized_name, entity
                    )
                    if score >= threshold:
                        is_sanctioned = True
                        lists_matched.append(list_name)
                        match_score = max(match_score, score)
                        break
            else:
                # Exact match
                if normalized_name in sanctioned_entities:
                    is_sanctioned = True
                    lists_matched.append(list_name)
                    match_score = 1.0
                    
        result = SanctionsCheckResult(
            entity_name=entity_name,
            is_sanctioned=is_sanctioned,
            match_score=match_score,
            lists_matched=lists_matched,
            aliases=aliases,
            metadata={
                "checked_at": datetime.utcnow().isoformat(),
                "fuzzy_match": fuzzy_match,
                "threshold": threshold
            }
        )
        
        # Cache result
        await self._sanctions_cache.put(cache_key, {
            "result": result,
            "timestamp": datetime.utcnow()
        })
        
        return result
        
    async def check_pep(
        self,
        name: str,
        country: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check if person is a Politically Exposed Person
        """
        # Check cache
        cache_key = f"pep:{name.lower()}"
        cached = await self._pep_cache.get(cache_key)
        
        if cached and self._is_cache_valid(cached.get("timestamp")):
            return cached["result"]
            
        is_pep = False
        pep_details = {}
        
        # Check PEP database
        normalized_name = name.upper().strip()
        
        for pep_name, details in self.pep_database.items():
            if self._calculate_name_similarity(normalized_name, pep_name) > 0.9:
                is_pep = True
                pep_details = details
                break
                
        result = {
            "is_pep": is_pep,
            "name": name,
            "details": pep_details,
            "checked_at": datetime.utcnow().isoformat()
        }
        
        # Cache result
        await self._pep_cache.put(cache_key, {
            "result": result,
            "timestamp": datetime.utcnow()
        })
        
        return result
        
    async def calculate_composite_risk_score(
        self,
        user_id: str,
        transaction_data: Dict[str, Any],
        kyc_data: Dict[str, Any],
        blockchain_analytics: Dict[str, BlockchainAnalytics]
    ) -> Tuple[float, List[str]]:
        """
        Calculate composite risk score combining multiple factors
        """
        risk_score = 0.0
        risk_factors = []
        
        # KYC completeness factor
        kyc_level = kyc_data.get("level", "unverified")
        if kyc_level == "unverified":
            risk_score += 0.3
            risk_factors.append("unverified_identity")
        elif kyc_level == "tier_1":
            risk_score += 0.1
            
        # Blockchain analytics factor
        if blockchain_analytics:
            # Average risk across providers
            analytics_scores = [
                analytics.risk_score 
                for analytics in blockchain_analytics.values()
            ]
            avg_blockchain_risk = sum(analytics_scores) / len(analytics_scores)
            
            risk_score += avg_blockchain_risk * 0.4
            
            # Check for high-risk exposures
            all_exposures = set()
            for analytics in blockchain_analytics.values():
                all_exposures.update(analytics.exposure_types)
                
            high_risk_exposures = {
                "darknet", "mixer", "hack", "scam", 
                "terrorism", "ransomware"
            }
            
            if all_exposures.intersection(high_risk_exposures):
                risk_score += 0.3
                risk_factors.extend(list(
                    all_exposures.intersection(high_risk_exposures)
                ))
                
        # Transaction pattern factors
        amount = Decimal(str(transaction_data.get("amount_usd", 0)))
        if amount > Decimal("100000"):
            risk_score += 0.2
            risk_factors.append("very_high_value")
        elif amount > Decimal("10000"):
            risk_score += 0.1
            risk_factors.append("high_value")
            
        # Geographic risk
        if transaction_data.get("cross_border"):
            risk_score += 0.1
            risk_factors.append("cross_border")
            
        # High-risk jurisdictions
        high_risk_countries = {"IR", "KP", "SY", "CU", "VE"}
        if transaction_data.get("destination_country") in high_risk_countries:
            risk_score += 0.3
            risk_factors.append("high_risk_jurisdiction")
            
        # Normalize score
        risk_score = min(risk_score, 1.0)
        
        return risk_score, risk_factors
        
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two names using Levenshtein distance
        """
        # Simple implementation - in production use proper fuzzy matching
        if name1 == name2:
            return 1.0
            
        # Calculate Levenshtein distance
        len1, len2 = len(name1), len(name2)
        if len1 > len2:
            name1, name2 = name2, name1
            len1, len2 = len2, len1
            
        current_row = range(len1 + 1)
        for i in range(1, len2 + 1):
            previous_row, current_row = current_row, [i] + [0] * len1
            for j in range(1, len1 + 1):
                add, delete, change = (
                    previous_row[j] + 1,
                    current_row[j - 1] + 1,
                    previous_row[j - 1]
                )
                if name1[j - 1] != name2[i - 1]:
                    change += 1
                current_row[j] = min(add, delete, change)
                
        distance = current_row[len1]
        max_len = max(len1, len2)
        
        return 1.0 - (distance / max_len) if max_len > 0 else 0.0
        
    def _map_elliptic_category(self, elliptic_type: str) -> str:
        """Map Elliptic categories to standard categories"""
        mapping = {
            "exchange": "exchange",
            "darknet": "illicit",
            "theft": "illicit",
            "mixing": "high_risk",
            "gambling": "medium_risk",
            "mining": "low_risk"
        }
        return mapping.get(elliptic_type, "unknown")
        
    def _extract_elliptic_exposures(self, data: Dict[str, Any]) -> List[str]:
        """Extract exposure types from Elliptic response"""
        exposures = []
        
        if data.get("is_sanctioned"):
            exposures.append("sanctions")
        if data.get("dark_market_exposure", 0) > 0:
            exposures.append("darknet")
        if data.get("mixer_exposure", 0) > 0:
            exposures.append("mixer")
            
        return exposures
        
    def _is_cache_valid(self, timestamp: Optional[datetime]) -> bool:
        """Check if cached data is still valid"""
        if not timestamp:
            return False
            
        # Cache valid for 24 hours
        return datetime.utcnow() - timestamp < timedelta(hours=24)
        
    async def _load_compliance_lists(self):
        """Load sanctions and PEP lists from external sources"""
        # In production, this would load from:
        # - OFAC SDN list
        # - UN Security Council list
        # - EU Consolidated list
        # - UK HM Treasury list
        # - World Bank PEP database
        
        # For now, use sample data
        self.sanctions_lists["OFAC"] = {
            "SANCTIONED ENTITY 1",
            "SANCTIONED ENTITY 2",
            "TERRORIST ORGANIZATION XYZ"
        }
        
        self.pep_database = {
            "JOHN DOE": {
                "position": "Minister of Finance",
                "country": "XX",
                "since": "2020-01-01"
            }
        }
        
    async def generate_risk_report(
        self,
        user_id: str,
        period_days: int = 30
    ) -> Dict[str, Any]:
        """Generate comprehensive risk report for user"""
        # This would aggregate all risk data for reporting
        return {
            "user_id": user_id,
            "period": f"{period_days} days",
            "report_date": datetime.utcnow().isoformat(),
            "summary": "Risk report generated"
        } 