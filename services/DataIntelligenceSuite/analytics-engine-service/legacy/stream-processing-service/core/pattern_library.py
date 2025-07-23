"""Pattern Library for Complex Event Processing

Manages CEP patterns for fraud detection, risk monitoring, and other pattern detection use cases.
"""

import logging
import os
import json
import yaml
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from pathlib import Path

from app.core.config import Settings


logger = logging.getLogger(__name__)


class PatternType:
    """Pattern type constants"""
    FRAUD_DETECTION = "fraud_detection"
    RISK_MONITORING = "risk_monitoring"
    ANOMALY_DETECTION = "anomaly_detection"
    TRADING_PATTERNS = "trading_patterns"
    COMPLIANCE_MONITORING = "compliance_monitoring"


class Pattern:
    """Represents a CEP pattern"""
    
    def __init__(self, pattern_id: str, name: str, pattern_type: str, 
                 definition: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None):
        self.id = pattern_id
        self.name = name
        self.type = pattern_type
        self.definition = definition
        self.metadata = metadata or {}
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.version = 1
        self.enabled = True
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert pattern to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "definition": self.definition,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version,
            "enabled": self.enabled
        }
        
    def to_flink_pattern(self) -> str:
        """Convert to Flink CEP pattern definition"""
        # This would generate actual Flink CEP pattern code
        # For now, returning a string representation
        return json.dumps(self.definition)


class PatternLibrary:
    """Manages CEP patterns"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.patterns: Dict[str, Pattern] = {}
        self.pattern_cache: Dict[str, Any] = {}
        self._reload_task: Optional[asyncio.Task] = None
        
    async def load_patterns(self):
        """Load patterns from configuration"""
        logger.info("Loading patterns from library")
        
        # Load built-in patterns
        await self._load_builtin_patterns()
        
        # Load custom patterns from files
        if os.path.exists(self.settings.pattern_library_path):
            await self._load_custom_patterns()
            
        # Start reload task
        self._reload_task = asyncio.create_task(self._pattern_reload_task())
        
        logger.info(f"Loaded {len(self.patterns)} patterns")
        
    async def _load_builtin_patterns(self):
        """Load built-in patterns"""
        
        # Fraud detection patterns
        fraud_patterns = {
            "velocity_check": {
                "id": "fraud_velocity",
                "name": "High Velocity Transactions",
                "type": PatternType.FRAUD_DETECTION,
                "definition": {
                    "pattern": "EVERY(a.amount > 1000)",
                    "within": "5 minutes",
                    "partition_by": "user_id",
                    "threshold": 5,
                    "actions": ["alert", "block"]
                }
            },
            "wash_trading": {
                "id": "fraud_wash_trading",
                "name": "Wash Trading Detection",
                "type": PatternType.FRAUD_DETECTION,
                "definition": {
                    "pattern": "a[user_id] -> b[user_id] WHERE a.asset_id = b.asset_id AND a.action = 'SELL' AND b.action = 'BUY'",
                    "within": "10 minutes",
                    "actions": ["alert", "investigate"]
                }
            },
            "account_takeover": {
                "id": "fraud_account_takeover",
                "name": "Account Takeover Detection",
                "type": PatternType.FRAUD_DETECTION,
                "definition": {
                    "pattern": "login[location != prev.location] -> transaction[amount > threshold]",
                    "within": "30 minutes",
                    "threshold_multiplier": 2,
                    "actions": ["alert", "require_2fa"]
                }
            }
        }
        
        # Risk monitoring patterns
        risk_patterns = {
            "liquidation_cascade": {
                "id": "risk_liquidation_cascade",
                "name": "Liquidation Cascade Risk",
                "type": PatternType.RISK_MONITORING,
                "definition": {
                    "pattern": "liquidation+ WHERE sum(liquidation.value) > market_cap * 0.1",
                    "within": "1 hour",
                    "alert_threshold": 0.05,
                    "critical_threshold": 0.1,
                    "actions": ["alert", "pause_liquidations"]
                }
            },
            "price_manipulation": {
                "id": "risk_price_manipulation",
                "name": "Price Manipulation Detection",
                "type": PatternType.RISK_MONITORING,
                "definition": {
                    "pattern": "trade WHERE abs(price - market_price) / market_price > 0.05",
                    "within": "5 minutes",
                    "count_threshold": 3,
                    "actions": ["alert", "investigate"]
                }
            },
            "exposure_concentration": {
                "id": "risk_exposure_concentration",
                "name": "Exposure Concentration Risk",
                "type": PatternType.RISK_MONITORING,
                "definition": {
                    "pattern": "position WHERE exposure > portfolio_value * 0.25",
                    "check_interval": "5 minutes",
                    "actions": ["alert", "suggest_rebalance"]
                }
            }
        }
        
        # Trading patterns
        trading_patterns = {
            "arbitrage_opportunity": {
                "id": "trading_arbitrage",
                "name": "Arbitrage Opportunity",
                "type": PatternType.TRADING_PATTERNS,
                "definition": {
                    "pattern": "price_a[exchange='A'] -> price_b[exchange='B'] WHERE abs(price_a - price_b) / price_a > 0.01",
                    "within": "1 second",
                    "min_profit_threshold": 0.001,
                    "actions": ["notify", "auto_execute"]
                }
            },
            "momentum_detection": {
                "id": "trading_momentum",
                "name": "Momentum Detection",
                "type": PatternType.TRADING_PATTERNS,
                "definition": {
                    "pattern": "price+ WHERE price[last] > price[first] * 1.05",
                    "within": "15 minutes",
                    "volume_threshold": 1000000,
                    "actions": ["alert", "analyze"]
                }
            }
        }
        
        # Compliance patterns
        compliance_patterns = {
            "kyc_violation": {
                "id": "compliance_kyc",
                "name": "KYC Violation Detection",
                "type": PatternType.COMPLIANCE_MONITORING,
                "definition": {
                    "pattern": "transaction WHERE user.kyc_status != 'VERIFIED' AND amount > kyc_threshold",
                    "kyc_threshold": 10000,
                    "actions": ["block", "report"]
                }
            },
            "suspicious_activity": {
                "id": "compliance_suspicious",
                "name": "Suspicious Activity Reporting",
                "type": PatternType.COMPLIANCE_MONITORING,
                "definition": {
                    "pattern": "transaction+ WHERE sum(amount) > sar_threshold",
                    "within": "24 hours",
                    "partition_by": "user_id",
                    "sar_threshold": 10000,
                    "actions": ["report", "investigate"]
                }
            }
        }
        
        # Load all patterns
        all_patterns = {
            **fraud_patterns,
            **risk_patterns,
            **trading_patterns,
            **compliance_patterns
        }
        
        for pattern_data in all_patterns.values():
            pattern = Pattern(
                pattern_id=pattern_data["id"],
                name=pattern_data["name"],
                pattern_type=pattern_data["type"],
                definition=pattern_data["definition"]
            )
            self.patterns[pattern.id] = pattern
            
    async def _load_custom_patterns(self):
        """Load custom patterns from files"""
        pattern_dir = Path(self.settings.pattern_library_path)
        
        for pattern_file in pattern_dir.glob("*.yaml"):
            try:
                with open(pattern_file, 'r') as f:
                    pattern_data = yaml.safe_load(f)
                    
                pattern = Pattern(
                    pattern_id=pattern_data["id"],
                    name=pattern_data["name"],
                    pattern_type=pattern_data["type"],
                    definition=pattern_data["definition"],
                    metadata=pattern_data.get("metadata", {})
                )
                
                self.patterns[pattern.id] = pattern
                logger.info(f"Loaded custom pattern: {pattern.name}")
                
            except Exception as e:
                logger.error(f"Failed to load pattern from {pattern_file}: {e}")
                
    async def get_pattern(self, pattern_id: str) -> Optional[Pattern]:
        """Get a pattern by ID"""
        return self.patterns.get(pattern_id)
        
    async def list_patterns(self, pattern_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all patterns"""
        patterns = []
        for pattern in self.patterns.values():
            if pattern_type and pattern.type != pattern_type:
                continue
            if pattern.enabled:
                patterns.append(pattern.to_dict())
        return patterns
        
    async def register_pattern(self, name: str, pattern_type: str, 
                             definition: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> str:
        """Register a new pattern"""
        pattern_id = f"custom_{name.lower().replace(' ', '_')}_{int(datetime.utcnow().timestamp())}"
        
        pattern = Pattern(
            pattern_id=pattern_id,
            name=name,
            pattern_type=pattern_type,
            definition=definition,
            metadata=metadata
        )
        
        self.patterns[pattern_id] = pattern
        
        # Save to file if custom patterns directory exists
        if os.path.exists(self.settings.pattern_library_path):
            await self._save_pattern_to_file(pattern)
            
        logger.info(f"Registered new pattern: {name} ({pattern_id})")
        return pattern_id
        
    async def update_pattern(self, pattern_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing pattern"""
        pattern = self.patterns.get(pattern_id)
        if not pattern:
            return False
            
        # Update fields
        if "name" in updates:
            pattern.name = updates["name"]
        if "definition" in updates:
            pattern.definition = updates["definition"]
        if "metadata" in updates:
            pattern.metadata.update(updates["metadata"])
        if "enabled" in updates:
            pattern.enabled = updates["enabled"]
            
        pattern.updated_at = datetime.utcnow()
        pattern.version += 1
        
        # Save updates if custom pattern
        if pattern_id.startswith("custom_"):
            await self._save_pattern_to_file(pattern)
            
        return True
        
    async def delete_pattern(self, pattern_id: str) -> bool:
        """Delete a pattern"""
        if pattern_id not in self.patterns:
            return False
            
        # Only allow deletion of custom patterns
        if not pattern_id.startswith("custom_"):
            logger.warning(f"Cannot delete built-in pattern: {pattern_id}")
            return False
            
        del self.patterns[pattern_id]
        
        # Remove file if exists
        pattern_file = Path(self.settings.pattern_library_path) / f"{pattern_id}.yaml"
        if pattern_file.exists():
            pattern_file.unlink()
            
        return True
        
    async def compile_pattern(self, pattern_id: str) -> Optional[str]:
        """Compile pattern to Flink CEP code"""
        pattern = self.patterns.get(pattern_id)
        if not pattern:
            return None
            
        # Check cache
        if pattern_id in self.pattern_cache:
            return self.pattern_cache[pattern_id]
            
        # Compile pattern
        compiled = pattern.to_flink_pattern()
        self.pattern_cache[pattern_id] = compiled
        
        return compiled
        
    async def _save_pattern_to_file(self, pattern: Pattern):
        """Save pattern to file"""
        pattern_file = Path(self.settings.pattern_library_path) / f"{pattern.id}.yaml"
        
        pattern_data = {
            "id": pattern.id,
            "name": pattern.name,
            "type": pattern.type,
            "definition": pattern.definition,
            "metadata": pattern.metadata,
            "version": pattern.version,
            "enabled": pattern.enabled
        }
        
        with open(pattern_file, 'w') as f:
            yaml.dump(pattern_data, f, default_flow_style=False)
            
    async def _pattern_reload_task(self):
        """Periodically reload patterns"""
        while True:
            try:
                await asyncio.sleep(self.settings.pattern_reload_interval)
                await self._load_custom_patterns()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error reloading patterns: {e}") 