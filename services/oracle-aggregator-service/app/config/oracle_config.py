from typing import Dict, List
from decimal import Decimal
from pydantic import BaseModel

class OracleSource(BaseModel):
    name: str
    weight: float  # Weight in aggregation
    timeout: int  # Milliseconds
    required: bool  # Must have for price validity
    
class AssetOracleConfig(BaseModel):
    min_sources: int = 3  # Minimum sources required
    max_deviation: float = 0.02  # 2% max deviation
    stale_threshold: int = 300  # 5 minutes
    outlier_threshold: float = 0.05  # 5% considered outlier
    
# Oracle configuration for different asset types
ORACLE_CONFIG = {
    "traditional_assets": {
        "stocks": AssetOracleConfig(min_sources=3, max_deviation=0.01),
        "forex": AssetOracleConfig(min_sources=5, max_deviation=0.005),
        "commodities": AssetOracleConfig(min_sources=4, max_deviation=0.02),
    },
    "crypto_assets": {
        "major": AssetOracleConfig(min_sources=5, max_deviation=0.02),
        "minor": AssetOracleConfig(min_sources=3, max_deviation=0.05),
    },
    "exotic_assets": {
        "prediction_markets": AssetOracleConfig(min_sources=2, max_deviation=0.1),
        "social_metrics": AssetOracleConfig(min_sources=1, max_deviation=0.2),
        "compute_resources": AssetOracleConfig(min_sources=3, max_deviation=0.03),
    }
}

# Data source priorities
ORACLE_SOURCES = {
    "chainlink": OracleSource(name="chainlink", weight=0.4, timeout=1000, required=True),
    "band_protocol": OracleSource(name="band", weight=0.3, timeout=1000, required=False),
    "internal_aggregator": OracleSource(name="internal", weight=0.2, timeout=500, required=True),
    "ai_discovery": OracleSource(name="ai", weight=0.1, timeout=2000, required=False),
} 