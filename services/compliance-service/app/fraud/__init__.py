"""
Fraud Detection Module

Provides graph-based fraud detection capabilities integrated with
the Graph Intelligence Service.
"""

from .fraud_detection_engine import FraudDetectionEngine, FraudCheckResult
from .fraud_patterns import FraudPattern, FraudPatternType

__all__ = [
    "FraudDetectionEngine",
    "FraudCheckResult",
    "FraudPattern",
    "FraudPatternType"
] 