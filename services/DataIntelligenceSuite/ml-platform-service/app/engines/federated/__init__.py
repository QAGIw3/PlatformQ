"""Federated Learning Engine Module"""

from .federated_coordinator import FederatedCoordinator
from .aggregation_strategies import AggregationStrategy, FedAvg, FedProx, SCAFFOLD
from .privacy_mechanisms import PrivacyMechanism, DifferentialPrivacy, SecureAggregation
from .client_manager import ClientManager

__all__ = [
    "FederatedCoordinator",
    "AggregationStrategy",
    "FedAvg",
    "FedProx",
    "SCAFFOLD",
    "PrivacyMechanism",
    "DifferentialPrivacy",
    "SecureAggregation",
    "ClientManager"
] 