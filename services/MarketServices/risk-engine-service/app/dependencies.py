"""Dependencies for the Risk Engine Service."""

from typing import Optional
from platformq_shared import get_current_user
from platformq_events import MarketEventPublisher
from platformq_direct_comm import DirectCommunicator
from pyignite import Client as IgniteClient
import pulsar

from .config import Settings
from .core.risk_calculator import RiskCalculator
from .core.var_calculator import VaRCalculator
from .core.stress_tester import StressTester
from .state.state_manager import StateManager
from .ml.risk_prediction import RiskPredictor
from .integrations.direct_comm_integration import DirectCommIntegration
from .core.risk_monitor import RiskMonitor

# Global instances
_settings: Optional[Settings] = None
_ignite_client: Optional[IgniteClient] = None
_pulsar_client: Optional[pulsar.Client] = None
_risk_calculator: Optional[RiskCalculator] = None
_var_calculator: Optional[VaRCalculator] = None
_stress_tester: Optional[StressTester] = None
_state_manager: Optional[StateManager] = None
_risk_predictor: Optional[RiskPredictor] = None
_direct_comm: Optional[DirectCommIntegration] = None
_event_publisher: Optional[MarketEventPublisher] = None
_risk_monitor: Optional[RiskMonitor] = None


def get_settings() -> Settings:
    """Get the settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def get_ignite_client() -> IgniteClient:
    """Get the Apache Ignite client."""
    global _ignite_client
    if _ignite_client is None:
        settings = get_settings()
        _ignite_client = IgniteClient()
        _ignite_client.connect(settings.IGNITE_ADDRESSES)
    return _ignite_client


def get_pulsar_client() -> pulsar.Client:
    """Get the Pulsar client."""
    global _pulsar_client
    if _pulsar_client is None:
        settings = get_settings()
        _pulsar_client = pulsar.Client(settings.PULSAR_URL)
    return _pulsar_client


def get_event_publisher() -> MarketEventPublisher:
    """Get the event publisher."""
    global _event_publisher
    if _event_publisher is None:
        pulsar_client = get_pulsar_client()
        settings = get_settings()
        _event_publisher = MarketEventPublisher(
            pulsar_client=pulsar_client,
            topic_prefix="persistent://derivatives"
        )
    return _event_publisher


def get_risk_calculator() -> RiskCalculator:
    """Get the risk calculator instance."""
    global _risk_calculator
    if _risk_calculator is None:
        settings = get_settings()
        _risk_calculator = RiskCalculator(settings)
    return _risk_calculator


def get_var_calculator() -> VaRCalculator:
    """Get the VaR calculator instance."""
    global _var_calculator
    if _var_calculator is None:
        settings = get_settings()
        ignite_client = get_ignite_client()
        _var_calculator = VaRCalculator(settings, ignite_client)
    return _var_calculator


def get_stress_tester() -> StressTester:
    """Get the stress tester instance."""
    global _stress_tester
    if _stress_tester is None:
        settings = get_settings()
        ignite_client = get_ignite_client()
        _stress_tester = StressTester(settings, ignite_client)
    return _stress_tester


def get_state_manager() -> StateManager:
    """Get the state manager instance."""
    global _state_manager
    if _state_manager is None:
        ignite_client = get_ignite_client()
        _state_manager = StateManager(ignite_client)
    return _state_manager


def get_risk_predictor() -> RiskPredictor:
    """Get the risk predictor instance."""
    global _risk_predictor
    if _risk_predictor is None:
        settings = get_settings()
        ignite_client = get_ignite_client()
        _risk_predictor = RiskPredictor(settings, ignite_client)
    return _risk_predictor


def get_direct_comm() -> DirectCommIntegration:
    """Get the direct communication integration."""
    global _direct_comm
    if _direct_comm is None:
        settings = get_settings()
        ignite_client = get_ignite_client()
        _direct_comm = DirectCommIntegration(
            service_id=settings.service_id,
            ignite_client=ignite_client
        )
    return _direct_comm


def get_risk_monitor() -> RiskMonitor:
    """Get the risk monitor instance."""
    global _risk_monitor
    if _risk_monitor is None:
        settings = get_settings()
        event_publisher = get_event_publisher()
        ignite_client = get_ignite_client()
        direct_comm = get_direct_comm()
        pulsar_client = get_pulsar_client()
        
        _risk_monitor = RiskMonitor(
            settings=settings,
            event_publisher=event_publisher,
            ignite_client=ignite_client,
            direct_comm=direct_comm,
            pulsar_client=pulsar_client
        )
    return _risk_monitor


async def initialize_dependencies():
    """Initialize all dependencies."""
    settings = get_settings()
    ignite_client = get_ignite_client()
    
    # Initialize state manager
    state_manager = get_state_manager()
    await state_manager.initialize()
    
    # Initialize direct communication
    direct_comm = get_direct_comm()
    await direct_comm.start()
    
    # Initialize risk monitor
    risk_monitor = get_risk_monitor()
    await risk_monitor.start()


async def cleanup_dependencies():
    """Cleanup all dependencies."""
    # Stop risk monitor
    if _risk_monitor:
        await _risk_monitor.stop()
    
    # Stop direct communication
    if _direct_comm:
        await _direct_comm.stop()
    
    # Close Pulsar client
    if _pulsar_client:
        _pulsar_client.close()
    
    # Close Ignite client
    if _ignite_client:
        _ignite_client.close() 