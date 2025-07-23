"""
API endpoints for unified orchestration service
"""

from .workflows import router as workflows_router, set_dependencies as set_workflows_deps
from .pipelines import router as pipelines_router, set_dependencies as set_pipelines_deps
from .optimization import router as optimization_router, set_dependencies as set_optimization_deps
from .seatunnel import router as seatunnel_router, set_dependencies as set_seatunnel_deps
from .event_mappings import router as event_mappings_router, set_dependencies as set_event_deps
from .attestations import router as attestations_router, set_dependencies as set_attestations_deps
from .k8s import router as k8s_router, set_k8s_deps
from .monitoring import router as monitoring_router, set_dependencies as set_monitoring_deps
from .health import router as health_router, set_dependencies as set_health_deps

__all__ = [
    'workflows_router',
    'pipelines_router',
    'optimization_router',
    'seatunnel_router',
    'event_mappings_router',
    'attestations_router',
    'k8s_router',
    'monitoring_router',
    'health_router',
    'set_workflows_deps',
    'set_pipelines_deps',
    'set_optimization_deps',
    'set_seatunnel_deps',
    'set_event_deps',
    'set_attestations_deps',
    'set_k8s_deps',
    'set_monitoring_deps',
    'set_health_deps'
] 