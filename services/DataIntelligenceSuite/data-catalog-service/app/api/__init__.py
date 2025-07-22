"""
API routers for Data Catalog Service
"""

from .entities import router as entities_router, set_dependencies as set_entities_deps
from .schemas import router as schemas_router, set_dependencies as set_schemas_deps
from .search import router as search_router, set_dependencies as set_search_deps
from .lineage import router as lineage_router, set_dependencies as set_lineage_deps
from .classifications import router as classifications_router, set_dependencies as set_classifications_deps
from .glossary import router as glossary_router, set_glossary_deps
from .monitoring import router as monitoring_router, set_dependencies as set_monitoring_deps
from .health import router as health_router, set_dependencies as set_health_deps

# New enhancement routers
from .discovery import router as discovery_router, set_discovery_deps
from .quality import router as quality_router, set_quality_deps
from .intelligent_search import router as intelligent_search_router, set_intelligent_search_deps
from .analytics import router as analytics_router, set_analytics_deps

__all__ = [
    'entities_router',
    'schemas_router',
    'search_router',
    'lineage_router',
    'classifications_router',
    'glossary_router',
    'monitoring_router',
    'health_router',
    'discovery_router',
    'quality_router',
    'intelligent_search_router',
    'analytics_router',
    'set_entities_deps',
    'set_schemas_deps',
    'set_search_deps',
    'set_lineage_deps',
    'set_classifications_deps',
    'set_glossary_deps',
    'set_monitoring_deps',
    'set_health_deps',
    'set_discovery_deps',
    'set_quality_deps',
    'set_intelligent_search_deps',
    'set_analytics_deps'
] 