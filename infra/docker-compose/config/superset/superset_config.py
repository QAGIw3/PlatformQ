# Superset configuration for platformQ analytics

import os

# Flask App Builder configuration
ROW_LIMIT = 5000
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'your-secret-key-here')

# SQLAlchemy connection string
SQLALCHEMY_DATABASE_URI = 'postgresql://analytics:analytics123@postgres:5432/analytics_metadata'

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Set this API key to enable Mapbox visualizations
MAPBOX_API_KEY = ''

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
}

# Feature flags
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_TEMPLATE_REMOVE_FILTERS": True,
    "DASHBOARD_RBAC": True,
    "ENABLE_JAVASCRIPT_CONTROLS": True,
    "DASHBOARD_VIRTUALIZATION": True,
    "GLOBAL_ASYNC_QUERIES": True,
    "ENABLE_EXPLORE_DRAG_AND_DROP": True,
    "ENABLE_DND_WITH_CLICK_UX": True,
}

# Celery configuration for async queries
class CeleryConfig(object):
    broker_url = 'redis://redis:6379/0'
    imports = (
        'superset.sql_lab',
        'superset.tasks',
    )
    result_backend = 'redis://redis:6379/0'
    worker_prefetch_multiplier = 10
    task_acks_late = True
    task_annotations = {
        'sql_lab.get_sql_results': {
            'rate_limit': '100/s',
        },
    }

CELERY_CONFIG = CeleryConfig

# Results backend for SQL queries
RESULTS_BACKEND = {
    'cache_type': 'RedisCache',
    'cache_key_prefix': 'superset_results',
    'cache_redis_url': 'redis://redis:6379/1',
    'cache_default_timeout': 86400,  # 24 hours
}

# SQL Lab configuration
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 5000

# Security configuration
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = False  # Set to True in production with HTTPS
SESSION_COOKIE_SAMESITE = 'Lax'

# Enable data upload functionality
UPLOAD_ENABLED = True
UPLOAD_FOLDER = '/app/superset_home/uploads/'
UPLOAD_ALLOWED_EXTENSIONS = ['csv', 'tsv', 'txt', 'xlsx', 'xls', 'json']

# Time zone for SQL Lab
TIME_ZONE = 'UTC'

# Default cache timeout
DATA_CACHE_CONFIG = {
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24,  # 24 hours
}

# WebServer configuration
WEBSERVER_THREADS = 8
WEBSERVER_PORT = 8088

# Logo configuration
APP_NAME = "PlatformQ Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

# Theme configuration
THEME_OVERRIDES = {
    "borderRadius": 4,
    "colors": {
        "primary": {
            "base": '#1890FF',
        },
        "secondary": {
            "base": '#444E7C',
        },
        "grayscale": {
            "base": '#666666',
        }
    }
} 