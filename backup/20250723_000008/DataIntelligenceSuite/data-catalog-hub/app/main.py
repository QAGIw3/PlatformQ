"""
Data Catalog Hub - Main Entry Point

Uses the application factory pattern for better separation of concerns and testability.
"""

import uvicorn
import logging
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.application import create_application
from app.core.config import settings
from app.core.logging_config import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def main():
    """Main entry point for the application"""
    try:
        # Create the application
        app = create_application()
        
        logger.info(f"Starting Data Catalog Hub on {settings.HOST}:{settings.PORT}")
        
        # Run the application
        uvicorn.run(
            app,
            host=settings.HOST,
            port=settings.PORT,
            reload=settings.RELOAD,
            log_config=None,  # Use our custom logging config
            access_log=settings.ACCESS_LOG,
            workers=settings.WORKERS if not settings.RELOAD else 1
        )
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main() 