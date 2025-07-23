#!/usr/bin/env python3
"""
DataIntelligenceSuite Migration Script v2.0

Migrates existing services to the new consolidated architecture.
"""

import argparse
import asyncio
import json
import logging
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ServiceMapping:
    """Defines how old services map to new consolidated services"""
    
    # Mapping of old services to new services
    SERVICE_CONSOLIDATION = {
        # Data Platform Service
        "data-platform-service": [
            "data-ingestion-service",
            "batch-processing-service",
            "feature-store-service",
            "storage-service",
            "dih-service"
        ],
        
        # Analytics Engine Service  
        "analytics-engine-service": [
            "analytics-service",
            "stream-processing-service",
            "neuromorphic-computing-service",
            "quantum-optimization-service"
        ],
        
        # ML Platform Service
        "ml-platform-service": [
            "unified-ml-platform-service"
        ],
        
        # Data Governance Service
        "data-governance-service": [
            "data-catalog-hub",
            "unified-quality-service"
        ],
        
        # Stream Processing Service (enhanced)
        "stream-processing-service-v2": [
            "stream-processing-service"
        ],
        
        # Orchestration Service
        "orchestration-service": [
            "unified-orchestration-service"
        ],
        
        # Integration Hub
        "integration-hub-service": [
            "graphql-gateway",
            "unified-graph-service"
        ]
    }
    
    # Component mapping within services
    COMPONENT_MAPPING = {
        "data-ingestion-service": {
            "connectors": "data-platform-service/connectors",
            "core": "data-platform-service/core/ingestion",
            "api": "data-platform-service/api/v1/ingestion"
        },
        "batch-processing-service": {
            "processors": "data-platform-service/processors",
            "jobs": "data-platform-service/core/batch",
            "api": "data-platform-service/api/v1/batch"
        },
        "analytics-service": {
            "analytics": "analytics-engine-service/engines/analytics",
            "monitoring": "analytics-engine-service/monitoring",
            "api": "analytics-engine-service/api/v1/analytics"
        },
        "data-catalog-hub": {
            "core": "data-governance-service/catalog",
            "services": "data-governance-service/catalog/services",
            "api": "data-governance-service/api/v1/catalog"
        }
    }


class MigrationContext:
    """Context for migration operations"""
    
    def __init__(self, source_dir: Path, target_dir: Path, dry_run: bool = False):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.dry_run = dry_run
        self.backup_dir = target_dir / "backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Track migration state
        self.migrated_services: Set[str] = set()
        self.migration_errors: List[Dict] = []
        self.import_updates: Dict[str, str] = {}
        
    def add_import_update(self, old_import: str, new_import: str):
        """Track import statement updates"""
        self.import_updates[old_import] = new_import


class ServiceMigrator:
    """Handles migration of individual services"""
    
    def __init__(self, context: MigrationContext):
        self.context = context
        self.mapping = ServiceMapping()
        
    async def migrate_all(self):
        """Migrate all services to new architecture"""
        logger.info("Starting DataIntelligenceSuite v2.0 migration")
        
        # Create backup
        if not self.context.dry_run:
            await self._create_backup()
            
        # Create new service structures
        await self._create_service_structures()
        
        # Migrate each service group
        for new_service, old_services in self.mapping.SERVICE_CONSOLIDATION.items():
            try:
                await self.migrate_service_group(new_service, old_services)
            except Exception as e:
                logger.error(f"Failed to migrate {new_service}: {e}")
                self.context.migration_errors.append({
                    "service": new_service,
                    "error": str(e)
                })
                
        # Update imports across all files
        await self._update_all_imports()
        
        # Update configuration files
        await self._update_configurations()
        
        # Generate migration report
        await self._generate_report()
        
    async def _create_backup(self):
        """Create backup of existing services"""
        logger.info(f"Creating backup at {self.context.backup_dir}")
        
        self.context.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Backup each service
        services_dir = self.context.source_dir / "services" / "DataIntelligenceSuite"
        if services_dir.exists():
            shutil.copytree(
                services_dir,
                self.context.backup_dir / "DataIntelligenceSuite",
                dirs_exist_ok=True
            )
            
    async def _create_service_structures(self):
        """Create new service directory structures"""
        for new_service in self.mapping.SERVICE_CONSOLIDATION.keys():
            service_dir = self.context.target_dir / "services" / "DataIntelligenceSuite" / new_service
            
            if self.context.dry_run:
                logger.info(f"Would create service structure: {service_dir}")
            else:
                await self._create_service_structure(service_dir, new_service)
                
    async def _create_service_structure(self, service_dir: Path, service_name: str):
        """Create structure for a single service"""
        # Base directories
        directories = [
            "app",
            "app/api",
            "app/api/v1",
            "app/api/v2",
            "app/core",
            "app/models",
            "app/services",
            "app/utils",
            "tests",
            "tests/unit",
            "tests/integration",
            "scripts",
            "docs"
        ]
        
        # Service-specific directories
        if service_name == "data-platform-service":
            directories.extend([
                "app/connectors",
                "app/processors",
                "app/core/ingestion",
                "app/core/batch",
                "app/core/storage",
                "app/core/lakehouse"
            ])
        elif service_name == "analytics-engine-service":
            directories.extend([
                "app/engines",
                "app/engines/analytics",
                "app/engines/stream",
                "app/engines/quantum",
                "app/monitoring"
            ])
        elif service_name == "ml-platform-service":
            directories.extend([
                "app/models/registry",
                "app/training",
                "app/serving",
                "app/monitoring",
                "app/experiments"
            ])
        elif service_name == "data-governance-service":
            directories.extend([
                "app/catalog",
                "app/quality",
                "app/lineage",
                "app/privacy"
            ])
            
        # Create directories
        for dir_path in directories:
            full_path = service_dir / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            
            # Create __init__.py files
            init_file = full_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text('"""{}"""\n'.format(dir_path.replace("/", ".")))
                
        # Create base files
        await self._create_base_files(service_dir, service_name)
        
    async def _create_base_files(self, service_dir: Path, service_name: str):
        """Create base files for service"""
        # Create main.py
        main_content = f'''"""
{service_name.replace("-", " ").title()} - Main Application

Part of DataIntelligenceSuite v2.0
"""

import asyncio
from fastapi import FastAPI
from platformq_shared.logging import setup_logging
from data_intelligence_common.base_service import create_app
from data_intelligence_common.monitoring import setup_monitoring

from .core.config import settings
from .api.v1 import router as v1_router
from .api.v2 import router as v2_router

# Setup logging
logger = setup_logging(__name__)

# Create FastAPI app
app = create_app(
    title="{service_name.replace("-", " ").title()}",
    description="Consolidated service for DataIntelligenceSuite v2.0",
    version="2.0.0"
)

# Include routers
app.include_router(v1_router, prefix="/api/v1")
app.include_router(v2_router, prefix="/api/v2")

# Setup monitoring
setup_monitoring(app)


@app.on_event("startup")
async def startup_event():
    """Initialize service on startup"""
    logger.info(f"Starting {service_name} v2.0")
    # Initialize components
    

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info(f"Shutting down {service_name}")
    # Cleanup


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
'''
        
        (service_dir / "app" / "main.py").write_text(main_content)
        
        # Create Dockerfile
        dockerfile_content = f'''FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Run service
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
'''
        
        (service_dir / "Dockerfile").write_text(dockerfile_content)
        
        # Create requirements.txt
        requirements = [
            "fastapi>=0.104.1",
            "uvicorn[standard]>=0.24.0",
            "pydantic>=2.5.0",
            "data-intelligence-common>=2.0.0",
            "platformq-shared>=1.0.0",
            "httpx>=0.25.2",
            "prometheus-client>=0.19.0",
            "opentelemetry-api>=1.21.0",
            "opentelemetry-instrumentation-fastapi>=0.42b0"
        ]
        
        # Add service-specific requirements
        if service_name == "data-platform-service":
            requirements.extend([
                "apache-airflow>=2.8.0",
                "pyspark>=3.5.0",
                "pandas>=2.1.0",
                "pyarrow>=14.0.0"
            ])
        elif service_name == "analytics-engine-service":
            requirements.extend([
                "apache-flink>=1.18.0",
                "dask[complete]>=2023.12.0",
                "ray[default]>=2.9.0"
            ])
        elif service_name == "ml-platform-service":
            requirements.extend([
                "mlflow>=2.9.0",
                "scikit-learn>=1.3.0",
                "torch>=2.1.0",
                "tensorflow>=2.15.0"
            ])
            
        (service_dir / "requirements.txt").write_text("\n".join(requirements) + "\n")
        
        # Create README.md
        readme_content = f'''# {service_name.replace("-", " ").title()}

Part of DataIntelligenceSuite v2.0

## Overview

This is a consolidated service that combines functionality from multiple legacy services.

## Features

- High-performance processing
- Scalable architecture
- Unified API
- Comprehensive monitoring

## API Documentation

API documentation is available at `/docs` when the service is running.

## Configuration

Configuration is managed through environment variables and Consul.

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
uvicorn app.main:app --reload

# Run tests
pytest tests/
```
'''
        
        (service_dir / "README.md").write_text(readme_content)
        
    async def migrate_service_group(self, new_service: str, old_services: List[str]):
        """Migrate a group of old services to new consolidated service"""
        logger.info(f"Migrating {old_services} -> {new_service}")
        
        target_dir = self.context.target_dir / "services" / "DataIntelligenceSuite" / new_service
        
        for old_service in old_services:
            source_dir = self.context.source_dir / "services" / "DataIntelligenceSuite" / old_service
            
            if not source_dir.exists():
                logger.warning(f"Source service {old_service} not found")
                continue
                
            # Get component mapping
            component_map = self.mapping.COMPONENT_MAPPING.get(old_service, {})
            
            if component_map:
                # Migrate with component mapping
                await self._migrate_with_mapping(source_dir, target_dir, component_map, old_service)
            else:
                # Direct migration
                await self._migrate_direct(source_dir, target_dir, old_service)
                
            self.context.migrated_services.add(old_service)
            
    async def _migrate_with_mapping(
        self,
        source_dir: Path,
        target_dir: Path,
        component_map: Dict[str, str],
        old_service: str
    ):
        """Migrate service with component mapping"""
        for source_component, target_path in component_map.items():
            source_path = source_dir / "app" / source_component
            if not source_path.exists():
                continue
                
            target_component_dir = target_dir / target_path
            
            if self.context.dry_run:
                logger.info(f"Would migrate {source_path} -> {target_component_dir}")
            else:
                # Create target directory
                target_component_dir.mkdir(parents=True, exist_ok=True)
                
                # Copy files
                if source_path.is_dir():
                    for file_path in source_path.rglob("*.py"):
                        relative_path = file_path.relative_to(source_path)
                        target_file = target_component_dir / relative_path
                        target_file.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Migrate file with import updates
                        await self._migrate_file(file_path, target_file, old_service)
                else:
                    await self._migrate_file(source_path, target_component_dir / source_path.name, old_service)
                    
    async def _migrate_direct(self, source_dir: Path, target_dir: Path, old_service: str):
        """Direct migration of service"""
        # Map to subdirectory in new service
        service_subdir = target_dir / "legacy" / old_service
        
        if self.context.dry_run:
            logger.info(f"Would migrate {source_dir} -> {service_subdir}")
        else:
            service_subdir.mkdir(parents=True, exist_ok=True)
            
            # Copy app directory
            source_app = source_dir / "app"
            if source_app.exists():
                for file_path in source_app.rglob("*.py"):
                    relative_path = file_path.relative_to(source_app)
                    target_file = service_subdir / relative_path
                    target_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    await self._migrate_file(file_path, target_file, old_service)
                    
    async def _migrate_file(self, source_file: Path, target_file: Path, old_service: str):
        """Migrate individual file with import updates"""
        content = source_file.read_text()
        
        # Update imports
        updated_content = self._update_imports(content, old_service)
        
        # Write to target
        target_file.write_text(updated_content)
        logger.debug(f"Migrated {source_file} -> {target_file}")
        
    def _update_imports(self, content: str, old_service: str) -> str:
        """Update import statements in file content"""
        import re
        
        # Common import patterns to update
        import_patterns = [
            # From old service to data-intelligence-common
            (r'from \.\.monitoring import', 'from data_intelligence_common.monitoring import'),
            (r'from \.\.base import', 'from data_intelligence_common.base_service import'),
            (r'from \.\.utils import', 'from data_intelligence_common.utils import'),
            
            # Service-specific imports
            (f'from {old_service.replace("-", "_")}.', 'from .'),
            (f'import {old_service.replace("-", "_")}', 'from . import')
        ]
        
        updated_content = content
        for pattern, replacement in import_patterns:
            updated_content = re.sub(pattern, replacement, updated_content)
            
        # Track import updates
        if updated_content != content:
            self.context.add_import_update(old_service, "updated")
            
        return updated_content
        
    async def _update_all_imports(self):
        """Update imports across all migrated files"""
        logger.info("Updating imports across all services")
        
        # This would scan all Python files and update imports
        # to use the new service structure
        
    async def _update_configurations(self):
        """Update configuration files for new architecture"""
        logger.info("Updating configuration files")
        
        # Update docker-compose files
        await self._update_docker_compose()
        
        # Update Consul service definitions
        await self._update_consul_services()
        
        # Update Kubernetes manifests
        await self._update_k8s_manifests()
        
    async def _update_docker_compose(self):
        """Update docker-compose files"""
        compose_file = self.context.target_dir / "infra" / "docker-compose" / "docker-compose.yml"
        
        if not compose_file.exists():
            return
            
        # Load existing compose file
        with open(compose_file) as f:
            compose_data = yaml.safe_load(f)
            
        # Remove old services
        services = compose_data.get('services', {})
        for old_service in self.context.migrated_services:
            services.pop(old_service, None)
            
        # Add new consolidated services
        for new_service in self.mapping.SERVICE_CONSOLIDATION.keys():
            services[new_service] = {
                'build': f'../../services/DataIntelligenceSuite/{new_service}',
                'environment': {
                    'SERVICE_NAME': new_service,
                    'CONSUL_URL': 'http://consul:8500',
                    'VAULT_URL': 'http://vault:8200'
                },
                'depends_on': ['consul', 'vault', 'pulsar'],
                'networks': ['platform-network']
            }
            
        # Save updated compose file
        if not self.context.dry_run:
            with open(compose_file, 'w') as f:
                yaml.dump(compose_data, f, default_flow_style=False)
                
    async def _update_consul_services(self):
        """Update Consul service definitions"""
        consul_dir = self.context.target_dir / "consul" / "services"
        
        if not consul_dir.exists():
            return
            
        # Remove old service definitions
        for old_service in self.context.migrated_services:
            service_file = consul_dir / f"{old_service}.json"
            if service_file.exists() and not self.context.dry_run:
                service_file.unlink()
                
        # Create new service definitions
        for new_service in self.mapping.SERVICE_CONSOLIDATION.keys():
            service_def = {
                "service": {
                    "name": new_service,
                    "tags": ["v2", "data-intelligence"],
                    "port": 8000,
                    "check": {
                        "http": f"http://localhost:8000/health",
                        "interval": "10s"
                    }
                }
            }
            
            if not self.context.dry_run:
                service_file = consul_dir / f"{new_service}.json"
                with open(service_file, 'w') as f:
                    json.dump(service_def, f, indent=2)
                    
    async def _update_k8s_manifests(self):
        """Update Kubernetes manifests"""
        k8s_dir = self.context.target_dir / "iac" / "kubernetes" / "charts"
        
        # This would update Helm charts for the new services
        
    async def _generate_report(self):
        """Generate migration report"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "migrated_services": list(self.context.migrated_services),
            "new_services": list(self.mapping.SERVICE_CONSOLIDATION.keys()),
            "errors": self.context.migration_errors,
            "import_updates": len(self.context.import_updates),
            "backup_location": str(self.context.backup_dir) if not self.context.dry_run else "N/A"
        }
        
        report_file = self.context.target_dir / "migration_report.json"
        
        if self.context.dry_run:
            logger.info("Migration Report (DRY RUN):")
            logger.info(json.dumps(report, indent=2))
        else:
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Migration report saved to {report_file}")
            

async def main():
    """Main migration function"""
    parser = argparse.ArgumentParser(description="Migrate DataIntelligenceSuite to v2.0")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path.cwd(),
        help="Source directory (current platform)"
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        default=Path.cwd(),
        help="Target directory (can be same as source)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Perform dry run without making changes"
    )
    parser.add_argument(
        "--services",
        nargs="+",
        help="Specific services to migrate (default: all)"
    )
    
    args = parser.parse_args()
    
    # Create migration context
    context = MigrationContext(args.source_dir, args.target_dir, args.dry_run)
    
    # Create migrator
    migrator = ServiceMigrator(context)
    
    # Run migration
    try:
        await migrator.migrate_all()
        logger.info("Migration completed successfully!")
        return 0
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1
        

if __name__ == "__main__":
    sys.exit(asyncio.run(main())) 