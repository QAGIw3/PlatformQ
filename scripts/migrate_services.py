#!/usr/bin/env python3
"""
Service Migration Script

Migrates existing PlatformQ services to use the new shared library patterns.
"""

import os
import re
import shutil
from pathlib import Path
from typing import List, Dict, Tuple
import click
import yaml

# Services to migrate
SERVICES_TO_MIGRATE = [
    "analytics-service",  # Already done
    "auth-service",  # Already done
    "digital-asset-service",  # Already done
    "blockchain-event-bridge",
    "cad-collaboration-service", 
    "connector-service",
    "data-governance-service",
    "data-lake-service",
    "federated-learning-service",
    "functions-service",
    "graph-intelligence-service",
    "mlops-service",
    "neuromorphic-service",
    "notification-service",
    "projects-service",
    "proposals-service",
    "provisioning-service",
    "quantum-optimization-service",
    "realtime-analytics-service",  # To be merged
    "schema-registry-service",
    "search-service",
    "seatunnel-service",
    "simulation-service",
    "storage-proxy-service",
    "unified-data-service",
    "verifiable-credential-service",
    "workflow-service"
]

# Services already migrated
MIGRATED_SERVICES = [
    "analytics-service",
    "auth-service", 
    "digital-asset-service"
]

# Service consolidation mapping
CONSOLIDATION_MAP = {
    "realtime-analytics-service": "analytics-service",
    "dataset-marketplace": "digital-asset-service",
    "compute-marketplace": "digital-asset-service",
    "data-governance-service": "unified-data-service",
    "policy-service": "auth-service"
}


class ServiceMigrator:
    """Handles migration of services to new patterns"""
    
    def __init__(self, workspace_root: Path):
        self.workspace_root = workspace_root
        self.services_dir = workspace_root / "services"
        
    def analyze_service(self, service_name: str) -> Dict[str, any]:
        """Analyze a service to determine migration needs"""
        service_path = self.services_dir / service_name
        
        if not service_path.exists():
            return {"exists": False}
            
        analysis = {
            "exists": True,
            "has_main": (service_path / "app" / "main.py").exists(),
            "has_repository": (service_path / "app" / "repository.py").exists(),
            "has_event_processors": (service_path / "app" / "event_processors.py").exists(),
            "uses_base_service": False,
            "uses_event_framework": False,
            "uses_repository_pattern": False,
            "database_type": self._detect_database_type(service_path),
            "has_pulsar": self._check_pulsar_usage(service_path),
            "has_api_endpoints": (service_path / "app" / "api").exists()
        }
        
        # Check if already using new patterns
        if analysis["has_main"]:
            main_content = (service_path / "app" / "main.py").read_text()
            analysis["uses_base_service"] = "create_base_app" in main_content
            analysis["uses_event_framework"] = "EventProcessor" in main_content
            
        if analysis["has_repository"]:
            repo_content = (service_path / "app" / "repository.py").read_text()
            analysis["uses_repository_pattern"] = any(
                pattern in repo_content 
                for pattern in ["PostgresRepository", "CassandraRepository"]
            )
            
        return analysis
        
    def _detect_database_type(self, service_path: Path) -> str:
        """Detect which database the service uses"""
        requirements_path = service_path / "requirements.txt"
        if not requirements_path.exists():
            requirements_path = service_path / "requirements.in"
            
        if requirements_path.exists():
            requirements = requirements_path.read_text().lower()
            has_postgres = "psycopg2" in requirements or "sqlalchemy" in requirements
            has_cassandra = "cassandra-driver" in requirements
            
            if has_postgres and has_cassandra:
                return "both"
            elif has_postgres:
                return "postgresql"
            elif has_cassandra:
                return "cassandra"
                
        return "none"
        
    def _check_pulsar_usage(self, service_path: Path) -> bool:
        """Check if service uses Pulsar"""
        main_path = service_path / "app" / "main.py"
        if main_path.exists():
            return "pulsar" in main_path.read_text().lower()
        return False
        
    def generate_migration_plan(self, service_name: str) -> List[str]:
        """Generate migration steps for a service"""
        analysis = self.analyze_service(service_name)
        
        if not analysis["exists"]:
            return [f"Service {service_name} not found"]
            
        steps = []
        
        # Check if consolidation is needed
        if service_name in CONSOLIDATION_MAP:
            target = CONSOLIDATION_MAP[service_name]
            steps.append(f"CONSOLIDATE: Merge into {target}")
            return steps
            
        # Generate migration steps
        if not analysis["uses_base_service"]:
            steps.append("UPDATE: Refactor main.py to use create_base_app")
            
        if not analysis["uses_event_framework"] and analysis["has_pulsar"]:
            steps.append("CREATE: Add event_processors.py with EventProcessor classes")
            steps.append("UPDATE: Refactor Pulsar consumers to use @event_handler")
            
        if not analysis["uses_repository_pattern"] and analysis["database_type"] != "none":
            steps.append(f"UPDATE: Refactor repository.py to use {analysis['database_type']} base classes")
            
        if analysis["has_api_endpoints"]:
            steps.append("UPDATE: Add ServiceClients for inter-service communication")
            steps.append("UPDATE: Use standardized error handling (NotFoundError, ValidationError, etc)")
            
        if not steps:
            steps.append("READY: Service already follows new patterns")
            
        return steps
        
    def migrate_service(self, service_name: str, dry_run: bool = True):
        """Perform migration for a service"""
        print(f"\n{'='*60}")
        print(f"Analyzing {service_name}...")
        print(f"{'='*60}")
        
        analysis = self.analyze_service(service_name)
        
        if not analysis["exists"]:
            print(f"❌ Service {service_name} not found")
            return
            
        # Print analysis
        print("\nCurrent State:")
        for key, value in analysis.items():
            if key != "exists":
                print(f"  - {key}: {value}")
                
        # Generate and print migration plan
        steps = self.generate_migration_plan(service_name)
        print("\nMigration Steps:")
        for i, step in enumerate(steps, 1):
            print(f"  {i}. {step}")
            
        if not dry_run and "READY" not in steps[0]:
            response = input("\nProceed with migration? (y/N): ")
            if response.lower() == 'y':
                self._apply_migration(service_name, analysis, steps)
                
    def _apply_migration(self, service_name: str, analysis: Dict, steps: List[str]):
        """Apply migration changes to a service"""
        print(f"\n🔧 Migrating {service_name}...")
        
        # This is where you would implement the actual file modifications
        # For now, we'll create a migration report
        
        report_path = self.workspace_root / f"migration_{service_name}.md"
        with open(report_path, 'w') as f:
            f.write(f"# Migration Report for {service_name}\n\n")
            f.write("## Analysis\n\n")
            for key, value in analysis.items():
                f.write(f"- {key}: {value}\n")
            f.write("\n## Migration Steps\n\n")
            for step in steps:
                f.write(f"- [ ] {step}\n")
                
        print(f"✅ Migration report created: {report_path}")


@click.command()
@click.option('--service', '-s', help='Specific service to migrate')
@click.option('--all', 'migrate_all', is_flag=True, help='Migrate all services')
@click.option('--dry-run/--no-dry-run', default=True, help='Show migration plan without applying')
@click.option('--skip-migrated', is_flag=True, help='Skip already migrated services')
def main(service: str, migrate_all: bool, dry_run: bool, skip_migrated: bool):
    """Migrate PlatformQ services to new patterns"""
    
    workspace_root = Path.cwd()
    migrator = ServiceMigrator(workspace_root)
    
    if service:
        # Migrate specific service
        migrator.migrate_service(service, dry_run)
    elif migrate_all:
        # Migrate all services
        services = SERVICES_TO_MIGRATE
        if skip_migrated:
            services = [s for s in services if s not in MIGRATED_SERVICES]
            
        print(f"Found {len(services)} services to migrate\n")
        
        for service_name in services:
            migrator.migrate_service(service_name, dry_run)
            
        # Summary
        print(f"\n{'='*60}")
        print("MIGRATION SUMMARY")
        print(f"{'='*60}")
        print(f"Total services: {len(SERVICES_TO_MIGRATE)}")
        print(f"Already migrated: {len(MIGRATED_SERVICES)}")
        print(f"Remaining: {len(services)}")
        
        # Show consolidation targets
        print("\nConsolidation Targets:")
        for source, target in CONSOLIDATION_MAP.items():
            print(f"  - {source} → {target}")
    else:
        print("Please specify --service NAME or --all")
        

if __name__ == "__main__":
    main() 