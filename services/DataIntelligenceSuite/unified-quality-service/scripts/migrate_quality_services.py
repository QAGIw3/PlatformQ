#!/usr/bin/env python3
"""
Migration script for consolidating quality services into the unified quality service.

This script helps migrate data, configurations, and rules from:
- data-quality-service
- quality-engine-service

To the new unified-quality-service.
"""

import asyncio
import json
import click
import httpx
from typing import Dict, List, Any, Optional
from datetime import datetime
import yaml
from pathlib import Path

# Service URLs
OLD_QUALITY_SERVICE_URL = "http://data-quality-service:8003"
OLD_ENGINE_SERVICE_URL = "http://quality-engine-service:8000"
NEW_UNIFIED_SERVICE_URL = "http://unified-quality-service:8000"


class QualityServiceMigrator:
    """Handles migration from old quality services to unified service."""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.migration_report = {
            "started_at": datetime.utcnow().isoformat(),
            "dry_run": dry_run,
            "rules_migrated": 0,
            "profiles_migrated": 0,
            "configurations_migrated": 0,
            "errors": []
        }
        
    async def migrate_all(self):
        """Perform complete migration."""
        click.echo("Starting quality service migration...")
        
        # 1. Export data from old services
        click.echo("\n1. Exporting from old services...")
        rules = await self.export_rules()
        profiles = await self.export_profiles()
        configs = await self.export_configurations()
        
        # 2. Transform data for unified service
        click.echo("\n2. Transforming data...")
        transformed_rules = self.transform_rules(rules)
        transformed_profiles = self.transform_profiles(profiles)
        transformed_configs = self.transform_configurations(configs)
        
        # 3. Import to unified service
        if not self.dry_run:
            click.echo("\n3. Importing to unified service...")
            await self.import_rules(transformed_rules)
            await self.import_profiles(transformed_profiles)
            await self.import_configurations(transformed_configs)
        else:
            click.echo("\n3. Dry run - skipping import")
            self._print_dry_run_summary(transformed_rules, transformed_profiles, transformed_configs)
            
        # 4. Verify migration
        if not self.dry_run:
            click.echo("\n4. Verifying migration...")
            await self.verify_migration()
            
        # 5. Generate report
        self.generate_report()
        
    async def export_rules(self) -> Dict[str, List[Dict[str, Any]]]:
        """Export rules from both old services."""
        rules = {
            "data-quality-service": [],
            "quality-engine-service": []
        }
        
        # Export from data-quality-service
        try:
            response = await self.http_client.get(f"{OLD_QUALITY_SERVICE_URL}/api/v1/rules")
            if response.status_code == 200:
                rules["data-quality-service"] = response.json()["rules"]
                click.echo(f"  Exported {len(rules['data-quality-service'])} rules from data-quality-service")
        except Exception as e:
            self.migration_report["errors"].append(f"Failed to export rules from data-quality-service: {e}")
            
        # Export from quality-engine-service
        try:
            response = await self.http_client.get(f"{OLD_ENGINE_SERVICE_URL}/api/v1/rules")
            if response.status_code == 200:
                rules["quality-engine-service"] = response.json()["rules"]
                click.echo(f"  Exported {len(rules['quality-engine-service'])} rules from quality-engine-service")
        except Exception as e:
            self.migration_report["errors"].append(f"Failed to export rules from quality-engine-service: {e}")
            
        return rules
        
    async def export_profiles(self) -> Dict[str, List[Dict[str, Any]]]:
        """Export data profiles from old services."""
        profiles = {
            "data-quality-service": [],
            "quality-engine-service": []
        }
        
        # Note: In a real implementation, you would fetch actual profiles
        # This is a placeholder for the migration logic
        click.echo("  Exported data profiles (placeholder)")
        
        return profiles
        
    async def export_configurations(self) -> Dict[str, Dict[str, Any]]:
        """Export service configurations."""
        configs = {}
        
        # Export monitoring configurations
        try:
            response = await self.http_client.get(f"{OLD_QUALITY_SERVICE_URL}/api/v1/monitoring/config")
            if response.status_code == 200:
                configs["monitoring"] = response.json()
                click.echo("  Exported monitoring configuration")
        except Exception as e:
            self.migration_report["errors"].append(f"Failed to export monitoring config: {e}")
            
        return configs
        
    def transform_rules(self, rules: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Transform rules to unified format."""
        transformed = []
        rule_map = {}  # To handle duplicates
        
        # Process rules from both services
        for service, service_rules in rules.items():
            for rule in service_rules:
                # Create unique key to detect duplicates
                rule_key = f"{rule.get('name', '')}_{rule.get('type', '')}"
                
                if rule_key in rule_map:
                    # Merge duplicate rules
                    existing = rule_map[rule_key]
                    existing["metadata"]["sources"].append(service)
                    # Merge tags
                    existing["tags"] = list(set(existing.get("tags", []) + rule.get("tags", [])))
                else:
                    # Transform rule to unified format
                    transformed_rule = {
                        "id": rule.get("id", f"migrated_{len(transformed)}"),
                        "name": rule.get("name"),
                        "description": rule.get("description", ""),
                        "type": rule.get("rule_type", rule.get("type", "validation")),
                        "definition": rule.get("definition", {}),
                        "severity": rule.get("severity", "warning"),
                        "tags": rule.get("tags", []),
                        "enabled": rule.get("enabled", True),
                        "metadata": {
                            "sources": [service],
                            "migrated_at": datetime.utcnow().isoformat(),
                            "original_id": rule.get("id")
                        }
                    }
                    
                    # Handle service-specific transformations
                    if service == "quality-engine-service":
                        # Quality engine specific fields
                        if "rule_type" in rule:
                            transformed_rule["engine_type"] = rule["rule_type"]
                            
                    transformed.append(transformed_rule)
                    rule_map[rule_key] = transformed_rule
                    
        click.echo(f"  Transformed {len(transformed)} unique rules")
        return transformed
        
    def transform_profiles(self, profiles: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Transform profiles to unified format."""
        # Placeholder transformation logic
        transformed = []
        click.echo(f"  Transformed {len(transformed)} profiles")
        return transformed
        
    def transform_configurations(self, configs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Transform configurations to unified format."""
        transformed = {
            "monitoring": configs.get("monitoring", {}),
            "alerts": configs.get("alerts", {}),
            "ml_settings": {
                "auto_retrain": True,
                "anomaly_models": ["isolation_forest", "prophet", "lstm"],
                "ensemble_strategy": "weighted_voting"
            },
            "seatunnel_integration": {
                "enabled": True,
                "quality_gates": ["completeness", "accuracy", "anomaly"]
            }
        }
        
        click.echo("  Transformed configurations")
        return transformed
        
    async def import_rules(self, rules: List[Dict[str, Any]]):
        """Import rules to unified service."""
        success_count = 0
        
        for rule in rules:
            try:
                response = await self.http_client.post(
                    f"{NEW_UNIFIED_SERVICE_URL}/api/v1/rules",
                    json=rule
                )
                if response.status_code == 200:
                    success_count += 1
                else:
                    self.migration_report["errors"].append(
                        f"Failed to import rule {rule['name']}: {response.text}"
                    )
            except Exception as e:
                self.migration_report["errors"].append(f"Error importing rule {rule['name']}: {e}")
                
        self.migration_report["rules_migrated"] = success_count
        click.echo(f"  Imported {success_count}/{len(rules)} rules")
        
    async def import_profiles(self, profiles: List[Dict[str, Any]]):
        """Import profiles to unified service."""
        # Placeholder import logic
        self.migration_report["profiles_migrated"] = len(profiles)
        click.echo(f"  Imported {len(profiles)} profiles")
        
    async def import_configurations(self, configs: Dict[str, Any]):
        """Import configurations to unified service."""
        try:
            response = await self.http_client.post(
                f"{NEW_UNIFIED_SERVICE_URL}/api/v1/admin/config",
                json=configs
            )
            if response.status_code == 200:
                self.migration_report["configurations_migrated"] = 1
                click.echo("  Imported configurations")
            else:
                self.migration_report["errors"].append(f"Failed to import config: {response.text}")
        except Exception as e:
            self.migration_report["errors"].append(f"Error importing config: {e}")
            
    async def verify_migration(self):
        """Verify the migration was successful."""
        try:
            # Check service health
            response = await self.http_client.get(f"{NEW_UNIFIED_SERVICE_URL}/health")
            if response.status_code != 200:
                self.migration_report["errors"].append("Unified service health check failed")
                return
                
            # Verify rule count
            response = await self.http_client.get(f"{NEW_UNIFIED_SERVICE_URL}/api/v1/rules")
            if response.status_code == 200:
                rule_count = len(response.json().get("rules", []))
                click.echo(f"  Verified {rule_count} rules in unified service")
                
        except Exception as e:
            self.migration_report["errors"].append(f"Verification failed: {e}")
            
    def _print_dry_run_summary(self, rules, profiles, configs):
        """Print summary for dry run."""
        click.echo("\nDry Run Summary:")
        click.echo(f"  Rules to migrate: {len(rules)}")
        click.echo(f"  Profiles to migrate: {len(profiles)}")
        click.echo("  Configurations to migrate: yes")
        click.echo("\nSample transformed rule:")
        if rules:
            click.echo(json.dumps(rules[0], indent=2))
            
    def generate_report(self):
        """Generate migration report."""
        self.migration_report["completed_at"] = datetime.utcnow().isoformat()
        
        report_path = Path("migration_report.json")
        with open(report_path, "w") as f:
            json.dump(self.migration_report, f, indent=2)
            
        click.echo(f"\nMigration Report saved to: {report_path}")
        click.echo(f"  Rules migrated: {self.migration_report['rules_migrated']}")
        click.echo(f"  Profiles migrated: {self.migration_report['profiles_migrated']}")
        click.echo(f"  Configurations migrated: {self.migration_report['configurations_migrated']}")
        
        if self.migration_report["errors"]:
            click.echo(f"\n  Errors encountered: {len(self.migration_report['errors'])}")
            for error in self.migration_report["errors"][:5]:
                click.echo(f"    - {error}")
                
    async def cleanup(self):
        """Clean up resources."""
        await self.http_client.aclose()


@click.command()
@click.option('--dry-run', is_flag=True, default=True, help='Perform dry run without actual migration')
@click.option('--source', type=click.Choice(['data-quality', 'quality-engine', 'both']), 
              default='both', help='Source service to migrate from')
@click.option('--export-only', is_flag=True, help='Only export data, do not import')
def main(dry_run: bool, source: str, export_only: bool):
    """Migrate quality services to unified quality service."""
    migrator = QualityServiceMigrator(dry_run=dry_run)
    
    async def run():
        try:
            if export_only:
                click.echo("Export-only mode...")
                rules = await migrator.export_rules()
                profiles = await migrator.export_profiles()
                configs = await migrator.export_configurations()
                
                # Save to files
                with open("exported_rules.json", "w") as f:
                    json.dump(rules, f, indent=2)
                with open("exported_profiles.json", "w") as f:
                    json.dump(profiles, f, indent=2)
                with open("exported_configs.json", "w") as f:
                    json.dump(configs, f, indent=2)
                    
                click.echo("\nExported data saved to files")
            else:
                await migrator.migrate_all()
        finally:
            await migrator.cleanup()
            
    asyncio.run(run())


if __name__ == "__main__":
    main() 