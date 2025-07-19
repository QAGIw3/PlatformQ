"""
Storage Service Integration

Provides integration with the Storage Service for:
- Data lake backup and archival
- Document conversion for reports
- Long-term data retention
- Cross-zone data migration
"""

import logging
from typing import Dict, Any, List, Optional, AsyncIterator
from datetime import datetime, timedelta
import asyncio
import httpx
from pathlib import Path
import json

from ..lake.medallion_architecture import MedallionLakeManager
from ..governance.catalog_manager import DataCatalogManager
from ..lineage.lineage_tracker import DataLineageTracker
from ..pipelines.pipeline_coordinator import PipelineCoordinator

logger = logging.getLogger(__name__)


class StorageIntegration:
    """Integration with Storage Service"""
    
    def __init__(self,
                 lake_manager: MedallionLakeManager,
                 catalog_manager: DataCatalogManager,
                 lineage_tracker: DataLineageTracker,
                 pipeline_coordinator: PipelineCoordinator,
                 storage_service_url: str = "http://storage-service:8000"):
        self.lake_manager = lake_manager
        self.catalog_manager = catalog_manager
        self.lineage_tracker = lineage_tracker
        self.pipeline_coordinator = pipeline_coordinator
        self.storage_service_url = storage_service_url
        self.http_client = httpx.AsyncClient(timeout=300.0)  # Longer timeout for large files
        
        # Archive configurations
        self.archive_configs = {}
        
    async def archive_dataset(self,
                            dataset_path: str,
                            archive_policy: Dict[str, Any],
                            metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Archive dataset to long-term storage
        
        Args:
            dataset_path: Path to dataset in data lake
            archive_policy: Archive policy configuration
            metadata: Optional metadata to include
            
        Returns:
            Archive operation result
        """
        try:
            # Get dataset info from catalog
            dataset_info = await self.catalog_manager.get_asset_by_path(dataset_path)
            
            # Prepare archive metadata
            archive_metadata = {
                "source_path": dataset_path,
                "dataset_name": dataset_info.get("name", Path(dataset_path).name),
                "created_at": datetime.utcnow().isoformat(),
                "policy": archive_policy,
                "catalog_info": dataset_info,
                **(metadata or {})
            }
            
            # Create archive package
            archive_id = f"archive_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{Path(dataset_path).name}"
            
            # Read data from lake
            df = self.lake_manager.spark.read.parquet(f"{self.lake_manager.base_path}/{dataset_path}")
            
            # Convert to temporary file for upload
            temp_path = f"/tmp/{archive_id}.parquet"
            df.coalesce(1).write.mode("overwrite").parquet(temp_path)
            
            # Upload to storage service
            with open(f"{temp_path}/part-00000-*.parquet", "rb") as f:
                files = {"file": (f"{archive_id}.parquet", f, "application/octet-stream")}
                response = await self.http_client.post(
                    f"{self.storage_service_url}/api/v1/upload",
                    files=files,
                    params={
                        "auto_convert": "false",
                        "preview": "false"
                    },
                    headers={"X-Metadata": json.dumps(archive_metadata)}
                )
                response.raise_for_status()
                
            storage_result = response.json()
            
            # Track in catalog
            await self.catalog_manager.register_archive(
                dataset_id=dataset_info["id"],
                archive_id=storage_result["identifier"],
                storage_path=storage_result["identifier"],
                metadata=archive_metadata
            )
            
            # Track lineage
            await self._track_archive_lineage(
                source_path=dataset_path,
                archive_id=storage_result["identifier"]
            )
            
            return {
                "archive_id": storage_result["identifier"],
                "status": "archived",
                "size_bytes": storage_result.get("size", 0),
                "storage_type": "long_term",
                "metadata": archive_metadata
            }
            
        except Exception as e:
            logger.error(f"Failed to archive dataset: {e}")
            raise
            
    async def create_backup_pipeline(self,
                                   source_pattern: str,
                                   backup_schedule: str,
                                   retention_policy: Dict[str, Any]) -> str:
        """
        Create automated backup pipeline
        
        Args:
            source_pattern: Pattern for datasets to backup
            backup_schedule: Cron schedule for backups
            retention_policy: Backup retention configuration
            
        Returns:
            Pipeline ID
        """
        try:
            # Create backup pipeline configuration
            pipeline_config = {
                "name": f"Backup Pipeline - {source_pattern}",
                "type": "batch",
                "schedule": backup_schedule,
                "steps": [
                    {
                        "name": "source",
                        "type": "source",
                        "connector": "file",
                        "config": {
                            "format": "parquet",
                            "path": source_pattern,
                            "recursive": True
                        }
                    },
                    {
                        "name": "backup",
                        "type": "transform",
                        "config": {
                            "operation": "backup",
                            "storage_service_url": self.storage_service_url,
                            "retention": retention_policy
                        }
                    },
                    {
                        "name": "catalog_update",
                        "type": "sink",
                        "config": {
                            "type": "catalog",
                            "update_backup_status": True
                        }
                    }
                ]
            }
            
            # Create pipeline
            pipeline_id = await self.pipeline_coordinator.create_pipeline(
                pipeline_id=f"backup_{datetime.utcnow().timestamp()}",
                config=pipeline_config
            )
            
            # Store backup configuration
            self.archive_configs[pipeline_id] = {
                "source_pattern": source_pattern,
                "schedule": backup_schedule,
                "retention": retention_policy,
                "created_at": datetime.utcnow()
            }
            
            return pipeline_id
            
        except Exception as e:
            logger.error(f"Failed to create backup pipeline: {e}")
            raise
            
    async def restore_from_archive(self,
                                 archive_id: str,
                                 target_path: Optional[str] = None,
                                 target_zone: str = "bronze") -> Dict[str, Any]:
        """
        Restore dataset from archive
        
        Args:
            archive_id: Archive identifier
            target_path: Optional target path (defaults to original)
            target_zone: Target zone in data lake
            
        Returns:
            Restore operation result
        """
        try:
            # Get archive metadata from catalog
            archive_info = await self.catalog_manager.get_archive_info(archive_id)
            
            if not archive_info:
                raise ValueError(f"Archive not found: {archive_id}")
                
            # Download from storage service
            response = await self.http_client.get(
                f"{self.storage_service_url}/api/v1/download/{archive_id}",
                params={"format": "parquet"}
            )
            response.raise_for_status()
            
            # Save to temporary location
            temp_path = f"/tmp/restore_{archive_id}.parquet"
            with open(temp_path, "wb") as f:
                async for chunk in response.aiter_bytes():
                    f.write(chunk)
                    
            # Determine target path
            if not target_path:
                target_path = archive_info["original_path"]
                
            # Restore to data lake
            df = self.lake_manager.spark.read.parquet(temp_path)
            full_target_path = f"{self.lake_manager.base_path}/{target_zone}/{target_path}"
            df.write.mode("overwrite").parquet(full_target_path)
            
            # Update catalog
            await self.catalog_manager.update_asset(
                asset_id=archive_info["dataset_id"],
                updates={
                    "last_restored": datetime.utcnow().isoformat(),
                    "restore_path": f"{target_zone}/{target_path}"
                }
            )
            
            # Track lineage
            await self._track_restore_lineage(
                archive_id=archive_id,
                target_path=f"{target_zone}/{target_path}"
            )
            
            return {
                "status": "restored",
                "archive_id": archive_id,
                "target_path": f"{target_zone}/{target_path}",
                "restored_at": datetime.utcnow().isoformat(),
                "size_bytes": archive_info.get("size_bytes", 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to restore from archive: {e}")
            raise
            
    async def generate_data_report(self,
                                 query: str,
                                 report_format: str = "pdf",
                                 template: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate formatted report from data
        
        Args:
            query: Query to get report data
            report_format: Output format (pdf, docx, html)
            template: Optional report template
            
        Returns:
            Report generation result
        """
        try:
            # Execute query
            from ..federation.federated_query_engine import FederatedQueryEngine
            self.federated_engine = FederatedQueryEngine(self.lake_manager.spark) # Initialize FederatedQueryEngine
            result = await self.federated_engine.execute_query(query)
            
            # Convert to report format
            report_data = {
                "title": f"Data Report - {datetime.utcnow().strftime('%Y-%m-%d')}",
                "query": query,
                "data": result["rows"],
                "columns": result["columns"],
                "row_count": len(result["rows"]),
                "generated_at": datetime.utcnow().isoformat()
            }
            
            # Create HTML report first
            html_content = self._generate_html_report(report_data, template)
            
            # Upload HTML for conversion
            files = {
                "file": (
                    "report.html",
                    html_content.encode('utf-8'),
                    "text/html"
                )
            }
            
            upload_response = await self.http_client.post(
                f"{self.storage_service_url}/api/v1/upload",
                files=files,
                params={"auto_convert": "true"}
            )
            upload_response.raise_for_status()
            
            upload_result = upload_response.json()
            
            # Request conversion if needed
            if report_format != "html":
                convert_response = await self.http_client.post(
                    f"{self.storage_service_url}/api/v1/convert",
                    json={
                        "source_identifier": upload_result["identifier"],
                        "target_format": report_format,
                        "options": {
                            "quality": "high",
                            "preserve_formatting": True
                        }
                    }
                )
                convert_response.raise_for_status()
                
                conversion = convert_response.json()
                
                # Wait for conversion
                while conversion["status"] == "processing":
                    await asyncio.sleep(2)
                    status_response = await self.http_client.get(
                        f"{self.storage_service_url}/api/v1/convert/status/{conversion['job_id']}"
                    )
                    conversion = status_response.json()
                    
                if conversion["status"] == "completed":
                    report_id = conversion["output_identifier"]
                else:
                    raise Exception(f"Conversion failed: {conversion.get('error')}")
            else:
                report_id = upload_result["identifier"]
                
            return {
                "report_id": report_id,
                "format": report_format,
                "download_url": f"{self.storage_service_url}/api/v1/download/{report_id}",
                "metadata": report_data
            }
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            raise
            
    async def migrate_to_cold_storage(self,
                                     age_threshold_days: int = 90,
                                     zones: List[str] = ["bronze", "silver"],
                                     dry_run: bool = False) -> Dict[str, Any]:
        """
        Migrate old data to cold storage
        
        Args:
            age_threshold_days: Age threshold for migration
            zones: Zones to check for old data
            dry_run: If True, only simulate migration
            
        Returns:
            Migration results
        """
        try:
            migration_candidates = []
            threshold_date = datetime.utcnow() - timedelta(days=age_threshold_days)
            
            # Find candidates for migration
            for zone in zones:
                assets = await self.catalog_manager.search_assets(
                    filters={
                        "zone": zone,
                        "last_accessed_before": threshold_date.isoformat()
                    }
                )
                
                for asset in assets:
                    size_gb = asset.get("size_bytes", 0) / (1024**3)
                    migration_candidates.append({
                        "asset_id": asset["id"],
                        "path": asset["path"],
                        "zone": zone,
                        "size_gb": size_gb,
                        "last_accessed": asset.get("last_accessed"),
                        "age_days": (datetime.utcnow() - datetime.fromisoformat(
                            asset.get("created_at", datetime.utcnow().isoformat())
                        )).days
                    })
                    
            if dry_run:
                return {
                    "mode": "dry_run",
                    "candidates": migration_candidates,
                    "total_size_gb": sum(c["size_gb"] for c in migration_candidates),
                    "count": len(migration_candidates)
                }
                
            # Perform migration
            migrated = []
            failed = []
            
            for candidate in migration_candidates:
                try:
                    # Archive to cold storage
                    result = await self.archive_dataset(
                        dataset_path=candidate["path"],
                        archive_policy={
                            "type": "cold_storage",
                            "retention_years": 7,
                            "compression": "high"
                        },
                        metadata={
                            "migration_reason": "age_threshold",
                            "original_zone": candidate["zone"]
                        }
                    )
                    
                    # Delete from hot storage
                    self.lake_manager.spark.sql(
                        f"DROP TABLE IF EXISTS {candidate['zone']}.{Path(candidate['path']).name}"
                    )
                    
                    migrated.append({
                        **candidate,
                        "archive_id": result["archive_id"]
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to migrate {candidate['path']}: {e}")
                    failed.append({
                        **candidate,
                        "error": str(e)
                    })
                    
            return {
                "mode": "executed",
                "migrated": migrated,
                "failed": failed,
                "total_migrated_gb": sum(m["size_gb"] for m in migrated),
                "success_rate": len(migrated) / len(migration_candidates) if migration_candidates else 0
            }
            
        except Exception as e:
            logger.error(f"Failed to migrate to cold storage: {e}")
            raise
            
    async def setup_disaster_recovery(self,
                                    critical_datasets: List[str],
                                    recovery_point_objective_hours: int = 24,
                                    recovery_regions: List[str] = ["us-east-1", "eu-west-1"]) -> Dict[str, Any]:
        """
        Set up disaster recovery for critical datasets
        
        Args:
            critical_datasets: List of critical dataset paths
            recovery_point_objective_hours: RPO in hours
            recovery_regions: Regions for replication
            
        Returns:
            DR setup configuration
        """
        try:
            dr_config = {
                "datasets": critical_datasets,
                "rpo_hours": recovery_point_objective_hours,
                "regions": recovery_regions,
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Create replication pipelines for each dataset
            replication_pipelines = []
            
            for dataset in critical_datasets:
                for region in recovery_regions:
                    # Create cross-region replication pipeline
                    pipeline_config = {
                        "name": f"DR Replication - {Path(dataset).name} to {region}",
                        "type": "streaming",
                        "steps": [
                            {
                                "name": "source",
                                "type": "source",
                                "connector": "change_data_capture",
                                "config": {
                                    "source_path": dataset,
                                    "capture_deletes": True
                                }
                            },
                            {
                                "name": "replicate",
                                "type": "sink",
                                "config": {
                                    "type": "cross_region_storage",
                                    "region": region,
                                    "storage_service_url": f"{self.storage_service_url}/regions/{region}"
                                }
                            }
                        ]
                    }
                    
                    pipeline_id = await self.pipeline_coordinator.create_pipeline(
                        pipeline_id=f"dr_{dataset}_{region}",
                        config=pipeline_config
                    )
                    
                    replication_pipelines.append({
                        "pipeline_id": pipeline_id,
                        "dataset": dataset,
                        "region": region
                    })
                    
            # Set up monitoring
            monitor_config = {
                "alert_on_lag_minutes": recovery_point_objective_hours * 60,
                "check_interval_minutes": 60,
                "notification_channels": ["ops-team@platform.io"]
            }
            
            return {
                "dr_config": dr_config,
                "replication_pipelines": replication_pipelines,
                "monitoring": monitor_config,
                "status": "active"
            }
            
        except Exception as e:
            logger.error(f"Failed to setup disaster recovery: {e}")
            raise
            
    # Helper methods
    
    def _generate_html_report(self, report_data: Dict[str, Any], template: Optional[str] = None) -> str:
        """Generate HTML report from data"""
        if template:
            # Use custom template
            html = template
            for key, value in report_data.items():
                html = html.replace(f"{{{{{key}}}}}", str(value))
        else:
            # Generate default table report
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{report_data['title']}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #4CAF50; color: white; }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    .metadata {{ margin: 20px 0; font-size: 14px; color: #666; }}
                </style>
            </head>
            <body>
                <h1>{report_data['title']}</h1>
                <div class="metadata">
                    <p>Generated: {report_data['generated_at']}</p>
                    <p>Total Rows: {report_data['row_count']}</p>
                </div>
                <table>
                    <thead>
                        <tr>
                            {''.join(f"<th>{col['name']}</th>" for col in report_data['columns'])}
                        </tr>
                    </thead>
                    <tbody>
                        {''.join(
                            f"<tr>{''.join(f'<td>{cell}</td>' for cell in row)}</tr>"
                            for row in report_data['data'][:1000]
                        )}
                    </tbody>
                </table>
            </body>
            </html>
            """
            
        return html
        
    async def _track_archive_lineage(self, source_path: str, archive_id: str):
        """Track lineage for archive operation"""
        # Create archive node
        archive_node = await self.lineage_tracker.add_node(
            node_type="archive",
            name=archive_id,
            namespace="storage",
            attributes={
                "storage_type": "long_term",
                "archived_at": datetime.utcnow().isoformat()
            }
        )
        
        # Create source node
        source_node = await self.lineage_tracker.add_node(
            node_type="dataset",
            name=source_path,
            namespace="lake"
        )
        
        # Create edge
        await self.lineage_tracker.add_edge(
            source_id=source_node["node_id"],
            target_id=archive_node["node_id"],
            edge_type="archived_to"
        )
        
    async def _track_restore_lineage(self, archive_id: str, target_path: str):
        """Track lineage for restore operation"""
        # Create nodes
        archive_node = await self.lineage_tracker.add_node(
            node_type="archive",
            name=archive_id,
            namespace="storage"
        )
        
        target_node = await self.lineage_tracker.add_node(
            node_type="dataset",
            name=target_path,
            namespace="lake",
            attributes={
                "restored_at": datetime.utcnow().isoformat()
            }
        )
        
        # Create edge
        await self.lineage_tracker.add_edge(
            source_id=archive_node["node_id"],
            target_id=target_node["node_id"],
            edge_type="restored_from"
        )
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 