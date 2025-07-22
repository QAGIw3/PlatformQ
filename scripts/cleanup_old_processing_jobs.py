#!/usr/bin/env python3
"""
Cleanup script for old processing jobs that have been migrated to DataIntelligenceSuite.

This script identifies old Flink and Spark job directories that are no longer needed
after the consolidation into Stream Processing Service and Batch Processing Service.
"""

import os
import shutil
import argparse
from pathlib import Path
from typing import List, Tuple


# Directories to be removed (migrated to consolidated services)
OLD_FLINK_JOBS = [
    "processing/flink/activity-stream-job",
    "processing/flink/graph-analytics-job",
    "processing/flink/complex-event-processing-job",
    "processing/flink/data-quality-job",
    "processing/flink/model-monitoring-job",
    "processing/flink/simulation-engine-job",
    "processing/flink/derivatives-cep-job",
    "processing/flink/fraud-detection-job",
    "processing/flink/risk-analytics-job",
    "processing/flink/graph-ingestion-job",
    "processing/flink/collaborative-ml-job",
    "processing/flink/data-quality-monitoring-job",
    "processing/flink/compute-futures-settlement-job",
    "processing/flink/mesh-optimization-job",
    "processing/flink/simulation-ml-trigger-job",
    "processing/flink/royalty-calculation-job",
    "processing/flink/resilience-job",
    "processing/flink/workflow-federation-job",
    "processing/flink/simulation-collaboration-job",
    "processing/flink/real_time_anomaly_detection.py",
    "processing/flink/predictive-alerting-job",
    "processing/flink/lineage-ingestion-job",
]

OLD_SPARK_JOBS = [
    "processing/spark/derivatives_ml_training.py",
    "processing/spark/ml/asset_classifier.py",
    "processing/spark/ml/federated_learning.py",
    "processing/spark/ml/simulation_ml_training.py",
    "processing/spark/ml/anomaly_predictor.py",
    "processing/spark/ml/trust_ranker.py",
    "processing/spark/ml/dag_pruner.py",
    "processing/spark/ml/pipeline_optimizer.py",
    "processing/spark/feature_selection_spark.py",
    "processing/spark/blender_distributed.py",
    "processing/spark/graphx/",
    "processing/spark/ml/failure_predictor.py",
    "processing/spark/ml/federated_aggregator.py",
]

# Files to update (remove references to old jobs)
FILES_TO_UPDATE = [
    "processing/README.md",
    "docs/architecture/data_processing.md",
]


def find_project_root() -> Path:
    """Find the project root directory."""
    current = Path.cwd()
    while current != current.parent:
        if (current / "services" / "DataIntelligenceSuite").exists():
            return current
        current = current.parent
    raise RuntimeError("Could not find project root")


def identify_old_files(project_root: Path) -> Tuple[List[Path], List[Path]]:
    """Identify old files and directories to be removed."""
    dirs_to_remove = []
    files_to_remove = []
    
    # Check Flink jobs
    for job_path in OLD_FLINK_JOBS:
        full_path = project_root / job_path
        if full_path.exists():
            if full_path.is_dir():
                dirs_to_remove.append(full_path)
            else:
                files_to_remove.append(full_path)
    
    # Check Spark jobs
    for job_path in OLD_SPARK_JOBS:
        full_path = project_root / job_path
        if full_path.exists():
            if full_path.is_dir():
                dirs_to_remove.append(full_path)
            else:
                files_to_remove.append(full_path)
    
    return dirs_to_remove, files_to_remove


def create_backup(paths: List[Path], backup_dir: Path) -> None:
    """Create a backup of files before deletion."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    
    for path in paths:
        if path.exists():
            relative_path = path.relative_to(find_project_root())
            backup_path = backup_dir / relative_path
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            if path.is_dir():
                shutil.copytree(path, backup_path, dirs_exist_ok=True)
            else:
                shutil.copy2(path, backup_path)
            
            print(f"Backed up: {relative_path}")


def remove_old_files(dirs: List[Path], files: List[Path], dry_run: bool = True) -> None:
    """Remove old files and directories."""
    if dry_run:
        print("\n=== DRY RUN MODE - No files will be deleted ===\n")
    
    # Remove files first
    for file_path in files:
        if file_path.exists():
            print(f"{'Would remove' if dry_run else 'Removing'} file: {file_path}")
            if not dry_run:
                file_path.unlink()
    
    # Remove directories
    for dir_path in dirs:
        if dir_path.exists():
            print(f"{'Would remove' if dry_run else 'Removing'} directory: {dir_path}")
            if not dry_run:
                shutil.rmtree(dir_path)


def main():
    parser = argparse.ArgumentParser(description="Cleanup old processing job files")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Show what would be deleted without actually deleting")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete the files (use with caution)")
    parser.add_argument("--backup", action="store_true",
                        help="Create backup before deletion")
    parser.add_argument("--backup-dir", default="backup_old_processing",
                        help="Directory to store backups")
    
    args = parser.parse_args()
    
    if args.execute:
        args.dry_run = False
    
    try:
        project_root = find_project_root()
        print(f"Project root: {project_root}\n")
        
        # Identify files to remove
        dirs_to_remove, files_to_remove = identify_old_files(project_root)
        
        print(f"Found {len(dirs_to_remove)} directories and {len(files_to_remove)} files to remove\n")
        
        if not dirs_to_remove and not files_to_remove:
            print("No old processing job files found. Cleanup may have already been completed.")
            return
        
        # Create backup if requested
        if args.backup and not args.dry_run:
            backup_dir = project_root / args.backup_dir
            print(f"Creating backup in: {backup_dir}\n")
            create_backup(dirs_to_remove + files_to_remove, backup_dir)
            print()
        
        # Remove files
        remove_old_files(dirs_to_remove, files_to_remove, args.dry_run)
        
        if args.dry_run:
            print("\n=== To actually delete these files, run with --execute flag ===")
            print("=== Consider using --backup flag to create a backup first ===")
        else:
            print("\n=== Cleanup completed successfully ===")
            
        # Remind about manual updates
        print("\n=== Manual updates required ===")
        print("Please update the following files to remove references to old jobs:")
        for file_path in FILES_TO_UPDATE:
            print(f"  - {file_path}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    exit(main() or 0) 