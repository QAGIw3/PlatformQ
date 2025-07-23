#!/usr/bin/env python3
"""
Service Analysis Tool for DataIntelligenceSuite

Analyzes existing services to identify:
- Redundant code and functionality
- Common patterns that can be extracted
- Consolidation opportunities
- Dependency relationships
"""

import ast
import asyncio
import json
import logging
from collections import defaultdict, Counter
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import networkx as nx
import matplotlib.pyplot as plt
from dataclasses import dataclass, field
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ServiceInfo:
    """Information about a service"""
    name: str
    path: Path
    dependencies: Set[str] = field(default_factory=set)
    imports: Set[str] = field(default_factory=set)
    classes: List[str] = field(default_factory=list)
    functions: List[str] = field(default_factory=list)
    api_endpoints: List[str] = field(default_factory=list)
    db_models: List[str] = field(default_factory=list)
    
    
@dataclass
class CodePattern:
    """Represents a code pattern found across services"""
    pattern_type: str  # 'class', 'function', 'import', 'api'
    pattern_name: str
    occurrences: List[Tuple[str, str]] = field(default_factory=list)  # (service, file)
    similarity_score: float = 0.0
    

class ServiceAnalyzer:
    """Analyzes services for consolidation opportunities"""
    
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir
        self.services: Dict[str, ServiceInfo] = {}
        self.patterns: List[CodePattern] = []
        self.dependency_graph = nx.DiGraph()
        
    async def analyze_all(self):
        """Run complete analysis"""
        logger.info("Starting service analysis...")
        
        # Discover services
        await self._discover_services()
        
        # Analyze each service
        for service_name, service_info in self.services.items():
            await self._analyze_service(service_info)
            
        # Find common patterns
        await self._find_common_patterns()
        
        # Analyze dependencies
        await self._analyze_dependencies()
        
        # Generate report
        await self._generate_report()
        
    async def _discover_services(self):
        """Discover all services in DataIntelligenceSuite"""
        services_dir = self.root_dir / "services" / "DataIntelligenceSuite"
        
        if not services_dir.exists():
            logger.error(f"Services directory not found: {services_dir}")
            return
            
        for service_path in services_dir.iterdir():
            if service_path.is_dir() and not service_path.name.startswith('.'):
                # Check if it's a service (has app directory or main.py)
                if (service_path / "app").exists() or (service_path / "main.py").exists():
                    self.services[service_path.name] = ServiceInfo(
                        name=service_path.name,
                        path=service_path
                    )
                    
        logger.info(f"Discovered {len(self.services)} services")
        
    async def _analyze_service(self, service: ServiceInfo):
        """Analyze a single service"""
        logger.info(f"Analyzing service: {service.name}")
        
        # Analyze Python files
        for py_file in service.path.rglob("*.py"):
            if "test" not in py_file.parts and "__pycache__" not in py_file.parts:
                await self._analyze_python_file(py_file, service)
                
        # Analyze requirements
        await self._analyze_requirements(service)
        
        # Analyze API endpoints
        await self._analyze_api_endpoints(service)
        
    async def _analyze_python_file(self, file_path: Path, service: ServiceInfo):
        """Analyze a Python file"""
        try:
            content = file_path.read_text()
            tree = ast.parse(content)
            
            # Extract imports
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        service.imports.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        service.imports.add(node.module)
                        
                # Extract classes
                elif isinstance(node, ast.ClassDef):
                    service.classes.append(node.name)
                    
                    # Check for DB models
                    if self._is_db_model(node):
                        service.db_models.append(node.name)
                        
                # Extract functions
                elif isinstance(node, ast.FunctionDef):
                    service.functions.append(node.name)
                    
        except Exception as e:
            logger.error(f"Error analyzing {file_path}: {e}")
            
    def _is_db_model(self, class_node: ast.ClassDef) -> bool:
        """Check if a class is a database model"""
        # Check base classes
        for base in class_node.bases:
            if isinstance(base, ast.Name):
                if base.id in ["Model", "Base", "Document"]:
                    return True
            elif isinstance(base, ast.Attribute):
                if base.attr in ["Model", "Base", "Document"]:
                    return True
        return False
        
    async def _analyze_requirements(self, service: ServiceInfo):
        """Analyze service requirements"""
        req_files = ["requirements.txt", "requirements.in", "pyproject.toml"]
        
        for req_file in req_files:
            file_path = service.path / req_file
            if file_path.exists():
                content = file_path.read_text()
                
                # Extract package names
                if req_file.endswith(".txt") or req_file.endswith(".in"):
                    for line in content.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            # Extract package name
                            match = re.match(r'^([a-zA-Z0-9\-_]+)', line)
                            if match:
                                service.dependencies.add(match.group(1))
                                
    async def _analyze_api_endpoints(self, service: ServiceInfo):
        """Analyze API endpoints"""
        # Look for FastAPI/Flask route definitions
        for py_file in service.path.rglob("*.py"):
            if "api" in py_file.parts or "routes" in py_file.parts:
                try:
                    content = py_file.read_text()
                    
                    # FastAPI patterns
                    fastapi_patterns = [
                        r'@app\.(get|post|put|delete|patch)\("([^"]+)"',
                        r'@router\.(get|post|put|delete|patch)\("([^"]+)"'
                    ]
                    
                    for pattern in fastapi_patterns:
                        matches = re.findall(pattern, content)
                        for method, endpoint in matches:
                            service.api_endpoints.append(f"{method.upper()} {endpoint}")
                            
                except Exception as e:
                    logger.error(f"Error analyzing API endpoints in {py_file}: {e}")
                    
    async def _find_common_patterns(self):
        """Find common patterns across services"""
        # Find common imports
        import_counter = Counter()
        for service in self.services.values():
            for imp in service.imports:
                if not imp.startswith('.'):  # Skip relative imports
                    import_counter[imp] += 1
                    
        # Find imports used by multiple services
        common_imports = {
            imp: count for imp, count in import_counter.items()
            if count > 1 and not imp.startswith('platformq')
        }
        
        # Find similar class names
        class_counter = Counter()
        class_locations = defaultdict(list)
        
        for service in self.services.values():
            for class_name in service.classes:
                class_counter[class_name] += 1
                class_locations[class_name].append(service.name)
                
        # Find duplicate classes
        for class_name, count in class_counter.items():
            if count > 1:
                pattern = CodePattern(
                    pattern_type="class",
                    pattern_name=class_name,
                    occurrences=[(svc, "") for svc in class_locations[class_name]],
                    similarity_score=1.0
                )
                self.patterns.append(pattern)
                
        # Find similar API endpoints
        endpoint_counter = Counter()
        endpoint_locations = defaultdict(list)
        
        for service in self.services.values():
            for endpoint in service.api_endpoints:
                # Normalize endpoint
                normalized = re.sub(r'\{[^}]+\}', '{id}', endpoint)
                endpoint_counter[normalized] += 1
                endpoint_locations[normalized].append(service.name)
                
        # Find duplicate endpoints
        for endpoint, count in endpoint_counter.items():
            if count > 1:
                pattern = CodePattern(
                    pattern_type="api",
                    pattern_name=endpoint,
                    occurrences=[(svc, "") for svc in endpoint_locations[endpoint]],
                    similarity_score=1.0
                )
                self.patterns.append(pattern)
                
        logger.info(f"Found {len(self.patterns)} common patterns")
        
    async def _analyze_dependencies(self):
        """Analyze service dependencies"""
        # Build dependency graph
        for service_name, service in self.services.items():
            self.dependency_graph.add_node(service_name)
            
            # Check imports for internal dependencies
            for imp in service.imports:
                for other_service in self.services:
                    if other_service != service_name:
                        # Check if import references another service
                        if (other_service.replace('-', '_') in imp or
                            f"services.{other_service}" in imp):
                            self.dependency_graph.add_edge(service_name, other_service)
                            
    async def _generate_report(self):
        """Generate analysis report"""
        report = {
            "summary": {
                "total_services": len(self.services),
                "total_classes": sum(len(s.classes) for s in self.services.values()),
                "total_functions": sum(len(s.functions) for s in self.services.values()),
                "total_api_endpoints": sum(len(s.api_endpoints) for s in self.services.values()),
                "total_db_models": sum(len(s.db_models) for s in self.services.values())
            },
            "services": {},
            "common_patterns": [],
            "consolidation_opportunities": [],
            "dependency_analysis": {}
        }
        
        # Service details
        for service_name, service in self.services.items():
            report["services"][service_name] = {
                "classes": len(service.classes),
                "functions": len(service.functions),
                "api_endpoints": len(service.api_endpoints),
                "db_models": len(service.db_models),
                "dependencies": list(service.dependencies),
                "top_imports": Counter(service.imports).most_common(10)
            }
            
        # Common patterns
        pattern_summary = defaultdict(list)
        for pattern in self.patterns:
            pattern_summary[pattern.pattern_type].append({
                "name": pattern.pattern_name,
                "occurrences": len(pattern.occurrences),
                "services": [occ[0] for occ in pattern.occurrences]
            })
            
        report["common_patterns"] = dict(pattern_summary)
        
        # Consolidation opportunities
        report["consolidation_opportunities"] = self._identify_consolidation_opportunities()
        
        # Dependency analysis
        report["dependency_analysis"] = {
            "strongly_connected_components": list(nx.strongly_connected_components(self.dependency_graph)),
            "circular_dependencies": list(nx.simple_cycles(self.dependency_graph)),
            "dependency_count": {
                node: self.dependency_graph.in_degree(node)
                for node in self.dependency_graph.nodes()
            }
        }
        
        # Save report
        report_path = self.root_dir / "service_analysis_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
            
        logger.info(f"Analysis report saved to {report_path}")
        
        # Generate visualizations
        await self._generate_visualizations()
        
    def _identify_consolidation_opportunities(self) -> List[Dict]:
        """Identify opportunities for service consolidation"""
        opportunities = []
        
        # Group services by similar functionality
        service_groups = {
            "data_processing": ["batch-processing-service", "stream-processing-service", "data-ingestion-service"],
            "analytics": ["analytics-service", "neuromorphic-computing-service", "quantum-optimization-service"],
            "ml_platform": ["unified-ml-platform-service"],
            "data_quality": ["unified-quality-service", "data-catalog-hub"],
            "orchestration": ["unified-orchestration-service"],
            "integration": ["graphql-gateway", "unified-graph-service"]
        }
        
        for group_name, services in service_groups.items():
            existing_services = [s for s in services if s in self.services]
            if len(existing_services) > 1:
                # Calculate overlap
                common_deps = set()
                all_deps = set()
                
                for service in existing_services:
                    service_deps = self.services[service].dependencies
                    if not common_deps:
                        common_deps = service_deps.copy()
                    else:
                        common_deps &= service_deps
                    all_deps |= service_deps
                    
                overlap_ratio = len(common_deps) / len(all_deps) if all_deps else 0
                
                opportunities.append({
                    "group": group_name,
                    "services": existing_services,
                    "common_dependencies": list(common_deps),
                    "overlap_ratio": overlap_ratio,
                    "recommendation": f"Consolidate into {group_name.replace('_', '-')}-service"
                })
                
        return opportunities
        
    async def _generate_visualizations(self):
        """Generate visualization graphs"""
        # Service dependency graph
        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(self.dependency_graph)
        nx.draw(self.dependency_graph, pos, with_labels=True, node_color='lightblue',
                node_size=3000, font_size=8, font_weight='bold', arrows=True)
        plt.title("Service Dependency Graph")
        plt.savefig(self.root_dir / "service_dependencies.png")
        plt.close()
        
        # Service size comparison
        service_sizes = {
            name: len(service.classes) + len(service.functions)
            for name, service in self.services.items()
        }
        
        plt.figure(figsize=(12, 6))
        plt.bar(service_sizes.keys(), service_sizes.values())
        plt.xticks(rotation=45, ha='right')
        plt.title("Service Complexity (Classes + Functions)")
        plt.tight_layout()
        plt.savefig(self.root_dir / "service_complexity.png")
        plt.close()
        
        logger.info("Visualizations saved")
        

async def main():
    """Main analysis function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze DataIntelligenceSuite services")
    parser.add_argument(
        "--root-dir",
        type=Path,
        default=Path.cwd(),
        help="Root directory of the platform"
    )
    
    args = parser.parse_args()
    
    analyzer = ServiceAnalyzer(args.root_dir)
    await analyzer.analyze_all()
    

if __name__ == "__main__":
    asyncio.run(main()) 