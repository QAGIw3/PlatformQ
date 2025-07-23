"""
Dependency Resolver

Resolves pipeline step dependencies.
"""

from typing import Dict, Any, List
from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class DependencyResolver:
    """Resolves pipeline dependencies"""
    
    def __init__(self):
        pass
    
    async def resolve_dependencies(self, steps: List[Dict[str, Any]], 
                                 dependencies: Dict[str, List[str]]) -> Dict[str, Any]:
        """Resolve step dependencies and create execution graph"""
        graph = {
            "steps": steps,
            "dependencies": dependencies,
            "execution_order": self._topological_sort(steps, dependencies)
        }
        
        return graph
    
    def _topological_sort(self, steps: List[Dict[str, Any]], 
                         dependencies: Dict[str, List[str]]) -> List[str]:
        """Perform topological sort on steps"""
        # Simple implementation
        step_names = [step["name"] for step in steps]
        
        # If no dependencies, return steps in order
        if not dependencies:
            return step_names
        
        # Otherwise, try to order based on dependencies
        ordered = []
        remaining = set(step_names)
        
        while remaining:
            # Find steps with no dependencies
            for step in remaining:
                deps = dependencies.get(step, [])
                if all(dep in ordered for dep in deps):
                    ordered.append(step)
                    remaining.remove(step)
                    break
            else:
                # Circular dependency or error
                ordered.extend(remaining)
                break
        
        return ordered 