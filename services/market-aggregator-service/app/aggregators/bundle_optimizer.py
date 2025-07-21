"""
Bundle Optimizer Service
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import numpy as np
from scipy.optimize import linprog
import random

from ..models.aggregation import (
    ResourceBundle, ResourceAllocation, OptimizationRequest,
    OptimizationResult, OptimizationObjective, ResourceType,
    QuantumRequirement, AIRequirement, NetworkRequirement
)
from ..core.market_client import MarketClient
from ..config import settings


logger = logging.getLogger(__name__)


class BundleOptimizer:
    """Optimizes resource bundles for cost and performance"""
    
    def __init__(self, market_client: MarketClient):
        self.market_client = market_client
        
    async def optimize_bundle(
        self,
        request: OptimizationRequest
    ) -> OptimizationResult:
        """Optimize resource allocation for a bundle"""
        start_time = datetime.utcnow()
        
        try:
            # Choose optimization algorithm
            if settings.OPTIMIZATION_ALGORITHM == "genetic":
                result = await self._genetic_algorithm_optimization(request)
            elif settings.OPTIMIZATION_ALGORITHM == "simulated_annealing":
                result = await self._simulated_annealing_optimization(request)
            else:
                result = await self._greedy_optimization(request)
            
            # Calculate optimization time
            optimization_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            result.optimization_time_ms = optimization_time_ms
            
            return result
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            raise
    
    async def _greedy_optimization(
        self,
        request: OptimizationRequest
    ) -> OptimizationResult:
        """Simple greedy optimization algorithm"""
        bundle = request.bundle
        allocations = []
        total_cost = 0
        warnings = []
        
        # Process each requirement
        for req in bundle.requirements:
            if req.resource_type == ResourceType.QUANTUM:
                allocation = await self._allocate_quantum_resource(req, request)
            elif req.resource_type == ResourceType.AI:
                allocation = await self._allocate_ai_resource(req, request)
            elif req.resource_type == ResourceType.NETWORK:
                allocation = await self._allocate_network_resource(req, request)
            else:
                warnings.append(f"Unknown resource type: {req.resource_type}")
                continue
            
            if allocation:
                allocations.append(allocation)
                total_cost += allocation.total_cost
            else:
                warnings.append(f"Failed to allocate {req.resource_type} resource")
        
        # Check budget constraint
        constraints_satisfied = True
        if request.budget_limit and total_cost > request.budget_limit:
            constraints_satisfied = False
            warnings.append(f"Budget exceeded: {total_cost} > {request.budget_limit}")
        
        # Calculate performance score
        performance_score = self._calculate_performance_score(allocations, bundle)
        
        return OptimizationResult(
            request_id=f"opt_{bundle.bundle_id}",
            bundle_id=bundle.bundle_id,
            optimal_allocations=allocations,
            total_cost=total_cost,
            performance_score=performance_score,
            optimization_time_ms=0,  # Will be set by caller
            algorithm_used="greedy",
            iterations_performed=len(bundle.requirements),
            constraints_satisfied=constraints_satisfied,
            warnings=warnings
        )
    
    async def _genetic_algorithm_optimization(
        self,
        request: OptimizationRequest
    ) -> OptimizationResult:
        """Genetic algorithm optimization"""
        bundle = request.bundle
        population_size = 50
        generations = min(settings.OPTIMIZATION_MAX_ITERATIONS, 100)
        
        # Initialize population
        population = await self._initialize_population(bundle, population_size)
        
        best_solution = None
        best_fitness = float('-inf')
        
        for generation in range(generations):
            # Evaluate fitness
            fitness_scores = []
            for solution in population:
                fitness = await self._evaluate_fitness(solution, request)
                fitness_scores.append(fitness)
                
                if fitness > best_fitness:
                    best_fitness = fitness
                    best_solution = solution
            
            # Selection
            selected = self._tournament_selection(population, fitness_scores)
            
            # Crossover and mutation
            new_population = []
            for i in range(0, len(selected), 2):
                if i + 1 < len(selected):
                    offspring1, offspring2 = self._crossover(selected[i], selected[i+1])
                    offspring1 = self._mutate(offspring1)
                    offspring2 = self._mutate(offspring2)
                    new_population.extend([offspring1, offspring2])
            
            population = new_population[:population_size]
        
        # Convert best solution to allocations
        allocations = await self._solution_to_allocations(best_solution, bundle)
        total_cost = sum(a.total_cost for a in allocations)
        performance_score = self._calculate_performance_score(allocations, bundle)
        
        return OptimizationResult(
            request_id=f"opt_{bundle.bundle_id}",
            bundle_id=bundle.bundle_id,
            optimal_allocations=allocations,
            total_cost=total_cost,
            performance_score=performance_score,
            optimization_time_ms=0,
            algorithm_used="genetic",
            iterations_performed=generations * population_size,
            constraints_satisfied=self._check_constraints(allocations, request),
            warnings=[]
        )
    
    async def _simulated_annealing_optimization(
        self,
        request: OptimizationRequest
    ) -> OptimizationResult:
        """Simulated annealing optimization"""
        bundle = request.bundle
        
        # Initialize with greedy solution
        current_solution = await self._greedy_optimization(request)
        best_solution = current_solution
        
        temperature = 1000.0
        cooling_rate = 0.95
        min_temperature = 1.0
        
        iterations = 0
        while temperature > min_temperature and iterations < settings.OPTIMIZATION_MAX_ITERATIONS:
            # Generate neighbor solution
            neighbor = await self._generate_neighbor_solution(current_solution, bundle)
            
            # Calculate energy (negative of objective value)
            current_energy = -self._calculate_objective_value(current_solution, request)
            neighbor_energy = -self._calculate_objective_value(neighbor, request)
            
            # Accept or reject
            delta = neighbor_energy - current_energy
            if delta < 0 or random.random() < np.exp(-delta / temperature):
                current_solution = neighbor
                
                if neighbor_energy < -self._calculate_objective_value(best_solution, request):
                    best_solution = neighbor
            
            temperature *= cooling_rate
            iterations += 1
        
        return OptimizationResult(
            request_id=f"opt_{bundle.bundle_id}",
            bundle_id=bundle.bundle_id,
            optimal_allocations=best_solution.optimal_allocations,
            total_cost=best_solution.total_cost,
            performance_score=best_solution.performance_score,
            optimization_time_ms=0,
            algorithm_used="simulated_annealing",
            iterations_performed=iterations,
            constraints_satisfied=best_solution.constraints_satisfied,
            warnings=best_solution.warnings
        )
    
    async def _allocate_quantum_resource(
        self,
        requirement: QuantumRequirement,
        request: OptimizationRequest
    ) -> Optional[ResourceAllocation]:
        """Allocate quantum resource based on requirement"""
        try:
            # Search for available QPUs
            qpus = await self.market_client.search_quantum_resources(
                min_qubit_count=requirement.min_qubit_count,
                min_coherence_minutes=requirement.min_coherence_minutes
            )
            
            if not qpus:
                return None
            
            # Filter by quality threshold if specified
            if request.quality_thresholds and ResourceType.QUANTUM in request.quality_thresholds:
                threshold = request.quality_thresholds[ResourceType.QUANTUM]
                qpus = [q for q in qpus if q.get('quality_score', 0) >= threshold]
            
            if not qpus:
                return None
            
            # Select best QPU based on objective
            if request.bundle.optimization_objective == OptimizationObjective.MINIMIZE_COST:
                best_qpu = min(qpus, key=lambda x: x['price_per_minute'])
            else:
                best_qpu = max(qpus, key=lambda x: x['quality_score'])
            
            # Calculate allocation details
            duration_minutes = requirement.min_coherence_minutes
            price_per_minute = best_qpu['price_per_minute']
            total_cost = price_per_minute * duration_minutes
            
            return ResourceAllocation(
                resource_type=ResourceType.QUANTUM,
                resource_id=best_qpu['qpu_id'],
                allocation_id=f"alloc_q_{best_qpu['qpu_id']}",
                specifications={
                    'qubit_count': best_qpu['qubit_count'],
                    'coherence_time': best_qpu['coherence_time'],
                    'gate_types': best_qpu.get('gate_types', [])
                },
                price_per_hour=price_per_minute * 60,
                total_cost=total_cost,
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow() + timedelta(minutes=duration_minutes),
                quality_score=best_qpu.get('quality_score')
            )
            
        except Exception as e:
            logger.error(f"Failed to allocate quantum resource: {e}")
            return None
    
    async def _allocate_ai_resource(
        self,
        requirement: AIRequirement,
        request: OptimizationRequest
    ) -> Optional[ResourceAllocation]:
        """Allocate AI accelerator based on requirement"""
        try:
            # Search for available accelerators
            accelerators = await self.market_client.search_ai_accelerators(
                accelerator_type=requirement.accelerator_type,
                min_tflops=requirement.min_tflops
            )
            
            if not accelerators:
                return None
            
            # Filter by quality threshold
            if request.quality_thresholds and ResourceType.AI in request.quality_thresholds:
                threshold = request.quality_thresholds[ResourceType.AI]
                accelerators = [a for a in accelerators if a.get('quality_score', 0) >= threshold]
            
            if not accelerators:
                return None
            
            # Select best accelerator
            if request.bundle.optimization_objective == OptimizationObjective.MINIMIZE_COST:
                best_accelerator = min(accelerators, key=lambda x: x['price_per_hour'])
            else:
                best_accelerator = max(accelerators, key=lambda x: x['performance_tflops'])
            
            # Calculate allocation
            duration_hours = requirement.duration_hours
            price_per_hour = best_accelerator['price_per_hour']
            total_cost = price_per_hour * duration_hours
            
            return ResourceAllocation(
                resource_type=ResourceType.AI,
                resource_id=best_accelerator['accelerator_id'],
                allocation_id=f"alloc_ai_{best_accelerator['accelerator_id']}",
                specifications={
                    'accelerator_type': best_accelerator['type'],
                    'tflops': best_accelerator['performance_tflops'],
                    'memory_gb': best_accelerator.get('memory_gb')
                },
                price_per_hour=price_per_hour,
                total_cost=total_cost,
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow() + timedelta(hours=duration_hours),
                quality_score=best_accelerator.get('quality_score')
            )
            
        except Exception as e:
            logger.error(f"Failed to allocate AI resource: {e}")
            return None
    
    async def _allocate_network_resource(
        self,
        requirement: NetworkRequirement,
        request: OptimizationRequest
    ) -> Optional[ResourceAllocation]:
        """Allocate network bandwidth based on requirement"""
        try:
            # Search for network paths
            paths = await self.market_client.search_network_paths(
                source=requirement.source_node,
                destination=requirement.destination_node,
                min_bandwidth_mbps=requirement.min_bandwidth_mbps,
                max_latency_ms=requirement.max_latency_ms
            )
            
            if not paths:
                return None
            
            # Filter by quality threshold
            if request.quality_thresholds and ResourceType.NETWORK in request.quality_thresholds:
                threshold = request.quality_thresholds[ResourceType.NETWORK]
                paths = [p for p in paths if p.get('quality_score', 0) >= threshold]
            
            if not paths:
                return None
            
            # Select best path
            if request.bundle.optimization_objective == OptimizationObjective.MINIMIZE_LATENCY:
                best_path = min(paths, key=lambda x: x['latency_ms'])
            elif request.bundle.optimization_objective == OptimizationObjective.MINIMIZE_COST:
                best_path = min(paths, key=lambda x: x['price_per_mbps_hour'])
            else:
                # Balance cost and performance
                best_path = min(paths, key=lambda x: x['price_per_mbps_hour'] / x['quality_score'])
            
            # Calculate allocation
            duration_hours = requirement.duration_hours
            bandwidth_mbps = requirement.min_bandwidth_mbps
            price_per_mbps_hour = best_path['price_per_mbps_hour']
            total_cost = price_per_mbps_hour * bandwidth_mbps * duration_hours
            
            return ResourceAllocation(
                resource_type=ResourceType.NETWORK,
                resource_id=best_path['path_id'],
                allocation_id=f"alloc_net_{best_path['path_id']}",
                specifications={
                    'bandwidth_mbps': bandwidth_mbps,
                    'latency_ms': best_path['latency_ms'],
                    'qos_class': requirement.qos_class
                },
                price_per_hour=price_per_mbps_hour * bandwidth_mbps,
                total_cost=total_cost,
                start_time=datetime.utcnow(),
                end_time=datetime.utcnow() + timedelta(hours=duration_hours),
                quality_score=best_path.get('quality_score')
            )
            
        except Exception as e:
            logger.error(f"Failed to allocate network resource: {e}")
            return None
    
    def _calculate_performance_score(
        self,
        allocations: List[ResourceAllocation],
        bundle: ResourceBundle
    ) -> float:
        """Calculate overall performance score for allocations"""
        if not allocations:
            return 0.0
        
        # Weight by resource importance (priority)
        total_weight = sum(req.priority for req in bundle.requirements)
        
        weighted_score = 0.0
        for i, allocation in enumerate(allocations):
            if i < len(bundle.requirements):
                req = bundle.requirements[i]
                quality = allocation.quality_score or 80.0  # Default if not provided
                weighted_score += (quality * req.priority) / total_weight
        
        return weighted_score
    
    def _calculate_objective_value(
        self,
        result: OptimizationResult,
        request: OptimizationRequest
    ) -> float:
        """Calculate objective function value"""
        objective = request.bundle.optimization_objective
        
        if objective == OptimizationObjective.MINIMIZE_COST:
            return -result.total_cost  # Negative for minimization
        elif objective == OptimizationObjective.MAXIMIZE_PERFORMANCE:
            return result.performance_score
        elif objective == OptimizationObjective.MINIMIZE_LATENCY:
            # Average latency across network resources
            latencies = []
            for alloc in result.optimal_allocations:
                if alloc.resource_type == ResourceType.NETWORK:
                    latencies.append(alloc.specifications.get('latency_ms', 0))
            return -np.mean(latencies) if latencies else 0
        else:  # BALANCE_COST_PERFORMANCE
            # Weighted combination
            cost_weight = 0.5
            perf_weight = 0.5
            normalized_cost = result.total_cost / (request.budget_limit or 10000)
            normalized_perf = result.performance_score / 100
            return perf_weight * normalized_perf - cost_weight * normalized_cost
    
    def _check_constraints(
        self,
        allocations: List[ResourceAllocation],
        request: OptimizationRequest
    ) -> bool:
        """Check if all constraints are satisfied"""
        # Budget constraint
        if request.budget_limit:
            total_cost = sum(a.total_cost for a in allocations)
            if total_cost > request.budget_limit:
                return False
        
        # Quality constraints
        if request.quality_thresholds:
            for alloc in allocations:
                threshold = request.quality_thresholds.get(alloc.resource_type)
                if threshold and (alloc.quality_score or 0) < threshold:
                    return False
        
        # Time constraints
        if request.time_constraints:
            for alloc in allocations:
                if alloc.start_time < request.time_constraints.get('earliest_start', datetime.min):
                    return False
                if alloc.end_time > request.time_constraints.get('latest_end', datetime.max):
                    return False
        
        return True
    
    # Genetic algorithm helper methods
    async def _initialize_population(
        self,
        bundle: ResourceBundle,
        size: int
    ) -> List[Dict]:
        """Initialize population for genetic algorithm"""
        population = []
        
        for _ in range(size):
            solution = {}
            for i, req in enumerate(bundle.requirements):
                # Random selection from available resources
                if req.resource_type == ResourceType.QUANTUM:
                    resources = await self.market_client.search_quantum_resources(
                        min_qubit_count=req.specifications.get('min_qubit_count', 1)
                    )
                elif req.resource_type == ResourceType.AI:
                    resources = await self.market_client.search_ai_accelerators(
                        accelerator_type=req.specifications.get('accelerator_type', 'GPU')
                    )
                else:  # NETWORK
                    resources = await self.market_client.search_network_paths(
                        source=req.specifications.get('source_node', ''),
                        destination=req.specifications.get('destination_node', '')
                    )
                
                if resources:
                    solution[i] = random.choice(resources)
            
            population.append(solution)
        
        return population
    
    async def _evaluate_fitness(
        self,
        solution: Dict,
        request: OptimizationRequest
    ) -> float:
        """Evaluate fitness of a solution"""
        # Convert solution to allocations
        allocations = await self._solution_to_allocations(solution, request.bundle)
        
        # Create temporary result
        temp_result = OptimizationResult(
            request_id="temp",
            bundle_id=request.bundle.bundle_id,
            optimal_allocations=allocations,
            total_cost=sum(a.total_cost for a in allocations),
            performance_score=self._calculate_performance_score(allocations, request.bundle),
            optimization_time_ms=0,
            algorithm_used="genetic",
            iterations_performed=0,
            constraints_satisfied=self._check_constraints(allocations, request),
            warnings=[]
        )
        
        # Calculate fitness
        fitness = self._calculate_objective_value(temp_result, request)
        
        # Penalty for constraint violations
        if not temp_result.constraints_satisfied:
            fitness -= 1000
        
        return fitness
    
    def _tournament_selection(
        self,
        population: List[Dict],
        fitness_scores: List[float],
        tournament_size: int = 3
    ) -> List[Dict]:
        """Tournament selection for genetic algorithm"""
        selected = []
        
        for _ in range(len(population)):
            # Random tournament
            tournament_indices = random.sample(range(len(population)), tournament_size)
            tournament_fitness = [fitness_scores[i] for i in tournament_indices]
            
            # Select winner
            winner_idx = tournament_indices[np.argmax(tournament_fitness)]
            selected.append(population[winner_idx].copy())
        
        return selected
    
    def _crossover(self, parent1: Dict, parent2: Dict) -> Tuple[Dict, Dict]:
        """Crossover operation for genetic algorithm"""
        offspring1 = {}
        offspring2 = {}
        
        for key in parent1.keys():
            if random.random() < 0.5:
                offspring1[key] = parent1[key]
                offspring2[key] = parent2.get(key, parent1[key])
            else:
                offspring1[key] = parent2.get(key, parent1[key])
                offspring2[key] = parent1[key]
        
        return offspring1, offspring2
    
    def _mutate(self, solution: Dict, mutation_rate: float = 0.1) -> Dict:
        """Mutation operation for genetic algorithm"""
        mutated = solution.copy()
        
        for key in mutated.keys():
            if random.random() < mutation_rate:
                # Mutation would re-select from available resources
                # For now, just mark for re-selection
                mutated[key] = None
        
        return mutated
    
    async def _solution_to_allocations(
        self,
        solution: Dict,
        bundle: ResourceBundle
    ) -> List[ResourceAllocation]:
        """Convert solution dictionary to resource allocations"""
        allocations = []
        
        for i, req in enumerate(bundle.requirements):
            if i in solution and solution[i]:
                resource = solution[i]
                
                # Create allocation based on resource type
                if req.resource_type == ResourceType.QUANTUM:
                    allocation = ResourceAllocation(
                        resource_type=ResourceType.QUANTUM,
                        resource_id=resource['qpu_id'],
                        allocation_id=f"alloc_q_{resource['qpu_id']}",
                        specifications=resource,
                        price_per_hour=resource.get('price_per_minute', 1) * 60,
                        total_cost=resource.get('price_per_minute', 1) * req.specifications.get('min_coherence_minutes', 10),
                        start_time=datetime.utcnow(),
                        end_time=datetime.utcnow() + timedelta(minutes=req.specifications.get('min_coherence_minutes', 10)),
                        quality_score=resource.get('quality_score', 80)
                    )
                elif req.resource_type == ResourceType.AI:
                    duration_hours = req.specifications.get('duration_hours', 1)
                    allocation = ResourceAllocation(
                        resource_type=ResourceType.AI,
                        resource_id=resource['accelerator_id'],
                        allocation_id=f"alloc_ai_{resource['accelerator_id']}",
                        specifications=resource,
                        price_per_hour=resource.get('price_per_hour', 10),
                        total_cost=resource.get('price_per_hour', 10) * duration_hours,
                        start_time=datetime.utcnow(),
                        end_time=datetime.utcnow() + timedelta(hours=duration_hours),
                        quality_score=resource.get('quality_score', 80)
                    )
                else:  # NETWORK
                    duration_hours = req.specifications.get('duration_hours', 1)
                    bandwidth_mbps = req.specifications.get('min_bandwidth_mbps', 100)
                    price_per_mbps_hour = resource.get('price_per_mbps_hour', 0.01)
                    allocation = ResourceAllocation(
                        resource_type=ResourceType.NETWORK,
                        resource_id=resource['path_id'],
                        allocation_id=f"alloc_net_{resource['path_id']}",
                        specifications=resource,
                        price_per_hour=price_per_mbps_hour * bandwidth_mbps,
                        total_cost=price_per_mbps_hour * bandwidth_mbps * duration_hours,
                        start_time=datetime.utcnow(),
                        end_time=datetime.utcnow() + timedelta(hours=duration_hours),
                        quality_score=resource.get('quality_score', 80)
                    )
                
                allocations.append(allocation)
        
        return allocations
    
    async def _generate_neighbor_solution(
        self,
        current: OptimizationResult,
        bundle: ResourceBundle
    ) -> OptimizationResult:
        """Generate neighbor solution for simulated annealing"""
        # Copy current solution
        new_allocations = current.optimal_allocations.copy()
        
        # Randomly modify one allocation
        if new_allocations:
            idx = random.randint(0, len(new_allocations) - 1)
            req = bundle.requirements[idx] if idx < len(bundle.requirements) else bundle.requirements[0]
            
            # Re-allocate this resource
            if req.resource_type == ResourceType.QUANTUM:
                new_alloc = await self._allocate_quantum_resource(req, OptimizationRequest(bundle=bundle))
            elif req.resource_type == ResourceType.AI:
                new_alloc = await self._allocate_ai_resource(req, OptimizationRequest(bundle=bundle))
            else:
                new_alloc = await self._allocate_network_resource(req, OptimizationRequest(bundle=bundle))
            
            if new_alloc:
                new_allocations[idx] = new_alloc
        
        # Create new result
        total_cost = sum(a.total_cost for a in new_allocations)
        performance_score = self._calculate_performance_score(new_allocations, bundle)
        
        return OptimizationResult(
            request_id=current.request_id,
            bundle_id=current.bundle_id,
            optimal_allocations=new_allocations,
            total_cost=total_cost,
            performance_score=performance_score,
            optimization_time_ms=0,
            algorithm_used=current.algorithm_used,
            iterations_performed=current.iterations_performed,
            constraints_satisfied=True,  # Will be checked separately
            warnings=[]
        ) 