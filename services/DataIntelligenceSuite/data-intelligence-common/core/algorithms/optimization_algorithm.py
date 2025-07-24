"""
Optimization Algorithm Implementation

Provides base class for optimization algorithms with support for various methods.
"""

from typing import Any, Dict, List, Optional, Union, Callable, TypeVar, Generic, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from datetime import datetime

from .base_algorithm import BaseAlgorithm, AlgorithmConfig, AlgorithmResult, AlgorithmType
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')  # Solution type
F = TypeVar('F')  # Fitness/objective value type


class OptimizationMethod(str, Enum):
    """Optimization methods"""
    GRADIENT_DESCENT = "gradient_descent"
    STOCHASTIC_GRADIENT_DESCENT = "sgd"
    ADAM = "adam"
    GENETIC_ALGORITHM = "genetic"
    SIMULATED_ANNEALING = "simulated_annealing"
    PARTICLE_SWARM = "particle_swarm"
    DIFFERENTIAL_EVOLUTION = "differential_evolution"
    NELDER_MEAD = "nelder_mead"
    BFGS = "bfgs"
    CUSTOM = "custom"


class ConstraintType(str, Enum):
    """Types of constraints"""
    EQUALITY = "equality"
    INEQUALITY = "inequality"
    BOUNDS = "bounds"


class OptimizationDirection(str, Enum):
    """Direction of optimization"""
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"


@dataclass
class Constraint:
    """Optimization constraint"""
    name: str
    type: ConstraintType
    function: Callable[[T], float]
    tolerance: float = 1e-6
    
    # For bounds constraints
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None


@dataclass
class OptimizationConfig(AlgorithmConfig):
    """Configuration for optimization algorithms"""
    method: OptimizationMethod = OptimizationMethod.GRADIENT_DESCENT
    direction: OptimizationDirection = OptimizationDirection.MINIMIZE
    
    # Convergence criteria
    tolerance: float = 1e-6
    max_iterations: int = 1000
    max_evaluations: Optional[int] = None
    
    # Method-specific parameters
    learning_rate: float = 0.01
    momentum: float = 0.9
    beta1: float = 0.9  # For Adam
    beta2: float = 0.999  # For Adam
    epsilon: float = 1e-8  # For Adam
    
    # Population-based methods
    population_size: int = 50
    crossover_rate: float = 0.8
    mutation_rate: float = 0.1
    
    # Constraints
    constraints: List[Constraint] = field(default_factory=list)
    penalty_factor: float = 1000.0
    
    # Search space
    bounds: Optional[List[Tuple[float, float]]] = None
    initial_guess: Optional[Any] = None
    
    # Advanced options
    enable_line_search: bool = False
    enable_adaptive_learning: bool = False
    enable_early_stopping: bool = True
    patience: int = 10
    
    def __post_init__(self):
        self.type = AlgorithmType.OPTIMIZATION


@dataclass
class OptimizationState:
    """Current state of optimization"""
    iteration: int = 0
    current_solution: Optional[T] = None
    current_value: Optional[F] = None
    best_solution: Optional[T] = None
    best_value: Optional[F] = None
    gradient: Optional[np.ndarray] = None
    
    # History
    solution_history: List[T] = field(default_factory=list)
    value_history: List[F] = field(default_factory=list)
    gradient_history: List[np.ndarray] = field(default_factory=list)
    
    # Convergence tracking
    improvement: float = float('inf')
    no_improvement_count: int = 0
    converged: bool = False


@dataclass
class OptimizationResult(AlgorithmResult[T]):
    """Result from optimization algorithm"""
    optimal_solution: Optional[T] = None
    optimal_value: Optional[F] = None
    
    # Convergence info
    converged: bool = False
    convergence_reason: Optional[str] = None
    final_gradient_norm: Optional[float] = None
    
    # Performance metrics
    num_evaluations: int = 0
    num_gradient_evaluations: int = 0
    
    # History
    solution_history: List[T] = field(default_factory=list)
    value_history: List[F] = field(default_factory=list)
    
    # Constraint satisfaction
    constraint_violations: Dict[str, float] = field(default_factory=dict)
    feasible: bool = True


class OptimizationAlgorithm(BaseAlgorithm[T, T], Generic[T, F]):
    """
    Base class for optimization algorithms.
    
    Provides:
    - Various optimization methods
    - Constraint handling
    - Convergence monitoring
    - Solution history tracking
    - Adaptive parameter tuning
    """
    
    def __init__(self, config: OptimizationConfig, **kwargs):
        super().__init__(config, **kwargs)
        self.config: OptimizationConfig = config
        self._state = OptimizationState()
        self._eval_count = 0
        self._grad_eval_count = 0
        
    async def _execute_algorithm(self, initial_solution: T, **kwargs) -> T:
        """Execute optimization algorithm"""
        # Initialize state
        self._state = OptimizationState(
            current_solution=initial_solution or self.config.initial_guess
        )
        
        # Evaluate initial solution
        self._state.current_value = await self.evaluate_objective(self._state.current_solution)
        self._state.best_solution = self._state.current_solution
        self._state.best_value = self._state.current_value
        
        logger.info(f"Starting optimization with {self.config.method.value} method")
        logger.info(f"Initial objective value: {self._state.current_value}")
        
        # Run optimization based on method
        if self.config.method == OptimizationMethod.GRADIENT_DESCENT:
            result = await self._gradient_descent()
        elif self.config.method == OptimizationMethod.ADAM:
            result = await self._adam_optimizer()
        elif self.config.method == OptimizationMethod.GENETIC_ALGORITHM:
            result = await self._genetic_algorithm()
        elif self.config.method == OptimizationMethod.SIMULATED_ANNEALING:
            result = await self._simulated_annealing()
        else:
            result = await self._custom_optimization()
        
        return result
    
    async def _gradient_descent(self) -> T:
        """Gradient descent optimization"""
        momentum = np.zeros_like(self._to_array(self._state.current_solution))
        
        while not self._state.converged and self._state.iteration < self.config.max_iterations:
            # Compute gradient
            gradient = await self.compute_gradient(self._state.current_solution)
            self._state.gradient = gradient
            
            # Update with momentum
            momentum = self.config.momentum * momentum - self.config.learning_rate * gradient
            
            # Update solution
            current_array = self._to_array(self._state.current_solution)
            new_array = current_array + momentum
            
            # Apply bounds if specified
            if self.config.bounds:
                new_array = self._apply_bounds(new_array)
            
            # Convert back to solution type
            new_solution = self._from_array(new_array)
            
            # Evaluate new solution
            new_value = await self.evaluate_objective(new_solution)
            
            # Update state
            await self._update_state(new_solution, new_value)
            
            # Check convergence
            self._check_convergence()
            
            self._state.iteration += 1
            self._increment_iterations()
        
        return self._state.best_solution
    
    async def _adam_optimizer(self) -> T:
        """Adam optimizer"""
        # Initialize Adam parameters
        m = np.zeros_like(self._to_array(self._state.current_solution))  # First moment
        v = np.zeros_like(m)  # Second moment
        
        while not self._state.converged and self._state.iteration < self.config.max_iterations:
            # Compute gradient
            gradient = await self.compute_gradient(self._state.current_solution)
            self._state.gradient = gradient
            
            # Update biased first moment estimate
            m = self.config.beta1 * m + (1 - self.config.beta1) * gradient
            
            # Update biased second raw moment estimate
            v = self.config.beta2 * v + (1 - self.config.beta2) * gradient**2
            
            # Compute bias-corrected first moment estimate
            m_hat = m / (1 - self.config.beta1**(self._state.iteration + 1))
            
            # Compute bias-corrected second raw moment estimate
            v_hat = v / (1 - self.config.beta2**(self._state.iteration + 1))
            
            # Update parameters
            current_array = self._to_array(self._state.current_solution)
            new_array = current_array - self.config.learning_rate * m_hat / (np.sqrt(v_hat) + self.config.epsilon)
            
            # Apply bounds if specified
            if self.config.bounds:
                new_array = self._apply_bounds(new_array)
            
            # Convert back to solution type
            new_solution = self._from_array(new_array)
            
            # Evaluate new solution
            new_value = await self.evaluate_objective(new_solution)
            
            # Update state
            await self._update_state(new_solution, new_value)
            
            # Check convergence
            self._check_convergence()
            
            self._state.iteration += 1
            self._increment_iterations()
        
        return self._state.best_solution
    
    async def _genetic_algorithm(self) -> T:
        """Genetic algorithm optimization"""
        # Initialize population
        population = await self._initialize_population()
        fitness_values = []
        
        for individual in population:
            fitness = await self.evaluate_objective(individual)
            fitness_values.append(fitness)
        
        while not self._state.converged and self._state.iteration < self.config.max_iterations:
            # Selection
            parents = self._selection(population, fitness_values)
            
            # Crossover
            offspring = []
            for i in range(0, len(parents), 2):
                if i + 1 < len(parents):
                    child1, child2 = self._crossover(parents[i], parents[i + 1])
                    offspring.extend([child1, child2])
            
            # Mutation
            for i in range(len(offspring)):
                if np.random.random() < self.config.mutation_rate:
                    offspring[i] = self._mutate(offspring[i])
            
            # Evaluate offspring
            offspring_fitness = []
            for individual in offspring:
                fitness = await self.evaluate_objective(individual)
                offspring_fitness.append(fitness)
            
            # Create new population
            all_individuals = population + offspring
            all_fitness = fitness_values + offspring_fitness
            
            # Select best individuals
            sorted_indices = np.argsort(all_fitness)
            if self.config.direction == OptimizationDirection.MAXIMIZE:
                sorted_indices = sorted_indices[::-1]
            
            population = [all_individuals[i] for i in sorted_indices[:self.config.population_size]]
            fitness_values = [all_fitness[i] for i in sorted_indices[:self.config.population_size]]
            
            # Update best solution
            best_idx = 0
            await self._update_state(population[best_idx], fitness_values[best_idx])
            
            # Check convergence
            self._check_convergence()
            
            self._state.iteration += 1
            self._increment_iterations()
        
        return self._state.best_solution
    
    async def _simulated_annealing(self) -> T:
        """Simulated annealing optimization"""
        temperature = 1.0
        cooling_rate = 0.95
        
        current_solution = self._state.current_solution
        current_value = self._state.current_value
        
        while not self._state.converged and self._state.iteration < self.config.max_iterations:
            # Generate neighbor solution
            neighbor = self._generate_neighbor(current_solution)
            
            # Evaluate neighbor
            neighbor_value = await self.evaluate_objective(neighbor)
            
            # Calculate acceptance probability
            if self.config.direction == OptimizationDirection.MINIMIZE:
                delta = neighbor_value - current_value
            else:
                delta = current_value - neighbor_value
            
            if delta < 0 or np.random.random() < np.exp(-delta / temperature):
                current_solution = neighbor
                current_value = neighbor_value
            
            # Update best solution
            await self._update_state(current_solution, current_value)
            
            # Cool down
            temperature *= cooling_rate
            
            # Check convergence
            self._check_convergence()
            
            self._state.iteration += 1
            self._increment_iterations()
        
        return self._state.best_solution
    
    async def _custom_optimization(self) -> T:
        """Custom optimization method - to be implemented by subclasses"""
        raise NotImplementedError("Custom optimization method must be implemented")
    
    async def _update_state(self, solution: T, value: F):
        """Update optimization state"""
        self._state.current_solution = solution
        self._state.current_value = value
        
        # Track history
        self._state.solution_history.append(solution)
        self._state.value_history.append(value)
        
        # Update best solution
        if self._is_better(value, self._state.best_value):
            self._state.improvement = abs(value - self._state.best_value)
            self._state.best_solution = solution
            self._state.best_value = value
            self._state.no_improvement_count = 0
        else:
            self._state.no_improvement_count += 1
    
    def _is_better(self, value1: F, value2: Optional[F]) -> bool:
        """Check if value1 is better than value2"""
        if value2 is None:
            return True
        
        if self.config.direction == OptimizationDirection.MINIMIZE:
            return value1 < value2
        else:
            return value1 > value2
    
    def _check_convergence(self):
        """Check convergence criteria"""
        # Check improvement threshold
        if self._state.improvement < self.config.tolerance:
            self._state.converged = True
            self._state.convergence_reason = "Improvement below tolerance"
        
        # Check gradient norm (if available)
        if self._state.gradient is not None:
            grad_norm = np.linalg.norm(self._state.gradient)
            if grad_norm < self.config.tolerance:
                self._state.converged = True
                self._state.convergence_reason = "Gradient norm below tolerance"
        
        # Check early stopping
        if self.config.enable_early_stopping:
            if self._state.no_improvement_count >= self.config.patience:
                self._state.converged = True
                self._state.convergence_reason = "Early stopping - no improvement"
    
    def _apply_bounds(self, array: np.ndarray) -> np.ndarray:
        """Apply bounds constraints to solution"""
        if self.config.bounds:
            for i, (lower, upper) in enumerate(self.config.bounds):
                if i < len(array):
                    array[i] = np.clip(array[i], lower, upper)
        return array
    
    # Abstract methods to be implemented by subclasses
    
    async def evaluate_objective(self, solution: T) -> F:
        """
        Evaluate objective function.
        
        Args:
            solution: Solution to evaluate
            
        Returns:
            Objective value
        """
        self._eval_count += 1
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement evaluate_objective method"
        )
    
    async def compute_gradient(self, solution: T) -> np.ndarray:
        """
        Compute gradient of objective function.
        
        Args:
            solution: Solution at which to compute gradient
            
        Returns:
            Gradient vector
        """
        self._grad_eval_count += 1
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement compute_gradient method"
        )
    
    def _to_array(self, solution: T) -> np.ndarray:
        """Convert solution to numpy array"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _to_array method"
        )
    
    def _from_array(self, array: np.ndarray) -> T:
        """Convert numpy array to solution"""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _from_array method"
        )
    
    # Population-based method helpers
    
    async def _initialize_population(self) -> List[T]:
        """Initialize population for population-based methods"""
        population = []
        
        # Add initial guess if provided
        if self.config.initial_guess:
            population.append(self.config.initial_guess)
        
        # Generate random individuals
        while len(population) < self.config.population_size:
            individual = self._generate_random_solution()
            population.append(individual)
        
        return population
    
    def _generate_random_solution(self) -> T:
        """Generate random solution within bounds"""
        if self.config.bounds:
            array = np.array([
                np.random.uniform(low, high) 
                for low, high in self.config.bounds
            ])
            return self._from_array(array)
        else:
            # Default implementation - override in subclass
            raise NotImplementedError("Random solution generation must be implemented")
    
    def _selection(self, population: List[T], fitness_values: List[F]) -> List[T]:
        """Tournament selection"""
        selected = []
        tournament_size = 3
        
        for _ in range(len(population)):
            # Random tournament
            indices = np.random.choice(len(population), tournament_size, replace=False)
            tournament_fitness = [fitness_values[i] for i in indices]
            
            # Select best from tournament
            if self.config.direction == OptimizationDirection.MINIMIZE:
                best_idx = indices[np.argmin(tournament_fitness)]
            else:
                best_idx = indices[np.argmax(tournament_fitness)]
            
            selected.append(population[best_idx])
        
        return selected
    
    def _crossover(self, parent1: T, parent2: T) -> Tuple[T, T]:
        """Uniform crossover"""
        if np.random.random() > self.config.crossover_rate:
            return parent1, parent2
        
        array1 = self._to_array(parent1)
        array2 = self._to_array(parent2)
        
        # Uniform crossover
        mask = np.random.random(len(array1)) < 0.5
        child1_array = np.where(mask, array1, array2)
        child2_array = np.where(mask, array2, array1)
        
        return self._from_array(child1_array), self._from_array(child2_array)
    
    def _mutate(self, solution: T) -> T:
        """Gaussian mutation"""
        array = self._to_array(solution)
        
        # Add Gaussian noise
        noise = np.random.normal(0, 0.1, size=array.shape)
        mutated = array + noise
        
        # Apply bounds
        if self.config.bounds:
            mutated = self._apply_bounds(mutated)
        
        return self._from_array(mutated)
    
    def _generate_neighbor(self, solution: T) -> T:
        """Generate neighbor solution for simulated annealing"""
        array = self._to_array(solution)
        
        # Random perturbation
        perturbation = np.random.normal(0, 0.1, size=array.shape)
        neighbor = array + perturbation
        
        # Apply bounds
        if self.config.bounds:
            neighbor = self._apply_bounds(neighbor)
        
        return self._from_array(neighbor)
    
    async def postprocess(self, result: T) -> T:
        """Create optimization result"""
        opt_result = OptimizationResult[T](
            algorithm_name=self.config.name,
            status=self._state.converged and "completed" or "failed",
            optimal_solution=self._state.best_solution,
            optimal_value=self._state.best_value,
            converged=self._state.converged,
            convergence_reason=self._state.convergence_reason,
            num_evaluations=self._eval_count,
            num_gradient_evaluations=self._grad_eval_count,
            solution_history=self._state.solution_history,
            value_history=self._state.value_history,
            iterations=self._state.iteration
        )
        
        # Check constraints
        if self.config.constraints:
            for constraint in self.config.constraints:
                violation = constraint.function(self._state.best_solution)
                if abs(violation) > constraint.tolerance:
                    opt_result.constraint_violations[constraint.name] = violation
                    opt_result.feasible = False
        
        return self._state.best_solution


__all__ = [
    "OptimizationAlgorithm",
    "OptimizationConfig",
    "OptimizationResult",
    "OptimizationMethod",
    "ConstraintType",
    "OptimizationDirection",
    "Constraint",
    "OptimizationState"
] 